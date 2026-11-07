-- Слой dwh: схема "звезда".
--   измерения: dim_date, dim_customer, dim_product
--   факты:     fact_orders (одна строка = один заказ),
--              fact_order_items (одна строка = одна позиция заказа)

DROP TABLE IF EXISTS dwh.fact_order_items;
DROP TABLE IF EXISTS dwh.fact_orders;
DROP TABLE IF EXISTS dwh.dim_date;
DROP TABLE IF EXISTS dwh.dim_customer;
DROP TABLE IF EXISTS dwh.dim_product;

-- ---------- измерения ----------
CREATE TABLE dwh.dim_date (
    date_key     integer PRIMARY KEY,          -- формат ГГГГММДД
    full_date    date NOT NULL,
    year         integer NOT NULL,
    quarter      integer NOT NULL,
    month        integer NOT NULL,
    month_name   text NOT NULL,
    day_of_week  integer NOT NULL,             -- 1 = понедельник
    is_weekend   boolean NOT NULL
);

INSERT INTO dwh.dim_date
SELECT
    to_char(d, 'YYYYMMDD')::integer,
    d::date,
    EXTRACT(YEAR    FROM d)::integer,
    EXTRACT(QUARTER FROM d)::integer,
    EXTRACT(MONTH   FROM d)::integer,
    to_char(d, 'FMMonth'),
    EXTRACT(ISODOW  FROM d)::integer,
    EXTRACT(ISODOW  FROM d) IN (6, 7)
FROM generate_series(
    (SELECT MIN(order_purchase_timestamp)::date FROM stg.orders),
    (SELECT GREATEST(MAX(order_estimated_delivery_date), MAX(order_delivered_customer_date))::date FROM stg.orders),
    interval '1 day'
) AS d;

CREATE TABLE dwh.dim_customer (
    customer_key        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id         text NOT NULL UNIQUE,
    customer_unique_id  text NOT NULL,
    zip_code_prefix     text,
    city                text,
    state               text
);

-- Очистка: убираем пробелы, приводим регистр (город: Sao Paulo, штат: SP)
INSERT INTO dwh.dim_customer (customer_id, customer_unique_id, zip_code_prefix, city, state)
SELECT DISTINCT ON (customer_id)
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    initcap(trim(customer_city)),
    upper(trim(customer_state))
FROM stg.customers
ORDER BY customer_id;

CREATE TABLE dwh.dim_product (
    product_key    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id     text NOT NULL UNIQUE,
    category_pt    text,
    category_en    text NOT NULL,
    weight_g       numeric,
    photos_qty     numeric
);

-- Название категории на английском берём из справочника;
-- если перевода нет, используем португальское, если нет и его - 'unknown'
INSERT INTO dwh.dim_product (product_id, category_pt, category_en, weight_g, photos_qty)
SELECT DISTINCT ON (p.product_id)
    p.product_id,
    p.product_category_name,
    COALESCE(t.product_category_name_english, p.product_category_name, 'unknown'),
    p.product_weight_g,
    p.product_photos_qty
FROM stg.products AS p
LEFT JOIN stg.category_translation AS t
       ON t.product_category_name = p.product_category_name
ORDER BY p.product_id;

-- ---------- факты ----------
CREATE TABLE dwh.fact_orders (
    order_id           text PRIMARY KEY,
    customer_key       bigint REFERENCES dwh.dim_customer (customer_key),
    date_key_purchase  integer REFERENCES dwh.dim_date (date_key),
    status             text NOT NULL,
    purchase_ts        timestamp,
    approved_ts        timestamp,
    delivered_ts       timestamp,
    estimated_ts       timestamp,
    delivery_days      numeric(8, 1),
    is_late            boolean,
    items_cnt          integer NOT NULL,
    items_amount       numeric(14, 2) NOT NULL,
    freight_amount     numeric(14, 2) NOT NULL,
    payment_amount     numeric(14, 2) NOT NULL,
    has_date_anomaly   boolean NOT NULL        -- даты идут в нелогичном порядке
);

-- Позиции и платежи сначала агрегируем по заказу, и только потом присоединяем.
-- Если соединить их напрямую, строки размножатся и суммы удвоятся.
WITH items AS (
    SELECT order_id,
           COUNT(*)           AS items_cnt,
           SUM(price)         AS items_amount,
           SUM(freight_value) AS freight_amount
    FROM stg.order_items
    GROUP BY order_id
),
pay AS (
    SELECT order_id,
           SUM(payment_value) AS payment_amount
    FROM stg.order_payments
    GROUP BY order_id
)
INSERT INTO dwh.fact_orders
SELECT
    o.order_id,
    c.customer_key,
    to_char(o.order_purchase_timestamp, 'YYYYMMDD')::integer,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    CASE WHEN o.order_delivered_customer_date IS NOT NULL
         THEN ROUND(EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp)) / 86400, 1)
    END,
    CASE WHEN o.order_delivered_customer_date IS NOT NULL
         THEN o.order_delivered_customer_date > o.order_estimated_delivery_date
    END,
    COALESCE(i.items_cnt, 0),
    COALESCE(i.items_amount, 0),
    COALESCE(i.freight_amount, 0),
    COALESCE(p.payment_amount, 0),
    COALESCE(
        o.order_delivered_customer_date < o.order_purchase_timestamp
        OR o.order_approved_at < o.order_purchase_timestamp,
        FALSE
    )
FROM stg.orders AS o
LEFT JOIN dwh.dim_customer AS c ON c.customer_id = o.customer_id
LEFT JOIN items AS i ON i.order_id = o.order_id
LEFT JOIN pay   AS p ON p.order_id = o.order_id;

CREATE TABLE dwh.fact_order_items (
    order_id       text    NOT NULL REFERENCES dwh.fact_orders (order_id),
    order_item_id  integer NOT NULL,
    product_key    bigint  REFERENCES dwh.dim_product (product_key),
    seller_id      text,
    price          numeric(12, 2) NOT NULL,
    freight_value  numeric(12, 2) NOT NULL,
    PRIMARY KEY (order_id, order_item_id)
);

INSERT INTO dwh.fact_order_items
SELECT
    i.order_id,
    i.order_item_id,
    p.product_key,
    i.seller_id,
    i.price,
    COALESCE(i.freight_value, 0)
FROM stg.order_items AS i
LEFT JOIN dwh.dim_product AS p ON p.product_id = i.product_id;

-- ---------- индексы под типовые запросы ----------
CREATE INDEX idx_fact_orders_customer ON dwh.fact_orders (customer_key);
CREATE INDEX idx_fact_orders_date     ON dwh.fact_orders (date_key_purchase);
CREATE INDEX idx_fact_orders_status   ON dwh.fact_orders (status);
CREATE INDEX idx_fact_items_product   ON dwh.fact_order_items (product_key);

ANALYZE dwh.dim_date;
ANALYZE dwh.dim_customer;
ANALYZE dwh.dim_product;
ANALYZE dwh.fact_orders;
ANALYZE dwh.fact_order_items;