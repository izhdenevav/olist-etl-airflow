-- Слои хранилища:
--   stg  - "сырые" данные из CSV как есть
--   dwh  - очищенные данные в виде схемы "звезда" (факты и измерения)
--   mart - витрины для аналитики
--   etl  - служебные таблицы: журнал загрузок и результаты проверок качества

CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS etl;

-- ---------- stg ----------
CREATE TABLE IF NOT EXISTS stg.orders (
    order_id                       text,
    customer_id                    text,
    order_status                   text,
    order_purchase_timestamp       timestamp,
    order_approved_at              timestamp,
    order_delivered_carrier_date   timestamp,
    order_delivered_customer_date  timestamp,
    order_estimated_delivery_date  timestamp
);

CREATE TABLE IF NOT EXISTS stg.order_items (
    order_id             text,
    order_item_id        integer,
    product_id           text,
    seller_id            text,
    shipping_limit_date  timestamp,
    price                numeric(12, 2),
    freight_value        numeric(12, 2)
);

CREATE TABLE IF NOT EXISTS stg.order_payments (
    order_id              text,
    payment_sequential    integer,
    payment_type          text,
    payment_installments  integer,
    payment_value         numeric(12, 2)
);

CREATE TABLE IF NOT EXISTS stg.customers (
    customer_id              text,
    customer_unique_id       text,
    customer_zip_code_prefix text,
    customer_city            text,
    customer_state           text
);

CREATE TABLE IF NOT EXISTS stg.products (
    product_id                  text,
    product_category_name       text,
    product_name_lenght         numeric,
    product_description_lenght  numeric,
    product_photos_qty          numeric,
    product_weight_g            numeric,
    product_length_cm           numeric,
    product_height_cm           numeric,
    product_width_cm            numeric
);

CREATE TABLE IF NOT EXISTS stg.category_translation (
    product_category_name          text,
    product_category_name_english  text
);

-- ---------- etl ----------
-- сколько строк загрузили в каждую таблицу при каждом запуске
CREATE TABLE IF NOT EXISTS etl.load_log (
    id           bigserial PRIMARY KEY,
    dag_run_id   text,
    table_name   text,
    rows_loaded  bigint,
    loaded_at    timestamptz NOT NULL DEFAULT now()
);

-- результаты проверки
CREATE TABLE IF NOT EXISTS etl.dq_results (
    id          bigserial PRIMARY KEY,
    dag_run_id  text,
    layer       text,
    check_name  text,
    bad_rows    bigint,
    severity    text,   -- error: останавливает пайплайн, warn: только предупреждение
    status      text,   -- OK / FAILED / WARN
    checked_at  timestamptz NOT NULL DEFAULT now()
);