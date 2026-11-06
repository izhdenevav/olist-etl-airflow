CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS mart;

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