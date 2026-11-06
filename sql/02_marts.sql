-- Витрина 1: выручка по месяцам и категориям товаров + нарастающий итог по категории. Берём только доставленные заказы.
DROP TABLE IF EXISTS mart.monthly_revenue;

CREATE TABLE mart.monthly_revenue AS
WITH delivered_items AS (
    SELECT
        o.order_id,
        date_trunc('month', o.order_purchase_timestamp)::date AS month,
        COALESCE(p.product_category_name, 'unknown')          AS category,
        i.price
    FROM stg.orders AS o
    JOIN stg.order_items AS i ON i.order_id = o.order_id
    LEFT JOIN stg.products AS p ON p.product_id = i.product_id
    WHERE o.order_status = 'delivered'
),
monthly AS (
    SELECT
        month,
        category,
        COUNT(DISTINCT order_id) AS orders_cnt,
        SUM(price)               AS revenue
    FROM delivered_items
    GROUP BY month, category
)
SELECT
    month,
    category,
    orders_cnt,
    revenue,
    SUM(revenue) OVER (PARTITION BY category ORDER BY month) AS revenue_running_total
FROM monthly
ORDER BY month, category;


-- Витрина 2: доставка по месяцам: доля заказов, пришедших позже обещанной даты, и среднее время доставки в днях.
DROP TABLE IF EXISTS mart.delivery_delays;

CREATE TABLE mart.delivery_delays AS
SELECT
    date_trunc('month', order_purchase_timestamp)::date AS month,
    COUNT(*)                                            AS delivered_orders,
    COUNT(*) FILTER (
        WHERE order_delivered_customer_date > order_estimated_delivery_date
    )                                                   AS late_orders,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE order_delivered_customer_date > order_estimated_delivery_date
        ) / COUNT(*), 2
    )                                                   AS late_share_pct,
    ROUND((AVG(
        EXTRACT(EPOCH FROM (order_delivered_customer_date - order_purchase_timestamp)) / 86400
    ))::numeric, 1)                                     AS avg_delivery_days
FROM stg.orders
WHERE order_status = 'delivered'
  AND order_delivered_customer_date IS NOT NULL
GROUP BY 1
ORDER BY 1;