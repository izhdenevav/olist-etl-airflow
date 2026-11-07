-- Витрины строятся из слоя dwh. Во всех учитываются только доставленные заказы.

-- Витрина 1: выручка по месяцам, нарастающий итог и прирост к предыдущему месяцу.
DROP TABLE IF EXISTS mart.monthly_revenue;

CREATE TABLE mart.monthly_revenue AS
WITH monthly AS (
    SELECT
        date_trunc('month', o.purchase_ts)::date AS month,
        COUNT(DISTINCT o.order_id)               AS orders_cnt,
        SUM(i.price)                             AS revenue
    FROM dwh.fact_order_items AS i
    JOIN dwh.fact_orders AS o ON o.order_id = i.order_id
    WHERE o.status = 'delivered'
    GROUP BY 1
)
SELECT
    month,
    orders_cnt,
    revenue,
    SUM(revenue) OVER (ORDER BY month) AS revenue_running_total,
    ROUND(
        100.0 * (revenue - LAG(revenue) OVER (ORDER BY month))
              / NULLIF(LAG(revenue) OVER (ORDER BY month), 0),
        2
    ) AS mom_growth_pct
FROM monthly
ORDER BY month;


-- Витрина 2: топ-3 категорий по выручке в каждом месяце.
DROP TABLE IF EXISTS mart.top_categories_monthly;

CREATE TABLE mart.top_categories_monthly AS
WITH category_revenue AS (
    SELECT
        date_trunc('month', o.purchase_ts)::date AS month,
        p.category_en                            AS category,
        SUM(i.price)                             AS revenue
    FROM dwh.fact_order_items AS i
    JOIN dwh.fact_orders AS o ON o.order_id = i.order_id
    LEFT JOIN dwh.dim_product AS p ON p.product_key = i.product_key
    WHERE o.status = 'delivered'
    GROUP BY 1, 2
),
ranked AS (
    SELECT
        month,
        category,
        revenue,
        RANK() OVER (PARTITION BY month ORDER BY revenue DESC) AS revenue_rank
    FROM category_revenue
)
SELECT month, category, revenue, revenue_rank
FROM ranked
WHERE revenue_rank <= 3
ORDER BY month, revenue_rank;


-- Витрина 3: доставка по штатам: доля опозданий и среднее время доставки.
-- Заказы с нелогичными датами в расчёт не берём.
DROP TABLE IF EXISTS mart.delivery_by_state;

CREATE TABLE mart.delivery_by_state AS
SELECT
    c.state,
    COUNT(*)                                          AS delivered_orders,
    COUNT(*) FILTER (WHERE o.is_late)                 AS late_orders,
    ROUND(100.0 * COUNT(*) FILTER (WHERE o.is_late) / COUNT(*), 2) AS late_share_pct,
    ROUND(AVG(o.delivery_days), 1)                    AS avg_delivery_days
FROM dwh.fact_orders AS o
JOIN dwh.dim_customer AS c ON c.customer_key = o.customer_key
WHERE o.status = 'delivered'
  AND o.delivered_ts IS NOT NULL
  AND NOT o.has_date_anomaly
GROUP BY c.state
ORDER BY late_share_pct DESC;


-- Витрина 4: RFM-сегментация клиентов.
--   recency   - сколько дней прошло с последней покупки (на дату последнего заказа в данных)
--   frequency - число заказов
--   monetary  - сумма платежей
DROP TABLE IF EXISTS mart.customer_rfm;

CREATE TABLE mart.customer_rfm AS
WITH customer_orders AS (
    SELECT
        c.customer_unique_id,
        COUNT(*)                   AS orders_cnt,
        SUM(o.payment_amount)      AS total_spent,
        MAX(o.purchase_ts)::date   AS last_purchase_date
    FROM dwh.fact_orders AS o
    JOIN dwh.dim_customer AS c ON c.customer_key = o.customer_key
    WHERE o.status = 'delivered'
    GROUP BY c.customer_unique_id
),
with_recency AS (
    SELECT
        *,
        (SELECT MAX(last_purchase_date) FROM customer_orders) - last_purchase_date AS recency_days
    FROM customer_orders
)
SELECT
    customer_unique_id,
    orders_cnt,
    total_spent,
    last_purchase_date,
    recency_days,
    NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,  -- 5 = покупал недавно
    NTILE(5) OVER (ORDER BY total_spent)       AS m_score,  -- 5 = тратит больше всех
    orders_cnt > 1                             AS is_repeat_customer
FROM with_recency;