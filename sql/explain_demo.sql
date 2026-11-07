-- Демонстрация влияния индекса. НЕ входит в DAG, запускается вручную.
-- Откройте psql (docker compose exec warehouse psql -U dwh -d warehouse)
-- и вставляйте блоки по одному.
-- Запишите время выполнения (Execution Time) до и после, они пойдут в README.

-- 1. Берём клиента, у которого есть заказы
SELECT customer_key, COUNT(*) AS orders FROM dwh.fact_orders GROUP BY 1 ORDER BY 2 DESC LIMIT 1;
-- Подставьте найденный customer_key вместо 1 в запросах ниже.

-- 2. С индексом idx_fact_orders_customer
EXPLAIN (ANALYZE, BUFFERS)
SELECT order_id, status, payment_amount
FROM dwh.fact_orders
WHERE customer_key = 1;

-- 3. Без индекса
DROP INDEX dwh.idx_fact_orders_customer;

EXPLAIN (ANALYZE, BUFFERS)
SELECT order_id, status, payment_amount
FROM dwh.fact_orders
WHERE customer_key = 1;

-- 4. Возвращаем индекс на место
CREATE INDEX idx_fact_orders_customer ON dwh.fact_orders (customer_key);
ANALYZE dwh.fact_orders;

-- Что смотреть в выводе:
--   "Index Scan" / "Bitmap Index Scan" - индекс используется
--   "Seq Scan"                         - таблица читается целиком
--   "Execution Time"                   - общее время выполнения запроса