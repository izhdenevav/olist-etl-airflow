"""ETL-пайплайн по датасету Olist (интернет-магазин, Бразилия), версия 2.

Граф:
  create_tables
      -> load_stg (6 задач, по одной на таблицу)
      -> check_stg      проверки сырых данных
      -> build_dwh      очистка и сборка схемы "звезда"
      -> check_dwh      сверка слоёв (количество строк, суммы)
      -> build_marts    4 витрины

Результаты загрузок и проверок пишутся в таблицы etl.load_log и etl.dq_results.
"""
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

CONN_ID = "warehouse"
DATA_DIR = "/opt/airflow/data"

# таблица в stg -> файл в папке data
TABLES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

# Каждая проверка: (название, SQL, серьёзность).
# SQL возвращает число "плохих" строк. Не 0 = проверка не пройдена.
#   error - пайплайн останавливается
#   warn  - только запись в журнал и в лог
QUALITY_CHECKS = {
    "stg": [
        ("orders не пустая", "SELECT (COUNT(*) = 0)::int FROM stg.orders", "error"),
        ("order_items не пустая", "SELECT (COUNT(*) = 0)::int FROM stg.order_items", "error"),
        ("order_payments не пустая", "SELECT (COUNT(*) = 0)::int FROM stg.order_payments", "error"),
        ("customers не пустая", "SELECT (COUNT(*) = 0)::int FROM stg.customers", "error"),
        ("products не пустая", "SELECT (COUNT(*) = 0)::int FROM stg.products", "error"),
        ("orders: order_id не NULL", "SELECT COUNT(*) FROM stg.orders WHERE order_id IS NULL", "error"),
        (
            "orders: дата покупки не NULL",
            "SELECT COUNT(*) FROM stg.orders WHERE order_purchase_timestamp IS NULL",
            "error",
        ),
        (
            "orders: order_id уникален",
            "SELECT COUNT(*) FROM (SELECT order_id FROM stg.orders GROUP BY order_id HAVING COUNT(*) > 1) AS t",
            "error",
        ),
        (
            "customers: customer_id уникален",
            "SELECT COUNT(*) FROM (SELECT customer_id FROM stg.customers GROUP BY customer_id HAVING COUNT(*) > 1) AS t",
            "error",
        ),
        (
            "order_items: цена больше 0",
            "SELECT COUNT(*) FROM stg.order_items WHERE price IS NULL OR price <= 0",
            "error",
        ),
        (
            "order_payments: сумма платежа не отрицательная",
            "SELECT COUNT(*) FROM stg.order_payments WHERE payment_value IS NULL OR payment_value < 0",
            "error",
        ),
        (
            "orders: у заказа нет клиента в customers",
            "SELECT COUNT(*) FROM stg.orders o LEFT JOIN stg.customers c ON c.customer_id = o.customer_id "
            "WHERE c.customer_id IS NULL",
            "warn",
        ),
    ],
    "dwh": [
        (
            "fact_orders: число строк совпадает с stg.orders",
            "SELECT ABS((SELECT COUNT(*) FROM dwh.fact_orders) - (SELECT COUNT(*) FROM stg.orders))",
            "error",
        ),
        (
            "fact_order_items: число строк совпадает с stg.order_items",
            "SELECT ABS((SELECT COUNT(*) FROM dwh.fact_order_items) - (SELECT COUNT(*) FROM stg.order_items))",
            "error",
        ),
        (
            "сумма цен позиций совпадает со stg",
            "SELECT (ABS(COALESCE((SELECT SUM(price) FROM dwh.fact_order_items), 0) "
            "- COALESCE((SELECT SUM(price) FROM stg.order_items), 0)) > 0.01)::int",
            "error",
        ),
        (
            "сумма платежей совпадает со stg",
            "SELECT (ABS(COALESCE((SELECT SUM(payment_amount) FROM dwh.fact_orders), 0) "
            "- COALESCE((SELECT SUM(payment_value) FROM stg.order_payments), 0)) > 0.01)::int",
            "error",
        ),
        (
            "fact_orders: заказы без клиента",
            "SELECT COUNT(*) FROM dwh.fact_orders WHERE customer_key IS NULL",
            "warn",
        ),
        (
            "fact_orders: заказы с нелогичными датами",
            "SELECT COUNT(*) FROM dwh.fact_orders WHERE has_date_anomaly",
            "warn",
        ),
    ],
}


def load_table(table, filename, run_id):
    """Загружает один CSV в stg. Перед загрузкой таблица очищается, поэтому повторный запуск не создаёт дубликаты. 
    Загрузка и запись в журнал идут в одной транзакции: при ошибке старые данные остаются нетронутыми."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE stg.{table}")
            with open(f"{DATA_DIR}/{filename}", "r", encoding="utf-8-sig") as f:
                cur.copy_expert(f"COPY stg.{table} FROM STDIN WITH (FORMAT csv, HEADER true)", f)
            cur.execute(f"SELECT COUNT(*) FROM stg.{table}")
            rows = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO etl.load_log (dag_run_id, table_name, rows_loaded) VALUES (%s, %s, %s)",
                (run_id, f"stg.{table}", rows),
            )
        conn.commit()
        logging.info("stg.%s: загружено строк: %s", table, rows)
    finally:
        conn.close()


def check_data_quality(layer, run_id):
    """Запускает проверки слоя, пишет результаты в etl.dq_results. Если провалена хотя бы одна проверка уровня error, задача падает."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    failed = []
    for name, sql, severity in QUALITY_CHECKS[layer]:
        bad_rows = hook.get_first(sql)[0]
        if bad_rows == 0:
            status = "OK"
            logging.info("OK: %s", name)
        elif severity == "warn":
            status = "WARN"
            logging.warning("ПРЕДУПРЕЖДЕНИЕ: %s (строк: %s)", name, bad_rows)
        else:
            status = "FAILED"
            logging.error("ПРОВАЛЕНО: %s (плохих строк: %s)", name, bad_rows)
            failed.append(name)
        hook.run(
            "INSERT INTO etl.dq_results (dag_run_id, layer, check_name, bad_rows, severity, status) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            autocommit=True,
            parameters=(run_id, layer, name, bad_rows, severity, status),
        )
    if failed:
        raise AirflowFailException(f"Проверки слоя {layer} провалены: " + "; ".join(failed))


with DAG(
    dag_id="olist_etl",
    description="CSV Olist -> stg -> проверки -> dwh (звезда) -> проверки -> витрины",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    template_searchpath=["/opt/airflow/sql"],
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["etl", "olist"],
) as dag:
    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id=CONN_ID,
        sql="01_create_tables.sql",
    )

    with TaskGroup(group_id="load_stg") as load_stg:
        for table, filename in TABLES.items():
            PythonOperator(
                task_id=f"load_{table}",
                python_callable=load_table,
                op_kwargs={"table": table, "filename": filename, "run_id": "{{ run_id }}"},
            )

    check_stg = PythonOperator(
        task_id="check_stg",
        python_callable=check_data_quality,
        op_kwargs={"layer": "stg", "run_id": "{{ run_id }}"},
    )

    build_dwh = SQLExecuteQueryOperator(
        task_id="build_dwh",
        conn_id=CONN_ID,
        sql="02_dwh.sql",
    )

    check_dwh = PythonOperator(
        task_id="check_dwh",
        python_callable=check_data_quality,
        op_kwargs={"layer": "dwh", "run_id": "{{ run_id }}"},
    )

    build_marts = SQLExecuteQueryOperator(
        task_id="build_marts",
        conn_id=CONN_ID,
        sql="03_marts.sql",
    )

    create_tables >> load_stg >> check_stg >> build_dwh >> check_dwh >> build_marts