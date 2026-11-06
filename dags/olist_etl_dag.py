"""ETL-пайплайн по датасету Olist (интернет-магазин, Бразилия).

Шаги:
  1. create_tables       - создать схемы и таблицы (если их ещё нет)
  2. load_stg            - загрузить 3 CSV-файла в слой stg
  3. check_data_quality  - проверить качество данных, при проблеме задача падает
  4. build_marts         - построить витрины в слое mart
"""
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "warehouse"
DATA_DIR = "/opt/airflow/data"

TABLES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "products": "olist_products_dataset.csv",
}

QUALITY_CHECKS = [
    ("orders не пустая", "SELECT (COUNT(*) = 0)::int FROM stg.orders"),
    ("order_items не пустая", "SELECT (COUNT(*) = 0)::int FROM stg.order_items"),
    ("products не пустая", "SELECT (COUNT(*) = 0)::int FROM stg.products"),
    ("orders: order_id не NULL", "SELECT COUNT(*) FROM stg.orders WHERE order_id IS NULL"),
    (
        "orders: order_id уникален",
        "SELECT COUNT(*) FROM (SELECT order_id FROM stg.orders "
        "GROUP BY order_id HAVING COUNT(*) > 1) AS t",
    ),
    (
        "order_items: цена больше 0",
        "SELECT COUNT(*) FROM stg.order_items WHERE price IS NULL OR price <= 0",
    ),
]


def load_stg():
    """Загружает CSV в stg. Перед загрузкой таблицы очищаются, поэтому повторный запуск не создаёт дубликаты. Всё выполняется в одной транзакции: если что-то упало, старые данные остаются нетронутыми."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            for table, filename in TABLES.items():
                cur.execute(f"TRUNCATE TABLE stg.{table}")
                with open(f"{DATA_DIR}/{filename}", "r", encoding="utf-8") as f:
                    cur.copy_expert(
                        f"COPY stg.{table} FROM STDIN WITH (FORMAT csv, HEADER true)", f
                    )
                cur.execute(f"SELECT COUNT(*) FROM stg.{table}")
                logging.info("stg.%s: загружено строк: %s", table, cur.fetchone()[0])
        conn.commit()
    finally:
        conn.close()


def check_data_quality():
    """Запускает проверки качества. Если хотя бы одна провалена, задача падает."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    failed = []
    for name, sql in QUALITY_CHECKS:
        bad_rows = hook.get_first(sql)[0]
        if bad_rows == 0:
            logging.info("OK: %s", name)
        else:
            logging.error("ПРОВАЛЕНО: %s (плохих строк: %s)", name, bad_rows)
            failed.append(name)
    if failed:
        raise AirflowFailException("Проверки качества провалены: " + "; ".join(failed))


with DAG(
    dag_id="olist_etl",
    description="CSV Olist -> stg -> проверки -> витрины mart",
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

    load = PythonOperator(task_id="load_stg", python_callable=load_stg)

    checks = PythonOperator(task_id="check_data_quality", python_callable=check_data_quality)

    build_marts = SQLExecuteQueryOperator(
        task_id="build_marts",
        conn_id=CONN_ID,
        sql="02_marts.sql",
    )

    create_tables >> load >> checks >> build_marts