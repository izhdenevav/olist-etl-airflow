# Olist ETL: Airflow + PostgreSQL

Учебный ETL-пайплайн по открытому датасету [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce): загрузка CSV в PostgreSQL, проверки качества данных и построение витрин. Оркестрация в Apache Airflow, запуск через Docker Compose.

## Что делает пайплайн

```mermaid
flowchart LR
    A[create_tables] --> B[load_stg]
    B --> C[check_data_quality]
    C --> D[build_marts]
```

1. **create\_tables**: создаёт схемы `stg` и `mart` и таблицы слоя `stg`.
2. **load\_stg**: загружает 3 CSV-файла (`orders`, `order_items`, `products`) в `stg` командой `COPY`. Перед загрузкой таблицы очищаются, поэтому повторный запуск не создаёт дубликаты, а вся загрузка идёт в одной транзакции.
3. **check\_data\_quality**: 6 проверок (таблицы не пустые, `order_id` не NULL и уникален, цена больше 0). Если проверка не прошла, задача падает и витрины не обновляются.
4. **build\_marts**: пересоздаёт две витрины на SQL.

## Слои хранилища


| Схема | Назначение                                                 |
| ---------- | -------------------------------------------------------------------- |
| `stg`      | сырые данные из CSV без преобразований |
| `mart`     | витрины для аналитики                             |

## Витрины

* `mart.monthly_revenue`: выручка по месяцам и категориям товаров для доставленных заказов, плюс нарастающий итог по категории (оконная функция `SUM() OVER`).
* `mart.delivery_delays`: по месяцам число доставленных заказов, доля заказов, пришедших позже обещанной даты, среднее время доставки в днях.

![1793942032043](images/README/1793942032043.png)

## Результат запуска

![1793941869809](images/README/1793941869809.png)


| Таблица        | Строк |
| --------------------- | ---------- |
| stg.orders            | 99441      |
| stg.order\_items      | 112650     |
| stg.products          | 32951      |
| mart.monthly\_revenue | 1273       |
| mart.delivery\_delays | 23         |

## Как запустить

Нужны Docker Desktop и около 4 ГБ свободной памяти.

1. Скачайте с Kaggle датасет Olist и положите в папку `data/` три файла: `olist_orders_dataset.csv`, `olist_order_items_dataset.csv`, `olist_products_dataset.csv`.
2. Запустите контейнеры:
   ```bash
   docker compose up -d
   ```
3. Получите пароль пользователя `admin`:
   ```bash
   docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
   ```
4. Откройте http://localhost:8080, войдите как `admin` и запустите DAG `olist_etl` кнопкой ▶.
5. Результат смотрите в базе (порт 5433, пользователь `dwh`, пароль `dwh`, база `warehouse`):
   ```bash
   docker compose exec warehouse psql -U dwh -d warehouse
   ```

Остановить: `docker compose stop`. Удалить всё вместе с данными: `docker compose down -v`.

## Стек

Apache Airflow 2.10, PostgreSQL 16, Python 3.12, SQL, Docker Compose.

## Ограничения

* Запуск ручной (`schedule=None`), потому что данные статичные.
* Метаданные Airflow хранятся в SQLite, а исполнитель последовательный. Для учебного проекта этого достаточно, для продакшена нужен отдельный сервер БД для метаданных.
* Загрузка полная (очистка и заливка заново), инкрементальной загрузки нет.
* Пароли в `docker-compose.yml` учебные.
