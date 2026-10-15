# DE Orders Platform

[English](#english) | [Русский](#русский)

## English

A local Data Engineering portfolio project for an online store. The project combines change data capture (CDC), streaming ingestion, object storage, and analytical data modeling.

Development is incremental. The current milestone implements and verifies CDC ingestion into the Bronze layer.

### Current architecture

```text
PostgreSQL
    │ WAL / logical replication
    ▼
Debezium on Kafka Connect
    │ CDC events
    ▼
Apache Kafka
    │ Spark Structured Streaming
    ▼
RustFS — S3-compatible storage
    └── Bronze / Parquet
```

### Technology stack

| Component | Version | Purpose |
|---|---|---|
| PostgreSQL | 17 | Operational source database |
| Apache Kafka | 4.1.2 | Event transport and retention |
| Debezium | 3.3.1.Final | PostgreSQL CDC |
| Apache Spark | 3.5.6 | Streaming ingestion and batch validation |
| RustFS | 1.0.1 | S3-compatible object storage |
| Docker Compose | — | Local service orchestration |

Spark runs locally inside its container using `local[2]`. Its runtime uses Scala 2.12 and Java 17; application code is written in Python.

### Source database

The operational schema contains:

- `customers`
- `products`
- `orders`
- `order_items`
- `payments`
- `refunds`

Tables have primary keys, foreign keys where applicable, and validation constraints. Monetary values use `numeric(18,2)`. Triggers update `updated_at` on row updates.

A dedicated `debezium` role has replication privileges and read access to the captured tables. The `shop_publication` publication explicitly includes all six tables.

### CDC configuration

The connector uses:

- PostgreSQL `pgoutput`;
- an initial snapshot followed by WAL streaming;
- replication slot `shop_cdc_slot`;
- topic prefix `shop`;
- JSON keys and values;
- string serialization for decimals.

CDC operations include snapshot reads (`r`), inserts (`c`), updates (`u`), and deletes (`d`). Deletes are followed by Kafka tombstones with a non-null key and a null value.

### Bronze layer

The current Spark job archives the `shop.public.customers` topic to:

```text
s3a://bronze/cdc/customers/
```

Each record preserves:

| Field | Description |
|---|---|
| `key` | Kafka key as a string |
| `value` | Original CDC JSON as a string; null for tombstones |
| `topic` | Kafka topic |
| `partition` | Kafka partition |
| `offset` | Offset within the partition |
| `kafka_timestamp` | Kafka record timestamp |
| `ingested_at` | Spark microbatch processing timestamp |
| `ingest_date` | UTC ingestion date used for partitioning |

Data is stored as Parquet, partitioned by `ingest_date`.

The streaming job uses a persistent checkpoint at:

```text
/checkpoints/customers-parquet-v1
```

Spark also maintains `_spark_metadata` under the output path. Both the checkpoint and the sink metadata must be preserved for recovery.

### Verified behavior

The following scenarios have been tested:

- Initial snapshot delivery.
- UPDATE delivery.
- INSERT and DELETE delivery.
- Tombstone preservation.
- Spark recovery from a persistent checkpoint.
- Reading archived Parquet through S3A.
- Recovery after stopping Spark, updating PostgreSQL, and restarting Spark.

After the final recovery test, Bronze contained:

| Metric | Result |
|---|---|
| Total records | 8 |
| Unique `(topic, partition, offset)` combinations | 8 |
| Tombstones | 1 |
| Offsets in the tested partition | 0–7 |

These results validate the tested graceful stop/restart scenario. Crash recovery during an active write and broader failure scenarios have not yet been tested.

### Local access

| Service | Address |
|---|---|
| PostgreSQL | `127.0.0.1:5432` |
| Kafka — host clients | `localhost:19092` |
| Kafka — Compose clients | `kafka:9092` |
| Kafka Connect REST API | `http://localhost:8083` |
| RustFS S3 API | `http://localhost:9000` |
| RustFS Console | `http://localhost:9001` |

Local credentials are configured through `.env`, which is excluded from Git. `.env.example` contains example settings.

The connector configuration references its password through Kafka Connect’s environment ConfigProvider.

### Useful commands

Check Compose configuration:

```bash
docker compose config --quiet
```

Inspect services:

```bash
docker compose ps -a
```

Check connector status:

```bash
curl --fail-with-body \
  http://127.0.0.1:8083/connectors/shop-postgres/status
```

Follow archive job logs:

```bash
docker compose logs --tail=80 -f spark-archive
```

Stop and resume the archive job:

```bash
docker compose stop spark-archive
docker compose start spark-archive
```

Build the Spark image after changing Python code:

```bash
docker compose build spark-archive
```

Source SQL scripts in `infra/postgres/init/` run automatically only when PostgreSQL initializes an empty data directory. Changes to an existing database must be applied separately.

`docker compose down -v` removes project volumes and their stored data.

### Planned architecture

- **Bronze / RustFS:** original CDC and event archive.
- **Silver / ClickHouse:** normalized states, facts, and conformed dimensions.
- **Gold / ClickHouse:** enriched orders OBT, with one row per order.
- **Airflow:** batch orchestration and backfills.
- **dbt:** analytical transformations, tests, and documentation.
- **Monitoring:** pipeline latency, lag, errors, and task health.

These components are planned and are not part of the completed milestone.

---

## Русский

Локальный проект для портфолио Data Engineer на примере интернет-магазина. Проект объединяет захват изменений данных (CDC), потоковую загрузку, объектное хранение и аналитическое моделирование.

Разработка ведётся поэтапно. На текущем этапе реализована и проверена загрузка CDC в слой Bronze.

### Текущая архитектура

```text
PostgreSQL
    │ WAL / логическая репликация
    ▼
Debezium в Kafka Connect
    │ CDC-события
    ▼
Apache Kafka
    │ Spark Structured Streaming
    ▼
RustFS — S3-совместимое хранилище
    └── Bronze / Parquet
```

### Стек

| Компонент | Версия | Назначение |
|---|---|---|
| PostgreSQL | 17 | Операционная база-источник |
| Apache Kafka | 4.1.2 | Передача и хранение событий |
| Debezium | 3.3.1.Final | CDC из PostgreSQL |
| Apache Spark | 3.5.6 | Потоковая загрузка и batch-проверки |
| RustFS | 1.0.1 | S3-совместимое объектное хранилище |
| Docker Compose | — | Локальный запуск сервисов |

Spark работает внутри одного контейнера в режиме `local[2]`. Среда выполнения использует Scala 2.12 и Java 17; код приложения написан на Python.

### Исходная база

Операционная схема содержит таблицы:

- `customers`
- `products`
- `orders`
- `order_items`
- `payments`
- `refunds`

В таблицах определены первичные ключи, необходимые внешние ключи и проверки данных. Денежные значения имеют тип `numeric(18,2)`. Триггеры обновляют `updated_at` при изменении строк.

Отдельная роль `debezium` имеет право репликации и чтения отслеживаемых таблиц. Publication `shop_publication` явно включает все шесть таблиц.

### Настройки CDC

Коннектор использует:

- плагин PostgreSQL `pgoutput`;
- начальный snapshot с последующим чтением WAL;
- replication slot `shop_cdc_slot`;
- префикс топиков `shop`;
- JSON для ключей и значений;
- строковую сериализацию денежных значений.

Поддерживаются snapshot (`r`), INSERT (`c`), UPDATE (`u`) и DELETE (`d`). После удаления поступает Kafka tombstone: сообщение с ключом и значением null.

### Слой Bronze

Текущий Spark-процесс архивирует топик `shop.public.customers` в:

```text
s3a://bronze/cdc/customers/
```

Каждая запись сохраняет:

| Поле | Описание |
|---|---|
| `key` | Ключ Kafka в строковом виде |
| `value` | Исходный CDC JSON; null для tombstone |
| `topic` | Топик Kafka |
| `partition` | Партиция Kafka |
| `offset` | Позиция сообщения внутри партиции |
| `kafka_timestamp` | Время записи Kafka |
| `ingested_at` | Время обработки микробатча Spark |
| `ingest_date` | Дата загрузки в UTC для партиционирования |

Данные записываются в Parquet с партиционированием по `ingest_date`.

Потоковый процесс использует постоянный checkpoint:

```text
/checkpoints/customers-parquet-v1
```

В выходном каталоге Spark также хранит `_spark_metadata`. Checkpoint и метаданные файлового sink необходимо сохранять для восстановления.

### Выполненные проверки

Проверены:

- доставка начального snapshot;
- доставка UPDATE;
- доставка INSERT и DELETE;
- сохранение tombstone;
- восстановление Spark по checkpoint;
- чтение Parquet через S3A;
- восстановление после остановки Spark, изменения PostgreSQL и повторного запуска Spark.

После последней проверки восстановления Bronze содержал:

| Показатель | Результат |
|---|---|
| Всего записей | 8 |
| Уникальных сочетаний `(topic, partition, offset)` | 8 |
| Tombstone | 1 |
| Offsets в проверенной партиции | 0–7 |

Результат подтверждает корректность проверенного сценария штатной остановки и запуска. Аварийное завершение во время записи и другие сценарии отказов пока не проверены.

### Локальные адреса

| Сервис | Адрес |
|---|---|
| PostgreSQL | `127.0.0.1:5432` |
| Kafka — клиенты на компьютере | `localhost:19092` |
| Kafka — клиенты внутри Compose | `kafka:9092` |
| Kafka Connect REST API | `http://localhost:8083` |
| RustFS S3 API | `http://localhost:9000` |
| Консоль RustFS | `http://localhost:9001` |

Локальные учётные данные задаются в `.env`, исключённом из Git. `.env.example` содержит демонстрационные настройки.

Конфигурация коннектора получает пароль через environment ConfigProvider Kafka Connect.

### Полезные команды

Проверка Compose:

```bash
docker compose config --quiet
```

Состояние сервисов:

```bash
docker compose ps -a
```

Статус коннектора:

```bash
curl --fail-with-body \
  http://127.0.0.1:8083/connectors/shop-postgres/status
```

Просмотр логов архиватора:

```bash
docker compose logs --tail=80 -f spark-archive
```

Остановка и запуск архиватора:

```bash
docker compose stop spark-archive
docker compose start spark-archive
```

Пересборка Spark-образа после изменения Python-кода:

```bash
docker compose build spark-archive
```

SQL-файлы из `infra/postgres/init/` автоматически выполняются только при инициализации пустого каталога данных PostgreSQL. Изменения существующей базы применяются отдельно.

Команда `docker compose down -v` удаляет volumes проекта вместе с данными.

### Планируемая архитектура

- **Bronze / RustFS:** архив исходных CDC-событий и пользовательских событий.
- **Silver / ClickHouse:** очищенные состояния, факты и согласованные измерения.
- **Gold / ClickHouse:** обогащённая OBT заказов с одной строкой на заказ.
- **Airflow:** batch-оркестрация и backfill.
- **dbt:** аналитические преобразования, тесты и документация.
- **Мониторинг:** задержки, отставание, ошибки и состояние задач.

Эти компоненты запланированы и пока не входят в завершённый этап проекта.