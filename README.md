# EDO Analytics VKR

Система сквозной real-time аналитики корпоративного документооборота (ЭДО) на основе ClickHouse.

## О проекте

Проект предназначен для приёма, хранения и аналитической обработки событий электронного документооборота в режиме, близком к реальному времени.

Kafka используется для передачи событий, ClickHouse — для распределённого хранения и аналитических запросов, а Superset предусмотрен для визуализации данных.

## Архитектура

```text
Генератор событий (Python)
          |
          v
        Kafka
          |
          v
  ClickHouse Kafka Engine
          |
          v
Materialized View
          |
          v
Distributed table
          |
          v
  ClickHouse cluster
          |
          v
       Superset
```

ZooKeeper используется для координации репликации ClickHouse в текущей конфигурации.

## Используемые технологии

- **Python** — генератор событий ЭДО (планируется).
- **Apache Kafka** — брокер потоковых событий.
- **ClickHouse** — распределённое аналитическое хранилище.
- **ZooKeeper** — координация репликации ClickHouse.
- **Apache Superset** — визуализация данных.
- **Docker Compose** — запуск сервисов в контейнерах.

## Конфигурация ClickHouse

Кластер `edo_cluster` включает два шарда с двумя репликами в каждом.

| Шард | Реплика 1 | Реплика 2 |
|---|---|---|
| 1 | `ch-shard1-replica1` | `ch-shard1-replica2` |
| 2 | `ch-shard2-replica1` | `ch-shard2-replica2` |

Основная база данных: `edo_analytics`.

Основные таблицы:

- `document_events_local` — локальная таблица на движке `ReplicatedMergeTree`.
- `document_events` — распределённая таблица на движке `Distributed`.
- `kafka_document_events` — источник событий на движке Kafka.
- `kafka_document_events_mv` — материализованное представление для загрузки событий в распределённую таблицу.

Распределение данных между шардами выполняется по хешу `DocumentId`.

## Модель события

| Поле | Тип ClickHouse | Назначение |
|---|---|---|
| `EventTime` | `DateTime` | Время возникновения события |
| `EventId` | `UUID` | Идентификатор события |
| `DocumentId` | `UUID` | Идентификатор документа |
| `RegionId` | `LowCardinality(String)` | Регион |
| `BranchId` | `LowCardinality(String)` | Филиал |
| `EventType` | `LowCardinality(String)` | Тип события |
| `DocumentType` | `LowCardinality(String)` | Тип документа |
| `PayloadSize` | `UInt64` | Размер полезной нагрузки |
| `DurationMs` | `UInt32` | Длительность операции в миллисекундах |
| `IngestTime` | `DateTime64(3)` | Время загрузки в ClickHouse |

Поле `IngestTime` формируется на стороне ClickHouse при обработке события материализованным представлением.

## Требования

Для запуска необходимы:

- Docker Engine;
- Docker Compose;
- Git.

## Запуск

Проверка конфигурации:

```bash
docker compose config
```

Запуск сервисов:

```bash
docker compose up -d
```

Проверка состояния контейнеров:

```bash
docker compose ps
```

Порты и параметры подключения указаны в `docker-compose.yml`.

## Kafka

Для передачи событий используется топик `edo-documents`. Его создание автоматизировано сервисом `kafka-init`.

Текущие параметры топика:

- число партиций: `1`;
- replication factor: `1`.


## Текущее состояние проекта

Реализовано на уровне конфигурации:

- Docker Compose для основных сервисов;
- кластер ClickHouse из двух шардов с двумя репликами;
- конфигурационные файлы кластера и макросов репликации;
- таблицы ClickHouse для событий ЭДО;
- интеграция Kafka с ClickHouse через Kafka Engine и материализованное представление;
- автоматизированное создание топика Kafka.

**Следующие задачи:**

- реализовать генератор событий на Python;
- проверить полный путь события от Kafka до ClickHouse;
- подготовить аналитические запросы;
- настроить и проверить визуализации в Superset.

