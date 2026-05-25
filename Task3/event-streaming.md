# Потоковая обработка событий

Связанные документы: [database-strategy.md](./database-strategy.md), [scaling.md](./scaling.md), [Task1/adr/ADR-003](../Task1/adr/ADR-003-event-streaming-technology.md).

---

## 1. Роль Kafka в архитектуре

Kafka — единый event backbone для всех доменных событий и CDC:

- **Pub/sub** — одно событие читают несколько независимых consumer-групп (impression обрабатывают параллельно Statistics, Financial и Bidding-консьюмер кэша).
- **Source of truth** для проекций — ClickHouse и Redis-кэш Bidding пересобираются из Kafka-лога (event sourcing).
- **Буфер пиков** — Delivery публикует impression-события быстрее, чем downstream-сервисы успевают их обрабатывать; Kafka гасит разницу.
- **Audit trail** — все доменные события и CDC сохраняются в retention для аудита и разбора инцидентов.

---

## 2. Каталог топиков

### 2.1. Доменные события

| Топик | Producer | Ключ партиции | Назначение |
|---|---|---|---|
| `bidding.bid_won.v1` | Bidding Service | `campaign_id` | биллинг (Financial Service) |
| `delivery.impression.v1` | Delivery Service | `campaign_id` | агрегация в ClickHouse (Statistics); биллинг (Financial); обновление Redis-счётчиков (Bidding) |
| `delivery.click.v1` | Delivery Service | `campaign_id` | агрегация в ClickHouse (Statistics); обновление Redis-счётчиков (Bidding) |
| `billing.transaction.v1` | Financial Service | `campaign_id` | финансовая проекция для аналитики |

### 2.2. CDC из OLTP

| Топик | Источник | Ключ партиции |
|---|---|---|
| `cdc.campaign.v1` | Debezium → Campaign PostgreSQL (таблица campaigns) | `campaign_id` |
| `cdc.budget.v1` | Debezium → Campaign PostgreSQL (таблица budgets) | `campaign_id` |
| `cdc.creative.v1` | Debezium → Campaign PostgreSQL (таблица creatives) | `creative_id` |

### 2.3. Dead Letter Queue

Для каждого основного топика поддерживается `*.dlq` для событий, которые consumer не смог обработать после max-attempts. Retention DLQ-топиков длиннее обычного, чтобы дать DevOps-команде время на разбор.

### 2.4. Принципы именования

- `{domain}.{event}.v{N}` — точечная семантика. Версия в имени топика, не в payload, чтобы producer и consumer могли обновляться независимо.
- При несовместимом изменении схемы — новый топик `v2` параллельно с `v1`. Старый депрекируется после миграции consumer'ов.
- CDC-топики префиксуются `cdc.` для отделения от доменных событий.

Конкретное число партиций per топик и значения retention определяются по результатам нагрузочного тестирования, исходя из реальной нагрузки и требований аудита.

---

## 3. Схемы событий

### 3.1. Выбор Avro

Формат сериализации — **Apache Avro** с **Schema Registry** (Confluent / Apicurio). Альтернатива JSON Schema отклонена:

- **Размер.** Avro бинарный — компактнее JSON, что значимо при высоком throughput для сетевого и дискового overhead.
- **Schema evolution с проверкой.** Backward compatibility mode не даёт зарегистрировать схему, которая поломает существующих consumer'ов.
- **Типизация.** Avro генерирует типизированные классы для каждого языка стека — несовместимое изменение детектится в CI.
- **Schema Registry уже в стеке** для Kafka — добавлять JSON-схему было бы дублированием.

### 3.2. Пример: `delivery.impression.v1`

Avro schema (subject `delivery.impression.v1-value` в Schema Registry):

```json
{
  "type": "record",
  "name": "ImpressionEvent",
  "namespace": "adscale.delivery.v1",
  "fields": [
    {"name": "event_id", "type": "string", "doc": "UUID, idempotency key для consumer'ов"},
    {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "bid_request_id", "type": "string"},
    {"name": "campaign_id", "type": "string"},
    {"name": "creative_id", "type": "string"},
    {"name": "user_id", "type": ["null", "string"], "default": null},
    {"name": "dsp_id", "type": "string"},
    {"name": "price_kop", "type": "long", "doc": "цена в копейках"},
    {"name": "currency", "type": "string", "default": "RUB"},
    {"name": "geo", "type": ["null", {"type": "record", "name": "Geo", "fields": [
      {"name": "country", "type": "string"},
      {"name": "region", "type": ["null", "string"], "default": null}
    ]}], "default": null}
  ]
}
```

### 3.3. Правила эволюции схем

- **Добавление поля** — только с `default`, и только в конец списка fields. Backward compatible.
- **Удаление поля** — запрещено. Если поле больше не нужно — помечается deprecated в `doc`, но остаётся в схеме на горизонте времени.
- **Переименование поля** — запрещено. Если нужно — новое поле + deprecation старого.
- **Несовместимое изменение** — новый топик `v2`. Старый продолжает работать.

Schema Registry настраивается в режиме **BACKWARD_TRANSITIVE** compatibility — новый producer работает со всеми старыми consumer'ами в окне retention.

---

## 4. Consumer-группы

Каждый сервис подписывается на нужные топики через свою consumer-group. Группы независимы — у каждой свой offset, своя скорость обработки, свои retries.

| Consumer-группа | Топики | Что делает | Идемпотентность |
|---|---|---|---|
| `statistics-service` | `delivery.impression.v1`, `delivery.click.v1` | стрим-агрегация (1m / 5m / 1h окна) → ClickHouse | dedup по `event_id` через Redis SET |
| `financial-service` | `delivery.impression.v1`, `bidding.bid_won.v1` | конвертация событий в финансовые транзакции | уникальный индекс `event_id` в Finance DB + `ON CONFLICT DO NOTHING` |
| `bidding-cache-updater` | `cdc.campaign.v1`, `cdc.budget.v1`, `cdc.creative.v1`, `delivery.impression.v1`, `delivery.click.v1` | инвалидация Redis-ключей по CDC и обновление счётчиков расходов / freqcap по событиям | DEL/SET идемпотентны по природе; для декрементов — dedup по `event_id` |
| `analytics-warehouse-sink` | все `cdc.*`, `delivery.*`, `bidding.bid_won.v1`, `billing.transaction.v1` | загрузка в ClickHouse через Kafka engine | exactly-once-effective через ClickHouse Materialized Views |

**Competing Consumers внутри группы.** Партиции топика распределяются между instance'ами consumer-сервиса — каждое сообщение обрабатывает ровно один instance. Это даёт горизонтальное масштабирование через увеличение реплик. Порядок сообщений сохраняется в пределах ключа партиции.

**Idempotent Consumer.** Все consumer'ы реализуют корректную обработку повторной доставки (at-least-once — норма). Детали реализации — в [Task2/reliability.md §5](../Task2/reliability.md).

**Consumer lag и автоскейлинг.** Скорость consumer-группы должна успевать за producer'ом, иначе lag растёт. Метрика `kafka.consumer.lag` per группа — основа для алертов и автоскейлинга в Kubernetes (через KEDA Kafka scaler). Конкретные пороги и диапазоны реплик определяются по результатам нагрузочного тестирования.

---

## 5. Политика хранения (retention)

Retention рассчитывается из двух соображений: **окно replay при сбое consumer** и **аудит**.

| Категория топиков | Назначение retention | Пример |
|---|---|---|
| Доменные события (impression/click/bid_won) | окно replay при сбое downstream-сервисов | дни |
| Финансовые события (billing.transaction) | финансовый аудит, требования compliance | недели |
| CDC из OLTP | аудит изменений сущностей, нужен для расследований | недели |
| `*.dlq` | время на разбор DevOps-командой; после — архив в Object Storage | недели |

Конкретные значения в днях/неделях задаются командой на этапе настройки.

**Tiered storage.** Для топиков с длинным retention применяется Tiered Storage Kafka: горячие сегменты на быстром локальном диске брокера, холодные автоматически переезжают в Object Storage (S3-compatible). Это экономит диск без потери возможности replay.

**Compaction для CDC.** CDC-топики настроены с политикой `compact` — Kafka сохраняет только последнее значение для каждого ключа. Это снижает объём диска (промежуточные изменения схлопываются), при этом любой consumer всегда может получить актуальное состояние сущности при rebuild'е проекции.

Для доменных событий — политика `delete` (стандартная по времени): каждое событие важно как факт, а не как «текущее состояние».

---

## 6. Топология кластера

| Параметр | Значение |
|---|---|
| Развёртывание | managed-сервис (AWS MSK / Yandex Managed Kafka — в зависимости от облака) |
| Multi-AZ | да |
| Replication factor | 3 |
| `min.insync.replicas` | 2 |
| `acks` (producer) | all |
| `enable.idempotence` (producer) | true |
| Compression | lz4 (баланс между CPU и размером) |
| Сеть | TLS между producer / consumer / brokers, SASL/SCRAM или mTLS |
| Schema Registry | HA, изолирован от брокеров |
| Kafka Connect (Debezium) | отдельный кластер, не на брокерах |

Managed-вариант снимает с команды без SRE задачи patching, балансировки партиций, рестартов брокеров — это критично при дефиците SRE-экспертизы (TC-06).
