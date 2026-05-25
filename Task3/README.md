# Task 3. Данные, масштабирование и отказоустойчивость

Раздел описывает стратегию работы с данными в микросервисной архитектуре AdScale: выбор типа БД для каждого сервиса, стратегии масштабирования и кэширования, потоковую обработку через Kafka и обеспечение отказоустойчивости (RPO/RTO + backup + 12-factor).

---

## Структура

```text
Task3/
├── README.md              ← этот файл
├── database-strategy.md   ← выбор БД для каждого сервиса (DB-per-service)
├── scaling.md             ← репликация, шардирование, read replicas, CQRS
├── caching.md             ← Redis Cluster, TTL, инвалидация, прогрев
├── event-streaming.md     ← Kafka: топики, схемы Avro, consumer groups, retention
├── failover.md            ← RPO/RTO, backup, 12-factor compliance
└── diagrams/
    ├── data-architecture-overview.puml  ← обзор хранилищ и владельцев
    └── cdc-and-event-flow.puml          ← CDC + impression flow + recovery
```

---

## Базы данных per service

| Сервис | Тип БД |
|---|---|
| Bidding Service | Redis Cluster (hot cache) + read-replica Campaign PostgreSQL |
| Campaign Service | PostgreSQL HA (primary + sync replica) |
| Statistics Service | Kafka (буфер) → ClickHouse (sink) |
| Analytics Service | ClickHouse (read-only OLAP) |
| Financial Service | Dedicated PostgreSQL HA |

Подробное обоснование — в [database-strategy.md](./database-strategy.md).

---

## Ключевые архитектурные решения

| # | Решение | Где |
|---|---|---|
| 1 | DB-per-service: на запись — один владелец; межсервисный обмен через CDC и доменные события. Явное исключение — read-only fallback Bidding на read-replica Campaign PostgreSQL для удержания SLA hot path (см. компенсирующие меры) | [database-strategy.md §1](./database-strategy.md) |
| 2 | Разделение OLTP (PostgreSQL для Campaign/Financial) и OLAP (ClickHouse для Analytics) — снимает главное узкое место AS-IS | [database-strategy.md §1](./database-strategy.md) |
| 3 | Master-Slave + read replica для Campaign и Financial PostgreSQL; шардирование откладывается до реальной потребности | [scaling.md §2, §3](./scaling.md) |
| 4 | Шардирование Redis по `campaign_id` / `user_id`; Kafka — партиционирование по тем же ключам; ClickHouse — по дате | [scaling.md §3](./scaling.md) |
| 5 | CQRS уже работает по факту: Bidding читает Redis-проекцию, Campaign Service пишет в PostgreSQL | [scaling.md §5](./scaling.md) |
| 6 | Cache Aside + гибридная инвалидация (CDC по событию + TTL fallback) + warming-job при старте подов | [caching.md](./caching.md) |
| 7 | Kafka как event backbone + source of truth; Avro со Schema Registry в режиме BACKWARD_TRANSITIVE | [event-streaming.md](./event-streaming.md) |
| 8 | RPO=0 только для Financial (sync replica); остальные — eventual recovery через replay из Kafka | [failover.md §2](./failover.md) |
| 9 | Все микросервисы соответствуют 12-factor + дополнительные практики (idempotency, CDC, multi-region) | [failover.md §5](./failover.md) |

---

## Ответы на ключевые вопросы

**Нужны ли read replicas для аналитики?**
Да, но не классическая PostgreSQL read-replica. Аналитика идёт в **отдельную OLAP-инфраструктуру ClickHouse**, заполняемую через CDC из OLTP и stream-агрегаты из Kafka. Read-replica PostgreSQL остаётся только для операционных нужд UI (просмотр одной кампании). Подробнее — [scaling.md §4](./scaling.md).

**Где применяется CQRS?**
Главная точка — Bidding ↔ Campaign: write через Campaign Service в PostgreSQL primary, read через Redis-кэш (проекция, синхронизированная через CDC). Частичный CQRS в Financial: writes в primary, reads в ClickHouse-проекции для отчётов. Подробнее — [scaling.md §5](./scaling.md).

**Что и как кэшировать?**
Кампании, бюджеты, индексы таргетинга, freq capping счётчики, креативы-метаданные — всё в Redis Cluster. TTL дифференцирован по частоте изменений: короткий (бюджеты), средний (кампании, креативы), длинный (сегменты, freq capping, сессии). Инвалидация гибридная: CDC по событию + TTL fallback + версия для конкурентных обновлений. Прогрев — warming-job в initContainer с batch-load из БД через rate-limit. Подробнее — [caching.md](./caching.md).

**Что в Kafka — топики и формат?**
Доменные топики (`bidding.bid_won.v1`, `delivery.impression.v1`, `delivery.click.v1`, `billing.transaction.v1`) + CDC из PostgreSQL (`cdc.campaign.v1`, `cdc.budget.v1`, `cdc.creative.v1`) + DLQ для каждого.

Сериализация — **Avro** через Schema Registry: схема событий хранится централизованно, в Kafka летит только её ID + бинарные данные. Перед регистрацией новой версии схемы Registry проверяет, что она обратно совместима со всеми старыми версиями в окне retention — это блокирует выкатку ломающих изменений ещё на стадии CI.

Каждый сервис, читающий поток, имеет **свою consumer-группу** с независимым offset: Statistics, Financial, Bidding (в роли consumer для обновления Redis-кэша), Analytics-warehouse-sink (загрузка событий в ClickHouse). Сбой или отставание одной группы не влияет на другие. Подробнее — [event-streaming.md](./event-streaming.md).

**Как обеспечена отказоустойчивость?**

Допустимые потери данных и время простоя дифференцированы по сервисам:

- **Financial** — нельзя терять ни одной финансовой операции. PostgreSQL с синхронной репликацией на резервный узел: каждая запись подтверждается обоими узлами, и при падении основного резервный гарантированно содержит все данные.
- **Campaign / Statistics / Bidding** — допустимо потерять минуты данных. Восстановление через перечитку событий из Kafka: Kafka хранит лог событий несколько дней, любые проекции (агрегаты в ClickHouse, счётчики в Redis) пересобираются заново. Redis-кэш дополнительно восстанавливается из БД по мере поступления запросов.

**Резервные копии:**

- PostgreSQL — непрерывное архивирование журнала изменений (WAL) в Object Storage, что позволяет восстановить состояние БД на любой момент времени в окне хранения.
- Kafka — встроенная репликация на 3 узла плюс автоматический перенос старых сегментов в Object Storage, что снимает необходимость классических backup'ов.
- ClickHouse — репликация шардов плюс утилита `clickhouse-backup`. Основной механизм восстановления — повторное чтение из Kafka, потому что ClickHouse у нас проекция, а не источник правды.

**Сценарии отказа:**

- На 3-мес фазе — все компоненты в нескольких зонах доступности одного региона; падение зоны не валит сервис.
- На 12-мес фазе добавляется развёртывание в нескольких регионах для защиты от катастрофического отказа региона.

Все микросервисы следуют принципам 12-factor app — это базовая планка для корректного масштабирования и развёртывания в Kubernetes. Подробнее — [failover.md](./failover.md).
