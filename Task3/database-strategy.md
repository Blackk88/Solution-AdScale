# Стратегия данных: база на каждый сервис

Связанные документы: [scaling.md](./scaling.md), [caching.md](./caching.md), [event-streaming.md](./event-streaming.md), [failover.md](./failover.md).

---

## 1. Принципы выбора

База данных подбирается под профиль работы конкретного сервиса по четырём критериям:

1. **Модель данных** — реляционная (PostgreSQL), колоночная OLAP (ClickHouse), key-value (Redis), event streaming (Kafka).
2. **Требования к консистентности** — strong (финансы), eventual (аналитика, события).
3. **Профиль нагрузки** — write-heavy / read-heavy / mixed / heavy analytical / event sourcing.
4. **Операционная зрелость команды** — в штате AdScale нет SRE и DB-инженеров. Managed-сервисы выбираются по умолчанию для снятия операционной сложности (patching, replication, backup), но не закрывают разработческую сложность нестандартных технологий. Для технологий с высоким когнитивным порогом (ClickHouse) фиксируются компенсирующие меры — см. соответствующие разделы.

Дополнительные сквозные принципы:

- **Database-per-service.** Владение БД на запись принадлежит одному сервису. Чтение чужой БД допускается только через сервис-владелец, его доменные события или CDC.

  **Исключение — Bidding → Campaign read-replica.** Bidding имеет доступ только на чтение к read-replica Campaign PostgreSQL при промахе кэша и при CB Open на Redis. Сознательный компромисс для удержания SLA hot path 80 ms; альтернатива (синхронный вызов через Campaign Service) добавляет сетевой переход в hot path; альтернатива (no-bid при любом промахе кэша) роняет коэффициент заполнения (fill rate) в инциденте Redis.

  Компенсирующие меры: доступ только на чтение фиксируется на уровне ролей PostgreSQL для Bidding-сервиса; связанность по схеме таблиц `campaigns` / `budgets` / `creatives` зафиксирована в интеграционных тестах Bidding ↔ Campaign DB; изменение этих таблиц требует согласования с командой Bidding.

  Условие пересмотра: при появлении собственной материализованной проекции в Bidding (CDC → отдельное хранилище на чтение) или при росте бюджета задержек hot path выше 150 ms — исключение снимается.
- **Разделение OLTP и OLAP.** Аналитика не ходит в production OLTP — конкуренция запросов снимается через CDC → отдельный OLAP-store.
- **Money в целочисленных копейках** (1 ₽ = 100 коп.). Защита от ошибок floating point.

---

## 2. Сводная таблица

| Сервис | Тип базы данных | Обоснование |
|---|---|---|
| **Bidding Service** | Redis Cluster (hot cache) + read-replica Campaign PostgreSQL | hot path с жёстким бюджетом latency; read-heavy профиль; собственного OLTP-стора нет — Bidding читает source-of-truth Campaign через кэш и реплику |
| **Campaign Service** | PostgreSQL HA (primary + sync replica) | OLTP CRUD кампаний / креативов / таргетинга; mixed workload, strong consistency, источник правды |
| **Statistics Service** | Kafka (буфер событий) → ClickHouse (sink) | append-only поток показов/кликов; нужен replay и retention; колоночное хранилище для агрегатов |
| **Analytics Service** | ClickHouse (read-only) | тяжёлые агрегатные запросы по большим объёмам данных за произвольный период; OLAP-природа — точечные запросы не нужны |
| **Financial Service** | Dedicated PostgreSQL HA (primary + sync replica) | strong consistency для денег; ACID-транзакции; идемпотентные операции; изоляция от других доменов |

---

## 3. Решения по сервисам

### 3.1. Bidding Service

Bidding не владеет собственным OLTP-стором. Работает с горячими сущностями (кампании, бюджеты, индексы таргетинга, счётчики) в Redis Cluster и обращается к read-replica Campaign PostgreSQL только при cache miss. Источник правды для кампаний и лимитов бюджетов — Campaign DB (владелец Campaign Service).

Почему именно так:

- **Hot path requires low latency** — достижимо только in-memory; PostgreSQL даже на read-replica даёт десятки миллисекунд.
- **Read-heavy при высоком RPS** — даже с шардированием PostgreSQL в hot path не вытянет.
- **Eventual consistency приемлема** — бюджеты могут немного «проскальзывать» в окне TTL; защита через жёсткий `bid_ceiling` + короткий TTL + CDC-инвалидация.

Детали структур кэша — в [caching.md](./caching.md).

### 3.2. Campaign Service

Campaign Service — источник правды для кампаний, креативов, таргетинга, лимитов бюджетов. Классическая OLTP-нагрузка: CRUD-операции рекламодателей, транзакционные обновления, целостность через foreign keys.

Почему PostgreSQL:

- **ACID** — рекламодатель не должен увидеть половинчатые обновления кампании.
- **Reference integrity** через foreign keys: кампания → группа → объявление → креатив.
- **Зрелая экосистема** — Debezium для CDC, знакомый команде синтаксис, готовые managed-варианты.
- **JSON-поля** для расширяемой части (например, targeting profile) — гибкость без перехода на NoSQL.

### 3.3. Statistics Service

Statistics — это consumer Kafka (`delivery.impression.v1`, `delivery.click.v1`), который выполняет стрим-агрегацию и записывает результаты в ClickHouse. Собственного OLTP-стора нет.

Почему именно Kafka + ClickHouse:

- **Kafka как источник правды событий** — даёт retention для replay и pub/sub fan-out на несколько independent consumer'ов.
- **ClickHouse колоночный** — агрегаты по большим объёмам выполняются быстрее, чем PostgreSQL для такого профиля.
- **Append-only поток** — нет UPDATE/DELETE на горячем пути, что снимает OLTP-сложности.

Детали топиков Kafka и consumer-групп — в [event-streaming.md](./event-streaming.md).

### 3.4. Analytics Service

Analytics строит отчёты для рекламодателей: показы, клики, CTR, расходы — обычно за дни и недели, с агрегациями по кампаниям / гео / устройствам. Аналитика **не ходит** в production PostgreSQL — это снимает риск каскадной деградации hot path (одна из ключевых проблем AS-IS).

Источник данных для ClickHouse:

- События impression/click через Kafka (потоковые агрегаты Statistics Service).
- CDC из Campaign DB через Debezium → Kafka → ClickHouse (справочные данные).
- События биллинга через Kafka (расходы для финансовых отчётов).

Почему ClickHouse:

- **Columnar storage** — запросы вида «расход по кампании X за период» выполняются за секунды.
- **Native sharding** по дате — горизонтальное масштабирование без внешних слоёв.
- **Materialized views** для предагрегации часто запрашиваемых отчётов.

**Признаваемый риск.** ClickHouse требует специализированной экспертизы (свой SQL-диалект, MergeTree-семейство, моделирование sorting key и partitioning); у команды AdScale её сейчас нет. Managed-сервис снимает операционную часть, разработческая сложность остаётся на команде и закрывается через накопление экспертизы по ходу.

Альтернативы (PostgreSQL для аналитики, TimescaleDB, BigQuery / Snowflake) рассмотрены и отклонены: либо возвращают исходную проблему AS-IS (тяжёлые агрегаты на OLTP), либо проигрывают по write throughput, стоимости или регуляторике. ClickHouse остаётся осознанным выбором с пониманием цены входа.

### 3.5. Financial Service

Financial владеет балансами, транзакциями биллинга, операциями возвратов, реквизитами платежей. Денежные операции — самая критичная зона по консистентности; отдельная БД нужна для изоляции от других доменов.

Почему PostgreSQL и **отдельный кластер**:

- **ACID** — списания/начисления транзакционные, никаких частичных коммитов.
- **Идемпотентность** — уникальный индекс на `event_id` + `ON CONFLICT DO NOTHING` (см. [Task2/reliability.md §5](../Task2/reliability.md)).
- **Изоляция** — отдельный кластер защищает финансы от шумных соседей.
- **Regulatory compliance** — финансовые данные подлежат audit-журналу и шифрованию at-rest; удобнее настраивать на выделенной БД.

---

## 4. Сводная карта потоков данных

```text
Campaign Service ──CRUD──▶ Campaign PostgreSQL ──CDC──▶ Kafka ──▶ Bidding (cache invalidation в Redis)
                                                              └──▶ ClickHouse (справочники)

DSP ──bid──▶ Bidding ──read──▶ Redis cache
                  │
                  └─publish bid_won──▶ Kafka ──▶ Financial Service ──▶ Finance PostgreSQL

Delivery ──impression/click──▶ Kafka ──▶ Statistics Service ──▶ ClickHouse
                                     ├──▶ Financial Service  ──▶ Finance PostgreSQL
                                     └──▶ Bidding Service    ──▶ Redis (обновление счётчиков)
```

Ключевые свойства:

- **Один источник правды на сущность.** Кампании и лимиты бюджетов — в Campaign DB, балансы — в Finance DB, события — в Kafka.
- **Изменения распространяются через CDC и domain events.** Никаких прямых cross-service SQL.
- **Аналитика — только из ClickHouse.** Production OLTP не нагружается тяжёлыми запросами.

---

## 5. Сводный профиль консистентности

| Сервис | Консистентность | Чем обеспечена |
|---|---|---|
| Campaign | strong (ACID) | PostgreSQL primary + sync replica для HA |
| Financial | strong (ACID) | dedicated PostgreSQL HA, идемпотентность через уникальный индекс |
| Bidding | eventual | TTL + CDC-инвалидация Redis + защитный `bid_ceiling` |
| Statistics (события) | at-least-once + dedup | Kafka `acks=all`, Idempotent Consumer на `event_id` |
| Analytics | eventual | stream-агрегация Kafka → ClickHouse |

Стратегии репликации, шардирования и CQRS, обеспечивающие эти свойства — в [scaling.md](./scaling.md).
