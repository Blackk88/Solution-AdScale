# Масштабирование баз данных

Связанные документы: [database-strategy.md](./database-strategy.md).

---

## 1. Подход

Целевая нагрузка на 12-мес горизонте — 50 000 RPS bid requests; поток событий impression/click — фракция от этого объёма (только выигранные аукционы с фактическим показом). Один сервер ни часть этого профиля не вытянет, поэтому опираемся на **горизонтальное масштабирование** для всех критичных хранилищ, оставляя вертикальное только для среднеразмерных OLTP-БД (Campaign, Financial).

---

## 2. Master-Slave репликация

Master-Slave (паттерн read-replica) применяется для сервисов с **преобладанием чтения** и требующих высокой доступности.

| Хранилище | Топология | Зачем |
|---|---|---|
| **Campaign PostgreSQL** | primary + sync replica (HA) + async read-replica | HA на случай отказа primary; async-реплика разгружает операционные чтения Advertiser UI; для Bidding она же — источник для warming-job и fallback при cache miss / CB Open на Redis |
| **Financial PostgreSQL** | primary + sync replica (HA) | HA, отсутствие потерь при отказе primary |
| **Redis Cluster** (Bidding cache) | каждый шард = master + replica | failover внутри шарда + распределение reads по replica |
| **ClickHouse** | ReplicatedMergeTree | failover + параллелизм аналитических запросов |

### 2.1. Потоковая и логическая репликация PostgreSQL

PostgreSQL поддерживает два режима:

- **Потоковая (Streaming Replication)** через WAL — для HA primary↔sync replica и для async read-replica. Используется в Campaign DB и Finance DB.
- **Логическая (Logical Replication)** через publication/subscription — для CDC через Debezium → Kafka.

### 2.2. Задержка репликации

При чтении с async-реплики возможны устаревшие данные. Решения:

- **Read-your-writes на critical paths.** После записи в Financial Service сразу читаем баланс с primary — пользователь видит актуальное значение списания.
- **Маршрутизация по типу запроса.** Bidding читает таргетинг из кэша (с read-replica как fallback); Advertiser UI читает кампании из read-replica; запись всегда через primary.
- **Мониторинг lag.** DevOps-команда настраивает алерт на превышение допустимого лага; при критическом расхождении — временное переключение чтений на primary.

### 2.3. Multi-master и multi-region (12-мес фаза)

Multi-master репликация используется только для двух сценариев на 12-мес фазе:

- **Bidding cache (Redis) cross-region** — active-active в двух регионах с гео-маршрутизацией DSP-трафика на ближайший. Конфликты разрешаются last-write-wins (допустимо для eventual cache).
- **Hot standby Financial DB** в другом регионе — async-репликация с возможностью promote при катастрофическом отказе primary региона.

PostgreSQL multi-master внутри одного региона **не используется** — для финансов риск конфликтов слишком велик.

---

## 3. Шардирование

Шардирование применяется там, где даже распределение нагрузки между master-slave недостаточно — обычно для write-heavy потоков и больших OLAP-объёмов.

| Хранилище | Шардируется? | Ключ | Обоснование |
|---|---|---|---|
| **Redis Cluster** | да | hash slot от `campaign_id` (для campaign/budget) или `user_id` (для freqcap-счётчиков) | RPS чтений превышает потолок одной ноды — кластер из нескольких шардов распределяет нагрузку |
| **Kafka** | да (партиционирование) | `campaign_id` для bid/billing-топиков, `user_id` для пользовательских счётчиков | партиции = единица параллелизма consumer'ов; ключ сохраняет порядок per кампания / per user |
| **ClickHouse** | да | по дате (`toYYYYMM(event_date)`) | агрегационные запросы выполняются параллельно по шардам, retention по партициям дешёвый |
| **Campaign PostgreSQL** | нет | — | объём данных умещается на одной ноде; вертикальное масштабирование + read-replica хватает; cross-shard JOIN'ы между кампанией / группой / креативом усложнили бы систему без выгоды |
| **Financial PostgreSQL** | нет | — | финансовый объём данных не требует шардирования; преждевременное шардирование в денежной логике — большой риск |

Конкретное число шардов и стратегия resharding определяются по результатам нагрузочного тестирования, исходя из реальной нагрузки.

---

## 4. Read replicas для Analytics

**Да, обязательно** — но не классический PostgreSQL read-replica, а **отдельная OLAP-инфраструктура** ClickHouse.

Причины:

- Аналитика не должна ходить в production OLTP — это снимает основной риск из AS-IS (heavy SQL запросы блокируют hot path).
- Простая read-replica PostgreSQL не решает проблему производительности агрегатов: те же индексы, та же построчная организация. Для запросов «расход по тысяче кампаний за 30 дней» read-replica просто медленный, а не быстрый.
- ClickHouse даёт колоночное хранилище и параллелизм, под который аналитика рассчитана.

Поток данных в ClickHouse:

1. **CDC из Campaign / Financial PostgreSQL** через Debezium → Kafka → ClickHouse Kafka engine → MergeTree таблица (справочные данные).
2. **Stream-агрегаты** из Statistics Service (Kafka consumer) → ClickHouse (метрики impression/click).
3. **Materialized views** внутри ClickHouse — для предагрегации частых запросов.

Целевая свежесть данных в ClickHouse — единицы минут от момента события за счёт прямого стрима через Kafka.

Дополнительно: async read-replica Campaign PostgreSQL остаётся для **операционных нужд UI** (просмотр одной кампании, история операций), но не для агрегатной аналитики.

---

## 5. CQRS

CQRS применяется там, где **read/write ratio высокий** и операции существенно различаются по характеру.

### 5.1. Bidding ↔ Campaign — полноценный CQRS

| Сторона | Реализация |
|---|---|
| **Command** (write) | Campaign Service пишет в Campaign PostgreSQL primary |
| **Query** (read) | Bidding читает из Redis (hot) или read-replica PostgreSQL (cold). Никогда не пишет в Campaign DB |

Синхронизация: CDC из Campaign DB → Kafka → Bidding-consumer инвалидирует Redis-ключи. Это и есть CQRS с проекциями. Подробности — в [caching.md](./caching.md) и [event-streaming.md](./event-streaming.md).

### 5.2. Financial — частичный CQRS

| Сторона | Реализация |
|---|---|
| **Command** (write) | Financial Service пишет в Finance PostgreSQL primary через ACID-транзакции (биллинг по событиям из Kafka) |
| **Query** (read) | Дашборды и отчёты читают из ClickHouse-проекций; операционные API UI — из read-replica PostgreSQL |

Биллинг — write-heavy в моменты пиковой обработки событий; чтения для UI и аналитики идут в отдельные хранилища.

### 5.3. Где CQRS НЕ применяется

- **Campaign Service** — обычный CRUD UI без массового чтения. Write и read идут через одну БД (primary + sync replica для HA). Усложнение CQRS не оправдано.
- **Analytics Service** — он сам и есть «query side». Без write-стороны на уровне сервиса — все записи приходят через CDC и события из Kafka.
- **Statistics Service** — append-only consumer Kafka, нет «обновлений». CQRS не уместен.

---

## 6. Stateless-сервисы и автоскейлинг

Микросервисы (Bidding, Delivery, Statistics, Financial, Analytics, Campaign Service, Advertiser Dashboard) — **stateless**, поэтому масштабируются горизонтально через Kubernetes HPA. Триггеры автоскейлинга подбираются под профиль сервиса:

- **Hot path (Bidding, Delivery)** — RPS на под + CPU.
- **Kafka consumer'ы (Statistics, Financial, Bidding-counter-consumer)** — Kafka consumer lag.
- **UI / CRUD (Campaign, Analytics)** — RPS + CPU.

Конкретные диапазоны реплик per сервис определяются по результатам нагрузочного тестирования.

Автоскейлинг особенно важен для непредсказуемого профиля нагрузки (сезонные всплески, сбой у конкурента → перевод трафика), который обрабатывается без ручного вмешательства.

---

## 7. Сводная карта решений

| Хранилище | Репликация | Шардирование | Read-replica | CQRS |
|---|---|---|---|---|
| Campaign PostgreSQL | Master-Slave (sync + async) | нет | да (Advertiser UI + warming-job и fallback Bidding) | да (Bidding читает проекцию из Redis) |
| Financial PostgreSQL | Master-Slave (sync) | нет | n/a (отчёты в ClickHouse) | частично |
| Redis Cluster | Master-Slave per шард | по `campaign_id` / `user_id` | да (внутри шарда) | как query-side для Bidding |
| Kafka | Replication factor 3 | партиционирование по ключу | n/a (consumer groups) | как event-bus для CQRS-проекций |
| ClickHouse | ReplicatedMergeTree | по дате | да (любой read со шардов) | как query-side для Analytics |
