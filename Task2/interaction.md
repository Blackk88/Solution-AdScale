# Взаимодействие между сервисами

Связанные документы: [bidding-service.md](./bidding-service.md), [api-gateway.md](./api-gateway.md), [reliability.md](./reliability.md).

---

## 1. Классы коммуникации

В AdScale присутствуют три принципиально разных класса коммуникации, для каждого выбран свой протокол:

| Класс | Примеры | Протокол |
|---|---|---|
| **Внешний (north-south)** | DSP → Gateway → Bidding; Advertiser → Dashboard; Financial → Платёжный шлюз | REST/HTTPS (OpenRTB для DSP, OpenAPI для UI) |
| **Внутренний синхронный (east-west)** | API Gateway → Bidding; Campaign Service → Financial Service | gRPC unary |
| **Внутренний асинхронный** | impressions / clicks → Kafka → Statistics / Financial / Bidding; CDC изменений Campaign / Budget | Kafka (event streaming) |

Обоснование каждого выбора — в §3.

---

## 2. Матрица протоколов

### 2.1. Внешние и внутренние sync-вызовы

| Поток | Протокол |
|---|---|
| DSP → AdScale (bid request / response) | REST/HTTPS (OpenRTB) через Gateway |
| Конечный пользователь → Delivery | HTTPS через Gateway |
| Рекламодатель → Advertiser Dashboard | HTTPS через Gateway |
| Advertiser Dashboard → Campaign Service / Analytics Service | REST/HTTPS + OpenAPI |
| Financial Service → Платёжный шлюз | REST/HTTPS |
| API Gateway → Bidding Service | gRPC unary |
| Campaign Service → Financial Service | gRPC unary |
| Bidding → Redis cache | Redis protocol |
| Bidding → Campaign DB read-replica (cache miss) | JDBC |

Bidding **не делает sync-вызов к Campaign Service** — обоснование в [§3.4](#34-почему-bidding-и-campaign-не-общаются-синхронно).

### 2.2. Событийный поток (Kafka)

| Топик | Producer | Потребители |
|---|---|---|
| `delivery.impression.v1`, `delivery.click.v1` | Delivery Service | Statistics (агрегация → ClickHouse), Financial (биллинг), Bidding (Redis-счётчики) |
| `bidding.bid_won.v1` | Bidding Service | Financial (биллинг) |
| `cdc.campaign.v1`, `cdc.budget.v1` | Debezium из Campaign PostgreSQL | Bidding (инвалидация Redis-ключей) |

Все consumer'ы — **Idempotent Consumer** с дедупликацией по `event_id`; партиционирование по `campaign_id` сохраняет порядок per кампания. Подробности — [reliability.md §3](./reliability.md).

---

## 3. Обоснование выбора протоколов

### 3.1. gRPC для внутренних sync-вызовов

Внутренние server-to-server вызовы (API Gateway → Bidding, Campaign Service → Financial Service) идут по **gRPC unary**:

- **Latency.** Бинарный protobuf + HTTP/2 multiplexing дают p99 в единицы миллисекунд против десятков у REST/JSON. При бюджете 80 ms на bid response — решающий фактор для Gateway → Bidding.
- **Контракт-first.** Proto-файлы — единый источник правды; SDK генерируется для всех языков стека AdScale (C++, Go, Python, Node) одной командой `protoc`.
- **Статическая типизация.** Несовместимое изменение схемы детектится в CI на сборке, не в runtime.

### 3.2. REST для внешнего API

DSP-партнёры и веб-клиенты обмениваются по REST/HTTPS:

- **OpenRTB** — отраслевой стандарт RTB-индустрии. Все DSP-партнёры интегрируются по нему.
- **Браузерные клиенты** работают по REST нативно (fetch, XHR); gRPC-Web требует прокси и ограничен.
- **OpenAPI 3.0** даёт партнёрам декларативный контракт, документацию и SDK на любом языке.

### 3.3. Kafka для потоковой обработки событий

Impressions, clicks, bid_won, CDC изменений — через Kafka:

- **Pub/Sub.** Одно событие impression параллельно обрабатывают Statistics, Financial и Bidding — нативный сценарий Kafka с consumer groups и независимыми offset'ами.
- **Replay.** При сбое consumer можно перечитать события за окно retention и восстановить проекцию.
- **Event sourcing.** Kafka — источник правды для проекций в ClickHouse / Redis / Finance DB.
- **Throughput.** При целевой нагрузке 50 000 RPS bid requests на 12-мес горизонте log-based брокер выдерживает поток событий с запасом; классические очереди (RabbitMQ, Artemis) при durable не вытянут.
- **Порядок в пределах ключа** (`campaign_id`) — критично для биллинга.

Формальный выбор Kafka — в [Task1/adr/ADR-003](../Task1/adr/ADR-003-event-streaming-technology.md).

### 3.4. Почему Bidding и Campaign не общаются синхронно

Сознательное архитектурное решение для удержания SLA hot path. Прямой sync-вызов Bidding → Campaign Service на каждый bid request даёт несколько проблем:

- **Дополнительная latency** — даже оптимизированный gRPC прибавляет 3–15 ms (сеть + сериализация + обработка). В 80 ms бюджете значительная доля.
- **Связь сервисов по доступности** — сбой Campaign Service приводит к остановке hot path; в текущей архитектуре Bidding продолжает работать с данными из кэша даже при недоступности Campaign Service.
- **Throughput на Campaign Service** — 50 000 sync RPS потребовали бы оптимизировать его как hot-path-сервис, превращая backoffice в критичный по SLA контур.
- **Кэш всё равно понадобится** — Campaign Service не может ходить в PG на каждый запрос; eventual consistency мы получили бы в любом случае, просто кэш переехал бы в другой сервис.

Вместо этого Bidding получает данные кампаний через **Cache Aside + CDC-инвалидацию**: hot path работает с Redis-кэшем, при cache miss — read-replica Campaign PostgreSQL, актуальность поддерживается асинхронным потоком CDC через Kafka. Источник правды остаётся за Campaign Service, прямого RPC между сервисами нет.

### 3.5. Нужен ли API Gateway

**Да.** Без единой точки входа каждый сервис обязан реализовать TLS, аутентификацию DSP, rate limiting, observability — это дублирование и риск рассогласования. Gateway даёт единую точку входа для внешних клиентов, protocol translation REST → gRPC, rate limiting per DSP с глобальной видимостью, Circuit Breaker перед upstream Bidding и инструмент canary rollout. Реализация и выбор Envoy — в [api-gateway.md](./api-gateway.md).

---

## 4. Диаграммы

- `diagrams/seq-bid-request-happy-path.puml` — bid request: DSP → Gateway → Bidding → Redis → response + async publish в Kafka.
- `diagrams/seq-bid-request-fallback.puml` — fallback при срабатывании Circuit Breaker (read-replica или no-bid).
- `diagrams/seq-event-flow.puml` — поток impression: Delivery → Kafka → независимые consumer'ы (Statistics / Financial / Bidding).
- `diagrams/cmp-bidding-internals.puml` — C4 Component внутренностей Bidding Service.
