# Task 2. Проектирование RTB и интеграции с DSP

Раздел документирует дизайн критичного контура AdScale — обработки bid requests от DSP-партнёров. Цель — выдержать SLA нового DSP-партнёра (p95 ≤ 80 ms) без регресса для существующих клиентов.

---

## Структура

```text
Task2/
├── README.md              ← этот файл
├── bidding-service.md     ← спецификация Bidding Service (границы, API, зависимости, модель данных)
├── interaction.md         ← протоколы и паттерны взаимодействия
├── api-gateway.md         ← дизайн API Gateway для DSP-интеграции
├── reliability.md         ← паттерны надёжности (CB, Retry, идемпотентность, fallback)
└── diagrams/
    ├── cmp-bidding-internals.puml     ← C4 Component внутренностей Bidding Service
    ├── seq-bid-request-happy-path.puml ← happy path bid request
    ├── seq-bid-request-fallback.puml   ← bid request с CB / fallback
    └── seq-event-flow.puml             ← impression → Kafka → независимые consumer'ы
```

---

## Ключевые архитектурные решения

| #   | Решение                                                                                 | Где                                                                                                              |
| --- | --------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| 1   | Bidding Service — самостоятельный Bounded Context, первым выделяется из монолита        | [bidding-service.md](./bidding-service.md), [Task1/adr/ADR-002](../Task1/adr/ADR-002-bidding-service-first.md)   |
| 2   | Внешний API: REST + OpenRTB                                                             | [bidding-service.md](./bidding-service.md), [interaction.md §3.2](./interaction.md)                              |
| 3   | Внутренний sync (east-west): **gRPC unary** между сервисами                             | [interaction.md §3.1](./interaction.md)                                                                          |
| 4   | Bidding не делает sync-вызовов к Campaign Service — связь через Redis cache + CDC       | [interaction.md §3.4](./interaction.md)                                                                          |
| 5   | Поток событий impressions / clicks / bid_won: **Kafka** с partitioning по `campaign_id` | [interaction.md §3.3](./interaction.md), [Task1/adr/ADR-003](../Task1/adr/ADR-003-event-streaming-technology.md) |
| 6   | **API Gateway (Envoy)** как единая точка входа для DSP и веб-клиентов                   | [api-gateway.md](./api-gateway.md)                                                                               |
| 7   | Двухслойная аутентификация: mTLS + API-ключ per DSP                                     | [api-gateway.md §3](./api-gateway.md)                                                                            |
| 8   | Circuit Breaker client-side, fallback no-bid / read-replica при сбое Redis              | [reliability.md §1, §4](./reliability.md)                                                                        |
| 9   | Идемпотентность биллинга через `event_id` и уникальный индекс в Finance DB              | [reliability.md §3](./reliability.md)                                                                            |
| 10  | Hot path — без retry; retry только вне аукциона (Kafka publish, внешние интеграции)     | [reliability.md §2](./reliability.md)                                                                            |

---

## Ответы на ключевые вопросы

**Почему gRPC, а не REST для внутренних вызовов между сервисами?**
Бюджет 80 ms на bid response не позволяет накапливать REST/JSON overhead в east-west трафике. gRPC + protobuf + HTTP/2 дают p99 в единицы миллисекунд, статическую типизацию и единый источник правды контракта (proto-файлы) для всех языков стека AdScale. Подробнее — [interaction.md §3.1](./interaction.md).

**Bidding и Campaign общаются по gRPC или REST?**
Никак из этого. Прямого sync-вызова между ними нет — это сознательное решение для удержания SLA hot path. Связь через Redis cache (hot read), Campaign DB read-replica (при cache miss) и поток CDC через Kafka. Подробнее — [interaction.md §3.4](./interaction.md).

**Почему Kafka для событий кликов и показов?**
Одно событие impression обрабатывают параллельно три независимых consumer'а (Statistics, Financial, Bidding) — нативный pub/sub Kafka с независимыми offset'ами и replay при сбое. При целевой нагрузке 50 000 RPS bid requests на 12-мес горизонте log-based брокер выдерживает поток событий с запасом; классические очереди — нет. Подробнее — [interaction.md §3.3](./interaction.md).

**Нужен ли API Gateway? Какой?**
Да, обязателен. Без единой точки входа каждый upstream дублирует TLS, аутентификацию, rate limiting, observability. Выбран **Envoy** за нативную поддержку gRPC и HTTP/2, встроенный REST→gRPC transcoder и xDS для динамической конфигурации. Managed-вариант (AWS App Mesh / Istio Ingress / Yandex ALB) снимает операционную нагрузку с команды без SRE. Подробнее — [api-gateway.md §7](./api-gateway.md).

**Как обеспечена надёжность?**
Четыре основных паттерна:

- **Circuit Breaker** на стороне клиента для всех upstream'ов.
- **Retry с exponential backoff + jitter** — только вне hot path (Kafka publish, ретраи внешних интеграций); в аукционе retry запрещён.
- **Идемпотентность** биллинга через уникальный индекс на `event_id` в Finance DB.
- **Резервные стратегии**: no-bid при недоступности зависимостей в hot path; переключение Redis → read-replica Campaign PostgreSQL при сбое кэша.

Подробнее — [reliability.md](./reliability.md).
