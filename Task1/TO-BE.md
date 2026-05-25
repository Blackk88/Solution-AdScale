# Целевая архитектура AdScale

Документ описывает целевое состояние платформы через 12 месяцев и стратегию эволюции от текущего монолита. Опирается на диагностику в [AS-IS.md](./AS-IS.md) и драйверы в [Drivers.md](./Drivers.md). Формальные архитектурные решения — в [adr/](./adr/).

---

## 1. Целевое видение

| Параметр | Сейчас | Через 3 мес | Через 12 мес |
|---|---|---|---|
| Throughput | 15 000 RPS | ≥ 18 000 RPS | ≥ 50 000 RPS |
| P95 bid response* | 110–120 ms | ≤ 80 ms | ≤ 80 ms |
| SLA доступности | — | без регресса от текущего | 99.9 % |
| Архитектурный стиль | монолит | монолит + выделенный bidding-сервис | микросервисы + event-driven |
| База данных | 1× PostgreSQL (всё) | PostgreSQL primary + replica | DB per service + OLAP-warehouse |
| Развёртывание | ручное, 1 кластер | контейнеризация ключевых сервисов, 1 регион | Kubernetes, multi-region, blue/green |
| Команды | 1 (на весь монолит) | 2 (bidding + платформа) | 4–5 (по доменам) |

\* Целевое значение определяется контрактным требованием DSP-партнёра — самым жёстким из применимых SLA.

Целевая система:

- Event-driven обработка показов/кликов/биллинга поверх Kafka.
- Микросервисы по доменам (bidding, delivery, statistics, financial, analytics, campaign + advertiser dashboard).
- Stateless компоненты в hot path с горизонтальным автоскейлингом.
- Кэш горячих сущностей в Redis cluster перед Bidding для удержания SLA.
- DB per service, разделение OLTP/OLAP, dedicated analytics warehouse.
- Multi-region развёртывание с гео-маршрутизацией DSP-трафика.
- API Gateway как единая точка входа для внешних потребителей.

---

## 2. Выбор архитектурного стиля

### 2.1. Постановка задачи

Эволюционировать монолит к целевой архитектуре нужно при трёх жёстких ограничениях из [Drivers.md](./Drivers.md):

- **TC-01 / BC-01:** нельзя переписать систему полностью, срок 3 месяца на промежуточное решение.
- **TC-02:** непрерывность бизнеса — никаких окон с остановкой системы.
- **BC-02 / BC-03:** ограниченный бюджет, 35 человек, нет SRE.

Задача распадается на два независимых решения:

1. **Целевой архитектурный стиль** — что должно получиться в итоге. Сравниваются три стилевые альтернативы: монолит, модульный монолит, микросервисы. Выбор обосновывается матрицей в §2.2.
2. **Стратегия перехода** — как двигаться от текущего монолита к целевому стилю без нарушения ограничений выше. Решение зафиксировано в [ADR-001](./adr/ADR-001-evolution-strategy.md).

### 2.2. Взвешенная матрица решений

Веса 1–5 (5 — максимально важно), оценка стиля 1–5. В скобках — произведение `вес × оценка`.

| Фактор | Вес | Монолит | Модульный монолит | Микросервисы |
|---|---|---|---|---|
| Поддержка 18 000 / 50 000 RPS | 5 | 1 (5) | 2 (10) | 5 (25) |
| P95 ≤ 80 ms для bidding | 5 | 1 (5) | 2 (10) | 5 (25) |
| Независимое масштабирование bidding vs Stats/Analytics | 5 | 1 (5) | 2 (10) | 5 (25) |
| Time-to-market 3 месяца (BC-01) | 4 | 5 (20) | 4 (16) | 3 (12) |
| Непрерывность бизнеса (TC-02) | 5 | 5 (25) | 5 (25) | 4 (20) |
| Зрелость DevOps / наличие SRE (TC-06) | 3 | 5 (15) | 5 (15) | 2 (6) |
| Готовность к multi-region | 3 | 1 (3) | 2 (6) | 5 (15) |
| Strong consistency для финансов | 4 | 5 (20) | 5 (20) | 3 (12) |
| Стоимость инфраструктуры (BC-02) | 3 | 5 (15) | 5 (15) | 3 (9) |
| **Итого** | — | **113** | **127** | **149** |

Микросервисы — лучший балл по целевому состоянию (149), но за 3 месяца недостижимы (Time-to-market и зрелость DevOps занижают оценки). Модульный монолит (127) — устойчивая промежуточная форма для ещё не выделенных доменов, через которую идёт постепенная экстракция. Матрица выбирает **целевой стиль** (микросервисы); конкретная стратегия перехода — в [ADR-001](./adr/ADR-001-evolution-strategy.md).

### 2.3. Решение

Из §2.2 принимаются два связанных решения:

- **Целевой стиль (12-мес горизонт)** — микросервисы.
- **Стратегия перехода** — Strangler Fig Pattern: поэтапная экстракция доменов из монолита, с использованием **модульного монолита** как промежуточной формы для ещё не выделенных частей. Формальная запись — [ADR-001](./adr/ADR-001-evolution-strategy.md).

Big-bang переписывание исключено (нарушает TC-02 и BC-01). На 3-мес фазе из монолита выделяется только bidding-контур; остальные домены приводятся к модульной структуре (отдельные схемы БД, межмодульные API). На 12-мес фазе все ключевые домены вынесены как самостоятельные сервисы.

**Порядок экстракции** (по убыванию приоритета):

1. **Bidding Service** (Ad Server + Auction Engine) — критический по SLA, компактный по предметной области. Решение — [ADR-002](./adr/ADR-002-bidding-service-first.md).
2. **Statistics Service** + Kafka — убирает write-bottleneck с критического пути, события публикуются Delivery в Kafka.
3. **Analytics Service** → ClickHouse — снимает конкуренцию OLAP с OLTP.
4. **Financial Service** → отдельная БД с strong consistency, читает биллинговые события из Kafka.
5. **Campaign Service + Advertiser Dashboard** — последние, после стабилизации остальных контуров.

---

## 3. Целевые сервисы и их границы

Границы проведены по **Bounded Context** (DDD). Каждый сервис определяет три границы: модели (свой Ubiquitous Language и инварианты), данных (своя БД, прямого доступа извне нет), деплоя (независимый жизненный цикл).

### 3.1. Hot path (Bidding и Delivery)

| Сервис | Назначение | Stateful? | SLA | Хранилище |
|---|---|---|---|---|
| **Bidding Service** | приём bid request, подбор кандидатов, проверка бюджета, выбор победителя | stateless | ≤ 80 ms p95 | Redis (cache), read-replica PG |
| **Delivery Service** | формирование HTML/JS, возврат победителя, публикация impression/click в Kafka | stateless | ≤ 30 ms p95 | Object storage (креативы) |

Логика подбора кандидатов, проверки бюджета и взвешивания ставок остаётся внутри Bidding Service как функциональные модули — это эволюция текущего Ad Server + Auction Engine, а не их дробление на самостоятельные сервисы.

### 3.2. Потребители событий из Kafka

Каждый сервис, который зависит от потока impression/click, подписывается через свою consumer-group. Группы независимы — у каждой свой offset, своя скорость, свой retry.

| Сервис | Подписан на | Назначение |
|---|---|---|
| **Statistics Service** | `delivery.impression`, `delivery.click` | стрим-агрегация (1m / 5m / 1h) → запись в ClickHouse |
| **Financial Service** | `delivery.impression`, `bidding.bid_won` | конвертация выигранных показов в финансовые транзакции (idempotent по `event_id`) |
| **Bidding Service** | `delivery.impression`, `delivery.click`, `cdc.campaign`, `cdc.budget` | обновление Redis-кэша: счётчики расходов кампаний, инвалидация по CDC |

### 3.3. Backoffice и UI

| Сервис | Назначение | Хранилище |
|---|---|---|
| **Financial Service** | списания, балансы, биллинг, интеграция с платёжным шлюзом (ACID) | Finance PostgreSQL HA |
| **Analytics Service** | отчёты, агрегаты | ClickHouse (read-only) |
| **Campaign Service** | CRUD кампаний, бюджетов, креативов | Campaign PostgreSQL |
| **Advertiser Dashboard** | web UI рекламодателя | через Campaign Service + Analytics |

### 3.4. Платформа

| Компонент | Назначение |
|---|---|
| **API Gateway** | edge для DSP-партнёров и UI: TLS, auth, rate limiting, routing, OpenRTB |
| **Service Mesh** (12-мес фаза) | mTLS, retries, circuit breaking между сервисами |
| **Redis Cluster** | hot cache Bidding Service: кампании, бюджеты, таргетинг |
| **Kafka** | магистраль событий: impression/click, CDC изменений OLTP, audit |
| **ClickHouse** | OLAP warehouse для аналитики и исторических данных |
| **Object Storage** | креативы, бэкапы, cold-tier |
| **Observability** | Prometheus + Grafana + OpenTelemetry + Loki |
| **Kubernetes** | оркестрация, автоскейлинг, multi-AZ |

---

## 4. Стратегия данных

Главная цель — устранить единственную PostgreSQL как общее узкое место (см. [AS-IS.md §4, §5](./AS-IS.md)).

### 4.1. Database-per-service

| Сервис | Хранилище | Профиль | Консистентность |
|---|---|---|---|
| Bidding | Redis (hot) + read-replica PostgreSQL | read-heavy, low latency | eventual (TTL + CDC) |
| Statistics | Kafka → ClickHouse | append-only, write-heavy | at-least-once + dedup |
| Financial | dedicated PostgreSQL HA | ACID, balanced | strong |
| Analytics | ClickHouse + Kafka feed | read-heavy, batch+stream | eventual |
| Campaign | PostgreSQL (мигрирована из монолита) | mixed | strong, OLTP |

### 4.2. Разделение OLTP и OLAP

Аналитика не ходит в production OLTP. Изменения OLTP стримятся в ClickHouse через **CDC** (Debezium → Kafka → ClickHouse) — отчёты и агрегаты строятся только по копии данных в OLAP-хранилище.

**Read-replica Campaign PostgreSQL** обслуживает два сценария:

- **Bidding Service** при cache miss из Redis (горячие сущности, временно вышедшие из кэша).
- **Advertiser Dashboard** через Campaign Service для тяжёлых read-запросов (списки кампаний, отчётные представления).

Primary Campaign PostgreSQL остаётся за writes (CRUD кампаний и креативов). Finance PostgreSQL работает в режиме HA (primary + sync replica) — её реплика отвечает за durability/failover, не за read-распределение.

### 4.3. Event sourcing для критичных событий

Показы, клики, изменения бюджета пишутся в Kafka как **source of truth**. Stats / Billing / Analytics — это **проекции** этого потока. Это даёт rebuild любой проекции, аудит и идемпотентность.

### 4.4. Шардирование и репликация

Hot-данные (Redis, ClickHouse, Kafka) шардируются и партиционируются — там, где объём или throughput превышают потолок одной ноды. Транзакционные OLTP-хранилища (Campaign, Financial PostgreSQL) шардирование откладывают до реальной потребности — десятки ГБ не требуют разделения по машинам. На 12-мес фазе вводится **multi-region** — асинхронная репликация хранилищ + гео-маршрутизация на API Gateway.

Подробное распределение per хранилище — в [Task3/scaling.md](../Task3/scaling.md).

---

## 5. C4 Container Diagram

Целевая архитектура визуализирована в PlantUML: [diagrams/c4-container-to-be.puml](./diagrams/c4-container-to-be.puml). Состав соответствует §3 (сервисы) и §4 (хранилища).

---

## 6. Roadmap миграции

### Фаза 1 — 3 месяца

Цель: 18 000 RPS, P95 bid response ≤ 80 ms (DSP-SLA соблюдён), разделение критичных потоков.

- API Gateway перед монолитом (Envoy / Kong / NGINX), TLS, rate limiting.
- Контейнеризация bidding-контура, выделение Bidding Service.
- Redis cluster для кэша кампаний / бюджетов / таргетинга — снимает 40–60 ms с критического пути.
- Kafka между Delivery и Statistics Service + DLQ — устраняет write-bottleneck.
- Read-replica PostgreSQL для Analytics + Advertiser UI.
- Базовая observability: Prometheus + Grafana + tracing на bidding.
- CI/CD с blue/green для bidding.

### Фаза 2 — 6 месяцев

Цель: декомпозиция и движение к event-driven.

- Statistics Service выделен из монолита, читает Kafka, пишет в ClickHouse.
- Financial Service выделен из монолита с собственной БД, подписан на Kafka для биллинговых событий.
- Analytics Service переключён на ClickHouse, не ходит в production OLTP.
- Kubernetes (managed) для всех новых сервисов.

### Фаза 3 — 12 месяцев

Цель: 50 000 RPS, 99.9 % SLA, multi-region.

- Campaign Service выделен из монолита, Advertiser Dashboard работает через него.
- Multi-AZ → multi-region для Bidding Service и API Gateway.
- Service mesh (mTLS, retries, CB) для east-west трафика.
- Удаление legacy кода из монолита.

---

## 7. Риски и митигации

| Риск | Вероятность | Влияние | Митигация |
|---|---|---|---|
| **Распределённый монолит** — сервисы выделены, но связаны синхронными цепочками или общей БД | средняя | критическое | строгое DB-per-service, async через Kafka, контракт-ориентированные API, ежемесячный архитектурный обзор |
| Срыв 3-мес срока выделения bidding | средняя | критическое (SLA партнёра) | минимальный scope фазы 1, CI/CD с canary, откат через API Gateway |
| Несогласованность кэша Redis с БД (stale data) | высокая | среднее | короткий TTL + CDC-инвалидация + защитный `bid_ceiling` |
| Потеря событий в Kafka | низкая | критическое (потеря денег) | RF ≥ 3, acks=all, Idempotent Consumer, transactional API |
| Накопление в DLQ без процесса разбора | средняя | среднее | алерт на размер DLQ, дежурный процесс разбора |
| Двойная поддержка (монолит + сервис) | высокая | среднее | короткое окно экстракции (≤ 4 недели), feature toggles |
| Расхождение контрактов между сервисами | средняя | среднее | OpenAPI / Protobuf в репозитории, контрактные тесты, Schema Registry для Kafka |
| Дефицит SRE-функции | высокая | среднее | managed Kafka/Redis/Postgres, инструкции по эксплуатации, дежурство по ротации |
| Регресс SLA при переключении трафика между legacy и новым сервисом | средняя | высокое | теневой трафик до переключения, canary 1 % → 100 % с автоматическим откатом |
