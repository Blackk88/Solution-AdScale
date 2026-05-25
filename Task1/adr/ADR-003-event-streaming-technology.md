# ADR-003: Поток событий реализован на Apache Kafka

- **Статус:** Принято
- **Дата:** 2026-05-23

---

## Контекст

В целевой архитектуре (см. [TO-BE.md](../TO-BE.md)) часть взаимодействий между сервисами должна стать асинхронной. Это снимает синхронную связь Bidding-контура с Statistics / Financial / Analytics, абсорбирует пики нагрузки и реализует event-driven обработку как целевой стиль системы на 12-мес горизонте.

Брокер событий нужен для:

- передачи показов и кликов от Delivery к нескольким независимым потребителям (Statistics, Financial, Bidding-кэш);
- передачи изменений OLTP-таблиц (campaigns, budgets) через CDC в downstream-проекции и кэши;
- хранения истории событий как source of truth для пересоздания проекций (Stats / Billing) при сбое или баге.

Полный набор архитектурных драйверов — в [Drivers.md](../Drivers.md).

**Ключевые архитектурные драйверы для этого решения:**

- Throughput: ≥ 18 000 events/s на 3 мес, ≥ 50 000 events/s к 12 мес (FR-DEL-02, Scalability P0)
- Гарантии доставки: at-least-once с дедупликацией, exactly-once-effective для биллинга (Data integrity P0)
- Сохранение порядка событий в пределах ключа `campaign_id` / `user_id` (Reliability P0)
- Retention для replay: ≥ 7 дней, 30 дней для billing (Reliability P0, audit)
- Multi-consumer на одно событие — Statistics / Financial / Bidding читают независимо
- Высокая доступность как часть общесистемного SLA 99.9 % (Availability P0)
- Managed-вариант — обязательно, в команде нет SRE (TC-06)
- Open-source или managed без enterprise-лицензий (BC-02)

**Рассматриваются три зрелых open-source брокера, актуальных для индустрии:**

1. Apache Kafka
2. RabbitMQ
3. Apache Artemis

---

### Альтернатива 1: Apache Kafka

- ➕ Распределённый журнал событий с разделами и репликацией.
- ➕ Очень высокий throughput: миллионы msg/s на кластере, с запасом на 12-мес горизонт.
- ➕ Retention (часы–дни–вечно), replay по offset — основа event sourcing и audit.
- ➕ Гарантии: at-least-once, exactly-once-effective через transactional API.
- ➕ Порядок в пределах партиции по ключу.
- ➕ Multi-consumer через consumer groups, независимые offset'ы.
- ➕ Экосистема: Kafka Connect (Debezium для CDC), Schema Registry, Streams.
- ➕ Managed-варианты: AWS MSK, Confluent Cloud, Yandex Managed Kafka.
- ➖ Эксплуатационная сложность при self-hosted (KRaft/ZooKeeper, балансировка партиций) — снимается выбором managed.
- ➖ Latency p99 — единицы-десятки ms (приемлемо вне hot path).

### Альтернатива 2: RabbitMQ

- ➕ Классическая очередь сообщений на AMQP с богатой маршрутизацией.
- ➕ Простая эксплуатация, низкий порог входа, низкая latency (< 1 ms p99).
- ➕ Retry, DLQ, delay scheduling — всё из коробки.
- ➖ Не log-based: после ack сообщение удаляется, **replay невозможен** без отдельного store.
- ➖ Throughput при durability — 10–50 K msg/s на ноде, **недостаточно** для 50 000 events/s потока показов/кликов.
- ➖ Слабые гарантии порядка при множественных consumer на одну очередь.
- ➖ Не подходит для event sourcing и audit trail.

### Альтернатива 3: Apache Artemis

- ➕ AMQP-брокер с поддержкой MQTT / STOMP / JMS, удобен в Java/Spring Boot экосистеме.
- ➕ Сопоставимая с RabbitMQ пропускная способность, низкая latency.
- ➖ Меньшее сообщество, чем у Kafka и RabbitMQ.
- ➖ Сильная сторона Artemis — JMS-интеграции; AdScale работает на гибридном стеке C++/Go/Python/Node (TC-03), JMS-зависимостей нет, преимущества Artemis не реализуются.
- ➖ Те же ограничения, что у RabbitMQ для event-streaming: нет log-based retention, нет replay по offset.

---

## Решение

Принимается **Apache Kafka** в managed-варианте как единственный брокер событий.

Kafka покрывает все три ключевых потребности: высокий throughput, multi-consumer pub/sub и replay для проекций. RabbitMQ и Artemis отклонены как очереди сообщений — нет log-based retention и replay, throughput на классе 10–50K msg/s не вытягивает 50 000 events/s; Artemis дополнительно непрофильный для нашего стека (TC-03).

Отдельный брокер очередей задач (RabbitMQ) **не вводится**. Сценарии, где он мог бы помочь (ретраи внешних интеграций), решаются на уровне сервисов: persistence retry-задач в собственной БД сервиса плюс in-process worker. Это сохраняет операционную поверхность минимальной.

Предварительный набор топиков:

- `bidding.bid_won.v1` — победа в аукционе.
- `delivery.impression.v1` — показ.
- `delivery.click.v1` — клик.
- `billing.transaction.v1` — финансовая операция.
- `cdc.campaign.v1` — изменения кампаний (для cache invalidation в Redis).
- `cdc.budget.v1` — изменения бюджета.

Конфигурация:

| Параметр | Значение | Обоснование |
|---|---|---|
| Брокеры | 3, multi-AZ | высокая доступность кластера |
| Replication factor | 3 | потеря broker'а не теряет данные |
| min.insync.replicas | 2 | защита от split-brain |
| acks producer | all | persistence до ack |
| Партиционирование | по `campaign_id` для bidding/billing, по `user_id` для пользовательских счётчиков | сохранение порядка в пределах ключа |
| Retention | 7 дней default, 30 дней для billing-топиков | replay, audit |
| Schema Registry | Confluent / Apicurio | контракт-first, обратная совместимость |
| CDC | Debezium → Kafka Connect | OLTP-изменения транслируются в брокер |

### Обязательные паттерны

- **Idempotent Consumer.** Каждый потребитель обязан корректно обрабатывать повторную доставку (at-least-once — норма). Реализация: dedup-таблица или Redis SET по `idempotency_key = entity_id + operation_type`.
- **DLQ-топики.** Для каждого основного топика поддерживается `*.dlq` для событий, которые consumer не смог обработать после max-попыток.
- **Competing Consumers внутри группы.** Горизонтальное масштабирование через consumer groups: партиции делятся между instance'ами consumer-сервиса.

---

## Последствия

### Положительные

- Один брокер вместо двух — минимальная операционная поверхность для команды без выделенного SRE.
- Запас по throughput Kafka (миллионы msg/s) — на 12-месячный горизонт и далее.
- Event sourcing на Kafka: Statistics / Financial проекции пересоздаются из лога; полный audit trail.
- Развязка hot path от Statistics / Financial / Analytics — отказ потребителей не валит Bidding.
- Зрелое индустриальное решение, широко применяемое в финтехе и AdTech — упрощает найм и обмен опытом.
- Managed-варианты у крупных провайдеров снимают эксплуатационную сложность.

### Отрицательные / Trade-offs

- Schema Registry и контрактная дисциплина для Kafka — дополнительная работа на каждом событии. Окупается на горизонте 6+ месяцев.
- Latency Kafka выше, чем у RabbitMQ (единицы-десятки ms против < 1 ms) — но Kafka не на hot path, поэтому приемлемо.
- Сценарии delay scheduling и сложного retry требуют ручной реализации поверх Kafka (например, через retry-топики с задержкой). Приемлемо при минимальном числе таких сценариев в системе.

### Риски

- Lag consumer'ов растёт быстрее, чем масштабируются partitions Kafka → отставание Statistics / Financial. Митигация: алерт на consumer lag, autoscaling consumer-групп, pre-provisioned partitions с запасом 2×.
- Потеря событий из-за неверной конфигурации (acks=1, min.insync.replicas=1). Митигация: cluster-policy через IaC, code review producer-конфигов, integration-тесты на устойчивость к broker failure.
- Двойная обработка событий при ребалансе consumer-группы → задвоение биллинга. Митигация: **Idempotent Consumer** на PK = `event_id` + operation_type, reconciliation jobs, transactional API для critical-flows.
- Расхождение схемы события между producer и consumer → runtime-сбои. Митигация: Schema Registry с режимом backward compatibility, контрактные тесты, обязательная регистрация схем в CI.
- Накопление в DLQ без процесса разбора → потеря инцидентов. Митигация: alert на размер DLQ, дежурный процесс разбора, dashboards в Grafana.
- Дефицит SRE-экспертизы → инциденты в продакшне. Митигация: managed-сервис берёт на себя установку патчей и обновлений, инструкции по эксплуатации на ключевые сценарии (отказ брокера, отставание consumer, несовместимость схем, переполнение DLQ).
- Бюджетный перерасход на managed-инфраструктуре. Митигация: оценка self-hosted при превышении порога.

## Связанные решения

- [ADR-001: Архитектура эволюционирует через Strangler Fig + модульный монолит](./ADR-001-evolution-strategy.md) — Kafka вводится в составе общей стратегии.
- [ADR-002: Bidding Service выделяется из монолита первым](./ADR-002-bidding-service-first.md) — Kafka входит в базовую платформу первой экстракции.
