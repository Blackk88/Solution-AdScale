# Отказоустойчивость данных

---

## 1. Принципы

RPO/RTO дифференцированы по сервисам: для финансов RPO стремится к нулю (нельзя терять списания), для аналитики допустимо потерять минуты. Это позволяет не платить overhead синхронной репликации там, где он не нужен.

---

## 2. RPO/RTO per сервис

| Сервис | RPO (приемлемые потери) | RTO (время восстановления) | Чем обеспечивается |
|---|---|---|---|
| **Bidding Service** | минуты (Redis-кэш можно потерять и пересобрать) | минуты | stateless поды + автоскейлинг; кэш пересобирается из Campaign DB и Kafka |
| **Campaign Service** | минуты | минуты | PostgreSQL HA с sync replica + async read-replica; failover через managed-сервис |
| **Financial Service** | **0** (нельзя терять списания) | минуты | PostgreSQL HA с sync replica; идемпотентность через `event_id`; replay из Kafka |
| **Statistics Service** | минуты (события буферизованы в Kafka) | минуты | Kafka RF=3 + min.insync=2; ClickHouse реплицирован |
| **Analytics Service** | десятки минут | десятки минут | terminal sink; проекции пересобираются из Kafka |

Конкретные числовые значения SLO (секунды, минуты, доли часа) определяются на этапе настройки исходя из контрактных обязательств и нагрузочного тестирования. В таблице — порядок величин.

### 2.1. Почему RPO=0 только для финансов

Synchronous-репликация (которая даёт RPO=0) платится latency: каждая запись ждёт ACK от sync replica перед коммитом. Для финансов это приемлемо (десятки writes/сек), для биддинга — нет (десятки тысяч ops/сек в Redis синхронно реплицировать нереалистично).

Поэтому:

- **Finance PostgreSQL** — синхронная репликация на одну ближайшую реплику, асинхронная на остальные.
- **Campaign PostgreSQL** — async-репликация (мини-потеря допустима, изменения кампаний не теряют деньги).
- **Bidding (Redis)** — кэш eventual; потерянные ключи восстанавливаются read-through из БД и CDC.
- **Statistics / Analytics** — события буферизованы в Kafka (RF=3); даже при полной потере ClickHouse-шарда проекция пересобирается из лога.

### 2.2. Replay как ключевой механизм восстановления

Главный приём в архитектуре — **Kafka как source of truth** для проекций. При потере проекций Statistics / Financial / Bidding-кэша они пересобираются consumer'ом, читающим Kafka с нужного offset'а. Это сокращает RTO даже при катастрофическом отказе downstream-БД.

---

## 3. Стратегия резервного копирования

### 3.1. PostgreSQL (Campaign, Financial)

- **WAL archiving** — непрерывное архивирование WAL-сегментов в Object Storage; даёт PITR (point-in-time recovery).
- **Full backup** — регулярный `pg_basebackup` в Object Storage.
- **Incremental backup** — между full-backup'ами через `pgBackRest` или аналог.
- **Cross-region copy** — копия backup'ов в географически удалённый bucket.
- **Restore drill** — регулярная автоматическая проверка восстановления из backup в изолированное окружение.

PITR позволяет восстановить состояние БД на произвольный момент в окне retention — критично для расследования инцидентов и compliance-запросов на финансовые данные. Конкретные интервалы full / incremental backup и retention настраивает DevOps-команда по требованиям compliance и RPO.

### 3.2. Kafka

- **Replication factor 3** (multi-AZ при наличии).
- **min.insync.replicas = 2.**
- **Tiered Storage** — холодные сегменты автоматически перевозятся в Object Storage.
- **Audit retention** — продлённый для финансовых и CDC-топиков (см. [event-streaming.md §5](./event-streaming.md)).
- **Snapshot Schema Registry** — регулярный bulk export метаданных в Object Storage.

Kafka не нуждается в отдельных backup'ах в классическом смысле — репликация + Tiered Storage уже обеспечивают долговременное хранение. При тотальной потере кластера данные восстанавливаются из Object Storage.

### 3.3. ClickHouse

- **Репликация** — `ReplicatedMergeTree` с несколькими копиями шарда.
- **Backup** — регулярный `clickhouse-backup` в Object Storage.
- **Recovery path** — replay из Kafka как основной механизм; backup нужен только для ускорения восстановления и для table metadata.

Поскольку ClickHouse — это projection из Kafka-лога, тотальный rebuild возможен за разумное время (порядок часа для типового окна retention).

### 3.4. Redis

- **AOF (Append-Only File)** или **RDB snapshots** для persistence — конкретный режим выбирается DevOps-командой по компромиссу durability/throughput.
- **Replication** — каждый шард имеет реплики (см. [scaling.md](./scaling.md)).
- **Recovery path** — при потере шарда failover на replica; при тотальной потере — read-through пересобирает горячие ключи из Campaign DB.

Полный Redis-кластер при катастрофическом отказе восстанавливается **не из backup, а из source-of-truth БД** — это правильная стратегия для кэша.

### 3.5. Object Storage

Сами backups лежат в Object Storage с **cross-region replication** — потеря дата-центра не уничтожает резервные копии.

---

## 4. Failover-сценарии

### 4.1. Отказ одного пода (Kubernetes)

Самый частый сценарий, покрывается без вмешательства человека: HPA удерживает заданное число реплик; Service автоматически исключает упавший под из ротации после fail в readiness probe. Восстановление — минуты.

### 4.2. Отказ одной зоны доступности (AZ)

Все stateful-компоненты развёрнуты multi-AZ:

| Компонент | Поведение при отказе AZ |
|---|---|
| Kubernetes pods | автоматически пересоздаются на узлах в других AZ |
| Kafka brokers | продолжают работать (RF=3 across AZ); один из оставшихся брокеров становится partition leader |
| PostgreSQL primary | failover на sync-replica в другой AZ через managed-сервис |
| Redis Cluster | автоматический failover master→replica внутри шарда |
| ClickHouse | партиции читаются с реплик в других AZ |

### 4.3. Отказ региона (12-мес фаза)

На 3-мес фазе multi-region не предполагается, отказ региона = downtime. На 12-мес фазе:

- **Bidding Service + API Gateway** — active-active в нескольких регионах с гео-маршрутизацией. Отказ региона = автоматический failover DSP-трафика на оставшиеся.
- **Финансы** — async cross-region replica с возможностью manual promote при катастрофическом отказе primary региона.
- **Kafka** — отдельный кластер в каждом регионе, синхронизация critical-топиков через MirrorMaker2.
- **ClickHouse** — независимые кластеры per регион, синхронизация через Kafka-replay.

### 4.4. Логическое повреждение данных

Сбой может быть не только в инфраструктуре, но и в коде или операторе — например, ошибочный SQL или баг в новой версии сервиса.

- **PostgreSQL** — PITR на момент до повреждения (см. §3.1).
- **Kafka events** — поскольку события immutable, логическое повреждение возможно только на стороне consumer'а. Решение — rebuild проекции из Kafka с исправленным кодом.
- **Redis** — flush + read-through из БД.

---

## 5. Соответствие 12-факторному приложению

Микросервисы AdScale спроектированы по принципам 12-факторного приложения — это снимает большую часть проблем масштабирования и развёртывания, на которые рассчитана методология.

| # | Принцип | Как реализовано в AdScale |
|---|---|---|
| 1 | Codebase | Каждый микросервис — отдельный Git-репозиторий; один артефакт деплоится во все окружения с разной конфигурацией |
| 2 | Dependencies | Зависимости декларируются в манифестах (`go.mod`, `requirements.txt`, `package.json`); сборка детерминированная |
| 3 | Config | Настройки (адреса хранилищ, секреты) — через env vars; в Kubernetes — ConfigMap и Secret |
| 4 | Backing services | PostgreSQL, Kafka, Redis, Object Storage доступны через URL из env vars; подмена managed-провайдера или локальной dev-инстанции — без изменений в коде |
| 5 | Build, release, run | Build — Docker-образ через CI; Release — образ + конфиг для окружения; Run — `kubectl apply`. Каждый release immutable |
| 6 | Processes | Stateless процессы; состояние живёт в Redis, PostgreSQL, Kafka; под можно убить в любой момент |
| 7 | Port binding | Каждый сервис самодостаточен, биндится к HTTP/gRPC-порту напрямую без внешнего web-сервера |
| 8 | Concurrency | Горизонтальное масштабирование через HPA; внутри пода — concurrency через goroutines / event loops |
| 9 | Disposability | Быстрый старт; graceful shutdown по SIGTERM (in-flight requests, Kafka offsets, закрытие соединений) |
| 10 | Dev/prod parity | Те же managed-сервисы (Kafka, PostgreSQL, Redis) в dev и staging; Docker Compose для локалки имитирует production-зависимости |
| 11 | Logs | Структурированный JSON в stdout; collector (Loki / Fluent Bit) забирает логи из подов в централизованное хранилище |
| 12 | Admin processes | Миграции БД, прогрев кэша, reconciliation — как Kubernetes Job или initContainer; тот же образ, что основной сервис |

### 5.1. Что 12-factor не покрывает

12-factor задаёт базовый уровень; для финтех-нагрузки AdScale поверх него действуют дополнительные практики:

- **Idempotent Consumer и идемпотентность операций** — критичная защита от двойных списаний (см. [Task2/reliability.md §5](../Task2/reliability.md)).
- **Multi-region** на 12-мес горизонте — выходит за рамки classic 12-factor.
- **CDC и event sourcing** — как механизм восстановления проекций.

Эти практики дополняют 12-factor, не конфликтуя с ним.
