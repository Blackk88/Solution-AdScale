# Паттерны надёжности

Связанные документы: [bidding-service.md](./bidding-service.md), [interaction.md](./interaction.md), [api-gateway.md](./api-gateway.md).

---

## 1. Circuit Breaker

CB реализуется **на стороне клиента**, отдельно для каждого upstream.

| Клиент | Защищаемая зависимость | Fallback при недоступности |
|---|---|---|
| API Gateway | Bidding Service cluster | HTTP 204 No Content партнёру (нормальный no-bid в OpenRTB) |
| Bidding | Redis Cluster | fallback на read-replica Campaign PostgreSQL (см. §4.2) |
| Bidding | Campaign DB read-replica | no-bid с `reason=STORAGE_UNAVAILABLE` |
| Financial | Платёжный шлюз | retry в очередь с exponential backoff (см. §2) |
| Campaign | Financial (gRPC) | возврат ошибки в UI |

Конкретные пороги (error rate %, latency p99, Half-Open timeout) определяются по результатам нагрузочного тестирования. Для Bidding → Redis пороги жёстче по latency, чем для Campaign DB read-replica — короткое замедление Redis критично для hot path.

Каждый переход в Open генерирует алерт DevOps-команде и фиксируется в Grafana.

---

## 2. Повторные попытки с экспоненциальной задержкой

Retry применяется **только вне hot path аукциона**. Внутри 80 ms бюджета повтор съест latency и нарушит SLA партнёра — если первая попытка не успешна, fail-fast в fallback (§4).

Формула: `delay(n) = min(base × 2^n, max_delay) × jitter`. Jitter обязателен — без него тысячи клиентов после восстановления одновременно повторят запрос и снова уронят сервис (retry-шторм).

**Применяется:**

- Kafka publish (Delivery, Campaign, Bidding) — idempotent producer повторяет до подтверждения; при длительной недоступности — local buffer.
- Kafka consumer'ы (Statistics, Financial, Bidding) — повтор обработки; при невозможности — DLQ.
- Financial → Платёжный шлюз — retry в фоне с длинным max-окном, далее manual review.

**Запрещён:**

- Любой sync-вызов в hot path аукциона (Bidding → Redis, Bidding → Campaign DB read-replica). Вместо retry — CB + fallback.
- API Gateway → Bidding. Партнёр сам решает, повторять ли на следующий impression.
- Любой вызов без idempotency key.

Retry **обязательно** комбинируется с CB: повтор в открытый upstream усугубит каскадный отказ.

---

## 3. Идемпотентность финансовых операций

Самый критичный кейс — биллинг. Financial Service читает из Kafka топики `delivery.impression.v1` и `bidding.bid_won.v1` и записывает транзакции в Finance PostgreSQL. Kafka даёт at-least-once доставку, ребаланс consumer-группы или retry producer'а могут привести к повторной обработке одного события — без защиты это означает двойное списание.

Защита — **уникальный индекс на `event_id` (UUID)** в таблице транзакций + `INSERT ... ON CONFLICT DO NOTHING`. При повторной доставке БД отклоняет вставку на уровне индекса, баланс не изменяется. Это паттерн **Idempotent Consumer**.

Дополнительно: producer'ы работают с `enable.idempotence=true` (защита от дубликатов на уровне Kafka client), consumer'ы делают dedup по `event_id` через Redis SET с TTL ≥ окна replay.

---

## 4. Резервные стратегии

При недоступности зависимости Bidding принимает решение за миллисекунды: вернуть no-bid или работать на альтернативных данных.

### 4.1. No-bid как универсальный fallback

При недоступности критичной зависимости Bidding возвращает no-bid с явной причиной (`nbr` reason code в OpenRTB-терминах). С точки зрения DSP это валидный ответ; партнёр переходит к следующему bidder'у. В метриках причина no-bid разделяется (storage unavailable, timeout, campaign unavailable), чтобы DevOps-команда видела реальный источник проблемы.

### 4.2. Fallback Redis → read-replica Campaign PostgreSQL

При CB Open на Redis Bidding переключает чтение кампаний и креативов на read-replica Campaign PostgreSQL. Это медленнее в несколько раз, поэтому:

- Активируется только для критичных ключей (кампания, креатив).
- Real-time счётчики расходов недоступны — Bidding работает с консервативной оценкой бюджета по дневному лимиту кампании.
- Автоскейлинг увеличивает количество подов Bidding для компенсации возросшей latency.

При двойном сбое (Redis + PostgreSQL) — no-bid.

### 4.3. Почему нет «кэшированных дефолтных ставок»

Бюджет в архитектуре уже представлен как **eventually consistent проекция** в Redis (обновляется через CDC из Campaign DB + событиями impression из Kafka). При сбое Redis fallback на read-replica даёт консервативную оценку: возможна недо-выдача трафика, но не перерасход. Отдельный «cached default» с защитным коэффициентом не нужен — Redis сам и есть кэш-слой.
