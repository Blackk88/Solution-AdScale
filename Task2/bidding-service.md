# Bidding Service

Спецификация микросервиса, обрабатывающего bid request от DSP-партнёров. Bidding Service — первый домен, выделяемый из монолита AdScale (см. [Task1/adr/ADR-002](../Task1/adr/ADR-002-bidding-service-first.md)).

Связанные документы: [interaction.md](./interaction.md), [api-gateway.md](./api-gateway.md), [reliability.md](./reliability.md).

---

## 1. Назначение

Bidding Service:

- Принимает bid request от DSP-партнёров (OpenRTB) через API Gateway.
- Подбирает кандидатов в рекламу по таргетингу из горячего кэша (Redis).
- Проверяет ограничения бюджета по данным в кэше.
- Применяет бизнес-правила взвешивания, определяет победителя аукциона.
- Возвращает bid response или no-bid в пределах `tmax` партнёра (типично 80 ms).
- Публикует событие `bidding.bid_won.v1` в Kafka для downstream-сервисов (биллинг, аналитика).

В монолите AdScale эту функциональность выполняли два компонента — Ad Server и Auction Engine, оба синхронно ходящих в общую PostgreSQL. Объединение их в один stateless сервис со своим Redis-кэшем убирает главное узкое место AS-IS и позволяет масштабировать hot path независимо от остальных доменов.

---

## 2. Границы сервиса

Bidding Service — самостоятельный Bounded Context с тремя границами.

**Модель.** Свой Ubiquitous Language: bid request, bid response, auction, winner, no-bid, floor price, bid_ceiling. Ключевые инварианты:

- Один bid request → ровно один bid response или no-bid.
- При риске превышения `tmax` возвращается no-bid (`nbr` reason code), не ошибка.
- Победителем может быть только кампания с подтверждённым бюджетом на момент аукциона.
- Bid response никогда не превышает `bid_ceiling`, заданный для кампании.

**Данные.** Сервис владеет собственным Redis-кэш-кластером и публикует событие `bidding.bid_won.v1` в Kafka. Read-only доступ к Campaign PostgreSQL — только через read-replica и только при cache miss. Прямого доступа извне к этим хранилищам нет.

**Деплой.** Отдельный контейнер, независимый rollout через API Gateway, собственный автоскейлинг. Stateless — любой под можно убить без потери данных.

---

## 3. API

Сервис предоставляет три класса API: внешний (OpenRTB через API Gateway), внутренний (gRPC) и события (Kafka). Обоснование выбора протоколов — в [interaction.md](./interaction.md).

**Внешний API: OpenRTB.** DSP-партнёр отправляет запрос на API Gateway по HTTPS, Gateway проксирует его как gRPC в Bidding Service. Контракт — стандарт OpenRTB; конкретная версия закладывается в путь URL (`/openrtb/vN/bid`).

**Внутренний API: gRPC.** Bidding принимает gRPC unary вызов `SubmitBidRequest` от API Gateway. Контракт — proto-файлы в общем репозитории; SDK генерируется для всех языков стека AdScale из одного источника правды.

**Публикуемые события:**

| Топик | Когда публикуется | Ключ партиции | Назначение |
|---|---|---|---|
| `bidding.bid_won.v1` | после возврата ставки победителя | `campaign_id` | биллинг (Financial Service) |

Гарантии publish: at-least-once с дедупликацией на consumer по `event_id`, partitioning по `campaign_id` сохраняет порядок per кампания.

---

## 4. Зависимости

| Зависимость | Тип | Назначение |
|---|---|---|
| Redis Cluster | sync (hot path) | hot cache: кампании, бюджеты, индексы таргетинга, счётчики |
| Read-replica Campaign PostgreSQL | sync, вне hot path | warming-job при старте пода + fallback при cache miss / CB Open на Redis |
| Kafka | async | publish `bidding.bid_won.v1`; consume CDC и events impression/click для обновления Redis |

Все sync-обращения обёрнуты в Circuit Breaker; при сбое — fallback no-bid (детали в [reliability.md](./reliability.md)). Publish в Kafka — async, не блокирует hot path.

Общий бюджет на bid response — **80 ms p95** (требование DSP-партнёра). Все шаги внутри Bidding выполняются in-process: чтение Redis, подбор кандидатов по таргетингу, проверка бюджета и частот, взвешивание ставок, выбор победителя, формирование response. Hot path **не делает sync-вызовов** к другим сервисам — это сознательное архитектурное решение для удержания SLA (см. [interaction.md §3.4](./interaction.md)).

---

## 5. Модель данных

Bidding Service не владеет OLTP-стором. Источник правды для кампаний, креативов и лимитов бюджетов — Campaign DB (владелец — Campaign Service). Bidding читает эти данные через Redis-кэш и read-replica Campaign PostgreSQL при cache miss. Текущий расход бюджета поддерживается eventual-consistent счётчиком в Redis, обновляемым из Kafka-событий impression — финальная сверка происходит на стороне Financial Service.

**Структуры в кэше Bidding:**

- Кампании (статус, `bid_ceiling`, профиль таргетинга, идентификаторы креативов).
- Бюджеты (остаток, дневной лимит, состояние pacing).
- Индексы таргетинга (множества пользователей по сегментам).
- Счётчики частоты показов per пользователь × кампания.
- Креативы (разметка, URL, размеры).

Кэш работает в режиме **Cache Aside** с гибридной инвалидацией: TTL + явные удаления ключей по CDC из Campaign DB (Debezium → Kafka → consumer внутри Bidding). Конкретные TTL и схемы ключей задаются на уровне реализации команды.

**Принципы работы с данными:**

- Все денежные суммы — в **целочисленных копейках** (1 ₽ = 100 коп.). Защита от ошибок floating point.
- Допустима **eventual consistency** для кампаний и бюджетов в пределах TTL и лага CDC. Защита от перерасхода — короткий TTL + CDC-инвалидация + жёсткий `bid_ceiling` на стороне Bidding.
- **Strong consistency** для финансовых проекций обеспечивается на стороне Financial Service — Idempotent Consumer + транзакции в Finance DB.
