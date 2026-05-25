# API Gateway для DSP-интеграции

Связанные документы: [bidding-service.md](./bidding-service.md), [interaction.md](./interaction.md), [reliability.md](./reliability.md).

---

## 1. Место в архитектуре

API Gateway стоит на границе AdScale и обрабатывает весь внешний трафик. Через него проходит каждый bid request до того, как попадёт в Bidding Service.

```text
DSP-партнёр ── HTTPS/OpenRTB ──► API Gateway ── gRPC ──► Bidding Service
                                     │
                                     ├─ TLS termination
                                     ├─ mTLS / API-key authn
                                     ├─ Rate limit per DSP
                                     ├─ REST → gRPC translation
                                     ├─ Circuit Breaker → upstream
                                     └─ Observability (metrics, traces, logs)
```

Gateway инкапсулирует сквозные функции (аутентификация, ограничение частоты запросов, observability), которые иначе пришлось бы дублировать в каждом сервисе. Изменения конфигурации (новый DSP-партнёр, ротация ключей) делаются в одной точке, без выкатки upstream-сервисов.

---

## 2. Маршрутизация bid requests

| Путь | Метод | Upstream | Протокол upstream |
|---|---|---|---|
| `/openrtb/bid` | POST | Bidding Service cluster | gRPC unary |
| `/health` | GET | внутренний health check | — |

**Версионирование.** Версия OpenRTB закладывается в путь URL (например, `/openrtb/vN/bid`). При появлении новой версии стандарта поднимается параллельный маршрут без ломки существующих интеграций.

**REST → gRPC трансляция.** Gateway конвертирует входящий JSON OpenRTB в protobuf для Bidding и обратно. Это снимает с Bidding необходимость парсить JSON на каждом поде. В Envoy реализуется через `grpc_json_transcoder` filter.

**Канареечный rollout.** Gateway поддерживает weighted routing для постепенного переключения трафика на новую версию Bidding Service с автоматическим откатом при превышении SLO.

---

## 3. Аутентификация DSP-партнёров

Используется **двухслойная аутентификация**: mTLS на транспортном уровне + API-ключ на уровне приложения.

**mTLS.** DSP-партнёр получает клиентский сертификат, подписанный CA AdScale. Gateway проверяет цепочку до CA, срок и соответствие `Subject CN` зарегистрированному `dsp_id`. При проблеме сертификата TLS handshake отклоняется на уровне Gateway, запрос не доходит до приложения.

**API-ключ.** После успешного mTLS Gateway требует Bearer-токен в заголовке `Authorization`. Ключ хранится в Gateway как salted hash. Из метаданных ключа извлекается `dsp_id` и передаётся в Bidding через gRPC-metadata. Ротация ключей без downtime — через параллельную работу старого и нового ключа в окне deprecation.

---

## 4. Ограничение частоты запросов

Rate limiting построен на двух уровнях:

- **Per DSP — по контракту.** Каждый DSP-партнёр имеет лимит, заданный его контрактом и хранящийся в профиле API-ключа. Превышение → HTTP 429 с заголовком `Retry-After`. Алгоритм — sliding window.
- **Per IP — защита от unauthenticated атак.** До прохождения аутентификации применяется лимит per IP для защиты от DDoS на этапе TLS handshake и брутфорса API-ключей.

Конкретные значения лимитов задаются в момент подключения партнёра и хранятся отдельно от кода Gateway.

---

## 5. Circuit Breaker перед upstream

Gateway держит Circuit Breaker per upstream cluster — защита от каскадного отказа: если Bidding деградирует, Gateway быстро возвращает no-bid, не нагружая больной сервис.

**Поведение в Open.** При CB Open Gateway возвращает партнёру `HTTP 204 No Content`. С точки зрения OpenRTB-партнёра это нормальный no-bid, что не нарушает контракт и не портит репутацию AdScale в его системе автоматического отбора эндпоинтов. В метриках разделяется (`nbr_reason=UPSTREAM_CIRCUIT_OPEN`).

**Retry в Gateway не применяется.** Hot path аукциона не выдерживает повторов внутри 80 ms бюджета. Партнёр сам решает, повторять ли bid request на следующий impression.

Параметры CB и общая модель — в [reliability.md §1](./reliability.md).

---

## 6. Мониторинг времени отклика

Каждый запрос через Gateway генерирует метрики, traces и структурированный лог.

**Ключевые метрики:**

- Total DSP-perceived latency (histogram) — основной SLI для 80 ms SLO.
- RPS и error rate per `dsp_id` и route.
- Состояние Circuit Breaker per upstream.
- Размер rate limit rejections.

Экспорт через OpenTelemetry → Prometheus, дашборды в Grafana по `dsp_id` и версии Bidding.

**Distributed tracing** через OpenTelemetry — `trace_id` пробрасывается сквозь весь путь bid request (Gateway → Bidding → downstream) для разбора latency по этапам.

**Алерты** на нарушение SLO 80 ms, рост error rate, открытие CB — конкретные пороги настраиваются по результатам нагрузочного тестирования.

---

## 7. Выбор реализации

В качестве Gateway выбран **Envoy** (managed-вариант — в зависимости от текущего облака AdScale: AWS App Mesh, Istio Ingress Gateway, Yandex Application Load Balancer на базе Envoy).

Обоснование:

- **Нативная поддержка gRPC и HTTP/2** — включая `grpc_json_transcoder` для REST → gRPC трансляции из коробки. У NGINX gRPC — вспомогательный режим, transcoding пришлось бы писать руками.
- **Развитая модель Circuit Breaker, retry, outlier detection** — задаются декларативно в YAML, без кастомного кода.
- **xDS protocol** — динамическая конфигурация без перезапуска (важно при добавлении новых DSP-партнёров и canary-переключении трафика).
- **Service Mesh ready** — на 12-мес фазе вводится Service Mesh (Istio / Linkerd), где Envoy уже выступает data plane. Выбор Envoy на Gateway-уровне согласует обе фазы.
- **Managed-вариант снимает операционную сложность.** Команда без SRE использует managed Envoy — внутренние детали реализации скрыты, наружу выходит декларативный YAML-конфиг (TC-06).

Альтернативы:

- **Kong** — хорош для веб-API, но слабее для high-throughput gRPC.
- **NGINX** — знакомый и операционно-простой, но gRPC и REST→gRPC трансляция не из коробки, нет xDS. На 12-мес фазе при введении Service Mesh всё равно потребуется заменить.
- **Cloud-managed gateways** (AWS API Gateway, Yandex API Gateway) — ограниченные возможности кастомизации для gRPC.
