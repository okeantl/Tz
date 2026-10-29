# Payments

Микросервис асинхронной обработки платежей. Клиент создает платеж, сервис сразу отвечает `202 Accepted`, а сама оплата идет в фоне через эмуляцию платежного шлюза. Когда результат готов, клиенту приходит webhook.

Главное, на чем я сосредоточился: чтобы событие о платеже не потерялось, если RabbitMQ лежит, повторный запрос клиента не создал второй платеж, а повторное сообщение из очереди не списало деньги второй раз.

## Стек

- **FastAPI + Pydantic v2** - API
- **PostgreSQL + SQLAlchemy 2.0** (async, psycopg3) + **Alembic** - данные и миграции
- **RabbitMQ + FastStream** - очередь `payments.new`, retry-очереди и DLQ
- **httpx** - отправка webhook
- **structlog** - логи в JSON
- **Docker Compose** - все поднимается одной командой
- **pytest + testcontainers** - тесты на настоящем Postgres в контейнере

## Архитектура

```
src/
  core/        конфиг, БД, брокер и топология RabbitMQ, ошибки, логи
  models/      модели payments и outbox
  modules/
    payments/  router -> service -> schemas, проверка X-API-Key
  worker/
    outbox_relay.py      публикует события из outbox в RabbitMQ
    payment_consumer.py  consumer очереди payments.new
    processing.py        обработка платежа: шлюз, статус, webhook
  webhook_mock.py        тестовый приемник webhook, чтобы было куда слать
```

Как проходит платеж:

```
POST /api/v1/payments  -> в одной транзакции: строка в payments (pending) + строка в outbox
outbox-relay           -> забирает неопубликованные события, шлет в exchange payments
                          с routing key payments.new, ставит published_at
consumer               -> берет платеж из БД -> шлюз (2-5 сек, 90% успех)
                       -> succeeded/failed + processed_at -> webhook -> webhook_sent_at
```

Схема RabbitMQ:

```
payments (direct) --payments.new--> [payments.new] --reject--> payments.dlx --> [payments.new.dlq]
                                        |      ^
                              ошибка    |      |  истек TTL
                                        v      |
                               [payments.new.retry.N]  TTL = 2с * 2^(N-1)
```

## Как решены основные проблемы

**Outbox.** Если публиковать в RabbitMQ прямо из API после commit, то при падении брокера или процесса между commit и publish событие теряется, платеж навсегда остается в pending. Поэтому API вообще не ходит в RabbitMQ: платеж и событие пишутся в одной транзакции, а отдельный процесс `outbox-relay` раз в секунду забирает неопубликованные события и отправляет их. Брокер работает с publisher confirms, `published_at` ставится только после подтверждения. События выбираются через `SELECT ... FOR UPDATE SKIP LOCKED`, поэтому можно запустить несколько relay и они не возьмут одно и то же. Если брокер недоступен, событие остается в таблице, растет `attempts`, пишется `last_error`, и на следующей итерации отправка повторяется. Проверял руками: останавливал rabbitmq, создавал платеж, после старта брокера он обработался.

**Idempotency key.** Заголовок `Idempotency-Key` обязательный, в БД на него уникальный индекс. Если платеж с таким ключом уже есть, возвращается он же (тот же `payment_id`, заголовок `Idempotent-Replayed: true`), второй строки в outbox не будет. Вместе с ключом хранится sha256 тела запроса: если ключ тот же, а тело другое, ответ `409`, иначе клиент мог бы по ошибке получить чужой платеж. Сумма перед хешем нормализуется, `100.5` и `100.50` считаются одним запросом. Гонку двух одновременных запросов с одним ключом решает уникальный индекс: второй ловит `IntegrityError` и возвращает уже созданный платеж. Это покрыто тестом на 10 параллельных запросов.

**Повторная обработка в consumer.** RabbitMQ доставляет at-least-once, плюс outbox сам по себе может отправить событие дважды (упал после publish, но до commit). Поэтому consumer идемпотентный:
- если `webhook_sent_at` уже стоит, сообщение просто подтверждается
- шлюз вызывается только для платежа в `pending`, статус обновляется условным `UPDATE ... WHERE status = PENDING`, так что параллельный дубль не перезапишет результат
- если упал только webhook, при повторе платеж второй раз не списывается, просто заново отправляется webhook

**Retry и DLQ.** При ошибке (webhook ответил не 2xx, таймаут, упала БД) consumer публикует сообщение в retry-очередь со счетчиком в заголовке `x-attempt` и подтверждает оригинал. У retry-очередей свой TTL и dead-letter обратно в `payments.new`, так получается задержка 2с, потом 4с, и consumer при этом не блокируется на `sleep`. Очередь на каждую попытку отдельная, потому что в одной очереди с разным per-message TTL сообщение с большим TTL держало бы все, что за ним. После 3-й неудачной попытки сообщение делает `reject` без requeue, и RabbitMQ сам кладет его через `payments.dlx` в `payments.new.dlq` (там видны заголовки `x-death` и `x-last-error`). Туда же сразу попадают битые сообщения, которые не проходят валидацию, и сообщения про несуществующий платеж, их ретраить нет смысла.

Неуспешная оплата (10% от шлюза) это не ошибка, а нормальный результат: статус `failed` и webhook с `payment.failed`.

**Аутентификация.** Статический ключ в заголовке `X-API-Key` на всех эндпоинтах `/api/v1/payments`, сравнение через `secrets.compare_digest`.

### Что я бы сделал иначе в проде

- С настоящим шлюзом передавал бы ему `payment_id` как idempotency key, чтобы шлюз сам не списал дважды, если consumer упадет после списания, но до записи статуса
- Подписывал бы webhook (HMAC тела в заголовке), чтобы клиент мог проверить, что запрос от нас
- Чистил бы старые опубликованные строки из outbox по крону
- Отдельный endpoint или скрипт для переотправки сообщений из DLQ после разбора

## API

| Метод | Путь | Что делает |
|---|---|---|
| POST | `/api/v1/payments` | создать платеж, нужен `Idempotency-Key` |
| GET | `/api/v1/payments/{payment_id}` | информация о платеже |
| GET | `/health` | проверка Postgres |

Swagger: http://localhost:8000/docs

## Запуск

Нужен Docker.

```bash
cp .env.example .env
docker compose up -d --build
```

Миграции накатываются автоматически сервисом `migrations` до старта остальных. Поднимутся api (8000), outbox-relay, consumer, webhook-mock (8080), Postgres (5432), RabbitMQ (5672, админка http://localhost:15672, логин и пароль из `.env`).

## Примеры

Создать платеж (`webhook-mock:8080` это адрес тестового приемника внутри docker-сети):

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: change_me" \
  -H "Idempotency-Key: order-1" \
  -H "Content-Type: application/json" \
  -d '{
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Order 1",
    "metadata": {"order_id": 1},
    "webhook_url": "http://webhook-mock:8080/webhook"
  }'
```

```json
{"payment_id": "4ba368da-e7b5-4858-a729-9917a472b53c", "status": "pending", "created_at": "2026-10-29T10:48:28.850718Z"}
```

Повтор с тем же ключом вернет тот же платеж, с тем же ключом и другим телом будет `409`.

Через несколько секунд:

```bash
curl http://localhost:8000/api/v1/payments/4ba368da-e7b5-4858-a729-9917a472b53c -H "X-API-Key: change_me"
```

```json
{
  "payment_id": "4ba368da-e7b5-4858-a729-9917a472b53c",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Order 1",
  "metadata": {"order_id": 1},
  "status": "succeeded",
  "idempotency_key": "order-1",
  "webhook_url": "http://webhook-mock:8080/webhook",
  "created_at": "2026-10-29T10:48:28.850718Z",
  "processed_at": "2026-10-29T10:48:33.011319Z"
}
```

Какие webhook пришли:

```bash
curl http://localhost:8080/webhook/received
```

Проверить retry и DLQ: создать платеж с `"webhook_url": "http://webhook-mock:8080/webhook/fail"`, этот адрес всегда отвечает 500. В логах consumer (`docker compose logs -f consumer`) будут попытки через 2 и 4 секунды, потом `payment_moved_to_dlq`, а сообщение появится в очереди `payments.new.dlq` в админке RabbitMQ.

## Настройки

Все через `.env`, основные:

| Переменная | По умолчанию | |
|---|---|---|
| `API_KEY` | - | ключ для `X-API-Key` |
| `MAX_ATTEMPTS` | 3 | попыток до DLQ |
| `RETRY_BASE_DELAY` | 2 | базовая задержка retry в секундах |
| `PROCESSING_SUCCESS_RATE` | 0.9 | доля успешных оплат |
| `OUTBOX_POLL_INTERVAL` | 1 | как часто relay смотрит в outbox |

`MAX_ATTEMPTS` и `RETRY_BASE_DELAY` задают аргументы retry-очередей, RabbitMQ не дает поменять аргументы у существующей очереди, поэтому после изменения их нужно удалить (или `docker compose down -v`).

## Тесты

```bash
pip install -r requirements.txt
pytest
```

Нужен запущенный Docker: testcontainers поднимает отдельный Postgres и накатывает миграции.

Что проверяется:
- создание платежа пишет событие в outbox в той же транзакции
- повтор с тем же ключом возвращает тот же платеж, другое тело с тем же ключом дает конфликт
- 10 параллельных запросов с одним ключом: один платеж, одно событие
- consumer: успешная и неуспешная оплата, повторное сообщение не списывает второй раз, после упавшего webhook повтор не вызывает шлюз снова
- outbox relay: помечает опубликованные, при недоступном брокере событие остается
- API: без ключа 401, без Idempotency-Key и с невалидным телом 422
- миграции создают таблицы
