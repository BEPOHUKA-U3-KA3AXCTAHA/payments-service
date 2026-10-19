# Payments service

Тестовое: асинхронный процессинг платежей, FastAPI + RabbitMQ + Postgres.

## Запуск

```bash
cp .env.example .env   # поменять API_KEY и WEBHOOK_SECRET
docker compose up -d --build
```

API на http://localhost:8000, админка кролика на http://localhost:15672 (payments / payments). Без `.env` ключ будет `dev-api-key`, для проверки этого хватит. В `docker-compose.override.yml` лежит `webhook-receiver`, он просто печатает в лог все, что ему пришло, удобно смотреть вебхуки.

```bash
curl -X POST localhost:8000/api/v1/payments -H "X-API-Key: dev-api-key" -H "Idempotency-Key: order-1" \
  -H "Content-Type: application/json" \
  -d '{"amount": "1500.00", "currency": "RUB", "description": "Заказ 1",
       "metadata": {"order_id": 1}, "webhook_url": "http://webhook-receiver:8080/payments"}'
curl localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: dev-api-key"
docker compose logs webhook-receiver
```

## Как работает

API пишет платеж и событие в outbox одной транзакцией, relay потом публикует события в кролик. Повтор с тем же `Idempotency-Key` отдает тот же платеж, с другим телом 409.

Consumer сначала берет платеж в базе под лок (`lock_token` + время), поэтому дубль сообщения просто ждет и попытку не тратит. В шлюз уходит id платежа как ключ идемпотентности, так что повтор после таймаута второй раз не спишет. Шлюз тут эмулятор (`emulator/`), отдельный контейнер со своей SQLite.

Ретраи через очереди с TTL: 3 попытки, паузы 5 и 25 секунд, у списания и у вебхука счетчики свои. Когда попытки кончились, платеж помечается `gave_up_at`, а сообщение в DLQ уходит через тот же outbox. Причина лежит в заголовках `x-stage`, `x-attempts`, `x-error`.

Из DLQ возвращать так:

```bash
docker compose run --rm --no-deps consumer python -m app.entrypoints.redrive --all
docker compose run --rm --no-deps consumer python -m app.entrypoints.redrive <payment_id>
```

Еще пара фоновых задач. Relay раз в минуту перепубликует платежи, которые давно висят без движения. Consumer раз в минуту добивает платежи, по которым шлюз так и не ответил (тем же ключом, так что без двойного списания).

Вебхук подписан HMAC: `X-Webhook-Signature: sha256=...` от `"<X-Webhook-Timestamp>.<тело>"`, `X-Webhook-Id` это id платежа, по нему получатель отсекает повторы. На localhost и внутренние адреса вебхуки не шлю, кроме хостов из `WEBHOOK_ALLOWED_HOSTS`.

Метрик нет, только логи.

Настройки в `app/infrastructure/config.py`.

## Тесты

```bash
pip install -r requirements-dev.txt
ruff check . && mypy
pytest
```

Нужен докер, Postgres и RabbitMQ поднимает testcontainers.
