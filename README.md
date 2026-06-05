# Telegram Bot «Отзывы о мужчинах»

## Быстрый старт (Docker)

```bash
docker build -t reviews-bot .

docker run -d --name reviews-bot \
  -e BOT_TOKEN="<token>" \
  -e BOT_USERNAME="<username>" \
  -e ADMIN_IDS="123456789,987654321" \
  -e SECRET_CODE="<secret>" \
  -e PROVIDER_TOKEN="<yookassa_provider_token>" \
  -e PRICE_WEEK=200 \
  -e PRICE_MONTH=500 \
  -e PRICE_SINGLE=99 \
  -e TAX_SYSTEM_CODE=1 \
  -e VAT_CODE=1 \
  -e PAYMENT_SUBJECT=service \
  -e PAYMENT_MODE=full_payment \
  -v $(pwd)/data:/data \
  reviews-bot
```

## Запуск без Docker (Maven)

```bash
mvn -q -DskipTests package
java -jar target/reviews-bot-1.0.0.jar
```

## Переменные окружения

- `BOT_TOKEN` — токен бота (обязательно)
- `BOT_USERNAME` — username бота (по умолчанию `ReviewsBot`)
- `ADMIN_IDS` — список Telegram ID админов через запятую
- `SECRET_CODE` — секретный код доступа для участниц
- `PROVIDER_TOKEN` — токен платежного провайдера YooKassa для Telegram Payments
- `DB_PATH` — путь к sqlite (по умолчанию `data/reviewsbot.sqlite`)
- `PRICE_WEEK`, `PRICE_MONTH`, `PRICE_SINGLE` — цены по умолчанию
- `TAX_SYSTEM_CODE`, `VAT_CODE`, `PAYMENT_SUBJECT`, `PAYMENT_MODE` — параметры фискализации для чеков
- `TG_AUTOFILL_ENABLED` — включить автозаполнение Telegram-данных для карточек с отзывами (по умолчанию `true`)
- `TG_AUTOFILL_INTERVAL_MINUTES` — интервал автозаполнения, по одной карточке за запуск (по умолчанию `5`)
- `TG_AUTOFILL_BACKFILL_FROM_USERNAME` — первый username старого backfill (по умолчанию `Marik_mOnitor`)
- `TG_AUTOFILL_BACKFILL_TO_USERNAME` — последний username старого backfill (по умолчанию `Umarov_od1`)

## Команды

- `/start` — запуск и авторизация
- `/admin` — открыть админ-панель
- `/add` — добавить карточку мужчины (для админа)

## Примечания

- Все кнопки сделаны inline.
- Из-за ограничения Telegram, запрос контакта через кнопку недоступен в inline, поэтому номер вводится текстом.
- Админ может менять цены в админ-панели (команда `admin:prices`).
- Новые отзывы требуют модерации: админы получают кнопки «Одобрить/Отклонить».
- В админ-панели есть пункт «Добавить админа» (ввод Telegram ID).
- Оплата полностью автоматизирована через Telegram Payments + YooKassa, чеки отправляются через провайдера.
- Автозаполнение Telegram-данных генерирует только `tg_id`, не создаёт Telegram username и не трогает карточки, созданные только по номеру телефона.
- Для старых карточек backfill ограничен диапазоном от `@Marik_mOnitor` до `@Umarov_od1`, а новые карточки после запуска сервиса обрабатываются отдельно.
