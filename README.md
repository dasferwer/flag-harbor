# FlagHarbor

[![Проверки](https://github.com/dasferwer/flag-harbor/actions/workflows/ci.yml/badge.svg)](https://github.com/dasferwer/flag-harbor/actions/workflows/ci.yml)

Сервис управляемого включения функций с Python SDK. Флаг можно открыть организации, группе или проценту аудитории, затем постепенно расширить доступ от 1% до 100%. Уже включённые пользователи остаются в группе, пока не меняются идентификаторы или правила таргетинга.

SDK проверяет флаги по локальному снимку без сетевого запроса на каждую проверку. Изменения приходят через SSE, фоновый опрос страхует потерянные уведомления. Если конфигурацию давно не удаётся обновить, SDK возвращает `false`.

## Запуск

Нужны Docker Compose, а для локальной разработки и recovery — Python 3.12 и uv.

```bash
make up
make test
make smoke
uv sync --extra dev --frozen
make check
make recovery
```

Swagger: <http://localhost:8190/docs>. Вход: `demo@example.com` / `FlagHarborDemo123!` через `/auth/login`.

Локальный SDK-ключ: `fh_local_demo_server_key_replace_before_deployment`. Он подходит к demo-окружению с флагом `new-search`: 1% пользователей, организация `pilot-org` и группа `beta`. В базе хранится только хеш ключа. Для каждого нового окружения выдаётся свой случайный ключ; demo-значение предназначено для запуска на своём компьютере.

## SDK

Из каталога проекта выполнить `uv sync --frozen`, затем запустить пример через `uv run python`:

```python
import asyncio
from flagharbor.sdk import FlagClient


async def main():
    async with FlagClient(
        "http://localhost:8190",
        "fh_local_demo_server_key_replace_before_deployment",
        max_stale=30,
        refresh_interval=2,
    ) as flags:
        result = flags.evaluate(
            "new-search",
            {"user_id": "user-42", "organization_id": "pilot-org"},
        )
        print(result.value, result.reason, result.revision)


asyncio.run(main())
```

В приложении клиент нужно создать один раз на его lifespan и закрыть при остановке. После `close` создать новый экземпляр. `evaluate` синхронный и не обращается к сети; обновление снимка работает в фоне. SDK поставляется в общем Python-пакете проекта, отдельной публикации в PyPI нет.

## Что проверяет демонстрация

`make smoke` расширяет аудиторию 1% → 10% → 100% и сверяет вложенность групп на 10 000 пользователях. Затем проверяет отключение по SSE, совпадение решения API и SDK, отзыв ключа и время локальной проверки. `make recovery` завершает API через SIGKILL, проверяет кратковременный кеш, отказ после его истечения и восстановление настроек после запуска сервера.

Изменения флагов защищены `If-Match`: устаревший редактор получает 412. Конфигурация, новая версия и журнал действий фиксируются одной транзакцией. SDK-ключ даёт чтение, но не управление.

Отключение быстрое при доступном сервере и соединении SSE. Мгновенное глобальное отключение во время сетевого разделения не гарантируется: предел задаёт `max_stale`, по умолчанию 30 секунд с последнего успешного обновления. Флаги управляют поведением продукта, но не заменяют проверку прав доступа.

- [Архитектура и согласованность](docs/architecture.md)
- [Работа с API и эксплуатация](docs/runbook.md)
- [Правила SDK и кеша](docs/sdk.md)
- [Фактические проверки](docs/verification.md)
- [Объяснение для интервью](docs/interview.md)
- [Снимок OpenAPI](docs/openapi.json)

Python 3.12, FastAPI, PostgreSQL 17, asyncpg, SQLAlchemy, httpx, JWT, Alembic. Код — [MIT](LICENSE).
