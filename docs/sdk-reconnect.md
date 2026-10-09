# Множество SDK при общем разрыве

Предварительный controlled503 опыт подтвердил синхронизацию фиксированного polling: 32 клиента, период 0.2s, окно 2s без initial snapshot — **288 attempts, peak32/50ms**. После ограниченного exponential backoff и jitter, seed 1909 — **102 attempts, peak17/50ms**. Это MockTransport, не HTTP throughput; данные в [sdk-retry-comparison.json](sdk-retry-comparison.json). Before — baseline `eca29e0da9bbd8a2430f392af1a49bdc7b08fa84`. Повтор текущего варианта: `uv run python -m scripts.sdk_retry_probe --seed 1909`; число attempts/buckets зависит от расписания event loop.

## Real HTTP / PostgreSQL 17

Собственный Docker namespace `codex-p2-19-1009`, сеть 10.241.19.0/24, изолированная БД и стабильный loopback port. Клиенты: Python 3.12.13, macOS-27.2-arm64-arm-64bit; сервер: Python 3.12, Linux ARM64, PostgreSQL 17, один Uvicorn process. Seed 1909, 32 SDK: 16 с SSE + polling, остальные только polling. Интервал 0.2s, max_stale1s. Данные и SDK-ключи созданы только в собственном demo environment; ключи и bearer tokens не сохраняются в отчёт.

Настоящий SIGKILL API прерывает stream и HTTP. Все 32 SDK сохраняли `true` на наблюдении до TTL; все стали `unavailable`/`false` при наблюдении через **1.696s**. Это момент проверки, а не точный измеренный момент истечения: evaluate сам проверяет монотонный TTL, ожидание backoff не продлевает срок.

| Фаза | Длительность | Snapshot attempts | SSE attempts | Peak/50ms |
|---|---|---|---|---|
| Разрыв | 2.198s | 125 | 40 | 16 |
| Восстановление | 3.687s | 197 | 24 | 14 |

Восстановление включает запуск контейнера и healthcheck, не чистую latency SDK. Все клиенты подтвердили текущий snapshot, затем получили новую revision/выключение флага. Ротация ключа очистила все 32 кеша; новый ключ получил текущую revision. Локальный тест подтверждает: более старая revision не заменяет snapshot **и не продлевает его возраст**.

Фоновых задач 48 (32 poll+16 watch), peak outstanding HTTP requests/streams **46**, на клиент не более **2**. После `close()` outstanding0. Это измерение жизненного цикла HTTP response, не число TCP sockets/RSS. Poll сериализован, один SSE iterator закрывается до следующего; concurrent start не создаёт потерянных задач (RED regression→GREEN).

## Границы

Две задачи/до двух outstanding requests на SDK дают предел относительно N клиентов, а не жёсткий глобальный server RPS. Initial start сразу запрашивает snapshot и может дать общий burst: stagger запуска делает вызывающий сервис. Jitter/backoff сглаживают retries; не заменяют admission control. Серверный предел 100 SSE относится к одному process; этот опыт с 16 streams не подтверждает многопроцессный или распределённый лимит. Polling по-прежнему не имеет общего лимита. Недоступность может продлиться после восстановления до backoff+HTTP timeout; TTL при этом fail-closed. Данные не production SLA.

## Повтор

Поднимите отдельный Compose namespace `proof-flagharbor-...` с собственными volumes, стабильным loopback port и свободной явно заданной сетью. Затем:

```bash
uv run python -m scripts.sdk_reconnect_proof --project proof-flagharbor-example --base-url http://127.0.0.1:8190 --compose-file /path/to/isolated-compose.json --clients 32 --seed 1909 --output /tmp/flagharbor-reconnect.json
```

**Сценарий останавливает и возвращает API данного proof namespace.** Обычный project name, внешние HTTP hosts, bind mounts и чужие volume names отвергаются до остановки. Cleanup SDK выполняется всегда; Compose resources создаёт/убирает вызывающий harness. Все counters — SDK transport attempts с monotonic timestamps, включая ошибки соединения, без admin HTTP. Полный receipt: [sdk-reconnect.json](sdk-reconnect.json).
