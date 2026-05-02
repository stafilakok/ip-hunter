# Yandex Cloud IP Hunter

Бедолаги, наколякал такое говно для вас: локальный скрипт, который крутит
публичные IPv4 в Yandex Cloud, проверяет их по списку нужных IP/CIDR и
останавливается, когда выпал подходящий адрес. Форкайте, играйте, чинитесь.
Может, иногда буду обновлять, а может и не буду. Всем БСов и хороших каналов!

Скрипт запускается с ПК, ходит в Yandex Cloud API по ключу сервисного аккаунта
и не требует `yc` CLI.

## Что умеет

- Резервировать случайные статические IPv4 в нужной зоне.
- Проверять IP по `target_ips` и `target_cidrs`.
- Сразу удалять неподходящие адреса.
- Пробовать до `max_addresses_per_cloud` адресов в одном cloud.
- При лимитах переходить к ротации cloud/folder, если включен hybrid/cloud mode.
- Останавливать цикл на первом подходящем IP и оставлять адрес зарезервированным.
- Писать состояние в `state.json`, логи в `run.log`.
- Отправлять Telegram-уведомление при успехе.

## Требования

- Python 3.9+
- Для авторизации через JSON-ключ сервисного аккаунта:

```powershell
python -m pip install PyJWT cryptography
```

Если вместо ключа передаете готовый IAM token через переменную окружения
`YC_IAM_TOKEN`, дополнительные зависимости не нужны.

## Быстрый старт

1. Скопируйте пример конфига:

```powershell
Copy-Item .\config.example.json .\config.json
```

2. Заполните в `config.json`:

- `organization_id`
- `billing_account_id`
- `service_cloud_id`
- `target_cloud_id`, если используете `rotation_mode=folder`
- `auth.service_account_key_file`, обычно `sa-key.json`
- `target_cidrs` или `target_ips`

3. Первый запуск делайте в dry-run:

```powershell
python .\yc_ip_hunter.py --config .\config.json --dry-run
```

4. Боевой запуск:

```powershell
python .\yc_ip_hunter.py --config .\config.json --run --yes-delete-cloud
```

`--yes-delete-cloud` нужен только там, где скрипту разрешено удалять cloud.

## Режимы

### `rotation_mode: "folder"`

Создает новые folder внутри одного существующего cloud. Самый спокойный режим:
меньше шансов забить квоту cloud в организации.

Нужно указать:

```json
{
  "rotation_mode": "folder",
  "target_cloud_id": "REPLACE_WITH_TARGET_CLOUD_ID_FOR_FOLDER_MODE"
}
```

### `rotation_mode: "cloud"`

Создает новый cloud, привязывает billing, пробует адреса, потом отправляет cloud
на удаление. Работает жестче, но упирается в лимит количества cloud в
организации. Async delete не освобождает квоту мгновенно.

Нужно указать:

```json
{
  "rotation_mode": "cloud",
  "allow_delete_cloud": true,
  "immediate_delete_cloud": true,
  "organization_id": "REPLACE_WITH_ORGANIZATION_ID",
  "billing_account_id": "REPLACE_WITH_BILLING_ACCOUNT_ID",
  "service_cloud_id": "REPLACE_WITH_SERVICE_CLOUD_ID"
}
```

### `rotation_mode: "hybrid"`

Основной рабочий режим. Сначала крутит адреса внутри cloud, а при лимитах
переходит к следующему cloud. Именно этот режим обычно нужен, если хочется
поставить процесс на конвейер.

Полезные настройки:

```json
{
  "rotation_mode": "hybrid",
  "max_addresses_per_cloud": 9,
  "max_parallel_clouds": 3,
  "address_iteration_sleep_seconds": 2,
  "cloud_iteration_sleep_seconds": 45,
  "cloud_quota_wait_seconds": 120,
  "max_iterations": 0
}
```

`max_iterations: 0` означает крутить до успеха.

## Telegram-уведомления

В `config.json`:

```json
{
  "notifications": {
    "enabled": true,
    "telegram": {
      "enabled": true,
      "bot_token_env": "TELEGRAM_BOT_TOKEN",
      "chat_id": "REPLACE_WITH_CHAT_ID"
    }
  }
}
```

Токен лучше хранить в переменной окружения:

```powershell
$env:TELEGRAM_BOT_TOKEN="123456:ABCDEF..."
```

Проверить, что бот реально пишет:

```powershell
python .\yc_ip_hunter.py --config .\config.json --test-telegram
```

Если сообщение не пришло, смотрите `run.log`: там будет причина, например
неверный token, chat id или сетевой сбой.

## Как понять, что IP найден

В терминале и `run.log` появится строка:

```text
TARGET MATCH: allocated IP ... is in configured target ranges.
```

После этого скрипт:

- сохраняет результат в `state.json`;
- не удаляет найденный address;
- отправляет Telegram-уведомление, если оно включено;
- завершает работу с кодом `0`.

## Тесты

```powershell
python -m unittest .\test_yc_ip_hunter.py
```

## Дисклеймер

Это утилита для управления собственными облачными ресурсами и проверки
выделенных вам адресов. За лимиты, квоты, биллинг, удаленные cloud и прочие
радости взрослой жизни отвечает тот, кто нажал Enter.
