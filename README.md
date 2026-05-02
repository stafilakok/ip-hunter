# Yandex Cloud IP Hunter

Бедолаги, наколякал такое добро для тех, кто устал покупать "секретный скрипт"
у очередного маркетного шамана. Оно запускается с вашего ПК, ходит в Yandex
Cloud API, крутит публичные IPv4 и останавливается, когда выпал адрес из ваших
`target_ips` или `target_cidrs`.

Форкайте, ломайте, чините, присылайте PR. Может, иногда буду обновлять. Может,
буду просто смотреть, как сообщество снова воюет с JSON без запятых.

## Что делает скрипт

- Резервирует случайные статические IPv4 в Yandex Cloud.
- Проверяет адрес по списку нужных IP/CIDR.
- Неподходящие адреса удаляет.
- При лимитах умеет менять folder/cloud, если это включено.
- При успехе оставляет найденный адрес зарезервированным.
- Пишет логи в `run.log`, состояние в `state.json`.
- Может отправить Telegram-уведомление.
- Может открыть сюрприз-видос, потому что страдать надо красиво.

## Что нельзя коммитить

Эти файлы должны жить только локально:

- `config.json`
- `sa-key.json`
- `state.json`
- `run.log`
- `runner.*.log`
- `.env`
- любые `*.key`, `*.pem`

Они уже в `.gitignore`. Если вы все равно запушили ключ в публичный репозиторий,
поздравляю: вы прошли обучение кибербезопасности методом лица об асфальт.
Удалите ключ в Yandex Cloud и создайте новый.

## Установка

Нужен Python 3.9+.

```powershell
python -m pip install PyJWT cryptography
```

Скопируйте пример конфига:

```powershell
Copy-Item .\config.example.json .\config.json
```

Ключ сервисного аккаунта положите рядом:

```text
yc_ip_hunter/
  yc_ip_hunter.py
  config.json
  sa-key.json
```

## Гайд для бедолаг: где что искать в Yandex Cloud

Открываем [console.yandex.cloud](https://console.yandex.cloud). Дальше без
магии, просто тыкаем нужные меню.

### 1. Организация: `organization_id`

Это ID организации, в которой создаются cloud.

Как найти:

1. В левом верхнем углу консоли нажмите на название организации.
2. Откройте **Cloud Center** или страницу организации.
3. Найдите строку **Идентификатор**.
4. Скопируйте значение в `organization_id`.

Выглядит примерно так:

```json
"organization_id": "REPLACE_WITH_ORGANIZATION_ID"
```

Если вы не видите организацию, значит вы либо не там, либо Яндекс снова решил
сыграть в UX-квест. Ищите в верхней панели переключатель организации/облака.

### 2. Платежный аккаунт: `billing_account_id`

Нужен, чтобы новые cloud можно было привязать к биллингу.

Как найти:

1. В левом меню нажмите **Биллинг**.
2. Откройте нужный платежный аккаунт.
3. Найдите **ID платежного аккаунта**.
4. Скопируйте его в `billing_account_id`.

```json
"billing_account_id": "REPLACE_WITH_BILLING_ACCOUNT_ID"
```

Если биллинг не привязан, новые cloud могут создаваться, но ресурсы внутри будут
падать с ошибками. Да, это тот самый момент, где полчаса жизни улетают в окно.

### 3. Служебное облако: `service_cloud_id`

Это cloud, который нельзя удалять. В нем живет сервисный аккаунт, через который
скрипт управляет остальными cloud.

Как найти:

1. В списке облаков выберите ваше служебное облако.
2. Под названием облака найдите ID.
3. Скопируйте его в `service_cloud_id`.

```json
"service_cloud_id": "REPLACE_WITH_SERVICE_CLOUD_ID"
```

Важно: это облако должно жить всегда. Удалите его - и скрипт останется без рук.

### 4. Сервисный аккаунт

Создаем аккаунт, который будет делать всю грязную работу.

Как создать:

1. Выберите служебное облако.
2. Выберите каталог, обычно `default`.
3. В левом меню откройте **Identity and Access Management**.
4. Откройте **Сервисные аккаунты**.
5. Нажмите **Создать сервисный аккаунт**.
6. Назовите его, например `huntersa`.

### 5. Права сервисному аккаунту

Минимально для folder mode:

- `admin` на target cloud.

Для cloud/hybrid mode:

- права на организацию для создания cloud;
- права на billing account для привязки billing;
- `admin` на создаваемые cloud/folder.

Самый простой путь для тех, кто не хочет читать IAM-доки:

1. Откройте нужный ресурс: cloud или organization.
2. Перейдите в **Права доступа**.
3. Нажмите **Добавить пользователя** или **Назначить роли**.
4. Выберите сервисный аккаунт `huntersa`.
5. Добавьте роль `admin`.
6. Сохраните.

Да, `admin` жирно. Да, можно тоньше. Нет, в гайде для бедолаг мы не будем
собирать IAM-мозаику из 14 ролей и трех молитв.

### 6. JSON-ключ: `sa-key.json`

Это файл, через который скрипт авторизуется.

Как скачать:

1. Откройте **Identity and Access Management**.
2. Перейдите в **Сервисные аккаунты**.
3. Откройте `huntersa`.
4. Справа сверху нажмите **Создать новый ключ**.
5. Выберите **Создать авторизованный ключ**.
6. Скачайте JSON.
7. Переименуйте файл в `sa-key.json`.
8. Положите его рядом со скриптом.

В `config.json` должно быть:

```json
"auth": {
  "service_account_key_file": "sa-key.json",
  "iam_token_env": "YC_IAM_TOKEN"
}
```

### 7. Target cloud: `target_cloud_id`

Нужен только для `rotation_mode: "folder"`.

Как найти:

1. Откройте cloud, внутри которого хотите создавать временные folder.
2. Под названием cloud скопируйте ID.
3. Вставьте в `target_cloud_id`.

```json
"target_cloud_id": "REPLACE_WITH_TARGET_CLOUD_ID_FOR_FOLDER_MODE"
```

Если используете `hybrid` или `cloud`, поле можно оставить плейсхолдером/пустым,
а рабочими будут `organization_id`, `billing_account_id`, `service_cloud_id`.

## Как заполнить `config.json`

Базовый живой вариант для hybrid:

```json
{
  "dry_run": false,
  "rotation_mode": "hybrid",
  "organization_id": "ВАШ_ORGANIZATION_ID",
  "billing_account_id": "ВАШ_BILLING_ACCOUNT_ID",
  "service_cloud_id": "ВАШ_SERVICE_CLOUD_ID",
  "auth": {
    "service_account_key_file": "sa-key.json",
    "iam_token_env": "YC_IAM_TOKEN"
  },
  "zone": "ru-central1-a",
  "zones": ["ru-central1-a"],
  "target_ips": [],
  "target_cidrs": [
    "84.201.188.0/23",
    "84.201.184.0/22",
    "84.201.128.0/18",
    "158.160.0.0/16"
  ],
  "max_iterations": 0,
  "max_addresses_per_cloud": 9,
  "max_parallel_clouds": 3,
  "allow_delete_cloud": true,
  "immediate_delete_cloud": true
}
```

`target_cidrs` - это ваши нужные подсети. Хотите другие - меняйте. Скрипт
остановится только на том, что вы сами туда положили.

## Режимы

### `folder`

Создает новый folder внутри одного существующего cloud. Самый спокойный режим,
но если лимиты душат на уровне cloud, будет грустно.

### `cloud`

Создает новый cloud, привязывает billing, крутит IP, удаляет cloud при промахе.
Жестче, но можно упереться в лимит cloud в организации.

### `hybrid`

Основной режим. Крутит адреса, при лимитах переходит к следующему cloud. Для
текущего хаоса это самый рабочий вариант.

## Telegram-уведомления

В `config.json`:

```json
"notifications": {
  "enabled": true,
  "telegram": {
    "enabled": true,
    "bot_token_env": "TELEGRAM_BOT_TOKEN",
    "chat_id": "ВАШ_CHAT_ID"
  }
}
```

Токен лучше хранить в переменной окружения:

```powershell
$env:TELEGRAM_BOT_TOKEN="123456:ABCDEF..."
```

Проверить:

```powershell
python .\yc_ip_hunter.py --config .\config.json --test-telegram
```

Если бот молчит, проверьте:

- токен бота;
- `chat_id`;
- что вы написали боту хотя бы одно сообщение;
- `run.log`.

## Запуск

Сначала dry-run:

```powershell
python .\yc_ip_hunter.py --config .\config.json --dry-run
```

Боевой запуск:

```powershell
python .\yc_ip_hunter.py --config .\config.json --run --yes-delete-cloud
```

Если `allow_delete_cloud: true`, но вы забыли `--yes-delete-cloud`, скрипт
откажется удалять cloud. Это не баг, это защита от пользователей с быстрыми
пальцами и медленным осознанием.

## Как понять, что IP найден

В терминале и `run.log` появится:

```text
TARGET MATCH: allocated IP ... is in configured target ranges.
```

После этого скрипт:

- сохраняет результат в `state.json`;
- не удаляет найденный address;
- отправляет Telegram, если включен;
- открывает сюрприз-видос;
- завершает работу с кодом `0`.

## Частые ошибки

### `Permission denied during create_cloud`

Сервисному аккаунту не хватает прав на organization. Идите в организацию,
**Права доступа**, добавляйте роль.

### `Permission denied to create external address`

Не хватает прав на cloud/folder/VPC. Дайте сервисному аккаунту `admin` на cloud
или folder, где идет охота.

### `Cloud creation quota exceeded`

В организации забита квота cloud. Старые cloud могут висеть в удалении долго.
Скрипт делает deep cleanup перед удалением, но внутреннюю очередь Яндекса за
вас не победит.

### Бот Telegram не пишет

Запустите:

```powershell
python .\yc_ip_hunter.py --config .\config.json --test-telegram
```

Потом смотрите `run.log`.

## Тесты

```powershell
python -m unittest .\test_yc_ip_hunter.py
```

## Дисклеймер

Это утилита для управления собственными облачными ресурсами и проверки
выделенных вам адресов. За лимиты, квоты, биллинг, удаленные cloud, внезапные
расходы и прочие радости взрослой жизни отвечает тот, кто нажал Enter.
