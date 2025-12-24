# Stage / Prod окружения (AWS)

Сейчас “prod” — это:
- EC2 инстанс `portal-ec2` (SSM Online), который крутит FastAPI портал в Docker.
- 2 бакета: `bronze-bucket-icvt` и `silver-bucket-icvt`
- 2 Lambda: `bronze-bucket-file-reader` и `silver-bucket-file-creator`
- DynamoDB: `invoice-portal-invoices` и `invoice-portal-audit` (портал ими пользуется)

Цель “stage” — отдельная песочница (отдельные бакеты/таблицы/лямбды/инстанс), чтобы можно было ломать/дописывать и не трогать прод данные и клиентские тесты.

## Что делает stage

Stage-ресурсы создаются с суффиксом `-stage`:
- S3: `bronze-bucket-icvt-stage`, `silver-bucket-icvt-stage`
- Lambda: `bronze-bucket-file-reader-stage`, `silver-bucket-file-creator-stage`
- DynamoDB: `invoice-portal-invoices-stage`, `invoice-portal-audit-stage`
- EC2: `portal-ec2-stage` (доступ на `:8000` ограничивается CIDR)

## Скрипты

Все скрипты используют `AWS_PROFILE` (по умолчанию `Adam`) и `AWS_REGION` (по умолчанию `ap-southeast-2`). Настройки окружений лежат в `scripts/env.sh`.

### 1) Разово поднять stage в AWS

Открой доступ к stage порталу только со своего IP:

```bash
STAGE_ALLOWED_CIDR="X.X.X.X/32" ./scripts/provision_stage.sh
```

Скрипт создаст бакеты/таблицы/роли/лямбды/SG/EC2 и выведет URL вида `http://<ip>:8000`.

### 2) Деплой кода в stage или prod

```bash
./scripts/deploy_env.sh stage
./scripts/deploy_env.sh prod
```

Что делает деплой:
- билдит артефакты (`dist/*.zip`, `dist/portal.tar.gz`) из **tracked** файлов (без локальных `.env`, `*.db`, `node_modules` и т.д.)
- заливает `portal.tar.gz` в `s3://<SILVER_BUCKET>/portal/portal.tar.gz`
- обновляет код обеих Lambda
- через SSM обновляет `/opt/portal/src` на EC2 (через `rsync`, с сохранением `.env` и `data/`)

### 3) Промоушен stage → prod

Практика простая: когда stage готов — деплой тот же коммит в prod:

```bash
./scripts/deploy_env.sh prod
```

Если хочешь “железно” фиксировать версии — делай тег/релиз в git и деплой с этого тега.

## Важно

- Stage изолирован по ресурсам (бакеты/таблицы/лямбды/EC2), чтобы “сломать stage” не могло повлиять на прод.
- В portal деплое сохранение `data/` критично, чтобы не перетирать SQLite DB на сервере.
- Если создание stage S3 бакетов упадёт из‑за глобальной коллизии имени — поменяй имена в `scripts/env.sh`.

