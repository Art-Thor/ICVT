# Invoice Portal POC (FastAPI)

Simple portal (FastAPI + static frontend) for the existing S3 → Textract/OpenAI → export pipeline. Ready for local run or EC2 via Docker.

## Included
- Auth: local users in SQLite by default. Routes: `/auth/login`, `/admin/users`, `/admin/jobs`.
- Upload: `/api/upload-url` returns presigned PUT for `bronze-bucket-icvt` (override via env).
- Textract view: `/api/textract` checks JSON in `silver-bucket-icvt/textract-results/...`, returns signed GET.
- Export: `/api/export` checks ZIP in `silver-bucket-icvt/export/...`, returns signed GET.
- UI: `static/index.html` (login → jobs list → upload → polling → export link). Includes document list and PDF overlay viewer.

## Run via Docker
```bash
cd portal
mkdir -p data  # SQLite file
docker compose up --build -d
```
Listens on `:8000`. DB at `./data/portal.db` (override with `DB_URL`).

## Environment vars
- `AWS_REGION` (default `ap-southeast-2`)
- `BRONZE_BUCKET` (default `bronze-bucket-icvt`)
- `SILVER_BUCKET` (default `silver-bucket-icvt`)
- `JWT_SECRET` — change in prod
- `ADMIN_EMAIL`, `ADMIN_PASSWORD` — seed admin
- `ADMIN_RESET` — set to `1` to reset admin password on start
- `DB_URL` — e.g. `sqlite:////data/portal.db`
- `PORT` — uvicorn port

Use EC2 IAM role for S3 access; locally rely on AWS profile/keys.

## Quick test
1. Open `http://<host>:8000`.
2. Login `admin@example.com` / `ChangeMe123!` (or your env).
3. In Admin, create job with `folder_prefix` matching S3 `textract-results/<folder>/`.
4. Select job, upload PDF — file lands in bronze, Lambda runs, JSON appears in silver → portal shows normalized_data/export link when ready.

## Pending for production
- Cognito/OIDC instead of local JWT.
- Move metadata to DynamoDB/RDS; write statuses from Lambda.
- Add Approved/Declined reasons, audit log.
- Batch export and richer UI for statuses/export.
