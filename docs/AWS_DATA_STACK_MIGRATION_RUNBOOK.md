# KairyxAI AWS Data Stack Migration Runbook

## 1) Purpose
This runbook describes the first-party AWS data stack for KairyxAI:

- `Amazon Redshift Serverless` for the analytical warehouse
- `Amazon S3` for raw shards, manifests, exports, and migration landing files
- `Amazon EventBridge + Amazon SQS` for async job dispatch
- `AWS Secrets Manager` for runtime secret resolution
- `RDS/Aurora PostgreSQL` for the control plane

It assumes the application image is the shared portable image from `Dockerfile` and that ECS/Fargate is the target runtime.

## 2) Runtime Contract

### 2.1 Required backend settings

- `DATA_BACKEND_MODE=aws`
- `WAREHOUSE_BACKEND=redshift`
- `OBJECT_STORAGE_BACKEND=s3`
- `MESSAGE_BACKEND=eventbridge_sqs`
- `SECRET_BACKEND=aws_secrets_manager`

### 2.2 Required AWS settings

- `AWS_REGION`
- `REDSHIFT_WORKGROUP_NAME`
- `REDSHIFT_DATABASE`
- `REDSHIFT_SCHEMA`
- `S3_BUCKET_NAME`
- `EVENTBRIDGE_BUS_NAME`
- `SQS_IMPORT_QUEUE_URL`
- `SQS_PREDICTION_QUEUE_URL`
- `SQS_EXPORT_QUEUE_URL`
- `SQS_SCHEDULER_QUEUE_URL`
- `CONTROL_PLANE_DATABASE_URL`

Optional:

- `REDSHIFT_SECRET_ARN`
- `REDSHIFT_DB_USER`

If `REDSHIFT_SECRET_ARN` is unset, the runtime expects IAM-based Redshift Data API access from the ECS task role.

### 2.3 Secret reference contract

Control-plane `*_ref` fields now support:

- `env://NAME`
- `asm://secret-name`
- `gsm://secret-name` during the migration window

Use `asm://...` for all AWS-native deployments.

## 3) AWS Service Topology

### 3.1 ECS services

- `operator-api`
- `import-worker`
- `prediction-worker`
- `export-worker`
- `scheduler-worker`

`operator-api` remains the public HTTP service.

The worker services keep `/health/live` but are expected to consume SQS in-process. They are not designed around public callback ingress in the AWS-native path.

### 3.2 Event flow

`operator-api` publishes command events to `EventBridge`.

Routing rules send those events into dedicated SQS queues:

- `kairyx-import-jobs -> SQS_IMPORT_QUEUE_URL`
- `kairyx-prediction-jobs -> SQS_PREDICTION_QUEUE_URL`
- `kairyx-export-jobs -> SQS_EXPORT_QUEUE_URL`

`EventBridge Scheduler` sends scheduled tick events into `SQS_SCHEDULER_QUEUE_URL`.

Each worker long-polls its queue, processes the message, and deletes it on success. Failed messages rely on queue visibility timeout and DLQ configuration.

## 4) IAM Requirements

### 4.1 Operator API task role

- `events:PutEvents` on the application event bus
- `redshift-data:ExecuteStatement`, `DescribeStatement`, and `GetStatementResult`
- `redshift-serverless:GetCredentials` if IAM auth is used
- `secretsmanager:GetSecretValue` for application-resolved `asm://` refs
- `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, and `s3:ListBucket` on the data bucket

### 4.2 Worker task roles

Each worker role needs:

- the same Redshift Data API permissions as above
- S3 object access for the data bucket
- `sqs:ReceiveMessage`, `DeleteMessage`, `ChangeMessageVisibility`, and `GetQueueAttributes` on its queue and DLQ
- `secretsmanager:GetSecretValue` for resolved connector/provider secrets

### 4.3 Redshift workgroup role

Attach an IAM role to the Redshift workgroup with S3 read access for bulk migration and backfill operations that use `COPY`.

## 5) Data Migration Sequence

### 5.1 Before the cutover

1. Provision Redshift Serverless, S3, EventBridge, SQS queues + DLQs, EventBridge Scheduler, Secrets Manager, and RDS/Aurora PostgreSQL.
2. Deploy the AWS task definitions from `deploy/aws/ecs/task-definitions/`.
3. Run staging validation with production-like data volume.
4. Audit saved SQL, SQL cohorts, and Copilot SQL templates for BigQuery-specific syntax and rewrite them before production cutover.

### 5.2 Direct cutover

1. Freeze new import, prediction, export, and scheduled execution.
2. Snapshot the control-plane Postgres database.
3. Export BigQuery tables to Parquet.
4. Transfer the exported warehouse data and raw GCS objects into S3. AWS DataSync supports Google Cloud Storage as a source for object transfers.
5. Load Redshift from S3 for bulk backfills using `COPY`.
6. Deploy or restart ECS services with the AWS-native env contract.
7. Enable EventBridge rules and the SQS-backed workers.
8. Run smoke jobs for import, prediction, export, scheduler tick, SQL preview, and SQL cohort refresh.
9. Unfreeze production traffic after the smoke jobs pass.

## 6) Validation Checklist

- `GET /health/live` on `operator-api` returns `200`
- one import job completes end-to-end with S3 raw shards and Redshift staging rows
- one prediction job completes and writes `prediction_results` rows in Redshift
- one export job completes from SQS dispatch
- one scheduler tick reaches `scheduler-worker` through `SQS_SCHEDULER_QUEUE_URL`
- representative saved queries and SQL cohorts return equivalent results in Redshift
- no unexpected backlog or DLQ growth is visible in CloudWatch

## 7) Repository Assets

- `deploy/aws/ecs/task-definitions/`
- `deploy/aws/ecs/service-definitions/`
- `deploy/aws/cloudwatch/alarms.json`
- `backend/services/.env.example`
- `docs/PORTABLE_DOCKER_DEPLOYMENT_RUNBOOK.md`
