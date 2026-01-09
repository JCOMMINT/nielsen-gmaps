# GMaps Pipeline

This workspace contains Lambda modules for a Google Maps SERP + Bright Data enrichment pipeline.

## Repository structure

- `lambdas/`: Lambda source, tests, and Dockerfiles.
- `infra/`: Infrastructure definitions.
- `INFO/`: Local reference assets (ignored by Git).

## Lambda modules

- `lambdas/s3_ingest_to_sqs`: S3 CSV ingest -> SQS.
- `lambdas/serp_fetcher`: SQS -> Google Maps SERP parsing -> S3 manifest + Bright Data trigger.
- `lambdas/enrich_to_s3`: Bright Data delivery handler -> merge with SERP manifest -> final CSV-level JSON in S3.

Each Lambda is packaged as a container image (dependencies bundled in the image).

## Expected CSV schema

The ingest Lambda expects a CSV with columns:

- `search_term`
- `id`
- `country`

## Environment variables

`s3_ingest_to_sqs`:

- `SQS_QUEUE_URL` (required)
- `REQUIRED_COLUMNS` (optional, comma-separated)
- `LOG_LEVEL` (optional)

`serp_fetcher`:

- `RESULTS_BUCKET` (required)
- `PERSIST_PREFIX` (optional, default `persistence`)
- `MAX_MESSAGES` (optional, cap SQS records per invocation)
- `MAX_RESULTS` (optional, default 5)
- `SERP_BASE_URL` (optional, default `https://www.google.com/maps/search/`)
- `SERP_PARAMS_JSON` (optional JSON string, merged into request params)
- `REQUEST_TIMEOUT` (optional)
- `VERIFY_TLS` (optional, `true`/`false`)
- `USER_AGENT` (optional)
    - `BRIGHTDATA_USERNAME`, `BRIGHTDATA_PASSWORD`, `BRIGHTDATA_PORT`, `BRIGHTDATA_HOST` (optional proxy)
- `BRIGHTDATA_DATASET_ID` (required for enrichment trigger)
- `BRIGHTDATA_TOKEN` (required for enrichment trigger)
- `BRIGHTDATA_S3_BUCKET` (required for Bright Data S3 delivery)
- `BRIGHTDATA_ROLE_ARN` (required for Bright Data S3 delivery)
- `BRIGHTDATA_EXTERNAL_ID` (required for Bright Data S3 delivery)
- Bright Data S3 directory is fixed: `wsapi/<dataset_id>/<timestamp>/csv_name=<csv>`
- `BRIGHTDATA_TRIGGER_TIMEOUT` (optional)

`enrich_to_s3`:

- `RESULTS_BUCKET` (required for persistence reads/writes)
- `FINAL_BUCKET` (optional, default `nielsen-input`)
- `FINAL_PREFIX` (optional, default `final`)
- `PERSIST_PREFIX` (optional, default `persistence`)
- `MAX_RETRIES` (optional, default 3)
- `BATCH_TIMEOUT_HOURS` (optional, default 24)
- `BRIGHTDATA_DATASET_ID`, `BRIGHTDATA_TOKEN`, `BRIGHTDATA_ROLE_ARN`, `BRIGHTDATA_EXTERNAL_ID` (required for retry triggers)
- `SMTP_SERVER`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_PASSWORD` (required for email notifications)
- `NOTIFY_EMAIL` (optional, default `accounts2@mint-data.co`)
- `ENABLE_EMAIL` (optional, set `true` to send notifications)

The notification event must include:

- `enrichment_bucket`, `enrichment_key`
  - Or be an S3 event (object-created) from the Bright Data output bucket.
  - `serp_bucket`, `serp_key` are optional if `RESULTS_BUCKET` is set.
## Local testing

From `GMaps/`:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest
```

## Build Lambda images

From each Lambda folder:

```bash
docker build -t s3-ingest .
```

## Step Functions (high level)

Use a `Map` state to iterate over IDs and run enrichment + merge once the notification flow is defined.
