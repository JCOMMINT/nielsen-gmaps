# GMaps Pipeline

This workspace contains Lambda modules for a Google Maps SERP + Bright Data enrichment pipeline.

## Repository structure

- `lambdas/`: Lambda source, tests, and Dockerfiles.
- `infra/`: Infrastructure definitions.
- `INFO/`: Local reference assets (ignored by Git).

## Lambda modules

- `lambdas/s3_ingest_to_sqs`: S3 CSV ingest -> SQS.
- `lambdas/serp_fetcher`: SQS -> Google Maps SERP -> DynamoDB.
- `lambdas/enrich_to_s3`: DynamoDB -> Bright Data enrichment -> S3 Parquet.
- `lambdas/enrichment_to_dynamo`: S3 Parquet -> DynamoDB update.

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

- `DDB_TABLE` (required)
- `SERP_BASE_URL` (optional, default `https://www.google.com/maps/search/`)
- `SERP_PARAMS_JSON` (optional JSON string, merged into request params)
- `REQUEST_TIMEOUT` (optional)
- `VERIFY_TLS` (optional, `true`/`false`)
- `USER_AGENT` (optional)
- `BRIGHTDATA_USERNAME`, `BRIGHTDATA_PASSWORD`, `BRIGHTDATA_PORT`, `BRIGHTDATA_HOST` (optional proxy)

`enrich_to_s3`:

- `DDB_TABLE` (required)
- `ENRICHMENT_BUCKET` (required)
- `ENRICHMENT_PREFIX` (optional)
- `MAX_URLS` (optional)
- `BRIGHTDATA_MODE` (`dataset` or `direct`)
- `BRIGHTDATA_TOKEN` (required)
- `BRIGHTDATA_DATASET_ID` (required for `dataset` mode)
- `BRIGHTDATA_ZONE` (required for `direct` mode)
- `BRIGHTDATA_TRIGGER_URL` (optional)
- `BRIGHTDATA_REQUEST_URL` (optional)

`enrichment_to_dynamo`:

- `DDB_TABLE` (required)

## DynamoDB table

The handlers assume a table with a partition key named `id` (String). Add a sort key if your model needs it, but update the handlers accordingly.

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

Use a `Map` state to iterate over IDs and run enrichment + Dynamo update:

1. `enrich_to_s3` (returns `s3_bucket` + `s3_key`)
2. `enrichment_to_dynamo` (updates Dynamo)

See `infra/step_functions.asl.json` for a starter definition.
