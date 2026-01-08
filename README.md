# GMaps Pipeline

This workspace contains Lambda modules for a Google Maps SERP + Bright Data enrichment pipeline.

## Repository structure

- `lambdas/`: Lambda source, tests, and Dockerfiles.
- `infra/`: Infrastructure definitions.
- `INFO/`: Local reference assets (ignored by Git).

## Lambda modules

- `lambdas/s3_ingest_to_sqs`: S3 CSV ingest -> SQS.
- `lambdas/serp_fetcher`: SQS -> Google Maps SERP parsing -> S3 (per-query JSON + enrichment request).
- `lambdas/enrich_to_s3`: Bright Data notification handler -> merge with SERP JSON -> final JSON in S3.

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
- Bright Data S3 directory is fixed: `bd-results/date=YYYY-MM-DD/csv_name=<csv>`
- `BRIGHTDATA_TRIGGER_TIMEOUT` (optional)

`enrich_to_s3`:

- `FINAL_PREFIX` (optional, default `final`)

The notification event must include:

- `enrichment_bucket`, `enrichment_key`
- `serp_bucket`, `serp_key`
- `final_bucket` (optional if it matches the enrichment bucket)
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
