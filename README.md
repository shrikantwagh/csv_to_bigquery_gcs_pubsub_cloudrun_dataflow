# CSV → BigQuery (GCS upload → Pub/Sub → Cloud Run → Dataflow)

This repository implements a **fully serverless, event‑driven CSV ingestion pipeline** on Google Cloud:

**Cloud Storage → Pub/Sub → Cloud Run → Dataflow (Apache Beam) → BigQuery**

It is designed for scenarios where CSV files are *dropped* into a bucket prefix (for example, `incoming/`) and should be ingested into BigQuery automatically, with minimal operational overhead and strong scalability.

---

## High‑level architecture

1. A **CSV file** is uploaded into a specific **Cloud Storage bucket + prefix** (for example, `gs://my-bucket/incoming/file.csv`).
2. **Cloud Storage notifications** publish an `OBJECT_FINALIZE` event to **Pub/Sub**.
3. A **Pub/Sub push subscription** delivers the event via HTTP POST to a **Cloud Run service**.
4. **Cloud Run**:
   - Validates the event (bucket + prefix filtering)
   - Reads a sample of the CSV
   - Infers the BigQuery schema and sanitized header
   - Creates the BigQuery dataset/table if needed
   - Launches a **Dataflow batch job**
5. **Dataflow (Apache Beam)**:
   - Reads the CSV from GCS
   - Parses rows using the inferred schema
   - Writes rows into **BigQuery**

> ⚠️ Cloud Storage notifications are **bucket‑wide**. Prefix filtering is handled inside Cloud Run.

---

## Project structure

```
csv_to_bigquery_gcs_pubsub_cloudrun_dataflow/
  cloudrun_service/
    main.py                  # Pub/Sub push endpoint (Cloud Run HTTP handler)
    schema_inference.py      # Infer BigQuery schema from CSV sample
    bq_utils.py              # BigQuery dataset/table helpers
    dataflow_launcher.py     # Submits Dataflow jobs
    requirements.txt         # Cloud Run dependencies
    Dockerfile
    setup.py                 # REQUIRED: packages Beam code for Dataflow workers
  beam_pipeline/
    __init__.py              # REQUIRED for Beam worker imports
    pipeline.py              # Apache Beam pipeline (CSV → BigQuery)
  README.md
```

> ❗ `setup.py` and `beam_pipeline/__init__.py` are **mandatory**. Without them, Dataflow workers will fail with `ModuleNotFoundError`.

---

## Prerequisites

- Google Cloud project with **billing enabled**
- `gcloud`, `gsutil`, and `bq` CLIs authenticated
- APIs enabled:
  - Cloud Run
  - Pub/Sub
  - Cloud Storage
  - Dataflow
  - BigQuery
  - Cloud Build
  - Artifact Registry

Enable APIs:

```bash
gcloud services enable \
  run.googleapis.com \
  pubsub.googleapis.com \
  storage.googleapis.com \
  dataflow.googleapis.com \
  bigquery.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com
```

---

## Step 1 — Set environment variables

```bash
export PROJECT_ID="YOUR_PROJECT_ID"
export REGION="us-central1"

export BUCKET="YOUR_BUCKET_NAME"          # bucket name only (NO gs://)
export PREFIX="incoming/"                 # watched prefix

export TOPIC="csv-ingest-topic"
export SUB="csv-ingest-push-sub"

export BQ_DATASET="csv_ingest"
export TABLE_PREFIX="csv_"

export DF_TEMP_LOCATION="gs://$BUCKET/dataflow/temp"
export DF_STAGING_LOCATION="gs://$BUCKET/dataflow/staging"
```

> ❗ Avoid `gs://gs://...` — double prefixes will break Dataflow staging.

---

## Step 2 — Create base resources

### Create bucket

```bash
gcloud storage buckets create gs://$BUCKET \
  --project=$PROJECT_ID \
  --location=$REGION
```

### Create Pub/Sub topic

```bash
gcloud pubsub topics create $TOPIC --project=$PROJECT_ID
```

### Create BigQuery dataset

```bash
bq --project_id=$PROJECT_ID mk --dataset --location=$REGION $BQ_DATASET
```

---

## Step 3 — Create Cloud Storage → Pub/Sub notification

```bash
gsutil notification create \
  -t projects/$PROJECT_ID/topics/$TOPIC \
  -f json \
  -e OBJECT_FINALIZE \
  gs://$BUCKET
```

> Cloud Storage notifications cannot filter by prefix. Cloud Run filters events internally.

Verify:

```bash
gsutil notification list gs://$BUCKET
```

---

## Step 4 — Create Cloud Run service account + IAM

### Create service account

```bash
export CR_SA="csv-ingest-cloudrun-sa"
gcloud iam service-accounts create $CR_SA --project=$PROJECT_ID
export CR_SA_EMAIL="$CR_SA@$PROJECT_ID.iam.gserviceaccount.com"
```

### Grant bucket‑level permissions (REQUIRED)

```bash
gsutil iam ch serviceAccount:$CR_SA_EMAIL:legacyBucketReader gs://$BUCKET
gsutil iam ch serviceAccount:$CR_SA_EMAIL:objectAdmin gs://$BUCKET
```

### Grant project‑level permissions

```bash
# BigQuery write + table creation
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$CR_SA_EMAIL" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$CR_SA_EMAIL" \
  --role="roles/bigquery.user"

# Dataflow job submission
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$CR_SA_EMAIL" \
  --role="roles/dataflow.developer"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$CR_SA_EMAIL" \
  --role="roles/iam.serviceAccountUser"
```

---

## Step 5 — Deploy Cloud Run service

```bash
gcloud run deploy csv-to-bq-ingester \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=./cloudrun_service \
  --service-account=$CR_SA_EMAIL \
  --allow-unauthenticated \
  --set-env-vars=PROJECT_ID=$PROJECT_ID,REGION=$REGION,BUCKET=$BUCKET,PREFIX=$PREFIX,BQ_DATASET=$BQ_DATASET,TABLE_PREFIX=$TABLE_PREFIX,DF_TEMP_LOCATION=$DF_TEMP_LOCATION,DF_STAGING_LOCATION=$DF_STAGING_LOCATION
```

Get the service URL:

```bash
export CR_URL="$(gcloud run services describe csv-to-bq-ingester \
  --region=$REGION \
  --project=$PROJECT_ID \
  --format='value(status.url)')"
```

---

## Step 6 — Create Pub/Sub push subscription

```bash
gcloud pubsub subscriptions create $SUB \
  --project=$PROJECT_ID \
  --topic=$TOPIC \
  --push-endpoint="$CR_URL/" \
  --push-auth-service-account=$CR_SA_EMAIL
```

Pub/Sub now POSTs events directly to Cloud Run using OIDC authentication.

---

## Step 7 — Test the pipeline

Upload a CSV:

```bash
gcloud storage cp ./sample.csv gs://$BUCKET/$PREFIX/sample.csv
```

Watch Cloud Run logs:

```bash
gcloud run services logs read csv-to-bq-ingester \
  --region=$REGION \
  --project=$PROJECT_ID
```

Check Dataflow jobs:

```bash
gcloud dataflow jobs list \
  --region=$REGION \
  --project=$PROJECT_ID
```

Verify BigQuery:

```bash
bq ls $BQ_DATASET
bq query --nouse_legacy_sql \
"SELECT COUNT(*) FROM \`$PROJECT_ID.$BQ_DATASET.${TABLE_PREFIX}sample\`"
```

---

## Configuration reference (Cloud Run env vars)

| Variable | Required | Purpose |
|---|---|---|
| PROJECT_ID | ✅ | GCP project ID |
| REGION | ✅ | Dataflow region |
| BUCKET | ✅ | Bucket name (no gs://) |
| PREFIX | ✅ | Only process objects under this prefix |
| BQ_DATASET | ✅ | Destination dataset |
| TABLE_PREFIX | ❌ | BigQuery table prefix |
| DF_TEMP_LOCATION | ✅ | Dataflow temp GCS path |
| DF_STAGING_LOCATION | ✅ | Dataflow staging GCS path |

---

## Operational notes & gotchas

- **Separate Dataflow staging bucket** in production to avoid noisy events
- **Deduplicate Pub/Sub events** (generation number)
- Add **dead‑letter topic** for push failures
- Use **Flex Templates** to avoid shipping Beam deps from Cloud Run
- Avoid schema inference on every file in production

---

## Clean shutdown (important for cost control)

```bash
# Cancel Dataflow jobs
gcloud dataflow jobs list --status=active --region=$REGION --project=$PROJECT_ID
gcloud dataflow jobs cancel JOB_ID --region=$REGION --project=$PROJECT_ID

# Delete Pub/Sub subscription
gcloud pubsub subscriptions delete $SUB --project=$PROJECT_ID

# Delete Cloud Run service
gcloud run services delete csv-to-bq-ingester --region=$REGION --project=$PROJECT_ID

# Delete GCS notification
gsutil notification list gs://$BUCKET
gsutil notification delete NOTIFICATION_ID
```

---

## License

MIT

