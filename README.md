# GCP Dataflow Pipelines — Apache Beam, Pub/Sub & BigQuery

Batch and streaming data pipelines on **Google Cloud Dataflow** written with the **Apache Beam Python SDK**, landing data in **BigQuery** tables designed with partitioning, clustering and incremental `MERGE` patterns.

## Pipelines
| Pipeline | Source | Sink | Notes |
|----------|--------|------|-------|
| `pipelines/batch_pipeline.py` | CSV files in GCS | BigQuery `staging.orders` | Parse → validate → dead-letter invalid rows |
| `pipelines/streaming_pipeline.py` | Pub/Sub topic | BigQuery `analytics.orders_minute` | Fixed 1-min windows, late-data handling |

## BigQuery design
- `sql/ddl_orders.sql` — table partitioned by `DATE(order_ts)` and clustered by `customer_id, region`
- `sql/merge_orders.sql` — incremental upsert from staging into the fact table, limited to recent partitions to keep scan cost low

## Run
```bash
pip install -r requirements.txt
pytest -q

# Local (DirectRunner)
python pipelines/batch_pipeline.py --input data/orders.csv --output_table PROJECT:staging.orders

# Dataflow
python pipelines/batch_pipeline.py --runner DataflowRunner --project PROJECT --region us-central1 \
  --temp_location gs://BUCKET/tmp --input gs://BUCKET/landing/orders/*.csv --output_table PROJECT:staging.orders

python pipelines/streaming_pipeline.py --runner DataflowRunner --streaming --project PROJECT \
  --input_topic projects/PROJECT/topics/orders --output_table PROJECT:analytics.orders_minute
```

## Tech
Python · Apache Beam · Cloud Dataflow · Pub/Sub · BigQuery · GCS
