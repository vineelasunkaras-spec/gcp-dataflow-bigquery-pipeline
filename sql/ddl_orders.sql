-- Fact table: partitioned by order date, clustered for common filters/joins.
CREATE TABLE IF NOT EXISTS `analytics.fct_orders` (
  order_id     STRING NOT NULL,
  customer_id  STRING,
  region       STRING,
  amount       NUMERIC,
  currency     STRING,
  order_ts     TIMESTAMP,
  _loaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(order_ts)
CLUSTER BY customer_id, region
OPTIONS (
  partition_expiration_days = 1095,
  require_partition_filter = TRUE,
  description = "Order facts loaded incrementally from staging.orders"
);
