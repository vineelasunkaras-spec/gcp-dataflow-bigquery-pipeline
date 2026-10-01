-- Incremental upsert: only scan the last 3 days of partitions on both sides.
DECLARE lookback DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY);

MERGE `analytics.fct_orders` T
USING (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_ts DESC) AS rn
    FROM `staging.orders`
    WHERE DATE(order_ts) >= lookback
  ) WHERE rn = 1
) S
ON T.order_id = S.order_id AND DATE(T.order_ts) >= lookback
WHEN MATCHED AND (T.amount != S.amount OR T.region != S.region) THEN
  UPDATE SET amount = S.amount, region = S.region, currency = S.currency,
             order_ts = S.order_ts, _loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (order_id, customer_id, region, amount, currency, order_ts)
  VALUES (S.order_id, S.customer_id, S.region, S.amount, S.currency, S.order_ts);
