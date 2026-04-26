# Demo Operations SQL

These are manual SQL examples for demonstrating UPDATE/DELETE behavior on the Hermes Kafka tables and propagation into `hermes_features` and Iceberg.

## Demo 1: Update Cart Item

```sql
UPDATE tbl_hermes_cart
SET item_id = 'premium-widget', event_time = NOW()
WHERE user_id = 123;
```

## Demo 2: Delete Test Data

```sql
DELETE FROM tbl_hermes_cart WHERE user_id = 99999;
DELETE FROM tbl_hermes_page WHERE user_id = 99999;
DELETE FROM tbl_hermes_purchase WHERE user_id = 99999;
```

## Demo 3: Correct Purchase Amount

```sql
UPDATE tbl_hermes_purchase
SET amount = 15.99
WHERE user_id = 456 AND amount = 1599.00;
```

## Demo 4: Insert Manual Records

```sql
INSERT INTO tbl_hermes_page (user_id, page_id, event_time)
VALUES (777, 'demo-landing-page', NOW());

INSERT INTO tbl_hermes_cart (user_id, item_id, event_time)
VALUES (777, 'demo-product', NOW());

INSERT INTO tbl_hermes_purchase (user_id, amount, event_time)
VALUES (777, 49.99, NOW());
```

## Demo 5: Batch Update Campaign

```sql
UPDATE tbl_hermes_cart
SET item_id = item_id || '-vip'
WHERE user_id IN (100, 200, 300);
```

## Demo 6: View Funnel Changes in Real-Time

```sql
SELECT * FROM hermes_features
ORDER BY window_start DESC
LIMIT 5;

SELECT
    COALESCE(s.window_start, t.window_start) as window_start,
    s.viewers as source_viewers,
    t.viewers as table_viewers,
    s.carters as source_carters,
    t.carters as table_carters,
    s.purchasers as source_purchasers,
    t.purchasers as table_purchasers
FROM funnel s
FULL OUTER JOIN hermes_features t
    ON s.window_start = t.window_start
ORDER BY COALESCE(s.window_start, t.window_start) DESC
LIMIT 10;
```

## Demo 7: Check Table Contents

```sql
SELECT 'page_views' as table_name, count(*) as record_count FROM tbl_hermes_page
UNION ALL
SELECT 'cart_events', count(*) FROM tbl_hermes_cart
UNION ALL
SELECT 'purchases', count(*) FROM tbl_hermes_purchase;

SELECT 'page' as event_type, page_id as detail, event_time
FROM tbl_hermes_page WHERE user_id = 123
UNION ALL
SELECT 'cart', item_id, event_time
FROM tbl_hermes_cart WHERE user_id = 123
UNION ALL
SELECT 'purchase', amount::varchar, event_time
FROM tbl_hermes_purchase WHERE user_id = 123
ORDER BY event_time DESC;
```

## Demo 8: Iceberg Sink Propagation

1. Ensure `sink_hermes_features_to_iceberg.sql` is deployed.
2. Check current state in RisingWave:

```sql
SELECT * FROM hermes_features ORDER BY window_start DESC LIMIT 5;
```

3. Check current state in Iceberg (via Trino):

```sql
SELECT * FROM iceberg.public.hermes_features ORDER BY window_start DESC LIMIT 5;
```

4. Modify source data in RisingWave:

```sql
UPDATE tbl_hermes_purchase SET amount = 999.99 WHERE user_id = 100;
```

5. Verify updates propagate:

```sql
SELECT * FROM hermes_features ORDER BY window_start DESC LIMIT 5;
SELECT * FROM iceberg.public.hermes_features ORDER BY window_start DESC LIMIT 5;
```
