```sql
-- ============================================================
-- Flink E-commerce Streaming Pipeline
-- Dataset: eCommerce Events History in Cosmetics Shop
-- ============================================================


-- ============================================================
-- 1. SOURCE TABLE
-- ============================================================

CREATE TABLE ecommerce_events (
    event_time STRING,
    event_type STRING,
    product_id BIGINT,
    category_id BIGINT,
    category_code STRING,
    brand STRING,
    price DECIMAL(10, 2),
    user_id BIGINT,
    user_session STRING,

    -- Convert event_time from STRING to TIMESTAMP
    event_time_ts AS TO_TIMESTAMP(
        event_time,
        'yyyy-MM-dd HH:mm:ss z'
    ),

    -- Allow events to arrive up to 5 seconds out of order
    WATERMARK FOR event_time_ts AS
        event_time_ts - INTERVAL '5' SECOND
)
WITH (
    'connector' = 'filesystem',
    'path' = '/opt/flink/data/2019-Oct-no-header.csv',
    'format' = 'csv',
    'csv.ignore-parse-errors' = 'true'
);


-- ============================================================
-- 2. OUTPUT TABLE
-- ============================================================

CREATE TABLE brand_window_sales (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    brand STRING,
    total_orders BIGINT,
    gross_revenue DECIMAL(18, 2),
    avg_order_value DECIMAL(18, 2),
    unique_buyers BIGINT
)
WITH (
    'connector' = 'filesystem',
    'path' = '/opt/flink/output/brand_window_sales',
    'format' = 'csv'
);


-- ============================================================
-- 3. 5-MINUTE TUMBLE WINDOW AGGREGATION
-- ============================================================

INSERT INTO brand_window_sales
SELECT
    window_start,
    window_end,
    brand,

    COUNT(*) AS total_orders,

    CAST(
        ROUND(SUM(price), 2)
        AS DECIMAL(18, 2)
    ) AS gross_revenue,

    CAST(
        ROUND(AVG(price), 2)
        AS DECIMAL(18, 2)
    ) AS avg_order_value,

    COUNT(DISTINCT user_id) AS unique_buyers

FROM TABLE(
    TUMBLE(
        TABLE ecommerce_events,
        DESCRIPTOR(event_time_ts),
        INTERVAL '5' MINUTES
    )
)

WHERE event_type = 'purchase'

GROUP BY
    window_start,
    window_end,
    brand;
```

