# Flink E-commerce Streaming Project

# Problems Encountered & Solutions

This document records the main problems encountered during the implementation of the Flink E-commerce Streaming project, including the error, its cause, the solution, and the lesson learned.

---

# 1. `SHOW TABLES;` Returned Empty Set

## Problem

After opening a new Flink SQL Client session:

```sql
SHOW TABLES;
```

returned:

```text
Empty set
```

## Why Did This Happen?

The Flink SQL tables had been created in a previous SQL session.

When a new SQL Client session was opened, the previous table definitions were not available in the new session.

The physical CSV files were still present, but the SQL table definitions had to be created again.

## Solution

Recreate the required tables in the current SQL session:

```sql
CREATE TABLE ecommerce_events (
    ...
);
```

and:

```sql
CREATE TABLE brand_window_sales (
    ...
);
```

## Lesson Learned

A physical dataset and a Flink SQL table definition are two different things.

```text
Physical File
     ≠
SQL Table Definition
```

The SQL table tells Flink how to read or write the physical data.

---

# 2. `SOURCE` Command Failed

## Problem

The following command was attempted:

```sql
SOURCE '/opt/flink-sql/flink_ecommerce_pipeline.sql';
```

It produced an error similar to:

```text
CalciteException:
Non-query expression encountered in illegal context
```

## Why Did This Happen?

The SQL file contained DDL statements such as:

```sql
CREATE TABLE
```

and an:

```sql
INSERT INTO
```

The way the `SOURCE` command was used did not execute this multi-statement file as expected in the SQL Client environment.

## Solution

The SQL statements were executed directly in the SQL Client.

For the final project, the SQL script was kept as:

```text
sql/flink_ecommerce_pipeline.sql
```

and the individual statements were verified interactively.

## Lesson Learned

A SQL Client command is not necessarily the same as a SQL statement.

It is important to distinguish between:

```text
SQL statements
```

and:

```text
SQL Client commands
```

---

# 3. `RowTime field should not be null`

## Problem

When querying the source table:

```sql
SELECT COUNT(*)
FROM ecommerce_events;
```

Flink returned:

```text
java.lang.RuntimeException:
RowTime field should not be null,
please convert it to a non-null long value.
```

## Why Did This Happen?

The original CSV contained a header:

```text
event_time,event_type,product_id,...
```

Flink was reading this header as if it were a normal data row.

Therefore:

```text
event_time = "event_time"
```

was passed to:

```sql
TO_TIMESTAMP(
    event_time,
    'yyyy-MM-dd HH:mm:ss z'
)
```

The string:

```text
event_time
```

is not a valid timestamp.

Therefore:

```text
event_time_ts = NULL
```

The Event-Time field used by the Watermark could not be NULL.

## Solution

A copy of the dataset was created without the header:

```bash
tail -n +2 data/2019-Oct.csv > data/2019-Oct-no-header.csv
```

Then the source path was changed to:

```text
/opt/flink/data/2019-Oct-no-header.csv
```

## Important

The original file:

```text
2019-Oct.csv
```

was not modified.

The new file:

```text
2019-Oct-no-header.csv
```

is only a processing copy.

## Lesson Learned

When a field is used as Event Time, its timestamp conversion must produce valid, non-null values.

---

# 4. Incorrect CSV Option: `format.ignore-first-line`

## Problem

An attempt was made to solve the header problem using:

```text
format.ignore-first-line
```

This resulted in an error such as:

```text
Missing required options: format
```

## Why Did This Happen?

The configuration was based on an older/legacy style of Flink CSV configuration and did not match the modern connector configuration being used.

## Solution

The header was removed from the processing copy instead.

The final configuration uses:

```sql
'format' = 'csv',
'csv.ignore-parse-errors' = 'true'
```

## Lesson Learned

Do not mix configuration options from different Flink connector versions or APIs.

Always use the configuration syntax supported by the connector/version being used.

---

# 5. Incorrect `format.type` Option

## Problem

Another attempt used:

```sql
'format.type' = 'csv'
```

This caused:

```text
Missing required options: format
```

## Why Did This Happen?

The modern configuration expects:

```sql
'format' = 'csv'
```

not:

```sql
'format.type' = 'csv'
```

## Solution

The final configuration is:

```sql
WITH (
    'connector' = 'filesystem',
    'path' = '/opt/flink/data/2019-Oct-no-header.csv',
    'format' = 'csv',
    'csv.ignore-parse-errors' = 'true'
);
```

## Lesson Learned

The correct connector property is:

```text
format
```

not:

```text
format.type
```

for this configuration.

---

# 6. Filesystem Output Directory Not Found

## Problem

When changing the output from the `print` connector to the filesystem connector, Flink produced an error similar to:

```text
FileNotFoundException:
File /opt/flink/output/brand_window_sales does not exist
```

or:

```text
user running Flink ('flink') has insufficient permissions
```

## Why Did This Happen?

The output path inside the container:

```text
/opt/flink/output/brand_window_sales
```

is connected to the host project directory through Docker volume mounting.

The directory either did not exist or was not writable.

## Solution

Create the directory on the host:

```bash
mkdir -p ~/flink-ecommerce-streaming/output/brand_window_sales
```

Then make it writable:

```bash
chmod -R 777 ~/flink-ecommerce-streaming/output/brand_window_sales
```

After that, the filesystem sink was recreated and the INSERT query was executed again.

## Lesson Learned

For filesystem sinks, always verify:

1. The directory exists.
2. The Docker volume is mounted correctly.
3. The Flink process has write permission.

---

# 7. First Output Used the `print` Connector

## Problem

The first version of:

```text
brand_window_sales
```

used:

```sql
'connector' = 'print'
```

The results appeared in the TaskManager logs.

Example:

```text
+I[2019-10-01T03:30, 2019-10-01T03:35, samsung, 10, 3055.86, 305.59, 9]
```

However, the results were not being stored as a persistent output dataset.

## Why Was This Not Ideal?

The `print` connector is useful for debugging and visual verification.

But the assignment required an executable pipeline with verifiable output, and it was better to persist the results.

## Solution

The output table was changed to use the filesystem connector:

```sql
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
```

## Lesson Learned

Use:

```text
print
```

for quick debugging.

Use:

```text
filesystem
```

when you want persisted output files.

---

# 8. Where Did the Other Results Go?

## Problem

After running:

```sql
SELECT *
FROM brand_window_sales;
```

the SQL Client showed:

```text
Page: Last of 14
```

It looked like only a small number of rows were available.

## Why Did This Happen?

The Flink SQL Client displays large result sets using pagination.

The message:

```text
Page: Last of 14
```

means:

```text
Total pages = 14
Current page = Last page
```

It does not mean that the other results disappeared.

## Solution

Navigate through the Result View pages.

The result can also be queried again:

```sql
SELECT *
FROM brand_window_sales;
```

## Lesson Learned

Always check the pagination indicator before assuming that query results are missing.

---

# 9. Blank Brand Values in the Output

## Problem

Some result rows contained an empty value in:

```text
brand
```

For example:

```text
2019-10-01 03:30:00
2019-10-01 03:35:00
brand = [empty]
```

## Why Did This Happen?

The dataset intentionally contains missing values in the `brand` field.

The pipeline did not invent or remove these values.

## Solution

No special fix was required.

The aggregation correctly preserved the records with missing brand values.

## Lesson Learned

Missing values in source data do not automatically mean that the streaming pipeline is broken.

---

# 10. Why Did the Streaming Job Finish?

## Problem

After processing the dataset, the Flink job changed to:

```text
FINISHED
```

It might seem strange because this is a streaming assignment.

## Why Did This Happen?

The source is a CSV file.

A CSV file is a **bounded source**.

That means Flink eventually reaches the end of the input:

```text
Read CSV
   ↓
Process events
   ↓
Create windows
   ↓
Write results
   ↓
End of file
   ↓
Job FINISHED
```

## Solution

No fix was required.

The assignment uses a file-based dataset, so finishing after processing the complete file is expected.

The project still demonstrates important streaming concepts:

* Event Time
* Watermarks
* Out-of-order events
* Tumbling Windows
* Streaming SQL aggregation

## Lesson Learned

Streaming processing does not necessarily mean that the job must run forever.

The behavior also depends on whether the source is bounded or unbounded.

---

# 11. Why Was `brand_window_sales_view` Not Needed?

## Problem

There was confusion about whether another output table/view was required.

## Why?

The project already has:

```text
ecommerce_events
```

as the source table and:

```text
brand_window_sales
```

as the final output table.

An additional:

```text
brand_window_sales_view
```

would not provide any necessary functionality for this assignment.

## Solution

The final project keeps only the required logical tables:

```text
ecommerce_events
brand_window_sales
```

## Lesson Learned

Avoid creating unnecessary SQL objects.

A simple pipeline is easier to understand and maintain.

---

# 12. Timestamp Format Issue

## Problem

The timestamp format initially needed to be checked carefully.

The dataset contains values such as:

```text
2019-10-01 00:00:00 UTC
```

## Correct Format

The correct Flink timestamp pattern is:

```text
yyyy-MM-dd HH:mm:ss z
```

Therefore:

```sql
TO_TIMESTAMP(
    event_time,
    'yyyy-MM-dd HH:mm:ss z'
)
```

is used.

## Lesson Learned

Timestamp parsing depends on the exact source format.

A small difference in the format string can cause timestamp conversion to return NULL.

---

# 13. Final Working Configuration

After resolving the problems, the working configuration was:

## Source

```text
Connector:
filesystem

Path:
/opt/flink/data/2019-Oct-no-header.csv

Format:
csv
```

## Event Time

```sql
TO_TIMESTAMP(
    event_time,
    'yyyy-MM-dd HH:mm:ss z'
)
```

## Watermark

```sql
WATERMARK FOR event_time_ts AS
    event_time_ts - INTERVAL '5' SECOND
```

## Window

```text
TUMBLE
5 minutes
```

## Filter

```sql
WHERE event_type = 'purchase'
```

## Output

```text
Connector:
filesystem

Path:
/opt/flink/output/brand_window_sales
```

---

# 14. Troubleshooting Summary

| Problem                            | Cause                         | Solution                                    |
| ---------------------------------- | ----------------------------- | ------------------------------------------- |
| `SHOW TABLES` empty                | New SQL session               | Recreate tables                             |
| `SOURCE` failed                    | Script execution issue        | Execute SQL statements correctly            |
| `RowTime field should not be null` | CSV header parsed as data     | Create headerless copy                      |
| `Missing required options: format` | Wrong/legacy CSV option       | Use `'format' = 'csv'`                      |
| `format.type` error                | Incorrect property            | Use `'format' = 'csv'`                      |
| Output `FileNotFoundException`     | Missing directory/permissions | Create directory and grant write permission |
| Results only in logs               | Print connector               | Use filesystem connector                    |
| Only some results visible          | Result pagination             | Navigate pages                              |
| Blank brand                        | Missing source value          | No fix required                             |
| Job became `FINISHED`              | CSV is bounded                | Expected behavior                           |

---

# 15. Final Status

All major implementation problems were resolved.

The final pipeline successfully:

```text
Reads the dataset
      ↓
Parses Event Time
      ↓
Uses a 5-second Watermark
      ↓
Creates 5-minute TUMBLE Windows
      ↓
Filters purchase events
      ↓
Aggregates by brand
      ↓
Calculates sales metrics
      ↓
Writes persistent output
      ↓
Verifies the results in Flink SQL Client
```

The final project is therefore ready for submission.
