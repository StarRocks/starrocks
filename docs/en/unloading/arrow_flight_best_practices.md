---
displayed_sidebar: docs
keywords: ['arrow flight sql', 'performance', 'best practices', 'optimization', 'parquet', 'jdbc']
---

# Arrow Flight SQL Best Practices

This page describes how to get the best performance when reading data via Arrow Flight SQL, whether you use the Arrow Flight JDBC driver or the native Flight SQL client with raw `VectorSchemaRoot` batches.

## JDBC: Use Typed Column Access

When using the Arrow Flight JDBC driver, use `rs.getObject(i)` and cast to the expected Java type. This lets the driver return the native Java type directly without an extra conversion step, which matters most for numeric columns.

```java
while (rs.next()) {
    Integer id   = (Integer) rs.getObject(1);
    String  name = (String)  rs.getObject(2);
    Long    ts   = (Long)    rs.getObject(3);
}
```

### Benchmark: JDBC Accessor Methods (5M rows, includes network)

| Approach | VARCHAR table | Numeric table |
| --------------------------------- | -------------- | -------------- |
| `getObject()` with typed cast | 17,482 ms | **3,755 ms** |
| MySQL JDBC (baseline) | 31,901 ms | 26,929 ms |

## Raw Arrow Batches: Pre-Resolve Vectors and Use Typed Access

When consuming raw Arrow batches via the native `FlightSqlClient` (i.e., iterating over `VectorSchemaRoot` objects), follow two rules.

**Resolve vectors once per batch, not once per row.** Call `root.getVector("column_name")` before the row loop so the lookup cost is paid once per batch rather than once per row.

**Use typed `.get(i)` for numeric vectors.** This returns a primitive value directly — no heap allocation, no GC pressure.

```java
IntVector      idVec    = (IntVector)      root.getVector("id");
SmallIntVector yearVec  = (SmallIntVector) root.getVector("birth_year");
TinyIntVector  monthVec = (TinyIntVector)  root.getVector("birth_month");

for (int i = 0; i < rowCount; i++) {
    record.id         = idVec.get(i);     // int   — no allocation
    record.birthYear  = yearVec.get(i);   // short — no allocation
    record.birthMonth = monthVec.get(i);  // byte  — no allocation
}
```

### Benchmark: Arrow Batch Conversion Cost (5M rows, pre-fetched)

| Approach | VARCHAR table | Numeric table |
| --------------------------------- | -------------- | -------------- |
| Typed `.get*()`, vectors resolved once per batch | **12,476 ms** | **1,055 ms** |
| MySQL JDBC (baseline) | 31,901 ms | 26,929 ms |

## Writing Results to Parquet

Apache Arrow does not include a pre-built Parquet writer for `VectorSchemaRoot`. If your goal is simply to export query results to Parquet files, [INSERT INTO FILES](./unload_using_insert_into_files.md) lets StarRocks write the files server-side without any client-side conversion code. The options below apply when you need client-side Parquet output via Arrow Flight.

### Option 1: Python with PyArrow (Recommended)

PyArrow handles the Arrow → Parquet conversion with no custom write logic. It preserves column types natively (INT32, INT64, TIMESTAMP_MICROS, etc.).

**Streaming batch-by-batch from Arrow Flight:**

```python
import pyarrow.flight as fl
import pyarrow.parquet as pq

client = fl.connect("grpc+tls://host:443")
info   = client.get_flight_info(
    fl.FlightDescriptor.for_command(b"SELECT ..."))

reader = client.do_get(info.endpoints[0].ticket)
with pq.ParquetWriter("output.parquet", reader.schema_arrow, compression="snappy") as writer:
    for batch in reader:
        writer.write_batch(batch)
```

**If the full result fits in memory:**

```python
table = reader.read_all()
pq.write_table(table, "output.parquet", compression="snappy")
```

**Via ADBC (the recommended Python Flight SQL client):**

```python
import adbc_driver_flightsql.dbapi as fl_sql
import pyarrow.parquet as pq

with fl_sql.connect("grpcs://host:443", db_kwargs={"username": "admin", "password": "..."}) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM my_table LIMIT 5000000")
        pq.write_table(cur.fetch_arrow_table(), "output.parquet", compression="snappy")
```

### Option 2: Java WriteSupport

For Java, build a custom `WriteSupport<VectorSchemaRoot>` on top of `org.apache.parquet:parquet-hadoop`. Build the schema and writer once per job, then use typed vector reads inside `WriteSupport.write()`.

**Build schema and writer once:**

```java
MessageType parquetSchema = new SchemaConverter().fromArrow(arrowSchema).getParquetSchema();
ParquetWriter<VectorSchemaRoot> writer = /* build once per job */;

// Per batch:
writer.write(batch);
```

**Use typed reads inside `WriteSupport.write()`:**

```java
class ArrowWriteSupport extends WriteSupport<VectorSchemaRoot> {
    private RecordConsumer recordConsumer;

    @Override
    public void prepareForWrite(RecordConsumer consumer) {
        this.recordConsumer = consumer;
    }

    @Override
    public void write(VectorSchemaRoot root) {
        int rowCount = root.getRowCount();
        for (FieldVector vec : root.getFieldVectors()) {
            if (vec instanceof IntVector) {
                IntVector iv = (IntVector) vec;
                for (int row = 0; row < rowCount; row++) {
                    recordConsumer.addInteger(iv.get(row));
                }
            } // else if (vec instanceof SmallIntVector) ... BigIntVector ... VarCharVector ...
        }
    }
}
```

### Parquet Benchmark

Numbers include both Parquet encoding and file I/O cost (5M rows, EC2 co-located with cluster). VARCHAR and numeric tables are benchmarked separately because they stress different parts of the Arrow encoding path: VARCHAR columns require offset-buffer arithmetic on variable-length data, while numeric columns use fixed-width typed vectors where the gains from typed access are much larger.

#### Java

| Approach | VARCHAR UNCOMP | VARCHAR Snappy | Numeric UNCOMP | Numeric Snappy |
| --------------------------------- | -------------- | -------------- | -------------- | -------------- |
| MySQL JDBC → Parquet | 61,065 ms | 60,181 ms | 31,852 ms | 33,810 ms |
| Arrow Flight JDBC → Parquet | 51,563 ms | 49,671 ms | 15,100 ms | 16,583 ms |

#### Python (PyArrow 21.0 / ADBC 1.8.0)

| Approach | VARCHAR UNCOMP | VARCHAR Snappy | Numeric UNCOMP | Numeric Snappy |
| --------------------------------- | -------------- | -------------- | -------------- | -------------- |
| MySQL JDBC → Parquet | 61,065 ms | 60,181 ms | 31,852 ms | 33,810 ms |
| Arrow Flight + PyArrow | **10,911 ms** | **14,566 ms** | **4,291 ms** | **4,015 ms** |

PyArrow adds almost no overhead on top of the raw network fetch and requires far less code than the Java path. Use PyArrow unless Java is a hard requirement.

## Summary

| Use case | Recommendation |
| --------------------------------- | -------------- |
| Arrow Flight JDBC | Use `getObject()` with typed cast |
| Raw `VectorSchemaRoot` batches | Resolve vectors once per batch; use typed `.get(i)` for numeric columns |
| Arrow → Parquet in Python | `pyarrow.parquet` via ADBC — single function call, no custom code |
| Arrow → Parquet in Java | Hand-written `WriteSupport<VectorSchemaRoot>` with typed vector reads |
