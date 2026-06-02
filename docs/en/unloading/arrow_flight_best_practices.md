---
displayed_sidebar: docs
keywords: ['arrow flight sql', 'performance', 'best practices', 'optimization', 'parquet', 'jdbc']
---

# Arrow Flight SQL Best Practices

Arrow Flight SQL is the fastest way to pull large result sets out of StarRocks. Against the MySQL protocol, on the same hardware and against the same cluster, Arrow Flight is consistently faster: **4×–12×** faster at the raw protocol fetch, and **22×–172×** faster end-to-end to a pandas DataFrame. The exact factor depends on row count, column shape, and which MySQL client you compare against. But the speedup is not automatic: how the client code reads the result has a large effect on the end-to-end time, and a few simple mistakes can give back most of it.

This page shows the overall numbers you can expect, summarises the aspects that affect them, and then describes each aspect with the code change and the measured impact.

## Overall Performance

Two comparisons follow. The first measures only the **protocol fetch** — how long it takes for the bytes to arrive and be parsed, with no language-level object conversion. The second measures a real-world Python application where data is read into a `pandas` DataFrame. See [Test Environment](#test-environment) for the hardware.

### Protocol-level fetch (Arrow Flight ADBC vs `mysql --quick`)

`fetch_arrow_table()` drains the network into Arrow buffers without converting cells into Python objects. `mysql --quick` drains the MySQL wire protocol with a streaming C client that parses rows. Both are protocol-only — neither pays for language-native object materialization.

| Workload | Rows | MySQL protocol (`mysql --quick`) | Arrow Flight (`fetch_arrow_table`) | Speedup |
| --- | --- | --- | --- | --- |
| Single numeric column (`SELECT id`) | 5 M | 1,650 ms | 164 ms | **10.1×** |
| Single numeric column (`SELECT id`) | 10 M | 3,120 ms | 250 ms | **12.5×** |
| 20 numeric columns (`SELECT *`) | 5 M | 8,990 ms | 1,252 ms | **7.2×** |
| 20 numeric columns (`SELECT *`) | 10 M | 18,370 ms | 2,449 ms | **7.5×** |
| 20 VARCHAR columns (`SELECT *`) | 5 M | 25,820 ms | 5,615 ms | **4.6×** |
| 20 VARCHAR columns (`SELECT *`) | 10 M | 44,570 ms | 11,059 ms | **4.0×** |

Even with no per-row object cost on either side, Arrow Flight is **4×–12.5×** faster than the MySQL protocol because columnar Arrow buffers travel as fixed-width arrays plus offset buffers, while MySQL wire packets encode each value individually with type-prefix framing. Narrow numeric queries hit the largest ratios because the MySQL protocol's per-value framing overhead dominates when each value is small.

### Real-world Python application — `pd.read_sql` with ADBC vs PyMySQL

The canonical Python pipeline is `pd.read_sql(sql, conn) → pandas.DataFrame`. The connection object you hand it is the entire migration: pass a PyMySQL `Connection` and pandas calls `cursor.fetchall()` + `pd.DataFrame(rows)`, walking every row to build the DataFrame. Pass an ADBC Flight SQL connection and pandas uses ADBC's native Arrow fetch + near-zero-copy DataFrame conversion.

| Workload | Rows | `pd.read_sql(sql, adbc_conn)` (fetch + `to_pandas`) | `pd.read_sql(sql, pymysql_conn)` (fetch + DataFrame) | Speedup |
| --- | --- | --- | --- | --- |
| Single numeric column (`SELECT id`) | 5 M | 164 + 22 = **186 ms** | 23,025 + 1,763 = **24,788 ms** | **133.3×** |
| Single numeric column (`SELECT id`) | 10 M | 250 + 46 = **296 ms** | 47,281 + 3,656 = **50,937 ms** | **172.1×** |
| 20 numeric columns (`SELECT *`) | 5 M | 1,252 + 95 = **1,347 ms** | 103,670 + 34,378 = **138,048 ms** | **102.5×** |
| 20 numeric columns (`SELECT *`) | 10 M | 2,449 + 113 = **2,562 ms** | 183,242 + 66,739 = **249,981 ms** | **97.6×** |
| 20 VARCHAR columns (`SELECT *`) | 5 M | 5,615 + 985 = **6,600 ms** | 105,364 + 39,684 = **145,048 ms** | **22.0×** |
| 20 VARCHAR columns (`SELECT *`) | 10 M | 11,059 + 2,063 = **13,122 ms** | 211,867 + 83,079 = **294,946 ms** | **22.5×** |

Each cell is *fetch* + *convert* = *total*; the speedup is total vs total. Narrow numeric queries hit the largest ratio because the PyMySQL side allocates a Python `int` per cell during fetch and pandas then walks the tuple list during conversion — the ADBC side skips both costs. Arrow's columnar memory format wins twice: it skips per-cell Python object allocation during fetch, and makes the DataFrame conversion almost free afterwards.

If your existing code already uses `pd.read_sql`, the migration is one line:

```python
import adbc_driver_manager
import adbc_driver_flightsql.dbapi as fl
import pandas as pd

with fl.connect(
        uri="grpcs://host:443",
        db_kwargs={
            adbc_driver_manager.DatabaseOptions.USERNAME.value: "admin",
            adbc_driver_manager.DatabaseOptions.PASSWORD.value: "...",
        }) as conn:
    df = pd.read_sql("SELECT * FROM my_table LIMIT 5000000", conn)
```

## Test Environment

| Component | Details                                                                                                                                      |
| --- |----------------------------------------------------------------------------------------------------------------------------------------------|
| Client host | AWS EC2 `t3.2xlarge`, same VPC subnet as the cluster                                                                                         |
| Cluster | 3 FE + 2 BE on `m6g.xlarge`; Arrow Flight on `grpcs://…:443`, MySQL on `:9030`                                                               |
| Java stack | OpenJDK 17, `jdbc:arrow-flight-sql`, `arrow-jdbc`, `parquet-hadoop`                                                                          |
| Python stack | `python` 3.12, `pyarrow` 24.0, `adbc-driver-flightsql` 1.11, `PyMySQL` 1.2                                                                   |
| Workload | Two 20-column tables — one VARCHAR-heavy, one all-integer — plus single-column projections; row counts of 5 M and 10 M via `SELECT … LIMIT N` |
| MySQL drain mode | `cursor.fetchall()` buffered for all measurements                                                                                            |

## Choosing a Client

Before any code-level tuning, the biggest single decision is which client API you read results through. [Interact with StarRocks via Arrow Flight SQL](./arrow_flight.md) covers the full setup for Python ADBC, the Arrow Flight JDBC driver, the Java ADBC driver, and the native `FlightClient`. For performance, those collapse into two paths:

- **Raw Arrow batches via `FlightSqlClient` or ADBC (recommended).** This is the columnar end-to-end path the Flight SQL protocol is designed for: your code receives `VectorSchemaRoot` batches and reads them with primitive-returning vector accessors, with no per-row object allocation. End-to-end (drain + typed conversion), this path is about **10×** faster than Java MySQL JDBC on 10 M numeric rows, and **up to 172×** faster than PyMySQL on narrow numeric queries delivered as a pandas DataFrame. Use it whenever your downstream code can consume columnar data (Pandas, Arrow, ML pipelines, Parquet writers, custom analytics).
- **Arrow Flight JDBC driver (`jdbc:arrow-flight-sql`).** Use this when you need a drop-in `ResultSet` for an existing JDBC code path, or for BI tools like Tableau, Power BI, and DBeaver where the JDBC interface is required. JDBC's API forces the driver to return a boxed `Object` for every cell, so this path cannot reach the performance of raw Arrow batches. The JDBC driver is still substantially faster than MySQL JDBC; it is the right tool when JDBC compatibility is the requirement.

The per-aspect tables further down switch baselines: they compare the Java Arrow Flight JDBC driver against Java MySQL JDBC, not against PyMySQL. The Java MySQL JDBC connector is much faster at row materialization than PyMySQL — for example the same 5 M VARCHAR `SELECT *` takes ~22 s through Java MySQL JDBC versus ~105 s through PyMySQL — so the Java ratios you'll see are smaller than the Python numbers in Overall Performance. Java MySQL JDBC is the right baseline when you are choosing between Java drivers.

The four aspects below apply within whichever client you choose: Aspect 1 is for JDBC consumers, Aspects 2–3 are for raw-batch consumers, and Aspect 4 covers Parquet output from either.

## What Affects Performance

The speedups above assume the client code is written for Arrow. The following four aspects each move the needle by 2× or more on the right workload. Getting them right is the difference between the "tuned" column in the table above and a fetch that looks no faster than MySQL.

1. **JDBC accessor method.** Use `rs.getObject(i)` with a typed cast for numeric columns. `rs.getString(i)` forces the driver to format every value as a string.
2. **Vector resolution scope.** When consuming raw `VectorSchemaRoot` batches, resolve each `FieldVector` once per batch outside the row loop, not once per row.
3. **Typed `.get(i)` for numerics.** On numeric vectors, the typed `.get(i)` returns a primitive with no allocation. The generic accessors box every value.
4. **Parquet writer choice.** PyArrow writes Parquet directly from the Arrow stream with no row-by-row code. Java has no pre-built library for this — every Java path requires a hand-written `WriteSupport<VectorSchemaRoot>` on top of `parquet-hadoop`.

### Aspect 1 — JDBC: Use Typed Column Access

When using the Arrow Flight JDBC driver, use `rs.getObject(i)` and cast to the expected Java type. This lets the driver return the native Java type directly without an extra conversion step, which matters most for numeric columns.

```java
while (rs.next()) {
    Integer id   = (Integer) rs.getObject(1);
    String  name = (String)  rs.getObject(2);
    Long    ts   = (Long)    rs.getObject(3);
}
```

### Benchmark: JDBC Accessor Methods (includes network)

| Workload | MySQL JDBC | Arrow Flight JDBC, typed `getObject()` | Speedup |
| --- | --- | --- | --- |
| VARCHAR — 5 M | 22,651 ms | 12,660 ms | **1.79×** |
| VARCHAR — 10 M | 49,216 ms | 27,646 ms | **1.78×** |
| Numeric — 5 M | 16,043 ms | 3,123 ms | **5.14×** |
| Numeric — 10 M | 38,134 ms | 9,123 ms | **4.18×** |

### Aspect 2 — Raw Arrow Batches: Pre-Resolve Vectors and Use Typed Access

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

### Benchmark: Arrow Batch Conversion Cost (pre-fetched)

The Arrow Flight numbers below isolate the conversion cost: batches are drained from the cluster into memory first, then timed.

| Workload | MySQL JDBC | Typed `.get*()`, vectors resolved once per batch | Speedup |
| --- | --- | --- | --- |
| VARCHAR — 5 M | 22,651 ms | 11,921 ms | **1.90×** |
| VARCHAR — 10 M | 49,216 ms | 24,686 ms | **1.99×** |
| Numeric — 5 M | 16,043 ms | 1,141 ms | **14.1×** |
| Numeric — 10 M | 38,134 ms | 2,092 ms | **18.2×** |

### Aspect 3 — Writing Results to Parquet

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

Numbers include both Parquet encoding and file I/O cost (see [Test Environment](#test-environment)). VARCHAR and numeric tables are benchmarked separately because they stress different parts of the Arrow encoding path: VARCHAR columns require offset-buffer arithmetic on variable-length data, while numeric columns use fixed-width typed vectors where the gains from typed access are much larger.

#### Java (5 M and 10 M rows)

Both rows use the same `parquet-hadoop` write path (`MySqlParquetConverter` + `arrow-jdbc` adapter, batch size 65,536) so the only variable is the inbound JDBC driver.

| Approach | Rows | VARCHAR UNCOMP | vs MySQL | VARCHAR Snappy | vs MySQL | Numeric UNCOMP | vs MySQL | Numeric Snappy | vs MySQL |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MySQL JDBC → Parquet | 5 M | 55,477 ms | 1.0× | 54,888 ms | 1.0× | 24,006 ms | 1.0× | 25,289 ms | 1.0× |
| Arrow Flight JDBC → Parquet | 5 M | **46,341 ms** | **1.20×** | **47,881 ms** | **1.15×** | **13,978 ms** | **1.72×** | **14,297 ms** | **1.77×** |
| MySQL JDBC → Parquet | 10 M | 110,229 ms | 1.0× | 116,999 ms | 1.0× | 50,509 ms | 1.0× | 49,126 ms | 1.0× |
| Arrow Flight JDBC → Parquet | 10 M | **91,386 ms** | **1.21×** | **102,534 ms** | **1.14×** | **29,739 ms** | **1.70×** | **30,102 ms** | **1.63×** |

#### Python (PyArrow 24.0.0 / ADBC 1.11.0)

The MySQL baseline is the same Java MySQL JDBC → Parquet number from the table above; "MySQL → PyArrow" is not a real path because there is no MySQL → Arrow adapter outside of `arrow-jdbc`. Python numbers were collected at 5 M only.

| Approach | VARCHAR UNCOMP | vs MySQL | VARCHAR Snappy | vs MySQL | Numeric UNCOMP | vs MySQL | Numeric Snappy | vs MySQL |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MySQL JDBC → Parquet (Java baseline, 5 M) | 55,477 ms | 1.0× | 54,888 ms | 1.0× | 24,006 ms | 1.0× | 25,289 ms | 1.0× |
| Arrow Flight + PyArrow (5 M) | **10,675 ms** | **5.20×** | **14,128 ms** | **3.89×** | **3,953 ms** | **6.07×** | **3,848 ms** | **6.57×** |

PyArrow adds almost no overhead on top of the raw network fetch and requires far less code than the Java path. Use PyArrow unless Java is a hard requirement.

### Summary

| Use case | Recommendation |
| --- | --- |
| Arrow Flight JDBC | Use `getObject()` with typed cast |
| Raw `VectorSchemaRoot` batches | Resolve vectors once per batch; use typed `.get(i)` for numeric columns |
| Arrow → Parquet in Python | `pyarrow.parquet` via ADBC — single function call, no custom code |
| Arrow → Parquet in Java | Hand-written `WriteSupport<VectorSchemaRoot>` with typed vector reads |
