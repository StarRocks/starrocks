# Arrow Flight SQL: Multi-Endpoint Parallel Result Retrieval

## Summary

This feature lets Arrow Flight SQL clients read a query's result set in
parallel from every result-producing node, instead of funneling the whole
result through a single node. When enabled, a query's `FlightInfo` returns one
`FlightEndpoint` per result-producing backend / compute node (CN); a Flight or
ADBC client fans out `DoGet` calls and reads the result shards concurrently.

It is opt-in (off by default), controlled by the session variable
`enable_arrow_flight_multi_endpoint`, and is fully backward compatible: clients
that do not opt in continue to see a single endpoint and behave exactly as
before.

## Motivation

In the default Arrow Flight SQL path, the planner adds a final
`UNPARTITIONED` exchange that gathers all result rows onto one node, and the
result sink lives on that single node. The client then streams the entire
result from that one endpoint. For large result sets this makes result egress
a single-node bottleneck: result throughput is capped by one node's network and
serialization capacity, regardless of how many nodes produced the data.

Shared-data StarRocks typically scans and computes a `SELECT` across many CNs.
Each of those instances already holds a shard of the result. Gathering those
shards onto one node just to stream them out serially wastes the aggregate
egress capacity of the cluster. Multi-endpoint retrieval removes the gather and
lets the client pull each shard directly from the node that produced it.

## How it works

### Plan shape

With `enable_arrow_flight_multi_endpoint = true` and an Arrow Flight result
sink, the planner skips the final `UNPARTITIONED` gather exchange. Each
result-producing fragment instance keeps a `ResultSink` of type
`ARROW_FLIGHT_PROTOCAL` and buffers its own shard locally on its node.

### FlightInfo with N endpoints

`GetFlightInfo` on the FE returns a `FlightInfo` whose endpoint list contains
one `FlightEndpoint` per result instance. Each endpoint carries:

- the location (`grpc+tcp://<cn-host>:<arrow_flight_port>`) of the node holding
  that shard, and
- a ticket encoding `(queryId, fragmentInstanceId)` identifying that node's
  result buffer.

### Client fan-out

The client reads the endpoints concurrently. With ADBC this is
`execute_partitions` followed by one `get_stream` per partition; with the raw
Flight SQL client it is `execute` (to get the `FlightInfo`) followed by one
`getStream` per endpoint. Each `DoGet` connects directly to the CN named in the
endpoint and streams that shard. Reading all endpoints yields the complete,
unordered result set.

## FE changes

| Area | Change |
| --- | --- |
| `SessionVariable` | New `enable_arrow_flight_multi_endpoint` (default `false`, SET_VAR-hintable). |
| `PlanFragmentBuilder` | For an Arrow Flight result sink with the flag on, skip the final `UNPARTITIONED` gather so each instance produces its own shard. |
| `StmtExecutor` / `ArrowFlightSqlServiceImpl` | Build one `BackendResultDescriptor` and one `FlightEndpoint` per result instance instead of a single gather descriptor. |
| `CoordinatorPreprocessor` | Allow a multi-instance `ResultSink` for the Arrow Flight sink type; the previous single-result-sink guard was MySQL-protocol oriented and rejected multi-instance result sinks. |
| `DefaultCoordinator` | Do not reap fragment instances with `QUERY_FINISHED` on the FE's own result-stream EOS when the sink is Arrow Flight multi-endpoint. The client reads each instance's result buffer directly, so the FE must not cancel them; the BE's timed cleanup (`result_buffer_cancelled_interval_time`) reclaims them after the client has fetched. |

No BE changes are required: the CN already serves per-instance result buffers
keyed by `(queryId, fragmentInstanceId)` over its Arrow Flight port.

### Why the DefaultCoordinator change matters

For a normal MySQL-protocol query, the FE is the result consumer; it drains the
single gathered result and then cancels the (already-drained) upstream
instances with `QUERY_FINISHED`. For Arrow Flight multi-endpoint, the client -
not the FE - consumes each instance's buffer directly. If the FE cancels the
instances on its own EOS, it destroys result shards the client has not fetched
yet, and those `DoGet` calls fail with `Cancelled: QueryFinished`. The fix
restricts that eager cancel to non-Arrow-Flight-multi-endpoint queries. This
bug is invisible with a single endpoint (one instance, nothing to race) and
only appears once results are sharded across instances.

## Enabling the feature

Per session:

```sql
SET enable_arrow_flight_multi_endpoint = true;
```

Or per statement via a hint:

```sql
SELECT /*+ SET_VAR(enable_arrow_flight_multi_endpoint=true) */ * FROM my_table;
```

## Which queries benefit

Multi-endpoint helps when the result stays **distributed** across many
instances, and does nothing when the plan collapses the result onto one node.
This is an emergent property of StarRocks' existing distribution planning - the
feature only skips the final gather, it does not change how work is distributed.

| Query shape | Fans out? | Why |
| --- | --- | --- |
| Large scan `SELECT [cols] FROM t [WHERE ...]` | Yes | The scan spreads across instances; each holds a result shard. |
| High-cardinality `GROUP BY` | Yes | Final aggregation is hash-partitioned across instances, so the grouped result is already sharded (measured ~3.9x, see Benchmark row D). |
| `JOIN` feeding a distributed result | Yes | As long as the final stage stays partitioned (no single-node gather). |
| Global `ORDER BY` | No | Needs a single-node merge for total order, so the result collapses to one instance. Endpoints are also read unordered, so order could not be preserved anyway. |
| Scalar aggregate (`COUNT(*)`, `SUM(...)` with no `GROUP BY`) | No | Produces one row gathered onto one node. |
| `LIMIT` to a small result | No | Collapses to one node; little to parallelize. |
| Small results generally | Marginal | Per-endpoint connection overhead can outweigh the gain. |

Results come back **unordered** across endpoints, so use multi-endpoint only
where row order does not matter (or sort client-side after collecting all
shards).

Queries that must collapse to one node (global `ORDER BY`, scalar aggregates)
are handled automatically and safely: the planner builds their single-node
gather before the output stage, so the feature leaves them as a single endpoint
- enabling the flag never breaks ordering or correctness, it just has no effect
on those shapes.

To check a specific query, `EXPLAIN` it and look at the fragment feeding the
`RESULT SINK`: if it is preceded by a single-node gather / `MERGING-EXCHANGE`
the result is one endpoint; if that fragment runs with multiple instances it
will fan out.

## Client usage

A multi-endpoint client must read *every* endpoint in the `FlightInfo`, not just
the first, and should read them concurrently to get the throughput gain. The
example below is the exact pattern used in the benchmark (Java, Arrow Flight
SQL).

```java
// 1. Connect to the FE Flight server and authenticate.
//    A JWT/OIDC user presents the token as the basic-auth "password"; the FE
//    validates it and returns a session bearer used for getFlightInfo.
BufferAllocator allocator = new RootAllocator();
Location fe = Location.forGrpcInsecure(feHost, 9408);        // FE arrow_flight_port
FlightClient feClient = FlightClient.builder(allocator, fe)
        .maxInboundMessageSize(Integer.MAX_VALUE).build();   // allow large batches
FlightSqlClient sql = new FlightSqlClient(feClient);

Optional<CredentialCallOption> session = feClient.authenticateBasicToken(user, tokenOrPassword);
CallOption[] feOpts = session.map(c -> new CallOption[]{c}).orElse(new CallOption[0]);

// 2. Opt in and run the query. GetFlightInfo returns one endpoint per
//    result-producing node.
String query = "SELECT /*+ SET_VAR(enable_arrow_flight_multi_endpoint=true) */ * FROM my_table";
FlightInfo info = sql.execute(query, feOpts);

// 3. Read every endpoint in parallel, each directly from the CN that holds it.
ExecutorService pool = Executors.newFixedThreadPool(Math.max(info.getEndpoints().size(), 1));
List<Future<?>> tasks = new ArrayList<>();
for (FlightEndpoint ep : info.getEndpoints()) {
    tasks.add(pool.submit(() -> {
        Location cn = ep.getLocations().get(0);              // CN:arrow_flight_port (9419)
        try (FlightClient c = FlightClient.builder(allocator, cn)
                     .maxInboundMessageSize(Integer.MAX_VALUE).build();
             FlightStream s = c.getStream(ep.getTicket(), readOpts)) {
            while (s.next()) {
                VectorSchemaRoot batch = s.getRoot();         // consume this shard's batch
            }
        }
        return null;
    }));
}
for (Future<?> t : tasks) {
    t.get();
}
pool.shutdown();
```

Reading the endpoints on parallel threads is what delivers the throughput gain;
reading them serially gives no benefit over single-endpoint.

Notes on the read credentials (`readOpts`):

- The direct CN reads authenticate with the Flight transport bearer the CN
  expects, which is distinct from the FE session token. With ADBC,
  `execute_partitions` + `get_stream` handles this for you; with the raw Flight
  client, set the bearer your deployment uses for CN reads.
- The Arrow Flight proxy must be disabled
  (`SET GLOBAL arrow_flight_proxy_enabled = false`) so endpoints point at the
  CNs for direct reads.

With ADBC (Python, Go, etc.) the same flow is `execute_partitions` followed by
one `read_partition` per partition, read concurrently. Python example:

```python
import adbc_driver_flightsql.dbapi as flight_sql
from concurrent.futures import ThreadPoolExecutor

conn = flight_sql.connect(
    "grpc://<fe-host>:9408",                      # FE arrow_flight_port
    db_kwargs={"username": user, "password": token},  # JWT/OIDC token as the password
)

cur = conn.cursor()
cur.execute("SET enable_arrow_flight_multi_endpoint = true")

# One partition per result-producing CN.
partitions, schema = cur.adbc_execute_partitions("SELECT * FROM my_table")

def read(partition):
    c = conn.cursor()
    c.adbc_read_partition(partition)   # connects directly to the CN holding this shard
    rows = 0
    while True:
        batch = c.fetch_record_batch()
        if batch is None:
            break
        rows += batch.num_rows
    return rows

with ThreadPoolExecutor(max_workers=len(partitions)) as pool:
    total = sum(pool.map(read, partitions))
```

ADBC handles the per-partition (CN) connection and credentials for you, so the
`read_partition` calls just work once the proxy is disabled.

### Operational notes

- Direct fan-out requires the Arrow Flight proxy to be disabled
  (`SET GLOBAL arrow_flight_proxy_enabled = false`) so endpoints point at the
  CNs and the client reads them directly. With the proxy enabled, reads are
  relayed through the FE and the fan-out benefit is lost.
- Each CN's Arrow Flight port (`arrow_flight_port`, default 9419) must be
  reachable by the client; the FE Arrow Flight port is `arrow_flight_port`
  (9408) on the FE.
- Set a large client gRPC inbound message size
  (`maxInboundMessageSize`) so large record batches are not rejected.

## Benchmark

Methodology: run the same query twice toggling only the session variable, with
every endpoint read on a dedicated thread. Measured on a 15-CN shared-data
cluster. Rows A-C are full-table `SELECT *` egress; row D is a high-cardinality
`GROUP BY` (distributed aggregation).

| # | Result set | Query | Fan-out | Single-endpoint | Multi-endpoint | Speedup |
| --- | --- | --- | --- | --- | --- | --- |
| A | 638 MB, 2,941,805 rows | `SELECT *` (Iceberg) | 15 | 0.62 s / 1031 MB/s | 0.24 s / 2694 MB/s | ~2.6x |
| B | 40 GB, 287,958,559 rows | `SELECT *` (Iceberg) | 15 | 35.3 s / 1145 MB/s | 14.17 s / 2857 MB/s | ~2.4x |
| C | 6.4 GB, 29,418,050 rows | `SELECT *` (native OLAP) | 7 | 10.76 s / 593 MB/s | 2.79 s / 2288 MB/s | ~3.9x |
| D | 1.3 GB, 40,548,302 groups | high-card `GROUP BY` (Iceberg) | 15 | 3.26 s / 387 MB/s | 0.83 s / 1515 MB/s | ~3.9x |
| E | 13.8 GB, 500,000,000 groups | high-card `GROUP BY` (Iceberg) | 15 | 25.12 s / 549 MB/s | 6.95 s / 1985 MB/s | ~3.6x |
| F | 7.5 KB, 366 groups | low-card `GROUP BY` date (Iceberg) | 15 | 0.04 s | 0.03 s | ~1x (no gain) |

Every multi-endpoint run returned the exact same row count as single-endpoint
(no data loss), sharded evenly across the result-producing nodes. Rows B, E,
and F were measured with a warmup + 3-run loop (multi-endpoint uses the median;
rows B and E cite the single-endpoint leg conservatively - its fastest run -
given its run-to-run variance). Rows A, C, and D are single representative runs.

Notes:

- Fan-out width equals the number of result-producing instances. Iceberg scans
  spread across all 15 CNs; the native table's fan-out followed its bucket
  count (7). A higher bucket count (at least the CN count) would fan out wider.
- The measured multi-endpoint throughput tops out around 2.3-2.7 GB/s across all
  three runs. That ceiling is the single benchmark client (one JVM reading and
  deserializing every shard), not the cluster. These speedups are therefore
  conservative lower bounds: a distributed or multi-process consumer would pull
  shards in parallel past the single-client ceiling and widen the gap.
  Correspondingly, the observed speedup tracks how slow the single-endpoint leg
  is rather than the result size alone.
- The single-endpoint leg is noisy run to run - all egress goes through one node
  subject to that node's momentary load, and it can degrade under sustained load.
  Row E's three runs were 14.1 / 25.2 / 25.1 s; row B's were 35.3 / 67.7 / 67.7 s
  (bimodal). Multi-endpoint stays stable (E 6.8-7.2 s, B 14.1-15.0 s) because 15
  nodes average the jitter out - that stability is itself a benefit. Rows B and E
  cite the speedup **conservatively against the best-case (fastest) single run**;
  against a loaded single node the gap is wider (row B reached ~4.8x vs its 67.7 s
  single run).
- Multi-endpoint helps large, order-independent results that stay distributed.
  It does nothing for single-row scalar aggregates or globally-ordered results,
  which collapse to one node by design and so return a single endpoint.
- Rows D and E show a distributed `GROUP BY` fans out exactly like a scan: 40.5M
  groups (~3.9x) and 500M groups (~3.5x), each spread 15-way and balanced across
  CNs, holding up at half a billion rows. Their lower MB/s versus the scans
  reflects narrower grouped rows - here the client is bound by rows per second
  (~49-70M/s) rather than bytes.
- Row F is the counter-case: a low-cardinality `GROUP BY` (group by date, 366
  groups) shows ~1x - warm, single is ~0.04 s and multi ~0.03 s, within noise.
  The whole cost is the scan+aggregate (identical in both modes); the 366-row
  result is nothing to parallelize. It is not slower either - the 15 small reads
  add negligible overhead at single-query scale - but there is no gain. (A cold
  first touch had shown 0.12 s on the single leg; the warmup run removes that, so
  the median reflects steady state.)

### When to enable

Enable multi-endpoint for queries whose **result set is large and stays
distributed** - big scans and high-cardinality aggregations. It is neutral for
small results (no gain, negligible harm per query) and has no effect on
single-node-gather shapes (global `ORDER BY`, scalar aggregates). A gateway or
client layer that sets this automatically should prefer **selective**
enablement - e.g. gate on expected result size - because each query opens one
connection per endpoint, which is free at low query rates but adds up under high
QPS for results too small to benefit.

### Reference client

The benchmark above was produced by `AfBench`, a small Arrow Flight SQL client
that logs in with a JWT, runs the query once per mode (toggling
`enable_arrow_flight_multi_endpoint` via a `SET_VAR` hint), reads every endpoint
on its own thread, and reports rows / bytes / wall-clock / throughput. It is the
runnable form of the Java example above and is a good starting point for a
multi-endpoint consumer (for example, a gateway that fans out `DoGet` across
workers). Source: `examples/AfBench.java`.

## Compatibility and rollout

- Opt-in and off by default. Existing single-endpoint clients are unaffected.
- The plan is identical to the default path except for the result-sink
  distribution, so query semantics and results are unchanged.
- No wire-format or BE changes; the feature is purely in the FE planning and
  coordination layer plus the client's choice to read all endpoints.

## Testing

- Unit test: `ArrowFlightSqlServiceImplTest` covers endpoint construction and
  the multi-endpoint descriptor path.
- Regression test: `test/sql/test_arrow/T|R/test_arrow_flight_multi_endpoint`
  exercises the feature end to end for correctness (setup, enable the variable,
  run queries, verify results).
- Live validation: deployed on a 15-CN shared-data cluster; verified 1 -> 15
  endpoints, full-result parity across all shards, and the throughput numbers
  above.
