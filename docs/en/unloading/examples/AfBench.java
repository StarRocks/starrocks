// Arrow Flight SQL throughput benchmark: single-endpoint funnel vs multi-endpoint fan-out.
// Logs in with a JWT (env SR_JWT) over Arrow Flight, runs the query WARMUP times (discarded)
// then RUNS times per mode, reads every endpoint in parallel, and reports the median
// wall-clock / throughput (with min/max). Optional SR_QUERY overrides the default SELECT *.
// Set SR_CHECKSUM=1 to also compute an order-independent value checksum per mode and assert
// single-endpoint == multi-endpoint (data sanity across the fan-out); adds per-row CPU, so
// leave it off for pure throughput runs.
//   SR_JWT=<jwt> [SR_USER=<dsid>] [RUNS=3] [WARMUP=1] [SR_CHECKSUM=1] [SR_QUERY="SELECT ..."] \
//     java --add-opens=java.base/java.nio=ALL-UNNAMED -cp "/opt/starrocks/fe/lib/*" \
//     AfBench.java <feHost> <fePort> <db.table>
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.BearerCredentialWriter;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AfBench {
    static BufferAllocator allocator;
    static Location feLocation;
    static CallOption[] feOpts;   // FE session bearer (from JWT login) for getFlightInfo
    static CallOption[] cnOpts;   // CN/BE transport bearer for direct result reads
    static final boolean CHECKSUM =
            "1".equals(System.getenv("SR_CHECKSUM")) || "true".equalsIgnoreCase(System.getenv("SR_CHECKSUM"));

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9408;
        String table = args.length > 2 ? args[2] : "afme.big";

        String user = System.getenv().getOrDefault("SR_USER", "root");
        String jwt = System.getenv("SR_JWT");
        if (jwt == null || jwt.isEmpty()) {
            System.err.println("ERROR: set SR_JWT (and SR_USER) env");
            System.exit(2);
        }

        // Base query: either a full custom query via SR_QUERY (e.g. a high-cardinality
        // GROUP BY) or the default full-table scan of <table>. The multi-endpoint SET_VAR
        // hint is injected right after the leading SELECT so the same query runs both ways.
        String custom = System.getenv("SR_QUERY");
        String qOff, qOn;
        if (custom != null && !custom.trim().isEmpty()) {
            qOff = withHint(custom, false);
            qOn = withHint(custom, true);
            System.out.println("[query] custom: " + custom.trim());
        } else {
            qOff = "SELECT /*+ SET_VAR(enable_arrow_flight_multi_endpoint=false) */ * FROM " + table;
            qOn  = "SELECT /*+ SET_VAR(enable_arrow_flight_multi_endpoint=true)  */ * FROM " + table;
        }

        allocator = new RootAllocator();
        feLocation = Location.forGrpcInsecure(host, port);
        FlightClient feClient = FlightClient.builder(allocator, feLocation)
                .maxInboundMessageSize(Integer.MAX_VALUE).build();
        FlightSqlClient sql = new FlightSqlClient(feClient);
        try {
            // StarRocks JWT login over Flight: present the JWT as the password in the
            // basic-token handshake (like `mysql -u <user> -p<jwt>`); the FE validates it
            // via the JWT security integration and issues a session bearer used for getFlightInfo.
            Optional<CredentialCallOption> cred = feClient.authenticateBasicToken(user, jwt);
            feOpts = cred.isPresent() ? new CallOption[]{cred.get()} : new CallOption[0];
            // Direct CN/BE result reads authenticate with the Flight transport bearer the BE
            // middleware expects (kBearerDefaultToken), NOT the FE session token. Requires the
            // proxy to be disabled (SET GLOBAL arrow_flight_proxy_enabled = false) so endpoints
            // point at the CNs and the client reads them directly.
            cnOpts = new CallOption[]{
                    new CredentialCallOption(new BearerCredentialWriter("bearertoken"))};
            System.out.println("[auth] user=" + user + " jwt-login token=" + cred.isPresent() + "  table=" + table);

            int runs = Integer.parseInt(System.getenv().getOrDefault("RUNS", "3"));
            int warmup = Integer.parseInt(System.getenv().getOrDefault("WARMUP", "1"));
            System.out.printf("[config] runs=%d warmup=%d checksum=%b%n", runs, warmup, CHECKSUM);

            long[] off = runMode(sql, "single-endpoint", qOff, runs, warmup);
            long[] on = runMode(sql, "multi-endpoint ", qOn, runs, warmup);

            if (CHECKSUM) {
                boolean ok = off[0] == on[0] && off[1] == on[1];
                System.out.printf("[checksum] single rows=%,d cksum=%d | multi rows=%,d cksum=%d | %s%n",
                        off[0], off[1], on[0], on[1],
                        ok ? "PASS (values identical across single vs multi)"
                           : "FAIL (MISMATCH - rows or values differ!)");
                if (!ok) {
                    System.exit(1);
                }
            }
        } finally {
            sql.close();
        }
    }

    // Insert the multi-endpoint SET_VAR hint right after the leading SELECT keyword.
    static String withHint(String q, boolean on) {
        String t = q.trim();
        if (t.regionMatches(true, 0, "SELECT", 0, 6)) {
            return "SELECT /*+ SET_VAR(enable_arrow_flight_multi_endpoint=" + on + ") */" + t.substring(6);
        }
        return t;  // not a SELECT; run as-is (hint not applied)
    }

    // Run one mode: WARMUP discarded passes (to warm scan/datacache so both modes compete
    // warm), then RUNS measured passes; report the median wall-clock with min/max.
    // Returns {rows, checksum} for the single-vs-multi value-sanity comparison.
    static long[] runMode(FlightSqlClient sql, String label, String query, int runs, int warmup) throws Exception {
        for (int i = 0; i < warmup; i++) {
            readAll(sql.execute(query, feOpts));
        }
        double[] secs = new double[runs];
        long rows = 0, bytes = 0, checksum = 0;
        int eps = 0;
        for (int i = 0; i < runs; i++) {
            FlightInfo info = sql.execute(query, feOpts);
            if (i == 0) {
                List<FlightEndpoint> e = info.getEndpoints();
                System.out.printf("  %s fan-out: %d endpoints%n", label.trim(), e.size());
                for (FlightEndpoint ep : e) {
                    String loc = ep.getLocations().isEmpty() ? "(none->FE)" : ep.getLocations().get(0).getUri().toString();
                    System.out.printf("    %s%n", loc);
                }
            }
            long t0 = System.nanoTime();
            long[] rb = readAll(info);
            secs[i] = (System.nanoTime() - t0) / 1e9;
            rows = rb[0];
            bytes = rb[1];
            checksum = rb[2];
            eps = info.getEndpoints().size();
            System.out.printf("  %s run %d/%d: %.2fs%n", label.trim(), i + 1, runs, secs[i]);
        }
        java.util.Arrays.sort(secs);
        double median = secs[runs / 2];
        double min = secs[0], max = secs[runs - 1];
        double mb = bytes / 1048576.0;
        System.out.printf("%s  endpoints=%d  rows=%,d  %.1f MB  median %.2fs (min %.2f / max %.2f)  |  %,.0f rows/s  %.1f MB/s%s%n",
                label, eps, rows, mb, median, min, max, rows / median, mb / median,
                CHECKSUM ? ("  cksum=" + checksum) : "");
        return new long[]{rows, checksum};
    }

    // Read every endpoint in parallel. Throws if any endpoint fails (so a dropped shard is loud).
    // Returns {rows, bytes, checksum}. checksum is a commutative (order-independent) sum of
    // per-row hashes, so it is comparable across single-endpoint and multi-endpoint regardless
    // of how rows are split across shards or ordered. Only computed when SR_CHECKSUM is set.
    static long[] readAll(FlightInfo info) throws Exception {
        List<FlightEndpoint> eps = info.getEndpoints();
        ExecutorService pool = Executors.newFixedThreadPool(Math.max(eps.size(), 1));
        java.util.List<Future<long[]>> futs = new java.util.ArrayList<>();
        for (FlightEndpoint ep : eps) {
            futs.add(pool.submit((Callable<long[]>) () -> readEndpoint(ep)));
        }
        long rows = 0, bytes = 0, checksum = 0;
        try {
            for (Future<long[]> f : futs) {
                long[] rb = f.get();
                rows += rb[0];
                bytes += rb[1];
                checksum += rb[2];   // commutative sum -> order-independent across shards
            }
        } finally {
            pool.shutdown();
        }
        return new long[]{rows, bytes, checksum};
    }

    static long[] readEndpoint(FlightEndpoint ep) throws Exception {
        Location loc = ep.getLocations().isEmpty() ? feLocation : ep.getLocations().get(0);
        try (FlightClient c = FlightClient.builder(allocator, loc)
                     .maxInboundMessageSize(Integer.MAX_VALUE).build();
             FlightStream s = c.getStream(ep.getTicket(), cnOpts)) {
            long rows = 0, bytes = 0, checksum = 0;
            while (s.next()) {
                VectorSchemaRoot root = s.getRoot();
                int rc = root.getRowCount();
                rows += rc;
                bytes += root.getFieldVectors().stream().mapToLong(v -> v.getBufferSize()).sum();
                if (CHECKSUM) {
                    List<FieldVector> vecs = root.getFieldVectors();
                    for (int r = 0; r < rc; r++) {
                        checksum += rowHash(vecs, r);   // wrapping add; commutative across rows
                    }
                }
            }
            return new long[]{rows, bytes, checksum};
        }
    }

    // 64-bit FNV-1a over the row's cell values (column index mixed in to catch column swaps,
    // nulls given a distinct sentinel). Independent of batching/ordering.
    static long rowHash(List<FieldVector> vecs, int r) {
        long h = 0xcbf29ce484222325L;
        for (int c = 0; c < vecs.size(); c++) {
            h = fnv(h, (byte) c);
            Object o = vecs.get(c).getObject(r);
            byte[] b = (o == null) ? NULL_SENTINEL : o.toString().getBytes(StandardCharsets.UTF_8);
            for (byte x : b) {
                h = fnv(h, x);
            }
            h = fnv(h, (byte) 0x1f);   // field separator
        }
        return h;
    }

    static final byte[] NULL_SENTINEL = new byte[]{0, 'N', 'U', 'L', 'L'};

    static long fnv(long h, byte b) {
        h ^= (b & 0xffL);
        h *= 0x100000001b3L;
        return h;
    }
}
