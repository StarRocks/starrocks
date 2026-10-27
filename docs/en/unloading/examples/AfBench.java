// Arrow Flight SQL throughput benchmark: single-endpoint funnel vs multi-endpoint fan-out.
// Logs in with a JWT (env SR_JWT) over Arrow Flight, runs the query WARMUP times (discarded)
// then RUNS times per mode, reads every endpoint in parallel, and reports the median
// wall-clock / throughput (with min/max). Optional SR_QUERY overrides the default SELECT *.
//   SR_JWT=<jwt> [SR_USER=<dsid>] [RUNS=3] [WARMUP=1] [SR_QUERY="SELECT ..."] \
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
import org.apache.arrow.vector.VectorSchemaRoot;

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
            System.out.printf("[config] runs=%d warmup=%d%n", runs, warmup);

            runMode(sql, "single-endpoint", qOff, runs, warmup);
            runMode(sql, "multi-endpoint ", qOn, runs, warmup);
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
    static void runMode(FlightSqlClient sql, String label, String query, int runs, int warmup) throws Exception {
        for (int i = 0; i < warmup; i++) {
            readAll(sql.execute(query, feOpts));
        }
        double[] secs = new double[runs];
        long rows = 0, bytes = 0;
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
            eps = info.getEndpoints().size();
            System.out.printf("  %s run %d/%d: %.2fs%n", label.trim(), i + 1, runs, secs[i]);
        }
        java.util.Arrays.sort(secs);
        double median = secs[runs / 2];
        double min = secs[0], max = secs[runs - 1];
        double mb = bytes / 1048576.0;
        System.out.printf("%s  endpoints=%d  rows=%,d  %.1f MB  median %.2fs (min %.2f / max %.2f)  |  %,.0f rows/s  %.1f MB/s%n",
                label, eps, rows, mb, median, min, max, rows / median, mb / median);
    }

    // Read every endpoint in parallel. Throws if any endpoint fails (so a dropped shard is loud).
    static long[] readAll(FlightInfo info) throws Exception {
        List<FlightEndpoint> eps = info.getEndpoints();
        ExecutorService pool = Executors.newFixedThreadPool(Math.max(eps.size(), 1));
        java.util.List<Future<long[]>> futs = new java.util.ArrayList<>();
        for (FlightEndpoint ep : eps) {
            futs.add(pool.submit((Callable<long[]>) () -> readEndpoint(ep)));
        }
        long rows = 0, bytes = 0;
        try {
            for (Future<long[]> f : futs) {
                long[] rb = f.get();
                rows += rb[0];
                bytes += rb[1];
            }
        } finally {
            pool.shutdown();
        }
        return new long[]{rows, bytes};
    }

    static long[] readEndpoint(FlightEndpoint ep) throws Exception {
        Location loc = ep.getLocations().isEmpty() ? feLocation : ep.getLocations().get(0);
        try (FlightClient c = FlightClient.builder(allocator, loc)
                     .maxInboundMessageSize(Integer.MAX_VALUE).build();
             FlightStream s = c.getStream(ep.getTicket(), cnOpts)) {
            long rows = 0, bytes = 0;
            while (s.next()) {
                VectorSchemaRoot root = s.getRoot();
                rows += root.getRowCount();
                bytes += root.getFieldVectors().stream().mapToLong(v -> v.getBufferSize()).sum();
            }
            return new long[]{rows, bytes};
        }
    }
}
