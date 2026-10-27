// Arrow Flight SQL throughput benchmark: single-endpoint funnel vs multi-endpoint fan-out.
// Auths with a JWT bearer token (env SR_JWT) sent as an Authorization header on every call
// (FE getFlightInfo + each CN getStream). Reads the full result both ways and reports
// rows, MB, wall-clock, rows/s, MB/s.
//   SR_JWT=<jwt> java --add-opens=java.base/java.nio=ALL-UNNAMED \
//        -cp "/opt/starrocks/fe/lib/*" AfBench.java <feHost> <fePort> <db.table>
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

        String qOff = "SELECT /*+ SET_VAR(enable_arrow_flight_multi_endpoint=false) */ * FROM " + table;
        String qOn  = "SELECT /*+ SET_VAR(enable_arrow_flight_multi_endpoint=true)  */ * FROM " + table;

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

            FlightInfo off = sql.execute(qOff, feOpts);
            bench("single-endpoint", off);
            FlightInfo on = sql.execute(qOn, feOpts);
            bench("multi-endpoint ", on);
        } finally {
            sql.close();
        }
    }

    static void bench(String label, FlightInfo info) throws Exception {
        List<FlightEndpoint> eps = info.getEndpoints();
        System.out.printf("%s  endpoints=%d%n", label, eps.size());
        for (int i = 0; i < eps.size(); i++) {
            FlightEndpoint ep = eps.get(i);
            String loc = ep.getLocations().isEmpty() ? "(none->FE)" : ep.getLocations().get(0).getUri().toString();
            System.out.printf("    ep[%d] loc=%s ticketBytes=%d%n", i, loc, ep.getTicket().getBytes().length);
        }
        long t0 = System.nanoTime();
        ExecutorService pool = Executors.newFixedThreadPool(Math.max(eps.size(), 1));
        java.util.List<Future<long[]>> futs = new java.util.ArrayList<>();
        for (FlightEndpoint ep : eps) {
            futs.add(pool.submit((Callable<long[]>) () -> readEndpoint(ep)));
        }
        // Read every endpoint independently; record per-endpoint outcome instead of
        // aborting the whole pass on the first failing fetch.
        long rows = 0, bytes = 0, ok = 0, failed = 0;
        for (int i = 0; i < futs.size(); i++) {
            try {
                long[] rb = futs.get(i).get();
                rows += rb[0];
                bytes += rb[1];
                ok++;
                System.out.printf("    ep[%d] OK rows=%,d bytes=%,d%n", i, rb[0], rb[1]);
            } catch (Exception e) {
                failed++;
                Throwable c = e.getCause() != null ? e.getCause() : e;
                System.out.printf("    ep[%d] FAIL %s%n", i, c.getMessage());
            }
        }
        pool.shutdown();
        double sec = (System.nanoTime() - t0) / 1e9;
        double mb = bytes / 1048576.0;
        System.out.printf("%s  endpoints=%d ok=%d failed=%d  rows=%,d  %.1f MB  %.2fs  |  %,.0f rows/s  %.1f MB/s%n",
                label, eps.size(), ok, failed, rows, mb, sec, rows / sec, mb / sec);
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
