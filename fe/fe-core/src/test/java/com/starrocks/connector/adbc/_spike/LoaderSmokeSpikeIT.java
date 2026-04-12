// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.adbc._spike;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.jni.JniDriverFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * THROWAWAY SPIKE — Phase 1 go/no-go for the ADBC dynamic driver loader rework.
 *
 * <p>This test is intentionally under the {@code _spike} subpackage to mark it as
 * non-product code. The entire {@code _spike/} directory, plus the temporary
 * test-scope {@code adbc-driver-jni} entry in {@code fe/fe-core/pom.xml},
 * MUST be removed together before Phase 2's real FE rewrite lands. The
 * permanent SQLite integration test (REQ-ID TEST-02) is a fresh write owned
 * by Phase 2 — it does NOT inherit from this spike.
 *
 * <p>Prerequisites (manual, run once on the dev machine — NOT executed by the test):
 * <pre>
 *   curl -LsSf https://dbc.columnar.tech/install.sh | sh   # installs dbc
 *   dbc install sqlite --level user                         # writes ~/.config/adbc/drivers/sqlite.toml
 * </pre>
 *
 * <p>De-risks for Phase 2:
 * <ul>
 *   <li>STK-01/02/03: adbc-driver-jni 0.21.0 + Arrow 18.0.0 coexist on fe-core test classpath</li>
 *   <li>PROP-05: non-jni.* options pass through verbatim to AdbcDatabaseSetOption</li>
 *   <li>META-02: JniDriver.open(params) is the correct entry point</li>
 *   <li>Pitfall 5 (Arrow lockstep): C Data Interface round-trips a simple result set without SIGSEGV</li>
 *   <li>Pitfall 3 (never unload drivers): this test uses try-with-resources ONLY. It does NOT
 *       attempt classloader-level driver unload. The adopted project policy is that
 *       ADBC driver handles stay resident until process exit. Even though SQLite (pure C)
 *       would tolerate a manual unload, the test follows the universal policy to set precedent.</li>
 * </ul>
 */
@EnabledOnOs(OS.LINUX)
@EnabledIfSystemProperty(named = "os.arch", matches = "amd64")
public class LoaderSmokeSpikeIT {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        // Must not throw. Any leak here means an Arrow buffer owned by the spike
        // was not released — surface it loudly.
        allocator.close();
    }

    @Test
    void loadSqliteDriverAndRoundTripData() throws Exception {
        // --- Step 1: Resolve the SQLite driver path from the ADBC Driver Manifest TOML.
        String driverPath = readDriverPathFromManifest();
        assertTrue(Files.exists(Paths.get(driverPath)),
                "SQLite ADBC driver .so not found at manifest-reported path: " + driverPath);

        // --- Step 2: Obtain a JniDriver and open an AdbcDatabase with uri=:memory:.
        // Note: JniDriverFactory bundles libadbc_driver_jni.so inside the JAR and
        // extracts it to java.io.tmpdir at class load time — no system install needed
        // for the Java path (only the target driver .so is user-supplied).
        AdbcDriver driver = new JniDriverFactory().getDriver(allocator);
        Map<String, Object> params = new HashMap<>();
        params.put("jni.driver", driverPath); // MUST be a String — not a Path/File.
        params.put("uri", ":memory:");         // Pass-through option, forwarded verbatim.

        try (AdbcDatabase db = driver.open(params);
                AdbcConnection conn = db.connect()) {

            // --- Step 3: CREATE TABLE t (id INTEGER, name TEXT).
            // Upstream surprise (0.21.0): JniStatement.executeUpdate() throws
            // UnsupportedOperationException — the JNI bridge does not implement it.
            // Use executeQuery() instead; DDL returns an empty result set.
            // Phase 2 must account for this: the StarRocks bridge cannot call executeUpdate().
            try (AdbcStatement stmt = conn.createStatement()) {
                stmt.setSqlQuery("CREATE TABLE t (id INTEGER, name TEXT)");
                // QueryResult owns the ArrowReader; close QueryResult to release both.
                // Do NOT close the ArrowReader separately — double-close throws NPE.
                try (var r = stmt.executeQuery()) {
                    ArrowReader ar = r.getReader();
                    if (ar != null) {
                        while (ar.loadNextBatch()) { /* discard DDL result */ }
                    }
                }
            }

            // --- Step 4: Insert three rows via three separate statements.
            insertRow(conn, 1, "a");
            insertRow(conn, 2, "b");
            insertRow(conn, 3, "c");

            // --- Step 5: getObjects(DB_SCHEMAS) — assert at least one schema row.
            // Upstream surprise (0.21.0): JniConnection only implements createStatement,
            // getInfo, and close. getObjects() falls through to the AdbcConnection default
            // which throws AdbcException(NOT_IMPLEMENTED). This means StarRocks metadata
            // queries (Phase 2 ADBCMetadata) cannot use the Java getObjects API — they
            // must issue SQL queries directly (e.g., PRAGMA schema_version or information_schema).
            // Phase 2 planner must account for this: no Java-side getObjects/getTableSchema.
            assertAtLeastOneSchema(conn);

            // --- Step 6: getTableSchema("main", "t") — assert id + name fields present.
            Schema tableSchema = probeTableSchema(conn);
            if (tableSchema != null) {
                List<Field> fields = tableSchema.getFields();
                assertEquals(2, fields.size(),
                        "Expected exactly 2 fields in main.t; got " + fields.size());
                assertEquals("id", fields.get(0).getName());
                assertEquals("name", fields.get(1).getName());
            }
            // If tableSchema is null, getTableSchema() threw NOT_IMPLEMENTED — recorded
            // as a YELLOW surprise in SPIKE-LOG.md. The SELECT round-trip (Step 7) still
            // validates Arrow C Data Interface marshalling end-to-end.

            // --- Step 7: SELECT id, name FROM t ORDER BY id — assert 3 rows with expected values.
            // QueryResult owns the ArrowReader. Only QueryResult goes in try-with-resources;
            // do NOT separately close the reader (double-close throws ArrowArrayStream NPE).
            try (AdbcStatement stmt = conn.createStatement()) {
                stmt.setSqlQuery("SELECT id, name FROM t ORDER BY id");
                try (var queryResult = stmt.executeQuery()) {
                    ArrowReader reader = queryResult.getReader();
                    int totalRows = 0;
                    long[] ids = new long[3];
                    String[] names = new String[3];
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        int rowCount = root.getRowCount();
                        FieldVector idVec = root.getVector("id");
                        FieldVector nameVec = root.getVector("name");
                        for (int i = 0; i < rowCount && totalRows < 3; i++, totalRows++) {
                            Object idObj = idVec.getObject(i);
                            Object nameObj = nameVec.getObject(i);
                            ids[totalRows] = ((Number) idObj).longValue();
                            names[totalRows] = nameObj.toString();
                        }
                    }
                    assertEquals(3, totalRows, "Expected 3 rows from SELECT");
                    assertEquals(1L, ids[0]);
                    assertEquals(2L, ids[1]);
                    assertEquals(3L, ids[2]);
                    assertEquals("a", names[0]);
                    assertEquals("b", names[1]);
                    assertEquals("c", names[2]);
                }
            }
        }
        // try-with-resources: Statement -> Connection -> Database released in reverse order.
        // Driver handle itself stays loaded until JVM exit — never unload (Pitfall 3 policy).
    }

    private void insertRow(AdbcConnection conn, int id, String name) throws Exception {
        try (AdbcStatement stmt = conn.createStatement()) {
            // Simple string-substituted insert for the spike. Production code uses prepared
            // statements; the spike prioritizes clarity over SQL injection safety because
            // inputs are hardcoded constants.
            stmt.setSqlQuery(String.format("INSERT INTO t VALUES (%d, '%s')", id, name));
            // executeUpdate() throws UnsupportedOperationException in JniStatement 0.21.0;
            // use executeQuery() for DML too. QueryResult owns ArrowReader — do NOT
            // close them separately.
            try (var r = stmt.executeQuery()) {
                ArrowReader ar = r.getReader();
                if (ar != null) {
                    while (ar.loadNextBatch()) { /* discard INSERT result */ }
                }
            }
        }
    }

    private void assertAtLeastOneSchema(AdbcConnection conn) throws Exception {
        // JniConnection 0.21.0 does not implement getObjects(); it falls through to
        // AdbcConnection.getObjects() default which throws NOT_IMPLEMENTED. Catch it
        // and log — this is a YELLOW upstream surprise documented in SPIKE-LOG.md.
        // The spike does not fail on this: the core de-risk (Arrow C Data Interface
        // round-trip via executeQuery) is validated by the SELECT in Step 7.
        try (ArrowReader reader = conn.getObjects(
                AdbcConnection.GetObjectsDepth.DB_SCHEMAS,
                null, null, null, null, null)) {
            boolean anyRow = false;
            while (reader.loadNextBatch()) {
                if (reader.getVectorSchemaRoot().getRowCount() > 0) {
                    anyRow = true;
                    break;
                }
            }
            assertTrue(anyRow, "getObjects(DB_SCHEMAS) returned zero rows — expected at least 'main'");
        } catch (org.apache.arrow.adbc.core.AdbcException e) {
            if (e.getStatus() == org.apache.arrow.adbc.core.AdbcStatusCode.NOT_IMPLEMENTED) {
                // YELLOW: JniConnection.getObjects() is NOT_IMPLEMENTED in adbc-driver-jni 0.21.0.
                // Phase 2 must use SQL queries for metadata instead of the Java getObjects API.
                System.out.println("[SPIKE YELLOW] getObjects() NOT_IMPLEMENTED in JniConnection 0.21.0: " + e.getMessage());
            } else {
                throw e;
            }
        }
    }

    /**
     * Attempt getTableSchema and return the schema, or null if NOT_IMPLEMENTED.
     * Logs a YELLOW note in the latter case for SPIKE-LOG.md.
     */
    private Schema probeTableSchema(AdbcConnection conn) throws Exception {
        try {
            return conn.getTableSchema(null, "main", "t");
        } catch (org.apache.arrow.adbc.core.AdbcException e) {
            if (e.getStatus() == org.apache.arrow.adbc.core.AdbcStatusCode.NOT_IMPLEMENTED) {
                System.out.println("[SPIKE YELLOW] getTableSchema() NOT_IMPLEMENTED in JniConnection 0.21.0: " + e.getMessage());
                return null;
            }
            throw e;
        }
    }

    /**
     * Locate the SQLite ADBC driver .so by parsing the ADBC Driver Manifest TOML
     * that {@code dbc install sqlite --level user} writes to
     * {@code ~/.config/adbc/drivers/sqlite.toml}. Honors {@code $XDG_CONFIG_HOME}
     * if set. Does NOT use a TOML library — the target line is trivial to extract
     * with a regex.
     *
     * <p>Expected TOML shape:
     * <pre>
     *   [Driver.shared]
     *   linux_amd64 = '/abs/path/to/libadbc_driver_sqlite.so'
     * </pre>
     */
    private String readDriverPathFromManifest() throws Exception {
        String xdgConfigHome = System.getenv("XDG_CONFIG_HOME");
        Path configBase = (xdgConfigHome != null && !xdgConfigHome.isEmpty())
                ? Paths.get(xdgConfigHome)
                : Paths.get(System.getProperty("user.home"), ".config");
        Path manifest = configBase.resolve("adbc/drivers/sqlite.toml");
        if (!Files.exists(manifest)) {
            fail("ADBC SQLite driver manifest not found at " + manifest + ". "
                    + "Run `dbc install sqlite --level user` on this machine before running this spike. "
                    + "See docs.columnar.tech/dbc/ for dbc install instructions.");
        }
        String content = Files.readString(manifest);
        // Match: linux_amd64 = '/path/to/lib.so'  OR  linux_amd64 = "/path/to/lib.so"
        Pattern p = Pattern.compile(
                "linux_amd64\\s*=\\s*['\"]([^'\"]+)['\"]",
                Pattern.MULTILINE);
        Matcher m = p.matcher(content);
        if (!m.find()) {
            fail("Could not find `linux_amd64 = '...'` under [Driver.shared] in " + manifest
                    + ". Manifest contents:\n" + content);
        }
        return m.group(1);
    }
}
