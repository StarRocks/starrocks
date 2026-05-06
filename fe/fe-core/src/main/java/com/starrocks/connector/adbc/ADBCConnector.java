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

package com.starrocks.connector.adbc;

import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.driver.jni.JniDriverFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ADBCConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(ADBCConnector.class);

    // Recognized non-adbc.* top-level keys
    static final Set<String> KNOWN_TOP_LEVEL_KEYS = Set.of(
            "type", "driver_url", "driver_name", "driver_entrypoint",
            "uri", "username", "password", "path", "_sr_identifier_quote");

    // Driver registry -- one AdbcDriver per resolved absolute driver_url
    private static final ConcurrentHashMap<String, AdbcDriver> DRIVER_REGISTRY = new ConcurrentHashMap<>();

    private final Map<String, String> properties;
    private final String catalogName;
    private BufferAllocator allocator;
    private ConnectorMetadata metadata;

    public ADBCConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = context.getProperties();

        // Validate properties
        validateProperties(properties);

        // Resolve file:// values in adbc.* properties — read file content once at
        // catalog creation so both FE (Java ADBC) and BE (C ADBC via thrift) get
        // the actual content, not a file path.
        resolveFileProperties(properties);

        // Try to load driver and create metadata eagerly. Mirror the JDBCConnector
        // pattern: if it fails, log and defer to first getMetadata() call. Two
        // reasons to defer rather than throw at construction:
        //   1. Edit log replay on a Follower whose filesystem layout differs from
        //      the Leader would otherwise wedge the FE on startup.
        //   2. Unit tests register a MockedADBCMetadata via MockedMetadataMgr
        //      after the catalog is created, so the eager dlopen on a fake
        //      driver_url path is wasted work.
        this.allocator = new RootAllocator();
        try {
            AdbcDriver driver = loadOrGetDriver(properties, allocator);
            AdbcDatabase db = openDatabase(driver, properties);
            String driverIdentifier = getDriverIdentifier(properties);
            probeDriverAndDiscoverQuoting(db, driverIdentifier, properties);
            this.metadata = new ADBCMetadata(properties, catalogName, allocator, db);
        } catch (Exception e) {
            this.metadata = null;
            try {
                allocator.close();
            } catch (Exception ignored) {
                // best-effort cleanup
            }
            this.allocator = null;
            LOG.error("Failed to create ADBC metadata on [catalog : {}]", catalogName, e);
        }
    }

    // Filesystem existence of driver_url is intentionally not checked here --
    // it would make catalog creation non-deterministic across FE replicas (edit log
    // replay on a Follower whose filesystem layout differs from the Leader would
    // fail). The native driver loader surfaces a dlopen error on the FE that
    // actually executes the catalog, classifyAdbcError wraps it into a user-facing message.
    static void validateProperties(Map<String, String> properties) {
        String driverUrl = properties.get("driver_url");
        String driverName = properties.get("driver_name");

        if (driverUrl != null && driverName != null) {
            throw new StarRocksConnectorException(
                    "ADBC catalog: 'driver_url' and 'driver_name' are mutually exclusive -- specify exactly one");
        }
        if (driverUrl == null && driverName == null) {
            throw new StarRocksConnectorException(
                    "ADBC catalog: one of 'driver_url' or 'driver_name' is required");
        }

        for (String key : properties.keySet()) {
            if (!key.startsWith("adbc.") && !KNOWN_TOP_LEVEL_KEYS.contains(key)) {
                throw new StarRocksConnectorException(
                        "ADBC catalog: unknown property '" + key
                                + "'. Use 'adbc.<key>' prefix for driver-specific options.");
            }
        }
    }

    /**
     * Load or retrieve a cached driver from the registry.
     * Uses ConcurrentHashMap.computeIfAbsent for thread-safe at-most-once loading.
     */
    private static AdbcDriver loadOrGetDriver(Map<String, String> properties, BufferAllocator allocator) {
        String driverIdentifier = getDriverIdentifier(properties);
        return DRIVER_REGISTRY.computeIfAbsent(driverIdentifier, path -> new JniDriverFactory().getDriver(allocator));
    }

    /**
     * Open an AdbcDatabase with the jni.driver params map.
     */
    private static AdbcDatabase openDatabase(AdbcDriver driver, Map<String, String> properties) throws AdbcException {
        String driverUrl = properties.get("driver_url");
        String driverName = properties.get("driver_name");
        String entrypoint = properties.get("driver_entrypoint");

        Map<String, Object> params = new HashMap<>();

        // jni.driver is REQUIRED for JniDriver
        params.put("jni.driver", driverUrl != null ? driverUrl : driverName);

        // Optional entrypoint
        if (entrypoint != null) {
            params.put("entrypoint", entrypoint);
        }

        // uri forwarded as-is
        String uri = properties.get("uri");
        if (uri != null) {
            params.put("uri", uri);
        }

        String username = properties.get("username");
        if (username != null) {
            params.put("username", username);
        }

        // password forwarded as-is
        String password = properties.get("password");
        if (password != null) {
            params.put("password", password);
        }

        // adbc.* pass-through — file:// values already resolved by resolveFileProperties()
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith("adbc.")) {
                params.put(entry.getKey(), entry.getValue());
            }
        }

        return driver.open(params);
    }

    /**
     * Probe driver: verify it responds and detect identifier quoting.
     *
     * <p>Detects the identifier quote character from the driver file path
     * (e.g. mysql → backtick, postgresql → double-quote) and stores it as
     * {@code _sr_identifier_quote} in the catalog properties. This internal
     * property is read by {@link com.starrocks.planner.ADBCScanNode} to
     * generate correctly-quoted SQL for each driver dialect.
     */
    private static void probeDriverAndDiscoverQuoting(
            AdbcDatabase db, String driverIdentifier, Map<String, String> properties) {
        try (AdbcConnection conn = db.connect()) {
            try (ArrowReader infoReader = conn.getInfo()) {
                while (infoReader.loadNextBatch()) {
                    // drain to release Arrow buffers
                }
            }
            LOG.info("ADBC driver '{}' loaded and responding", driverIdentifier);
        } catch (AdbcException e) {
            if (e.getStatus() == AdbcStatusCode.NOT_IMPLEMENTED) {
                LOG.warn("ADBC driver '{}': getInfo() not implemented -- skipping probe",
                        driverIdentifier);
            } else {
                throw new StarRocksConnectorException(classifyAdbcError(e, driverIdentifier), e);
            }
        } catch (Exception e) {
            LOG.warn("ADBC driver '{}': probe failed -- {}", driverIdentifier, e.getMessage());
        }

        // Detect identifier quote character
        if (!properties.containsKey("_sr_identifier_quote")) {
            String quoteChar = detectQuoteFromDriverPath(driverIdentifier);
            if (quoteChar == null && isFlightSqlDriver(driverIdentifier)) {
                // FlightSQL is a transport — the quote depends on the remote database.
                // Probe by trying a backtick-quoted query first (MySQL/StarRocks), then double-quote.
                quoteChar = probeQuoteChar(db);
            }
            if (quoteChar != null) {
                properties.put("_sr_identifier_quote", quoteChar);
                LOG.info("ADBC driver '{}': identifier quote = '{}'",
                        driverIdentifier, quoteChar);
            }
        }
    }

    /**
     * Probe the remote database's identifier quoting by running test queries.
     * Tries backtick first (MySQL/StarRocks), then double-quote (ANSI SQL).
     * Returns empty string if neither works (use unquoted identifiers).
     */
    private static String probeQuoteChar(AdbcDatabase db) {
        String[] candidates = {"`", "\""};
        for (String q : candidates) {
            try (AdbcConnection conn = db.connect();
                    AdbcStatement stmt = conn.createStatement()) {
                stmt.setSqlQuery("SELECT 1 AS " + q + "probe" + q);
                AdbcStatement.QueryResult qr = stmt.executeQuery();
                try (ArrowReader reader = qr.getReader()) {
                    reader.loadNextBatch();
                    LOG.info("Quote probe succeeded with '{}'", q);
                    return q;
                }
            } catch (Exception e) {
                LOG.debug("Quote probe with '{}' failed: {}", q, e.getMessage());
            }
        }
        LOG.info("All quote probes failed; using unquoted identifiers");
        return "";
    }

    private static boolean isFlightSqlDriver(String driverPath) {
        return driverPath != null && driverPath.toLowerCase().contains("flightsql");
    }

    /**
     * Resolve {@code file://} prefixed values in {@code adbc.*} properties.
     * Reads the file content and replaces the property value in-place so that
     * both FE (Java ADBC driver) and BE (C ADBC driver via thrift) receive
     * the actual content, not a file path.
     */
    private static void resolveFileProperties(Map<String, String> properties) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith("adbc.") && entry.getValue().startsWith("file://")) {
                String filePath = entry.getValue().substring(7);
                try {
                    entry.setValue(Files.readString(Path.of(filePath)));
                } catch (IOException e) {
                    throw new StarRocksConnectorException(
                            "ADBC catalog: cannot read file for property '" + entry.getKey()
                                    + "': " + filePath + ". Detail: " + e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Detect identifier quote character from the driver file path.
     * Returns null if the driver type cannot be determined (defaults to double-quote
     * in {@link com.starrocks.planner.ADBCScanNode}).
     */
    static String detectQuoteFromDriverPath(String driverPath) {
        if (driverPath == null) {
            return null;
        }
        String lower = driverPath.toLowerCase();
        if (lower.contains("mysql") || lower.contains("mariadb")) {
            return "`";
        }
        if (lower.contains("postgresql") || lower.contains("postgres")) {
            return "\"";
        }
        // SQLite, DuckDB: standard ANSI double-quote
        // FlightSQL is excluded — it's a transport, so quoting is probed at runtime
        if (lower.contains("sqlite") || lower.contains("duckdb")) {
            return "\"";
        }
        return null;
    }

    /**
     * Classify AdbcException into distinct error messages for the 5 failure classes.
     */
    static String classifyAdbcError(AdbcException e, String driverPath) {
        String msg = e.getMessage() != null ? e.getMessage() : "(no message)";
        switch (e.getStatus()) {
            case IO:
                return "ADBC catalog: failed to load driver at '" + driverPath
                        + "' -- file not found or not readable. Detail: " + msg;
            case INVALID_STATE:
                return "ADBC catalog: ABI mismatch loading driver '" + driverPath
                        + "' -- driver's AdbcDriverInit version is incompatible. Detail: " + msg;
            case INVALID_ARGUMENT:
                if (msg.toLowerCase().contains("missing") || msg.toLowerCase().contains("required")) {
                    return "ADBC catalog: driver '" + driverPath
                            + "' rejected a required option -- check uri / entrypoint. Detail: " + msg;
                }
                return "ADBC catalog: invalid argument to driver '" + driverPath + "'. Detail: " + msg;
            case NOT_FOUND:
                return "ADBC catalog: entrypoint symbol not found in driver '" + driverPath
                        + "'. Set 'driver_entrypoint' if the driver uses a non-default init symbol. Detail: " + msg;
            default:
                return "ADBC catalog: connection to driver '" + driverPath
                        + "' failed. Detail: " + msg;
        }
    }

    /**
     * Get the driver identifier (absolute path for driver_url, or name for driver_name).
     */
    private static String getDriverIdentifier(Map<String, String> properties) {
        String driverUrl = properties.get("driver_url");
        String driverName = properties.get("driver_name");
        return (driverUrl != null) ? new File(driverUrl).getAbsolutePath() : driverName;
    }

    @Override
    public synchronized ConnectorMetadata getMetadata() {
        if (metadata == null) {
            BufferAllocator newAllocator = new RootAllocator();
            try {
                AdbcDriver driver = loadOrGetDriver(properties, newAllocator);
                AdbcDatabase db = openDatabase(driver, properties);
                metadata = new ADBCMetadata(properties, catalogName, newAllocator, db);
                this.allocator = newAllocator;
            } catch (AdbcException e) {
                newAllocator.close();
                String driverIdentifier = getDriverIdentifier(properties);
                LOG.error("Failed to create adbc metadata on [catalog : {}]", catalogName, e);
                throw new StarRocksConnectorException(classifyAdbcError(e, driverIdentifier), e);
            }
        }
        return metadata;
    }

    @Override
    public void shutdown() {
        if (metadata != null) {
            ((ADBCMetadata) metadata).shutdown();
        }
        if (allocator != null) {
            allocator.close();
        }
        // Do NOT close DRIVER_REGISTRY entries -- drivers are never unloaded (Pitfall 3)
    }
}
