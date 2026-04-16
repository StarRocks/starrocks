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
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.driver.jni.JniDriverFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ADBCConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(ADBCConnector.class);

    // Recognized non-adbc.* top-level keys (PROP-04)
    static final Set<String> KNOWN_TOP_LEVEL_KEYS = Set.of(
            "type", "driver_url", "driver_name", "driver_entrypoint",
            "uri", "user", "password", "path");

    // Driver registry -- one AdbcDriver per resolved absolute driver_url (D-01, META-05)
    private static final ConcurrentHashMap<String, AdbcDriver> DRIVER_REGISTRY = new ConcurrentHashMap<>();

    private final Map<String, String> properties;
    private final String catalogName;
    private BufferAllocator allocator;
    private ConnectorMetadata metadata;

    public ADBCConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = context.getProperties();

        // Legacy v1 catalog detection (VAL-05): properties have adbc.driver but no driver_url/driver_name
        if (!properties.containsKey("driver_url") && !properties.containsKey("driver_name")) {
            if (properties.containsKey("adbc.driver") || properties.containsKey("adbc.url")) {
                LOG.warn("Legacy ADBC catalog '{}' detected -- DROP and recreate with driver_url/driver_name",
                        catalogName);
                this.metadata = null;
                return;
            }
        }

        // Validate properties (D-12 order)
        validateProperties(properties);

        // Eager driver loading (D-03, VAL-01)
        this.allocator = new RootAllocator();
        try {
            AdbcDriver driver = loadOrGetDriver(properties, allocator);
            AdbcDatabase db = openDatabase(driver, properties);
            String driverIdentifier = getDriverIdentifier(properties);
            probeDriverVersion(db, driverIdentifier);
            this.metadata = new ADBCMetadata(properties, catalogName, allocator, db);
        } catch (AdbcException e) {
            allocator.close();
            String driverIdentifier = getDriverIdentifier(properties);
            throw new StarRocksConnectorException(classifyAdbcError(e, driverIdentifier), e);
        } catch (Exception e) {
            allocator.close();
            if (e instanceof StarRocksConnectorException) {
                throw (StarRocksConnectorException) e;
            }
            throw new StarRocksConnectorException("ADBC catalog: unexpected error during creation: " + e.getMessage(), e);
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
     * Load or retrieve a cached driver from the registry (D-01, META-05).
     * Uses ConcurrentHashMap.computeIfAbsent for thread-safe at-most-once loading.
     */
    private static AdbcDriver loadOrGetDriver(Map<String, String> properties, BufferAllocator allocator) {
        String driverIdentifier = getDriverIdentifier(properties);
        return DRIVER_REGISTRY.computeIfAbsent(driverIdentifier, path -> new JniDriverFactory().getDriver(allocator));
    }

    /**
     * Open an AdbcDatabase with the jni.driver params map (META-02, PROP-05, PROP-06).
     */
    private static AdbcDatabase openDatabase(AdbcDriver driver, Map<String, String> properties) throws AdbcException {
        String driverUrl = properties.get("driver_url");
        String driverName = properties.get("driver_name");
        String entrypoint = properties.get("driver_entrypoint");

        Map<String, Object> params = new HashMap<>();

        // jni.driver is REQUIRED for JniDriver (META-02)
        params.put("jni.driver", driverUrl != null ? driverUrl : driverName);

        // Optional entrypoint (PROP-03)
        if (entrypoint != null) {
            params.put("entrypoint", entrypoint);
        }

        // uri forwarded as-is
        String uri = properties.get("uri");
        if (uri != null) {
            params.put("uri", uri);
        }

        // user -> username translation (PROP-06)
        String user = properties.get("user");
        if (user != null) {
            params.put("username", user);
        }

        // password forwarded as-is
        String password = properties.get("password");
        if (password != null) {
            params.put("password", password);
        }

        // adbc.* pass-through -- forwarded verbatim (PROP-05)
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith("adbc.")) {
                params.put(entry.getKey(), entry.getValue());
            }
        }

        return driver.open(params);
    }

    /**
     * Probe driver version via getInfo to verify the driver responds (VAL-02).
     * If getInfo throws NOT_IMPLEMENTED, log a warning but do not fail.
     */
    private static void probeDriverVersion(AdbcDatabase db, String driverIdentifier) {
        try (AdbcConnection conn = db.connect();
                ArrowReader infoReader = conn.getInfo()) {
            // Consume and close the ArrowReader to release Arrow buffers.
            // Without this, the RootAllocator leaks 64 bytes per catalog creation.
            while (infoReader.loadNextBatch()) {
                // drain
            }
            LOG.info("ADBC driver '{}' loaded and responding", driverIdentifier);
        } catch (AdbcException e) {
            if (e.getStatus() == AdbcStatusCode.NOT_IMPLEMENTED) {
                LOG.warn("ADBC driver '{}': getInfo() not implemented -- skipping version probe", driverIdentifier);
            } else {
                throw new StarRocksConnectorException(classifyAdbcError(e, driverIdentifier), e);
            }
        } catch (Exception e) {
            LOG.warn("ADBC driver '{}': version probe failed -- {}", driverIdentifier, e.getMessage());
        }
    }

    /**
     * Classify AdbcException into distinct error messages for the 5 failure classes (VAL-03).
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
    public ConnectorMetadata getMetadata() {
        if (metadata == null && !properties.containsKey("driver_url") && !properties.containsKey("driver_name")) {
            throw new StarRocksConnectorException("ADBC catalog '" + catalogName
                    + "' uses legacy property schema -- DROP and recreate with driver_url / driver_name");
        }
        if (metadata == null) {
            try {
                this.allocator = new RootAllocator();
                AdbcDriver driver = loadOrGetDriver(properties, allocator);
                AdbcDatabase db = openDatabase(driver, properties);
                metadata = new ADBCMetadata(properties, catalogName, allocator, db);
            } catch (AdbcException e) {
                if (allocator != null) {
                    allocator.close();
                    allocator = null;
                }
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
