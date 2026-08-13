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

import com.starrocks.common.Config;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.driver.jni.JniDriverFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Generic ADBC catalog connector.
 *
 * <p>The {@code driver} property is a logical ADBC driver name. The JNI driver
 * manager resolves it with its upstream default discovery rules; StarRocks does
 * not accept or distribute the external driver library.
 */
public class ADBCConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(ADBCConnector.class);
    private static final Set<String> KNOWN_TOP_LEVEL_KEYS =
            Set.of("type", "driver", "uri", "username", "password");

    private final Map<String, String> properties;
    private final String catalogName;
    private BufferAllocator allocator;
    private ConnectorMetadata metadata;
    private Exception initializationFailure;

    public ADBCConnector(ConnectorContext context) {
        catalogName = context.getCatalogName();
        properties = context.getProperties();
        validateProperties(properties);
        initializeMetadata();
    }

    static void validateProperties(Map<String, String> properties) {
        String driver = properties.get("driver");
        if (driver == null || driver.isBlank()) {
            throw new StarRocksConnectorException("ADBC catalog: 'driver' is required");
        }
        if (driver.contains("/") || driver.contains("\\")) {
            throw new StarRocksConnectorException(
                    "ADBC catalog: 'driver' must be a logical driver name, not a filesystem path");
        }
        if (properties.getOrDefault("uri", "").isBlank()) {
            throw new StarRocksConnectorException("ADBC catalog: 'uri' is required");
        }
        for (String key : properties.keySet()) {
            if (!key.startsWith("adbc.") && !KNOWN_TOP_LEVEL_KEYS.contains(key)) {
                throw new StarRocksConnectorException(
                        "ADBC catalog: unknown property '" + key
                                + "'. Use the 'adbc.' prefix for driver-specific options");
            }
        }
    }

    private static AdbcDatabase openDatabase(BufferAllocator allocator, Map<String, String> properties)
            throws AdbcException {
        Path jniLibrary = resolveJniLibrary();
        System.setProperty("arrow.adbc.driver.jni.library.path", jniLibrary.getParent().toString());
        AdbcDriver driver = new JniDriverFactory().getDriver(allocator);
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("jni.driver", properties.get("driver"));
        parameters.put("uri", properties.get("uri"));
        if (properties.containsKey("username")) {
            parameters.put("username", properties.get("username"));
        }
        if (properties.containsKey("password")) {
            parameters.put("password", properties.get("password"));
        }
        properties.forEach((key, value) -> {
            if (key.startsWith("adbc.")) {
                parameters.put(key, value);
            }
        });
        return driver.open(parameters);
    }

    static Path resolveJniLibrary() {
        if (Config.adbc_jni_library_path == null || Config.adbc_jni_library_path.isBlank()) {
            throw new StarRocksConnectorException(
                    "ADBC catalog: 'adbc_jni_library_path' must point to the StarRocks-built JNI bridge");
        }
        Path jniLibrary = Path.of(Config.adbc_jni_library_path,
                System.mapLibraryName("adbc_driver_jni"));
        if (!Files.isRegularFile(jniLibrary)) {
            throw new StarRocksConnectorException(
                    "ADBC catalog: StarRocks-built JNI bridge not found: " + jniLibrary);
        }
        return jniLibrary.toAbsolutePath();
    }

    private void initializeMetadata() {
        BufferAllocator newAllocator = new RootAllocator();
        try {
            AdbcDatabase database = openDatabase(newAllocator, properties);
            metadata = new ADBCMetadata(properties, catalogName, database);
            allocator = newAllocator;
            initializationFailure = null;
        } catch (Exception e) {
            newAllocator.close();
            metadata = null;
            allocator = null;
            initializationFailure = e;
            LOG.warn("Failed to initialize ADBC catalog '{}'; retrying on first metadata access",
                    catalogName, e);
        }
    }

    @Override
    public synchronized ConnectorMetadata getMetadata() {
        if (metadata == null) {
            initializeMetadata();
        }
        if (metadata == null) {
            throw new StarRocksConnectorException(
                    "ADBC catalog '" + catalogName + "': failed to load driver '"
                            + properties.get("driver") + "' or connect to '" + properties.get("uri") + "'",
                    initializationFailure);
        }
        return metadata;
    }

    @Override
    public synchronized void shutdown() {
        if (metadata != null) {
            ((ADBCMetadata) metadata).shutdown();
            metadata = null;
        }
        if (allocator != null) {
            allocator.close();
            allocator = null;
        }
    }
}
