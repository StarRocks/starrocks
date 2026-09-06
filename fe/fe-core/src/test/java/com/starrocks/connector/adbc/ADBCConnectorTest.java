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
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.exception.StarRocksConnectorException;
import mockit.Mock;
import mockit.MockUp;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.driver.jni.JniDriverFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ADBCConnectorTest {
    private String originalJniPath;

    @TempDir
    private Path tempDir;

    @BeforeEach
    public void saveJniPath() {
        originalJniPath = Config.adbc_jni_library_path;
    }

    @AfterEach
    public void restoreJniPath() {
        Config.adbc_jni_library_path = originalJniPath;
    }

    private static Map<String, String> validProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "adbc");
        properties.put("driver", "adbc_driver_flightsql");
        properties.put("uri", "grpc+tcp://localhost:32010");
        return properties;
    }

    @Test
    public void testNativeLinkageFailureDoesNotAbortCatalogReplay() throws Exception {
        Files.createFile(tempDir.resolve(System.mapLibraryName("adbc_driver_jni")));
        Config.adbc_jni_library_path = tempDir.toString();
        new MockUp<JniDriverFactory>() {
            @Mock
            public AdbcDriver getDriver(BufferAllocator allocator) {
                throw new UnsatisfiedLinkError("test native dependency unavailable");
            }
        };
        ADBCConnector connector = assertDoesNotThrow(() -> new ADBCConnector(
                new ConnectorContext("adbc0", "adbc", validProperties())));
        try {
            StarRocksConnectorException error = assertThrows(StarRocksConnectorException.class, connector::getMetadata);
            assertInstanceOf(UnsatisfiedLinkError.class, error.getCause());
        } finally {
            connector.shutdown();
        }
    }

    @Test
    public void testLogicalDriverNameAccepted() {
        assertDoesNotThrow(() -> ADBCConnector.validateProperties(validProperties()));
    }

    @Test
    public void testDriverRequired() {
        Map<String, String> properties = validProperties();
        properties.remove("driver");
        assertThrows(StarRocksConnectorException.class,
                () -> ADBCConnector.validateProperties(properties));
    }

    @Test
    public void testDriverPathRejected() {
        Map<String, String> properties = validProperties();
        properties.put("driver", "/opt/adbc/libadbc_driver_flightsql.so");
        assertThrows(StarRocksConnectorException.class,
                () -> ADBCConnector.validateProperties(properties));
    }

    @Test
    public void testUriRequired() {
        Map<String, String> properties = validProperties();
        properties.remove("uri");
        assertThrows(StarRocksConnectorException.class,
                () -> ADBCConnector.validateProperties(properties));
    }

    @Test
    public void testDriverOptionsAccepted() {
        Map<String, String> properties = validProperties();
        properties.put("adbc.flight.sql.rpc.timeout_seconds", "30");
        assertDoesNotThrow(() -> ADBCConnector.validateProperties(properties));
    }

    @Test
    public void testStarRocksBuiltJniLibraryRequired() {
        Config.adbc_jni_library_path = tempDir.toString();
        assertThrows(StarRocksConnectorException.class, ADBCConnector::resolveJniLibrary);
    }

    @Test
    public void testStarRocksBuiltJniLibraryAccepted() throws Exception {
        Path library = tempDir.resolve(System.mapLibraryName("adbc_driver_jni"));
        Files.createFile(library);
        Config.adbc_jni_library_path = tempDir.toString();

        assertDoesNotThrow(ADBCConnector::resolveJniLibrary);
    }
}
