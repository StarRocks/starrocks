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
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.driver.jni.JniDriverFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ADBCConnectorTest {
    private String originalJniPath;
    private String originalJniSystemPath;

    @Mocked
    private AdbcDriver driver;

    @Mocked
    private AdbcDatabase database;

    @TempDir
    private Path tempDir;

    @BeforeEach
    public void saveJniPath() {
        originalJniPath = Config.adbc_jni_library_path;
        originalJniSystemPath = System.getProperty("arrow.adbc.driver.jni.library.path");
    }

    @AfterEach
    public void restoreJniPath() {
        Config.adbc_jni_library_path = originalJniPath;
        if (originalJniSystemPath == null) {
            System.clearProperty("arrow.adbc.driver.jni.library.path");
        } else {
            System.setProperty("arrow.adbc.driver.jni.library.path", originalJniSystemPath);
        }
    }

    private static Map<String, String> validProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "adbc");
        properties.put("driver", "adbc_driver_flightsql");
        properties.put("uri", "grpc+tcp://localhost:32010");
        return properties;
    }

    @Test
    public void testReplayRetryForwardsDriverOptionsAndClosesResources() throws Exception {
        Files.createFile(tempDir.resolve(System.mapLibraryName("adbc_driver_jni")));
        Config.adbc_jni_library_path = tempDir.toString();
        List<BufferAllocator> allocators = new ArrayList<>();
        new MockUp<JniDriverFactory>() {
            @Mock
            public AdbcDriver getDriver(BufferAllocator allocator) {
                allocators.add(allocator);
                if (allocators.size() == 1) {
                    throw new UnsatisfiedLinkError("driver unavailable during replay");
                }
                return driver;
            }
        };
        Map<String, String> properties = validProperties();
        properties.put("username", "reader");
        properties.put("password", "test-password");
        properties.put("adbc.flight.sql.rpc.timeout_seconds", "30");
        Map<String, Object> expectedParameters = new HashMap<>(properties);
        expectedParameters.remove("type");
        expectedParameters.remove("driver");
        expectedParameters.put("jni.driver", "adbc_driver_flightsql");
        new Expectations() {{
                driver.open(expectedParameters);
                result = database;
            }};

        ADBCConnector connector = new ADBCConnector(new ConnectorContext("adbc0", "adbc", properties));
        try {
            assertThrows(IllegalStateException.class, () -> allocators.get(0).buffer(1));
            ConnectorMetadata metadata = connector.getMetadata();
            assertInstanceOf(ADBCMetadata.class, metadata);
            assertSame(metadata, connector.getMetadata());
        } finally {
            connector.shutdown();
        }
        connector.shutdown();
        assertThrows(IllegalStateException.class, () -> allocators.get(1).buffer(1));
        new Verifications() {{
                driver.open(expectedParameters);
                times = 1;
                database.close();
                times = 1;
            }};
    }

    @Test
    public void testUnknownPropertyIsRejected() {
        Map<String, String> properties = validProperties();
        properties.put("timeout_seconds", "30");
        assertThrows(StarRocksConnectorException.class, () -> ADBCConnector.validateProperties(properties));
    }

    @Test
    public void testUnsetJniPathIsRejected() {
        Config.adbc_jni_library_path = null;
        assertThrows(StarRocksConnectorException.class, ADBCConnector::resolveJniLibrary);
        Config.adbc_jni_library_path = " ";
        assertThrows(StarRocksConnectorException.class, ADBCConnector::resolveJniLibrary);
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
