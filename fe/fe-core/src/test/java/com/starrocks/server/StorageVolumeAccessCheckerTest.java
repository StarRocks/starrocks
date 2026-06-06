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

package com.starrocks.server;

import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.fs.HdfsUtil;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class StorageVolumeAccessCheckerTest {

    @Test
    public void testCheckSuccess() {
        String svName = "test_sv";
        String svType = "S3";
        List<String> locations = Arrays.asList("s3://bucket/path1", "s3://bucket/path2");
        Map<String, String> params = new HashMap<>();
        params.put(CloudConfigurationConstants.AWS_S3_REGION, "us-east-1");
        params.put(CloudConfigurationConstants.AWS_S3_ENDPOINT, "https://s3.us-east-1.amazonaws.com");

        new MockUp<HdfsUtil>() {
            @Mock
            public void writeFile(byte[] data, String destFilePath, Map<String, String> properties)
                    throws StarRocksException {
                // success: no exception
            }

            @Mock
            public void deletePath(String path, Map<String, String> properties) throws StarRocksException {
                // success: no exception
            }
        };

        // Should not throw any exception
        Assertions.assertDoesNotThrow(() ->
                StorageVolumeAccessChecker.check(svName, svType, locations, params));
    }

    @Test
    public void testCheckFailureWithSingleLocation() {
        String svName = "test_sv";
        String svType = "S3";
        List<String> locations = Collections.singletonList("s3://bucket/path");
        Map<String, String> params = new HashMap<>();
        params.put(CloudConfigurationConstants.AWS_S3_REGION, "us-east-1");

        new MockUp<HdfsUtil>() {
            @Mock
            public void writeFile(byte[] data, String destFilePath, Map<String, String> properties)
                    throws StarRocksException {
                throw new StarRocksException("Access denied");
            }
        };

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> StorageVolumeAccessChecker.check(svName, svType, locations, params));

        Assertions.assertTrue(ex.getMessage().contains("Storage volume accessibility check failed"));
        Assertions.assertTrue(ex.getMessage().contains(svName));
        Assertions.assertTrue(ex.getMessage().contains(svType));
        Assertions.assertTrue(ex.getMessage().contains("s3://bucket/path"));
        Assertions.assertTrue(ex.getMessage().contains("Access denied"));
    }

    @Test
    public void testCheckFailureWithNestedException() {
        String svName = "test_sv";
        String svType = "HDFS";
        List<String> locations = Collections.singletonList("hdfs://namenode:8020/path");
        Map<String, String> params = new HashMap<>();

        new MockUp<HdfsUtil>() {
            @Mock
            public void writeFile(byte[] data, String destFilePath, Map<String, String> properties)
                    throws StarRocksException {
                // Create nested exception chain
                Exception rootCause = new RuntimeException("Connection refused");
                Exception middle = new RuntimeException("Network error", rootCause);
                throw new StarRocksException("Failed to write temp check file", middle);
            }
        };

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> StorageVolumeAccessChecker.check(svName, svType, locations, params));

        // Should extract the root cause message
        Assertions.assertTrue(ex.getMessage().contains("Connection refused"));
    }

    @Test
    public void testCheckFailureWithNullExceptionMessage() {
        String svName = "test_sv";
        String svType = "AZBLOB";
        List<String> locations = Collections.singletonList("azblob://container/path");
        Map<String, String> params = new HashMap<>();

        new MockUp<HdfsUtil>() {
            @Mock
            public void writeFile(byte[] data, String destFilePath, Map<String, String> properties)
                    throws StarRocksException {
                // Exception with null message (Strings.nullToEmpty converts to empty string)
                throw new StarRocksException((String) null);
            }
        };

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> StorageVolumeAccessChecker.check(svName, svType, locations, params));

        // When message is null/empty, should fall back to cause.toString()
        Assertions.assertTrue(ex.getMessage().contains("Storage volume accessibility check failed"));
        Assertions.assertTrue(ex.getMessage().contains(svName));
        Assertions.assertTrue(ex.getMessage().contains(svType));
        Assertions.assertTrue(ex.getMessage().contains("StarRocksException"));
    }

    @Test
    public void testCheckWithMultipleLocationsFailsOnFirst() {
        String svName = "test_sv";
        String svType = "S3";
        List<String> locations = Arrays.asList("s3://bucket/path1", "s3://bucket/path2");
        Map<String, String> params = new HashMap<>();

        AtomicInteger writeCount = new AtomicInteger(0);

        new MockUp<HdfsUtil>() {
            @Mock
            public void writeFile(byte[] data, String destFilePath, Map<String, String> properties)
                    throws StarRocksException {
                writeCount.incrementAndGet();
                if (destFilePath.startsWith("s3://bucket/path1/")) {
                    throw new StarRocksException("First path not accessible");
                }
            }
        };

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> StorageVolumeAccessChecker.check(svName, svType, locations, params));

        Assertions.assertTrue(ex.getMessage().contains("First path not accessible"));
        Assertions.assertEquals(1, writeCount.get()); // Should stop at first failure
    }

    @Test
    public void testCheckWithMultipleLocationsFailsOnSecond() {
        String svName = "test_sv";
        String svType = "S3";
        List<String> locations = Arrays.asList("s3://bucket/path1", "s3://bucket/path2");
        Map<String, String> params = new HashMap<>();

        AtomicInteger writeCount = new AtomicInteger(0);

        new MockUp<HdfsUtil>() {
            @Mock
            public void writeFile(byte[] data, String destFilePath, Map<String, String> properties)
                    throws StarRocksException {
                writeCount.incrementAndGet();
                if (destFilePath.startsWith("s3://bucket/path2/")) {
                    throw new StarRocksException("Second path not accessible");
                }
            }

            @Mock
            public void deletePath(String path, Map<String, String> properties) throws StarRocksException {
                // success
            }
        };

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> StorageVolumeAccessChecker.check(svName, svType, locations, params));

        Assertions.assertTrue(ex.getMessage().contains("Second path not accessible"));
        Assertions.assertEquals(2, writeCount.get()); // First passed, second failed
    }

    @Test
    public void testCheckWithEmptyLocations() {
        String svName = "test_sv";
        String svType = "S3";
        List<String> locations = Collections.emptyList();
        Map<String, String> params = new HashMap<>();

        // Should not throw any exception for empty locations
        Assertions.assertDoesNotThrow(() ->
                StorageVolumeAccessChecker.check(svName, svType, locations, params));
    }

    @Test
    public void testCheckParamsAreCopied() {
        String svName = "test_sv";
        String svType = "S3";
        List<String> locations = Collections.singletonList("s3://bucket/path");
        Map<String, String> params = new HashMap<>();
        params.put("key", "value");

        new MockUp<HdfsUtil>() {
            @Mock
            public void writeFile(byte[] data, String destFilePath, Map<String, String> properties)
                    throws StarRocksException {
                // Try to modify the passed properties
                properties.put("modified", "true");
            }

            @Mock
            public void deletePath(String path, Map<String, String> properties) throws StarRocksException {
                // success
            }
        };

        // Should not throw, and original params should not be modified
        Assertions.assertDoesNotThrow(() ->
                StorageVolumeAccessChecker.check(svName, svType, locations, params));

        Assertions.assertFalse(params.containsKey("modified"));
    }

    @Test
    public void testCheckErrorFormatContainsAllInfo() {
        String svName = "my_storage_volume";
        String svType = "HDFS";
        String location = "hdfs://mycluster/data";
        List<String> locations = Collections.singletonList(location);
        Map<String, String> params = new HashMap<>();
        String errorMsg = "Permission denied: user=admin, access=WRITE";

        new MockUp<HdfsUtil>() {
            @Mock
            public void writeFile(byte[] data, String destFilePath, Map<String, String> properties)
                    throws StarRocksException {
                throw new StarRocksException(errorMsg);
            }
        };

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> StorageVolumeAccessChecker.check(svName, svType, locations, params));

        String message = ex.getMessage();
        // Verify the error message format includes all expected parts
        Assertions.assertTrue(message.contains("Storage volume accessibility check failed"));
        Assertions.assertTrue(message.contains("'my_storage_volume'"));
        Assertions.assertTrue(message.contains("'HDFS'"));
        Assertions.assertTrue(message.contains("'hdfs://mycluster/data'"));
        Assertions.assertTrue(message.contains(errorMsg));
    }

    @Test
    public void testCheckWithDeeplyNestedExceptionChain() {
        String svName = "test_sv";
        String svType = "S3";
        List<String> locations = Collections.singletonList("s3://bucket/path");
        Map<String, String> params = new HashMap<>();

        new MockUp<HdfsUtil>() {
            @Mock
            public void writeFile(byte[] data, String destFilePath, Map<String, String> properties)
                    throws StarRocksException {
                // Create a deeply nested exception chain
                Throwable level4 = new RuntimeException("Root cause: Invalid credentials");
                Throwable level3 = new RuntimeException("Authentication failed", level4);
                Throwable level2 = new RuntimeException("S3 client error", level3);
                Throwable level1 = new RuntimeException("AWS SDK exception", level2);
                throw new StarRocksException("Check failed", level1);
            }
        };

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> StorageVolumeAccessChecker.check(svName, svType, locations, params));

        // Should extract the deepest root cause message
        Assertions.assertTrue(ex.getMessage().contains("Root cause: Invalid credentials"));
    }

    @Test
    public void testCheckWithNullExceptionCause() {
        String svName = "test_sv";
        String svType = "S3";
        List<String> locations = Collections.singletonList("s3://bucket/path");
        Map<String, String> params = new HashMap<>();

        new MockUp<HdfsUtil>() {
            @Mock
            public void writeFile(byte[] data, String destFilePath, Map<String, String> properties)
                    throws StarRocksException {
                // Exception with message but no cause
                throw new StarRocksException("Simple error message");
            }
        };

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> StorageVolumeAccessChecker.check(svName, svType, locations, params));

        Assertions.assertTrue(ex.getMessage().contains("Simple error message"));
    }

    @Test
    public void testCheckWithNullMessageAndNullCause() {
        String svName = "test_sv";
        String svType = "S3";
        List<String> locations = Collections.singletonList("s3://bucket/path");
        Map<String, String> params = new HashMap<>();

        new MockUp<HdfsUtil>() {
            @Mock
            public void writeFile(byte[] data, String destFilePath, Map<String, String> properties)
                    throws StarRocksException {
                // Exception with null message and null cause
                throw new StarRocksException((Throwable) null);
            }
        };

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> StorageVolumeAccessChecker.check(svName, svType, locations, params));

        // Should handle gracefully
        Assertions.assertTrue(ex.getMessage().contains("Storage volume accessibility check failed"));
    }

    /**
     * Verifies that a deletePath failure during cleanup does not cause the overall check to fail.
     * Cleanup of the temp file is best-effort.
     */
    @Test
    public void testDeleteFailureIsNonFatal() {
        String svName = "test_sv";
        String svType = "S3";
        List<String> locations = Collections.singletonList("s3://bucket/path");
        Map<String, String> params = new HashMap<>();

        new MockUp<HdfsUtil>() {
            @Mock
            public void writeFile(byte[] data, String destFilePath, Map<String, String> properties)
                    throws StarRocksException {
                // write succeeds
            }

            @Mock
            public void deletePath(String path, Map<String, String> properties) throws StarRocksException {
                // cleanup fails, but should not surface to the caller
                throw new StarRocksException("delete failed");
            }
        };

        // Delete failure must not fail the overall accessibility check
        Assertions.assertDoesNotThrow(() ->
                StorageVolumeAccessChecker.check(svName, svType, locations, params));
    }

    /**
     * Verifies that the temp file is written inside the given location (trailing slash normalised).
     */
    @Test
    public void testTempFileWrittenInsideLocation() {
        String svName = "test_sv";
        String svType = "S3";
        String location = "s3://bucket/prefix"; // no trailing slash
        List<String> locations = Collections.singletonList(location);
        Map<String, String> params = new HashMap<>();

        AtomicInteger writeCount = new AtomicInteger(0);

        new MockUp<HdfsUtil>() {
            @Mock
            public void writeFile(byte[] data, String destFilePath, Map<String, String> properties)
                    throws StarRocksException {
                writeCount.incrementAndGet();
                // The temp path must be a child of the normalised location
                Assertions.assertTrue(destFilePath.startsWith(location + "/"),
                        "Temp file must be written inside the location");
                Assertions.assertTrue(destFilePath.contains(".starrocks_sv_access_check_"),
                        "Temp file must use the expected naming prefix");
            }

            @Mock
            public void deletePath(String path, Map<String, String> properties) throws StarRocksException {
                // success
            }
        };

        Assertions.assertDoesNotThrow(() ->
                StorageVolumeAccessChecker.check(svName, svType, locations, params));
        Assertions.assertEquals(1, writeCount.get());
    }
}
