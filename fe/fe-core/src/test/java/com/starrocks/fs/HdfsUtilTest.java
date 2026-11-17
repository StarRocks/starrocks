// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.fs;

import com.starrocks.common.StarRocksException;
import com.starrocks.fs.hdfs.HdfsFs;
import com.starrocks.fs.hdfs.HdfsFsManager;
import com.starrocks.fs.hdfs.HdfsService;
import com.starrocks.thrift.TBrokerCloseWriterRequest;
import com.starrocks.thrift.TBrokerFD;
import com.starrocks.thrift.TBrokerOpenWriterRequest;
import com.starrocks.thrift.TBrokerPWriteRequest;
import com.starrocks.thrift.THdfsProperties;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class HdfsUtilTest {
    @TempDir
    Path tempDir;

    @Test
    public void testColumnFromPath() {
        try {
            // normal case
            List<String> columns = HdfsUtil.parseColumnsFromPath("hdfs://key1=val1/some_path/key2=val2/key3=val3/*", Arrays.asList("key3", "key2", "key1"));
            Assertions.assertEquals(3, columns.size());
            Assertions.assertEquals("val3", columns.get(0));
            Assertions.assertEquals("val2", columns.get(1));
            Assertions.assertEquals("val1", columns.get(2));

            // invalid path
            Assertions.assertThrows(StarRocksException.class, () ->
                    HdfsUtil.parseColumnsFromPath("invalid_path", Arrays.asList("key3", "key2", "key1")));

            // missing key of columns from path
            Assertions.assertThrows(StarRocksException.class, () ->
                    HdfsUtil.parseColumnsFromPath("hdfs://key1=val1/some_path/key3=val3/*", Arrays.asList("key3", "key2", "key1")));
        } catch (StarRocksException e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testException() {
        new MockUp<HdfsFsManager>() {
            @Mock
            public HdfsFs getFileSystem(String path, Map<String, String> loadProperties, THdfsProperties tProperties)
                                        throws StarRocksException {
                return null;
            }
        };

        Assertions.assertThrows(StarRocksException.class, () ->
                HdfsUtil.deletePath("hdfs://abc/dbf", new HashMap<>()));

        Assertions.assertThrows(StarRocksException.class, () ->
                HdfsUtil.rename("hdfs://abc/dbf", "hdfs://abc/dba", new HashMap<>(), 1000));

        Assertions.assertThrows(StarRocksException.class, () ->
                HdfsUtil.checkPathExist("hdfs://abc/dbf", new HashMap<>()));

        HdfsFsManager fileSystemManager = new HdfsFsManager();
        Assertions.assertThrows(StarRocksException.class, () ->
                fileSystemManager.openReader("hdfs://abc/dbf", 0, new HashMap<>()));

        Assertions.assertThrows(StarRocksException.class, () ->
                fileSystemManager.openWriter("hdfs://abc/dbf", new HashMap<>()));
    }

    /**
     * Mocks the writer side of HdfsService. A null message means the call succeeds.
     */
    private static void mockWriter(String pwriteError, String closeError, AtomicInteger closeCalls) {
        new MockUp<HdfsService>() {
            @Mock
            public TBrokerFD openWriter(TBrokerOpenWriterRequest request) {
                return new TBrokerFD(1L, 2L);
            }

            @Mock
            public void pwrite(TBrokerPWriteRequest request) throws StarRocksException {
                if (pwriteError != null) {
                    throw new StarRocksException(pwriteError);
                }
            }

            @Mock
            public void closeWriter(TBrokerCloseWriterRequest request) throws StarRocksException {
                closeCalls.incrementAndGet();
                if (closeError != null) {
                    throw new StarRocksException(closeError);
                }
            }
        };
    }

    @Test
    public void testWriteFileReportsCloseFailure() {
        // S3A uploads on close, so a close failure (e.g. 403 on PutObject) is a failed write.
        AtomicInteger closeCalls = new AtomicInteger();
        mockWriter(null, "Forbidden (Service: Amazon S3; Status Code: 403)", closeCalls);

        StarRocksException e = Assertions.assertThrows(StarRocksException.class, () ->
                HdfsUtil.writeFile("data".getBytes(), "s3://bucket/path/file", new HashMap<>()));
        Assertions.assertTrue(e.getMessage().contains("403"), e.getMessage());
        // The failed close already released the stream; the finally block must not close it again.
        Assertions.assertEquals(1, closeCalls.get());
    }

    @Test
    public void testWriteFileKeepsWriteFailureAndClosesOnce() {
        AtomicInteger closeCalls = new AtomicInteger();
        mockWriter("pwrite failed", "close failed", closeCalls);

        StarRocksException e = Assertions.assertThrows(StarRocksException.class, () ->
                HdfsUtil.writeFile("data".getBytes(), "s3://bucket/path/file", new HashMap<>()));
        Assertions.assertEquals("pwrite failed", e.getMessage());
        Assertions.assertEquals(1, closeCalls.get());
    }

    @Test
    public void testWriteFileSucceeds() throws Exception {
        AtomicInteger closeCalls = new AtomicInteger();
        mockWriter(null, null, closeCalls);

        HdfsUtil.writeFile("data".getBytes(), "s3://bucket/path/file", new HashMap<>());
        Assertions.assertEquals(1, closeCalls.get());
    }

    @Test
    public void testWriteLocalFileReportsCloseFailure() throws Exception {
        AtomicInteger closeCalls = new AtomicInteger();
        mockWriter(null, "close failed", closeCalls);

        Path src = Files.write(tempDir.resolve("src.dat"), "data".getBytes());

        StarRocksException e = Assertions.assertThrows(StarRocksException.class, () ->
                HdfsUtil.writeFile(src.toString(), "s3://bucket/path/file", new HashMap<>()));
        Assertions.assertEquals("close failed", e.getMessage());
        Assertions.assertEquals(1, closeCalls.get());
    }

    @Test
    public void testWriterCloseIsIdempotent() throws Exception {
        AtomicInteger closeCalls = new AtomicInteger();
        mockWriter(null, "close failed", closeCalls);

        HdfsUtil.HdfsWriter writer = new HdfsUtil.HdfsWriter("s3://bucket/path/file", new HashMap<>());
        // Never opened: nothing to close.
        writer.close();
        Assertions.assertEquals(0, closeCalls.get());

        writer.open();
        Assertions.assertThrows(StarRocksException.class, writer::close);
        writer.close();
        writer.closeQuietly();
        Assertions.assertEquals(1, closeCalls.get());
    }
}
