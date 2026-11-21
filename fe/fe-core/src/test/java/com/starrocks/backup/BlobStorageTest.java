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

package com.starrocks.backup;

import com.starrocks.common.StarRocksException;
import com.starrocks.fs.hdfs.HdfsService;
import com.starrocks.thrift.TBrokerCloseWriterRequest;
import com.starrocks.thrift.TBrokerFD;
import com.starrocks.thrift.TBrokerOpenWriterRequest;
import com.starrocks.thrift.TBrokerPWriteRequest;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class BlobStorageTest {
    private static final String REMOTE_PATH = "s3://bucket/backup/__ss_1/file.dat";

    @TempDir
    Path tempDir;

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

    private String localFile() throws Exception {
        File file = tempDir.resolve("file.dat").toFile();
        Files.write(file.toPath(), "data".getBytes());
        return file.getAbsolutePath();
    }

    @Test
    public void testUploadWithoutBrokerReportsCloseFailure() throws Exception {
        // S3A uploads on close, so a close failure (e.g. 403 on PutObject) is a failed upload.
        AtomicInteger closeCalls = new AtomicInteger();
        mockWriter(null, "Forbidden (Service: Amazon S3; Status Code: 403)", closeCalls);

        BlobStorage storage = new BlobStorage(null, new HashMap<>(), false);
        Status status = storage.upload(localFile(), REMOTE_PATH);
        Assertions.assertFalse(status.ok());
        Assertions.assertTrue(status.getErrMsg().contains("403"), status.getErrMsg());
        Assertions.assertEquals(1, closeCalls.get());
    }

    @Test
    public void testUploadWithoutBrokerKeepsWriteFailure() throws Exception {
        AtomicInteger closeCalls = new AtomicInteger();
        mockWriter("pwrite failed", "close failed", closeCalls);

        BlobStorage storage = new BlobStorage(null, new HashMap<>(), false);
        Status status = storage.upload(localFile(), REMOTE_PATH);
        Assertions.assertFalse(status.ok());
        Assertions.assertTrue(status.getErrMsg().contains("pwrite failed"), status.getErrMsg());
        Assertions.assertFalse(status.getErrMsg().contains("close failed"), status.getErrMsg());
        Assertions.assertEquals(1, closeCalls.get());
    }

    @Test
    public void testUploadWithoutBrokerKeepsLocalFileError() {
        AtomicInteger closeCalls = new AtomicInteger();
        mockWriter(null, "close failed", closeCalls);

        BlobStorage storage = new BlobStorage(null, new HashMap<>(), false);
        Status status = storage.upload(tempDir.resolve("missing.dat").toString(), REMOTE_PATH);
        Assertions.assertFalse(status.ok());
        Assertions.assertTrue(status.getErrMsg().contains("file not found"), status.getErrMsg());
        Assertions.assertEquals(1, closeCalls.get());
    }

    @Test
    public void testUploadWithoutBrokerSucceeds() throws Exception {
        AtomicInteger closeCalls = new AtomicInteger();
        mockWriter(null, null, closeCalls);

        BlobStorage storage = new BlobStorage(null, new HashMap<>(), false);
        Status status = storage.upload(localFile(), REMOTE_PATH);
        Assertions.assertTrue(status.ok(), status.getErrMsg());
        Assertions.assertEquals(1, closeCalls.get());
    }
}
