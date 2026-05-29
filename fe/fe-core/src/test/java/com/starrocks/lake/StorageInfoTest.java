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

package com.starrocks.lake;

import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StorageInfoTest {

    private FilePathInfo createTestFilePathInfo() {
        FilePathInfo.Builder builder = FilePathInfo.newBuilder();
        FileStoreInfo.Builder fsBuilder = builder.getFsInfoBuilder();
        S3FileStoreInfo.Builder s3FsBuilder = fsBuilder.getS3FsInfoBuilder();
        s3FsBuilder.setBucket("test-bucket");
        s3FsBuilder.setRegion("test-region");
        fsBuilder.setFsType(FileStoreType.S3);
        fsBuilder.setFsKey("test-bucket");
        fsBuilder.setS3FsInfo(s3FsBuilder.build());
        builder.setFsInfo(fsBuilder.build());
        builder.setFullPath("s3://test-bucket/test-path");
        return builder.build();
    }

    @Test
    public void testSetDataCacheEnableFromFalseToTrue() {
        FileCacheInfo cacheInfo = FileCacheInfo.newBuilder()
                .setEnableCache(false)
                .setTtlSeconds(0)
                .setAsyncWriteBack(false)
                .build();
        StorageInfo storageInfo = new StorageInfo(createTestFilePathInfo(), cacheInfo);

        Assertions.assertFalse(storageInfo.isEnableDataCache());

        storageInfo.setDataCacheEnable(true);
        Assertions.assertTrue(storageInfo.isEnableDataCache());
    }

    @Test
    public void testSetDataCacheEnableFromTrueToFalse() {
        FileCacheInfo cacheInfo = FileCacheInfo.newBuilder()
                .setEnableCache(true)
                .setTtlSeconds(-1)
                .setAsyncWriteBack(false)
                .build();
        StorageInfo storageInfo = new StorageInfo(createTestFilePathInfo(), cacheInfo);

        Assertions.assertTrue(storageInfo.isEnableDataCache());

        storageInfo.setDataCacheEnable(false);
        Assertions.assertFalse(storageInfo.isEnableDataCache());
    }

    @Test
    public void testSetDataCacheEnablePreservesOtherFields() {
        FileCacheInfo cacheInfo = FileCacheInfo.newBuilder()
                .setEnableCache(false)
                .setTtlSeconds(3600)
                .setAsyncWriteBack(false)
                .build();
        StorageInfo storageInfo = new StorageInfo(createTestFilePathInfo(), cacheInfo);

        storageInfo.setDataCacheEnable(true);

        // Other fields should be preserved
        Assertions.assertTrue(storageInfo.isEnableDataCache());
        Assertions.assertFalse(storageInfo.isEnableAsyncWriteBack());
    }

    @Test
    public void testGetDataCacheInfo() {
        FileCacheInfo cacheInfo = FileCacheInfo.newBuilder()
                .setEnableCache(true)
                .setTtlSeconds(-1)
                .setAsyncWriteBack(false)
                .build();
        StorageInfo storageInfo = new StorageInfo(createTestFilePathInfo(), cacheInfo);

        DataCacheInfo dataCacheInfo = storageInfo.getDataCacheInfo();
        Assertions.assertNotNull(dataCacheInfo);
        Assertions.assertTrue(dataCacheInfo.isEnabled());
        Assertions.assertFalse(dataCacheInfo.isAsyncWriteBack());
    }
}
