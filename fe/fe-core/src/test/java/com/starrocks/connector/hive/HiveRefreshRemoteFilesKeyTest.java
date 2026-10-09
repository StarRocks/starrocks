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

package com.starrocks.connector.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.starrocks.catalog.Table;
import com.starrocks.connector.CachingRemoteFileIO;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileIO;
import com.starrocks.connector.RemotePathKey;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

// A full table refresh (auto refresh of INSERT ... SELECT) must update the file-cache entry a scan reads,
// which is keyed by the raw partition location without a trailing slash.
public class HiveRefreshRemoteFilesKeyTest {
    private static final String LOCATION = "hdfs://nn/warehouse/t/p=1";

    @Test
    public void testRefreshUpdatesScanCacheKey() {
        AtomicInteger fileCount = new AtomicInteger(1);
        RemoteFileIO underlying = new RemoteFileIO() {
            @Override
            public Map<RemotePathKey, List<RemoteFileDesc>> getRemoteFiles(RemotePathKey pathKey) {
                List<RemoteFileDesc> files = Lists.newArrayList();
                for (int i = 0; i < fileCount.get(); i++) {
                    files.add(new RemoteFileDesc("f" + i, "", 1, 1, ImmutableList.of()));
                }
                return ImmutableMap.of(pathKey, files);
            }

            @Override
            public FileStatus[] getFileStatus(Path... files) {
                return new FileStatus[0];
            }
        };
        ExecutorService executor = MoreExecutors.newDirectExecutorService();
        CachingRemoteFileIO fileIO = CachingRemoteFileIO.createCatalogLevelInstance(underlying, executor, 3600, -1, 0.1);

        IHiveMetastore metastore = mockMetastore();
        Table table = mockTable();

        HiveCacheUpdateProcessor processor =
                new HiveCacheUpdateProcessor("hive_catalog", metastore, fileIO, executor, false, false);

        // The scan side keys the cache with the raw partition location (RemoteFileOperations.getRemoteFiles).
        RemotePathKey scanKey = RemotePathKey.of(LOCATION, false);
        Assertions.assertEquals(1, fileIO.getRemoteFiles(scanKey).get(scanKey).size());
        // An entry keyed with a trailing slash, as an earlier full refresh used to leave behind.
        RemotePathKey slashKey = RemotePathKey.of(LOCATION + "/", false);
        Assertions.assertEquals(1, fileIO.getRemoteFiles(slashKey).get(slashKey).size());

        // A file is appended to the existing partition, then the table is refreshed as auto refresh does.
        fileCount.set(2);
        processor.refreshTable("db", table, false);

        Assertions.assertEquals(2, fileIO.getPresentRemoteFiles(Lists.newArrayList(scanKey)).get(scanKey).size());
        Assertions.assertEquals(2, fileIO.getPresentRemoteFiles(Lists.newArrayList(slashKey)).get(slashKey).size());
    }

    @Test
    public void testRefreshLoadsUncachedPartitionUnderScanKey() {
        RemoteFileIO underlying = new RemoteFileIO() {
            @Override
            public Map<RemotePathKey, List<RemoteFileDesc>> getRemoteFiles(RemotePathKey pathKey) {
                return ImmutableMap.of(pathKey, Lists.newArrayList(new RemoteFileDesc("f0", "", 1, 1, ImmutableList.of())));
            }

            @Override
            public FileStatus[] getFileStatus(Path... files) {
                return new FileStatus[0];
            }
        };
        ExecutorService executor = MoreExecutors.newDirectExecutorService();
        CachingRemoteFileIO fileIO = CachingRemoteFileIO.createCatalogLevelInstance(underlying, executor, 3600, -1, 0.1);
        HiveCacheUpdateProcessor processor =
                new HiveCacheUpdateProcessor("hive_catalog", mockMetastore(), fileIO, executor, false, false);

        processor.refreshTable("db", mockTable(), false);

        RemotePathKey scanKey = RemotePathKey.of(LOCATION, false);
        Assertions.assertTrue(fileIO.getPresentRemoteFiles(Lists.newArrayList(scanKey)).containsKey(scanKey));
        RemotePathKey slashKey = RemotePathKey.of(LOCATION + "/", false);
        Assertions.assertFalse(fileIO.getPresentRemoteFiles(Lists.newArrayList(slashKey)).containsKey(slashKey));
    }

    private static IHiveMetastore mockMetastore() {
        IHiveMetastore metastore = Mockito.mock(IHiveMetastore.class);
        Mockito.when(metastore.getPartitionKeysByValue(Mockito.anyString(), Mockito.anyString(), Mockito.any()))
                .thenReturn(Lists.newArrayList("p=1"));
        Mockito.when(metastore.getPartitionsByNames(Mockito.anyString(), Mockito.anyString(), Mockito.anyList()))
                .thenReturn(ImmutableMap.of("p=1", new Partition(ImmutableMap.of(), null, null, LOCATION, true)));
        return metastore;
    }

    private static Table mockTable() {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.isHMSTable()).thenReturn(true);
        Mockito.when(table.isUnPartitioned()).thenReturn(false);
        Mockito.when(table.getCatalogDBName()).thenReturn("db");
        Mockito.when(table.getCatalogTableName()).thenReturn("t");
        Mockito.when(table.getTableLocation()).thenReturn("hdfs://nn/warehouse/t");
        return table;
    }
}
