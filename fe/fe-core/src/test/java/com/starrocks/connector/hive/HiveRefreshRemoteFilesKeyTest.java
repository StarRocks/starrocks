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
import com.starrocks.connector.RemoteFileScanContext;
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

/**
 * The refreshes an INSERT ... SELECT runs on its Hive sources must reach the file-cache entry a scan reads, which is
 * keyed by the raw partition location (usually without a trailing slash) and need not lie under the table location.
 */
public class HiveRefreshRemoteFilesKeyTest {
    private static final String TABLE_LOCATION = "hdfs://nn/warehouse/t";
    private static final String LOCATION = TABLE_LOCATION + "/p=1";
    // A partition whose location was set outside its table's directory.
    private static final String CUSTOM_LOCATION = "hdfs://nn/elsewhere/p=2";
    private static final String OTHER_TABLE_LOCATION = "hdfs://nn/warehouse/other/p=1";

    private final AtomicInteger fileCount = new AtomicInteger(1);
    private final ExecutorService executor = MoreExecutors.newDirectExecutorService();

    private RemoteFileIO countingFileIO() {
        return new RemoteFileIO() {
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
    }

    @Test
    public void testRefreshUpdatesScanCacheKey() {
        CachingRemoteFileIO fileIO = CachingRemoteFileIO.createCatalogLevelInstance(countingFileIO(), executor, 3600, -1, 0.1);
        HiveCacheUpdateProcessor processor =
                new HiveCacheUpdateProcessor("hive_catalog", mockMetastore(), fileIO, executor, false, false);

        // The scan side keys the cache with the raw partition location (RemoteFileOperations.getRemoteFiles).
        RemotePathKey scanKey = RemotePathKey.of(LOCATION, false);
        Assertions.assertEquals(1, fileIO.getRemoteFiles(scanKey).get(scanKey).size());
        // An entry keyed with a trailing slash, as an earlier full refresh used to leave behind.
        RemotePathKey slashKey = RemotePathKey.of(LOCATION + "/", false);
        Assertions.assertEquals(1, fileIO.getRemoteFiles(slashKey).get(slashKey).size());

        // A file is appended to the existing partition, then the whole table is refreshed.
        fileCount.set(2);
        processor.refreshTable("db", mockTable(), false);

        Assertions.assertEquals(2, fileIO.getPresentRemoteFiles(Lists.newArrayList(scanKey)).get(scanKey).size());
        Assertions.assertEquals(2, fileIO.getPresentRemoteFiles(Lists.newArrayList(slashKey)).get(slashKey).size());
    }

    @Test
    public void testRefreshLoadsUncachedPartitionUnderScanKey() {
        CachingRemoteFileIO fileIO = CachingRemoteFileIO.createCatalogLevelInstance(countingFileIO(), executor, 3600, -1, 0.1);
        HiveCacheUpdateProcessor processor =
                new HiveCacheUpdateProcessor("hive_catalog", mockMetastore(), fileIO, executor, false, false);

        processor.refreshTable("db", mockTable(), false);

        RemotePathKey scanKey = RemotePathKey.of(LOCATION, false);
        Assertions.assertTrue(fileIO.getPresentRemoteFiles(Lists.newArrayList(scanKey)).containsKey(scanKey));
        RemotePathKey slashKey = RemotePathKey.of(LOCATION + "/", false);
        Assertions.assertFalse(fileIO.getPresentRemoteFiles(Lists.newArrayList(slashKey)).containsKey(slashKey));
    }

    @Test
    public void testRefreshUpdatesSlashKeyOutsideTableLocation() {
        // A custom partition location outside the table directory, spelled with a trailing slash in HMS: the scan
        // keys the cache with exactly that spelling.
        CachingRemoteFileIO fileIO = CachingRemoteFileIO.createCatalogLevelInstance(countingFileIO(), executor, 3600, -1, 0.1);
        IHiveMetastore metastore = Mockito.mock(IHiveMetastore.class);
        Mockito.when(metastore.getPartitionKeysByValue(Mockito.anyString(), Mockito.anyString(), Mockito.any()))
                .thenReturn(Lists.newArrayList("p=2"));
        Mockito.when(metastore.getPartitionsByNames(Mockito.anyString(), Mockito.anyString(), Mockito.anyList()))
                .thenReturn(ImmutableMap.of("p=2",
                        new Partition(ImmutableMap.of(), null, null, CUSTOM_LOCATION + "/", true)));
        RemotePathKey scanKey = RemotePathKey.of(CUSTOM_LOCATION + "/", false);
        Assertions.assertEquals(1, fileIO.getRemoteFiles(scanKey).get(scanKey).size());

        fileCount.set(2);
        new HiveCacheUpdateProcessor("hive_catalog", metastore, fileIO, executor, false, false)
                .refreshTable("db", mockTable(), false);

        Assertions.assertEquals(2, fileIO.getPresentRemoteFiles(Lists.newArrayList(scanKey)).get(scanKey).size());
    }

    @Test
    public void testInvalidateForReadDropsEveryKeyOfTheTable() {
        CachingRemoteFileIO fileIO = CachingRemoteFileIO.createCatalogLevelInstance(countingFileIO(), executor, 3600, -1, 0.1);
        IHiveMetastore underlying = Mockito.mock(IHiveMetastore.class);
        Mockito.when(underlying.getPartitionsByNames(Mockito.anyString(), Mockito.anyString(), Mockito.anyList()))
                .thenReturn(ImmutableMap.of("p=2", new Partition(ImmutableMap.of(), null, null, CUSTOM_LOCATION, true)));
        CachingHiveMetastore metastore = CachingHiveMetastore.createCatalogLevelInstance(
                underlying, executor, executor, 3600, -1, 1000, false);
        // The custom-location partition is cached, so its location is known without asking the metastore.
        metastore.getPartitionsByNames("db", "t", Lists.newArrayList("p=2"));
        Assertions.assertEquals(1, metastore.getCachedPartitionsOfTable("db", "t").size());

        Table table = mockTable();
        // Under the table location, in both slash forms.
        RemotePathKey scanKey = loadKey(fileIO, LOCATION, table);
        RemotePathKey slashKey = loadKey(fileIO, LOCATION + "/", null);
        // Outside it: one loaded by a scan of this table, one known only through its cached partition.
        RemotePathKey customScanKey = loadKey(fileIO, CUSTOM_LOCATION + "/x", table);
        RemotePathKey customKey = loadKey(fileIO, CUSTOM_LOCATION, null);
        // Another table's entry must survive.
        RemotePathKey otherKey = loadKey(fileIO, OTHER_TABLE_LOCATION, null);

        HiveCacheUpdateProcessor processor =
                new HiveCacheUpdateProcessor("hive_catalog", metastore, fileIO, executor, false, false);
        processor.invalidateTableForRead(table);

        Map<RemotePathKey, List<RemoteFileDesc>> present = fileIO.getPresentRemoteFiles(
                Lists.newArrayList(scanKey, slashKey, customScanKey, customKey, otherKey));
        Assertions.assertEquals(ImmutableList.of(otherKey), ImmutableList.copyOf(present.keySet()));
        Assertions.assertTrue(metastore.getCachedPartitionsOfTable("db", "t").isEmpty());
    }

    @Test
    public void testScanSeesAppendedFileAfterInvalidateForRead() {
        CachingRemoteFileIO fileIO = CachingRemoteFileIO.createCatalogLevelInstance(countingFileIO(), executor, 3600, -1, 0.1);
        // The query-level cache sits on the catalog-level one, as during planning.
        CachingRemoteFileIO queryLevel = CachingRemoteFileIO.createQueryLevelInstance(fileIO, 0.1);
        Table table = mockTable();
        RemotePathKey scanKey = loadKey(fileIO, LOCATION, table);
        Assertions.assertEquals(1, fileIO.getPresentRemoteFiles(Lists.newArrayList(scanKey)).get(scanKey).size());

        fileCount.set(2);
        new HiveCacheUpdateProcessor("hive_catalog", mockMetastore(), fileIO, executor, false, false)
                .invalidateTableForRead(table);

        RemotePathKey nextScan = RemotePathKey.of(LOCATION, false);
        nextScan.setScanContext(new RemoteFileScanContext(table));
        Assertions.assertEquals(2, queryLevel.getRemoteFiles(nextScan).get(nextScan).size());
    }

    private static RemotePathKey loadKey(CachingRemoteFileIO fileIO, String path, Table scannedFor) {
        RemotePathKey key = RemotePathKey.of(path, false);
        if (scannedFor != null) {
            key.setScanContext(new RemoteFileScanContext(scannedFor));
        }
        fileIO.getRemoteFiles(key);
        return key;
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
        Mockito.when(table.getTableLocation()).thenReturn(TABLE_LOCATION);
        return table;
    }
}
