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
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileIO;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.connector.RemotePathKey;
import com.starrocks.qe.ConnectContext;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An INSERT ... SELECT refreshes its Hive sources for the statement (InsertSourceRefresher), and used to do it by
 * dropping them from the catalog-level cache. A background refresh takes no lock: it reads the source, then writes
 * what it read into that cache. One that read before the drop wrote the old file list or partition names back after
 * it, the INSERT planned and scanned on them, and the rows it missed were missing from the target for good. The
 * INSERT now also reads the partition names and file lists of its Hive sources past the catalog-level cache (the
 * session's useConnectorMetadataCache, set by InsertAnalyzer), which lasts until the scan lists its files at
 * scheduling time -- after planning has already dropped the query-level metadata the refresh went through.
 */
public class HiveReadPastCatalogCacheTest {
    private static final String TABLE_LOCATION = "hdfs://nn/warehouse/t";
    private static final String LOCATION = TABLE_LOCATION + "/p=1";

    private final ExecutorService executor = MoreExecutors.newDirectExecutorService();

    // What the source holds now.
    private final AtomicInteger fileCount = new AtomicInteger(1);
    private final AtomicReference<List<String>> partitionNames = new AtomicReference<>(List.of("p=1"));

    // A read made by this thread stalls after reading the source, until released.
    private volatile Thread background;
    private final CountDownLatch backgroundRead = new CountDownLatch(1);
    private final CountDownLatch releaseBackground = new CountDownLatch(1);

    private CachingHiveMetastore catalogMetastore;
    private CachingRemoteFileIO catalogFileIO;
    private ConnectContext session;

    @BeforeEach
    public void setUp() {
        catalogMetastore = CachingHiveMetastore.createCatalogLevelInstance(source(), executor, executor,
                3600, -1, 1000, true);
        catalogFileIO = CachingRemoteFileIO.createCatalogLevelInstance(sourceFiles(), executor, 3600, -1, 0.1);
        session = new ConnectContext();
        session.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    /** What an INSERT reading a Hive source leaves on the session (InsertAnalyzer), then its source refresh. */
    private void refreshForInsert() {
        session.setUseConnectorMetadataCache(Optional.of(false));
        // The refresh goes through the query-level metadata of planning.
        queryMetadata().invalidateTableForRead("db", hmsTable());
    }

    @Test
    public void testInsertDoesNotScanAFileListWrittenBackByABackgroundRefresh() throws Exception {
        // An earlier query cached the partition's single file.
        RemotePathKey key = RemotePathKey.of(LOCATION, false);
        Assertions.assertEquals(1, catalogFileIO.getRemoteFiles(key).get(key).size());

        // A background refresh lists the partition and stalls before it writes the listing into the cache.
        Thread refresher = startBackground(() -> catalogFileIO.updateRemoteFiles(RemotePathKey.of(LOCATION, false)));
        // A file is appended, and an INSERT refreshes the table for its statement.
        fileCount.set(2);
        refreshForInsert();
        finishBackground(refresher);
        Assertions.assertEquals(1, catalogFileIO.getPresentRemoteFiles(List.of(key)).get(key).size());

        // The scan lists its files when it is scheduled, on query-level objects made afresh: planning dropped the
        // ones the refresh went through. It asks for the cache the way HiveConnectorScanRangeSource does.
        List<RemoteFileInfo> files = queryFileOps().getRemoteFiles(hmsTable(), List.of(partition()),
                GetRemoteFilesParams.newBuilder().setUseCache(HiveMetadata.useMetadataCache()).build());
        Assertions.assertEquals(1, files.size());
        Assertions.assertEquals(2, files.get(0).getFiles().size(), "the INSERT scanned the file list written back");
    }

    @Test
    public void testInsertDoesNotPruneOnPartitionNamesWrittenBackByABackgroundRefresh() throws Exception {
        // An earlier query cached the partition names.
        Assertions.assertEquals(List.of("p=1"), catalogMetastore.getPartitionKeysByValue("db", "t",
                HivePartitionValue.ALL_PARTITION_VALUES));

        // A background refresh reads the partition names and stalls before it writes them into the cache.
        Thread refresher = startBackground(() -> catalogMetastore.refreshTable("db", "t", true));
        // A partition is added, and an INSERT refreshes the table for its statement.
        partitionNames.set(List.of("p=1", "p=2"));
        refreshForInsert();
        finishBackground(refresher);
        Assertions.assertEquals(List.of("p=1"), catalogMetastore.getPartitionKeysByValue("db", "t",
                HivePartitionValue.ALL_PARTITION_VALUES));

        Assertions.assertEquals(List.of("p=1", "p=2"), queryHmsOps().getPartitionKeys("db", "t"),
                "the INSERT read the partition names written back");
    }

    /**
     * Reading past the cache used to mean dropping the entry and loading through the cache (#22272), right at the
     * scan. A write-back that landed between the two was what the scan then read.
     */
    @Test
    public void testScanDoesNotReadAFileListWrittenBackWhileItReads() throws Exception {
        RemotePathKey key = RemotePathKey.of(LOCATION, false);
        catalogFileIO.getRemoteFiles(key);
        Thread refresher = startBackground(() -> catalogFileIO.updateRemoteFiles(RemotePathKey.of(LOCATION, false)));
        fileCount.set(2);
        refreshForInsert();
        new MockUp<CachingRemoteFileIO>() {
            @Mock
            public void invalidatePartition(Invocation invocation, RemotePathKey pathKey) throws Exception {
                invocation.proceed();
                // Once the query level has dropped both levels' entries, the background refresh writes back.
                if (invocation.getInvokedInstance() != catalogFileIO && refresher.isAlive()) {
                    finishBackground(refresher);
                }
            }
        };

        List<RemoteFileInfo> files = queryFileOps().getRemoteFiles(hmsTable(), List.of(partition()),
                GetRemoteFilesParams.newBuilder().setUseCache(HiveMetadata.useMetadataCache()).build());
        finishBackground(refresher);
        Assertions.assertEquals(2, files.get(0).getFiles().size(), "the scan read the file list written back");
    }

    @Test
    public void testPruningDoesNotReadPartitionNamesWrittenBackWhileItReads() throws Exception {
        catalogMetastore.getPartitionKeysByValue("db", "t", HivePartitionValue.ALL_PARTITION_VALUES);
        Thread refresher = startBackground(() -> catalogMetastore.refreshTable("db", "t", true));
        partitionNames.set(List.of("p=1", "p=2"));
        refreshForInsert();
        new MockUp<CachingHiveMetastore>() {
            @Mock
            public void invalidatePartitionKeys(Invocation invocation, HivePartitionValue value) throws Exception {
                invocation.proceed();
                if (invocation.getInvokedInstance() != catalogMetastore && refresher.isAlive()) {
                    finishBackground(refresher);
                }
            }
        };

        List<String> names = queryHmsOps().getPartitionKeys("db", "t");
        finishBackground(refresher);
        Assertions.assertEquals(List.of("p=1", "p=2"), names, "pruning read the partition names written back");
    }

    // The query-level objects on top of the catalog-level caches, as HiveMetadataFactory#create builds them.
    private HiveMetastoreOperations queryHmsOps() {
        return new HiveMetastoreOperations(CachingHiveMetastore.createQueryLevelInstance(catalogMetastore, 100),
                true, new Configuration(), MetastoreType.HMS, "hive_catalog");
    }

    private RemoteFileOperations queryFileOps() {
        return new RemoteFileOperations(CachingRemoteFileIO.createQueryLevelInstance(catalogFileIO, 0),
                executor, executor, false, true, new Configuration());
    }

    private HiveMetadata queryMetadata() {
        HiveMetastoreOperations hmsOps = queryHmsOps();
        RemoteFileOperations fileOps = queryFileOps();
        return new HiveMetadata("hive_catalog", new HdfsEnvironment(), hmsOps, fileOps,
                new HiveStatisticsProvider(hmsOps, fileOps),
                Optional.of(new HiveCacheUpdateProcessor("hive_catalog", catalogMetastore, catalogFileIO, executor,
                        false, false)),
                executor, new ConnectorProperties(ConnectorType.HIVE));
    }

    private Thread startBackground(Runnable refresh) throws InterruptedException {
        Thread thread = new Thread(refresh, "background-refresh");
        background = thread;
        thread.start();
        Assertions.assertTrue(backgroundRead.await(60, TimeUnit.SECONDS), "the background refresh never read");
        return thread;
    }

    private void finishBackground(Thread thread) throws InterruptedException {
        releaseBackground.countDown();
        thread.join(60_000);
        Assertions.assertFalse(thread.isAlive(), "the background refresh did not finish");
    }

    private void stallIfBackground() {
        if (Thread.currentThread() != background) {
            return;
        }
        backgroundRead.countDown();
        try {
            Assertions.assertTrue(releaseBackground.await(60, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private RemoteFileIO sourceFiles() {
        return new RemoteFileIO() {
            @Override
            public Map<RemotePathKey, List<RemoteFileDesc>> getRemoteFiles(RemotePathKey pathKey) {
                List<RemoteFileDesc> files = Lists.newArrayList();
                for (int i = 0; i < fileCount.get(); i++) {
                    files.add(new RemoteFileDesc("f" + i, "", 1, 1, ImmutableList.of()));
                }
                stallIfBackground();
                return ImmutableMap.of(pathKey, files);
            }

            @Override
            public FileStatus[] getFileStatus(Path... files) {
                return new FileStatus[0];
            }
        };
    }

    private IHiveMetastore source() {
        IHiveMetastore metastore = Mockito.mock(IHiveMetastore.class);
        Table table = hmsTable();
        Mockito.when(metastore.getTable("db", "t")).thenReturn(table);
        Mockito.when(metastore.getPartitionKeysByValue(Mockito.eq("db"), Mockito.eq("t"), Mockito.any()))
                .thenAnswer(invocation -> {
                    List<String> names = partitionNames.get();
                    stallIfBackground();
                    return names;
                });
        Mockito.when(metastore.getPartitionsByNames(Mockito.eq("db"), Mockito.eq("t"), Mockito.anyList()))
                .thenAnswer(invocation -> {
                    List<String> names = invocation.getArgument(2);
                    ImmutableMap.Builder<String, Partition> partitions = ImmutableMap.builder();
                    names.forEach(name -> partitions.put(name, new Partition(ImmutableMap.of(), null, null,
                            TABLE_LOCATION + "/" + name, true)));
                    return partitions.build();
                });
        return metastore;
    }

    private static Partition partition() {
        return new Partition(ImmutableMap.of(), null, null, LOCATION, true);
    }

    private static Table hmsTable() {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.isHMSTable()).thenReturn(true);
        Mockito.when(table.isHiveTable()).thenReturn(true);
        Mockito.when(table.isUnPartitioned()).thenReturn(false);
        Mockito.when(table.getCatalogDBName()).thenReturn("db");
        Mockito.when(table.getCatalogTableName()).thenReturn("t");
        Mockito.when(table.getTableLocation()).thenReturn(TABLE_LOCATION);
        return table;
    }
}
