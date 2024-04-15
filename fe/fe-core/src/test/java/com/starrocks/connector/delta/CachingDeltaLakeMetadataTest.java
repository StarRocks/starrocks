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
package com.starrocks.connector.delta;

import com.ibm.icu.impl.data.ResourceReader;
import com.starrocks.common.Config;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.delta.cache.DeltaLakeTableName;
import com.starrocks.connector.delta.cache.DeltaTableMetadata;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.connector.hive.HiveMetastoreTest;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.internal.util.DeltaJsonUtil;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class CachingDeltaLakeMetadataTest {
    private HiveMetaClient client;
    private HiveMetastore metastore;
    private CachingDeltaLakeMetadata cachingDeltaLakeMetadata;
    private ExecutorService executor;

    private final long expireAfterWriteSec = 10;
    private final long refreshAfterWriteSec = -1;
    private DeltaLakeTableName tbl;
    private Snapshot snapshot;

    static {
        String tmpCachePath = Objects.requireNonNull(ResourceReader.class.getClassLoader()
                .getResource("data/delta")).getPath();
        Config.delta_metadata_cache_disk_path = tmpCachePath;
    }

    @Before
    public void setUp() throws Exception {
        client = new HiveMetastoreTest.MockedHiveMetaClient();
        metastore = new HiveMetastore(client, "delta_lake_catalog", MetastoreType.HMS);
        executor = Executors.newFixedThreadPool(5);
        cachingDeltaLakeMetadata = new CachingDeltaLakeMetadata(metastore, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        String hdfsPath = Objects.requireNonNull(ResourceReader.class.getClassLoader()
                .getResource("data/delta/read_partition")).getPath();
        hdfsPath = "file://" + hdfsPath;

        tbl = new DeltaLakeTableName("delta_lake_catalog", "db1", "tbl1", hdfsPath);

        DeltaLog deltaLog = DeltaLog.forTable(new Configuration(), hdfsPath);
        snapshot = deltaLog.snapshot();
        cachingDeltaLakeMetadata.refreshTable(snapshot, tbl);
    }

    @After
    public void tearDown() {
        cachingDeltaLakeMetadata.invalidateAll();
        executor.shutdown();
    }

    @Test
    public void testloaderCacheTable() {
        DeltaTableMetadata deltaMetadata = cachingDeltaLakeMetadata.loaderCacheTable(tbl);
        Assert.assertNotNull(deltaMetadata);

        Assert.assertEquals(deltaMetadata.getSnapshotVersion().longValue(),
                snapshot.getVersion());
        Assert.assertEquals(deltaMetadata.getMetadata(), DeltaJsonUtil.convertMetadataJToJson(snapshot.getMetadata()));
    }

    @Test
    public void testGetCacheSnapshotVersion() {

        Assert.assertEquals(
                cachingDeltaLakeMetadata.getCacheSnapshotVersion(tbl).longValue(),
                snapshot.getVersion());
    }

    @Test
    public void testGetLastSnapshotVersion() throws IOException {

        final Long lastSnapshotVersion = cachingDeltaLakeMetadata
                .getLastSnapshotVersion(tbl, new Configuration());
        Assert.assertEquals(lastSnapshotVersion.longValue(),
                snapshot.getVersion());
    }


    @Test
    public void testGetMetadata() throws Exception {
        final Metadata metadata = cachingDeltaLakeMetadata.getMetadata(tbl);
        Assert.assertEquals(snapshot.getMetadata(), metadata);
    }

    @Test
    public void testGetAllPartitions() throws Exception {
        Set<Map<String, String>> cacheAllPartitions = cachingDeltaLakeMetadata.getAllPartitions(tbl);

        List<AddFile> allFiles = snapshot.getAllFiles();

        Set<Map<String, String>> expectAllParitions = allFiles
                .stream()
                .map(AddFile::getPartitionValues)
                .collect(Collectors.toSet());

        Assert.assertEquals(cacheAllPartitions, expectAllParitions);
    }
}
