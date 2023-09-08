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
package com.starrocks.connector.delta.cache;

import com.ibm.icu.impl.data.ResourceReader;
import com.starrocks.common.Config;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.delta.CachingDeltaLakeMetadata;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.connector.hive.HiveMetastoreTest;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.internal.DeltaLogImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CachingDeltaLogImplTest {

    private HiveMetaClient client;
    private HiveMetastore metastore;
    private ExecutorService executor;
    private long expireAfterWriteSec = 10;
    private long refreshAfterWriteSec = -1;

    private DeltaLakeTableName deltaTbl;

    static {
        String tmpCachePath = Objects.requireNonNull(ResourceReader.class.getClassLoader()
                .getResource("data/delta")).getPath();
        Config.delta_metadata_cache_disk_path = tmpCachePath;
    }

    @Before
    public void setUp() throws Exception {
        client = new HiveMetastoreTest.MockedHiveMetaClient();
        metastore = new HiveMetastore(client, "delta_catalog", MetastoreType.HMS);
        executor = Executors.newFixedThreadPool(5);
        String path = Objects.requireNonNull(ResourceReader.class.getClassLoader()
                .getResource("data/delta/read_partition")).getPath();
        path = "file://" + path;
        deltaTbl = new DeltaLakeTableName("delta_catalog", "db1", "tb1", path);
    }

    @After
    public void tearDown() {
        executor.shutdown();
    }


    @Test
    public void testDeltaLog() throws IOException {
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment();
        Configuration config = hdfsEnvironment.getConfiguration();

        CachingDeltaLakeMetadata cachingDeltaLakeMeta = new CachingDeltaLakeMetadata(metastore,
                executor, expireAfterWriteSec, refreshAfterWriteSec,
                1000, false);
        DeltaLog cacheDeltaLog = CachingDeltaLogImpl.forTable(config, cachingDeltaLakeMeta, deltaTbl);

        Assert.assertTrue("deltalog is be an instance of CachingDeltaLogImpl",
                cacheDeltaLog instanceof CachingDeltaLogImpl);

        Assert.assertFalse("deltalog is not an instance of CachingDeltaLogImpl",
                cacheDeltaLog instanceof DeltaLogImpl);

        cachingDeltaLakeMeta.invalidateAll();
    }

    @Test
    public void testSnapshot() throws IOException {

        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment();
        Configuration config = hdfsEnvironment.getConfiguration();

        CachingDeltaLakeMetadata cachingDeltaLakeMeta = new CachingDeltaLakeMetadata(metastore,
                executor, expireAfterWriteSec, refreshAfterWriteSec,
                1000, false);

        DeltaLog cacheDeltaLog = CachingDeltaLogImpl.forTable(config, cachingDeltaLakeMeta, deltaTbl);
        Snapshot cacheSnapshot = cacheDeltaLog.snapshot();
        DeltaLog deltaLog = DeltaLog.forTable(config, deltaTbl.getDataPath());
        //test schema
        Assert.assertEquals(deltaLog.snapshot().getMetadata(), cacheSnapshot.getMetadata());
        //test snapshot version
        Assert.assertEquals(deltaLog.snapshot().getVersion(), cacheSnapshot.getVersion());
        //test addfile
        Assert.assertEquals(deltaLog.snapshot().getAllFiles().size(), cacheSnapshot.getAllFiles().size());

        cachingDeltaLakeMeta.invalidateAll();

    }

    @Test
    public void testUpdate() throws IOException, URISyntaxException {
        //test update cache
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment();
        Configuration config = hdfsEnvironment.getConfiguration();
        CachingDeltaLakeMetadata cachingDeltaLakeMeta = new CachingDeltaLakeMetadata(metastore,
                executor, expireAfterWriteSec, refreshAfterWriteSec,
                1000, false);
        DeltaLog cacheDeltaLog = CachingDeltaLogImpl.forTable(config, cachingDeltaLakeMeta, deltaTbl);
        Snapshot cacheSnapshot = cacheDeltaLog.snapshot();
        //test update before
        DeltaLog deltaLog = DeltaLog.forTable(config, deltaTbl.getDataPath());
        Assert.assertEquals(cacheSnapshot.getVersion(), deltaLog.snapshot().getVersion());
        Assert.assertEquals(cacheSnapshot.getAllFiles().size(), deltaLog.snapshot().getAllFiles().size());

        newCommitter();

        //test delta append
        deltaLog = DeltaLog.forTable(config, deltaTbl.getDataPath());
        Assert.assertNotEquals(cacheSnapshot.getAllFiles().size(), deltaLog.snapshot().getAllFiles().size());
        Assert.assertNotEquals(cacheSnapshot.getVersion(), deltaLog.snapshot().getVersion());

        //test updated after
        cacheSnapshot = cacheDeltaLog.update();
        Assert.assertEquals(cacheSnapshot.getAllFiles().size(), deltaLog.snapshot().getAllFiles().size());
        Assert.assertEquals(cacheSnapshot.getVersion(), deltaLog.snapshot().getVersion());

        cachingDeltaLakeMeta.invalidateAll();

    }


    private static void newCommitter() throws URISyntaxException, IOException {
        Path path = Paths.get(Objects.requireNonNull(ResourceReader.class.getClassLoader()
                .getResource("data/delta/read_partition/")).toURI());
        Path appendingDeltaLog = Paths.get(path.getParent().toString(),
                "/append/_delta_log/00000000000000000006.json");
        Path appendingData = Paths.get(path.getParent().toString(),
                "/append/dt=2023-08-18/part-00001-5c533b12-24eb-470d-bf6c-83767c05ebc4.c000.snappy.parquet");
        Path deltaLogPath = Paths.get(path.toString(), "_delta_log", appendingDeltaLog.getFileName().toString());
        Path dataPath = Paths.get(path.toString(), "dt=2023-08-18", appendingData.getFileName().toString());
        Files.deleteIfExists(deltaLogPath);
        Files.deleteIfExists(dataPath);
        Files.deleteIfExists(dataPath.getParent());
        Files.copy(appendingDeltaLog, deltaLogPath);
        Files.createDirectory(dataPath.getParent());
        Files.copy(appendingData, dataPath);
    }


}
