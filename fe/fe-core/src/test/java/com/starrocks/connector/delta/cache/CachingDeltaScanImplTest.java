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
import io.delta.standalone.DeltaScan;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.expressions.Column;
import io.delta.standalone.expressions.Expression;
import io.delta.standalone.expressions.GreaterThanOrEqual;
import io.delta.standalone.expressions.Literal;
import io.delta.standalone.types.StringType;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CachingDeltaScanImplTest {
    private HiveMetaClient client;
    private HiveMetastore metastore;
    private ExecutorService executor;
    private long expireAfterWriteSec = 10;
    private long refreshAfterWriteSec = -1;

    private DeltaLakeTableName deltaTbl;
    private CachingDeltaLakeMetadata cachingDeltaLakeMeta;

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
        cachingDeltaLakeMeta = new CachingDeltaLakeMetadata(metastore,
                executor, expireAfterWriteSec, refreshAfterWriteSec,
                1000, false);
    }

    @After
    public void tearDown() {
        cachingDeltaLakeMeta.invalidateAll();
        executor.shutdown();
    }

    @Test
    public void testGetFiles() throws IOException {
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment();
        Configuration config = hdfsEnvironment.getConfiguration();
        DeltaLog deltaLog = CachingDeltaLogImpl.forTable(config, cachingDeltaLakeMeta, deltaTbl);


        Expression expression = new GreaterThanOrEqual(new Column("dt", new StringType()),
                Literal.of("2023-08-03"));
        DeltaScan deltaScan = deltaLog.snapshot().scan(expression);

        List<AddFile> addFileList = new ArrayList<>();
        for (CloseableIterator<AddFile> it = deltaScan.getFiles(); it.hasNext(); ) {
            AddFile addFile = it.next();
            addFileList.add(addFile);
        }
        Assert.assertEquals(addFileList.size(), 1);

    }


}
