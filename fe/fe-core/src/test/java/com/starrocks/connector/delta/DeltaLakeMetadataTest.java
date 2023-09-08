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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.ibm.icu.impl.data.ResourceReader;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.hive.CachingHiveMetastore;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.connector.hive.HiveMetastoreOperations;
import com.starrocks.connector.hive.HiveMetastoreTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.hadoop.hive.common.StatsSetupConst.ROW_COUNT;
import static org.apache.hadoop.hive.common.StatsSetupConst.TOTAL_SIZE;

public class DeltaLakeMetadataTest {
    private ExecutorService executorForHmsRefresh;
    private DeltaLakeMetadata deltaLakeMetadata;
    private CachingHiveMetastore cachingHiveMetastore;


    static {
        String tmpCachePath = Objects.requireNonNull(ResourceReader.class.getClassLoader()
                .getResource("data/delta")).getPath();
        Config.delta_metadata_cache_disk_path = tmpCachePath;
    }

    @Before
    public void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        executorForHmsRefresh = Executors.newFixedThreadPool(5);
        HiveMetaClient client = new MockedDeltaHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "delta_catalog", MetastoreType.HMS);
        cachingHiveMetastore = CachingDeltaLakeMetadata.createCatalogLevelInstance(
                metastore, executorForHmsRefresh, 100, 10, 1000, false);
        HiveMetastoreOperations hmsOps = new HiveMetastoreOperations(cachingHiveMetastore, true,
                new Configuration(), MetastoreType.HMS, "delta_lake_catalog");
        // create connect context
        deltaLakeMetadata = new DeltaLakeMetadata(new HdfsEnvironment(), cachingHiveMetastore, "delta_catalog", hmsOps);
    }

    @After
    public void tearDown() {
        cachingHiveMetastore.invalidateAll();
    }

    @Test
    public void testGetTable() throws TException {
        DeltaLakeTable deltaLakeTable = (DeltaLakeTable) deltaLakeMetadata.getTable("db1", "tbl1");
        Assert.assertEquals("db1", deltaLakeTable.getDbName());
        Assert.assertEquals("tbl1", deltaLakeTable.getTableName());
        Assert.assertEquals(Lists.newArrayList("dt"), deltaLakeTable.getPartitionColumnNames());
        Column[] columns = new Column[] {
                new Column("dt", ScalarType.DEFAULT_STRING, true),
                new Column("id", ScalarType.INT, true),
                new Column("name", ScalarType.DEFAULT_STRING, true)
        };
        Assert.assertEquals(Lists.newArrayList(columns), deltaLakeTable.getBaseSchema());
        String path = Objects.requireNonNull(ResourceReader.class.getClassLoader()
                .getResource("data/delta/read_partition")).getPath();
        path = "file:" + path;
        Assert.assertEquals(path, deltaLakeTable.getTableLocation());
        Assert.assertEquals(ScalarType.DEFAULT_STRING, deltaLakeTable.getPartitionColumns().get(0).getType());
        Assert.assertEquals("delta_catalog", deltaLakeTable.getCatalogName());
    }

    public static class MockedDeltaHiveMetaClient extends HiveMetastoreTest.MockedHiveMetaClient {
        @Override
        public Table getTable(String dbName, String tblName) {
            String path = Objects.requireNonNull(ResourceReader.class.getClassLoader()
                    .getResource("data/delta/read_partition")).getPath();
            path = "file://" + path;
            List<FieldSchema> partKeys = Lists.newArrayList(new FieldSchema("dt", "String", ""));
            List<FieldSchema> unPartKeys = Lists.newArrayList(new FieldSchema("col", "String", ""));

            StorageDescriptor sd = new StorageDescriptor();
            sd.setLocation(path);
            sd.setCols(unPartKeys);
            sd.setInputFormat("io.delta.hive.DeltaInputFormat");
            SerDeInfo serDeInfo = new SerDeInfo();
            serDeInfo.setParameters(ImmutableMap.of());
            sd.setSerdeInfo(serDeInfo);
            Table msTable1 = new Table();

            msTable1.setPartitionKeys(partKeys);
            msTable1.setDbName(dbName);
            msTable1.setTableName(tblName);
            msTable1.setSd(sd);
            msTable1.setTableType("MANAGED_TABLE");
            msTable1.setParameters(ImmutableMap.of(ROW_COUNT, "50", TOTAL_SIZE, "100"));
            return msTable1;
        }
    }

}
