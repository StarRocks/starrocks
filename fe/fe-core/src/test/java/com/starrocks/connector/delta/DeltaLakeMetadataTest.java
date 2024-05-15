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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.connector.hive.HiveMetastoreTest;
import com.starrocks.connector.metastore.IMetastore;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class DeltaLakeMetadataTest {
    private HiveMetaClient client;
    private DeltaLakeMetadata deltaLakeMetadata;

    @Before
    public void setUp() throws Exception {
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(Maps.newHashMap());

        client = new HiveMetastoreTest.MockedHiveMetaClient();
        IMetastore hiveMetastore = new HiveMetastore(client, "delta0", MetastoreType.HMS);
        DeltaMetastoreOperations deltaOps = new DeltaMetastoreOperations("delta0", hiveMetastore, false,
                null, MetastoreType.HMS);

        deltaLakeMetadata = new DeltaLakeMetadata(hdfsEnvironment, "delta0", deltaOps);
    }

    @Test
    public void testListDbNames() {
        List<String> dbNames = deltaLakeMetadata.listDbNames();
        Assert.assertEquals(2, dbNames.size());
        Assert.assertEquals("db1", dbNames.get(0));
        Assert.assertEquals("db2", dbNames.get(1));
    }

    @Test
    public void testListTableNames() {
        List<String> tableNames = deltaLakeMetadata.listTableNames("db1");
        Assert.assertEquals(2, tableNames.size());
        Assert.assertEquals("table1", tableNames.get(0));
        Assert.assertEquals("table2", tableNames.get(1));
    }

    @Test
    public void testListPartitionNames() {
        List<String> partitionNames = deltaLakeMetadata.listPartitionNames("db1", "table1", -1);
        Assert.assertEquals(1, partitionNames.size());
        Assert.assertEquals("col1", partitionNames.get(0));
    }

    @Test
    public void testTableExists() {
        Assert.assertTrue(deltaLakeMetadata.tableExists("db1", "table1"));
    }

    @Test
    public void testGetTable() {
        new MockUp<DeltaUtils>() {
            @mockit.Mock
            public DeltaLakeTable convertDeltaToSRTable(String catalog, String dbName, String tblName, String path,
                                                        Configuration configuration, long createTime) {
                return new DeltaLakeTable(1, "delta0", "db1", "table1", Lists.newArrayList(),
                        Lists.newArrayList("col1"), null, "path/to/table", null, 0);
            }
        };
        DeltaLakeTable deltaTable = (DeltaLakeTable) deltaLakeMetadata.getTable("db1", "table1");
        Assert.assertNotNull(deltaTable);
        Assert.assertEquals("table1", deltaTable.getName());
        Assert.assertEquals(Table.TableType.DELTALAKE, deltaTable.getType());
        Assert.assertEquals("path/to/table", deltaTable.getTableLocation());
    }
}
