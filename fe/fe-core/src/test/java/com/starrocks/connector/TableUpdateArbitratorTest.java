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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/qe/ShowExecutorTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.connector;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Type;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.toResourceName;

public class TableUpdateArbitratorTest {
    @Test
    public void testHiveOnHdfs() {
        String location = "hdfs://hadoop/hive/warehouse/test.db/test";
        HiveTable hiveTable = createHiveTable(location);
        List<String> partitionNames = Lists.newArrayList();
        TableUpdateArbitrator.UpdateContext updateContext =
                new TableUpdateArbitrator.UpdateContext(hiveTable, -1, partitionNames);
        TableUpdateArbitrator arbitrator = TableUpdateArbitrator.create(updateContext);
        Assert.assertTrue(arbitrator instanceof DirectoryBasedUpdateArbitrator);
    }

    @Test
    public void testHiveOnOss() {
        String location = "oss://bucket_name/lineorder_part";
        HiveTable hiveTable = createHiveTable(location);
        List<String> partitionNames = Lists.newArrayList();
        TableUpdateArbitrator.UpdateContext updateContext =
                new TableUpdateArbitrator.UpdateContext(hiveTable, -1, partitionNames);
        TableUpdateArbitrator arbitrator = TableUpdateArbitrator.create(updateContext);
        Assert.assertTrue(arbitrator instanceof ObjectBasedUpdateArbitrator);
    }

    @Test
    public void testHiveOnS3() {
        String location = "s3://bucket_name/lineorder_part";
        HiveTable hiveTable = createHiveTable(location);
        List<String> partitionNames = Lists.newArrayList();
        TableUpdateArbitrator.UpdateContext updateContext =
                new TableUpdateArbitrator.UpdateContext(hiveTable, -1, partitionNames);
        TableUpdateArbitrator arbitrator = TableUpdateArbitrator.create(updateContext);
        Assert.assertTrue(arbitrator instanceof ObjectBasedUpdateArbitrator);
    }

    HiveTable createHiveTable(String location) {
        List<Column> fullSchema = new ArrayList<>();
        Column columnId = new Column("id", Type.INT, true);
        columnId.setComment("id");
        Column columnName = new Column("name", Type.VARCHAR);
        Column columnYear = new Column("year", Type.INT);
        Column columnDt = new Column("dt", Type.INT);
        fullSchema.add(columnId);
        fullSchema.add(columnName);
        fullSchema.add(columnYear);
        fullSchema.add(columnDt);
        List<String> partitions = Lists.newArrayList();
        partitions.add("year");
        partitions.add("dt");
        HiveTable.Builder tableBuilder = HiveTable.builder()
                .setId(1)
                .setTableName("test_table")
                .setCatalogName("hive_catalog")
                .setResourceName(toResourceName("hive_catalog", "hive"))
                .setHiveDbName("hive_db")
                .setHiveTableName("test_table")
                .setPartitionColumnNames(partitions)
                .setFullSchema(fullSchema)
                .setTableLocation(location)
                .setCreateTime(10000)
                .setHiveTableType(HiveTable.HiveTableType.EXTERNAL_TABLE);
        HiveTable hiveTable = tableBuilder.build();
        return hiveTable;
    }
}
