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
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.toResourceName;

public class DirectoryBasedUpdateArbitratorTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withResource("create external resource 'hive0' PROPERTIES(" +
                "\"type\"  =  \"hive\", \"hive.metastore.uris\"  =  \"thrift://127.0.0.1:9083\")");
        starRocksAssert.withDatabase("db");
    }

    @Test
    public void testGetPartitionDataInfos(@Mocked MetadataMgr metadataMgr) {
        List<PartitionInfo> partitionInfoList = createRemotePartitions(4);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                metadataMgr.getRemotePartitions((Table) any, (List<String>) any);
                result = partitionInfoList;
                minTimes = 0;
            }
        };

        String location = "hdfs://path_to_file/lineorder_part";
        HiveTable hiveTable = createHiveTable(location);
        List<String> partitionNames = Lists.newArrayList(
                "date=20240501", "date=20240502", "date=20240503", "date=20240504");
        TableUpdateArbitrator.UpdateContext updateContext =
                new TableUpdateArbitrator.UpdateContext(hiveTable, -1, partitionNames);
        TableUpdateArbitrator arbitrator = TableUpdateArbitrator.create(updateContext);
        Assertions.assertTrue(arbitrator instanceof DirectoryBasedUpdateArbitrator);
        Map<String, Optional<HivePartitionDataInfo>> hivePartitionDataInfo = arbitrator.getPartitionDataInfos();
        Assertions.assertEquals(4, hivePartitionDataInfo.size());
        Assertions.assertTrue(hivePartitionDataInfo.containsKey("date=20240501"));
        Assertions.assertTrue(hivePartitionDataInfo.containsKey("date=20240502"));
        Assertions.assertTrue(hivePartitionDataInfo.containsKey("date=20240503"));
        Assertions.assertTrue(hivePartitionDataInfo.containsKey("date=20240504"));
    }

    @Test
    public void testGetPartitionDataInfosWithLimit(@Mocked MetadataMgr metadataMgr) {
        List<PartitionInfo> partitionInfoList = createRemotePartitions(4);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                metadataMgr.getRemotePartitions((Table) any, (List<String>) any);
                result = partitionInfoList;
                minTimes = 0;
            }
        };

        String location = "hdfs://path_to_file/lineorder_part";
        HiveTable hiveTable = createHiveTable(location);
        List<String> partitionNames = Lists.newArrayList(
                "date=20240501", "date=20240502", "date=20240503", "date=20240504");
        {
            TableUpdateArbitrator.UpdateContext updateContext =
                    new TableUpdateArbitrator.UpdateContext(hiveTable, 2, partitionNames);
            TableUpdateArbitrator arbitrator = TableUpdateArbitrator.create(updateContext);
            Assertions.assertTrue(arbitrator instanceof DirectoryBasedUpdateArbitrator);
            Map<String, Optional<HivePartitionDataInfo>> hivePartitionDataInfo = arbitrator.getPartitionDataInfos();
            Assertions.assertEquals(2, hivePartitionDataInfo.size());
            Assertions.assertTrue(hivePartitionDataInfo.containsKey("date=20240503"));
            Assertions.assertTrue(hivePartitionDataInfo.get("date=20240503").isPresent());
            Assertions.assertTrue(hivePartitionDataInfo.containsKey("date=20240504"));
            Assertions.assertTrue(hivePartitionDataInfo.get("date=20240504").isPresent());
        }

        {
            TableUpdateArbitrator.UpdateContext updateContext =
                    new TableUpdateArbitrator.UpdateContext(hiveTable, 10, partitionNames);
            TableUpdateArbitrator arbitrator = TableUpdateArbitrator.create(updateContext);
            Assertions.assertTrue(arbitrator instanceof DirectoryBasedUpdateArbitrator);
            Map<String, Optional<HivePartitionDataInfo>> hivePartitionDataInfo = arbitrator.getPartitionDataInfos();
            Assertions.assertEquals(4, hivePartitionDataInfo.size());
            Assertions.assertTrue(hivePartitionDataInfo.containsKey("date=20240501"));
            Assertions.assertTrue(hivePartitionDataInfo.get("date=20240501").isPresent());
            Assertions.assertTrue(hivePartitionDataInfo.containsKey("date=20240502"));
            Assertions.assertTrue(hivePartitionDataInfo.get("date=20240502").isPresent());
            Assertions.assertTrue(hivePartitionDataInfo.containsKey("date=20240503"));
            Assertions.assertTrue(hivePartitionDataInfo.get("date=20240503").isPresent());
            Assertions.assertTrue(hivePartitionDataInfo.containsKey("date=20240504"));
            Assertions.assertTrue(hivePartitionDataInfo.get("date=20240504").isPresent());
        }
    }

    private List<PartitionInfo> createRemotePartitions(int num) {
        List<PartitionInfo> partitionInfoList = Lists.newArrayList();
        long modificationTime = 100;
        for (int i = 0; i < num; i++) {
            final long time = modificationTime + i;
            partitionInfoList.add(new PartitionInfo() {
                @Override
                public long getModifiedTime() {
                    return time;
                }
            });
        }
        return partitionInfoList;
    }

    private HiveTable createHiveTable(String location) {
        List<Column> fullSchema = new ArrayList<>();
        Column columnId = new Column("id", IntegerType.INT, true);
        columnId.setComment("id");
        Column columnName = new Column("name", VarcharType.VARCHAR);
        Column columnYear = new Column("year", IntegerType.INT);
        Column columnDt = new Column("dt", IntegerType.INT);
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
