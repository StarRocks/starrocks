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
import com.starrocks.catalog.Type;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.toResourceName;

public class ObjectBasedUpdateArbitratorTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @Before
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
        List<RemoteFileInfo> remoteFileInfos = createRemoteFileInfos(4);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                metadataMgr.getRemoteFiles((Table) any, (GetRemoteFilesParams) any);
                result = remoteFileInfos;
                minTimes = 0;
            }
        };

        String location = "oss://bucket_name/lineorder_part";
        HiveTable hiveTable = createHiveTable(location);
        List<String> partitionNames = Lists.newArrayList(
                "date=20240501", "date=20240502", "date=20240503", "date=20240504");
        TableUpdateArbitrator.UpdateContext updateContext =
                new TableUpdateArbitrator.UpdateContext(hiveTable, -1, partitionNames);
        TableUpdateArbitrator arbitrator = TableUpdateArbitrator.create(updateContext);
        Assert.assertTrue(arbitrator instanceof ObjectBasedUpdateArbitrator);
        Map<String, Optional<HivePartitionDataInfo>> hivePartitionDataInfo = arbitrator.getPartitionDataInfos();
        Assert.assertEquals(4, hivePartitionDataInfo.size());
        Assert.assertTrue(hivePartitionDataInfo.containsKey("date=20240501"));
        Assert.assertTrue(hivePartitionDataInfo.containsKey("date=20240502"));
        Assert.assertTrue(hivePartitionDataInfo.containsKey("date=20240503"));
        Assert.assertTrue(hivePartitionDataInfo.containsKey("date=20240504"));
    }

    @Test
    public void testGetPartitionDataInfosWithLimit(@Mocked MetadataMgr metadataMgr) {
        List<RemoteFileInfo> remoteFileInfos = createRemoteFileInfos(4);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                metadataMgr.getRemoteFiles((Table) any, (GetRemoteFilesParams) any);
                result = remoteFileInfos;
                minTimes = 0;
            }
        };

        String location = "oss://bucket_name/lineorder_part";
        HiveTable hiveTable = createHiveTable(location);
        List<String> partitionNames = Lists.newArrayList(
                "date=20240501", "date=20240502", "date=20240503", "date=20240504");
        {
            TableUpdateArbitrator.UpdateContext updateContext =
                    new TableUpdateArbitrator.UpdateContext(hiveTable, 2, partitionNames);
            TableUpdateArbitrator arbitrator = TableUpdateArbitrator.create(updateContext);
            Assert.assertTrue(arbitrator instanceof ObjectBasedUpdateArbitrator);
            Map<String, Optional<HivePartitionDataInfo>> hivePartitionDataInfo = arbitrator.getPartitionDataInfos();
            Assert.assertEquals(2, hivePartitionDataInfo.size());
            Assert.assertTrue(hivePartitionDataInfo.containsKey("date=20240503"));
            Assert.assertFalse(hivePartitionDataInfo.get("date=20240503").isPresent());
            Assert.assertTrue(hivePartitionDataInfo.containsKey("date=20240504"));
            Assert.assertTrue(hivePartitionDataInfo.get("date=20240504").isPresent());
        }

        {
            TableUpdateArbitrator.UpdateContext updateContext =
                    new TableUpdateArbitrator.UpdateContext(hiveTable, 10, partitionNames);
            TableUpdateArbitrator arbitrator = TableUpdateArbitrator.create(updateContext);
            Assert.assertTrue(arbitrator instanceof ObjectBasedUpdateArbitrator);
            Map<String, Optional<HivePartitionDataInfo>> hivePartitionDataInfo = arbitrator.getPartitionDataInfos();
            Assert.assertEquals(4, hivePartitionDataInfo.size());
            Assert.assertTrue(hivePartitionDataInfo.containsKey("date=20240501"));
            Assert.assertFalse(hivePartitionDataInfo.get("date=20240501").isPresent());
            Assert.assertTrue(hivePartitionDataInfo.containsKey("date=20240502"));
            Assert.assertTrue(hivePartitionDataInfo.get("date=20240502").isPresent());
            Assert.assertTrue(hivePartitionDataInfo.containsKey("date=20240503"));
            Assert.assertTrue(hivePartitionDataInfo.get("date=20240503").isPresent());
            Assert.assertTrue(hivePartitionDataInfo.containsKey("date=20240504"));
            Assert.assertTrue(hivePartitionDataInfo.get("date=20240504").isPresent());
        }
    }

    private List<RemoteFileInfo> createRemoteFileInfos(int num) {
        List<RemoteFileInfo> remoteFileInfos = Lists.newArrayList();
        long modificationTime = 100;
        for (int i = 0; i < num; i++) {
            List<RemoteFileDesc> remoteFileDescs = null;
            if (i != 0) {
                int fileDescNum = i + 1;
                remoteFileDescs = Lists.newArrayList();
                for (int j = 0; j < fileDescNum; j++) {
                    RemoteFileDesc fileDesc = new RemoteFileDesc("name", "gzip", 100, modificationTime + j, null);
                    remoteFileDescs.add(fileDesc);
                }
            }

            RemoteFileInfo remoteFileInfo = new RemoteFileInfo(RemoteFileInputFormat.PARQUET, remoteFileDescs, "fullpath");
            remoteFileInfos.add(remoteFileInfo);
        }
        return remoteFileInfos;
    }

    private HiveTable createHiveTable(String location) {
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
