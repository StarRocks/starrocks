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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/alter/AlterTest.java

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

package com.starrocks.alter;

import com.google.common.collect.Lists;
import com.staros.proto.AwsCredentialInfo;
import com.staros.proto.AwsDefaultCredentialInfo;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.SharedNothingStorageVolumeMgr;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * @author xiaolong
 * @date 2024/02/06/17:18
 */
public class AlterLakeTableTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @Mocked
    private StarOSAgent agent;

    @Before
    public void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.alter_scheduler_interval_millisecond = 100;
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test");

        FilePathInfo.Builder builder = FilePathInfo.newBuilder();
        FileStoreInfo.Builder fsBuilder = builder.getFsInfoBuilder();

        S3FileStoreInfo.Builder s3FsBuilder = fsBuilder.getS3FsInfoBuilder();
        s3FsBuilder.setBucket("test-bucket");
        s3FsBuilder.setRegion("test-region");
        fsBuilder.setFsName("test-fsname");
        s3FsBuilder.setCredential(AwsCredentialInfo.newBuilder()
                .setDefaultCredential(AwsDefaultCredentialInfo.newBuilder().build()));
        S3FileStoreInfo s3FsInfo = s3FsBuilder.build();

        fsBuilder.setFsType(FileStoreType.S3);
        fsBuilder.setFsKey("test-bucket");
        fsBuilder.setS3FsInfo(s3FsInfo);
        FileStoreInfo fsInfo = fsBuilder.build();

        builder.setFsInfo(fsInfo);
        builder.setFullPath("s3://test-bucket/1/");
        FilePathInfo pathInfo = builder.build();

        new Expectations() {
            {
                agent.allocateFilePath(anyString, anyLong, anyLong);
                result = pathInfo;
                agent.createShardGroup(anyLong, anyLong, anyLong);
                result = GlobalStateMgr.getCurrentState().getNextId();
                agent.createShards(anyInt, (FilePathInfo) any, (FileCacheInfo) any, anyLong, (Map<String, String>) any);
                returns(Lists.newArrayList(30001L, 30002L, 30003L),
                        Lists.newArrayList(30004L, 30005L, 30006L),
                        Lists.newArrayList(30007L, 30008L, 30009L),
                        Lists.newArrayList(30010L, 30011L, 30012L),
                        Lists.newArrayList(30013L, 30014L, 30015L),
                        Lists.newArrayList(30016L, 30017L, 30018L));
                agent.getPrimaryComputeNodeIdByShard(anyLong, anyLong);
                result = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds(true).get(0);
            }
        };

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        new MockUp<SharedNothingStorageVolumeMgr>() {
            @Mock
            public StorageVolume getStorageVolumeByName(String svName) throws AnalysisException {
                return StorageVolume.fromFileStoreInfo(fsInfo);
            }

            @Mock
            public StorageVolume getStorageVolume(String svKey) throws AnalysisException {
                return StorageVolume.fromFileStoreInfo(fsInfo);
            }

            @Mock
            public String getStorageVolumeIdOfTable(long tableId) {
                return fsInfo.getFsKey();
            }
        };

        Deencapsulation.setField(GlobalStateMgr.getCurrentState(), "starOSAgent", agent);
    }


    @Test
    public void testAddPartitionForLakeTable() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table if exists test_lake_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        String createSQL = "CREATE TABLE test.test_lake_partition (\n" +
                "      k1 DATE,\n" +
                "      k2 INT,\n" +
                "      k3 SMALLINT,\n" +
                "      v1 VARCHAR(2048),\n" +
                "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2, k3)\n" +
                "PARTITION BY RANGE (k1, k2, k3) (\n" +
                "    PARTITION p1 VALUES [(\"2014-01-01\", \"10\", \"200\"), (\"2014-01-01\", \"20\", \"300\")),\n" +
                "    PARTITION p2 VALUES [(\"2014-06-01\", \"100\", \"200\"), (\"2014-07-01\", \"100\", \"300\"))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "   \"datacache.enable\" = \"true\"\n" +
                ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getDb("test");

        String alterSQL = "ALTER TABLE test_lake_partition ADD\n" +
                "    PARTITION p3 VALUES LESS THAN (\"2014-01-01\")";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getOps().get(0);
        GlobalStateMgr.getCurrentState().getLocalMetastore().addPartitions(db, "test_lake_partition", addPartitionClause);

        Table table = GlobalStateMgr.getCurrentState().getDb("test")
                .getTable("test_lake_partition");

        Assert.assertNotNull(table.getPartition("p1"));
        Assert.assertNotNull(table.getPartition("p2"));
        Assert.assertNotNull(table.getPartition("p3"));

        dropSQL = "drop table test_lake_partition";
        dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    @Test
    public void testMultiRangePartitionForLakeTable() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table if exists site_access";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        String createSQL = "CREATE TABLE site_access (\n" +
                "    datekey INT,\n" +
                "    site_id INT,\n" +
                "    city_code SMALLINT,\n" +
                "    user_name VARCHAR(32),\n" +
                "    pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "DUPLICATE KEY(datekey, site_id, city_code, user_name)\n" +
                "PARTITION BY RANGE (datekey) (\n" +
                "    START (\"1\") END (\"5\") EVERY (1)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(site_id) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getDb("test");

        String alterSQL = "ALTER TABLE site_access \n" +
                "   ADD PARTITIONS START (\"7\") END (\"9\") EVERY (1)";

        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getOps().get(0);
        GlobalStateMgr.getCurrentState().getLocalMetastore().addPartitions(db, "site_access", addPartitionClause);

        Table table = GlobalStateMgr.getCurrentState().getDb("test")
                .getTable("site_access");

        Assert.assertNotNull(table.getPartition("p1"));
        Assert.assertNotNull(table.getPartition("p2"));
        Assert.assertNotNull(table.getPartition("p3"));
        Assert.assertNotNull(table.getPartition("p4"));
        Assert.assertNotNull(table.getPartition("p7"));
        Assert.assertNotNull(table.getPartition("p8"));

        dropSQL = "drop table site_access";
        dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    @Test
    public void testAlterTableCompactionForLakeTable() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table if exists test_lake_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        String createSQL = "CREATE TABLE test.t1 (\n" +
                "      k1 DATE,\n" +
                "      k2 INT,\n" +
                "      k3 SMALLINT,\n" +
                "      v1 VARCHAR(2048),\n" +
                "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2, k3)\n" +
                "PARTITION BY RANGE (k1, k2, k3) (\n" +
                "    PARTITION p1 VALUES [(\"2014-01-01\", \"10\", \"200\"), (\"2014-01-01\", \"20\", \"300\")),\n" +
                "    PARTITION p2 VALUES [(\"2014-06-01\", \"100\", \"200\"), (\"2014-07-01\", \"100\", \"300\"))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "   \"datacache.enable\" = \"true\"\n" +
                ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);

        String sql = "ALTER TABLE t1 COMPACT p1";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(alterTableStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
