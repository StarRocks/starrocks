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

package com.starrocks.alter;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.thrift.TPersistentIndexType;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class LakeTableDropPersistentIndexTest {
    private static final Logger LOG = LogManager.getLogger(LakeTableDropPersistentIndexTest.class);

    private static ConnectContext connectContext;
    private static final String DB_NAME = "test_lake_drop_pindex";

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        String createDbStmtStr = "create database " + DB_NAME;
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        connectContext.setDatabase(DB_NAME);
    }

    private static LakeTable createTable(ConnectContext connectContext, String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(createTableStmt.getDbName());
        LakeTable table = (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), createTableStmt.getTableName());
        table.addRelatedMaterializedView(new MvId(db.getId(), GlobalStateMgr.getCurrentState().getNextId()));
        return table;
    }


    @Test
    public void test() throws Exception {
        LakeTable table = createTable(connectContext, "CREATE TABLE t0(c0 INT, c1 INT) PRIMARY KEY(c0) DISTRIBUTED BY HASH(c0) " +
                    "BUCKETS 2 PROPERTIES('persistent_index_type'='LOCAL')");
        
        List<Tablet> tablets = Lists.newArrayList();
        List<Partition> partitions = Lists.newArrayList();
        List<PhysicalPartition> physicalPartitions = Lists.newArrayList();
        partitions.addAll(table.getPartitions());
        for (Partition partition : table.getPartitions()) {
            physicalPartitions.addAll(partition.getSubPartitions());
            for (PhysicalPartition physicalPartition : physicalPartitions) {
                for (MaterializedIndex index : physicalPartition.getMaterializedIndices(
                                MaterializedIndex.IndexExtState.VISIBLE)) {
                    tablets.addAll(index.getTablets());
                }
            }
        }
        Assert.assertTrue(physicalPartitions.size() == 1);
        Assert.assertTrue(partitions.size() == 1);
        Assert.assertTrue(tablets.size() == 2);

        {
            String sql = "ALTER TABLE t0 DROP PERSISTENT INDEX ON TABLETS (2333)";
            AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            try {
                GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, stmt);
            } catch (Exception e) {
                // do nothing
            }
        }

        {
            table.setPersistentIndexType(TPersistentIndexType.CLOUD_NATIVE);
            String sql = "ALTER TABLE t0 DROP PERSISTENT INDEX ON TABLETS (2333)";
            AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            try {
                GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, stmt);
            } catch (Exception e) {
                // do nothing
            }
        }

        {
            String sql = "ALTER TABLE t0 DROP PERSISTENT INDEX ON TABLETS (" + String.valueOf(tablets.get(0).getId()) + ")";
            AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            try {
                GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, stmt);
                long visibleVersion = physicalPartitions.get(0).getVisibleVersion();
                Assert.assertTrue(((LakeTablet) tablets.get(0)).rebuildPindexVersion() == visibleVersion);
                Assert.assertTrue(((LakeTablet) tablets.get(1)).rebuildPindexVersion() == 0);

                physicalPartitions.get(0).setVisibleVersion(10, 1000);
                sql = "ALTER TABLE t0 DROP PERSISTENT INDEX ON TABLETS (" + String.valueOf(tablets.get(1).getId()) + ")";
                stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
                GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, stmt);
                long rebuildVersion = ((LakeTablet) tablets.get(0)).rebuildPindexVersion();
                LOG.info("tablet {} rebuildPindexVersion: {}, visible version: {}", tablets.get(0).getId(),
                        rebuildVersion, visibleVersion);
                Assert.assertTrue(((LakeTablet) tablets.get(0)).rebuildPindexVersion() == visibleVersion);
                Assert.assertTrue(((LakeTablet) tablets.get(1)).rebuildPindexVersion() == 10);

            } catch (Exception e) {
                // do nothing
                Assert.assertTrue(false);
            }
        }

    }

}