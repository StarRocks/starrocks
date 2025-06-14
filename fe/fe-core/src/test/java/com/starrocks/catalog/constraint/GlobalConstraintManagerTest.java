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

package com.starrocks.catalog.constraint;

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Set;

public class GlobalConstraintManagerTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.alter_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.enable_strict_storage_medium_check = false;
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
    }

    @Test
    public void testUnRegisterFK1() throws Exception {
        String s1 = "CREATE TABLE test.s1 \n" +
                "(\n" +
                "    k1 int, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\", 'unique_constraints'='test.s1.k1');";
        String s2 = "CREATE TABLE test.s2 \n" +
                "(\n" +
                "    k1 int, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\", 'foreign_key_constraints'='s2(k1) REFERENCES s1(k1)');";
        String s3 = "CREATE TABLE test.s3 \n" +
                "(\n" +
                "    k1 int, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\", 'foreign_key_constraints'='s3(k1) REFERENCES s1(k1)');";
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        // test global constraint manager
        GlobalConstraintManager cm = GlobalStateMgr.getCurrentState().getGlobalConstraintManager();
        Assert.assertTrue(cm != null);

        createTable(s1);
        OlapTable tbl1 = (OlapTable) db.getTable("s1");
        List<UniqueConstraint> uk1 = tbl1.getUniqueConstraints();
        Assert.assertEquals(1, uk1.size());
        UniqueConstraint uk10 = uk1.get(0);
        Assert.assertEquals("s1", uk10.getTableName());
        // s1 has no fk constraints
        Assert.assertTrue(cm.getRefConstraints(tbl1).isEmpty());

        createTable(s2);
        OlapTable tbl2 = (OlapTable) db.getTable("s2");
        List<ForeignKeyConstraint> fk2 = tbl2.getForeignKeyConstraints();
        Assert.assertEquals(1, fk2.size());
        ForeignKeyConstraint fk20 = fk2.get(0);
        BaseTableInfo baseTableInfo20 = fk20.getChildTableInfo();
        Assert.assertTrue(baseTableInfo20 == null);
        BaseTableInfo parentTableInfo = fk20.getParentTableInfo();
        Assert.assertTrue(parentTableInfo != null);
        Assert.assertEquals("s1", parentTableInfo.getTableName());
        Assert.assertEquals(tbl1.getId(), parentTableInfo.getTableId());
        // constraint manager contains one constraint
        Set<TableWithFKConstraint> tableWithFKConstraintSet = cm.getRefConstraints(tbl1);
        Assert.assertTrue(tableWithFKConstraintSet != null);
        Assert.assertTrue(tableWithFKConstraintSet.size() == 1);
        Assert.assertTrue(tableWithFKConstraintSet.contains(TableWithFKConstraint.of(tbl2, fk20)));

        createTable(s3);
        OlapTable tbl3 = (OlapTable) db.getTable("s3");
        List<ForeignKeyConstraint> fk3 = tbl3.getForeignKeyConstraints();
        Assert.assertEquals(1, fk3.size());
        ForeignKeyConstraint fk30 = fk3.get(0);
        BaseTableInfo baseTableInfo30 = fk30.getChildTableInfo();
        Assert.assertTrue(baseTableInfo30 == null);
        parentTableInfo = fk30.getParentTableInfo();
        Assert.assertTrue(parentTableInfo != null);
        Assert.assertEquals("s1", parentTableInfo.getTableName());
        Assert.assertEquals(tbl1.getId(), parentTableInfo.getTableId());
        // constraint manager contains two constraints
        tableWithFKConstraintSet = cm.getRefConstraints(tbl1);
        Assert.assertTrue(tableWithFKConstraintSet != null);
        Assert.assertTrue(tableWithFKConstraintSet.size() == 2);
        Assert.assertTrue(tableWithFKConstraintSet.contains(TableWithFKConstraint.of(tbl2, fk20)));
        Assert.assertTrue(tableWithFKConstraintSet.contains(TableWithFKConstraint.of(tbl3, fk30)));

        starRocksAssert.dropTable("s2");
        tableWithFKConstraintSet = cm.getRefConstraints(tbl1);
        Assert.assertTrue(tableWithFKConstraintSet != null);
        Assert.assertTrue(tableWithFKConstraintSet.size() == 1);
        Assert.assertFalse(tableWithFKConstraintSet.contains(TableWithFKConstraint.of(tbl2, fk20)));
        Assert.assertTrue(tableWithFKConstraintSet.contains(TableWithFKConstraint.of(tbl3, fk30)));

        starRocksAssert.dropTable("s3");
        tableWithFKConstraintSet = cm.getRefConstraints(tbl1);
        Assert.assertTrue(tableWithFKConstraintSet.isEmpty());

        starRocksAssert.dropTable("s1");
    }

    @Test
    public void testUnRegisterFK2() throws Exception {
        String s1 = "CREATE TABLE test.s1 \n" +
                "(\n" +
                "    k1 int, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\", 'unique_constraints'='test.s1.k1');";
        String s2 = "CREATE TABLE test.s2 \n" +
                "(\n" +
                "    k1 int, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\", 'foreign_key_constraints'='s2(k1) REFERENCES s1(k1)');";
        String s3 = "CREATE TABLE test.s3 \n" +
                "(\n" +
                "    k1 int, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\", 'foreign_key_constraints'='s3(k1) REFERENCES s1(k1)');";
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        // test global constraint manager
        GlobalConstraintManager cm = GlobalStateMgr.getCurrentState().getGlobalConstraintManager();
        Assert.assertTrue(cm != null);

        createTable(s1);
        OlapTable tbl1 = (OlapTable) db.getTable("s1");
        List<UniqueConstraint> uk1 = tbl1.getUniqueConstraints();
        Assert.assertEquals(1, uk1.size());
        UniqueConstraint uk10 = uk1.get(0);
        Assert.assertEquals("s1", uk10.getTableName());
        // s1 has no fk constraints
        Assert.assertTrue(cm.getRefConstraints(tbl1).isEmpty());

        createTable(s2);
        OlapTable tbl2 = (OlapTable) db.getTable("s2");
        List<ForeignKeyConstraint> fk2 = tbl2.getForeignKeyConstraints();
        Assert.assertEquals(1, fk2.size());
        ForeignKeyConstraint fk20 = fk2.get(0);
        BaseTableInfo baseTableInfo20 = fk20.getChildTableInfo();
        Assert.assertTrue(baseTableInfo20 == null);
        BaseTableInfo parentTableInfo = fk20.getParentTableInfo();
        Assert.assertTrue(parentTableInfo != null);
        Assert.assertEquals("s1", parentTableInfo.getTableName());
        Assert.assertEquals(tbl1.getId(), parentTableInfo.getTableId());
        // constraint manager contains one constraint
        Set<TableWithFKConstraint> tableWithFKConstraintSet = cm.getRefConstraints(tbl1);
        Assert.assertTrue(tableWithFKConstraintSet != null);
        Assert.assertTrue(tableWithFKConstraintSet.size() == 1);
        Assert.assertTrue(tableWithFKConstraintSet.contains(TableWithFKConstraint.of(tbl2, fk20)));

        createTable(s3);
        OlapTable tbl3 = (OlapTable) db.getTable("s3");
        List<ForeignKeyConstraint> fk3 = tbl3.getForeignKeyConstraints();
        Assert.assertEquals(1, fk3.size());
        ForeignKeyConstraint fk30 = fk3.get(0);
        BaseTableInfo baseTableInfo30 = fk30.getChildTableInfo();
        Assert.assertTrue(baseTableInfo30 == null);
        parentTableInfo = fk30.getParentTableInfo();
        Assert.assertTrue(parentTableInfo != null);
        Assert.assertEquals("s1", parentTableInfo.getTableName());
        Assert.assertEquals(tbl1.getId(), parentTableInfo.getTableId());
        // constraint manager contains two constraints
        tableWithFKConstraintSet = cm.getRefConstraints(tbl1);
        Assert.assertTrue(tableWithFKConstraintSet != null);
        Assert.assertTrue(tableWithFKConstraintSet.size() == 2);
        Assert.assertTrue(tableWithFKConstraintSet.contains(TableWithFKConstraint.of(tbl2, fk20)));
        Assert.assertTrue(tableWithFKConstraintSet.contains(TableWithFKConstraint.of(tbl3, fk30)));

        starRocksAssert.dropTable("s1");
        // since parent is dropped, child's fk constraints should be removed
        fk2 = tbl2.getForeignKeyConstraints();
        Assert.assertTrue(CollectionUtils.isEmpty(fk2));
        fk3 = tbl3.getForeignKeyConstraints();
        Assert.assertTrue(CollectionUtils.isEmpty(fk3));

        starRocksAssert.dropTable("s2");
        starRocksAssert.dropTable("s3");
    }
}
