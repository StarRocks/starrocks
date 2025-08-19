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

package com.starrocks.lake;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.Util;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AlterTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");
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
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        String alterSQL = "ALTER TABLE test_lake_partition ADD\n" +
                    "    PARTITION p3 VALUES LESS THAN (\"2014-01-01\")";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getAlterClauseList().get(0);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPartitions(Util.getOrCreateInnerContext(), db, "test_lake_partition", addPartitionClause);

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test")
                    .getTable("test_lake_partition");

        Assertions.assertNotNull(table.getPartition("p1"));
        Assertions.assertNotNull(table.getPartition("p2"));
        Assertions.assertNotNull(table.getPartition("p3"));

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
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        String alterSQL = "ALTER TABLE site_access \n" +
                    "   ADD PARTITIONS START (\"7\") END (\"9\") EVERY (1)";

        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getAlterClauseList().get(0);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPartitions(Util.getOrCreateInnerContext(), db, "site_access", addPartitionClause);

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test")
                    .getTable("site_access");

        Assertions.assertNotNull(table.getPartition("p1"));
        Assertions.assertNotNull(table.getPartition("p2"));
        Assertions.assertNotNull(table.getPartition("p3"));
        Assertions.assertNotNull(table.getPartition("p4"));
        Assertions.assertNotNull(table.getPartition("p7"));
        Assertions.assertNotNull(table.getPartition("p8"));

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
            GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, alterTableStmt);
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail();
        }
    }

    @Test
    public void testAlterWarehouse() throws Exception {
        Exception e = Assertions.assertThrows(DdlException.class, () ->
                starRocksAssert.ddl("alter warehouse default_warehouse set ('compute_replica'='2')")
        );
        Assertions.assertEquals("Multi-Warehouse is not implemented", e.getMessage());
    }
}
