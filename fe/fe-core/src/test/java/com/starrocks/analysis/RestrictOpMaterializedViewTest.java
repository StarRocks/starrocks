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


package com.starrocks.analysis;

import com.google.common.collect.Maps;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.load.DeleteMgr;
import com.starrocks.load.routineload.KafkaRoutineLoadJob;
import com.starrocks.load.routineload.LoadDataSourceType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RestrictOpMaterializedViewTest {
    private static StarRocksAssert starRocksAssert;

    private static ConnectContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        // set default config for async mvs
        String createTblStmtStr =
                "CREATE TABLE tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');";
        String createMvStmtStr = "create materialized view if not exists mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh manual\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        ctx = UtFrameUtils.createDefaultCtx();

        // set default config for async mvs
        UtFrameUtils.setDefaultConfigForAsyncMVTest(ctx);

        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("db1").useDatabase("db1");
        starRocksAssert.withTable(createTblStmtStr);
        starRocksAssert.withMaterializedView(createMvStmtStr);

    }

    @Test
    public void testInsert() {
        String sql1 = "INSERT INTO db1.mv1\n" +
                "VALUES\n" +
                "  (\"2021-02-02\", \"1\");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql1, ctx);
            Assert.fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("the data of materialized view must be consistent with the base table"));
        }

    }

    @Test
    public void testInsertNormal() {
        String sql1 = "INSERT INTO db1.mv1\n" +
                "VALUES\n" +
                "  (\"2021-02-02\", \"1\");";
        StatementBase statementBase =
                com.starrocks.sql.parser.SqlParser.parse(sql1, ctx.getSessionVariable().getSqlMode()).get(0);
        InsertStmt insertStmt = (InsertStmt) statementBase;
        insertStmt.setSystem(true);
        try {
            com.starrocks.sql.analyzer.Analyzer.analyze(insertStmt, ctx);
        } catch (Exception e) {
            assertFalse(
                    e.getMessage().contains("the data of materialized view must be consistent with the base table"));
        }

    }

    @Test
    public void testDelete() {
        String sql1 = "delete from db1.mv1 where k2 = 3;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql1, ctx);
            DeleteMgr deleteHandler = new DeleteMgr();
            deleteHandler.process((DeleteStmt) statementBase);
            Assert.fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("the data of materialized view must be consistent with the base table"));
        }

    }

    @Test
    public void testUpdate() {
        String sql1 = "update db1.mv1 set k2 = 1 where k2 = 3;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql1, ctx);
            Assert.fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("the data of materialized view must be consistent with the base table"));
        }

    }

    @Test
    public void testBrokerLoad() {
        String sql1 = "LOAD LABEL db1.label0 (DATA INFILE('/path/file1') INTO TABLE mv1) with broker 'broker0';";
        try {
            LoadStmt loadStmt = (LoadStmt) com.starrocks.sql.parser.SqlParser.parse(sql1, ctx.getSessionVariable().getSqlMode()).get(0);
            Deencapsulation.setField(loadStmt, "label", new LabelName("db1", "mv1"));
            com.starrocks.sql.analyzer.Analyzer.analyze(loadStmt, ctx);
            Assert.fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("the data of materialized view must be consistent with the base table"));
        }
    }

    @Test
    public void testRoutineLoad() {
        LabelName labelName = new LabelName("db1", "job1");
        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(labelName, "mv1",
                new ArrayList<>(), Maps.newHashMap(),
                LoadDataSourceType.KAFKA.name(), Maps.newHashMap());

        Deencapsulation.setField(createRoutineLoadStmt, "dbName", "db1");

        try {
            KafkaRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
            Assert.fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("the data of materialized view must be consistent with the base table"));
        }
    }

    @Test
    public void testAlterTable() {
        String sql1 = "alter table db1.mv1 rename mv2;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql1, ctx);
            Assert.fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("is a materialized view,you can use 'ALTER MATERIALIZED VIEW' to alter it."));
        }
    }

    @Test
    public void testDropTable() {
        String sql1 = "drop table db1.mv1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql1, ctx);
            Assert.fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("is a materialized view,use 'drop materialized view mv1' to drop it."));
        }
    }

}

