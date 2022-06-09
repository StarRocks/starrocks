// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.SqlParserUtils;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.load.DeleteHandler;
import com.starrocks.load.routineload.KafkaRoutineLoadJob;
import com.starrocks.load.routineload.LoadDataSourceType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class RestrictOpMaterializedViewTest {
    private static StarRocksAssert starRocksAssert;

    private static ConnectContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();
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
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("db1").useDatabase("db1");
        starRocksAssert.withTable(createTblStmtStr);
        starRocksAssert.withNewMaterializedView(createMvStmtStr);

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
            assertEquals(e.getMessage(), "not support MaterializedView");
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
        insertStmt.setInsertToMvFromBaseTable(true);
        try {
            com.starrocks.sql.analyzer.Analyzer.analyze(insertStmt, ctx);
        } catch (Exception e) {
            assertNotEquals(e.getMessage(), "not support MaterializedView");
        }

    }

    @Test
    public void testDelete() {
        String sql1 = "delete from db1.mv1 where k2 = 3;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql1, ctx);
            DeleteHandler deleteHandler = new DeleteHandler();
            deleteHandler.process((DeleteStmt) statementBase);
            Assert.fail();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "not support MaterializedView");
        }

    }

    @Test
    public void testUpdate() {
        String sql1 = "update db1.mv1 set k2 = 1 where k2 = 3;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql1, ctx);
            Assert.fail();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "only support updating primary key table");
        }

    }

    @Test
    public void testBrokerLoad() {
        String sql1 = "LOAD LABEL label0 (DATA INFILE('/path/file1') INTO TABLE mv1) with broker 'broker0';";
        try {
            SqlParser parser = new SqlParser(new SqlScanner(new StringReader(sql1)));
            LoadStmt loadStmt = (LoadStmt) SqlParserUtils.getFirstStmt(parser);
            List<Expr> columnMappingList = Deencapsulation.getField(loadStmt.getDataDescriptions().get(0),
                    "columnMappingList");

            List<String> files = Lists.newArrayList("path/k2=1/file1");
            List<String> columnNames = Lists.newArrayList("ss", "k2");
            DataDescription desc = new DataDescription("mv1", null, files, columnNames,
                    null, null, "ORC", Lists.newArrayList("k2"),
                    false, columnMappingList, null);
            BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
            Database db1 = GlobalStateMgr.getCurrentState().getDb("default_cluster:db1");
            brokerFileGroup.parse(db1, desc);
            Assert.fail();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "not support MaterializedView");
        }
    }

    @Test
    public void testRoutineLoad() {
        LabelName labelName = new LabelName("default_cluster:db1", "job1");
        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(labelName, "mv1",
                new ArrayList<>(), Maps.newHashMap(),
                LoadDataSourceType.KAFKA.name(), Maps.newHashMap());

        Deencapsulation.setField(createRoutineLoadStmt, "dbName", "default_cluster:db1");

        try {
            KafkaRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
            Assert.fail();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "not support MaterializedView");
        }
    }

}

