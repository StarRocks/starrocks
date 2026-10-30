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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.common.DdlException;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.TestWithFeService;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.List;
import java.util.Map;
import java.util.Set;

@TestMethodOrder(MethodName.class)
public class SchemaChangeHandlerWithMVTest extends TestWithFeService {

    private static final Logger LOG = LogManager.getLogger(SchemaChangeHandlerWithMVTest.class);
    private int jobSize = 0;
    private StarRocksAssert starRocksAssert;

    @Override
    public void runBeforeEach() throws Exception {
        String createDupTbl2StmtStr = "CREATE TABLE IF NOT EXISTS sc_dup3 (\n" + "timestamp DATETIME,\n"
                + "type INT,\n" + "error_code INT,\n" + "error_msg VARCHAR(1024),\n" + "op_id BIGINT,\n"
                + "op_time DATETIME)\n" + "DUPLICATE  KEY(timestamp, type)\n" + "DISTRIBUTED BY HASH(type) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'fast_schema_evolution' = 'true');";
        starRocksAssert.withTable(createDupTbl2StmtStr);
    }

    @Override
    public void runAfterEach() throws Exception {
        try {
            starRocksAssert.dropTable("sc_dup3");
        } catch (Exception e) {
            // ignore
        }
    }

    @Override
    protected void runBeforeAll() throws Exception {
        //create database db1
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test");
        starRocksAssert.useDatabase("test");

        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);

        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }
    }

    @Override
    protected void createStarrocksCluster() {
        UtFrameUtils.createMinStarRocksCluster(false, runMode);
    }

    private void waitAlterJobDone(Map<Long, AlterJobV2> alterJobs) throws Exception {
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                LOG.info("alter job {} is running. state: {}", alterJobV2.getJobId(), alterJobV2.getJobState());
                Thread.sleep(500);
            }
            LOG.info("alter job {} is done. state: {}", alterJobV2.getJobId(), alterJobV2.getJobState());
            Assertions.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());

            OlapTable tbl = (OlapTable) starRocksAssert.getTable("test", alterJobV2.tableName);
            while (tbl.getState() != OlapTableState.NORMAL) {
                Thread.sleep(500);
            }
        }
    }
    private void checkModifyColumnsWithMaterializedViews(StarRocksAssert starRocksAssert,
                                                         String mv, String alterColumn) throws Exception {
        checkModifyColumnsWithMaterializedViews(starRocksAssert, mv, alterColumn, true);
    }

    private void checkModifyColumnsWithMaterializedViews(StarRocksAssert starRocksAssert,
                                                         String mv, String alterColumn,
                                                         boolean isDropMV) throws Exception {
        String mvName = starRocksAssert.getMVName(mv);
        try {
            starRocksAssert.withRefreshedMaterializedView(mv);

            AlterTableStmt dropValColStm = (AlterTableStmt) parseAndAnalyzeStmt(alterColumn);
            DDLStmtExecutor.execute(dropValColStm, connectContext);
            Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
            waitAlterJobDone(alterJobs);
        } finally {
            if (isDropMV) {
                try {
                    starRocksAssert.dropMaterializedView(mvName);
                } catch (Exception e) {
                    // ignore.
                }
            }
        }
    }

    @Test
    public void testModifyColumnsWithNoAggregateRollup1() {
        try {
            checkModifyColumnsWithMaterializedViews(starRocksAssert,
                    "create materialized view mv1 as select error_code, error_msg, " +
                            "timestamp from sc_dup3",
                    "alter table sc_dup3 drop column op_id");
            jobSize++;
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testModifyColumnsWithNoAggregateRollup2() {
        try {
            checkModifyColumnsWithMaterializedViews(starRocksAssert,
                    "create materialized view mv1 as select error_code, error_msg, timestamp from sc_dup3 ",
                    "alter table sc_dup3 drop column error_code");
            jobSize++;
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testModifyColumnsWithNoAggregateRollup3() {
        // drop column without aggregate
        // OK: associated column with modify column
        try {
            checkModifyColumnsWithMaterializedViews(starRocksAssert,
                    "create materialized view mv1 as select error_code, error_msg, timestamp from sc_dup3",
                    "alter table sc_dup3 modify column error_code BIGINT");
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testModifyColumnsWithNoAggregateRollup4() throws Exception {
        try {
            checkModifyColumnsWithMaterializedViews(starRocksAssert,
                    "create materialized view mv1 as select error_code, error_msg, timestamp from sc_dup3 " +
                            "where type > 10",
                    "alter table sc_dup3 modify column error_code BIGINT");
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testModifyColumnsWithNoAggregateRollup5() {
        try {
            checkModifyColumnsWithMaterializedViews(starRocksAssert,
                    "create materialized view mv1 as select error_code, error_msg, timestamp from sc_dup3 " +
                            "where type > 10",
                    "alter table sc_dup3 modify column type BIGINT");
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Can not drop/modify the column type, because the column is used " +
                    "in the related rollup mv1 with the where expr:`test`.`sc_dup3`.`type` > 10, " +
                    "please drop the rollup index first."));
        }
    }

    @Test
    public void testModifyColumnsWithAggregateRollup1() {
        // drop column with aggregate: no associated column
        try {
            checkModifyColumnsWithMaterializedViews(starRocksAssert,
                    "create materialized view mv1 as select timestamp, count(error_code), sum(type) from sc_dup3 " +
                            "group by timestamp",
                    "alter table sc_dup3 drop column op_id");
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Can not drop/modify the column timestamp, because the column " +
                    "is used in the related rollup mv1, please drop the rollup index first."),
                    e.getMessage());
        }
    }

    @Test
    public void testModifyColumnsWithAggregateRollup2() {
        try {
            checkModifyColumnsWithMaterializedViews(starRocksAssert,
                    "create materialized view mv1 as select timestamp, count(error_code), sum(type) from sc_dup3 " +
                            "group by timestamp",
                    "alter table sc_dup3 drop column timestamp");
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Can not drop/modify the column timestamp, because the column " +
                    "is used in the related rollup mv1, please drop the rollup index first."));
        }
    }

    @Test
    public void testModifyColumnsWithAggregateRollup3() {
        // drop column with aggregate: no associated column
        try {
            checkModifyColumnsWithMaterializedViews(starRocksAssert,
                    "create materialized view mv1 as select timestamp, count(error_code), sum(type) from sc_dup3 " +
                            "group by timestamp",
                    "alter table sc_dup3 drop column error_code");
        } catch (Exception e) {
            Assertions.assertTrue(
                    e.getMessage().contains("Can not drop/modify the column error_code, " +
                            "because the column is used in the related rollup mv1 with the define exp"), e.getMessage());
        }
    }

    @Test
    public void testModifyColumnsWithAggregateRollup4() {
        // drop column with aggregate: no associated column
        try {
            checkModifyColumnsWithMaterializedViews(starRocksAssert,
                    "create materialized view mv1 as select timestamp, count(error_code), sum(type) from sc_dup3 " +
                            "group by timestamp",
                    "alter table sc_dup3 modify column error_code BIGINT");
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Can not drop/modify the column error_code, because " +
                    "the column is used in the related rollup mv1 with the define expr"), e.getMessage());
        }
    }

    @Test
    public void testModifyColumnsWithAggregateRollup5() {
        try {
            checkModifyColumnsWithMaterializedViews(starRocksAssert,
                    "create materialized view mv1 as select timestamp, count(error_code)  from sc_dup3 " +
                            "where type > 10 group by timestamp",
                    "alter table sc_dup3 modify column type BIGINT");
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Can not drop/modify the column type, because the column is " +
                    "used in the related rollup mv1 with the where expr:`test`.`sc_dup3`.`type` > 10, " +
                    "please drop the rollup index first."));
        }
    }

    @Test
    public void testModifyColumnsWithAggregateRollup6() {
        // drop column with aggregate: no associated column
        try {
            checkModifyColumnsWithMaterializedViews(starRocksAssert,
                    "create materialized view mv1 as select timestamp, count(error_code) from sc_dup3 " +
                            "where type > 10 group by timestamp",
                    "alter table sc_dup3 drop column type");
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Can not drop/modify the column type, because the column is " +
                    "used in the related rollup mv1 with the where expr:`test`.`sc_dup3`.`type` > 10, " +
                    "please drop the rollup index first."));
        }
    }

    @Test
    public void testModifyColumnsWithAMV1() {
        try {
            checkModifyColumnsWithMaterializedViews(starRocksAssert,
                    "create materialized view mv1 distributed by random refresh deferred manual " +
                            "as select timestamp, count(error_code) from sc_dup3 " +
                            "where op_id > 10 group by timestamp",
                    "alter table sc_dup3 drop column op_id",
                    false);
            MaterializedView mv = (MaterializedView) starRocksAssert.getTable("test", "mv1");
            Assertions.assertFalse(mv.isActive());
            Assertions.assertTrue(mv.getInactiveReason().contains("base table schema changed for columns: op_id"));
        } catch (Exception e) {
            Assertions.fail();
        } finally {
            try {
                starRocksAssert.dropMaterializedView("mv1");
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Test
    public void testModifyColumnsWithAMV2() {
        try {
            checkModifyColumnsWithMaterializedViews(starRocksAssert,
                    "create materialized view mv1 distributed by random refresh deferred manual " +
                            "as select timestamp, count(error_code) as cnt from sc_dup3 " +
                            "where op_id * 2 > 10 group by timestamp",
                    "alter table sc_dup3 drop column error_code",
                    false);
            MaterializedView mv = (MaterializedView) starRocksAssert.getTable("test", "mv1");
            Assertions.assertFalse(mv.isActive());
            Assertions.assertTrue(mv.getInactiveReason().contains("base table schema changed for columns: error_code"));
        } catch (Exception e) {
            Assertions.fail();
        } finally {
            try {
                starRocksAssert.dropMaterializedView("mv1");
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Test
    public void testModifyColumnsWithAMV3() {
        try {
            checkModifyColumnsWithMaterializedViews(starRocksAssert,
                    "create materialized view mv1 distributed by random refresh deferred manual " +
                            "as select timestamp, count(error_code) from sc_dup3 " +
                            "where op_id * 2> 10 group by timestamp",
                    "alter table sc_dup3 modify column error_code BIGINT",
                    false);
            MaterializedView mv = (MaterializedView) starRocksAssert.getTable("test", "mv1");
            Assertions.assertFalse(mv.isActive());
            Assertions.assertTrue(mv.getInactiveReason().contains("base table schema changed for columns: error_code"));
        } catch (Exception e) {
            Assertions.fail();
        } finally {
            try {
                starRocksAssert.dropMaterializedView("mv1");
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Test
    public void testModifyColumnsWithAMV4() {
        try {
            checkModifyColumnsWithMaterializedViews(starRocksAssert,
                    "create materialized view mv1 distributed by random refresh deferred manual " +
                            "as select timestamp, count(error_code) from sc_dup3 " +
                            "where op_id * 2> 10 group by timestamp",
                    "alter table sc_dup3 modify column op_id VARCHAR",
                    false);
            MaterializedView mv = (MaterializedView) starRocksAssert.getTable("test", "mv1");
            Assertions.assertFalse(mv.isActive());
            Assertions.assertTrue(mv.getInactiveReason().contains("base table schema changed for columns: op_id"));
        } catch (Exception e) {
            Assertions.fail();
        } finally {
            try {
                starRocksAssert.dropMaterializedView("mv1");
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Test
    public void testModifyColumnsWithAMV5() {
        try {
            checkModifyColumnsWithMaterializedViews(starRocksAssert,
                    "create materialized view mv1 distributed by random refresh deferred manual " +
                            "as select timestamp, count(error_code) from sc_dup3 " +
                            "where op_id * 2> 10 group by timestamp",
                    "alter table sc_dup3 modify column error_msg VARCHAR(1025)",
                    false);
            MaterializedView mv = (MaterializedView) starRocksAssert.getTable("test", "mv1");
            Assertions.assertTrue(mv.isActive());
        } catch (Exception e) {
            Assertions.fail();
        } finally {
            try {
                starRocksAssert.dropMaterializedView("mv1");
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Test
    public void testModifyColumnsWithAMV6() {
        try {
            checkModifyColumnsWithMaterializedViews(starRocksAssert,
                    "create materialized view mv1 distributed by random refresh deferred manual " +
                            "as select timestamp, count(error_code) from sc_dup3 " +
                            "where op_id * 2> 10 group by timestamp",
                    "alter table sc_dup3 drop column error_code, add column col_add bigint",
                    false);
            MaterializedView mv = (MaterializedView) starRocksAssert.getTable("test", "mv1");
            Assertions.assertFalse(mv.isActive());
        } catch (Exception e) {
            Assertions.fail();
        } finally {
            try {
                starRocksAssert.dropMaterializedView("mv1");
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Test
    public void testBitmapIndexOnRollupOnlyColumnRejectsUnsupportedTypeChange() throws Exception {
        String tbl = "sc_bm_rollup_tbl";
        String mv = "sc_bm_rollup_mv";
        starRocksAssert.withTable("CREATE TABLE " + tbl + " (k1 INT, k2 INT, k3 INT, x INT)\n"
                + "DUPLICATE KEY(k1, k2, k3)\nDISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1');");
        try {
            starRocksAssert.withRefreshedMaterializedView(
                    "create materialized view " + mv + " as select k1, k2, k3, x + 1 as z from " + tbl);
            // The sync MV's expression column is stored as mv_z; only the rollup carries it.
            DDLStmtExecutor.execute(parseAndAnalyzeStmt(
                    "alter table " + tbl + " add index ix_z (mv_z) using bitmap"), connectContext);
            waitAlterJobDone(GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2());

            int jobsBefore = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2().size();
            Exception e = Assertions.assertThrows(Exception.class, () -> DDLStmtExecutor.execute(parseAndAnalyzeStmt(
                    "alter table " + tbl + " modify column mv_z DOUBLE after k3 from " + mv), connectContext));
            Assertions.assertTrue(e.getMessage().contains("is not supported in bitmap index"), e.getMessage());
            Assertions.assertEquals(jobsBefore,
                    GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2().size());
        } finally {
            try {
                starRocksAssert.dropMaterializedView(mv);
            } catch (Exception ignore) {
                // ignore
            }
            try {
                starRocksAssert.dropTable(tbl);
            } catch (Exception ignore) {
                // ignore
            }
            // Finished jobs of the dropped table stay in the handlers' maps and would break the
            // other tests' waitAlterJobDone, which looks every job's table up.
            GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2().values()
                    .removeIf(job -> tbl.equals(job.tableName));
            GlobalStateMgr.getCurrentState().getRollupHandler().getAlterJobsV2().values()
                    .removeIf(job -> tbl.equals(job.tableName));
        }
    }

    @Test
    public void testBloomFilterOnRollupColumnRejectsUnsupportedTypeChange() throws Exception {
        String tbl = "sc_bf_rollup_tbl";
        String mv = "sc_bf_rollup_mv";
        starRocksAssert.withTable("CREATE TABLE " + tbl + " (k1 INT, k2 INT, k3 INT, x INT) "
                + "DUPLICATE KEY(k1, k2, k3) DISTRIBUTED BY HASH(k1) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1');");
        try {
            starRocksAssert.withRefreshedMaterializedView(
                    "create materialized view " + mv + " as select k1, k2, k3, x + 1 as z from " + tbl);
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            OlapTable table = (OlapTable) db.getTable(tbl);
            Assertions.assertNull(table.getBaseSchema().stream()
                    .filter(column -> column.getName().equals("mv_z")).findFirst().orElse(null));
            table.setBloomFilterInfo(Set.of(table.getColumn("mv_z").getColumnId()), 0.05);
            List<Column> baseSchemaBefore = List.copyOf(table.getBaseSchema());
            Set<ColumnId> bfColumnsBefore = table.getBfColumnIds();
            SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
            int jobsBefore = handler.getAlterJobsV2().size();
            AlterTableStmt stmt = (AlterTableStmt) parseAndAnalyzeStmt(
                    "alter table " + tbl + " modify column mv_z DOUBLE after k3 from " + mv);
            DdlException e = Assertions.assertThrows(DdlException.class,
                    () -> handler.process(stmt.getAlterClauseList(), db, table));
            Assertions.assertEquals("Invalid bloom filter column 'mv_z': unsupported type DOUBLE", e.getMessage());
            Assertions.assertEquals(jobsBefore, handler.getAlterJobsV2().size());
            Assertions.assertEquals(baseSchemaBefore, table.getBaseSchema());
            Assertions.assertEquals(bfColumnsBefore, table.getBfColumnIds());
            Assertions.assertEquals(OlapTableState.NORMAL, table.getState());

            // The rollup has mv_z, but the base schema does not. Adding an unsupported base column
            // exercises the newly present (oldColumn == null) path while retaining the inherited BF.
            AlterTableStmt add = (AlterTableStmt) parseAndAnalyzeStmt(
                    "alter table " + tbl + " add column mv_z DOUBLE");
            DdlException addError = Assertions.assertThrows(DdlException.class,
                    () -> handler.process(add.getAlterClauseList(), db, table));
            Assertions.assertEquals("Invalid bloom filter column 'mv_z': unsupported type DOUBLE",
                    addError.getMessage());
            Assertions.assertEquals(jobsBefore, handler.getAlterJobsV2().size());
            Assertions.assertEquals(baseSchemaBefore, table.getBaseSchema());
            Assertions.assertEquals(bfColumnsBefore, table.getBfColumnIds());
            Assertions.assertEquals(OlapTableState.NORMAL, table.getState());
        } finally {
            try {
                starRocksAssert.dropMaterializedView(mv);
            } catch (Exception ignore) {
                // ignore
            }
            try {
                starRocksAssert.dropTable(tbl);
            } catch (Exception ignore) {
                // ignore
            }
            GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2().values()
                    .removeIf(job -> tbl.equals(job.tableName));
            GlobalStateMgr.getCurrentState().getRollupHandler().getAlterJobsV2().values()
                    .removeIf(job -> tbl.equals(job.tableName));
        }
    }

    @Test
    public void testBitmapIndexColumnNewlyAddedToIndexSchemaIsChecked() throws Exception {
        String tbl = "sc_bm_added_tbl";
        String mv = "sc_bm_added_mv";
        starRocksAssert.withTable("CREATE TABLE " + tbl + " (k1 INT, k2 INT, k3 INT, x INT)\n"
                + "DUPLICATE KEY(k1, k2, k3)\nDISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1');");
        try {
            starRocksAssert.withRefreshedMaterializedView(
                    "create materialized view " + mv + " as select k1, k2, k3, x + 1 as z from " + tbl);
            DDLStmtExecutor.execute(parseAndAnalyzeStmt(
                    "alter table " + tbl + " add index ix_z (mv_z) using bitmap"), connectContext);
            waitAlterJobDone(GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2());

            // The base schema has no mv_z and the rollup's mv_z keeps its type, but the new base column
            // is covered by the BITMAP index and must be a type the BE can encode.
            int jobsBefore = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2().size();
            Exception e = Assertions.assertThrows(Exception.class, () -> DDLStmtExecutor.execute(parseAndAnalyzeStmt(
                    "alter table " + tbl + " add column mv_z DOUBLE"), connectContext));
            Assertions.assertTrue(e.getMessage().contains("is not supported in bitmap index"), e.getMessage());
            Assertions.assertTrue(e.getMessage().contains("mv_z"), e.getMessage());
            Assertions.assertEquals(jobsBefore,
                    GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2().size());

            // Control: a type the BITMAP index supports is not rejected by this check.
            DDLStmtExecutor.execute(parseAndAnalyzeStmt(
                    "alter table " + tbl + " add column mv_z BIGINT"), connectContext);
            Assertions.assertEquals(jobsBefore + 1,
                    GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2().size());
            waitAlterJobDone(GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2());
        } finally {
            try {
                starRocksAssert.dropMaterializedView(mv);
            } catch (Exception ignore) {
                // ignore
            }
            try {
                starRocksAssert.dropTable(tbl);
            } catch (Exception ignore) {
                // ignore
            }
            GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2().values()
                    .removeIf(job -> tbl.equals(job.tableName));
            GlobalStateMgr.getCurrentState().getRollupHandler().getAlterJobsV2().values()
                    .removeIf(job -> tbl.equals(job.tableName));
        }
    }
}
