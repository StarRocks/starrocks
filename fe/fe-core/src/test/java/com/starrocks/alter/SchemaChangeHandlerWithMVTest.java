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

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.TestWithFeService;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.jupiter.api.Test;
import org.junit.runners.MethodSorters;

import java.util.Map;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
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

    private void waitAlterJobDone(Map<Long, AlterJobV2> alterJobs) throws Exception {
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                LOG.info("alter job {} is running. state: {}", alterJobV2.getJobId(), alterJobV2.getJobState());
                Thread.sleep(500);
            }
            LOG.info("alter job {} is done. state: {}", alterJobV2.getJobId(), alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());

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
            GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(dropValColStm);

            Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
            waitAlterJobDone(alterJobs);
        } finally {
            if (isDropMV) {
                dropMV(mv);
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
            Assert.fail();
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
            Assert.fail();
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
            Assert.fail();
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
            Assert.fail();
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
            Assert.assertTrue(e.getMessage().contains("Can not drop/modify the column type, because the column is used " +
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
            Assert.assertTrue(e.getMessage(),
                    e.getMessage().contains("Can not drop/modify the column timestamp, because the column " +
                    "is used in the related rollup mv1, please drop the rollup index first."));
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
            Assert.assertTrue(e.getMessage().contains("Can not drop/modify the column timestamp, because the column " +
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
            Assert.assertTrue(e.getMessage().contains("Can not drop/modify the column mv_count_error_code, because the column " +
                    "is used in the related rollup mv1 with the define expr:" +
                    "CASE WHEN `test`.`sc_dup3`.`error_code` IS NULL THEN 0 ELSE 1 END, please drop the rollup index first."));
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
            Assert.assertTrue(e.getMessage().contains("Can not drop/modify the column mv_count_error_code, because " +
                    "the column is used in the related rollup mv1 with the define expr:" +
                    "CASE WHEN `test`.`sc_dup3`.`error_code` IS NULL THEN 0 ELSE 1 END, please drop the rollup index first."));
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
            Assert.assertTrue(e.getMessage().contains("Can not drop/modify the column type, because the column is " +
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
            Assert.assertTrue(e.getMessage().contains("Can not drop/modify the column type, because the column is " +
                    "used in the related rollup mv1 with the where expr:`test`.`sc_dup3`.`type` > 10, " +
                    "please drop the rollup index first."));
        }
    }

    private void dropMV(String mvName) {
        try {
            starRocksAssert.dropMaterializedView(mvName);
        } catch (Exception e) {
            // ignore
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
            Assert.assertFalse(mv.isActive());
            Assert.assertTrue(mv.getInactiveReason().contains("base table schema changed for columns: op_id"));
        } catch (Exception e) {
            Assert.fail();
        } finally {
            dropMV("mv1");
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
            Assert.assertFalse(mv.isActive());
            Assert.assertTrue(mv.getInactiveReason().contains("base table schema changed for columns: error_code"));
        } catch (Exception e) {
            Assert.fail();
        } finally {
            dropMV("mv1");
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
            Assert.assertFalse(mv.isActive());
            Assert.assertTrue(mv.getInactiveReason().contains("base table schema changed for columns: error_code"));
        } catch (Exception e) {
            Assert.fail();
        } finally {
            dropMV("mv1");
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
            Assert.assertFalse(mv.isActive());
            Assert.assertTrue(mv.getInactiveReason().contains("base table schema changed for columns: op_id"));
        } catch (Exception e) {
            Assert.fail();
        } finally {
            dropMV("mv1");
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
            Assert.assertTrue(mv.isActive());
        } catch (Exception e) {
            Assert.fail();
        } finally {
            dropMV("mv1");
        }
    }
}