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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/alter/SchemaChangeHandlerTest.java

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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FlatJsonConfig;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.optimizer.statistics.CacheDictManager;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.utframe.TestWithFeService;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

@TestMethodOrder(MethodName.class)
public class SchemaChangeHandlerTest extends TestWithFeService {

    private static final Logger LOG = LogManager.getLogger(SchemaChangeHandlerTest.class);
    private int jobSize = 0;

    @Override
    protected void createStarrocksCluster() {
        UtFrameUtils.createMinStarRocksCluster(false, runMode);
    }

    @Override
    protected void runBeforeAll() throws Exception {
        // set some parameters to speedup test
        Config.tablet_sched_checker_interval_seconds = 1;
        Config.tablet_sched_repair_delay_factor_second = 1;
        Config.enable_new_publish_mechanism = true;
        Config.alter_scheduler_interval_millisecond = 100;

        //create database db1
        createDatabase("test");

        //create tables
        String createAggTblStmtStr = "CREATE TABLE IF NOT EXISTS test.sc_agg (\n" + "user_id LARGEINT NOT NULL,\n"
                    + "date DATE NOT NULL,\n" + "city VARCHAR(20),\n" + "age SMALLINT,\n" + "sex TINYINT,\n"
                    + "last_visit_date DATETIME REPLACE DEFAULT '1970-01-01 00:00:00',\n" + "cost BIGINT SUM DEFAULT '0',\n"
                    + "max_dwell_time INT MAX DEFAULT '0',\n" + "min_dwell_time INT MIN DEFAULT '99999')\n"
                    + "AGGREGATE KEY(user_id, date, city, age, sex)\n" + "DISTRIBUTED BY HASH(user_id) BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1', 'fast_schema_evolution' = 'true');";
        createTable(createAggTblStmtStr);

        String createUniqTblStmtStr = "CREATE TABLE IF NOT EXISTS test.sc_uniq (\n" + "user_id LARGEINT NOT NULL,\n"
                    + "username VARCHAR(50) NOT NULL,\n" + "city VARCHAR(20),\n" + "age SMALLINT,\n" + "sex TINYINT,\n"
                    + "phone LARGEINT,\n" + "address VARCHAR(500),\n" + "register_time DATETIME)\n"
                    + "UNIQUE  KEY(user_id, username)\n" + "DISTRIBUTED BY HASH(user_id) BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1', 'fast_schema_evolution' = 'true');";
        createTable(createUniqTblStmtStr);

        String createDupTblStmtStr = "CREATE TABLE IF NOT EXISTS test.sc_dup (\n" + "timestamp DATETIME,\n"
                    + "type INT,\n" + "error_code INT,\n" + "error_msg VARCHAR(1024),\n" + "op_id BIGINT,\n"
                    + "op_time DATETIME)\n" + "DUPLICATE  KEY(timestamp, type)\n" + "DISTRIBUTED BY HASH(type) BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1', 'fast_schema_evolution' = 'true');";

        createTable(createDupTblStmtStr);

        String createDupTbl2StmtStr = "CREATE TABLE IF NOT EXISTS test.sc_dup2 (\n" + "timestamp DATETIME,\n"
                    + "type INT,\n" + "error_code INT,\n" + "error_msg VARCHAR(1024),\n" + "op_id BIGINT,\n"
                    + "op_time DATETIME)\n" + "DUPLICATE  KEY(timestamp, type)\n" + "DISTRIBUTED BY HASH(type) BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1', 'fast_schema_evolution' = 'true');";

        createTable(createDupTbl2StmtStr);

        String createPKTblStmtStr = "CREATE TABLE IF NOT EXISTS test.sc_pk (\n" + "timestamp DATETIME,\n"
                    + "type INT,\n" + "error_code INT,\n" + "error_msg VARCHAR(1024),\n" + "op_id BIGINT,\n"
                    + "op_time DATETIME)\n" + "PRIMARY  KEY(timestamp, type)\n" + "DISTRIBUTED BY HASH(type) BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1', 'fast_schema_evolution' = 'true');";

        createTable(createPKTblStmtStr);

    }

    private void waitAlterJobDone(Map<Long, AlterJobV2> alterJobs) throws Exception {
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                LOG.info("alter job {} is running. state: {}", alterJobV2.getJobId(), alterJobV2.getJobState());
                Thread.sleep(1000);
            }
            LOG.info("alter job {} is done. state: {}", alterJobV2.getJobId(), alterJobV2.getJobState());
            Assertions.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());

            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(alterJobV2.getDbId());
            OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(db.getId(), alterJobV2.getTableId());
            while (tbl.getState() != OlapTable.OlapTableState.NORMAL) {
                Thread.sleep(1000);
            }
        }
    }

    @Test
    public void testBuildSchemaMapAndGet() {
        LinkedList<Column> schemaList = new LinkedList<>();
        String colName1 = "__starrocks_shadow_c1";
        String colName2 = "__starrocks_shadow_c2";
        Column col1 = new Column(colName1, Type.INT);
        Column col2 = new Column(colName2, Type.INT);
        schemaList.add(col1);
        schemaList.add(col2);

        Map<String, Column> schemaMap = SchemaChangeHandler.buildSchemaMapFromList(schemaList, true, true);
        Column col = SchemaChangeHandler.getColumnFromSchemaMap(schemaMap, "c1", true, true);
        Assertions.assertEquals(col.getName(), colName1);
        col = SchemaChangeHandler.getColumnFromSchemaMap(schemaMap, colName1, true, true);
        Assertions.assertEquals(col.getName(), colName1);
        col = SchemaChangeHandler.getColumnFromSchemaMap(schemaMap, "__starrocks_shadow_C2", true, true);
        Assertions.assertNull(col);

        schemaMap = SchemaChangeHandler.buildSchemaMapFromList(schemaList, true, false);
        col = SchemaChangeHandler.getColumnFromSchemaMap(schemaMap, "c1", true, false);
        Assertions.assertEquals(col.getName(), colName1);
        col = SchemaChangeHandler.getColumnFromSchemaMap(schemaMap, colName1, true, false);
        Assertions.assertEquals(col.getName(), colName1);
        col = SchemaChangeHandler.getColumnFromSchemaMap(schemaMap, "__starrocks_shadow_C2", true, false);
        Assertions.assertEquals(col.getName(), colName2);

        schemaMap = SchemaChangeHandler.buildSchemaMapFromList(schemaList, false, true);
        col = SchemaChangeHandler.getColumnFromSchemaMap(schemaMap, "c1", false, true);
        Assertions.assertNull(col);
        col = SchemaChangeHandler.getColumnFromSchemaMap(schemaMap, colName1, false, true);
        Assertions.assertEquals(col.getName(), colName1);
        col = SchemaChangeHandler.getColumnFromSchemaMap(schemaMap, "__starrocks_shadow_C2", false, true);
        Assertions.assertNull(col);

        schemaMap = SchemaChangeHandler.buildSchemaMapFromList(schemaList, false, false);
        col = SchemaChangeHandler.getColumnFromSchemaMap(schemaMap, "c1", false, false);
        Assertions.assertNull(col);
        col = SchemaChangeHandler.getColumnFromSchemaMap(schemaMap, colName1, false, false);
        Assertions.assertEquals(col.getName(), colName1);
        col = SchemaChangeHandler.getColumnFromSchemaMap(schemaMap, "__starrocks_shadow_C2", false, false);
        Assertions.assertEquals(col.getName(), colName2);
    }

    @Test
    public void testAggAddColumnWithoutAggTypeOrKeyIsRejected() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "sc_agg");

        // Pin the config and assert the guard itself, not whichever default the branch ships.
        // The compiled default is expected to differ between branches over time.
        boolean saved = Config.allow_implicit_key_column_in_agg_add_column;
        Config.allow_implicit_key_column_in_agg_add_column = false;
        try {
            // Neither an agg function nor KEY: ambiguous, so it must be rejected rather than silently
            // promoted to a key column.
            AlterTableStmt ambiguous = (AlterTableStmt) parseAndAnalyzeStmt(
                    "alter table test.sc_agg add column amb_col int default '0'");
            DdlException exception = Assertions.assertThrows(DdlException.class, () ->
                    new SchemaChangeHandler().process(ambiguous.getAlterClauseList(), db, tbl));
            Assertions.assertTrue(exception.getMessage().contains("must specify either an aggregate function"),
                    exception.getMessage());
            Assertions.assertTrue(exception.getMessage().contains("allow_implicit_key_column_in_agg_add_column"),
                    exception.getMessage());
        } finally {
            Config.allow_implicit_key_column_in_agg_add_column = saved;
        }

        // A type that cannot be a key column keeps its own pre-existing message, because KEY is not
        // a legal alternative for it and the new message must not suggest one. That rejection runs
        // ahead of the config check, so it must hold whichever default a branch ships.
        for (boolean allowImplicitKey : new boolean[] {false, true}) {
            Config.allow_implicit_key_column_in_agg_add_column = allowImplicitKey;
            try {
                AlterTableStmt nonKeyable = (AlterTableStmt) parseAndAnalyzeStmt(
                        "alter table test.sc_agg add column amb_float float default '0'");
                DdlException floatException = Assertions.assertThrows(DdlException.class, () ->
                        new SchemaChangeHandler().process(nonKeyable.getAlterClauseList(), db, tbl));
                Assertions.assertTrue(floatException.getMessage().contains("type can not be key column"),
                        "allowImplicitKey=" + allowImplicitKey + ": " + floatException.getMessage());
            } finally {
                Config.allow_implicit_key_column_in_agg_add_column = saved;
            }
        }
    }

    @Test
    public void testAggAddColumnWithoutAggTypeAllowedByConfig() throws Exception {
        // Own table, so the implicit key column this adds cannot disturb the schema assertions in
        // the other aggregate tests, which run after this one under MethodName ordering.
        createTable("CREATE TABLE test.sc_agg_legacy_key (\n"
                + "user_id LARGEINT NOT NULL,\n"
                + "dt DATE NOT NULL,\n"
                + "cost BIGINT SUM DEFAULT '0')\n"
                + "AGGREGATE KEY(user_id, dt)\n"
                + "DISTRIBUTED BY HASH(user_id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1');");

        boolean saved = Config.allow_implicit_key_column_in_agg_add_column;
        Config.allow_implicit_key_column_in_agg_add_column = true;
        try {
            executeAlterAndWaitDone(
                    "alter table test.sc_agg_legacy_key add column legacy_k int default '1'");

            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), "sc_agg_legacy_key");
            Column legacyCol = tbl.getColumn("legacy_k");
            Assertions.assertNotNull(legacyCol);
            Assertions.assertTrue(legacyCol.isKey(), "legacy config should restore the implicit key column");
        } finally {
            Config.allow_implicit_key_column_in_agg_add_column = saved;
        }
    }

    @Test
    public void testAggAddOrDropColumn() throws Exception {
        LOG.info("dbName: {}", GlobalStateMgr.getCurrentState().getLocalMetastore().listDbNames(new ConnectContext()));

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "sc_agg");
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        try {
            Assertions.assertNotNull(tbl);
            System.out.println(tbl.getName());
            Assertions.assertEquals("StarRocks", tbl.getEngine());
            Assertions.assertEquals(9, tbl.getBaseSchema().size());
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        }

        //process agg add value column schema change
        String addValColStmtStr = "alter table test.sc_agg add column new_v1 int MAX default '0'";
        AlterTableStmt addValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(addValColStmtStr);
        DDLStmtExecutor.execute(addValColStmt, connectContext);
        jobSize++;
        // check alter job, do not create job
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        Assertions.assertEquals(jobSize, alterJobs.size());

        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        try {
            Assertions.assertEquals(10, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        }

        //process agg add  key column schema change
        String addKeyColStmtStr = "alter table test.sc_agg add column new_k1 int key default '1'";
        AlterTableStmt addKeyColStmt = (AlterTableStmt) parseAndAnalyzeStmt(addKeyColStmtStr);
        DDLStmtExecutor.execute(addKeyColStmt, connectContext);

        //check alter job
        jobSize++;
        Assertions.assertEquals(jobSize, alterJobs.size());
        waitAlterJobDone(alterJobs);

        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        try {
            Assertions.assertEquals(11, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        }

        //process agg drop value column schema change
        String dropValColStmtStr = "alter table test.sc_agg drop column new_v1";
        AlterTableStmt dropValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(dropValColStmtStr);
        DDLStmtExecutor.execute(dropValColStmt, connectContext);
        jobSize++;
        //check alter job, do not create job
        LOG.info("alterJobs:{}", alterJobs);
        Assertions.assertEquals(jobSize, alterJobs.size());

        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        try {
            Assertions.assertEquals(10, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        }

        //process agg drop key column with replace schema change, expect exception.
        String dropKeyColStmtStr = "alter table test.sc_agg drop column new_k1";
        AlterTableStmt dropKeyColStmt = (AlterTableStmt) parseAndAnalyzeStmt(dropKeyColStmtStr);
        Assertions.assertThrows(Exception.class, () -> DDLStmtExecutor.execute(dropKeyColStmt, connectContext));

        LOG.info("getIndexIdToSchema 1: {}", tbl.getIndexIdToSchema());

        //process agg drop value column with rollup schema change
        String dropRollUpValColStmtStr = "alter table test.sc_agg drop column max_dwell_time";
        AlterTableStmt dropRollUpValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(dropRollUpValColStmtStr);
        DDLStmtExecutor.execute(dropRollUpValColStmt, connectContext);
        jobSize++;
        //check alter job, need create job
        LOG.info("alterJobs:{}", alterJobs);
        Assertions.assertEquals(jobSize, alterJobs.size());

        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        try {
            Assertions.assertEquals(9, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        }
    }

    @Test
    public void testUniqAddOrDropColumn() throws Exception {

        LOG.info("dbName: {}", GlobalStateMgr.getCurrentState().getLocalMetastore().listDbNames(new ConnectContext()));

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "sc_uniq");
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        try {
            Assertions.assertNotNull(tbl);
            System.out.println(tbl.getName());
            Assertions.assertEquals("StarRocks", tbl.getEngine());
            Assertions.assertEquals(8, tbl.getBaseSchema().size());
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        }

        //process uniq add value column schema change
        String addValColStmtStr = "alter table test.sc_uniq add column new_v1 int default '0'";
        AlterTableStmt addValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(addValColStmtStr);
        DDLStmtExecutor.execute(addValColStmt, connectContext);
        jobSize++;
        //check alter job, do not create job
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        LOG.info("alterJobs:{}", alterJobs);
        Assertions.assertEquals(jobSize, alterJobs.size());

        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        try {
            Assertions.assertEquals(9, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        }

        //process uniq drop val column schema change
        String dropValColStmtStr = "alter table test.sc_uniq drop column new_v1";
        AlterTableStmt dropValColStm = (AlterTableStmt) parseAndAnalyzeStmt(dropValColStmtStr);
        DDLStmtExecutor.execute(dropValColStm, connectContext);
        jobSize++;
        //check alter job
        Assertions.assertEquals(jobSize, alterJobs.size());
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        try {
            Assertions.assertEquals(8, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        }
    }

    @Test
    public void testDupAddOrDropColumn() throws Exception {

        LOG.info("dbName: {}", GlobalStateMgr.getCurrentState().getLocalMetastore().listDbNames(new ConnectContext()));

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "sc_dup");
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        try {
            Assertions.assertNotNull(tbl);
            System.out.println(tbl.getName());
            Assertions.assertEquals("StarRocks", tbl.getEngine());
            Assertions.assertEquals(6, tbl.getBaseSchema().size());
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        }

        //process uniq add value column schema change
        String addValColStmtStr = "alter table test.sc_dup add column new_v1 int default '0'";
        AlterTableStmt addValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(addValColStmtStr);
        DDLStmtExecutor.execute(addValColStmt, connectContext);
        jobSize++;
        //check alter job, do not create job
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        LOG.info("alterJobs:{}", alterJobs);
        Assertions.assertEquals(jobSize, alterJobs.size());

        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        try {
            Assertions.assertEquals(7, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        }

        //process uniq drop val column schema change
        String dropValColStmtStr = "alter table test.sc_dup drop column new_v1";
        AlterTableStmt dropValColStm = (AlterTableStmt) parseAndAnalyzeStmt(dropValColStmtStr);
        DDLStmtExecutor.execute(dropValColStm, connectContext);
        jobSize++;
        //check alter job
        Assertions.assertEquals(jobSize, alterJobs.size());
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        try {
            Assertions.assertEquals(6, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        }
    }

    @Test
    public void testModifyTableAddOrDropColumns() {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getLocalMetastore().getDb("test");
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "sc_dup2");
        Map<Long, AlterJobV2> alterJobs = globalStateMgr.getSchemaChangeHandler().getAlterJobsV2();

        // origin columns
        Map<Long, List<Column>> indexSchemaMap = new HashMap<>();
        Map<Long, Long> indexToNewSchemaId = new HashMap<>();
        for (Map.Entry<Long, List<Column>> entry : tbl.getIndexIdToSchema().entrySet()) {
            indexSchemaMap.put(entry.getKey(), new LinkedList<>(entry.getValue()));
            indexToNewSchemaId.put(entry.getKey(), globalStateMgr.getNextId());
        }
        List<Index> newIndexes = tbl.getCopiedIndexes();

        Assertions.assertDoesNotThrow(
                    () -> GlobalStateMgr.getCurrentState().getAlterJobMgr().getSchemaChangeHandler()
                                .modifyTableAddOrDrop(db, tbl, indexSchemaMap, newIndexes, 100,
                                            indexToNewSchemaId, false, -1));
        jobSize++;
        Assertions.assertEquals(jobSize, alterJobs.size());

        Assertions.assertDoesNotThrow(
                    () -> GlobalStateMgr.getCurrentState().getAlterJobMgr().getSchemaChangeHandler()
                                .modifyTableAddOrDrop(db, tbl, indexSchemaMap, newIndexes, 101,
                                            indexToNewSchemaId, true, 101));
        jobSize++;
        Assertions.assertEquals(jobSize, alterJobs.size());

        OlapTableState beforeState = tbl.getState();
        tbl.setState(OlapTableState.ROLLUP);
        Assertions.assertThrows(DdlException.class,
                    () -> GlobalStateMgr.getCurrentState().getAlterJobMgr().getSchemaChangeHandler()
                                .modifyTableAddOrDrop(db, tbl, indexSchemaMap, newIndexes, 102, indexToNewSchemaId,
                                            false, -1));
        tbl.setState(beforeState);
    }

    @Test
    public void testSetPrimaryIndexCacheExpireSec() throws Exception {

        LOG.info("dbName: {}", GlobalStateMgr.getCurrentState().getLocalMetastore().listDbNames(new ConnectContext()));

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "sc_pk");
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        try {
            Assertions.assertNotNull(tbl);
            System.out.println(tbl.getName());
            Assertions.assertEquals("StarRocks", tbl.getEngine());
            Assertions.assertEquals(6, tbl.getBaseSchema().size());
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        }

        //process set properties
        String addValColStmtStr = "alter table test.sc_pk set ('primary_index_cache_expire_sec' = '3600');";
        AlterTableStmt addValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(addValColStmtStr);
        DDLStmtExecutor.execute(addValColStmt, connectContext);

        try {
            String addValColStmtStr2 = "alter table test.sc_pk set ('primary_index_cache_expire_sec' = '-12');";
            AlterTableStmt addValColStmt2 = (AlterTableStmt) parseAndAnalyzeStmt(addValColStmtStr2);
            DDLStmtExecutor.execute(addValColStmt2, connectContext);
        } catch (Exception e) {
            LOG.warn(e.getMessage(), e);
            Assertions.assertTrue(e.getMessage().contains("Property primary_index_cache_expire_sec must not be less than 0"));
        }

        try {
            String addValColStmtStr3 = "alter table test.sc_pk set ('primary_index_cache_expire_sec' = 'asd');";
            AlterTableStmt addValColStmt3 = (AlterTableStmt) parseAndAnalyzeStmt(addValColStmtStr3);
            DDLStmtExecutor.execute(addValColStmt3, connectContext);
        } catch (Exception e) {
            LOG.warn(e.getMessage(), e);
            Assertions.assertTrue(e.getMessage().contains("Property primary_index_cache_expire_sec must be integer"));
        }
    }

    @Test
    public void testAddReserveColumn() throws Exception {

        LOG.info("dbName: {}", GlobalStateMgr.getCurrentState().getLocalMetastore().listDbNames(new ConnectContext()));

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "sc_pk");
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        try {
            Assertions.assertNotNull(tbl);
            System.out.println(tbl.getName());
            Assertions.assertEquals("StarRocks", tbl.getEngine());
            Assertions.assertEquals(6, tbl.getBaseSchema().size());
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        }
        Config.allow_system_reserved_names = true;

        try {
            String addValColStmtStr2 = "alter table test.sc_pk add column __op int";
            AlterTableStmt addValColStmt2 = (AlterTableStmt) parseAndAnalyzeStmt(addValColStmtStr2);
            DDLStmtExecutor.execute(addValColStmt2, connectContext);
        } catch (Exception e) {
            LOG.warn(e.getMessage(), e);
            Assertions.assertTrue(e.getMessage().contains("Column name '__op' is reserved for primary key table"));
        }

        try {
            String addValColStmtStr3 = "alter table test.sc_pk add column __row int";
            AlterTableStmt addValColStmt3 = (AlterTableStmt) parseAndAnalyzeStmt(addValColStmtStr3);
            DDLStmtExecutor.execute(addValColStmt3, connectContext);
        } catch (Exception e) {
            LOG.warn(e.getMessage(), e);
            Assertions.assertTrue(e.getMessage().contains("Column name '__row' is reserved for primary key table"));
        }
        Config.allow_system_reserved_names = false;
    }

    @Test
    public void testUpdateFlatJsonConfigMeta() throws Exception {
        // Create a test table
        String createTableStmt = "CREATE TABLE test.flat_json_test (\n" +
                "id INT NOT NULL,\n" +
                "data JSON\n" +
                ") DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES ('replication_num' = '1');";
        createTable(createTableStmt);

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) db.getTable("flat_json_test");
        Assertions.assertNotNull(table);

        SchemaChangeHandler handler = new SchemaChangeHandler();

        // Test case 1: Enable flat JSON with all properties
        Map<String, String> properties1 = new HashMap<>();
        properties1.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "true");
        properties1.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, "0.1");
        properties1.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR, "0.8");
        properties1.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX, "50");

        boolean result1 = handler.updateFlatJsonConfigMeta(db, table.getId(), properties1,
                TTabletMetaType.FLAT_JSON_CONFIG);
        Assertions.assertTrue(result1);

        // Verify the configuration was applied
        Assertions.assertTrue(table.containsFlatJsonConfig());
        FlatJsonConfig config1 = table.getFlatJsonConfig();
        Assertions.assertTrue(config1.getFlatJsonEnable());
        Assertions.assertEquals(0.1, config1.getFlatJsonNullFactor(), 0.001);
        Assertions.assertEquals(0.8, config1.getFlatJsonSparsityFactor(), 0.001);
        Assertions.assertEquals(50, config1.getFlatJsonColumnMax());

        // Test case 2: Try to set flat JSON properties when flat_json.enable is false - should throw exception
        Map<String, String> properties2 = new HashMap<>();
        properties2.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "false");
        properties2.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, "0.2");

        try {
            handler.updateFlatJsonConfigMeta(db, table.getId(), properties2,
                    TTabletMetaType.FLAT_JSON_CONFIG);
            Assertions.fail("Should throw exception when setting flat JSON properties with flat_json.enable=false");
        } catch (RuntimeException e) {
            Assertions.assertTrue(
                    e.getMessage().contains("flat JSON configuration must be set after enabling flat JSON"));
        }

        // Test case 3: Update existing flat JSON configuration
        Map<String, String> properties3 = new HashMap<>();
        properties3.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, "0.3");
        properties3.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR, "0.9");

        boolean result3 = handler.updateFlatJsonConfigMeta(db, table.getId(), properties3,
                TTabletMetaType.FLAT_JSON_CONFIG);
        Assertions.assertTrue(result3);

        // Verify the configuration was updated
        FlatJsonConfig config3 = table.getFlatJsonConfig();
        Assertions.assertTrue(config3.getFlatJsonEnable());
        Assertions.assertEquals(0.3, config3.getFlatJsonNullFactor(), 0.001);
        Assertions.assertEquals(0.9, config3.getFlatJsonSparsityFactor(), 0.001);
        Assertions.assertEquals(50, config3.getFlatJsonColumnMax()); // Should remain unchanged

        // Test case 4: Disable flat JSON
        Map<String, String> properties4 = new HashMap<>();
        properties4.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "false");

        boolean result4 = handler.updateFlatJsonConfigMeta(db, table.getId(), properties4,
                TTabletMetaType.FLAT_JSON_CONFIG);
        Assertions.assertTrue(result4);

        // Verify flat JSON is disabled
        FlatJsonConfig config4 = table.getFlatJsonConfig();
        Assertions.assertFalse(config4.getFlatJsonEnable());

        // Test case 5: Try to set properties when flat JSON is disabled - should throw exception
        Map<String, String> properties5 = new HashMap<>();
        properties5.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX, "100");

        try {
            handler.updateFlatJsonConfigMeta(db, table.getId(), properties5,
                    TTabletMetaType.FLAT_JSON_CONFIG);
            Assertions.fail("Should throw exception when setting flat JSON properties with flat_json.enable=false");
        } catch (RuntimeException e) {
            Assertions.assertTrue(
                    e.getMessage().contains("flat JSON configuration must be set after enabling flat JSON"));
        }
    }

    @Test
    public void testUpdateFlatJsonConfigMetaWithNoChange() throws Exception {
        // Create a test table
        String createTableStmt = "CREATE TABLE test.flat_json_no_change (\n" +
                "id INT NOT NULL,\n" +
                "data JSON\n" +
                ") DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES ('replication_num' = '1');";
        createTable(createTableStmt);

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) db.getTable("flat_json_no_change");
        Assertions.assertNotNull(table);

        SchemaChangeHandler handler = new SchemaChangeHandler();

        // First, set up flat JSON configuration
        Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "true");
        initialProperties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, "0.1");
        initialProperties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR, "0.8");
        initialProperties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX, "50");

        boolean result1 = handler.updateFlatJsonConfigMeta(db, table.getId(), initialProperties,
                TTabletMetaType.FLAT_JSON_CONFIG);
        Assertions.assertTrue(result1);

        // Now try to set the same values again - should return true but no actual change
        Map<String, String> sameProperties = new HashMap<>();
        sameProperties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, "0.1");
        sameProperties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR, "0.8");
        sameProperties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX, "50");

        boolean result2 = handler.updateFlatJsonConfigMeta(db, table.getId(), sameProperties,
                TTabletMetaType.FLAT_JSON_CONFIG);
        Assertions.assertTrue(result2);

        // Verify the configuration remains the same
        FlatJsonConfig config = table.getFlatJsonConfig();
        Assertions.assertTrue(config.getFlatJsonEnable());
        Assertions.assertEquals(0.1, config.getFlatJsonNullFactor(), 0.001);
        Assertions.assertEquals(0.8, config.getFlatJsonSparsityFactor(), 0.001);
        Assertions.assertEquals(50, config.getFlatJsonColumnMax());
    }

    @Test
    public void testUpdateFlatJsonConfigMetaWithInvalidTable() throws Exception {
        SchemaChangeHandler handler = new SchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        // Test with non-existent table ID
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "true");

        boolean result = handler.updateFlatJsonConfigMeta(db, 99999L, properties,
                TTabletMetaType.FLAT_JSON_CONFIG);
        Assertions.assertFalse(result);
    }

    @Test
    public void testAlterTableConflictClauses() throws Exception {
        String createTableStmt =
                "create table test.test_crash2(c1 varchar(255),c2 datetime, c3 bigint, c4 varchar(255)," +
                        " c5 varchar(255)) " +
                        " primary key(c1,c2) partition by date_trunc('day',c2) distributed by hash(c1) buckets 2" +
                        " order by (c1,c3,c4,c5) PROPERTIES ('replication_num' = '1');";
        createTable(createTableStmt);
        { // reorder conflict
            String alterTableSql = "ALTER TABLE test.test_crash2 ADD COLUMN c6 VARCHAR(255) " +
                    "NULL COMMENT 'ccc', ORDER BY(c1, c3,c4,c6);";
            AlterTableStmt alterTableStmt = (AlterTableStmt) parseAndAnalyzeStmt(alterTableSql);
            Assertions.assertEquals(2L, alterTableStmt.getAlterClauseList().size());
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            OlapTable table = (OlapTable) db.getTable("test_crash2");
            SchemaChangeHandler handler = new SchemaChangeHandler();
            DdlException exception = Assertions.assertThrows(DdlException.class, () ->
                    handler.process(alterTableStmt.getAlterClauseList(), db, table));
            Assertions.assertTrue(exception.getMessage()
                            .contains("MODIFY SORT KEY COLUMNS can not be combined with other alter operations"),
                    exception.getMessage());
        }
        {
            String alterTableSql = "ALTER TABLE test.test_crash2 ADD COLUMN c7 VARCHAR(255) " +
                    "NULL COMMENT 'ccc', MODIFY COLUMN c1 COMMENT 'c1c1' ;";
            AlterTableStmt alterTableStmt = (AlterTableStmt) parseAndAnalyzeStmt(alterTableSql);
            Assertions.assertEquals(2L, alterTableStmt.getAlterClauseList().size());
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            OlapTable table = (OlapTable) db.getTable("test_crash2");
            SchemaChangeHandler handler = new SchemaChangeHandler();
            DdlException exception = Assertions.assertThrows(DdlException.class, () ->
                    handler.process(alterTableStmt.getAlterClauseList(), db, table));
            Assertions.assertTrue(exception.getMessage()
                            .contains("MODIFY COLUMN COMMENT can not be combined with other alter operations"),
                    exception.getMessage());
        }
        {
            String alterTableSql = "ALTER TABLE test.test_crash2 ADD COLUMN c8 VARCHAR(255) " +
                    "NULL COMMENT 'c8c8', DISTRIBUTED BY HASH(c2) BUCKETS 2;";
            SemanticException exception = Assertions.assertThrows(SemanticException.class, () ->
                    parseAndAnalyzeStmt(alterTableSql));
            Assertions.assertTrue(
                    exception.getMessage().contains("Alter operation OPTIMIZE conflicts with operation SCHEMA_CHANGE"),
                    exception.getMessage());
        }
    }

    @Test
    public void testAlterTableAddColumnDefaultCurrentTimestampPreservesGenerator() throws Exception {
        String createTableStmt = "CREATE TABLE test.test_alter_add_now (\n" +
                "id BIGINT NOT NULL\n" +
                ") PRIMARY KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "    'replication_num' = '1',\n" +
                "    'fast_schema_evolution' = 'true'\n" +
                ");";
        createTable(createTableStmt);

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) db.getTable("test_alter_add_now");
        Assertions.assertNotNull(table);

        String alterSql = "ALTER TABLE test.test_alter_add_now ADD COLUMN ts DATETIME DEFAULT current_timestamp";
        AlterTableStmt alterStmt = (AlterTableStmt) parseAndAnalyzeStmt(alterSql);
        Assertions.assertDoesNotThrow(() -> {
            SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
            handler.process(alterStmt.getAlterClauseList(), db, table);
        });
        jobSize++;

        Column tsCol = table.getColumn("ts");
        Assertions.assertNotNull(tsCol);
        Assertions.assertNotNull(tsCol.getDefaultExpr());
        // ALTER-time string is preserved on defaultValue so BE can backfill pre-existing rows.
        Assertions.assertNotNull(tsCol.getDefaultValue(),
                "ALTER-time materialization must be kept for BE backfill of pre-existing rows");
        Assertions.assertEquals(Column.DefaultValueType.CONST, tsCol.getDefaultValueType());

        // Metadata callers (DESCRIBE, information_schema.columns) must surface the generator, not the
        // frozen literal stashed in defaultValue by the schema-change backfill path.
        List<String> extras = Lists.newArrayList();
        Assertions.assertEquals("CURRENT_TIMESTAMP", tsCol.getMetaDefaultValue(extras));
        Assertions.assertTrue(extras.contains("DEFAULT_GENERATED"),
                "current_timestamp default must be reported as DEFAULT_GENERATED");
    }

    @Test
    public void testSortKeyUpdatedAfterAddKeyColumnAgg() throws Exception {
        createTable("CREATE TABLE test.sc_agg_sort_key (\n"
                + "k0 INT,\n"
                + "k1 INT,\n"
                + "k2 INT,\n"
                + "k3 INT,\n"
                + "v0 INT SUM DEFAULT '0'\n"
                + ") AGGREGATE KEY(k0, k1, k2, k3)\n"
                + "DISTRIBUTED BY HASH(k0) BUCKETS 1\n"
                + "ORDER BY(k3, k2, k1, k0)\n"
                + "PROPERTIES ('replication_num' = '1', 'fast_schema_evolution' = 'true');");
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "sc_agg_sort_key");
        doTestSortKeyUpdatedAfterAddKeyColumn(tbl, "test.sc_agg_sort_key");
    }

    @Test
    public void testSortKeyUpdatedAfterAddKeyColumnUniq() throws Exception {
        createTable("CREATE TABLE test.sc_uniq_sort_key (\n"
                + "k0 INT,\n"
                + "k1 INT,\n"
                + "k2 INT,\n"
                + "k3 INT,\n"
                + "v0 VARCHAR(1024)\n"
                + ") UNIQUE KEY(k0, k1, k2, k3)\n"
                + "DISTRIBUTED BY HASH(k0) BUCKETS 1\n"
                + "ORDER BY(k3, k2, k1, k0)\n"
                + "PROPERTIES ('replication_num' = '1', 'fast_schema_evolution' = 'true');");
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "sc_uniq_sort_key");
        doTestSortKeyUpdatedAfterAddKeyColumn(tbl, "test.sc_uniq_sort_key");
    }

    private void doTestSortKeyUpdatedAfterAddKeyColumn(OlapTable tbl, String qualifiedName) throws Exception {
        Assertions.assertEquals(Arrays.asList(3, 2, 1, 0), tbl.getIndexMetaByIndexId(tbl.getBaseIndexId()).getSortKeyIdxes());

        // Add a key column with positional clause (AFTER k2)
        executeAlterAndWaitDone("alter table " + qualifiedName + " add column k_new1 int key after k2");
        Assertions.assertEquals("k0", tbl.getBaseSchema().get(0).getName());
        Assertions.assertEquals("k1", tbl.getBaseSchema().get(1).getName());
        Assertions.assertEquals("k2", tbl.getBaseSchema().get(2).getName());
        Assertions.assertEquals("k_new1", tbl.getBaseSchema().get(3).getName());
        Assertions.assertEquals("k3", tbl.getBaseSchema().get(4).getName());
        assertSortKey(tbl, Arrays.asList(4, 2, 1, 0, 3));

        // Add another key column at the default (last key) position
        executeAlterAndWaitDone("alter table " + qualifiedName + " add column k_new2 int key default '0'");
        Assertions.assertEquals("k0", tbl.getBaseSchema().get(0).getName());
        Assertions.assertEquals("k1", tbl.getBaseSchema().get(1).getName());
        Assertions.assertEquals("k2", tbl.getBaseSchema().get(2).getName());
        Assertions.assertEquals("k_new1", tbl.getBaseSchema().get(3).getName());
        Assertions.assertEquals("k3", tbl.getBaseSchema().get(4).getName());
        Assertions.assertEquals("k_new2", tbl.getBaseSchema().get(5).getName());
        assertSortKey(tbl, Arrays.asList(4, 2, 1, 0, 3, 5));
    }


    private void assertSortKey(OlapTable tbl, List<Integer> expectedSortKeyIndexes) {
        List<Column> columns = tbl.getBaseSchema();
        MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
        Assertions.assertEquals(expectedSortKeyIndexes, indexMeta.getSortKeyIdxes());
        List<Integer> sortKeyUniqueIds = indexMeta.getSortKeyUniqueIds();
        Assertions.assertNotNull(sortKeyUniqueIds);
        Assertions.assertEquals(expectedSortKeyIndexes.size(), sortKeyUniqueIds.size());
        for (int i = 0; i < expectedSortKeyIndexes.size(); i++) {
            Assertions.assertEquals(columns.get(expectedSortKeyIndexes.get(i)).getUniqueId(),
                    (int) sortKeyUniqueIds.get(i));
        }
    }

    private void executeAlterAndWaitDone(String sql) throws Exception {
        AlterTableStmt alterStmt = (AlterTableStmt) parseAndAnalyzeStmt(sql);
        DDLStmtExecutor.execute(alterStmt, connectContext);
        jobSize++;
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        waitAlterJobDone(alterJobs);
    }

    @Test
    public void testModifyBloomFilterColumnToUnsupportedTypeIsRejected() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        for (String keys : new String[] {"DUPLICATE KEY(k)", "PRIMARY KEY(k)"}) {
            for (boolean fast : new boolean[] {false, true}) {
                for (String target : new String[] {"DOUBLE", "FLOAT", "TINYINT", "JSON"}) {
                    String name = "bf_reject_" + keys.charAt(0) + (fast ? "_fast_" : "_slow_")
                            + target.toLowerCase();
                    createTable("CREATE TABLE test." + name + " (k BIGINT NOT NULL, x VARCHAR(16)) "
                            + keys + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ("
                            + "'replication_num'='1', 'bloom_filter_columns'='x', "
                            + "'fast_schema_evolution'='" + fast + "')");
                    OlapTable table = (OlapTable) db.getTable(name);
                    AlterTableStmt stmt = (AlterTableStmt) parseAndAnalyzeStmt(
                            "ALTER TABLE test." + name + " MODIFY COLUMN x " + target);
                    DdlException e = Assertions.assertThrows(DdlException.class,
                            () -> handler.process(stmt.getAlterClauseList(), db, table), name);
                    Assertions.assertEquals("Invalid bloom filter column 'x': unsupported type " + target,
                            e.getMessage());
                    Assertions.assertEquals(PrimitiveType.VARCHAR, table.getColumn("x").getPrimitiveType());
                    Assertions.assertTrue(table.getBfColumnNames().contains("x"));
                    Assertions.assertEquals(OlapTableState.NORMAL, table.getState());
                    Assertions.assertFalse(hasAlterJobOf(handler, table));
                }
            }
        }
    }

    private OlapTable createBloomFilterTable(String name, String sourceType, boolean fast) throws Exception {
        createTable("CREATE TABLE test." + name + " (k BIGINT NOT NULL, x " + sourceType + ", y INT) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ("
                + "'replication_num'='1', 'bloom_filter_columns'='x', "
                + "'fast_schema_evolution'='" + fast + "')");
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test").getTable(name);
    }

    @Test
    public void testModifyBloomFilterColumnToSupportedTypeIsAccepted() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        String[][] types = { {"INT", "BIGINT"}, {"INT", "LARGEINT"}, {"INT", "VARCHAR(32)"},
                {"VARCHAR(16)", "SMALLINT"}, {"VARCHAR(16)", "INT"}, {"VARCHAR(16)", "BIGINT"},
                {"VARCHAR(16)", "LARGEINT"}, {"VARCHAR(16)", "DATE"}, {"DATE", "DATETIME"},
                {"VARCHAR(16)", "DECIMAL(9,2)"}, {"VARCHAR(16)", "DECIMAL(18,2)"},
                {"VARCHAR(16)", "DECIMAL(38,2)"}, {"VARCHAR(16)", "DECIMAL(76,2)"}};
        for (int i = 0; i < types.length; i++) {
            String name = "bf_accept_" + i;
            OlapTable table = createBloomFilterTable(name, types[i][0], false);
            PrimitiveType originalType = table.getColumn("x").getPrimitiveType();
            Set<ColumnId> ids = table.getBfColumnIds();
            int jobsBefore = handler.getAlterJobsV2().size();
            AlterTableStmt stmt = (AlterTableStmt) parseAndAnalyzeStmt(
                    "ALTER TABLE test." + name + " MODIFY COLUMN x " + types[i][1]);
            Assertions.assertNotNull(handler.analyzeAndCreateJob(stmt.getAlterClauseList(), db, table), name);
            Assertions.assertEquals(jobsBefore, handler.getAlterJobsV2().size(), name);
            Assertions.assertEquals(originalType, table.getColumn("x").getPrimitiveType(), name);
            Assertions.assertEquals(ids, table.getBfColumnIds(), name);
            Assertions.assertEquals(OlapTableState.NORMAL, table.getState(), name);
        }
    }

    @Test
    public void testBloomFilterTypeChangeUsesFinalColumns() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        OlapTable table = createBloomFilterTable("bf_remove_bundled", "INT", false);
        Map<Long, AlterJobV2> jobsBefore = new HashMap<>(handler.getAlterJobsV2());
        try {
            String bundledSql = "ALTER TABLE test.bf_remove_bundled MODIFY COLUMN x DOUBLE "
                    + "PROPERTIES ('bloom_filter_columns'='')";
            AlterTableStmt bundled = (AlterTableStmt) parseAndAnalyzeStmt(bundledSql);
            Assertions.assertNotNull(handler.analyzeAndCreateJob(bundled.getAlterClauseList(), db, table));
            Assertions.assertEquals(Type.INT, table.getColumn("x").getType());
            Assertions.assertTrue(table.getBfColumnNames().contains("x"));
            // This branch mutates the analyzed column while creating the job, so execute a fresh statement.
            AlterTableStmt applyBundled = (AlterTableStmt) parseAndAnalyzeStmt(bundledSql);
            handler.process(applyBundled.getAlterClauseList(), db, table);
            waitAlterJobDone(handler.getAlterJobsV2());
            Assertions.assertEquals(PrimitiveType.DOUBLE, table.getColumn("x").getPrimitiveType());
            Assertions.assertTrue(table.getBfColumnIds() == null || table.getBfColumnIds().isEmpty());
            Assertions.assertEquals(OlapTableState.NORMAL, table.getState());

            OlapTable separate = createBloomFilterTable("bf_remove_separate", "INT", false);
            AlterTableStmt remove = (AlterTableStmt) parseAndAnalyzeStmt(
                    "ALTER TABLE test.bf_remove_separate SET ('bloom_filter_columns'='')");
            handler.process(remove.getAlterClauseList(), db, separate);
            waitAlterJobDone(handler.getAlterJobsV2());
            Assertions.assertTrue(separate.getBfColumnIds() == null || separate.getBfColumnIds().isEmpty());
            AlterTableStmt convert = (AlterTableStmt) parseAndAnalyzeStmt(
                    "ALTER TABLE test.bf_remove_separate MODIFY COLUMN x DOUBLE");
            Assertions.assertNotNull(handler.analyzeAndCreateJob(convert.getAlterClauseList(), db, separate));
        } finally {
            handler.getAlterJobsV2().keySet().removeIf(id -> !jobsBefore.containsKey(id));
        }
    }

    @Test
    public void testBloomFilterTypeChangeMatchesColumnIdsCaseInsensitively() throws Exception {
        OlapTable table = createBloomFilterTable("bf_case_reject", "VARCHAR(16)", true);
        Set<ColumnId> ids = new TreeSet<>(ColumnId.CASE_INSENSITIVE_ORDER);
        ids.add(ColumnId.create("X"));
        table.setBloomFilterInfo(ids, table.getBfFpp());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        AlterTableStmt stmt = (AlterTableStmt) parseAndAnalyzeStmt(
                "ALTER TABLE test.bf_case_reject MODIFY COLUMN x DOUBLE");
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> handler.process(stmt.getAlterClauseList(), db, table));
        Assertions.assertEquals("Invalid bloom filter column 'x': unsupported type DOUBLE", e.getMessage());
        Assertions.assertFalse(hasAlterJobOf(handler, table));
    }

    @Test
    public void testUnchangedLegacyBloomFilterColumnDoesNotBlockAlter() throws Exception {
        createTable("CREATE TABLE test.bf_legacy (k BIGINT NOT NULL, x DOUBLE) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1', "
                + "'fast_schema_evolution'='false')");
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) db.getTable("bf_legacy");
        table.setBloomFilterInfo(Set.of(table.getColumn("x").getColumnId()), 0.05);
        SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        AlterTableStmt stmt = (AlterTableStmt) parseAndAnalyzeStmt(
                "ALTER TABLE test.bf_legacy ADD COLUMN y INT");
        Assertions.assertNotNull(handler.analyzeAndCreateJob(stmt.getAlterClauseList(), db, table));
        Assertions.assertEquals(PrimitiveType.DOUBLE, table.getColumn("x").getPrimitiveType());
    }

    @Test
    public void testIncompatibleBloomFilterTypeChangeKeepsItsOwnMessage() throws Exception {
        OlapTable table = createBloomFilterTable("bf_date_incompat", "DATE", true);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        AlterTableStmt stmt = (AlterTableStmt) parseAndAnalyzeStmt(
                "ALTER TABLE test.bf_date_incompat MODIFY COLUMN x DOUBLE");
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> handler.process(stmt.getAlterClauseList(), db, table));
        Assertions.assertFalse(e.getMessage().startsWith("Invalid bloom filter column"), e.getMessage());
        Assertions.assertEquals(PrimitiveType.DATE, table.getColumn("x").getPrimitiveType());
        Assertions.assertFalse(hasAlterJobOf(handler, table));
        Assertions.assertEquals(OlapTableState.NORMAL, table.getState());
    }

    @Test
    public void testNgramBloomFilterWithoutPlainBloomFilterKeepsExistingAlterBehavior() throws Exception {
        createTable("CREATE TABLE test.ngram_only_bf (k BIGINT NOT NULL, x VARCHAR(16), "
                + "INDEX ix_x (x) USING NGRAMBF ('gram_num'='4', 'bloom_filter_fpp'='0.05')) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1', 'fast_schema_evolution'='false')");
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) db.getTable("ngram_only_bf");
        Assertions.assertNull(table.getBfColumnIds());
        SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        AlterTableStmt stmt = (AlterTableStmt) parseAndAnalyzeStmt(
                "ALTER TABLE test.ngram_only_bf MODIFY COLUMN x VARCHAR(32)");
        Assertions.assertNotNull(handler.analyzeAndCreateJob(stmt.getAlterClauseList(), db, table));
        Assertions.assertEquals(16, ((ScalarType) table.getColumn("x").getType()).getLength());
    }

    private boolean hasAlterJobOf(SchemaChangeHandler handler, OlapTable table) {
        return handler.getAlterJobsV2().values().stream().anyMatch(job -> job.getTableId() == table.getId());
    }

    @Test
    public void testFastSchemaEvolutionInvalidatesGlobalDict() throws Exception {
        // A schema change does not bump the partition visible version that a global dict's validity is
        // checked against, so after a fast ADD/DROP COLUMN the collected dicts stayed "valid" while new
        // rowsets were encoded independently, and decoding them failed with "Dict Decode failed". Both the
        // leader and a follower replaying the change go through modifyTableAddOrDrop, so both must
        // invalidate the dict of every varchar column, including ones the change did not touch.
        createTable("CREATE TABLE test.t_fast_dict_invalidate (\n"
                + "  k1 INT NOT NULL,\n"
                + "  name VARCHAR(50),\n"
                + "  v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\",\n"
                + "  \"fast_schema_evolution\" = \"true\"\n"
                + ");");

        List<String> invalidated = new ArrayList<>();
        new MockUp<CacheDictManager>() {
            @Mock
            public void removeGlobalDict(OlapTable table, ColumnId columnName) {
                invalidated.add(columnName.getId());
            }
        };

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getLocalMetastore().getDb("test");
        OlapTable tbl = (OlapTable) globalStateMgr.getLocalMetastore().getTable(db.getFullName(),
                "t_fast_dict_invalidate");
        Map<Long, List<Column>> indexSchemaMap = new HashMap<>();
        Map<Long, Long> indexToNewSchemaId = new HashMap<>();
        for (Map.Entry<Long, List<Column>> entry : tbl.getIndexIdToSchema().entrySet()) {
            indexSchemaMap.put(entry.getKey(), new LinkedList<>(entry.getValue()));
            indexToNewSchemaId.put(entry.getKey(), globalStateMgr.getNextId());
        }
        List<Index> newIndexes = tbl.getCopiedIndexes();
        SchemaChangeHandler handler = globalStateMgr.getAlterJobMgr().getSchemaChangeHandler();
        try {
            assertFastSchemaEvolutionInvalidatesGlobalDict(handler, db, tbl, indexSchemaMap, newIndexes,
                    indexToNewSchemaId, invalidated);
        } finally {
            // the other tests count every alter job the handler holds
            handler.getAlterJobsV2().values().removeIf(job -> job.getTableId() == tbl.getId());
        }
    }

    private void assertFastSchemaEvolutionInvalidatesGlobalDict(SchemaChangeHandler handler, Database db,
                                                                OlapTable tbl,
                                                                Map<Long, List<Column>> indexSchemaMap,
                                                                List<Index> newIndexes,
                                                                Map<Long, Long> indexToNewSchemaId,
                                                                List<String> invalidated) throws Exception {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        // leader
        handler.modifyTableAddOrDrop(db, tbl, indexSchemaMap, newIndexes, globalStateMgr.getNextId(),
                indexToNewSchemaId, false, -1);
        Assertions.assertTrue(invalidated.contains("name"), "varchar column must be invalidated: " + invalidated);
        Assertions.assertFalse(invalidated.contains("k1"), "non-varchar column must not be invalidated");
        Assertions.assertFalse(invalidated.contains("v1"), "non-varchar column must not be invalidated");

        // follower replaying the same change
        invalidated.clear();
        handler.modifyTableAddOrDrop(db, tbl, indexSchemaMap, newIndexes, globalStateMgr.getNextId(),
                indexToNewSchemaId, true, 101);
        Assertions.assertTrue(invalidated.contains("name"), "replay must invalidate too: " + invalidated);
    }
}
