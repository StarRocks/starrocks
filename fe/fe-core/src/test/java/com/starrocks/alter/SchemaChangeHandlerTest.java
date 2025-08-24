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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FlatJsonConfig;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.utframe.TestWithFeService;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
        String addKeyColStmtStr = "alter table test.sc_agg add column new_k1 int default '1'";
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
                    () -> ((SchemaChangeHandler) GlobalStateMgr.getCurrentState().getAlterJobMgr().getSchemaChangeHandler())
                                .modifyTableAddOrDrop(db, tbl, indexSchemaMap, newIndexes, 100, 100,
                                            indexToNewSchemaId, false));
        jobSize++;
        Assertions.assertEquals(jobSize, alterJobs.size());

        Assertions.assertDoesNotThrow(
                    () -> ((SchemaChangeHandler) GlobalStateMgr.getCurrentState().getAlterJobMgr().getSchemaChangeHandler())
                                .modifyTableAddOrDrop(db, tbl, indexSchemaMap, newIndexes, 101, 101,
                                            indexToNewSchemaId, true));
        jobSize++;
        Assertions.assertEquals(jobSize, alterJobs.size());

        OlapTableState beforeState = tbl.getState();
        tbl.setState(OlapTableState.ROLLUP);
        Assertions.assertThrows(DdlException.class,
                    () -> ((SchemaChangeHandler) GlobalStateMgr.getCurrentState().getAlterJobMgr().getSchemaChangeHandler())
                                .modifyTableAddOrDrop(db, tbl, indexSchemaMap, newIndexes, 102, 102, indexToNewSchemaId,
                                            false));
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
}
