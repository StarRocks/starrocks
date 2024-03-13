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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.utframe.TestWithFeService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.runners.MethodSorters;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SchemaChangeHandlerTest extends TestWithFeService {

    private static final Logger LOG = LogManager.getLogger(SchemaChangeHandlerTest.class);
    private int jobSize = 0;

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
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());

            Database db = GlobalStateMgr.getCurrentState().getDb(alterJobV2.getDbId());
            OlapTable tbl = (OlapTable) db.getTable(alterJobV2.getTableId());
            while (tbl.getState() != OlapTable.OlapTableState.NORMAL) {
                Thread.sleep(1000);
            }
        }
    }

    @Test
    public void testAggAddOrDropColumn() throws Exception {
        LOG.info("dbName: {}", GlobalStateMgr.getCurrentState().getLocalMetastore().listDbNames());

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable tbl = (OlapTable) db.getTable("sc_agg");
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            Assertions.assertNotNull(tbl);
            System.out.println(tbl.getName());
            Assertions.assertEquals("StarRocks", tbl.getEngine());
            Assertions.assertEquals(9, tbl.getBaseSchema().size());
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }

        //process agg add value column schema change
        String addValColStmtStr = "alter table test.sc_agg add column new_v1 int MAX default '0'";
        AlterTableStmt addValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(addValColStmtStr);
        GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(addValColStmt);
        jobSize++;
        // check alter job, do not create job
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        Assertions.assertEquals(jobSize, alterJobs.size());

        locker.lockDatabase(db, LockType.READ);
        try {
            Assertions.assertEquals(10, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }

        //process agg add  key column schema change
        String addKeyColStmtStr = "alter table test.sc_agg add column new_k1 int default '1'";
        AlterTableStmt addKeyColStmt = (AlterTableStmt) parseAndAnalyzeStmt(addKeyColStmtStr);
        GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(addKeyColStmt);

        //check alter job
        jobSize++;
        Assertions.assertEquals(jobSize, alterJobs.size());
        waitAlterJobDone(alterJobs);

        locker.lockDatabase(db, LockType.READ);
        try {
            Assertions.assertEquals(11, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }

        //process agg drop value column schema change
        String dropValColStmtStr = "alter table test.sc_agg drop column new_v1";
        AlterTableStmt dropValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(dropValColStmtStr);
        GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(dropValColStmt);
        jobSize++;
        //check alter job, do not create job
        LOG.info("alterJobs:{}", alterJobs);
        Assertions.assertEquals(jobSize, alterJobs.size());

        locker.lockDatabase(db, LockType.READ);
        try {
            Assertions.assertEquals(10, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }

        //process agg drop key column with replace schema change, expect exception.
        String dropKeyColStmtStr = "alter table test.sc_agg drop column new_k1";
        AlterTableStmt dropKeyColStmt = (AlterTableStmt) parseAndAnalyzeStmt(dropKeyColStmtStr);
        Assertions.assertThrows(Exception.class,
                () -> GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(dropKeyColStmt));

        LOG.info("getIndexIdToSchema 1: {}", tbl.getIndexIdToSchema());

        //process agg drop value column with rollup schema change
        String dropRollUpValColStmtStr = "alter table test.sc_agg drop column max_dwell_time";
        AlterTableStmt dropRollUpValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(dropRollUpValColStmtStr);
        GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(dropRollUpValColStmt);
        jobSize++;
        //check alter job, need create job
        LOG.info("alterJobs:{}", alterJobs);
        Assertions.assertEquals(jobSize, alterJobs.size());

        locker.lockDatabase(db, LockType.READ);
        try {
            Assertions.assertEquals(9, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
    }

    @Test
    public void testUniqAddOrDropColumn() throws Exception {

        LOG.info("dbName: {}", GlobalStateMgr.getCurrentState().getLocalMetastore().listDbNames());

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable tbl = (OlapTable) db.getTable("sc_uniq");
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            Assertions.assertNotNull(tbl);
            System.out.println(tbl.getName());
            Assertions.assertEquals("StarRocks", tbl.getEngine());
            Assertions.assertEquals(8, tbl.getBaseSchema().size());
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }

        //process uniq add value column schema change
        String addValColStmtStr = "alter table test.sc_uniq add column new_v1 int default '0'";
        AlterTableStmt addValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(addValColStmtStr);
        GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(addValColStmt);
        jobSize++;
        //check alter job, do not create job
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        LOG.info("alterJobs:{}", alterJobs);
        Assertions.assertEquals(jobSize, alterJobs.size());

        locker.lockDatabase(db, LockType.READ);
        try {
            Assertions.assertEquals(9, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }

        //process uniq drop val column schema change
        String dropValColStmtStr = "alter table test.sc_uniq drop column new_v1";
        AlterTableStmt dropValColStm = (AlterTableStmt) parseAndAnalyzeStmt(dropValColStmtStr);
        GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(dropValColStm);
        jobSize++;
        //check alter job
        Assertions.assertEquals(jobSize, alterJobs.size());
        locker.lockDatabase(db, LockType.READ);
        try {
            Assertions.assertEquals(8, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
    }

    @Test
    public void testDupAddOrDropColumn() throws Exception {

        LOG.info("dbName: {}", GlobalStateMgr.getCurrentState().getLocalMetastore().listDbNames());

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable tbl = (OlapTable) db.getTable("sc_dup");
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            Assertions.assertNotNull(tbl);
            System.out.println(tbl.getName());
            Assertions.assertEquals("StarRocks", tbl.getEngine());
            Assertions.assertEquals(6, tbl.getBaseSchema().size());
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }

        //process uniq add value column schema change
        String addValColStmtStr = "alter table test.sc_dup add column new_v1 int default '0'";
        AlterTableStmt addValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(addValColStmtStr);
        GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(addValColStmt);
        jobSize++;
        //check alter job, do not create job
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        LOG.info("alterJobs:{}", alterJobs);
        Assertions.assertEquals(jobSize, alterJobs.size());

        locker.lockDatabase(db, LockType.READ);
        try {
            Assertions.assertEquals(7, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }

        //process uniq drop val column schema change
        String dropValColStmtStr = "alter table test.sc_dup drop column new_v1";
        AlterTableStmt dropValColStm = (AlterTableStmt) parseAndAnalyzeStmt(dropValColStmtStr);
        GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(dropValColStm);
        jobSize++;
        //check alter job
        Assertions.assertEquals(jobSize, alterJobs.size());
        locker.lockDatabase(db, LockType.READ);
        try {
            Assertions.assertEquals(6, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
    }

    @Test
    public void testModifyTableAddOrDropColumns() {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getDb("test");
        OlapTable tbl = (OlapTable) db.getTable("sc_dup2");
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
                        .modifyTableAddOrDropColumns(db, tbl, indexSchemaMap, newIndexes, 100, 100,
                                                     indexToNewSchemaId, false));
        jobSize++;
        Assertions.assertEquals(jobSize, alterJobs.size());

        Assertions.assertDoesNotThrow(
                () -> ((SchemaChangeHandler) GlobalStateMgr.getCurrentState().getAlterJobMgr().getSchemaChangeHandler())
                        .modifyTableAddOrDropColumns(db, tbl, indexSchemaMap, newIndexes, 101, 101,
                                                     indexToNewSchemaId, true));
        jobSize++;
        Assertions.assertEquals(jobSize, alterJobs.size());

        OlapTableState beforeState = tbl.getState();
        tbl.setState(OlapTableState.ROLLUP);
        Assertions.assertThrows(DdlException.class,
                () -> ((SchemaChangeHandler) GlobalStateMgr.getCurrentState().getAlterJobMgr().getSchemaChangeHandler())
                        .modifyTableAddOrDropColumns(db, tbl, indexSchemaMap, newIndexes, 102, 102, indexToNewSchemaId,
                                                     false));
        tbl.setState(beforeState);
    }

    @Test
    public void testSetPrimaryIndexCacheExpireSec() throws Exception {

        LOG.info("dbName: {}", GlobalStateMgr.getCurrentState().getLocalMetastore().listDbNames());

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable tbl = (OlapTable) db.getTable("sc_pk");
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            Assertions.assertNotNull(tbl);
            System.out.println(tbl.getName());
            Assertions.assertEquals("StarRocks", tbl.getEngine());
            Assertions.assertEquals(6, tbl.getBaseSchema().size());
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }

        //process set properties
        String addValColStmtStr = "alter table test.sc_pk set ('primary_index_cache_expire_sec' = '3600');";
        AlterTableStmt addValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(addValColStmtStr);
        GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(addValColStmt);

        try {
            String addValColStmtStr2 = "alter table test.sc_pk set ('primary_index_cache_expire_sec' = '-12');";
            AlterTableStmt addValColStmt2 = (AlterTableStmt) parseAndAnalyzeStmt(addValColStmtStr2);
            GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(addValColStmt2);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("Property primary_index_cache_expire_sec must not be less than 0"));
        }

        try {
            String addValColStmtStr3 = "alter table test.sc_pk set ('primary_index_cache_expire_sec' = 'asd');";
            AlterTableStmt addValColStmt3 = (AlterTableStmt) parseAndAnalyzeStmt(addValColStmtStr3);
            GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(addValColStmt3);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("Property primary_index_cache_expire_sec must be integer"));
        }
    }
}
