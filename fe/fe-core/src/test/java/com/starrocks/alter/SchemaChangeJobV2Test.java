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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/alter/SchemaChangeJobV2Test.java

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

import com.google.common.collect.Maps;
import com.starrocks.alter.AlterJobV2.JobState;
import com.starrocks.backup.CatalogMocker;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Partition.PartitionState;
import com.starrocks.catalog.Replica;
import com.starrocks.common.DdlException;
import com.starrocks.common.SchemaVersionAndHash;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SchemaChangeJobV2Test extends DDLTestBase {
    private static String fileName = "./SchemaChangeV2Test";
    private AlterTableStmt alterTableStmt;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    private static final Logger LOG = LogManager.getLogger(SchemaChangeJobV2Test.class);

    @Before
    public void setUp() throws Exception {
        super.setUp();
        String stmt = "alter table testTable1 add column add_v int default '1' after v3";
        alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
    }

    @After
    public void clear() {
        GlobalStateMgr.getCurrentState().getSchemaChangeHandler().clearJobs();
    }

    @Test
    public void testAddSchemaChange() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);

        schemaChangeHandler.process(alterTableStmt.getOps(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());
    }

    // start a schema change, then finished
    @Test
    public void testSchemaChange1() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);
        olapTable.setUseFastSchemaEvolution(false);
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable1);

        schemaChangeHandler.process(alterTableStmt.getOps(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        SchemaChangeJobV2 schemaChangeJob = (SchemaChangeJobV2) alterJobsV2.values().stream().findAny().get();
        alterJobsV2.clear();

        MaterializedIndex baseIndex = testPartition.getBaseIndex();
        assertEquals(IndexState.NORMAL, baseIndex.getState());
        assertEquals(PartitionState.NORMAL, testPartition.getState());
        assertEquals(OlapTableState.SCHEMA_CHANGE, olapTable.getState());

        LocalTablet baseTablet = (LocalTablet) baseIndex.getTablets().get(0);
        List<Replica> replicas = baseTablet.getImmutableReplicas();
        Replica replica1 = replicas.get(0);

        assertEquals(1, replica1.getVersion());
        assertEquals(-1, replica1.getLastFailedVersion());
        assertEquals(1, replica1.getLastSuccessVersion());

        // runPendingJob
        schemaChangeJob.runPendingJob();
        Assert.assertEquals(JobState.WAITING_TXN, schemaChangeJob.getJobState());
        Assert.assertEquals(2, testPartition.getMaterializedIndices(IndexExtState.ALL).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.VISIBLE).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.SHADOW).size());

        // runWaitingTxnJob
        schemaChangeJob.runWaitingTxnJob();
        Assert.assertEquals(JobState.RUNNING, schemaChangeJob.getJobState());

        int retryCount = 0;
        int maxRetry = 5;
        while (retryCount < maxRetry) {
            ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
            schemaChangeJob.runRunningJob();
            if (schemaChangeJob.getJobState() == JobState.FINISHED) {
                break;
            }
            retryCount++;
            LOG.info("testSchemaChange1 is waiting for JobState retryCount:" + retryCount);
        }

        // finish alter tasks
        Assert.assertEquals(JobState.FINISHED, schemaChangeJob.getJobState());
    }

    @Test
    public void testSchemaChangeWhileTabletNotStable() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);
        olapTable.setUseFastSchemaEvolution(false);
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable1);

        schemaChangeHandler.process(alterTableStmt.getOps(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        SchemaChangeJobV2 schemaChangeJob = (SchemaChangeJobV2) alterJobsV2.values().stream().findAny().get();
        alterJobsV2.clear();

        MaterializedIndex baseIndex = testPartition.getBaseIndex();
        assertEquals(IndexState.NORMAL, baseIndex.getState());
        assertEquals(PartitionState.NORMAL, testPartition.getState());
        assertEquals(OlapTableState.SCHEMA_CHANGE, olapTable.getState());

        LocalTablet baseTablet = (LocalTablet) baseIndex.getTablets().get(0);
        List<Replica> replicas = baseTablet.getImmutableReplicas();
        Replica replica1 = replicas.get(0);

        assertEquals(1, replica1.getVersion());
        assertEquals(-1, replica1.getLastFailedVersion());
        assertEquals(1, replica1.getLastSuccessVersion());

        // runPendingJob
        replica1.setState(Replica.ReplicaState.DECOMMISSION);
        schemaChangeJob.runPendingJob();
        Assert.assertEquals(JobState.PENDING, schemaChangeJob.getJobState());

        // table is stable runPendingJob again
        replica1.setState(Replica.ReplicaState.NORMAL);
        schemaChangeJob.runPendingJob();
        Assert.assertEquals(JobState.WAITING_TXN, schemaChangeJob.getJobState());
        Assert.assertEquals(2, testPartition.getMaterializedIndices(IndexExtState.ALL).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.VISIBLE).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.SHADOW).size());

        // runWaitingTxnJob
        schemaChangeJob.runWaitingTxnJob();
        Assert.assertEquals(JobState.RUNNING, schemaChangeJob.getJobState());

        int retryCount = 0;
        int maxRetry = 5;
        while (retryCount < maxRetry) {
            ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
            schemaChangeJob.runRunningJob();
            if (schemaChangeJob.getJobState() == JobState.FINISHED) {
                break;
            }
            retryCount++;
            LOG.info("testSchemaChange1 is waiting for JobState retryCount:" + retryCount);
        }
    }

    @Test
    public void testModifyDynamicPartitionNormal() throws UserException {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        Map<String, String> properties = new HashMap<>();
        properties.put(DynamicPartitionProperty.ENABLE, "true");
        properties.put(DynamicPartitionProperty.TIME_UNIT, "day");
        properties.put(DynamicPartitionProperty.END, "3");
        properties.put(DynamicPartitionProperty.PREFIX, "p");
        properties.put(DynamicPartitionProperty.BUCKETS, "30");
        alterClauses.add(new ModifyTablePropertiesClause(properties));
        Database db = CatalogMocker.mockDb();
        OlapTable olapTable = (OlapTable) db.getTable(CatalogMocker.TEST_TBL2_ID);
        olapTable.setUseFastSchemaEvolution(false);
        schemaChangeHandler.process(alterClauses, db, olapTable);
        Assert.assertTrue(olapTable.getTableProperty().getDynamicPartitionProperty().isExist());
        Assert.assertTrue(olapTable.getTableProperty().getDynamicPartitionProperty().getEnable());
        Assert.assertEquals("day", olapTable.getTableProperty().getDynamicPartitionProperty().getTimeUnit());
        Assert.assertEquals(3, olapTable.getTableProperty().getDynamicPartitionProperty().getEnd());
        Assert.assertEquals("p", olapTable.getTableProperty().getDynamicPartitionProperty().getPrefix());
        Assert.assertEquals(30, olapTable.getTableProperty().getDynamicPartitionProperty().getBuckets());

        // set dynamic_partition.enable = false
        ArrayList<AlterClause> tmpAlterClauses = new ArrayList<>();
        properties.put(DynamicPartitionProperty.ENABLE, "false");
        tmpAlterClauses.add(new ModifyTablePropertiesClause(properties));
        schemaChangeHandler.process(tmpAlterClauses, db, olapTable);
        Assert.assertFalse(olapTable.getTableProperty().getDynamicPartitionProperty().getEnable());
        // set dynamic_partition.time_unit = week
        tmpAlterClauses = new ArrayList<>();
        properties.put(DynamicPartitionProperty.TIME_UNIT, "week");
        tmpAlterClauses.add(new ModifyTablePropertiesClause(properties));
        schemaChangeHandler.process(tmpAlterClauses, db, olapTable);
        Assert.assertEquals("week", olapTable.getTableProperty().getDynamicPartitionProperty().getTimeUnit());
        // set dynamic_partition.end = 10
        tmpAlterClauses = new ArrayList<>();
        properties.put(DynamicPartitionProperty.END, "10");
        tmpAlterClauses.add(new ModifyTablePropertiesClause(properties));
        schemaChangeHandler.process(tmpAlterClauses, db, olapTable);
        Assert.assertEquals(10, olapTable.getTableProperty().getDynamicPartitionProperty().getEnd());
        // set dynamic_partition.prefix = p1
        tmpAlterClauses = new ArrayList<>();
        properties.put(DynamicPartitionProperty.PREFIX, "p1");
        tmpAlterClauses.add(new ModifyTablePropertiesClause(properties));
        schemaChangeHandler.process(tmpAlterClauses, db, olapTable);
        Assert.assertEquals("p1", olapTable.getTableProperty().getDynamicPartitionProperty().getPrefix());
        // set dynamic_partition.buckets = 3
        tmpAlterClauses = new ArrayList<>();
        properties.put(DynamicPartitionProperty.BUCKETS, "3");
        tmpAlterClauses.add(new ModifyTablePropertiesClause(properties));
        schemaChangeHandler.process(tmpAlterClauses, db, olapTable);
        Assert.assertEquals(3, olapTable.getTableProperty().getDynamicPartitionProperty().getBuckets());
    }

    public void modifyDynamicPartitionWithoutTableProperty(String propertyKey, String propertyValue, String expectErrMsg)
            throws UserException {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        Map<String, String> properties = new HashMap<>();
        properties.put(propertyKey, propertyValue);
        alterClauses.add(new ModifyTablePropertiesClause(properties));

        Database db = CatalogMocker.mockDb();
        OlapTable olapTable = (OlapTable) db.getTable(CatalogMocker.TEST_TBL2_ID);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage(expectErrMsg);
        schemaChangeHandler.process(alterClauses, db, olapTable);
    }

    @Test
    public void testModifyDynamicPartitionWithoutTableProperty() throws UserException {
        modifyDynamicPartitionWithoutTableProperty(DynamicPartitionProperty.ENABLE, "false",
                "not a dynamic partition table");
        modifyDynamicPartitionWithoutTableProperty(DynamicPartitionProperty.TIME_UNIT, "day",
                DynamicPartitionProperty.ENABLE);
        modifyDynamicPartitionWithoutTableProperty(DynamicPartitionProperty.END, "3", DynamicPartitionProperty.ENABLE);
        modifyDynamicPartitionWithoutTableProperty(DynamicPartitionProperty.PREFIX, "p",
                DynamicPartitionProperty.ENABLE);
        modifyDynamicPartitionWithoutTableProperty(DynamicPartitionProperty.BUCKETS, "30",
                DynamicPartitionProperty.ENABLE);
    }

    @Test
    public void testSerializeOfSchemaChangeJob() throws IOException {
        // prepare file
        File file = new File(fileName);
        file.createNewFile();
        file.deleteOnExit();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        SchemaChangeJobV2 schemaChangeJobV2 = new SchemaChangeJobV2(1, 1, 1, "test", 600000);
        Deencapsulation.setField(schemaChangeJobV2, "jobState", AlterJobV2.JobState.FINISHED);
        Map<Long, SchemaVersionAndHash> indexSchemaVersionAndHashMap = Maps.newHashMap();
        indexSchemaVersionAndHashMap.put(Long.valueOf(1000), new SchemaVersionAndHash(10, 20));
        Deencapsulation.setField(schemaChangeJobV2, "indexSchemaVersionAndHashMap", indexSchemaVersionAndHashMap);

        // write schema change job
        schemaChangeJobV2.write(out);
        out.flush();
        out.close();

        // read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        SchemaChangeJobV2 result = (SchemaChangeJobV2) AlterJobV2.read(in);
        Assert.assertEquals(1, result.getJobId());
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, result.getJobState());

        Assert.assertNotNull(Deencapsulation.getField(result, "physicalPartitionIndexMap"));
        Assert.assertNotNull(Deencapsulation.getField(result, "physicalPartitionIndexTabletMap"));

        Map<Long, SchemaVersionAndHash> map = Deencapsulation.getField(result, "indexSchemaVersionAndHashMap");
        Assert.assertEquals(10, map.get(1000L).schemaVersion);
        Assert.assertEquals(20, map.get(1000L).schemaHash);
    }
}
