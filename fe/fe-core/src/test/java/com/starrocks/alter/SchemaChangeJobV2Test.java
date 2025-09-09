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

import com.google.common.collect.Lists;
import com.starrocks.alter.AlterJobV2.JobState;
import com.starrocks.backup.CatalogMocker;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Partition.PartitionState;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.ThreadUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.ReorderColumnsClause;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.utframe.MockedWarehouseManager;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResourceProvider;
import mockit.Mock;
import mockit.MockUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SchemaChangeJobV2Test extends DDLTestBase {
    private static final String TEST_FILE_NAME = SchemaChangeJobV2Test.class.getCanonicalName();
    private AlterTableStmt alterTableStmt;

    private static final Logger LOG = LogManager.getLogger(SchemaChangeJobV2Test.class);

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        String stmt = "alter table testTable1 add column add_v int default '1' after v3";
        alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
    }

    @AfterEach
    public void clear() {
        GlobalStateMgr.getCurrentState().getSchemaChangeHandler().clearJobs();
    }

    @Test
    public void testAddSchemaChange() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);

        schemaChangeHandler.process(alterTableStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assertions.assertEquals(1, alterJobsV2.size());
        Assertions.assertEquals(OlapTableState.NORMAL, olapTable.getState());
    }

    // start a schema change, then finished
    @Test
    public void testSchemaChange1() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);
        olapTable.setUseFastSchemaEvolution(false);
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable1);

        schemaChangeHandler.process(alterTableStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assertions.assertEquals(1, alterJobsV2.size());
        SchemaChangeJobV2 schemaChangeJob = (SchemaChangeJobV2) alterJobsV2.values().stream().findAny().get();
        alterJobsV2.clear();

        MaterializedIndex baseIndex = testPartition.getDefaultPhysicalPartition().getBaseIndex();
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
        Assertions.assertEquals(JobState.WAITING_TXN, schemaChangeJob.getJobState());
        Assertions.assertEquals(2, testPartition.getDefaultPhysicalPartition()
                .getMaterializedIndices(IndexExtState.ALL).size());
        Assertions.assertEquals(1, testPartition.getDefaultPhysicalPartition()
                .getMaterializedIndices(IndexExtState.VISIBLE).size());
        Assertions.assertEquals(1, testPartition.getDefaultPhysicalPartition()
                .getMaterializedIndices(IndexExtState.SHADOW).size());

        // runWaitingTxnJob
        schemaChangeJob.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, schemaChangeJob.getJobState());

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
        Assertions.assertEquals(JobState.FINISHED, schemaChangeJob.getJobState());
    }

    @Test
    public void testSchemaChangeWhileTabletNotStable() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);
        olapTable.setUseFastSchemaEvolution(false);
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable1);

        schemaChangeHandler.process(alterTableStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assertions.assertEquals(1, alterJobsV2.size());
        SchemaChangeJobV2 schemaChangeJob = (SchemaChangeJobV2) alterJobsV2.values().stream().findAny().get();
        alterJobsV2.clear();

        MaterializedIndex baseIndex = testPartition.getDefaultPhysicalPartition().getBaseIndex();
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
        Assertions.assertEquals(JobState.PENDING, schemaChangeJob.getJobState());

        // table is stable runPendingJob again
        replica1.setState(Replica.ReplicaState.NORMAL);
        schemaChangeJob.runPendingJob();
        Assertions.assertEquals(JobState.WAITING_TXN, schemaChangeJob.getJobState());
        Assertions.assertEquals(2, testPartition.getDefaultPhysicalPartition()
                .getMaterializedIndices(IndexExtState.ALL).size());
        Assertions.assertEquals(1, testPartition.getDefaultPhysicalPartition()
                .getMaterializedIndices(IndexExtState.VISIBLE).size());
        Assertions.assertEquals(1, testPartition.getDefaultPhysicalPartition()
                .getMaterializedIndices(IndexExtState.SHADOW).size());

        // runWaitingTxnJob
        schemaChangeJob.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, schemaChangeJob.getJobState());

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
    public void testModifyDynamicPartitionNormal() throws Exception {
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

        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(String dbName) {
                return db;
            }

            @Mock
            public Table getTable(String dbName, String tblName) {
                return olapTable;
            }
        };
        TableName tableName = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db.getFullName(), olapTable.getName());
        new AlterJobExecutor().process(new AlterTableStmt(tableName, alterClauses), ctx);

        //schemaChangeHandler.process(alterClauses, db, olapTable);
        Assertions.assertTrue(olapTable.getTableProperty().getDynamicPartitionProperty().isExists());
        Assertions.assertTrue(olapTable.getTableProperty().getDynamicPartitionProperty().isEnabled());
        Assertions.assertEquals("day", olapTable.getTableProperty().getDynamicPartitionProperty().getTimeUnit());
        Assertions.assertEquals(3, olapTable.getTableProperty().getDynamicPartitionProperty().getEnd());
        Assertions.assertEquals("p", olapTable.getTableProperty().getDynamicPartitionProperty().getPrefix());
        Assertions.assertEquals(30, olapTable.getTableProperty().getDynamicPartitionProperty().getBuckets());

        // set dynamic_partition.enable = false
        ArrayList<AlterClause> tmpAlterClauses = new ArrayList<>();
        properties.put(DynamicPartitionProperty.ENABLE, "false");
        tmpAlterClauses.add(new ModifyTablePropertiesClause(properties));
        new AlterJobExecutor().process(new AlterTableStmt(tableName, tmpAlterClauses), ctx);
        Assertions.assertFalse(olapTable.getTableProperty().getDynamicPartitionProperty().isEnabled());
        // set dynamic_partition.time_unit = week
        tmpAlterClauses = new ArrayList<>();
        properties.put(DynamicPartitionProperty.TIME_UNIT, "week");
        tmpAlterClauses.add(new ModifyTablePropertiesClause(properties));
        new AlterJobExecutor().process(new AlterTableStmt(tableName, tmpAlterClauses), ctx);

        Assertions.assertEquals("week", olapTable.getTableProperty().getDynamicPartitionProperty().getTimeUnit());
        // set dynamic_partition.end = 10
        tmpAlterClauses = new ArrayList<>();
        properties.put(DynamicPartitionProperty.END, "10");
        tmpAlterClauses.add(new ModifyTablePropertiesClause(properties));
        new AlterJobExecutor().process(new AlterTableStmt(tableName, tmpAlterClauses), ctx);

        Assertions.assertEquals(10, olapTable.getTableProperty().getDynamicPartitionProperty().getEnd());
        // set dynamic_partition.prefix = p1
        tmpAlterClauses = new ArrayList<>();
        properties.put(DynamicPartitionProperty.PREFIX, "p1");
        tmpAlterClauses.add(new ModifyTablePropertiesClause(properties));
        new AlterJobExecutor().process(new AlterTableStmt(tableName, tmpAlterClauses), ctx);

        Assertions.assertEquals("p1", olapTable.getTableProperty().getDynamicPartitionProperty().getPrefix());
        // set dynamic_partition.buckets = 3
        tmpAlterClauses = new ArrayList<>();
        properties.put(DynamicPartitionProperty.BUCKETS, "3");
        tmpAlterClauses.add(new ModifyTablePropertiesClause(properties));
        new AlterJobExecutor().process(new AlterTableStmt(tableName, tmpAlterClauses), ctx);

        Assertions.assertEquals(3, olapTable.getTableProperty().getDynamicPartitionProperty().getBuckets());
    }

    @Test
    public void testModifyDynamicPropertyTrim() throws Exception {
        String sql = "ALTER TABLE testDb1.testTable1 SET(\"dynamic_partition.buckets \"=\"1\")";
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        ModifyTablePropertiesClause modifyTablePropertiesClause = (ModifyTablePropertiesClause) stmt.getAlterClauseList().get(0);
        Assertions.assertEquals("1", modifyTablePropertiesClause.getProperties().get("dynamic_partition.buckets"));
    }

    public void modifyDynamicPartitionWithoutTableProperty(String propertyKey, String propertyValue, String expectErrMsg)
            throws StarRocksException {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        Map<String, String> properties = new HashMap<>();
        properties.put(propertyKey, propertyValue);
        alterClauses.add(new ModifyTablePropertiesClause(properties));

        Database db = CatalogMocker.mockDb();
        OlapTable olapTable = (OlapTable) db.getTable(CatalogMocker.TEST_TBL2_ID);

        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(String dbName) {
                return db;
            }

            @Mock
            public Table getTable(String dbName, String tblName) {
                return olapTable;
            }
        };
        TableName tableName = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db.getFullName(), olapTable.getName());

        try {
            new AlterJobExecutor().process(new AlterTableStmt(tableName, alterClauses), ctx);
        } catch (AlterJobException e) {
            assertThat(e.getMessage(), containsString(expectErrMsg));
        }
    }

    @Test
    public void testModifyDynamicPartitionWithoutTableProperty() throws AlterJobException, StarRocksException {
        modifyDynamicPartitionWithoutTableProperty(DynamicPartitionProperty.ENABLE, "false",
                    "not a dynamic partition table");
        // after exception of the last line, the following lines will not be executed, seems like a bug, just comment them out
        //        modifyDynamicPartitionWithoutTableProperty(DynamicPartitionProperty.TIME_UNIT, "day",
        //                DynamicPartitionProperty.ENABLE);
        //        modifyDynamicPartitionWithoutTableProperty(DynamicPartitionProperty.END, "3", DynamicPartitionProperty.ENABLE);
        //        modifyDynamicPartitionWithoutTableProperty(DynamicPartitionProperty.PREFIX, "p",
        //                DynamicPartitionProperty.ENABLE);
        //        modifyDynamicPartitionWithoutTableProperty(DynamicPartitionProperty.BUCKETS, "30",
        //                    DynamicPartitionProperty.ENABLE);
    }

    @Test
    public void testWarehouse() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        MockedWarehouseManager mockedWarehouseManager = new MockedWarehouseManager();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return mockedWarehouseManager;
            }
        };

        new MockUp<WarehouseComputeResourceProvider>() {
            @Mock
            public boolean isResourceAvailable(ComputeResource computeResource) {
                return true;
            }
        };

        String stmt = "alter table testDb1.testTable1 order by (v1, v2)";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
        ReorderColumnsClause clause = (ReorderColumnsClause) alterTableStmt.getAlterClauseList().get(0);

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);

        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        AlterJobV2 alterJobV2 = schemaChangeHandler.analyzeAndCreateJob(Lists.newArrayList(clause), db, olapTable);
        Assertions.assertEquals(0L, alterJobV2.warehouseId);

        mockedWarehouseManager.setAllComputeNodeIds(Lists.newArrayList());
        new MockUp<WarehouseComputeResourceProvider>() {
            @Mock
            public boolean isResourceAvailable(ComputeResource computeResource) {
                return false;
            }
        };

        try {
            alterJobV2 = schemaChangeHandler.analyzeAndCreateJob(Lists.newArrayList(clause), db, olapTable);
            Assertions.fail();
        } catch (DdlException e) {
            Assertions.assertTrue(e.getMessage().contains("no available compute nodes"));
        }
    }

    @Test
    public void testCancelPendingJobWithFlag() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);
        olapTable.setUseFastSchemaEvolution(false);
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable1);

        schemaChangeHandler.process(alterTableStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assertions.assertEquals(1, alterJobsV2.size());
        SchemaChangeJobV2 schemaChangeJob = (SchemaChangeJobV2) alterJobsV2.values().stream().findAny().get();
        alterJobsV2.clear();

        MaterializedIndex baseIndex = testPartition.getDefaultPhysicalPartition().getBaseIndex();
        assertEquals(IndexState.NORMAL, baseIndex.getState());
        assertEquals(PartitionState.NORMAL, testPartition.getState());
        assertEquals(OlapTableState.SCHEMA_CHANGE, olapTable.getState());

        LocalTablet baseTablet = (LocalTablet) baseIndex.getTablets().get(0);
        List<Replica> replicas = baseTablet.getImmutableReplicas();
        Replica replica1 = replicas.get(0);

        assertEquals(1, replica1.getVersion());
        assertEquals(-1, replica1.getLastFailedVersion());
        assertEquals(1, replica1.getLastSuccessVersion());

        schemaChangeJob.setIsCancelling(true);
        schemaChangeJob.runPendingJob();
        schemaChangeJob.setIsCancelling(false);

        schemaChangeJob.setWaitingCreatingReplica(true);
        schemaChangeJob.cancel("");
        schemaChangeJob.setWaitingCreatingReplica(false);
    }
}
