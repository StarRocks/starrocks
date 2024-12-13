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

<<<<<<< HEAD
import com.google.common.collect.Maps;
import com.starrocks.alter.AlterJobV2.JobState;
=======
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.alter.AlterJobV2.JobState;
import com.starrocks.analysis.TableName;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.backup.CatalogMocker;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
<<<<<<< HEAD
=======
import com.starrocks.catalog.InternalCatalog;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Partition.PartitionState;
import com.starrocks.catalog.Replica;
<<<<<<< HEAD
import com.starrocks.common.DdlException;
import com.starrocks.common.SchemaVersionAndHash;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
=======
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.SchemaVersionAndHash;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
<<<<<<< HEAD
import com.starrocks.utframe.UtFrameUtils;
=======
import com.starrocks.sql.ast.ReorderColumnsClause;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.Warehouse;
import mockit.Mock;
import mockit.MockUp;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
    private static String fileName = "./SchemaChangeV2Test";
=======
    private static final String TEST_FILE_NAME = SchemaChangeJobV2Test.class.getCanonicalName();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
        Database db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);

        schemaChangeHandler.process(alterTableStmt.getOps(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, olapTable.getState());
=======
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);

        schemaChangeHandler.process(alterTableStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    // start a schema change, then finished
    @Test
    public void testSchemaChange1() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
<<<<<<< HEAD
        Database db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable1);

        schemaChangeHandler.process(alterTableStmt.getOps(), db, olapTable);
=======
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);
        olapTable.setUseFastSchemaEvolution(false);
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable1);

        schemaChangeHandler.process(alterTableStmt.getAlterClauseList(), db, olapTable);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        SchemaChangeJobV2 schemaChangeJob = (SchemaChangeJobV2) alterJobsV2.values().stream().findAny().get();
        alterJobsV2.clear();

<<<<<<< HEAD
        MaterializedIndex baseIndex = testPartition.getBaseIndex();
=======
        MaterializedIndex baseIndex = testPartition.getDefaultPhysicalPartition().getBaseIndex();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
        Assert.assertEquals(2, testPartition.getMaterializedIndices(IndexExtState.ALL).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.VISIBLE).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.SHADOW).size());
=======
        Assert.assertEquals(2, testPartition.getDefaultPhysicalPartition()
                .getMaterializedIndices(IndexExtState.ALL).size());
        Assert.assertEquals(1, testPartition.getDefaultPhysicalPartition()
                .getMaterializedIndices(IndexExtState.VISIBLE).size());
        Assert.assertEquals(1, testPartition.getDefaultPhysicalPartition()
                .getMaterializedIndices(IndexExtState.SHADOW).size());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

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
<<<<<<< HEAD
        Database db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable1);

        schemaChangeHandler.process(alterTableStmt.getOps(), db, olapTable);
=======
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);
        olapTable.setUseFastSchemaEvolution(false);
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable1);

        schemaChangeHandler.process(alterTableStmt.getAlterClauseList(), db, olapTable);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        SchemaChangeJobV2 schemaChangeJob = (SchemaChangeJobV2) alterJobsV2.values().stream().findAny().get();
        alterJobsV2.clear();

<<<<<<< HEAD
        MaterializedIndex baseIndex = testPartition.getBaseIndex();
=======
        MaterializedIndex baseIndex = testPartition.getDefaultPhysicalPartition().getBaseIndex();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
        Assert.assertEquals(2, testPartition.getMaterializedIndices(IndexExtState.ALL).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.VISIBLE).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.SHADOW).size());
=======
        Assert.assertEquals(2, testPartition.getDefaultPhysicalPartition()
                .getMaterializedIndices(IndexExtState.ALL).size());
        Assert.assertEquals(1, testPartition.getDefaultPhysicalPartition()
                .getMaterializedIndices(IndexExtState.VISIBLE).size());
        Assert.assertEquals(1, testPartition.getDefaultPhysicalPartition()
                .getMaterializedIndices(IndexExtState.SHADOW).size());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

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
<<<<<<< HEAD
        schemaChangeHandler.process(alterClauses, db, olapTable);
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        Assert.assertTrue(olapTable.getTableProperty().getDynamicPartitionProperty().isExists());
        Assert.assertTrue(olapTable.getTableProperty().getDynamicPartitionProperty().isEnabled());
        Assert.assertEquals("day", olapTable.getTableProperty().getDynamicPartitionProperty().getTimeUnit());
        Assert.assertEquals(3, olapTable.getTableProperty().getDynamicPartitionProperty().getEnd());
        Assert.assertEquals("p", olapTable.getTableProperty().getDynamicPartitionProperty().getPrefix());
        Assert.assertEquals(30, olapTable.getTableProperty().getDynamicPartitionProperty().getBuckets());

        // set dynamic_partition.enable = false
        ArrayList<AlterClause> tmpAlterClauses = new ArrayList<>();
        properties.put(DynamicPartitionProperty.ENABLE, "false");
        tmpAlterClauses.add(new ModifyTablePropertiesClause(properties));
<<<<<<< HEAD
        schemaChangeHandler.process(tmpAlterClauses, db, olapTable);
=======
        new AlterJobExecutor().process(new AlterTableStmt(tableName, tmpAlterClauses), ctx);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        Assert.assertFalse(olapTable.getTableProperty().getDynamicPartitionProperty().isEnabled());
        // set dynamic_partition.time_unit = week
        tmpAlterClauses = new ArrayList<>();
        properties.put(DynamicPartitionProperty.TIME_UNIT, "week");
        tmpAlterClauses.add(new ModifyTablePropertiesClause(properties));
<<<<<<< HEAD
        schemaChangeHandler.process(tmpAlterClauses, db, olapTable);
=======
        new AlterJobExecutor().process(new AlterTableStmt(tableName, tmpAlterClauses), ctx);

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        Assert.assertEquals("week", olapTable.getTableProperty().getDynamicPartitionProperty().getTimeUnit());
        // set dynamic_partition.end = 10
        tmpAlterClauses = new ArrayList<>();
        properties.put(DynamicPartitionProperty.END, "10");
        tmpAlterClauses.add(new ModifyTablePropertiesClause(properties));
<<<<<<< HEAD
        schemaChangeHandler.process(tmpAlterClauses, db, olapTable);
=======
        new AlterJobExecutor().process(new AlterTableStmt(tableName, tmpAlterClauses), ctx);

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        Assert.assertEquals(10, olapTable.getTableProperty().getDynamicPartitionProperty().getEnd());
        // set dynamic_partition.prefix = p1
        tmpAlterClauses = new ArrayList<>();
        properties.put(DynamicPartitionProperty.PREFIX, "p1");
        tmpAlterClauses.add(new ModifyTablePropertiesClause(properties));
<<<<<<< HEAD
        schemaChangeHandler.process(tmpAlterClauses, db, olapTable);
=======
        new AlterJobExecutor().process(new AlterTableStmt(tableName, tmpAlterClauses), ctx);

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        Assert.assertEquals("p1", olapTable.getTableProperty().getDynamicPartitionProperty().getPrefix());
        // set dynamic_partition.buckets = 3
        tmpAlterClauses = new ArrayList<>();
        properties.put(DynamicPartitionProperty.BUCKETS, "3");
        tmpAlterClauses.add(new ModifyTablePropertiesClause(properties));
<<<<<<< HEAD
        schemaChangeHandler.process(tmpAlterClauses, db, olapTable);
        Assert.assertEquals(3, olapTable.getTableProperty().getDynamicPartitionProperty().getBuckets());
    }
=======
        new AlterJobExecutor().process(new AlterTableStmt(tableName, tmpAlterClauses), ctx);

        Assert.assertEquals(3, olapTable.getTableProperty().getDynamicPartitionProperty().getBuckets());
    }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    @Test
    public void testModifyDynamicPropertyTrim() throws Exception {
        String sql = "ALTER TABLE testDb1.testTable1 SET(\"dynamic_partition.buckets \"=\"1\")";
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
<<<<<<< HEAD
        Assert.assertEquals("1", stmt.getOps().get(0).getProperties().get("dynamic_partition.buckets"));
    }

    public void modifyDynamicPartitionWithoutTableProperty(String propertyKey, String propertyValue, String expectErrMsg)
            throws UserException {
=======

        ModifyTablePropertiesClause modifyTablePropertiesClause = (ModifyTablePropertiesClause) stmt.getAlterClauseList().get(0);
        Assert.assertEquals("1", modifyTablePropertiesClause.getProperties().get("dynamic_partition.buckets"));
    }

    public void modifyDynamicPartitionWithoutTableProperty(String propertyKey, String propertyValue, String expectErrMsg)
                throws StarRocksException {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        Map<String, String> properties = new HashMap<>();
        properties.put(propertyKey, propertyValue);
        alterClauses.add(new ModifyTablePropertiesClause(properties));

        Database db = CatalogMocker.mockDb();
        OlapTable olapTable = (OlapTable) db.getTable(CatalogMocker.TEST_TBL2_ID);

<<<<<<< HEAD
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
=======
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

        expectedEx.expect(AlterJobException.class);
        expectedEx.expectMessage(expectErrMsg);
        new AlterJobExecutor().process(new AlterTableStmt(tableName, alterClauses), ctx);
    }

    @Test
    public void testModifyDynamicPartitionWithoutTableProperty() throws AlterJobException, StarRocksException {
        modifyDynamicPartitionWithoutTableProperty(DynamicPartitionProperty.ENABLE, "false",
                    "not a dynamic partition table");
        modifyDynamicPartitionWithoutTableProperty(DynamicPartitionProperty.TIME_UNIT, "day",
                    DynamicPartitionProperty.ENABLE);
        modifyDynamicPartitionWithoutTableProperty(DynamicPartitionProperty.END, "3", DynamicPartitionProperty.ENABLE);
        modifyDynamicPartitionWithoutTableProperty(DynamicPartitionProperty.PREFIX, "p",
                    DynamicPartitionProperty.ENABLE);
        modifyDynamicPartitionWithoutTableProperty(DynamicPartitionProperty.BUCKETS, "30",
                    DynamicPartitionProperty.ENABLE);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Test
    public void testSerializeOfSchemaChangeJob() throws IOException {
        // prepare file
<<<<<<< HEAD
        File file = new File(fileName);
=======
        File file = new File(TEST_FILE_NAME);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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

<<<<<<< HEAD
        Assert.assertNotNull(Deencapsulation.getField(result, "partitionIndexMap"));
        Assert.assertNotNull(Deencapsulation.getField(result, "partitionIndexTabletMap"));
=======
        Assert.assertNotNull(Deencapsulation.getField(result, "physicalPartitionIndexMap"));
        Assert.assertNotNull(Deencapsulation.getField(result, "physicalPartitionIndexTabletMap"));
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

        Map<Long, SchemaVersionAndHash> map = Deencapsulation.getField(result, "indexSchemaVersionAndHashMap");
        Assert.assertEquals(10, map.get(1000L).schemaVersion);
        Assert.assertEquals(20, map.get(1000L).schemaHash);
    }
<<<<<<< HEAD
=======

    @Test
    public void testWarehouse() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        GlobalStateMgr.getCurrentState().initDefaultWarehouse();

        new MockUp<WarehouseManager>() {
            @Mock
            public Warehouse getWarehouse(long warehouseId) {
                return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                            WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }

            @Mock
            public List<Long> getAllComputeNodeIds(long warehouseId) {
                return Lists.newArrayList(1L);
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
        Assert.assertEquals(0L, alterJobV2.warehouseId);

        new MockUp<WarehouseManager>() {
            @Mock
            public Warehouse getWarehouse(long warehouseId) {
                return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                            WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }

            @Mock
            public List<Long> getAllComputeNodeIds(long warehouseId) {
                return Lists.newArrayList();
            }
        };

        try {
            alterJobV2 = schemaChangeHandler.analyzeAndCreateJob(Lists.newArrayList(clause), db, olapTable);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("no available compute nodes"));
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
        Assert.assertEquals(1, alterJobsV2.size());
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
}
