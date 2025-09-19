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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/MetadataViewerTest.java

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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.backup.CatalogMocker;
import com.starrocks.catalog.Replica.ReplicaStatus;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AdminShowReplicaStatusStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.ast.expression.TableRef;
import com.starrocks.system.SystemInfoService;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

public class MetadataViewerTest {

    private static Method getTabletStatusMethod;
    private static Method getTabletDistributionMethod;

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private SystemInfoService infoService;

    @Mocked
    private ConnectContext connectContext;

    private static Database db;

    @BeforeAll
    public static void setUp() throws NoSuchMethodException, SecurityException, InstantiationException,
            IllegalAccessException, IllegalArgumentException, InvocationTargetException, AnalysisException {
        Class[] argTypes = new Class[] {String.class, String.class, List.class, ReplicaStatus.class, BinaryType.class};
        getTabletStatusMethod = MetadataViewer.class.getDeclaredMethod("getTabletStatus", argTypes);
        getTabletStatusMethod.setAccessible(true);

        argTypes = new Class[] {String.class, String.class, PartitionNames.class};
        getTabletDistributionMethod = MetadataViewer.class.getDeclaredMethod("getTabletDistribution", argTypes);
        getTabletDistributionMethod.setAccessible(true);

        db = CatalogMocker.mockDb();
    }

    @BeforeEach
    public void before() {

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getLocalMetastore().getDb(anyString);
                minTimes = 0;
                result = db;

                globalStateMgr.getLocalMetastore().getTable(anyString, anyString);
                minTimes = 0;
                result = db.getTable(CatalogMocker.TEST_TBL_NAME);
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                minTimes = 0;
                result = infoService;

                infoService.getBackendIds(anyBoolean);
                minTimes = 0;
                result = Lists.newArrayList(10000L, 10001L, 10002L);
            }
        };
    }

    @Test
    public void testGetTabletStatus()
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        List<String> partitions = Lists.newArrayList();
        Object[] args = new Object[] {CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME, partitions, null,
                null};
        List<List<String>> result = (List<List<String>>) getTabletStatusMethod.invoke(null, args);
        Assertions.assertEquals(3, result.size());

        args = new Object[] {CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME, partitions, ReplicaStatus.DEAD,
                BinaryType.EQ};
        result = (List<List<String>>) getTabletStatusMethod.invoke(null, args);
        Assertions.assertEquals(3, result.size());

        args = new Object[] {CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME, partitions, ReplicaStatus.DEAD,
                BinaryType.NE};
        result = (List<List<String>>) getTabletStatusMethod.invoke(null, args);
        Assertions.assertEquals(0, result.size());
    }

    @Test
    public void testGetTabletDistribution()
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        Object[] args = new Object[] {CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME, null};
        List<List<String>> result = (List<List<String>>) getTabletDistributionMethod.invoke(null, args);
        Assertions.assertEquals(3, result.size());
    }

    @Test
    public void testGetTabletDistributionForSharedDataMode()
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        new Expectations() {
            {
                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                long warehouseId = 10000L;

                connectContext.getCurrentWarehouseId();
                minTimes = 0;
                result = warehouseId;

                GlobalStateMgr.getCurrentState().getWarehouseMgr().getAllComputeNodeIds(anyLong);
                minTimes = 0;
                result = Lists.newArrayList(10003L, 10004L, 10005L);
            }
        };

        Object[] args = new Object[] {CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME, null};
        List<List<String>> result = (List<List<String>>) getTabletDistributionMethod.invoke(null, args);
        Assertions.assertEquals(3, result.size());
    }

    @Test
    public void testGetTabletStatusWithStatement() throws Exception {
        // Test with null where clause
        TableName tableName = new TableName(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        TableRef tableRef = new TableRef(tableName, null);
        AdminShowReplicaStatusStmt stmt = new AdminShowReplicaStatusStmt(tableRef, null);

        List<List<String>> result = MetadataViewer.getTabletStatus(stmt);
        Assertions.assertEquals(3, result.size());
    }

    @Test
    public void testGetTabletStatusWithWhereClause() throws Exception {
        // Test with where clause: status = 'DEAD'
        TableName tableName = new TableName(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        TableRef tableRef = new TableRef(tableName, null);

        // Create where clause: status = 'DEAD'
        SlotRef leftChild = new SlotRef(tableName, "status");
        StringLiteral rightChild = new StringLiteral("DEAD");
        BinaryPredicate where = new BinaryPredicate(BinaryType.EQ, leftChild, rightChild);

        AdminShowReplicaStatusStmt stmt = new AdminShowReplicaStatusStmt(tableRef, where);

        List<List<String>> result = MetadataViewer.getTabletStatus(stmt);
        Assertions.assertEquals(3, result.size());
    }

    @Test
    public void testGetTabletStatusWithNotEqualWhereClause() throws Exception {
        // Test with where clause: status != 'DEAD'
        TableName tableName = new TableName(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        TableRef tableRef = new TableRef(tableName, null);

        // Create where clause: status != 'DEAD'
        SlotRef leftChild = new SlotRef(tableName, "status");
        StringLiteral rightChild = new StringLiteral("DEAD");
        BinaryPredicate where = new BinaryPredicate(BinaryType.NE, leftChild, rightChild);

        AdminShowReplicaStatusStmt stmt = new AdminShowReplicaStatusStmt(tableRef, where);

        List<List<String>> result = MetadataViewer.getTabletStatus(stmt);
        Assertions.assertEquals(0, result.size());
    }

    @Test
    public void testGetTabletStatusWithInvalidStatusFilter() throws Exception {
        // Test with where clause: status = 'INVALID_STATUS'
        TableName tableName = new TableName(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        TableRef tableRef = new TableRef(tableName, null);

        // Create where clause: status = 'INVALID_STATUS'
        SlotRef leftChild = new SlotRef(tableName, "status");
        StringLiteral rightChild = new StringLiteral("INVALID_STATUS");
        BinaryPredicate where = new BinaryPredicate(BinaryType.EQ, leftChild, rightChild);

        AdminShowReplicaStatusStmt stmt = new AdminShowReplicaStatusStmt(tableRef, where);

        List<List<String>> result = MetadataViewer.getTabletStatus(stmt);
        // Should return all results since invalid status filter is treated as null
        Assertions.assertEquals(3, result.size());
    }

    @Test
    public void testGetTabletStatusWithNonStatusColumn() throws Exception {
        // Test with where clause on non-status column: name = 'test'
        TableName tableName = new TableName(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        TableRef tableRef = new TableRef(tableName, null);

        // Create where clause: name = 'test' (not status column)
        SlotRef leftChild = new SlotRef(tableName, "name");
        StringLiteral rightChild = new StringLiteral("test");
        BinaryPredicate where = new BinaryPredicate(BinaryType.EQ, leftChild, rightChild);

        AdminShowReplicaStatusStmt stmt = new AdminShowReplicaStatusStmt(tableRef, where);

        List<List<String>> result = MetadataViewer.getTabletStatus(stmt);
        // Should return all results since non-status column filter is ignored
        Assertions.assertEquals(3, result.size());
    }

    @Test
    public void testGetTabletStatusWithOKStatus() throws Exception {
        // Test with where clause: status = 'OK'
        TableName tableName = new TableName(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        TableRef tableRef = new TableRef(tableName, null);

        // Create where clause: status = 'OK'
        SlotRef leftChild = new SlotRef(tableName, "status");
        StringLiteral rightChild = new StringLiteral("OK");
        BinaryPredicate where = new BinaryPredicate(BinaryType.EQ, leftChild, rightChild);

        AdminShowReplicaStatusStmt stmt = new AdminShowReplicaStatusStmt(tableRef, where);

        List<List<String>> result = MetadataViewer.getTabletStatus(stmt);
        // Should return 0 results since no replicas have OK status in the mock
        Assertions.assertEquals(0, result.size());
    }
}
