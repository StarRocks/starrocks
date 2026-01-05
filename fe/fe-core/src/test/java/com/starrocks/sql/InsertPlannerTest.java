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

package com.starrocks.sql;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.system.SystemInfoService;
import com.starrocks.type.DateType;
import com.starrocks.type.VarcharType;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for InsertPlanner
 */
public class InsertPlannerTest {

    private InsertPlanner insertPlanner;

    @BeforeEach
    public void setUp() {
        insertPlanner = new InsertPlanner();
    }

    /**
     * Test adaptive shuffle is enabled when partition count exceeds backend count * ratio
     */
    @Test
    public void testAdaptiveShuffleEnabledByPartitionRatio(@Mocked GlobalStateMgr gsm,
                                                           @Mocked MetadataMgr metadataMgr,
                                                           @Mocked IcebergTable icebergTable,
                                                           @Mocked InsertStmt insertStmt,
                                                           @Mocked SessionVariable sessionVariable,
                                                           @Mocked QueryStatement queryStatement,
                                                           @Mocked SelectRelation selectRelation,
                                                           @Mocked NodeMgr nodeMgr,
                                                           @Mocked SystemInfoService clusterInfo) {
        // Setup: 10 backends, 50 partitions, ratio = 2.0
        // Expected: 50 >= 10 * 2.0 = 20, so should enable shuffle
        setupMockExpectationsForAdaptiveShuffle(gsm, metadataMgr, icebergTable, insertStmt, sessionVariable,
                queryStatement, selectRelation, 10, 50, 100L, 2.0, false, null, null, false, nodeMgr, clusterInfo);

        boolean enabled = Deencapsulation.invoke(insertPlanner, "shouldEnableAdaptiveGlobalShuffle",
                insertStmt, icebergTable, sessionVariable);
        assertTrue(enabled);
    }

    /**
     * Test adaptive shuffle is enabled when partition count exceeds absolute threshold
     */
    @Test
    public void testAdaptiveShuffleEnabledByAbsoluteThreshold(@Mocked GlobalStateMgr gsm,
                                                              @Mocked MetadataMgr metadataMgr,
                                                              @Mocked IcebergTable icebergTable,
                                                              @Mocked InsertStmt insertStmt,
                                                              @Mocked SessionVariable sessionVariable,
                                                              @Mocked QueryStatement queryStatement,
                                                              @Mocked SelectRelation selectRelation,
                                                              @Mocked NodeMgr nodeMgr,
                                                              @Mocked SystemInfoService clusterInfo) {
        // Setup: 10 backends, 150 partitions, threshold = 100, ratio = 2.0
        // Expected: 1500 >= 500, so should enable shuffle
        setupMockExpectationsForAdaptiveShuffle(gsm, metadataMgr, icebergTable, insertStmt, sessionVariable,
                queryStatement, selectRelation, 10, 1500, 100L, 2.0, false, null, null, false, nodeMgr, clusterInfo);

        boolean enabled = Deencapsulation.invoke(insertPlanner, "shouldEnableAdaptiveGlobalShuffle",
                insertStmt, icebergTable, sessionVariable);
        assertTrue(enabled);
    }

    /**
     * Test adaptive shuffle is disabled when partition count is below both thresholds
     */
    @Test
    public void testAdaptiveShuffleDisabledWhenPartitionsLow(@Mocked GlobalStateMgr gsm,
                                                             @Mocked MetadataMgr metadataMgr,
                                                             @Mocked IcebergTable icebergTable,
                                                             @Mocked InsertStmt insertStmt,
                                                             @Mocked SessionVariable sessionVariable,
                                                             @Mocked QueryStatement queryStatement,
                                                             @Mocked SelectRelation selectRelation,
                                                             @Mocked NodeMgr nodeMgr,
                                                             @Mocked SystemInfoService clusterInfo) {
        // Setup: 10 backends, 5 partitions, threshold = 100, ratio = 2.0
        // Expected: 5 < 100 and 5 < 10 * 2.0 = 20, so should NOT enable shuffle
        setupMockExpectationsForAdaptiveShuffle(gsm, metadataMgr, icebergTable, insertStmt, sessionVariable,
                queryStatement, selectRelation, 10, 5, 100L, 2.0, false, null, null, false, nodeMgr, clusterInfo);

        boolean enabled = Deencapsulation.invoke(insertPlanner, "shouldEnableAdaptiveGlobalShuffle",
                insertStmt, icebergTable, sessionVariable);
        assertFalse(enabled);
    }

    /**
     * Test static partition insert returns partition count of 1
     */
    @Test
    public void testStaticPartitionInsert(@Mocked GlobalStateMgr gsm,
                                          @Mocked MetadataMgr metadataMgr,
                                          @Mocked IcebergTable icebergTable,
                                          @Mocked InsertStmt insertStmt,
                                          @Mocked SessionVariable sessionVariable,
                                          @Mocked PartitionRef partitionRef,
                                          @Mocked QueryStatement queryStatement,
                                          @Mocked SelectRelation selectRelation,
                                          @Mocked NodeMgr nodeMgr,
                                          @Mocked SystemInfoService clusterInfo) {
        // Setup: static partition insert
        setupMockExpectationsForAdaptiveShuffle(gsm, metadataMgr, icebergTable, insertStmt, sessionVariable,
                queryStatement, selectRelation, 10, 100, 100L, 2.0, true, partitionRef, null, false, nodeMgr, clusterInfo);

        long partitionCount = Deencapsulation.invoke(insertPlanner, "estimatePartitionCountForInsert",
                insertStmt, icebergTable);
        assertEquals(1L, partitionCount);
    }

    /**
     * Test adaptive shuffle is disabled when no backends available
     */
    @Test
    public void testAdaptiveShuffleDisabledWhenNoBackends(@Mocked GlobalStateMgr gsm,
                                                          @Mocked MetadataMgr metadataMgr,
                                                          @Mocked IcebergTable icebergTable,
                                                          @Mocked InsertStmt insertStmt,
                                                          @Mocked SessionVariable sessionVariable,
                                                          @Mocked QueryStatement queryStatement,
                                                          @Mocked SelectRelation selectRelation,
                                                          @Mocked NodeMgr nodeMgr,
                                                          @Mocked SystemInfoService clusterInfo) {
        // Setup: 0 backends
        setupMockExpectationsForAdaptiveShuffle(gsm, metadataMgr, icebergTable, insertStmt, sessionVariable,
                queryStatement, selectRelation, 0, 50, 100L, 2.0, false, null, null, false, nodeMgr, clusterInfo);

        boolean enabled = Deencapsulation.invoke(insertPlanner, "shouldEnableAdaptiveGlobalShuffle",
                insertStmt, icebergTable, sessionVariable);
        assertFalse(enabled);
    }

    /**
     * Test behavior when partition list is empty
     */
    @Test
    public void testEmptyPartitionList(@Mocked GlobalStateMgr gsm,
                                       @Mocked MetadataMgr metadataMgr,
                                       @Mocked IcebergTable icebergTable,
                                       @Mocked InsertStmt insertStmt,
                                       @Mocked SessionVariable sessionVariable) {
        // Setup: empty partition list (should return MAX_VALUE to enable shuffle)
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = gsm;
                minTimes = 0;

                gsm.getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                metadataMgr.listPartitionNames(anyString, anyString, anyString,
                        withInstanceOf(ConnectorMetadatRequestContext.class));
                result = new ArrayList<String>(); // Empty partition list
                minTimes = 0;
            }
        };

        long partitionCount = Deencapsulation.invoke(insertPlanner, "estimatePartitionCountForInsert",
                insertStmt, icebergTable);
        assertEquals(Long.MAX_VALUE, partitionCount);
    }

    /**
     * Test behavior when partition list retrieval throws exception
     */
    @Test
    public void testPartitionListException(@Mocked GlobalStateMgr gsm,
                                           @Mocked MetadataMgr metadataMgr,
                                           @Mocked IcebergTable icebergTable,
                                           @Mocked InsertStmt insertStmt,
                                           @Mocked SessionVariable sessionVariable) {
        // Setup: exception when getting partition list
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = gsm;
                minTimes = 0;

                gsm.getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                metadataMgr.listPartitionNames(anyString, anyString, anyString,
                        withInstanceOf(ConnectorMetadatRequestContext.class));
                result = new Exception("Failed to get partitions");
                minTimes = 0;
            }
        };

        long partitionCount = Deencapsulation.invoke(insertPlanner, "estimatePartitionCountForInsert",
                insertStmt, icebergTable);
        assertEquals(Long.MAX_VALUE, partitionCount);
    }

    /**
     * Test partition estimation falls back to column statistics when partition names are unavailable.
     */

    @Test
    public void testPartitionCountFromStatistics(@Mocked GlobalStateMgr gsm,
                                                 @Mocked MetadataMgr metadataMgr,
                                                 @Mocked IcebergTable icebergTable,
                                                 @Mocked InsertStmt insertStmt,
                                                 @Mocked SessionVariable sessionVariable,
                                                 @Mocked QueryStatement queryStatement,
                                                 @Mocked SelectRelation selectRelation,
                                                 @Mocked StatisticStorage statisticStorage,
                                                 @Mocked NodeMgr nodeMgr,
                                                 @Mocked SystemInfoService clusterInfo) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = gsm;
                minTimes = 0;

                gsm.getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                metadataMgr.listPartitionNames(anyString, anyString, anyString,
                        withInstanceOf(ConnectorMetadatRequestContext.class));
                result = new ArrayList<String>();
                minTimes = 0;

                gsm.getStatisticStorage();
                result = statisticStorage;
                minTimes = 0;

                statisticStorage.getColumnStatistic(icebergTable, "dt");
                result = ColumnStatistic.buildFrom(ColumnStatistic.unknown()).setDistinctValuesCount(10).setType(
                        ColumnStatistic.StatisticType.ESTIMATE).build();
                minTimes = 0;

                statisticStorage.getColumnStatistic(icebergTable, "country");
                result = ColumnStatistic.buildFrom(ColumnStatistic.unknown()).setDistinctValuesCount(5).setType(
                        ColumnStatistic.StatisticType.ESTIMATE).build();
                minTimes = 0;

                icebergTable.getPartitionColumns();
                result = Lists.newArrayList(
                        new Column("dt", DateType.DATE),
                        new Column("country", VarcharType.VARCHAR)
                );
                minTimes = 0;

                icebergTable.getCatalogName();
                result = "test_catalog";
                minTimes = 0;

                icebergTable.getCatalogDBName();
                result = "test_db";
                minTimes = 0;

                icebergTable.getCatalogTableName();
                result = "test_table";
                minTimes = 0;

                insertStmt.isStaticKeyPartitionInsert();
                result = false;
                minTimes = 0;

                insertStmt.getQueryStatement();
                result = queryStatement;
                minTimes = 0;

                queryStatement.getQueryRelation();
                result = selectRelation;
                minTimes = 0;

                selectRelation.hasWhereClause();
                result = false;
                minTimes = 0;

                sessionVariable.getConnectorSinkShufflePartitionThreshold();
                result = 100L;
                minTimes = 0;

                sessionVariable.getConnectorSinkShufflePartitionNodeRatio();
                result = 2.0;
                minTimes = 0;

                gsm.getNodeMgr();
                result = nodeMgr;
                minTimes = 0;

                nodeMgr.getClusterInfo();
                result = clusterInfo;
                minTimes = 0;

                clusterInfo.getAliveBackendNumber();
                result = 5;
                minTimes = 0;
            }
        };

        long partitionCount = Deencapsulation.invoke(insertPlanner, "estimatePartitionCountForInsert",
                insertStmt, icebergTable);
        assertEquals(50L, partitionCount);
    }

    /**
     * Test partition predicate estimation reduces partition count.
     */
    @Test
    public void testPartitionPredicateEstimation(@Mocked GlobalStateMgr gsm,
                                                 @Mocked MetadataMgr metadataMgr,
                                                 @Mocked IcebergTable icebergTable,
                                                 @Mocked InsertStmt insertStmt,
                                                 @Mocked SessionVariable sessionVariable,
                                                 @Mocked QueryStatement queryStatement,
                                                 @Mocked SelectRelation selectRelation,
                                                 @Mocked NodeMgr nodeMgr,
                                                 @Mocked SystemInfoService clusterInfo) {
        Expr dtPredicate = new BinaryPredicate(BinaryType.EQ, new SlotRef(null, "dt"),
                new StringLiteral("2024-01-01"));
        Expr countryPredicate = new InPredicate(new SlotRef(null, "country"),
                Lists.newArrayList(new StringLiteral("US"), new StringLiteral("CA")), false);
        Expr predicate = new CompoundPredicate(CompoundPredicate.Operator.AND, dtPredicate, countryPredicate);

        setupMockExpectationsForAdaptiveShuffle(gsm, metadataMgr, icebergTable, insertStmt, sessionVariable,
                queryStatement, selectRelation, 10, 100, 200L, 10.0, false, null, predicate, true, nodeMgr, clusterInfo);

        long partitionCount = Deencapsulation.invoke(insertPlanner, "estimatePartitionCountForInsert",
                insertStmt, icebergTable);
        assertEquals(2L, partitionCount);
    }

    // Helper method to setup common mock expectations
    private void setupMockExpectationsForAdaptiveShuffle(GlobalStateMgr gsm,
                                                         MetadataMgr metadataMgr,
                                                         IcebergTable icebergTable,
                                                         InsertStmt insertStmt,
                                                         SessionVariable sessionVariable,
                                                         QueryStatement queryStatement,
                                                         SelectRelation selectRelation,
                                                         int backendCount,
                                                         int partitionCount,
                                                         long threshold,
                                                         double ratio,
                                                         boolean isStaticPartitionInsert,
                                                         PartitionRef partitionRef,
                                                         Expr predicate,
                                                         boolean hasWhereClause,
                                                         NodeMgr nodeMgr,
                                                         SystemInfoService clusterInfo) {
        new Expectations() {
            {
                // GlobalStateMgr setup
                GlobalStateMgr.getCurrentState();
                result = gsm;
                minTimes = 0;

                gsm.getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                gsm.getNodeMgr();
                result = nodeMgr;
                minTimes = 0;

                nodeMgr.getClusterInfo();
                result = clusterInfo;
                minTimes = 0;

                clusterInfo.getAliveBackendNumber();
                result = backendCount;
                minTimes = 0;

                // MetadataMgr setup for partition names
                metadataMgr.listPartitionNames(anyString, anyString, anyString,
                        withInstanceOf(ConnectorMetadatRequestContext.class));
                result = new Delegate<List<String>>() {
                    @SuppressWarnings("unused")
                    List<String> delegate(String catalog, String db, String table,
                                          ConnectorMetadatRequestContext context) {
                        List<String> partitions = new ArrayList<>();
                        for (int i = 0; i < partitionCount; i++) {
                            partitions.add("p" + i);
                        }
                        return partitions;
                    }
                };
                minTimes = 0;

                // IcebergTable setup
                icebergTable.getCatalogName();
                result = "test_catalog";
                minTimes = 0;

                icebergTable.getCatalogDBName();
                result = "test_db";
                minTimes = 0;

                icebergTable.getCatalogTableName();
                result = "test_table";
                minTimes = 0;

                icebergTable.getPartitionColumns();
                result = Lists.newArrayList(
                        new Column("dt", DateType.DATE),
                        new Column("country", VarcharType.VARCHAR)
                );
                minTimes = 0;

                // InsertStmt setup
                insertStmt.isStaticKeyPartitionInsert();
                result = isStaticPartitionInsert;
                minTimes = 0;

                insertStmt.getTargetPartitionNames();
                result = partitionRef;
                minTimes = 0;

                if (partitionRef != null) {
                    partitionRef.getPartitionColNames();
                    result = Lists.newArrayList("p1");
                    minTimes = 0;
                }

                insertStmt.getQueryStatement();
                result = queryStatement;
                minTimes = 0;

                queryStatement.getQueryRelation();
                result = selectRelation;
                minTimes = 0;

                selectRelation.hasWhereClause();
                result = hasWhereClause;
                minTimes = 0;

                selectRelation.getPredicate();
                result = predicate;
                minTimes = 0;

                // SessionVariable setup
                sessionVariable.getConnectorSinkShufflePartitionThreshold();
                result = threshold;
                minTimes = 0;

                sessionVariable.getConnectorSinkShufflePartitionNodeRatio();
                result = ratio;
                minTimes = 0;
            }
        };
    }

    // ========== Transform Partition Tests ==========

    /**
     * Test extractTransformParam for bucket transform
     */
    @Test
    public void testExtractTransformParamForBucket() {
        int param = Deencapsulation.invoke(insertPlanner, "extractTransformParam", "bucket[5]");
        assertEquals(5, param);
    }

    /**
     * Test extractTransformParam for truncate transform
     */
    @Test
    public void testExtractTransformParamForTruncate() {
        int param = Deencapsulation.invoke(insertPlanner, "extractTransformParam", "truncate[10]");
        assertEquals(10, param);
    }

    /**
     * Test extractTransformParam with large number
     */
    @Test
    public void testExtractTransformParamWithLargeNumber() {
        int param = Deencapsulation.invoke(insertPlanner, "extractTransformParam", "bucket[1000]");
        assertEquals(1000, param);
    }

    /**
     * Test extractTransformParam with invalid format throws exception
     */
    @Test
    public void testExtractTransformParamWithInvalidFormat() {
        try {
            Deencapsulation.invoke(insertPlanner, "extractTransformParam", "bucket5");
            throw new AssertionError("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Invalid transform string"));
        }
    }

    /**
     * Test createTransformExpression for year transform
     */
    @Test
    public void testCreateTransformExpressionForYear(@Mocked ColumnRefOperator columnRef) {
        new Expectations() {
            {
                columnRef.getType();
                result = com.starrocks.type.DateType.DATE;
                minTimes = 0;

                columnRef.isNullable();
                result = true;
                minTimes = 0;
            }
        };

        org.apache.iceberg.PartitionField partitionField = mockPartitionField("year");

        com.starrocks.sql.optimizer.operator.scalar.ScalarOperator expr = Deencapsulation.invoke(
                insertPlanner, "createTransformExpression", columnRef, "year", partitionField);

        assertNotNull(expr);
        assertTrue(expr instanceof com.starrocks.sql.optimizer.operator.scalar.CallOperator);
        com.starrocks.sql.optimizer.operator.scalar.CallOperator callOp =
                (com.starrocks.sql.optimizer.operator.scalar.CallOperator) expr;
        assertEquals("__iceberg_transform_year", callOp.getFnName());
        assertEquals(com.starrocks.type.DateType.DATE, callOp.getType());
        assertEquals(1, callOp.getChildren().size());
    }

    /**
     * Test createTransformExpression for day transform
     */
    @Test
    public void testCreateTransformExpressionForDay(@Mocked ColumnRefOperator columnRef) {
        new Expectations() {
            {
                columnRef.getType();
                result = com.starrocks.type.DateType.DATE;
                minTimes = 0;

                columnRef.isNullable();
                result = true;
                minTimes = 0;
            }
        };

        org.apache.iceberg.PartitionField partitionField = mockPartitionField("day");

        com.starrocks.sql.optimizer.operator.scalar.ScalarOperator expr = Deencapsulation.invoke(
                insertPlanner, "createTransformExpression", columnRef, "day", partitionField);

        assertNotNull(expr);
        assertTrue(expr instanceof com.starrocks.sql.optimizer.operator.scalar.CallOperator);
        com.starrocks.sql.optimizer.operator.scalar.CallOperator callOp =
                (com.starrocks.sql.optimizer.operator.scalar.CallOperator) expr;
        assertEquals("__iceberg_transform_day", callOp.getFnName());
    }

    /**
     * Test createTransformExpression for hour transform
     */
    @Test
    public void testCreateTransformExpressionForHour(@Mocked ColumnRefOperator columnRef) {
        new Expectations() {
            {
                columnRef.getType();
                result = com.starrocks.type.DateType.DATETIME;
                minTimes = 0;

                columnRef.isNullable();
                result = true;
                minTimes = 0;
            }
        };

        org.apache.iceberg.PartitionField partitionField = mockPartitionField("hour");

        com.starrocks.sql.optimizer.operator.scalar.ScalarOperator expr = Deencapsulation.invoke(
                insertPlanner, "createTransformExpression", columnRef, "hour", partitionField);

        assertNotNull(expr);
        assertTrue(expr instanceof com.starrocks.sql.optimizer.operator.scalar.CallOperator);
        com.starrocks.sql.optimizer.operator.scalar.CallOperator callOp =
                (com.starrocks.sql.optimizer.operator.scalar.CallOperator) expr;
        assertEquals("__iceberg_transform_hour", callOp.getFnName());
        assertEquals(com.starrocks.type.DateType.DATETIME, callOp.getType());
    }

    /**
     * Test createTransformExpression for void transform returns null constant
     */
    @Test
    public void testCreateTransformExpressionForVoid(@Mocked ColumnRefOperator columnRef) {
        org.apache.iceberg.PartitionField partitionField = mockPartitionField("void");

        com.starrocks.sql.optimizer.operator.scalar.ScalarOperator expr = Deencapsulation.invoke(
                insertPlanner, "createTransformExpression", columnRef, "void", partitionField);

        assertNotNull(expr);
        assertTrue(expr instanceof com.starrocks.sql.optimizer.operator.scalar.ConstantOperator);
        com.starrocks.sql.optimizer.operator.scalar.ConstantOperator constOp =
                (com.starrocks.sql.optimizer.operator.scalar.ConstantOperator) expr;
        assertTrue(constOp.isNull());
    }

    /**
     * Test createTransformExpression for bucket transform
     */
    @Test
    public void testCreateTransformExpressionForBucket(@Mocked ColumnRefOperator columnRef) {
        new Expectations() {
            {
                columnRef.isNullable();
                result = true;
                minTimes = 0;
            }
        };

        org.apache.iceberg.PartitionField partitionField = mockPartitionField("bucket[5]");

        com.starrocks.sql.optimizer.operator.scalar.ScalarOperator expr = Deencapsulation.invoke(
                insertPlanner, "createTransformExpression", columnRef, "bucket[5]", partitionField);

        assertNotNull(expr);
        assertTrue(expr instanceof com.starrocks.sql.optimizer.operator.scalar.CallOperator);
        com.starrocks.sql.optimizer.operator.scalar.CallOperator callOp =
                (com.starrocks.sql.optimizer.operator.scalar.CallOperator) expr;
        assertEquals("__iceberg_transform_bucket[5]", callOp.getFnName());
        assertEquals(com.starrocks.type.IntegerType.INT, callOp.getType());
        assertEquals(2, callOp.getChildren().size()); // column + bucket count
    }

    /**
     * Test createTransformExpression for truncate transform
     */
    @Test
    public void testCreateTransformExpressionForTruncate(@Mocked ColumnRefOperator columnRef) {
        new Expectations() {
            {
                columnRef.getType();
                result = com.starrocks.type.VarcharType.VARCHAR;
                minTimes = 0;

                columnRef.isNullable();
                result = true;
                minTimes = 0;
            }
        };

        org.apache.iceberg.PartitionField partitionField = mockPartitionField("truncate[10]");

        com.starrocks.sql.optimizer.operator.scalar.ScalarOperator expr = Deencapsulation.invoke(
                insertPlanner, "createTransformExpression", columnRef, "truncate[10]", partitionField);

        assertNotNull(expr);
        assertTrue(expr instanceof com.starrocks.sql.optimizer.operator.scalar.CallOperator);
        com.starrocks.sql.optimizer.operator.scalar.CallOperator callOp =
                (com.starrocks.sql.optimizer.operator.scalar.CallOperator) expr;
        assertEquals("__iceberg_transform_truncate[10]", callOp.getFnName());
        assertEquals(2, callOp.getChildren().size()); // column + width
    }

    /**
     * Test createTransformExpression for unknown transform returns null
     */
    @Test
    public void testCreateTransformExpressionForUnknownTransform(@Mocked ColumnRefOperator columnRef) {
        org.apache.iceberg.PartitionField partitionField = mockPartitionField("unknown");

        com.starrocks.sql.optimizer.operator.scalar.ScalarOperator expr = Deencapsulation.invoke(
                insertPlanner, "createTransformExpression", columnRef, "unknown", partitionField);

        assertNull(expr);
    }

    private org.apache.iceberg.PartitionField mockPartitionField(String transformStr) {
        org.apache.iceberg.PartitionField pf = Mockito.mock(org.apache.iceberg.PartitionField.class);
        org.apache.iceberg.transforms.Transform tf = Mockito.mock(org.apache.iceberg.transforms.Transform.class);

        Mockito.when(tf.toString()).thenReturn(transformStr);
        Mockito.when(tf.isIdentity()).thenReturn("identity".equals(transformStr));
        Mockito.when(pf.transform()).thenReturn(tf);
        Mockito.when(pf.sourceId()).thenReturn(1);
        Mockito.when(pf.name()).thenReturn("test_field"); // 如果需要

        return pf;
    }

    // Helper method to create a mock PartitionField
    /*
    private org.apache.iceberg.PartitionField mockPartitionField(String transformStr) {
        //return null;
        return new org.apache.iceberg.PartitionField(1, 0, "test_field",
                new org.apache.iceberg.transforms.Transform<Void>() {
                    @Override
                    public String toString() {
                        return transformStr;
                    }

                    @Override
                    public boolean canTransform(Type var1) { return true; }

                    @Override
                    public org.apache.iceberg.types.Type getResultType(org.apache.iceberg.types.Type sourceType) {
                        return sourceType;
                    }

                    @Override
                    public boolean equals(Object other) {
                        return this == other;
                    }

                    @Override
                    public int hashCode() {
                        return System.identityHashCode(this);
                    }

                    @Override
                    public boolean isVoid() {
                        return "void".equals(transformStr);
                    }

                    @Override
                    public boolean preservesOrder() {
                        return true;
                    }

                    @Override
                    public java.lang.Integer satisfySourceIds(org.apache.iceberg.schema.Schema schema) {
                        return null;
                    }

                    @Override
                    public org.apache.iceberg.transforms.Transform<Void> bind(org.apache.iceberg.types.Type type) {
                        return this;
                    }

                    @Override
                    public boolean isIdentity() {
                        return "identity".equals(transformStr);
                    }

                    @Override
                    public Void apply(Void value) {
                        return null;
                    }
                });
    }
     */
}
