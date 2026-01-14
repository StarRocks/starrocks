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

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
        // Setup: 10 backends, 0 CN, total workers = 10, 50 partitions, ratio = 2.0
        // Expected: 50 >= 10 * 2.0 = 20, so should enable shuffle
        setupMockExpectationsForAdaptiveShuffle(gsm, metadataMgr, icebergTable, insertStmt, sessionVariable,
                queryStatement, selectRelation, 10, 0, 50, 100L, 2.0, false, null, null, false, nodeMgr, clusterInfo);

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
        // Setup: 10 backends, 0 CN, total workers = 10, 1500 partitions, threshold = 100, ratio = 2.0
        // Expected: 1500 >= 100, so should enable shuffle
        setupMockExpectationsForAdaptiveShuffle(gsm, metadataMgr, icebergTable, insertStmt, sessionVariable,
                queryStatement, selectRelation, 10, 0, 1500, 100L, 2.0, false, null, null, false, nodeMgr, clusterInfo);

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
        // Setup: 10 backends, 0 CN, total workers = 10, 5 partitions, threshold = 100, ratio = 2.0
        // Expected: 5 < 100 and 5 < 10 * 2.0 = 20, so should NOT enable shuffle
        setupMockExpectationsForAdaptiveShuffle(gsm, metadataMgr, icebergTable, insertStmt, sessionVariable,
                queryStatement, selectRelation, 10, 0, 5, 100L, 2.0, false, null, null, false, nodeMgr, clusterInfo);

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
        // Setup: static partition insert, 10 backends, 0 CN
        setupMockExpectationsForAdaptiveShuffle(gsm, metadataMgr, icebergTable, insertStmt, sessionVariable,
                queryStatement, selectRelation, 10, 0, 100, 100L, 2.0, true, partitionRef, null, false, nodeMgr, clusterInfo);

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
        // Setup: 0 backends, 0 CN, total workers = 0
        setupMockExpectationsForAdaptiveShuffle(gsm, metadataMgr, icebergTable, insertStmt, sessionVariable,
                queryStatement, selectRelation, 0, 0, 50, 100L, 2.0, false, null, null, false, nodeMgr, clusterInfo);

        boolean enabled = Deencapsulation.invoke(insertPlanner, "shouldEnableAdaptiveGlobalShuffle",
                insertStmt, icebergTable, sessionVariable);
        assertFalse(enabled);
    }

    /**
     * Test adaptive shuffle is disabled when total worker count is 1 (1 BE + 0 CN)
     */
    @Test
    public void testAdaptiveShuffleDisabledWhenSingleWorker(@Mocked GlobalStateMgr gsm,
                                                             @Mocked MetadataMgr metadataMgr,
                                                             @Mocked IcebergTable icebergTable,
                                                             @Mocked InsertStmt insertStmt,
                                                             @Mocked SessionVariable sessionVariable,
                                                             @Mocked QueryStatement queryStatement,
                                                             @Mocked SelectRelation selectRelation,
                                                             @Mocked NodeMgr nodeMgr,
                                                             @Mocked SystemInfoService clusterInfo) {
        // Setup: 1 backend, 0 CN, total workers = 1, should NOT enable shuffle even with many partitions
        setupMockExpectationsForAdaptiveShuffle(gsm, metadataMgr, icebergTable, insertStmt, sessionVariable,
                queryStatement, selectRelation, 1, 0, 1000, 100L, 2.0, false, null, null, false, nodeMgr, clusterInfo);

        boolean enabled = Deencapsulation.invoke(insertPlanner, "shouldEnableAdaptiveGlobalShuffle",
                insertStmt, icebergTable, sessionVariable);
        assertFalse(enabled);
    }

    /**
     * Test adaptive shuffle is disabled when partition count is 1
     */
    @Test
    public void testAdaptiveShuffleDisabledWhenSinglePartition(@Mocked GlobalStateMgr gsm,
                                                                @Mocked MetadataMgr metadataMgr,
                                                                @Mocked IcebergTable icebergTable,
                                                                @Mocked InsertStmt insertStmt,
                                                                @Mocked SessionVariable sessionVariable,
                                                                @Mocked QueryStatement queryStatement,
                                                                @Mocked SelectRelation selectRelation,
                                                                @Mocked NodeMgr nodeMgr,
                                                                @Mocked SystemInfoService clusterInfo) {
        // Setup: 10 backends, 0 CN, 1 partition, should NOT enable shuffle
        setupMockExpectationsForAdaptiveShuffle(gsm, metadataMgr, icebergTable, insertStmt, sessionVariable,
                queryStatement, selectRelation, 10, 0, 1, 100L, 2.0, false, null, null, false, nodeMgr, clusterInfo);

        boolean enabled = Deencapsulation.invoke(insertPlanner, "shouldEnableAdaptiveGlobalShuffle",
                insertStmt, icebergTable, sessionVariable);
        assertFalse(enabled);
    }

    /**
     * Test adaptive shuffle is disabled when partition count is 0 (new table with no partitions)
     */
    @Test
    public void testAdaptiveShuffleDisabledWhenUnableToEstimate(@Mocked GlobalStateMgr gsm,
                                                                 @Mocked MetadataMgr metadataMgr,
                                                                 @Mocked IcebergTable icebergTable,
                                                                 @Mocked InsertStmt insertStmt,
                                                                 @Mocked SessionVariable sessionVariable,
                                                                 @Mocked QueryStatement queryStatement,
                                                                 @Mocked SelectRelation selectRelation,
                                                                 @Mocked NodeMgr nodeMgr,
                                                                 @Mocked SystemInfoService clusterInfo) {
        // Setup: return empty partition list (new table with no partitions yet)
        new Expectations() {
            {
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
                result = 10;
                minTimes = 0;

                clusterInfo.getTotalComputeNodeNumber();
                result = 0;
                minTimes = 0;

                // Return empty partition list
                metadataMgr.listPartitionNames(anyString, anyString, anyString,
                        withInstanceOf(ConnectorMetadatRequestContext.class));
                result = new ArrayList<String>();
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

                icebergTable.getPartitionColumns();
                result = Lists.newArrayList(new Column("dt", DateType.DATE));
                minTimes = 0;

                sessionVariable.getConnectorSinkShufflePartitionThreshold();
                result = 100L;
                minTimes = 0;

                sessionVariable.getConnectorSinkShufflePartitionNodeRatio();
                result = 2.0;
                minTimes = 0;
            }
        };

        long partitionCount = Deencapsulation.invoke(insertPlanner, "estimatePartitionCountForInsert",
                insertStmt, icebergTable);
        // Empty partition list returns 0 (new table with no partitions yet)
        // This correctly disables shuffle since estimatedPartitionCount (0) <= 1
        assertEquals(0L, partitionCount);

        boolean enabled = Deencapsulation.invoke(insertPlanner, "shouldEnableAdaptiveGlobalShuffle",
                insertStmt, icebergTable, sessionVariable);
        assertFalse(enabled);
    }

    /**
     * Test adaptive shuffle with BE + CN combination
     */
    @Test
    public void testAdaptiveShuffleWithBEAndCN(@Mocked GlobalStateMgr gsm,
                                               @Mocked MetadataMgr metadataMgr,
                                               @Mocked IcebergTable icebergTable,
                                               @Mocked InsertStmt insertStmt,
                                               @Mocked SessionVariable sessionVariable,
                                               @Mocked QueryStatement queryStatement,
                                               @Mocked SelectRelation selectRelation,
                                               @Mocked NodeMgr nodeMgr,
                                               @Mocked SystemInfoService clusterInfo) {
        // Setup: 5 backends + 5 CN = 10 workers, 50 partitions, ratio = 2.0
        // Expected: 50 >= 10 * 2.0 = 20, so should enable shuffle
        setupMockExpectationsForAdaptiveShuffle(gsm, metadataMgr, icebergTable, insertStmt, sessionVariable,
                queryStatement, selectRelation, 5, 5, 50, 100L, 2.0, false, null, null, false, nodeMgr, clusterInfo);

        boolean enabled = Deencapsulation.invoke(insertPlanner, "shouldEnableAdaptiveGlobalShuffle",
                insertStmt, icebergTable, sessionVariable);
        assertTrue(enabled);
    }

    /**
     * Test behavior when partition list is empty (returns 0, disables shuffle)
     */
    @Test
    public void testEmptyPartitionList(@Mocked GlobalStateMgr gsm,
                                       @Mocked MetadataMgr metadataMgr,
                                       @Mocked IcebergTable icebergTable,
                                       @Mocked InsertStmt insertStmt,
                                       @Mocked SessionVariable sessionVariable) {
        // Setup: empty partition list (new table with no partitions yet)
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
                result = Lists.newArrayList(new Column("dt", DateType.DATE));
                minTimes = 0;

                GlobalStateMgr.getCurrentState();
                result = gsm;
                minTimes = 0;

                gsm.getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;
            }
        };

        long partitionCount = Deencapsulation.invoke(insertPlanner, "estimatePartitionCountForInsert",
                insertStmt, icebergTable);
        // Empty partition list returns 0 (new table with no partitions yet)
        assertEquals(0L, partitionCount);
    }

    /**
     * Test behavior when partition list retrieval throws exception (returns -1, disables shuffle)
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
                result = Lists.newArrayList(new Column("dt", DateType.DATE));
                minTimes = 0;
            }
        };

        long partitionCount = Deencapsulation.invoke(insertPlanner, "estimatePartitionCountForInsert",
                insertStmt, icebergTable);
        assertEquals(-1L, partitionCount);
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

                clusterInfo.getTotalComputeNodeNumber();
                result = 0;
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
                queryStatement, selectRelation, 10, 0, 100, 200L, 10.0, false, null, predicate, true, nodeMgr, clusterInfo);

        long partitionCount = Deencapsulation.invoke(insertPlanner, "estimatePartitionCountForInsert",
                insertStmt, icebergTable);
        assertEquals(2L, partitionCount);
    }

    /**
     * Test: when WHERE exists but predicate is too complex to estimate, return unknown (-1) instead of
     * falling back to existing partition count.
     */
    @Test
    public void testPartitionPredicateUnestimatableReturnsUnknown(@Mocked GlobalStateMgr gsm,
                                                                  @Mocked MetadataMgr metadataMgr,
                                                                  @Mocked IcebergTable icebergTable,
                                                                  @Mocked InsertStmt insertStmt,
                                                                  @Mocked SessionVariable sessionVariable,
                                                                  @Mocked QueryStatement queryStatement,
                                                                  @Mocked SelectRelation selectRelation,
                                                                  @Mocked NodeMgr nodeMgr,
                                                                  @Mocked SystemInfoService clusterInfo) {
        Expr dtEq1 = new BinaryPredicate(BinaryType.EQ, new SlotRef(null, "dt"), new StringLiteral("2024-01-01"));
        Expr dtEq2 = new BinaryPredicate(BinaryType.EQ, new SlotRef(null, "dt"), new StringLiteral("2024-01-02"));
        Expr countryEq1 = new BinaryPredicate(BinaryType.EQ, new SlotRef(null, "country"), new StringLiteral("US"));
        Expr countryEq2 = new BinaryPredicate(BinaryType.EQ, new SlotRef(null, "country"), new StringLiteral("CA"));

        // (dt='2024-01-01' AND country='US') OR (dt='2024-01-02' AND country='CA')
        // This OR-of-AND is intentionally too complex for InsertPartitionEstimator.
        Expr left = new CompoundPredicate(CompoundPredicate.Operator.AND, dtEq1, countryEq1);
        Expr right = new CompoundPredicate(CompoundPredicate.Operator.AND, dtEq2, countryEq2);
        Expr predicate = new CompoundPredicate(CompoundPredicate.Operator.OR, left, right);

        setupMockExpectationsForAdaptiveShuffle(gsm, metadataMgr, icebergTable, insertStmt, sessionVariable,
                queryStatement, selectRelation, 10, 0, 200, 100L, 2.0, false, null, predicate, true, nodeMgr, clusterInfo);

        long partitionCount = Deencapsulation.invoke(insertPlanner, "estimatePartitionCountForInsert",
                insertStmt, icebergTable);
        assertEquals(-1L, partitionCount);

        boolean enabled = Deencapsulation.invoke(insertPlanner, "shouldEnableAdaptiveGlobalShuffle",
                insertStmt, icebergTable, sessionVariable);
        assertFalse(enabled);
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
                                                         int computeNodeCount,
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

                clusterInfo.getTotalComputeNodeNumber();
                result = computeNodeCount;
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
}
