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
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
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

    private static void setInsertPlannerLoggerLevel(Level level) {
        Configurator.setLevel(InsertPlanner.class.getName(), level);
    }

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

    @Test
    public void testAdaptiveShuffleEnabledByPartitionRatio_DebugLogCovered(@Mocked GlobalStateMgr gsm,
                                                                           @Mocked MetadataMgr metadataMgr,
                                                                           @Mocked IcebergTable icebergTable,
                                                                           @Mocked InsertStmt insertStmt,
                                                                           @Mocked SessionVariable sessionVariable,
                                                                           @Mocked QueryStatement queryStatement,
                                                                           @Mocked SelectRelation selectRelation,
                                                                           @Mocked NodeMgr nodeMgr,
                                                                           @Mocked SystemInfoService clusterInfo) {
        setInsertPlannerLoggerLevel(Level.DEBUG);
        try {
            setupMockExpectationsForAdaptiveShuffle(gsm, metadataMgr, icebergTable, insertStmt, sessionVariable,
                    queryStatement, selectRelation, 10, 0, 50, 100L, 2.0, false, null, null, false, nodeMgr, clusterInfo);

            boolean enabled = Deencapsulation.invoke(insertPlanner, "shouldEnableAdaptiveGlobalShuffle",
                    insertStmt, icebergTable, sessionVariable);
            assertTrue(enabled);
        } finally {
            setInsertPlannerLoggerLevel(Level.INFO);
        }
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

    @Test
    public void testAdaptiveShuffleDisabledWhenSingleWorker_DebugLogCovered(@Mocked GlobalStateMgr gsm,
                                                                            @Mocked MetadataMgr metadataMgr,
                                                                            @Mocked IcebergTable icebergTable,
                                                                            @Mocked InsertStmt insertStmt,
                                                                            @Mocked SessionVariable sessionVariable,
                                                                            @Mocked QueryStatement queryStatement,
                                                                            @Mocked SelectRelation selectRelation,
                                                                            @Mocked NodeMgr nodeMgr,
                                                                            @Mocked SystemInfoService clusterInfo) {
        setInsertPlannerLoggerLevel(Level.DEBUG);
        try {
            setupMockExpectationsForAdaptiveShuffle(gsm, metadataMgr, icebergTable, insertStmt, sessionVariable,
                    queryStatement, selectRelation, 1, 0, 1000, 100L, 2.0, false, null, null, false, nodeMgr, clusterInfo);

            boolean enabled = Deencapsulation.invoke(insertPlanner, "shouldEnableAdaptiveGlobalShuffle",
                    insertStmt, icebergTable, sessionVariable);
            assertFalse(enabled);
        } finally {
            setInsertPlannerLoggerLevel(Level.INFO);
        }
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

    @Test
    public void testAdaptiveShuffleDisabledWhenSinglePartition_DebugLogCovered(@Mocked GlobalStateMgr gsm,
                                                                               @Mocked MetadataMgr metadataMgr,
                                                                               @Mocked IcebergTable icebergTable,
                                                                               @Mocked InsertStmt insertStmt,
                                                                               @Mocked SessionVariable sessionVariable,
                                                                               @Mocked QueryStatement queryStatement,
                                                                               @Mocked SelectRelation selectRelation,
                                                                               @Mocked NodeMgr nodeMgr,
                                                                               @Mocked SystemInfoService clusterInfo) {
        setInsertPlannerLoggerLevel(Level.DEBUG);
        try {
            setupMockExpectationsForAdaptiveShuffle(gsm, metadataMgr, icebergTable, insertStmt, sessionVariable,
                    queryStatement, selectRelation, 10, 0, 1, 100L, 2.0, false, null, null, false, nodeMgr, clusterInfo);

            boolean enabled = Deencapsulation.invoke(insertPlanner, "shouldEnableAdaptiveGlobalShuffle",
                    insertStmt, icebergTable, sessionVariable);
            assertFalse(enabled);
        } finally {
            setInsertPlannerLoggerLevel(Level.INFO);
        }
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

    // ==================== PredicateEstimator Coverage Tests ====================
    // These tests are designed to improve coverage of InsertPartitionEstimator.PredicateEstimator
    // by testing various predicate scenarios and verifying the estimated partition count

    /**
     * Test partition estimation with equality predicate on single partition column
     * Expected: estimate = 1 (single value from equality predicate)
     */
    @Test
    public void testEstimatePartitionWithEqualityPredicate(@Mocked GlobalStateMgr gsm,
                                                           @Mocked MetadataMgr metadataMgr,
                                                           @Mocked IcebergTable icebergTable,
                                                           @Mocked InsertStmt insertStmt,
                                                           @Mocked QueryStatement queryStatement,
                                                           @Mocked SelectRelation selectRelation,
                                                           @Mocked SlotRef slotRef,
                                                           @Mocked StringLiteral literal) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = gsm;
                minTimes = 0;

                gsm.getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                // 100 existing partitions
                metadataMgr.listPartitionNames(anyString, anyString, anyString,
                        withInstanceOf(ConnectorMetadatRequestContext.class));
                result = createPartitionList(100);
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

                insertStmt.getQueryStatement();
                result = queryStatement;
                minTimes = 0;

                queryStatement.getQueryRelation();
                result = selectRelation;
                minTimes = 0;

                selectRelation.hasWhereClause();
                result = true;
                minTimes = 0;

                // WHERE dt = '2024-01-01'
                selectRelation.getPredicate();
                result = createBinaryPredicate(BinaryType.EQ, slotRef, literal);
                minTimes = 0;

                slotRef.getColumnName();
                result = "dt";
                minTimes = 0;

                literal.getStringValue();
                result = "2024-01-01";
                minTimes = 0;

                insertStmt.isStaticKeyPartitionInsert();
                result = false;
                minTimes = 0;
            }
        };

        long estimatedCount = InsertPartitionEstimator.estimatePartitionCountForInsert(insertStmt, icebergTable);
        // Equality predicate on single partition column should estimate 1 partition
        assertEquals(1L, estimatedCount);
    }

    /**
     * Test partition estimation with IN predicate
     * Expected: estimate = 3 (number of values in IN list)
     */
    @Test
    public void testEstimatePartitionWithInPredicate(@Mocked GlobalStateMgr gsm,
                                                      @Mocked MetadataMgr metadataMgr,
                                                      @Mocked IcebergTable icebergTable,
                                                      @Mocked InsertStmt insertStmt,
                                                      @Mocked QueryStatement queryStatement,
                                                      @Mocked SelectRelation selectRelation,
                                                      @Mocked SlotRef slotRef,
                                                      @Mocked InPredicate inPredicate,
                                                      @Mocked StringLiteral literal1,
                                                      @Mocked StringLiteral literal2,
                                                      @Mocked StringLiteral literal3) {
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
                result = createPartitionList(100);
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

                insertStmt.getQueryStatement();
                result = queryStatement;
                minTimes = 0;

                queryStatement.getQueryRelation();
                result = selectRelation;
                minTimes = 0;

                selectRelation.hasWhereClause();
                result = true;
                minTimes = 0;

                // WHERE dt IN ('2024-01-01', '2024-01-02', '2024-01-03')
                selectRelation.getPredicate();
                result = inPredicate;
                minTimes = 0;

                inPredicate.isNotIn();
                result = false;
                minTimes = 0;

                inPredicate.isConstantValues();
                result = true;
                minTimes = 0;

                inPredicate.getChild(0);
                result = slotRef;
                minTimes = 0;

                inPredicate.getInElementNum();
                result = 3;
                minTimes = 0;

                inPredicate.getChildren();
                result = Lists.newArrayList(slotRef, literal1, literal2, literal3);
                minTimes = 0;

                slotRef.getColumnName();
                result = "dt";
                minTimes = 0;

                literal1.getStringValue();
                result = "2024-01-01";
                minTimes = 0;

                literal2.getStringValue();
                result = "2024-01-02";
                minTimes = 0;

                literal3.getStringValue();
                result = "2024-01-03";
                minTimes = 0;

                insertStmt.isStaticKeyPartitionInsert();
                result = false;
                minTimes = 0;
            }
        };

        long estimatedCount = InsertPartitionEstimator.estimatePartitionCountForInsert(insertStmt, icebergTable);
        // IN predicate should estimate 3 partitions
        assertEquals(3L, estimatedCount);
    }

    /**
     * Test partition estimation with OR predicate on same column
     * Expected: estimate = 2 (sum of OR values)
     */
    @Test
    public void testEstimatePartitionWithOrSameColumn(@Mocked GlobalStateMgr gsm,
                                                       @Mocked MetadataMgr metadataMgr,
                                                       @Mocked IcebergTable icebergTable,
                                                       @Mocked InsertStmt insertStmt,
                                                       @Mocked QueryStatement queryStatement,
                                                       @Mocked SelectRelation selectRelation,
                                                       @Mocked SlotRef slotRef1,
                                                       @Mocked SlotRef slotRef2,
                                                       @Mocked StringLiteral literal1,
                                                       @Mocked StringLiteral literal2,
                                                       @Mocked BinaryPredicate leftPred,
                                                       @Mocked BinaryPredicate rightPred,
                                                       @Mocked CompoundPredicate orPredicate) {
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
                result = createPartitionList(100);
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

                insertStmt.getQueryStatement();
                result = queryStatement;
                minTimes = 0;

                queryStatement.getQueryRelation();
                result = selectRelation;
                minTimes = 0;

                selectRelation.hasWhereClause();
                result = true;
                minTimes = 0;

                // WHERE dt = '2024-01-01' OR dt = '2024-01-02'
                selectRelation.getPredicate();
                result = orPredicate;
                minTimes = 0;

                orPredicate.getOp();
                result = CompoundPredicate.Operator.OR;
                minTimes = 0;

                orPredicate.getChild(0);
                result = leftPred;
                minTimes = 0;

                orPredicate.getChild(1);
                result = rightPred;
                minTimes = 0;

                setupBinaryPredicateExpectations(leftPred, slotRef1, literal1, "dt", "2024-01-01", BinaryType.EQ);
                setupBinaryPredicateExpectations(rightPred, slotRef2, literal2, "dt", "2024-01-02", BinaryType.EQ);

                insertStmt.isStaticKeyPartitionInsert();
                result = false;
                minTimes = 0;
            }
        };

        long estimatedCount = InsertPartitionEstimator.estimatePartitionCountForInsert(insertStmt, icebergTable);
        // OR on same column should estimate 2 partitions
        assertEquals(2L, estimatedCount);
    }

    /**
     * Test partition estimation with OR predicate on different columns
     * Expected: estimate = -1 (return -1 due to complexity)
     */
    @Test
    public void testEstimatePartitionWithOrDifferentColumns(@Mocked GlobalStateMgr gsm,
                                                            @Mocked MetadataMgr metadataMgr,
                                                            @Mocked IcebergTable icebergTable,
                                                            @Mocked InsertStmt insertStmt,
                                                            @Mocked QueryStatement queryStatement,
                                                            @Mocked SelectRelation selectRelation,
                                                            @Mocked SlotRef slotRef1,
                                                            @Mocked SlotRef slotRef2,
                                                            @Mocked StringLiteral literal1,
                                                            @Mocked StringLiteral literal2,
                                                            @Mocked BinaryPredicate leftPred,
                                                            @Mocked BinaryPredicate rightPred,
                                                            @Mocked CompoundPredicate orPredicate) {
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
                result = createPartitionList(100);
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
                result = Lists.newArrayList(
                        new Column("dt", DateType.DATE),
                        new Column("country", VarcharType.VARCHAR)
                );
                minTimes = 0;

                insertStmt.getQueryStatement();
                result = queryStatement;
                minTimes = 0;

                queryStatement.getQueryRelation();
                result = selectRelation;
                minTimes = 0;

                selectRelation.hasWhereClause();
                result = true;
                minTimes = 0;

                // WHERE dt = '2024-01-01' OR country = 'US'
                selectRelation.getPredicate();
                result = orPredicate;
                minTimes = 0;

                orPredicate.getOp();
                result = CompoundPredicate.Operator.OR;
                minTimes = 0;

                orPredicate.getChild(0);
                result = leftPred;
                minTimes = 0;

                orPredicate.getChild(1);
                result = rightPred;
                minTimes = 0;

                setupBinaryPredicateExpectations(leftPred, slotRef1, literal1, "dt", "2024-01-01", BinaryType.EQ);
                setupBinaryPredicateExpectations(rightPred, slotRef2, literal2, "country", "US", BinaryType.EQ);

                insertStmt.isStaticKeyPartitionInsert();
                result = false;
                minTimes = 0;
            }
        };

        long estimatedCount = InsertPartitionEstimator.estimatePartitionCountForInsert(insertStmt, icebergTable);
        // OR on different columns should fall back to -1
        assertEquals(-1, estimatedCount);
    }

    /**
     * Test partition estimation with AND predicate on multiple partition columns
     * Expected: estimate = 1 (product of equality predicates: 1 * 1 = 1)
     */
    @Test
    public void testEstimatePartitionWithAndPredicate(@Mocked GlobalStateMgr gsm,
                                                      @Mocked MetadataMgr metadataMgr,
                                                      @Mocked IcebergTable icebergTable,
                                                      @Mocked InsertStmt insertStmt,
                                                      @Mocked QueryStatement queryStatement,
                                                      @Mocked SelectRelation selectRelation,
                                                      @Mocked SlotRef slotRef1,
                                                      @Mocked SlotRef slotRef2,
                                                      @Mocked StringLiteral literal1,
                                                      @Mocked StringLiteral literal2,
                                                      @Mocked BinaryPredicate leftPred,
                                                      @Mocked BinaryPredicate rightPred,
                                                      @Mocked CompoundPredicate andPredicate) {
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
                result = createPartitionList(100);
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
                result = Lists.newArrayList(
                        new Column("dt", DateType.DATE),
                        new Column("country", VarcharType.VARCHAR)
                );
                minTimes = 0;

                insertStmt.getQueryStatement();
                result = queryStatement;
                minTimes = 0;

                queryStatement.getQueryRelation();
                result = selectRelation;
                minTimes = 0;

                selectRelation.hasWhereClause();
                result = true;
                minTimes = 0;

                // WHERE dt = '2024-01-01' AND country = 'US'
                selectRelation.getPredicate();
                result = andPredicate;
                minTimes = 0;

                andPredicate.getOp();
                result = CompoundPredicate.Operator.AND;
                minTimes = 0;

                andPredicate.getChild(0);
                result = leftPred;
                minTimes = 0;

                andPredicate.getChild(1);
                result = rightPred;
                minTimes = 0;

                setupBinaryPredicateExpectations(leftPred, slotRef1, literal1, "dt", "2024-01-01", BinaryType.EQ);
                setupBinaryPredicateExpectations(rightPred, slotRef2, literal2, "country", "US", BinaryType.EQ);

                insertStmt.isStaticKeyPartitionInsert();
                result = false;
                minTimes = 0;
            }
        };

        long estimatedCount = InsertPartitionEstimator.estimatePartitionCountForInsert(insertStmt, icebergTable);
        // AND on multiple partition columns should estimate 1 partition (1 * 1 = 1)
        assertEquals(1L, estimatedCount);
    }

    /**
     * Test partition estimation with range predicate (>)
     * Expected: estimate = 10 (10% of 100 existing partitions)
     */
    @Test
    public void testEstimatePartitionWithRangePredicate(@Mocked GlobalStateMgr gsm,
                                                        @Mocked MetadataMgr metadataMgr,
                                                        @Mocked StatisticStorage statisticStorage,
                                                        @Mocked IcebergTable icebergTable,
                                                        @Mocked InsertStmt insertStmt,
                                                        @Mocked QueryStatement queryStatement,
                                                        @Mocked SelectRelation selectRelation,
                                                        @Mocked NodeMgr nodeMgr,
                                                        @Mocked SystemInfoService clusterInfo,
                                                        @Mocked SlotRef slotRef,
                                                        @Mocked StringLiteral literal) {
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

                metadataMgr.listPartitionNames(anyString, anyString, anyString,
                        withInstanceOf(ConnectorMetadatRequestContext.class));
                result = createPartitionList(100);
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

                gsm.getStatisticStorage();
                result = statisticStorage;
                minTimes = 0;

                // NDV = 100
                ColumnStatistic columnStatistic = new ColumnStatistic(
                        Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 1, 100);
                statisticStorage.getColumnStatistic(icebergTable, "dt");
                result = columnStatistic;
                minTimes = 0;

                insertStmt.getQueryStatement();
                result = queryStatement;
                minTimes = 0;

                queryStatement.getQueryRelation();
                result = selectRelation;
                minTimes = 0;

                selectRelation.hasWhereClause();
                result = true;
                minTimes = 0;

                // WHERE dt > '2024-01-01'
                selectRelation.getPredicate();
                result = createBinaryPredicate(BinaryType.GT, slotRef, literal);
                minTimes = 0;

                slotRef.getColumnName();
                result = "dt";
                minTimes = 0;

                literal.getStringValue();
                result = "2024-01-01";
                minTimes = 0;

                insertStmt.isStaticKeyPartitionInsert();
                result = false;
                minTimes = 0;
            }
        };

        long estimatedCount = InsertPartitionEstimator.estimatePartitionCountForInsert(insertStmt, icebergTable);
        // Range predicate should estimate 10% of NDV
        assertEquals(10L, estimatedCount);
    }

    /**
     * Test partition estimation with NOT IN predicate
     * Expected: estimate = 100 (falls back to existing partition count)
     */
    @Test
    public void testEstimatePartitionWithNotInPredicate(@Mocked GlobalStateMgr gsm,
                                                         @Mocked MetadataMgr metadataMgr,
                                                         @Mocked StatisticStorage statisticStorage,
                                                         @Mocked IcebergTable icebergTable,
                                                         @Mocked InsertStmt insertStmt,
                                                         @Mocked QueryStatement queryStatement,
                                                         @Mocked SelectRelation selectRelation,
                                                         @Mocked NodeMgr nodeMgr,
                                                         @Mocked SystemInfoService clusterInfo,
                                                         @Mocked SlotRef slotRef,
                                                         @Mocked InPredicate notInPredicate) {
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

                metadataMgr.listPartitionNames(anyString, anyString, anyString,
                        withInstanceOf(ConnectorMetadatRequestContext.class));
                result = createPartitionList(100);
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

                gsm.getStatisticStorage();
                result = statisticStorage;
                minTimes = 0;

                ColumnStatistic columnStatistic = new ColumnStatistic(
                        Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 1, 100);
                statisticStorage.getColumnStatistic(icebergTable, "dt");
                result = columnStatistic;
                minTimes = 0;

                insertStmt.getQueryStatement();
                result = queryStatement;
                minTimes = 0;

                queryStatement.getQueryRelation();
                result = selectRelation;
                minTimes = 0;

                selectRelation.hasWhereClause();
                result = true;
                minTimes = 0;

                // WHERE dt NOT IN ('2024-01-01', '2024-01-02')
                selectRelation.getPredicate();
                result = notInPredicate;
                minTimes = 0;

                notInPredicate.isNotIn();
                result = true;
                minTimes = 0;

                notInPredicate.getChild(0);
                result = slotRef;
                minTimes = 0;

                slotRef.getColumnName();
                result = "dt";
                minTimes = 0;

                insertStmt.isStaticKeyPartitionInsert();
                result = false;
                minTimes = 0;
            }
        };

        long estimatedCount = InsertPartitionEstimator.estimatePartitionCountForInsert(insertStmt, icebergTable);
        // NOT IN should fall back to existing partition count
        assertEquals(100L, estimatedCount);
    }

    // ==================== Helper Methods ====================

    private List<String> createPartitionList(int count) {
        List<String> partitions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            partitions.add("p" + i);
        }
        return partitions;
    }

    private void setupBinaryPredicateExpectations(@Mocked BinaryPredicate predicate,
                                                   @Mocked SlotRef slotRef,
                                                   @Mocked StringLiteral literal,
                                                   String columnName,
                                                   String literalValue,
                                                   BinaryType binaryType) {
        new Expectations() {
            {
                predicate.getOp();
                result = binaryType;
                minTimes = 0;

                predicate.getChild(0);
                result = slotRef;
                minTimes = 0;

                predicate.getChild(1);
                result = literal;
                minTimes = 0;

                slotRef.getColumnName();
                result = columnName;
                minTimes = 0;

                literal.getStringValue();
                result = literalValue;
                minTimes = 0;
            }
        };
    }

    private BinaryPredicate createBinaryPredicate(BinaryType type, SlotRef slotRef, StringLiteral literal) {
        BinaryPredicate pred = new BinaryPredicate(type, slotRef, literal);
        return pred;
    }
}
