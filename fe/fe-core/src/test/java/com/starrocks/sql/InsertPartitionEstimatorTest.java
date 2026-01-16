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
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.type.DateType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for InsertPartitionEstimator
 */
public class InsertPartitionEstimatorTest {

    // ==================== estimatePartitionCountForInsert Tests ====================

    @Test
    public void testEstimatePartitionCountForInsert_MetadataUnavailable(@Mocked InsertStmt insertStmt,
                                                                         @Mocked IcebergTable table) {
        new Expectations() {
            {
                table.getCatalogName();
                result = new RuntimeException("Connection failed");
                minTimes = 0;
            }
        };

        long result = InsertPartitionEstimator.estimatePartitionCountForInsert(insertStmt, table);
        assertEquals(-1L, result);
    }

    @Test
    public void testEstimatePartitionCountForInsert_EmptyPartitionList(@Mocked InsertStmt insertStmt,
                                                                        @Mocked IcebergTable table,
                                                                        @Mocked com.starrocks.server.GlobalStateMgr gsm,
                                                                        @Mocked com.starrocks.server.MetadataMgr metadataMgr,
                                                                        @Mocked QueryStatement queryStatement,
                                                                        @Mocked QueryRelation queryRelation,
                                                                        @Mocked SelectRelation selectRelation) {
        new Expectations() {
            {
                com.starrocks.server.GlobalStateMgr.getCurrentState();
                result = gsm;
                minTimes = 0;

                gsm.getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                metadataMgr.listPartitionNames(anyString, anyString, anyString,
                        withInstanceOf(ConnectorMetadatRequestContext.class));
                result = Lists.newArrayList();  // Empty partition list
                minTimes = 0;

                table.getCatalogName();
                result = "test_catalog";
                minTimes = 0;

                table.getCatalogDBName();
                result = "test_db";
                minTimes = 0;

                table.getCatalogTableName();
                result = "test_table";
                minTimes = 0;

                table.getPartitionColumns();
                result = Lists.newArrayList(new Column("dt", DateType.DATE));
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

                gsm.getStatisticStorage();
                result = null;
                minTimes = 0;
            }
        };

        long result = InsertPartitionEstimator.estimatePartitionCountForInsert(insertStmt, table);
        assertTrue(result <= 0);
    }

    // ==================== estimatePartitionCount Tests ====================

    @Test
    public void testEstimatePartitionCount_StaticPartitionInsert(@Mocked InsertStmt insertStmt) {
        new Expectations() {
            {
                insertStmt.isStaticKeyPartitionInsert();
                result = true;
                minTimes = 0;
            }
        };

        long result = InsertPartitionEstimator.estimatePartitionCount(
                insertStmt, null, 100L, false, null);
        assertEquals(1L, result);
    }

    @Test
    public void testEstimatePartitionCount_NoWhereClauseWithNdv(@Mocked InsertStmt insertStmt) {
        new Expectations() {
            {
                insertStmt.isStaticKeyPartitionInsert();
                result = false;
                minTimes = 0;
            }
        };

        Map<String, Double> ndv = new HashMap<>();
        ndv.put("dt", 10.0);
        ndv.put("country", 5.0);

        long result = InsertPartitionEstimator.estimatePartitionCount(
                insertStmt, null, 0L, false, ndv);
        assertEquals(50L, result);
    }

    @Test
    public void testEstimatePartitionCount_NoWhereClauseExistingPartitions(@Mocked InsertStmt insertStmt) {
        new Expectations() {
            {
                insertStmt.isStaticKeyPartitionInsert();
                result = false;
                minTimes = 0;
            }
        };

        long result = InsertPartitionEstimator.estimatePartitionCount(
                insertStmt, null, 100L, false, null);
        assertEquals(100L, result);
    }

    // ==================== estimateFromStatistics Tests ====================

    @Test
    public void testEstimateFromStatistics_NullMap() {
        long result = InsertPartitionEstimator.estimateFromStatistics(null);
        assertEquals(-1L, result);
    }

    @Test
    public void testEstimateFromStatistics_EmptyMap() {
        long result = InsertPartitionEstimator.estimateFromStatistics(new HashMap<>());
        assertEquals(-1L, result);
    }

    @Test
    public void testEstimateFromStatistics_InvalidNdv() {
        Map<String, Double> ndv = new HashMap<>();
        ndv.put("dt", Double.NaN);

        long result = InsertPartitionEstimator.estimateFromStatistics(ndv);
        assertEquals(-1L, result);
    }

    @Test
    public void testEstimateFromStatistics_NegativeNdv() {
        Map<String, Double> ndv = new HashMap<>();
        ndv.put("dt", -1.0);

        long result = InsertPartitionEstimator.estimateFromStatistics(ndv);
        assertEquals(-1L, result);
    }

    @Test
    public void testEstimateFromStatistics_ZeroNdv() {
        Map<String, Double> ndv = new HashMap<>();
        ndv.put("dt", 0.0);

        long result = InsertPartitionEstimator.estimateFromStatistics(ndv);
        assertEquals(-1L, result);
    }

    @Test
    public void testEstimateFromStatistics_Overflow() {
        Map<String, Double> ndv = new HashMap<>();
        ndv.put("col1", (double) Long.MAX_VALUE);
        ndv.put("col2", 2.0);

        long result = InsertPartitionEstimator.estimateFromStatistics(ndv);
        assertEquals(-1L, result);
    }

    @Test
    public void testEstimateFromStatistics_Success() {
        Map<String, Double> ndv = new HashMap<>();
        ndv.put("dt", 10.0);
        ndv.put("country", 5.0);

        long result = InsertPartitionEstimator.estimateFromStatistics(ndv);
        assertEquals(50L, result);
    }

    // ==================== multiplyWithOverflowCheck Tests ====================

    @Test
    public void testMultiplyWithOverflowCheck_ZeroBase() {
        long result = InsertPartitionEstimator.multiplyWithOverflowCheck(0, 100);
        assertEquals(0L, result);
    }

    @Test
    public void testMultiplyWithOverflowCheck_ZeroFactor() {
        long result = InsertPartitionEstimator.multiplyWithOverflowCheck(100, 0);
        assertEquals(0L, result);
    }

    @Test
    public void testMultiplyWithOverflowCheck_Overflow() {
        long result = InsertPartitionEstimator.multiplyWithOverflowCheck(Long.MAX_VALUE, 2);
        assertEquals(-1L, result);
    }

    @Test
    public void testMultiplyWithOverflowCheck_Success() {
        long result = InsertPartitionEstimator.multiplyWithOverflowCheck(10, 5);
        assertEquals(50L, result);
    }

    // ==================== buildSelectToTargetPartitionMapping Tests ====================

    @Test
    public void testBuildSelectToTargetPartitionMapping_NoPartitionColumns(@Mocked InsertStmt insertStmt,
                                                                            @Mocked IcebergTable table,
                                                                            @Mocked SelectRelation selectRelation) {
        new Expectations() {
            {
                table.getPartitionColumns();
                result = Lists.newArrayList();
                minTimes = 0;
            }
        };

        Map<String, String> result = Deencapsulation.invoke(
                InsertPartitionEstimator.class,
                "buildSelectToTargetPartitionMapping",
                insertStmt, table, selectRelation);

        assertTrue(result.isEmpty());
    }

    @Test
    public void testBuildSelectToTargetPartitionMapping_NullSelectList(@Mocked InsertStmt insertStmt,
                                                                        @Mocked IcebergTable table,
                                                                        @Mocked SelectRelation selectRelation) {
        Column partCol = new Column("dt", DateType.DATE);
        new Expectations() {
            {
                table.getPartitionColumns();
                result = Lists.newArrayList(partCol);
                minTimes = 0;

                selectRelation.getSelectList();
                result = null;
                minTimes = 0;
            }
        };

        Map<String, String> result = Deencapsulation.invoke(
                InsertPartitionEstimator.class,
                "buildSelectToTargetPartitionMapping",
                insertStmt, table, selectRelation);

        assertTrue(result.isEmpty());
    }

    @Test
    public void testBuildSelectToTargetPartitionMapping_EmptySelectItems(@Mocked InsertStmt insertStmt,
                                                                          @Mocked IcebergTable table,
                                                                          @Mocked SelectRelation selectRelation,
                                                                          @Mocked SelectList selectList) {
        Column partCol = new Column("dt", DateType.DATE);
        new Expectations() {
            {
                table.getPartitionColumns();
                result = Lists.newArrayList(partCol);
                minTimes = 0;

                selectRelation.getSelectList();
                result = selectList;
                minTimes = 0;

                selectList.getItems();
                result = Lists.newArrayList();
                minTimes = 0;
            }
        };

        Map<String, String> result = Deencapsulation.invoke(
                InsertPartitionEstimator.class,
                "buildSelectToTargetPartitionMapping",
                insertStmt, table, selectRelation);

        assertTrue(result.isEmpty());
    }

    // ==================== extractSourceColumnName Tests ====================

    @Test
    public void testExtractSourceColumnName_NullExpr(@Mocked SelectListItem selectItem,
                                                      @Mocked SelectRelation selectRelation) {
        new Expectations() {
            {
                selectItem.getExpr();
                result = null;
                minTimes = 0;
            }
        };

        String result = Deencapsulation.invoke(
                InsertPartitionEstimator.class,
                "extractSourceColumnName",
                selectItem, selectRelation, 0);

        assertNull(result);
    }

    // ==================== getSourceColumnNameForStarSelect Tests ====================

    @Test
    public void testGetSourceColumnNameForStarSelect_NullRelation(@Mocked SelectRelation selectRelation) {
        new Expectations() {
            {
                selectRelation.getRelation();
                result = null;
                minTimes = 0;
            }
        };

        String result = Deencapsulation.invoke(
                InsertPartitionEstimator.class,
                "getSourceColumnNameForStarSelect",
                selectRelation, 0);

        assertNull(result);
    }

    @Test
    public void testEstimatePartitionCountForInsert_ExceptionInGetNdv(@Mocked InsertStmt insertStmt,
                                                                       @Mocked IcebergTable table,
                                                                       @Mocked com.starrocks.server.GlobalStateMgr gsm,
                                                                       @Mocked com.starrocks.server.MetadataMgr metadataMgr,
                                                                       @Mocked QueryStatement queryStatement,
                                                                       @Mocked QueryRelation queryRelation,
                                                                       @Mocked SelectRelation selectRelation,
                                                                       @Mocked StatisticStorage statisticStorage) {
        new Expectations() {
            {
                com.starrocks.server.GlobalStateMgr.getCurrentState();
                result = gsm;
                minTimes = 0;

                gsm.getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                metadataMgr.listPartitionNames(anyString, anyString, anyString,
                        withInstanceOf(ConnectorMetadatRequestContext.class));
                result = Lists.newArrayList("p1", "p2");
                minTimes = 0;

                table.getCatalogName();
                result = "catalog";
                minTimes = 0;

                table.getCatalogDBName();
                result = "db";
                minTimes = 0;

                table.getCatalogTableName();
                result = "table";
                minTimes = 0;

                table.getPartitionColumns();
                result = Lists.newArrayList(new Column("dt", DateType.DATE));
                minTimes = 0;

                gsm.getStatisticStorage();
                result = statisticStorage;
                minTimes = 0;

                statisticStorage.getColumnStatistic(table, "dt");
                result = new RuntimeException("Statistics error");
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
            }
        };

        long result = InsertPartitionEstimator.estimatePartitionCountForInsert(insertStmt, table);
        // Should return existing partition count when statistics fetch fails
        assertEquals(2L, result);
    }
}
