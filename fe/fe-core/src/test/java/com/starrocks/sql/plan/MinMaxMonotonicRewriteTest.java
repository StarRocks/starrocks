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

package com.starrocks.sql.plan;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions;
import com.starrocks.sql.optimizer.statistics.ColumnMinMaxMgr;
import com.starrocks.sql.optimizer.statistics.IMinMaxStatsMgr;
import com.starrocks.sql.optimizer.statistics.StatsVersion;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MinMaxMonotonicRewriteTest extends PlanTestBase {

    @BeforeAll
    public static void beforeAll() {
        starRocksAssert.getCtx().getSessionVariable().setCboRewriteMonotonicMinmax(true);
    }

    @AfterAll
    public static void afterAll() {
        starRocksAssert.getCtx().getSessionVariable().setCboRewriteMonotonicMinmax(false);
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|',
            value = {
                    "1756099237 | 0 | '2025-08-25 13:20:37'",
                    "1756099237 | 0 | '2025-08-25 13:20:37'",
                    "1756099237001 | 3 | '2025-08-25 13:20:37.001000'",
                    "1756099237001000 | 6 | '2025-08-25 13:20:37.001000'",

                    "-1 | 0 |null",
                    "1756099237000000000 | 4 |null",
            })
    public void testToDatetime(long ts, int scale, String expect) throws Exception {
        ConstantOperator datetime = ScalarOperatorFunctions.toDatetime(ConstantOperator.createBigint(ts),
                ConstantOperator.createInt(scale));
        assertEquals(expect, datetime.toString());
    }

    @ParameterizedTest
    @CsvSource(
            delimiter = '|',
            value = {
                    "select min(to_datetime(t1d)) from test_all_type " +
                            "| to_datetime(13: min) ",
                    "select max(to_datetime(t1d)) from test_all_type " +
                            "| to_datetime(13: max) ",
                    "select min(to_datetime(t1d, 6)) from test_all_type " +
                            "| to_datetime(13: min) ",

                    "select max(from_unixtime(t1d)) from test_all_type " +
                            "| from_unixtime(13: max) ",
                    "select max(from_unixtime(get_json_int(v_json, 'ts'))) from tjson " +
                            "| from_unixtime(6: max)",
                    "select date_diff('millisecond',    " +
                            "   min(from_unixtime(t1d)), " +
                            "   max(from_unixtime(t1d))) from test_all_type " +
                            "|  from_unixtime(15: min)",
                    "select date_diff('millisecond',    " +
                            "   min(to_datetime(t1d)), " +
                            "   max(to_datetime(t1d))) from test_all_type " +
                            "|  to_datetime(15: min)",
                    "select date_diff('millisecond',    " +
                            "   min(to_datetime(get_json_int(v_json, 'ts'), 6)), " +
                            "   max(to_datetime(get_json_int(v_json, 'ts'), 6))) from tjson" +
                            "|  to_datetime(8: min)",
            })
    public void testRewriteMinMaxMonotonic(String sql, String expectedAggregation)
            throws Exception {
        {
            // with MinMaxStats
            new MockUp<ColumnMinMaxMgr>() {
                @Mock
                public Optional<IMinMaxStatsMgr.ColumnMinMax> getStats(ColumnIdentifier identifier,
                                                                       StatsVersion version) {
                    return Optional.of(new IMinMaxStatsMgr.ColumnMinMax("1", "100"));
                }
            };
            String plan = getFragmentPlan(sql);
            assertContains(plan, expectedAggregation);
        }

        {
            // without MinMaxStats
            new MockUp<ColumnMinMaxMgr>() {
                @Mock
                public Optional<IMinMaxStatsMgr.ColumnMinMax> getStats(ColumnIdentifier identifier,
                                                                       StatsVersion version) {
                    return Optional.empty();
                }
            };
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, expectedAggregation);
        }

        {
            // invalid MinMaxStats
            new MockUp<ColumnMinMaxMgr>() {
                @Mock
                public Optional<IMinMaxStatsMgr.ColumnMinMax> getStats(ColumnIdentifier identifier,
                                                                       StatsVersion version) {
                    return Optional.of(new IMinMaxStatsMgr.ColumnMinMax("-1", "100"));
                }
            };
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, expectedAggregation);
        }
    }

    @ParameterizedTest
    @CsvSource(
            delimiter = '|',
            value = {
                    "select min(t1d + 1) from test_all_type " +
                            "| output: min(4: t1d + 1)",
                    "select max(t1d * 2) from test_all_type " +
                            "| output: max(4: t1d * 2)",
                    "select min(concat(t1a, 'suffix')) from test_all_type " +
                            "| output: min(concat(1: t1a, 'suffix'))",
                    "select max(abs(t1d)) from test_all_type " +
                            "| output: max(abs(4: t1d))",
                    "select min(if(t1d > 0, t1d, 0)) from test_all_type " +
                            "| output: min(if(4: t1d > 0, 4: t1d, 0))",
                    "select min(cast(t1d as date)) from test_all_type " +
                            "| output: min(CAST(4: t1d AS DATE))",
                    "select min(t1d) from test_all_type having min(t1d) > 100 " +
                            "| output: min(4: t1d)",
            })
    public void testNoRewriteForNonMonotonicExpressions(String sql, String expectedAggregation)
            throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, expectedAggregation);
    }

    @Test
    public void testColumnMinMaxMgrGenSql() throws Exception {
        // Test regular column
        Database mockDb = mock(Database.class);
        when(mockDb.getOriginName()).thenReturn("test_db");

        OlapTable mockTable = mock(OlapTable.class);
        when(mockTable.getName()).thenReturn("test_table");

        Column regularColumn = mock(Column.class);
        when(regularColumn.getName()).thenReturn("id");
        when(regularColumn.getType()).thenReturn(Type.BIGINT);
        when(regularColumn.getColumnId()).thenReturn(ColumnId.create("id"));

        String sql1 = ColumnMinMaxMgr.genMinMaxSql("default_catalog", mockDb, mockTable, regularColumn);
        String expectedSql1 =
                "select min(id) as min, max(id) as max from `default_catalog`.`test_db`.`test_table`[_META_];";
        assertEquals(expectedSql1, sql1);

        // Test JSON column with BIGINT type
        Column jsonColumn = mock(Column.class);
        when(jsonColumn.getName()).thenReturn("json_col");
        when(jsonColumn.getType()).thenReturn(Type.BIGINT);
        when(jsonColumn.getColumnId()).thenReturn(ColumnId.create("json_col.field1"));

        String sql2 = ColumnMinMaxMgr.genMinMaxSql("default_catalog", mockDb, mockTable, jsonColumn);
        String expectedSql2 = "select min(get_json_int(`json_col`, 'field1')) as min, " +
                "max(get_json_int(`json_col`, 'field1')) as max " +
                "from `default_catalog`.`test_db`.`test_table`[_META_];";
        assertEquals(expectedSql2, sql2);

        // Test JSON column with STRING type
        when(jsonColumn.getName()).thenReturn("json_col");
        when(jsonColumn.getType()).thenReturn(Type.VARCHAR);
        when(jsonColumn.getColumnId()).thenReturn(ColumnId.create("json_col.field2"));

        String sql3 = ColumnMinMaxMgr.genMinMaxSql("default_catalog", mockDb, mockTable, jsonColumn);
        String expectedSql3 = "select min(get_json_string(`json_col`, 'field2')) as min, " +
                "max(get_json_string(`json_col`, 'field2')) as max " +
                "from `default_catalog`.`test_db`.`test_table`[_META_];";
        assertEquals(expectedSql3, sql3);
    }
}