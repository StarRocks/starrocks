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

import com.google.common.collect.Sets;
import com.starrocks.catalog.OlapTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.MultiColumnCombinedStatistics;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import mockit.Expectations;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static com.starrocks.sql.optimizer.Utils.getLongFromDateTime;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;

public class MultipleEqualityPredicateCardinalityEstimationTest extends PlanWithCostTestBase {
    @Before
    public void before() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable table = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("test_all_type");

        long tableRowCount = 10000;
        setTableStatistics(table, tableRowCount);

        StatisticStorage ss = GlobalStateMgr.getCurrentState().getStatisticStorage();
        new Expectations(ss) {
            {
                ss.getColumnStatistic(table, "t1c");
                result = new ColumnStatistic(0, 100, 0, 4, tableRowCount / 100.0);
                minTimes = 0;
                ss.getColumnStatistic(table, "t1d");
                result = new ColumnStatistic(0, 100, 0, 4, tableRowCount / 100.0);
                minTimes = 0;

                ss.getColumnStatistic(table, "t1a");
                result = new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 25, tableRowCount / 100.0);
                minTimes = 0;

                ss.getColumnStatistic(table, "id_decimal");
                result = new ColumnStatistic(1.0, 200.0, 0, 8, tableRowCount / 100.0);
                minTimes = 0;

                ss.getColumnStatistic(table, "id_date");
                result = new ColumnStatistic(getLongFromDateTime(formatDateFromString("1992-01-02")),
                        getLongFromDateTime(formatDateFromString("1998-12-01")), 0, 4, 10);
            }
        };
    }

    @Test
    public void testCardinalityEstimation() throws Exception {
        connectContext.getSessionVariable().setUseCorrelatedPredicateEstimate(false);
        String sql = "select * from test_all_type where t1c = 1 and t1d = 1";
        String plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 1");

        connectContext.getSessionVariable().setUseCorrelatedPredicateEstimate(true);
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 10");

        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb("test").getTable("test_all_type");
        StatisticStorage ss = GlobalStateMgr.getCurrentState().getStatisticStorage();
        new Expectations(ss) {
            {
                ss.getMultiColumnCombinedStatistics(table.getId());
                result = new MultiColumnCombinedStatistics(Sets.newHashSet(table.getColumn("t1c").getUniqueId(),
                        table.getColumn("t1d").getUniqueId()), 100);
                minTimes = 0;
            }
        };

        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 100");

        sql = "select * from test_all_type where t1c = -999 and t1d = 1";
        plan = getCostExplain(sql);
        assertCContains(plan, "cardinality: 1");

        new Expectations(ss) {
            {
                ss.getColumnStatistic(table, "t1c");
                result = new ColumnStatistic(0, 100, 0.1, 4, 100);
                minTimes = 0;
                ss.getColumnStatistic(table, "t1d");
                result = new ColumnStatistic(0, 100, 0.2, 4, 100);
                minTimes = 0;
            }
        };

        sql = "select * from  test_all_type where t1c = 1 and t1d = 1";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 80");

        sql = "select * from test_all_type where t1a = \"xx\" and id_decimal = 222.22 and id_date = \"1995-01-01\"";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 1");

        new Expectations(ss) {
            {
                ss.getMultiColumnCombinedStatistics(table.getId());
                result = new MultiColumnCombinedStatistics(Sets.newHashSet(
                        table.getColumn("t1a").getUniqueId(),
                        table.getColumn("id_decimal").getUniqueId(),
                        table.getColumn("id_date").getUniqueId()), 5);
                minTimes = 0;
            }
        };

        sql = "select * from test_all_type where t1a = \"xx\" and id_decimal = 222.22 and id_date = \"1995-01-01\" and t1e = 6";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 1");

        setTableStatistics(table, 10000000);

        sql = "SELECT * FROM test_all_type \n" +
                "WHERE t1c = 75 AND t1d = 75\n" +
                "  AND t1a IN (-1000, 0, 1000)\n" +
                "  AND id_decimal > 100";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 114");

        sql = "SELECT * FROM test_all_type \n" +
                "WHERE id_date BETWEEN '1995-01-01' AND '1997-12-31'\n" +
                "  AND t1c = 25 \n" +
                "  AND t1d = 25";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 3291");

        new Expectations(ss) {
            {
                ss.getMultiColumnCombinedStatistics(table.getId());
                result = new MultiColumnCombinedStatistics(Sets.newHashSet(table.getColumn("t1c").getUniqueId(),
                        table.getColumn("t1d").getUniqueId()), 100);
                minTimes = 0;
            }
        };

        sql = "SELECT * FROM test_all_type \n" +
                "WHERE (t1c = 10 AND t1d = 90) \n" +
                "   OR (t1a > 5000 AND id_decimal < 50.5)\n" +
                "   OR (id_date > '1998-01-01' AND t1c < 20)";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 1517112");

        sql = "SELECT * FROM test_all_type \n" +
                "WHERE t1c = 33 \n" +
                "  AND t1d = 33\n" +
                "  AND id_decimal BETWEEN 100.0 AND 150.0\n" +
                "  AND id_date > '1995-01-01'\n" +
                "  AND id_date < '1998-01-01'\n" +
                "  AND t1a IS NOT NULL";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 8725");
    }

    private LocalDateTime formatDateFromString(String dateStr) {
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate date = LocalDate.parse(dateStr, fmt);
        return date.atStartOfDay();
    }
}
