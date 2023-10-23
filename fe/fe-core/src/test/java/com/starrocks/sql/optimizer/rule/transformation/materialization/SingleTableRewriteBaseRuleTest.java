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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.SingleTableRewriteBaseRule;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class SingleTableRewriteBaseRuleTest {
    @Test
    public void testComparator() {
        // test different output row count
        {
            Map<ColumnRefOperator, ColumnStatistic> columnStatistics = Maps.newHashMap();
            ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(1, Type.INT, "a", true);
            ColumnStatistic columnStatistic1 = new ColumnStatistic(10, 1000, 0, 4, 100);
            ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(2, Type.INT, "b", true);
            ColumnStatistic columnStatistic2 = new ColumnStatistic(10, 1000, 0, 4, 100);
            columnStatistics.put(columnRefOperator1, columnStatistic1);
            columnStatistics.put(columnRefOperator2, columnStatistic2);
            Statistics.Builder statisticsBuilder1 = new Statistics.Builder();
            statisticsBuilder1.setTableRowCountMayInaccurate(true);
            statisticsBuilder1.setOutputRowCount(100);
            statisticsBuilder1.addColumnStatistics(columnStatistics);
            Statistics statistics1 = statisticsBuilder1.build();

            Map<ColumnRefOperator, ColumnStatistic> columnStatistics2 = Maps.newHashMap();
            ColumnRefOperator columnRefOperator3 = new ColumnRefOperator(1, Type.INT, "a", true);
            ColumnStatistic columnStatistic3 = new ColumnStatistic(10, 1000, 0, 4, 100);
            ColumnRefOperator columnRefOperator4 = new ColumnRefOperator(2, Type.INT, "b", true);
            ColumnStatistic columnStatistic4 = new ColumnStatistic(10, 1000, 0, 4, 100);
            columnStatistics2.put(columnRefOperator3, columnStatistic3);
            columnStatistics2.put(columnRefOperator4, columnStatistic4);
            Statistics.Builder statisticsBuilder2 = new Statistics.Builder();
            statisticsBuilder2.setTableRowCountMayInaccurate(true);
            statisticsBuilder2.setOutputRowCount(10000);
            statisticsBuilder2.addColumnStatistics(columnStatistics2);
            Statistics statistics2 = statisticsBuilder2.build();

            Map<ColumnRefOperator, ColumnStatistic> columnStatistics3 = Maps.newHashMap();
            ColumnRefOperator columnRefOperator5 = new ColumnRefOperator(1, Type.INT, "a", true);
            ColumnStatistic columnStatistic5 = new ColumnStatistic(10, 1000, 0, 4, 100);
            ColumnRefOperator columnRefOperator6 = new ColumnRefOperator(2, Type.INT, "b", true);
            ColumnStatistic columnStatistic6 = new ColumnStatistic(10, 1000, 0, 4, 100);
            columnStatistics3.put(columnRefOperator5, columnStatistic5);
            columnStatistics3.put(columnRefOperator6, columnStatistic6);
            Statistics.Builder statisticsBuilder3 = new Statistics.Builder();
            statisticsBuilder3.setTableRowCountMayInaccurate(true);
            statisticsBuilder3.setOutputRowCount(1000);
            statisticsBuilder3.addColumnStatistics(columnStatistics3);
            Statistics statistics3 = statisticsBuilder3.build();

            {
                List<SingleTableRewriteBaseRule.CandidateContext> contexts = Lists.newArrayList();
                contexts.add(new SingleTableRewriteBaseRule.CandidateContext(statistics1, 1));
                contexts.add(new SingleTableRewriteBaseRule.CandidateContext(statistics2, 2));
                contexts.add(new SingleTableRewriteBaseRule.CandidateContext(statistics3, 3));
                contexts.sort(new SingleTableRewriteBaseRule.CandidateContextComparator());
                Assert.assertEquals(1, contexts.get(0).getIndex());
                Assert.assertEquals(3, contexts.get(1).getIndex());
                Assert.assertEquals(2, contexts.get(2).getIndex());
            }

            {
                List<SingleTableRewriteBaseRule.CandidateContext> contexts = Lists.newArrayList();
                contexts.add(new SingleTableRewriteBaseRule.CandidateContext(statistics2, 2));
                contexts.add(new SingleTableRewriteBaseRule.CandidateContext(statistics3, 3));
                contexts.add(new SingleTableRewriteBaseRule.CandidateContext(statistics1, 1));
                contexts.sort(new SingleTableRewriteBaseRule.CandidateContextComparator());
                Assert.assertEquals(1, contexts.get(0).getIndex());
                Assert.assertEquals(3, contexts.get(1).getIndex());
                Assert.assertEquals(2, contexts.get(2).getIndex());
            }
        }

        // test different compute size
        {
            Map<ColumnRefOperator, ColumnStatistic> columnStatistics = Maps.newHashMap();
            ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(1, Type.INT, "a", true);
            ColumnStatistic columnStatistic1 = new ColumnStatistic(10, 1000, 0, 4, 100);
            columnStatistics.put(columnRefOperator1, columnStatistic1);
            Statistics.Builder statisticsBuilder1 = new Statistics.Builder();
            statisticsBuilder1.setTableRowCountMayInaccurate(true);
            statisticsBuilder1.setOutputRowCount(100);
            statisticsBuilder1.addColumnStatistics(columnStatistics);
            Statistics statistics1 = statisticsBuilder1.build();

            Map<ColumnRefOperator, ColumnStatistic> columnStatistics2 = Maps.newHashMap();
            ColumnRefOperator columnRefOperator3 = new ColumnRefOperator(1, Type.INT, "a", true);
            ColumnStatistic columnStatistic3 = new ColumnStatistic(10, 1000, 0, 4, 100);
            ColumnRefOperator columnRefOperator4 = new ColumnRefOperator(2, Type.INT, "b", true);
            ColumnStatistic columnStatistic4 = new ColumnStatistic(10, 1000, 0, 4, 100);
            columnStatistics2.put(columnRefOperator3, columnStatistic3);
            columnStatistics2.put(columnRefOperator4, columnStatistic4);
            Statistics.Builder statisticsBuilder2 = new Statistics.Builder();
            statisticsBuilder2.setTableRowCountMayInaccurate(true);
            statisticsBuilder2.setOutputRowCount(100);
            statisticsBuilder2.addColumnStatistics(columnStatistics2);
            Statistics statistics2 = statisticsBuilder2.build();

            Map<ColumnRefOperator, ColumnStatistic> columnStatistics3 = Maps.newHashMap();
            ColumnRefOperator columnRefOperator5 = new ColumnRefOperator(1, Type.INT, "a", true);
            ColumnStatistic columnStatistic5 = new ColumnStatistic(10, 1000, 0, 4, 100);
            ColumnRefOperator columnRefOperator6 = new ColumnRefOperator(2, Type.INT, "b", true);
            ColumnStatistic columnStatistic6 = new ColumnStatistic(10, 1000, 0, 4, 100);
            ColumnRefOperator columnRefOperator7 = new ColumnRefOperator(3, Type.INT, "c", true);
            ColumnStatistic columnStatistic7 = new ColumnStatistic(10, 1000, 0, 4, 100);
            columnStatistics3.put(columnRefOperator5, columnStatistic5);
            columnStatistics3.put(columnRefOperator6, columnStatistic6);
            columnStatistics3.put(columnRefOperator7, columnStatistic7);
            Statistics.Builder statisticsBuilder3 = new Statistics.Builder();
            statisticsBuilder3.setTableRowCountMayInaccurate(true);
            statisticsBuilder3.setOutputRowCount(100);
            statisticsBuilder3.addColumnStatistics(columnStatistics3);
            Statistics statistics3 = statisticsBuilder3.build();

            {
                List<SingleTableRewriteBaseRule.CandidateContext> contexts = Lists.newArrayList();
                contexts.add(new SingleTableRewriteBaseRule.CandidateContext(statistics1, 1));
                contexts.add(new SingleTableRewriteBaseRule.CandidateContext(statistics2, 2));
                contexts.add(new SingleTableRewriteBaseRule.CandidateContext(statistics3, 3));
                contexts.sort(new SingleTableRewriteBaseRule.CandidateContextComparator());
                Assert.assertEquals(1, contexts.get(0).getIndex());
                Assert.assertEquals(2, contexts.get(1).getIndex());
                Assert.assertEquals(3, contexts.get(2).getIndex());
            }

            {
                List<SingleTableRewriteBaseRule.CandidateContext> contexts = Lists.newArrayList();
                contexts.add(new SingleTableRewriteBaseRule.CandidateContext(statistics2, 2));
                contexts.add(new SingleTableRewriteBaseRule.CandidateContext(statistics3, 3));
                contexts.add(new SingleTableRewriteBaseRule.CandidateContext(statistics1, 1));
                contexts.sort(new SingleTableRewriteBaseRule.CandidateContextComparator());
                Assert.assertEquals(1, contexts.get(0).getIndex());
                Assert.assertEquals(2, contexts.get(1).getIndex());
                Assert.assertEquals(3, contexts.get(2).getIndex());
            }

            {
                List<SingleTableRewriteBaseRule.CandidateContext> contexts = Lists.newArrayList();
                contexts.add(new SingleTableRewriteBaseRule.CandidateContext(statistics2, 2));
                contexts.add(new SingleTableRewriteBaseRule.CandidateContext(statistics1, 1));
                contexts.add(new SingleTableRewriteBaseRule.CandidateContext(statistics3, 3));
                contexts.sort(new SingleTableRewriteBaseRule.CandidateContextComparator());
                Assert.assertEquals(1, contexts.get(0).getIndex());
                Assert.assertEquals(2, contexts.get(1).getIndex());
                Assert.assertEquals(3, contexts.get(2).getIndex());
            }
        }
    }
}
