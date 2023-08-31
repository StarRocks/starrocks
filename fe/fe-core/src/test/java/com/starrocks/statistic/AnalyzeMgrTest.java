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

package com.starrocks.statistic;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorTableColumnStats;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.statistics.CachedStatisticStorage;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.BeforeClass;
import org.junit.Test;

public class AnalyzeMgrTest {
    public static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    @Test
    public void testRefreshConnectorTableBasicStatisticsCache(@Mocked CachedStatisticStorage cachedStatisticStorage) {
        Table table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("hive0", "partitioned_db", "t1");
        new Expectations() {
            {
                cachedStatisticStorage.getConnectorTableStatistics(table, ImmutableList.of("c1", "c2"));
                result = ImmutableList.of(
                        new ConnectorTableColumnStats(new ColumnStatistic(0, 10, 0, 20, 5), 5),
                        new ConnectorTableColumnStats(new ColumnStatistic(0, 100, 0, 200, 50), 50)
                );
                minTimes = 1;
            }
        };


        AnalyzeMgr analyzeMgr = new AnalyzeMgr();
        analyzeMgr.refreshConnectorTableBasicStatisticsCache(table, ImmutableList.of("c1", "c2"), true);

        new Expectations() {
            {
                cachedStatisticStorage.getConnectorTableStatisticsSync(table, ImmutableList.of("c1", "c2"));
                result = ImmutableList.of(
                        new ConnectorTableColumnStats(new ColumnStatistic(0, 10, 0, 20, 5), 5),
                        new ConnectorTableColumnStats(new ColumnStatistic(0, 100, 0, 200, 50), 50)
                );
                minTimes = 1;
            }
        };
        analyzeMgr.refreshConnectorTableBasicStatisticsCache(table, ImmutableList.of("c1", "c2"), false);
    }
}
