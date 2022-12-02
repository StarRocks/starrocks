// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TStatisticData;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.List;

public class StatisticExecutorTest extends PlanTestBase {
    @Test
    public void testEmpty() throws Exception {
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        Database db = GlobalStateMgr.getCurrentState().getDb(10002);
        OlapTable olapTable = (OlapTable) db.getTable("t0");

        GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(new BasicStatsMeta(10002, olapTable.getId(),null,
                StatsConstants.AnalyzeType.FULL,
                LocalDateTime.of(2020, 1, 1, 1, 1, 1),
                Maps.newHashMap()));

        Assert.assertThrows(IllegalStateException.class,
                () -> statisticExecutor.queryStatisticSync(
                        StatisticUtils.buildConnectContext(), db.getId(), olapTable.getId(), Lists.newArrayList("foo", "bar")));
    }

    @Test
    public void testDroppedDB() throws Exception {
        StatisticExecutor statisticExecutor = new StatisticExecutor();


        GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(new BasicStatsMeta(10002, 10003, null,
                StatsConstants.AnalyzeType.FULL,
                LocalDateTime.of(2020, 1, 1, 1, 1, 1),
                Maps.newHashMap()));

        List<TStatisticData> stats = statisticExecutor.queryStatisticSync(
                StatisticUtils.buildConnectContext(), null, 10003L, Lists.newArrayList("foo", "bar"));
        Assert.assertEquals(0, stats.size());
    }
}
