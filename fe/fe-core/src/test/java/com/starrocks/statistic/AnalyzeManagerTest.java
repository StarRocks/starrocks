// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;

public class AnalyzeManagerTest extends PlanTestBase {
    @Test
    public void testClearStatisticFromDroppedTable() {
        GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(new BasicStatsMeta(
                1, 2, Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                LocalDateTime.MIN, Maps.newHashMap()));
        Assert.assertNotNull(GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().get(2L));
        GlobalStateMgr.getCurrentAnalyzeMgr().clearStatisticFromDroppedTable();
        Assert.assertNull(GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().get(2L));
    }
}
