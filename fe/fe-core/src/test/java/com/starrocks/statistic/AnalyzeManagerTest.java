// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.TransactionState;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.UUID;

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

    @Test
    public void testCheckoutAnalyzeTime() {
        //2022-12-01 16:01:02
        LocalTime time = Instant.ofEpochMilli(1669881662000L).atZone(TimeUtils.getTimeZone().toZoneId()).toLocalTime();

        StatisticAutoCollector statisticAutoCollector
                = Deencapsulation.newInstance(StatisticAutoCollector.class);
        boolean result = Deencapsulation.invoke(statisticAutoCollector, "checkoutAnalyzeTime", time);
        Assert.assertTrue(result);

        Config.statistic_auto_analyze_start_time = "20:00:00";
        Config.statistic_auto_analyze_end_time = "06:00:00";
        statisticAutoCollector = Deencapsulation.newInstance(StatisticAutoCollector.class);
        result = Deencapsulation.invoke(statisticAutoCollector, "checkoutAnalyzeTime", time);
        Assert.assertFalse(result);

        Config.statistic_auto_analyze_start_time = "06:00:00";
        Config.statistic_auto_analyze_end_time = "17:00:00";
        statisticAutoCollector = Deencapsulation.newInstance(StatisticAutoCollector.class);
        result = Deencapsulation.invoke(statisticAutoCollector, "checkoutAnalyzeTime", time);
        Assert.assertTrue(result);

        Config.statistic_auto_analyze_start_time = "36:00:00";
        Config.statistic_auto_analyze_end_time = "xx:00:00";
        statisticAutoCollector = Deencapsulation.newInstance(StatisticAutoCollector.class);
        result = Deencapsulation.invoke(statisticAutoCollector, "checkoutAnalyzeTime", time);
        Assert.assertTrue(result);
    }

    @Test
    public void testUpdateLoadRowsWithTableDropped() {
        long dbId = 11111L;
        long tableId = 22222L;
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(new Database(dbId, "test"));
        GlobalStateMgr.getCurrentState().getAnalyzeManager().addBasicStatsMeta(new BasicStatsMeta(dbId, tableId,
                Lists.newArrayList("c1"), StatsConstants.AnalyzeType.FULL, LocalDateTime.now(), new HashMap<>()));

        UUID uuid = UUID.randomUUID();
        TUniqueId requestId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        TransactionState transactionState = new TransactionState(dbId, Lists.newArrayList(tableId), 33333L, "xxx",
                requestId, TransactionState.LoadJobSourceType.INSERT_STREAMING, null, 44444L, 10000);
        transactionState.setTxnCommitAttachment(new InsertTxnCommitAttachment(0));
        GlobalStateMgr.getCurrentState().getAnalyzeManager().updateLoadRows(transactionState);
    }
}
