// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.google.common.collect.ImmutableMap;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

public class TPCDSPushAggTest extends TPCDSPlanTestBase {
    private static final Map<String, Long> ROW_COUNT_MAP = ImmutableMap.<String, Long>builder()
            .put("call_center", 42L)
            .put("catalog_page", 30000L)
            .put("catalog_returns", 143996756L)
            .put("catalog_sales", 1439980416L)
            .put("customer", 12000000L)
            .put("customer_address", 6000000L)
            .put("customer_demographics", 1920800L)
            .put("date_dim", 73049L)
            .put("date_dim_varchar", 73049L)
            .put("household_demographics", 7200L)
            .put("income_band", 20L)
            .put("inventory", 783000000L)
            .put("item", 300000L)
            .put("promotion", 1500L)
            .put("reason", 65L)
            .put("ship_mode", 20L)
            .put("store", 1002L)
            .put("store_returns", 287999764L)
            .put("store_sales", 2879987999L)
            .put("time_dim", 86400L)
            .put("warehouse", 20L)
            .put("web_page", 3000L)
            .put("web_returns", 71997522L)
            .put("web_sales", 720000376L)
            .put("web_site", 54L)
            .build();

    private StatisticStorage origin;

    @BeforeAll
    public static void beforeClass() throws Exception {
        TPCDSPlanTestBase.beforeClass();
        connectContext.getSessionVariable().setCboCteReuse(true);
        connectContext.getSessionVariable().setEnablePipelineEngine(true);
    }

    @AfterAll
    public static void afterClass() {
        TPCDSPlanTestBase.afterClass();
    }

    @BeforeEach
    public void setUp() throws Exception {
        origin = GlobalStateMgr.getCurrentStatisticStorage();
        connectContext.getGlobalStateMgr().setStatisticStorage(new MockTPCDSStatisticStorage());
        setTPCDSTableStats(ROW_COUNT_MAP);
        FeConstants.runningUnitTest = true;
    }

    @AfterEach
    public void tearDown() throws Exception {
        FeConstants.runningUnitTest = false;
        connectContext.getGlobalStateMgr().setStatisticStorage(origin);
    }

    private void check(int mode, String sql, int aggNum) throws Exception {
        connectContext.getSessionVariable().setCboPushDownAggregateMode(mode);
        sql = getTPCDS(sql);
        String plan = getFragmentPlan(sql);
        int actual = StringUtils.countMatches(plan, ":AGGREGATE ");
        String msg = "\nmode: " + mode + ", except: " + aggNum + ", actual: " + actual + "\n" + plan;
        Assertions.assertEquals(aggNum, actual, msg);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testCastProvider")
    public void testTPCDSPushDownAgg(String sql, int orig, int auto, int force, int mid, int high) throws Exception {
        check(-1, sql, orig);
        check(0, sql, auto);
        check(1, sql, force);
        check(2, sql, mid);
        check(3, sql, high);
    }

    //    @ParameterizedTest(name = "{0}")
    //    @MethodSource("testCastProvider")
    public void debugTPCDSPushDownAgg(String sql, int orig, int auto, int force, int mid, int high) throws Exception {
        orig = getAggNum(-1, sql);
        auto = getAggNum(0, sql);
        force = getAggNum(1, sql);
        mid = getAggNum(2, sql);
        high = getAggNum(3, sql);
        System.out.printf("Arguments.of(\"%s\", %d, %d, %d, %d, %d),\n", sql, orig, auto, force, mid, high);
    }

    private int getAggNum(int cboPushDownAggregateMode, String sql) throws Exception {
        connectContext.getSessionVariable().setCboPushDownAggregateMode(cboPushDownAggregateMode);
        sql = getTPCDS(sql);
        String plan = getFragmentPlan(sql);
        return StringUtils.countMatches(plan, ":AGGREGATE ");
    }

    private static Stream<Arguments> testCastProvider() {
        // orig(-1), auto(0), force(1), mid(2), high(3)
        Arguments[] cases = new Arguments[] {
                Arguments.of("Q01", 4, 4, 6, 6, 6),
                Arguments.of("Q02", 2, 5, 5, 5, 5),
                Arguments.of("Q03", 2, 4, 4, 4, 4),
                Arguments.of("Q04", 6, 12, 12, 12, 12),
                Arguments.of("Q05", 8, 16, 16, 16, 16),
                Arguments.of("Q06", 5, 5, 5, 5, 5),
                Arguments.of("Q07", 2, 2, 2, 2, 2),
                Arguments.of("Q08", 4, 6, 6, 6, 6),
                Arguments.of("Q09", 30, 30, 30, 30, 30),
                Arguments.of("Q10", 2, 2, 2, 2, 2),
                Arguments.of("Q11", 4, 8, 8, 8, 8),
                Arguments.of("Q12", 2, 4, 4, 4, 4),
                Arguments.of("Q13", 2, 2, 2, 2, 2),
                Arguments.of("Q14_1", 10, 10, 10, 10, 10),
                Arguments.of("Q14_2", 6, 6, 6, 6, 6),
                Arguments.of("Q15", 2, 2, 4, 4, 4),
                Arguments.of("Q16", 4, 4, 4, 4, 4),
                Arguments.of("Q17", 2, 2, 2, 2, 2),
                Arguments.of("Q18", 2, 2, 2, 2, 2),
                Arguments.of("Q19", 2, 2, 4, 4, 4),
                Arguments.of("Q20", 2, 4, 4, 4, 4),
                Arguments.of("Q21", 2, 2, 2, 2, 2),
                Arguments.of("Q22", 2, 2, 2, 2, 2),
                Arguments.of("Q23_1", 9, 12, 12, 12, 12),
                Arguments.of("Q23_2", 11, 14, 18, 18, 18),
                Arguments.of("Q24_1", 6, 6, 7, 6, 6),
                Arguments.of("Q24_2", 6, 6, 7, 6, 6),
                Arguments.of("Q25", 2, 2, 2, 2, 2),
                Arguments.of("Q26", 2, 2, 2, 2, 2),
                Arguments.of("Q27", 2, 2, 2, 2, 2),
                Arguments.of("Q28", 24, 24, 24, 24, 24),
                Arguments.of("Q29", 2, 2, 2, 2, 2),
                Arguments.of("Q30", 4, 6, 6, 6, 6),
                Arguments.of("Q31", 4, 8, 8, 8, 8),
                Arguments.of("Q32", 4, 4, 4, 4, 4),
                Arguments.of("Q33", 8, 8, 14, 14, 14),
                Arguments.of("Q34", 2, 2, 2, 2, 2),
                Arguments.of("Q35", 2, 2, 2, 2, 2),
                Arguments.of("Q36", 2, 2, 2, 2, 2),
                Arguments.of("Q37", 2, 6, 8, 6, 7),
                Arguments.of("Q38", 8, 14, 20, 14, 17),
                Arguments.of("Q39_1", 2, 2, 2, 2, 2),
                Arguments.of("Q39_2", 2, 2, 2, 2, 2),
                Arguments.of("Q40", 2, 2, 2, 2, 2),
                Arguments.of("Q41", 3, 3, 5, 3, 3),
                Arguments.of("Q42", 2, 4, 4, 4, 4),
                Arguments.of("Q43", 2, 4, 4, 4, 4),
                Arguments.of("Q44", 8, 8, 8, 8, 8),
                Arguments.of("Q45", 5, 5, 7, 7, 7),
                Arguments.of("Q46", 1, 1, 3, 1, 1),
                Arguments.of("Q47", 2, 2, 4, 4, 4),
                Arguments.of("Q48", 2, 2, 2, 2, 2),
                Arguments.of("Q49", 8, 8, 8, 8, 8),
                Arguments.of("Q50", 2, 2, 2, 2, 2),
                Arguments.of("Q51", 4, 8, 8, 8, 8),
                Arguments.of("Q52", 2, 4, 4, 4, 4),
                Arguments.of("Q53", 2, 2, 4, 4, 4),
                Arguments.of("Q54", 7, 9, 16, 13, 15),
                Arguments.of("Q55", 2, 4, 4, 4, 4),
                Arguments.of("Q56", 8, 8, 14, 14, 14),
                Arguments.of("Q57", 2, 2, 4, 4, 4),
                Arguments.of("Q58", 3, 9, 9, 9, 9),
                Arguments.of("Q59", 2, 4, 4, 4, 4),
                Arguments.of("Q60", 8, 8, 14, 14, 14),
                Arguments.of("Q61", 4, 4, 4, 4, 4),
                Arguments.of("Q62", 2, 2, 2, 2, 2),
                Arguments.of("Q63", 2, 2, 4, 4, 4),
                Arguments.of("Q64", 4, 4, 4, 4, 4),
                Arguments.of("Q65", 6, 6, 10, 10, 10),
                Arguments.of("Q66", 6, 6, 6, 6, 6),
                Arguments.of("Q67", 2, 2, 2, 2, 2),
                Arguments.of("Q68", 1, 1, 3, 1, 1),
                Arguments.of("Q69", 2, 2, 2, 2, 2),
                Arguments.of("Q70", 4, 6, 6, 6, 6),
                Arguments.of("Q71", 2, 2, 8, 8, 8),
                Arguments.of("Q72", 2, 2, 2, 2, 2),
                Arguments.of("Q73", 2, 2, 2, 2, 2),
                Arguments.of("Q74", 4, 8, 8, 8, 8),
                Arguments.of("Q75", 4, 4, 16, 4, 4),
                Arguments.of("Q76", 2, 2, 2, 2, 2),
                Arguments.of("Q77", 14, 26, 26, 26, 26),
                Arguments.of("Q78", 6, 6, 9, 6, 6),
                Arguments.of("Q79", 2, 2, 4, 2, 2),
                Arguments.of("Q80", 8, 8, 8, 8, 8),
                Arguments.of("Q81", 4, 6, 6, 6, 6),
                Arguments.of("Q82", 2, 6, 8, 6, 7),
                Arguments.of("Q83", 3, 9, 9, 9, 9),
                Arguments.of("Q84", 0, 0, 0, 0, 0),
                Arguments.of("Q85", 2, 2, 2, 2, 2),
                Arguments.of("Q86", 2, 2, 2, 2, 2),
                Arguments.of("Q87", 8, 14, 20, 14, 17),
                Arguments.of("Q88", 16, 16, 16, 16, 16),
                Arguments.of("Q89", 2, 2, 4, 4, 4),
                Arguments.of("Q90", 4, 4, 4, 4, 4),
                Arguments.of("Q91", 2, 4, 4, 4, 4),
                Arguments.of("Q92", 4, 4, 4, 4, 4),
                Arguments.of("Q93", 2, 2, 2, 2, 2),
                Arguments.of("Q94", 4, 4, 4, 4, 4),
                Arguments.of("Q95", 4, 4, 4, 4, 4),
                Arguments.of("Q96", 2, 2, 2, 2, 2),
                Arguments.of("Q97", 6, 6, 12, 10, 12),
                Arguments.of("Q98", 2, 4, 4, 4, 4),
                Arguments.of("Q99", 2, 2, 2, 2, 2),
        };

        return Arrays.stream(cases);
    }

}
