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

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TPCDS1TRuntimeFilterTest extends TPCDS1TTestBase {
    private final static Pattern RUNTIME_FILTER_PATTERN = Pattern.compile("-.+((probe_expr)|(build_expr)) =.+\n");

    @BeforeAll
    public static void beforeClass() throws Exception {
        TPCDSPlanTestBase.beforeClass();
    }

    @Test
    public void testQ87() throws Exception {
        String sql = "select count(*) \n" +
                "from (" +
                "      select distinct c_last_name, c_first_name, d_date\n" +
                "       from catalog_sales, date_dim, customer\n" +
                "       where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk\n" +
                "         and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk\n" +
                "         and d_month_seq between 1200 and 1200+11\n" +
                ") cool_cust;";
        String plan = getVerboseExplain(sql);
        assertCContains(plan, "filter_id = 0, probe_expr = (3: cs_sold_date_sk)");
        assertNotContains(plan, "filter_id = 1");
    }

    @Test
    public void testWaitMs() throws Exception {
        String sql = "select * " +
                "from store_returns inner join[shuffle] date_dim\n" +
                " on sr_returned_date_sk = d_date_sk and d_year = 2000;";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "probe_expr = (3: sr_returned_date_sk), wait = 1000ms");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("effectiveCases")
    public void testEffectiveRuntimeFilter(String name, int build, int probe, int remote) throws Exception {
        String plan = getCostExplain(getSql(name));
        int remoteActual = StringUtils.countMatches(plan, "remote = true");
        int probeActual = StringUtils.countMatches(plan, "probe_expr =");
        int buildActual = StringUtils.countMatches(plan, "build_expr =");

        if (probeActual != probe || buildActual != build || remoteActual != remote) {
            connectContext.getSessionVariable().setGlobalRuntimeFilterProbeMinSize(0);
            String diff = getCostExplain(getSql(name));
            connectContext.getSessionVariable().setGlobalRuntimeFilterProbeMinSize(100L * 1024L);
            System.out.println("expect: \"" + name + "\", " + build + ", " + probe + ", " + remote);
            System.out.println("actual: \"" + name + "\", " + buildActual + ", " + probeActual + ", " + remoteActual);
            Assertions.assertEquals(diff, plan);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("forceEffectiveCases")
    public void testForceEffectiveRuntimeFilter(String name, int build, int probe, int remote) throws Exception {
        connectContext.getSessionVariable().setGlobalRuntimeFilterProbeMinSize(0);
        String plan = getVerboseExplain(getSql(name));
        connectContext.getSessionVariable().setGlobalRuntimeFilterProbeMinSize(100L * 1024L);
        int remoteActual = StringUtils.countMatches(plan, "remote = true");
        int probeActual = StringUtils.countMatches(plan, "probe_expr =");
        int buildActual = StringUtils.countMatches(plan, "build_expr =");

        if (probeActual != probe || buildActual != build || remoteActual != remote) {
            System.out.println("expect: \"" + name + "\", " + build + ", " + probe + ", " + remote);
            System.out.println("actual: \"" + name + "\", " + buildActual + ", " + probeActual + ", " + remoteActual);
            Assertions.fail();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("waitCases")
    public void testRuntimeFilterWaitMs(String name) throws Exception {
        String plan = getVerboseExplain(getSql(name));
        Assertions.assertTrue(plan.contains("wait = "), () -> extractRuntimeFilter(plan));
    }

    public void debugAll() throws Exception {
        for (String name : getSqlMap().keySet()) {
            String plan = getVerboseExplain(getSql(name));
            int remote = StringUtils.countMatches(plan, "remote = true");
            int probe = StringUtils.countMatches(plan, "probe_expr =");
            int build = StringUtils.countMatches(plan, "build_expr =");
            System.out.println("\"" + name + "\"" + ", " + build + ", " + probe + ", " + remote);
        }
    }

    public void debugSQL(String name, String sql) throws Exception {
        String plan = getVerboseExplain(sql);
        int remote = StringUtils.countMatches(plan, "remote = true");
        int probe = StringUtils.countMatches(plan, "probe_expr =");
        int build = StringUtils.countMatches(plan, "build_expr =");
        System.out.println("\"" + name + "\"" + ", " + build + ", " + probe + ", " + remote);
        System.out.println(extractRuntimeFilter(plan));
    }

    private String extractRuntimeFilter(String plan) {
        return RUNTIME_FILTER_PATTERN.matcher(plan).results().map(matchResult -> {
            String re = matchResult.group();
            return StringUtils.strip(re, " -|\r\n").trim();
        }).sorted().collect(Collectors.joining("\n"));
    }

    private static Stream<Arguments> waitCases() {
        Arguments[] list = new Arguments[] {
                Arguments.of("query05"),
                Arguments.of("query10"),
                Arguments.of("query13"),
                Arguments.of("query16"),
                Arguments.of("query23-1"),
                Arguments.of("query23-2"),
                Arguments.of("query25"),
                Arguments.of("query32"),
                Arguments.of("query35"),
                Arguments.of("query54"),
                Arguments.of("query64"),
                Arguments.of("query64-2"),
                Arguments.of("query65"),
                Arguments.of("query69"),
                Arguments.of("query80"),
                Arguments.of("query85"),
                Arguments.of("query91"),
                Arguments.of("query92"),
                Arguments.of("query94"),
                Arguments.of("query95"),
        };

        return Arrays.stream(list);
    }

    private static Stream<Arguments> effectiveCases() {
        Arguments[] list = new Arguments[] {
                Arguments.of("query01", 3, 5, 1),
                Arguments.of("query02", 4, 7, 1),
                Arguments.of("query03", 2, 2, 0),
                Arguments.of("query04", 6, 6, 0),
                Arguments.of("query05", 8, 26, 6),
                Arguments.of("query06", 4, 4, 0),
                Arguments.of("query07", 2, 2, 0),
                Arguments.of("query08", 3, 3, 0),
                Arguments.of("query09", 0, 0, 0),
                Arguments.of("query10", 7, 9, 2),
                Arguments.of("query11", 4, 4, 0),
                Arguments.of("query12", 2, 2, 0),
                Arguments.of("query13", 5, 6, 1),
                Arguments.of("query14-1", 12, 12, 0),
                Arguments.of("query15", 1, 1, 0),
                Arguments.of("query16", 5, 5, 2),
                Arguments.of("query17", 7, 7, 0),
                Arguments.of("query18", 4, 4, 0),
                Arguments.of("query19", 5, 5, 0),
                Arguments.of("query20", 2, 2, 0),
                Arguments.of("query21", 3, 3, 0),
                Arguments.of("query22", 1, 1, 0),
                Arguments.of("query23-1", 7, 8, 3),
                Arguments.of("query23-2", 7, 8, 3),
                Arguments.of("query24-1", 6, 6, 0),
                Arguments.of("query24-2", 6, 6, 0),
                Arguments.of("query25", 9, 9, 1),
                Arguments.of("query26", 2, 2, 0),
                Arguments.of("query27", 2, 2, 0),
                Arguments.of("query28", 0, 0, 0),
                Arguments.of("query29", 9, 9, 0),
                Arguments.of("query30", 3, 3, 0),
                Arguments.of("query31", 2, 2, 0),
                Arguments.of("query32", 4, 5, 1),
                Arguments.of("query33", 6, 6, 0),
                Arguments.of("query34", 4, 4, 0),
                Arguments.of("query35", 7, 10, 3),
                Arguments.of("query36", 2, 2, 0),
                Arguments.of("query37", 3, 3, 0),
                Arguments.of("query38", 3, 3, 0),
                Arguments.of("query39-1", 2, 2, 0),
                Arguments.of("query39-2", 2, 2, 0),
                Arguments.of("query40", 5, 5, 0),
                Arguments.of("query41", 1, 1, 0),
                Arguments.of("query42", 2, 2, 0),
                Arguments.of("query43", 2, 2, 0),
                Arguments.of("query44", 3, 4, 1),
                Arguments.of("query45", 1, 1, 0),
                Arguments.of("query46", 3, 3, 0),
                Arguments.of("query47", 12, 17, 0),
                Arguments.of("query48", 3, 3, 0),
                Arguments.of("query49", 9, 9, 0),
                Arguments.of("query50", 6, 6, 0),
                Arguments.of("query51", 2, 2, 0),
                Arguments.of("query52", 2, 2, 0),
                Arguments.of("query53", 3, 3, 0),
                Arguments.of("query54", 4, 7, 1),
                Arguments.of("query55", 2, 2, 0),
                Arguments.of("query56", 12, 12, 0),
                Arguments.of("query57", 10, 14, 0),
                Arguments.of("query58", 11, 18, 5),
                Arguments.of("query59", 7, 8, 1),
                Arguments.of("query60", 6, 6, 0),
                Arguments.of("query61", 9, 9, 0),
                Arguments.of("query62", 4, 4, 0),
                Arguments.of("query63", 3, 3, 0),
                Arguments.of("query64", 13, 14, 1),
                Arguments.of("query64-2", 13, 14, 1),
                Arguments.of("query65", 4, 8, 2),
                Arguments.of("query66", 8, 8, 0),
                Arguments.of("query67", 2, 2, 0),
                Arguments.of("query68", 6, 6, 0),
                Arguments.of("query69", 8, 10, 3),
                Arguments.of("query70", 5, 5, 0),
                Arguments.of("query71", 5, 15, 2),
                Arguments.of("query72", 5, 5, 0),
                Arguments.of("query73", 4, 4, 0),
                Arguments.of("query74", 4, 4, 0),
                Arguments.of("query75", 3, 3, 0),
                Arguments.of("query76", 3, 3, 0),
                Arguments.of("query77", 10, 10, 0),
                Arguments.of("query78", 3, 3, 0),
                Arguments.of("query79", 3, 3, 0),
                Arguments.of("query80", 18, 21, 3),
                Arguments.of("query81", 2, 2, 0),
                Arguments.of("query82", 3, 3, 0),
                Arguments.of("query83", 11, 18, 5),
                Arguments.of("query84", 2, 2, 0),
                Arguments.of("query85", 10, 12, 2),
                Arguments.of("query86", 1, 1, 0),
                Arguments.of("query87", 3, 3, 0),
                Arguments.of("query88", 24, 24, 0),
                Arguments.of("query89", 3, 3, 0),
                Arguments.of("query90", 6, 6, 0),
                Arguments.of("query91", 6, 7, 1),
                Arguments.of("query92", 4, 5, 1),
                Arguments.of("query93", 3, 3, 0),
                Arguments.of("query94", 3, 4, 1),
                Arguments.of("query95", 5, 13, 2),
                Arguments.of("query96", 3, 3, 0),
                Arguments.of("query97", 2, 2, 0),
                Arguments.of("query98", 2, 2, 0),
                Arguments.of("query99", 4, 4, 0),
        };

        return Arrays.stream(list);
    }

    private static Stream<Arguments> forceEffectiveCases() {
        Arguments[] list = new Arguments[] {
                Arguments.of("query01", 4, 6, 2),
                Arguments.of("query02", 4, 7, 1),
                Arguments.of("query03", 2, 2, 0),
                Arguments.of("query04", 12, 12, 0),
                Arguments.of("query05", 8, 26, 6),
                Arguments.of("query06", 6, 6, 1),
                Arguments.of("query07", 4, 4, 0),
                Arguments.of("query08", 4, 4, 0),
                Arguments.of("query09", 0, 0, 0),
                Arguments.of("query10", 7, 9, 2),
                Arguments.of("query11", 8, 8, 0),
                Arguments.of("query12", 2, 2, 0),
                Arguments.of("query13", 5, 6, 1),
                Arguments.of("query14-1", 21, 27, 3),
                Arguments.of("query15", 3, 3, 0),
                Arguments.of("query16", 5, 5, 2),
                Arguments.of("query17", 8, 10, 1),
                Arguments.of("query18", 6, 6, 0),
                Arguments.of("query19", 5, 5, 0),
                Arguments.of("query20", 2, 2, 0),
                Arguments.of("query21", 3, 3, 0),
                Arguments.of("query22", 2, 2, 0),
                Arguments.of("query23-1", 11, 13, 4),
                Arguments.of("query23-2", 13, 21, 6),
                Arguments.of("query24-1", 7, 9, 1),
                Arguments.of("query24-2", 7, 9, 1),
                Arguments.of("query25", 10, 12, 2),
                Arguments.of("query26", 4, 4, 0),
                Arguments.of("query27", 4, 4, 0),
                Arguments.of("query28", 0, 0, 0),
                Arguments.of("query29", 10, 12, 1),
                Arguments.of("query30", 5, 5, 0),
                Arguments.of("query31", 9, 27, 5),
                Arguments.of("query32", 4, 5, 1),
                Arguments.of("query33", 12, 12, 0),
                Arguments.of("query34", 4, 4, 0),
                Arguments.of("query35", 7, 10, 3),
                Arguments.of("query36", 3, 3, 0),
                Arguments.of("query37", 3, 3, 0),
                Arguments.of("query38", 6, 6, 0),
                Arguments.of("query39-1", 5, 5, 0),
                Arguments.of("query39-2", 5, 5, 0),
                Arguments.of("query39-1-2", 5, 5, 0),
                Arguments.of("query39-2-2", 5, 5, 0),
                Arguments.of("query40", 5, 5, 0),
                Arguments.of("query41", 1, 1, 0),
                Arguments.of("query42", 2, 2, 0),
                Arguments.of("query43", 2, 2, 0),
                Arguments.of("query44", 3, 4, 1),
                Arguments.of("query45", 4, 4, 1),
                Arguments.of("query46", 6, 6, 0),
                Arguments.of("query47", 13, 18, 0),
                Arguments.of("query48", 4, 4, 0),
                Arguments.of("query49", 9, 9, 0),
                Arguments.of("query50", 6, 6, 0),
                Arguments.of("query51", 2, 2, 0),
                Arguments.of("query52", 2, 2, 0),
                Arguments.of("query53", 3, 3, 0),
                Arguments.of("query54", 8, 16, 4),
                Arguments.of("query55", 2, 2, 0),
                Arguments.of("query56", 12, 12, 0),
                Arguments.of("query57", 11, 15, 0),
                Arguments.of("query58", 14, 21, 5),
                Arguments.of("query59", 7, 8, 1),
                Arguments.of("query60", 12, 12, 0),
                Arguments.of("query61", 11, 11, 0),
                Arguments.of("query62", 4, 4, 0),
                Arguments.of("query63", 3, 3, 0),
                Arguments.of("query64", 23, 26, 2),
                Arguments.of("query64-2", 23, 26, 2),
                Arguments.of("query65", 5, 10, 3),
                Arguments.of("query66", 8, 8, 0),
                Arguments.of("query67", 3, 3, 0),
                Arguments.of("query68", 6, 6, 0),
                Arguments.of("query69", 8, 10, 3),
                Arguments.of("query70", 5, 5, 0),
                Arguments.of("query71", 5, 15, 2),
                Arguments.of("query72", 7, 7, 0),
                Arguments.of("query73", 4, 4, 0),
                Arguments.of("query74", 8, 8, 0),
                Arguments.of("query75", 6, 6, 0),
                Arguments.of("query76", 6, 6, 0),
                Arguments.of("query77", 10, 10, 0),
                Arguments.of("query78", 3, 3, 0),
                Arguments.of("query79", 4, 5, 1),
                Arguments.of("query80", 18, 21, 3),
                Arguments.of("query81", 5, 5, 1),
                Arguments.of("query82", 3, 3, 0),
                Arguments.of("query83", 14, 21, 5),
                Arguments.of("query84", 5, 5, 0),
                Arguments.of("query85", 10, 12, 2),
                Arguments.of("query86", 2, 2, 0),
                Arguments.of("query87", 6, 6, 0),
                Arguments.of("query88", 24, 24, 0),
                Arguments.of("query89", 3, 3, 0),
                Arguments.of("query90", 6, 6, 0),
                Arguments.of("query91", 6, 7, 1),
                Arguments.of("query92", 4, 5, 1),
                Arguments.of("query93", 3, 3, 0),
                Arguments.of("query94", 4, 6, 2),
                Arguments.of("query95", 5, 13, 2),
                Arguments.of("query96", 3, 3, 0),
                Arguments.of("query97", 2, 2, 0),
                Arguments.of("query98", 2, 2, 0),
                Arguments.of("query99", 4, 4, 0),
        };

        return Arrays.stream(list);
    }

}
