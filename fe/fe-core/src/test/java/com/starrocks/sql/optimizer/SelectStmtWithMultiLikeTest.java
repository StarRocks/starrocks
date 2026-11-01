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

package com.starrocks.sql.optimizer;

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import com.starrocks.sql.optimizer.rewrite.scalar.ConsolidateLikesRule;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Stream;

public class SelectStmtWithMultiLikeTest {

    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp()
            throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        String createTblStmtStr = " CREATE TABLE `t0` (\n" +
                "  `region` varchar(128) NOT NULL COMMENT \"\",\n" +
                "  `order_date` date NOT NULL COMMENT \"\",\n" +
                "  `site` varchar(128) NOT NULL COMMENT \"\",\n" +
                "  `income` decimal(7, 0) NOT NULL COMMENT \"\",\n" +
                "  `ship_mode` int NOT NULL COMMENT \"\",\n" +
                "  `ship_code` int" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`region`, `order_date`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`region`, `order_date`) BUCKETS 10 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\" " +
                ");";

        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("test").useDatabase("test");
        starRocksAssert.withTable(createTblStmtStr);
        FeConstants.enablePruneEmptyOutputScan = false;
        FeConstants.setLengthForVarchar = false;
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("multiLikeTestCases")
    void testMultiLikeClause(String sql, List<String> patterns) throws Exception {
        test(sql, patterns);
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("joinOnMultiLikeTestCases")
    void testJoinOnMultiLikeClause(String sql, List<String> patterns) throws Exception {
        test(sql, patterns);
    }

    @Test
    public void testMultiNotLikes() throws Exception {
        String sql = "select count(1) from t0 where " +
                "site not like '%ABC%' and " +
                "region not like '%ABC%' and region not like '%DEF%'";
        List<String> patterns = Lists.newArrayList(
                "PREDICATES: NOT (1: region REGEXP '^((.*ABC.*)|(.*DEF.*))$'), NOT (3: site LIKE '%ABC%')");
        test(sql, patterns);
    }

    @Test
    public void testLikePredicateConsolidateByteLimit() {
        ColumnRefOperator region = new ColumnRefOperator(1, VarcharType.VARCHAR, "region", false);
        String cjk = "甲".repeat(2663);

        ScalarOperator atLimit = applyConsolidateLikes(CompoundPredicateOperator.or(
                like(region, "%" + cjk + "aa%"), like(region, "%" + cjk + "aaa%")));
        LikePredicateOperator regexp = getOnlyRegexp(atLimit);
        String anchoredRegexp = Utils.mustCast(regexp.getChild(1), ConstantOperator.class).getVarchar();
        Assertions.assertEquals(16_000, anchoredRegexp.getBytes(StandardCharsets.UTF_8).length);
        Assertions.assertTrue(anchoredRegexp.length() < 16_000);

        ScalarOperator overLimit = applyConsolidateLikes(CompoundPredicateOperator.or(
                like(region, "%" + cjk + "aa%"), like(region, "%" + cjk + "aaaa%")));
        assertRegexpCount(overLimit, 0);
        assertRawLikeCount(overLimit, 2);

        List<ScalarOperator> doiLikeOps = new ArrayList<>();
        for (int i = 0; i < 259; i++) {
            doiLikeOps.add(like(region, "%" + "x".repeat(80) + "-" + i + "%"));
        }
        ScalarOperator doiResult = applyConsolidateLikes(CompoundPredicateOperator.or(doiLikeOps));
        assertRegexpCount(doiResult, 0);
        assertRawLikeCount(doiResult, 259);
    }

    @Test
    public void testOversizedLikeGroupDoesNotPartiallyConsolidate() {
        ColumnRefOperator region = new ColumnRefOperator(1, VarcharType.VARCHAR, "region", false);
        String first = "%" + "a".repeat(5326) + "%";
        String second = "%" + "b".repeat(5326) + "%";
        String third = "%" + "c".repeat(5325) + "%";

        ScalarOperator nestedOr = CompoundPredicateOperator.or(
                CompoundPredicateOperator.or(like(region, first), like(region, second)), like(region, third));
        ScalarOperator rewrittenOr = applyConsolidateLikes(nestedOr);
        assertRegexpCount(rewrittenOr, 0);
        assertRawLikeCount(rewrittenOr, 3);

        ScalarOperator nestedAnd = CompoundPredicateOperator.and(
                CompoundPredicateOperator.and(CompoundPredicateOperator.not(like(region, first)),
                        CompoundPredicateOperator.not(like(region, second))),
                CompoundPredicateOperator.not(like(region, third)));
        ScalarOperator rewrittenAnd = applyConsolidateLikes(nestedAnd);
        assertRegexpCount(rewrittenAnd, 0);
        assertRawLikeCount(rewrittenAnd, 3);
        Assertions.assertEquals(3, Utils.extractConjuncts(rewrittenAnd).stream()
                .filter(ScalarOperatorUtil::isSimpleNotLike).count());
    }

    @Test
    public void testLikePatternsWithinConsolidationByteLimit() throws Exception {
        test("select count(1) from t0 where region like '%suffix' or region like '%other'",
                Lists.newArrayList("1: region REGEXP '^((.*suffix)|(.*other))$'"));
        test("select count(1) from t0 where region like '%needle%' or region like '%value%'",
                Lists.newArrayList("1: region REGEXP '^((.*needle.*)|(.*value.*))$'"));
    }

    private static Stream<Arguments> multiLikeTestCases() {
        String sqlFormat = "select count(1) from t0 where %s";
        String[][] testCases = new String[][] {
                {"order_date > '2024-01-1' and site = 'ABC' and income = 10.0 and ship_mode = 3 and " +
                        "ship_code = 3 and region not like '%ABC%' and region not like '%DEF%'",
                        "2: order_date > '2024-01-01', 3: site = 'ABC', CAST(4: income AS DECIMAL64(8,1)) = 10.0, 5: ship_mode "
                                + "= 3, 6: ship_code = 3, NOT (1: region REGEXP '^((.*ABC.*)|(.*DEF.*))$')"
                },
                {"region like '%ABC' or region like 'DEF_G'", "1: region REGEXP '^((.*ABC)|(DEF.G))$'"},
                {"region like '%ABC'", "1: region LIKE '%ABC'"},
                {"region like '%ABC' or order_date between '2012-01-01' and '2023-01-01'",
                        "region LIKE '%ABC'"
                },
                {"region like '%ABC' and order_date between '2012-01-01' and '2023-01-01'",
                        "region LIKE '%ABC'"
                },
                {"region like '%ABC' or region like 'DEF_G' or order_date between '2012-01-01' and '2023-01-01'",
                        "region REGEXP '^((.*ABC)|(DEF.G))$'"
                },
                {"region like '%ABC' or region like 'DEF_G' and order_date between '2012-01-01' and '2023-01-01'",
                        "region LIKE '%ABC'"
                },
                {"(region like '%ABC' or region like 'DEF_G') and order_date between '2012-01-01' and '2023-01-01'",
                        "region REGEXP '^((.*ABC)|(DEF.G))$'"
                },
                {"region not like '%ABC' and region not like 'DEF_G'",
                        "NOT (1: region REGEXP '^((.*ABC)|(DEF.G))$')"
                },
                {"region like '^$%ABC\n' or region like '%BC[]\r' or region like '\t%AC{}' ",
                        "1: region REGEXP '^((\\\\^\\\\$.*ABC\\\\n)|(.*BC\\\\[\\\\]\\\\r)|(\\\\t.*AC\\\\{\\\\}))$'"
                },
                {"region not like '^$%ABC\n' and region not like '%BC[]\r' and region not like '\t%AC{}' ",
                        "NOT (1: region REGEXP '^((\\\\^\\\\$.*ABC\\\\n)|(.*BC\\\\[\\\\]\\\\r)|(\\\\t.*AC\\\\{\\\\}))$')"
                },
                {"region like '甲乙.子丑%' or " +
                        "region like '丙丁.寅卯%' or " +
                        "region like '戊己.辰巳%' or " +
                        "region like '庚辛.午未%'",
                        "region LIKE '甲乙.子丑%'"
                },
                {"region not like '甲乙.%子丑' and " +
                        "region not like '丙丁.%寅卯' and " +
                        "region not like '戊己.%辰巳' and " +
                        "region not like '庚辛.午未%'",
                        "region REGEXP '^((甲乙\\\\..*子丑)|(丙丁\\\\..*寅卯)|(戊己\\\\..*辰巳))$'",
                        "region LIKE '庚辛.午未%'"
                },
                {"region like '甲乙.%子丑' or " +
                        "site like '东*青?%木' or " +
                        "region like '丙丁.%寅卯' or " +
                        "site like '南*赤?%火' or " +
                        "region like '戊己.%辰巳' or " +
                        "site like '西*白?%金' or " +
                        "region like '庚辛.午未%'",
                        "region REGEXP '^((甲乙\\\\..*子丑)|(丙丁\\\\..*寅卯)|(戊己\\\\..*辰巳))$'",
                        "region LIKE '庚辛.午未%')) OR (3: site REGEXP '^((东\\\\*青\\\\?.*木)|" +
                                "(南\\\\*赤\\\\?.*火)|(西\\\\*白\\\\?.*金))$'",
                },
                {"region not like '甲乙.%子丑' and " +
                        "site not like '东*青?木%' and " +
                        "region not like '丙丁.%寅卯' and " +
                        "site not like '南*%赤?火' and " +
                        "region not like '戊己.%辰巳' and " +
                        "site not like '西*%白?金' and " +
                        "region not like '庚辛.%午未'",
                        "region REGEXP '^((甲乙\\\\..*子丑)|(丙丁\\\\..*寅卯)|(戊己\\\\..*辰巳)|(庚辛\\\\..*午未))$'"
                },
                {"order_date = '2023-01-01' or region like '甲乙.%子丑' or " +
                        "site like '东*%青?木' or " +
                        "region like '丙丁%.寅卯' or " +
                        "site like '南*赤%?火' or " +
                        "region like '戊己.%辰巳' or " +
                        "site like '西*白%?金' or " +
                        "region like '庚辛.%午未'",
                        "region REGEXP '^((甲乙\\\\..*子丑)|(丙丁.*\\\\.寅卯)|(戊己\\\\..*辰巳)|(庚辛\\\\..*午未))$'"
                },
                {"region not like '甲乙.子丑%' and " +
                        "site not like '东*青?木%' and " +
                        "region not like '丙丁.寅卯%' and " +
                        "site not like '南*%赤?火' and " +
                        "region not like '戊己.辰巳%' and " +
                        "site not like '西*%白?金' and " +
                        "region not like '庚辛.午未%'",
                        "site REGEXP '^((南\\\\*.*赤\\\\?火)|(西\\\\*.*白\\\\?金))$'"
                },
                {"(case when ship_mode in (1,2,3,4)\n" +
                        " and (region like '甲乙.%子丑'\n" +
                        "      or region like '丙丁.%寅卯' \n" +
                        "      or region like '戊己.%辰巳' \n" +
                        "      or region like '庚辛.%午未'\n" +
                        "      or region like '壬癸.%申酉' )\n" +
                        " then  '01'\n" +
                        " when ship_mode in (5,6,7,8)\n" +
                        " and (region like '中央.%黄土'\n" +
                        "      or region like '东方.%青木%' \n" +
                        "      or region like '南方.%赤火%' \n" +
                        "      or region like '北方.%黑水%'\n" +
                        "      or region like '西方.%白金%' )\n" +
                        " then  '02'\n" +
                        " when ship_mode in (9,10,11,12)\n" +
                        " and (region like '天.地.玄.%黄%'\n" +
                        "      or region like '宇.宙.%洪.荒%' \n" +
                        "      or region like '日.月.%盈.昃%' \n" +
                        "      or region like '辰.宿.%列.张%'\n" +
                        "      or region like '寒.来.%暑.往%' )\n" +
                        " then  '03'\n" +
                        " when ship_mode in (13,14,15,16)\n" +
                        " and (region like 'Α.Β.Γ.%Δ%'\n" +
                        "      or region like 'α.β.%γ.δ%' \n" +
                        "      or region like 'A.B.%C.D%' \n" +
                        "      or region like 'a.b.%c.d%'\n" +
                        "      or region like '一.二.%三.四%' )\n" +
                        " then  '04'\n" +
                        " else  '00'\n" +
                        " end) = '02'\n",
                        "region REGEXP '^((中央\\\\..*黄土)|(东方\\\\..*青木.*)|(南方\\\\..*赤火.*)|(北方\\\\.....'",
                        "region REGEXP '^((甲乙\\\\..*子丑)|(丙丁\\\\..*寅卯)|(戊己\\\\..*辰巳)|(庚辛\\\\..*午未)...'",
                },
                {"region like '\\\\b%' or region like '\\\\d%' or region like '\\\\S+'",
                        "region LIKE '\\\\b%'",
                        "region LIKE '\\\\d%'",
                        "region LIKE '\\\\S+'"
                }
        };

        List<Arguments> argumentsList = Lists.newArrayList();
        for (String[] tc : testCases) {
            String sql = String.format(sqlFormat, tc[0]);
            argumentsList.add(Arguments.of(sql, Arrays.asList(tc).subList(1, tc.length)));
        }
        return argumentsList.stream();
    }

    private static Stream<Arguments> joinOnMultiLikeTestCases() {
        String sqlFormat = "select count(1) from t0 ta join t0 tb on " +
                "ta.order_date = days_add(tb.order_date, 1) and %s";
        String[][] testCases = new String[][] {
                {"(case when ta.ship_mode in (1,2,3,4)\n" +
                        " and (tb.region like '甲乙.子丑%'\n" +
                        "      or tb.region like '丙丁.寅卯%' \n" +
                        "      or tb.region like '戊己.辰巳%' \n" +
                        "      or tb.region like '庚辛.午未%'\n" +
                        "      or tb.region like '壬癸.申酉%' )\n" +
                        " then  '01'\n" +
                        " when ta.ship_mode in (5,6,7,8)\n" +
                        " and (tb.region like '中央.黄土%'\n" +
                        "      or tb.region like '东方.青木%' \n" +
                        "      or tb.region like '南方.赤火%' \n" +
                        "      or tb.region like '北方.黑水%'\n" +
                        "      or tb.region like '西方.白金%' )\n" +
                        " then  '02'\n" +
                        " when ta.ship_mode in (9,10,11,12)\n" +
                        " and (tb.region like '天.地.玄.黄%'\n" +
                        "      or tb.region like '宇.宙.洪.荒%' \n" +
                        "      or tb.region like '日.月.盈.昃%' \n" +
                        "      or tb.region like '辰.宿.列.张%'\n" +
                        "      or tb.region like '寒.来.暑.往%' )\n" +
                        " then  '03'\n" +
                        " when ta.ship_mode in (13,14,15,16)\n" +
                        " and (tb.region like 'Α.Β.Γ.Δ%'\n" +
                        "      or tb.region like 'α.β.γ.δ%' \n" +
                        "      or tb.region like 'A.B.C.D%' \n" +
                        "      or tb.region like 'a.b.c.d%'\n" +
                        "      or tb.region like '一.二.三.四%' )\n" +
                        " then  '04'\n" +
                        " else  '00'\n" +
                        " end) = '02'\n",
                        "5: ship_mode IN (5, 6, 7, 8), " +
                                "((((7: region LIKE '中央.黄土%') OR " +
                                "(7: region LIKE '东方.青木%')) OR " +
                                "(7: region LIKE '南方.赤火%')) OR " +
                                "(7: region LIKE '北方.黑水%')) OR " +
                                "(7: region LIKE '西方.白金%'), " +
                                "NOT ((5: ship_mode IN (1, 2, 3, 4)) AND " +
                                "(((((7: region LIKE '甲乙.子丑%') OR " +
                                "(7: region LIKE '丙丁.寅卯%')) OR " +
                                "(7: region LIKE '戊己.辰巳%')) OR " +
                                "(7: region LIKE '庚辛.午未%')) OR " +
                                "(7: region LIKE '壬癸.申酉%')))\n"
                }
        };

        List<Arguments> argumentsList = Lists.newArrayList();
        for (String[] tc : testCases) {
            String sql = String.format(sqlFormat, tc[0]);
            argumentsList.add(Arguments.of(sql, Arrays.asList(tc).subList(1, tc.length)));
        }
        return argumentsList.stream();
    }

    private void test(String sql, List<String> patterns) throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setOptimizerExecuteTimeout(3000000);
        starRocksAssert.getCtx().getSessionVariable().setLikePredicateConsolidateMin(2);
        String plan = UtFrameUtils.getFragmentPlan(starRocksAssert.getCtx(), sql);
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add(sql);
        joiner.add(patterns.toString());
        joiner.add(plan);
        Assertions.assertTrue(patterns.stream().allMatch(plan::contains), joiner.toString());
    }

    private LikePredicateOperator like(ColumnRefOperator column, String pattern) {
        return new LikePredicateOperator(column, ConstantOperator.createVarchar(pattern));
    }

    private ScalarOperator applyConsolidateLikes(ScalarOperator root) {
        ConnectContext previousContext = ConnectContext.get();
        int previousMin = starRocksAssert.getCtx().getSessionVariable().getLikePredicateConsolidateMin();
        ConnectContext.set(starRocksAssert.getCtx());
        try {
            starRocksAssert.getCtx().getSessionVariable().setLikePredicateConsolidateMin(2);
            return ConsolidateLikesRule.INSTANCE.apply(root, new ScalarOperatorRewriteContext());
        } finally {
            starRocksAssert.getCtx().getSessionVariable().setLikePredicateConsolidateMin(previousMin);
            if (previousContext == null) {
                ConnectContext.remove();
            } else {
                ConnectContext.set(previousContext);
            }
        }
    }

    private LikePredicateOperator getOnlyRegexp(ScalarOperator operator) {
        assertRegexpCount(operator, 1);
        return ScalarOperatorUtil.getStream(operator)
                .filter(child -> child instanceof LikePredicateOperator &&
                        ((LikePredicateOperator) child).isRegexp())
                .map(child -> (LikePredicateOperator) child)
                .findFirst()
                .orElseThrow();
    }

    private void assertRegexpCount(ScalarOperator operator, long expected) {
        long actual = ScalarOperatorUtil.getStream(operator)
                .filter(child -> child instanceof LikePredicateOperator &&
                        ((LikePredicateOperator) child).isRegexp())
                .count();
        Assertions.assertEquals(expected, actual, operator.toString());
    }

    private void assertRawLikeCount(ScalarOperator operator, long expected) {
        long actual = ScalarOperatorUtil.getStream(operator).filter(ScalarOperatorUtil::isSimpleLike).count();
        Assertions.assertEquals(expected, actual, operator.toString());
    }
}
