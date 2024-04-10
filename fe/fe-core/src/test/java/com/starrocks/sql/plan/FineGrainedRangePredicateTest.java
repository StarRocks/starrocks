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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.FineGrainedRangePredicateRule;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Stream;

public class FineGrainedRangePredicateTest extends DistributedEnvPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setEnableFineGrainedRangePredicate(true);
    }

    @Test
    void testCutDateRange1() {
        ColumnRefOperator datetimeCol = new ColumnRefOperator(1, Type.DATETIME, "datetime_col", true);

        List<Triple<String, LocalDateTime, LocalDateTime>> tripleList;
        List<ScalarOperator> rangePredicates;
        LocalDateTime begin = LocalDateTime.of(2021, 1, 14, 1, 1);
        LocalDateTime end = LocalDateTime.of(2021, 1, 17, 0, 0);
        ConstantOperator datetimeBegin = ConstantOperator.createDatetime(begin);
        ConstantOperator datetimeEnd = ConstantOperator.createDatetime(end);

        tripleList = FineGrainedRangePredicateRule.cutDateRange(begin, end);
        Assert.assertEquals(1, tripleList.size());
        Assert.assertEquals("DAY", tripleList.get(0).getLeft());
        Assert.assertEquals(begin, tripleList.get(0).getMiddle());
        Assert.assertEquals(end, tripleList.get(0).getRight());

        List<BinaryPredicateOperator> predicates = Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.GT, datetimeCol, datetimeBegin),
                new BinaryPredicateOperator(BinaryType.LT, datetimeCol, datetimeEnd));
        rangePredicates = FineGrainedRangePredicateRule.buildRangePredicates(datetimeCol, predicates);
        Assert.assertEquals(1, rangePredicates.size());
        Assert.assertEquals("1: datetime_col > 2021-01-14 01:01:00 AND 1: datetime_col < 2021-01-17 00:00:00",
                rangePredicates.get(0).toString());
    }

    @Test
    void testCutDateRange2() {
        ColumnRefOperator datetimeCol = new ColumnRefOperator(1, Type.DATETIME, "datetime_col", true);
        LocalDateTime begin = LocalDateTime.of(2020, 12, 14, 1, 1);
        LocalDateTime end = LocalDateTime.of(2021, 2, 17, 2, 1);
        ConstantOperator datetimeBegin = ConstantOperator.createDatetime(begin);
        ConstantOperator datetimeEnd = ConstantOperator.createDatetime(end);

        List<Triple<String, LocalDateTime, LocalDateTime>> tripleList;
        List<ScalarOperator> rangePredicates;

        tripleList = FineGrainedRangePredicateRule.cutDateRange(begin, end);

        Assert.assertEquals(3, tripleList.size());
        Assert.assertEquals("DAY", tripleList.get(0).getLeft());
        Assert.assertEquals(begin, tripleList.get(0).getMiddle());
        Assert.assertEquals(LocalDateTime.of(2021, 1, 1, 0, 0), tripleList.get(0).getRight());

        Assert.assertEquals("MONTH", tripleList.get(1).getLeft());
        Assert.assertEquals(LocalDateTime.of(2021, 1, 1, 0, 0), tripleList.get(1).getMiddle());
        Assert.assertEquals(LocalDateTime.of(2021, 2, 1, 0, 0), tripleList.get(1).getRight());


        Assert.assertEquals("DAY", tripleList.get(2).getLeft());
        Assert.assertEquals(LocalDateTime.of(2021, 2, 1, 0, 0), tripleList.get(2).getMiddle());
        Assert.assertEquals(LocalDateTime.of(2021, 2, 17, 2, 1), tripleList.get(2).getRight());
        List<BinaryPredicateOperator> predicates = Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.GE, datetimeCol, datetimeBegin),
                new BinaryPredicateOperator(BinaryType.LE, datetimeCol, datetimeEnd));
        rangePredicates = FineGrainedRangePredicateRule.buildRangePredicates(datetimeCol, predicates);

        Assert.assertEquals(3, rangePredicates.size());
        Assert.assertEquals("1: datetime_col >= 2020-12-14 01:01:00 AND 1: datetime_col < 2021-01-01 00:00:00",
                rangePredicates.get(0).toString());
        Assert.assertEquals("date_trunc(month, 1: datetime_col) >= 2021-01-01 00:00:00 AND " +
                        "date_trunc(month, 1: datetime_col) < 2021-02-01 00:00:00",
                rangePredicates.get(1).toString());
        Assert.assertEquals("1: datetime_col >= 2021-02-01 00:00:00 AND 1: datetime_col <= 2021-02-17 02:01:00",
                rangePredicates.get(2).toString());
    }

    @Test
    void testCutRange3() {
        ColumnRefOperator dateCol = new ColumnRefOperator(1, Type.DATE, "date_col", true);
        LocalDateTime begin = LocalDateTime.of(2020, 12, 14, 0, 0);
        LocalDateTime end = LocalDateTime.of(2022, 1, 17, 0, 0);
        ConstantOperator dateBegin = ConstantOperator.createDate(begin);
        ConstantOperator dateEnd = ConstantOperator.createDate(end);

        List<Triple<String, LocalDateTime, LocalDateTime>> tripleList;
        List<ScalarOperator> rangePredicates;
        tripleList = FineGrainedRangePredicateRule.cutDateRange(begin, end);

        Assert.assertEquals(3, tripleList.size());
        Assert.assertEquals("DAY", tripleList.get(0).getLeft());
        Assert.assertEquals(begin, tripleList.get(0).getMiddle());
        Assert.assertEquals(LocalDateTime.of(2021, 1, 1, 0, 0), tripleList.get(0).getRight());

        Assert.assertEquals("YEAR", tripleList.get(1).getLeft());
        Assert.assertEquals(LocalDateTime.of(2021, 1, 1, 0, 0), tripleList.get(1).getMiddle());
        Assert.assertEquals(LocalDateTime.of(2022, 1, 1, 0, 0), tripleList.get(1).getRight());


        Assert.assertEquals("DAY", tripleList.get(2).getLeft());
        Assert.assertEquals(LocalDateTime.of(2022, 1, 1, 0, 0), tripleList.get(2).getMiddle());
        Assert.assertEquals(LocalDateTime.of(2022, 1, 17, 0, 0), tripleList.get(2).getRight());
        List<BinaryPredicateOperator> predicates = Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.GE, dateCol, dateBegin),
                new BinaryPredicateOperator(BinaryType.LT, dateCol, dateEnd));

        rangePredicates = FineGrainedRangePredicateRule.buildRangePredicates(dateCol, predicates);
        Assert.assertEquals(3, rangePredicates.size());
        Assert.assertEquals("1: date_col >= 2020-12-14 AND 1: date_col < 2021-01-01",
                rangePredicates.get(0).toString());
        Assert.assertEquals("date_trunc(year, 1: date_col) >= 2021-01-01 AND date_trunc(year, 1: date_col) < 2022-01-01",
                rangePredicates.get(1).toString());
        Assert.assertEquals("1: date_col >= 2022-01-01 AND 1: date_col < 2022-01-17",
                rangePredicates.get(2).toString());
    }

    @Test
    void testCutDateRange4() {
        ColumnRefOperator datetimeCol = new ColumnRefOperator(1, Type.DATETIME, "datetime_col", true);
        LocalDateTime begin = LocalDateTime.of(2020, 8, 14, 14, 1);
        LocalDateTime end = LocalDateTime.of(2022, 2, 17, 2, 26);
        ConstantOperator datetimeBegin = ConstantOperator.createDatetime(begin);
        ConstantOperator datetimeEnd = ConstantOperator.createDatetime(end);

        List<Triple<String, LocalDateTime, LocalDateTime>> tripleList;
        List<ScalarOperator> rangePredicates;

        tripleList = FineGrainedRangePredicateRule.cutDateRange(begin, end);

        Assert.assertEquals(5, tripleList.size());
        Assert.assertEquals("DAY", tripleList.get(0).getLeft());
        Assert.assertEquals(begin, tripleList.get(0).getMiddle());
        Assert.assertEquals(LocalDateTime.of(2020, 9, 1, 0, 0), tripleList.get(0).getRight());

        Assert.assertEquals("MONTH", tripleList.get(1).getLeft());
        Assert.assertEquals(LocalDateTime.of(2020, 9, 1, 0, 0), tripleList.get(1).getMiddle());
        Assert.assertEquals(LocalDateTime.of(2021, 1, 1, 0, 0), tripleList.get(1).getRight());

        Assert.assertEquals("YEAR", tripleList.get(2).getLeft());
        Assert.assertEquals(LocalDateTime.of(2021, 1, 1, 0, 0), tripleList.get(2).getMiddle());
        Assert.assertEquals(LocalDateTime.of(2022, 1, 1, 0, 0), tripleList.get(2).getRight());

        Assert.assertEquals("MONTH", tripleList.get(3).getLeft());
        Assert.assertEquals(LocalDateTime.of(2022, 1, 1, 0, 0), tripleList.get(3).getMiddle());
        Assert.assertEquals(LocalDateTime.of(2022, 2, 1, 0, 0), tripleList.get(3).getRight());

        Assert.assertEquals("DAY", tripleList.get(4).getLeft());
        Assert.assertEquals(LocalDateTime.of(2022, 2, 1, 0, 0), tripleList.get(4).getMiddle());
        Assert.assertEquals(LocalDateTime.of(2022, 2, 17, 2, 26), tripleList.get(4).getRight());

        List<BinaryPredicateOperator> predicates = Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.GT, datetimeCol, datetimeBegin),
                new BinaryPredicateOperator(BinaryType.LE, datetimeCol, datetimeEnd));
        rangePredicates = FineGrainedRangePredicateRule.buildRangePredicates(datetimeCol, predicates);
        System.out.println(rangePredicates);

        Assert.assertEquals(5, rangePredicates.size());
        Assert.assertEquals("1: datetime_col > 2020-08-14 14:01:00 AND 1: datetime_col < 2020-09-01 00:00:00",
                rangePredicates.get(0).toString());
        Assert.assertEquals("date_trunc(month, 1: datetime_col) >= 2020-09-01 00:00:00 AND " +
                        "date_trunc(month, 1: datetime_col) < 2021-01-01 00:00:00",
                rangePredicates.get(1).toString());
        Assert.assertEquals("date_trunc(year, 1: datetime_col) >= 2021-01-01 00:00:00 AND " +
                        "date_trunc(year, 1: datetime_col) < 2022-01-01 00:00:00",
                rangePredicates.get(2).toString());
        Assert.assertEquals("date_trunc(month, 1: datetime_col) >= 2022-01-01 00:00:00 AND " +
                        "date_trunc(month, 1: datetime_col) < 2022-02-01 00:00:00",
                rangePredicates.get(3).toString());
        Assert.assertEquals("1: datetime_col >= 2022-02-01 00:00:00 AND 1: datetime_col <= 2022-02-17 02:26:00",
                rangePredicates.get(4).toString());
    }

    @ParameterizedTest
    @MethodSource("testSplitUnionSqls")
    void testSplitUnion(String sql, List<String> expectedPlan) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, expectedPlan);
    }

    @ParameterizedTest
    @MethodSource("testNotSplitUnionSqls")
    void testNotSplitUnion(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "UNION");
    }

    private static Stream<Arguments> testSplitUnionSqls() {
        List<Arguments> list = Lists.newArrayList();
        String sql = "select count(t1b + t1c) from test_all_type where id_date > '2021-01-02' and id_date < '2021-06-17'";
        list.add(Arguments.of(sql, ImmutableList.of("0:UNION",
                "45: id_date >= '2021-06-01', 45: id_date < '2021-06-17'",
                "date_trunc('month', 33: id_date) >= '2021-02-01', date_trunc('month', 33: id_date) < '2021-06-01'",
                "21: id_date > '2021-01-02', 21: id_date < '2021-02-01'")));
        sql = "select count(t1b + v1), max(v1) from test_all_type join t0 where id_datetime > '2021-01-02 12:11:02' " +
                "and id_datetime< '2030-06-17 21:59:13'";
        list.add(Arguments.of(sql, ImmutableList.of("0:UNION",
                "88: id_datetime >= '2030-06-01 00:00:00', 88: id_datetime < '2030-06-17 21:59:13'",
                "date_trunc('month', 72: id_datetime) >= '2030-01-01 00:00:00', " +
                        "date_trunc('month', 72: id_datetime) < '2030-06-01 00:00:00'",
                "date_trunc('year', 56: id_datetime) >= '2022-01-01 00:00:00', " +
                        "date_trunc('year', 56: id_datetime) < '2030-01-01 00:00:00'",
                "date_trunc('month', 40: id_datetime) >= '2021-02-01 00:00:00', " +
                        "date_trunc('month', 40: id_datetime) < '2022-01-01 00:00:00'",
                "24: id_datetime > '2021-01-02 12:11:02', 24: id_datetime < '2021-02-01 00:00:00'")));
        sql = "select count(t1b + v1), sum(t1c + v3), min(v1) from test_all_type join t0 " +
                "where id_datetime > '2021-12-02 12:11:02' " +
                "and id_datetime< '2023-06-17 21:59:13' and v3 + t1a = 10 group by t1c, v2";
        list.add(Arguments.of(sql, ImmutableList.of("0:UNION",
                "other join predicates: CAST(85: v3 AS DOUBLE) + CAST(73: t1a AS DOUBLE) = 10.0",
                "80: id_datetime >= '2023-06-01 00:00:00', 80: id_datetime < '2023-06-17 21:59:13'",
                "date_trunc('month', 62: id_datetime) >= '2023-01-01 00:00:00', " +
                        "date_trunc('month', 62: id_datetime) < '2023-06-01 00:00:00'",
                "date_trunc('year', 44: id_datetime) >= '2022-01-01 00:00:00', " +
                        "date_trunc('year', 44: id_datetime) < '2023-01-01 00:00:00'",
                "26: id_datetime > '2021-12-02 12:11:02', 26: id_datetime < '2022-01-01 00:00:00'")));

        return list.stream();
    }

    private static Stream<Arguments> testNotSplitUnionSqls() {
        List<Arguments> list = Lists.newArrayList();
        list.add(Arguments.of("select count( distinct t1b + t1c) from test_all_type where " +
                "id_date > '2021-01-02' and id_date < '2021-06-17'"));
        list.add(Arguments.of("select avg(t1c) from test_all_type where id_date > '2021-01-02' and id_date < '2021-06-17'"));
        list.add(Arguments.of("select count(t1c) from test_all_type where id_date > '2021-01-02' and id_date < '2021-02-17'"));
        return list.stream();
    }


    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableFineGrainedRangePredicate(false);
    }
}
