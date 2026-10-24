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


package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SimplifiedPredicateRuleTest extends PlanTestBase {
    private static final ConstantOperator OI_NULL = ConstantOperator.createNull(IntegerType.INT);
    private static final ConstantOperator OI_100 = ConstantOperator.createInt(100);
    private static final ConstantOperator OI_200 = ConstantOperator.createInt(200);
    private static final ConstantOperator OI_300 = ConstantOperator.createInt(300);

    private static final ConstantOperator OB_FALSE = ConstantOperator.createBoolean(false);
    private static final ConstantOperator OB_TRUE = ConstantOperator.createBoolean(true);

    // The {add, sub} functions of each unit whose nested calls with constant amounts are merged
    private static final String[][] TIME_FN_UNITS = {
            {FunctionSet.YEARS_ADD, FunctionSet.YEARS_SUB},
            {FunctionSet.QUARTERS_ADD, FunctionSet.QUARTERS_SUB},
            {FunctionSet.MONTHS_ADD, FunctionSet.MONTHS_SUB},
            {FunctionSet.WEEKS_ADD, FunctionSet.WEEKS_SUB},
            {FunctionSet.DAYS_ADD, FunctionSet.DAYS_SUB},
            {FunctionSet.HOURS_ADD, FunctionSet.HOURS_SUB},
            {FunctionSet.MINUTES_ADD, FunctionSet.MINUTES_SUB},
            {FunctionSet.SECONDS_ADD, FunctionSet.SECONDS_SUB},
            {FunctionSet.MILLISECONDS_ADD, FunctionSet.MILLISECONDS_SUB},
            {FunctionSet.MICROSECONDS_ADD, FunctionSet.MICROSECONDS_SUB},
            {FunctionSet.DATE_ADD, FunctionSet.DATE_SUB},
    };

    private SimplifiedPredicateRule rule = new SimplifiedPredicateRule();

    @BeforeAll
    public static void beforeAll() throws Exception {
        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `test_timestamp` (\n" +
                "  `id` bigint NULL COMMENT \"\",\n" +
                "  `ts` bigint NULL COMMENT \"unix timestamp\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @Test
    public void applyCaseWhen() {
        CaseWhenOperator cwo1 = new CaseWhenOperator(IntegerType.INT,
                new ColumnRefOperator(1, IntegerType.INT, "id", true), null,
                Lists.newArrayList(ConstantOperator.createInt(1), ConstantOperator.createVarchar("test"),
                        ConstantOperator.createInt(2), ConstantOperator.createVarchar("test2")));
        assertEquals(cwo1, rule.apply(cwo1, null));

        CaseWhenOperator cwo2 = new CaseWhenOperator(IntegerType.INT, ConstantOperator.createNull(BooleanType.BOOLEAN), null,
                Lists.newArrayList(ConstantOperator.createInt(1), ConstantOperator.createVarchar("test")));
        assertEquals(OI_NULL, rule.apply(cwo2, null));

        CaseWhenOperator cwo3 = new CaseWhenOperator(IntegerType.INT,
                ConstantOperator.createNull(BooleanType.BOOLEAN), OI_100,
                Lists.newArrayList(ConstantOperator.createInt(1), ConstantOperator.createVarchar("test")));
        assertEquals(OI_100, rule.apply(cwo3, null));

        CaseWhenOperator cwo4 = new CaseWhenOperator(IntegerType.INT, null, null,
                Lists.newArrayList(new ColumnRefOperator(1, BooleanType.BOOLEAN, "id", true), OI_200,
                        new ColumnRefOperator(2, BooleanType.BOOLEAN, "id", true), OI_100));
        assertEquals(cwo4, rule.apply(cwo4, null));

        CaseWhenOperator cwo5 = new CaseWhenOperator(IntegerType.INT, null, null,
                Lists.newArrayList(OB_FALSE, OI_200, OB_TRUE, OI_300));
        assertEquals(OI_300, rule.apply(cwo5, null));

        CaseWhenOperator cwo6 = new CaseWhenOperator(IntegerType.INT, null, null,
                Lists.newArrayList(OB_FALSE, OI_200, OI_NULL, OI_300));
        assertEquals(OI_NULL, rule.apply(cwo6, null));

        CaseWhenOperator cwo7 = new CaseWhenOperator(IntegerType.INT, null, OI_100,
                Lists.newArrayList(OB_FALSE, OI_200, OI_NULL, OI_300));
        assertEquals(OI_100, rule.apply(cwo7, null));
    }

    @Test
    public void applyLike() {
        SimplifiedPredicateRule rule = new SimplifiedPredicateRule();

        ScalarOperator operator = new LikePredicateOperator(new ColumnRefOperator(1, VarcharType.VARCHAR, "name", true),
                ConstantOperator.createVarchar("zxcv"));
        ScalarOperator result = rule.apply(operator, null);

        assertEquals(OperatorType.BINARY, result.getOpType());
        assertEquals(BinaryType.EQ, ((BinaryPredicateOperator) result).getBinaryType());
        assertEquals(ConstantOperator.createVarchar("zxcv"), result.getChild(1));

        operator = new LikePredicateOperator(new ColumnRefOperator(1, VarcharType.VARCHAR, "name", true),
                ConstantOperator.createVarchar("%zxcv"));
        result = rule.apply(operator, null);
        assertEquals(OperatorType.LIKE, result.getOpType());

        operator = new LikePredicateOperator(new ColumnRefOperator(1, VarcharType.VARCHAR, "name", true),
                ConstantOperator.createVarchar("_zxcv"));
        result = rule.apply(operator, null);
        assertEquals(OperatorType.LIKE, result.getOpType());

        // test for none-string right child
        operator = new LikePredicateOperator(new ColumnRefOperator(1, VarcharType.VARCHAR, "name", true),
                ConstantOperator.createBoolean(false));
        result = rule.apply(operator, null);
        assertEquals(OperatorType.LIKE, result.getOpType());

        // pattern with backslash escape should NOT be simplified to EQ,
        // because LIKE treats '\\' as an escaped literal '\', while EQ treats it as two backslashes
        operator = new LikePredicateOperator(new ColumnRefOperator(1, VarcharType.VARCHAR, "name", true),
                ConstantOperator.createVarchar("star\\\\"));
        result = rule.apply(operator, null);
        assertEquals(OperatorType.LIKE, result.getOpType());

        operator = new LikePredicateOperator(new ColumnRefOperator(1, VarcharType.VARCHAR, "name", true),
                ConstantOperator.createVarchar("abc\\\\def"));
        result = rule.apply(operator, null);
        assertEquals(OperatorType.LIKE, result.getOpType());
    }

    @Test
    public void applyHourFromUnixTime() throws Exception {
        starRocksAssert.query("SELECT hour(from_unixtime(ts)) FROM test_timestamp")
                .explainContains("hour_from_unixtime");

        starRocksAssert.query("SELECT hour(ts) FROM test_timestamp")
                .explainWithout("hour_from_unixtime");

        starRocksAssert.query("SELECT hour(from_unixtime(ts, '%Y-%m-%d %H:%i:%s')) FROM test_timestamp")
                .explainWithout("hour_from_unixtime");
    }

    @Test
    public void applyHourToDatetimeRewrite() throws Exception {
        starRocksAssert.query("SELECT hour(to_datetime(ts)) FROM test_timestamp")
                .explainContains("hour_from_unixtime");

        starRocksAssert.query("SELECT hour(to_datetime(ts, 0)) FROM test_timestamp")
                .explainContains("hour_from_unixtime");

        starRocksAssert.query("SELECT hour(to_datetime(ts, 3)) FROM test_timestamp")
                .explainContains("hour_from_unixtime", "/ 1000");

        starRocksAssert.query("SELECT hour(to_datetime(ts, 6)) FROM test_timestamp")
                .explainContains("hour_from_unixtime", "/ 1000000");

        starRocksAssert.query("SELECT hour(to_datetime(ts, 4)) FROM test_timestamp")
                .explainWithout("hour_from_unixtime");
    }

    private static CallOperator timeFn(String name, ScalarOperator datetime, int amount) {
        Function fn = ExprUtils.getBuiltinFunction(name, new Type[] {DateType.DATETIME, IntegerType.INT},
                Function.CompareMode.IS_IDENTICAL);
        return new CallOperator(name, DateType.DATETIME,
                Lists.newArrayList(datetime, ConstantOperator.createInt(amount)), fn);
    }

    @Test
    public void mergeTimeFnsOfTheSameUnit() {
        ColumnRefOperator ts = new ColumnRefOperator(1, DateType.DATETIME, "ts", true);
        for (String[] unit : TIME_FN_UNITS) {
            String add = unit[0];
            String sub = unit[1];
            assertEquals(add + "(1: ts, 7)", rule.apply(timeFn(add, timeFn(add, ts, 3), 4), null).toString());
            assertEquals(sub + "(1: ts, 2)", rule.apply(timeFn(sub, timeFn(add, ts, 3), 5), null).toString());
            assertEquals(add + "(1: ts, 2)", rule.apply(timeFn(add, timeFn(sub, ts, 3), 5), null).toString());
            assertEquals(sub + "(1: ts, 7)", rule.apply(timeFn(sub, timeFn(sub, ts, 3), 4), null).toString());
            // the amounts cancel out
            assertEquals("1: ts", rule.apply(timeFn(sub, timeFn(add, ts, 3), 3), null).toString());
        }
    }

    @Test
    public void doNotMergeTimeFnsOfDifferentUnits() {
        ColumnRefOperator ts = new ColumnRefOperator(1, DateType.DATETIME, "ts", true);
        for (String[] outer : TIME_FN_UNITS) {
            for (String[] inner : TIME_FN_UNITS) {
                if (outer == inner) {
                    continue;
                }
                for (String outerFn : outer) {
                    for (String innerFn : inner) {
                        CallOperator call = timeFn(outerFn, timeFn(innerFn, ts, 1000), 1);
                        assertEquals(call.toString(), rule.apply(call, null).toString(), outerFn + " over " + innerFn);
                    }
                }
            }
        }
    }

    @Test
    public void mergeNestedIntervalsOfTheSameSubSecondUnit() throws Exception {
        String plan = getFragmentPlan("select milliseconds_add(milliseconds_add(id_datetime, 5), 9) from test_all_type");
        assertContains(plan, "milliseconds_add(8: id_datetime, 14)");

        plan = getFragmentPlan("select id_datetime + interval 500 microsecond - interval 999 microsecond from test_all_type");
        assertContains(plan, "microseconds_sub(8: id_datetime, 499)");
    }

    @Test
    public void doNotMergeNestedIntervalsOfSecondsAndSubSecondUnits() throws Exception {
        // these used to be merged into seconds_add(id_datetime, 501), seconds_add(id_datetime, 1001) and so on
        String plan = getFragmentPlan(
                "select id_datetime + interval 1 second + interval 500 millisecond from test_all_type");
        assertContains(plan, "milliseconds_add(seconds_add(8: id_datetime, 1), 500)");

        plan = getFragmentPlan("select id_datetime + interval 500 millisecond + interval 1 second from test_all_type");
        assertContains(plan, "seconds_add(milliseconds_add(8: id_datetime, 500), 1)");

        plan = getFragmentPlan("select id_datetime + interval 500 microsecond + interval 1 second from test_all_type");
        assertContains(plan, "seconds_add(microseconds_add(8: id_datetime, 500), 1)");

        plan = getFragmentPlan("select id_datetime + interval 500 microsecond - interval 1 millisecond from test_all_type");
        assertContains(plan, "milliseconds_sub(microseconds_add(8: id_datetime, 500), 1)");
    }
}
