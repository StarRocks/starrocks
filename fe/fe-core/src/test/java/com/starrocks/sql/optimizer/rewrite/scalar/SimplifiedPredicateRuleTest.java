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
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SimplifiedPredicateRuleTest {
    private static final ConstantOperator OI_NULL = ConstantOperator.createNull(Type.INT);
    private static final ConstantOperator OI_100 = ConstantOperator.createInt(100);
    private static final ConstantOperator OI_200 = ConstantOperator.createInt(200);
    private static final ConstantOperator OI_300 = ConstantOperator.createInt(300);

    private static final ConstantOperator OB_FALSE = ConstantOperator.createBoolean(false);
    private static final ConstantOperator OB_TRUE = ConstantOperator.createBoolean(true);

    private SimplifiedPredicateRule rule = new SimplifiedPredicateRule();

    @Test
    public void applyCaseWhen() {
        CaseWhenOperator cwo1 = new CaseWhenOperator(Type.INT, new ColumnRefOperator(1, Type.INT, "id", true), null,
                Lists.newArrayList(ConstantOperator.createInt(1), ConstantOperator.createVarchar("test"),
                        ConstantOperator.createInt(2), ConstantOperator.createVarchar("test2")));
        assertEquals(cwo1, rule.apply(cwo1, null));

        CaseWhenOperator cwo2 = new CaseWhenOperator(Type.INT, ConstantOperator.createNull(Type.BOOLEAN), null,
                Lists.newArrayList(ConstantOperator.createInt(1), ConstantOperator.createVarchar("test")));
        assertEquals(OI_NULL, rule.apply(cwo2, null));

        CaseWhenOperator cwo3 = new CaseWhenOperator(Type.INT, ConstantOperator.createNull(Type.BOOLEAN), OI_100,
                Lists.newArrayList(ConstantOperator.createInt(1), ConstantOperator.createVarchar("test")));
        assertEquals(OI_100, rule.apply(cwo3, null));

        CaseWhenOperator cwo4 = new CaseWhenOperator(Type.INT, null, null,
                Lists.newArrayList(new ColumnRefOperator(1, Type.BOOLEAN, "id", true), OI_200,
                        new ColumnRefOperator(2, Type.BOOLEAN, "id", true), OI_100));
        assertEquals(cwo4, rule.apply(cwo4, null));

        CaseWhenOperator cwo5 = new CaseWhenOperator(Type.INT, null, null,
                Lists.newArrayList(OB_FALSE, OI_200, OB_TRUE, OI_300));
        assertEquals(OI_300, rule.apply(cwo5, null));

        CaseWhenOperator cwo6 = new CaseWhenOperator(Type.INT, null, null,
                Lists.newArrayList(OB_FALSE, OI_200, OI_NULL, OI_300));
        assertEquals(OI_NULL, rule.apply(cwo6, null));

        CaseWhenOperator cwo7 = new CaseWhenOperator(Type.INT, null, OI_100,
                Lists.newArrayList(OB_FALSE, OI_200, OI_NULL, OI_300));
        assertEquals(OI_100, rule.apply(cwo7, null));
    }

    @Test
    public void applyLike() {
        SimplifiedPredicateRule rule = new SimplifiedPredicateRule();

        ScalarOperator operator = new LikePredicateOperator(new ColumnRefOperator(1, Type.VARCHAR, "name", true),
                ConstantOperator.createVarchar("zxcv"));
        ScalarOperator result = rule.apply(operator, null);

        assertEquals(OperatorType.BINARY, result.getOpType());
        assertEquals(BinaryType.EQ, ((BinaryPredicateOperator) result).getBinaryType());
        assertEquals(ConstantOperator.createVarchar("zxcv"), result.getChild(1));

        operator = new LikePredicateOperator(new ColumnRefOperator(1, Type.VARCHAR, "name", true),
                ConstantOperator.createVarchar("%zxcv"));
        result = rule.apply(operator, null);
        assertEquals(OperatorType.LIKE, result.getOpType());

        operator = new LikePredicateOperator(new ColumnRefOperator(1, Type.VARCHAR, "name", true),
                ConstantOperator.createVarchar("_zxcv"));
        result = rule.apply(operator, null);
        assertEquals(OperatorType.LIKE, result.getOpType());

        // test for none-string right child
        operator = new LikePredicateOperator(new ColumnRefOperator(1, Type.VARCHAR, "name", true),
                ConstantOperator.createBoolean(false));
        result = rule.apply(operator, null);
        assertEquals(OperatorType.LIKE, result.getOpType());
    }

    @Test
    public void applyHourFromUnixTime() {
        // Test hour(from_unixtime(ts)) -> hour_from_unixtime(ts)
        ColumnRefOperator tsColumn = new ColumnRefOperator(1, Type.BIGINT, "ts", true);

        // Create from_unixtime(ts) call
        CallOperator fromUnixTimeCall = new CallOperator(FunctionSet.FROM_UNIXTIME, Type.VARCHAR,
                Lists.newArrayList(tsColumn), null);

        // Create hour(from_unixtime(ts)) call
        CallOperator hourCall = new CallOperator(FunctionSet.HOUR, Type.TINYINT,
                Lists.newArrayList(fromUnixTimeCall), null);

        ScalarOperator result = rule.apply(hourCall, null);

        // Verify the result is hour_from_unixtime(ts)
        assertEquals(OperatorType.CALL, result.getOpType());
        CallOperator resultCall = (CallOperator) result;
        assertEquals(FunctionSet.HOUR_FROM_UNIXTIME, resultCall.getFnName());
        assertEquals(1, resultCall.getChildren().size());
        assertEquals(tsColumn, resultCall.getChild(0));

        // Test that hour(ts) is not optimized (not from_unixtime)
        CallOperator simpleHourCall = new CallOperator(FunctionSet.HOUR, Type.TINYINT,
                Lists.newArrayList(tsColumn), null);
        ScalarOperator simpleResult = rule.apply(simpleHourCall, null);
        assertEquals(simpleHourCall, simpleResult);

        // Test that hour(from_unixtime(ts, format)) is not optimized (multiple arguments)
        CallOperator fromUnixTimeCall2 = new CallOperator(FunctionSet.FROM_UNIXTIME, Type.VARCHAR,
                Lists.newArrayList(tsColumn, ConstantOperator.createVarchar("format")), null);
        CallOperator hourCall2 = new CallOperator(FunctionSet.HOUR, Type.TINYINT,
                Lists.newArrayList(fromUnixTimeCall2), null);
        ScalarOperator result2 = rule.apply(hourCall2, null);
        assertEquals(hourCall2, result2);
    }

    @Test
    public void applyHourToDatetimeRewrite() {
        // hour(to_datetime(ts)) -> hour_from_unixtime(ts)
        ColumnRefOperator tsColumn = new ColumnRefOperator(2, Type.BIGINT, "ts2", true);

        CallOperator toDatetimeCall = new CallOperator(FunctionSet.TO_DATETIME, Type.DATETIME,
                Lists.newArrayList(tsColumn), null);
        CallOperator hourCall = new CallOperator(FunctionSet.HOUR, Type.TINYINT,
                Lists.newArrayList(toDatetimeCall), null);

        ScalarOperator result = rule.apply(hourCall, null);
        assertEquals(OperatorType.CALL, result.getOpType());
        CallOperator resultCall = (CallOperator) result;
        assertEquals(FunctionSet.HOUR_FROM_UNIXTIME, resultCall.getFnName());
        assertEquals(1, resultCall.getChildren().size());
        assertEquals(tsColumn, resultCall.getChild(0));

        // hour(to_datetime(ts, 0)) -> hour_from_unixtime(ts)
        CallOperator toDatetimeCallScale0 = new CallOperator(FunctionSet.TO_DATETIME, Type.DATETIME,
                Lists.newArrayList(tsColumn, ConstantOperator.createInt(0)), null);
        CallOperator hourCall2 = new CallOperator(FunctionSet.HOUR, Type.TINYINT,
                Lists.newArrayList(toDatetimeCallScale0), null);

        ScalarOperator result2 = rule.apply(hourCall2, null);
        assertEquals(OperatorType.CALL, result2.getOpType());
        CallOperator resultCall2 = (CallOperator) result2;
        assertEquals(FunctionSet.HOUR_FROM_UNIXTIME, resultCall2.getFnName());
        assertEquals(1, resultCall2.getChildren().size());
        assertEquals(tsColumn, resultCall2.getChild(0));

        // hour(to_datetime(ts, 3)) -> hour_from_unixtime(ts/1000)
        CallOperator toDatetimeCallScale3 = new CallOperator(FunctionSet.TO_DATETIME, Type.DATETIME,
                Lists.newArrayList(tsColumn, ConstantOperator.createInt(3)), null);
        CallOperator hourCall3 = new CallOperator(FunctionSet.HOUR, Type.TINYINT,
                Lists.newArrayList(toDatetimeCallScale3), null);
        ScalarOperator result3 = rule.apply(hourCall3, null);
        assertEquals(OperatorType.CALL, result3.getOpType());
        CallOperator resultCall3 = (CallOperator) result3;
        assertEquals(FunctionSet.HOUR_FROM_UNIXTIME, resultCall3.getFnName());
        assertEquals(1, resultCall3.getChildren().size());
        // Expect a divide(ts, 1000) as the argument
        ScalarOperator arg3 = resultCall3.getChild(0);
        assertEquals(OperatorType.CALL, arg3.getOpType());
        CallOperator div3 = (CallOperator) arg3;
        assertEquals(FunctionSet.DIVIDE, div3.getFnName());
        assertEquals(tsColumn, div3.getChild(0));
        assertEquals(ConstantOperator.createInt(1000), div3.getChild(1));

        // hour(to_datetime(ts, 6)) -> hour_from_unixtime(ts/1000000)
        CallOperator toDatetimeCallScale6 = new CallOperator(FunctionSet.TO_DATETIME, Type.DATETIME,
                Lists.newArrayList(tsColumn, ConstantOperator.createInt(6)), null);
        CallOperator hourCall6 = new CallOperator(FunctionSet.HOUR, Type.TINYINT,
                Lists.newArrayList(toDatetimeCallScale6), null);
        ScalarOperator result6 = rule.apply(hourCall6, null);
        assertEquals(OperatorType.CALL, result6.getOpType());
        CallOperator resultCall6 = (CallOperator) result6;
        assertEquals(FunctionSet.HOUR_FROM_UNIXTIME, resultCall6.getFnName());
        assertEquals(1, resultCall6.getChildren().size());
        ScalarOperator arg6 = resultCall6.getChild(0);
        assertEquals(OperatorType.CALL, arg6.getOpType());
        CallOperator div6 = (CallOperator) arg6;
        assertEquals(FunctionSet.DIVIDE, div6.getFnName());
        assertEquals(tsColumn, div6.getChild(0));
        assertEquals(ConstantOperator.createInt(1_000_000), div6.getChild(1));

        // Unsupported scale like 4 should not be rewritten
        CallOperator toDatetimeCallScale4 = new CallOperator(FunctionSet.TO_DATETIME, Type.DATETIME,
                Lists.newArrayList(tsColumn, ConstantOperator.createInt(4)), null);
        CallOperator hourCall4 = new CallOperator(FunctionSet.HOUR, Type.TINYINT,
                Lists.newArrayList(toDatetimeCallScale4), null);
        ScalarOperator result4 = rule.apply(hourCall4, null);
        assertEquals(hourCall4, result4);
    }

    @Test
    public void applyExtractFromUnixTime() {
        ColumnRefOperator tsColumn = new ColumnRefOperator(1, Type.BIGINT, "ts", true);

        CallOperator fromUnixTimeCall = new CallOperator(FunctionSet.FROM_UNIXTIME, Type.VARCHAR,
                Lists.newArrayList(tsColumn), null);

        // year(from_unixtime(ts)) -> year_from_unixtime(ts)
        CallOperator yearCall = new CallOperator(FunctionSet.YEAR, Type.INT,
                Lists.newArrayList(fromUnixTimeCall), null);
        ScalarOperator yearResult = rule.apply(yearCall, null);
        assertEquals(OperatorType.CALL, yearResult.getOpType());
        CallOperator yearResultCall = (CallOperator) yearResult;
        assertEquals(FunctionSet.YEAR_FROM_UNIXTIME, yearResultCall.getFnName());
        assertEquals(tsColumn, yearResultCall.getChild(0));

        // month(from_unixtime(ts)) -> month_from_unixtime(ts)
        CallOperator monthCall = new CallOperator(FunctionSet.MONTH, Type.INT,
                Lists.newArrayList(fromUnixTimeCall), null);
        ScalarOperator monthResult = rule.apply(monthCall, null);
        assertEquals(OperatorType.CALL, monthResult.getOpType());
        CallOperator monthResultCall = (CallOperator) monthResult;
        assertEquals(FunctionSet.MONTH_FROM_UNIXTIME, monthResultCall.getFnName());
        assertEquals(tsColumn, monthResultCall.getChild(0));

        // day(from_unixtime(ts)) -> day_from_unixtime(ts)
        CallOperator dayCall = new CallOperator(FunctionSet.DAY, Type.INT,
                Lists.newArrayList(fromUnixTimeCall), null);
        ScalarOperator dayResult = rule.apply(dayCall, null);
        assertEquals(OperatorType.CALL, dayResult.getOpType());
        CallOperator dayResultCall = (CallOperator) dayResult;
        assertEquals(FunctionSet.DAY_FROM_UNIXTIME, dayResultCall.getFnName());
        assertEquals(tsColumn, dayResultCall.getChild(0));

        // minute(from_unixtime(ts)) -> minute_from_unixtime(ts)
        CallOperator minuteCall = new CallOperator(FunctionSet.MINUTE, Type.INT,
                Lists.newArrayList(fromUnixTimeCall), null);
        ScalarOperator minuteResult = rule.apply(minuteCall, null);
        assertEquals(OperatorType.CALL, minuteResult.getOpType());
        CallOperator minuteResultCall = (CallOperator) minuteResult;
        assertEquals(FunctionSet.MINUTE_FROM_UNIXTIME, minuteResultCall.getFnName());
        assertEquals(tsColumn, minuteResultCall.getChild(0));

        // second(from_unixtime(ts)) -> second_from_unixtime(ts)
        CallOperator secondCall = new CallOperator(FunctionSet.SECOND, Type.INT,
                Lists.newArrayList(fromUnixTimeCall), null);
        ScalarOperator secondResult = rule.apply(secondCall, null);
        assertEquals(OperatorType.CALL, secondResult.getOpType());
        CallOperator secondResultCall = (CallOperator) secondResult;
        assertEquals(FunctionSet.SECOND_FROM_UNIXTIME, secondResultCall.getFnName());
        assertEquals(tsColumn, secondResultCall.getChild(0));
    }
}