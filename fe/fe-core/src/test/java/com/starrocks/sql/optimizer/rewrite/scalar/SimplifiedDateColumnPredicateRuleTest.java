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

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.Test;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class SimplifiedDateColumnPredicateRuleTest {
    private static final ConstantOperator DATE_BEGIN = ConstantOperator.createVarchar("20240506");
    private static final ConstantOperator DATE_BEGIN2 = ConstantOperator.createVarchar("2024-05-06");

    private final SimplifiedDateColumnPredicateRule rule = new SimplifiedDateColumnPredicateRule();

    @Test
    public void testDateFormat() {
        {
            // dt is date
            ScalarOperator call = new CallOperator("date_format", Type.VARCHAR, ImmutableList.of(
                    new ColumnRefOperator(1, Type.DATE, "dt", true),
                    ConstantOperator.createVarchar("%Y%m%d")
            ));
            verifyDate(new BinaryPredicateOperator(BinaryType.EQ, call, DATE_BEGIN));
            verifyDate(new BinaryPredicateOperator(BinaryType.GE, call, DATE_BEGIN));
            verifyNotDate(new BinaryPredicateOperator(BinaryType.EQ, call, DATE_BEGIN2));
            verifyNotDate(
                    new BinaryPredicateOperator(BinaryType.GT, call, ConstantOperator.createVarchar("2024050600")));
            verifyNotDate(
                    new BinaryPredicateOperator(BinaryType.GT, call, ConstantOperator.createVarchar("20240500")));
            verifyNotDate(
                    new BinaryPredicateOperator(BinaryType.GT, call, ConstantOperator.createVarchar(" 20240506 ")));
        }
        {
            // dt is date
            ScalarOperator call = new CallOperator("date_format", Type.VARCHAR, ImmutableList.of(
                    new ColumnRefOperator(1, Type.DATE, "dt", true),
                    ConstantOperator.createVarchar("%Y-%m-%d")
            ));
            verifyNotDate(new BinaryPredicateOperator(BinaryType.EQ, call, DATE_BEGIN));
            verifyDate(new BinaryPredicateOperator(BinaryType.GE, call, DATE_BEGIN2));
            verifyNotDate(new BinaryPredicateOperator(BinaryType.EQ, call, DATE_BEGIN));
            verifyNotDate(
                    new BinaryPredicateOperator(BinaryType.EQ, call, ConstantOperator.createVarchar("2024050600")));
        }
        {
            // dt is datetime
            ScalarOperator datetimeColumn = new ColumnRefOperator(1, Type.DATETIME, "dt", true);
            ScalarOperator call = new CallOperator("date_format", Type.VARCHAR, ImmutableList.of(
                    datetimeColumn,
                    ConstantOperator.createVarchar("%Y%m%d")
            ));
            verifyNotDateTime(new BinaryPredicateOperator(BinaryType.GT, call, DATE_BEGIN2));
            verifyDateTime(new BinaryPredicateOperator(BinaryType.GT, call, DATE_BEGIN));
            verifyDateTime(new BinaryPredicateOperator(BinaryType.GE, call, DATE_BEGIN));
            verifyDateTime(new BinaryPredicateOperator(BinaryType.LT, call, DATE_BEGIN));
            verifyDateTime(new BinaryPredicateOperator(BinaryType.LE, call, DATE_BEGIN));
            verifyNotDateTime(new BinaryPredicateOperator(BinaryType.EQ, call, DATE_BEGIN));
        }
        // dt is varchar
        ScalarOperator varcharCall = new CallOperator("date_format", Type.VARCHAR, ImmutableList.of(
                new ColumnRefOperator(1, Type.VARCHAR, "dt", true),
                ConstantOperator.createVarchar("%Y%m%d")
        ));
        verifyNotDate(new BinaryPredicateOperator(BinaryType.EQ, varcharCall, DATE_BEGIN2));
        verifyNotDate(new BinaryPredicateOperator(BinaryType.GE, varcharCall, DATE_BEGIN2));
    }

    @Test
    public void testSubstr() {
        for (String fn : new String[] {"substr", "substring"}) {
            {
                // dt is date
                ScalarOperator call = new CallOperator(fn, Type.VARCHAR, ImmutableList.of(
                        new CastOperator(Type.VARCHAR, new ColumnRefOperator(1, Type.DATE, "dt", true)),
                        ConstantOperator.createInt(1),
                        ConstantOperator.createInt(10)
                ));
                verifyDate(new BinaryPredicateOperator(BinaryType.EQ, call, DATE_BEGIN2));
                verifyNotDate(new BinaryPredicateOperator(BinaryType.EQ, call, DATE_BEGIN));
                verifyDate(new BinaryPredicateOperator(BinaryType.EQ, call, DATE_BEGIN2));
            }
            {
                // dt is datetime
                ScalarOperator datetimeColumn = new ColumnRefOperator(1, Type.DATETIME, "dt", true);
                ScalarOperator call = new CallOperator(fn, Type.VARCHAR, ImmutableList.of(
                        new CastOperator(Type.VARCHAR, datetimeColumn),
                        ConstantOperator.createInt(1),
                        ConstantOperator.createInt(10)
                ));
                verifyNotDateTime(new BinaryPredicateOperator(BinaryType.GT, call, DATE_BEGIN));
                verifyDateTime(new BinaryPredicateOperator(BinaryType.GT, call, DATE_BEGIN2));
                verifyDateTime(new BinaryPredicateOperator(BinaryType.GE, call, DATE_BEGIN2));
                verifyDateTime(new BinaryPredicateOperator(BinaryType.LT, call, DATE_BEGIN2));
                verifyDateTime(new BinaryPredicateOperator(BinaryType.LE, call, DATE_BEGIN2));
            }
            {
                // dt is varchar
                ScalarOperator varcharCall = new CallOperator(fn, Type.VARCHAR, ImmutableList.of(
                        new ColumnRefOperator(1, Type.VARCHAR, "dt", true),
                        ConstantOperator.createInt(1),
                        ConstantOperator.createInt(10)
                ));
                verifyNotDate(new BinaryPredicateOperator(BinaryType.EQ, varcharCall, DATE_BEGIN2));
                verifyNotDate(new BinaryPredicateOperator(BinaryType.GE, varcharCall, DATE_BEGIN2));
                // dt is date, but substr end offset is not 10
                ScalarOperator call = new CallOperator(fn, Type.VARCHAR, ImmutableList.of(
                        new ColumnRefOperator(1, Type.DATE, "dt", true),
                        ConstantOperator.createInt(1),
                        ConstantOperator.createInt(9)
                ));
                verifyNotDate(new BinaryPredicateOperator(BinaryType.EQ, call, DATE_BEGIN2));
                verifyNotDate(new BinaryPredicateOperator(BinaryType.GE, call, DATE_BEGIN2));
            }
        }
    }

    @Test
    public void testReplaceAndSubstr() {
        {
            // dt is date
            ScalarOperator call = new CallOperator(FunctionSet.SUBSTR, Type.VARCHAR, ImmutableList.of(
                    new CastOperator(Type.VARCHAR, new ColumnRefOperator(1, Type.DATE, "dt", true)),
                    ConstantOperator.createInt(1),
                    ConstantOperator.createInt(10)
            ));
            ScalarOperator replaceCall = new CallOperator(FunctionSet.REPLACE, Type.VARCHAR, ImmutableList.of(
                    call,
                    ConstantOperator.createVarchar("-"),
                    ConstantOperator.createVarchar("")
            ));
            verifyDate(new BinaryPredicateOperator(BinaryType.EQ, replaceCall, DATE_BEGIN));
            verifyNotDate(new BinaryPredicateOperator(BinaryType.GE, replaceCall, DATE_BEGIN2));
        }
        {
            // dt is varchar
            ScalarOperator varcharCall = new CallOperator(FunctionSet.SUBSTR, Type.VARCHAR, ImmutableList.of(
                    new ColumnRefOperator(1, Type.VARCHAR, "dt", true),
                    ConstantOperator.createInt(1),
                    ConstantOperator.createInt(10)
            ));
            CallOperator replaceCall = new CallOperator(FunctionSet.REPLACE, Type.VARCHAR, ImmutableList.of(
                    varcharCall,
                    ConstantOperator.createVarchar("-"),
                    ConstantOperator.createVarchar("")
            ));
            verifyNotDate(new BinaryPredicateOperator(BinaryType.EQ, replaceCall, DATE_BEGIN2));
            verifyNotDate(new BinaryPredicateOperator(BinaryType.GE, replaceCall, DATE_BEGIN2));
        }
        {
            // dt is date
            ScalarOperator call = new CallOperator(FunctionSet.SUBSTR, Type.VARCHAR, ImmutableList.of(
                    new CastOperator(Type.VARCHAR, new ColumnRefOperator(1, Type.DATE, "dt", true)),
                    ConstantOperator.createInt(1),
                    ConstantOperator.createInt(10)
            ));
            // not replace '-' to ''
            CallOperator replaceCall = new CallOperator(FunctionSet.REPLACE, Type.VARCHAR, ImmutableList.of(
                    call,
                    ConstantOperator.createVarchar("-"),
                    ConstantOperator.createVarchar("a")
            ));
            verifyNotDate(new BinaryPredicateOperator(BinaryType.EQ, replaceCall, DATE_BEGIN2));
            verifyNotDate(new BinaryPredicateOperator(BinaryType.GE, replaceCall, DATE_BEGIN2));
        }
        {
            // dt is date, but substr end offset is not 10
            ScalarOperator call = new CallOperator(FunctionSet.SUBSTR, Type.VARCHAR, ImmutableList.of(
                    new ColumnRefOperator(1, Type.DATE, "dt", true),
                    ConstantOperator.createInt(1),
                    ConstantOperator.createInt(9)
            ));
            CallOperator replaceCall = new CallOperator(FunctionSet.REPLACE, Type.VARCHAR, ImmutableList.of(
                    call,
                    ConstantOperator.createVarchar("-"),
                    ConstantOperator.createVarchar("")
            ));
            verifyNotDate(new BinaryPredicateOperator(BinaryType.EQ, replaceCall, DATE_BEGIN2));
            verifyNotDate(new BinaryPredicateOperator(BinaryType.GE, replaceCall, DATE_BEGIN2));
        }
        {
            // dt is datetime
            ScalarOperator datetimeColumn = new ColumnRefOperator(1, Type.DATETIME, "dt", true);
            ScalarOperator call = new CallOperator(FunctionSet.SUBSTR, Type.VARCHAR, ImmutableList.of(
                    new CastOperator(Type.VARCHAR, datetimeColumn),
                    ConstantOperator.createInt(1),
                    ConstantOperator.createInt(10)
            ));
            CallOperator replaceCall = new CallOperator(FunctionSet.REPLACE, Type.VARCHAR, ImmutableList.of(
                    call,
                    ConstantOperator.createVarchar("-"),
                    ConstantOperator.createVarchar("")
            ));
            verifyNotDateTime(new BinaryPredicateOperator(BinaryType.GT, replaceCall, DATE_BEGIN2));
            verifyDateTime(new BinaryPredicateOperator(BinaryType.GT, replaceCall, DATE_BEGIN));
            verifyDateTime(new BinaryPredicateOperator(BinaryType.GE, replaceCall, DATE_BEGIN));
            verifyDateTime(new BinaryPredicateOperator(BinaryType.LT, replaceCall, DATE_BEGIN));
            verifyDateTime(new BinaryPredicateOperator(BinaryType.LE, replaceCall, DATE_BEGIN));
        }
    }

    private void verifyDate(ScalarOperator operator) {
        ScalarOperator result = rule.apply(operator, null);
        assertSame(PrimitiveType.DATE, result.getChild(0).getType().getPrimitiveType());
    }

    private void verifyNotDate(ScalarOperator operator) {
        ScalarOperator result = rule.apply(operator, null);
        assertNotSame(PrimitiveType.DATE, result.getChild(0).getType().getPrimitiveType());
    }

    private void verifyDateTime(ScalarOperator operator) {
        ScalarOperator result = rule.apply(operator, null);
        assertSame(PrimitiveType.DATETIME, result.getChild(0).getType().getPrimitiveType());
    }

    private void verifyNotDateTime(ScalarOperator operator) {
        ScalarOperator result = rule.apply(operator, null);
        assertNotSame(PrimitiveType.DATETIME, result.getChild(0).getType().getPrimitiveType());
    }
}