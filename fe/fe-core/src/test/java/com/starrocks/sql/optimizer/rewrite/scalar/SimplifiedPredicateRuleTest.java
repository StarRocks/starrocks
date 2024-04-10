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
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
}