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

import com.google.common.collect.Maps;
import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScalarOperatorToExprTest {

    @Test
    public void testVisitVariableReferenceWithNullExpr() {
        // Test the new exception handling when expr is null
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(1, Type.INT, "test_col", true);

        ScalarOperatorToExpr.buildExprIgnoreSlot(columnRefOperator,
                new ScalarOperatorToExpr.FormatterContext(Maps.newHashMap()));

        // This should throw SemanticException
        SemanticException exception = assertThrows(SemanticException.class, () -> {
            ScalarOperatorToExpr.buildExecExpression(columnRefOperator,
                    new ScalarOperatorToExpr.FormatterContext(Maps.newHashMap()));
        });
        
        assertTrue(exception.getMessage().contains("Cannot convert ColumnRefOperator to Expr"));
        assertTrue(exception.getMessage().contains("test_col"));
    }
}