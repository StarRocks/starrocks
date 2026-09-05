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

package com.starrocks.sql;

import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;

import java.util.stream.Collectors;

public class Expr2SQLPrinter<T> extends ExpressionPrinter<T> {
    @Override
    public String visitVariableReference(ColumnRefOperator variable, T context) {
        return variable.getName();
    }

    @Override
    public String visitArray(ArrayOperator array, T context) {
        String child = array.getChildren().stream().map(p -> print(p, context)).collect(Collectors.joining(", "));
        return array.getType().toTypeString() + "[" + child + "]";
    }

    @Override
    public String visitConstant(ConstantOperator literal, T context) {
        String x = super.visitConstant(literal, context);
        if (literal.getType().isDateType()) {
            return "'" + x + "'";
        }
        return x;
    }
}