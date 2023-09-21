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
package com.starrocks.common.util;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LargeIntLiteral;

public abstract class ExprUtil {
    public static boolean isPositiveConstantInteger(Expr expr) {
        if (!expr.isConstant()) {
            return false;
        }

        double value = 0;
        if (expr instanceof IntLiteral) {
            IntLiteral intl = (IntLiteral) expr;
            value = intl.getDoubleValue();
        } else if (expr instanceof LargeIntLiteral) {
            LargeIntLiteral intl = (LargeIntLiteral) expr;
            value = intl.getDoubleValue();
        }

        return value > 0;
    }

    public static Long getIntegerConstant(Expr expr) {
        if (!expr.isConstant()) {
            return null;
        }

        Long value = null;
        if (expr instanceof IntLiteral) {
            IntLiteral intl = (IntLiteral) expr;
            value = intl.getValue();
        }

        return value;
    }
}
