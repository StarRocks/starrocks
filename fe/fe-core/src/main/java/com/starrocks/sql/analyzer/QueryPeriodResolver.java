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

package com.starrocks.sql.analyzer;

import com.starrocks.connector.ConnectorTableVersion;
import com.starrocks.connector.PointerType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.QueryPeriod;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;

import java.util.Optional;

public class QueryPeriodResolver {
    private QueryPeriodResolver() {
    }

    public static Optional<ConnectorTableVersion> resolve(Optional<Expr> version,
                                                          QueryPeriod.PeriodType type,
                                                          ConnectContext session) {
        if (version.isEmpty()) {
            return Optional.empty();
        }
        ScalarOperator result;
        try {
            Scope scope = new Scope(RelationId.anonymous(), new RelationFields());
            ExpressionAnalyzer.analyzeExpression(version.get(), new AnalyzeState(), scope, session);
            ExpressionMapping expressionMapping = new ExpressionMapping(scope);
            result = SqlToScalarOperatorTranslator.translate(version.get(), expressionMapping, new ColumnRefFactory());
        } catch (Exception e) {
            throw new SemanticException("Failed to resolve query period [type: %s, value: %s]. msg: %s",
                    type.toString(), version.get().toString(), e.getMessage());
        }

        if (!(result instanceof ConstantOperator)) {
            if (version.get() instanceof FunctionCallExpr) {
                throw new SemanticException("Invalid datetime function: [type: %s, value: %s]. " +
                        "The function requirement must be inferred in frontend.", type.toString(),
                        version.get().toString());
            } else {
                throw new SemanticException("Invalid version value. [type: %s, value: %s]",
                        type.toString(), version.get().toString());
            }
        }
        PointerType pointerType = type == QueryPeriod.PeriodType.TIMESTAMP ? PointerType.TEMPORAL : PointerType.VERSION;
        return Optional.of(new ConnectorTableVersion(pointerType, (ConstantOperator) result));
    }
}
