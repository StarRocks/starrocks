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


package com.starrocks.sql.optimizer.rule.mv;

import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

public class MVUtils {
    public static final String MATERIALIZED_VIEW_NAME_PREFIX = "mv_";

    public static boolean isEquivalencePredicate(ScalarOperator predicate) {
        if (predicate instanceof InPredicateOperator) {
            return true;
        }
        if (predicate instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator binary = (BinaryPredicateOperator) predicate;
            return binary.getBinaryType().isEquivalence();
        }
        return false;
    }

    public static boolean isPredicateUsedForPrefixIndex(ScalarOperator predicate) {
        if (!(predicate instanceof InPredicateOperator)
                && !(predicate instanceof BinaryPredicateOperator)) {
            return false;
        }
        if (predicate instanceof InPredicateOperator) {
            return isInPredicateUsedForPrefixIndex((InPredicateOperator) predicate);
        } else {
            return isBinaryPredicateUsedForPrefixIndex((BinaryPredicateOperator) predicate);
        }
    }

    private static boolean isInPredicateUsedForPrefixIndex(InPredicateOperator predicate) {
        if (predicate.isNotIn()) {
            return false;
        }
        return isColumnRefNested(predicate.getChild(0)) && predicate.allValuesMatch(ScalarOperator::isConstant);
    }

    private static boolean isBinaryPredicateUsedForPrefixIndex(BinaryPredicateOperator predicate) {
        if (predicate.getBinaryType().isNotEqual()) {
            return false;
        }
        return (isColumnRefNested(predicate.getChild(0)) && predicate.getChild(1).isConstant())
                || (isColumnRefNested(predicate.getChild(1)) && predicate.getChild(0).isConstant());
    }

    private static boolean isColumnRefNested(ScalarOperator operator) {
        while (operator instanceof CastOperator) {
            operator = operator.getChild(0);
        }
        return operator.isColumnRef();
    }

    public static String getMVAggColumnName(String functionName, String baseColumnName) {
        return new StringBuilder().append(MATERIALIZED_VIEW_NAME_PREFIX)
                .append(functionName).append("_").append(baseColumnName).toString();
    }

    public static String getMVColumnName(String baseColumName) {
        return new StringBuilder().append(MATERIALIZED_VIEW_NAME_PREFIX).append(baseColumName).toString();
    }

    // NOTE:
    // 1. check all materialized view column's defineExpr in MaterializedViewRule:
    // - only support slot-ref for non-aggregate column.
    // - only support specific function  for aggregate column.
    // 2. MV with Complex expressions will be used to rewrite query by AggregatedMaterializedViewRewriter.
    public static boolean containComplexExpresses(MaterializedIndexMeta mvMeta) {
        for (Column mvColumn : mvMeta.getSchema()) {
            Expr definedExpr = mvColumn.getDefineExpr();
            if (definedExpr == null) {
                continue;
            }

            if (mvColumn.isAggregated()) {
                if (definedExpr instanceof SlotRef) {
                    continue;
                } else if (definedExpr instanceof FunctionCallExpr) {
                    FunctionCallExpr functionCallExpr = (FunctionCallExpr) definedExpr;
                    String argFuncName = functionCallExpr.getFnName().getFunction();
                    Expr arg0FuncExpr = functionCallExpr.getChild(0);
                    if (!(arg0FuncExpr instanceof SlotRef)) {
                        return true;
                    }
                    switch (mvColumn.getAggregationType()) {
                        case BITMAP_UNION: {
                            if (!argFuncName.equalsIgnoreCase(FunctionSet.TO_BITMAP)) {
                                return true;
                            }
                            break;
                        }
                        case HLL_UNION: {
                            if (!argFuncName.equalsIgnoreCase(FunctionSet.HLL_HASH)) {
                                return true;
                            }
                            break;
                        }
                        case PERCENTILE_UNION: {
                            if (!argFuncName.equalsIgnoreCase(FunctionSet.PERCENTILE_HASH)) {
                                return true;
                            }
                            break;
                        }
                        default:
                            return true;
                    }
                } else if (definedExpr instanceof CaseExpr) {
                    CaseExpr caseExpr = (CaseExpr) definedExpr;
                    if (mvColumn.getAggregationType() != AggregateType.SUM) {
                        return true;
                    }
                    if (caseExpr.getChildren().size() != 3) {
                        return true;
                    }
                    if (!(caseExpr.getChild(0) instanceof IsNullPredicate) ||
                            !(((IsNullPredicate) caseExpr.getChild(0)).getChild(0) instanceof SlotRef)) {
                        return true;
                    }
                    if (!(caseExpr.getChild(1) instanceof IntLiteral) ||
                            ((IntLiteral) (caseExpr.getChild(1))).getLongValue() != 0L) {
                        return true;
                    }
                    if (!(caseExpr.getChild(2) instanceof IntLiteral) ||
                            ((IntLiteral) (caseExpr.getChild(2))).getLongValue() != 1L) {
                        return true;
                    }
                } else {
                    return true;
                }
            } else {
                if (!(definedExpr instanceof SlotRef)) {
                    return true;
                }
            }
        }
        return false;
    }

}
