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

package com.starrocks.sql.optimizer.rewrite;

import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.Pair;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.BetweenPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.TimestampArithmeticExpr;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.parser.SqlParser;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class TimeDriftConstraint {

    private final String spec;

    private final String targetColumn;
    private final String referenceColumn;

    private final long lowerGapSecs;
    private final long upperGapSecs;

    private TimeDriftConstraint(String spec, String targetColumn, String referenceColumn, long lowerGapSecs,
                                long upperGapSecs) {
        this.spec = spec;
        this.targetColumn = targetColumn;
        this.referenceColumn = referenceColumn;
        this.lowerGapSecs = lowerGapSecs;
        this.upperGapSecs = upperGapSecs;
    }

    private static final Map<String, Long> SUPPORT_BOUND_FUNCTIONS = ImmutableMap.<String, Long>builder()
            .put(FunctionSet.SECONDS_ADD, 1L)
            .put(FunctionSet.MINUTES_ADD, 60L)
            .put(FunctionSet.HOURS_ADD, 60L * 60L)
            .put(FunctionSet.DAYS_ADD, 24L * 60L * 60L)
            .put(FunctionSet.SECONDS_SUB, -1L)
            .put(FunctionSet.MINUTES_SUB, -60L)
            .put(FunctionSet.HOURS_SUB, -60L * 60L)
            .put(FunctionSet.DAYS_SUB, -24 * 60L * 60L)
            .build();
    private static final String SUPPORT_BOUND_FUNCTION_NAMES = SUPPORT_BOUND_FUNCTIONS.keySet()
            .stream().sorted().collect(Collectors.joining("/"));

    public String getTargetColumn() {
        return targetColumn;
    }

    public String getReferenceColumn() {
        return referenceColumn;
    }

    public long getLowerGapSecs() {
        return lowerGapSecs;
    }

    public long getUpperGapSecs() {
        return upperGapSecs;
    }

    private static Pair<String, Long> parseBoundExpr(String spec, Expr expr) {
        // days_sub is translated into TimestampArithmeticExpr in parse-time, so translated it back.
        if ((expr instanceof TimestampArithmeticExpr) &&
                ((TimestampArithmeticExpr) expr).getFuncName().equalsIgnoreCase(FunctionSet.DAYS_SUB)) {
            expr = new FunctionCallExpr(FunctionSet.DAYS_SUB, expr.getChildren());
        }
        if (!(expr instanceof FunctionCallExpr)) {
            throw new SemanticException(ERR_MSG_FORMAT, spec);
        }
        FunctionCallExpr fcall = (FunctionCallExpr) expr;

        String fname = fcall.getFnName().getFunction();
        if (!SUPPORT_BOUND_FUNCTIONS.containsKey(fname)) {
            throw new SemanticException(ERR_MSG_FORMAT, spec);
        }

        if ((fcall.getChild(0) instanceof SlotRef) && (fcall.getChild(1) instanceof IntLiteral)) {
            SlotRef column = (SlotRef) fcall.getChild(0);
            IntLiteral delta = (IntLiteral) fcall.getChild(1);
            String columnName = column.getColumnName();
            long deltaValue = SUPPORT_BOUND_FUNCTIONS.get(fname) * delta.getLongValue();
            return Pair.create(columnName, deltaValue);
        } else {
            throw new SemanticException(ERR_MSG_FORMAT, spec);
        }
    }

    private static final String ERR_MSG_FORMAT = "Invalid time_drift_constraint: '%s', " +
            "legal format is '<targetColumn> between <boundExpr1> and <boundExpr2>', boundExpr must be " +
            SUPPORT_BOUND_FUNCTION_NAMES + "(<referenceColumn>, <num>), " +
            "targetColumn and referenceColumn must be DATE/DATETIME type";

    public static TimeDriftConstraint parseSpec(String spec) {
        Expr expr = SqlParser.parseSqlToExpr(spec, SqlModeHelper.MODE_DEFAULT);
        String targetColumnName;
        if (expr instanceof BetweenPredicate) {
            BetweenPredicate betweenExpr = (BetweenPredicate) expr;
            Expr timeColumnExpr = betweenExpr.getChild(0);
            Expr lbExpr = betweenExpr.getChild(1);
            Expr ubExpr = betweenExpr.getChild(2);
            if ((timeColumnExpr instanceof SlotRef)) {
                targetColumnName = ((SlotRef) timeColumnExpr).getColumnName();
            } else {
                throw new SemanticException(ERR_MSG_FORMAT, spec);
            }

            Pair<String, Long> lbGap = parseBoundExpr(spec, lbExpr);
            Pair<String, Long> ubGap = parseBoundExpr(spec, ubExpr);
            if (!lbGap.first.equals(ubGap.first) || lbGap.second > ubGap.second) {
                throw new SemanticException(ERR_MSG_FORMAT, spec);
            }
            if (targetColumnName.equals(lbGap.first)) {
                throw new SemanticException(ERR_MSG_FORMAT, spec);
            }
            return new TimeDriftConstraint(spec, targetColumnName, lbGap.first, lbGap.second, ubGap.second);
        }
        throw new SemanticException(ERR_MSG_FORMAT, spec);
    }

    private List<ScalarOperator> derive(ScalarOperator predicate,
                                        ColumnRefOperator targetColumnRef,
                                        ColumnRefOperator referenceColumnRef) {
        MinMax minMax = Utils.extractConjuncts(predicate).stream()
                .filter(conjunct -> conjunct.getUsedColumns().size() == 1 &&
                        conjunct.getUsedColumns().contains(targetColumnRef))
                .map(conjunct -> conjunct.accept(RangePredicateInference.INSTANCE, null))
                .collect(MinMax.intersectionAll());
        if (minMax == MinMax.ALL) {
            return Collections.emptyList();
        }
        if (minMax == MinMax.EMPTY) {
            return Collections.singletonList(ConstantOperator.FALSE);
        }

        List<ScalarOperator> newConjuncts = Lists.newArrayList();
        Optional<ConstantOperator> optMax = minMax.getMax();
        if (optMax.isPresent()) {
            ConstantOperator maxValue =
                    ScalarOperatorFunctions.secondsSub(optMax.get(), ConstantOperator.createInt((int) lowerGapSecs));
            ScalarOperator pred = minMax.isMaxInclusive() ? BinaryPredicateOperator.le(referenceColumnRef, maxValue) :
                    BinaryPredicateOperator.lt(referenceColumnRef, maxValue);
            newConjuncts.add(pred);
        }
        Optional<ConstantOperator> optMin = minMax.getMin();
        if (optMin.isPresent()) {
            ConstantOperator minValue =
                    ScalarOperatorFunctions.secondsSub(optMin.get(), ConstantOperator.createInt((int) upperGapSecs));
            ScalarOperator pred = minMax.isMinInclusive() ? BinaryPredicateOperator.ge(referenceColumnRef, minValue) :
                    BinaryPredicateOperator.gt(referenceColumnRef, minValue);
            newConjuncts.add(pred);
        }
        return newConjuncts;
    }

    public static ScalarOperator tryAddDerivedPredicates(ScalarOperator predicate,
                                                         Table table,
                                                         Map<String, ColumnRefOperator> columnNameToColumnRefMap) {
        if (!(table instanceof OlapTable)) {
            return predicate;
        }
        OlapTable olapTable = (OlapTable) table;
        TableProperty tableProperty = olapTable.getTableProperty();
        if (tableProperty == null) {
            return predicate;
        }
        String spec = olapTable.getTableProperty().getTimeDriftConstraintSpec();
        if (spec == null || spec.isEmpty()) {
            return predicate;
        }

        TimeDriftConstraint timeDriftConstraint;
        try {
            timeDriftConstraint = parseSpec(spec);
        } catch (Throwable ignored) {
            return predicate;
        }

        if (!columnNameToColumnRefMap.containsKey(timeDriftConstraint.getTargetColumn()) ||
                !columnNameToColumnRefMap.containsKey(timeDriftConstraint.getReferenceColumn())) {
            return predicate;
        }
        ColumnRefOperator targetColumnRef = columnNameToColumnRefMap.get(timeDriftConstraint.getTargetColumn());
        ColumnRefOperator referenceColumnRef = columnNameToColumnRefMap.get(timeDriftConstraint.getReferenceColumn());
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        conjuncts.addAll(timeDriftConstraint.derive(predicate, targetColumnRef, referenceColumnRef));
        return Utils.compoundAnd(conjuncts);
    }
}
