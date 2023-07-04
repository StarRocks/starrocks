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


package com.starrocks.rowstore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.planner.tupledomain.ColumnValue;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class RowStoreUtils {

    /**
     * Columns from predicate is prefix of key columns, and all columns is eq filter 
     * except the last of columns support range filter.
     * 
     * <p>PrefixScan with filter can only use a single scanner. This method is used for short circuit executor.
     */
    public static boolean isPrefixRangeScan(List<String> keyColumns, List<ScalarOperator> predicates) {
        Set<String> keyColumnSet = ImmutableSet.copyOf(keyColumns);
        Set<String> eqKeys = new HashSet<>();
        Set<String> rangeKeys = new HashSet<>();
        for (ScalarOperator conjunct : predicates) {
            ScalarOperator column = conjunct.getChild(0);
            if (!column.isColumnRef()) {
                return false;
            }
            if (conjunct instanceof BinaryPredicateOperator) {
                BinaryPredicateOperator binaryOp = conjunct.cast();
                String columnName = ((ColumnRefOperator) column).getName();
                if (!keyColumnSet.contains(columnName)) {
                    return false;
                }

                if (!(binaryOp.getChild(1) instanceof ConstantOperator)) {
                    return false;
                }
                switch (binaryOp.getBinaryType()) {
                    case GE:
                    case GT:
                    case LE:
                    case LT:
                        rangeKeys.add(columnName);
                        break;
                    case EQ:
                        eqKeys.add(columnName);
                        break;
                    default:
                        return false;
                }
                continue;
            }
            return false;
        }
        return isPrefixScan(keyColumns, eqKeys, rangeKeys);
    }


    public static boolean isPrefixScan(List<String> keyColumns, List<Expr> predicates) {
        Set<String> keyColumnSet = ImmutableSet.copyOf(keyColumns);
        Set<String> eqKeys = new HashSet<>();
        Set<String> rangeKeys = new HashSet<>();
        for (Expr conjunct : predicates) {
            Expr column = conjunct.getChild(0);
            if (!(column instanceof SlotRef)) {
                return false;
            }
            if (conjunct instanceof BinaryPredicate) {
                BinaryPredicate binaryOp = conjunct.cast();
                String columnName = ((SlotRef) column).getDesc().getColumn().getName();
                if (!keyColumnSet.contains(columnName)) {
                    return false;
                }

                if (!(binaryOp.getChild(1) instanceof LiteralExpr)) {
                    return false;
                }
                switch (binaryOp.getOp()) {
                    case GE:
                    case GT:
                    case LE:
                    case LT:
                        rangeKeys.add(columnName);
                        break;
                    case EQ:
                        eqKeys.add(columnName);
                        break;
                    default:
                        return false;
                }
                continue;
            }
            return false;
        }
        return isPrefixScan(keyColumns, eqKeys, rangeKeys);
    }

    private static boolean isPrefixScan(List<String> keyColumns, Set<String> eqKeys, Set<String> rangeKeys) {
        int i = 0;
        for (; i < keyColumns.size(); i++) {
            if (eqKeys.contains(keyColumns.get(i))) {
                continue;
            }
            break;
        }
        if (rangeKeys.size() == 1) {
            if (!keyColumns.get(i++).equals(rangeKeys.iterator().next())) {
                return false;
            }
            
        }
        return keyColumns.stream().skip(i).noneMatch(c -> eqKeys.contains(c) || rangeKeys.contains(c));
    }

    public static Optional<List<RowStoreKeyTuple>> extractPoints(List<Expr> conjuncts, List<String> keyColumns) {
        Set<String> keyColumnSet = ImmutableSet.copyOf(keyColumns);
        Map<String, List<LiteralExpr>> keyToValues = new HashMap<>();

        for (Expr expr : conjuncts) {
            Expr column = expr.getChild(0);
            if (!(column instanceof SlotRef)) {
                continue;
            }
            String columnName = ((SlotRef) column).getDesc().getColumn().getName();
            if (!keyColumnSet.contains(columnName)) {
                continue;
            }
            if (expr instanceof BinaryPredicate) {
                Expr literal = expr.getChild(1);
                if (!(literal instanceof LiteralExpr)) {
                    continue;
                }
                if (!BinaryPredicate.IS_EQ_PREDICATE.apply((BinaryPredicate) expr)) {
                    continue;
                }
                if (keyToValues.containsKey(columnName)) {
                    return Optional.empty(); // don't deal with it here
                }
                keyToValues.put(columnName, ImmutableList.of((LiteralExpr) literal));
            } else if (expr instanceof InPredicate) {
                List<LiteralExpr> literalExprs = new ArrayList<>();
                for (Expr literal : expr.getChildren().subList(1, expr.getChildren().size())) {
                    if (!(literal instanceof LiteralExpr)) {
                        continue;
                    }
                    literalExprs.add((LiteralExpr) literal);
                }
                if (keyToValues.containsKey(columnName)) {
                    return Optional.empty();
                }
                keyToValues.put(columnName, literalExprs);
            }
        }

        if (keyToValues.size() != keyColumns.size()) {
            return Optional.empty();
        }

        List<List<LiteralExpr>> values = keyColumns.stream().map(keyToValues::get).collect(Collectors.toList());
        List<List<LiteralExpr>> cartesianProduct = Lists.cartesianProduct(values);
        List<RowStoreKeyTuple> rows = cartesianProduct.stream()
                .map(e -> new RowStoreKeyTuple(e.stream().map(ColumnValue::new).collect(Collectors.toList())))
                .collect(Collectors.toList());
        return Optional.of(rows);
    }

    public static Optional<List<List<LiteralExpr>>> extractPointsUsingOriginLiteral(List<Expr> conjuncts,
                                                                              List<String> keyColumns) {
        Set<String> keyColumnSet = ImmutableSet.copyOf(keyColumns);
        Map<String, List<LiteralExpr>> keyToValues = new HashMap<>();

        for (Expr expr : conjuncts) {
            Expr column = expr.getChild(0);
            if (!(column instanceof SlotRef)) {
                continue;
            }
            String columnName = ((SlotRef) column).getDesc().getColumn().getName();
            if (!keyColumnSet.contains(columnName)) {
                continue;
            }
            if (expr instanceof BinaryPredicate) {
                Expr literal = expr.getChild(1);
                if (!(literal instanceof LiteralExpr)) {
                    continue;
                }
                if (!BinaryPredicate.IS_EQ_PREDICATE.apply((BinaryPredicate) expr)) {
                    continue;
                }
                if (keyToValues.containsKey(columnName)) {
                    return Optional.empty(); // don't deal with it here
                }
                keyToValues.put(columnName, ImmutableList.of((LiteralExpr) literal));
            } else if (expr instanceof InPredicate) {
                List<LiteralExpr> literalExprs = new ArrayList<>();
                for (Expr literal : expr.getChildren().subList(1, expr.getChildren().size())) {
                    if (!(literal instanceof LiteralExpr)) {
                        continue;
                    }
                    literalExprs.add((LiteralExpr) literal);
                }
                if (keyToValues.containsKey(columnName)) {
                    return Optional.empty();
                }
                keyToValues.put(columnName, literalExprs);
            }
        }

        if (keyToValues.size() != keyColumns.size()) {
            return Optional.empty();
        }

        List<List<LiteralExpr>> values = keyColumns.stream().map(keyToValues::get).collect(Collectors.toList());
        List<List<LiteralExpr>> cartesianProduct = Lists.cartesianProduct(values);
        return Optional.of(cartesianProduct);
    }
}

