// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;

import java.util.Iterator;
import java.util.List;

public class RewriteUtils {

    public static ScalarOperator splitOr(ScalarOperator src, ScalarOperator target) {
        List<ScalarOperator> srcItems = Utils.extractDisjunctive(src);
        List<ScalarOperator> targetItems = Utils.extractDisjunctive(target);
        int srcLength = srcItems.size();
        int targetLength = targetItems.size();
        for (ScalarOperator item : srcItems) {
            removeAll(targetItems, item);
        }
        if (srcItems.isEmpty() && srcLength == targetLength) {
            // it is the same, so return true constant
            return ConstantOperator.createBoolean(true);
        } else if (srcItems.isEmpty()) {
            // the target has more or item, so return src
            return src;
        } else {
            return null;
        }
    }

    public static void removeAll(List<ScalarOperator> scalars, ScalarOperator predicate) {
        Iterator<ScalarOperator> iter = scalars.iterator();
        while (iter.hasNext()) {
            ScalarOperator current = iter.next();
            if (current.equals(predicate)) {
                iter.remove();
            }
        }
    }
    public static ScalarOperator canonizeNode(ScalarOperator predicate) {
        if (predicate == null) {
            return null;
        }
        ScalarOperatorRewriter rewrite = new ScalarOperatorRewriter();
        return rewrite.rewrite(predicate, ScalarOperatorRewriter.MV_SCALAR_REWRITE_RULES);
    }

    public static boolean isLogicalSPJG(OptExpression root) {
        if (root == null) {
            return false;
        }
        Operator operator = root.getOp();
        if (!(operator instanceof LogicalAggregationOperator)) {
            return false;
        }
        LogicalAggregationOperator agg = (LogicalAggregationOperator) operator;
        if (agg.getType() != AggType.GLOBAL) {
            return false;
        }

        OptExpression child = root.inputAt(0);
        return isLogicalSPJ(child);
    }

    public static boolean isLogicalSPJ(OptExpression root) {
        if (root == null) {
            return false;
        }
        Operator operator = root.getOp();
        if (!(operator instanceof LogicalOperator)) {
            return false;
        }
        if (!(operator instanceof LogicalScanOperator)
                && !(operator instanceof LogicalProjectOperator)
                && !(operator instanceof LogicalFilterOperator)
                && !(operator instanceof LogicalJoinOperator)) {
            return false;
        }
        for (OptExpression child : root.getInputs()) {
            if (!isLogicalSPJ(child)) {
                return false;
            }
        }
        return true;
    }

    // get all ref tables within and below root
    public static List<LogicalScanOperator> getAllScanOperator(OptExpression root) {
        List<LogicalScanOperator> scanOperators = Lists.newArrayList();
        getAllScanOperator(root, scanOperators);
        return scanOperators;
    }

    private static void getAllScanOperator(OptExpression root, List<LogicalScanOperator> scanOperators) {
        if (root.getOp() instanceof LogicalScanOperator) {
            scanOperators.add((LogicalScanOperator) root.getOp());
        } else {
            for (OptExpression child : root.getInputs()) {
                getAllScanOperator(child, scanOperators);
            }
        }
    }

    // get all ref tables within and below root
    public static List<Table> getAllTables(OptExpression root) {
        List<Table> tables = Lists.newArrayList();
        getAllTables(root, tables);
        return tables;
    }

    private static void getAllTables(OptExpression root, List<Table> tables) {
        if (root.getOp() instanceof LogicalScanOperator) {
            LogicalScanOperator scanOperator = (LogicalScanOperator) root.getOp();
            tables.add(scanOperator.getTable());
        } else {
            for (OptExpression child : root.getInputs()) {
                getAllTables(child, tables);
            }
        }
    }

    // get all predicates within and below root
    public static List<ScalarOperator> getAllPredicates(OptExpression root) {
        List<ScalarOperator> predicates = Lists.newArrayList();
        getAllPredicates(root, predicates);
        return predicates;
    }

    private static void getAllPredicates(OptExpression root, List<ScalarOperator> predicates) {
        Operator operator = root.getOp();
        if (operator.getPredicate() != null) {
            predicates.add(root.getOp().getPredicate());
        }
        if (operator instanceof LogicalJoinOperator) {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) operator;
            if (joinOperator.getOnPredicate() != null) {
                predicates.add(joinOperator.getOnPredicate());
            }
        }
        for (OptExpression child : root.getInputs()) {
            getAllPredicates(child, predicates);
        }
    }

    // split predicate into three parts: equal columns predicates, range predicates, and residual predicates
    public static PredicateSplit splitPredicate(ScalarOperator predicate) {
        if (predicate == null) {
            return PredicateSplit.of(null, null, null);
        }
        List<ScalarOperator> predicateConjuncts = Utils.extractConjuncts(predicate);
        List<ScalarOperator> columnEqualityPredicates = Lists.newArrayList();
        List<ScalarOperator> rangePredicates = Lists.newArrayList();
        List<ScalarOperator> residualPredicates = Lists.newArrayList();
        for (ScalarOperator scalarOperator : predicateConjuncts) {
            if (scalarOperator instanceof BinaryPredicateOperator) {
                BinaryPredicateOperator binary = (BinaryPredicateOperator) scalarOperator;
                ScalarOperator leftChild = scalarOperator.getChild(0);
                ScalarOperator rightChild = scalarOperator.getChild(1);
                if (binary.getBinaryType().isEqual()) {
                    if (leftChild.isColumnRef() && rightChild.isColumnRef()) {
                        columnEqualityPredicates.add(scalarOperator);
                    } else if (leftChild.isColumnRef() && rightChild.isConstantRef()) {
                        rangePredicates.add(scalarOperator);
                    } else {
                        residualPredicates.add(scalarOperator);
                    }
                } else if (binary.getBinaryType().isRange()) {
                    if (leftChild.isColumnRef() && rightChild.isConstantRef()) {
                        rangePredicates.add(scalarOperator);
                    } else {
                        residualPredicates.add(scalarOperator);
                    }
                }
            } else {
                residualPredicates.add(scalarOperator);
            }
        }
        return PredicateSplit.of(Utils.compoundAnd(columnEqualityPredicates), Utils.compoundAnd(rangePredicates),
                Utils.compoundAnd(residualPredicates));
    }

    // may merge with isLogicalSPJG
    // for one table, returns true
    public static boolean isAllEqualInnerJoin(OptExpression root) {
        Operator operator = root.getOp();
        if (!(operator instanceof LogicalOperator)) {
            return false;
        }
        if (operator instanceof LogicalJoinOperator) {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) operator;
            boolean isEqualPredicate = isEqualPredicate(joinOperator.getOnPredicate());
            if (joinOperator.getJoinType() == JoinOperator.INNER_JOIN && isEqualPredicate) {
                return true;
            }
            return false;
        }
        for (OptExpression child : root.getInputs()) {
            if (!isAllEqualInnerJoin(child)) {
                return false;
            }
        }
        return true;
    }

    public static boolean isEqualPredicate(ScalarOperator predicate) {
        if (predicate == null) {
            return false;
        }
        if (predicate instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator binaryPredicate = (BinaryPredicateOperator) predicate;
            if (binaryPredicate.getBinaryType().isEqual()
                    && binaryPredicate.getChild(0).isColumnRef()
                    && binaryPredicate.getChild(1).isColumnRef()) {
                return true;
            }
        }
        return false;
    }

    public static boolean isAlwaysFalse(ScalarOperator predicate) {
        if (predicate instanceof ConstantOperator) {
            ConstantOperator constant = (ConstantOperator) predicate;
            if (constant.getType() == Type.BOOLEAN && constant.getBoolean() == false) {
                return true;
            }
        } else if (predicate instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator compound = (CompoundPredicateOperator) predicate;
            if (compound.isAnd()) {
                return isAlwaysFalse(compound.getChild(0)) || isAlwaysFalse(compound.getChild(1));
            } else if (compound.isOr()) {
                return isAlwaysFalse(compound.getChild(0)) && isAlwaysFalse(compound.getChild(1));
            } else if (compound.isNot()) {
                return isAlwaysTrue(predicate.getChild(0));
            }
        }
        return false;
    }

    public static boolean isAlwaysTrue(ScalarOperator predicate) {
        if (predicate instanceof ConstantOperator) {
            ConstantOperator constant = (ConstantOperator) predicate;
            if (constant.getType() == Type.BOOLEAN && constant.getBoolean() == true) {
                return true;
            }
        } else if (predicate instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator compound = (CompoundPredicateOperator) predicate;
            if (compound.isAnd()) {
                return isAlwaysTrue(compound.getChild(0)) && isAlwaysTrue(compound.getChild(1));
            } else if (compound.isOr()) {
                return isAlwaysTrue(compound.getChild(0)) || isAlwaysTrue(compound.getChild(1));
            } else if (compound.isNot()) {
                return isAlwaysFalse(predicate.getChild(0));
            }
        }
        return false;
    }
}
