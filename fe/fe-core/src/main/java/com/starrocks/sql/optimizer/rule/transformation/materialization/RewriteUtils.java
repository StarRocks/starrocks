// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
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
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import org.apache.commons.lang3.tuple.Triple;

import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator.BinaryType.EQ;
import static com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator.BinaryType.GE;
import static com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator.BinaryType.GT;
import static com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator.BinaryType.LE;
import static com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator.BinaryType.LT;

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
        ScalarOperatorRewriter rewrite = new ScalarOperatorRewriter();
        return rewrite.rewrite(predicate, ScalarOperatorRewriter.MV_SCALAR_REWRITE_RULES);
    }

    // this function can be optimized
    public static ScalarOperator canonizeNode2(ScalarOperator predicate) {
        switch (predicate.getOpType()) {
            case COMPOUND:
                SortedMap<String, ScalarOperator> canonizedChildren = new TreeMap<>();
                for (ScalarOperator child : predicate.getChildren()) {
                    ScalarOperator canonizedChild = canonizeNode(child);
                    canonizedChildren.put(canonizedChild.toString(), canonizedChild);
                }
                return new CompoundPredicateOperator(((CompoundPredicateOperator) predicate).getCompoundType(),
                        ImmutableList.copyOf(canonizedChildren.values()));
            case BINARY:
                BinaryPredicateOperator binary = (BinaryPredicateOperator) predicate;
                ScalarOperator left = canonizeNode(predicate.getChild(0));
                ScalarOperator right = canonizeNode(predicate.getChild(1));
                BinaryPredicateOperator canonizedPredicate = new BinaryPredicateOperator(binary.getBinaryType(), left, right);
                // default logic
                if (left.isConstant() && right instanceof ColumnRefOperator) {
                    // if 3 >= col, return col <= 3
                    canonizedPredicate = canonizedPredicate.commutative();
                } else if (!(left instanceof ConstantOperator && right.isConstant())
                        && left.toString().compareTo(right.toString()) > 0) {
                    canonizedPredicate = canonizedPredicate.commutative();
                }
                switch (binary.getBinaryType()) {
                    case LT:
                        // convert col(int) < 3 to col(int) <= 2
                        right = canonizedPredicate.getChild(1);
                        if (right.isConstantRef() && right.getType().isIntegerType()) {
                            ConstantOperator constantOperator = (ConstantOperator) right;
                            if (constantOperator.isNull()) {
                                break;
                            }
                            if (right.getType().isTinyint()) {
                                byte value = ((ConstantOperator) right).getTinyInt();
                                if (value == Byte.MIN_VALUE) {
                                    if (left.getType().isTinyint()) {
                                        // left < Byte.MIN_VALUE is always false, so just return false
                                        return ConstantOperator.createBoolean(false);
                                    } else {
                                        break;
                                    }
                                }
                                ConstantOperator newRight = ConstantOperator.createTinyInt((byte) (value - 1));
                                return new BinaryPredicateOperator(LE, canonizedPredicate.getChild(0), newRight);
                            } else if (right.getType().isSmallint()) {
                                short value = ((ConstantOperator) right).getSmallint();
                                if (value == Short.MIN_VALUE) {
                                    if (left.getType().isSmallint()) {
                                        return ConstantOperator.createBoolean(false);
                                    } else {
                                        break;
                                    }
                                }
                                ConstantOperator newRight = ConstantOperator.createSmallInt((short) (value - 1));
                                return new BinaryPredicateOperator(LE, canonizedPredicate.getChild(0), newRight);
                            } else if (right.getType().isInt()) {
                                int value = ((ConstantOperator) right).getInt();
                                if (value == Integer.MIN_VALUE) {
                                    if (left.getType().isInt()) {
                                        return ConstantOperator.createBoolean(false);
                                    } else {
                                        break;
                                    }
                                }
                                ConstantOperator newRight = ConstantOperator.createInt(value - 1);
                                return new BinaryPredicateOperator(LE, canonizedPredicate.getChild(0), newRight);
                            } else {
                                // for type BIGINT
                                long value = ((ConstantOperator) right).getBigint();
                                if (value == Long.MIN_VALUE) {
                                    if (left.getType().isBigint()) {
                                        return ConstantOperator.createBoolean(false);
                                    } else {
                                        break;
                                    }
                                }
                                ConstantOperator newRight = ConstantOperator.createBigint(value - 1);
                                return new BinaryPredicateOperator(LE, canonizedPredicate.getChild(0), newRight);
                            }
                        }
                        break;
                    case GT:
                        // convert col(int) > 3 to col(int) >= 4
                        right = canonizedPredicate.getChild(1);
                        if (right.isConstantRef() && right.getType().isIntegerType()) {
                            ConstantOperator constantOperator = (ConstantOperator) right;
                            if (constantOperator.isNull()) {
                                break;
                            }
                            if (right.getType().isTinyint()) {
                                byte value = ((ConstantOperator) right).getTinyInt();
                                if (value == Byte.MAX_VALUE) {
                                    if (left.getType().isTinyint()) {
                                        // left > Byte.MAX_VALUE is always false, so just return false
                                        return ConstantOperator.createBoolean(false);
                                    } else {
                                        break;
                                    }
                                }
                                ConstantOperator newRight = ConstantOperator.createTinyInt((byte) (value + 1));
                                return new BinaryPredicateOperator(GE, canonizedPredicate.getChild(0), newRight);
                            } else if (right.getType().isSmallint()) {
                                short value = ((ConstantOperator) right).getSmallint();
                                if (value == Short.MAX_VALUE) {
                                    if (left.getType().isSmallint()) {
                                        // left > Short.MAX_VALUE is always false, so just return false
                                        return ConstantOperator.createBoolean(false);
                                    } else {
                                        break;
                                    }
                                }
                                ConstantOperator newRight = ConstantOperator.createSmallInt((short) (value + 1));
                                return new BinaryPredicateOperator(GE, canonizedPredicate.getChild(0), newRight);
                            } else if (right.getType().isInt()) {
                                int value = ((ConstantOperator) right).getInt();
                                if (value == Integer.MAX_VALUE) {
                                    if (left.getType().isInt()) {
                                        // left > Integer.MAX_VALUE is always false, so just return false
                                        return ConstantOperator.createBoolean(false);
                                    } else {
                                        break;
                                    }
                                }
                                ConstantOperator newRight = ConstantOperator.createInt(value + 1);
                                return new BinaryPredicateOperator(GE, canonizedPredicate.getChild(0), newRight);
                            } else {
                                // for type BIGINT
                                long value = ((ConstantOperator) right).getBigint();
                                if (value == Long.MAX_VALUE) {
                                    if (left.getType().isBigint()) {
                                        // left > Integer.MAX_VALUE is always false, so just return false
                                        return ConstantOperator.createBoolean(false);
                                    } else {
                                        break;
                                    }
                                }
                                ConstantOperator newRight = ConstantOperator.createBigint(value + 1);
                                return new BinaryPredicateOperator(GE, canonizedPredicate.getChild(0), newRight);
                            }
                        }
                        break;
                    case EQ:
                        // convert col = 3 to 3 <= col and col <= 3
                        if (canonizedPredicate.getChild(0) instanceof ColumnRefOperator
                                && canonizedPredicate.getChild(1) instanceof ConstantOperator) {
                            ColumnRefOperator columnRef = (ColumnRefOperator) canonizedPredicate.getChild(0);
                            ConstantOperator constant = (ConstantOperator) canonizedPredicate.getChild(1);

                            BinaryPredicateOperator gePart = new BinaryPredicateOperator(GE, columnRef, constant);
                            BinaryPredicateOperator lePart = new BinaryPredicateOperator(LE, columnRef, constant);
                            return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, gePart, lePart);
                        }
                        break;
                    case NE:
                        // convert col != 3 to 4 <= col and col <= 2
                        if (canonizedPredicate.getChild(0) instanceof ColumnRefOperator
                                && canonizedPredicate.getChild(1) instanceof ConstantOperator) {
                            ColumnRefOperator columnRef = (ColumnRefOperator) canonizedPredicate.getChild(0);
                            ConstantOperator constant = (ConstantOperator) canonizedPredicate.getChild(1);
                            BinaryPredicateOperator gePart = new BinaryPredicateOperator(GT, columnRef, constant);
                            ScalarOperator canonizedGePart = canonizeNode(gePart);
                            BinaryPredicateOperator lePart = new BinaryPredicateOperator(LT, columnRef, constant);
                            ScalarOperator canonizedLePart = canonizeNode(lePart);
                            return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                                    canonizedGePart, canonizedLePart);
                        }
                        break;
                    default:
                        break;
                }
                return canonizedPredicate;
            case IN:
                InPredicateOperator inPredicateOperator = (InPredicateOperator) predicate;
                if (!inPredicateOperator.isNotIn()) {
                    return inPredicateOperator;
                }
                List<ScalarOperator> inList = inPredicateOperator.getChildren().stream().skip(1).collect(Collectors.toList());
                ScalarOperator first = inPredicateOperator.getChild(0);
                List<ScalarOperator> orItems = Lists.newArrayList();
                for (ScalarOperator item : inList) {
                    BinaryPredicateOperator equal = new BinaryPredicateOperator(EQ, first, item);
                    orItems.add(equal);
                }
                CompoundPredicateOperator orPredicate =
                        new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, orItems);
                return orPredicate;
            default:
                return predicate;
        }
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
        if (child == null) {
            return false;
        }
        Operator childOp = child.getOp();
        if (!(childOp instanceof LogicalScanOperator)
                && !(childOp instanceof LogicalProjectOperator)
                && !(childOp instanceof LogicalFilterOperator)
                && !(childOp instanceof LogicalJoinOperator)) {
            return false;
        }
        for (OptExpression input : root.getInputs()) {
            if (!isLogicalSPJ(input)) {
                return false;
            }
        }
        return true;
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

    // split predicate into two parts: equal columns predicates and residual predicates
    // the result pair's left is equal columns predicates, right is residual predicates
    public static Pair<ScalarOperator, ScalarOperator> splitPredicate(ScalarOperator predicate) {
        List<ScalarOperator> predicateConjuncts = Utils.extractConjuncts(predicate);
        List<ScalarOperator> columnEqualityPredicates = Lists.newArrayList();
        List<ScalarOperator> residualPredicates = Lists.newArrayList();
        for (ScalarOperator scalarOperator : predicateConjuncts) {
            if (scalarOperator instanceof BinaryPredicateOperator
                    && ((BinaryPredicateOperator) scalarOperator).getBinaryType().isEqual()) {
                ScalarOperator leftChild = scalarOperator.getChild(0);
                ScalarOperator rightChild = scalarOperator.getChild(1);
                if (leftChild.isColumnRef() && rightChild.isColumnRef()) {
                    columnEqualityPredicates.add(scalarOperator);
                } else {
                    residualPredicates.add(scalarOperator);
                }
            } else {
                residualPredicates.add(scalarOperator);
            }
        }
        return Pair.create(
                Utils.compoundAnd(columnEqualityPredicates),
                Utils.compoundAnd(residualPredicates));
    }

    // split predicate into three parts: equal columns predicates, range predicates, and residual predicates
    // result triple:
    //    left: pe
    //    middle: pr
    //    right: pu
    public static Triple<ScalarOperator, ScalarOperator, ScalarOperator> splitPredicateToTriple(ScalarOperator predicate) {
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
        return Triple.of(
                Utils.compoundAnd(columnEqualityPredicates),
                Utils.compoundAnd(rangePredicates),
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
}
