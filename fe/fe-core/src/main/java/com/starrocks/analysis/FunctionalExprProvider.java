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

package com.starrocks.analysis;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ConnectContext;
import org.apache.parquet.Strings;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * Provide the predicate chain and comparator chain
 * which would be used in `List<U>.stream().filter(predicateChain).sorted(comparatorChain).skip().limit()`
 * with a group of pre-defined ColumnValueSuppliers.
 */
public abstract class FunctionalExprProvider<U> {

    /**
     * A column's `getColumnName`, `getColumnType` are offered just after initialized,
     * while the `getColumnValue` is invoked with an instance of `<U>` during the streaming process.
     */
    protected interface ColumnValueSupplier<E> {
        String getColumnName();

        PrimitiveType getColumnType();

        <T extends Comparable<? super T>> T getColumnValue(E row);
    }

    /**
     * BiPredicate used to filter `LONG`, `DATETIME(will be converted from STRING to LONG)`
     * type values during the streaming process
     */
    private static final BiPredicate<Long, Map.Entry<BinaryPredicate.Operator, Long>> LONG_BI_FILTER_FUNC =
            (num, entry) -> {
                BinaryPredicate.Operator op = entry.getKey();
                Long rValue = entry.getValue();
                switch (op) {
                    case EQ:
                        return num.equals(rValue);
                    case NE:
                        return !num.equals(rValue);
                    case LE:
                        return num.longValue() <= rValue.longValue();
                    case GE:
                        return num.longValue() >= rValue.longValue();
                    case LT:
                        return num.longValue() < rValue.longValue();
                    case GT:
                        return num.longValue() > rValue.longValue();
                    default:
                        return false;
                }
            };

    /**
     * BiPredicate used to filter `STRING` type values during the streaming process
     */
    private static final BiPredicate<String, Map.Entry<BinaryPredicate.Operator, StringLiteral>> STRING_BI_FILTER_FUNC =
            (str, entry) -> {
                BinaryPredicate.Operator op = entry.getKey();
                String rValue = entry.getValue().getStringValue();
                switch (op) {
                    case EQ:
                        return str.equalsIgnoreCase(rValue);
                    case NE:
                        return !str.equalsIgnoreCase(rValue);
                    default:
                        return false;
                }
            };

    /**
     * BiPredicate used to filter `STRING` within a `in (xxx, xxx, xx)` clause during the streaming process
     */
    private static final BiPredicate<String, List<StringLiteral>> STRING_IN_FILTER_FUNC = (str, stringLiterals) -> {
        return stringLiterals.parallelStream().anyMatch(s -> str.equalsIgnoreCase(s.getValue()));
    };

    /**
     * BiPredicate used to filter `STRING` within a `not in (xxx, xxx, xx)` clause during the streaming process
     */
    private static final BiPredicate<String, List<StringLiteral>> STRING_NOT_IN_FILTER_FUNC =
            STRING_IN_FILTER_FUNC.negate();

    /**
     * BiPredicate used to filter `STRING` within a `like '%xxx%'` clause during the streaming process
     */
    private static final BiPredicate<String, StringLiteral> LIKE_FILTER_FUNC = (str, stringLiteral) -> {
        String likeRegx =
                stringLiteral.getValue().replace(".", "\\.").replace("?", ".").replace("%", ".*").toLowerCase();
        return str.toLowerCase().matches(likeRegx);
    };

    /**
     * Predicate chain that is used in `List.stream().filter()` to filter instances(`<U>`).
     * Generated and connected after analyzing the `where` clause expr.
     */
    private Predicate<U> predicateChain;

    /**
     * Comparator chain that is used in `List.stream().sorted()` to order the instances(`<U>`) with a given List<OrderByElement>.
     * Generated and connected after analyzing the `order by` exprs.
     */
    private Comparator<U> orderComparator;
    private long offset;
    private long limit;
    private ConnectContext connCtx;

    /**
     * Subclasses should offer a list of supported ColumnValueSupplier for `where`, `order by` clause.
     */
    protected abstract ImmutableList<ColumnValueSupplier<U>> delegateWhereSuppliers();

    /**
     * This function will be invoked at the end of the predicate chain.
     */
    protected abstract boolean delegatePostRowFilter(ConnectContext ctx, U row);

    @Deprecated
    public void analyze(Analyzer analyzer, Expr predicate, List<OrderByElement> orderByElements, LimitElement limit)
            throws AnalysisException {
        this.connCtx = analyzer.getContext();
        // analyze where clase
        predicateChain = null;
        analyzeWhere(predicate);
        connectPredicate(row -> delegatePostRowFilter(connCtx, row));
        // analyze order by
        analyzeOrder(orderByElements);
        // analyze limit
        analyzeLimit(limit);
    }

    public void analyze(ConnectContext connCtx, Expr predicate, List<OrderByElement> orderByElements, LimitElement limit)
            throws AnalysisException {
        this.connCtx = connCtx;
        // analyze where clase
        predicateChain = null;
        analyzeWhere(predicate);
        connectPredicate(row -> delegatePostRowFilter(connCtx, row));
        // analyze order by
        analyzeOrder(orderByElements);
        // analyze limit
        analyzeLimit(limit);
    }

    /**
     * Generated and connected predicates that is used in `List.stream().filter()` to filter instances(`<U>`).
     */
    private void analyzeWhere(Expr predicate) throws AnalysisException {
        if (null == predicate) {
            return;
        }
        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate cp = (CompoundPredicate) predicate;
            if (cp.getOp() != CompoundPredicate.Operator.AND) {
                throw new AnalysisException("Only `AND` operator is allowed in compound predicates.");
            }
            analyzeWhere(cp.getChild(0));
            analyzeWhere(cp.getChild(1));
            return;
        }

        if (!(predicate.getChild(0) instanceof SlotRef)) {
            throw new AnalysisException(
                    String.format("`%s` is not allowed in `where` clause.", predicate.getChild(0).toSql()));
        }
        String leftKey = ((SlotRef) predicate.getChild(0)).getColumnName();
        Optional<ColumnValueSupplier<U>> colValSupplier =
                delegateWhereSuppliers().parallelStream().filter(s -> s.getColumnName().equalsIgnoreCase(leftKey))
                        .findAny();
        if (!colValSupplier.isPresent()) {
            throw new AnalysisException(String.format("Column `%s` is not allowed in `where` clause", leftKey));
        }
        final String operatorNotSupported = "Operator `%s` is not allowed in `where` clause for column `%s`.";
        final String wontRValueType = "Wrong RValue's type in `where` clause for column `%s`";
        if (predicate instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) predicate;
            if (predicate.getChild(1) instanceof StringLiteral) {
                if (PrimitiveType.DATETIME == colValSupplier.get().getColumnType()) {
                    // handle predicates like `where time >= '2020-01-01 00:00:01'`
                    StringLiteral stringLiteral = (StringLiteral) binaryPredicate.getChild(1);
                    Long ts = 0L;
                    if (!Strings.isNullOrEmpty(stringLiteral.getStringValue())) {
                        ts = TimeUtils.timeStringToLong(stringLiteral.getStringValue());
                    }
                    connectPredicate(newFilterPredicate(colValSupplier.get(),
                            Maps.immutableEntry(binaryPredicate.getOp(), ts),
                            LONG_BI_FILTER_FUNC));
                    return;
                }
                if (PrimitiveType.VARCHAR == colValSupplier.get().getColumnType()) {
                    // handle predicates like `where col = 'starrocks'`
                    if (binaryPredicate.getOp() != BinaryPredicate.Operator.EQ &&
                            binaryPredicate.getOp() != BinaryPredicate.Operator.NE) {
                        throw new AnalysisException(
                                String.format(operatorNotSupported, binaryPredicate.getOp().toString(), leftKey));
                    }
                    connectPredicate(newFilterPredicate(colValSupplier.get(),
                            Maps.immutableEntry(binaryPredicate.getOp(), (StringLiteral) binaryPredicate.getChild(1)),
                            STRING_BI_FILTER_FUNC));
                    return;
                }
                throw new AnalysisException(String.format(wontRValueType, leftKey));
            }
            if (predicate.getChild(1) instanceof IntLiteral) {
                if (PrimitiveType.BIGINT != colValSupplier.get().getColumnType()) {
                    throw new AnalysisException(String.format(wontRValueType, leftKey));
                }
                // handle predicates like `where col >= 2`
                IntLiteral intLiteral = (IntLiteral) binaryPredicate.getChild(1);
                connectPredicate(newFilterPredicate(colValSupplier.get(),
                        Maps.immutableEntry(binaryPredicate.getOp(), intLiteral.getLongValue()),
                        LONG_BI_FILTER_FUNC));
                return;
            }
            throw new AnalysisException(
                    String.format("RValue should be `string` or `integer` in `where` clause for column `%s`", leftKey));
        }
        if (predicate instanceof LikePredicate) {
            LikePredicate likePredicate = (LikePredicate) predicate;
            if (likePredicate.getOp() != LikePredicate.Operator.LIKE) {
                throw new AnalysisException(
                        String.format(operatorNotSupported, likePredicate.getOp().toString(), leftKey));
            }
            // handle predicates like `where col like '%xx%'`
            connectPredicate(newFilterPredicate(colValSupplier.get(), (StringLiteral) likePredicate.getChild(1),
                    LIKE_FILTER_FUNC));
            return;
        }
        if (predicate instanceof InPredicate) {
            InPredicate inPredicate = (InPredicate) predicate;
            List<StringLiteral> stringLiterals = Lists.newArrayList();
            for (int i = 1; i < inPredicate.getChildren().size(); ++i) {
                if (!(inPredicate.getChildren().get(i) instanceof StringLiteral)) {
                    throw new AnalysisException(String.format(
                            "Only `string` values are allowed in the predicate `in()` or `not in()` for column `%s`",
                            leftKey));
                }
                stringLiterals.add((StringLiteral) inPredicate.getChildren().get(i));
            }
            // handle predicates like `where col in ('xx', 'xx')`
            connectPredicate(newFilterPredicate(colValSupplier.get(), stringLiterals,
                    inPredicate.isNotIn() ? STRING_NOT_IN_FILTER_FUNC : STRING_IN_FILTER_FUNC));
            return;
        }
        throw new AnalysisException(
                String.format("`%s` is not allowed in `where` clause for column `%s`.", predicate.toSql(), leftKey));
    }

    /**
     * Generated and connected comparators that is used in `List.stream().sorted()`
     * to order the instances(`<U>`) with a given List<OrderByElement>.
     */
    public <T extends Comparable<? super T>> void analyzeOrder(List<OrderByElement> orderByElements)
            throws AnalysisException {
        if (null == orderByElements || orderByElements.isEmpty()) {
            return;
        }
        orderComparator = null;
        for (OrderByElement orderByElement : orderByElements) {
            if (!(orderByElement.getExpr() instanceof SlotRef)) {
                throw new AnalysisException(
                        String.format("`%s` is not allowed in `order by` clause.", orderByElement.toSql()));
            }
            SlotRef slotRef = (SlotRef) orderByElement.getExpr();
            Optional<ColumnValueSupplier<U>> colValSupplier = delegateWhereSuppliers().parallelStream()
                    .filter(c -> c.getColumnName().equalsIgnoreCase(slotRef.getColumnName())).findAny();
            if (!colValSupplier.isPresent()) {
                throw new AnalysisException(
                        String.format("Column `%s` is not allowed in `order by` clause", slotRef.getColumnName()));
            }
            // build comparator chain
            if (null == orderComparator) {
                orderComparator = Comparator.comparing(j -> colValSupplier.get().getColumnValue(j),
                        Comparator.nullsFirst(Comparator.<T>naturalOrder()));
            } else {
                orderComparator = orderComparator.thenComparing(j -> colValSupplier.get().getColumnValue(j),
                        Comparator.nullsFirst(Comparator.<T>naturalOrder()));
            }
            if (!orderByElement.getIsAsc()) {
                orderComparator = orderComparator.reversed();
            }
        }
    }

    public void analyzeLimit(LimitElement limit) {
        if (null == limit) {
            this.offset = 0;
            this.limit = Long.MAX_VALUE;
            return;
        }
        this.limit = limit.getLimit();
        this.offset = limit.getOffset();
    }

    public Comparator<U> getOrderComparator() {
        if (null == orderComparator) {
            return (o1, o2) -> 0;
        }
        return orderComparator;
    }

    public Predicate<U> getPredicateChain() {
        if (null == predicateChain) {
            return row -> delegatePostRowFilter(connCtx, row);
        }
        return predicateChain;
    }

    public long getSkipCount() {
        return offset >= 0 ? offset : 0;
    }

    public long getLimitCount() {
        return limit >= 0 ? limit : Long.MAX_VALUE;
    }

    private void connectPredicate(Predicate<U> filter) {
        if (null == predicateChain) {
            predicateChain = filter;
            return;
        }
        predicateChain = predicateChain.and(filter);
    }

    private <T, E> Predicate<U> newFilterPredicate(ColumnValueSupplier<U> colValSupplier, E comparedValues,
                                                   BiPredicate<T, E> filterFunc) {
        return row -> {
            T value = colValSupplier.getColumnValue(row);
            if (null == value) {
                return false;
            }
            return filterFunc.test(value, comparedValues);
        };
    }
}
