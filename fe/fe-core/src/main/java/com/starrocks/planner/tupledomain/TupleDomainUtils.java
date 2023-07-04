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

package com.starrocks.planner.tupledomain;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.starrocks.analysis.BetweenPredicate;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

public class TupleDomainUtils {
    private TupleDomainUtils() {
    }

    public static <C extends Comparable<?>> boolean isSingleton(Range<C> range) {
        return range.hasLowerBound() && range.lowerBoundType() == BoundType.CLOSED
                && range.hasUpperBound() && range.upperBoundType() == BoundType.CLOSED
                && range.lowerEndpoint().equals(range.upperEndpoint());
    }

    private static Optional<Column> getColumn(Expr expr) {
        if (!(expr.getChild(0) instanceof SlotRef))
            return Optional.empty();
        return Optional.of(((SlotRef) expr.getChild(0)).getDesc().getColumn());
    }

    private static Optional<ColumnValue> getColumnValue(Column column, Expr expr) {
        if (!(expr instanceof LiteralExpr)) {
            return Optional.empty();
        }
        return ColumnValue.create(column, (LiteralExpr) expr);
    }

    public static TupleDomainUnion extractExpr(Expr expr) {
        if (expr instanceof BinaryPredicate)
            return extractBinaryPredicate((BinaryPredicate) expr);

        if (expr instanceof BetweenPredicate)
            return extractBetweenPredicate((BetweenPredicate) expr);

        if (expr instanceof LikePredicate)
            return extractLikePredicate((LikePredicate) expr);

        if (expr instanceof InPredicate)
            return extractInPredicate((InPredicate) expr);

        if (expr instanceof CompoundPredicate) {
            CompoundPredicate comp = (CompoundPredicate) expr;
            switch (comp.getOp()) {
                case AND:
                    return extractConjuncts(comp.getChildren());
                case OR:
                    return extractDisjuncts(comp.getChildren());
                default:
                    return TupleDomainUnion.all();
            }
        }

        return TupleDomainUnion.all();
    }

    public static TupleDomainUnion extractBinaryPredicate(BinaryPredicate predicate) {
        Optional<Column> columnOptional = getColumn(predicate);
        if (!columnOptional.isPresent())
            return TupleDomainUnion.all();
        Column column = columnOptional.get();

        Optional<ColumnValue> valueOptional = getColumnValue(column, predicate.getChild(1));
        if (!valueOptional.isPresent())
            return TupleDomainUnion.all();
        ColumnValue value = valueOptional.get();

        ColumnDomain columnDomain;
        switch (predicate.getOp()) {
            case EQ:
                columnDomain = ColumnDomain.equalTo(value);
                break;
            case LE:
                columnDomain = ColumnDomain.lessThanOrEqualTo(value);
                break;
            case GE:
                columnDomain = ColumnDomain.greaterThanOrEqualTo(value);
                break;
            case LT:
                columnDomain = ColumnDomain.lessThan(value);
                break;
            case GT:
                columnDomain = ColumnDomain.greaterThan(value);
                break;
            default:
                return TupleDomainUnion.all();
        }

        Map<String, ColumnDomain> columnDomains = new HashMap<>();
        columnDomains.put(column.getName(), columnDomain);
        return TupleDomainUnion.withColumnDomains(columnDomains);
    }

    public static TupleDomainUnion extractLikePredicate(LikePredicate predicate) {
        return TupleDomainUnion.all();
    }

    public static TupleDomainUnion extractInPredicate(InPredicate predicate) {
        if (predicate.isNotIn())
            return TupleDomainUnion.all();

        Optional<Column> columnOptional = getColumn(predicate);
        if (!columnOptional.isPresent())
            return TupleDomainUnion.all();
        Column column = columnOptional.get();

        SortedSet<ColumnValue> values = new TreeSet<>();
        for (Expr inElem : predicate.getListChildren())
        {
            Optional<ColumnValue> valueOptional = getColumnValue(column, inElem);
            if (!valueOptional.isPresent()) {
                return TupleDomainUnion.all();
            }
            values.add(valueOptional.get());
        }

        Map<String, ColumnDomain> columnDomains = new HashMap<>();
        columnDomains.put(column.getName(), ColumnDomain.in(values));
        return TupleDomainUnion.withColumnDomains(columnDomains);
    }

    public static TupleDomainUnion extractBetweenPredicate(BetweenPredicate predicate) {
        if (predicate.isNotBetween())
            return TupleDomainUnion.all();

        Optional<Column> columnOptional = getColumn(predicate);
        if (!columnOptional.isPresent())
            return TupleDomainUnion.all();
        Column column = columnOptional.get();

        Optional<ColumnValue> lowValueOptional = getColumnValue(column, predicate.getChild(1));
        if (!lowValueOptional.isPresent())
            return TupleDomainUnion.all();
        ColumnValue lowValue = lowValueOptional.get();

        Optional<ColumnValue> highValueOptional = getColumnValue(column, predicate.getChild(2));
        if (!highValueOptional.isPresent())
            return TupleDomainUnion.all();
        ColumnValue highValue = highValueOptional.get();

        Map<String, ColumnDomain> columnDomains = new HashMap<>();
        columnDomains.put(column.getName(), ColumnDomain.between(lowValue, highValue));
        return TupleDomainUnion.withColumnDomains(columnDomains);
    }

    public static TupleDomainUnion extractConjuncts(List<Expr> conjuncts) {
        TupleDomainUnion intersection = TupleDomainUnion.all();
        for (Expr expr : conjuncts) {
            TupleDomainUnion exprDomain = extractExpr(expr);
            if (exprDomain.isNone())
                return TupleDomainUnion.none();
            intersection = intersection.intersect(exprDomain);
        }
        return intersection;
    }

    public static TupleDomainUnion extractDisjuncts(List<Expr> disjuncts) {
        TupleDomainUnion union = TupleDomainUnion.none();
        for (Expr expr : disjuncts) {
            TupleDomainUnion exprDomain = extractExpr(expr);
            if (exprDomain.isAll())
                return TupleDomainUnion.all();
            union = union.union(exprDomain);
        }
        return union;
    }

}
