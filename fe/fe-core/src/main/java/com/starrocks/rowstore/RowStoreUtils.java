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
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class RowStoreUtils {

    public static Optional<List<List<LiteralExpr>>> extractPointsLiteral(List<Expr> conjuncts,
                                                                         List<String> keyColumns) {
        Set<String> keyColumnSet = ImmutableSet.copyOf(keyColumns);
        Map<String, List<LiteralExpr>> keyToValues = new HashMap<>();
        for (Expr expr : conjuncts) {
            // for simplify binary equals expr (#38582)
            // pk_col = true will be transformed pk_col
            if (expr instanceof SlotRef && ((SlotRef) expr).getDesc() != null) {
                keyToValues.put(((SlotRef) expr).getDesc().getColumn().getName(),
                        ImmutableList.of(new BoolLiteral(true)));
                continue;
            }

            Expr column = expr.getChild(0);
            if (!(column instanceof SlotRef) && !(expr instanceof SlotRef)) {
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