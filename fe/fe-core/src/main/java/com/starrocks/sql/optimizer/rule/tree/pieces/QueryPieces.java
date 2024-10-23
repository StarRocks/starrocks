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

package com.starrocks.sql.optimizer.rule.tree.pieces;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class QueryPieces {
    String algebra;

    // raw operator
    Operator op;

    // filter used columns
    List<ColumnRefOperator> filterUsedRefs;

    List<QueryPieces> inputs = Lists.newArrayList();

    public static Optional<QueryPieces> of(Operator op, List<ColumnRefOperator> filterUsedRefs, QueryPieces... input) {
        QueryPieces p = new QueryPieces();
        p.op = op;
        p.inputs.addAll(Arrays.asList(input));
        p.filterUsedRefs = filterUsedRefs;
        return Optional.of(p);
    }

    @Override
    public String toString() {
        return "QueryPieces{" +
                "algebra='" + algebra + '\'' +
                ", op=" + op +
                '}';
    }
}

class QueryPiecesPlan {
    ScalarOperatorConverter columnRefConverter;
    int planId;
    QueryPieces root;

    public QueryPiecesPlan(int planId, ScalarOperatorConverter converter) {
        this.planId = planId;
        this.columnRefConverter = converter;
    }

    public String planIdentifier() {
        return root.algebra;
    }

    public OptExpression toOptExpression() {
        return toOptExpressionImpl(root);
    }

    public OptExpression toOptExpressionImpl(QueryPieces pieces) {
        List<OptExpression> inputs = Lists.newArrayList();
        for (QueryPieces input : pieces.inputs) {
            inputs.add(toOptExpressionImpl(input));
        }
        return OptExpression.create(pieces.op, inputs);
    }
}



