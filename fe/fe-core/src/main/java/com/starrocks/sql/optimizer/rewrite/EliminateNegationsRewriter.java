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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator.CompoundType;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.List;

public class EliminateNegationsRewriter extends ScalarOperatorVisitor<ScalarOperator, Void> {
    private static final List<OperatorType> COMPOUND_ALLOW_LIST = ImmutableList.<OperatorType>builder()
            .add(OperatorType.BINARY)
            .add(OperatorType.IS_NULL)
            .add(OperatorType.IN)
            .add(OperatorType.CONSTANT)
            .add(OperatorType.EXISTS)
            .add(OperatorType.COMPOUND)
            .build();

    @Override
    public ScalarOperator visit(ScalarOperator node, Void context) {
        return null;
    }

    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
        return predicate.negative();
    }

    @Override
    public ScalarOperator visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
        return new IsNullPredicateOperator(!predicate.isNotNull(), predicate.getChild(0));
    }

    @Override
    public ScalarOperator visitInPredicate(InPredicateOperator predicate, Void context) {
        return new InPredicateOperator(!predicate.isNotIn(), predicate.getChildren());
    }

    @Override
    public ScalarOperator visitConstant(ConstantOperator literal, Void context) {
        if (literal.isNull()) {
            return ConstantOperator.createNull(Type.BOOLEAN);
        }

        return ConstantOperator.createBoolean(!literal.getBoolean());
    }

    @Override
    public ScalarOperator visitExistsPredicate(ExistsPredicateOperator predicate, Void context) {
        return new ExistsPredicateOperator(!predicate.isNotExists(), predicate.getChildren());
    }

    @Override
    public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
        if (predicate.isNot()) {
            return predicate.getChild(0);
        }

        if (predicate.getChildren().stream().noneMatch(c -> COMPOUND_ALLOW_LIST.contains(c.getOpType()))) {
            return null;
        }

        if (predicate.isAnd()) {
            return new CompoundPredicateOperator(CompoundType.OR,
                    new CompoundPredicateOperator(CompoundType.NOT, predicate.getChild(0)),
                    new CompoundPredicateOperator(CompoundType.NOT, predicate.getChild(1)));
        } else if (predicate.isOr()) {
            return new CompoundPredicateOperator(CompoundType.AND,
                    new CompoundPredicateOperator(CompoundType.NOT, predicate.getChild(0)),
                    new CompoundPredicateOperator(CompoundType.NOT, predicate.getChild(1)));
        }

        return null;
    }

}