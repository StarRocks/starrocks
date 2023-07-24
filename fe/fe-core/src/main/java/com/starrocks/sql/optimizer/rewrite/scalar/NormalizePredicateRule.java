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

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class NormalizePredicateRule extends BottomUpScalarOperatorRewriteRule {

    //
    // Normalize Binary Predicate
    //
    // example:
    //        Binary(=)
    //        /      \
    //    a(int)   b(column)
    //
    // After rule:
    //        Binary(=)
    //        /      \
    //  b(column)   a(int)
    //
    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate,
                                               ScalarOperatorRewriteContext context) {
        if (predicate.getChild(0).isVariable()) {
            return predicate;
        }

        if (predicate.getChild(1).isConstant()) {
            return predicate;
        }

        ScalarOperator result = predicate.commutative();
        Preconditions.checkState(!(result.getChild(0).isConstant() && result.getChild(1).isVariable()),
                "Normalized predicate error: " + result);
        return result;
    }

    //
    // Normalize Between Predicate
    // example:
    //          Between
    //        /    |    \
    //      col   "a"     "b"
    //
    // After rule:
    //                 AND
    //                /   \
    //               /     \
    //       Binary(>)      Binary(<)
    //        /    \         /    \
    //      col      "a"   col      "b"
    //
    @Override
    public ScalarOperator visitBetweenPredicate(BetweenPredicateOperator predicate,
                                                ScalarOperatorRewriteContext context) {
        if (predicate.isNotBetween()) {
            ScalarOperator lower =
                    new BinaryPredicateOperator(BinaryType.LT, predicate.getChild(0),
                            predicate.getChild(1));

            ScalarOperator upper =
                    new BinaryPredicateOperator(BinaryType.GT, predicate.getChild(0),
                            predicate.getChild(2));

            return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, lower, upper);
        } else {
            ScalarOperator lower =
                    new BinaryPredicateOperator(BinaryType.GE, predicate.getChild(0),
                            predicate.getChild(1));

            ScalarOperator upper =
                    new BinaryPredicateOperator(BinaryType.LE, predicate.getChild(0),
                            predicate.getChild(2));

            return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, lower, upper);
        }
    }

    // Remove repeat predicate
    // example:
    //           AND
    //        /        \
    //      AND         AND
    //     /   \       /  \
    // a = b   a = b  a = b  a = b
    //
    // After rule:
    //            a = b
    //
    @Override
    public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate,
                                                 ScalarOperatorRewriteContext context) {

        if (predicate.isAnd()) {
            Set<ScalarOperator> after = Sets.newLinkedHashSet();
            List<ScalarOperator> before = Utils.extractConjuncts(predicate);

            after.addAll(before);
            if (after.size() != before.size()) {
                return Utils.compoundAnd(Lists.newArrayList(after));
            }
        } else if (predicate.isOr()) {
            Set<ScalarOperator> after = Sets.newLinkedHashSet();
            List<ScalarOperator> before = Utils.extractDisjunctive(predicate);

            after.addAll(before);

            if (after.size() != before.size()) {
                return Utils.compoundOr(Lists.newArrayList(after));
            }
        }

        return predicate;
    }

    /*
     * Rewrite column ref into comparison predicate *
     * Before
     * example:
     *         IN
     *        / | \
     * left  1  a  b
     * After rule:
     * left = 1 OR left = a OR left = b
     */
    @Override
    public ScalarOperator visitInPredicate(InPredicateOperator predicate, ScalarOperatorRewriteContext context) {
        List<ScalarOperator> rhs = predicate.getChildren().subList(1, predicate.getChildren().size());
        if (predicate.isSubquery()) {
            return predicate;
        }
        if (rhs.stream().allMatch(ScalarOperator::isConstant)) {
            return predicate;
        }

        List<ScalarOperator> result = new ArrayList<>();
        ScalarOperator lhs = predicate.getChild(0);
        boolean isIn = !predicate.isNotIn();

        List<ScalarOperator> constants = predicate.getChildren().stream().skip(1).filter(ScalarOperator::isConstant)
                .collect(Collectors.toList());
        if (constants.size() == 1) {
            BinaryType op =
                    isIn ? BinaryType.EQ : BinaryType.NE;
            result.add(new BinaryPredicateOperator(op, lhs, constants.get(0)));
        } else if (!constants.isEmpty()) {
            constants.add(0, lhs);
            result.add(new InPredicateOperator(predicate.isNotIn(), constants));
        }

        predicate.getChildren().stream().skip(1).filter(ScalarOperator::isVariable).forEach(child -> {
            BinaryPredicateOperator newOp;
            if (isIn) {
                newOp = new BinaryPredicateOperator(BinaryType.EQ, lhs, child);
            } else {
                newOp = new BinaryPredicateOperator(BinaryType.NE, lhs, child);
            }
            result.add(newOp);
        });

        return isIn ? Utils.compoundOr(result) : Utils.compoundAnd(result);
    }

    // rewrite collection element to subfiled
    @Override
    public ScalarOperator visitCollectionElement(CollectionElementOperator collectionElement,
                                                 ScalarOperatorRewriteContext context) {
        if (collectionElement.getChild(0).getType().isStructType()) {
            Preconditions.checkState(collectionElement.getChild(1).isConstantRef());
            Preconditions.checkState(collectionElement.getChild(1).getType().isIntegerType());

            ConstantOperator op = collectionElement.getChild(1).cast();
            int index = 0;
            try {
                index = op.castTo(Type.INT).getInt();
            } catch (Exception e) {
                throw new SemanticException("Invalid index for struct element: " + collectionElement);
            }

            if (index > 0) {
                index = index - 1;
            } else if (index < 0) {
                index += ((StructType) collectionElement.getChild(0).getType()).getFields().size();
            } else {
                throw new SemanticException("Invalid index for struct element: " + collectionElement);
            }

            return SubfieldOperator.build(collectionElement.getChild(0),
                    collectionElement.getChild(0).getType(),
                    Lists.newArrayList(index));
        }
        return collectionElement;
    }

    /*
     * rewrite map/array is null -> map_size(map)/array_size(array) is null
     */
    @Override
    public ScalarOperator visitIsNullPredicate(IsNullPredicateOperator predicate,
                                               ScalarOperatorRewriteContext context) {
        if (predicate.getChild(0).getType().isMapType()) {
            Function fn = Expr.getBuiltinFunction(FunctionSet.MAP_SIZE,
                    new Type[] {predicate.getChild(0).getType()}, Function.CompareMode.IS_SUPERTYPE_OF);
            CallOperator call = new CallOperator(fn.functionName(), fn.getReturnType(), predicate.getChildren(), fn);
            return new IsNullPredicateOperator(predicate.isNotNull(), call);
        } else if (predicate.getChild(0).getType().isArrayType()) {
            Function fn = Expr.getBuiltinFunction(FunctionSet.ARRAY_LENGTH,
                    new Type[] {predicate.getChild(0).getType()}, Function.CompareMode.IS_SUPERTYPE_OF);
            CallOperator call = new CallOperator(fn.functionName(), fn.getReturnType(), predicate.getChildren(), fn);
            return new IsNullPredicateOperator(predicate.isNotNull(), call);
        }

        return visit(predicate, context);
    }
}
