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

package com.starrocks.sql.automv.pn;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.automv.boolalgebra.Modifier;
import com.starrocks.sql.automv.column.ColumnRefToIdConverter;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArraySliceOperator;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.CloneOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.MapOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReverseTranscriptase extends ScalarOperatorVisitor<Op, List<Op>> {

    final ColumnRefToIdConverter idConverter;

    private ReverseTranscriptase(ColumnRefToIdConverter idConverter) {
        this.idConverter = idConverter;
    }

    public static Op tenured(Op op, Map<Integer, GenericColumn> substMap) {
        op.getSymTab().getEntries().forEach(e -> {
            if (e.isSymbol()) {
                Symbol sym = (Symbol) e;
                if (!sym.isTenured()) {
                    // EXISTS/IN subquery can generate dangling variables in for LogicalApplyOperator
                    GenericColumn column = substMap.getOrDefault(sym.getId(), GenericColumn.derived(Val.NULL_VAL));
                    sym.tenured(column);
                }
            } else {
                SymbolSet symbolSet = (SymbolSet) e;
                symbolSet.getEntries().forEach(sym -> {
                    if (!sym.isTenured()) {
                        sym.tenured(Objects.requireNonNull(substMap.get(sym.getId())));
                    }
                });
            }
        });
        return op;
    }

    public static Op reverseTranscript(ScalarOperator scalarOperator, ColumnRefToIdConverter idConverter,
                                       Map<Integer, GenericColumn> substMap) {
        return tenured(new ReverseTranscriptase(idConverter).reverseTranscriptImpl(scalarOperator).standardize(),
                substMap);
    }

    @Override
    public Op visit(ScalarOperator scalarOperator, List<Op> args) {
        throw new UnsupportedOperationException(
                String.format("ScalarOperator of type '%s' is not supported", scalarOperator.getOpType()));
    }

    @Override
    public Op visitVariableReference(ColumnRefOperator variable, List<Op> args) {
        return Op.var(variable.getType(), idConverter.getId(variable));
    }

    @Override
    public Op visitConstant(ConstantOperator literal, List<Op> args) {
        return Op.val(literal);
    }

    @Override
    public Op visitCloneOperator(CloneOperator operator, List<Op> args) {
        Preconditions.checkArgument(args.get(0).isVar());
        return args.get(0);
    }

    @Override
    public Op visitBinaryPredicate(BinaryPredicateOperator predicate, List<Op> args) {
        Op a = args.get(0);
        Op b = args.get(1);

        switch (predicate.getBinaryType()) {
            case EQ: {
                return Op.eqOrInRange(a, b);
            }
            case EQ_FOR_NULL: {
                return Op.nullSafeEq(args);
            }
            case NE: {
                return Op.ne(a, b);
            }
            case LE: {
                return Op.leOrInRange(a, b);
            }
            case LT: {
                return Op.ltOrInRange(a, b);
            }
            case GE: {
                return Op.leOrInRange(b, a);
            }
            case GT: {
                return Op.ltOrInRange(b, a);
            }
        }
        Preconditions.checkArgument(false, "Never reach here");
        return null;
    }

    @Override
    public Op visitBetweenPredicate(BetweenPredicateOperator predicate, List<Op> args) {
        Op v = args.get(0);
        Op a = args.get(1);
        Op b = args.get(2);

        Op op = Op.or(Op.leOrInRange(a, v), Op.leOrInRange(v, b));
        return predicate.isNotBetween() ? op.not() : op;
    }

    @Override
    public Op visitCaseWhenOperator(CaseWhenOperator operator, List<Op> args) {
        List<Op> newArgs = args;
        if (operator.hasCase()) {
            Op caseOp = args.get(0);
            int numWhens = (args.size() - 1) / 2;
            newArgs = Lists.newArrayListWithCapacity(numWhens + 1);
            for (int i = 0; i < numWhens; ++i) {
                int idx = 1 + i * 2;
                Op whenOp = args.get(idx);
                Op thenOp = args.get(idx + 1);
                newArgs.add(Op.eqOrInRange(caseOp, whenOp));
                newArgs.add(thenOp);
            }
            if (operator.hasElse()) {
                newArgs.add(args.get(args.size() - 1));
            }
        }

        if (!operator.hasElse()) {
            newArgs.add(Val.NULL_VAL);
        }

        return Op.apply(operator.getType(), BuiltinKind.CASE, true, newArgs);
    }

    @Override
    public Op visitInPredicate(InPredicateOperator predicate, List<Op> args) {
        Op item = args.get(0);
        List<Op> values = args.subList(1, args.size());
        Op op = Op.in(item, values);
        return predicate.isNotIn() ? op.not() : op;
    }

    private Op translateDistinctArgs(List<Op> args) {
        return translateDistinctArgsImpl(args, 0);
    }

    private Op translateDistinctArgsImpl(List<Op> args, int idx) {
        if (idx == args.size() - 1) {
            return args.get(idx);
        } else {
            Op a = args.get(idx);
            Op b = translateDistinctArgsImpl(args, idx + 1);
            Op aIsNull = Op.modify(Modifier.M_IS_NULL, a);
            List<Op> ifArgs = ImmutableList.of(aIsNull, Val.NULL_VAL, b);
            return Op.apply(b.getType(), FunctionKind.of(FunctionSet.IF), true, ifArgs);
        }
    }

    @Override
    public Op visitCall(CallOperator call, List<Op> args) {
        if (call.isCountStar()) {
            Op const1 = Op.val(ConstantOperator.createInt(1));
            return Op.apply(call.getType(), FunctionKind.of(call.getFnName()), true, Collections.singletonList(const1));
        } else if (call.isDistinct()) {
            Preconditions.checkArgument(call.getFunction() instanceof AggregateFunction);
            Preconditions.checkArgument(Op.DISTINCT_FUNCTIONS.contains(call.getFnName()));
            if (args.size() > 1) {
                args = ImmutableList.of(translateDistinctArgs(args));
            }
            Op distinctOp = Op.distinct(call.getType(), args);
            return Op.apply(call.getType(), FunctionKind.of(call.getFnName()), true,
                    Collections.singletonList(distinctOp));
        } else {
            return Op.apply(call.getType(), FunctionKind.of(call.getFnName()), true, args);
        }
    }

    @Override
    public Op visitArray(ArrayOperator array, List<Op> args) {
        return Op.apply(array.getType(), BuiltinKind.ARRAY, true, args);
    }

    @Override
    public Op visitArraySlice(ArraySliceOperator array, List<Op> args) {
        return Op.apply(array.getType(), BuiltinKind.ARRAY_SLICE, true, args);
    }

    @Override
    public Op visitLikePredicateOperator(LikePredicateOperator predicate, List<Op> args) {
        BuiltinKind builtin = predicate.isRegexp() ? BuiltinKind.REGEXP : BuiltinKind.LIKE;
        return Op.apply(predicate.getType(), builtin, true, args);
    }

    @Override
    public Op visitCollectionElement(CollectionElementOperator collectionElementOp, List<Op> args) {
        return Op.apply(collectionElementOp.getType(), BuiltinKind.COLLECTION_ELEMENT, true, args);
    }

    @Override
    public Op visitSubfield(SubfieldOperator subfieldOperator, List<Op> args) {
        List<Op> newArgs = Stream.concat(
                        args.stream(),
                        subfieldOperator.getFieldNames()
                                .stream()
                                .map(name -> Apply.val(ConstantOperator.createVarchar(name))))
                .collect(Collectors.toList());
        return Op.apply(subfieldOperator.getType(), BuiltinKind.SUBFIELD, true, newArgs);
    }

    @Override
    public Op visitMap(MapOperator map, List<Op> args) {
        return Op.apply(map.getType(), BuiltinKind.MAP, true, args);
    }

    @Override
    public Op visitLambdaFunctionOperator(LambdaFunctionOperator operator, List<Op> args) {
        List<Integer> lambdaArgs =
                operator.getRefColumns().stream().map(idConverter::getId).collect(Collectors.toList());
        return Op.lambda(args.get(0), lambdaArgs);
    }

    @Override
    public Op visitIsNullPredicate(IsNullPredicateOperator predicate, List<Op> args) {
        Op arg = args.get(0);
        return predicate.isNotNull() ? arg.toIsNotNull() : arg.toIsNull();
    }

    @Override
    public Op visitCastOperator(CastOperator operator, List<Op> args) {
        // Automatically interpolated cast expressions' types may change as type conversion rule changes
        // in later version, so it is unstable, we just drop them; The user-written cast expr are retained.
        if (operator.isImplicit()) {
            return args.get(0);
        } else {
            return Op.apply(operator.getType(), BuiltinKind.CAST, true, args);
        }
    }

    @Override
    public Op visitCompoundPredicate(CompoundPredicateOperator predicate, List<Op> args) {
        if (predicate.isNot()) {
            return args.get(0).not();
        } else if (predicate.isAnd()) {
            return Op.and(args);
        } else {
            return Op.or(args);
        }
    }

    private Op reverseTranscriptImpl(ScalarOperator scalarOperator) {
        List<Op> args =
                scalarOperator.getChildren().stream().map(this::reverseTranscriptImpl).collect(Collectors.toList());
        return scalarOperator.accept(this, args);
    }
}
