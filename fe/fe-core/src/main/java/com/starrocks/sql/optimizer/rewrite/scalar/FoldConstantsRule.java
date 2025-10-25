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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LargeInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorEvaluator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FoldConstantsRule extends BottomUpScalarOperatorRewriteRule {
    private static final Logger LOG = LogManager.getLogger(FoldConstantsRule.class);

    private final boolean needMonotonicFunc;

    public FoldConstantsRule() {
        this(false);
    }

    public FoldConstantsRule(boolean needMonotonicFunc) {
        this.needMonotonicFunc = needMonotonicFunc;
    }

    private <T extends ScalarOperator> Optional<ScalarOperator> arrayScalarFun(
            CallOperator call, BiFunction<List<ScalarOperator>, ScalarOperator, T> cb) {
        Preconditions.checkArgument(call.getArguments().size() == 2);
        ScalarOperator arg0 = call.getArguments().get(0);
        ScalarOperator arg1 = call.getArguments().get(1);
        if (arg0.isConstantNull()) {
            return Optional.of(ConstantOperator.createNull(call.getType()));
        }
        if (!(arg0 instanceof ArrayOperator) || !arg1.isConstantRef() ||
                !arg0.getChildren().stream().allMatch(ScalarOperator::isConstantRef)) {
            return Optional.empty();
        }
        return Optional.of(cb.apply(arg0.getChildren(), arg1));
    }

    private Optional<ScalarOperator> arrayUnaryFun(CallOperator call,
                                                   Function<List<ScalarOperator>, ScalarOperator> cb) {
        Preconditions.checkArgument(call.getArguments().size() == 1);
        ScalarOperator arg0 = call.getChild(0);
        if (arg0 instanceof CastOperator && arg0.getChild(0) instanceof ArrayOperator) {
            arg0 = call.getChild(0).getChild(0);
        }
        if (arg0.isConstantNull()) {
            return Optional.of(ConstantOperator.createNull(arg0.getType()));
        }
        if (!(arg0 instanceof ArrayOperator) || !arg0.getChildren().stream().allMatch(ScalarOperator::isConstantRef)) {
            return Optional.empty();
        }
        try {
            return Optional.of(cb.apply(arg0.getChildren()));
        } catch (IllegalArgumentException ex) {
            return Optional.empty();
        }
    }

    private Optional<ScalarOperator> constArrayAppend(CallOperator call) {
        return arrayScalarFun(call, (arrayElements, target) ->
                new ArrayOperator(call.getType(), call.isNullable(),
                        Lists.newArrayList(Iterables.concat(arrayElements, List.of(target)))));
    }

    boolean constantEqual(ScalarOperator lhs, ScalarOperator rhs) {
        Preconditions.checkArgument(lhs.isConstantRef() && rhs.isConstantRef());
        ConstantOperator constLhs = (ConstantOperator) lhs;
        ConstantOperator constRhs = (ConstantOperator) rhs;
        if (constLhs.getType().isDecimalV3()) {
            return constLhs.getDecimal().compareTo(constRhs.getDecimal()) == 0 &&
                    constLhs.isNull() == constRhs.isNull();
        } else {
            return lhs.equals(rhs);
        }
    }

    private Optional<ScalarOperator> constArrayContains(CallOperator call) {

        return arrayScalarFun(call, (arrayElements, target) ->
                ConstantOperator.createBoolean(arrayElements.stream().anyMatch(elem -> constantEqual(elem, target))));
    }

    private Optional<ScalarOperator> constArrayRemove(CallOperator call) {
        return arrayScalarFun(call, (arrayElements, target) ->
                new ArrayOperator(call.getType(), call.isNullable(),
                        arrayElements.stream().filter((elem) -> !constantEqual(elem, target))
                                .collect(Collectors.toList())));
    }

    private Optional<ScalarOperator> constArrayPosition(CallOperator call) {
        return arrayScalarFun(call, (arrayElements, target) ->
                ConstantOperator.createInt(IntStream.range(0, arrayElements.size())
                        .boxed()
                        .filter(i -> constantEqual(arrayElements.get(i), target))
                        .findAny().map(i -> i + 1).orElse(0)));
    }

    private Optional<ScalarOperator> constArrayLength(CallOperator call) {
        ScalarOperator arg0 = call.getChild(0);
        if (arg0.isConstantNull()) {
            return Optional.of(ConstantOperator.createNull(call.getType()));
        } else if (arg0 instanceof ArrayOperator) {
            return Optional.of(ConstantOperator.createInt(arg0.getChildren().size()));
        } else if (arg0 instanceof CastOperator && (arg0.getChild(0) instanceof ArrayOperator)) {
            // array_length([]) and array_length([NULL, NULL]) would be
            // array_length(cast([] as ARRAY<BOOL>)) and array_length(cast([NULL, NULL] as ARRAY<BOOL>))
            return Optional.of(ConstantOperator.createInt(arg0.getChild(0).getChildren().size()));
        } else {
            return Optional.empty();
        }
    }

    private Optional<ScalarOperator> constArraySum(CallOperator call) {
        BinaryOperator<ConstantOperator> add = (lhs, rhs) -> {
            String opName = ArithmeticExpr.Operator.ADD.getName();
            com.starrocks.catalog.Function fn =
                    Expr.getBuiltinFunction(opName, new Type[] {lhs.getType(), rhs.getType()},
                            com.starrocks.catalog.Function.CompareMode.IS_SUPERTYPE_OF);
            // for decimal types, add function should be rectified.
            if (call.getType().isDecimalV3()) {
                Type type = call.getType();
                fn = new ScalarFunction(fn.getFunctionName(), new Type[] {type, type}, type, fn.hasVarArgs());
            }

            CallOperator addition =
                    new CallOperator(ArithmeticExpr.Operator.ADD.getName(), lhs.getType(), List.of(lhs, rhs), fn);
            ScalarOperator result = addition.accept(this, new ScalarOperatorRewriteContext());
            if (result instanceof ConstantOperator) {
                return (ConstantOperator) result;
            } else {
                throw new IllegalArgumentException();
            }
        };
        return arrayUnaryFun(call, (elements) -> elements.stream().filter(elem -> !elem.isConstantNull())
                .map(elem -> (ConstantOperator) elem).reduce(add).orElse(ConstantOperator.NULL));
    }

    private Optional<ScalarOperator> constArrayMinMax(CallOperator call, boolean lessThan) {
        Comparator<ConstantOperator> comparator = (lhs, rhs) -> {
            BinaryPredicateOperator cmp =
                    lessThan ? BinaryPredicateOperator.lt(lhs, rhs) : BinaryPredicateOperator.gt(lhs, rhs);
            ScalarOperator result = cmp.accept(this, new ScalarOperatorRewriteContext());
            if (result.isConstantRef() && result.getType().isBoolean()) {
                return ((ConstantOperator) result).getBoolean() ? -1 : 1;
            } else {
                throw new IllegalArgumentException();
            }
        };
        return arrayUnaryFun(call, (elements) -> elements.stream().filter(elem -> !elem.isConstantNull())
                .map(elem -> (ConstantOperator) elem).min(comparator).orElse(ConstantOperator.NULL));
    }

    private Optional<ScalarOperator> constArrayMin(CallOperator call) {
        return constArrayMinMax(call, true);
    }

    private Optional<ScalarOperator> constArrayMax(CallOperator call) {
        return constArrayMinMax(call, false);
    }

    private Optional<ScalarOperator> constArrayAvg(CallOperator call) {
        Optional<ScalarOperator> optSum = constArraySum(call);
        Optional<ScalarOperator> optLength = constArrayLength(call);
        if (!optSum.isPresent() || !optLength.isPresent() ||
                !optSum.get().isConstant() || !optLength.get().isConstant()) {
            return Optional.empty();
        }
        ConstantOperator sum = (ConstantOperator) optSum.get();
        ConstantOperator length = (ConstantOperator) optLength.get();
        if (sum.isNull() || length.isNull()) {
            return Optional.of(ConstantOperator.createNull(call.getType()));
        }

        String opName = ArithmeticExpr.Operator.DIVIDE.getName();
        com.starrocks.catalog.Function
                fn = Expr.getBuiltinFunction(opName, new Type[] {call.getType(), call.getType()},
                com.starrocks.catalog.Function.CompareMode.IS_SUPERTYPE_OF);

        // for decimal types, divide function should be rectified.
        if (call.getType().isDecimalV3()) {
            Preconditions.checkArgument(sum.getType().isDecimalV3());
            ScalarType sumType = (ScalarType) sum.getType();
            sumType = ScalarType.createDecimalV3Type(call.getType().getPrimitiveType(), sumType.getScalarPrecision(),
                    sumType.getScalarScale());
            int precision = PrimitiveType.getMaxPrecisionOfDecimal(call.getType().getPrimitiveType());
            ScalarType countType = ScalarType.createDecimalV3Type(call.getType().getPrimitiveType(), precision, 0);
            fn = new ScalarFunction(fn.getFunctionName(), new Type[] {sumType, countType}, call.getType(),
                    fn.hasVarArgs());
        }

        CallOperator avg =
                new CallOperator(opName, call.getType(), List.of(sum, length), fn);
        ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
        ScalarOperator result = rewriter.rewrite(avg, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
        if (result.isConstantRef()) {
            return Optional.of(result);
        } else {
            return Optional.empty();
        }
    }

    private Optional<ScalarOperator> returnNullIfExistsNullArg(CallOperator call) {
        ImmutableSet<String> functions = new ImmutableSortedSet.Builder<String>(String.CASE_INSENSITIVE_ORDER)
                .add("split")
                .add("str_to_map")
                .add("regexp_extract_all")
                .add("regexp_split")
                .add("array_length")
                .add("array_sum")
                .add("array_avg")
                .add("array_min")
                .add("array_max")
                .add("array_distinct")
                .add("array_sort")
                .add("reverse")
                .add("array_join")
                .add("array_difference")
                .add("array_slice")
                .add("array_concat")
                .add("arrays_overlap")
                .add("array_intersect")
                .add("array_cum_sum")
                .add("array_contains_all")
                .add("array_contains_seq")
                .add("all_match")
                .add("any_match")
                .add("array_generate")
                .add("array_repeat")
                .add("array_flatten")
                .add("array_map")
                .add("map_size")
                .add("map_keys")
                .add("map_values")
                .add("map_from_arrays")
                .add("distinct_map_keys")
                .add("cardinality")
                .add("tokenize")
                .build();
        if (functions.contains(call.getFnName()) && call.getArguments().stream().anyMatch(
                ScalarOperator::isConstantNull)) {
            return Optional.of(ConstantOperator.createNull(call.getType()));
        } else {
            return Optional.empty();
        }
    }

    private Optional<ScalarOperator> tryToProcessConstantArrayFunctions(CallOperator call) {
        Optional<ScalarOperator> result = returnNullIfExistsNullArg(call);
        if (result.isPresent()) {
            return result;
        }
        ImmutableMap<String, Function<CallOperator, Optional<ScalarOperator>>> handlers =
                new ImmutableSortedMap.Builder<String, Function<CallOperator, Optional<ScalarOperator>>>(
                        String.CASE_INSENSITIVE_ORDER)
                        .put("array_length", this::constArrayLength)
                        .put("array_sum", this::constArraySum)
                        .put("array_min", this::constArrayMin)
                        .put("array_max", this::constArrayMax)
                        .put("array_avg", this::constArrayAvg)
                        .put("array_contains", this::constArrayContains)
                        .put("array_append", this::constArrayAppend)
                        .put("array_remove", this::constArrayRemove)
                        .put("array_position", this::constArrayPosition)
                        .build();
        if (handlers.containsKey(call.getFnName())) {
            Optional<ScalarOperator> optResult = handlers.get(call.getFnName()).apply(call);
            if (optResult.isPresent()) {
                CastOperator castOp = new CastOperator(call.getType(), optResult.get());
                ScalarOperator op =
                        new ScalarOperatorRewriter().rewrite(castOp, Lists.newArrayList(new ReduceCastRule()));
                return Optional.of(op);
            }
            return optResult;
        }
        return Optional.empty();
    }

    @Override
    public ScalarOperator visitCall(CallOperator call, ScalarOperatorRewriteContext context) {
        if (call.isAggregate()) {
            return call;
        }

        Optional<ScalarOperator> mayBeConstant = tryToProcessConstantArrayFunctions(call);
        if (mayBeConstant.isPresent()) {
            return mayBeConstant.get();
        }

        if (notAllConstant(call.getChildren())) {
            if (call.getFunction() != null && call.getFunction().isMetaFunction()) {
                String errMsg = String.format("Meta function %s does not support non-constant arguments",
                        call.getFunction().getFunctionName());
                throw new SemanticException(errMsg);
            }
            return call;
        }

        return ScalarOperatorEvaluator.INSTANCE.evaluation(call, needMonotonicFunc);
    }

    @Override
    public ScalarOperator visitInPredicate(InPredicateOperator predicate, ScalarOperatorRewriteContext context) {
        ScalarOperator c1 = predicate.getChild(0);
        if (c1.isConstantRef() && ((ConstantOperator) c1).isNull()) {
            return ConstantOperator.createNull(Type.BOOLEAN);
        }

        if (notAllConstant(predicate.getChildren())) {
            return predicate;
        }

        ConstantOperator child1 = (ConstantOperator) predicate.getChild(0);
        for (int i = 1; i < predicate.getChildren().size(); i++) {
            if (0 == child1.compareTo((ConstantOperator) predicate.getChild(i))) {
                return ConstantOperator.createBoolean(!predicate.isNotIn());
            }
        }

        if (hasNull(predicate.getChildren())) {
            return ConstantOperator.createNull(Type.BOOLEAN);
        }

        return ConstantOperator.createBoolean(predicate.isNotIn());
    }

    @Override
    public ScalarOperator visitLargeInPredicate(LargeInPredicateOperator predicate, ScalarOperatorRewriteContext context) {
        return predicate;
    }

    //
    // Add cast function when children's type different with parent required type
    //
    // example:
    //        isNull(+)
    //           |
    //         b(int)
    //
    // After rule:
    //          false
    // example:
    //        isNull(+)
    //           |
    //         b(Null)
    //
    // After rule:
    //          true
    @Override
    public ScalarOperator visitIsNullPredicate(IsNullPredicateOperator predicate,
                                               ScalarOperatorRewriteContext context) {
        if (predicate.getChild(0).getType().equals(Type.NULL)) {
            return ConstantOperator.createBoolean(!predicate.isNotNull());
        }

        if (notAllConstant(predicate.getChildren())) {
            return predicate;
        }

        if (predicate.isNotNull()) {
            return ConstantOperator.createBoolean(!hasNull(predicate.getChildren()));
        }

        return ConstantOperator.createBoolean(hasNull(predicate.getChildren()));
    }

    //
    // cast value type
    //
    // example:
    //        Cast(Int)
    //            |
    //          "1"(char)
    //
    // After rule:
    //           1(Int)
    //
    @Override
    public ScalarOperator visitCastOperator(CastOperator operator, ScalarOperatorRewriteContext context) {
        // cast null/null_type to any type
        if (hasNull(operator.getChildren()) || operator.getChild(0).getType().isNull()) {
            return ConstantOperator.createNull(operator.getType());
        }

        ScalarOperator arg0 = operator.getChild(0);
        if (arg0 instanceof ArrayOperator
                && arg0.getChildren().stream().allMatch(ScalarOperator::isConstantRef)
                && !arg0.getChildren().isEmpty() && operator.getType().isArrayType()
                && ((ArrayType) operator.getType()).getItemType().isScalarType()) {
            if (notAllConstant(arg0.getChildren())) {
                return operator;
            }
            Type arrayElemType = ((ArrayType) operator.getType()).getItemType();
            ScalarOperatorRewriteContext subContext = new ScalarOperatorRewriteContext();
            List<ScalarOperator> newArguments = arg0.getChildren().stream()
                    .map(arg -> visitCastOperator(new CastOperator(arrayElemType, arg), subContext))
                    .collect(Collectors.toList());
            if (newArguments.stream().allMatch(arg -> arg != null && arg.isConstantRef())) {
                return new ArrayOperator(operator.getType(), operator.isNullable(), newArguments);
            }
            return operator;
        }

        if (notAllConstant(operator.getChildren())) {
            return operator;
        }

        ConstantOperator child = (ConstantOperator) operator.getChild(0);

        Optional<ConstantOperator> result = child.castTo(operator.getType());
        if (!result.isPresent()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Fold cast constant error: " + operator + ", " + child.toString());
            }
            return operator;
        } else {
            return result.get();
        }
    }

    //
    // Fold Const Binary Predicate
    //
    // example:
    //        Binary(=)
    //        /      \
    //      "a"      "b"
    //
    // After rule:
    //         false
    //
    // example:
    //        Binary(<=)
    //        /      \
    //       1        2
    //
    // After rule:
    //         true
    //
    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate,
                                               ScalarOperatorRewriteContext context) {
        if (!BinaryType.EQ_FOR_NULL.equals(predicate.getBinaryType())
                && hasNull(predicate.getChildren())) {
            return ConstantOperator.createNull(Type.BOOLEAN);
        }

        if (notAllConstant(predicate.getChildren())) {
            return predicate;
        }

        ConstantOperator child1 = (ConstantOperator) predicate.getChild(0);
        ConstantOperator child2 = (ConstantOperator) predicate.getChild(1);

        switch (predicate.getBinaryType()) {
            case EQ:
                return ConstantOperator.createBoolean(child1.compareTo(child2) == 0);
            case NE:
                return ConstantOperator.createBoolean(child1.compareTo(child2) != 0);
            case EQ_FOR_NULL: {
                if (child1.isNull() && child2.isNull()) {
                    return ConstantOperator.createBoolean(true);
                } else if (!child1.isNull() && !child2.isNull()) {
                    return ConstantOperator.createBoolean(child1.compareTo(child2) == 0);
                } else {
                    return ConstantOperator.createBoolean(false);
                }
            }
            case GE:
                return ConstantOperator.createBoolean(child1.compareTo(child2) >= 0);
            case GT:
                return ConstantOperator.createBoolean(child1.compareTo(child2) > 0);
            case LE:
                return ConstantOperator.createBoolean(child1.compareTo(child2) <= 0);
            case LT:
                return ConstantOperator.createBoolean(child1.compareTo(child2) < 0);
        }

        return predicate;
    }

    @Override
    public ScalarOperator visitLikePredicateOperator(LikePredicateOperator predicate,
                                                     ScalarOperatorRewriteContext context) {
        if (hasNull(predicate.getChildren())) {
            return ConstantOperator.createNull(Type.BOOLEAN);
        }
        if (notAllConstant(predicate.getChildren()) || predicate.getLikeType() != LikePredicateOperator.LikeType.LIKE) {
            return predicate;
        }

        String text = ((ConstantOperator) predicate.getChild(0)).getVarchar();
        String pattern = ((ConstantOperator) predicate.getChild(1)).getVarchar();

        if (pattern.matches("^[^_%]+$")) {
            return ConstantOperator.createBoolean(text.equals(pattern));
        }
        if (pattern.matches("^%+[^_%]+$")) {
            pattern = StringUtils.stripStart(pattern, "%");
            return ConstantOperator.createBoolean(text.endsWith(pattern));
        } else if (pattern.matches("^[^_%]+%+$")) {
            pattern = StringUtils.stripEnd(pattern, "%");
            return ConstantOperator.createBoolean(text.startsWith(pattern));
        } else if (pattern.matches("^%+[^_%]+%+$")) {
            pattern = StringUtils.stripStart(pattern, "%");
            pattern = StringUtils.stripEnd(pattern, "%");
            return ConstantOperator.createBoolean(text.contains(pattern));
        }
        return predicate;
    }

    private boolean notAllConstant(List<ScalarOperator> operators) {
        return !operators.stream().allMatch(ScalarOperator::isConstantRef);
    }

    private boolean hasNull(List<ScalarOperator> operators) {
        return operators.stream().anyMatch(d -> (d.isConstantRef()) && ((ConstantOperator) d).isNull());
    }
}
