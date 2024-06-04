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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.common.util.UnionFind;
import com.starrocks.sql.automv.boolalgebra.Modifier;
import com.starrocks.sql.automv.boolalgebra.TriBool;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.util.Box;
import com.starrocks.sql.automv.util.Util;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

// pn.Op is base class of Polish-Notion Expressions in AutoMV.
public abstract class Op {
    public static final Set<String> DISTINCT_FUNCTIONS =
            ImmutableSet.<String>builder()
                    .add(FunctionSet.SUM)
                    .add(FunctionSet.AVG)
                    .add(FunctionSet.COUNT)
                    .add(FunctionSet.ARRAY_AGG).build();
    private static final List<Op> LOCALS =
            Collections.unmodifiableList(IntStream.range(0, 20).mapToObj(Op::buildLocal).collect(Collectors.toList()));
    public static Map<ApplyKind, String> COMPARE_OP_TABLE = ImmutableMap.<ApplyKind, String>builder()
            .put(BuiltinKind.EQ, "=")
            .put(BuiltinKind.NE, "!=")
            .put(BuiltinKind.NULL_SAFE_EQ, "<=>")
            .put(BuiltinKind.LT, "<")
            .put(BuiltinKind.LE, "<=")
            .put(BuiltinKind.GT, ">")
            .put(BuiltinKind.GE, ">=").build();
    protected final Type type;
    private transient Integer cachedHashCode = null;
    private transient String cachedString = null;

    private transient String cachedIsomorphicString = null;
    private transient SymTab cachedSymTab = null;
    private transient Set<Integer> cachedIdSet = null;
    private transient ColumnRefSet cachedIds = null;
    private transient Integer height = null;

    private transient Op norm = null;

    public Op(Type type) {
        this.type = type;
    }

    public static Function<Op, Op> toVEVSwapper(ColumnRefSet lhsIds, ColumnRefSet rhsIds) {
        return vev -> {
            Preconditions.checkArgument(vev.isVEV());
            Op a = vev.arg(0);
            Op b = vev.arg(1);

            if (lhsIds.contains(a.getId())) {
                Preconditions.checkArgument(rhsIds.contains(b.getId()));
                return vev;
            } else {
                Preconditions.checkArgument(lhsIds.contains(b.getId()));
                Preconditions.checkArgument(rhsIds.contains(a.getId()));
                return eqOrInRange(a, b);
            }
        };
    }

    public static boolean equivalent(Op a, Op b, UnionFind<Integer> equivSymbols) {
        if (!a.isomorphic(b)) {
            return false;
        }
        return SymTab.equivalent(a, b, equivSymbols);
    }

    public static Map<String, Set<Box<Op>>> getCse(Op a, Op b, UnionFind<Integer> equivSymbols) {
        if (a.getHeight() > b.getHeight()) {
            Op tmp = a;
            a = b;
            b = tmp;
        }
        Map<String, List<Op>> groups = a.flatten();
        Map<String, Set<Box<Op>>> equivGroups = Maps.newHashMap();
        b.searchCse(groups, equivSymbols, equivGroups);
        equivGroups.replaceAll((k, v) -> new HashSet<>(v));
        return equivGroups;
    }

    public static Var var(Type type, int id) {
        return new Var(type, id);
    }

    public static Val val(ConstantOperator constant) {
        return new Val(constant);
    }

    public static Op inRange(Op item, List<Op> values) {
        Preconditions.checkArgument(item.isVar() && !values.isEmpty());
        Preconditions.checkArgument(values.stream().allMatch(Op::isRangeOf));
        return apply(Type.BOOLEAN, BuiltinKind.IN_RANGE, true, Lists.newArrayList(item, setOf(values)));
    }

    public static Op inRange(Op item, Op... values) {
        return inRange(item, Arrays.asList(values));
    }

    private static Optional<Op> eliminateInRangeImpl(Op op) {
        if (op.isVar() || op.isVal()) {
            return Optional.empty();
        }

        List<Pair<Op, Optional<Op>>> argPairs = op.getArgs().stream()
                .map(arg -> Pair.create(arg, eliminateInRangeImpl(arg)))
                .collect(Collectors.toList());
        boolean argsUnchanged = argPairs.stream().noneMatch(p -> p.second.isPresent());
        if (argsUnchanged) {
            return op.inRangeToLtLe();
        } else {
            List<Op> newArgs = argPairs.stream()
                    .map(p -> p.second.orElse(p.first))
                    .collect(Collectors.toList());
            Apply apply = op.cast();
            Op newOp = Apply.apply(apply.type, apply.getKind(), apply.isOrdered(), newArgs);
            return Optional.of(newOp.inRangeToLtLe().orElse(newOp));
        }
    }

    public static Op in(Op item, Op... values) {
        return in(item, Arrays.asList(values));
    }

    public static Op in(Op item, List<Op> values) {
        if (item.isVar()) {
            Map<Boolean, List<Op>> groups = values.stream().collect(Collectors.partitioningBy(Op::isVal));
            List<Op> vals = groups.get(true);
            Optional<Op> optOrEqNonVals = groups.get(false).stream()
                    .map(nonVal -> eqOrInRange(item, nonVal)).reduce(Op::or);
            if (vals.isEmpty()) {
                Preconditions.checkArgument(optOrEqNonVals.isPresent());
                return optOrEqNonVals.get();
            } else {
                Op inOp = apply(Type.BOOLEAN, BuiltinKind.IN, true, Lists.newArrayList(item, setOf(vals)));
                return optOrEqNonVals.map(op -> or(inOp, op)).orElse(inOp);
            }
        } else {
            return apply(Type.BOOLEAN, BuiltinKind.IN, true, Lists.newArrayList(item, setOf(values)));
        }
    }

    public static Op eq(Op a, Op b) {
        return apply(Type.BOOLEAN, BuiltinKind.EQ, false, Lists.newArrayList(a, b));
    }

    public static Op eqOrInRange(Op a, Op b) {
        if (a.isVar() && b.isVal()) {
            return inRange(a, closedRangeOf(b, b));
        } else if (a.isVal() && b.isVar()) {
            return inRange(b, closedRangeOf(a, a));
        } else {
            return eq(a, b);
        }
    }

    public static Op ne(Op a, Op b) {
        return apply(Type.BOOLEAN, BuiltinKind.NE, false, Lists.newArrayList(a, b));
    }

    public static Op neOrInRange(Op a, Op b) {
        if (a.isVar() && b.isVal()) {
            return inRange(a, openClosedRangeOf(Val.NULL_VAL, b), closedOpenRangeOf(b, Val.NULL_VAL));
        } else if (a.isVal() && b.isVar()) {
            return inRange(b, openClosedRangeOf(Val.NULL_VAL, a), closedOpenRangeOf(a, Val.NULL_VAL));
        } else {
            return apply(Type.BOOLEAN, BuiltinKind.NE, false, Lists.newArrayList(a, b));
        }
    }

    public static Op le(Op a, Op b) {
        return apply(Type.BOOLEAN, BuiltinKind.LE, true, Lists.newArrayList(a, b));
    }

    public static Op leOrInRange(Op a, Op b) {
        if (a.isVar() && b.isVal()) {
            return inRange(a, openClosedRangeOf(Val.NULL_VAL, b));
        } else if (a.isVal() && b.isVar()) {
            return inRange(b, closedOpenRangeOf(a, Val.NULL_VAL));
        } else {
            return le(a, b);
        }
    }

    public static Op lt(Op a, Op b) {
        return apply(Type.BOOLEAN, BuiltinKind.LT, true, Lists.newArrayList(a, b));
    }

    public static Op ltOrInRange(Op a, Op b) {
        if (a.isVar() && b.isVal()) {
            return inRange(a, openRangeOf(Val.NULL_VAL, b));
        } else if (a.isVal() && b.isVar()) {
            return inRange(b, openRangeOf(a, Val.NULL_VAL));
        } else {
            return lt(a, b);
        }
    }

    public static Op nullSafeEq(List<Op> args) {
        return apply(Type.BOOLEAN, BuiltinKind.NULL_SAFE_EQ, false, args);
    }

    public static Op apply(Type type, String name, boolean ordered, Op... args) {
        return apply(type, name, ordered, Arrays.asList(args));
    }

    public static Op apply(Type type, String name, boolean ordered, List<Op> args) {
        return apply(type, FunctionKind.of(name), ordered, args);
    }

    public static Op apply(Type type, BuiltinKind builtin, boolean ordered, Op... args) {
        return apply(type, builtin, ordered, Arrays.asList(args));
    }

    public static Op apply(Type type, ApplyKind kind, boolean ordered, List<Op> args) {
        List<Op> newArgs;
        if (ordered) {
            newArgs = args;
        } else {
            Map<Boolean, List<Op>> argGroups = args.stream().collect(Collectors.partitioningBy(Op::isVar));
            List<Op> nonVarArgs = argGroups.get(false).stream().sorted(Comparator.comparing(Op::toString))
                    .collect(Collectors.toList());
            List<Op> varArgs = argGroups.get(true);

            newArgs = Stream.of(varArgs, nonVarArgs).flatMap(Collection::stream).collect(Collectors.toList());
        }
        return new Apply(type, kind, ordered, newArgs);
    }

    public static Op setOf(Op... args) {
        return setOf(Arrays.asList(args));
    }

    public static Op setOf(List<Op> args) {
        // TODO: args deduplication
        Optional<Op> notNullOp = args.stream().filter(arg -> !arg.isNullVal()).findFirst();
        return apply(notNullOp.map(Op::getType).orElse(Type.NULL), BuiltinKind.SET_OF, false, args);
    }

    private static Op buildLocal(int id) {
        Preconditions.checkArgument(0 <= id);
        return apply(Type.INT, BuiltinKind.LOCAL, true, Collections.singletonList(val(ConstantOperator.createInt(id))));
    }

    private static int getMaxLocal(Op op) {
        if (op.isLambda()) {
            // Lambda without arguments.
            if (op.getArgs().size() == 1) {
                return -1;
            }
            Op lastArg = op.arg(-1);
            Preconditions.checkArgument(lastArg.isLocal());
            return ((Val) (lastArg.arg(0))).getValue().getInt();
        }
        return op.getArgs().stream().map(Op::getMaxLocal).reduce(Math::max).orElse(-1);
    }

    private static Optional<Op> substLocals(Op op, Map<Integer, Op> remapping) {
        if (op.isVar()) {
            Var var = (Var) op;
            return Optional.ofNullable(remapping.get(var.getId()));
        }
        if (op.isVal()) {
            return Optional.empty();
        }
        List<Pair<Op, Optional<Op>>> argPairs = op.getArgs().stream()
                .map(arg -> Pair.create(arg, substLocals(arg, remapping)))
                .collect(Collectors.toList());
        boolean noLocals = argPairs.stream().noneMatch(p -> p.second.isPresent());
        if (noLocals) {
            return Optional.empty();
        } else {
            Apply oldOp = (Apply) op;
            List<Op> newArgs = argPairs.stream().map(p -> p.second.orElse(p.first)).collect(Collectors.toList());
            return Optional.of(apply(oldOp.type, oldOp.getKind(), oldOp.isOrdered(), newArgs));
        }
    }

    public static Op lambda(Op lambdaBody, List<Integer> args) {
        int nextLocalId = getMaxLocal(lambdaBody) + 1;
        List<Op> locals = getLocals(nextLocalId, args.size());
        Map<Integer, Op> remapping = IntStream.range(0, args.size())
                .mapToObj(i -> Pair.create(args.get(i), locals.get(i)))
                .collect(Collectors.toMap(p -> p.first, p -> p.second));
        Op newLambdaBody = substLocals(lambdaBody, remapping).orElse(lambdaBody);
        return simpleLambda(newLambdaBody, locals);
    }

    public static Op simpleLambda(Op lambda, List<Op> locals) {
        ImmutableList.Builder<Op> argsBuilder = ImmutableList.builderWithExpectedSize(locals.size() + 1);
        argsBuilder.add(lambda);
        argsBuilder.addAll(locals);
        return apply(Type.FUNCTION, BuiltinKind.LAMBDA, true, argsBuilder.build());
    }

    public static List<Op> getLocals(int i, int len) {
        Preconditions.checkArgument(0 <= i && i < len);
        if ((i + len) <= LOCALS.size()) {
            return LOCALS.subList(i, i + len);
        } else {
            return Collections.unmodifiableList(
                    IntStream.range(i, i + len).mapToObj(Op::buildLocal).collect(Collectors.toList()));
        }
    }

    public static Op distinct(Type type, List<Op> args) {
        return apply(type, BuiltinKind.DISTINCT, true, args);
    }

    public static Op variable(Type type, String colFqName) {
        return apply(type, BuiltinKind.VARIABLE, true, Op.val(ConstantOperator.createChar(colFqName)));
    }

    private static Op rangeOf(BuiltinKind builtin, Op a, Op b) {
        Preconditions.checkArgument(!(a.isNullVal() && b.isNullVal()));
        Type type = a.isNullVal() ? b.type : a.type;
        return apply(type, builtin, true, Lists.newArrayList(a, b));
    }

    public static Op openRangeOf(Op a, Op b) {
        return rangeOf(BuiltinKind.OPEN_RANGE_OF, a, b);
    }

    public static Op openClosedRangeOf(Op a, Op b) {
        return rangeOf(BuiltinKind.OPEN_CLOSED_RANGE_OF, a, b);
    }

    public static Op closedOpenRangeOf(Op a, Op b) {
        return rangeOf(BuiltinKind.CLOSED_OPEN_RANGE_OF, a, b);
    }

    public static Op closedRangeOf(Op a, Op b) {
        return rangeOf(BuiltinKind.CLOSED_RANGE_OF, a, b);
    }

    public static Op or(Op... values) {
        return or(Arrays.asList(values));
    }

    public static Op or(List<Op> values) {
        List<Op> args = values.stream()
                .flatMap(arg -> arg.isOr() ? arg.getArgs().stream() : Stream.of(arg))
                .filter(arg -> !arg.isFalseVal()).collect(Collectors.toList());
        if (args.isEmpty()) {
            return Val.FALSE_VAL;
        } else if (args.size() == 1) {
            return args.get(0);
        } else if (args.stream().anyMatch(Op::isTrueVal)) {
            return Val.TRUE_VAL;
        } else {
            return Op.apply(Type.BOOLEAN, BuiltinKind.OR, false, args);
        }
    }

    public static Op and(Op... values) {
        return and(Arrays.asList(values));
    }

    public static Op and(List<Op> values) {
        List<Op> args = values.stream()
                .flatMap(arg -> arg.isAnd() ? arg.getArgs().stream() : Stream.of(arg))
                .filter(arg -> !arg.isTrueVal()).collect(Collectors.toList());
        if (args.isEmpty()) {
            return Val.TRUE_VAL;
        } else if (args.size() == 1) {
            return args.get(0);
        } else if (args.stream().anyMatch(Op::isFalseVal)) {
            return Val.FALSE_VAL;
        } else {
            return Op.apply(Type.BOOLEAN, BuiltinKind.AND, false, args);
        }
    }

    private static Op reduceCompoundComponents(BuiltinKind builtin, Function<Op, Boolean> isTop,
                                               Function<Op, Boolean> isBottom,
                                               Op top,
                                               Op bottom, List<Op> args, BinaryOperator<Modifier> reduceFunc) {
        Map<String, List<Op>> isomorphicGroups = args.stream()
                .collect(Collectors.groupingBy(op -> op.unmodified().toString()));
        Map<Boolean, List<List<Op>>> groupsOfSizes =
                isomorphicGroups.values().stream().collect(Collectors.partitioningBy(ops -> ops.size() == 1));

        List<Op> newArgs = groupsOfSizes.get(true).stream().flatMap(Collection::stream).collect(Collectors.toList());
        for (List<Op> group : groupsOfSizes.get(false)) {
            Map<String, List<Op>> equivSubGroups =
                    group.stream().collect(Collectors.groupingBy(op -> op.getSymTab().toString()));
            Map<Boolean, List<List<Op>>> subGroupsOfSizes =
                    equivSubGroups.values().stream().collect(Collectors.partitioningBy(ops -> ops.size() == 1));
            List<Op> size1SubGroup =
                    subGroupsOfSizes.get(true).stream().flatMap(Collection::stream).collect(Collectors.toList());
            newArgs.addAll(size1SubGroup);
            for (List<Op> subGroup : subGroupsOfSizes.get(false)) {
                Modifier modifier =
                        subGroup.stream().map(Op::getModifier).reduce(Modifier.M_POSITIVE, reduceFunc);
                newArgs.add(Op.modify(modifier, subGroup.get(0).unmodified()));
            }
        }
        boolean existsBottom = false;
        boolean allNull = true;
        boolean allTop = true;
        for (Op op : newArgs) {
            if (!isTop.apply(op)) {
                allTop = false;
            }
            if (!op.isNullVal()) {
                allNull = false;
            }
            if (isBottom.apply(op)) {
                existsBottom = true;
            }
        }
        if (allTop) {
            return top;
        } else if (allNull) {
            return Val.NULL_VAL;
        } else if (existsBottom) {
            return bottom;
        }
        return Op.apply(Type.BOOLEAN, builtin, false, args);
    }

    private static Op reduceOrComponents(List<Op> components) {
        return reduceCompoundComponents(BuiltinKind.OR, Op::isFalseVal, Op::isTrueVal, Val.FALSE_VAL, Val.TRUE_VAL,
                components, Modifier::or);
    }

    private static Op reduceAndComponents(List<Op> components) {
        return reduceCompoundComponents(BuiltinKind.AND, Op::isTrueVal, Op::isFalseVal, Val.TRUE_VAL, Val.FALSE_VAL,
                components, Modifier::and);
    }

    public static Op modify(Modifier modifier, Op op) {
        if (modifier.equals(Modifier.M_POSITIVE)) {
            return op;
        }
        Modifier newModifier = modifier;
        Op arg1 = op;
        if (op.isModify()) {
            List<Op> args = op.getArgs();
            Modifier oldModifier = Modifier.valueOf(((Val) args.get(0)).getValue().getVarchar());
            newModifier = modifier.compose(oldModifier);
            arg1 = args.get(1);
        }

        if (newModifier.equals(Modifier.M_ALWAYS_FALSE)) {
            return Val.FALSE_VAL;
        } else if (newModifier.equals(Modifier.M_ALWAYS_NULL)) {
            return Val.NULL_VAL;
        } else if (newModifier.equals(Modifier.M_ALWAYS_TRUE)) {
            return Val.TRUE_VAL;
        }

        Optional<TriBool> optFinalOp = Optional.empty();
        if (arg1.isFalseVal()) {
            optFinalOp = Optional.of(modifier.transfer(0));
        } else if (arg1.isNullVal()) {
            optFinalOp = Optional.of(modifier.transfer(1));
        } else if (arg1.isTrueVal()) {
            optFinalOp = Optional.of(modifier.transfer(2));
        }

        if (optFinalOp.isPresent()) {
            TriBool finalOp = optFinalOp.get();
            if (finalOp.equals(TriBool.TB_FALSE)) {
                return Val.FALSE_VAL;
            } else if (finalOp.equals(TriBool.TB_NULL)) {
                return Val.NULL_VAL;
            } else if (finalOp.equals(TriBool.TB_TRUE)) {
                return Val.TRUE_VAL;
            }
        }

        Op arg0 = val(ConstantOperator.createVarchar(newModifier.name()));
        List<Op> newArgs = Lists.newArrayList(arg0, arg1);
        return apply(Type.BOOLEAN, BuiltinKind.MODIFY, true, newArgs);
    }

    private static List<Op> rangeNot(Op r) {
        Preconditions.checkArgument(r.isRangeOf());
        Op l = r.arg(0);
        Op u = r.arg(1);
        if (r.isClosedRangeOf()) {
            return Lists.newArrayList(openRangeOf(Val.NULL_VAL, l), openRangeOf(u, Val.NULL_VAL));
        } else if (r.isOpenRangeOf()) {
            if (l.isNullVal()) {
                return Collections.singletonList(closedOpenRangeOf(u, Val.NULL_VAL));
            } else if (u.isNullVal()) {
                return Collections.singletonList(openClosedRangeOf(Val.NULL_VAL, l));
            } else {
                return Lists.newArrayList(openClosedRangeOf(Val.NULL_VAL, l), closedOpenRangeOf(u, Val.NULL_VAL));
            }
        } else if (r.isOpenClosedRangeOf()) {
            if (l.isNullVal()) {
                return Collections.singletonList(openRangeOf(u, Val.NULL_VAL));
            } else {
                return Lists.newArrayList(openClosedRangeOf(Val.NULL_VAL, l), openRangeOf(u, Val.NULL_VAL));
            }
        } else if (r.isClosedOpenRangeOf()) {
            if (u.isNullVal()) {
                return Collections.singletonList(openRangeOf(Val.NULL_VAL, l));
            } else {
                return Lists.newArrayList(closedOpenRangeOf(u, Val.NULL_VAL), openRangeOf(Val.NULL_VAL, l));
            }
        } else {
            Preconditions.checkArgument(false, "Never reach here");
            return Collections.emptyList();
        }
    }

    public abstract Op clone();

    public Op getNorm() {
        return norm;
    }

    public void setNorm(Op norm) {
        this.norm = norm;
    }

    protected abstract int hashCodeImpl();

    protected abstract String toStringImpl();

    protected abstract String toIsomorphicStringImpl();

    protected abstract int getHeightImpl();

    protected abstract SymTab getSymTabImpl();

    public final boolean isVar() {
        return this instanceof Var;
    }

    public List<Op> conjuncts() {
        if (this.isAnd()) {
            return this.getArgs();
        } else {
            return Collections.singletonList(this);
        }
    }

    public abstract <R, C> R accept(OpVisitor<R, C> visitor, C context);

    public boolean isFun(String name) {
        return this.cast(Apply.class)
                .map(Apply::getKind)
                .map(FunctionKind.of(name)::equals)
                .orElse(false);
    }

    public boolean isVEV() {
        return isEq() && arg(0).isVar() && arg(1).isVar();
    }

    public boolean isVE() {
        return isEq() && arg(0).isVar();
    }

    public List<Op> disjuncts() {
        if (this.isOr()) {
            return this.getArgs();
        } else {
            return Collections.singletonList(this);
        }
    }

    public int getId() {
        Preconditions.checkArgument(isVar(), "Op must be a Var instance");
        Var var = (Var) this;
        return var.getId();
    }

    public GenericColumn getColumn() {
        Preconditions.checkArgument(isVar(), "Op must be a Var instance");
        Var var = (Var) this;
        Preconditions.checkArgument(var.getSymbol().isTenured(), "Var's Symbol must be tenured");
        return var.getSymbol().getTenuredColumn();
    }

    public final boolean isVal() {
        return this instanceof Val;
    }

    public final Type getType() {
        return type;
    }

    public final boolean isTrueVal() {
        return (this instanceof Val) && ((Val) this).getValue().isConstantTrue();
    }

    public final boolean isFalseVal() {
        return (this instanceof Val) && ((Val) this).getValue().isConstantFalse();
    }

    public final boolean isNullVal() {
        return (this instanceof Val) && ((Val) this).getValue().isConstantNull();
    }

    public final boolean isApply() {
        return this instanceof Apply;
    }

    private boolean isBuiltin(ApplyKind kind) {
        if (!(this instanceof Apply)) {
            return false;
        }
        Apply apply = this.cast();
        if (!(apply.getKind() instanceof BuiltinKind)) {
            return false;
        }
        return apply.getKind().equals(kind);
    }

    public final boolean isCompareOp() {
        return this.cast(Apply.class).map(Apply::getKind).map(COMPARE_OP_TABLE::containsKey).orElse(false);
    }

    public final boolean isNot() {
        return getModifier().equals(Modifier.M_NEGATIVE);
    }

    @Override
    public final int hashCode() {
        if (cachedHashCode == null) {
            cachedHashCode = hashCodeImpl();
        }
        return cachedHashCode;
    }

    @Override
    public final String toString() {
        if (cachedString == null) {
            cachedString = toStringImpl();
        }
        return cachedString;
    }

    public final String toIsoMorphicString() {
        if (cachedIsomorphicString == null) {
            cachedIsomorphicString = toIsomorphicStringImpl();
        }
        return cachedIsomorphicString;
    }

    public final int getHeight() {
        if (height == null) {
            return height = getHeightImpl();
        }
        return height;
    }

    public List<Op> getArgs() {
        return Collections.emptyList();
    }

    public StrictOp strict() {
        return new StrictOp(this);
    }

    public StrictOp strict(UnionFind<Integer> unionFind) {
        return new StrictOp(this, unionFind);
    }

    public Op arg(int idx) {
        List<Op> args = getArgs();
        int n = args.size();
        Preconditions.checkArgument(-n <= idx && idx < n);
        return args.get(idx >= 0 ? idx : n + idx);
    }

    private void collect(List<Op> opList) {
        if (this instanceof Apply) {
            Apply apply = (Apply) this;
            List<Op> sortedArgs = apply.getArgs().stream().map(arg -> Pair.create(arg, arg.getHeight()))
                    .sorted(Pair.comparingBySecond())
                    .map(p -> p.first).collect(Collectors.toList());
            for (Op op : sortedArgs) {
                op.collect(opList);
            }
        }
        opList.add(this);
    }

    public boolean isomorphic(Op op) {
        if (op == null) {
            return false;
        }
        if (!op.getClass().equals(this.getClass())) {
            return false;
        }
        if (this.hashCode() != op.hashCode()) {
            return false;
        }
        if (this.getSymTab().getEntries().size() != op.getSymTab().getEntries().size()) {
            return false;
        }
        return this.toIsoMorphicString().equals(op.toIsoMorphicString());
    }

    public SymTab getSymTab() {
        if (cachedSymTab == null) {
            cachedSymTab = getSymTabImpl();
        }
        return cachedSymTab;
    }

    private ColumnRefSet getIdsImpl() {
        return ColumnRefSet.createByIds(getIdSet());
    }

    public ColumnRefSet getIds() {
        if (cachedIds == null) {
            cachedIds = getIdsImpl();
        }
        return cachedIds;
    }

    private Set<Integer> getIdSetImpl() {
        ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();
        for (SymTabEntry entry : getSymTab().getEntries()) {
            if (entry.isSymbol()) {
                Symbol sym = (Symbol) entry;
                builder.add(sym.getId());
            } else {
                SymbolSet symSet = (SymbolSet) entry;
                symSet.getEntries().forEach(sym -> builder.add(sym.getId()));
            }
        }
        return builder.build();
    }

    public Set<Integer> getIdSet() {
        if (cachedIdSet == null) {
            cachedIdSet = getIdSetImpl();
        }
        return cachedIdSet;
    }

    public Map<String, List<Op>> flatten() {
        List<Op> opList = Lists.newArrayList();
        this.collect(opList);
        return opList.stream().collect(Collectors.groupingBy(Op::toString));
    }

    private void searchCse(Map<String, List<Op>> groups, UnionFind<Integer> equivSymbols,
                           Map<String, Set<Box<Op>>> equivGroups) {
        String key = this.toString();
        List<Op> candidates = groups.get(key);
        if (candidates != null) {
            Set<Box<Op>> group =
                    candidates.stream().filter(op -> SymTab.equivalent(op, this, equivSymbols))
                            .map(Box::of).collect(Collectors.toSet());
            group.add(Box.of(this));
            equivGroups.merge(key, group, Sets::union);
        } else {
            for (Op op : getArgs()) {
                op.searchCse(groups, equivSymbols, equivGroups);
            }
        }
    }

    public abstract int nary();

    public Optional<Op> inRangeToLtLe() {
        if (!this.isInRange()) {
            return Optional.empty();
        }
        Op item = arg(0);
        Op rangeSet = arg(1);
        Preconditions.checkArgument(rangeSet.isSetOf());
        Preconditions.checkArgument(rangeSet.getArgs().stream().allMatch(Op::isRangeOf));
        List<Op> newArgs = Lists.newArrayList();
        for (Op range : rangeSet.getArgs()) {
            Op l = range.arg(0);
            Op u = range.arg(1);
            if (range.isOpenRangeOf()) {
                if (!l.isNullVal()) {
                    newArgs.add(lt(l, item));
                }
                if (!u.isNullVal()) {
                    newArgs.add(lt(item, u));
                }
            } else if (range.isOpenClosedRangeOf()) {
                if (!l.isNullVal()) {
                    newArgs.add(lt(l, item));
                }
                Preconditions.checkArgument(!u.isNullVal());
                newArgs.add(le(item, u));
            } else if (range.isClosedOpenRangeOf()) {
                Preconditions.checkArgument(!l.isNullVal());
                newArgs.add(le(l, item));
                if (!u.isNullVal()) {
                    newArgs.add(lt(item, u));
                }
            } else if (range.isClosedRangeOf()) {
                Preconditions.checkArgument(!l.isNullVal() && !u.isNullVal());
                if (l.strict().equals(u.strict())) {
                    newArgs.add(eq(item, l));
                } else {
                    newArgs.add(le(l, item));
                    newArgs.add(le(item, u));
                }
            }
        }
        return Optional.of(or(newArgs));
    }

    public Op eliminateInRange() {
        return eliminateInRangeImpl(this).orElse(this);
    }

    public final boolean isSetOf() {
        return isBuiltin(BuiltinKind.SET_OF);
    }

    public final boolean isOpenRangeOf() {
        return isBuiltin(BuiltinKind.OPEN_RANGE_OF);
    }

    public final boolean isOpenClosedRangeOf() {
        return isBuiltin(BuiltinKind.OPEN_CLOSED_RANGE_OF);
    }

    public final boolean isClosedOpenRangeOf() {
        return isBuiltin(BuiltinKind.CLOSED_OPEN_RANGE_OF);
    }

    public final boolean isClosedRangeOf() {
        return isBuiltin(BuiltinKind.CLOSED_RANGE_OF);
    }

    public final boolean isModify() {
        return isBuiltin(BuiltinKind.MODIFY);
    }

    public final boolean isIn() {
        return isBuiltin(BuiltinKind.IN);
    }

    public final boolean isNotIn() {
        return getModifier() == Modifier.M_NEGATIVE && unmodified().isIn();
    }

    public final boolean isInRange() {
        return isBuiltin(BuiltinKind.IN_RANGE);
    }

    public final boolean isCase() {
        return isBuiltin(BuiltinKind.CASE);
    }

    public final boolean isCast() {
        return isBuiltin(BuiltinKind.CAST);
    }

    public final boolean isAnd() {
        return isBuiltin(BuiltinKind.AND);
    }

    public final boolean isOr() {
        return isBuiltin(BuiltinKind.OR);
    }

    public final boolean isEq() {
        return isBuiltin(BuiltinKind.EQ);
    }

    public final boolean isNullSafeEq() {
        return isBuiltin(BuiltinKind.NULL_SAFE_EQ);
    }

    public final boolean isNe() {
        return isBuiltin(BuiltinKind.NE);
    }

    public final boolean isGt() {
        return isBuiltin(BuiltinKind.GT);
    }

    public final boolean isGe() {
        return isBuiltin(BuiltinKind.GE);
    }

    public final boolean isLt() {
        return isBuiltin(BuiltinKind.LT);
    }

    public final boolean isLe() {
        return isBuiltin(BuiltinKind.LE);
    }

    public final boolean isArray() {
        return isBuiltin(BuiltinKind.ARRAY);
    }

    public final boolean isMap() {
        return isBuiltin(BuiltinKind.MAP);
    }

    public final boolean isArraySlice() {
        return isBuiltin(BuiltinKind.ARRAY_SLICE);
    }

    public final boolean isCollectionElement() {
        return isBuiltin(BuiltinKind.COLLECTION_ELEMENT);
    }

    public final boolean isSubfield() {
        return isBuiltin(BuiltinKind.SUBFIELD);
    }

    public final boolean isLike() {
        return isBuiltin(BuiltinKind.LIKE);
    }

    public final boolean isNotLike() {
        return isNot() && unmodified().isLike();
    }

    public final boolean isRegexp() {
        return isBuiltin(BuiltinKind.REGEXP);
    }

    public final boolean isNotRegex() {
        return isNot() && unmodified().isRegexp();
    }

    public final boolean isLambda() {
        return isBuiltin(BuiltinKind.LAMBDA);
    }

    public final boolean isLocal() {
        return isBuiltin(BuiltinKind.LOCAL);
    }

    public final boolean isDistinct() {
        return isBuiltin(BuiltinKind.DISTINCT);
    }

    public Modifier getModifier() {
        if (this.isModify()) {
            return Modifier.valueOf(((Val) arg(0)).getValue().getVarchar());
        } else {
            return Modifier.M_POSITIVE;
        }
    }

    public Op unmodified() {
        if (this.isModify()) {
            return arg(1);
        } else {
            return this;
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Op> T cast() {
        return (T) this;
    }

    public <T extends Op> Optional<T> cast(Class<T> cls) {
        return Util.downcast(this, cls);
    }

    public <T extends Op> T mustCast(Class<T> cls) {
        return Objects.requireNonNull(Util.downcast(this, cls).orElse(null));
    }

    public boolean isDistinctAgg() {
        return isSumDistinct() || isCountDistinct() || isAvgDistinct();
    }

    public boolean isSum() {
        return this.isFun(FunctionSet.SUM);
    }

    public boolean isCount() {
        return this.isFun(FunctionSet.COUNT);
    }

    public boolean isAvg() {
        return this.isFun(FunctionSet.AVG);
    }

    public boolean isCountDistinct() {
        return this.isCount() && this.arg(0).isDistinct();
    }

    public boolean isSumDistinct() {
        return this.isSum() && this.arg(0).isDistinct();
    }

    public boolean isAvgDistinct() {
        return this.isAvg() && this.arg(0).isDistinct();
    }

    public boolean isRangeOf() {
        return isOpenRangeOf() || isClosedRangeOf() || isOpenClosedRangeOf() || isClosedOpenRangeOf();
    }

    public Op toAlwaysFalse() {
        return Op.modify(Modifier.M_ALWAYS_FALSE, this);
    }

    public Op toPositive() {
        return Op.modify(Modifier.M_POSITIVE, this);
    }

    public Op toIsNull() {
        return Op.modify(Modifier.M_IS_NULL, this);
    }

    public Op toNegative() {
        return Op.modify(Modifier.M_NEGATIVE, this);
    }

    public Op toTrueOrNull() {
        return Op.modify(Modifier.M_TRUE_OR_NULL, this);
    }

    public Op toFalseOrNull() {
        return Op.modify(Modifier.M_FALSE_OR_NULL, this);
    }

    public Op toIsNotNull() {
        return Op.modify(Modifier.M_IS_NOT_NULL, this);
    }

    public boolean isVarIsNotNull() {
        return getModifier().equals(Modifier.M_IS_NOT_NULL) && unmodified().isVar();
    }

    public boolean isVarIsNull() {
        return getModifier().equals(Modifier.M_IS_NULL) && unmodified().isVar();
    }

    public Op toAlwaysTrue() {
        return Op.modify(Modifier.M_ALWAYS_TRUE, this);
    }

    private Op replaceArgs(List<Op> newArgs) {
        Preconditions.checkArgument(this.isApply());
        Preconditions.checkArgument(newArgs.size() == getArgs().size());
        Apply old = (Apply) this;
        return apply(old.type, old.getKind(), old.isOrdered(), newArgs);
    }

    private Op lowerNot() {
        return this.lowerNotRec().orElse(this);
    }

    private Optional<Op> lowerNotRec() {
        List<Pair<Op, Optional<Op>>> argAlts = getArgs().stream()
                .map(arg -> Pair.create(arg, arg.lowerNotRec()))
                .collect(Collectors.toList());
        // process args;
        if (argAlts.stream().noneMatch(p -> p.second.isPresent())) {
            return isNot() ? this.lowerOnceNot() : Optional.empty();
        } else {
            List<Op> newArgs = argAlts.stream().map(p -> p.second.orElse(p.first)).collect(Collectors.toList());
            Op op = this.replaceArgs(newArgs);
            return Optional.of(isNot() ? op.lowerOnceNot().orElse(op) : op);
        }
    }

    public Optional<Op> lowerOnceNot() {
        Preconditions.checkArgument(isNot());
        Op op = unmodified();
        if (op.isOr() || op.isAnd()) {
            List<Op> newArgs = op.getArgs().stream()
                    .map(Op::toNegative)
                    .map(arg -> arg.lowerNotRec().orElse(arg))
                    .collect(Collectors.toList());
            return Optional.of(op.isAnd() ? or(newArgs) : and(newArgs));
        } else if (op.isEq()) {
            return Optional.of(ne(op.arg(0), op.arg(1)));
        } else if (op.isNe()) {
            return Optional.of(eqOrInRange(op.arg(0), op.arg(1)));
        } else if (op.isLe()) {
            return Optional.of(ltOrInRange(op.arg(1), op.arg(0)));
        } else if (op.isLt()) {
            return Optional.of(leOrInRange(op.arg(1), op.arg(0)));
        } else if (op.isInRange()) {
            //TODO: intersection ranges must be merged into minimal set of ranges.
            Op item = op.arg(0);
            List<Op> inRanges = op.arg(1).getArgs().stream()
                    .map(Op::rangeNot)
                    .map(rs -> inRange(item, rs))
                    .collect(Collectors.toList());
            return Optional.of(and(inRanges));
        } else {
            return Optional.empty();
        }
    }

    private Optional<Op> reduceCompoundsRecursive() {
        List<Pair<Op, Optional<Op>>> argAlts = getArgs().stream()
                .map(arg -> Pair.create(arg, arg.reduceCompoundsRecursive()))
                .collect(Collectors.toList());
        boolean changed = argAlts.stream().anyMatch(p -> p.second.isPresent());
        List<Op> newArgs = argAlts.stream().map(p -> p.second.orElse(p.first)).collect(Collectors.toList());
        if (isAnd()) {
            return Optional.of(reduceAndComponents(newArgs));
        } else if (isOr()) {
            return Optional.of(reduceOrComponents(newArgs));
        } else if (changed) {
            return Optional.of(replaceArgs(newArgs));
        } else {
            return Optional.empty();
        }
    }

    private Op reduceCompounds() {
        return reduceCompoundsRecursive().orElse(this);
    }

    public Op standardize() {
        if (isVar() || isVal()) {
            return this;
        }
        return lowerNot().reduceCompounds();
    }

    public Op not() {
        return this.toNegative();
    }

    public Op complement() {
        return this.toFalseOrNull();
    }

    public List<Op> collect(Predicate<Op> pred) {
        List<Op> opList = Lists.newArrayList();
        collectImpl(pred, opList);
        return opList;
    }

    private void collectImpl(Predicate<Op> pred, List<Op> opList) {
        getArgs().forEach(arg -> arg.collectImpl(pred, opList));
        if (pred.test(this)) {
            opList.add(this);
        }
    }
}
