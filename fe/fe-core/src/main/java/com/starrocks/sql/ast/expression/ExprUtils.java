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

package com.starrocks.sql.ast.expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.AnalysisException;
import com.starrocks.planner.SlotId;
import com.starrocks.planner.TupleId;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.plan.ScalarOperatorToExpr;
import com.starrocks.type.InvalidType;
import com.starrocks.type.Type;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

public class ExprUtils {
    public static final float FUNCTION_CALL_COST = 10;

    private static final Set<String> TIMESTAMP_ADD_FUNCTIONS = ImmutableSet.of(
            FunctionSet.DATE_ADD, FunctionSet.ADDDATE, FunctionSet.DAYS_ADD, FunctionSet.TIMESTAMPADD);
    private static final Set<String> TIMESTAMP_SUB_FUNCTIONS = ImmutableSet.of(
            FunctionSet.DATE_SUB, FunctionSet.SUBDATE, FunctionSet.DAYS_SUB);

    // Returns true if an Expr is a NOT CompoundPredicate.
    public static final com.google.common.base.Predicate<Expr> IS_NOT_PREDICATE =
            new com.google.common.base.Predicate<Expr>() {
                @Override
                public boolean apply(Expr arg) {
                    return arg instanceof CompoundPredicate &&
                            ((CompoundPredicate) arg).getOp() == CompoundPredicate.Operator.NOT;
                }
            };
    public static final com.google.common.base.Predicate<Expr> IS_NULL_LITERAL =
            new com.google.common.base.Predicate<Expr>() {
                @Override
                public boolean apply(Expr arg) {
                    return arg instanceof NullLiteral;
                }
            };
    public static final com.google.common.base.Predicate<Expr> IS_VARCHAR_SLOT_REF_IMPLICIT_CAST =
            new com.google.common.base.Predicate<Expr>() {
                @Override
                public boolean apply(Expr arg) {
                    // exclude explicit cast
                    // like set(t2=cast(k4 as datetime)) in load stmt
                    if (!ExprUtils.isImplicitCast(arg)) {
                        return false;
                    }
                    List<Expr> children = arg.getChildren();
                    if (children.isEmpty()) {
                        return false;
                    }
                    Expr child = children.get(0);
                    if (child instanceof SlotRef && child.getType().isVarchar()) {
                        return true;
                    }
                    return false;
                }
            };
    // returns true if an Expr is a non-analytic aggregate.
    static final com.google.common.base.Predicate<Expr> IS_AGGREGATE_PREDICATE =
            new com.google.common.base.Predicate<Expr>() {
                public boolean apply(Expr arg) {
                    return arg instanceof FunctionCallExpr &&
                            ((FunctionCallExpr) arg).isAggregateFunction();
                }
            };

    public static RoaringBitmap getUsedSlotIds(Expr expr) {
        RoaringBitmap usedSlotIds = new RoaringBitmap();
        List<SlotRef> slotRefs = Lists.newArrayList();
        expr.collect(SlotRef.class, slotRefs);
        slotRefs.stream().map(SlotRef::getSlotId).map(SlotId::asInt).forEach(usedSlotIds::add);
        return usedSlotIds;
    }

    public static Optional<Expr> replaceLargeStringLiteralImpl(Expr expr) {
        if (expr instanceof LargeStringLiteral) {
            return Optional.of(new StringLiteral(((LargeStringLiteral) expr).getValue()));
        }
        List<Expr> children = expr.getChildren();
        if (children == null || children.isEmpty()) {
            return Optional.empty();
        }
        List<Expr> newChildren = new ArrayList<>(children.size());
        boolean hasReplacement = false;
        for (Expr child : children) {
            Optional<Expr> replaced = replaceLargeStringLiteralImpl(child);
            if (replaced.isPresent()) {
                hasReplacement = true;
                newChildren.add(replaced.get());
            } else {
                newChildren.add(child);
            }
        }
        if (!hasReplacement) {
            return Optional.empty();
        }
        for (int i = 0; i < newChildren.size(); i++) {
            expr.setChild(i, newChildren.get(i));
        }
        return Optional.of(expr);
    }

    public static Expr replaceLargeStringLiteral(Expr expr) {
        return replaceLargeStringLiteralImpl(expr).orElse(expr);
    }

    public static boolean isLiteral(Expr expr) {
        return expr instanceof LiteralExpr;
    }

    public static SlotRef unwrapSlotRef(Expr expr) {
        if (expr instanceof SlotRef) {
            return (SlotRef) expr;
        } else if (expr instanceof CastExpr && expr.getChild(0) instanceof SlotRef) {
            return (SlotRef) expr.getChild(0);
        } else {
            return null;
        }
    }

    public static List<SlotRef> collectAllSlotRefs(Expr expr) {
        return collectAllSlotRefs(expr, false);
    }

    public static List<SlotRef> collectAllSlotRefs(Expr expr, boolean distinct) {
        Collection<SlotRef> result = distinct ? new LinkedHashSet<>() : Lists.newArrayList();
        Queue<Expr> queue = Lists.newLinkedList();
        queue.add(expr);
        while (!queue.isEmpty()) {
            Expr head = queue.poll();
            if (head instanceof SlotRef) {
                result.add((SlotRef) head);
            }
            queue.addAll(head.getChildren());
        }
        return distinct ? Lists.newArrayList(result) : (List<SlotRef>) result;
    }

    public static boolean isImplicitCast(Expr expr) {
        return expr instanceof CastExpr && ((CastExpr) expr).isImplicit();
    }

    public static boolean isBoundByTupleIds(Expr expr, List<TupleId> tupleIds) {
        Preconditions.checkNotNull(expr, "expression cannot be null");
        Preconditions.checkNotNull(tupleIds, "tuple ids cannot be null");
        if (expr instanceof SlotRef) {
            return isSlotRefBoundByTupleIds((SlotRef) expr, tupleIds);
        }
        for (Expr child : expr.getChildren()) {
            if (!isBoundByTupleIds(child, tupleIds)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isSlotRefBoundByTupleIds(SlotRef slotRef, List<TupleId> tupleIds) {
        Preconditions.checkState(slotRef.getDesc() != null, "slot descriptor is null");
        if (slotRef.isFromLambda()) {
            return true;
        }
        for (TupleId tupleId : tupleIds) {
            if (tupleId.equals(slotRef.getDesc().getParent().getId())) {
                return true;
            }
        }
        return false;
    }

    public static boolean isBound(Expr expr, SlotId slotId) {
        Preconditions.checkNotNull(expr, "expression cannot be null");
        Preconditions.checkNotNull(slotId, "slot id cannot be null");
        if (expr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) expr;
            Preconditions.checkState(slotRef.isAnalyzed(), "slot ref is not analyzed");
            Preconditions.checkNotNull(slotRef.getDesc(), "slot descriptor is null");
            return slotRef.getDesc().getId().equals(slotId);
        }
        for (Expr child : expr.getChildren()) {
            if (!isBound(child, slotId)) {
                return false;
            }
        }
        return true;
    }

    public static Expr compoundAnd(Collection<Expr> conjuncts) {
        return createCompound(CompoundPredicate.Operator.AND, conjuncts);
    }

    // Build a compound tree by bottom up
    //
    // Example: compoundType.OR
    // Initial state:
    //  a b c d e
    //
    // First iteration:
    //  or    or
    //  /\    /\   e
    // a  b  c  d
    //
    // Second iteration:
    //     or   e
    //    / \
    //  or   or
    //  /\   /\
    // a  b c  d
    //
    // Last iteration:
    //       or
    //      / \
    //     or  e
    //    / \
    //  or   or
    //  /\   /\
    // a  b c  d
    public static Expr createCompound(CompoundPredicate.Operator type, Collection<Expr> nodes) {
        LinkedList<Expr> link =
                nodes.stream().filter(java.util.Objects::nonNull)
                        .collect(Collectors.toCollection(Lists::newLinkedList));

        if (link.size() < 1) {
            return null;
        }
        if (link.size() == 1) {
            return link.get(0);
        }

        while (link.size() > 1) {
            LinkedList<Expr> buffer = Lists.newLinkedList();

            // combine pairs of elements
            while (link.size() >= 2) {
                buffer.add(new CompoundPredicate(type, link.poll(), link.poll()));
            }

            // if there's and odd number of elements, just append the last one
            if (!link.isEmpty()) {
                buffer.add(link.remove());
            }

            link = buffer;
        }

        return link.remove();
    }

    /**
     * Create a deep copy of 'l'. If sMap is non-null, use it to substitute the
     * elements of l.
     */
    public static <C extends Expr> ArrayList<C> cloneList(List<C> l, ExprSubstitutionMap sMap) {
        Preconditions.checkNotNull(l);
        ArrayList<C> result = new ArrayList<C>();
        for (C element : l) {
            C cloned = (C) (sMap == null ? element.clone() : ExprSubstitutionVisitor.rewrite(element, sMap));
            result.add(cloned);
        }
        return result;
    }

    /**
     * Create a deep copy of 'l'. If sMap is non-null, use it to substitute the
     * elements of l.
     */
    public static <C extends Expr> ArrayList<C> cloneList(List<C> l) {
        Preconditions.checkNotNull(l);
        ArrayList<C> result = new ArrayList<C>();
        for (C element : l) {
            result.add((C) element.clone());
        }
        return result;
    }

    public static <C extends Expr> ArrayList<C> cloneAndResetList(List<C> l) {
        Preconditions.checkNotNull(l);
        ArrayList<C> result = new ArrayList<C>();
        for (C element : l) {
            result.add((C) reset(element.clone()));
        }
        return result;
    }

    public static Expr reset(Expr expr) {
        if (expr == null) {
            return null;
        }
        if (expr instanceof GroupingFunctionCallExpr) {
            GroupingFunctionCallExpr grouping = (GroupingFunctionCallExpr) expr;
            if (grouping.isChildrenResetedForReset()) {
                List<Expr> restoredChildren = grouping.getRealChildrenForReset();
                List<Expr> newChildren = restoredChildren == null ? new ArrayList<>() : new ArrayList<>(restoredChildren);
                grouping.setChildrenForReset(newChildren);
                grouping.setChildrenResetedForReset(false);
                grouping.setRealChildrenForReset(null);
            }
        }

        Expr result;
        if (isImplicitCast(expr)) {
            result = reset(expr.getChild(0));
        } else {
            for (int i = 0; i < expr.getChildren().size(); ++i) {
                expr.setChild(i, reset(expr.getChild(i)));
            }
            expr.resetAnalysisState();
            result = expr;
        }

        if (expr instanceof CastExpr) {
            CastExpr castExpr = (CastExpr) expr;
            if (castExpr.isNoOp() && !castExpr.getChild(0).getType().matchesType(castExpr.getType())) {
                castExpr.setNoOpForReset(false);
            }
        }
        return result;
    }

    /**
     * Collect all unique Expr nodes of type 'cl' present in 'input' and add them to
     * 'output' if they do not exist in 'output'.
     * This can't go into TreeNode<>, because we'd be using the template param
     * NodeType.
     */
    public static <C extends Expr> void collectList(List<? extends Expr> input, Class<C> cl,
                                                    List<C> output) {
        Preconditions.checkNotNull(input);
        for (Expr e : input) {
            e.collect(cl, output);
        }
    }

    public static Expr analyzeAndCastFold(Expr expr) {
        ExpressionAnalyzer.analyzeExpressionIgnoreSlot(expr, ConnectContext.get());
        // Translating expr to scalar in order to do some rewrites
        try {
            ScalarOperator scalarOperator = SqlToScalarOperatorTranslator.translate(expr);
            ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
            // Add cast and constant fold
            scalarOperator = scalarRewriter.rewrite(scalarOperator, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
            return ScalarOperatorToExpr.buildExprIgnoreSlot(scalarOperator,
                    new ScalarOperatorToExpr.FormatterContext(Maps.newHashMap()));
        } catch (UnsupportedException e) {
            return expr;
        }
    }

    public static Expr analyzeLoadExpr(Expr expr, java.util.function.Function<SlotRef, ColumnRefOperator> slotResolver) {
        ExpressionAnalyzer.analyzeExpressionIgnoreSlot(expr, ConnectContext.get());
        // Translating expr to scalar in order to do some rewrites
        try {
            ScalarOperator scalarOperator = SqlToScalarOperatorTranslator.translateLoadExpr(expr, slotResolver);
            ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
            // Add cast and constant fold
            scalarOperator = scalarRewriter.rewrite(scalarOperator, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
            return ScalarOperatorToExpr.buildExprIgnoreSlot(scalarOperator,
                    new ScalarOperatorToExpr.FormatterContext(Maps.newHashMap()));
        } catch (UnsupportedException e) {
            return expr;
        }
    }

    public static boolean containsDictMappingExpr(Expr expr) {
        if (expr instanceof DictMappingExpr) {
            return true;
        }
        return expr.getChildren().stream().anyMatch(child -> containsDictMappingExpr(child));
    }

    public static Function getBuiltinFunction(String name, Type[] argTypes, Function.CompareMode mode) {
        FunctionName fnName = new FunctionName(name);
        Function searchDesc = new Function(fnName, argTypes, InvalidType.INVALID, false);
        return GlobalStateMgr.getCurrentState().getFunction(searchDesc, mode);
    }

    public static Function getBuiltinFunction(String name, Type[] argTypes, String[] argNames, Function.CompareMode mode) {
        if (argNames == null) {
            return getBuiltinFunction(name, argTypes, mode);
        }
        FunctionName fnName = new FunctionName(name);
        Function searchDesc = new Function(fnName, argTypes, argNames, InvalidType.INVALID, false);
        return GlobalStateMgr.getCurrentState().getFunction(searchDesc, mode);
    }

    public static Function getBuiltinFunction(String name, Type[] argTypes, boolean varArgs, Type retType,
                                              Function.CompareMode mode) {
        FunctionName fnName = new FunctionName(name);
        Function searchDesc = new Function(fnName, argTypes, retType, varArgs);
        return GlobalStateMgr.getCurrentState().getFunction(searchDesc, mode);
    }

    public static boolean requiresTimestampDiffCast(String funcName) {
        if (funcName == null) {
            return false;
        }
        return !TIMESTAMP_ADD_FUNCTIONS.contains(funcName) && !TIMESTAMP_SUB_FUNCTIONS.contains(funcName);
    }

    public static String getTimestampArithmeticFunctionName(TimestampArithmeticExpr expr) {
        Preconditions.checkNotNull(expr.getTimeUnitIdent(), "time unit identifier cannot be null");
        if (expr.getFuncName() != null) {
            if (TIMESTAMP_ADD_FUNCTIONS.contains(expr.getFuncName())) {
                return formatTimestampFunction(expr.getTimeUnitIdent(), "add");
            } else if (TIMESTAMP_SUB_FUNCTIONS.contains(expr.getFuncName())) {
                return formatTimestampFunction(expr.getTimeUnitIdent(), "sub");
            } else {
                return formatTimestampFunction(expr.getTimeUnitIdent(), "diff");
            }
        }
        String suffix = expr.getOp() == ArithmeticExpr.Operator.ADD ? "add" : "sub";
        return formatTimestampFunction(expr.getTimeUnitIdent(), suffix);
    }

    public static Function getTimestampArithmeticFunction(TimestampArithmeticExpr expr) {
        String funcOpName = getTimestampArithmeticFunctionName(expr);
        Type[] argumentTypes = expr.getChildren().stream().map(Expr::getType).toArray(Type[]::new);
        return getBuiltinFunction(funcOpName.toLowerCase(), argumentTypes,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    }

    private static String formatTimestampFunction(String timeUnitIdent, String suffix) {
        return String.format("%sS_%s", timeUnitIdent, suffix);
    }

    public static boolean containsSlotRef(Expr root) {
        if (root == null) {
            return false;
        }
        if (root instanceof SlotRef) {
            return true;
        }
        for (Expr child : root.getChildren()) {
            if (containsSlotRef(child)) {
                return true;
            }
        }
        return false;
    }

    public static Expr compoundOr(Collection<Expr> conjuncts) {
        return createCompound(CompoundPredicate.Operator.OR, conjuncts);
    }

    /**
     * get the expr which in l1 and l2 in the same time.
     * Return the intersection of l1 and l2
     */
    public static <C extends Expr> List<C> intersect(List<C> l1, List<C> l2) {
        List<C> result = new ArrayList<C>();

        for (C element : l1) {
            if (l2.contains(element)) {
                result.add(element);
            }
        }

        return result;
    }

    public static com.google.common.base.Predicate<Expr> isAggregatePredicate() {
        return IS_AGGREGATE_PREDICATE;
    }

    public static boolean isAggregate(Expr expr) {
        return IS_AGGREGATE_PREDICATE.apply(expr);
    }

    /**
     * Pushes negation to the individual operands of a predicate
     * tree rooted at 'root'.
     */
    // @Todo: Remove the dependence of CBO Optimizer on this method.
    //    At present, we transform SubqueryExpr to ApplyNode direct(SubqueryTransformer), it's need do eliminate
    //    negations on Expr not ScalarOperator
    public static Expr pushNegationToOperands(Expr root) {
        Preconditions.checkNotNull(root);
        if (IS_NOT_PREDICATE.apply(root)) {
            // Make sure we call function 'negate' only on classes that support it,
            // otherwise we may recurse infinitely.
            if (ExprNegateFunction.isSupportNegate(root.getChild(0))) {
                return pushNegationToOperands(ExprNegateFunction.negate(root.getChild(0)));
            } else {
                return root;
            }
        }

        if (root instanceof CompoundPredicate) {
            Expr left = pushNegationToOperands(root.getChild(0));
            Expr right = pushNegationToOperands(root.getChild(1));
            CompoundPredicate compoundPredicate =
                    new CompoundPredicate(((CompoundPredicate) root).getOp(), left, right);
            compoundPredicate.setPrintSqlInParens(root.getPrintSqlInParens());
            return compoundPredicate;
        }

        return root;
    }

    // only the first/last one can be lambda functions.
    public static boolean hasLambdaFunction(Expr expression) {
        int idx = -1;
        int num = 0;
        List<Expr> children = expression.getChildren();
        for (int i = 0; i < children.size(); ++i) {
            if (children.get(i) instanceof LambdaFunctionExpr) {
                num++;
                idx = i;
            }
        }
        if (num == 1 && (idx == 0 || idx == children.size() - 1)) {
            if (children.size() <= 1) {
                throw new SemanticException("Lambda functions need array/map inputs in high-order functions");
            }
            return true;
        } else if (num > 1) {
            throw new SemanticException("A high-order function should have only 1 lambda function, " +
                    "but there are " + num + " lambda functions");
        } else if (idx > 0 && idx < children.size() - 1) {
            throw new SemanticException(
                    "Lambda functions should only be the first or last argument of any high-order function, " +
                            "or lambda arguments should be in () if there are more than one lambda arguments, " +
                            "like (x,y)->x+y");
        } else if (num == 0) {
            if (expression instanceof FunctionCallExpr) {
                String funcName = ((FunctionCallExpr) expression).getFnName().getFunction();
                if (funcName.equals(FunctionSet.ARRAY_MAP) || funcName.equals(FunctionSet.TRANSFORM) ||
                        funcName.equals(FunctionSet.MAP_APPLY)) {
                    throw new SemanticException("There are no lambda functions in high-order function " + funcName);
                }
            }
        }
        return false;
    }

    public static double getConstFromExpr(Expr e) throws AnalysisException {
        Preconditions.checkState(e.isConstant());
        double value;
        if (e instanceof UserVariableExpr) {
            e = ((UserVariableExpr) e).getValue();
        }

        if (e instanceof LiteralExpr) {
            LiteralExpr lit = (LiteralExpr) e;
            value = lit.getDoubleValue();
        } else {
            throw new AnalysisException("To const value not a LiteralExpr ");
        }
        return value;
    }

    /**
     * Collects the returns types of the child nodes in an array.
     */
    protected static Type[] collectChildReturnTypes(Expr expr) {
        List<Expr> children = expr.getChildren();
        Type[] childTypes = new Type[children.size()];
        for (int i = 0; i < children.size(); ++i) {
            childTypes[i] = children.get(i).type;
        }
        return childTypes;
    }

    /**
     * Returns true if the list contains an aggregate expr.
     */
    public static <C extends Expr> boolean containsAggregate(List<? extends Expr> input) {
        for (Expr e : input) {
            if (containsAggregate(e)) {
                return true;
            }
        }
        return false;
    }

    public static boolean containsAggregate(Expr expr) {
        if (isAggregate(expr)) {
            return true;
        }
        return containsAggregate(expr.getChildren());
    }
}
