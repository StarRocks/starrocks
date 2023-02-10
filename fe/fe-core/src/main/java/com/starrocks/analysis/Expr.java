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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/Expr.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.TreeNode;
import com.starrocks.common.io.Writable;
import com.starrocks.planner.FragmentNormalizer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.LambdaFunctionExpr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.plan.ScalarOperatorToExpr;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprOpcode;
import com.starrocks.thrift.TFunction;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Root of the expr node hierarchy.
 */
abstract public class Expr extends TreeNode<Expr> implements ParseNode, Cloneable, Writable {
    // Name of the function that needs to be implemented by every Expr that
    // supports negation.
    private static final String NEGATE_FN = "negate";

    // to be used where we can't come up with a better estimate
    protected static final double DEFAULT_SELECTIVITY = 0.1;

    public static final float FUNCTION_CALL_COST = 10;

    // returns true if an Expr is a non-analytic aggregate.
    private static final com.google.common.base.Predicate<Expr> IS_AGGREGATE_PREDICATE =
            new com.google.common.base.Predicate<Expr>() {
                public boolean apply(Expr arg) {
                    return arg instanceof FunctionCallExpr &&
                            ((FunctionCallExpr) arg).isAggregateFunction();
                }
            };

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

    public static final com.google.common.base.Predicate<Expr> IS_LITERAL =
            new com.google.common.base.Predicate<Expr>() {
                @Override
                public boolean apply(Expr arg) {
                    return arg instanceof LiteralExpr;
                }
            };

    public static final com.google.common.base.Predicate<Expr> IS_VARCHAR_SLOT_REF_IMPLICIT_CAST =
            new com.google.common.base.Predicate<Expr>() {
                @Override
                public boolean apply(Expr arg) {
                    // exclude explicit cast
                    // like set(t2=cast(k4 as datetime)) in load stmt
                    if (!arg.isImplicitCast()) {
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

    // id that's unique across the entire query statement and is assigned by
    // Analyzer.registerConjuncts(); only assigned for the top-level terms of a
    // conjunction, and therefore null for most Exprs
    protected ExprId id;

    // true if Expr is an auxiliary predicate that was generated by the plan generation
    // process to facilitate predicate propagation;
    // false if Expr originated with a query stmt directly
    private boolean isAuxExpr = false;

    protected Type type;  // result of analysis

    // only for query result set metadata
    // set if related to an actual column
    protected Type originType;

    protected boolean isOnClauseConjunct_; // set by analyzer

    protected boolean isAnalyzed = false;  // true after analyze() has been called

    protected TExprOpcode opcode;  // opcode for this expr
    protected TExprOpcode vectorOpcode;  // vector opcode for this expr

    // estimated probability of a predicate evaluating to true;
    // set during analysis;
    // between 0 and 1 if valid: invalid: -1
    protected double selectivity;

    // estimated number of distinct values produced by Expr; invalid: -1
    // set during analysis
    protected long numDistinctValues;

    protected int outputScale = -1;

    protected int outputColumn = -1;

    protected boolean isFilter = false;

    // The function to call. This can either be a scalar or aggregate function.
    // Set in analyze().
    protected Function fn;

    // Ignore nulls.
    private boolean ignoreNulls = false;

    // Cached value of IsConstant(), set during analyze() and valid if isAnalyzed_ is true.
    private boolean isConstant_;

    // Flag to indicate whether to wrap this expr's toSql() in parenthesis. Set by parser.
    // Needed for properly capturing expr precedences in the SQL string.
    protected boolean printSqlInParens = false;

    protected Expr() {
        super();
        type = Type.INVALID;
        originType = Type.INVALID;
        opcode = TExprOpcode.INVALID_OPCODE;
        vectorOpcode = TExprOpcode.INVALID_OPCODE;
        selectivity = -1.0;
        numDistinctValues = -1;
    }

    protected Expr(Expr other) {
        super();
        id = other.id;
        isAuxExpr = other.isAuxExpr;
        type = other.type;
        originType = other.type;
        isAnalyzed = other.isAnalyzed;
        selectivity = other.selectivity;
        numDistinctValues = other.numDistinctValues;
        opcode = other.opcode;
        isConstant_ = other.isConstant_;
        fn = other.fn;
        printSqlInParens = other.printSqlInParens;
        children = Expr.cloneList(other.children);
    }

    @SuppressWarnings("unchecked")
    public <T> T cast() {
        return (T) this;
    }

    public boolean isAnalyzed() {
        return isAnalyzed;
    }

    public ExprId getId() {
        return id;
    }

    public Type getType() {
        return type;
    }

    // add by cmy. for restoring
    public void setType(Type type) {
        this.type = type;
    }

    // return member type if there is no actual column with this
    public Type getOriginType() {
        if (originType == null || !originType.isValid()) {
            return type;
        }
        return originType;
    }

    // Used to differ from getOriginType(), return originType directly.
    public Type getTrueOriginType() {
        return originType;
    }

    public void setOriginType(Type originType) {
        this.originType = originType;
    }

    public TExprOpcode getOpcode() {
        return opcode;
    }

    public long getNumDistinctValues() {
        return numDistinctValues;
    }

    public int getOutputScale() {
        return outputScale;
    }

    public int getOutputColumn() {
        return outputColumn;
    }

    public boolean isFilter() {
        return isFilter;
    }

    public boolean isAuxExpr() {
        return isAuxExpr;
    }

    public Function getFn() {
        return fn;
    }

    public boolean getPrintSqlInParens() {
        return printSqlInParens;
    }

    public void setPrintSqlInParens(boolean b) {
        printSqlInParens = b;
    }

    /**
     * Perform semantic analysis of node and all of its children.
     * Throws exception if any errors found.
     */
    public final void analyze(Analyzer analyzer) throws AnalysisException {
    }

    /**
     * Does subclass-specific analysis. Subclasses should override analyzeImpl().
     */
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    }

    /**
     * Set the expr to be analyzed and computes isConstant_.
     */
    protected void analysisDone() {
        Preconditions.checkState(!isAnalyzed);
        // We need to compute the const-ness as the last step, since analysis may change
        // the result, e.g. by resolving function.
        isConstant_ = isConstantImpl();
        isAnalyzed = true;
    }

    /**
     * Collects the returns types of the child nodes in an array.
     */
    protected Type[] collectChildReturnTypes() {
        Type[] childTypes = new Type[children.size()];
        for (int i = 0; i < children.size(); ++i) {
            childTypes[i] = children.get(i).type;
        }
        return childTypes;
    }

    public static List<TExpr> treesToThrift(List<? extends Expr> exprs) {
        List<TExpr> result = Lists.newArrayList();
        for (Expr expr : exprs) {
            result.add(expr.treeToThrift());
        }
        return result;
    }

    public static String debugString(List<? extends Expr> exprs) {
        if (exprs == null || exprs.isEmpty()) {
            return "";
        }
        List<String> strings = Lists.newArrayList();
        for (Expr expr : exprs) {
            strings.add(expr.debugString());
        }
        return "(" + Joiner.on(" ").join(strings) + ")";
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

    /**
     * Return true if l1[i].equals(l2[i]) for all i.
     */
    public static <C extends Expr> boolean equalLists(List<C> l1, List<C> l2) {
        if (l1.size() != l2.size()) {
            return false;
        }
        Iterator<C> l1Iter = l1.iterator();
        Iterator<C> l2Iter = l2.iterator();
        while (l1Iter.hasNext()) {
            if (!l1Iter.next().equals(l2Iter.next())) {
                return false;
            }
        }
        return true;
    }

    public void analyzeNoThrow(Analyzer analyzer) {
        try {
            analyze(analyzer);
        } catch (AnalysisException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Gather conjuncts from this expr and return them in a list.
     * A conjunct is an expr that returns a boolean, e.g., Predicates, function calls,
     * SlotRefs, etc. Hence, this method is placed here and not in Predicate.
     */
    public static List<Expr> extractConjuncts(Expr root) {
        List<Expr> conjuncts = Lists.newArrayList();
        if (null == root) {
            return conjuncts;
        }

        extractConjunctsImpl(root, conjuncts);
        return conjuncts;
    }

    private static void extractConjunctsImpl(Expr root, List<Expr> conjuncts) {
        if (!(root instanceof CompoundPredicate)) {
            conjuncts.add(root);
            return;
        }

        CompoundPredicate cpe = (CompoundPredicate) root;
        if (!CompoundPredicate.Operator.AND.equals(cpe.getOp())) {
            conjuncts.add(root);
            return;
        }

        extractConjunctsImpl(cpe.getChild(0), conjuncts);
        extractConjunctsImpl(cpe.getChild(1), conjuncts);
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
            result.add((C) element.clone(sMap));
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
            result.add((C) element.clone().reset());
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

    /**
     * Returns true if the list contains an aggregate expr.
     */
    public static <C extends Expr> boolean containsAggregate(List<? extends Expr> input) {
        for (Expr e : input) {
            if (e.containsAggregate()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns an analyzed clone of 'this' with exprs substituted according to smap.
     * Removes implicit casts and analysis state while cloning/substituting exprs within
     * this tree, such that the returned result has minimal implicit casts and types.
     * Throws if analyzing the post-substitution expr tree failed.
     * If smap is null, this function is equivalent to clone().
     * If preserveRootType is true, the resulting expr tree will be cast if necessary to
     * the type of 'this'.
     */
    public Expr trySubstitute(ExprSubstitutionMap smap, Analyzer analyzer,
                              boolean preserveRootType) throws AnalysisException {
        Expr result = clone();
        // Return clone to avoid removing casts.
        if (smap == null) {
            return result;
        }
        result = result.substituteImpl(smap, analyzer);
        result.analyze(analyzer);
        if (preserveRootType && !type.isInvalid() && !type.equals(result.getType())) {
            result = result.castTo(type);
        }
        return result;
    }

    /**
     * Returns an analyzed clone of 'this' with exprs substituted according to smap.
     * Removes implicit casts and analysis state while cloning/substituting exprs within
     * this tree, such that the returned result has minimal implicit casts and types.
     * Expects the analysis of the post-substitution expr to succeed.
     * If smap is null, this function is equivalent to clone().
     * If preserveRootType is true, the resulting expr tree will be cast if necessary to
     * the type of 'this'.
     *
     * @throws AnalysisException
     */
    public Expr substitute(ExprSubstitutionMap smap, Analyzer analyzer, boolean preserveRootType)
            throws AnalysisException {
        try {
            return trySubstitute(smap, analyzer, preserveRootType);
        } catch (AnalysisException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Failed analysis after expr substitution.", e);
        }
    }

    public static ArrayList<Expr> trySubstituteList(Iterable<? extends Expr> exprs,
                                                    ExprSubstitutionMap smap, Analyzer analyzer,
                                                    boolean preserveRootTypes)
            throws AnalysisException {
        if (exprs == null) {
            return null;
        }
        ArrayList<Expr> result = new ArrayList<Expr>();
        for (Expr e : exprs) {
            result.add(e.trySubstitute(smap, analyzer, preserveRootTypes));
        }
        return result;
    }

    public static ArrayList<Expr> substituteList(
            Iterable<? extends Expr> exprs,
            ExprSubstitutionMap smap, Analyzer analyzer, boolean preserveRootTypes) {
        try {
            return trySubstituteList(exprs, smap, analyzer, preserveRootTypes);
        } catch (Exception e) {
            throw new IllegalStateException("Failed analysis after expr substitution.", e);
        }
    }

    /**
     * Recursive method that performs the actual substitution for try/substitute() while
     * removing implicit casts. Resets the analysis state in all non-SlotRef expressions.
     * Exprs that have non-child exprs which should be affected by substitutions must
     * override this method and apply the substitution to such exprs as well.
     */
    protected Expr substituteImpl(ExprSubstitutionMap smap, Analyzer analyzer)
            throws AnalysisException {
        if (isImplicitCast()) {
            return getChild(0).substituteImpl(smap, analyzer);
        }
        if (smap != null) {
            Expr substExpr = smap.get(this);
            if (substExpr != null) {
                return substExpr.clone();
            }
        }
        for (int i = 0; i < children.size(); ++i) {
            children.set(i, children.get(i).substituteImpl(smap, analyzer));
        }
        // SlotRefs must remain analyzed to support substitution across query blocks. All
        // other exprs must be analyzed again after the substitution to add implicit casts
        // and for resolving their correct function signature.
        if (!(this instanceof SlotRef)) {
            resetAnalysisState();
        }
        return this;
    }

    /**
     * Removes duplicate exprs (according to equals()).
     */
    public static <C extends Expr> void removeDuplicates(List<C> l) {
        if (l == null) {
            return;
        }
        ListIterator<C> it1 = l.listIterator();
        while (it1.hasNext()) {
            C e1 = it1.next();
            ListIterator<C> it2 = l.listIterator();
            boolean duplicate = false;
            while (it2.hasNext()) {
                C e2 = it2.next();
                if (e1 == e2) {
                    // only check up to but excluding e1
                    break;
                }
                if (e1.equals(e2)) {
                    duplicate = true;
                    break;
                }
            }
            if (duplicate) {
                it1.remove();
            }
        }
    }

    public String toSql() {
        return (printSqlInParens) ? "(" + toSqlImpl() + ")" : toSqlImpl();
    }

    public String explain() {
        return (printSqlInParens) ? "(" + explainImpl() + ")" : explainImpl();
    }

    /**
     * Returns a SQL string representing this expr. Subclasses should override this method
     * instead of toSql() to ensure that parenthesis are properly added around the toSql().
     */
    protected String toSqlImpl() {
        throw new StarRocksPlannerException("Not implement toSqlImpl function", ErrorType.INTERNAL_ERROR);
    }

    protected String explainImpl() {
        return toSqlImpl();
    }

    public String toMySql() {
        return toSql();
    }

    public String toJDBCSQL(boolean isMySQL) {
        return toSql();
    }

    // Convert this expr, including all children, to its Thrift representation.
    public TExpr treeToThrift() {
        TExpr result = new TExpr();
        treeToThriftHelper(result, Expr::toThrift);
        return result;
    }

    public void toNormalForm(TExprNode tExprNode, FragmentNormalizer normalizer) {
        this.toThrift(tExprNode);
    }

    public TExpr normalize(FragmentNormalizer normalizer) {
        TExpr result = new TExpr();
        treeToThriftHelper(result, (expr, texprNode) -> expr.toNormalForm(texprNode, normalizer));
        return result;
    }

    public interface ExprVisitor {
        void visit(Expr expr, TExprNode texprNode);
    }

    // Append a flattened version of this expr, including all children, to 'container'.
    final void treeToThriftHelper(TExpr container, ExprVisitor visitor) {
        TExprNode msg = new TExprNode();

        Preconditions.checkState(!type.isNull(), "NULL_TYPE is illegal in thrift stage");
        Preconditions.checkState(!Objects.equal(Type.ARRAY_NULL, type), "Array<NULL_TYPE> is illegal in thrift stage");

        msg.type = type.toThrift();
        msg.num_children = children.size();
        msg.setHas_nullable_child(hasNullableChild());
        msg.setIs_nullable(isNullable());
        if (fn != null) {
            TFunction tfn = fn.toThrift();
            tfn.setIgnore_nulls(getIgnoreNulls());
            msg.setFn(tfn);
            if (fn.hasVarArgs()) {
                msg.setVararg_start_idx(fn.getNumArgs() - 1);
            }
        }
        msg.output_scale = getOutputScale();
        msg.setIs_monotonic(isMonotonic());
        visitor.visit(this, msg);
        container.addToNodes(msg);
        for (Expr child : children) {
            child.treeToThriftHelper(container, visitor);
        }
    }

    // Convert this expr into msg (excluding children), which requires setting
    // msg.op as well as the expr-specific field.
    protected void toThrift(TExprNode msg) {
        throw new StarRocksPlannerException("Not implement toThrift function", ErrorType.INTERNAL_ERROR);
    }

    public List<String> childrenToSql() {
        List<String> result = Lists.newArrayList();
        for (Expr child : children) {
            result.add(child.toSql());
        }
        return result;
    }

    public static com.google.common.base.Predicate<Expr> isAggregatePredicate() {
        return IS_AGGREGATE_PREDICATE;
    }

    public boolean isAggregate() {
        return IS_AGGREGATE_PREDICATE.apply(this);
    }

    public String debugString() {
        return debugString(children);
    }

    /**
     * Resets the internal state of this expr produced by analyze().
     * Only modifies this expr, and not its child exprs.
     */
    protected void resetAnalysisState() {
        isAnalyzed = false;
    }

    /**
     * Resets the internal analysis state of this expr tree. Removes implicit casts.
     */
    public Expr reset() {
        if (isImplicitCast()) {
            return getChild(0).reset();
        }
        for (int i = 0; i < children.size(); ++i) {
            children.set(i, children.get(i).reset());
        }
        resetAnalysisState();
        return this;
    }

    public static ArrayList<Expr> resetList(ArrayList<Expr> l) {
        for (int i = 0; i < l.size(); ++i) {
            l.set(i, l.get(i).reset());
        }
        return l;
    }

    /**
     * Creates a deep copy of this expr including its analysis state. The method is
     * abstract in this class to force new Exprs to implement it.
     */
    @Override
    public abstract Expr clone();

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != this.getClass()) {
            return false;
        }
        // don't compare type, this could be called pre-analysis
        Expr expr = (Expr) obj;
        if (children.size() != expr.children.size()) {
            return false;
        }
        for (int i = 0; i < children.size(); ++i) {
            if (!children.get(i).equals(expr.children.get(i))) {
                return false;
            }
        }
        if (fn == null && expr.fn == null) {
            return true;
        }
        if (fn == null || expr.fn == null) {
            return false;
        }
        // Both fn_'s are not null
        return fn.equals(expr.fn);
    }

    @Override
    public int hashCode() {
        // in group by clause, group by list need to remove duplicate exprs, the expr may be not not analyzed, the id
        // may be null.
        // NOTE that all the types of the related member variables must implement hashCode() and equals().
        if (id == null) {
            int result = 31 * Objects.hashCode(type) + Objects.hashCode(opcode);
            for (Expr child : children) {
                result = 31 * result + Objects.hashCode(child);
            }
            return result;
        }
        return id.asInt();
    }

    /**
     * Create a deep copy of 'this'. If sMap is non-null,
     * use it to substitute 'this' or its subnodes.
     * <p/>
     * Expr subclasses that add non-value-type members must override this.
     */
    public Expr clone(ExprSubstitutionMap sMap) {
        if (sMap != null) {
            for (int i = 0; i < sMap.getLhs().size(); ++i) {
                if (this.equals(sMap.getLhs().get(i))) {
                    return sMap.getRhs().get(i).clone(null);
                }
            }
        }
        Expr result = (Expr) this.clone();
        result.children = Lists.newArrayList();
        for (Expr child : children) {
            result.children.add(((Expr) child).clone(sMap));
        }
        return result;
    }

    public boolean containsAggregate() {
        if (isAggregate()) {
            return true;
        }
        return containsAggregate(children);
    }

    /**
     * Return 'this' with all sub-exprs substituted according to
     * sMap. Ids of 'this' and its children are retained.
     */
    @Deprecated
    public Expr substitute(ExprSubstitutionMap sMap) {
        Preconditions.checkNotNull(sMap);
        for (int i = 0; i < sMap.getLhs().size(); ++i) {
            if (this.equals(sMap.getLhs().get(i))) {
                Expr result = sMap.getRhs().get(i).clone(null);
                if (id != null) {
                    result.id = id;
                }
                return result;
            }
        }
        for (int i = 0; i < children.size(); ++i) {
            children.set(i, children.get(i).substitute(sMap));
        }
        return this;
    }

    /**
     * Returns true if expr is fully bound by tids, otherwise false.
     */
    public boolean isBoundByTupleIds(List<TupleId> tids) {
        for (Expr child : children) {
            if (!child.isBoundByTupleIds(tids)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true if expr is fully bound by slotId, otherwise false.
     */
    public boolean isBound(SlotId slotId) {
        for (Expr child : children) {
            if (!child.isBound(slotId)) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return true if this is an instance of LiteralExpr
     */
    public boolean isLiteral() {
        return this instanceof LiteralExpr;
    }

    /**
     * Returns true if this expression should be treated as constant. I.e. if the frontend
     * and backend should assume that two evaluations of the expression within a query will
     * return the same value. Examples of constant expressions include:
     * - Literal values like 1, "foo", or NULL
     * - Deterministic operators applied to constant arguments, e.g. 1 + 2, or
     * concat("foo", "bar")
     * - Functions that should be always return the same value within a query but may
     * return different values for different queries. E.g. now(), which we want to
     * evaluate only once during planning.
     * May incorrectly return true if the expression is not analyzed.
     * TODO: isAnalyzed_ should be a precondition for isConstant(), since it is not always
     * possible to correctly determine const-ness before analysis (e.g. see
     * FunctionCallExpr.isConstant()).
     */
    public final boolean isConstant() {
        if (isAnalyzed) {
            return isConstant_;
        }
        return isConstantImpl();
    }

    /**
     * Implements isConstant() - computes the value without using 'isConstant_'.
     */
    protected boolean isConstantImpl() {
        for (Expr expr : children) {
            if (!expr.isConstant()) {
                return false;
            }
        }
        return true;
    }

    public boolean hasNullableChild() {
        for (Expr expr : children) {
            if (expr.isNullable()) {
                return true;
            }
        }
        return false;
    }

    // Whether the expr itself is nullable
    public boolean isNullable() {
        return true;
    }

    /**
     * Checks validity of cast, and
     * calls uncheckedCastTo() to
     * create a cast expression that casts
     * this to a specific type.
     *
     * @param targetType type to be cast to
     * @return cast expression, or converted literal,
     * should never return null
     * @throws AnalysisException when an invalid cast is asked for, for example,
     *                           failure to convert a string literal to a date literal
     */
    public final Expr castTo(Type targetType) throws AnalysisException {
        // If the targetType is NULL_TYPE then ignore the cast because NULL_TYPE
        // is compatible with all types and no cast is necessary.
        if (targetType.isNull()) {
            return this;
        }

        if (targetType.isHllType() && this.type.isStringType()) {
            return this;
        }
        if (!Type.canCastTo(this.type, targetType)) {
            throw new AnalysisException("Cannot cast '" + this.toSql() + "' from " + this.type + " to " + targetType);
        }
        return uncheckedCastTo(targetType);
    }

    /**
     * Create an expression equivalent to 'this' but returning targetType;
     * possibly by inserting an implicit cast,
     * or by returning an altogether new expression
     *
     * @param targetType type to be cast to
     * @return cast expression, or converted literal,
     * should never return null
     * @throws AnalysisException when an invalid cast is asked for, for example,
     *                           failure to convert a string literal to a date literal
     */
    public Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        return new CastExpr(targetType, this);
    }

    /**
     * Add a cast expression above child.
     * If child is a literal expression, we attempt to
     * convert the value of the child directly, and not insert a cast node.
     *
     * @param targetType type to be cast to
     * @param childIndex index of child to be cast
     */
    public void castChild(Type targetType, int childIndex) throws AnalysisException {
        Expr child = getChild(childIndex);
        Expr newChild = child.castTo(targetType);
        setChild(childIndex, newChild);
    }

    /**
     * Add a cast expression above child.
     * If child is a literal expression, we attempt to
     * convert the value of the child directly, and not insert a cast node.
     *
     * @param targetType type to be cast to
     * @param childIndex index of child to be cast
     */
    public void uncheckedCastChild(Type targetType, int childIndex)
            throws AnalysisException {
        Expr child = getChild(childIndex);
        Expr newChild = child.uncheckedCastTo(targetType);
        setChild(childIndex, newChild);
    }

    /**
     * Returns child expr if this expr is an implicit cast, otherwise returns 'this'.
     */
    public Expr ignoreImplicitCast() {
        return this;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this.getClass()).add("id", id).add("type", type).add("sel",
                selectivity).add("#distinct", numDistinctValues).add("scale", outputScale).toString();
    }

    /**
     * If 'this' is a SlotRef or a Cast that wraps a SlotRef, returns that SlotRef.
     * Otherwise returns null.
     */
    public SlotRef unwrapSlotRef() {
        if (this instanceof SlotRef) {
            return (SlotRef) this;
        } else if (this instanceof CastExpr && getChild(0) instanceof SlotRef) {
            return (SlotRef) getChild(0);
        } else {
            return null;
        }
    }

    /**
     * If 'this' is a SlotRef or a Cast that wraps a SlotRef, returns that SlotRef.
     * Otherwise returns null.
     */
    public SlotRef unwrapSlotRef(boolean implicitOnly) {
        Expr unwrappedExpr = unwrapExpr(implicitOnly);
        if (unwrappedExpr instanceof SlotRef) {
            return (SlotRef) unwrappedExpr;
        }
        return null;
    }

    /**
     * Returns the first child if this Expr is a CastExpr. Otherwise, returns 'this'.
     */
    public Expr unwrapExpr(boolean implicitOnly) {
        if (this instanceof CastExpr
                && (!implicitOnly || ((CastExpr) this).isImplicit())) {
            return children.get(0);
        }
        return this;
    }

    public static double getConstFromExpr(Expr e) throws AnalysisException {
        Preconditions.checkState(e.isConstant());
        double value = 0;
        if (e instanceof LiteralExpr) {
            LiteralExpr lit = (LiteralExpr) e;
            value = lit.getDoubleValue();
        } else {
            throw new AnalysisException("To const value not a LiteralExpr ");
        }
        return value;
    }

    public boolean isImplicitCast() {
        return this instanceof CastExpr && ((CastExpr) this).isImplicit();
    }

    public static Function getBuiltinFunction(String name, Type[] argTypes, Function.CompareMode mode) {
        FunctionName fnName = new FunctionName(name);
        Function searchDesc = new Function(fnName, argTypes, Type.INVALID, false);
        return GlobalStateMgr.getCurrentState().getFunction(searchDesc, mode);
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
        if (Expr.IS_NOT_PREDICATE.apply(root)) {
            try {
                // Make sure we call function 'negate' only on classes that support it,
                // otherwise we may recurse infinitely.
                Method m = root.getChild(0).getClass().getDeclaredMethod(NEGATE_FN);
                return pushNegationToOperands(root.getChild(0).negate());
            } catch (NoSuchMethodException|IllegalStateException e) {
                // The 'negate' function is not implemented. Break the recursion.
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

    /**
     * Negates a boolean Expr.
     */
    public Expr negate() {
        return new CompoundPredicate(CompoundPredicate.Operator.NOT, this, null);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new IOException("Not implemented serializable ");
    }

    public void readFields(DataInput in) throws IOException {
        throw new IOException("Not implemented serializable ");
    }

    enum ExprSerCode {
        SLOT_REF(1),
        NULL_LITERAL(2),
        BOOL_LITERAL(3),
        INT_LITERAL(4),
        LARGE_INT_LITERAL(5),
        FLOAT_LITERAL(6),
        DECIMAL_LITERAL(7),
        STRING_LITERAL(8),
        DATE_LITERAL(9),
        MAX_LITERAL(10),
        BINARY_PREDICATE(11),
        FUNCTION_CALL(12);

        private static Map<Integer, ExprSerCode> codeMap = Maps.newHashMap();

        static {
            for (ExprSerCode item : ExprSerCode.values()) {
                codeMap.put(item.code, item);
            }
        }

        private int code;

        ExprSerCode(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static ExprSerCode fromCode(int code) {
            return codeMap.get(code);
        }
    }

    public static void writeTo(Expr expr, DataOutput output) throws IOException {
        if (expr instanceof SlotRef) {
            output.writeInt(ExprSerCode.SLOT_REF.getCode());
        } else if (expr instanceof NullLiteral) {
            output.writeInt(ExprSerCode.NULL_LITERAL.getCode());
        } else if (expr instanceof BoolLiteral) {
            output.writeInt(ExprSerCode.BOOL_LITERAL.getCode());
        } else if (expr instanceof IntLiteral) {
            output.writeInt(ExprSerCode.INT_LITERAL.getCode());
        } else if (expr instanceof LargeIntLiteral) {
            output.writeInt(ExprSerCode.LARGE_INT_LITERAL.getCode());
        } else if (expr instanceof FloatLiteral) {
            output.writeInt(ExprSerCode.FLOAT_LITERAL.getCode());
        } else if (expr instanceof DecimalLiteral) {
            output.writeInt(ExprSerCode.DECIMAL_LITERAL.getCode());
        } else if (expr instanceof StringLiteral) {
            output.writeInt(ExprSerCode.STRING_LITERAL.getCode());
        } else if (expr instanceof MaxLiteral) {
            output.writeInt(ExprSerCode.MAX_LITERAL.getCode());
        } else if (expr instanceof BinaryPredicate) {
            output.writeInt(ExprSerCode.BINARY_PREDICATE.getCode());
        } else if (expr instanceof FunctionCallExpr) {
            output.writeInt(ExprSerCode.FUNCTION_CALL.getCode());
        } else {
            throw new IOException("Unknown class " + expr.getClass().getName());
        }
        expr.write(output);
    }

    /**
     * The expr result may be null
     *
     * @param in
     * @return
     * @throws IOException
     */
    public static Expr readIn(DataInput in) throws IOException {
        int code = in.readInt();
        ExprSerCode exprSerCode = ExprSerCode.fromCode(code);
        if (exprSerCode == null) {
            throw new IOException("Unknown code: " + code);
        }
        switch (exprSerCode) {
            case SLOT_REF:
                return SlotRef.read(in);
            case NULL_LITERAL:
                return NullLiteral.read(in);
            case BOOL_LITERAL:
                return BoolLiteral.read(in);
            case INT_LITERAL:
                return IntLiteral.read(in);
            case LARGE_INT_LITERAL:
                return LargeIntLiteral.read(in);
            case FLOAT_LITERAL:
                return FloatLiteral.read(in);
            case DECIMAL_LITERAL:
                return DecimalLiteral.read(in);
            case STRING_LITERAL:
                return StringLiteral.read(in);
            case MAX_LITERAL:
                return MaxLiteral.read(in);
            case BINARY_PREDICATE:
                return BinaryPredicate.read(in);
            case FUNCTION_CALL:
                return FunctionCallExpr.read(in);
            default:
                throw new IOException("Unknown code: " + code);
        }
    }

    // If this expr can serialize and deserialize,
    // Expr will be serialized when this in load statement.
    // If one expr implement write/readFields, must override this function
    public boolean supportSerializable() {
        return false;
    }

    /**
     * Below function is added by new analyzer
     */
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitExpression(this, context);
    }

    public void setFn(Function fn) {
        this.fn = fn;
    }

    public void setIgnoreNulls(boolean ignoreNulls) { this.ignoreNulls = ignoreNulls; }

    public boolean getIgnoreNulls() { return ignoreNulls; }

    // only the first/last one can be lambda functions.
    public boolean hasLambdaFunction(Expr expression) {
        int pos = -1, num = 0;
        for (int i = 0; i < children.size(); ++i) {
            if (children.get(i) instanceof LambdaFunctionExpr) {
                num++;
                pos = i;
            }
        }
        if (num == 1 && (pos == 0 || pos == children.size() - 1)) {
            if (children.size() <= 1) {
                throw new SemanticException("Lambda functions need array inputs in high-order functions.");
            }
            return true;
        } else if (num > 1) {
            throw new SemanticException("A high-order function should have only 1 lambda function, " +
                    "but there are " + num + " lambda functions.");
        } else if (pos > 0 && pos < children.size() - 1) {
            throw new SemanticException(
                    "Lambda functions should only be the first or last argument of any high-order function, " +
                            "or lambda arguments should be in () if there are more than one lambda arguments, " +
                            "like (x,y)->x+y.");
        } else if (num == 0) {
            if (expression instanceof FunctionCallExpr) {
                String funcName = ((FunctionCallExpr) expression).getFnName().getFunction();
                if (funcName.equals(FunctionSet.ARRAY_MAP) || funcName.equals(FunctionSet.TRANSFORM)) {
                    throw new SemanticException("There are no lambda functions in high-order function " + funcName);
                }
            }
        }
        return false;
    }

    public boolean isSelfMonotonic() {
        return false;
    }

    public final boolean isMonotonic() {
        if (!isSelfMonotonic()) {
            return false;
        }
        for (Expr child : this.children) {
            if (!child.isMonotonic()) {
                return false;
            }
        }
        return true;
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
}
