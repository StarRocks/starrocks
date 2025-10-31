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

package com.starrocks.sql.ast.expression;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.TreeNode;
import com.starrocks.common.io.Writable;
import com.starrocks.planner.SlotId;
import com.starrocks.planner.TupleId;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.formatter.AST2SQLVisitor;
import com.starrocks.sql.formatter.ExprExplainVisitor;
import com.starrocks.sql.formatter.ExprVerboseVisitor;
import com.starrocks.sql.formatter.FormatOptions;
import com.starrocks.sql.parser.NodePosition;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;

/**
 * Root of the expr node hierarchy.
 */
public abstract class Expr extends TreeNode<Expr> implements ParseNode, Cloneable, Writable {

    // id that's unique across the entire query statement and is assigned by
    // Analyzer.registerConjuncts(); only assigned for the top-level terms of a
    // conjunction, and therefore null for most Exprs
    protected ExprId id;

    protected Type type;  // result of analysis

    // only for query result set metadata
    // set if related to an actual column
    protected Type originType;

    protected boolean isAnalyzed = false;  // true after analyze() has been called

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
    private boolean isConstant;

    // Flag to indicate whether to wrap this expr's toSql() in parenthesis. Set by parser.
    // Needed for properly capturing expr precedences in the SQL string.
    protected boolean printSqlInParens = false;

    protected final NodePosition pos;

    private List<String> hints = Collections.emptyList();

    private RoaringBitmap cachedUsedSlotIds = null;

    // is this Expr can be used in index filter and expr filter or only index filter
    // passed to BE storage engine
    private boolean isIndexOnlyFilter = false;

    // depth is used to indicate the depth of the same operator in the tree
    protected int depth = 0;

    protected Expr() {
        pos = NodePosition.ZERO;
        type = Type.INVALID;
        originType = Type.INVALID;
        selectivity = -1.0;
        numDistinctValues = -1;
    }

    protected Expr(NodePosition pos) {
        this.pos = pos;
        type = Type.INVALID;
        originType = Type.INVALID;
        selectivity = -1.0;
        numDistinctValues = -1;
    }

    protected Expr(Expr other) {
        super();
        pos = other.pos;
        id = other.id;
        type = other.type;
        originType = other.type;
        isAnalyzed = other.isAnalyzed;
        selectivity = other.selectivity;
        numDistinctValues = other.numDistinctValues;
        isConstant = other.isConstant;
        fn = other.fn;
        printSqlInParens = other.printSqlInParens;
        children = ExprUtils.cloneList(other.children);
        hints = Lists.newArrayList(hints);
        depth = other.depth;
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

    public int getDepth() {
        return depth;
    }

    public void incrDepth() {
        int curDepth = children.stream().mapToInt(Expr::getDepth).max().orElse(0);
        this.depth = curDepth + 1;
    }

    private Optional<Expr> replaceLargeStringLiteralImpl() {
        if (this instanceof LargeStringLiteral) {
            return Optional.of(new StringLiteral(((LargeStringLiteral) this).getValue()));
        }
        if (children == null || children.isEmpty()) {
            return Optional.empty();
        }
        List<Pair<Expr, Optional<Expr>>> childAndNewChildPairList = children.stream()
                .map(child -> Pair.create(child, child.replaceLargeStringLiteralImpl()))
                .collect(Collectors.toList());
        if (childAndNewChildPairList.stream().noneMatch(p -> p.second.isPresent())) {
            return Optional.empty();
        }

        for (int i = 0; i < this.children.size(); ++i) {
            Pair<Expr, Optional<Expr>> childPair = childAndNewChildPairList.get(i);
            Expr newChild = childPair.second.orElse(childPair.first);
            setChild(i, newChild);
        }
        return Optional.of(this);
    }

    public Expr replaceLargeStringLiteral() {
        return this.replaceLargeStringLiteralImpl().orElse(this);
    }

    // Used to differ from getOriginType(), return originType directly.
    public Type getTrueOriginType() {
        return originType;
    }

    public void setOriginType(Type originType) {
        this.originType = originType;
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

    public Function getFn() {
        return fn;
    }

    public boolean getPrintSqlInParens() {
        return printSqlInParens;
    }

    public void setPrintSqlInParens(boolean b) {
        printSqlInParens = b;
    }

    public boolean isIndexOnlyFilter() {
        return isIndexOnlyFilter;
    }

    public void setIndexOnlyFilter(boolean indexOnlyFilter) {
        isIndexOnlyFilter = indexOnlyFilter;
    }

    /**
     * Set the expr to be analyzed and computes isConstant_.
     */
    protected void analysisDone() {
        Preconditions.checkState(!isAnalyzed);
        // We need to compute the const-ness as the last step, since analysis may change
        // the result, e.g. by resolving function.
        isConstant = isConstantImpl();
        isAnalyzed = true;
    }

    public RoaringBitmap getUsedSlotIds() {
        if (cachedUsedSlotIds == null) {
            cachedUsedSlotIds = new RoaringBitmap();
            List<SlotRef> slotRefs = Lists.newArrayList();
            this.collect(SlotRef.class, slotRefs);
            slotRefs.stream().map(SlotRef::getSlotId).map(SlotId::asInt).forEach(cachedUsedSlotIds::add);
        }
        return cachedUsedSlotIds;
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

    /**
     * toSql is an obsolete interface, because of historical reasons, the implementation of toSql is not rigorous enough.
     * Newly developed code should use AstToSQLBuilder::toSQL instead
     */
    @Deprecated
    public String toSql() {
        Preconditions.checkState(!printSqlInParens);
        ExprExplainVisitor explain = new ExprExplainVisitor();
        return explain.visit(this);
    }

    /**
     * `toSqlWithoutTbl` will return sql without table name for column name, so it can be easier to compare two expr.
     */
    public String toSqlWithoutTbl() {
        return AST2SQLVisitor.withOptions(
                FormatOptions.allEnable().setColumnSimplifyTableName(false).setColumnWithTableName(false)
                        .setEnableDigest(false)).visit(this);
    }

    public String explain() {
        Preconditions.checkState(!printSqlInParens);
        ExprVerboseVisitor explain = new ExprVerboseVisitor();
        return explain.visit(this);
    }

    public String toMySql() {
        return toSql();
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

    public boolean equalsWithoutChild(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || (obj.getClass() != this.getClass())) {
            return false;
        }
        Expr expr = (Expr) obj;
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
    public boolean equals(Object obj) {
        if (!equalsWithoutChild(obj)) {
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
        return true;
    }

    @Override
    public int hashCode() {
        // in group by clause, group by list need to remove duplicate exprs, the expr may be not not analyzed, the id
        // may be null.
        // NOTE that all the types of the related member variables must implement hashCode() and equals().
        if (id == null) {
            int result = 31 * Objects.hashCode(type);
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
     * Attention: if you just want check whether the expr is a constant literal, please consider
     * use isLiteral()
     * <p>
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
            return isConstant;
        }
        return isConstantImpl();
    }

    public final boolean isParameter() {
        return this instanceof Parameter;
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

    public List<SlotRef> collectAllSlotRefs() {
        return collectAllSlotRefs(false);
    }

    public List<SlotRef> collectAllSlotRefs(boolean distinct) {
        Collection<SlotRef> result = distinct ? Sets.newHashSet() : Lists.newArrayList();
        Queue<Expr> q = Lists.newLinkedList();
        q.add(this);
        while (!q.isEmpty()) {
            Expr head = q.poll();
            if (head instanceof SlotRef) {
                result.add((SlotRef) head);
            }
            q.addAll(head.getChildren());
        }

        return distinct ? Lists.newArrayList(result) : (List<SlotRef>) result;
    }

    public boolean isImplicitCast() {
        return this instanceof CastExpr && ((CastExpr) this).isImplicit();
    }

    /**
     * Negates a boolean Expr.
     */
    public Expr negate() {
        return new CompoundPredicate(CompoundPredicate.Operator.NOT, this, null);
    }

    @Override
    public NodePosition getPos() {
        return pos;
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
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitExpression(this, context);
    }

    public void setFn(Function fn) {
        this.fn = fn;
    }

    public void setIgnoreNulls(boolean ignoreNulls) {
        this.ignoreNulls = ignoreNulls;
    }

    public boolean getIgnoreNulls() {
        return ignoreNulls;
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

    public void setHints(List<String> hints) {
        this.hints = hints;
    }

    public List<String> getHints() {
        return hints;
    }

}
