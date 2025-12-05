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

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.TreeNode;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.InvalidType;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Root of the expr node hierarchy.
 */
public abstract class Expr extends TreeNode<Expr> implements ParseNode, Cloneable {

    protected Type type;  // result of analysis

    // only for query result set metadata
    // set if related to an actual column
    protected Type originType;

    protected boolean isAnalyzed = false;  // true after analyze() has been called

    protected boolean isFilter = false;

    // Ignore nulls.
    private boolean ignoreNulls = false;

    // Cached value of IsConstant(), set during analyze() and valid if isAnalyzed_ is true.
    private boolean isConstant;

    // Flag to indicate whether to wrap this expr's toSql() in parenthesis. Set by parser.
    // Needed for properly capturing expr precedences in the SQL string.
    protected boolean printSqlInParens = false;

    protected final NodePosition pos;

    private List<String> hints = Collections.emptyList();

    // is this Expr can be used in index filter and expr filter or only index filter
    // passed to BE storage engine
    private boolean isIndexOnlyFilter = false;

    // depth is used to indicate the depth of the same operator in the tree
    protected int depth = 0;

    protected Expr() {
        pos = NodePosition.ZERO;
        type = InvalidType.INVALID;
        originType = InvalidType.INVALID;
    }

    protected Expr(NodePosition pos) {
        this.pos = pos;
        type = InvalidType.INVALID;
        originType = InvalidType.INVALID;
    }

    protected Expr(Expr other) {
        super();
        pos = other.pos;
        type = other.type;
        originType = other.type;
        isAnalyzed = other.isAnalyzed;
        isConstant = other.isConstant;
        printSqlInParens = other.printSqlInParens;

        ArrayList<Expr> result = new ArrayList<>();
        for (Expr element : other.children) {
            result.add(element.clone());
        }
        children = result;

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

    // Used to differ from getOriginType(), return originType directly.
    public Type getTrueOriginType() {
        return originType;
    }

    public void setOriginType(Type originType) {
        this.originType = originType;
    }

    public boolean isFilter() {
        return isFilter;
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

    public String debugString() {
        return debugString(children);
    }

    /**
     * Resets the internal state of this expr produced by analyze().
     * Only modifies this expr, and not its child exprs.
     */
    public void resetAnalysisState() {
        isAnalyzed = false;
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
        return true;
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
        // in group by clause, group by list need to remove duplicate exprs, the expr may be not analyzed.
        // NOTE that all the types of the related member variables must implement hashCode() and equals().
        int result = 31 * Objects.hashCode(type);
        for (Expr child : children) {
            result = 31 * result + Objects.hashCode(child);
        }
        return result;
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

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this.getClass()).add("type", type).toString();
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
        return visitor.visitExpression(this, context);
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
