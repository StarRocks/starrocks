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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/AnalyticExpr.java

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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExprNode;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Representation of an analytic function call with OVER clause.
 * All "subexpressions" (such as the actual function call parameters as well as the
 * partition/ordering exprs, etc.) are embedded as children in order to allow expr
 * substitution:
 * function call params: child 0 .. #params
 * partition exprs: children #params + 1 .. #params + #partition-exprs
 * ordering exprs:
 * children #params + #partition-exprs + 1 ..
 * #params + #partition-exprs + #order-by-elements
 * exprs in windowing clause: remaining children
 * <p>
 * Note that it's wrong to embed the FunctionCallExpr itself as a child,
 * because in 'COUNT(..) OVER (..)' the 'COUNT(..)' is not part of a standard aggregate
 * computation and must not be substituted as such. However, the parameters of the
 * analytic function call might reference the output of an aggregate computation
 * and need to be substituted as such; example: COUNT(COUNT(..)) OVER (..)
 */
public class AnalyticExpr extends Expr {
    private FunctionCallExpr fnCall;
    private final List<Expr> partitionExprs;
    // These elements are modified to point to the corresponding child exprs to keep them
    // in sync through expr substitutions.
    private List<OrderByElement> orderByElements = Lists.newArrayList();
    private AnalyticWindow window;

    // If set, requires the window to be set to null in resetAnalysisState(). Required for
    // proper substitution/cloning because standardization may set a window that is illegal
    // in SQL, and hence, will fail analysis().
    private boolean resetWindow = false;

    private String partitionHint;
    private String skewHint;

    private boolean useHashBasedPartition;
    private boolean isSkewed;

    // SQL string of this AnalyticExpr before standardization. Returned in toSqlImpl().
    private String sqlString;

    private static final String HINT_SORT = "sort";
    private static final String HINT_HASH = "hash";
    private static final String HINT_SKEW = "skewed";

    public static String LEAD = "LEAD";
    public static String LAG = "LAG";
    public static String FIRSTVALUE = "FIRST_VALUE";
    public static String LASTVALUE = "LAST_VALUE";
    public static String RANK = "RANK";
    public static String DENSERANK = "DENSE_RANK";
    public static String ROWNUMBER = "ROW_NUMBER";
    public static String CUMEDIST = "CUME_DIST";
    public static String PERCENTRANK = "PERCENT_RANK";
    public static String NTILE = "NTILE";
    public static String MIN = "MIN";
    public static String MAX = "MAX";
    public static String SUM = "SUM";
    public static String COUNT = "COUNT";
    public static String SESSION_NUMBER = "SESSION_NUMBER";
    public static String APPROX_TOP_K = "approx_top_k";

    // The function of HLL_UNION_AGG can't be used with a window by now.
    public static String HLL_UNION_AGG = "HLL_UNION_AGG";

    public AnalyticExpr(FunctionCallExpr fnCall, List<Expr> partitionExprs, List<OrderByElement> orderByElements,
                        AnalyticWindow window, List<String> hints) {
        this(fnCall, partitionExprs, orderByElements, window, hints, NodePosition.ZERO);
    }

    public AnalyticExpr(FunctionCallExpr fnCall, List<Expr> partitionExprs, List<OrderByElement> orderByElements,
                        AnalyticWindow window, List<String> hints, NodePosition pos) {
        super(pos);
        Preconditions.checkNotNull(fnCall);
        this.fnCall = fnCall;
        this.partitionExprs = partitionExprs != null ? partitionExprs : new ArrayList<>();

        if (orderByElements != null) {
            this.orderByElements.addAll(orderByElements);
        }

        this.window = window;

        if (CollectionUtils.isNotEmpty(hints)) {
            for (String hint : hints) {
                if (HINT_SORT.equalsIgnoreCase(hint) || HINT_HASH.equalsIgnoreCase(hint)) {
                    this.partitionHint = hint;
                    this.useHashBasedPartition = !HINT_SORT.equalsIgnoreCase(hint);
                } else if (HINT_SKEW.equalsIgnoreCase(hint)) {
                    this.skewHint = hint;
                    this.isSkewed = true;
                } else {
                    Preconditions.checkState(false, "partition by hint can only be 'sort' or 'hash' or 'skew'");
                }
            }
        }

        setChildren();
    }

    /**
     * clone() c'tor
     */
    protected AnalyticExpr(AnalyticExpr other) {
        super(other);
        fnCall = (FunctionCallExpr) other.fnCall.clone();

        for (OrderByElement e : other.orderByElements) {
            orderByElements.add(e.clone());
        }

        partitionExprs = Expr.cloneList(other.partitionExprs);
        window = (other.window != null ? other.window.clone() : null);
        resetWindow = other.resetWindow;
        partitionHint = other.partitionHint;
        skewHint = other.skewHint;
        useHashBasedPartition = other.useHashBasedPartition;
        isSkewed = other.isSkewed;
        sqlString = other.sqlString;
        setChildren();
    }

    public FunctionCallExpr getFnCall() {
        return fnCall;
    }

    public List<Expr> getPartitionExprs() {
        return partitionExprs;
    }

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public AnalyticWindow getWindow() {
        return window;
    }

    public String getPartitionHint() {
        return partitionHint;
    }

    public String getSkewHint() {
        return skewHint;
    }

    public boolean isUseHashBasedPartition() {
        return useHashBasedPartition;
    }

    public boolean isSkewed() {
        return isSkewed;
    }

    @Override
    public boolean equalsWithoutChild(Object obj) {
        if (!super.equalsWithoutChild(obj)) {
            return false;
        }

        AnalyticExpr o = (AnalyticExpr) obj;

        return Objects.equals(fnCall, o.fnCall) &&
                Objects.equals(partitionExprs, o.partitionExprs) &&
                Objects.equals(orderByElements, o.orderByElements) &&
                Objects.equals(window, o.window) &&
                Objects.equals(partitionHint, o.partitionHint) &&
                Objects.equals(skewHint, o.skewHint) &&
                Objects.equals(useHashBasedPartition, o.useHashBasedPartition) &&
                Objects.equals(isSkewed, o.isSkewed);
    }

    /**
     * Analytic exprs cannot be constant.
     */
    @Override
    protected boolean isConstantImpl() {
        return false;
    }

    @Override
    public Expr clone() {
        return new AnalyticExpr(this);
    }

    @Override
    public String debugString() {
        return MoreObjects.toStringHelper(this)
                .add("fn", getFnCall())
                .add("window", window)
                .addValue(super.debugString())
                .toString();
    }

    @Override
    protected void toThrift(TExprNode msg) {
    }

    public static boolean isAnalyticFn(Function fn) {
        return fn instanceof AggregateFunction
                && ((AggregateFunction) fn).isAnalyticFn();
    }

    public static boolean isOffsetFn(Function fn) {
        if (!isAnalyticFn(fn)) {
            return false;
        }

        return fn.functionName().equalsIgnoreCase(LEAD) || fn.functionName().equalsIgnoreCase(LAG);
    }

    public static boolean isNtileFn(Function fn) {
        if (!isAnalyticFn(fn)) {
            return false;
        }

        return fn.functionName().equalsIgnoreCase(NTILE);
    }

    public static boolean isCumeFn(Function fn) {
        if (!isAnalyticFn(fn)) {
            return false;
        }

        return fn.functionName().equalsIgnoreCase(CUMEDIST) || fn.functionName().equalsIgnoreCase(PERCENTRANK);
    }

    public static boolean isRowNumberFn(Function fn) {
        if (!isAnalyticFn(fn)) {
            return false;
        }

        return fn.functionName().equalsIgnoreCase(ROWNUMBER);
    }

    public static boolean isApproxTopKFn(Function fn) {
        if (!isAnalyticFn(fn)) {
            return false;
        }

        return fn.functionName().equalsIgnoreCase(APPROX_TOP_K);
    }

    /**
     * check the value out of range in lag/lead() function
     */
    public static void checkDefaultValue(FunctionCallExpr call) throws AnalysisException {
        Expr val = call.getChild(2);

        if (!(val instanceof LiteralExpr)) {
            return;
        }

        if (!call.getChild(0).getType().getPrimitiveType().isNumericType()) {
            return;
        }

        double value = getConstFromExpr(val);
        PrimitiveType type = call.getChild(0).getType().getPrimitiveType();
        boolean out = false;

        if (type == PrimitiveType.TINYINT) {
            if (value > Byte.MAX_VALUE) {
                out = true;
            }
        } else if (type == PrimitiveType.SMALLINT) {
            if (value > Short.MAX_VALUE) {
                out = true;
            }
        } else if (type == PrimitiveType.INT) {
            if (value > Integer.MAX_VALUE) {
                out = true;
            }
        } else if (type == PrimitiveType.BIGINT) {
            if (value > Long.MAX_VALUE) {
                out = true;
            }
        } else if (type == PrimitiveType.FLOAT) {
            if (value > Float.MAX_VALUE) {
                out = true;
            }
        } else if (type == PrimitiveType.DOUBLE) {
            if (value > Double.MAX_VALUE) {
                out = true;
            }
        } else {
            return;
        }

        if (out) {
            throw new AnalysisException("Column type="
                    + call.getChildren().get(0).getType() + ", value is out of range ");
        }
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    }

    /**
     * Keep fnCall_, partitionExprs_ and orderByElements_ in sync with children_.
     */
    private void syncWithChildren() {
        int numArgs = fnCall.getChildren().size();

        for (int i = 0; i < numArgs; ++i) {
            fnCall.setChild(i, getChild(i));
        }

        int numPartitionExprs = partitionExprs.size();

        for (int i = 0; i < numPartitionExprs; ++i) {
            partitionExprs.set(i, getChild(numArgs + i));
        }

        for (int i = 0; i < orderByElements.size(); ++i) {
            orderByElements.get(i).setExpr(getChild(numArgs + numPartitionExprs + i));
        }
    }

    /**
     * Populate children_ from fnCall_, partitionExprs_, orderByElements_
     */
    private void setChildren() {
        getChildren().clear();
        addChildren(fnCall.getChildren());
        addChildren(partitionExprs);

        for (OrderByElement e : orderByElements) {
            addChild(e.getExpr());
        }

        if (window != null) {
            if (window.getLeftBoundary().getExpr() != null) {
                addChild(window.getLeftBoundary().getExpr());
            }

            if (window.getRightBoundary() != null
                    && window.getRightBoundary().getExpr() != null) {
                addChild(window.getRightBoundary().getExpr());
            }
        }
    }

    @Override
    protected void resetAnalysisState() {
        super.resetAnalysisState();
        fnCall.resetAnalysisState();

        if (resetWindow) {
            window = null;
        }

        resetWindow = false;
        // sync with children, now that they've been reset
        syncWithChildren();
    }

    @Override
    protected Expr substituteImpl(ExprSubstitutionMap sMap, Analyzer analyzer)
            throws AnalysisException {
        Expr e = super.substituteImpl(sMap, analyzer);
        if (!(e instanceof AnalyticExpr)) {
            return e;
        }
        // Re-sync state after possible child substitution.
        ((AnalyticExpr) e).syncWithChildren();
        return e;
    }

    @Override
    public String toSqlImpl() {
        if (sqlString != null) {
            return sqlString;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(fnCall.toSql()).append(" OVER (");
        boolean needsSpace = false;
        if (!partitionExprs.isEmpty()) {
            sb.append("PARTITION BY ").append(exprListToSql(partitionExprs));
            needsSpace = true;
        }
        if (!orderByElements.isEmpty()) {
            List<String> orderByStrings = Lists.newArrayList();
            for (OrderByElement e : orderByElements) {
                orderByStrings.add(e.toSql());
            }
            if (needsSpace) {
                sb.append(" ");
            }
            sb.append("ORDER BY ").append(Joiner.on(", ").join(orderByStrings));
            needsSpace = true;
        }
        if (window != null) {
            if (needsSpace) {
                sb.append(" ");
            }
            sb.append(window.toSql());
        }
        sb.append(")");
        return sb.toString();
    }

    private String exprListToSql(List<? extends Expr> exprs) {
        if (exprs == null || exprs.isEmpty()) {
            return "";
        }
        List<String> strings = Lists.newArrayList();
        for (Expr expr : exprs) {
            strings.add(expr.toSql());
        }
        return Joiner.on(", ").join(strings);
    }

    /**
     * Below function is added by new analyzer
     */
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAnalyticExpr(this, context);
    }

    @Override
    public int hashCode() {
        // all children information is contained in the group of fnCall, partitionExprs, orderByElements and window,
        // so need to calculate super's hashCode.
        // field window is correlated with field resetWindow, so no need to add resetWindow when calculating hashCode.
        return Objects.hash(type, opcode, fnCall, partitionExprs, orderByElements, window, partitionHint, skewHint,
                useHashBasedPartition, isSkewed);
    }
}
