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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/OrderByElement.java

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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Combination of expr and ASC/DESC, and nulls ordering.
 */
public class OrderByElement implements ParseNode {
    private Expr expr;
    private final boolean isAsc;

    // Represents the NULLs ordering specified: true when "NULLS FIRST", false when
    // "NULLS LAST", and null if not specified.
    private final Boolean nullsFirstParam;

    private final NodePosition pos;

    public OrderByElement(Expr expr, boolean isAsc, Boolean nullsFirstParam) {
        this(expr, isAsc, nullsFirstParam, NodePosition.ZERO);
    }

    public OrderByElement(Expr expr, boolean isAsc, Boolean nullsFirstParam, NodePosition pos) {
        this.pos = pos;
        this.expr = expr;
        this.isAsc = isAsc;
        this.nullsFirstParam = nullsFirstParam;
    }

    public void setExpr(Expr e) {
        this.expr = e;
    }

    public Expr getExpr() {
        return expr;
    }

    public boolean getIsAsc() {
        return isAsc;
    }

    public Boolean getNullsFirstParam() {
        return nullsFirstParam;
    }

    public OrderByElement clone() {
        return new OrderByElement(expr.clone(), isAsc, nullsFirstParam);
    }

    /**
     * Returns a new list of OrderByElements with the same (cloned) expressions but the
     * ordering direction reversed (asc becomes desc, nulls first becomes nulls last, etc.)
     */
    public static List<OrderByElement> reverse(List<OrderByElement> src) {
        List<OrderByElement> result = Lists.newArrayListWithCapacity(src.size());

        for (int i = 0; i < src.size(); ++i) {
            OrderByElement element = src.get(i);
            OrderByElement reverseElement =
                    new OrderByElement(element.getExpr().clone(), !element.isAsc,
                            !nullsFirst(element.nullsFirstParam));
            result.add(reverseElement);
        }

        return result;
    }

    /**
     * Extracts the order-by exprs from the list of order-by elements and returns them.
     */
    public static List<Expr> getOrderByExprs(List<OrderByElement> src) {
        List<Expr> result = Lists.newArrayListWithCapacity(src.size());

        for (OrderByElement element : src) {
            result.add(element.getExpr());
        }

        return result;
    }

    /**
     * Returns a new list of order-by elements with the order by exprs of src substituted
     * according to smap. Preserves the other sort params from src.
     *
     * @throws AnalysisException
     */
    public static ArrayList<OrderByElement> substitute(List<OrderByElement> src,
                                                       ExprSubstitutionMap smap, Analyzer analyzer)
            throws AnalysisException {
        ArrayList<OrderByElement> result = Lists.newArrayListWithCapacity(src.size());

        for (OrderByElement element : src) {
            result.add(new OrderByElement(element.getExpr().substitute(smap, analyzer, false),
                    element.isAsc, element.nullsFirstParam));
        }

        return result;
    }

    public String toSql() {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(expr.toSql());
        strBuilder.append(isAsc ? " ASC" : " DESC");

        // When ASC and NULLS FIRST or DESC and NULLS LAST, we do not print NULLS FIRST/LAST
        // because it is the default behavior
        if (nullsFirstParam != null) {
            if (isAsc && !nullsFirstParam) {
                // If ascending, nulls are first by default, so only add if nulls last.
                strBuilder.append(" NULLS LAST");
            } else if (!isAsc && nullsFirstParam) {
                // If descending, nulls are last by default, so only add if nulls first.
                strBuilder.append(" NULLS FIRST");
            }
        }
        return strBuilder.toString();
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    public String explain() {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(expr.explain());
        strBuilder.append(isAsc ? " ASC" : " DESC");
        if (nullsFirstParam != null) {
            if (isAsc && !nullsFirstParam) {
                strBuilder.append(" NULLS LAST");
            } else if (!isAsc && nullsFirstParam) {
                strBuilder.append(" NULLS FIRST");
            }
        }
        return strBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public int hashCode() {
        return Objects.hash(expr, isAsc, nullsFirstParam);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass()) {
            return false;
        }

        OrderByElement o = (OrderByElement) obj;
        return expr.equals(o.expr) && isAsc == o.isAsc && nullsFirstParam == o.nullsFirstParam;
    }

    /**
     * Compute nullsFirst.
     *
     * @param nullsFirstParam True if "NULLS FIRST", false if "NULLS LAST", or null if
     *                        the NULLs order was not specified.
     * @return Returns true if nulls are ordered first or false if nulls are ordered last.
     * Independent of isAsc.
     */
    public static boolean nullsFirst(Boolean nullsFirstParam) {
        Preconditions.checkNotNull(nullsFirstParam);
        return nullsFirstParam;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitOrderByElement(this, context);
    }
}
