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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/AnalyticWindow.java

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

import com.google.common.base.Preconditions;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TAnalyticWindow;
import com.starrocks.thrift.TAnalyticWindowBoundary;
import com.starrocks.thrift.TAnalyticWindowBoundaryType;
import com.starrocks.thrift.TAnalyticWindowType;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Windowing clause of an analytic expr
 * Both left and right boundaries are always non-null after analyze().
 */
public class AnalyticWindow implements ParseNode {
    // default window used when an analytic expr was given an order by but no window
    public static final AnalyticWindow DEFAULT_WINDOW = new AnalyticWindow(Type.RANGE,
            new Boundary(BoundaryType.UNBOUNDED_PRECEDING, null),
            new Boundary(BoundaryType.CURRENT_ROW, null));

    public static final AnalyticWindow DEFAULT_ROWS_WINDOW = new AnalyticWindow(AnalyticWindow.Type.ROWS,
            new AnalyticWindow.Boundary(AnalyticWindow.BoundaryType.UNBOUNDED_PRECEDING, null),
            new AnalyticWindow.Boundary(AnalyticWindow.BoundaryType.CURRENT_ROW, null));

    public static final AnalyticWindow DEFAULT_UNBOUNDED_WINDOW = new AnalyticWindow(AnalyticWindow.Type.ROWS,
            new AnalyticWindow.Boundary(AnalyticWindow.BoundaryType.UNBOUNDED_PRECEDING, null),
            new AnalyticWindow.Boundary(AnalyticWindow.BoundaryType.UNBOUNDED_FOLLOWING, null));

    public enum Type {
        ROWS("ROWS"),
        RANGE("RANGE");

        private final String description;

        private Type(String d) {
            description = d;
        }

        @Override
        public String toString() {
            return description;
        }

        public TAnalyticWindowType toThrift() {
            return this == ROWS ? TAnalyticWindowType.ROWS : TAnalyticWindowType.RANGE;
        }
    }

    public enum BoundaryType {
        UNBOUNDED_PRECEDING("UNBOUNDED PRECEDING"),
        UNBOUNDED_FOLLOWING("UNBOUNDED FOLLOWING"),
        CURRENT_ROW("CURRENT ROW"),
        PRECEDING("PRECEDING"),
        FOLLOWING("FOLLOWING");

        private final String description;

        private BoundaryType(String d) {
            description = d;
        }

        @Override
        public String toString() {
            return description;
        }

        public TAnalyticWindowBoundaryType toThrift() {
            Preconditions.checkState(!isAbsolutePos());

            if (this == CURRENT_ROW) {
                return TAnalyticWindowBoundaryType.CURRENT_ROW;
            } else if (this == PRECEDING) {
                return TAnalyticWindowBoundaryType.PRECEDING;
            } else if (this == FOLLOWING) {
                return TAnalyticWindowBoundaryType.FOLLOWING;
            }

            return null;
        }

        public boolean isAbsolutePos() {
            return this == UNBOUNDED_PRECEDING || this == UNBOUNDED_FOLLOWING;
        }

        public boolean isOffset() {
            return this == PRECEDING || this == FOLLOWING;
        }

        public boolean isPreceding() {
            return this == UNBOUNDED_PRECEDING || this == PRECEDING;
        }

        public boolean isFollowing() {
            return this == UNBOUNDED_FOLLOWING || this == FOLLOWING;
        }

        public BoundaryType converse() {
            switch (this) {
                case UNBOUNDED_PRECEDING:
                    return UNBOUNDED_FOLLOWING;

                case UNBOUNDED_FOLLOWING:
                    return UNBOUNDED_PRECEDING;

                case PRECEDING:
                    return FOLLOWING;

                case FOLLOWING:
                    return PRECEDING;

                default:
                    return CURRENT_ROW;
            }
        }
    }

    public static class Boundary implements ParseNode {

        private final NodePosition pos;
        private BoundaryType type;

        // Offset expr. Only set for PRECEDING/FOLLOWING. Needed for toSql().
        private final Expr expr;

        // The offset value. Set during analysis after evaluating expr_. Integral valued
        // for ROWS windows.
        private BigDecimal offsetValue;

        public BoundaryType getType() {
            return type;
        }

        public Expr getExpr() {
            return expr;
        }

        public Boundary(BoundaryType type, Expr e) {
            this(type, e, null);
        }

        // c'tor used by clone()
        public Boundary(BoundaryType type, Expr e, BigDecimal offsetValue) {
            this(type, e, offsetValue, NodePosition.ZERO);
        }

        public Boundary(BoundaryType type, Expr e, BigDecimal offsetValue, NodePosition pos) {
            Preconditions.checkState(
                    (type.isOffset() && e != null)
                            || (!type.isOffset() && e == null));
            this.pos = pos;
            this.type = type;
            this.expr = e;
            this.offsetValue = offsetValue;
        }

        public String toSql() {
            StringBuilder sb = new StringBuilder();

            if (expr != null) {
                sb.append(expr.toSql()).append(" ");
            }

            sb.append(type.toString());
            return sb.toString();
        }

        @Override
        public NodePosition getPos() {
            return pos;
        }

        public TAnalyticWindowBoundary toThrift(Type windowType) {
            TAnalyticWindowBoundary result = new TAnalyticWindowBoundary(type.toThrift());

            if (type.isOffset() && windowType == Type.ROWS) {
                result.setRows_offset_value(offsetValue.longValue());
            }

            // TODO: range windows need range_offset_predicate
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }

            if (obj.getClass() != this.getClass()) {
                return false;
            }

            Boundary o = (Boundary) obj;
            boolean exprEqual = (expr == null) == (o.expr == null);

            if (exprEqual && expr != null) {
                exprEqual = expr.equals(o.expr);
            }

            return type == o.type && exprEqual;
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, expr, offsetValue);
        }

        public Boundary converse() {
            Boundary result = new Boundary(type.converse(),
                    (expr != null) ? expr.clone() : null);
            result.offsetValue = offsetValue;
            return result;
        }

        @Override
        public Boundary clone() {
            return new Boundary(type, expr != null ? expr.clone() : null, offsetValue);
        }

        public void setOffsetValue(BigDecimal offsetValue) {
            this.offsetValue = offsetValue;
        }
    }

    private final NodePosition pos;

    private final Type type;
    private final Boundary leftBoundary;
    private Boundary rightBoundary;  // may be null before analyze()
    private String toSqlString;  // cached after analysis

    public Type getType() {
        return type;
    }

    public Boundary getLeftBoundary() {
        return leftBoundary;
    }

    public Boundary getRightBoundary() {
        return rightBoundary;
    }

    public Boundary setRightBoundary(Boundary b) {
        return rightBoundary = b;
    }

    public AnalyticWindow(Type type, Boundary b) {
        this(type, b, NodePosition.ZERO);
    }

    public AnalyticWindow(Type type, Boundary b, NodePosition pos) {
        this.pos = pos;
        this.type = type;
        Preconditions.checkNotNull(b);
        leftBoundary = b;
        rightBoundary = null;
    }

    public AnalyticWindow(Type type, Boundary l, Boundary r) {
        this(type, l, r, NodePosition.ZERO);
    }

    public AnalyticWindow(Type type, Boundary l, Boundary r, NodePosition pos) {
        this.pos = pos;
        this.type = type;
        Preconditions.checkNotNull(l);
        leftBoundary = l;
        Preconditions.checkNotNull(r);
        rightBoundary = r;
    }

    /**
     * Clone c'tor
     */
    private AnalyticWindow(AnalyticWindow other) {
        pos = other.pos;
        type = other.type;
        Preconditions.checkNotNull(other.leftBoundary);
        leftBoundary = other.leftBoundary.clone();

        if (other.rightBoundary != null) {
            rightBoundary = other.rightBoundary.clone();
        }

        toSqlString = other.toSqlString;  // safe to share
    }

    public AnalyticWindow reverse() {
        Boundary newRightBoundary = leftBoundary.converse();
        Boundary newLeftBoundary = null;

        if (rightBoundary == null) {
            newLeftBoundary = new Boundary(leftBoundary.getType(), null);
        } else {
            newLeftBoundary = rightBoundary.converse();
        }

        return new AnalyticWindow(type, newLeftBoundary, newRightBoundary, pos);
    }

    public String toSql() {
        if (toSqlString != null) {
            return toSqlString;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(type.toString()).append(" ");

        if (rightBoundary == null) {
            sb.append(leftBoundary.toSql());
        } else {
            sb.append("BETWEEN ").append(leftBoundary.toSql()).append(" AND ");
            sb.append(rightBoundary.toSql());
        }

        return sb.toString();
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    public TAnalyticWindow toThrift() {
        TAnalyticWindow result = new TAnalyticWindow(type.toThrift());

        if (leftBoundary.getType() != BoundaryType.UNBOUNDED_PRECEDING) {
            result.setWindow_start(leftBoundary.toThrift(type));
        }

        Preconditions.checkNotNull(rightBoundary);

        if (rightBoundary.getType() != BoundaryType.UNBOUNDED_FOLLOWING) {
            result.setWindow_end(rightBoundary.toThrift(type));
        }

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass()) {
            return false;
        }

        AnalyticWindow o = (AnalyticWindow) obj;
        boolean rightBoundaryEqual =
                (rightBoundary == null) == (o.rightBoundary == null);

        if (rightBoundaryEqual && rightBoundary != null) {
            rightBoundaryEqual = rightBoundary.equals(o.rightBoundary);
        }

        return type == o.type
                && leftBoundary.equals(o.leftBoundary)
                && rightBoundaryEqual;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, leftBoundary, rightBoundary);
    }

    @Override
    public AnalyticWindow clone() {
        return new AnalyticWindow(this);
    }
}
