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
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.parser.NodePosition;

import java.math.BigDecimal;
import java.util.Objects;

public class AnalyticWindowBoundary implements ParseNode {

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

    private final NodePosition pos;
    private final BoundaryType boundaryType;

    // Offset expr. Only set for PRECEDING/FOLLOWING. Needed for toSql().
    private final Expr expr;

    // The offset value. Set during analysis after evaluating expr_. Integral valued
    // for ROWS windows.
    private BigDecimal offsetValue;

    public BoundaryType getBoundaryType() {
        return boundaryType;
    }

    public Expr getExpr() {
        return expr;
    }

    public AnalyticWindowBoundary(BoundaryType boundaryType, Expr e) {
        this(boundaryType, e, null);
    }

    // c'tor used by clone()
    public AnalyticWindowBoundary(BoundaryType boundaryType, Expr e, BigDecimal offsetValue) {
        this(boundaryType, e, offsetValue, NodePosition.ZERO);
    }

    public AnalyticWindowBoundary(BoundaryType boundaryType, Expr e, BigDecimal offsetValue, NodePosition pos) {
        Preconditions.checkState(
                (boundaryType.isOffset() && e != null)
                        || (!boundaryType.isOffset() && e == null));
        this.pos = pos;
        this.boundaryType = boundaryType;
        this.expr = e;
        this.offsetValue = offsetValue;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass()) {
            return false;
        }

        AnalyticWindowBoundary o = (AnalyticWindowBoundary) obj;
        boolean exprEqual = (expr == null) == (o.expr == null);

        if (exprEqual && expr != null) {
            exprEqual = expr.equals(o.expr);
        }

        return boundaryType == o.boundaryType && exprEqual;
    }

    @Override
    public int hashCode() {
        return Objects.hash(boundaryType, expr, offsetValue);
    }

    public AnalyticWindowBoundary converse() {
        AnalyticWindowBoundary result = new AnalyticWindowBoundary(boundaryType.converse(),
                (expr != null) ? expr.clone() : null);
        result.offsetValue = offsetValue;
        return result;
    }

    @Override
    public AnalyticWindowBoundary clone() {
        return new AnalyticWindowBoundary(boundaryType, expr != null ? expr.clone() : null, offsetValue);
    }

    public void setOffsetValue(BigDecimal offsetValue) {
        this.offsetValue = offsetValue;
    }

    public BigDecimal getOffsetValue() {
        return offsetValue;
    }
}