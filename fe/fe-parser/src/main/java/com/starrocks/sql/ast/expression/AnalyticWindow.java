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

import java.util.Objects;

/**
 * Windowing clause of an analytic expr
 * Both left and right boundaries are always non-null after analyze().
 */
public class AnalyticWindow implements ParseNode {
    // default window used when an analytic expr was given an order by but no window
    public static final AnalyticWindow DEFAULT_WINDOW = new AnalyticWindow(Type.RANGE,
            new AnalyticWindowBoundary(AnalyticWindowBoundary.BoundaryType.UNBOUNDED_PRECEDING, null),
            new AnalyticWindowBoundary(AnalyticWindowBoundary.BoundaryType.CURRENT_ROW, null));

    public static final AnalyticWindow DEFAULT_ROWS_WINDOW = new AnalyticWindow(AnalyticWindow.Type.ROWS,
            new AnalyticWindowBoundary(AnalyticWindowBoundary.BoundaryType.UNBOUNDED_PRECEDING, null),
            new AnalyticWindowBoundary(AnalyticWindowBoundary.BoundaryType.CURRENT_ROW, null));

    public static final AnalyticWindow DEFAULT_UNBOUNDED_WINDOW = new AnalyticWindow(AnalyticWindow.Type.ROWS,
            new AnalyticWindowBoundary(AnalyticWindowBoundary.BoundaryType.UNBOUNDED_PRECEDING, null),
            new AnalyticWindowBoundary(AnalyticWindowBoundary.BoundaryType.UNBOUNDED_FOLLOWING, null));

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

    }

    private final NodePosition pos;

    private final Type type;
    private final AnalyticWindowBoundary leftBoundary;
    private AnalyticWindowBoundary rightBoundary;  // may be null before analyze()

    public Type getType() {
        return type;
    }

    public AnalyticWindowBoundary getLeftBoundary() {
        return leftBoundary;
    }

    public AnalyticWindowBoundary getRightBoundary() {
        return rightBoundary;
    }

    public AnalyticWindowBoundary setRightBoundary(AnalyticWindowBoundary b) {
        return rightBoundary = b;
    }

    public AnalyticWindow(Type type, AnalyticWindowBoundary b) {
        this(type, b, NodePosition.ZERO);
    }

    public AnalyticWindow(Type type, AnalyticWindowBoundary b, NodePosition pos) {
        this.pos = pos;
        this.type = type;
        Preconditions.checkNotNull(b);
        leftBoundary = b;
        rightBoundary = null;
    }

    public AnalyticWindow(Type type, AnalyticWindowBoundary l, AnalyticWindowBoundary r) {
        this(type, l, r, NodePosition.ZERO);
    }

    public AnalyticWindow(Type type, AnalyticWindowBoundary l, AnalyticWindowBoundary r, NodePosition pos) {
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
    }

    public AnalyticWindow reverse() {
        AnalyticWindowBoundary newRightBoundary = leftBoundary.converse();
        AnalyticWindowBoundary newLeftBoundary = null;

        if (rightBoundary == null) {
            newLeftBoundary = new AnalyticWindowBoundary(leftBoundary.getBoundaryType(), null);
        } else {
            newLeftBoundary = rightBoundary.converse();
        }

        return new AnalyticWindow(type, newLeftBoundary, newRightBoundary, pos);
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
