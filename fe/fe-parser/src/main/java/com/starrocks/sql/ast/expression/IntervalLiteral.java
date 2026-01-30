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

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.UnitIdentifier;
import com.starrocks.sql.parser.NodePosition;


public class IntervalLiteral extends LiteralExpr {
    private final Expr value;
    private final UnitIdentifier unitIdentifier;

    public IntervalLiteral(Expr value, UnitIdentifier unitIdentifier) {
        this(value, unitIdentifier, value.getPos());
    }

    public IntervalLiteral(Expr value, UnitIdentifier unitIdentifier, NodePosition pos) {
        super(pos);
        this.value = value;
        this.unitIdentifier = unitIdentifier;
    }

    public Expr getValue() {
        return value;
    }

    public UnitIdentifier getUnitIdentifier() {
        return unitIdentifier;
    }

    public long toSeconds() {
        if (!(value instanceof IntLiteral)) {
            throw new IllegalArgumentException("Interval value must be an integer literal");
        }
        long step = ((IntLiteral) value).getLongValue();
        if (step <= 0) {
            throw new IllegalArgumentException("Interval value must be greater than 0");
        }
        String unit = unitIdentifier.getDescription().toUpperCase();
        switch (unit) {
            case "SECOND":
                return step;
            case "MINUTE":
                return step * 60L;
            case "HOUR":
                return step * 3600L;
            case "DAY":
                return step * 86400L;
            case "WEEK":
                return step * 604800L;
            default:
                throw new IllegalArgumentException("Unsupported interval unit: " + unit
                        + ", only SECOND, MINUTE, HOUR, DAY, WEEK are supported");
        }
    }

    public static String formatIntervalSeconds(long intervalSeconds) {
        if (intervalSeconds <= 0) {
            return null;
        }

        long value;
        String unit;
        if (intervalSeconds % 604800L == 0) {
            value = intervalSeconds / 604800L;
            unit = "WEEK";
        } else if (intervalSeconds % 86400L == 0) {
            value = intervalSeconds / 86400L;
            unit = "DAY";
        } else if (intervalSeconds % 3600L == 0) {
            value = intervalSeconds / 3600L;
            unit = "HOUR";
        } else if (intervalSeconds % 60L == 0) {
            value = intervalSeconds / 60L;
            unit = "MINUTE";
        } else {
            value = intervalSeconds;
            unit = "SECOND";
        }
        return value + " " + unit;
    }

    @Override
    public Expr clone() {
        return new IntervalLiteral(this.value, this.unitIdentifier, this.pos);
    }

    @Override
    public boolean isMinValue() {
        return false;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        return 0;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitIntervalLiteral(this, context);
    }
}
