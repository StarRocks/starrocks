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

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExprNode;


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

<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/sql/ast/IntervalLiteral.java
    @Override
    protected String toSqlImpl() {
        return "interval " + value.toSql() + " " + unitIdentifier.toSql();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        throw new StarRocksPlannerException("IntervalLiteral not implement toThrift", ErrorType.INTERNAL_ERROR);
=======
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
>>>>>>> ea1a872649 ([Enhancement] Add interval support for automated cluster snapshots (#67525)):fe/fe-parser/src/main/java/com/starrocks/sql/ast/expression/IntervalLiteral.java
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
}
