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

import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.Optional;

/**
 * AST node for the CHANGES clause in CDC queries.
 * <p>
 * Syntax: CHANGES FROM {VERSION|TIMESTAMP} expr TO {VERSION|TIMESTAMP} expr
 * <p>
 * The version interval is left-open right-closed: (FROM, TO].
 */
public class ChangePeriod implements ParseNode {
    private final QueryPeriod.PeriodType periodType;
    private final Expr start;
    private final Optional<Expr> end;
    private final boolean isStats;
    private final NodePosition pos;

    public ChangePeriod(QueryPeriod.PeriodType periodType, Expr start,
                        Optional<Expr> end, boolean isStats, NodePosition pos) {
        this.periodType = periodType;
        this.start = start;
        this.end = end;
        this.isStats = isStats;
        this.pos = pos;
    }

    public QueryPeriod.PeriodType getPeriodType() {
        return periodType;
    }

    public Expr getStart() {
        return start;
    }

    public Optional<Expr> getEnd() {
        return end;
    }

    public boolean isStats() {
        return isStats;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
