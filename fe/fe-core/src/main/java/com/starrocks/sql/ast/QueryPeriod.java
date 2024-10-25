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
import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.parser.NodePosition;

import java.util.Optional;

public class QueryPeriod implements ParseNode {
    private final Optional<Expr> start;
    private final Optional<Expr> end;
    private final PeriodType periodType;

    public QueryPeriod(PeriodType periodType, Expr end) {
        this(periodType, Optional.empty(), Optional.of(end));
    }

    private QueryPeriod(PeriodType periodType, Optional<Expr> start, Optional<Expr> end) {
        this.periodType = periodType;
        this.start = start;
        this.end = end;
    }

    public enum PeriodType {
        TIMESTAMP,
        VERSION
    }

    public Optional<Expr> getStart() {
        return start;
    }

    public Optional<Expr> getEnd() {
        return end;
    }

    public PeriodType getPeriodType() {
        return periodType;
    }

    @Override
    public NodePosition getPos() {
        return null;
    }
}
