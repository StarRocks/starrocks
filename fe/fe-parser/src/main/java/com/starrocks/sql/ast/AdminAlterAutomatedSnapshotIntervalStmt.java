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

import com.starrocks.sql.ast.expression.IntervalLiteral;
import com.starrocks.sql.parser.NodePosition;

public class AdminAlterAutomatedSnapshotIntervalStmt extends DdlStmt {
    private final IntervalLiteral intervalLiteral;
    private long intervalSeconds = 0;

    public AdminAlterAutomatedSnapshotIntervalStmt(IntervalLiteral intervalLiteral) {
        super(NodePosition.ZERO);
        this.intervalLiteral = intervalLiteral;
    }

    public AdminAlterAutomatedSnapshotIntervalStmt(IntervalLiteral intervalLiteral, NodePosition pos) {
        super(pos);
        this.intervalLiteral = intervalLiteral;
    }

    public IntervalLiteral getIntervalLiteral() {
        return intervalLiteral;
    }

    public long getIntervalSeconds() {
        return intervalSeconds;
    }

    public void setIntervalSeconds(long intervalSeconds) {
        this.intervalSeconds = intervalSeconds;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAdminAlterAutomatedSnapshotIntervalStatement(this, context);
    }
}
