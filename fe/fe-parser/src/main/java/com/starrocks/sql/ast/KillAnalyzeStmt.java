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

import com.starrocks.sql.ast.expression.UserVariableExpr;
import com.starrocks.sql.parser.NodePosition;

public class KillAnalyzeStmt extends StatementBase {
    private final long analyzeId;
    private final UserVariableExpr userVariableExpr;

    public KillAnalyzeStmt(long analyzeId) {
        this(analyzeId, NodePosition.ZERO);
    }

    public KillAnalyzeStmt(long analyzeId, NodePosition pos) {
        super(pos);
        this.analyzeId = analyzeId;
        this.userVariableExpr = null;
    }

    public KillAnalyzeStmt(UserVariableExpr userVariableExpr, NodePosition pos) {
        super(pos);
        this.analyzeId = -1;
        this.userVariableExpr = userVariableExpr;
    }

    public boolean isKillAllPendingTasks() {
        return analyzeId == -1 && userVariableExpr == null;
    }

    public long getAnalyzeId() {
        return analyzeId;
    }

    public UserVariableExpr getUserVariableExpr() {
        return userVariableExpr;
    }

    public boolean hasUserVariable() {
        return userVariableExpr != null;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitKillAnalyzeStatement(this, context);
    }
}
