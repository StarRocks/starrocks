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

package com.starrocks.sql.ast.context;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public class WorkspaceUpsertStmt extends DdlStmt {

    private final WorkspaceName workspace;
    private final Map<String, Expr> objectArgs;
    private final Map<String, Expr> options;

    public WorkspaceUpsertStmt(WorkspaceName workspace, Map<String, Expr> objectArgs,
                               Map<String, Expr> options, NodePosition pos) {
        super(pos);
        this.workspace = workspace;
        this.objectArgs = objectArgs;
        this.options = options;
    }

    public WorkspaceName getWorkspace() {
        return workspace;
    }

    public Map<String, Expr> getObjectArgs() {
        return objectArgs;
    }

    public Map<String, Expr> getOptions() {
        return options;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitWorkspaceUpsertStatement(this, context);
    }

    @Override
    public String toSql() {
        return ContextStmtFormatter.workspaceUpsert(workspace, objectArgs, options);
    }
}
