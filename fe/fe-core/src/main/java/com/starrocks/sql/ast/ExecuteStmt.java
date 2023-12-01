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
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.stream.Collectors;

public class ExecuteStmt extends StatementBase {
    private final String stmtName;
    private final List<Expr> paramsExpr;

    public ExecuteStmt(String stmtName, List<Expr> paramsExpr) {
        super(NodePosition.ZERO);
        this.stmtName = stmtName;
        this.paramsExpr = paramsExpr;
    }

    public List<Expr> getParamsExpr() {
        return paramsExpr;
    }

    public String getStmtName() {
        return stmtName;
    }

    @Override
    public String toSql() {
        return "EXECUTE `" + stmtName + "`"
                + paramsExpr.stream().map(Expr::toSql).collect(Collectors.joining(", ", " USING ", ""));
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitExecuteStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return null;
    }
}
