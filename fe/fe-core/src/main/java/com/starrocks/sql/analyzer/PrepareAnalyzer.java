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

package com.starrocks.sql.analyzer;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.PrepareStmtContext;
import com.starrocks.sql.ast.DeallocateStmt;
import com.starrocks.sql.ast.ExecuteStmt;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.optimizer.validate.ValidateException;

public class PrepareAnalyzer {
    private final ConnectContext session;

    public PrepareAnalyzer(ConnectContext session) {
        this.session = session;
    }

    public void analyze(PrepareStmt prepareStmt) {
        StatementBase innerStmt = prepareStmt.getInnerStmt();
        if (innerStmt instanceof PrepareStmt || innerStmt instanceof  ExecuteStmt
                || innerStmt instanceof DeallocateStmt) {
            throw new ValidateException("Invalid statement type for prepared statement", ErrorType.USER_ERROR);
        }
    }

    public void analyze(ExecuteStmt executeStmt) {
        PrepareStmtContext preparedStmtCtx = session.getPreparedStmt(executeStmt.getStmtName());
        if (preparedStmtCtx == null) {
            throw new ValidateException(
                    "Could not execute, since `" + executeStmt.getStmtName() + "` not exist", ErrorType.USER_ERROR);
        }
        if (executeStmt.getParamsExpr().size() != preparedStmtCtx.getStmt().getParameters().size()) {
            throw new SemanticException("Invalid arguments size "
                    + executeStmt.getParamsExpr().size() + ", expected "
                    + preparedStmtCtx.getStmt().getParameters().size());
        }
        // analyze arguments
        executeStmt.getParamsExpr().forEach(e -> ExpressionAnalyzer.analyzeExpressionIgnoreSlot(e, session));
    }
}
