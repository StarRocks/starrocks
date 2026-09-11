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

import com.starrocks.catalog.AIModel;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterAIModelStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateAIModelStmt;
import com.starrocks.sql.ast.DescAIModelStmt;
import com.starrocks.sql.ast.DropAIModelStmt;
import com.starrocks.sql.ast.StatementBase;

public final class AIModelAnalyzer {
    private AIModelAnalyzer() {
    }

    public static void analyze(StatementBase statement, ConnectContext context) {
        new Visitor().visit(statement, context);
    }

    private static class Visitor implements AstVisitor<Void, ConnectContext> {
        @Override
        public Void visitCreateAIModelStatement(CreateAIModelStmt statement, ConnectContext context) {
            FeNameFormat.checkCommonName("AI model", statement.getName());
            try {
                AIModel.validateCreateProperties(statement.getProperties());
            } catch (DdlException e) {
                throw new SemanticException(e.getMessage(), statement.getPos());
            }
            return null;
        }

        @Override
        public Void visitAlterAIModelStatement(AlterAIModelStmt statement, ConnectContext context) {
            FeNameFormat.checkCommonName("AI model", statement.getName());
            try {
                AIModel.validateAlterProperties(statement.getProperties());
            } catch (DdlException e) {
                throw new SemanticException(e.getMessage(), statement.getPos());
            }
            return null;
        }

        @Override
        public Void visitDropAIModelStatement(DropAIModelStmt statement, ConnectContext context) {
            FeNameFormat.checkCommonName("AI model", statement.getName());
            return null;
        }

        @Override
        public Void visitDescAIModelStatement(DescAIModelStmt statement, ConnectContext context) {
            FeNameFormat.checkCommonName("AI model", statement.getName());
            return null;
        }
    }
}
