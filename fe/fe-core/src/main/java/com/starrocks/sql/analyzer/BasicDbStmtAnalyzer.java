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

import com.google.common.base.Strings;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.sql.ast.ShowCreateDbStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UseDbStmt;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;

/**
 * This is a basic analyzer for some database statements, currently used by
 * `use database`, `recover database`, and `show create database`, mainly to
 * initialize the catalog name to which the database belongs.
 */
public class BasicDbStmtAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new BasicDbStmtAnalyzerVisitor().analyze(statement, session);
    }

    private static class BasicDbStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitUseDbStatement(UseDbStmt statement, ConnectContext context) {
            statement.setCatalogName(getCatalogNameIfNotSet(statement.getCatalogName(), context));
            return null;
        }

        @Override
        public Void visitRecoverDbStatement(RecoverDbStmt statement, ConnectContext context) {
            statement.setCatalogName(getCatalogNameIfNotSet(statement.getCatalogName(), context));
            return null;
        }

        @Override
        public Void visitShowCreateDbStatement(ShowCreateDbStmt statement, ConnectContext context) {
            statement.setCatalogName(getCatalogNameIfNotSet(statement.getCatalogName(), context));
            return null;
        }

        private String getCatalogNameIfNotSet(String currentCatalogName, ConnectContext context) {
            String result = currentCatalogName;
            if (currentCatalogName == null) {
                if (Strings.isNullOrEmpty(context.getCurrentCatalog())) {
                    throw new SemanticException(PARSER_ERROR_MSG.noCatalogSelected());
                }
                result = context.getCurrentCatalog();
            }
            return result;
        }
    }
}
