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
import com.starrocks.sql.ast.AlterWarehouseStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateWarehouseStmt;
import com.starrocks.sql.ast.DropWarehouseStmt;
import com.starrocks.sql.ast.ResumeWarehouseStmt;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SuspendWarehouseStmt;

public class WarehouseAnalyzer {
    public static void analyze(StatementBase stmt, ConnectContext session) {
        new WarehouseAnalyzer.WarehouseAnalyzerVisitor().visit(stmt, session);
    }

    static class WarehouseAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(ShowStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCreateWarehouseStatement(CreateWarehouseStmt statement, ConnectContext context) {
            String whName = statement.getFullWhName();
            if (Strings.isNullOrEmpty(whName)) {
                throw new SemanticException("'warehouse name' can not be null or empty");
            }
            FeNameFormat.checkWarehouseName(whName);
            return null;
        }

        @Override
        public Void visitAlterWarehouseStatement(AlterWarehouseStmt statement, ConnectContext context) {
            String whName = statement.getFullWhName();
            if (Strings.isNullOrEmpty(whName)) {
                throw new SemanticException("'warehouse name' can not be null or empty");
            }
            return null;
        }

        @Override
        public Void visitSuspendWarehouseStatement(SuspendWarehouseStmt statement, ConnectContext context) {
            String whName = statement.getFullWhName();
            if (Strings.isNullOrEmpty(whName)) {
                throw new SemanticException("'warehouse name' can not be null or empty");
            }
            return null;
        }

        public Void visitResumeWarehouseStatement(ResumeWarehouseStmt statement, ConnectContext context) {
            String whName = statement.getFullWhName();
            if (Strings.isNullOrEmpty(whName)) {
                throw new SemanticException("'warehouse name' can not be null or empty");
            }
            return null;
        }

        @Override
        public Void visitDropWarehouseStatement(DropWarehouseStmt statement, ConnectContext context) {
            String whName = statement.getFullWhName();
            if (Strings.isNullOrEmpty(whName)) {
                throw new SemanticException("'warehouse name' can not be null or empty");
            }
            return null;
        }
    }

}
