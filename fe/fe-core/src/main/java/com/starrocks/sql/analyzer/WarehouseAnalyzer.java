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
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.warehouse.AlterWarehouseStmt;
import com.starrocks.sql.ast.warehouse.CreateWarehouseStmt;
import com.starrocks.sql.ast.warehouse.DropWarehouseStmt;
import com.starrocks.sql.ast.warehouse.ResumeWarehouseStmt;
import com.starrocks.sql.ast.warehouse.SetWarehouseStmt;
import com.starrocks.sql.ast.warehouse.ShowWarehousesStmt;
import com.starrocks.sql.ast.warehouse.SuspendWarehouseStmt;

public class WarehouseAnalyzer {
    public static void analyze(StatementBase stmt, ConnectContext session) {
        new WarehouseAnalyzerVisitor().visit(stmt, session);
    }

    static class WarehouseAnalyzerVisitor implements AstVisitor<Void, ConnectContext> {
        public void analyze(ShowStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCreateWarehouseStatement(CreateWarehouseStmt statement, ConnectContext context) {
            String whName = statement.getWarehouseName();
            if (Strings.isNullOrEmpty(whName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_WAREHOUSE_NAME);
            }
            FeNameFormat.checkWarehouseName(whName);
            return null;
        }

        @Override
        public Void visitSuspendWarehouseStatement(SuspendWarehouseStmt statement, ConnectContext context) {
            String whName = statement.getWarehouseName();
            if (Strings.isNullOrEmpty(whName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_WAREHOUSE_NAME);
            }
            return null;
        }

        public Void visitResumeWarehouseStatement(ResumeWarehouseStmt statement, ConnectContext context) {
            String whName = statement.getWarehouseName();
            if (Strings.isNullOrEmpty(whName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_WAREHOUSE_NAME);
            }
            return null;
        }

        @Override
        public Void visitDropWarehouseStatement(DropWarehouseStmt statement, ConnectContext context) {
            String whName = statement.getWarehouseName();
            if (Strings.isNullOrEmpty(whName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_WAREHOUSE_NAME);
            }

            if (whName.equals(WarehouseManager.DEFAULT_WAREHOUSE_NAME)) {
                throw new SemanticException("Can't drop the default_warehouse");
            }

            return null;
        }

        @Override
        public Void visitSetWarehouseStatement(SetWarehouseStmt statement, ConnectContext context) {
            if (RunMode.getCurrentRunMode() == RunMode.SHARED_NOTHING) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NOT_SUPPORTED_STATEMENT_IN_SHARED_NOTHING_MODE);
            }

            String whName = statement.getWarehouseName();
            if (Strings.isNullOrEmpty(whName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_WAREHOUSE_NAME);
            }
            FeNameFormat.checkWarehouseName(whName);
            return null;
        }

        @Override
        public Void visitShowWarehousesStatement(ShowWarehousesStmt node, ConnectContext context) {
            return null;
        }

        @Override
        public Void visitAlterWarehouseStatement(AlterWarehouseStmt statement, ConnectContext context) {
            String whName = statement.getWarehouseName();
            if (Strings.isNullOrEmpty(whName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_WAREHOUSE_NAME);
            }

            return null;
        }
    }

}