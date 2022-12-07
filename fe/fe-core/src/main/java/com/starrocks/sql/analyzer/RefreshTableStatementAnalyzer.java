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

import com.starrocks.analysis.TableName;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.common.MetaUtils;

public class RefreshTableStatementAnalyzer {
    public static void analyze(RefreshTableStmt statement, ConnectContext context) {
        new RefreshTableStatementAnalyzer.RefreshTableStatementAnalyzerVisitor().visit(statement, context);
    }

    static class RefreshTableStatementAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        private final MetadataMgr metadataMgr;

        public RefreshTableStatementAnalyzerVisitor() {
            this.metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        }

        public void analyze(DdlStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitRefreshTableStatement(RefreshTableStmt statement, ConnectContext context) {
            TableName tableName = statement.getTableName();
            MetaUtils.normalizationTableName(context, tableName);
            String catalogName = tableName.getCatalog();
            String dbName = tableName.getDb();
            String tblName = tableName.getTbl();

            if (!GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(catalogName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_CATALOG_ERROR, catalogName);
            }
            if (metadataMgr.getDb(catalogName, dbName) == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR, dbName);
            }
            if (metadataMgr.getTable(catalogName, dbName, tblName) == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR, tblName);
            }

            return null;
        }
    }
}
