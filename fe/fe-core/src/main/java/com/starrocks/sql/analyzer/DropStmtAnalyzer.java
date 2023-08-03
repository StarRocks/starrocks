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
import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSearchDesc;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.catalog.system.sys.SysDb;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropFunctionStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.FunctionArgsDef;
import com.starrocks.sql.common.MetaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DropStmtAnalyzer {
    private static final Logger LOG = LogManager.getLogger(DropStmtAnalyzer.class);

    public static void analyze(DdlStmt ddlStmt, ConnectContext session) {
        new DropStmtAnalyzerVisitor().analyze(ddlStmt, session);
    }

    static class DropStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(DdlStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitDropTableStatement(DropTableStmt statement, ConnectContext context) {
            MetaUtils.normalizationTableName(context, statement.getTableNameObject());
            String dbName = statement.getDbName();
            // check database
            Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
            if (db == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
            }
            db.readLock();
            Table table;
            String tableName = statement.getTableName();
            try {
                table = db.getTable(statement.getTableName());
                if (table == null) {
                    if (statement.isSetIfExists()) {
                        LOG.info("drop table[{}] which does not exist", tableName);
                        return null;
                    } else {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
                    }
                } else {
                    if (table instanceof MaterializedView) {
                        throw new SemanticException(
                                "The data of '%s' cannot be dropped because '%s' is a materialized view," +
                                        "use 'drop materialized view %s' to drop it.",
                                tableName, tableName, tableName);
                    }
                }
            } finally {
                db.readUnlock();
            }
            // Check if a view
            if (statement.isView()) {
                if (!(table instanceof View)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_OBJECT, db.getOriginName(), tableName, "VIEW");
                }
            } else {
                if (table instanceof View) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_OBJECT, db.getOriginName(), tableName, "TABLE");
                }
            }
            return null;
        }

        @Override
        public Void visitDropDbStatement(DropDbStmt statement, ConnectContext context) {
            if (Strings.isNullOrEmpty(statement.getCatalogName())) {
                if (Strings.isNullOrEmpty(context.getCurrentCatalog())) {
                    throw new SemanticException("No catalog selected");
                }
                statement.setCatalogName(context.getCurrentCatalog());
            }

            String dbName = statement.getDbName();
            if (dbName.equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, context.getQualifiedUser(), dbName);
            } else if (dbName.equalsIgnoreCase(SysDb.DATABASE_NAME)) {
                Database db = GlobalStateMgr.getCurrentState().getDb(SysDb.DATABASE_NAME.toLowerCase());
                if (db.getId() == SystemId.SYS_DB_ID) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, context.getQualifiedUser(), dbName);
                }
            }
            return null;
        }

        @Override
        public Void visitDropFunctionStatement(DropFunctionStmt statement, ConnectContext context) {
            try {
                // analyze function name
                FunctionName functionName = statement.getFunctionName();
                functionName.analyze(context.getDatabase());
                // analyze arguments
                FunctionArgsDef argsDef = statement.getArgsDef();
                argsDef.analyze();

                FunctionSearchDesc funcDesc = new FunctionSearchDesc(functionName, argsDef.getArgTypes(),
                        argsDef.isVariadic());
                statement.setFunctionSearchDesc(funcDesc);

                // check function existence
                Function func;
                if (functionName.isGlobalFunction()) {
                    func = GlobalStateMgr.getCurrentState().getGlobalFunctionMgr().getFunction(funcDesc);
                    if (func == null) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_FUNC_ERROR, funcDesc.toString());
                    }
                } else {
                    Database db = GlobalStateMgr.getCurrentState().getDb(functionName.getDb());
                    if (db != null) {
                        try {
                            db.readLock();
                            func = db.getFunction(statement.getFunctionSearchDesc());
                            if (func == null) {
                                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_FUNC_ERROR, funcDesc.toString());
                            }
                        } finally {
                            db.readUnlock();
                        }
                    }
                }
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }

            return null;
        }
    }

}
