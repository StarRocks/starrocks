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
import com.starrocks.analysis.TableName;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSearchDesc;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.catalog.system.sys.SysDb;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.TemporaryTableMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropFunctionStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.DropTemporaryTableStmt;
import com.starrocks.sql.ast.FunctionArgsDef;
import com.starrocks.sql.common.MetaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;

public class DropStmtAnalyzer {
    private static final Logger LOG = LogManager.getLogger(DropStmtAnalyzer.class);

    public static void analyze(DdlStmt ddlStmt, ConnectContext session) {
        new DropStmtAnalyzerVisitor().analyze(ddlStmt, session);
    }

    static class DropStmtAnalyzerVisitor implements AstVisitor<Void, ConnectContext> {
        public void analyze(DdlStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitDropTableStatement(DropTableStmt statement, ConnectContext context) {
            statement.getTableNameObject().normalization(context);

            // check catalog
            String catalogName = statement.getCatalogName();
            MetaUtils.checkCatalogExistAndReport(catalogName);

            String dbName = statement.getDbName();
            // check database
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(context, catalogName, dbName);
            if (db == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
            }
            Table table = null;
            String tableName = statement.getTableName();
            try {
                table = MetaUtils.getSessionAwareTable(context, db, new TableName(catalogName, dbName, tableName));
            } catch (Exception e) {
                // an exception will be thrown if table is not found, just ignore it
            }
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
                if (table.isTemporaryTable()) {
                    statement.setTemporaryTableMark(true);
                }
            }
            // Check if a view
            if (statement.isView()) {
                if (!table.isView()) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_OBJECT, db.getOriginName(), tableName, "VIEW");
                }
            } else {
                if (table.isView()) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_OBJECT, db.getOriginName(), tableName, "TABLE");
                }
            }
            return null;
        }

        @Override
        public Void visitDropTemporaryTableStatement(DropTemporaryTableStmt statement, ConnectContext context) {
            statement.setSessionId(context.getSessionId());
            statement.getTableNameObject().normalization(context);

            // check catalog
            String catalogName = statement.getCatalogName();
            if (!CatalogMgr.isInternalCatalog(catalogName)) {
                throw new SemanticException("drop temporary table can only be execute under default catalog");
            }
            MetaUtils.checkCatalogExistAndReport(catalogName);

            String dbName = statement.getDbName();
            // check database
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(context, catalogName, dbName);
            if (db == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
            }
            statement.setSessionId(context.getSessionId());
            String tableName = statement.getTableName();
            TemporaryTableMgr temporaryTableMgr = GlobalStateMgr.getServingState().getTemporaryTableMgr();
            UUID sessionId = statement.getSessionId();
            if (!temporaryTableMgr.tableExists(sessionId, db.getId(), tableName)) {
                if (statement.isSetIfExists()) {
                    LOG.info("drop temporary table[{}.{}] in session[{}] which does not exist",
                            dbName, tableName, sessionId);
                    return null;
                } else {
                    LOG.info("drop temporary table[{}.{}] in session[{}] which does not exist",
                            dbName, tableName, sessionId);
                    ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
                }
            }
            return null;
        }

        @Override
        public Void visitDropDbStatement(DropDbStmt statement, ConnectContext context) {
            if (Strings.isNullOrEmpty(statement.getCatalogName())) {
                if (Strings.isNullOrEmpty(context.getCurrentCatalog())) {
                    throw new SemanticException(PARSER_ERROR_MSG.noCatalogSelected());
                }
                statement.setCatalogName(context.getCurrentCatalog());
            }

            MetaUtils.checkCatalogExistAndReport(statement.getCatalogName());

            String dbName = statement.getDbName();
            if (dbName.equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME)) {
                AccessDeniedException.reportAccessDenied(context.getCurrentCatalog(),
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.DROP.name(), ObjectType.DATABASE.name(), dbName);
            } else if (dbName.equalsIgnoreCase(SysDb.DATABASE_NAME)) {
                Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(SysDb.DATABASE_NAME.toLowerCase());
                if (db.getId() == SystemId.SYS_DB_ID) {
                    AccessDeniedException.reportAccessDenied(context.getCurrentCatalog(),
                            context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            PrivilegeType.DROP.name(), ObjectType.DATABASE.name(), dbName);
                }
            }
            return null;
        }

        @Override
        public Void visitDropFunctionStatement(DropFunctionStmt statement, ConnectContext context) {
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
                Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(functionName.getDb());
                if (db != null) {
                    func = db.getFunction(statement.getFunctionSearchDesc());
                    if (func == null) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_FUNC_ERROR, funcDesc.toString());
                    }
                }
            }

            return null;
        }
    }

}
