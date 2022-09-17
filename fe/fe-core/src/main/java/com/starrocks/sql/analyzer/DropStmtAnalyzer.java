// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FunctionSearchDesc;
import com.starrocks.catalog.InfoSchemaDb;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
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
        public Void visitDropTableStmt(DropTableStmt statement, ConnectContext context) {
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
            String dbName = statement.getDbName();
            if (dbName.equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, context.getQualifiedUser(), dbName);
            }
            return null;
        }

        @Override
        public Void visitDropFunction(DropFunctionStmt statement, ConnectContext context) {
            try {
                // analyze function name
                FunctionName functionName = statement.getFunctionName();
                functionName.analyze(context.getDatabase());
                // analyze arguments
                FunctionArgsDef argsDef = statement.getArgsDef();
                argsDef.analyze();

                statement.setFunction(
                        new FunctionSearchDesc(functionName, argsDef.getArgTypes(), argsDef.isVariadic()));
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }

            return null;
        }
    }

}
