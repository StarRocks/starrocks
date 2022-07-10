// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.DropDbStmt;
import com.starrocks.analysis.DropFunctionStmt;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.analysis.FunctionArgsDef;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FunctionSearchDesc;
import com.starrocks.catalog.InfoSchemaDb;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.View;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.common.MetaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

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
                    ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_OBJECT, db.getFullName(), tableName, "VIEW");
                }
            } else {
                if (table instanceof View) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_OBJECT, db.getFullName(), tableName, "TABLE");
                }
            }
            return null;
        }

        @Override
        public Void visitDropDbStatement(DropDbStmt statement, ConnectContext context) {
            String dbName = statement.getDbName();
            String catalog = context.getCurrentCatalog();
            if (CatalogMgr.isInternalCatalog(catalog)) {
                dbName = ClusterNamespace.getFullName(dbName);
            }
            statement.setDbName(dbName);
            if (dbName.equalsIgnoreCase(ClusterNamespace.getFullName(InfoSchemaDb.DATABASE_NAME))) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, context.getQualifiedUser(), dbName);
            }
            return null;
        }

        @Override
        public Void visitDropFunction(DropFunctionStmt statement, ConnectContext context) {

            // analyze function name
            FunctionName functionName = statement.getFunctionName();
            FunctionAnalyzer.analyzeFunctionName(functionName, context);
            // analyze arguments
            FunctionArgsDef argsDef = statement.getArgsDef();

            try {
                List<TypeDef> argDefs = argsDef.getArgTypeDefs();
                Type[] argTypes = new Type[argDefs.size()];
                int i = 0;
                for (TypeDef typeDef : argDefs) {
                    typeDef.analyze(null);
                    argTypes[i++] = typeDef.getType();
                }
                argsDef.setArgTypes(argTypes);

            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }

            statement.setFunction(new FunctionSearchDesc(functionName, argsDef.getArgTypes(), argsDef.isVariadic()));
            return null;
        }
    }

}
