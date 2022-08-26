// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.common;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.starrocks.analysis.CreateMaterializedViewStmt;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SqlScanner;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.util.SqlParserUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.SqlParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.StringReader;
import java.util.List;
import java.util.Map;

public class MetaUtils {

    private static final Logger LOG = LogManager.getLogger(MetaUtils.class);

    public static Database getDatabase(ConnectContext session, long dbId) {
        Database db = session.getGlobalStateMgr().getDb(dbId);
        if (db == null) {
            throw new SemanticException("Database %s is not find", dbId);
        }
        return db;
    }

    public static Table getTable(ConnectContext session, long dbId, long tableId) {
        Database db = session.getGlobalStateMgr().getDb(dbId);
        if (db == null) {
            throw new SemanticException("Database %s is not find", dbId);
        }
        Table table = db.getTable(tableId);
        if (table == null) {
            throw new SemanticException("Unknown table: %s", tableId);
        }
        return table;
    }

    public static Database getDatabase(ConnectContext session, TableName tableName) {
        Database db = session.getGlobalStateMgr().getDb(tableName.getDb());
        if (db == null) {
            throw new SemanticException("Database %s is not find", tableName.getDb());
        }
        return db;
    }

    public static Table getTable(TableName tableName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(tableName.getDb());
        if (db == null) {
            throw new SemanticException("Database %s is not find", tableName.getDb());
        }
        Table table = db.getTable(tableName.getTbl());
        if (table == null) {
            throw new SemanticException("Unknown table '%s", tableName.getTbl());
        }
        return table;
    }

    public static Table getTable(ConnectContext session, TableName tableName) {
        Database db = session.getGlobalStateMgr().getDb(tableName.getDb());
        if (db == null) {
            throw new SemanticException("Database %s is not find", tableName.getDb());
        }
        Table table = db.getTable(tableName.getTbl());
        if (table == null) {
            throw new SemanticException("Unknown table %s", tableName.getTbl());
        }
        return table;
    }

    public static void normalizationTableName(ConnectContext connectContext, TableName tableName) {
        if (Strings.isNullOrEmpty(tableName.getCatalog())) {
            if (Strings.isNullOrEmpty(connectContext.getCurrentCatalog())) {
                throw new SemanticException("No catalog selected");
            }
            tableName.setCatalog(connectContext.getCurrentCatalog());
        }
        if (Strings.isNullOrEmpty(tableName.getDb())) {
            if (Strings.isNullOrEmpty(connectContext.getDatabase())) {
                throw new SemanticException("No database selected");
            }
            tableName.setDb(connectContext.getDatabase());
        } else {
            if (CatalogMgr.isInternalCatalog(tableName.getCatalog())) {
                tableName.setDb(ClusterNamespace.getFullName(tableName.getDb()));
            }
        }

        if (Strings.isNullOrEmpty(tableName.getTbl())) {
            throw new SemanticException("Table name is null");
        }
    }

    public static Map<String, Expr> parseColumnNameToDefineExpr(OriginStatement originStmt) {
        CreateMaterializedViewStmt stmt;

        try {
            List<StatementBase> stmts = SqlParser.parse(originStmt.originStmt, SqlModeHelper.MODE_DEFAULT);
            stmt = (CreateMaterializedViewStmt) stmts.get(originStmt.idx);
            stmt.setIsReplay(true);
            return stmt.parseDefineExprWithoutAnalyze(originStmt.originStmt);
        } catch (Exception e) {
            LOG.warn("error happens when parsing create materialized view stmt [{}] use new parser",
                    originStmt, e);
        }

        // compatibility old parser can work but new parser failed
        com.starrocks.analysis.SqlParser parser = new com.starrocks.analysis.SqlParser(
                new SqlScanner(new StringReader(originStmt.originStmt),
                        SqlModeHelper.MODE_DEFAULT));
        try {
            stmt = (CreateMaterializedViewStmt) SqlParserUtils.getStmt(parser, originStmt.idx);
            stmt.setIsReplay(true);
            return stmt.parseDefineExprWithoutAnalyze(originStmt.originStmt);
        } catch (Exception e) {
            LOG.warn("error happens when parsing create materialized view stmt [{}] use old parser:",
                    originStmt, e);
        }
        // suggestion
        LOG.warn("The materialized view [{}] has encountered compatibility problems. " +
                "It is best to delete the materialized view and rebuild it to maintain the best compatibility.",
                originStmt.originStmt);
        return Maps.newConcurrentMap();
    }

}
