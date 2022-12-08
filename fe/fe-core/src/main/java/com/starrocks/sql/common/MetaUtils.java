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

package com.starrocks.sql.common;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.rule.mv.MVUtils;
import com.starrocks.sql.parser.SqlParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class MetaUtils {

    private static final Logger LOG = LogManager.getLogger(MVUtils.class);

    public static void checkDbNullAndReport(Database db, String name) throws AnalysisException {
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, name);
        }
    }

    public static Database getDatabase(long dbId) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new SemanticException("Database %s is not found", dbId);
        }
        return db;
    }

    public static Table getTable(long dbId, long tableId) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new SemanticException("Database %s is not found", dbId);
        }
        Table table = db.getTable(tableId);
        if (table == null) {
            throw new SemanticException("Table %s is not found", tableId);
        }
        return table;
    }

    public static Database getDatabase(ConnectContext session, TableName tableName) {
        Database db = session.getGlobalStateMgr().getDb(tableName.getDb());
        if (db == null) {
            throw new SemanticException("Database %s is not found", tableName.getDb());
        }
        return db;
    }

    public static Table getTable(TableName tableName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(tableName.getDb());
        if (db == null) {
            throw new SemanticException("Database %s is not found", tableName.getDb());
        }
        Table table = db.getTable(tableName.getTbl());
        if (table == null) {
            throw new SemanticException("Table %s is not found", tableName.getTbl());
        }
        return table;
    }

    public static Table getTable(ConnectContext session, TableName tableName) {
        Database db = session.getGlobalStateMgr().getDb(tableName.getDb());
        if (db == null) {
            throw new SemanticException("Database %s is not found", tableName.getDb());
        }
        Table table = db.getTable(tableName.getTbl());
        if (table == null) {
            throw new SemanticException("Table %s is not found", tableName.getTbl());
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

        // suggestion
        LOG.warn("The materialized view [{}] has encountered compatibility problems. " +
                        "It is best to delete the materialized view and rebuild it to maintain the best compatibility.",
                originStmt.originStmt);
        return Maps.newConcurrentMap();
    }

}
