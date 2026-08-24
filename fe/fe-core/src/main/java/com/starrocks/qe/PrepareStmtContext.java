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

package com.starrocks.qe;

import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.QueryAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;

import java.util.Collections;
import java.util.List;

public class PrepareStmtContext {
    // stmt is the analyzed working copy used only for COM_STMT_PREPARE metadata and protocol state.
    private final PrepareStmt stmt;
    private final ConnectContext connectContext;
    // Captured before the metadata AST is marked/analyzed. Reparse the original statement rather
    // than serializing the AST: the normal serializer intentionally redacts credentials.
    private final OriginStatement originalStatement;
    // Parsing semantics belong to the PREPARE boundary. A later SET sql_dialect/sql_mode (or
    // parser-limit change) must not reinterpret the stored statement during EXECUTE.
    private final SessionVariable parserSessionVariable;
    private final String preparedCatalog;
    private final String preparedDatabase;
    private final boolean preparedRelationAliasCaseInsensitive;

    private ExecPlan execPlan;
    // A separately parsed and analyzed, policy-free point-query statement. Never aliases stmt.
    private PrepareStmt cachedExecutableStmt;
    private boolean isCached = false;
    private long lastSchemaUpdateTime = -1;
    private long tableId = -1;

    public PrepareStmtContext(PrepareStmt stmt, ConnectContext connectContext, ExecPlan execPlan) {
        this(stmt, connectContext, execPlan,
                new OriginStatement(AstToSQLBuilder.toSQLWithCredential(stmt.getInnerStmt()), 0));
    }

    public PrepareStmtContext(PrepareStmt stmt, ConnectContext connectContext, ExecPlan execPlan,
                              OriginStatement originalStatement) {
        this.stmt = stmt;
        this.connectContext = connectContext;
        this.execPlan = execPlan;
        this.originalStatement = originalStatement;
        this.parserSessionVariable = connectContext.getSessionVariable().clone();
        this.preparedCatalog = connectContext.getCurrentCatalog();
        this.preparedDatabase = connectContext.getDatabase();
        this.preparedRelationAliasCaseInsensitive = connectContext.isRelationAliasCaseInsensitive();
    }

    public PrepareStmt getStmt() {
        return stmt;
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    public void setExecPlan(ExecPlan execPlan) {
        this.execPlan = execPlan;
    }

    public ExecPlan getExecPlan() {
        return execPlan;
    }

    public PrepareStmt instantiate(List<Expr> values) {
        List<StatementBase> parsedStatements = parseOriginalStatement();
        if (originalStatement.getIdx() >= parsedStatements.size()) {
            throw new SemanticException("Prepared statement index changed while rebuilding execution AST");
        }
        StatementBase parsed = parsedStatements.get(originalStatement.getIdx());
        PrepareStmt executableStmt;
        if (parsed instanceof PrepareStmt) {
            executableStmt = (PrepareStmt) parsed;
            executableStmt.setName(stmt.getName());
        } else {
            executableStmt = new PrepareStmt(stmt.getName(), parsed, Collections.emptyList());
        }

        if (executableStmt.getParameters().size() != stmt.getParameters().size()) {
            throw new SemanticException("Prepared statement parameter count changed while rebuilding execution AST");
        }
        executableStmt.assignValues(values);
        return executableStmt;
    }

    private List<StatementBase> parseOriginalStatement() {
        SessionVariable currentSessionVariable = connectContext.getSessionVariable();
        String currentDatabase = connectContext.getDatabase();
        boolean currentRelationAliasCaseInsensitive = connectContext.isRelationAliasCaseInsensitive();
        ConnectContext previousThreadContext = ConnectContext.exchangeThreadLocalInfo(connectContext);
        try {
            // Some parser paths consult ConnectContext.get() instead of their explicit argument
            // (for example, large IN predicates), and Trino parsing updates the alias mode on the
            // context. Parse under the complete PREPARE-time snapshot, then restore every bit of
            // caller-visible state.
            SessionVariable parserVariables = parserSessionVariable.clone();
            parserVariables.setCatalog(preparedCatalog);
            connectContext.setSessionVariable(parserVariables);
            connectContext.setDatabase(preparedDatabase);
            connectContext.setRelationAliasCaseInSensitive(preparedRelationAliasCaseInsensitive);
            return SqlParser.parse(originalStatement.getOrigStmt(), parserVariables);
        } finally {
            connectContext.setSessionVariable(currentSessionVariable);
            connectContext.setDatabase(currentDatabase);
            connectContext.setRelationAliasCaseInSensitive(currentRelationAliasCaseInsensitive);
            if (previousThreadContext == null) {
                ConnectContext.remove();
            } else {
                ConnectContext.set(previousThreadContext);
            }
        }
    }

    public String getPreparedCatalog() {
        return preparedCatalog;
    }

    public String getPreparedDatabase() {
        return preparedDatabase;
    }

    public boolean isPreparedRelationAliasCaseInsensitive() {
        return preparedRelationAliasCaseInsensitive;
    }

    public StatementBase bindCached(List<Expr> values) {
        if (cachedExecutableStmt == null) {
            throw new IllegalStateException("Cached prepared statement is missing");
        }
        return cachedExecutableStmt.assignValues(values);
    }

    public String getBoundSqlForAudit(List<Expr> values) {
        PrepareStmt executableStmt;
        if (cachedExecutableStmt == null) {
            executableStmt = instantiate(values);
        } else {
            cachedExecutableStmt.assignValues(values);
            executableStmt = cachedExecutableStmt;
        }
        return AstToSQLBuilder.toSQL(executableStmt.getInnerStmt());
    }

    public boolean isCached() {
        return isCached;
    }

    public void updateLastSchemaUpdateTime(QueryStatement stmt, ConnectContext session) {
        SelectRelation selectRelation = (SelectRelation) (stmt.getQueryRelation());
        TableRelation tableRelation = (TableRelation) selectRelation.getRelation();
        QueryAnalyzer queryAnalyzer = new QueryAnalyzer(session);
        OlapTable table = (OlapTable) queryAnalyzer.resolveTable(tableRelation);
        this.lastSchemaUpdateTime = table.lastSchemaUpdateTime.get();
        this.tableId = table.getId();
    }

    public void cachePlan(ExecPlan execPlan, PrepareStmt executableStmt) {
        this.execPlan = execPlan;
        this.cachedExecutableStmt = executableStmt;
        this.isCached = true;
    }

    public boolean needReAnalyze(QueryStatement stmt, ConnectContext session) {
        SelectRelation selectRelation = (SelectRelation) (stmt.getQueryRelation());
        TableRelation tableRelation = (TableRelation) selectRelation.getRelation();
        QueryAnalyzer queryAnalyzer = new QueryAnalyzer(session);
        OlapTable table = (OlapTable) queryAnalyzer.resolveTable(tableRelation);
        long lastSchemaUpdateTime = table.lastSchemaUpdateTime.get();
        long tableId = table.getId();
        if (lastSchemaUpdateTime > this.lastSchemaUpdateTime) {
            return true;
        }
        if (tableId != this.tableId) {
            return true;
        }
        return false;
    }

    public void reset() {
        this.isCached = false;
        this.lastSchemaUpdateTime = -1;
        this.tableId = -1;
        this.execPlan = null;
        this.cachedExecutableStmt = null;
    }

}
