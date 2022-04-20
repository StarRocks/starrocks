// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.AlterClause;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRenameClause;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.common.MetaUtils;

import java.util.List;

public class AlterTableStatementAnalyzer {
    public static void analyze(DdlStmt ddlStmt, ConnectContext session) {
        new AlterStmtAnalyzerVisitor().analyze(ddlStmt, session);
    }

    public static void analyze(AlterClause alterTableClause, ConnectContext session) {
        new AlterTableClauseAnalyzerVisitor().analyze(alterTableClause, session);
    }

    static class AlterStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(DdlStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitAlterTableStatement(AlterTableStmt statement, ConnectContext context) {
            TableName tbl = statement.getTbl();
            MetaUtils.normalizationTableName(context, tbl);
            try {
                CatalogUtils.checkOlapTableHasStarOSPartition(tbl.getDb(), tbl.getTbl());
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_TABLES_USED);
            }
            List<AlterClause> alterClauseList = statement.getOps();
            if (alterClauseList == null || alterClauseList.isEmpty()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_ALTER_OPERATION);
            }
            for (AlterClause alterClause : alterClauseList) {
                if (StatementPlanner.isNewAlterTableClause(alterClause)) {
                    AlterTableStatementAnalyzer.analyze(alterClause, context);
                }
            }
            return null;
        }
    }

    static class AlterTableClauseAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(AlterClause statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitTableRenameClause(TableRenameClause statement, ConnectContext context) {
            String newTableName = statement.getNewTableName();
            if (Strings.isNullOrEmpty(newTableName)) {
                throw new SemanticException("New Table name is not set");
            }
            try {
                FeNameFormat.checkTableName(newTableName);
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_TABLE_NAME);
            }
            return null;
        }
    }


}
