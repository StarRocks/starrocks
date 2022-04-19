// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.AlterClause;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.common.MetaUtils;

import java.util.List;

public class AlterStmtAnalyzer {
    public static void analyze(DdlStmt ddlStmt, ConnectContext session) {
        new AlterStmtAnalyzerVisitor().analyze(ddlStmt, session);
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
            com.starrocks.analysis.Analyzer analyzer = new Analyzer(context.getCatalog(), context);
            for (AlterClause alterClause : alterClauseList) {
                if (StatementPlanner.isNewAlterTableClause(alterClause)) {
                    AlterTableClauseAnalyzer.analyze(alterClause, context);
                } else {
                    try {
                        alterClause.analyze(analyzer);
                    } catch (UserException e) {
                        throw new SemanticException(e.getMessage());
                    }
                }

            }
            return null;
        }


    }


}
