// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.AlterClause;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.CreateIndexClause;
import com.starrocks.analysis.IndexDef;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRenameClause;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.common.MetaUtils;

import java.util.List;

public class AlterTableStatementAnalyzer {
    public static void analyze(AlterTableStmt statement, ConnectContext context) {
        TableName tbl = statement.getTbl();
        MetaUtils.normalizationTableName(context, tbl);
        Table table = MetaUtils.getTable(context, tbl);
        if (table instanceof MaterializedView) {
            throw new SemanticException(
                    "The '%s' cannot be alter by 'ALTER TABLE', because '%s' is a materialized view," +
                            "you can use 'ALTER MATERIALIZED VIEW' to alter it.",
                    tbl.getTbl(), tbl.getTbl());
        }
        try {
            CatalogUtils.checkIsLakeTable(tbl.getDb(), tbl.getTbl());
        } catch (AnalysisException e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_NO_TABLES_USED);
        }
        List<AlterClause> alterClauseList = statement.getOps();
        if (alterClauseList == null || alterClauseList.isEmpty()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_NO_ALTER_OPERATION);
        }
        AlterTableClauseAnalyzerVisitor alterTableClauseAnalyzerVisitor = new AlterTableClauseAnalyzerVisitor();
        for (AlterClause alterClause : alterClauseList) {
            alterTableClauseAnalyzerVisitor.analyze(alterClause, context);
        }
    }

    static class AlterTableClauseAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(AlterClause statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCreateIndexClause(CreateIndexClause clause, ConnectContext context) {
            IndexDef indexDef = clause.getIndexDef();
            indexDef.analyze();
            clause.setIndex(new Index(indexDef.getIndexName(), indexDef.getColumns(),
                    indexDef.getIndexType(), indexDef.getComment()));
            return null;
        }

        @Override
        public Void visitTableRenameClause(TableRenameClause clause, ConnectContext context) {
            String newTableName = clause.getNewTableName();
            if (Strings.isNullOrEmpty(newTableName)) {
                throw new SemanticException("New Table name is not set");
            }
            try {
                FeNameFormat.checkTableName(newTableName);
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_TABLE_NAME, newTableName);
            }
            return null;
        }
    }
}
