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

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.error.ErrorCode;
import com.starrocks.common.error.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableColumnClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CompactionClause;
import com.starrocks.sql.ast.CreateIndexClause;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.DropIndexClause;
import com.starrocks.sql.common.MetaUtils;

import java.util.List;

import static com.starrocks.common.util.PropertyAnalyzer.PROPERTIES_BF_COLUMNS;

public class AlterTableStatementAnalyzer {
    public static void analyze(AlterTableStmt statement, ConnectContext context) {
        TableName tbl = statement.getTbl();
        MetaUtils.normalizationTableName(context, tbl);
        MetaUtils.checkNotSupportCatalog(tbl.getCatalog(), "ALTER");
        List<AlterClause> alterClauseList = statement.getOps();
        if (alterClauseList == null || alterClauseList.isEmpty()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_NO_ALTER_OPERATION);
        }

        Table table = MetaUtils.getTable(context, tbl);
        if (table instanceof MaterializedView && alterClauseList != null) {
            for (AlterClause alterClause : alterClauseList) {
                if (!indexCluase(alterClause)) {
                    String msg = String.format("The '%s' cannot be alter by 'ALTER TABLE', because it is a materialized view," +
                            "you can use 'ALTER MATERIALIZED VIEW' to alter it.", tbl.getTbl());
                    throw new SemanticException(msg, tbl.getPos());
                }
            }
        }
        AlterTableClauseVisitor alterTableClauseAnalyzerVisitor = new AlterTableClauseVisitor();
        alterTableClauseAnalyzerVisitor.setTable(table);
        for (AlterClause alterClause : alterClauseList) {
            if ((table instanceof OlapTable) &&
                    ((OlapTable) table).hasRowStorageType() &&
                    (alterClause instanceof AddColumnClause || alterClause instanceof DropColumnClause ||
                            alterClause instanceof AlterTableColumnClause)) {
                throw new SemanticException(String.format("row store table %s can't do schema change", table.getName()));
            }
            if (RunMode.isSharedDataMode() && alterClause instanceof CompactionClause) {
                throw new SemanticException("manually compact not supported in SHARED_DATA runMode");
            }
            alterTableClauseAnalyzerVisitor.analyze(alterClause, context);
        }
    }

    public static boolean indexCluase(AlterClause alterClause) {
        if (alterClause instanceof CreateIndexClause || alterClause instanceof DropIndexClause) {
            return true;
        } else if (alterClause.getProperties() != null && alterClause.getProperties().containsKey(PROPERTIES_BF_COLUMNS)) {
            return true;
        }
        return false;
    }
}
