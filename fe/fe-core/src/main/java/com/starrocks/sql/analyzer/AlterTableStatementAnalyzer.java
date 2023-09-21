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
import com.starrocks.catalog.Table;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.common.MetaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class AlterTableStatementAnalyzer {
    private static final Logger LOG = LogManager.getLogger(AlterTableStatementAnalyzer.class);

    public static void analyze(AlterTableStmt statement, ConnectContext context) {
        TableName tbl = statement.getTbl();
        MetaUtils.normalizationTableName(context, tbl);
        Table table = MetaUtils.getTable(context, tbl);
        MetaUtils.checkNotSupportCatalog(tbl.getCatalog(), "ALTER");
        List<AlterClause> alterClauseList = statement.getOps();
        if (alterClauseList == null || alterClauseList.isEmpty()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_NO_ALTER_OPERATION);
        }
        AlterTableClauseVisitor alterTableClauseAnalyzerVisitor = new AlterTableClauseVisitor();
        alterTableClauseAnalyzerVisitor.setTable(table);
        for (AlterClause alterClause : alterClauseList) {
            alterTableClauseAnalyzerVisitor.analyze(alterClause, context);
        }
    }
}
