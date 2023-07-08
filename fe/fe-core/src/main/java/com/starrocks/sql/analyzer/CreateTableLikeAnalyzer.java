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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.parser.SqlParser;

import java.util.List;

public class CreateTableLikeAnalyzer {

    public static void analyze(CreateTableLikeStmt stmt, ConnectContext context) {
        MetaUtils.normalizationTableName(context, stmt.getDbTbl());
        MetaUtils.normalizationTableName(context, stmt.getExistedDbTbl());
        String tableName = stmt.getTableName();
        FeNameFormat.checkTableName(tableName);

        Database db = MetaUtils.getDatabase(context, stmt.getExistedDbTbl());
        Table table = MetaUtils.getTable(stmt.getExistedDbTbl());

        List<String> createTableStmt = Lists.newArrayList();
        GlobalStateMgr.getDdlStmt(stmt.getDbName(), table, createTableStmt, null, null, false, false);
        if (createTableStmt.isEmpty()) {
            ErrorReport.reportSemanticException(ErrorCode.ERROR_CREATE_TABLE_LIKE_EMPTY, "CREATE");
        }

        StatementBase statementBase =
                SqlParser.parseOneWithStarRocksDialect(createTableStmt.get(0), context.getSessionVariable());
        if (statementBase instanceof CreateTableStmt) {
            CreateTableStmt parsedCreateTableStmt = (CreateTableStmt) statementBase;
            parsedCreateTableStmt.setTableName(stmt.getTableName());
            if (stmt.isSetIfNotExists()) {
                parsedCreateTableStmt.setIfNotExists();
            }
            if (stmt.getProperties() != null) {
                parsedCreateTableStmt.updateProperties(stmt.getProperties());
            }
            if (stmt.getDistributionDesc() != null) {
                parsedCreateTableStmt.setDistributionDesc(stmt.getDistributionDesc());
            }
            if (stmt.getPartitionDesc() != null) {
                parsedCreateTableStmt.setPartitionDesc(stmt.getPartitionDesc());
            }

            com.starrocks.sql.analyzer.Analyzer.analyze(parsedCreateTableStmt, context);
            stmt.setCreateTableStmt(parsedCreateTableStmt);
        } else {
            ErrorReport.reportSemanticException(ErrorCode.ERROR_CREATE_TABLE_LIKE_UNSUPPORTED_VIEW);
        }
    }
}