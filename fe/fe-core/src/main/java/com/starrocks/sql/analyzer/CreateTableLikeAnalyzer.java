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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.TableOperation;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateTemporaryTableLikeStmt;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;

import java.util.List;

public class CreateTableLikeAnalyzer {

    public static void analyze(CreateTableLikeStmt stmt, ConnectContext context) {
        TableRef tableRef = AnalyzerUtils.normalizedTableRef(stmt.getTableRef(), context);
        TableRef existedTableRef = AnalyzerUtils.normalizedTableRef(stmt.getExistedTableRef(), context);
        stmt.setTableRef(tableRef);
        stmt.setExistedTableRef(existedTableRef);

        TableName targetTableName = TableName.fromTableRef(tableRef);
        TableName existedTableName = TableName.fromTableRef(existedTableRef);
        FeNameFormat.checkTableName(targetTableName.getTbl());

        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(
                context, existedTableName.getCatalog(), existedTableName.getDb(), existedTableName.getTbl());
        if (table == null) {
            throw new SemanticException("Table %s is not found", existedTableName.getTbl());
        }
        MetaUtils.checkNotSupportCatalog(table, TableOperation.CREATE_TABLE_LIKE, existedTableName.getCatalog());

        List<String> createTableStmt = Lists.newArrayList();

        if (stmt instanceof CreateTemporaryTableLikeStmt) {
            if (!(table instanceof OlapTable)) {
                throw new SemanticException("temporary table only support olap engine");
            }
        }
        AstToStringBuilder.getDdlStmt(targetTableName.getDb(), table, createTableStmt, null,
                null, false, false, stmt instanceof CreateTemporaryTableLikeStmt);
        if (createTableStmt.isEmpty()) {
            ErrorReport.reportSemanticException(ErrorCode.ERROR_CREATE_TABLE_LIKE_EMPTY, "CREATE");
        }

        StatementBase statementBase =
                SqlParser.parseOneWithStarRocksDialect(createTableStmt.get(0), context.getSessionVariable());
        if (statementBase instanceof CreateTableStmt) {
            CreateTableStmt parsedCreateTableStmt = (CreateTableStmt) statementBase;
            NodePosition tablePos = parsedCreateTableStmt.getTableRef() != null
                    ? parsedCreateTableStmt.getTableRef().getPos()
                    : NodePosition.ZERO;
            QualifiedName normalizedName = QualifiedName.of(Lists.newArrayList(
                    targetTableName.getCatalog(), targetTableName.getDb(), targetTableName.getTbl()));
            parsedCreateTableStmt.setTableRef(new TableRef(normalizedName, null, tablePos));
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
