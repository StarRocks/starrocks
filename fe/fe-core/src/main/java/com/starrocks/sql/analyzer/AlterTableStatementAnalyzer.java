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

import com.google.common.collect.Sets;
import com.starrocks.alter.AlterOpType;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateIndexClause;
import com.starrocks.sql.ast.DropIndexClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.common.MetaUtils;

import java.util.List;
import java.util.Set;

import static com.starrocks.common.util.PropertyAnalyzer.PROPERTIES_BF_COLUMNS;

public class AlterTableStatementAnalyzer {
    public static void analyze(AlterTableStmt statement, ConnectContext context) {
        TableName tbl = statement.getTbl();
        tbl.normalization(context);
        MetaUtils.checkNotSupportCatalog(tbl.getCatalog(), "ALTER");

        List<AlterClause> alterClauseList = statement.getAlterClauseList();
        if (alterClauseList == null || alterClauseList.isEmpty()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_NO_ALTER_OPERATION);
        }

        checkAlterOpConflict(alterClauseList);

        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(context, tbl.getCatalog(), tbl.getDb());
        if (db == null) {
            throw new SemanticException("Database %s is not found", tbl.getCatalogAndDb());
        }

        if (alterClauseList.stream().map(AlterClause::getOpType).anyMatch(AlterOpType::needCheckCapacity)) {
            try {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().checkClusterCapacity();
                GlobalStateMgr.getCurrentState().getLocalMetastore().checkDataSizeQuota(db);
                GlobalStateMgr.getCurrentState().getLocalMetastore().checkReplicaQuota(db);
            } catch (StarRocksException e) {
                throw new SemanticException(e.getMessage());
            }
        }

        Table table = MetaUtils.getSessionAwareTable(context, null, tbl);
        if (table.isTemporaryTable()) {
            throw new SemanticException("temporary table doesn't support alter table statement");
        }
        if (table instanceof MaterializedView) {
            for (AlterClause alterClause : alterClauseList) {
                if (!indexClause(alterClause)) {
                    String msg = String.format("The '%s' cannot be alter by 'ALTER TABLE', because it is a materialized view," +
                            "you can use 'ALTER MATERIALIZED VIEW' to alter it.", tbl.getTbl());
                    throw new SemanticException(msg, tbl.getPos());
                }
            }
        }
        AlterTableClauseAnalyzer alterTableClauseAnalyzerVisitor = new AlterTableClauseAnalyzer(table);
        for (AlterClause alterClause : alterClauseList) {
            alterTableClauseAnalyzerVisitor.analyze(context, alterClause);
        }
    }

    private static void checkAlterOpConflict(List<AlterClause> alterClauses) {
        Set<AlterOpType> checkedAlterOpTypes = Sets.newHashSet();
        for (AlterClause alterClause : alterClauses) {
            AlterOpType opType = alterClause.getOpType();
            for (AlterOpType currentOp : checkedAlterOpTypes) {
                if (!AlterOpType.COMPATIBITLITY_MATRIX[currentOp.ordinal()][opType.ordinal()]) {
                    throw new SemanticException("Alter operation " + opType + " conflicts with operation " + currentOp);
                }
            }

            checkedAlterOpTypes.add(opType);
        }
    }

    public static boolean indexClause(AlterClause alterClause) {
        if (alterClause instanceof CreateIndexClause || alterClause instanceof DropIndexClause) {
            return true;
        } else if (alterClause instanceof ModifyTablePropertiesClause
                && ((ModifyTablePropertiesClause) alterClause).getProperties().containsKey(PROPERTIES_BF_COLUMNS)) {
            return true;
        }
        return false;
    }
}
