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

import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.sql.ast.ShowAlterStmt.AlterType;
import com.starrocks.sql.ast.TableRef;

import static com.starrocks.sql.analyzer.AnalyzerUtils.normalizedTableRef;

public class CancelAlterTableStatementAnalyzer {

    public static void analyze(CancelAlterTableStmt statement, ConnectContext context) {
        TableRef tableRef = normalizedTableRef(statement.getTableRef(), context);
        statement.setTableRef(tableRef);
        // FORCE (the publish-stuck escape hatch) is only implemented for COLUMN
        // alter jobs on shared-data (lake) tables — i.e. LakeTableSchemaChangeJob
        // (heavy schema change) and LakeTableAlterMetaJob (metadata alter), the
        // only jobs that override cancelImpl(force) to do the no-op publish +
        // version bump. Other alter types are NOT supported:
        //   - OPTIMIZE -> OptimizeJobV2 / OnlineOptimizeJobV2 (no force override);
        //   - ROLLUP -> MaterializedViewHandler.cancel (ignores isForce());
        //   - MATERIALIZED_VIEW -> cancelMV (ignores isForce()).
        // The grammar would otherwise accept `... FORCE` for these and either
        // silently no-op or hit a misleading handler error, so reject up front.
        if (statement.isForce() && statement.getAlterType() != AlterType.COLUMN) {
            throw new SemanticException("CANCEL ALTER TABLE ... FORCE is only supported for COLUMN alter jobs "
                    + "(schema change / metadata alter) on shared-data (lake) tables; it is not supported for "
                    + statement.getAlterType() + ".");
        }
        // Check db.
        if (context.getGlobalStateMgr().getLocalMetastore().getDb(statement.getDbName()) == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, statement.getDbName());
        }
    }
}
