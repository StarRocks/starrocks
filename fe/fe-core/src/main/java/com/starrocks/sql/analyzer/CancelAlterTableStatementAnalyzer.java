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

import com.starrocks.common.error.ErrorCode;
import com.starrocks.common.error.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.sql.common.MetaUtils;

public class CancelAlterTableStatementAnalyzer {

    public static void analyze(CancelAlterTableStmt statement, ConnectContext context) {
        MetaUtils.normalizationTableName(context, statement.getDbTableName());
        // Check db.
        if (context.getGlobalStateMgr().getDb(statement.getDbName()) == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, statement.getDbName());
        }
    }
}
