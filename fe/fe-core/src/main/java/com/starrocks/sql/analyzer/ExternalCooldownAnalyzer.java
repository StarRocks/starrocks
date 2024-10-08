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

import com.google.common.base.Strings;
import com.starrocks.analysis.TableName;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CancelExternalCooldownStmt;
import com.starrocks.sql.ast.CreateExternalCooldownStmt;
import com.starrocks.sql.ast.StatementBase;
import org.apache.commons.collections.MapUtils;

import java.util.Map;

public class ExternalCooldownAnalyzer {
    public static void analyze(StatementBase stmt, ConnectContext session) {
        new ExternalCooldownAnalyzer.ExternalCooldownAnalyzerVisitor().visit(stmt, session);
    }

    static class ExternalCooldownAnalyzerVisitor implements AstVisitor<Void, ConnectContext> {
        @Override
        public Void visitCreateExternalCooldownStatement(CreateExternalCooldownStmt createExternalCooldownStmt,
                    ConnectContext context) {
            TableName tableName = createExternalCooldownStmt.getTableName();
            if (tableName.getDb() == null) {
                String dbName = context.getDatabase();
                if (Strings.isNullOrEmpty(dbName)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
                }
                tableName.setDb(dbName);
            }
            createExternalCooldownStmt.setTableName(tableName);
            createExternalCooldownStmt.setPartitionRangeDesc(createExternalCooldownStmt.getPartitionRangeDesc());
            analyzeExternalCooldownProperties(createExternalCooldownStmt.getProperties());
            return null;
        }

        public static void analyzeExternalCooldownProperties(Map<String, String> properties) {
            if (MapUtils.isEmpty(properties)) {
                return;
            }
        }

        @Override
        public Void visitCancelExternalCooldownStatement(
                CancelExternalCooldownStmt cancelExternalCooldownStmt, ConnectContext context) {
            TableName tableName = cancelExternalCooldownStmt.getTableName();
            if (tableName.getDb() == null) {
                String dbName = context.getDatabase();
                if (Strings.isNullOrEmpty(dbName)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
                }
                tableName.setDb(dbName);
            }
            cancelExternalCooldownStmt.setTableName(tableName);
            return null;
        }
    }
}
