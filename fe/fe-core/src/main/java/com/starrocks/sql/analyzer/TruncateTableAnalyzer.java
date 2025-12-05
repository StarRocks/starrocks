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
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.KeyPartitionRef;
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.TruncateTablePartitionStmt;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.ast.expression.Expr;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TruncateTableAnalyzer {

    public static void analyze(TruncateTableStmt statement, ConnectContext context) {
        TableRef tableRef = normalizedTableRef(statement.getTblRef(), context);

        // Validate partition reference if present
        PartitionRef partitionRef = tableRef.getPartitionDef();
        if (partitionRef != null) {
            if (partitionRef.isTemp()) {
                throw new SemanticException("Not support truncate temp partitions");
            }
            // Check if partition name is not empty string
            if (partitionRef.getPartitionNames().stream().anyMatch(Strings::isNullOrEmpty)) {
                throw new SemanticException("there are empty partition name");
            }
        }
        statement.setTblRef(tableRef);

        if (statement instanceof TruncateTablePartitionStmt) {
            analyzeKeyPartitions((TruncateTablePartitionStmt) statement);
        }
    }

    public static void analyzeKeyPartitions(TruncateTablePartitionStmt statement) {
        KeyPartitionRef partitionRef = statement.getKeyPartitionRef();
        if (partitionRef == null) {
            throw new SemanticException("KeyPartitionRef in TruncateTablePartitionStmt is null");
        }

        List<String> partitionColNames = partitionRef.getPartitionColNames();
        List<Expr> partitionColValues = partitionRef.getPartitionColValues();
        if (partitionColNames.size() != partitionColValues.size()) {
            throw new SemanticException("The size of partition columns and values do not match");
        }

        // Check for duplicate partition column names
        Set<String> uniqueColNames = new HashSet<>(partitionColNames);
        if (uniqueColNames.size() != partitionColNames.size()) {
            throw new SemanticException("Duplicate partition column names are not allowed");
        }
    }

    private static TableRef normalizedTableRef(TableRef tableRef, ConnectContext context) {
        // Validate catalog
        String catalog = tableRef.getCatalogName();
        if (Strings.isNullOrEmpty(catalog)) {
            catalog = context.getCurrentCatalog();
            if (Strings.isNullOrEmpty(catalog)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_CATALOG_ERROR, catalog);
            }
        }

        // Validate database
        String db = tableRef.getDbName();
        if (Strings.isNullOrEmpty(db)) {
            db = context.getDatabase();
            if (Strings.isNullOrEmpty(db)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }

        // Validate table name
        String tbl = tableRef.getTableName();
        if (Strings.isNullOrEmpty(tbl)) {
            throw new SemanticException("Table name is null");
        }

        return new TableRef(QualifiedName.of(List.of(catalog, db, tbl)), tableRef.getPartitionDef(), tableRef.getPos());
    }
}
