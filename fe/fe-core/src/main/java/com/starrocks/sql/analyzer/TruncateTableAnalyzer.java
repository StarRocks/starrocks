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
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.TruncateTableStmt;

import java.util.List;

public class TruncateTableAnalyzer {

    public static void analyze(TruncateTableStmt statement, ConnectContext context) {
        TableRef tableRef = statement.getTblRef();
        
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

        TableRef nomalizedTableRef = new TableRef(QualifiedName.of(List.of(catalog, db, tbl)), partitionRef,
                tableRef.getPos());
        statement.setTblRef(nomalizedTableRef);
    }
}
