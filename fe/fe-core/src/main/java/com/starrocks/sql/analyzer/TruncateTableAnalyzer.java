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
import com.starrocks.qe.ConnectContext;
<<<<<<< HEAD
import com.starrocks.sql.ast.PartitionNames;
=======
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.TableRef;
>>>>>>> f658882e9e ([Enhancement] Support truncate iceberg table (#64768))
import com.starrocks.sql.ast.TruncateTableStmt;

import java.util.List;

public class TruncateTableAnalyzer {

    public static void analyze(TruncateTableStmt statement, ConnectContext context) {
<<<<<<< HEAD
        statement.getTblRef().getName().normalization(context);
        if (statement.getTblRef().hasExplicitAlias()) {
            throw new SemanticException("Not support truncate table with alias");
=======
        TableRef tableRef = statement.getTblRef();
        
        // Validate catalog
        String catalog = tableRef.getCatalogName();
        if (Strings.isNullOrEmpty(catalog)) {
            catalog = context.getCurrentCatalog();
            if (Strings.isNullOrEmpty(catalog)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_CATALOG_ERROR, catalog);
            }
>>>>>>> f658882e9e ([Enhancement] Support truncate iceberg table (#64768))
        }

        PartitionNames partitionNames = statement.getTblRef().getPartitionNames();
        if (partitionNames != null) {
            if (partitionNames.isTemp()) {
                throw new SemanticException("Not support truncate temp partitions");
            }
            // check if partition name is not empty string
            if (partitionNames.getPartitionNames().stream().anyMatch(entity -> Strings.isNullOrEmpty(entity))) {
                throw new SemanticException("there are empty partition name");
            }
        }

        TableRef nomalizedTableRef = new TableRef(QualifiedName.of(List.of(catalog, db, tbl)), partitionRef,
                tableRef.getPos());
        statement.setTblRef(nomalizedTableRef);
    }
}
