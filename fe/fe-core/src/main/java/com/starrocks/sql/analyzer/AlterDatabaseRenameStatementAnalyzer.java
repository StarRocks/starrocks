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
import com.starrocks.sql.ast.AlterDatabaseRenameStatement;

import static com.starrocks.catalog.InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;

public class AlterDatabaseRenameStatementAnalyzer {
    public static void analyze(AlterDatabaseRenameStatement statement, ConnectContext context) {
        if (Strings.isNullOrEmpty(statement.getCatalogName())) {
            if (Strings.isNullOrEmpty(context.getCurrentCatalog())) {
                throw new SemanticException(PARSER_ERROR_MSG.noCatalogSelected());
            }
            statement.setCatalogName(context.getCurrentCatalog());
        }

        if (!DEFAULT_INTERNAL_CATALOG_NAME.equalsIgnoreCase(statement.getCatalogName())) {
            throw new SemanticException(PARSER_ERROR_MSG.unsupportedOp("rename db under external catalog"));
        }

        String newName = statement.getNewDbName();
        FeNameFormat.checkDbName(newName);
    }
}
