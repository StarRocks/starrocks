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
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.connector.ConnectorType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.SetCatalogStmt;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UseCatalogStmt;

import java.util.Map;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;
import static com.starrocks.sql.ast.CreateCatalogStmt.TYPE;

public class CatalogAnalyzer {
    private static final String CATALOG = "CATALOG";

    private static final String WHITESPACE = "\\s+";

    public static void analyze(StatementBase stmt, ConnectContext session) {
        new CatalogAnalyzerVisitor().visit(stmt, session);
    }

    static class CatalogAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(ShowStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCreateCatalogStatement(CreateCatalogStmt statement, ConnectContext context) {
            String catalogName = statement.getCatalogName();
            if (Strings.isNullOrEmpty(catalogName)) {
                throw new SemanticException("'catalog name' can not be null or empty");
            }

            FeNameFormat.checkCatalogName(catalogName);

            if (catalogName.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
                throw new SemanticException(
                        String.format("External catalog name can't be the same as internal catalog name '%s'",
                                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME));
            }
            Map<String, String> properties = statement.getProperties();
            String catalogType = properties.get(TYPE);
            if (Strings.isNullOrEmpty(catalogType)) {
                throw new SemanticException("'type' can not be null or empty");
            }
            statement.setCatalogType(catalogType);
            if (!ConnectorType.isSupport(catalogType)) {
                throw new SemanticException("[type : %s] is not supported", catalogType);
            }
            return null;
        }

        @Override
        public Void visitDropCatalogStatement(DropCatalogStmt statement, ConnectContext context) {
            String name = statement.getName();
            if (Strings.isNullOrEmpty(name)) {
                throw new SemanticException("'catalog name' can not be null or empty");
            }

            if (name.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
                throw new SemanticException("Can't drop the default internal catalog");
            }

            if (isResourceMappingCatalog(name)) {
                throw new SemanticException("Can't drop the resource mapping catalog");
            }

            return null;
        }

        @Override
        public Void visitUseCatalogStatement(UseCatalogStmt statement, ConnectContext context) {
            if (Strings.isNullOrEmpty(statement.getCatalogParts())) {
                throw new SemanticException("You have an error in your SQL. The correct syntax is: USE 'CATALOG catalog_name'.");
            }

            String[] splitParts = statement.getCatalogParts().split(WHITESPACE);
            if (!splitParts[0].equalsIgnoreCase(CATALOG) || splitParts.length != 2) {
                throw new SemanticException("You have an error in your SQL. The correct syntax is: USE 'CATALOG catalog_name'.");
            }

            FeNameFormat.checkCatalogName(splitParts[1]);
            statement.setCatalogName(splitParts[1]);

            return null;
        }

        @Override
        public Void visitSetCatalogStatement(SetCatalogStmt statement, ConnectContext context) {
            if (Strings.isNullOrEmpty(statement.getCatalogName())) {
                throw new SemanticException("You have an error in your SQL. The correct syntax is: USE 'CATALOG catalog_name'.");
            }

            FeNameFormat.checkCatalogName(statement.getCatalogName());
            return null;
        }
    }
}
