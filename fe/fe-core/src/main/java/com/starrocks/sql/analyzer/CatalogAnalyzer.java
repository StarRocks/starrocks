// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeNameFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.StatementBase;

import java.util.Map;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;
import static com.starrocks.sql.ast.CreateCatalogStmt.TYPE;

public class CatalogAnalyzer {
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
            try {
                FeNameFormat.checkCatalogName(catalogName);
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }

            if (catalogName.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
                throw new SemanticException("External catalog name can't be the same as internal catalog name 'default'");
            }
            Map<String, String> properties = statement.getProperties();
            String catalogType = properties.get(TYPE);
            if (Strings.isNullOrEmpty(catalogType)) {
                throw new SemanticException("'type' can not be null or empty");
            }
            statement.setCatalogType(catalogType);
            if (!CreateCatalogStmt.SUPPORTED_CATALOG.contains(catalogType)) {
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
    }
}
