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

import com.starrocks.catalog.Table;
import com.starrocks.common.util.SqlCredentialRedactor;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.formatter.AST2SQLVisitor;
import com.starrocks.sql.formatter.FormatOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Optional;

/**
 * AstToSQLBuilder inherits AstToStringBuilder and rewrites some special AST logic to
 * ensure that the generated SQL must be a legal SQL,
 * which can be used in some scenarios that require serialization and deserialization.
 * Such as string serialization of views
 */
public class AstToSQLBuilder {
    private static final Logger LOG = LogManager.getLogger(AstToSQLBuilder.class);

    public static String buildSimple(StatementBase statement) {
        Map<TableName, Table> tables = AnalyzerUtils.collectAllTableAndViewWithAlias(statement);
        boolean sameCatalogDb = tables.keySet().stream().map(TableName::getCatalogAndDb).distinct().count() == 1;
        return AST2SQLVisitor.withOptions(
                        FormatOptions.allEnable().setColumnSimplifyTableName(sameCatalogDb).setEnableDigest(false))
                .visit(statement);
    }

    public static String toSQL(ParseNode statement) {
        return AST2SQLVisitor.withOptions(
                FormatOptions.allEnable().setColumnSimplifyTableName(false).setEnableDigest(false)).visit(statement);
    }

    // for executable SQL with credential, such as pipe insert sql
    public static String toSQLWithCredential(ParseNode statement) {
        return AST2SQLVisitor.withOptions(FormatOptions.allEnable()
                        .setColumnSimplifyTableName(false)
                        .setHideCredential(false)
                        .setEnableDigest(false))
                .visit(statement);
    }

    // return sql from ast or default sql if builder throws exception.
    // for example, `select from files` needs file schema to generate sql from ast.
    // If BE is down, the schema will be null, and an exception will be thrown when writing audit log.
    public static String toSQLOrDefault(ParseNode statement, String defaultSql) {
        try {
            return toSQL(statement);
        } catch (Exception e) {
            LOG.info("Ast to sql failed.", e);
            return SqlCredentialRedactor.redact(defaultSql);
        }
    }

    public static Optional<String> toSQL(StatementBase statement, FormatOptions options) {
        try {
            return Optional.of(AST2SQLVisitor.withOptions(options).visit(statement));
        } catch (Exception e) {
            LOG.info("Ast to sql failed.", e);
            return Optional.empty();
        }
    }

    public static String toDigest(StatementBase statement) {
        return AST2SQLVisitor.withOptions(FormatOptions.allEnable()
                        .setColumnSimplifyTableName(false)
                        .setColumnWithTableName(false)
                        .setEnableNewLine(false)
                        .setEnableMassiveExpr(false)
                        .setPrintActualSelectItem(false)
                        .setPrintLevelCompound(false))
                .visit(statement);
    }
}
