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

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.TextAnalyzer;
import com.starrocks.catalog.TextAnalyzerMgr;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateTextAnalyzerStmt;
import com.starrocks.sql.ast.DescTextAnalyzerStmt;
import com.starrocks.sql.ast.DropTextAnalyzerStmt;
import com.starrocks.sql.ast.ShowCreateTextAnalyzerStmt;
import com.starrocks.sql.ast.ShowTextAnalyzersStmt;
import com.starrocks.sql.ast.StatementBase;

import java.util.Map;

public final class TextAnalyzerAnalyzer {
    private TextAnalyzerAnalyzer() {
    }

    public static void analyze(StatementBase statement, ConnectContext context) {
        if (statement instanceof CreateTextAnalyzerStmt) {
            CreateTextAnalyzerStmt create = (CreateTextAnalyzerStmt) statement;
            TextAnalyzerMgr.ResolvedName name =
                    TextAnalyzerMgr.resolveName(create.getAnalyzerName(), context, false);
            if (name.getDb().getTextAnalyzer(name.getObjectName()) != null) {
                throw new SemanticException("TEXT ANALYZER " + create.getAnalyzerName() + " already exists");
            }
            Map<String, String> properties = create.getProperties();
            if (properties != null && properties.get("definition") != null) {
                TextAnalyzer.canonicalize(properties.get("definition"));
            }
        } else if (statement instanceof DropTextAnalyzerStmt) {
            TextAnalyzerMgr.resolveName(((DropTextAnalyzerStmt) statement).getAnalyzerName(), context, false);
        } else if (statement instanceof DescTextAnalyzerStmt) {
            DescTextAnalyzerStmt desc = (DescTextAnalyzerStmt) statement;
            TextAnalyzerMgr.require(desc.getAnalyzerName(), context);
        } else if (statement instanceof ShowCreateTextAnalyzerStmt) {
            ShowCreateTextAnalyzerStmt showCreate = (ShowCreateTextAnalyzerStmt) statement;
            TextAnalyzerMgr.require(showCreate.getAnalyzerName(), context);
        } else if (statement instanceof ShowTextAnalyzersStmt) {
            String dbName = ((ShowTextAnalyzersStmt) statement).getDbName();
            TextAnalyzerMgr.resolveName(dbName == null ? context.getDatabase() : dbName, context, true);
        }
    }

    public static String resolveAnalyzerArgument(String functionName, String value, ConnectContext context) {
        boolean byDefinition = "tokenize_detail_by_definition".equalsIgnoreCase(functionName);
        boolean analyzerFunction = "tokenize".equalsIgnoreCase(functionName)
                || "tokenize_detail".equalsIgnoreCase(functionName) || byDefinition;
        if (!analyzerFunction) {
            return value;
        }

        TextAnalyzer analyzer = null;
        Database analyzerDb = null;
        if (!byDefinition && !value.trim().startsWith("{")) {
            String dbName = context.getDatabase();
            String analyzerName = value;
            int separator = value.lastIndexOf('.');
            if (separator > 0 && separator + 1 < value.length()) {
                dbName = value.substring(0, separator);
                analyzerName = value.substring(separator + 1);
            }
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
            if (db != null) {
                analyzer = db.getTextAnalyzer(analyzerName);
                analyzerDb = db;
            }
            if (separator > 0 && analyzer == null) {
                throw new SemanticException("Unknown TEXT ANALYZER " + value);
            }
        }
        if (analyzer != null) {
            try {
                Authorizer.checkAnyActionOnOrInDb(
                        context, context.getCurrentCatalog(), analyzerDb.getOriginName());
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(context.getCurrentCatalog(), context.getCurrentUserIdentity(),
                        context.getCurrentRoleIds(), PrivilegeType.ANY.name(), ObjectType.DATABASE.name(),
                        analyzerDb.getOriginName());
            }
            return analyzer.getCanonicalDefinition();
        }
        if (byDefinition || value.trim().startsWith("{")) {
            return TextAnalyzer.canonicalize(value).getCanonicalJson();
        }
        return value;
    }
}
