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

package com.starrocks.sql.ast.context;

import com.starrocks.sql.ast.expression.Expr;

import java.util.List;
import java.util.Map;

/**
 * Minimal SQL serializer for the {@code context}-family statements. Each new {@code *Stmt}
 * delegates its {@code toSql()} here so dump / audit / replay paths (which all call
 * {@code StatementBase.toSql()} as a fallback) get a stable canonical form instead of the
 * {@code "New AST not implement toSql function"} runtime exception the base class throws.
 *
 * <p>The output is intentionally simple and not guaranteed to round-trip through the parser
 * — the goal is auditability, not bidirectional rewriting.
 */
final class ContextStmtFormatter {

    private ContextStmtFormatter() {}

    static String createContextBase(boolean ifNotExists, ContextBaseName name, Map<String, String> properties) {
        StringBuilder sb = new StringBuilder("CREATE CONTEXTBASE ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(escIdent(name == null ? null : name.getName()));
        appendStringProperties(sb, properties);
        return sb.toString();
    }

    static String alterContextBase(ContextBaseName name, Map<String, String> properties) {
        StringBuilder sb = new StringBuilder("ALTER CONTEXTBASE ");
        sb.append(escIdent(name == null ? null : name.getName()));
        appendStringProperties(sb, properties);
        return sb.toString();
    }

    static String alterContextBaseRename(ContextBaseName name, String newName) {
        StringBuilder sb = new StringBuilder("ALTER CONTEXTBASE ");
        sb.append(escIdent(name == null ? null : name.getName()));
        sb.append(" RENAME TO ").append(escIdent(newName));
        return sb.toString();
    }

    static String dropContextBase(boolean ifExists, ContextBaseName name) {
        StringBuilder sb = new StringBuilder("DROP CONTEXTBASE ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        sb.append(escIdent(name == null ? null : name.getName()));
        return sb.toString();
    }

    static String createContextCollection(boolean ifNotExists, ContextCollectionName name,
                                          String collectionType, Map<String, String> properties) {
        StringBuilder sb = new StringBuilder("CREATE CONTEXT COLLECTION ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        appendQualifiedCollection(sb, name);
        if (collectionType != null) {
            sb.append(" TYPE ").append(escIdent(collectionType));
        }
        appendStringProperties(sb, properties);
        return sb.toString();
    }

    static String dropContextCollection(boolean ifExists, ContextCollectionName name) {
        StringBuilder sb = new StringBuilder("DROP CONTEXT COLLECTION ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        appendQualifiedCollection(sb, name);
        return sb.toString();
    }

    static String createWorkspace(boolean ifNotExists, WorkspaceName name, Map<String, String> properties) {
        StringBuilder sb = new StringBuilder("CREATE CONTEXT WORKSPACE ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        appendQualifiedWorkspace(sb, name);
        appendStringProperties(sb, properties);
        return sb.toString();
    }

    static String dropWorkspace(boolean ifExists, WorkspaceName name) {
        StringBuilder sb = new StringBuilder("DROP CONTEXT WORKSPACE ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        appendQualifiedWorkspace(sb, name);
        return sb.toString();
    }

    static String workspaceUpsert(WorkspaceName name, Map<String, Expr> objectArgs, Map<String, Expr> options) {
        StringBuilder sb = new StringBuilder("WORKSPACE UPSERT INTO ");
        appendQualifiedWorkspace(sb, name);
        appendExprOptions(sb, "OBJECT", objectArgs);
        appendExprOptions(sb, "OPTIONS", options);
        return sb.toString();
    }

    static String createRetrievalProfile(boolean ifNotExists, String name, Map<String, String> properties) {
        StringBuilder sb = new StringBuilder("CREATE RETRIEVAL PROFILE ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(escIdent(name));
        appendStringProperties(sb, properties);
        return sb.toString();
    }

    static String dropRetrievalProfile(boolean ifExists, String name) {
        StringBuilder sb = new StringBuilder("DROP RETRIEVAL PROFILE ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        sb.append(escIdent(name));
        return sb.toString();
    }

    static String contextUpsert(ContextCollectionName collection, Map<String, Expr> entityArgs,
                                List<Expr> edges, Map<String, Expr> options) {
        StringBuilder sb = new StringBuilder("CONTEXT UPSERT INTO ");
        appendQualifiedCollection(sb, collection);
        appendExprOptions(sb, "ENTITY", entityArgs);
        if (edges != null && !edges.isEmpty()) {
            sb.append(" EDGES (");
            boolean first = true;
            for (Expr e : edges) {
                if (!first) {
                    sb.append(", ");
                }
                sb.append(exprToSqlSafe(e));
                first = false;
            }
            sb.append(")");
        }
        appendExprOptions(sb, "OPTIONS", options);
        return sb.toString();
    }

    static String contextDelete(ContextCollectionName collection, Expr predicate,
                                Map<String, Expr> options) {
        StringBuilder sb = new StringBuilder("CONTEXT DELETE FROM ");
        appendQualifiedCollection(sb, collection);
        if (predicate != null) {
            sb.append(" WHERE ").append(exprToSqlSafe(predicate));
        }
        appendExprOptions(sb, "OPTIONS", options);
        return sb.toString();
    }

    static String showContext(String form) {
        return "SHOW CONTEXT " + form;
    }

    static String showContextScoped(String form, String contextBase) {
        if (contextBase == null) {
            return "SHOW CONTEXT " + form;
        }
        return "SHOW CONTEXT " + form + " IN " + escIdent(contextBase);
    }

    private static void appendQualifiedCollection(StringBuilder sb, ContextCollectionName name) {
        if (name == null) {
            sb.append("?");
            return;
        }
        if (name.getContextBase() != null) {
            sb.append(escIdent(name.getContextBase())).append('.');
        }
        sb.append(escIdent(name.getCollection()));
    }

    private static void appendQualifiedWorkspace(StringBuilder sb, WorkspaceName name) {
        if (name == null) {
            sb.append("?");
            return;
        }
        sb.append(escIdent(name.toString()));
    }

    private static void appendStringProperties(StringBuilder sb, Map<String, String> properties) {
        if (properties == null || properties.isEmpty()) {
            return;
        }
        sb.append(" PROPERTIES (");
        boolean first = true;
        for (Map.Entry<String, String> e : properties.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            sb.append('"').append(escString(e.getKey())).append('"')
                    .append(" = ")
                    .append('"').append(escString(e.getValue())).append('"');
            first = false;
        }
        sb.append(")");
    }

    private static void appendExprOptions(StringBuilder sb, String label, Map<String, Expr> options) {
        if (options == null || options.isEmpty()) {
            return;
        }
        sb.append(" ").append(label).append(" (");
        boolean first = true;
        for (Map.Entry<String, Expr> e : options.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(escIdent(e.getKey())).append(" = ").append(exprToSqlSafe(e.getValue()));
            first = false;
        }
        sb.append(")");
    }

    private static String exprToSqlSafe(Expr expr) {
        if (expr == null) {
            return "NULL";
        }
        try {
            // Expr exposes debugString() rather than toSql(); good enough for audit/log output.
            return expr.debugString();
        } catch (Exception e) {
            return "?";
        }
    }

    private static String escIdent(String s) {
        if (s == null || s.isEmpty()) {
            return "?";
        }
        return s;
    }

    private static String escString(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
