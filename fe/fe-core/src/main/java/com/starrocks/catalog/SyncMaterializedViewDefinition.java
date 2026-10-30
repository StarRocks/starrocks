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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/Column.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.sql.parser.SqlParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** The executable, per-row definition of a synchronous materialized index. */
public final class SyncMaterializedViewDefinition {
    @SerializedName("columnExprSql")
    private final Map<String, String> columnExprSql;
    @SerializedName("sourceColumnNames")
    private final List<String> sourceColumnNames;
    @SerializedName("whereClauseSql")
    private final String whereClauseSql;

    private SyncMaterializedViewDefinition(Map<String, String> columnExprSql,
                                          List<String> sourceColumnNames, String whereClauseSql) {
        this.columnExprSql = Collections.unmodifiableMap(new LinkedHashMap<>(columnExprSql));
        this.sourceColumnNames = Collections.unmodifiableList(new ArrayList<>(sourceColumnNames));
        this.whereClauseSql = whereClauseSql;
    }

    public static SyncMaterializedViewDefinition capture(List<Column> schema, Expr whereClause) {
        Map<String, String> expressions = new LinkedHashMap<>();
        List<String> sources = new ArrayList<>();
        schemaNames(schema);
        for (Column column : schema) {
            captureColumn(column, expressions, sources);
        }
        return new SyncMaterializedViewDefinition(expressions, sources,
                whereClause == null ? null : render(whereClause));
    }

    /** Restores syntax only; the owning table must analyze the definition before executing it. */
    public Expr restore(List<Column> schema) {
        Map<String, String> expressions = expressionInventory();
        Set<String> sources = sourceInventory();
        Set<String> inventory = new LinkedHashSet<>(sources);
        for (String name : expressions.keySet()) {
            if (!inventory.add(name)) {
                throw new SemanticException("Overlapping synchronous MV definition column: %s", name);
            }
        }
        if (!inventory.equals(schemaNames(schema))) {
            throw new SemanticException("Synchronous MV definition inventory does not match its schema");
        }
        // Parse everything before mutating the caller's schema.
        Map<String, Expr> parsed = new LinkedHashMap<>();
        expressions.forEach((name, sql) -> parsed.put(name, parse(sql)));
        Expr where = whereClauseSql == null ? null : parse(whereClauseSql);
        for (Column column : schema) {
            column.setDefineExpr(parsed.get(logicalName(column.getName())));
        }
        return where;
    }

    /** Return the exact stored per-row projection, preserving casts and literal widths for sampling. */
    public String projectionSql(Column column) {
        String name = logicalName(column.getName());
        String sql = expressionInventory().get(name);
        if (sql != null) {
            return sql;
        }
        if (!sourceInventory().contains(name)) {
            throw new SemanticException("Missing synchronous MV projection: %s", name);
        }
        return ParseUtil.backquote(Column.removeNamePrefix(column.getName()));
    }

    /** Reconcile only on an authorized catalog schema update, never during Gson restoration. */
    public SyncMaterializedViewDefinition withSchema(List<Column> schema) {
        Map<String, String> oldExpressions = expressionInventory();
        Set<String> oldSources = sourceInventory();
        for (String name : oldExpressions.keySet()) {
            if (oldSources.contains(name)) {
                throw new SemanticException("Overlapping synchronous MV definition column: %s", name);
            }
        }
        schemaNames(schema);
        Map<String, String> expressions = new LinkedHashMap<>();
        List<String> sources = new ArrayList<>();
        for (Column column : schema) {
            String name = logicalName(column.getName());
            if (oldExpressions.containsKey(name)) {
                expressions.put(name, oldExpressions.get(name));
            } else if (oldSources.contains(name)) {
                sources.add(name);
            } else {
                captureColumn(column, expressions, sources);
            }
        }
        return new SyncMaterializedViewDefinition(expressions, sources, whereClauseSql);
    }

    private static void captureColumn(Column column, Map<String, String> expressions, List<String> sources) {
        String name = logicalName(column.getName());
        Expr expr = column.getDefineExpr();
        if (expr == null || (expr instanceof SlotRef &&
                logicalName(((SlotRef) expr).getColumnName()).equals(name))) {
            sources.add(name);
        } else {
            expressions.put(name, render(expr));
        }
    }

    private Map<String, String> expressionInventory() {
        if (columnExprSql == null) {
            throw new SemanticException("Missing synchronous MV expression inventory");
        }
        Map<String, String> expressions = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : columnExprSql.entrySet()) {
            String name = logicalName(entry.getKey());
            if (entry.getValue() == null || entry.getValue().isBlank() || expressions.containsKey(name)) {
                throw new SemanticException("Invalid or ambiguous synchronous MV expression: %s", name);
            }
            expressions.put(name, entry.getValue());
        }
        return expressions;
    }

    private Set<String> sourceInventory() {
        if (sourceColumnNames == null) {
            throw new SemanticException("Missing synchronous MV source inventory");
        }
        Set<String> names = new LinkedHashSet<>();
        for (String name : sourceColumnNames) {
            if (!names.add(logicalName(name))) {
                throw new SemanticException("Ambiguous synchronous MV source column: %s", name);
            }
        }
        return names;
    }

    private static Set<String> schemaNames(List<Column> schema) {
        if (schema == null || schema.isEmpty()) {
            throw new SemanticException("Missing synchronous MV schema");
        }
        Set<String> names = new LinkedHashSet<>();
        for (Column column : schema) {
            if (!names.add(logicalName(column.getName()))) {
                throw new SemanticException("Ambiguous synchronous MV schema column: %s", column.getName());
            }
        }
        return names;
    }

    private static String logicalName(String name) {
        if (name == null || name.isBlank()) {
            throw new SemanticException("Missing synchronous MV column name");
        }
        return Column.removeNamePrefix(name).toLowerCase(Locale.ROOT);
    }

    private static Expr parse(String sql) {
        if (sql.isBlank()) {
            throw new SemanticException("Empty synchronous MV expression SQL");
        }
        try {
            return SqlParser.parseSqlToExpr(sql, SqlModeHelper.MODE_DEFAULT);
        } catch (Exception e) {
            throw new SemanticException("Invalid synchronous MV expression SQL: " + sql, e);
        }
    }

    private static String render(Expr expr) {
        return AstToSQLBuilder.toSQL(preserveTypes(expr.clone()));
    }

    private static Expr preserveTypes(Expr expr) {
        for (int i = 0; i < expr.getChildren().size(); i++) {
            expr.setChild(i, preserveTypes(expr.getChild(i)));
        }
        if (expr instanceof SlotRef) {
            ((SlotRef) expr).setTblName(null);
        } else if (expr instanceof CastExpr && ((CastExpr) expr).isImplicit()) {
            return new CastExpr(new TypeDef(expr.getType()), expr.getChild(0));
        } else if (expr instanceof LiteralExpr && expr.getType().isValid() &&
                expr.getType().isScalarType() && !expr.getType().isNull() && expr.getType().getAggStateDesc() == null) {
            // Bare literals lose analyzed width/precision, including COUNT's BIGINT 0/1 arms.
            return new CastExpr(new TypeDef(expr.getType()), expr);
        }
        return expr;
    }
}
