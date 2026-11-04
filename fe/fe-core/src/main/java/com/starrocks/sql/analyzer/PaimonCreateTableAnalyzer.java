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

import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.Identifier;
import com.starrocks.sql.ast.KeysDesc;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.expression.NullLiteral;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

// SQL clause validation for Paimon CREATE TABLE. Native schema validation belongs to catalog execution.
final class PaimonCreateTableAnalyzer {
    private PaimonCreateTableAnalyzer() {
    }

    static void analyzeColumnDef(ColumnDef columnDef) {
        if (columnDef.isAutoIncrement()) {
            throw new SemanticException("Paimon does not support AUTO_INCREMENT columns: %s", columnDef.getName());
        }
        if (columnDef.isGeneratedColumn()) {
            throw new SemanticException("Paimon does not support generated columns: %s", columnDef.getName());
        }
        if (columnDef.getAggregateType() != null || columnDef.getAggStateDesc() != null) {
            throw new SemanticException("Paimon does not support aggregate column definitions: %s", columnDef.getName());
        }
        ColumnDef.DefaultValueDef defaultValue = columnDef.getDefaultValueDef();
        if (defaultValue != null && defaultValue.isSet && !(defaultValue.expr instanceof NullLiteral)) {
            throw new SemanticException("Paimon only supports DEFAULT NULL: %s", columnDef.getName());
        }
    }

    static void analyzeKeysDesc(CreateTableStmt stmt) {
        KeysDesc keysDesc = stmt.getKeysDesc();
        // Paimon primary keys need not be a prefix of the schema and do not imply REPLACE aggregation.
        if (keysDesc != null && keysDesc.getKeysType() != KeysType.PRIMARY_KEYS) {
            throw new SemanticException("Paimon only supports PRIMARY KEY, not %s", keysDesc.getKeysType());
        }
        String property = null;
        if (stmt.getProperties() != null) {
            for (Map.Entry<String, String> entry : stmt.getProperties().entrySet()) {
                if (entry.getKey().equalsIgnoreCase("primary-key")) {
                    property = entry.getValue();
                }
            }
        }
        List<String> sqlKeys = keysDesc == null ? List.of()
                : resolveColumns(stmt, keysDesc.getKeysColumnNames(), "primary key");
        List<String> propertyKeys = property == null ? List.of()
                : resolveColumns(stmt, Arrays.stream(property.split(",", -1)).map(String::trim).toList(), "primary key");
        if (keysDesc != null && property != null && !sqlKeys.equals(propertyKeys)) {
            throw new SemanticException("Paimon PRIMARY KEY and primary-key property must specify the same columns in order");
        }
        List<String> primaryKeys = keysDesc == null ? propertyKeys : sqlKeys;
        for (ColumnDef column : stmt.getColumnDefs()) {
            if (!primaryKeys.contains(column.getName())) {
                continue;
            }
            if (column.isAllowNull() && !column.isAllowNullImplicit()) {
                throw new SemanticException("Paimon primary key column[%s] cannot be nullable", column.getName());
            }
            ColumnDef.DefaultValueDef defaultValue = column.getDefaultValueDef();
            if (defaultValue != null && defaultValue.isSet) {
                throw new SemanticException("Invalid default value for '%s'", column.getName());
            }
        }
    }

    static void analyzePartitionDesc(CreateTableStmt stmt) {
        PartitionDesc partitionDesc = stmt.getPartitionDesc();
        if (partitionDesc == null) {
            return;
        }
        if (!(partitionDesc instanceof ListPartitionDesc)) {
            throw new SemanticException("Paimon only supports PARTITION BY (column, ...) without partition definitions");
        }
        ListPartitionDesc list = (ListPartitionDesc) partitionDesc;
        if (!list.getPartitionDescs().isEmpty() || CollectionUtils.isNotEmpty(list.getPartitionExprs())) {
            throw new SemanticException("Paimon only supports ordinary partition columns, " +
                    "not expressions or partition definitions");
        }
        List<String> names = list.getPartitionColNames();
        if (CollectionUtils.isNotEmpty(list.getMultiDescList())) {
            names = new ArrayList<>();
            for (ParseNode node : list.getMultiDescList()) {
                if (!(node instanceof Identifier)) {
                    throw new SemanticException("Paimon only supports ordinary partition columns, " +
                            "not partition expressions");
                }
                names.add(((Identifier) node).getValue());
            }
        }
        resolveColumns(stmt, names, "partition");
    }

    static void analyzeProperties(CreateTableStmt stmt) {
        if (stmt.getExtProperties() != null && !stmt.getExtProperties().isEmpty()) {
            throw new SemanticException("Paimon table options must be specified in PROPERTIES");
        }
    }

    static void analyzeSortKeys(CreateTableStmt stmt) {
        if (CollectionUtils.isNotEmpty(stmt.getOrderByElements())) {
            throw new SemanticException("Paimon tables do not support ORDER BY in CREATE TABLE");
        }
    }

    static void analyzeRollup(CreateTableStmt statement) {
        if (!statement.getRollupAlterClauseList().isEmpty()) {
            throw new SemanticException("Paimon tables do not support ROLLUP");
        }
    }

    private static List<String> resolveColumns(CreateTableStmt stmt, List<String> names, String kind) {
        if (names == null || names.isEmpty()) {
            throw new SemanticException("Paimon %s columns must not be empty", kind);
        }
        Map<String, String> columns = new HashMap<>();
        for (ColumnDef column : stmt.getColumnDefs()) {
            columns.put(column.getName().toLowerCase(Locale.ROOT), column.getName());
        }
        List<String> resolved = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (String name : names) {
            String canonical = columns.get(name.toLowerCase(Locale.ROOT));
            if (canonical == null) {
                throw new SemanticException("Paimon %s column[%s] does not exist", kind, name);
            }
            if (!seen.add(canonical)) {
                throw new SemanticException("Duplicate Paimon %s column: %s", kind, name);
            }
            resolved.add(canonical);
        }
        return resolved;
    }
}
