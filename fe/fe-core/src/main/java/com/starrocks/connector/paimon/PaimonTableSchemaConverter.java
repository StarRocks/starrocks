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

package com.starrocks.connector.paimon;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.Identifier;
import com.starrocks.sql.ast.KeysDesc;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.type.ArrayType;
import com.starrocks.type.MapType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** Lossless CREATE TABLE conversion, deliberately separate from the read/view converter. */
public final class PaimonTableSchemaConverter {
    private PaimonTableSchemaConverter() {
    }

    /** Validate Paimon clauses before the generic analyzer can apply native-table rewrites. */
    public static void analyze(CreateTableStmt stmt) {
        if (stmt.getDistributionDesc() != null) {
            throw new SemanticException("Paimon tables do not support DISTRIBUTED BY; use bucket and bucket-key options");
        }
        if (stmt.getOrderByElements() != null && !stmt.getOrderByElements().isEmpty()) {
            throw new SemanticException("Paimon tables do not support ORDER BY in CREATE TABLE");
        }
        if (!stmt.getRollupAlterClauseList().isEmpty()) {
            throw new SemanticException("Paimon tables do not support ROLLUP");
        }
        if (stmt.getExtProperties() != null && !stmt.getExtProperties().isEmpty()) {
            throw new SemanticException("Paimon table options must be specified in PROPERTIES");
        }
        Map<String, String> options = tableOptions(stmt);
        List<String> primaryKeys = primaryKeys(stmt, options);
        partitionKeys(stmt);
        for (ColumnDef column : stmt.getColumnDefs()) {
            if (column.isAutoIncrement()) {
                throw new SemanticException("Paimon does not support AUTO_INCREMENT columns: %s", column.getName());
            }
            if (column.isGeneratedColumn()) {
                throw new SemanticException("Paimon does not support generated columns: %s", column.getName());
            }
            if (column.getAggregateType() != null || column.getAggStateDesc() != null) {
                throw new SemanticException("Paimon does not support aggregate column definitions: %s", column.getName());
            }
            ColumnDef.DefaultValueDef defaultValue = column.getDefaultValueDef();
            if (defaultValue != null && defaultValue.isSet && !(defaultValue.expr instanceof NullLiteral)) {
                throw new SemanticException("Paimon only supports DEFAULT NULL: %s", column.getName());
            }
            if (column.isKey() && !primaryKeys.contains(column.getName())) {
                throw new SemanticException("Paimon key columns must be declared in PRIMARY KEY: %s", column.getName());
            }
            if (primaryKeys.contains(column.getName())) {
                column.setPrimaryKeyNonNullable();
                if (column.isAllowNull()) {
                    throw new SemanticException("Paimon primary key column[%s] cannot be nullable", column.getName());
                }
            }
            column.setIsKey(primaryKeys.contains(column.getName()));
        }
        if (options.containsKey(CoreOptions.BUCKET.key())) {
            int bucket = new CoreOptions(options).bucket();
            if (bucket <= 0 && (bucket != -1 || !primaryKeys.isEmpty())) {
                throw new SemanticException("Paimon writes require bucket > 0 for primary-key tables, " +
                        "or bucket = -1 / bucket > 0 for append tables");
            }
        }
    }

    /** Resolve either SQL PRIMARY KEY or the Paimon primary-key option using SQL identifier rules. */
    public static List<String> primaryKeys(CreateTableStmt stmt) {
        return primaryKeys(stmt, tableOptions(stmt));
    }

    public static Schema toSchema(CreateTableStmt stmt) {
        analyze(stmt);
        Map<String, String> options = tableOptions(stmt);
        List<String> primaryKeys = primaryKeys(stmt, options);
        options.remove(CoreOptions.PRIMARY_KEY.key());
        Schema.Builder builder = Schema.newBuilder();
        List<Column> columns = stmt.getColumns();
        if (columns == null) {
            columns = stmt.getColumnDefs().stream().map(ColumnBuilder::buildColumn).collect(Collectors.toList());
        }
        addColumns(builder, columns);
        builder.primaryKey(primaryKeys).partitionKeys(partitionKeys(stmt)).options(options).comment(stmt.getComment());
        try {
            Schema schema = builder.build();
            SchemaValidation.validateTableSchema(new TableSchema(0, schema.fields(), builder.getHighestFieldId(),
                    schema.partitionKeys(), schema.primaryKeys(), schema.options(), schema.comment()));
            return schema;
        } catch (RuntimeException e) {
            throw new SemanticException("Invalid Paimon table schema: %s", e.getMessage());
        }
    }

    public static RowType toRowType(List<Column> columns) {
        Schema.Builder builder = Schema.newBuilder();
        addColumns(builder, columns);
        return builder.build().rowType();
    }

    private static void addColumns(Schema.Builder builder, List<Column> columns) {
        for (Column column : columns) {
            if (column.isAutoIncrement() || column.isGeneratedColumn() || column.getAggregationType() != null
                    || column.getType().getAggStateDesc() != null) {
                throw new SemanticException("Unsupported Paimon column definition: %s", column.getName());
            }
            if (column.getDefaultValue() != null || column.getDefaultExpr() != null) {
                throw new SemanticException("Paimon only supports DEFAULT NULL: %s", column.getName());
            }
            builder.column(column.getName(), toDataType(column.getType()).copy(column.isAllowNull()), column.getComment());
        }
    }

    private static DataType toDataType(Type type) {
        if (type.getAggStateDesc() != null) {
            throw new SemanticException("Paimon does not support aggregate-state types");
        }
        if (type instanceof ArrayType) {
            return DataTypes.ARRAY(toDataType(((ArrayType) type).getItemType()));
        }
        if (type instanceof MapType) {
            MapType map = (MapType) type;
            return DataTypes.MAP(toDataType(map.getKeyType()), toDataType(map.getValueType()));
        }
        if (type instanceof StructType) {
            List<DataField> fields = new ArrayList<>();
            Set<String> names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            for (StructField field : ((StructType) type).getFields()) {
                if (!names.add(field.getName())) {
                    throw new SemanticException("Duplicate Paimon struct field: %s", field.getName());
                }
                // Schema.Builder reassigns these temporary IDs recursively across the whole schema.
                fields.add(new DataField(fields.size(), field.getName(), toDataType(field.getType()), field.getComment()));
            }
            return new RowType(fields);
        }
        if (!(type instanceof ScalarType)) {
            throw new SemanticException("Unsupported Paimon column type: %s", type);
        }
        ScalarType scalar = (ScalarType) type;
        switch (type.getPrimitiveType()) {
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case TINYINT:
                return DataTypes.TINYINT();
            case SMALLINT:
                return DataTypes.SMALLINT();
            case INT:
                return DataTypes.INT();
            case BIGINT:
                return DataTypes.BIGINT();
            case FLOAT:
                return DataTypes.FLOAT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case DATE:
                return DataTypes.DATE();
            case DATETIME:
                return DataTypes.TIMESTAMP(6);
            case CHAR:
                return DataTypes.CHAR(scalar.getLength());
            case VARCHAR:
                return scalar.getLength() == -1 ? DataTypes.STRING() : DataTypes.VARCHAR(scalar.getLength());
            case VARBINARY:
                return scalar.getLength() == -1 ? DataTypes.BYTES() : DataTypes.VARBINARY(scalar.getLength());
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return DataTypes.DECIMAL(scalar.getScalarPrecision(), scalar.getScalarScale());
            case VARIANT:
                return DataTypes.VARIANT();
            default:
                throw new SemanticException("Unsupported Paimon column type: %s", type);
        }
    }

    private static List<String> primaryKeys(CreateTableStmt stmt, Map<String, String> options) {
        KeysDesc keys = stmt.getKeysDesc();
        if (keys != null && keys.getKeysType() != KeysType.PRIMARY_KEYS) {
            throw new SemanticException("Paimon only supports PRIMARY KEY, not %s", keys.getKeysType());
        }
        List<String> sqlKeys = keys == null ? List.of() : resolveColumns(stmt, keys.getKeysColumnNames(), "primary key");
        String property = options.get(CoreOptions.PRIMARY_KEY.key());
        List<String> propertyKeys = property == null ? List.of()
                : resolveColumns(stmt, Arrays.asList(property.split(",", -1)), "primary key");
        if (keys != null && property != null && !sqlKeys.equals(propertyKeys)) {
            throw new SemanticException("Paimon PRIMARY KEY and primary-key property must specify the same columns in order");
        }
        return keys == null ? propertyKeys : sqlKeys;
    }

    private static List<String> partitionKeys(CreateTableStmt stmt) {
        PartitionDesc partition = stmt.getPartitionDesc();
        if (partition == null) {
            return List.of();
        }
        if (!(partition instanceof ListPartitionDesc)) {
            throw new SemanticException("Paimon only supports PARTITION BY (column, ...) without partition definitions");
        }
        ListPartitionDesc list = (ListPartitionDesc) partition;
        if (!list.getPartitionDescs().isEmpty() || (list.getPartitionExprs() != null && !list.getPartitionExprs().isEmpty())) {
            throw new SemanticException("Paimon only supports ordinary partition columns, " +
                    "not expressions or partition definitions");
        }
        List<String> names = list.getPartitionColNames();
        if (list.getMultiDescList() != null && !list.getMultiDescList().isEmpty()) {
            names = new ArrayList<>();
            for (ParseNode node : list.getMultiDescList()) {
                if (!(node instanceof Identifier)) {
                    throw new SemanticException("Paimon only supports ordinary partition columns, not partition expressions");
                }
                names.add(((Identifier) node).getValue());
            }
        }
        return resolveColumns(stmt, names, "partition");
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
            String canonical = columns.get(name.trim().toLowerCase(Locale.ROOT));
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

    private static Map<String, String> tableOptions(CreateTableStmt stmt) {
        return tableOptions(stmt.getProperties());
    }

    /** Validate and copy table options without requiring a CREATE TABLE AST. */
    public static Map<String, String> tableOptions(Map<String, String> properties) {
        Map<String, String> options = new LinkedHashMap<>();
        Map<String, ConfigOption<?>> knownOptions = CoreOptions.getOptions().stream()
                .collect(Collectors.toMap(ConfigOption::key, option -> option, (first, second) -> first));
        if (properties == null) {
            return options;
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey().toLowerCase(Locale.ROOT);
            if (isForbiddenOption(key)) {
                throw new SemanticException("Paimon table property '%s' is a catalog, location or schema override", key);
            }
            if (!knownOptions.containsKey(key)) {
                throw new SemanticException("Unknown Paimon table property: %s", key);
            }
            if (entry.getValue() == null || options.putIfAbsent(key, entry.getValue()) != null) {
                throw new SemanticException("Invalid or duplicate Paimon table property: %s", key);
            }
        }
        Options typed = Options.fromMap(options);
        for (String key : options.keySet()) {
            try {
                typed.get(knownOptions.get(key));
            } catch (RuntimeException e) {
                // Do not include raw option values in errors (credentials may be supplied by mistake).
                throw new SemanticException("Invalid value for Paimon table property: %s", key);
            }
        }
        return options;
    }

    private static boolean isForbiddenOption(String key) {
        return key.equals("type") || key.equals("warehouse") || key.equals("partition") || key.equals("upsert-key")
                || key.equals("branch") || key.equals("owner") || key.equals("path") || key.contains(".path")
                || key.contains("external-path") || key.contains("schema") || key.contains("password")
                || key.contains("secret") || key.contains("credential") || key.contains("token")
                || key.contains("access-key") || key.contains("access_key") || key.startsWith("catalog.")
                || key.startsWith("paimon.catalog.") || key.startsWith("hive.") || key.startsWith("hadoop.")
                || key.startsWith("fs.") || key.startsWith("s3.") || key.startsWith("s3a.") || key.startsWith("oss.")
                || key.startsWith("azure.") || key.startsWith("gcs.") || key.startsWith("format-table.");
    }
}
