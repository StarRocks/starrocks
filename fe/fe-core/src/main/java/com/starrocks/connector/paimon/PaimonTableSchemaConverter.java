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
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
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

    /** Build the native schema after SQL clause analysis and before mutating the catalog. */
    public static Schema toSchema(CreateTableStmt stmt) {
        Map<String, String> options = tableOptions(stmt.getProperties());
        List<String> primaryKeys = primaryKeys(stmt, options);
        options.remove(CoreOptions.PRIMARY_KEY.key());
        Schema.Builder builder = Schema.newBuilder();
        List<Column> columns = stmt.getColumns();
        if (columns == null) {
            columns = stmt.getColumnDefs().stream().map(ColumnBuilder::buildColumn).collect(Collectors.toList());
        }
        for (Column column : columns) {
            builder.column(column.getName(), toDataType(column.getType()).copy(column.isAllowNull()), column.getComment());
        }
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
        if (keys != null) {
            return resolveColumns(stmt, keys.getKeysColumnNames());
        }
        String property = options.get(CoreOptions.PRIMARY_KEY.key());
        return property == null ? List.of()
                : resolveColumns(stmt, Arrays.stream(property.split(",", -1)).map(String::trim).toList());
    }

    private static List<String> partitionKeys(CreateTableStmt stmt) {
        PartitionDesc partition = stmt.getPartitionDesc();
        if (partition == null) {
            return List.of();
        }
        ListPartitionDesc list = (ListPartitionDesc) partition;
        List<String> names = list.getPartitionColNames();
        if (list.getMultiDescList() != null && !list.getMultiDescList().isEmpty()) {
            names = list.getMultiDescList().stream().map(node -> ((Identifier) node).getValue()).collect(Collectors.toList());
        }
        return resolveColumns(stmt, names);
    }

    private static List<String> resolveColumns(CreateTableStmt stmt, List<String> names) {
        Map<String, String> columns = new HashMap<>();
        for (ColumnDef column : stmt.getColumnDefs()) {
            columns.put(column.getName().toLowerCase(Locale.ROOT), column.getName());
        }
        return names.stream().map(name -> columns.getOrDefault(name.toLowerCase(Locale.ROOT), name))
                .collect(Collectors.toList());
    }

    private static Map<String, String> tableOptions(Map<String, String> properties) {
        Map<String, String> options = new LinkedHashMap<>();
        Map<String, ConfigOption<?>> knownOptions = CoreOptions.getOptions().stream()
                .collect(Collectors.toMap(option -> option.key().toLowerCase(Locale.ROOT),
                        option -> option, (first, second) -> first));
        if (properties == null) {
            return options;
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey().toLowerCase(Locale.ROOT);
            if (isForbiddenOption(key)) {
                throw new SemanticException("Paimon table property '%s' is a catalog, location or schema override", key);
            }
            ConfigOption<?> option = knownOptions.get(key);
            if (option == null) {
                throw new SemanticException("Unknown Paimon table property: %s", key);
            }
            String canonicalKey = option.key();
            if (entry.getValue() == null || options.putIfAbsent(canonicalKey, entry.getValue()) != null) {
                throw new SemanticException("Invalid or duplicate Paimon table property: %s", key);
            }
        }
        Options typed = Options.fromMap(options);
        for (String key : options.keySet()) {
            ConfigOption<?> option = knownOptions.get(key.toLowerCase(Locale.ROOT));
            try {
                typed.get(option);
            } catch (RuntimeException e) {
                // Do not include raw option values in errors (credentials may be supplied by mistake).
                throw new SemanticException("Invalid value for Paimon table property: %s", key);
            }
        }
        return options;
    }

    private static boolean isForbiddenOption(String key) {
        return key.equals("type") || key.equals("partition") || key.equals("upsert-key") || key.equals("branch")
                || key.equals("path") || key.contains(".path") || key.contains("external-path")
                || key.startsWith("format-table.");
    }
}
