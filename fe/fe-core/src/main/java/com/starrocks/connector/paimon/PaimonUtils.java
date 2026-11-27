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

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.thrift.TIcebergSchema;
import com.starrocks.thrift.TIcebergSchemaField;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.format.parquet.ParquetSchemaConverter;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class PaimonUtils {

    /** Returns the HMS base name for a native Paimon system table, including branch-qualified names. */
    public static String systemTableBaseName(String dbName, String fullName) {
        Identifier id = Identifier.create(dbName, fullName);
        try {
            String suffix = id.getSystemTableName();
            if (suffix != null && SystemTableLoader.SYSTEM_TABLES.contains(suffix.toLowerCase(Locale.ROOT))
                    && !id.getTableName().isEmpty()) {
                return id.getTableName();
            }
        } catch (IllegalArgumentException ignored) {
            // A literal HMS name containing '$' need not be a valid Paimon identifier.
        }
        return null;
    }

    public static TIcebergSchema getTPaimonSchema(RowType rowType) {
        // reuse TIcebergSchema directly for compatibility.
        TIcebergSchema tPaimonSchema = new TIcebergSchema();
        List<DataField> paimonFields = rowType.getFields();
        List<TIcebergSchemaField> tIcebergFields = new ArrayList<>(paimonFields.size());
        for (DataField field : paimonFields) {
            tIcebergFields.add(getTPaimonSchemaField(field.name(), field.type(), field.id(), 0, -1));
        }
        tPaimonSchema.setFields(tIcebergFields);
        return tPaimonSchema;
    }

    public static TIcebergSchemaField getTPaimonSchemaField(String name, DataType type, int fieldId, int depth, int parentId) {
        TIcebergSchemaField tPaimonSchemaField = new TIcebergSchemaField();
        if (parentId != -1) {
            tPaimonSchemaField.setField_id(parentId);
        } else {
            tPaimonSchemaField.setField_id(fieldId);
        }
        tPaimonSchemaField.setName(name);
        if (type.getTypeRoot() == DataTypeRoot.MAP) {
            org.apache.paimon.types.MapType mapType = (MapType) type;
            DataType keyType = mapType.getKeyType();
            DataType valueType = mapType.getValueType();
            int mapKeyFieldId = SpecialFields.getMapKeyFieldId(fieldId, depth + 1);
            int mapValueFieldId = SpecialFields.getMapValueFieldId(fieldId, depth + 1);
            List<TIcebergSchemaField> children = new ArrayList<>(2);
            children.add(getTPaimonSchemaField(ParquetSchemaConverter.MAP_KEY_NAME, keyType, fieldId, depth + 1, mapKeyFieldId));
            children.add(getTPaimonSchemaField(ParquetSchemaConverter.MAP_VALUE_NAME, valueType, fieldId,
                    depth + 1, mapValueFieldId));
            tPaimonSchemaField.setChildren(children);
        }
        if (type.getTypeRoot() == DataTypeRoot.ARRAY) {
            org.apache.paimon.types.ArrayType arrayType = (ArrayType) type;
            DataType elementType = arrayType.getElementType();
            int elementId = SpecialFields.getArrayElementFieldId(fieldId, depth + 1);
            List<TIcebergSchemaField> children = new ArrayList<>(1);
            children.add(getTPaimonSchemaField(ParquetSchemaConverter.LIST_ELEMENT_NAME, elementType, fieldId,
                    depth + 1, elementId));
            tPaimonSchemaField.setChildren(children);
        }
        // the parent id of row type is always -1, refer to: org.apache.paimon.format.parquet.ParquetSchemaConverter
        if (type.getTypeRoot() == DataTypeRoot.ROW) {
            RowType rowType = (RowType) type;
            List<DataField> childrenFields = rowType.getFields();
            List<TIcebergSchemaField> children = new ArrayList<>(rowType.getFieldCount());
            for (DataField childrenField : childrenFields) {
                children.add(getTPaimonSchemaField(childrenField.name(), childrenField.type(), childrenField.id(),
                        depth + 1, -1));
            }
            tPaimonSchemaField.setChildren(children);
        }
        return tPaimonSchemaField;
    }
    public static void validateWriteTarget(InsertStmt statement, PaimonTable table) {
        try {
            PaimonWriteCapabilities.validateWrite(table.getNativeTable());
        } catch (StarRocksConnectorException e) {
            throw new SemanticException(e.getMessage());
        }
        if (statement.usePartialUpdate()) {
            throw new SemanticException("Paimon writes do not support partial-update mode");
        }
        List<org.apache.paimon.types.DataField> nativeFields = table.getNativeTable().rowType().getFields();
        List<Column> schema = table.getFullSchema();
        if (schema.size() != nativeFields.size()) {
            throw new SemanticException("Paimon target schema changed during planning; refresh the table and retry");
        }
        for (int i = 0; i < schema.size(); ++i) {
            if (!schema.get(i).getName().equals(nativeFields.get(i).name())
                    || !schema.get(i).getType().equals(ColumnTypeConverter.fromPaimonType(nativeFields.get(i).type()))) {
                throw new SemanticException("Paimon target schema changed during planning; refresh the table and retry");
            }
        }
    }

    public static boolean isWriteColumnNullable(PaimonTable table, String columnName) {
        return table.getNativeTable().rowType().getFields().stream()
                .filter(field -> field.name().equals(columnName))
                .findFirst().orElseThrow(() -> new SemanticException("Paimon target column changed during planning"))
                .type().isNullable();
    }

    public static Map<String, String> getStaticPartitionValues(InsertStmt insertStmt, PaimonTable table) {
        if (!insertStmt.isStaticKeyPartitionInsert()) {
            return Collections.emptyMap();
        }
        Map<String, String> partition = new LinkedHashMap<>();
        List<String> names = insertStmt.getTargetPartitionNames().getPartitionColNames();
        List<Expr> values = insertStmt.getTargetPartitionNames().getPartitionColValues();
        String nullPartition = table.getNativeTable().options().getOrDefault(
                CoreOptions.PARTITION_DEFAULT_NAME.key(), CoreOptions.PARTITION_DEFAULT_NAME.defaultValue());
        for (String column : table.getPartitionColumnNames()) {
            int index = -1;
            for (int i = 0; i < names.size(); ++i) {
                if (column.equalsIgnoreCase(names.get(i))) {
                    index = i;
                    break;
                }
            }
            Preconditions.checkState(index >= 0, "Paimon static partition target mismatch");
            LiteralExpr value = (LiteralExpr) values.get(index);
            String partitionValue;
            if (value.isConstantNull()) {
                partitionValue = nullPartition;
            } else if (value instanceof BoolLiteral boolLiteral) {
                partitionValue = Boolean.toString(boolLiteral.getValue());
            } else {
                partitionValue = value.getStringValue();
            }
            partition.put(column, partitionValue);
        }
        return partition;
    }

    public static String getWriteOperationId(ConnectContext session) {
        if (session.getQueryId() != null) {
            return session.getQueryId().toString().replace('-', '_');
        }
        if (session.getExecutionId() != null) {
            return DebugUtil.printId(session.getExecutionId()).replace('-', '_');
        }
        throw new SemanticException("Paimon writes require a query execution identity");
    }

}
