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

import com.starrocks.thrift.TIcebergSchema;
import com.starrocks.thrift.TIcebergSchemaField;
import org.apache.paimon.format.parquet.ParquetSchemaConverter;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;

public class PaimonUtils {

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
}
