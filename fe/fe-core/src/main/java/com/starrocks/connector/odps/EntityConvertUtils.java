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

package com.starrocks.connector.odps;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.CharTypeInfo;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.VarcharTypeInfo;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class EntityConvertUtils {

    public static Type convertType(TypeInfo typeInfo) {
        switch (typeInfo.getOdpsType()) {
            case BIGINT:
                return Type.BIGINT;
            case INT:
                return Type.INT;
            case SMALLINT:
                return Type.SMALLINT;
            case TINYINT:
                return Type.TINYINT;
            case FLOAT:
                return Type.FLOAT;
            case DECIMAL:
                DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
                return ScalarType.createUnifiedDecimalType(decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale());
            case DOUBLE:
                return Type.DOUBLE;
            case CHAR:
                CharTypeInfo charTypeInfo = (CharTypeInfo) typeInfo;
                return ScalarType.createCharType(charTypeInfo.getLength());
            case VARCHAR:
                VarcharTypeInfo varcharTypeInfo = (VarcharTypeInfo) typeInfo;
                return ScalarType.createVarcharType(varcharTypeInfo.getLength());
            case STRING:
            case JSON:
                return ScalarType.createDefaultCatalogString();
            case BINARY:
                return Type.VARBINARY;
            case BOOLEAN:
                return Type.BOOLEAN;
            case DATE:
                return Type.DATE;
            case TIMESTAMP:
            case DATETIME:
                return Type.DATETIME;
            case MAP:
                MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
                return new MapType(convertType(mapTypeInfo.getKeyTypeInfo()),
                        convertType(mapTypeInfo.getValueTypeInfo()));
            case ARRAY:
                ArrayTypeInfo arrayTypeInfo = (ArrayTypeInfo) typeInfo;
                return new ArrayType(convertType(arrayTypeInfo.getElementTypeInfo()));
            case STRUCT:
                StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
                List<Type> fieldTypeList =
                        structTypeInfo.getFieldTypeInfos().stream().map(EntityConvertUtils::convertType)
                                .collect(Collectors.toList());
                return new StructType(fieldTypeList);
            default:
                return Type.VARCHAR;
        }
    }

    public static Column convertColumn(com.aliyun.odps.Column column) {
        return new Column(column.getName(), convertType(column.getTypeInfo()), true);
    }

    public static List<Column> getFullSchema(com.aliyun.odps.Table odpsTable) {
        TableSchema tableSchema = odpsTable.getSchema();
        List<com.aliyun.odps.Column> columns = new ArrayList<>(tableSchema.getColumns());
        columns.addAll(tableSchema.getPartitionColumns());
        return columns.stream().map(EntityConvertUtils::convertColumn).collect(
                Collectors.toList());
    }
}
