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

package com.starrocks.connector.bigquery;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.starrocks.catalog.Column;
import com.starrocks.type.ArrayType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static com.starrocks.type.BooleanType.BOOLEAN;
import static com.starrocks.type.DateType.DATE;
import static com.starrocks.type.DateType.DATETIME;
import static com.starrocks.type.FloatType.DOUBLE;
import static com.starrocks.type.IntegerType.BIGINT;
import static com.starrocks.type.VarbinaryType.VARBINARY;

public class BigQuerySchemaUtils {
    private static final Logger LOG = LogManager.getLogger(BigQuerySchemaUtils.class);

    private BigQuerySchemaUtils() {}

    /**
     * Convert a BigQuery {@link Schema} to a list of StarRocks {@link Column}s.
     * Top-level REPEATED fields become ARRAY<element_type>.
     */
    public static List<Column> toStarRocksColumns(Schema schema) {
        List<Column> columns = new ArrayList<>();
        for (Field field : schema.getFields()) {
            Type srType = convertField(field);
            columns.add(new Column(field.getName().toLowerCase(), srType, true));
        }
        return columns;
    }

    /**
     * Convert a single BigQuery {@link Field} to a StarRocks {@link Type}.
     * REPEATED mode wraps the base type in an {@link ArrayType}.
     */
    public static Type convertField(Field field) {
        Type baseType = convertFieldType(field);
        if (field.getMode() == Field.Mode.REPEATED) {
            return new ArrayType(baseType);
        }
        return baseType;
    }

    private static Type convertFieldType(Field field) {
        StandardSQLTypeName typeName = field.getType().getStandardType();
        switch (typeName) {
            case INT64:
            case INTEGER:
                return BIGINT;
            case FLOAT64:
            case FLOAT:
                return DOUBLE;
            case NUMERIC:
                return TypeFactory.createUnifiedDecimalType(38, 9);
            case BIGNUMERIC: {
                // BigQuery BIGNUMERIC has 76 digits of precision; StarRocks DECIMAL caps at 38.
                LOG.warn("BIGNUMERIC field '{}' exceeds StarRocks max DECIMAL precision (38). " +
                        "Values may lose precision.", field.getName());
                return TypeFactory.createUnifiedDecimalType(38, 38);
            }
            case BOOL:
            case BOOLEAN:
                return BOOLEAN;
            case STRING:
                return TypeFactory.createDefaultCatalogString();
            case BYTES:
                return VARBINARY;
            case DATE:
                return DATE;
            case TIME:
                // StarRocks has no TIME type; represent as VARCHAR.
                return TypeFactory.createVarcharType(16);
            case DATETIME:
            case TIMESTAMP:
                return DATETIME;
            case GEOGRAPHY:
            case JSON:
                return TypeFactory.createDefaultCatalogString();
            case STRUCT: {
                FieldList subFields = field.getSubFields();
                if (subFields == null || subFields.isEmpty()) {
                    return TypeFactory.createDefaultCatalogString();
                }
                List<StructField> srStructFields = new ArrayList<>();
                for (Field subField : subFields) {
                    Type subType = convertField(subField);
                    srStructFields.add(new StructField(subField.getName().toLowerCase(), subType));
                }
                return new StructType(srStructFields);
            }
            case RANGE:
                return TypeFactory.createDefaultCatalogString();
            default:
                LOG.warn("Unknown BigQuery type '{}' for field '{}'; mapping to VARCHAR.",
                        typeName, field.getName());
                return TypeFactory.createDefaultCatalogString();
        }
    }
}
