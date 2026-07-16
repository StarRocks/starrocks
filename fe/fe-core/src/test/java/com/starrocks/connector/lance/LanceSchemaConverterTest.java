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

package com.starrocks.connector.lance;

import com.starrocks.catalog.Column;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.util.JsonFields;

import java.util.List;
import java.util.Map;

public class LanceSchemaConverterTest {
    @Test
    public void testNativeJsonExtensionMapping() {
        Field json = JsonFields.jsonUtf8("payload", true);
        Field largeJson = JsonFields.jsonLargeUtf8("large_payload", true);
        Field string = new Field("text", FieldType.nullable(new ArrowType.Utf8()), List.of());

        List<Column> columns = LanceSchemaConverter.fromArrowSchema(new Schema(List.of(json, largeJson, string)));

        Assertions.assertTrue(columns.get(0).getType().isJsonType());
        Assertions.assertTrue(columns.get(1).getType().isJsonType());
        Assertions.assertTrue(columns.get(2).getType().isStringType());
    }

    @Test
    public void testPhysicalLanceJsonExtensionMapping() {
        Field field = extensionField("payload", new ArrowType.LargeBinary(),
                LanceSchemaConverter.LANCE_JSON_EXTENSION_NAME);

        Assertions.assertTrue(LanceSchemaConverter.fromArrowField(field).isJsonType());
    }

    @Test
    public void testNestedJsonExtensionMapping() {
        Field struct = new Field("record", FieldType.nullable(new ArrowType.Struct()),
                List.of(JsonFields.jsonUtf8("payload", true)));

        Type type = LanceSchemaConverter.fromArrowField(struct);

        Assertions.assertTrue(type.isStructType());
        Assertions.assertTrue(((StructType) type).getFields().get(0).getType().isJsonType());
    }

    @Test
    public void testRejectInvalidJsonExtensionStorageType() {
        Field field = extensionField("payload", new ArrowType.Binary(),
                LanceSchemaConverter.ARROW_JSON_EXTENSION_NAME);

        StarRocksConnectorException exception = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> LanceSchemaConverter.fromArrowField(field));
        Assertions.assertTrue(exception.getMessage().contains("Invalid Lance JSON field"));
    }

    private static Field extensionField(String name, ArrowType type, String extensionName) {
        FieldType fieldType = new FieldType(true, type, null,
                Map.of(LanceSchemaConverter.ARROW_EXTENSION_NAME_KEY, extensionName));
        return new Field(name, fieldType, List.of());
    }
}
