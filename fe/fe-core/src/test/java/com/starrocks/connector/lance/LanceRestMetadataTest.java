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

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.starrocks.catalog.LanceTable;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.type.ArrayType;
import com.starrocks.type.TypeFactory;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.starrocks.type.DateType.DATE;
import static com.starrocks.type.IntegerType.BIGINT;
import static com.starrocks.type.VarbinaryType.VARBINARY;
import static com.starrocks.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LanceRestMetadataTest {
    private static final Map<String, String> PROPERTIES = Map.of("lance.catalog.type", "rest",
            "lance.catalog.uri", "https://catalog.example.com/warehouse", "lance.catalog.bearer-token-file", "/run/lance/token");

    private LanceMetadata metadata() {
        return new LanceMetadata("remote", PROPERTIES, new LanceCatalogBridge() {
            @Override
            String invoke(String method, String... args) {
                assertEquals(PROPERTIES.get("lance.catalog.uri"), args[0]);
                assertEquals("/run/lance/token", args[1]);
                switch (method) {
                    case "listNamespaces":
                        return "[[],[\"ns\"],[\"ns\",\"a.b\"],[\"empty\"]]";
                    case "listTables":
                        return args[2].equals("[\"empty\"]") ? "[]" : "[\"first\",\"second\"]";
                    case "loadTable":
                        var schema = new Schema(List.of(Field.nullable("id", new ArrowType.Int(64, false)),
                                Field.nullable("text", ArrowType.LargeUtf8.INSTANCE)));
                        return new Gson().toJson(Map.of("schema", schema.toJson(),
                                "location", "s3://bucket/table.lance", "version", 7));
                    default:
                        throw new AssertionError(method);
                }
            }
        });
    }

    @Test
    public void testDiscoveryAndStableIds() {
        var metadata = metadata();
        assertEquals(List.of("$", "ns", "ns.a%2Eb", "empty"), metadata.listDbNames(null));
        assertEquals(List.of("first", "second"), metadata.listTableNames(null, "ns.a%2Eb"));
        assertTrue(metadata.listTableNames(null, "empty").isEmpty());
        assertEquals(metadata.getDb(null, "ns").getId(), metadata.getDb(null, "ns").getId());
        assertNull(metadata.getDb(null, "missing"));
        assertNull(metadata.getTable(null, "ns", "missing"));
        LanceTable first = (LanceTable) metadata.getTable(null, "ns.a%2Eb", "first");
        assertEquals("remote", first.getCatalogName());
        assertEquals("ns.a%2Eb", first.getCatalogDBName());
        assertEquals("ns.a%2Eb", first.toThrift(List.of()).getDbName());
        assertEquals(first.getId(), metadata.getTable(null, "ns.a%2Eb", "first").getId());
        assertNotEquals(first.getId(), metadata.getTable(null, "ns.a%2Eb", "second").getId());
        assertEquals(TypeFactory.createUnifiedDecimalType(20, 0), first.getColumn("id").getType());
        assertEquals(VARCHAR, first.getColumn("text").getType());
        assertTrue(first.getColumn("id").isAllowNull());
        var info = JsonParser.parseString(first.getRestCatalogInfo()).getAsJsonObject();
        assertEquals(JsonParser.parseString("[\"ns\",\"a.b\",\"first\"]"), info.get("table_id"));
        assertEquals("/run/lance/token", info.get("token_file").getAsString());
        assertFalse(info.has("storage_options"));
        assertEquals(7, info.get("dataset_version").getAsLong());
    }

    @Test
    public void testNamespaceRoundTrip() {
        for (List<String> namespace : List.of(List.<String>of(), List.of("a.b", "%2E", "$"), List.of("a", "b"))) {
            assertEquals(namespace, LanceMetadata.namespace(LanceMetadata.databaseName(namespace)));
        }
        assertNotEquals(LanceMetadata.databaseName(List.of("a.b")), LanceMetadata.databaseName(List.of("a", "b")));
        assertThrows(StarRocksConnectorException.class, () -> LanceMetadata.namespace("a..b"));
        assertThrows(StarRocksConnectorException.class, () -> LanceMetadata.namespace("%invalid"));
    }

    @Test
    public void testConfigurationAndStaticCompatibility() {
        assertThrows(IllegalArgumentException.class, () -> new LanceMetadata("bad", Map.of("lance.catalog.type", "unknown")));
        assertThrows(IllegalArgumentException.class, () -> new LanceMetadata("bad", Map.of("lance.catalog.type", "rest")));
        assertThrows(IllegalArgumentException.class, () -> new LanceMetadata("bad", Map.of("lance.catalog.type", "rest",
                "lance.catalog.uri", "https://catalog.example.com", "table.t.uri", "s3://bucket/data.lance")));
        assertNull(new LanceTable(1, "old", List.of(), "s3://bucket/old.lance").getRestCatalogInfo());
    }

    @Test
    public void testArrowSchemaConversionRejectsUnsupportedTypes() {
        assertEquals(VARBINARY, LanceApiConverter.fromArrowField(Field.nullable("data", ArrowType.LargeBinary.INSTANCE)));
        assertEquals(DATE, LanceApiConverter.fromArrowField(Field.nullable("date", new ArrowType.Date(DateUnit.MILLISECOND))));
        var list = new Field("ids", FieldType.nullable(new ArrowType.FixedSizeList(2)),
                List.of(Field.nullable("item", new ArrowType.Int(32, false))));
        assertEquals(BIGINT, ((ArrayType) LanceApiConverter.fromArrowField(list)).getItemType());
        assertThrows(StarRocksConnectorException.class,
                () -> LanceApiConverter.fromArrowField(Field.nullable("struct", ArrowType.Struct.INSTANCE)));
        assertThrows(StarRocksConnectorException.class,
                () -> LanceApiConverter.fromArrowField(Field.nullable("decimal256", new ArrowType.Decimal(50, 0, 256))));
        assertThrows(StarRocksConnectorException.class,
                () -> LanceApiConverter.fromArrowField(Field.nullable("negative_scale", new ArrowType.Decimal(10, -1, 128))));
    }
}
