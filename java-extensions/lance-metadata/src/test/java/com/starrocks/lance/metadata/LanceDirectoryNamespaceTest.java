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

package com.starrocks.lance.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.Dataset;
import org.lance.WriteParams;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LanceDirectoryNamespaceTest {
    @TempDir
    Path warehouse;

    @Test
    void readNumericAndTaggedSchemasAfterDroppingAColumn() throws Exception {
        Field id = new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null);
        Field old = new Field("old_name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        String root = warehouse.toString();
        try (RootAllocator allocator = new RootAllocator();
                Dataset dataset = Dataset.create(allocator, warehouse.resolve("events.lance").toString(),
                        new Schema(List.of(id, old)), new WriteParams.Builder().build())) {
            dataset.tags().create("Release", dataset.version());
            dataset.dropColumns(List.of("old_name"));
        }
        ObjectMapper json = new ObjectMapper();
        JsonNode latest = json.readTree(LanceDirectoryNamespace.describeTable(root, "{}", "events"));
        JsonNode first = json.readTree(LanceDirectoryNamespace.describeTable(root, "{}", "events", "1", null));
        JsonNode tagged = json.readTree(LanceDirectoryNamespace.describeTable(root, "{}", "events", null, "Release"));
        assertEquals(2, latest.get("version").asLong());
        assertEquals(1, latest.at("/schema/fields").size());
        assertEquals(1, first.get("version").asLong());
        assertEquals("old_name", first.at("/schema/fields/1/name").asText());
        assertEquals(first, tagged);
        assertThrows(Exception.class, () -> LanceDirectoryNamespace.describeTable(root, "{}", "events", null, "unknown-tag"));
        assertThrows(Exception.class, () -> LanceDirectoryNamespace.describeTable(root, "{}", "events", "99", null));
    }

    @Test
    void discoverDatasetsAndReadSchemaWithoutRegistration() throws Exception {
        Schema schema = new Schema(List.of(
                new Field("ID", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field("label", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
                new Field("vector", FieldType.nullable(new ArrowType.FixedSizeList(3)), List.of(
                        new Field("item", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null))),
                new Field("price", FieldType.nullable(new ArrowType.Decimal(20, 2, 128)), null)));
        try (RootAllocator allocator = new RootAllocator();
                Dataset ignored = Dataset.create(allocator, warehouse.resolve("vectors.lance").toString(), schema,
                        new WriteParams.Builder().build())) {
            // Dataset schema is persisted before catalog discovery.
        }
        Files.createDirectory(warehouse.resolve("unrelated"));
        ObjectMapper json = new ObjectMapper();
        assertEquals("[\"vectors\"]", LanceDirectoryNamespace.listTables(warehouse.toString(), "{}"));
        JsonNode table = json.readTree(LanceDirectoryNamespace.describeTable(warehouse.toString(), "{}", "vectors"));
        assertEquals(warehouse.resolve("vectors.lance").toString(), table.get("location").asText());
        assertEquals(1L, table.get("version").asLong());
        JsonNode fields = table.get("schema").get("fields");
        assertEquals(4, fields.size());
        assertEquals("ID", fields.get(0).get("name").asText());
        assertFalse(fields.get(0).get("nullable").asBoolean());
        assertTrue(fields.get(1).get("nullable").asBoolean());
        assertEquals("fixedsizelist", fields.get(2).get("type").get("name").asText());
        assertEquals("decimal", fields.get(3).get("type").get("name").asText());
        assertEquals(20, fields.get(3).get("type").get("precision").asInt());
        assertEquals(2, fields.get(3).get("type").get("scale").asInt());
        try (var files = Files.list(warehouse)) {
            assertEquals(List.of("unrelated", "vectors.lance"),
                    files.map(path -> path.getFileName().toString()).sorted().toList());
        }
        assertThrows(Exception.class, () -> LanceDirectoryNamespace.describeTable(warehouse.toString(), "{}", "missing"));
    }
}
