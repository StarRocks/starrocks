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

package com.starrocks.connector.share.iceberg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Test;

import java.io.ObjectStreamClass;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IcebergTablePayloadTest {
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "dt", Types.StringType.get()));

    @Test
    public void testSerialVersionUidIsPinned() {
        // FE and BE of different releases exchange the Java form: the value must never change.
        assertEquals(-1554890835414786287L, ObjectStreamClass.lookup(SerializableTable.class).getSerialVersionUID());
    }

    @Test
    public void testJsonFormMatchesJavaForm() {
        Table table = createTable("parity");
        Configuration conf = new Configuration(false);
        conf.set("fs.s3a.access.key", "ak");
        conf.set("fs.s3a.endpoint", "http://s3.local");
        ResolvingFileIO io = new ResolvingFileIO();
        io.setConf(conf);
        Map<String, String> ioProperties = new HashMap<>();
        ioProperties.put("s3.endpoint", "http://s3.local");
        ioProperties.put("client.region", "us-west-2");
        io.initialize(ioProperties);
        io.setCredentials(List.of(StorageCredential.create("s3://bucket/a", Map.of("s3.access-key-id", "vended"))));
        SerializableTable source = new SerializableTable(table, io);

        SerializableTable fromJava = SerializationUtil.deserializeFromBase64(SerializationUtil.serializeToBase64(source));
        SerializableTable fromJson = IcebergTablePayload.fromJson(toJson(source));

        for (SerializableTable rebuilt : List.of(fromJava, fromJson)) {
            assertEquals(table.name(), rebuilt.name());
            assertEquals(table.location(), rebuilt.location());
            assertEquals(table.uuid(), rebuilt.uuid());
            assertEquals(table.properties(), rebuilt.properties());
            assertTrue(table.schema().sameSchema(rebuilt.schema()));
            assertEquals(table.spec(), rebuilt.spec());
            assertEquals(table.specs(), rebuilt.specs());
            assertEquals(table.sortOrder(), rebuilt.sortOrder());
            assertEquals(table.refs(), rebuilt.refs());
            assertEquals(source.metadataFileLocation(), rebuilt.metadataFileLocation());
            assertInstanceOf(AzurePathNormalizingFileIO.class, rebuilt.io());

            ResolvingFileIO rebuiltIO = assertInstanceOf(ResolvingFileIO.class, rebuilt.rawIO());
            assertEquals(io.properties(), rebuiltIO.properties());
            assertEquals(toMap(io.getConf()), toMap(rebuiltIO.getConf()));
            assertEquals(credentials(io.credentials()), credentials(rebuiltIO.credentials()));
        }
    }

    @Test
    public void testJsonFormLoadsMetadataLazily() {
        Table table = createTable("lazy");
        DataFile file = DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/dt=a/f1.parquet")
                .withPartitionPath("dt=a")
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .build();
        table.newAppend().appendFile(file).commit();
        table.newAppend().appendFile(DataFiles.builder(table.spec()).copy(file)
                .withPath(table.location() + "/data/dt=a/f2.parquet").build()).commit();
        table.manageSnapshots().createBranch("b1", table.currentSnapshot().snapshotId()).commit();
        table.refresh();

        SerializableTable rebuilt =
                IcebergTablePayload.fromJson(toJson(new SerializableTable(table, table.io())));

        assertEquals(snapshotIds(table.snapshots()), snapshotIds(rebuilt.snapshots()));
        assertEquals(table.currentSnapshot().snapshotId(), rebuilt.currentSnapshot().snapshotId());
        assertEquals(table.refs(), rebuilt.refs());
        assertEquals(table.history().size(), rebuilt.history().size());
        assertEquals(table.schemas().keySet(), rebuilt.schemas().keySet());
        assertEquals(2, rebuilt.currentSnapshot().allManifests(rebuilt.io()).stream()
                .mapToInt(m -> m.addedFilesCount() + m.existingFilesCount()).sum());
    }

    @Test
    public void testReaderIgnoresUnknownFields() throws Exception {
        Table table = createTable("unknown");
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode envelope = (ObjectNode) mapper.readTree(toJson(new SerializableTable(table, table.io())));
        // a newer writer may put fields anywhere, before the header ones too
        ObjectNode reordered = mapper.createObjectNode();
        reordered.put("added_by_a_newer_fe", "x");
        reordered.putObject("also_new").putArray("a").add(1);
        reordered.setAll(envelope);
        ((ObjectNode) reordered.get("table")).put("added_by_a_newer_fe", "x");
        ((ObjectNode) reordered.get("table").get("file_io")).putObject("also_new").put("k", 1);
        String json = mapper.writeValueAsString(reordered);

        SerializableTable rebuilt = IcebergTablePayload.fromJson(json);
        assertEquals(table.name(), rebuilt.name());
        assertTrue(table.schema().sameSchema(rebuilt.schema()));
        assertEquals("b1", IcebergTablePayload.readBuild(json));
    }

    @Test
    public void testRejectsPayloadWithoutFormat() {
        assertThrows(IllegalArgumentException.class,
                () -> IcebergTablePayload.fromJson("{\"table\":{\"name\":\"t\"}}"));
        assertThrows(IllegalArgumentException.class, () -> IcebergTablePayload.fromJson("{\"format\":1}"));
        assertThrows(IllegalArgumentException.class, () -> IcebergTablePayload.readBuild("{\"format\":0}"));
    }

    @Test
    public void testHeaderReadsBuildWithoutTable() {
        Table table = createTable("header");
        String json = IcebergTablePayload.toJson(new SerializableTable(table, table.io()), "v1-abc");
        assertEquals("v1-abc", IcebergTablePayload.readBuild(json));
        // the header comes first, so it is read without reaching the table
        assertTrue(json.indexOf("\"build\"") < json.indexOf("\"table\""));
        assertEquals("", IcebergTablePayload.readBuild("{\"format\":1,\"table\":{}}"));
        // the predicate travels apart as Iceberg ExpressionParser JSON
        Expression predicate = Expressions.and(
                Expressions.equal("dt", "a"),
                Expressions.or(Expressions.greaterThan("id", 10L), Expressions.isNull("data")));
        assertEquals(predicate.toString(), ExpressionParser.fromJson(ExpressionParser.toJson(predicate)).toString());
    }

    private static String toJson(SerializableTable table) {
        return IcebergTablePayload.toJson(table, "b1");
    }

    private static Table createTable(String name) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", new HashMap<>());
        catalog.createNamespace(Namespace.of("db"));
        Map<String, String> properties = new HashMap<>();
        properties.put("format-version", "2");
        properties.put("write.format.default", "parquet");
        Table table = catalog.createTable(TableIdentifier.of("db", name), SCHEMA,
                PartitionSpec.builderFor(SCHEMA).identity("dt").build(), properties);
        assertInstanceOf(InMemoryFileIO.class, table.io());
        assertNotNull(table.uuid());
        return table;
    }

    private static Map<String, String> toMap(Configuration conf) {
        Map<String, String> map = new HashMap<>();
        conf.forEach(e -> map.put(e.getKey(), e.getValue()));
        return map;
    }

    private static List<String> credentials(List<StorageCredential> credentials) {
        List<String> result = new ArrayList<>();
        credentials.forEach(c -> result.add(c.prefix() + "=" + c.config()));
        return result;
    }

    private static List<Long> snapshotIds(Iterable<Snapshot> snapshots) {
        return StreamSupport.stream(snapshots.spliterator(), false)
                .map(Snapshot::snapshotId).collect(Collectors.toList());
    }
}
