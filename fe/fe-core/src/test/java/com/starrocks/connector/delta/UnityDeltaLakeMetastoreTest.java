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

package com.starrocks.connector.delta;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.planner.DeltaLakeScanNode;
import com.starrocks.planner.PartitionIdGenerator;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.planner.TupleId;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.ScanOptimizeOption;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.connector.delta.UnityCatalogTestSupport.SAS_KEY;
import static com.starrocks.connector.delta.UnityCatalogTestSupport.STORAGE;
import static com.starrocks.connector.delta.UnityCatalogTestSupport.TOKEN;
import static com.starrocks.connector.delta.UnityCatalogTestSupport.sas;

public class UnityDeltaLakeMetastoreTest {
    @TempDir
    Path temporaryDirectory;

    private UnityCatalogTestSupport.FakeCatalog catalog;
    private ConnectContext previousContext;

    @BeforeEach
    public void setUp() throws IOException {
        catalog = new UnityCatalogTestSupport.FakeCatalog();
        previousContext = ConnectContext.get();
        ConnectContext.remove();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.set(previousContext);
        catalog.close();
    }

    @Test
    public void testFactoryCapturesEachQueryCallerAndDoesNotShareUserCaches() {
        DeltaLakeConnector connector = new DeltaLakeConnector(new ConnectorContext("unity", "deltalake", catalog.properties()));
        Assertions.assertFalse(connector.supportMemoryTrack());
        Assertions.assertEquals(0, connector.estimateSize());
        ConnectContext alice = context("alice", TOKEN);
        ConnectContext.set(alice);
        ConnectorMetadata aliceMetadata = connector.getMetadata();
        alice.setAuthToken("synthetic.alice.changed-token");
        ConnectContext bob = context("bob", "synthetic.bob.signature");
        ConnectContext.set(bob);
        ConnectorMetadata bobMetadata = connector.getMetadata();
        ConnectContext.remove();
        catalog.enqueue(200, "{\"schemas\":[{\"name\":\"alice_schema\",\"catalog_name\":\"main\"}]}");
        catalog.enqueue(200, "{\"schemas\":[{\"name\":\"bob_schema\",\"catalog_name\":\"main\"}]}");

        Assertions.assertNotSame(aliceMetadata, bobMetadata);
        Assertions.assertEquals(MetastoreType.UNITY, ((DeltaLakeMetadata) aliceMetadata).getMetastoreType());
        Assertions.assertEquals(List.of("alice_schema"), aliceMetadata.listDbNames(bob));
        Assertions.assertEquals(List.of("bob_schema"), bobMetadata.listDbNames(alice));
        Assertions.assertEquals(List.of("Bearer " + TOKEN, "Bearer synthetic.bob.signature"), catalog.authorizations());
    }

    @Test
    public void testUnityRequiresSessionAndTokenWhileHmsMetadataStillWorksWithoutSession() {
        DeltaLakeConnector unity = new DeltaLakeConnector(new ConnectorContext("unity", "deltalake", catalog.properties()));
        Assertions.assertThrows(StarRocksConnectorException.class, unity::getMetadata);
        ConnectContext.set(context("password_user", null));
        Assertions.assertThrows(StarRocksConnectorException.class, unity::getMetadata);
        ConnectContext.remove();
        DeltaLakeConnector hms = new DeltaLakeConnector(new ConnectorContext("hms", "deltalake", Map.of(
                "hive.metastore.type", "hive", "hive.metastore.uris", "thrift://127.0.0.1:9083",
                "enable_deltalake_table_cache", "false")));
        Assertions.assertEquals(MetastoreType.HMS, ((DeltaLakeMetadata) hms.getMetadata()).getMetastoreType());
        Assertions.assertTrue(catalog.requests.isEmpty());
    }

    @Test
    public void testReadsDeltaRowsWithIndependentFixedTableCredentialsAndSerializesThemToBackend() throws IOException {
        String alphaLocation = createTable("alpha", 7L);
        String betaLocation = createTable("beta", 11L);
        String alphaSas = sas("alpha-signature", alphaLocation);
        String betaSas = sas("beta-signature", betaLocation);
        Configuration configuration = new Configuration();
        configuration.setClass("fs.abfss.impl", LocalCredentialFileSystem.class, org.apache.hadoop.fs.FileSystem.class);
        configuration.setBoolean("fs.abfss.impl.disable.cache", true);
        configuration.set("test.fixture.root", temporaryDirectory.toString());
        configuration.set("test.expected-sas.alpha", alphaSas);
        configuration.set("test.expected-sas.beta", betaSas);
        UnityDeltaLakeMetastore metastore = new UnityDeltaLakeMetastore("unity", catalog.properties(), configuration,
                context("alice", TOKEN));
        catalog.enqueueTable("alpha", alphaLocation, alphaSas);
        catalog.enqueueTable("beta", betaLocation, betaSas);

        DeltaLakeTable alpha = metastore.getTable("schema", "alpha");
        DeltaLakeTable beta = metastore.getTable("schema", "beta");
        Assertions.assertTrue(alpha.isUnityCatalogTable());
        Assertions.assertTrue(beta.isUnityCatalogTable());
        Assertions.assertEquals(List.of(11L), readRows(beta));
        Assertions.assertEquals(List.of(7L), readRows(alpha));
        Assertions.assertNull(configuration.get(SAS_KEY));
        Assertions.assertEquals(4, catalog.requests.size());
        Assertions.assertEquals(List.of("Bearer " + TOKEN, "Bearer " + TOKEN, "Bearer " + TOKEN, "Bearer " + TOKEN),
                catalog.authorizations());
        Assertions.assertEquals(alphaSas, backendSas(alpha));
        Assertions.assertEquals(betaSas, backendSas(beta));
        Assertions.assertEquals(4, catalog.requests.size(), "Reading and serializing a loaded table must not renew its SAS");
    }

    private static ConnectContext context(String user, String token) {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(new UserIdentity(user, "%"));
        context.setAuthToken(token);
        return context;
    }

    private String createTable(String name, long value) throws IOException {
        Path tablePath = temporaryDirectory.resolve(name);
        Path log = Files.createDirectories(tablePath.resolve("_delta_log"));
        Path parquet = tablePath.resolve("part-000.parquet");
        MessageType parquetSchema = MessageTypeParser.parseMessageType("message rows { required int64 id; }");
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new org.apache.hadoop.fs.Path(parquet.toUri()))
                .withType(parquetSchema).withConf(new Configuration()).build()) {
            writer.write(new SimpleGroup(parquetSchema).append("id", value));
        }
        JsonObject format = new JsonObject();
        format.addProperty("provider", "parquet");
        format.add("options", new JsonObject());
        JsonObject metadata = new JsonObject();
        metadata.addProperty("id", "id-" + name);
        metadata.add("format", format);
        metadata.addProperty("schemaString", "{\"type\":\"struct\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}}]}");
        metadata.add("partitionColumns", new JsonArray());
        metadata.add("configuration", new JsonObject());
        metadata.addProperty("createdTime", 1700000000000L);
        JsonObject add = new JsonObject();
        add.addProperty("path", parquet.getFileName().toString());
        add.add("partitionValues", new JsonObject());
        add.addProperty("size", Files.size(parquet));
        add.addProperty("modificationTime", 1700000000000L);
        add.addProperty("dataChange", true);
        add.addProperty("stats", "{\"numRecords\":1}");
        Files.writeString(log.resolve("00000000000000000000.json"),
                "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n"
                        + "{\"metaData\":" + metadata + "}\n{\"add\":" + add + "}\n");
        return STORAGE + tablePath.toUri().getRawPath();
    }

    private static List<Long> readRows(DeltaLakeTable table) throws IOException {
        Engine engine = table.getDeltaEngine();
        List<Long> values = new ArrayList<>();
        try (CloseableIterator<FilteredColumnarBatch> batches = table.getDeltaSnapshot().getScanBuilder()
                .build().getScanFiles(engine)) {
            while (batches.hasNext()) {
                try (CloseableIterator<Row> files = batches.next().getRows()) {
                    while (files.hasNext()) {
                        FileStatus file = InternalScanFileUtils.getAddFileStatus(files.next());
                        try (CloseableIterator<FileReadResult> data = engine.getParquetHandler().readParquetFiles(
                                Utils.singletonCloseableIterator(file), table.getDeltaSnapshot().getSchema(),
                                Optional.empty())) {
                            while (data.hasNext()) {
                                try (CloseableIterator<Row> rows = data.next().getData().getRows()) {
                                    while (rows.hasNext()) {
                                        values.add(rows.next().getLong(0));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return values;
    }

    private static String backendSas(DeltaLakeTable table) {
        TupleDescriptor tuple = new TupleDescriptor(new TupleId(0));
        tuple.setTable(table);
        DeltaLakeScanNode node = new DeltaLakeScanNode(new PlanNodeId(0), tuple, "Delta scan", null,
                List.of("id"), PartitionIdGenerator.of());
        node.setScanOptimizeOption(new ScanOptimizeOption());
        return node.treeToThrift().getNodes().get(0).getHdfs_scan_node().getCloud_configuration()
                .getCloud_properties().get(SAS_KEY);
    }

    /** Reads a real local Delta fixture through its ADLS URI and checks the credentials on every file open. */
    public static final class LocalCredentialFileSystem extends RawLocalFileSystem {
        @Override
        public URI getUri() {
            return URI.create(STORAGE + "/");
        }

        @Override
        public FSDataInputStream open(org.apache.hadoop.fs.Path path, int bufferSize) throws IOException {
            Path localPath = Path.of(path.toUri().getPath());
            Path root = Path.of(getConf().get("test.fixture.root"));
            Assertions.assertTrue(localPath.startsWith(root));
            String table = root.relativize(localPath).getName(0).toString();
            Assertions.assertEquals(getConf().get("test.expected-sas." + table), getConf().get(SAS_KEY));
            return super.open(path, bufferSize);
        }
    }
}
