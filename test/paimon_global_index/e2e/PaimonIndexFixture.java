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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

/** Creates disposable Paimon 2.0 files for the SQL matrix. */
public class PaimonIndexFixture {
    private static final String DATABASE = "gi_e2e";

    private static FileStoreTable create(Catalog catalog, String name, boolean disabled) throws Exception {
        Schema.Builder schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("part", DataTypes.STRING())
                .partitionKeys("part")
                .option("bucket", "-1")
                .option("row-tracking.enabled", "true")
                .option("data-evolution.enabled", "true");
        if (disabled) {
            schema.option("global-index.enabled", "false");
        }
        Identifier id = Identifier.create(DATABASE, name);
        catalog.createTable(id, schema.build(), false);
        return (FileStoreTable) catalog.getTable(id);
    }

    private static void write(FileStoreTable table, GenericRow... rows) throws Exception {
        var builder = table.newBatchWriteBuilder();
        try (var writer = builder.newWrite(); var commit = builder.newCommit()) {
            for (GenericRow row : rows) {
                writer.write(row);
            }
            commit.commit(writer.prepareCommit());
        }
    }

    private static void index(FileStoreTable table, Path scratch) throws Exception {
        var builder = new SortedGlobalIndexBuilder(table, "btree").withIndexField("id");
        var splits = builder.scan().orElseThrow().getRight();
        if (splits.size() < 2) {
            throw new IllegalStateException("Expected index splits from two partitions");
        }
        var messages = new ArrayList<CommitMessage>();
        try (var io = new IOManagerImpl(scratch.toString())) {
            for (var split : splits) {
                messages.addAll(builder.build(split, io));
            }
        }
        try (var commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(messages);
        }
        System.out.println(table.name() + " index input splits=" + splits.size()
                + " snapshot=" + table.latestSnapshot().orElseThrow().id());
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: PaimonIndexFixture NEW_WAREHOUSE SCRATCH");
        }
        Path warehouse = Files.createDirectory(Path.of(args[0]));
        Path scratch = Files.createDirectories(Path.of(args[1]));
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(
                new org.apache.paimon.fs.Path(warehouse.toAbsolutePath().toString())))) {
            catalog.createDatabase(DATABASE, false);
            BinaryString left = BinaryString.fromString("left");
            BinaryString right = BinaryString.fromString("right");
            for (String name : new String[]{"healthy", "evolved_string", "evolved_bigint", "partial", "disabled"}) {
                FileStoreTable table = create(catalog, name, name.equals("disabled"));
                if (name.equals("healthy")) {
                    write(table, GenericRow.of(1, left), GenericRow.of(2, right), GenericRow.of(null, right));
                } else {
                    write(table, GenericRow.of(1, left), GenericRow.of(2, right));
                }
                index(table, scratch);
                Identifier id = Identifier.create(DATABASE, name);
                switch (name) {
                    case "evolved_string":
                        catalog.alterTable(id, SchemaChange.updateColumnType("id", DataTypes.STRING()), false);
                        break;
                    case "evolved_bigint":
                        catalog.alterTable(id, SchemaChange.updateColumnType("id", DataTypes.BIGINT()), false);
                        write((FileStoreTable) catalog.getTable(id), GenericRow.of(2147483648L, right));
                        break;
                    case "partial":
                        write(table, GenericRow.of(3, right));
                        break;
                    default:
                        break;
                }
            }
        }
    }
}
