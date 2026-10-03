// Copyright 2021-present StarRocks, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Generate and independently verify pk_merge_v1 with Apache Paimon 2.0.0. */
public class GeneratePrimaryKeyFixture {
    private static final Identifier TABLE = Identifier.create("paimon_test", "pk_merge_v1");

    public static void main(String[] args) throws Exception {
        if (args.length != 2 || !(args[0].equals("generate") || args[0].equals("verify"))) {
            throw new IllegalArgumentException("Usage: GeneratePrimaryKeyFixture generate|verify <local-warehouse>");
        }
        Path warehouse = Path.of(args[1]).toAbsolutePath();
        if (args[0].equals("generate")) {
            // Refuse to overwrite an existing fixture or warehouse.
            Files.createDirectory(warehouse);
        } else if (!Files.isDirectory(warehouse.resolve("paimon_test.db/pk_merge_v1"))) {
            throw new IllegalArgumentException("Fixture directory does not exist");
        }
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(
                new org.apache.paimon.fs.Path(warehouse.toUri())))) {
            if (args[0].equals("generate")) {
                catalog.createDatabase("paimon_test", false);
                Schema schema = Schema.newBuilder()
                        .column("id", DataTypes.INT().notNull())
                        .column("amount", DataTypes.INT())
                        .column("note", DataTypes.STRING())
                        .primaryKey("id")
                        .option("bucket", "1")
                        .option("file.format", "parquet")
                        .option("merge-engine", "deduplicate")
                        .option("write-only", "true")
                        .option("write-buffer-size", "1 mb")
                        .build();
                catalog.createTable(TABLE, schema, false);
                StreamWriteBuilder builder = catalog.getTable(TABLE).newStreamWriteBuilder()
                        .withCommitUser("starrocks-pk-fixture-v1");
                try (StreamTableWrite write = builder.newWrite(); StreamTableCommit commit = builder.newCommit()) {
                    write.write(row(RowKind.INSERT, 1, 10, "old"));
                    write.write(row(RowKind.INSERT, 2, 20, "deleted"));
                    write.write(row(RowKind.INSERT, 3, 30, "kept"));
                    commit.commit(1, write.prepareCommit(true, 1));

                    write.write(row(RowKind.UPDATE_AFTER, 1, 100, "updated"));
                    write.write(row(RowKind.DELETE, 2, 20, "deleted"));
                    write.write(row(RowKind.INSERT, 4, 40, "added"));
                    commit.commit(2, write.prepareCommit(true, 2));
                }
            }
            verify(catalog.getTable(TABLE));
        }
    }

    private static GenericRow row(RowKind kind, int id, int amount, String note) {
        return GenericRow.ofKind(kind, id, amount, BinaryString.fromString(note));
    }

    private static void verify(Table table) throws Exception {
        require(table.latestSnapshot().orElseThrow().id() == 2, "Expected exactly two snapshots");
        ReadBuilder read = table.newReadBuilder();
        List<Split> splits = read.newScan().plan().splits();
        require(splits.size() == 1 && splits.get(0) instanceof DataSplit, "Expected one merge split");
        DataSplit split = (DataSplit) splits.get(0);
        require(!split.convertToRawFiles().isPresent(), "Fixture must require merging, not a raw file scan");
        List<DataFileMeta> files = split.dataFiles();
        require(files.size() == 2 && files.stream().allMatch(f -> f.level() == 0), "Expected two level-0 files");
        require(files.stream().mapToLong(DataFileMeta::rowCount).sum() == 6, "Expected six physical records");
        require(files.stream().mapToLong(f -> f.deleteRowCount().orElseThrow()).sum() == 1,
                "Expected one physical delete record");
        List<String> rows = new ArrayList<>();
        try (RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(read.newRead().createReader(splits))) {
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                rows.add(row.getInt(0) + "," + row.getInt(1) + "," + row.getString(2));
            }
        }
        Collections.sort(rows);
        require(rows.equals(Arrays.asList("1,100,updated", "3,30,kept", "4,40,added")), "Unexpected rows: " + rows);
        System.out.println("Verified: two L0 files, six physical records, one delete, raw scan unavailable");
        System.out.println("Merged rows: " + rows);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
