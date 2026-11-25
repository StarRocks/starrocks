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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

// Offline Paimon 2.0 fixture. Args: a new local warehouse directory.
public class GenerateStructParentNullFixture {
    private static final int ROWS = 8192;

    private static GenericRow detail(int kind) {
        switch (kind) {
            case 0:
                return GenericRow.of(10, GenericVariant.fromJson("42"),
                        GenericRow.of(GenericVariant.fromJson("\"ok\"")));
            case 1:
            case 6:
                return null;
            case 2:
                return GenericRow.of(30, GenericVariant.fromJson("\"visible\""), null);
            case 3:
                return GenericRow.of(40, null, GenericRow.of((Object) null));
            case 4:
                return GenericRow.of(50, GenericVariant.fromJson("null"),
                        GenericRow.of(GenericVariant.fromJson("null")));
            case 5:
                return GenericRow.of(60, GenericVariant.fromJson("{\"n\":7}"),
                        GenericRow.of(GenericVariant.fromJson("[1,true,\"x\"]")));
            case 7:
                return GenericRow.of(80, GenericVariant.fromJson("true"),
                        GenericRow.of(GenericVariant.fromJson("3.5")));
            default:
                throw new AssertionError(kind);
        }
    }

    private static String describe(InternalRow row) {
        if (row == null) {
            return "NULL";
        }
        String payload = row.isNullAt(1) ? "SQL_NULL" : row.getVariant(1).toJson();
        String nested = row.isNullAt(2) ? "NULL" : (row.getRow(2, 1).isNullAt(0)
                ? "SQL_NULL" : row.getRow(2, 1).getVariant(0).toJson());
        return row.getInt(0) + "|" + payload + "|" + nested;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1 || Files.exists(Path.of(args[0]))) {
            throw new IllegalArgumentException("Expected a new local warehouse directory");
        }
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(
                new org.apache.paimon.fs.Path(Path.of(args[0]).toAbsolutePath().toUri())))) {
            catalog.createDatabase("paimon_test", false);
            var id = Identifier.create("paimon_test", "reader_struct_parent_null_v1");
            var nestedType = DataTypes.ROW(DataTypes.FIELD(0, "payload", DataTypes.VARIANT()));
            var detailType = DataTypes.ROW(DataTypes.FIELD(0, "marker", DataTypes.INT()),
                    DataTypes.FIELD(1, "payload", DataTypes.VARIANT()),
                    DataTypes.FIELD(2, "nested", nestedType));
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT().notNull())
                    .column("detail", detailType).option("file.format", "parquet")
                    .primaryKey("id").option("bucket", "1").option("write-only", "true").build(), false);
            var table = catalog.getTable(id);
            // Retain two overlapping L0 files so reads must use the Paimon SDK merge path.
            var initial = table.newBatchWriteBuilder();
            try (var write = initial.newWrite(); var commit = initial.newCommit()) {
                write.write(GenericRow.of(0, detail(7)));
                commit.commit(write.prepareCommit());
            }
            var writer = table.newBatchWriteBuilder();
            try (var write = writer.newWrite(); var commit = writer.newCommit()) {
                for (int i = 0; i < ROWS; i++) {
                    write.write(GenericRow.of(i, detail(i % 8)));
                }
                commit.commit(write.prepareCommit());
            }
            boolean[] seen = new boolean[ROWS];
            int count = 0;
            var read = table.newReadBuilder();
            var splits = read.newScan().plan().splits();
            for (var split : splits) {
                if (((DataSplit) split).convertToRawFiles().isPresent()) {
                    throw new AssertionError("Fixture must require Paimon SDK reads");
                }
            }
            try (var rows = new RecordReaderIterator<>(read.newRead().createReader(splits))) {
                while (rows.hasNext()) {
                    var row = rows.next();
                    int key = row.getInt(0);
                    if (key < 0 || key >= ROWS || seen[key] || !Objects.equals(describe(detail(key % 8)),
                            describe(row.isNullAt(1) ? null : row.getRow(1, 3)))) {
                        throw new AssertionError("Unexpected row: " + key);
                    }
                    seen[key] = true;
                    count++;
                }
            }
            if (count != ROWS) {
                throw new AssertionError("Unexpected row count: " + count);
            }
            System.out.println("Verified " + id + ": " + count + " rows, eight repeated NULL/value patterns");
        }
    }
}
