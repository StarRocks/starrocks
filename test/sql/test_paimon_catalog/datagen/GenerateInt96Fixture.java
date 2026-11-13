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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataTypes;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

// Build a complete, relocatable Paimon fixture around the static legacy INT96 file.
// Args: new local warehouse directory, path to int96_signed_nanoseconds.parquet.
public class GenerateInt96Fixture {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Expected a new warehouse path and the INT96 Parquet fixture path");
        }
        Path warehouse = Path.of(args[0]);
        if (Files.exists(warehouse)) {
            throw new IllegalArgumentException("Use a new warehouse directory");
        }
        String name = "reader_int96_signed_nanos_v1";
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(
                new org.apache.paimon.fs.Path(warehouse.toUri())))) {
            catalog.createDatabase("paimon_test", false);
            var id = Identifier.create("paimon_test", name);
            catalog.createTable(id, Schema.newBuilder()
                    .column("before_epoch_us", DataTypes.TIMESTAMP(6))
                    .column("before_epoch_half_second", DataTypes.TIMESTAMP(6))
                    .column("after_epoch", DataTypes.TIMESTAMP(6))
                    .option("file.format", "parquet").option("bucket", "-1").build(), false);
            var table = catalog.getTable(id);
            String filename = "data-int96-signed-nanos.parquet";
            Path data = warehouse.resolve("paimon_test.db").resolve(name).resolve("bucket-0").resolve(filename);
            Files.createDirectories(data.getParent());
            Files.copy(Path.of(args[1]), data);
            var meta = DataFileMeta.forAppend(filename, Files.size(data), 5, SimpleStats.EMPTY_STATS,
                    0, 4, 0, List.of(), null, FileSource.APPEND, null, null, null, null);
            var message = new CommitMessageImpl(BinaryRow.EMPTY_ROW, 0, 1,
                    new DataIncrement(List.of(meta), List.of(), List.of()), CompactIncrement.emptyIncrement());
            try (var commit = table.newBatchWriteBuilder().newCommit()) {
                commit.commit(List.of(message));
            }
            var read = table.newReadBuilder();
            int rows = 0;
            int nulls = 0;
            try (var reader = new RecordReaderIterator<>(read.newRead().createReader(read.newScan().plan().splits()))) {
                while (reader.hasNext()) {
                    var row = reader.next();
                    rows++;
                    if (row.isNullAt(0)) {
                        if (!row.isNullAt(1) || !row.isNullAt(2)) {
                            throw new AssertionError("NULL mismatch");
                        }
                        nulls++;
                    } else {
                        String[] expected = {"1969-12-31T23:59:59.999999", "1969-12-31T23:59:59.500",
                                "1970-01-01T00:02:03.456789"};
                        for (int i = 0; i < expected.length; i++) {
                            var actual = row.getTimestamp(i, 6).toLocalDateTime().toString();
                            if (!expected[i].equals(actual)) {
                                throw new AssertionError(actual);
                            }
                        }
                    }
                }
            }
            if (rows != 5 || nulls != 1) {
                throw new AssertionError("Unexpected rows/NULLs");
            }
            System.out.println("Verified Paimon table: rows=" + rows + ", nulls=" + nulls);
        }
    }
}
