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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexBuilder;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Builds a small frozen scalar-index fixture and verifies it using Paimon 2.0.0. */
public final class GenerateFullGlobalIndexFixture {
    private static final String SDK_VERSION = "2.0.0";
    private static final Identifier TABLE = Identifier.create("paimon_test", "global_index_full_v1");
    private static final long SNAPSHOT_ID = 5;
    private static final long SCHEMA_ID = 0;
    private static final List<String> INDEX_FIELDS = List.of("id", "tag");
    private static final List<Range> SHARDS = List.of(new Range(0, 2), new Range(3, 5), new Range(6, 8));
    private static final Comparator<Row> ROW_ORDER = Comparator.comparingInt(Row::id).thenComparing(Row::part);
    private static final List<Row> ROWS = List.of(
            new Row("p0", 0, "x", 10), new Row("p0", 2, "x", 30), new Row("p0", 12, "y", 20),
            new Row("p1", 4, "x", 40), new Row("p1", 6, "z", 60), new Row("p1", 14, "y", 50),
            new Row("p2", 8, "x", 70), new Row("p2", 14, "z", 90), new Row("p2", 16, "y", 80));

    private record Row(String part, int id, String tag, int amount) {
        GenericRow toPaimonRow() {
            return GenericRow.of(BinaryString.fromString(part), id, BinaryString.fromString(tag), amount);
        }
    }

    private GenerateFullGlobalIndexFixture() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2 || !Set.of("generate", "verify").contains(args[0])) {
            throw new IllegalArgumentException("Usage: GenerateFullGlobalIndexFixture generate|verify <warehouse>");
        }
        String sdkVersion = FileStoreTable.class.getPackage().getImplementationVersion();
        require(SDK_VERSION.equals(sdkVersion), "Expected Paimon " + SDK_VERSION + ", loaded " + sdkVersion);
        Path warehouse = Path.of(args[1]).toAbsolutePath();
        if (args[0].equals("generate")) {
            Files.createDirectory(warehouse);
            generate(warehouse);
        } else {
            require(Files.isDirectory(warehouse), "Fixture warehouse is missing: " + warehouse);
            try (Catalog catalog = open(warehouse)) {
                verify((FileStoreTable) catalog.getTable(TABLE));
            }
            System.out.printf("FIXTURE_MANIFEST\t%s\t%s\t%d\t%d%n",
                    TABLE.getTableName(), SDK_VERSION, SNAPSHOT_ID, SCHEMA_ID);
        }
    }

    private static Catalog open(Path warehouse) {
        return CatalogFactory.createCatalog(CatalogContext.create(new org.apache.paimon.fs.Path(warehouse.toUri())));
    }

    private static void generate(Path warehouse) throws Exception {
        try (Catalog catalog = open(warehouse)) {
            catalog.createDatabase(TABLE.getDatabaseName(), false);
            catalog.createTable(TABLE, Schema.newBuilder()
                    .column("part", DataTypes.STRING())
                    .column("id", DataTypes.INT().notNull())
                    .column("tag", DataTypes.STRING())
                    .column("amount", DataTypes.INT())
                    .partitionKeys("part")
                    .option(CoreOptions.BUCKET.key(), "-1")
                    .option(CoreOptions.FILE_FORMAT.key(), "parquet")
                    .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                    .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                    .option("global-index.enabled", "true")
                    .build(), false);

            for (String partition : List.of("p0", "p1", "p2")) {
                var batch = ((FileStoreTable) catalog.getTable(TABLE)).newBatchWriteBuilder();
                try (var writer = batch.newWrite(); var commit = batch.newCommit()) {
                    for (Row row : ROWS) {
                        if (row.part().equals(partition)) {
                            writer.write(row.toPaimonRow());
                        }
                    }
                    commit.commit(writer.prepareCommit());
                }
            }

            for (String field : INDEX_FIELDS) {
                var table = (FileStoreTable) catalog.getTable(TABLE);
                var builder = new SortedGlobalIndexBuilder(table, "btree").withIndexField(field);
                List<CommitMessage> messages = new ArrayList<>();
                Path spillDir = Files.createTempDirectory(warehouse.getParent(), "paimon-index-spill-");
                try (var spill = new IOManagerImpl(spillDir.toString())) {
                    for (var split : builder.scan().orElseThrow().getRight()) {
                        messages.addAll(builder.build(split, spill));
                    }
                } finally {
                    Files.deleteIfExists(spillDir);
                }
                try (var commit = table.newBatchWriteBuilder().newCommit()) {
                    commit.commit(messages);
                }
            }
        }
    }

    private static void verify(FileStoreTable table) throws Exception {
        var snapshot = table.snapshotManager().snapshot(SNAPSHOT_ID);
        require(table.latestSnapshot().orElseThrow().id() == SNAPSHOT_ID
                && snapshot.schemaId() == SCHEMA_ID && table.schema().id() == SCHEMA_ID,
                "Expected frozen snapshot 5 / schema 0");
        List<Range> dataRanges = new ArrayList<>();
        for (Split split : table.newSnapshotReader().withSnapshot(snapshot).withMode(ScanMode.ALL).read().splits()) {
            for (var file : ((DataSplit) split).dataFiles()) {
                dataRanges.add(file.nonNullRowIdRange());
            }
        }
        requireRanges(dataRanges, "data files");

        var handler = table.store().newIndexFileHandler();
        Map<Integer, List<Range>> byField = new HashMap<>();
        for (var entry : handler.scan(snapshot, ignored -> true)) {
            var file = entry.indexFile();
            var index = file.globalIndexMeta();
            require(index != null && "btree".equals(file.indexType()) && file.fileSize() > 0
                    && handler.existsIndexFile(entry), "Unexpected or missing index file");
            byField.computeIfAbsent(index.indexFieldId(), ignored -> new ArrayList<>()).add(index.rowRange());
        }
        var fieldIds = table.schema().fields().stream().filter(field -> INDEX_FIELDS.contains(field.name()))
                .map(field -> field.id()).collect(Collectors.toSet());
        require(fieldIds.size() == INDEX_FIELDS.size() && byField.keySet().equals(fieldIds),
                "Expected precisely the id and tag indexes");
        // Both fields have the exact data boundaries. Their intersection also has these three shards.
        byField.forEach((fieldId, ranges) -> requireRanges(ranges, "index field " + fieldId));

        FileStoreTable ordinary = table.copyWithoutTimeTravel(Map.of(
                CoreOptions.SCAN_SNAPSHOT_ID.key(), String.valueOf(SNAPSHOT_ID), "global-index.enabled", "false"));
        FileStoreTable indexed = table.copyWithoutTimeTravel(Map.of(
                CoreOptions.SCAN_SNAPSHOT_ID.key(), String.valueOf(SNAPSHOT_ID), "global-index.enabled", "true",
                CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), CoreOptions.GlobalIndexSearchMode.FULL.toString()));
        require(readRows(ordinary.newReadBuilder(), ordinary.newReadBuilder().newScan().plan().splits(), false)
                .equals(ROWS.stream().sorted(ROW_ORDER).toList()), "Frozen rows disagree with the fixture");

        var predicates = new PredicateBuilder(table.rowType());
        int id = table.rowType().getFieldIndex("id");
        int tag = table.rowType().getFieldIndex("tag");
        int amount = table.rowType().getFieldIndex("amount");
        Predicate in = predicates.in(id, List.of(2, 14));
        Predicate eq = predicates.equal(id, 6);
        verifyQuery(indexed, ordinary, in, in, row -> row.id() == 2 || row.id() == 14,
                row -> row.id() == 2 || row.id() == 14, false);
        verifyQuery(indexed, ordinary, eq, eq, row -> row.id() == 6, row -> row.id() == 6, true);
        Predicate range = PredicateBuilder.and(predicates.greaterOrEqual(id, 8), predicates.lessThan(id, 15));
        verifyQuery(indexed, ordinary, range, range, row -> row.id() >= 8 && row.id() < 15,
                row -> row.id() >= 8 && row.id() < 15, false);
        Predicate both = PredicateBuilder.and(in, predicates.equal(tag, BinaryString.fromString("y")));
        verifyQuery(indexed, ordinary, both, both, row -> row.id() == 14 && row.tag().equals("y"),
                row -> row.id() == 14 && row.tag().equals("y"), false);
        Predicate either = PredicateBuilder.or(predicates.equal(id, 2),
                predicates.equal(tag, BinaryString.fromString("z")));
        verifyQuery(indexed, ordinary, either, either, row -> row.id() == 2 || row.tag().equals("z"),
                row -> row.id() == 2 || row.tag().equals("z"), false);
        Predicate mixed = PredicateBuilder.or(predicates.equal(id, 2), predicates.equal(amount, 70));
        var mixedRead = ordinary.newReadBuilder().withFilter(mixed);
        require(readRows(mixedRead, mixedRead.newScan().plan().splits(), true)
                .equals(expected(row -> row.id() == 2 || row.amount() == 70)), "Unexpected mixed OR rows");
        verifyQuery(indexed, ordinary, in, PredicateBuilder.and(in, predicates.greaterOrEqual(amount, 50)),
                row -> row.id() == 2 || row.id() == 14, row -> row.id() == 14 && row.amount() >= 50, false);
        Predicate empty = predicates.equal(id, 5);
        verifyQuery(indexed, ordinary, empty, empty, row -> false, row -> false, false);
    }

    private static void verifyQuery(FileStoreTable indexed, FileStoreTable ordinary, Predicate indexPredicate,
                                    Predicate fullPredicate, java.util.function.Predicate<Row> candidate,
                                    java.util.function.Predicate<Row> expectedRow, boolean checkPartitions) throws Exception {
        // The local SDK scan evaluates the actual BTree files. Full coverage makes its ranges
        // the exact candidate row IDs, which we reattach to a fresh ordinary scan.
        var indexRead = indexed.newReadBuilder().withFilter(indexPredicate);
        var ranges = indexRead.newScan().plan().splits().stream().map(IndexedSplit.class::cast)
                .flatMap(split -> split.rowRanges().stream()).toList();
        var expectedCandidates = expected(candidate);
        require(ranges.stream().mapToLong(range -> range.to - range.from + 1).sum() == expectedCandidates.size(),
                "Unexpected BTree candidate cardinality for " + indexPredicate);
        var bitmap = GlobalIndexResult.fromRanges(ranges);
        var candidates = ordinary.newReadBuilder();
        var candidateSplits = ((InnerTableScan) candidates.newScan()).withGlobalIndexResult(bitmap).plan().splits();
        require(readRows(candidates, candidateSplits, false).equals(expectedCandidates),
                "Unexpected BTree candidate rows for " + indexPredicate);

        var read = ordinary.newReadBuilder().withFilter(fullPredicate);
        List<Split> ordinarySplits = read.newScan().plan().splits();
        List<Split> appliedSplits = ((InnerTableScan) read.newScan()).withGlobalIndexResult(bitmap).plan().splits();
        var expectedRows = expected(expectedRow);
        require(readRows(read, ordinarySplits, true).equals(expectedRows), "Unexpected ordinary rows for " + fullPredicate);
        require(readRows(read, appliedSplits, true).equals(expectedRows), "Unexpected indexed rows for " + fullPredicate);
        if (checkPartitions) {
            require(partitions(ordinarySplits).equals(Set.of("p0", "p1"))
                    && partitions(appliedSplits).equals(Set.of("p1")), "Expected id=6 to prune p0 after index application");
        }
    }

    private static List<Row> expected(java.util.function.Predicate<Row> filter) {
        return ROWS.stream().filter(filter).sorted(ROW_ORDER).toList();
    }

    private static List<Row> readRows(ReadBuilder builder, List<Split> splits, boolean executeFilter) throws Exception {
        var read = builder.newRead();
        if (executeFilter) {
            read = read.executeFilter();
        }
        List<Row> rows = new ArrayList<>();
        try (var iterator = new RecordReaderIterator<>(read.createReader(splits))) {
            while (iterator.hasNext()) {
                var row = iterator.next();
                rows.add(new Row(row.getString(0).toString(), row.getInt(1), row.getString(2).toString(), row.getInt(3)));
            }
        }
        rows.sort(ROW_ORDER);
        return rows;
    }

    private static Set<String> partitions(List<Split> splits) {
        return splits.stream().map(split -> split instanceof IndexedSplit indexed ? indexed.dataSplit() : (DataSplit) split)
                .map(split -> split.partition().getString(0).toString()).collect(Collectors.toSet());
    }

    private static void requireRanges(List<Range> actual, String description) {
        actual.sort(Comparator.comparingLong(range -> range.from));
        require(actual.equals(SHARDS), "Unexpected " + description + " ranges: " + actual);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
