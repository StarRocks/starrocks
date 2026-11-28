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

package com.starrocks.connector.paimon;

import com.google.gson.JsonParser;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.RemoteMetaSplit;
import com.starrocks.connector.SerializedMetaSpec;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.thrift.TPaimonGlobalIndexFile;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

/** Snapshot-bound request and shard discovery, scoped entirely to the Paimon connector. */
public final class PaimonGlobalIndexPlanner {
    public static final int MAX_SHARDS = 4096;
    private static final int MAX_FILES = 65536;
    private static final int MAX_INDEX_META_BYTES = 64 * 1024;
    private static final String ANN_FIELD_TYPE = "array<float>";

    private PaimonGlobalIndexPlanner() {
    }

    private record Candidate(ScalarOperator predicate, Map<Integer, Integer> fieldIds) {
    }

    public static PaimonGlobalIndexRequest request(FileStoreTable table, long snapshotId, ScalarOperator predicate,
                                                  Map<Integer, String> columnNames) {
        // Fallback scans can read multiple branches and do not apply an external row-ID bitmap.
        if (table instanceof FallbackReadFileStoreTable || snapshotId < 0 || predicate == null
                || predicate.getColumnRefs().isEmpty() || !eligible(table.schema())
                || !"main".equals(org.apache.paimon.CoreOptions.branch(table.options()))) {
            return null;
        }
        TableSchema schema = table.schema();
        Map<String, DataField> schemaFields = schema.fields().stream()
                .collect(Collectors.toMap(DataField::name, field -> field));
        List<Candidate> candidates = new ArrayList<>();
        for (ScalarOperator conjunct : Utils.extractConjuncts(predicate)) {
            Map<Integer, DataField> fields = new HashMap<>();
            boolean compatible = !conjunct.getColumnRefs().isEmpty();
            for (ColumnRefOperator column : conjunct.getColumnRefs()) {
                // Resolve the scan's actual column, not an alias or a same-named column from another scan.
                DataField field = schemaFields.get(columnNames.get(column.getId()));
                if (field == null || schema.partitionKeys().contains(field.name()) || !supportedKey(field.type())) {
                    compatible = false;
                    break;
                }
                fields.put(column.getId(), field);
            }
            if (!compatible) {
                // A top-level AND conjunct may be omitted, but never prune only part of an OR.
                continue;
            }
            try {
                ScalarOperator bound = bindColumnNames(conjunct.clone(), fields);
                Map<Integer, Integer> fieldIds = fields.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().id()));
                var candidate = PaimonGlobalIndexRequest.create(table.location().toString(), "main", snapshotId,
                        schema.id(), schema.timeMillis(), bound, fieldIds);
                // Reuse the request validator so the local filter accepts exactly the supported predicate shapes.
                candidate.validatePredicate(schema);
                candidates.add(new Candidate(bound, fieldIds));
            } catch (IllegalArgumentException ignored) {
                // Unsupported expressions remain normal data-scan predicates.
            }
        }
        if (candidates.isEmpty()) {
            return null;
        }
        // Only access the snapshot and index manifest after finding a locally usable predicate.
        Set<Integer> indexedFields = btreeFields(table, snapshotId);
        List<ScalarOperator> supported = new ArrayList<>();
        Map<Integer, Integer> bindings = new HashMap<>();
        for (Candidate candidate : candidates) {
            if (indexedFields.containsAll(candidate.fieldIds().values())) {
                supported.add(candidate.predicate());
                bindings.putAll(candidate.fieldIds());
            }
        }
        return supported.isEmpty() ? null : PaimonGlobalIndexRequest.create(table.location().toString(), "main", snapshotId,
                schema.id(), schema.timeMillis(), Utils.compoundAnd(supported), bindings);
    }

    static PaimonGlobalIndexRequest annRequest(FileStoreTable table, long snapshotId, PaimonAnnQuery ann) {
        if (table instanceof FallbackReadFileStoreTable || snapshotId < 0 || !annEligible(table.schema())
                || !"main".equals(CoreOptions.branch(table.options()))) {
            return null;
        }
        TableSchema schema = table.schema();
        DataField field = schema.fields().stream().filter(f -> f.name().equals(ann.columnName())).findFirst().orElse(null);
        if (!PaimonGlobalIndexRequest.isAnnField(field)) {
            return null;
        }
        // As with scalar requests, a table with no matching index is not eligible, even in distributed mode.
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "PaimonGlobalIndex.DiscoverIndexes")) {
            Snapshot snapshot = table.snapshotManager().snapshot(snapshotId);
            boolean hasIndex = table.store().newIndexFileHandler().scan(snapshot, "lumina").stream()
                    .anyMatch(entry -> entry.indexFile().globalIndexMeta() != null
                            && entry.indexFile().globalIndexMeta().indexFieldId() == field.id());
            return hasIndex ? PaimonGlobalIndexRequest.createAnn(
                    table.location().toString(), "main", snapshotId, schema.id(), schema.timeMillis(),
                    field.id(), field.name(), ann.metric(), ann.queryVector(), ann.k()) : null;
        }
    }

    private static boolean annEligible(TableSchema schema) {
        return eligible(schema) && CoreOptions.fromMap(schema.options()).rowTrackingEnabled();
    }

    // Only the request's cloned predicate is renamed. The original predicate remains on the data scan.
    private static ScalarOperator bindColumnNames(ScalarOperator predicate, Map<Integer, DataField> fields) {
        if (predicate instanceof ColumnRefOperator column) {
            DataField field = fields.get(column.getId());
            if (field == null) {
                throw new IllegalArgumentException("Unbound Paimon index column");
            }
            return new ColumnRefOperator(column.getId(), column.getType(), field.name(), column.isNullable());
        }
        for (int i = 0; i < predicate.getChildren().size(); i++) {
            predicate.setChild(i, bindColumnNames(predicate.getChild(i), fields));
        }
        return predicate;
    }

    private static Set<Integer> btreeFields(FileStoreTable table, long snapshotId) {
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "PaimonGlobalIndex.DiscoverIndexes")) {
            Snapshot snapshot = table.snapshotManager().snapshot(snapshotId);
            Set<Integer> fields = new HashSet<>();
            Set<Integer> unsupported = new HashSet<>();
            for (IndexManifestEntry entry : table.store().newIndexFileHandler().scan(snapshot, unused -> true)) {
                GlobalIndexMeta index = entry.indexFile().globalIndexMeta();
                if (index != null) {
                    if ("btree".equals(entry.indexFile().indexType())) {
                        fields.add(index.indexFieldId());
                    } else {
                        unsupported.add(index.indexFieldId());
                    }
                }
            }
            fields.removeAll(unsupported);
            return fields;
        }
    }

    private static boolean eligible(TableSchema schema) {
        return schema.primaryKeys().isEmpty()
                && Boolean.parseBoolean(schema.options().getOrDefault("data-evolution.enabled", "false"));
    }

    private static boolean supportedKey(DataType type) {
        return switch (type.getTypeRoot()) {
            case BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, CHAR, VARCHAR -> true;
            default -> false;
        };
    }

    public static Metadata metadata(PaimonTable origin, PaimonGlobalIndexRequest request) {
        if (!(origin.getNativeTable() instanceof FileStoreTable table)
                || !request.getTablePath().equals(table.location().toString())
                || !"main".equals(org.apache.paimon.CoreOptions.branch(table.options()))
                || !request.matchesSchema(table.schema().id(), table.schema().timeMillis())) {
            throw new IllegalArgumentException("Paimon Global Index table, branch or schema changed");
        }
        if (request.isAnn()) {
            return annMetadata(table, request);
        }
        request.validatePredicate(table.schema());
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "PaimonGlobalIndex.ValidateCoverage")) {
            Snapshot snapshot = table.snapshotManager().snapshot(request.getSnapshotId());
            TableSchema schema = table.schemaManager().schema(snapshot.schemaId());
            Set<Integer> fields = request.getRequiredFieldIds();
            if (!eligible(schema) || !eligible(table.schema()) || !schema.idToFieldMap().keySet().containsAll(fields)
                    || !loadStableScalarFields(table, table.schema()).containsAll(fields)
                    || fields.stream().anyMatch(id -> !supportedKey(schema.idToFieldMap().get(id).type()))) {
                throw new IllegalArgumentException("Paimon Global Index has incompatible key schema");
            }
            Map<Integer, List<Range>> indexes = new HashMap<>();
            List<TPaimonGlobalIndexFile> files = new ArrayList<>();
            long fileMetadataBytes = 0;
            var handler = table.store().newIndexFileHandler();
            for (IndexManifestEntry entry : handler.scan(snapshot, unused -> true)) {
                GlobalIndexMeta index = entry.indexFile().globalIndexMeta();
                if (index == null || !fields.contains(index.indexFieldId())) {
                    continue;
                }
                if (!"btree".equals(entry.indexFile().indexType())) {
                    throw new IllegalArgumentException("Paimon Global Index has mixed providers");
                }
                indexes.computeIfAbsent(index.indexFieldId(), unused -> new ArrayList<>()).add(index.rowRange());
                byte[] metadata = index.indexMeta();
                if (files.size() >= MAX_FILES || metadata == null || metadata.length < 9
                        || metadata.length > MAX_INDEX_META_BYTES || entry.indexFile().fileSize() <= 0
                        || index.rowRangeStart() < 0 || index.rowRangeEnd() < index.rowRangeStart()
                        || index.rowRangeEnd() == Long.MAX_VALUE) {
                    throw new IllegalArgumentException("Invalid or oversized Paimon Global Index file metadata");
                }
                TPaimonGlobalIndexFile file = new TPaimonGlobalIndexFile();
                file.setField_id(index.indexFieldId());
                file.setField_type(keyType(table.schema().idToFieldMap().get(index.indexFieldId()).type()));
                file.setPath(handler.filePath(entry).toString());
                file.setFile_size(entry.indexFile().fileSize());
                file.setRow_range_from(index.rowRangeStart());
                file.setRow_range_to(index.rowRangeEnd());
                file.setMetadata(metadata);
                fileMetadataBytes = Math.addExact(fileMetadataBytes, metadata.length + 128L
                        + file.getPath().getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
                if (fileMetadataBytes > PaimonGlobalIndexRequest.resultBudget() / 4) {
                    throw new IllegalArgumentException("Paimon Global Index file metadata exceeds the planning budget");
                }
                files.add(file);
            }
            if (!indexes.keySet().containsAll(fields)) {
                throw new IllegalArgumentException("Paimon Global Index has no BTree index for a required field");
            }
            List<Range> dataRanges = new ArrayList<>();
            for (Split split : table.newSnapshotReader().withSnapshot(snapshot).withMode(ScanMode.ALL).read().splits()) {
                if (!(split instanceof DataSplit data)) {
                    throw new IllegalArgumentException("Unsupported Paimon data split");
                }
                for (DataFileMeta file : data.dataFiles()) {
                    if (file.firstRowId() == null) {
                        throw new IllegalArgumentException("Paimon data file has no stable row ID");
                    }
                    dataRanges.add(file.nonNullRowIdRange());
                }
            }
            List<Range> dataCoverage = mergeRanges(dataRanges);
            List<Range> shards = null;
            for (int field : fields) {
                List<Range> physical = mergeRanges(indexes.getOrDefault(field, List.of()), false);
                if (dataCoverage.isEmpty() || !covers(mergeRanges(physical), dataCoverage)) {
                    throw new IllegalArgumentException("Paimon Global Index does not completely cover the snapshot");
                }
                shards = shards == null ? physical : intersectRangesPreservingBoundaries(shards, physical);
            }
            if (shards == null || shards.isEmpty() || shards.size() > MAX_SHARDS) {
                throw new IllegalArgumentException("Unsupported Paimon Global Index shard count");
            }
            long requestBytes = request.toJson().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            long payloadBytes = 0;
            long payloadBudget = PaimonGlobalIndexRequest.resultBudget() / 4;
            List<RemoteMetaSplit> splits = new ArrayList<>();
            for (int id = 0; id < shards.size(); id++) {
                Range range = shards.get(id);
                if (range.from < 0 || range.to == Long.MAX_VALUE) {
                    throw new IllegalArgumentException("Invalid Paimon Global Index row ID range");
                }
                List<TPaimonGlobalIndexFile> selected = new ArrayList<>();
                long bytes = 0;
                long taskBytes = requestBytes + 128;
                for (TPaimonGlobalIndexFile file : files) {
                    if (file.getRow_range_from() <= range.to && file.getRow_range_to() >= range.from) {
                        selected.add(file);
                        bytes = Math.addExact(bytes, file.getFile_size());
                        // Include binary metadata, UTF-8 paths/types and conservative Thrift overhead.
                        taskBytes = Math.addExact(taskBytes, file.getMetadata().length + 128L
                                + file.getPath().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                                + file.getField_type().length());
                    }
                }
                payloadBytes = Math.addExact(payloadBytes, taskBytes);
                if (taskBytes > 1024 * 1024 || payloadBytes > payloadBudget) {
                    throw new IllegalArgumentException("Paimon Global Index task payload exceeds the planning budget");
                }
                splits.add(new Shard(id, range.from, range.to, bytes, request, List.copyOf(selected)));
            }
            return new Metadata(request, List.copyOf(splits));
        }
    }

    private record AnnPartition(BinaryRow value, int bucket) {
    }

    private static Metadata annMetadata(FileStoreTable table, PaimonGlobalIndexRequest request) {
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "PaimonGlobalIndex.AnnCoverage")) {
            Snapshot snapshot = table.snapshotManager().snapshot(request.getSnapshotId());
            TableSchema snapshotSchema = table.schemaManager().schema(snapshot.schemaId());
            if (table instanceof FallbackReadFileStoreTable || !annEligible(table.schema()) || !annEligible(snapshotSchema)) {
                throw new IllegalArgumentException("Paimon ANN requires a row-tracked append-only Data Evolution snapshot");
            }
            request.validateVector(table.schema());
            request.validateVector(snapshotSchema);
            return annShards(table, request, snapshot, snapshotSchema);
        }
    }

    private static Map<AnnPartition, List<Range>> visibleAnnRanges(
            FileStoreTable table, PaimonGlobalIndexRequest request, Snapshot snapshot, TableSchema snapshotSchema) {
        Map<AnnPartition, List<Range>> visible = new HashMap<>();
        List<Range> visibleRanges = new ArrayList<>();
        Set<Long> checkedSchemas = new HashSet<>(Set.of(snapshotSchema.id()));
        for (Split split : table.newSnapshotReader().withSnapshot(snapshot).withMode(ScanMode.ALL).read().splits()) {
            if (!(split instanceof DataSplit data) || data.isStreaming()
                    || data.deletionFiles().stream().flatMap(List::stream).anyMatch(file -> file != null)) {
                throw new IllegalArgumentException("Paimon ANN cannot prove row visibility for the data split");
            }
            var partition = new AnnPartition(data.partition(), data.bucket());
            for (DataFileMeta file : data.dataFiles()) {
                if (checkedSchemas.add(file.schemaId())) {
                    request.validateVector(table.schemaManager().schema(file.schemaId()));
                }
                if (file.firstRowId() == null || file.rowCount() <= 0 || file.writeCols() != null
                        && !file.writeCols().contains(request.getVector().fieldName())) {
                    throw new IllegalArgumentException("Paimon ANN data file does not provide the vector row IDs");
                }
                var range = new Range(file.firstRowId(), Math.addExact(file.firstRowId(), file.rowCount() - 1));
                visible.computeIfAbsent(partition, unused -> new ArrayList<>()).add(range);
                visibleRanges.add(range);
            }
        }
        requireDisjointAnnRanges(visibleRanges);
        return visible;
    }

    private static Metadata annShards(FileStoreTable table, PaimonGlobalIndexRequest request, Snapshot snapshot,
                                      TableSchema snapshotSchema) {
        var vector = request.getVector();
        var handler = table.store().newIndexFileHandler();
        Map<AnnPartition, List<Range>> rangesByPartition = new HashMap<>();
        List<TPaimonGlobalIndexFile> files = new ArrayList<>();
        List<Range> shardRanges = new ArrayList<>();
        Set<String> paths = new HashSet<>();
        long requestBytes = request.toJson().getBytes(StandardCharsets.UTF_8).length;
        long totalTaskBytes = 0;
        long totalResultBytes = 0;
        long transportBudget = PaimonGlobalIndexRequest.resultBudget() / 4;
        for (IndexManifestEntry entry : handler.scan(snapshot, vector.provider())) {
            GlobalIndexMeta index = entry.indexFile().globalIndexMeta();
            if (index == null || index.indexFieldId() != vector.fieldId()) {
                continue;
            }
            byte[] metadata = index.indexMeta();
            if (entry.kind() != FileKind.ADD || files.size() >= MAX_SHARDS || entry.indexFile().fileSize() <= 0
                    || index.rowRangeStart() < 0 || index.rowRangeEnd() < index.rowRangeStart()
                    || index.rowRangeEnd() == Long.MAX_VALUE || metadata == null || metadata.length == 0
                    || metadata.length > MAX_INDEX_META_BYTES) {
                throw new IllegalArgumentException("Invalid or oversized Paimon ANN index descriptor");
            }
            Range range = index.rowRange();
            if (entry.indexFile().rowCount() != range.count()) {
                throw new IllegalArgumentException("Paimon ANN index rows do not match its segment range");
            }
            validateAnnFileMetadata(metadata, vector);
            String path = handler.filePath(entry).toString();
            if (!paths.add(path)) {
                throw new IllegalArgumentException("Duplicate Paimon ANN index file");
            }
            long taskBytes = requestBytes + metadata.length + path.getBytes(StandardCharsets.UTF_8).length + 256;
            totalTaskBytes += taskBytes;
            totalResultBytes += Integer.BYTES + 12 * Math.min(vector.k(), range.count()) + 64;
            if (taskBytes > 1024 * 1024 || totalTaskBytes > transportBudget || totalResultBytes > transportBudget) {
                throw new IllegalArgumentException("Paimon ANN shard payload exceeds the planning budget");
            }
            rangesByPartition.computeIfAbsent(new AnnPartition(entry.partition(), entry.bucket()),
                    unused -> new ArrayList<>()).add(range);
            shardRanges.add(range);
            TPaimonGlobalIndexFile file = new TPaimonGlobalIndexFile();
            file.setField_id(vector.fieldId());
            file.setField_type(ANN_FIELD_TYPE);
            file.setPath(path);
            file.setFile_size(entry.indexFile().fileSize());
            file.setRow_range_from(range.from);
            file.setRow_range_to(range.to);
            file.setMetadata(metadata);
            files.add(file);
        }
        if (files.isEmpty()) {
            throw new IllegalArgumentException("Paimon ANN snapshot has no Lumina index for " + vector.fieldName());
        }
        requireDisjointAnnRanges(shardRanges);
        Map<AnnPartition, List<Range>> visible = visibleAnnRanges(table, request, snapshot, snapshotSchema);
        if (visible.isEmpty() || !visible.keySet().equals(rangesByPartition.keySet())
                || visible.entrySet().stream().anyMatch(entry ->
                        !mergeRanges(entry.getValue()).equals(mergeRanges(rangesByPartition.get(entry.getKey()))))) {
            // Every segment must be wholly visible. Filtering row IDs after local Top-K would lose
            // valid candidates if obsolete rows had already filled that segment's result.
            throw new IllegalArgumentException("Paimon ANN index ranges do not exactly cover the snapshot");
        }
        files.sort(Comparator.comparingLong(TPaimonGlobalIndexFile::getRow_range_from));
        List<RemoteMetaSplit> splits = new ArrayList<>(files.size());
        for (TPaimonGlobalIndexFile file : files) {
            splits.add(new Shard(splits.size(), file.getRow_range_from(), file.getRow_range_to(),
                    file.getFile_size(), request, List.of(file)));
        }
        return new Metadata(request, List.copyOf(splits));
    }

    private static void requireDisjointAnnRanges(List<Range> ranges) {
        ranges.sort(Comparator.comparingLong(range -> range.from));
        long previousEnd = -1;
        for (Range range : ranges) {
            if (range.from <= previousEnd || range.to < range.from || range.to == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Paimon ANN has overlapping or invalid row ID ranges");
            }
            previousEnd = range.to;
        }
    }

    private static void validateAnnFileMetadata(byte[] bytes, PaimonGlobalIndexRequest.VectorQuery vector) {
        var metadata = JsonParser.parseString(new String(bytes, StandardCharsets.UTF_8)).getAsJsonObject();
        if (metadata.entrySet().stream().anyMatch(e -> !e.getValue().isJsonPrimitive()
                    || !e.getValue().getAsJsonPrimitive().isString()
                    || e.getKey().startsWith("extension.build.tag.") || e.getKey().equals("extension.search_with_tag"))
                || !metadata.has("index.dimension") || !metadata.has("index.type")
                || !metadata.has("distance.metric") || !metadata.has("encoding.type")
                || !Integer.toString(vector.queryVector().size()).equals(metadata.get("index.dimension").getAsString())
                || !Set.of("bruteforce", "diskann").contains(metadata.get("index.type").getAsString())
                || !vector.metric().equals(metadata.get("distance.metric").getAsString())
                || !"rawf32".equals(metadata.get("encoding.type").getAsString())) {
            throw new IllegalArgumentException("Paimon ANN index metadata does not match the vector query");
        }
    }

    public record Metadata(PaimonGlobalIndexRequest request, List<RemoteMetaSplit> splits) implements SerializedMetaSpec {
        @Override
        public String getTable() {
            return request.getTablePath();
        }

        @Override
        public List<RemoteMetaSplit> getSplits() {
            return splits;
        }
    }

    public record Shard(int id, long from, long to, long bytes, PaimonGlobalIndexRequest request,
                        List<TPaimonGlobalIndexFile> files) implements RemoteMetaSplit {
        @Override
        public String getSerializeSplit() {
            return request.toJson();
        }

        @Override
        public long length() {
            return bytes;
        }

        @Override
        public String path() {
            return request.getTablePath() + "/#global-index/" + request.getSnapshotId() + "/" + id;
        }
    }

    static List<Range> mergeRanges(List<Range> ranges) {
        return mergeRanges(ranges, true);
    }

    static List<Range> mergeRanges(List<Range> ranges, boolean mergeAdjacent) {
        if (ranges.isEmpty()) {
            return Collections.emptyList();
        }
        List<Range> sorted = ranges.stream()
                .sorted(Comparator.comparingLong((Range range) -> range.from).thenComparingLong(range -> range.to))
                .collect(Collectors.toList());
        List<Range> merged = new ArrayList<>();
        long from = sorted.get(0).from;
        long to = sorted.get(0).to;
        for (int i = 1; i < sorted.size(); i++) {
            Range current = sorted.get(i);
            boolean adjacent = mergeAdjacent && to != Long.MAX_VALUE && current.from == to + 1;
            if (current.from <= to || adjacent) {
                to = Math.max(to, current.to);
            } else {
                merged.add(new Range(from, to));
                from = current.from;
                to = current.to;
            }
        }
        merged.add(new Range(from, to));
        return merged;
    }

    static List<Range> intersectRangesPreservingBoundaries(List<Range> left, List<Range> right) {
        List<Range> intersections = new ArrayList<>();
        int leftIndex = 0;
        int rightIndex = 0;
        while (leftIndex < left.size() && rightIndex < right.size()) {
            Range leftRange = left.get(leftIndex);
            Range rightRange = right.get(rightIndex);
            long from = Math.max(leftRange.from, rightRange.from);
            long to = Math.min(leftRange.to, rightRange.to);
            if (from <= to) {
                intersections.add(new Range(from, to));
            }
            if (leftRange.to <= rightRange.to) {
                leftIndex++;
            }
            if (rightRange.to <= leftRange.to) {
                rightIndex++;
            }
        }
        return intersections;
    }

    static boolean covers(List<Range> coverage, List<Range> required) {
        int coverageIndex = 0;
        for (Range target : required) {
            while (coverageIndex < coverage.size() && coverage.get(coverageIndex).to < target.from) {
                coverageIndex++;
            }
            if (coverageIndex >= coverage.size()) {
                return false;
            }
            Range candidate = coverage.get(coverageIndex);
            if (candidate.from > target.from || candidate.to < target.to) {
                return false;
            }
        }
        return true;
    }

    private static String keyType(DataType type) {
        return switch (type.getTypeRoot()) {
            case BOOLEAN -> "boolean";
            case TINYINT -> "tinyint";
            case SMALLINT -> "smallint";
            case INTEGER -> "int";
            case BIGINT -> "bigint";
            case CHAR, VARCHAR -> "string";
            default -> throw new IllegalArgumentException("Unsupported Paimon Global Index key type");
        };
    }

    private static Set<Integer> loadStableScalarFields(FileStoreTable table, TableSchema current) {
        Map<Integer, DataType> encodings = new HashMap<>();
        current.fields().forEach(field -> encodings.put(field.id(), scalarEncoding(field.type())));
        Set<Integer> stable = new HashSet<>(encodings.keySet());
        for (long id = current.id() - 1; id >= 0 && !encodings.isEmpty(); id--) {
            TableSchema previous = table.schemaManager().schema(id);
            if (previous == null || previous.id() != id) {
                throw new IllegalStateException("Missing Paimon schema history at " + id);
            }
            var fields = previous.idToFieldMap();
            encodings.entrySet().removeIf(entry -> {
                var field = fields.get(entry.getKey());
                if (field == null) {
                    return true; // Stable IDs are never reused: the field did not exist yet.
                }
                if (!entry.getValue().equals(scalarEncoding(field.type()))) {
                    stable.remove(entry.getKey());
                    return true;
                }
                return false;
            });
        }
        return Set.copyOf(stable);
    }

    private static DataType scalarEncoding(DataType type) {
        if (type.getTypeRoot() == DataTypeRoot.CHAR || type.getTypeRoot() == DataTypeRoot.VARCHAR) {
            return DataTypes.STRING();
        }
        return type.copy(true);
    }

}
