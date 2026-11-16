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

import com.starrocks.catalog.PaimonTable;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.RemoteMetaSplit;
import com.starrocks.connector.SerializedMetaSpec;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.thrift.TPaimonGlobalIndexFile;
import org.apache.paimon.Snapshot;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;

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

    private PaimonGlobalIndexPlanner() {
    }

    public static PaimonGlobalIndexRequest request(FileStoreTable table, long snapshotId, ScalarOperator predicate) {
        if (snapshotId < 0 || predicate == null || predicate.getColumnRefs().isEmpty() || !eligible(table.schema())
                || !"main".equals(org.apache.paimon.CoreOptions.branch(table.options()))) {
            return null;
        }
        Set<Integer> indexedFields = btreeFields(table, snapshotId);
        if (indexedFields.isEmpty()) {
            return null;
        }
        List<ScalarOperator> supported = new ArrayList<>();
        Map<Integer, Integer> bindings = new HashMap<>();
        for (ScalarOperator conjunct : Utils.extractConjuncts(predicate)) {
            Map<Integer, Integer> fields = new HashMap<>();
            boolean compatible = !conjunct.getColumnRefs().isEmpty();
            for (ColumnRefOperator column : conjunct.getColumnRefs()) {
                var field = table.rowType().getFields().stream().filter(f -> f.name().equals(column.getName()))
                        .findFirst().orElse(null);
                if (field == null || !indexedFields.contains(field.id()) || table.partitionKeys().contains(field.name())
                        || !supportedKey(field.type())) {
                    compatible = false;
                    break;
                }
                fields.put(column.getId(), field.id());
            }
            if (!compatible) {
                // A top-level AND conjunct may be omitted, but never prune only part of an OR.
                continue;
            }
            try {
                var candidate = PaimonGlobalIndexRequest.create(table.location().toString(), "main", snapshotId,
                        table.schema().id(), table.schema().timeMillis(), conjunct, fields);
                candidate.validatePredicate(table.schema());
                supported.add(conjunct);
                bindings.putAll(fields);
            } catch (IllegalArgumentException ignored) {
                // Unsupported expressions remain normal data-scan predicates.
            }
        }
        return supported.isEmpty() ? null : PaimonGlobalIndexRequest.create(table.location().toString(), "main", snapshotId,
                table.schema().id(), table.schema().timeMillis(), Utils.compoundAnd(supported), bindings);
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
