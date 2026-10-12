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
import com.starrocks.connector.index.ConnectorIndexMetadata;
import com.starrocks.connector.index.ConnectorIndexType;
import com.starrocks.connector.index.IndexAnalyzer;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.connector.index.TopNIndexCondition;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.paimon.Snapshot;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.Range;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Exact filters only: a residual applied after ANN could remove all of the nearest candidates. */
public final class PaimonTopNFilter {
    private PaimonTopNFilter() {
    }

    public record Selection(ScalarOperator scalar, ScalarOperator partition,
                            Map<String, ConnectorIndexType> indexes) {
    }

    public static boolean supportsFilteredVector(PaimonTable table, String column) {
        if (!(table.getNativeTable() instanceof FileStoreTable nativeTable)) {
            return false;
        }
        if (!nativeTable.primaryKeys().isEmpty()
                || !Boolean.parseBoolean(nativeTable.options().getOrDefault("row-tracking.enabled", "false"))
                || !Boolean.parseBoolean(nativeTable.options().getOrDefault("data-evolution.enabled", "false"))) {
            return false;
        }
        DataType type = nativeTable.rowType().getField(column).type();
        return type instanceof ArrayType array && !array.isNullable() && !array.getElementType().isNullable();
    }

    static void validateFilteredSnapshot(PaimonTable table, long snapshotId, TopNIndexCondition condition) {
        if (!(table.getNativeTable() instanceof FileStoreTable nativeTable)) {
            throw new IllegalStateException("Paimon filtered ANN requires a FileStoreTable");
        }
        Snapshot snapshot = nativeTable.snapshot(snapshotId);
        if (snapshot == null || snapshot.schemaId() != nativeTable.schema().id()) {
            throw new IllegalStateException("Paimon filtered ANN schema does not match the pinned snapshot");
        }
        for (Map.Entry<String, ConnectorIndexType> index : condition.getRequiredIndexes().entrySet()) {
            if (index.getValue() == ConnectorIndexType.VECTOR && !supportsFilteredVector(table, index.getKey())) {
                throw new IllegalStateException("Paimon filtered ANN requires data evolution and non-null vectors");
            }
        }
    }

    public static Selection analyze(PaimonTable table, ScalarOperator predicate, IndexAnalyzer analyzer) {
        if (predicate == null) {
            return new Selection(null, null, Map.of());
        }
        if (table.getPartitionColumnNames() == null) {
            return null;
        }
        Map<String, Set<ConnectorIndexType>> partitionIndexes = new LinkedHashMap<>();
        table.getPartitionColumnNames().forEach(name -> partitionIndexes.put(name, Set.of(ConnectorIndexType.RANGE)));
        IndexAnalyzer partitionAnalyzer = new IndexAnalyzer(ConnectorIndexMetadata.of(partitionIndexes));
        List<ScalarOperator> scalar = new ArrayList<>();
        List<ScalarOperator> partition = new ArrayList<>();
        for (ScalarOperator conjunct : Utils.extractConjuncts(predicate)) {
            // Only these direct column/literal predicates have identical partition-row semantics.
            boolean simple = conjunct instanceof BinaryPredicateOperator || conjunct instanceof InPredicateOperator
                    || conjunct instanceof IsNullPredicateOperator;
            boolean direct = simple && conjunct.getChild(0) instanceof ColumnRefOperator
                    && conjunct.getChildren().stream().skip(1).allMatch(ConstantOperator.class::isInstance);
            if (direct && partitionAnalyzer.getIndexCondition(conjunct) != null) {
                partition.add(conjunct);
            } else if ((simple || conjunct instanceof CallOperator)
                    && !conjunct.getChildren().isEmpty()
                    && conjunct.getChild(0) instanceof ColumnRefOperator
                    && conjunct.getChildren().stream().skip(1).allMatch(ConstantOperator.class::isInstance)
                    && analyzer.getIndexCondition(conjunct) != null) {
                scalar.add(conjunct);
            } else {
                return null;
            }
        }
        IndexCondition scalarCondition = analyzer.getIndexCondition(Utils.compoundAnd(scalar));
        return new Selection(Utils.compoundAnd(scalar), Utils.compoundAnd(partition),
                scalarCondition == null ? Map.of() : scalarCondition.getRequiredIndexes());
    }

    static List<List<Long>> partitionRanges(PaimonTable table, long snapshotId, ScalarOperator predicate) {
        if (!(table.getNativeTable() instanceof FileStoreTable)) {
            throw new IllegalStateException("Paimon partition prefilter requires a FileStoreTable");
        }
        FileStoreTable nativeTable = (FileStoreTable) table.getNativeTable();
        Snapshot snapshot = nativeTable.snapshot(snapshotId);
        if (snapshot == null || snapshot.schemaId() != nativeTable.schema().id()) {
            throw new IllegalStateException("Paimon partition prefilter schema does not match the pinned snapshot");
        }
        PaimonPredicateConverter converter = new PaimonPredicateConverter(
                nativeTable.rowType().project(nativeTable.partitionKeys()));
        // Convert each conjunct separately: the general converter may return a partial AND.
        List<Predicate> predicates = new ArrayList<>();
        for (ScalarOperator conjunct : Utils.extractConjuncts(predicate)) {
            Predicate converted = converter.convert(conjunct);
            if (converted == null) {
                throw new IllegalStateException("Paimon partition prefilter is not fully convertible");
            }
            predicates.add(converted);
        }
        List<Range> ranges = new ArrayList<>();
        for (Split split : nativeTable.newSnapshotReader().withSnapshot(snapshot).withMode(ScanMode.ALL).read().splits()) {
            if (!(split instanceof DataSplit)) {
                throw new IllegalStateException("Paimon partition prefilter requires data splits");
            }
            DataSplit dataSplit = (DataSplit) split;
            if (!predicates.stream().allMatch(filter -> filter.test(dataSplit.partition()))) {
                continue;
            }
            for (DataFileMeta file : dataSplit.dataFiles()) {
                if (file.firstRowId() == null) {
                    throw new IllegalStateException("Paimon partition prefilter requires stable row IDs");
                }
                ranges.add(file.nonNullRowIdRange());
            }
        }
        return PaimonMetadata.mergeRanges(ranges).stream().map(range -> List.of(range.from, range.to)).toList();
    }
}
