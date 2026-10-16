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

package com.starrocks.connector.index;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.connector.index.ConnectorIndexDescriptor.Operation;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

/** Normalizes SQL index operations and matches them against connector-declared capabilities. */
public final class IndexAnalyzer {
    private final ConnectorIndexMetadata metadata;
    private final Map<Integer, Integer> columnRefToFieldId;
    private final Set<Integer> partitionColumnRefs;

    public IndexAnalyzer(ConnectorIndexMetadata metadata) {
        this(metadata, Map.of(), Set.of());
    }

    public IndexAnalyzer(ConnectorIndexMetadata metadata, Map<Integer, Integer> columnRefToFieldId,
                         Set<Integer> partitionColumnRefs) {
        this.metadata = Objects.requireNonNull(metadata, "metadata is null");
        this.columnRefToFieldId = Map.copyOf(
                Objects.requireNonNull(columnRefToFieldId, "columnRefToFieldId is null"));
        this.partitionColumnRefs = Set.copyOf(
                Objects.requireNonNull(partitionColumnRefs, "partitionColumnRefs is null"));
    }

    /** Builds a stable ColumnRef-to-connector-field binding from the scan's current table schema. */
    public static IndexAnalyzer forScan(ConnectorIndexMetadata metadata, LogicalScanOperator scan) {
        Map<Integer, Integer> fieldIds = new HashMap<>();
        scan.getColRefToColumnMetaMap().forEach((columnRef, column) ->
                metadata.getCurrentFieldId(column.getName()).ifPresent(
                        fieldId -> fieldIds.put(columnRef.getId(), fieldId)));
        Set<Integer> partitionRefs = new HashSet<>();
        List<String> partitionNames = scan.getTable().getPartitionColumnNames();
        if (partitionNames != null && !partitionNames.isEmpty()) {
            scan.getColRefToColumnMetaMap().forEach((columnRef, column) -> {
                if (partitionNames.stream().anyMatch(name -> name.equalsIgnoreCase(column.getName()))) {
                    partitionRefs.add(columnRef.getId());
                }
            });
        }
        return new IndexAnalyzer(metadata, fieldIds, partitionRefs);
    }

    /** Returns whether a predicate has a supported syntactic shape without loading connector metadata. */
    public static boolean hasIndexablePredicateShape(ScalarOperator predicate) {
        return predicate != null && Utils.extractConjuncts(predicate).stream()
                .anyMatch(conjunct -> normalizeScalarPredicate(conjunct).isPresent());
    }

    /** Excludes partition pruning predicates before any remote index metadata is loaded. */
    public static boolean hasIndexablePredicateShape(LogicalScanOperator scan) {
        if (scan.getPredicate() == null) {
            return false;
        }
        IndexAnalyzer analyzer = forScan(ConnectorIndexMetadata.empty(), scan);
        return Utils.extractConjuncts(scan.getPredicate()).stream()
                .anyMatch(conjunct -> normalizeScalarPredicate(conjunct).isPresent()
                        && !analyzer.isPartitionPredicate(conjunct));
    }

    /** Splits a predicate into connector-indexable conjuncts and residual conjuncts. */
    public PredicateAnalysis analyzePredicate(ScalarOperator predicate) {
        if (predicate == null) {
            return PredicateAnalysis.EMPTY;
        }
        List<ScalarOperator> supported = new ArrayList<>();
        List<ScalarOperator> partition = new ArrayList<>();
        List<ScalarOperator> residual = new ArrayList<>();
        for (ScalarOperator conjunct : Utils.extractConjuncts(predicate)) {
            if (isPartitionPredicate(conjunct)) {
                partition.add(conjunct);
                continue;
            }
            Optional<ScalarPredicateShape> shape = normalizeScalarPredicate(conjunct);
            if (shape.isPresent() && supports(shape.get())) {
                supported.add(conjunct);
            } else {
                residual.add(conjunct);
            }
        }
        return new PredicateAnalysis(compoundAndOrNull(supported), compoundAndOrNull(partition),
                compoundAndOrNull(residual));
    }

    /** Returns supported conjuncts without removing them from the original scan predicate. */
    public ScalarOperator getIndexPredicate(ScalarOperator predicate) {
        return analyzePredicate(predicate).getIndexPredicate();
    }

    /** Binds normalized scalar requests to stable connector fields before plan copies rename columns. */
    public IndexCondition getIndexCondition(ScalarOperator predicate) {
        ScalarOperator indexPredicate = getIndexPredicate(predicate);
        if (indexPredicate == null) {
            return null;
        }
        Map<Integer, Integer> fieldIds = new HashMap<>();
        for (ColumnRefOperator column : indexPredicate.getColumnRefs()) {
            OptionalInt fieldId = resolveFieldId(column);
            if (fieldId.isEmpty()) {
                return null;
            }
            fieldIds.put(column.getId(), fieldId.getAsInt());
        }
        return new IndexCondition(indexPredicate, fieldIds);
    }

    private boolean supports(ScalarPredicateShape shape) {
        OptionalInt fieldId = resolveFieldId(shape.getColumn());
        return fieldId.isPresent() && metadata.getDescriptors(fieldId.getAsInt()).stream()
                .anyMatch(descriptor -> descriptor.supports(shape.getOperation()));
    }

    private boolean isPartitionPredicate(ScalarOperator predicate) {
        // Partition pruning only consumes the same simple scalar shapes accepted by the
        // connector index normalizer. A function over a partition column is still a residual
        // predicate even though every referenced column happens to be a partition column.
        if (normalizeScalarPredicate(predicate).isEmpty()) {
            return false;
        }
        List<ColumnRefOperator> columns = predicate.getColumnRefs();
        if (columns.isEmpty()) {
            return false;
        }
        for (ColumnRefOperator column : columns) {
            if (!partitionColumnRefs.isEmpty()) {
                if (!partitionColumnRefs.contains(column.getId())) {
                    return false;
                }
                continue;
            }
            OptionalInt fieldId = resolveFieldId(column);
            if (fieldId.isEmpty() || !metadata.isPartitionField(fieldId.getAsInt())) {
                return false;
            }
        }
        return true;
    }

    private OptionalInt resolveFieldId(ColumnRefOperator column) {
        Integer fieldId = columnRefToFieldId.get(column.getId());
        return fieldId == null ? metadata.getCurrentFieldId(column.getName()) : OptionalInt.of(fieldId);
    }

    private static Optional<ScalarPredicateShape> normalizeScalarPredicate(ScalarOperator predicate) {
        if (predicate instanceof BinaryPredicateOperator) {
            return normalizeBinaryPredicate((BinaryPredicateOperator) predicate);
        }
        if (predicate instanceof InPredicateOperator) {
            InPredicateOperator in = (InPredicateOperator) predicate;
            if (in.isSubquery() || !(in.getChild(0) instanceof ColumnRefOperator)
                    || !in.allValuesMatch(IndexAnalyzer::isNonNullLiteral)) {
                return Optional.empty();
            }
            Operation operation = in.isNotIn()
                    ? Operation.NOT_IN : Operation.IN;
            return Optional.of(new ScalarPredicateShape(
                    (ColumnRefOperator) in.getChild(0), operation));
        }
        if (predicate instanceof IsNullPredicateOperator) {
            IsNullPredicateOperator isNull = (IsNullPredicateOperator) predicate;
            if (!(isNull.getChild(0) instanceof ColumnRefOperator)) {
                return Optional.empty();
            }
            Operation operation = isNull.isNotNull()
                    ? Operation.IS_NOT_NULL : Operation.IS_NULL;
            return Optional.of(new ScalarPredicateShape(
                    (ColumnRefOperator) isNull.getChild(0), operation));
        }
        if (predicate instanceof CallOperator) {
            CallOperator call = (CallOperator) predicate;
            if (FunctionSet.STARTS_WITH.equalsIgnoreCase(call.getFnName())
                    && call.getChildren().size() == 2
                    && call.getChild(0) instanceof ColumnRefOperator
                    && isNonNullLiteral(call.getChild(1))) {
                return Optional.of(new ScalarPredicateShape(
                        (ColumnRefOperator) call.getChild(0), Operation.STARTS_WITH));
            }
        }
        return Optional.empty();
    }

    private static Optional<ScalarPredicateShape> normalizeBinaryPredicate(BinaryPredicateOperator predicate) {
        ScalarOperator first = predicate.getChild(0);
        ScalarOperator second = predicate.getChild(1);
        boolean reversed;
        ColumnRefOperator column;
        if (first instanceof ColumnRefOperator && isNonNullLiteral(second)) {
            column = (ColumnRefOperator) first;
            reversed = false;
        } else if (second instanceof ColumnRefOperator && isNonNullLiteral(first)) {
            column = (ColumnRefOperator) second;
            reversed = true;
        } else {
            return Optional.empty();
        }

        BinaryType type = predicate.getBinaryType();
        Operation operation = binaryOperation(reversed ? type.commutative() : type);
        return operation == null ? Optional.empty() : Optional.of(new ScalarPredicateShape(column, operation));
    }

    private static Operation binaryOperation(BinaryType type) {
        switch (type) {
            case EQ:
                return Operation.EQUAL;
            case NE:
                return Operation.NOT_EQUAL;
            case LT:
                return Operation.LESS_THAN;
            case LE:
                return Operation.LESS_THAN_OR_EQUAL;
            case GT:
                return Operation.GREATER_THAN;
            case GE:
                return Operation.GREATER_THAN_OR_EQUAL;
            default:
                return null;
        }
    }

    private static boolean isNonNullLiteral(ScalarOperator expression) {
        if (expression instanceof ConstantOperator) {
            return !((ConstantOperator) expression).isNull();
        }
        if (expression instanceof CastOperator) {
            return isNonNullLiteral(expression.getChild(0));
        }
        return expression instanceof ArrayOperator
                && !expression.getChildren().isEmpty()
                && expression.getChildren().stream().allMatch(IndexAnalyzer::isNonNullLiteral);
    }

    private static ScalarOperator compoundAndOrNull(List<ScalarOperator> conjuncts) {
        return conjuncts.isEmpty() ? null : Utils.compoundAnd(conjuncts);
    }

    private static final class ScalarPredicateShape {
        private final ColumnRefOperator column;
        private final Operation operation;

        private ScalarPredicateShape(ColumnRefOperator column, Operation operation) {
            this.column = column;
            this.operation = operation;
        }

        private ColumnRefOperator getColumn() {
            return column;
        }

        private Operation getOperation() {
            return operation;
        }
    }

    public static final class PredicateAnalysis {
        private static final PredicateAnalysis EMPTY = new PredicateAnalysis(null, null, null);

        private final ScalarOperator indexPredicate;
        private final ScalarOperator partitionPredicate;
        private final ScalarOperator residualPredicate;

        private PredicateAnalysis(ScalarOperator indexPredicate, ScalarOperator partitionPredicate,
                                  ScalarOperator residualPredicate) {
            this.indexPredicate = indexPredicate;
            this.partitionPredicate = partitionPredicate;
            this.residualPredicate = residualPredicate;
        }

        public ScalarOperator getIndexPredicate() {
            return indexPredicate;
        }

        public ScalarOperator getPartitionPredicate() {
            return partitionPredicate;
        }

        public ScalarOperator getResidualPredicate() {
            return residualPredicate;
        }

        public boolean hasResidual() {
            return residualPredicate != null;
        }
    }
}
