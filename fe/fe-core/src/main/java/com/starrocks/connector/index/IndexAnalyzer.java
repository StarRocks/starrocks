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
import com.starrocks.type.ArrayType;
import com.starrocks.type.FloatType;

import java.util.ArrayList;
import java.util.Arrays;
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

    /**
     * Normalizes a vector score expression without consulting connector metadata.
     * L2 is ascending; cosine similarity and inner product are descending.
     */
    public static Optional<VectorTopNShape> analyzeVectorTopNShape(
            ScalarOperator scoreExpression, boolean ascending) {
        ScalarOperator unwrapped = unwrapFloatingPointCast(scoreExpression);
        if (!(unwrapped instanceof CallOperator)) {
            return Optional.empty();
        }

        CallOperator call = (CallOperator) unwrapped;
        Optional<VectorIndexMetric> metric = metricForFunction(call.getFnName());
        if (metric.isEmpty() || call.getChildren().size() != 2 || !supportsDirection(metric.get(), ascending)) {
            return Optional.empty();
        }

        ScalarOperator first = call.getChild(0);
        ScalarOperator second = call.getChild(1);
        if (first instanceof ColumnRefOperator) {
            Optional<float[]> vector = extractFloatArrayLiteral(second);
            if (vector.isPresent()) {
                return Optional.of(new VectorTopNShape(
                        (ColumnRefOperator) first, vector.get(), metric.get(), ascending));
            }
        }
        if (second instanceof ColumnRefOperator) {
            Optional<float[]> vector = extractFloatArrayLiteral(first);
            if (vector.isPresent()) {
                return Optional.of(new VectorTopNShape(
                        (ColumnRefOperator) second, vector.get(), metric.get(), ascending));
            }
        }
        return Optional.empty();
    }

    /** Finds the exact vector index required by a normalized TopN expression. */
    public Optional<ConnectorIndexDescriptor> findVectorIndex(VectorTopNShape shape) {
        return findVectorIndex(shape, false);
    }

    /**
     * Finds an ANN descriptor for the requested null ordering.
     * ANN candidates omit null vectors, so a nullable vector cannot satisfy NULLS FIRST.
     * NULLS LAST remains a valid planning request; the later execution layer must either
     * replenish trailing null rows when fewer than {@code k} non-null rows exist or fall back.
     */
    public Optional<ConnectorIndexDescriptor> findVectorIndex(VectorTopNShape shape, boolean nullsFirst) {
        OptionalInt fieldId = resolveFieldId(shape.getColumn());
        if (fieldId.isEmpty()) {
            return Optional.empty();
        }
        return metadata.getDescriptors(fieldId.getAsInt()).stream()
                .filter(descriptor -> descriptor.getType() == ConnectorIndexType.VECTOR)
                .filter(descriptor -> descriptor.supports(ConnectorIndexOperation.VECTOR_TOP_N))
                .filter(descriptor -> descriptor.getVectorMetric().filter(shape.getMetric()::equals).isPresent())
                .filter(descriptor -> descriptor.getVectorDimension().orElse(-1) == shape.getQueryVector().length)
                .filter(descriptor -> !nullsFirst || !descriptor.isNullable())
                .findFirst();
    }

    public boolean supportsVectorTopN(ScalarOperator scoreExpression, boolean ascending) {
        return analyzeVectorTopNShape(scoreExpression, ascending)
                .flatMap(this::findVectorIndex)
                .isPresent();
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
            ConnectorIndexOperation operation = in.isNotIn()
                    ? ConnectorIndexOperation.NOT_IN : ConnectorIndexOperation.IN;
            return Optional.of(new ScalarPredicateShape(
                    (ColumnRefOperator) in.getChild(0), operation));
        }
        if (predicate instanceof IsNullPredicateOperator) {
            IsNullPredicateOperator isNull = (IsNullPredicateOperator) predicate;
            if (!(isNull.getChild(0) instanceof ColumnRefOperator)) {
                return Optional.empty();
            }
            ConnectorIndexOperation operation = isNull.isNotNull()
                    ? ConnectorIndexOperation.IS_NOT_NULL : ConnectorIndexOperation.IS_NULL;
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
                        (ColumnRefOperator) call.getChild(0), ConnectorIndexOperation.STARTS_WITH));
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

        ConnectorIndexOperation operation = binaryOperation(predicate.getBinaryType(), reversed);
        return operation == null ? Optional.empty() : Optional.of(new ScalarPredicateShape(column, operation));
    }

    private static ConnectorIndexOperation binaryOperation(BinaryType type, boolean reversed) {
        switch (type) {
            case EQ:
                return ConnectorIndexOperation.EQUAL;
            case NE:
                return ConnectorIndexOperation.NOT_EQUAL;
            case LT:
                return reversed ? ConnectorIndexOperation.GREATER_THAN : ConnectorIndexOperation.LESS_THAN;
            case LE:
                return reversed ? ConnectorIndexOperation.GREATER_THAN_OR_EQUAL
                        : ConnectorIndexOperation.LESS_THAN_OR_EQUAL;
            case GT:
                return reversed ? ConnectorIndexOperation.LESS_THAN : ConnectorIndexOperation.GREATER_THAN;
            case GE:
                return reversed ? ConnectorIndexOperation.LESS_THAN_OR_EQUAL
                        : ConnectorIndexOperation.GREATER_THAN_OR_EQUAL;
            default:
                return null;
        }
    }

    private static Optional<VectorIndexMetric> metricForFunction(String function) {
        if (FunctionSet.APPROX_L2_DISTANCE.equalsIgnoreCase(function)) {
            return Optional.of(VectorIndexMetric.L2);
        }
        if (FunctionSet.APPROX_COSINE_SIMILARITY.equalsIgnoreCase(function)) {
            return Optional.of(VectorIndexMetric.COSINE);
        }
        if (FunctionSet.APPROX_INNER_PRODUCT.equalsIgnoreCase(function)) {
            return Optional.of(VectorIndexMetric.INNER_PRODUCT);
        }
        return Optional.empty();
    }

    private static boolean supportsDirection(VectorIndexMetric metric, boolean ascending) {
        return metric == VectorIndexMetric.L2 ? ascending : !ascending;
    }

    private static ScalarOperator unwrapFloatingPointCast(ScalarOperator expression) {
        ScalarOperator current = expression;
        while (current instanceof CastOperator && current.getType().isFloatingPointType()) {
            current = current.getChild(0);
        }
        return current;
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

    private static Optional<float[]> extractFloatArrayLiteral(ScalarOperator expression) {
        ScalarOperator unwrapped = expression;
        while (unwrapped instanceof CastOperator) {
            unwrapped = unwrapped.getChild(0);
        }
        if (!(unwrapped instanceof ArrayOperator) || !(unwrapped.getType() instanceof ArrayType)
                || !((ArrayType) unwrapped.getType()).getItemType().isFloatingPointType()
                || unwrapped.getChildren().isEmpty()) {
            return Optional.empty();
        }

        float[] vector = new float[unwrapped.getChildren().size()];
        for (int i = 0; i < unwrapped.getChildren().size(); i++) {
            Optional<Float> value = extractFloatLiteral(unwrapped.getChild(i));
            if (value.isEmpty() || !Float.isFinite(value.get())) {
                return Optional.empty();
            }
            vector[i] = value.get();
        }
        return Optional.of(vector);
    }

    private static Optional<Float> extractFloatLiteral(ScalarOperator expression) {
        if (expression instanceof CastOperator) {
            return extractFloatLiteral(expression.getChild(0));
        }
        if (!(expression instanceof ConstantOperator)) {
            return Optional.empty();
        }
        ConstantOperator constant = (ConstantOperator) expression;
        if (constant.isNull()) {
            return Optional.empty();
        }
        if (constant.getType().equals(FloatType.FLOAT)) {
            return Optional.of((float) constant.getFloat());
        }
        if (constant.getType().equals(FloatType.DOUBLE)) {
            return Optional.of((float) constant.getDouble());
        }
        Optional<ConstantOperator> converted = constant.castTo(FloatType.FLOAT);
        if (converted.isEmpty() || converted.get().isNull()) {
            return Optional.empty();
        }
        return Optional.of((float) converted.get().getFloat());
    }

    private static ScalarOperator compoundAndOrNull(List<ScalarOperator> conjuncts) {
        return conjuncts.isEmpty() ? null : Utils.compoundAnd(conjuncts);
    }

    private static final class ScalarPredicateShape {
        private final ColumnRefOperator column;
        private final ConnectorIndexOperation operation;

        private ScalarPredicateShape(ColumnRefOperator column, ConnectorIndexOperation operation) {
            this.column = column;
            this.operation = operation;
        }

        private ColumnRefOperator getColumn() {
            return column;
        }

        private ConnectorIndexOperation getOperation() {
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

    public static final class VectorTopNShape {
        private final ColumnRefOperator column;
        private final float[] queryVector;
        private final VectorIndexMetric metric;
        private final boolean ascending;

        private VectorTopNShape(ColumnRefOperator column, float[] queryVector,
                                VectorIndexMetric metric, boolean ascending) {
            this.column = column;
            this.queryVector = queryVector.clone();
            this.metric = metric;
            this.ascending = ascending;
        }

        public String getColumnName() {
            return column.getName();
        }

        public ColumnRefOperator getColumn() {
            return column;
        }

        public float[] getQueryVector() {
            return queryVector.clone();
        }

        public VectorIndexMetric getMetric() {
            return metric;
        }

        public boolean isAscending() {
            return ascending;
        }

        @Override
        public String toString() {
            return "column=" + column + ", vector=" + Arrays.toString(queryVector)
                    + ", metric=" + metric + ", ascending=" + ascending;
        }
    }
}
