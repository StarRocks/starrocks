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

package com.starrocks.sql.optimizer.base;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.connector.ConnectorSinkShuffleMode;
import com.starrocks.connector.ConnectorSinkSortScope;
import com.starrocks.connector.iceberg.IcebergPartitionTransform;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.Type;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Iceberg {@link ConnectorSinkShuffleSpec}. Handles partition transforms (identity /
 * bucket / truncate / year / month / day / hour) by injecting a transform projection
 * column for non-identity cases, and uses the virtual column refs as the hash key.
 * Also supports host-level sort property derived from the table's native sort order.
 */
public class IcebergSinkShuffleSpec implements ConnectorSinkShuffleSpec {

    private final IcebergTable icebergTable;

    public IcebergSinkShuffleSpec(IcebergTable icebergTable) {
        this.icebergTable = icebergTable;
    }

    @Override
    public Table table() {
        return icebergTable;
    }

    @Override
    public ConnectorSinkShuffleMode effectiveShuffleMode(SessionVariable sv) {
        return sv.getIcebergConnectorSinkShuffleMode();
    }

    @Override
    public PreOptimizeResult prepare(InsertStmt insertStmt,
                                     List<ColumnRefOperator> outputColumns,
                                     ColumnRefFactory columnRefFactory,
                                     OptExpression root,
                                     ColumnRefSet requiredColumns) {
        Pair<List<Integer>, Map<ColumnRefOperator, ScalarOperator>> result =
                buildIcebergPartitionShuffleColumns(outputColumns, columnRefFactory);
        Map<ColumnRefOperator, ScalarOperator> transformProjection = result.second;

        if (transformProjection.isEmpty()) {
            // All-identity partitions: no plan rewrite needed
            return new PreOptimizeResult(root, requiredColumns, result.first);
        }

        // Inject transform projection columns into the logical plan and extend requiredColumns
        Map<ColumnRefOperator, ScalarOperator> projectionMap = new HashMap<>();
        for (ColumnRefOperator col : outputColumns) {
            projectionMap.put(col, col);
        }
        projectionMap.putAll(transformProjection);
        for (ColumnRefOperator transformCol : transformProjection.keySet()) {
            requiredColumns.union(transformCol.getId());
        }
        OptExpression newRoot = OptExpression.create(new LogicalProjectOperator(projectionMap), root);
        return new PreOptimizeResult(newRoot, requiredColumns, result.first);
    }

    @Override
    public SortProperty computeSortProperty(SessionVariable session, List<ColumnRefOperator> outputColumns) {
        ConnectorSinkSortScope sortScope = ConnectorSinkSortScope.fromName(session.getConnectorSinkSortScope());
        if (sortScope != ConnectorSinkSortScope.HOST) {
            return SortProperty.createProperty(java.util.Collections.emptyList());
        }

        org.apache.iceberg.Table nativeTable = icebergTable.getNativeTable();
        SortOrder sortOrder = nativeTable.sortOrder();
        List<com.starrocks.sql.optimizer.base.Ordering> sortOrderings = new ArrayList<>();
        if (sortOrder != null && sortOrder.isSorted()) {
            List<Integer> sortKeyIndexes = icebergTable.getSortKeyIndexes();
            for (int idx = 0; idx < sortKeyIndexes.size(); ++idx) {
                int sortKeyIndex = sortKeyIndexes.get(idx);
                SortField sortField = sortOrder.fields().get(idx);
                // Skip non-identity transforms
                if (!sortField.transform().isIdentity()) {
                    continue;
                }
                boolean isAsc = sortField.direction() == SortDirection.ASC;
                boolean isNullFirst = sortField.nullOrder() == NullOrder.NULLS_FIRST;
                sortOrderings.add(new com.starrocks.sql.optimizer.base.Ordering(
                        outputColumns.get(sortKeyIndex), isAsc, isNullFirst));
            }
        }
        return SortProperty.createProperty(sortOrderings);
    }

    /**
     * Compute partition column IDs, optionally adding transform projection columns.
     * Mirrors the logic previously in {@code InsertPlanner.buildIcebergPartitionShuffleColumns}.
     */
    private Pair<List<Integer>, Map<ColumnRefOperator, ScalarOperator>>
            buildIcebergPartitionShuffleColumns(List<ColumnRefOperator> outputColumns,
                                                ColumnRefFactory columnRefFactory) {
        List<Integer> partitionColumnIDs = new ArrayList<>();
        Map<ColumnRefOperator, ScalarOperator> transformProjection = new HashMap<>();

        org.apache.iceberg.Table nativeTable = icebergTable.getNativeTable();
        PartitionSpec spec = nativeTable.spec();

        // Unpartitioned / identity-only: fall back to source column indexes
        if (spec.isUnpartitioned()) {
            List<Integer> fallbackIDs = icebergTable.partitionColumnIndexes().stream()
                    .filter(x -> x >= 0 && x < outputColumns.size())
                    .map(x -> outputColumns.get(x).getId()).collect(Collectors.toList());
            return Pair.create(fallbackIDs, transformProjection);
        }
        boolean allIdentity = spec.fields().stream().allMatch(f -> f.transform().isIdentity());
        if (allIdentity) {
            List<Integer> fallbackIDs = icebergTable.partitionColumnIndexes().stream()
                    .filter(x -> x >= 0 && x < outputColumns.size())
                    .map(x -> outputColumns.get(x).getId()).collect(Collectors.toList());
            return Pair.create(fallbackIDs, transformProjection);
        }

        org.apache.iceberg.Schema icebergSchema = nativeTable.schema();
        List<Column> fullSchema = icebergTable.getFullSchema();

        for (PartitionField field : spec.fields()) {
            String sourceColumnName = icebergSchema.findColumnName(field.sourceId());
            boolean isTimestampWithZone =
                    icebergSchema.findType(field.sourceId()).equals(Types.TimestampType.withZone());
            int sourceColumnIndex = -1;
            for (int i = 0; i < fullSchema.size(); i++) {
                if (fullSchema.get(i).getName().equalsIgnoreCase(sourceColumnName)) {
                    sourceColumnIndex = i;
                    break;
                }
            }
            if (sourceColumnIndex < 0 || sourceColumnIndex >= outputColumns.size()) {
                continue;
            }

            ColumnRefOperator sourceRef = outputColumns.get(sourceColumnIndex);
            IcebergPartitionTransform transform =
                    IcebergPartitionTransform.fromString(field.transform().toString());

            switch (transform) {
                case IDENTITY:
                    partitionColumnIDs.add(sourceRef.getId());
                    break;
                case YEAR:
                case MONTH:
                case DAY:
                case HOUR: {
                    String funcName = FeConstants.ICEBERG_TRANSFORM_EXPRESSION_PREFIX +
                            (isTimestampWithZone ? "timestamptz_" : "") + transform.name().toLowerCase();
                    Type[] argTypes = new Type[] {sourceRef.getType()};
                    com.starrocks.catalog.Function fn = ExprUtils.getBuiltinFunction(funcName, argTypes,
                            com.starrocks.catalog.Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                    if (fn == null) {
                        partitionColumnIDs.add(sourceRef.getId());
                        break;
                    }
                    Type returnType = fn.getReturnType();
                    CallOperator callOp = new CallOperator(funcName, returnType, Lists.newArrayList(sourceRef), fn);
                    ColumnRefOperator transformRef = columnRefFactory.create(funcName, returnType, true);
                    transformProjection.put(transformRef, callOp);
                    partitionColumnIDs.add(transformRef.getId());
                    break;
                }
                case BUCKET: {
                    int numBuckets = extractTransformParam(field.transform().toString());
                    if (numBuckets <= 0) {
                        partitionColumnIDs.add(sourceRef.getId());
                        break;
                    }
                    String funcName = isTimestampWithZone
                            ? FeConstants.ICEBERG_TRANSFORM_EXPRESSION_PREFIX + "timestamptz_bucket"
                            : FunctionSet.ICEBERG_TRANSFORM_BUCKET;
                    Type[] argTypes = new Type[] {sourceRef.getType(), com.starrocks.type.IntegerType.INT};
                    com.starrocks.catalog.Function fn = ExprUtils.getBuiltinFunction(funcName, argTypes,
                            com.starrocks.catalog.Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                    if (fn == null) {
                        partitionColumnIDs.add(sourceRef.getId());
                        break;
                    }
                    Type returnType = fn.getReturnType();
                    CallOperator callOp = new CallOperator(funcName, returnType,
                            Lists.newArrayList(sourceRef, ConstantOperator.createInt(numBuckets)), fn);
                    ColumnRefOperator transformRef = columnRefFactory.create(funcName, returnType, true);
                    transformProjection.put(transformRef, callOp);
                    partitionColumnIDs.add(transformRef.getId());
                    break;
                }
                case TRUNCATE: {
                    int width = extractTransformParam(field.transform().toString());
                    if (width <= 0) {
                        partitionColumnIDs.add(sourceRef.getId());
                        break;
                    }
                    String funcName = FunctionSet.ICEBERG_TRANSFORM_TRUNCATE;
                    Type[] argTypes = new Type[] {sourceRef.getType(), com.starrocks.type.IntegerType.INT};
                    com.starrocks.catalog.Function fn = ExprUtils.getBuiltinFunction(funcName, argTypes,
                            com.starrocks.catalog.Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                    if (fn == null) {
                        partitionColumnIDs.add(sourceRef.getId());
                        break;
                    }
                    // Truncate's return type matches the source column type
                    // (fn.getReturnType() returns wildcard DECIMAL for decimal cols, wrong hash key).
                    Type returnType = sourceRef.getType();
                    CallOperator callOp = new CallOperator(funcName, returnType,
                            Lists.newArrayList(sourceRef, ConstantOperator.createInt(width)), fn);
                    ColumnRefOperator transformRef = columnRefFactory.create(funcName, returnType, true);
                    transformProjection.put(transformRef, callOp);
                    partitionColumnIDs.add(transformRef.getId());
                    break;
                }
                case UNKNOWN:
                default:
                    // Skip void/unknown transforms
                    break;
            }
        }
        return Pair.create(partitionColumnIDs, transformProjection);
    }

    private static int extractTransformParam(String transform) {
        int l = transform.indexOf('[');
        int r = transform.indexOf(']');
        if (l >= 0 && r > l) {
            try {
                return Integer.parseInt(transform.substring(l + 1, r));
            } catch (NumberFormatException ignore) {
                // fall through
            }
        }
        return 0;
    }
}
