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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Type;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergApiConverter;
import com.starrocks.connector.iceberg.IcebergRemoteFileDesc;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergEqualityDeleteScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.catalog.IcebergTable.DATA_SEQUENCE_NUMBER;
import static com.starrocks.catalog.IcebergTable.EQUALITY_DELETE_TABLE_COMMENT;
import static com.starrocks.catalog.IcebergTable.SPEC_ID;

/**
 * <p>This optimizer implements iceberg equality deletes as a join, rather than reading the data file as the left table and
 * the delete the file as the right table for local left anti join.
 * This approach significantly enhances performance for equality deletes.
 * This optimization replaces the previous solution of using local hash joiner in each scanner thread.
 * Compared to the previous solution, the main purpose is to reduce the overhead of repeatedly reading delete files and
 * repeatedly building hashtable since a iceberg equality delete file may be matched by many data files after iceberg planning.
 * This rule needs to strictly meet the check requirements before it can be rewritten.
 *
 * There are three rewriting patterns when meeting rewritten condition in this rule.
 *
 * The first case (common case):
 * <p>For example, consider the following query:
 * <code>SELECT * FROM table;</code>
 * With 1 delete schema: [k1], the query will be transformed into:
 * <pre>
 * SELECT *, $data_sequence_number FROM table left
 * LEFT ANTI JOIN table_eq_deletes_k1 t1 ON left.k1 = t1.k1 AND left.$data_sequence_number < t1.$data_sequence_number
 * </pre>
 *
 * The second case (query hit mutable delete schema):
 * <p>For example, consider the following query:
 * <code>SELECT * FROM table;</code>
 * With 2 delete schemas: [k1], [k2], the query will be transformed into:
 * <pre>
 * SELECT *, $data_sequence_number FROM table left
 * LEFT ANTI JOIN table_eq_deletes_k1 t1 ON left.k1 = t1.k1 AND left.$data_sequence_number < t1.$data_sequence_number
 * LEFT ANTI JOIN table_eq_deletes_K2 t2 ON left.k2 = t2.k2 AND left."$data_sequence_number" < d2."$data_sequence_number"
 * </pre>
 *
 * The third case (the same iceberg identifier column but with different partition layout or partition evolution):
 * <code>SELECT * FROM table;</code>
 * Partition Table With 1 delete schema: [k1, p1], Partition column: (p1). Write some records to this table.
 * Then alter table partition field (partition evolution): (p1 -> bucket(5, p1)). Write some records to this table.
 * the query will be transformed into:
 * <pre>
 * SELECT *, $data_sequence_number, $spec_id FROM table left
 * LEFT ANTI JOIN table_eq_deletes_k1_p1 t1 ON left.k1 = t1.k1 AND left.p1 = t1.p1 AND
 * left.$data_sequence_number < t1.$data_sequence_number AND left.$spec_id = t1.$spec_id.
 * </pre>
 */

public class IcebergEqualityDeleteRewriteRule extends TransformationRule {
    public IcebergEqualityDeleteRewriteRule() {
        super(RuleType.TF_ICEBERG_EQUALITY_REWRITE_RULE, Pattern.create(OperatorType.LOGICAL_ICEBERG_SCAN));
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        Operator op = input.getOp();
        if (!(op instanceof LogicalIcebergScanOperator)) {
            return false;
        }
        LogicalIcebergScanOperator scanOperator = op.cast();
        if (scanOperator.isFromEqDeleteRewriteRule()) {
            return false;
        }

        Optional<Long> snapshotId = scanOperator.getTableVersionRange().end();
        if (snapshotId.isEmpty()) {
            return false;
        }
        IcebergTable icebergTable = (IcebergTable) scanOperator.getTable();
        if (!icebergTable.isV2Format()) {
            return false;
        }

        Table table = icebergTable.getNativeTable();
        Snapshot snapshot = table.snapshot(snapshotId.get());
        if (snapshot == null) {
            return false;
        }

        boolean mayHaveEqualityDeletes = IcebergApiConverter.mayHaveEqualityDeletes(snapshot);
        if (!mayHaveEqualityDeletes) {
            return false;
        }

        // Use the same PredicateSearchKey to get iceberg scan tasks in the query level cache.
        GetRemoteFilesParams params =
                GetRemoteFilesParams.newBuilder().setTableVersionRange(scanOperator.getTableVersionRange())
                        .setPredicate(scanOperator.getPredicate()).build();
        List<RemoteFileInfo> splits = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFiles(icebergTable, params);
        if (splits.isEmpty()) {
            return false;
        }

        IcebergRemoteFileDesc remoteFileDesc = (IcebergRemoteFileDesc) splits.get(0).getFiles().get(0);
        if (remoteFileDesc == null) {
            return false;
        }

        for (FileScanTask fileScanTask : remoteFileDesc.getIcebergScanTasks()) {
            for (DeleteFile deleteFile : fileScanTask.deletes()) {
                if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalIcebergScanOperator scanOperator = input.getOp().cast();
        IcebergTable icebergTable = (IcebergTable) scanOperator.getTable();
        Table table = icebergTable.getNativeTable();
        Set<DeleteSchema> deleteSchemas = collectDeleteSchemas(scanOperator, icebergTable);
        if (deleteSchemas.isEmpty()) {
            scanOperator.setFromEqDeleteRewriteRule(true);
            return Collections.singletonList(input);
        }

        long limit = scanOperator.getLimit();
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
        boolean hasPartitionEvolution = deleteSchemas.stream().map(x -> x.specId).distinct().count() > 1;
        if (hasPartitionEvolution && !context.getSessionVariable().enableReadIcebergEqDeleteWithPartitionEvolution()) {
            throw new StarRocksConnectorException("Equality delete files aren't supported for tables with partition evolution." +
                    "You can execute `set enable_read_iceberg_equality_delete_with_partition_evolution = true` then rerun it");
        }

        LogicalIcebergScanOperator newScanOp = buildNewScanOperatorWithUnselectedAndExtendedField(
                deleteSchemas, scanOperator, columnRefFactory, hasPartitionEvolution);
        OptExpression optExpression = OptExpression.create(newScanOp);

        List<List<Integer>> allIds = deleteSchemas.stream().map(x -> x.equalityIds).distinct().collect(Collectors.toList());
        for (int i = 0; i < allIds.size(); i++) {
            List<Integer> equalityIds = allIds.get(i);
            String equalityDeleteTableName = buildEqualityDeleteTableName(icebergTable, equalityIds);
            List<String> columnNames = equalityIds.stream()
                    .map(id -> table.schema().findColumnName(id))
                    .collect(Collectors.toList());
            List<Column> deleteColumns = columnNames.stream().map(icebergTable::getColumn).collect(Collectors.toList());
            IcebergTable equalityDeleteTable = new IcebergTable(
                    ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(), equalityDeleteTableName,
                    icebergTable.getCatalogName(), icebergTable.getResourceName(), icebergTable.getRemoteDbName(),
                    icebergTable.getRemoteTableName(), EQUALITY_DELETE_TABLE_COMMENT, deleteColumns,
                    icebergTable.getNativeTable(), ImmutableMap.of());

            ImmutableMap.Builder<ColumnRefOperator, Column> colRefToColumn = ImmutableMap.builder();
            ImmutableMap.Builder<Column, ColumnRefOperator> columnToColRef = ImmutableMap.builder();
            for (Column column : deleteColumns) {
                ColumnRefOperator columnRef = columnRefFactory.create(column.getName(), column.getType(), true);
                colRefToColumn.put(columnRef, column);
                columnToColRef.put(column, columnRef);
            }
            fillExtendedColumns(columnRefFactory, colRefToColumn, columnToColRef, hasPartitionEvolution, icebergTable);
            LogicalIcebergEqualityDeleteScanOperator eqScanOp = new LogicalIcebergEqualityDeleteScanOperator(
                    equalityDeleteTable, colRefToColumn.build(), columnToColRef.build(), -1, null,
                    scanOperator.getTableVersionRange());
            eqScanOp.setOriginPredicate(newScanOp.getPredicate());
            eqScanOp.setEqualityIds(equalityIds);
            eqScanOp.setHitMutableIdentifierColumns(allIds.size() > 1);

            ScalarOperator onPredicate = buildOnPredicate(newScanOp.getColumnNameToColRefMap(), eqScanOp.getOutputColumns());

            LogicalJoinOperator.Builder builder = LogicalJoinOperator.builder()
                    .setJoinType(JoinOperator.LEFT_ANTI_JOIN)
                    .setOnPredicate(onPredicate)
                    .setOriginalOnPredicate(onPredicate);
            if (i == allIds.size() - 1) {
                builder.setLimit(limit);
            }

            optExpression = OptExpression.create(builder.build(), optExpression, OptExpression.create(eqScanOp));
        }

        return Collections.singletonList(optExpression);
    }

    private ScalarOperator buildOnPredicate(Map<String, ColumnRefOperator> leftCols, List<ColumnRefOperator> rightCols) {
        List<BinaryPredicateOperator> onPredicates = new ArrayList<>();
        for (ColumnRefOperator rightColRef : rightCols) {
            String icebergIdentifierColumnName = rightColRef.getName();
            ColumnRefOperator leftColRef = leftCols.get(icebergIdentifierColumnName);
            if (leftColRef == null) {
                throw new StarRocksConnectorException("can not find iceberg identifier column {}",
                        rightColRef.getName());
            }
            BinaryType binaryType = icebergIdentifierColumnName.equals(DATA_SEQUENCE_NUMBER) ? BinaryType.LT : BinaryType.EQ;
            BinaryPredicateOperator binaryPredicateOperator = new BinaryPredicateOperator(binaryType,
                    List.of(leftColRef, rightColRef));
            onPredicates.add(binaryPredicateOperator);
        }

        List<ScalarOperator> onOps = onPredicates.stream().map(PredicateOperator::clone).collect(Collectors.toList());
        return Utils.createCompound(CompoundPredicateOperator.CompoundType.AND, onOps);
    }

    private LogicalIcebergScanOperator buildNewScanOperatorWithUnselectedAndExtendedField(
            Set<DeleteSchema> deleteSchemas,
            LogicalIcebergScanOperator scanOperator,
            ColumnRefFactory columnRefFactory,
            boolean hasPartitionEvolution) {
        IcebergTable icebergTable = (IcebergTable) scanOperator.getTable();
        Table nativeTable = icebergTable.getNativeTable();
        List<Column> deleteColumns = deleteSchemas.stream()
                .map(schema -> schema.equalityIds)
                .flatMap(List::stream)
                .distinct()
                .map(fieldId -> nativeTable.schema().findColumnName(fieldId))
                .map(icebergTable::getColumn)
                .collect(Collectors.toList());

        ImmutableMap.Builder<ColumnRefOperator, Column> newColRefToColBuilder =
                ImmutableMap.builder();
        ImmutableMap.Builder<Column, ColumnRefOperator> newColToColRefBuilder =
                ImmutableMap.builder();

        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = scanOperator.getColRefToColumnMetaMap();
        Map<Column, ColumnRefOperator> columnMetaToColRefMap = scanOperator.getColumnMetaToColRefMap();
        newColRefToColBuilder.putAll(colRefToColumnMetaMap);
        newColToColRefBuilder.putAll(columnMetaToColRefMap);

        for (Column column : deleteColumns) {
            ColumnRefOperator columnRefOperator = columnMetaToColRefMap.get(column);
            if (!colRefToColumnMetaMap.containsKey(columnRefOperator)) {
                newColRefToColBuilder.put(columnRefOperator, column);
            }
        }

        fillExtendedColumns(columnRefFactory, newColRefToColBuilder, newColToColRefBuilder, hasPartitionEvolution, icebergTable);

        // we shouldn't push down limit to scan node in this pattern.
        LogicalIcebergScanOperator newOp =  new LogicalIcebergScanOperator(icebergTable, newColRefToColBuilder.build(),
                newColToColRefBuilder.build(), -1, scanOperator.getPredicate(), scanOperator.getTableVersionRange());
        newOp.setFromEqDeleteRewriteRule(true);
        return newOp;
    }

    private void fillExtendedColumns(ColumnRefFactory columnRefFactory,
                                     ImmutableMap.Builder<ColumnRefOperator, Column> newColRefToColumnMetaMapBuilder,
                                     ImmutableMap.Builder<Column, ColumnRefOperator> newColumnMetaToColRefMapBuilder,
                                     boolean hasPartitionEvolution,
                                     IcebergTable icebergTable) {
        int relationId = columnRefFactory.getNextRelationId();
        ColumnRefOperator columnRef = columnRefFactory.create(DATA_SEQUENCE_NUMBER, Type.BIGINT, true);
        Column column = new Column(DATA_SEQUENCE_NUMBER, Type.BIGINT, true);
        columnRefFactory.updateColumnToRelationIds(columnRef.getId(), relationId);
        columnRefFactory.updateColumnRefToColumns(columnRef, column, icebergTable);
        newColRefToColumnMetaMapBuilder.put(columnRef, column);
        newColumnMetaToColRefMapBuilder.put(column, columnRef);

        if (hasPartitionEvolution) {
            ColumnRefOperator specIdColumnRef = columnRefFactory.create(SPEC_ID, Type.INT, true);
            Column specIdcolumn = new Column(SPEC_ID, Type.INT, true);
            columnRefFactory.updateColumnToRelationIds(specIdColumnRef.getId(), relationId);
            columnRefFactory.updateColumnRefToColumns(specIdColumnRef, specIdcolumn, icebergTable);
            newColRefToColumnMetaMapBuilder.put(specIdColumnRef, specIdcolumn);
            newColumnMetaToColRefMapBuilder.put(specIdcolumn, specIdColumnRef);
        }
    }

    // equality table name format : origin_table_name + "_eq_delete_" + [pk1_pk2_pk3_..._]
    private String buildEqualityDeleteTableName(IcebergTable icebergTable, List<Integer> equalityIds) {
        return icebergTable.getRemoteTableName() + "_eq_delete_" + equalityIds.stream()
                        .map(id -> icebergTable.getNativeTable().schema().findColumnName(id))
                        .collect(Collectors.joining("_"));
    }

    private Set<DeleteSchema> collectDeleteSchemas(LogicalIcebergScanOperator scanOperator,
                                                   IcebergTable icebergTable) {
        Set<DeleteSchema> deleteSchemas = Sets.newHashSet();
        GetRemoteFilesParams params =
                GetRemoteFilesParams.newBuilder().setTableVersionRange(scanOperator.getTableVersionRange())
                        .setPredicate(scanOperator.getPredicate()).build();
        IcebergRemoteFileDesc remoteFileDesc = (IcebergRemoteFileDesc) GlobalStateMgr.getCurrentState()
                .getMetadataMgr().getRemoteFiles(icebergTable, params).get(0).getFiles().get(0);
        for (FileScanTask fileScanTask : remoteFileDesc.getIcebergScanTasks()) {
            for (DeleteFile deleteFile : fileScanTask.deletes()) {
                if (deleteFile.content() != FileContent.EQUALITY_DELETES) {
                    continue;
                }
                deleteSchemas.add(DeleteSchema.of(deleteFile.equalityFieldIds(), deleteFile.specId()));
            }
        }
        return deleteSchemas;
    }

    private static class DeleteSchema {
        private final List<Integer> equalityIds;
        private final Integer specId;

        public static DeleteSchema of(List<Integer> equalityIds, Integer specId) {
            return new DeleteSchema(equalityIds, specId);
        }

        public DeleteSchema(List<Integer> equalityIds, Integer specId) {
            this.equalityIds = equalityIds;
            this.specId = specId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DeleteSchema that = (DeleteSchema) o;
            return Objects.equals(equalityIds, that.equalityIds) && Objects.equals(specId, that.specId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(equalityIds, specId);
        }
    }
}
