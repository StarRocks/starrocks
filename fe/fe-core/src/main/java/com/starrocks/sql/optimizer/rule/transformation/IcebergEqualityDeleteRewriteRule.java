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
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Type;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergApiConverter;
import com.starrocks.connector.iceberg.IcebergDeleteSchema;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergTableMORParams;
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
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.starrocks.catalog.IcebergTable.DATA_SEQUENCE_NUMBER;
import static com.starrocks.catalog.IcebergTable.EQUALITY_DELETE_TABLE_COMMENT;
import static com.starrocks.catalog.IcebergTable.SPEC_ID;

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

        Set<DeleteFile> deleteFiles = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getDeleteFiles(icebergTable, snapshotId.get(), scanOperator.getPredicate(), FileContent.EQUALITY_DELETES);

        Set<IcebergDeleteSchema> deleteSchemas = deleteFiles.stream()
                .map(f -> IcebergDeleteSchema.of(f.equalityFieldIds(), f.specId()))
                .collect(Collectors.toSet());
        scanOperator.setDeleteSchemas(deleteSchemas);

        return !deleteSchemas.isEmpty();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalIcebergScanOperator scanOperator = input.getOp().cast();
        IcebergTable icebergTable = (IcebergTable) scanOperator.getTable();
        Table table = icebergTable.getNativeTable();
        Set<IcebergDeleteSchema> deleteSchemas = scanOperator.getDeleteSchemas();
        if (deleteSchemas.isEmpty()) {
            scanOperator.setFromEqDeleteRewriteRule(true);
            return Collections.singletonList(input);
        }

        long limit = scanOperator.getLimit();
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
        boolean hasPartitionEvolution = deleteSchemas.stream().map(IcebergDeleteSchema::specId).distinct().count() > 1;
        if (hasPartitionEvolution && !context.getSessionVariable().enableReadIcebergEqDeleteWithPartitionEvolution()) {
            throw new StarRocksConnectorException("Equality delete files aren't supported for tables with partition evolution." +
                    "You can execute `set enable_read_iceberg_equality_delete_with_partition_evolution = true` then rerun it");
        }

        List<List<Integer>> allIds = deleteSchemas.stream()
                .map(IcebergDeleteSchema::equalityIds)
                .distinct()
                .collect(Collectors.toList());

        List<IcebergMORParams> tableFullMorParams = Stream.concat(
                        Stream.of(IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE, IcebergMORParams.DATA_FILE_WITH_EQ_DELETE),
                        allIds.stream().map(ids -> IcebergMORParams.of(IcebergMORParams.ScanTaskType.EQ_DELETE, ids)))
                .collect(Collectors.toList());
        IcebergTableMORParams icebergTableFullMorParams = new IcebergTableMORParams(icebergTable.getId(), tableFullMorParams);

        scanOperator.setMORParam(IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE);
        scanOperator.setTableFullMORParams(icebergTableFullMorParams);
        scanOperator.setFromEqDeleteRewriteRule(true);
        OptExpression dataFileWithoutDeleteScanOp = OptExpression.create(scanOperator);

        ImmutableMap.Builder<ColumnRefOperator, ScalarOperator> projectForUnion = ImmutableMap.builder();
        LogicalIcebergScanOperator icebergDataFileWithDeleteScanOp = buildNewScanOperatorWithUnselectedAndExtendedField(
                deleteSchemas, scanOperator, columnRefFactory, projectForUnion, hasPartitionEvolution);
        icebergDataFileWithDeleteScanOp.setMORParam(IcebergMORParams.DATA_FILE_WITH_EQ_DELETE);
        icebergDataFileWithDeleteScanOp.setTableFullMORParams(icebergTableFullMorParams);
        icebergDataFileWithDeleteScanOp.setFromEqDeleteRewriteRule(true);
        OptExpression optExpression = OptExpression.create(icebergDataFileWithDeleteScanOp);

        for (int i = 0; i < allIds.size(); i++) {
            List<Integer> equalityIds = allIds.get(i);
            String equalityDeleteTableName = buildEqualityDeleteTableName(icebergTable, equalityIds);
            List<String> columnNames = equalityIds.stream()
                    .map(id -> table.schema().findColumnName(id))
                    .collect(Collectors.toList());
            List<Column> deleteColumns = columnNames.stream().map(icebergTable::getColumn).collect(Collectors.toList());
            IcebergTable equalityDeleteTable = new IcebergTable(
                    ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(), equalityDeleteTableName,
                    icebergTable.getCatalogName(), icebergTable.getResourceName(), icebergTable.getCatalogDBName(),
                    icebergTable.getCatalogTableName(), EQUALITY_DELETE_TABLE_COMMENT, deleteColumns,
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
            eqScanOp.setOriginPredicate(scanOperator.getPredicate());
            eqScanOp.setTableFullMORParams(icebergTableFullMorParams);
            eqScanOp.setMORParams(IcebergMORParams.of(IcebergMORParams.ScanTaskType.EQ_DELETE, equalityIds));

            ScalarOperator onPredicate = buildOnPredicate(
                    icebergDataFileWithDeleteScanOp.getColumnNameToColRefMap(), eqScanOp.getOutputColumns());

            LogicalJoinOperator.Builder builder = LogicalJoinOperator.builder()
                    .setJoinType(JoinOperator.LEFT_ANTI_JOIN)
                    .setJoinHint(JoinOperator.HINT_BROADCAST)
                    .setOnPredicate(onPredicate)
                    .setOriginalOnPredicate(onPredicate);

            if (i == allIds.size() - 1) {
                builder.setLimit(limit);
            }

            optExpression = OptExpression.create(builder.build(), optExpression, OptExpression.create(eqScanOp));
        }

        // build projection for correct union all
        LogicalProjectOperator logicalProjectOperator = new LogicalProjectOperator(projectForUnion.build(), limit);
        optExpression = OptExpression.create(logicalProjectOperator, optExpression);

        // build union all
        List<List<ColumnRefOperator>> childOutputColumns = new ArrayList<>();
        childOutputColumns.add(scanOperator.getOutputColumns());
        childOutputColumns.add(new ArrayList<>(logicalProjectOperator.getColumnRefMap().keySet()));

        LogicalUnionOperator unionOperator = LogicalUnionOperator.builder()
                .isUnionAll(true)
                .isFromIcebergEqualityDeleteRewrite(true)
                .setOutputColumnRefOp(scanOperator.getOutputColumns())
                .setChildOutputColumns(childOutputColumns)
                .setLimit(limit)
                .build();

        optExpression = OptExpression.create(unionOperator, dataFileWithoutDeleteScanOp, optExpression);
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
            Set<IcebergDeleteSchema> deleteSchemas,
            LogicalIcebergScanOperator scanOperator,
            ColumnRefFactory columnRefFactory,
            ImmutableMap.Builder<ColumnRefOperator, ScalarOperator> projectForUnion,
            boolean hasPartitionEvolution) {
        IcebergTable noDeleteTable = (IcebergTable) scanOperator.getTable();
        String tableName = noDeleteTable.getName() + "_" + "with_delete_file";
        IcebergTable withDeleteIcebergTable = new IcebergTable(
                ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(), tableName,
                noDeleteTable.getCatalogName(), noDeleteTable.getResourceName(), noDeleteTable.getCatalogDBName(),
                noDeleteTable.getCatalogTableName(), "iceberg_table_with_delete", noDeleteTable.getFullSchema(),
                noDeleteTable.getNativeTable(), ImmutableMap.of());
        Table nativeTable = noDeleteTable.getNativeTable();

        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = scanOperator.getColRefToColumnMetaMap();
        Map<Column, ColumnRefOperator> columnMetaToColRefMap = scanOperator.getColumnMetaToColRefMap();

        Map<ColumnRefOperator, Column> deleteColumns = deleteSchemas.stream()
                .map(IcebergDeleteSchema::equalityIds)
                .flatMap(List::stream)
                .distinct()
                .map(fieldId -> nativeTable.schema().findColumnName(fieldId))
                .map(withDeleteIcebergTable::getColumn)
                .collect(Collectors.toMap(columnMetaToColRefMap::get, column -> column));

        ImmutableMap.Builder<ColumnRefOperator, Column> newColRefToColBuilder =
                ImmutableMap.builder();
        ImmutableMap.Builder<Column, ColumnRefOperator> newColToColRefBuilder =
                ImmutableMap.builder();

        // fill ImmutableMap.Builder<Column, ColumnRefOperator> newColToColRefBuilder
        Map<ColumnRefOperator, ColumnRefOperator> originToNewCols = new HashMap<>();
        for (Map.Entry<Column, ColumnRefOperator> entry : columnMetaToColRefMap.entrySet()) {
            Column originalCol = entry.getKey();
            ColumnRefOperator originalColRef = entry.getValue();
            ColumnRefOperator newColRef = buildNewColumnRef(originalCol, columnRefFactory, withDeleteIcebergTable);
            newColToColRefBuilder.put(originalCol, newColRef);
            originToNewCols.put(originalColRef, newColRef);
        }

        // fill newColRefToColBuilder and projectForUnion to guarantee column order.
        for (Map.Entry<ColumnRefOperator, Column> entry : colRefToColumnMetaMap.entrySet()) {
            ColumnRefOperator originRef = entry.getKey();
            Column originCol = entry.getValue();
            ColumnRefOperator newRef = originToNewCols.get(originRef);
            projectForUnion.put(newRef, newRef);
            newColRefToColBuilder.put(newRef, originCol);
        }

        // fill unselected delete columns
        for (Map.Entry<ColumnRefOperator, Column> entry : deleteColumns.entrySet()) {
            ColumnRefOperator deleteColRef = entry.getKey();
            Column deleteCol = entry.getValue();
            if (!colRefToColumnMetaMap.containsKey(deleteColRef)) {
                newColRefToColBuilder.put(originToNewCols.get(deleteColRef), deleteCol);
            }
        }

        fillExtendedColumns(columnRefFactory, newColRefToColBuilder, newColToColRefBuilder, hasPartitionEvolution, noDeleteTable);

        // build new table's predicate
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(originToNewCols);
        ScalarOperator newPredicate = rewriter.rewrite(scanOperator.getPredicate());

        // we shouldn't push down limit to scan node in this pattern.
        LogicalIcebergScanOperator newOp =  new LogicalIcebergScanOperator(withDeleteIcebergTable, newColRefToColBuilder.build(),
                newColToColRefBuilder.build(), -1, newPredicate, scanOperator.getTableVersionRange());
        newOp.setFromEqDeleteRewriteRule(true);
        return newOp;
    }

    private ColumnRefOperator buildNewColumnRef(Column originalCol, ColumnRefFactory factory, IcebergTable table) {
        int relationId = factory.getNextRelationId();
        ColumnRefOperator newColumnRef = factory.create(originalCol.getName(), originalCol.getType(), true);
        factory.updateColumnToRelationIds(newColumnRef.getId(), relationId);
        factory.updateColumnRefToColumns(newColumnRef, originalCol, table);
        return newColumnRef;
    }

    private void fillExtendedColumns(ColumnRefFactory columnRefFactory,
                                     ImmutableMap.Builder<ColumnRefOperator, Column> newColRefToColumnMetaMapBuilder,
                                     ImmutableMap.Builder<Column, ColumnRefOperator> newColumnMetaToColRefMapBuilder,
                                     boolean hasPartitionEvolution,
                                     IcebergTable icebergTable) {
        Column column = new Column(DATA_SEQUENCE_NUMBER, Type.BIGINT, true);
        ColumnRefOperator columnRef = buildNewColumnRef(column, columnRefFactory, icebergTable);
        newColRefToColumnMetaMapBuilder.put(columnRef, column);
        newColumnMetaToColRefMapBuilder.put(column, columnRef);

        if (hasPartitionEvolution) {
            Column specIdcolumn = new Column(SPEC_ID, Type.INT, true);
            ColumnRefOperator specIdColumnRef = buildNewColumnRef(specIdcolumn, columnRefFactory, icebergTable);
            newColRefToColumnMetaMapBuilder.put(specIdColumnRef, specIdcolumn);
            newColumnMetaToColRefMapBuilder.put(specIdcolumn, specIdColumnRef);
        }
    }

    // equality table name format : origin_table_name + "_eq_delete_" + [pk1_pk2_pk3_..._]
    private String buildEqualityDeleteTableName(IcebergTable icebergTable, List<Integer> equalityIds) {
        return icebergTable.getCatalogTableName() + "_eq_delete_" + equalityIds.stream()
                        .map(id -> icebergTable.getNativeTable().schema().findColumnName(id))
                        .collect(Collectors.joining("_"));
    }
}
