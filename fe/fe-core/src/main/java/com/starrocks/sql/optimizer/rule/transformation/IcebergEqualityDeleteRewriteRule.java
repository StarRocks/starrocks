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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.iceberg.IcebergApiConverter;
import com.starrocks.connector.iceberg.IcebergDeleteSchema;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergTableMORParams;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.HintNode;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergEqualityDeleteScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.PartitionSpec;
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

        Optional<Long> snapshotId = scanOperator.getTvrVersionRange().end();
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

        // One anti-join leg per distinct (equalityIds, partition scope). Scope mirrors Iceberg's
        // DeleteFileIndex routing: a delete file under an unpartitioned spec is GLOBAL (applies to
        // every data file), otherwise PARTITIONED (applies only within its own (specId, partition),
        // matched via the $partition_id key). This is what scopes partitioned equality deletes so they
        // don't delete same-key rows in other partitions, including across partition evolution.
        Map<Integer, PartitionSpec> specs = table.specs();
        List<IcebergMORParams> eqDeleteLegs = IcebergEqualityDeleteScanBuilder.splitIntoLegs(deleteSchemas, specs);
        boolean hasPartitionLeg = IcebergEqualityDeleteScanBuilder.hasPartitionedLeg(eqDeleteLegs);

        List<IcebergMORParams> tableFullMorParams = Stream.concat(
                        Stream.of(IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE, IcebergMORParams.DATA_FILE_WITH_EQ_DELETE),
                        eqDeleteLegs.stream())
                .collect(Collectors.toList());
        IcebergTableMORParams icebergTableFullMorParams = new IcebergTableMORParams(icebergTable.getId(), tableFullMorParams);

        scanOperator.setMORParam(IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE);
        scanOperator.setTableFullMORParams(icebergTableFullMorParams);
        scanOperator.setFromEqDeleteRewriteRule(true);

        ImmutableMap.Builder<ColumnRefOperator, ScalarOperator> projectForUnion = ImmutableMap.builder();
        LogicalIcebergScanOperator icebergDataFileWithDeleteScanOp = buildNewScanOperatorWithUnselectedAndExtendedField(
                deleteSchemas, scanOperator, columnRefFactory, projectForUnion, hasPartitionLeg);
        icebergDataFileWithDeleteScanOp.setMORParam(IcebergMORParams.DATA_FILE_WITH_EQ_DELETE);
        icebergDataFileWithDeleteScanOp.setTableFullMORParams(icebergTableFullMorParams);
        icebergDataFileWithDeleteScanOp.setFromEqDeleteRewriteRule(true);
        OptExpression optExpression = OptExpression.create(icebergDataFileWithDeleteScanOp);

        for (int i = 0; i < eqDeleteLegs.size(); i++) {
            IcebergMORParams leg = eqDeleteLegs.get(i);
            LogicalIcebergEqualityDeleteScanOperator eqScanOp =
                    IcebergEqualityDeleteScanBuilder.buildEqualityDeleteScanOperator(leg, icebergTable, columnRefFactory,
                            scanOperator.getPredicate(), icebergTableFullMorParams, scanOperator.getTvrVersionRange());

            ScalarOperator onPredicate = IcebergEqualityDeleteScanBuilder.buildOnPredicate(
                    icebergDataFileWithDeleteScanOp.getColumnNameToColRefMap(), eqScanOp.getOutputColumns());

            LogicalJoinOperator.Builder builder = LogicalJoinOperator.builder()
                    .setJoinType(JoinOperator.LEFT_ANTI_JOIN)
                    .setJoinHint(HintNode.HINT_JOIN_BROADCAST)
                    .setOnPredicate(onPredicate)
                    .setOriginalOnPredicate(onPredicate);

            // Limit may only sit on the topmost anti-join: an inner leg's limit would cap rows that a
            // higher leg can still delete, yielding a wrong limited result.
            if (i == eqDeleteLegs.size() - 1) {
                builder.setLimit(limit);
            }

            optExpression = OptExpression.create(builder.build(), optExpression, OptExpression.create(eqScanOp));
        }

        // build projection for correct union all
        LogicalProjectOperator logicalProjectOperator = new LogicalProjectOperator(projectForUnion.build(), limit);
        optExpression = OptExpression.create(logicalProjectOperator, optExpression);

        LogicalIcebergScanOperator withoutDeleteScanOperator = buildNewScanOperatorWithoutDelete(
                scanOperator, columnRefFactory);
        OptExpression optExpressionWithoutDelete = OptExpression.create(withoutDeleteScanOperator);

        // build union all
        List<List<ColumnRefOperator>> childOutputColumns = new ArrayList<>();
        childOutputColumns.add(withoutDeleteScanOperator.getOutputColumns());
        childOutputColumns.add(new ArrayList<>(logicalProjectOperator.getColumnRefMap().keySet()));

        LogicalUnionOperator unionOperator = LogicalUnionOperator.builder()
                .isUnionAll(true)
                .isFromIcebergEqualityDeleteRewrite(true)
                .setOutputColumnRefOp(scanOperator.getOutputColumns())
                .setChildOutputColumns(childOutputColumns)
                .setLimit(limit)
                .build();

        optExpression = OptExpression.create(unionOperator, optExpressionWithoutDelete, optExpression);
        return Collections.singletonList(optExpression);
    }

    private LogicalIcebergScanOperator buildNewScanOperatorWithoutDelete(
            LogicalIcebergScanOperator scanOperator,
            ColumnRefFactory columnRefFactory) {
        IcebergTable table = (IcebergTable) scanOperator.getTable();

        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = scanOperator.getColRefToColumnMetaMap();
        Map<Column, ColumnRefOperator> columnMetaToColRefMap = scanOperator.getColumnMetaToColRefMap();

        ImmutableMap.Builder<ColumnRefOperator, Column> newColRefToColBuilder =
                ImmutableMap.builder();
        ImmutableMap.Builder<Column, ColumnRefOperator> newColToColRefBuilder =
                ImmutableMap.builder();

        // fill ImmutableMap.Builder<Column, ColumnRefOperator> newColToColRefBuilder
        Map<ColumnRefOperator, ColumnRefOperator> originToNewCols = new HashMap<>();
        for (Map.Entry<Column, ColumnRefOperator> entry : columnMetaToColRefMap.entrySet()) {
            Column originalCol = entry.getKey();
            ColumnRefOperator originalColRef = entry.getValue();
            ColumnRefOperator newColRef = IcebergEqualityDeleteScanBuilder.buildColumnRef(originalCol, columnRefFactory, table);
            newColToColRefBuilder.put(originalCol, newColRef);
            originToNewCols.put(originalColRef, newColRef);
        }

        // fill newColRefToColBuilder and projectForUnion to guarantee column order.
        for (Map.Entry<ColumnRefOperator, Column> entry : colRefToColumnMetaMap.entrySet()) {
            ColumnRefOperator originRef = entry.getKey();
            Column originCol = entry.getValue();
            ColumnRefOperator newRef = originToNewCols.get(originRef);
            newColRefToColBuilder.put(newRef, originCol);
        }

        // build new table's predicate
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(originToNewCols);
        ScalarOperator newPredicate = rewriter.rewrite(scanOperator.getPredicate());

        LogicalIcebergScanOperator newOp =  new LogicalIcebergScanOperator(table, newColRefToColBuilder.build(),
                newColToColRefBuilder.build(), scanOperator.getLimit(), newPredicate, scanOperator.getTvrVersionRange());

        newOp.setMORParam(scanOperator.getMORParam());
        newOp.setTableFullMORParams(scanOperator.getTableFullMORParams());
        newOp.setFromEqDeleteRewriteRule(true);
        return newOp;
    }

    private LogicalIcebergScanOperator buildNewScanOperatorWithUnselectedAndExtendedField(
            Set<IcebergDeleteSchema> deleteSchemas,
            LogicalIcebergScanOperator scanOperator,
            ColumnRefFactory columnRefFactory,
            ImmutableMap.Builder<ColumnRefOperator, ScalarOperator> projectForUnion,
            boolean addPartitionId) {
        IcebergTable noDeleteTable = (IcebergTable) scanOperator.getTable();
        String tableName = noDeleteTable.getName() + "_" + "with_delete_file";
        IcebergTable withDeleteIcebergTable = new IcebergTable(
                ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asLong(), tableName,
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
            ColumnRefOperator newColRef =
                    IcebergEqualityDeleteScanBuilder.buildColumnRef(originalCol, columnRefFactory, withDeleteIcebergTable);
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

        IcebergEqualityDeleteScanBuilder.fillExtendedColumns(
                columnRefFactory, newColRefToColBuilder, newColToColRefBuilder, addPartitionId, noDeleteTable);

        // build new table's predicate
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(originToNewCols);
        ScalarOperator newPredicate = rewriter.rewrite(scanOperator.getPredicate());

        // we shouldn't push down limit to scan node in this pattern.
        LogicalIcebergScanOperator newOp =  new LogicalIcebergScanOperator(withDeleteIcebergTable, newColRefToColBuilder.build(),
                newColToColRefBuilder.build(), -1, newPredicate, scanOperator.getTvrVersionRange());
        newOp.setFromEqDeleteRewriteRule(true);
        return newOp;
    }
}
