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
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergApiConverter;
import com.starrocks.connector.iceberg.IcebergDeleteSchema;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergTableMORParams;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.HintNode;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
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
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.starrocks.catalog.IcebergTable.DATA_SEQUENCE_NUMBER;
import static com.starrocks.catalog.IcebergTable.EQUALITY_DELETE_TABLE_COMMENT;
import static com.starrocks.catalog.IcebergTable.PARTITION_KEY;
import static com.starrocks.catalog.IcebergTable.SPEC_ID;

public class IcebergEqualityDeleteRewriteRule extends TransformationRule {
    private static final Set<String> EXTENDED_COLUMN_NAMES = Set.of(DATA_SEQUENCE_NUMBER, SPEC_ID, PARTITION_KEY);

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
        boolean hasPartitionEvolution = deleteSchemas.stream().map(IcebergDeleteSchema::specId).distinct().count() > 1;
        if (hasPartitionEvolution && !context.getSessionVariable().enableReadIcebergEqDeleteWithPartitionEvolution()) {
            throw new StarRocksConnectorException("Equality delete files aren't supported for tables with partition evolution." +
                    "You can execute `set enable_read_iceberg_equality_delete_with_partition_evolution = true` then rerun it");
        }
        if (hasPartitionEvolution && (table.schema().caseInsensitiveFindField(PARTITION_KEY) != null ||
                icebergTable.getReadSchema().caseInsensitiveFindField(PARTITION_KEY) != null)) {
            // without the partition key the anti join cannot keep a partitioned delete inside its own spec; a
            // time-travel read may still see such a column that the current schema has dropped
            throw new StarRocksConnectorException("Equality delete files that span partition specs aren't supported " +
                    "for a table with a column named " + PARTITION_KEY);
        }

        List<List<Integer>> allIds = deleteSchemas.stream()
                .map(IcebergDeleteSchema::equalityIds)
                .distinct()
                .toList();
        // a key whose partition match is done by its identity partition columns needs no $partition_key
        Map<List<Integer>, Set<String>> pruningColumnsByIds = allIds.stream()
                .collect(Collectors.toMap(ids -> ids,
                        ids -> runtimeFilterPruningColumns(context, scanOperator, icebergTable, deleteSchemas, ids)));
        Set<List<Integer>> idsNeedingPartitionMatch = allIds.stream()
                .filter(ids -> pruningColumnsByIds.get(ids).isEmpty() && needsPartitionMatch(table, deleteSchemas, ids))
                .collect(Collectors.toSet());

        List<IcebergMORParams> tableFullMorParams = Stream.concat(
                        Stream.of(IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE, IcebergMORParams.DATA_FILE_WITH_EQ_DELETE),
                        allIds.stream().map(ids -> IcebergMORParams.of(IcebergMORParams.ScanTaskType.EQ_DELETE, ids)))
                .collect(Collectors.toList());
        IcebergTableMORParams icebergTableFullMorParams = new IcebergTableMORParams(icebergTable.getId(), tableFullMorParams);

        scanOperator.setMORParam(IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE);
        scanOperator.setTableFullMORParams(icebergTableFullMorParams);
        scanOperator.setFromEqDeleteRewriteRule(true);

        ImmutableMap.Builder<ColumnRefOperator, ScalarOperator> projectForUnion = ImmutableMap.builder();
        Set<String> allPruningColumns = pruningColumnsByIds.values().stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
        LogicalIcebergScanOperator icebergDataFileWithDeleteScanOp = buildNewScanOperatorWithUnselectedAndExtendedField(
                deleteSchemas, allPruningColumns, scanOperator, columnRefFactory, projectForUnion, hasPartitionEvolution,
                !idsNeedingPartitionMatch.isEmpty());
        icebergDataFileWithDeleteScanOp.setMORParam(IcebergMORParams.DATA_FILE_WITH_EQ_DELETE);
        icebergDataFileWithDeleteScanOp.setTableFullMORParams(icebergTableFullMorParams);
        icebergDataFileWithDeleteScanOp.setFromEqDeleteRewriteRule(true);
        OptExpression optExpression = OptExpression.create(icebergDataFileWithDeleteScanOp);

        for (int i = 0; i < allIds.size(); i++) {
            List<Integer> equalityIds = allIds.get(i);
            String equalityDeleteTableName = buildEqualityDeleteTableName(icebergTable, equalityIds);
            List<String> columnNames = equalityIds.stream()
                    .map(id -> findEqualityColumnName(icebergTable, id))
                    .toList();
            List<Column> deleteColumns = columnNames.stream().map(icebergTable::getColumn).collect(Collectors.toList());
            // a pruning column outside the key gets a delete-side slot holding the partition value, and
            // buildOnPredicate matches it with `<=>` like a key column, in place of the $partition_key match
            Set<String> pruningColumns = pruningColumnsByIds.get(equalityIds);
            pruningColumns.stream()
                    .filter(name -> !columnNames.contains(name))
                    .sorted()
                    .forEach(name -> deleteColumns.add(icebergTable.getColumn(name)));
            IcebergTable equalityDeleteTable = new IcebergTable(
                    ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asLong(), equalityDeleteTableName,
                    icebergTable.getCatalogName(), icebergTable.getResourceName(), icebergTable.getCatalogDBName(),
                    icebergTable.getCatalogTableName(), EQUALITY_DELETE_TABLE_COMMENT, deleteColumns,
                    icebergTable.getNativeTable(), ImmutableMap.of());
            equalityDeleteTable.inheritReadView(icebergTable);

            ImmutableMap.Builder<ColumnRefOperator, Column> colRefToColumn = ImmutableMap.builder();
            ImmutableMap.Builder<Column, ColumnRefOperator> columnToColRef = ImmutableMap.builder();
            for (Column column : deleteColumns) {
                ColumnRefOperator columnRef = columnRefFactory.create(column.getName(), column.getType(), true);
                colRefToColumn.put(columnRef, column);
                columnToColRef.put(column, columnRef);
            }

            // the partition key already carries the spec id, and a $spec_id match would keep a global delete from
            // reaching data files of other specs
            boolean needsPartitionMatch = idsNeedingPartitionMatch.contains(equalityIds);
            boolean withGlobalDeletes = hasGlobalDeletes(table, deleteSchemas, equalityIds);
            fillExtendedColumns(columnRefFactory, colRefToColumn, columnToColRef,
                    hasPartitionEvolution && !needsPartitionMatch && !withGlobalDeletes, needsPartitionMatch,
                    icebergTable);
            LogicalIcebergEqualityDeleteScanOperator eqScanOp = new LogicalIcebergEqualityDeleteScanOperator(
                    equalityDeleteTable, colRefToColumn.build(), columnToColRef.build(), -1, null,
                    scanOperator.getTvrVersionRange());
            eqScanOp.setOriginPredicate(scanOperator.getPredicate());
            eqScanOp.setTableFullMORParams(icebergTableFullMorParams);
            eqScanOp.setMORParams(IcebergMORParams.of(IcebergMORParams.ScanTaskType.EQ_DELETE, equalityIds));
            eqScanOp.setRuntimeFilterPruningColumns(pruningColumns);

            ScalarOperator onPredicate = buildOnPredicate(
                    icebergDataFileWithDeleteScanOp.getColumnNameToColRefMap(), eqScanOp.getOutputColumns(),
                    withGlobalDeletes);

            LogicalJoinOperator.Builder builder = LogicalJoinOperator.builder()
                    .setJoinType(JoinOperator.LEFT_ANTI_JOIN)
                    .setJoinHint(HintNode.HINT_JOIN_BROADCAST)
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

    private ScalarOperator buildOnPredicate(Map<String, ColumnRefOperator> leftCols, List<ColumnRefOperator> rightCols,
                                            boolean withGlobalDeletes) {
        List<PredicateOperator> onPredicates = new ArrayList<>();
        for (ColumnRefOperator rightColRef : rightCols) {
            String icebergIdentifierColumnName = rightColRef.getName();
            ColumnRefOperator leftColRef = leftCols.get(icebergIdentifierColumnName);
            if (leftColRef == null) {
                throw new StarRocksConnectorException("can not find iceberg identifier column {}",
                        rightColRef.getName());
            }
            if (icebergIdentifierColumnName.equals(PARTITION_KEY)) {
                BinaryPredicateOperator samePartition = new BinaryPredicateOperator(BinaryType.EQ, leftColRef, rightColRef);
                // a NULL key on the delete side is a global delete, which applies to every partition; without
                // global deletes the plain equality stays a hash join key
                onPredicates.add(withGlobalDeletes ? new CompoundPredicateOperator(
                        CompoundPredicateOperator.CompoundType.OR, new IsNullPredicateOperator(rightColRef), samePartition)
                        : samePartition);
                continue;
            }
            // Use EQ_FOR_NULL (null-safe equals) for identity columns to match Iceberg spec:
            // "A null value in a delete column matches a row if the row's value is null"
            BinaryType binaryType = icebergIdentifierColumnName.equals(DATA_SEQUENCE_NUMBER)
                    ? BinaryType.LT
                    : BinaryType.EQ_FOR_NULL;
            BinaryPredicateOperator binaryPredicateOperator = new BinaryPredicateOperator(binaryType,
                    List.of(leftColRef, rightColRef));
            onPredicates.add(binaryPredicateOperator);
        }

        List<ScalarOperator> onOps = onPredicates.stream().map(PredicateOperator::clone).collect(Collectors.toList());
        return Utils.createCompound(CompoundPredicateOperator.CompoundType.AND, onOps);
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
            ColumnRefOperator newColRef = buildNewColumnRef(originalCol, columnRefFactory, table);
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
            Set<String> pruningColumns,
            LogicalIcebergScanOperator scanOperator,
            ColumnRefFactory columnRefFactory,
            ImmutableMap.Builder<ColumnRefOperator, ScalarOperator> projectForUnion,
            boolean hasPartitionEvolution,
            boolean withPartitionKey) {
        IcebergTable noDeleteTable = (IcebergTable) scanOperator.getTable();
        String tableName = noDeleteTable.getName() + "_" + "with_delete_file";
        IcebergTable withDeleteIcebergTable = new IcebergTable(
                ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asLong(), tableName,
                noDeleteTable.getCatalogName(), noDeleteTable.getResourceName(), noDeleteTable.getCatalogDBName(),
                noDeleteTable.getCatalogTableName(), "iceberg_table_with_delete", noDeleteTable.getFullSchema(),
                noDeleteTable.getNativeTable(), ImmutableMap.of());
        withDeleteIcebergTable.inheritReadView(noDeleteTable);

        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = scanOperator.getColRefToColumnMetaMap();
        Map<Column, ColumnRefOperator> columnMetaToColRefMap = scanOperator.getColumnMetaToColRefMap();

        // the pruning columns stand in for $partition_key in the anti join, so the data side reads them even when
        // the query does not
        Stream<String> keyColumnNames = deleteSchemas.stream()
                .map(IcebergDeleteSchema::equalityIds)
                .flatMap(List::stream)
                .map(fieldId -> findEqualityColumnName(noDeleteTable, fieldId));
        Map<ColumnRefOperator, Column> deleteColumns = Stream.concat(keyColumnNames, pruningColumns.stream())
                .distinct()
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

        fillExtendedColumns(columnRefFactory, newColRefToColBuilder, newColToColRefBuilder, hasPartitionEvolution,
                withPartitionKey, noDeleteTable);

        // build new table's predicate
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(originToNewCols);
        ScalarOperator newPredicate = rewriter.rewrite(scanOperator.getPredicate());

        // we shouldn't push down limit to scan node in this pattern.
        LogicalIcebergScanOperator newOp =  new LogicalIcebergScanOperator(withDeleteIcebergTable, newColRefToColBuilder.build(),
                newColToColRefBuilder.build(), -1, newPredicate, scanOperator.getTvrVersionRange());
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
                                     boolean withPartitionKey,
                                     IcebergTable icebergTable) {
        Column column = new Column(DATA_SEQUENCE_NUMBER, IntegerType.BIGINT, true);
        ColumnRefOperator columnRef = buildNewColumnRef(column, columnRefFactory, icebergTable);
        newColRefToColumnMetaMapBuilder.put(columnRef, column);
        newColumnMetaToColRefMapBuilder.put(column, columnRef);

        if (hasPartitionEvolution) {
            Column specIdcolumn = new Column(SPEC_ID, IntegerType.INT, true);
            ColumnRefOperator specIdColumnRef = buildNewColumnRef(specIdcolumn, columnRefFactory, icebergTable);
            newColRefToColumnMetaMapBuilder.put(specIdColumnRef, specIdcolumn);
            newColumnMetaToColRefMapBuilder.put(specIdcolumn, specIdColumnRef);
        }

        if (withPartitionKey) {
            Column partitionKeyColumn = new Column(PARTITION_KEY, StringType.STRING, true);
            ColumnRefOperator partitionKeyColumnRef = buildNewColumnRef(partitionKeyColumn, columnRefFactory, icebergTable);
            newColRefToColumnMetaMapBuilder.put(partitionKeyColumnRef, partitionKeyColumn);
            newColumnMetaToColRefMapBuilder.put(partitionKeyColumn, partitionKeyColumnRef);
        }
    }

    // A partitioned equality delete only applies to data files with the same spec and partition. The anti join
    // must check that unless a key match already implies it: the deletes share one spec, and every partition field
    // of it is an identity transform of an equality column (for example a Flink upsert table). Deletes of an
    // unpartitioned spec are global; next to deletes of another spec they also need the partition key, whose NULL
    // marks them as global.
    static boolean hasGlobalDeletes(Table table, Set<IcebergDeleteSchema> deleteSchemas, List<Integer> equalityIds) {
        return deleteSchemas.stream()
                .filter(deleteSchema -> deleteSchema.equalityIds().equals(equalityIds))
                .map(deleteSchema -> table.specs().get(deleteSchema.specId()))
                .anyMatch(spec -> spec == null || spec.isUnpartitioned());
    }

    private static Set<String> runtimeFilterPruningColumns(OptimizerContext context, LogicalIcebergScanOperator scan,
                                                           IcebergTable icebergTable, Set<IcebergDeleteSchema> deleteSchemas,
                                                           List<Integer> equalityIds) {
        if (!context.getSessionVariable().isEnableIcebergEqDeleteRuntimeFilterPruning()) {
            return Set.of();
        }
        // only the delete scan always takes these columns from partition metadata; with the switch off the data-file
        // scan reads them from the file, which may not store them (e.g. migrated Hive files), and a NULL never matches,
        // so match by $partition_key instead, unless a real column of that name leaves no partition match at all
        if (!context.getSessionVariable().getEnableIcebergIdentityColumnOptimize() &&
                icebergTable.getNativeTable().schema().caseInsensitiveFindField(PARTITION_KEY) == null) {
            return Set.of();
        }
        Set<String> scanColumnNames = scan.getColRefToColumnMetaMap().values().stream()
                .map(Column::getName)
                .collect(Collectors.toSet());
        // DELETE, UPDATE and MERGE read row locators; keep every delete for them
        if (scanColumnNames.contains(IcebergTable.FILE_PATH) || scanColumnNames.contains(IcebergTable.ROW_POSITION)) {
            return Set.of();
        }
        // the full schema follows the read schema, so it has every column a partition field resolves to
        Map<String, Column> tableColumns = icebergTable.getFullSchema().stream()
                .collect(Collectors.toMap(Column::getName, column -> column, (a, b) -> a));
        return runtimeFilterPruningColumns(icebergTable.getNativeTable(), icebergTable.getReadSchema(), deleteSchemas,
                equalityIds, tableColumns);
    }

    // The identity partition columns that replace the $partition_key match of a key, so a runtime filter on them
    // may skip the delete files of the partitions it rejects. The table must never have changed its spec and every
    // partition field must be an identity of a column: a data row and a delete then share a partition exactly when
    // `data.c <=> delete.c` holds for all of them, so the match needs no wider hash key. The columns are read even
    // when the query does not select them, as a fixed-width key is much cheaper than $partition_key. A filter that
    // rejects v on data.c may drop the deletes of partition v: the join that built it drops the data rows with
    // c = v anyway (JoinNode pushes such filters into the delete scan).
    static Set<String> runtimeFilterPruningColumns(Table table, Schema readSchema,
                                                   Set<IcebergDeleteSchema> deleteSchemas, List<Integer> equalityIds,
                                                   Map<String, Column> tableColumns) {
        if (table.specs().size() != 1 || table.spec().isUnpartitioned()) {
            return Set.of();
        }
        PartitionSpec spec = table.spec();
        boolean allOfThisSpec = deleteSchemas.stream()
                .filter(deleteSchema -> deleteSchema.equalityIds().equals(equalityIds))
                .allMatch(deleteSchema -> deleteSchema.specId() == spec.specId());
        if (!allOfThisSpec) {
            return Set.of();
        }
        Set<String> columns = new HashSet<>();
        for (PartitionField field : spec.fields()) {
            // the query reads with this schema (a snapshot's for time travel), like the equality columns
            String name = readSchema.findColumnName(field.sourceId());
            // the scan range finds an identity value by the partition field name, so it must be the column name;
            // a real column named like an extended column would be matched as that column by buildOnPredicate
            if (!field.transform().isIdentity() || name == null || !field.name().equals(name) ||
                    EXTENDED_COLUMN_NAMES.stream().anyMatch(name::equalsIgnoreCase) ||
                    !isRuntimeFilterPruningType(tableColumns.get(name))) {
                return Set.of();
            }
            columns.add(name);
        }
        return Set.copyOf(columns);
    }

    // types whose partition value reads back exactly as the data value; timestamps depend on the session time zone
    private static boolean isRuntimeFilterPruningType(Column column) {
        return column != null && (column.getType().isIntegerType() || column.getType().isDate() ||
                column.getType().isStringType());
    }

    static boolean needsPartitionMatch(Table table, Set<IcebergDeleteSchema> deleteSchemas, List<Integer> equalityIds) {
        if (table.schema().caseInsensitiveFindField(PARTITION_KEY) != null) {
            // a real column already has this name; keep the plan without the partition check
            return false;
        }
        long specCount = deleteSchemas.stream()
                .filter(deleteSchema -> deleteSchema.equalityIds().equals(equalityIds))
                .map(IcebergDeleteSchema::specId)
                .distinct()
                .count();
        if (specCount > 1) {
            return true;
        }
        for (IcebergDeleteSchema deleteSchema : deleteSchemas) {
            if (!deleteSchema.equalityIds().equals(equalityIds)) {
                continue;
            }
            PartitionSpec spec = table.specs().get(deleteSchema.specId());
            if (spec == null) {
                return true;
            }
            boolean impliedByKey = spec.fields().stream()
                    .allMatch(field -> field.transform().isIdentity() && equalityIds.contains(field.sourceId()));
            if (!impliedByKey) {
                return true;
            }
        }
        return false;
    }

    // Equality columns are resolved by field id under the schema the query reads with: the targeted snapshot's
    // schema for time travel, so a key renamed or dropped since then still resolves.
    private static String findEqualityColumnName(IcebergTable icebergTable, int fieldId) {
        String name = icebergTable.getReadSchema().findColumnName(fieldId);
        if (name == null) {
            throw new StarRocksConnectorException("Equality delete column with field id %s is not in the read schema " +
                    "of table %s", fieldId, icebergTable.getCatalogTableName());
        }
        return name;
    }

    // equality table name format : origin_table_name + "_eq_delete_" + [pk1_pk2_pk3_..._]
    private String buildEqualityDeleteTableName(IcebergTable icebergTable, List<Integer> equalityIds) {
        return icebergTable.getCatalogTableName() + "_eq_delete_" + equalityIds.stream()
                        .map(id -> findEqualityColumnName(icebergTable, id))
                        .collect(Collectors.joining("_"));
    }
}
