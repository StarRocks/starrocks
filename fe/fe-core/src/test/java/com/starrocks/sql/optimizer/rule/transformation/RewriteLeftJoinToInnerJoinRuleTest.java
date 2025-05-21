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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.task.TaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class RewriteLeftJoinToInnerJoinRuleTest {

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.task.TaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class RewriteLeftJoinToInnerJoinRuleTest {

    private OptimizerContext optimizerContext;
    private ColumnRefFactory columnRefFactory;
    private static final long MOCK_DB_ID = 1L;
    private static final String MOCK_DB_NAME = "test_db";
    private Map<Long, OlapTable> mockTables = new HashMap<>();

    @BeforeEach
    public void setUp() {
        columnRefFactory = new ColumnRefFactory();
        optimizerContext = new OptimizerContext(new Memo(), columnRefFactory, new TaskContext(null, null, null));
        mockTables.clear(); // Clear for each test to ensure isolation
    }

    private OlapTable createMockTable(String name,
                                      List<Column> columns,
                                      List<ColumnId> pkColumnIds,
                                      List<ForeignKeyConstraint> fkConstraints,
                                      long tableId) {
        MaterializedIndex baseIndex = new MaterializedIndex(1, MaterializedIndex.IndexState.NORMAL);
        PartitionInfo partitionInfo = new PartitionInfo();
        List<String> pkColNames = pkColumnIds.stream()
                .map(cid -> columns.stream().filter(c -> c.getColumnId().equals(cid)).findFirst().get().getName())
                .collect(Collectors.toList());
        DistributionInfo distributionInfo = new HashDistributionInfo(10, pkColNames);

        OlapTable table = new OlapTable(tableId, name, columns, KeysType.PRIMARY_KEYS, partitionInfo, distributionInfo);
        table.setIndexMeta(1L, name, columns, 0, 0, (short) pkColumnIds.size(), KeysType.PRIMARY_KEYS, null);
        table.setBaseIndexId(baseIndex.getId());

        if (fkConstraints != null) {
            for (ForeignKeyConstraint fk : fkConstraints) {
                if (fk.getParentTableInfo() != null && fk.getParentTableInfo().getTableId() != 0) {
                    OlapTable parentTable = findMockTableById(fk.getParentTableInfo().getTableId());
                    if (parentTable != null) {
                        BaseTableInfo parentBaseTableInfo = new BaseTableInfo(MOCK_DB_ID, parentTable.getId(),
                                MOCK_DB_NAME, parentTable.getName());
                        fk.setParentTableInfo(parentBaseTableInfo);
                    }
                }
            }
            table.setForeignKeyConstraints(fkConstraints);
        }
        mockTables.put(tableId, table);
        return table;
    }

    private OlapTable findMockTableById(long tableId) {
        return mockTables.get(tableId);
    }

    private Column createColumn(String name, Type type, boolean isAllowNull, boolean isKey, String colId) {
        Column col = new Column(name, type, isKey);
        col.setIsAllowNull(isAllowNull);
        col.setColumnId(ColumnId.create(colId));
        return col;
    }

    private Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> createScanOperator(OlapTable table,
                                                                                             List<String> selectedColNames) {
        Map<ColumnRefOperator, Column> colRefToColumnMeta = new HashMap<>();
        Map<Column, ColumnRefOperator> columnMetaToColRef = new HashMap<>();
        List<ColumnRefOperator> outputVariables = new ArrayList<>();

        for (String colName : selectedColNames) {
            Column column = table.getColumn(colName);
            assert column != null;
            ColumnRefOperator colRef = columnRefFactory.create(column.getName(), column.getType(), column.isAllowNull());
            colRefToColumnMeta.put(colRef, column);
            columnMetaToColRef.put(column, colRef);
            outputVariables.add(colRef);
        }

        LogicalOlapScanOperator scanOp = LogicalOlapScanOperator.builder()
                .setTable(table)
                .setColRefToColumnMetaMap(colRefToColumnMeta)
                .setColumnMetaToColRefMap(columnMetaToColRef)
                .setOutputColumnRefOp(outputVariables)
                .setDistributionSpec(DistributionSpec.createAnyDistributionSpec())
                .setLimit(com.starrocks.sql.optimizer.operator.Operator.DEFAULT_LIMIT)
                .setPredicate(null)
                .setSelectedIndexId(table.getBaseIndexId())
                .setProjection(new Projection(Maps.newHashMap()))
                .build();
        return Pair.create(scanOp, columnMetaToColRef);
    }

    private LogicalJoinOperator createJoinOperator(OptExpression leftChild, OptExpression rightChild,
                                                   JoinOperator joinType, ScalarOperator onPredicate) {
        return LogicalJoinOperator.builder()
                .setJoinType(joinType)
                .setOnPredicate(onPredicate)
                .setProjection(null)
                .build();
    }

    private OptExpression applyRule(OptExpression joinExpression) {
        RewriteLeftJoinToInnerJoinRule rule = new RewriteLeftJoinToInnerJoinRule();
        List<OptExpression> result = rule.transform(joinExpression, optimizerContext);
        if (result.isEmpty()) {
            return joinExpression;
        }
        return result.get(0);
    }

    @Test
    public void testBasicTransformation() {
        long t1Id = System.nanoTime();
        long t2Id = System.nanoTime();

        Column t1Pk1 = createColumn("pk1", ScalarType.INT, false, true, "t1_c1");
        OlapTable t1 = createMockTable("t1", ImmutableList.of(t1Pk1), ImmutableList.of(t1Pk1.getColumnId()), null, t1Id);

        Column t2Fk2 = createColumn("fk2", ScalarType.INT, false, true, "t2_c1");
        Column t2Val = createColumn("val", ScalarType.VARCHAR, true, false, "t2_c2");

        ForeignKeyConstraint fk = new ForeignKeyConstraint(
                "fk_t2_t1",
                new BaseTableInfo(MOCK_DB_ID, t1.getId(), MOCK_DB_NAME, t1.getName()),
                ImmutableList.of(Pair.create(t2Fk2.getColumnId(), t1Pk1.getColumnId()))
        );
        OlapTable t2 = createMockTable("t2", ImmutableList.of(t2Fk2, t2Val), ImmutableList.of(t2Fk2.getColumnId()),
                ImmutableList.of(fk), t2Id);

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT1Info =
                createScanOperator(t1, ImmutableList.of("pk1"));
        LogicalOlapScanOperator scanT1 = scanT1Info.first;
        Map<Column, ColumnRefOperator> scanT1ColMap = scanT1Info.second;

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT2Info =
                createScanOperator(t2, ImmutableList.of("fk2", "val"));
        LogicalOlapScanOperator scanT2 = scanT2Info.first;
        Map<Column, ColumnRefOperator> scanT2ColMap = scanT2Info.second;

        ColumnRefOperator t1Pk1Ref = scanT1ColMap.get(t1Pk1);
        ColumnRefOperator t2Fk2Ref = scanT2ColMap.get(t2Fk2);

        ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryType.EQ, t2Fk2Ref, t1Pk1Ref);
        LogicalJoinOperator joinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.LEFT_OUTER_JOIN, onPredicate);
        OptExpression joinExpr = OptExpression.create(joinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));

        OptExpression transformedExpr = applyRule(joinExpr);
        assertTrue(transformedExpr.getOp() instanceof LogicalJoinOperator);
        assertEquals(JoinOperator.INNER_JOIN, ((LogicalJoinOperator) transformedExpr.getOp()).getJoinType());
    }

    @Test
    public void testFkColumnNullable() {
        long t1Id = System.nanoTime();
        long t2Id = System.nanoTime();

        Column t1Pk1 = createColumn("pk1", ScalarType.INT, false, true, "t1_c1");
        OlapTable t1 = createMockTable("t1", ImmutableList.of(t1Pk1), ImmutableList.of(t1Pk1.getColumnId()), null, t1Id);

        Column t2Fk2 = createColumn("fk2", ScalarType.INT, true, true, "t2_c1"); // ALLOW NULL
        Column t2Val = createColumn("val", ScalarType.VARCHAR, true, false, "t2_c2");
        ForeignKeyConstraint fk = new ForeignKeyConstraint(
                "fk_t2_t1",
                new BaseTableInfo(MOCK_DB_ID, t1.getId(), MOCK_DB_NAME, t1.getName()),
                ImmutableList.of(Pair.create(t2Fk2.getColumnId(), t1Pk1.getColumnId()))
        );
        OlapTable t2 = createMockTable("t2", ImmutableList.of(t2Fk2, t2Val), ImmutableList.of(t2Fk2.getColumnId()),
                ImmutableList.of(fk), t2Id);

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT1Info =
                createScanOperator(t1, ImmutableList.of("pk1"));
        LogicalOlapScanOperator scanT1 = scanT1Info.first;
        Map<Column, ColumnRefOperator> scanT1ColMap = scanT1Info.second;

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT2Info =
                createScanOperator(t2, ImmutableList.of("fk2", "val"));
        LogicalOlapScanOperator scanT2 = scanT2Info.first;
        Map<Column, ColumnRefOperator> scanT2ColMap = scanT2Info.second;

        ColumnRefOperator t1Pk1Ref = scanT1ColMap.get(t1Pk1);
        ColumnRefOperator t2Fk2Ref = scanT2ColMap.get(t2Fk2);
        ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryType.EQ, t2Fk2Ref, t1Pk1Ref);
        LogicalJoinOperator joinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.LEFT_OUTER_JOIN, onPredicate);
        OptExpression joinExpr = OptExpression.create(joinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));

        OptExpression transformedExpr = applyRule(joinExpr);
        assertEquals(JoinOperator.LEFT_OUTER_JOIN, ((LogicalJoinOperator) transformedExpr.getOp()).getJoinType());
    }

    @Test
    public void testNoForeignKeyConstraint() {
        long t1Id = System.nanoTime();
        long t2Id = System.nanoTime();

        Column t1Pk1 = createColumn("pk1", ScalarType.INT, false, true, "t1_c1");
        OlapTable t1 = createMockTable("t1", ImmutableList.of(t1Pk1), ImmutableList.of(t1Pk1.getColumnId()), null, t1Id);

        Column t2Fk2 = createColumn("fk2", ScalarType.INT, false, true, "t2_c1"); // NOT NULL
        Column t2Val = createColumn("val", ScalarType.VARCHAR, true, false, "t2_c2");
        OlapTable t2 = createMockTable("t2", ImmutableList.of(t2Fk2, t2Val), ImmutableList.of(t2Fk2.getColumnId()), null, t2Id);

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT1Info =
                createScanOperator(t1, ImmutableList.of("pk1"));
        LogicalOlapScanOperator scanT1 = scanT1Info.first;
        Map<Column, ColumnRefOperator> scanT1ColMap = scanT1Info.second;

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT2Info =
                createScanOperator(t2, ImmutableList.of("fk2", "val"));
        LogicalOlapScanOperator scanT2 = scanT2Info.first;
        Map<Column, ColumnRefOperator> scanT2ColMap = scanT2Info.second;

        ColumnRefOperator t1Pk1Ref = scanT1ColMap.get(t1Pk1);
        ColumnRefOperator t2Fk2Ref = scanT2ColMap.get(t2Fk2);
        ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryType.EQ, t2Fk2Ref, t1Pk1Ref);
        LogicalJoinOperator joinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.LEFT_OUTER_JOIN, onPredicate);
        OptExpression joinExpr = OptExpression.create(joinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));

        OptExpression transformedExpr = applyRule(joinExpr);
        assertEquals(JoinOperator.LEFT_OUTER_JOIN, ((LogicalJoinOperator) transformedExpr.getOp()).getJoinType());
    }

    @Test
    public void testJoinPredicateMismatch() {
        long t1Id = System.nanoTime();
        long t2Id = System.nanoTime();

        Column t1Pk1 = createColumn("pk1", ScalarType.INT, false, true, "t1_c1");
        OlapTable t1 = createMockTable("t1", ImmutableList.of(t1Pk1), ImmutableList.of(t1Pk1.getColumnId()), null, t1Id);

        Column t2Fk2 = createColumn("fk2", ScalarType.INT, false, true, "t2_c1");
        Column t2OtherCol = createColumn("other_col", ScalarType.INT, false, false, "t2_c2");
        ForeignKeyConstraint fk = new ForeignKeyConstraint(
                "fk_t2_t1",
                new BaseTableInfo(MOCK_DB_ID, t1.getId(), MOCK_DB_NAME, t1.getName()),
                ImmutableList.of(Pair.create(t2Fk2.getColumnId(), t1Pk1.getColumnId()))
        );
        OlapTable t2 = createMockTable("t2", ImmutableList.of(t2Fk2, t2OtherCol),
                ImmutableList.of(t2Fk2.getColumnId()), ImmutableList.of(fk), t2Id);

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT1Info =
                createScanOperator(t1, ImmutableList.of("pk1"));
        LogicalOlapScanOperator scanT1 = scanT1Info.first;
        Map<Column, ColumnRefOperator> scanT1ColMap = scanT1Info.second;

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT2Info =
                createScanOperator(t2, ImmutableList.of("fk2", "other_col"));
        LogicalOlapScanOperator scanT2 = scanT2Info.first;
        Map<Column, ColumnRefOperator> scanT2ColMap = scanT2Info.second;

        ColumnRefOperator t1Pk1Ref = scanT1ColMap.get(t1Pk1);
        ColumnRefOperator t2OtherColRef = scanT2ColMap.get(t2OtherCol);

        ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryType.EQ, t2OtherColRef, t1Pk1Ref);
        LogicalJoinOperator joinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.LEFT_OUTER_JOIN, onPredicate);
        OptExpression joinExpr = OptExpression.create(joinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));

        OptExpression transformedExpr = applyRule(joinExpr);
        assertEquals(JoinOperator.LEFT_OUTER_JOIN, ((LogicalJoinOperator) transformedExpr.getOp()).getJoinType());
    }

    @Test
    public void testCompositeFkIncompletePredicate() {
        long t1Id = System.nanoTime();
        long t2Id = System.nanoTime();

        Column t1Pk1a = createColumn("pk1a", ScalarType.INT, false, true, "t1_c1");
        Column t1Pk1b = createColumn("pk1b", ScalarType.INT, false, true, "t1_c2");
        OlapTable t1 = createMockTable("t1", ImmutableList.of(t1Pk1a, t1Pk1b),
                ImmutableList.of(t1Pk1a.getColumnId(), t1Pk1b.getColumnId()), null, t1Id);

        Column t2Fk2a = createColumn("fk2a", ScalarType.INT, false, true, "t2_c1");
        Column t2Fk2b = createColumn("fk2b", ScalarType.INT, false, true, "t2_c2");
        ForeignKeyConstraint fk = new ForeignKeyConstraint(
                "fk_t2_t1_comp",
                new BaseTableInfo(MOCK_DB_ID, t1.getId(), MOCK_DB_NAME, t1.getName()),
                ImmutableList.of(
                        Pair.create(t2Fk2a.getColumnId(), t1Pk1a.getColumnId()),
                        Pair.create(t2Fk2b.getColumnId(), t1Pk1b.getColumnId())
                )
        );
        OlapTable t2 = createMockTable("t2", ImmutableList.of(t2Fk2a, t2Fk2b),
                ImmutableList.of(t2Fk2a.getColumnId(), t2Fk2b.getColumnId()), ImmutableList.of(fk), t2Id);

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT1Info =
                createScanOperator(t1, ImmutableList.of("pk1a", "pk1b"));
        LogicalOlapScanOperator scanT1 = scanT1Info.first;
        Map<Column, ColumnRefOperator> scanT1ColMap = scanT1Info.second;

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT2Info =
                createScanOperator(t2, ImmutableList.of("fk2a", "fk2b"));
        LogicalOlapScanOperator scanT2 = scanT2Info.first;
        Map<Column, ColumnRefOperator> scanT2ColMap = scanT2Info.second;

        ColumnRefOperator t1Pk1aRef = scanT1ColMap.get(t1Pk1a);
        ColumnRefOperator t2Fk2aRef = scanT2ColMap.get(t2Fk2a);

        ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryType.EQ, t2Fk2aRef, t1Pk1aRef);
        LogicalJoinOperator joinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.LEFT_OUTER_JOIN, onPredicate);
        OptExpression joinExpr = OptExpression.create(joinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));

        OptExpression transformedExpr = applyRule(joinExpr);
        assertEquals(JoinOperator.LEFT_OUTER_JOIN, ((LogicalJoinOperator) transformedExpr.getOp()).getJoinType());
    }

    @Test
    public void testCompositeFkCompletePredicate() {
        long t1Id = System.nanoTime();
        long t2Id = System.nanoTime();

        Column t1Pk1a = createColumn("pk1a", ScalarType.INT, false, true, "t1_c1");
        Column t1Pk1b = createColumn("pk1b", ScalarType.INT, false, true, "t1_c2");
        OlapTable t1 = createMockTable("t1", ImmutableList.of(t1Pk1a, t1Pk1b),
                ImmutableList.of(t1Pk1a.getColumnId(), t1Pk1b.getColumnId()), null, t1Id);

        Column t2Fk2a = createColumn("fk2a", ScalarType.INT, false, true, "t2_c1");
        Column t2Fk2b = createColumn("fk2b", ScalarType.INT, false, true, "t2_c2");
        ForeignKeyConstraint fk = new ForeignKeyConstraint(
                "fk_t2_t1_comp",
                new BaseTableInfo(MOCK_DB_ID, t1.getId(), MOCK_DB_NAME, t1.getName()),
                ImmutableList.of(
                        Pair.create(t2Fk2a.getColumnId(), t1Pk1a.getColumnId()),
                        Pair.create(t2Fk2b.getColumnId(), t1Pk1b.getColumnId())
                )
        );
        OlapTable t2 = createMockTable("t2", ImmutableList.of(t2Fk2a, t2Fk2b),
                ImmutableList.of(t2Fk2a.getColumnId(), t2Fk2b.getColumnId()), ImmutableList.of(fk), t2Id);

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT1Info =
                createScanOperator(t1, ImmutableList.of("pk1a", "pk1b"));
        LogicalOlapScanOperator scanT1 = scanT1Info.first;
        Map<Column, ColumnRefOperator> scanT1ColMap = scanT1Info.second;

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT2Info =
                createScanOperator(t2, ImmutableList.of("fk2a", "fk2b"));
        LogicalOlapScanOperator scanT2 = scanT2Info.first;
        Map<Column, ColumnRefOperator> scanT2ColMap = scanT2Info.second;

        ColumnRefOperator t1Pk1aRef = scanT1ColMap.get(t1Pk1a);
        ColumnRefOperator t1Pk1bRef = scanT1ColMap.get(t1Pk1b);
        ColumnRefOperator t2Fk2aRef = scanT2ColMap.get(t2Fk2a);
        ColumnRefOperator t2Fk2bRef = scanT2ColMap.get(t2Fk2b);

        ScalarOperator pred1 = new BinaryPredicateOperator(BinaryType.EQ, t2Fk2aRef, t1Pk1aRef);
        ScalarOperator pred2 = new BinaryPredicateOperator(BinaryType.EQ, t2Fk2bRef, t1Pk1bRef);
        ScalarOperator onPredicate = BinaryPredicateOperator.and(pred1, pred2);

        LogicalJoinOperator joinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.LEFT_OUTER_JOIN, onPredicate);
        OptExpression joinExpr = OptExpression.create(joinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));

        OptExpression transformedExpr = applyRule(joinExpr);
        assertEquals(JoinOperator.INNER_JOIN, ((LogicalJoinOperator) transformedExpr.getOp()).getJoinType());
    }

    @Test
    public void testGitHubIssue59101Case1() {
        long t1Id = System.nanoTime();
        long t2Id = System.nanoTime();

        Column t1CustCode = createColumn("cust_code", ScalarType.VARCHAR, false, true, "t1_c1");
        OlapTable t1 = createMockTable("t1", ImmutableList.of(t1CustCode),
                ImmutableList.of(t1CustCode.getColumnId()), null, t1Id);

        Column t2CustCode = createColumn("cust_code", ScalarType.VARCHAR, false, true, "t2_c1");
        Column t2IvstPrtcpId = createColumn("ivst_prtcp_id", ScalarType.VARCHAR, true, false, "t2_c2");
        Column t2OneId = createColumn("one_id", ScalarType.VARCHAR, true, false, "t2_c3");

        ForeignKeyConstraint fk = new ForeignKeyConstraint(
                "fk_t2_t1_cust_code",
                new BaseTableInfo(MOCK_DB_ID, t1.getId(), MOCK_DB_NAME, t1.getName()),
                ImmutableList.of(Pair.create(t2CustCode.getColumnId(), t1CustCode.getColumnId()))
        );
        OlapTable t2 = createMockTable("t2", ImmutableList.of(t2CustCode, t2IvstPrtcpId, t2OneId),
                ImmutableList.of(t2CustCode.getColumnId()), ImmutableList.of(fk), t2Id);

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT1Info =
                createScanOperator(t1, ImmutableList.of("cust_code"));
        LogicalOlapScanOperator scanT1 = scanT1Info.first;
        Map<Column, ColumnRefOperator> scanT1ColMap = scanT1Info.second;

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT2Info =
                createScanOperator(t2, ImmutableList.of("cust_code", "ivst_prtcp_id", "one_id"));
        LogicalOlapScanOperator scanT2 = scanT2Info.first;
        Map<Column, ColumnRefOperator> scanT2ColMap = scanT2Info.second;

        ColumnRefOperator t1CustCodeRef = scanT1ColMap.get(t1CustCode);
        ColumnRefOperator t2CustCodeRef = scanT2ColMap.get(t2CustCode);

        ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryType.EQ, t2CustCodeRef, t1CustCodeRef);
        LogicalJoinOperator joinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.LEFT_OUTER_JOIN, onPredicate);
        OptExpression joinExpr = OptExpression.create(joinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));

        OptExpression transformedExpr = applyRule(joinExpr);
        assertEquals(JoinOperator.INNER_JOIN, ((LogicalJoinOperator) transformedExpr.getOp()).getJoinType());
    }

    @Test
    public void testTablePruningInteractionFocusJoinType() {
        long t1Id = System.nanoTime();
        long t2Id = System.nanoTime();

        Column t1CustCode = createColumn("cust_code", ScalarType.VARCHAR, false, true, "t1_c1");
        OlapTable t1 = createMockTable("t1", ImmutableList.of(t1CustCode),
                ImmutableList.of(t1CustCode.getColumnId()), null, t1Id);

        Column t2CustCode = createColumn("cust_code", ScalarType.VARCHAR, false, true, "t2_c1");
        Column t2IvstPrtcpId = createColumn("ivst_prtcp_id", ScalarType.VARCHAR, true, false, "t2_c2");
        ForeignKeyConstraint fk = new ForeignKeyConstraint(
                "fk_t2_t1_cust_code",
                new BaseTableInfo(MOCK_DB_ID, t1.getId(), MOCK_DB_NAME, t1.getName()),
                ImmutableList.of(Pair.create(t2CustCode.getColumnId(), t1CustCode.getColumnId()))
        );
        OlapTable t2 = createMockTable("t2", ImmutableList.of(t2CustCode, t2IvstPrtcpId),
                ImmutableList.of(t2CustCode.getColumnId()), ImmutableList.of(fk), t2Id);

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT1Info =
                createScanOperator(t1, ImmutableList.of("cust_code"));
        LogicalOlapScanOperator scanT1 = scanT1Info.first;
        Map<Column, ColumnRefOperator> scanT1ColMap = scanT1Info.second;

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT2Info =
                createScanOperator(t2, ImmutableList.of("cust_code", "ivst_prtcp_id"));
        LogicalOlapScanOperator scanT2 = scanT2Info.first;
        Map<Column, ColumnRefOperator> scanT2ColMap = scanT2Info.second;

        ColumnRefOperator t1CustCodeRef = scanT1ColMap.get(t1CustCode);
        ColumnRefOperator t2CustCodeRef = scanT2ColMap.get(t2CustCode);

        ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryType.EQ, t2CustCodeRef, t1CustCodeRef);
        LogicalJoinOperator joinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.LEFT_OUTER_JOIN, onPredicate);
        OptExpression joinExpr = OptExpression.create(joinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));

        OptExpression transformedExpr = applyRule(joinExpr);
        assertEquals(JoinOperator.INNER_JOIN, ((LogicalJoinOperator) transformedExpr.getOp()).getJoinType(),
                "Join should be transformed to INNER JOIN first.");
    }

    @Test
    public void testMaterializedViewDefinition() {
        long t1Id = System.nanoTime();
        long t2Id = System.nanoTime();

        Column t1CustCode = createColumn("cust_code", ScalarType.VARCHAR, false, true, "t1_c1_mv");
        OlapTable t1 = createMockTable("t1_mv", ImmutableList.of(t1CustCode),
                ImmutableList.of(t1CustCode.getColumnId()), null, t1Id);

        Column t2CustCode = createColumn("cust_code", ScalarType.VARCHAR, false, true, "t2_c1_mv");
        Column t2IvstPrtcpId = createColumn("ivst_prtcp_id", ScalarType.VARCHAR, true, false, "t2_c2_mv");
        ForeignKeyConstraint fk = new ForeignKeyConstraint(
                "fk_t2_t1_mv_cust_code",
                new BaseTableInfo(MOCK_DB_ID, t1.getId(), MOCK_DB_NAME, t1.getName()),
                ImmutableList.of(Pair.create(t2CustCode.getColumnId(), t1CustCode.getColumnId()))
        );
        OlapTable t2 = createMockTable("t2_mv", ImmutableList.of(t2CustCode, t2IvstPrtcpId),
                ImmutableList.of(t2CustCode.getColumnId()), ImmutableList.of(fk), t2Id);

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT1Info =
                createScanOperator(t1, ImmutableList.of("cust_code"));
        LogicalOlapScanOperator scanT1 = scanT1Info.first;
        Map<Column, ColumnRefOperator> scanT1ColMap = scanT1Info.second;

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT2Info =
                createScanOperator(t2, ImmutableList.of("cust_code", "ivst_prtcp_id"));
        LogicalOlapScanOperator scanT2 = scanT2Info.first;
        Map<Column, ColumnRefOperator> scanT2ColMap = scanT2Info.second;

        ColumnRefOperator t1CustCodeRef = scanT1ColMap.get(t1CustCode);
        ColumnRefOperator t2CustCodeRef = scanT2ColMap.get(t2CustCode);
        ColumnRefOperator t2IvstPrtcpIdRef = scanT2ColMap.get(t2IvstPrtcpId);

        ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryType.EQ, t2CustCodeRef, t1CustCodeRef);
        LogicalJoinOperator joinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.LEFT_OUTER_JOIN, onPredicate);

        Map<ColumnRefOperator, ScalarOperator> mvProjectionMap = Maps.newHashMap();
        mvProjectionMap.put(columnRefFactory.create("output_b_cust_code", t1CustCodeRef.getType(),
                t1CustCodeRef.isNullable()), t1CustCodeRef);
        mvProjectionMap.put(columnRefFactory.create("output_a_ivst_prtcp_id", t2IvstPrtcpIdRef.getType(),
                t2IvstPrtcpIdRef.isNullable()), t2IvstPrtcpIdRef);
        joinOp.setProjection(new Projection(mvProjectionMap));

        OptExpression joinExpr = OptExpression.create(joinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));

        OptExpression transformedExpr = applyRule(joinExpr);
        assertTrue(transformedExpr.getOp() instanceof LogicalJoinOperator);
        assertEquals(JoinOperator.INNER_JOIN, ((LogicalJoinOperator) transformedExpr.getOp()).getJoinType());
        assertNotNull(((LogicalJoinOperator) transformedExpr.getOp()).getProjection(), "Projection should be preserved");
    }

    @Test
    public void testNotLeftOuterJoin() {
        long t1Id = System.nanoTime();
        long t2Id = System.nanoTime();

        Column t1Pk1 = createColumn("pk1", ScalarType.INT, false, true, "t1_c1");
        OlapTable t1 = createMockTable("t1", ImmutableList.of(t1Pk1), ImmutableList.of(t1Pk1.getColumnId()), null, t1Id);

        Column t2Fk2 = createColumn("fk2", ScalarType.INT, false, true, "t2_c1");
        ForeignKeyConstraint fk = new ForeignKeyConstraint(
                "fk_t2_t1",
                new BaseTableInfo(MOCK_DB_ID, t1.getId(), MOCK_DB_NAME, t1.getName()),
                ImmutableList.of(Pair.create(t2Fk2.getColumnId(), t1Pk1.getColumnId()))
        );
        OlapTable t2 = createMockTable("t2", ImmutableList.of(t2Fk2), ImmutableList.of(t2Fk2.getColumnId()),
                ImmutableList.of(fk), t2Id);

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT1Info =
                createScanOperator(t1, ImmutableList.of("pk1"));
        LogicalOlapScanOperator scanT1 = scanT1Info.first;
        Map<Column, ColumnRefOperator> scanT1ColMap = scanT1Info.second;

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT2Info =
                createScanOperator(t2, ImmutableList.of("fk2"));
        LogicalOlapScanOperator scanT2 = scanT2Info.first;
        Map<Column, ColumnRefOperator> scanT2ColMap = scanT2Info.second;

        ColumnRefOperator t1Pk1Ref = scanT1ColMap.get(t1Pk1);
        ColumnRefOperator t2Fk2Ref = scanT2ColMap.get(t2Fk2);

        ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryType.EQ, t2Fk2Ref, t1Pk1Ref);

        LogicalJoinOperator innerJoinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.INNER_JOIN, onPredicate);
        OptExpression innerJoinExpr = OptExpression.create(innerJoinOp, OptExpression.create(scanT2),
                OptExpression.create(scanT1));
        OptExpression transformedInnerExpr = applyRule(innerJoinExpr);
        assertEquals(JoinOperator.INNER_JOIN, ((LogicalJoinOperator) transformedInnerExpr.getOp()).getJoinType());

        LogicalJoinOperator rightOuterJoinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.RIGHT_OUTER_JOIN, onPredicate);
        OptExpression rightOuterJoinExpr = OptExpression.create(rightOuterJoinOp, OptExpression.create(scanT2),
                OptExpression.create(scanT1));
        OptExpression transformedRightOuterExpr = applyRule(rightOuterJoinExpr);
        assertEquals(JoinOperator.RIGHT_OUTER_JOIN,
                ((LogicalJoinOperator) transformedRightOuterExpr.getOp()).getJoinType());
    }

    @Test
    public void testFkToDifferentTable() {
        long t1Id = System.nanoTime();
        long t2Id = System.nanoTime();
        long t3Id = System.nanoTime();

        Column t1Pk1 = createColumn("pk1", ScalarType.INT, false, true, "t1_c1");
        OlapTable t1 = createMockTable("t1", ImmutableList.of(t1Pk1), ImmutableList.of(t1Pk1.getColumnId()), null, t1Id);

        Column t3Pk3 = createColumn("pk3", ScalarType.INT, false, true, "t3_c1");
        OlapTable t3 = createMockTable("t3", ImmutableList.of(t3Pk3), ImmutableList.of(t3Pk3.getColumnId()), null, t3Id);

        Column t2Fk2 = createColumn("fk2", ScalarType.INT, false, true, "t2_c1");
        ForeignKeyConstraint fkToT3 = new ForeignKeyConstraint(
                "fk_t2_t3",
                new BaseTableInfo(MOCK_DB_ID, t3.getId(), MOCK_DB_NAME, t3.getName()),
                ImmutableList.of(Pair.create(t2Fk2.getColumnId(), t3Pk3.getColumnId()))
        );
        OlapTable t2 = createMockTable("t2", ImmutableList.of(t2Fk2), ImmutableList.of(t2Fk2.getColumnId()),
                ImmutableList.of(fkToT3), t2Id);

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT1Info =
                createScanOperator(t1, ImmutableList.of("pk1"));
        LogicalOlapScanOperator scanT1 = scanT1Info.first;
        Map<Column, ColumnRefOperator> scanT1ColMap = scanT1Info.second;

        Pair<LogicalOlapScanOperator, Map<Column, ColumnRefOperator>> scanT2Info =
                createScanOperator(t2, ImmutableList.of("fk2"));
        LogicalOlapScanOperator scanT2 = scanT2Info.first;
        Map<Column, ColumnRefOperator> scanT2ColMap = scanT2Info.second;

        ColumnRefOperator t1Pk1Ref = scanT1ColMap.get(t1Pk1);
        ColumnRefOperator t2Fk2Ref = scanT2ColMap.get(t2Fk2);

        ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryType.EQ, t2Fk2Ref, t1Pk1Ref);
        LogicalJoinOperator joinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.LEFT_OUTER_JOIN, onPredicate);
        OptExpression joinExpr = OptExpression.create(joinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));

        OptExpression transformedExpr = applyRule(joinExpr);
        assertEquals(JoinOperator.LEFT_OUTER_JOIN, ((LogicalJoinOperator) transformedExpr.getOp()).getJoinType());
    }
}
