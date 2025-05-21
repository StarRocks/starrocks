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
import com.starrocks.analysis.JoinOperator;
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

    private OptimizerContext optimizerContext;
    private ColumnRefFactory columnRefFactory;

    @BeforeEach
    public void setUp() {
        columnRefFactory = new ColumnRefFactory();
        optimizerContext = new OptimizerContext(new Memo(), columnRefFactory, new TaskContext(null, null, null));
        // Mock GlobalStateMgr and GlobalConstraintManager if necessary,
        // For this rule, direct OlapTable.getForeignKeyConstraints() is used.
    }

    private OlapTable createMockTable(String name,
                                      List<Column> columns,
                                      List<ColumnId> pkColumnIds,
                                      List<ForeignKeyConstraint> fkConstraints) {
        long tableId = System.nanoTime(); // Quick way to get unique enough IDs for tests
        MaterializedIndex baseIndex = new MaterializedIndex(1, MaterializedIndex.IndexState.NORMAL);
        PartitionInfo partitionInfo = new PartitionInfo(); // Dummy partition info
        DistributionInfo distributionInfo = new HashDistributionInfo(10, pkColumnIds.stream()
                .map(cid -> columns.stream().filter(c -> c.getColumnId().equals(cid)).findFirst().get().getName())
                .collect(Collectors.toList())); // Dummy distribution

        OlapTable table = new OlapTable(tableId, name, columns, KeysType.PRIMARY_KEYS, partitionInfo, distributionInfo);
        table.setIndexMeta(1L, name, columns, 0, 0, (short) pkColumnIds.size(), KeysType.PRIMARY_KEYS, null);
        table.setBaseIndexId(baseIndex.getId());
        if (fkConstraints != null) {
            table.setForeignKeyConstraints(fkConstraints);
        }
        return table;
    }

    private Column createColumn(String name, Type type, boolean isAllowNull, boolean isKey, String colId) {
        Column col = new Column(name, type, isKey);
        col.setIsAllowNull(isAllowNull);
        col.setColumnId(ColumnId.create(colId));
        return col;
    }

    private LogicalOlapScanOperator createScanOperator(OlapTable table, List<String> selectedColNames) {
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

        return LogicalOlapScanOperator.builder()
                .setTable(table)
                .setColRefToColumnMetaMap(colRefToColumnMeta)
                .setColumnMetaToColRefMap(columnMetaToColRef)
                .setOutputColumnRefOp(outputVariables)
                .setDistributionSpec(DistributionSpec.createAnyDistributionSpec())
                .setLimit(Operator.DEFAULT_LIMIT)
                .setPredicate(null)
                .setSelectedIndexId(table.getBaseIndexId())
                .setProjection(new Projection(Maps.newHashMap()))
                .build();
    }


    private LogicalJoinOperator createJoinOperator(OptExpression leftChild, OptExpression rightChild,
                                                   JoinOperator joinType, ScalarOperator onPredicate) {
        return LogicalJoinOperator.builder()
                .setJoinType(joinType)
                .setOnPredicate(onPredicate)
                .setProjection(null) // Not strictly needed for this rule test
                .build();
    }

    private OptExpression applyRule(OptExpression joinExpression) {
        RewriteLeftJoinToInnerJoinRule rule = new RewriteLeftJoinToInnerJoinRule();
        List<OptExpression> result = rule.transform(joinExpression, optimizerContext);
        if (result.isEmpty()) {
            return joinExpression; // Rule didn't fire or returned empty
        }
        return result.get(0);
    }

    // Test Case 1: Basic Transformation
    @Test
    public void testBasicTransformation() {
        // t1 (pk1 INT PK)
        Column t1_pk1 = createColumn("pk1", ScalarType.INT, false, true, "t1_c1");
        OlapTable t1 = createMockTable("t1", ImmutableList.of(t1_pk1), ImmutableList.of(t1_pk1.getColumnId()), null);

        // t2 (fk2 INT NOT NULL, FK(fk2) refs t1(pk1))
        Column t2_fk2 = createColumn("fk2", ScalarType.INT, false, true, "t2_c1"); // Treat as key for simplicity
        Column t2_val = createColumn("val", ScalarType.VARCHAR, true, false, "t2_c2");

        ForeignKeyConstraint fk = new ForeignKeyConstraint(
                "fk_t2_t1",
                new ForeignKeyConstraint.TableIdentifier(t1.getId(), "test_db", t1.getName()),
                ImmutableList.of(Pair.create(t2_fk2.getColumnId(), t1_pk1.getColumnId()))
        );
        OlapTable t2 = createMockTable("t2", ImmutableList.of(t2_fk2, t2_val), ImmutableList.of(t2_fk2.getColumnId()), ImmutableList.of(fk));


        LogicalOlapScanOperator scanT1 = createScanOperator(t1, ImmutableList.of("pk1"));
        LogicalOlapScanOperator scanT2 = createScanOperator(t2, ImmutableList.of("fk2", "val"));

        ColumnRefOperator t1_pk1_ref = scanT1.getOutputVariables().get(0);
        ColumnRefOperator t2_fk2_ref = scanT2.getOutputVariables().get(0);

        ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, t2_fk2_ref, t1_pk1_ref);
        LogicalJoinOperator joinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.LEFT_OUTER_JOIN, onPredicate);
        OptExpression joinExpr = OptExpression.create(joinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));

        OptExpression transformedExpr = applyRule(joinExpr);
        assertTrue(transformedExpr.getOp() instanceof LogicalJoinOperator);
        assertEquals(JoinOperator.INNER_JOIN, ((LogicalJoinOperator) transformedExpr.getOp()).getJoinType());
    }

    // Test Case 2: Condition Not Met - FK Column is Nullable
    @Test
    public void testFkColumnNullable() {
        Column t1_pk1 = createColumn("pk1", ScalarType.INT, false, true, "t1_c1");
        OlapTable t1 = createMockTable("t1", ImmutableList.of(t1_pk1), ImmutableList.of(t1_pk1.getColumnId()), null);

        Column t2_fk2 = createColumn("fk2", ScalarType.INT, true, true, "t2_c1"); // ALLOW NULL
        Column t2_val = createColumn("val", ScalarType.VARCHAR, true, false, "t2_c2");
        ForeignKeyConstraint fk = new ForeignKeyConstraint(
                "fk_t2_t1",
                new ForeignKeyConstraint.TableIdentifier(t1.getId(), "test_db", t1.getName()),
                ImmutableList.of(Pair.create(t2_fk2.getColumnId(), t1_pk1.getColumnId()))
        );
        OlapTable t2 = createMockTable("t2", ImmutableList.of(t2_fk2, t2_val), ImmutableList.of(t2_fk2.getColumnId()), ImmutableList.of(fk));

        LogicalOlapScanOperator scanT1 = createScanOperator(t1, ImmutableList.of("pk1"));
        LogicalOlapScanOperator scanT2 = createScanOperator(t2, ImmutableList.of("fk2", "val"));
        ColumnRefOperator t1_pk1_ref = scanT1.getOutputVariables().get(0);
        ColumnRefOperator t2_fk2_ref = scanT2.getOutputVariables().get(0);
        ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, t2_fk2_ref, t1_pk1_ref);
        LogicalJoinOperator joinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.LEFT_OUTER_JOIN, onPredicate);
        OptExpression joinExpr = OptExpression.create(joinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));

        OptExpression transformedExpr = applyRule(joinExpr);
        assertEquals(JoinOperator.LEFT_OUTER_JOIN, ((LogicalJoinOperator) transformedExpr.getOp()).getJoinType());
    }

    // Test Case 3: Condition Not Met - No Foreign Key Constraint
    @Test
    public void testNoForeignKeyConstraint() {
        Column t1_pk1 = createColumn("pk1", ScalarType.INT, false, true, "t1_c1");
        OlapTable t1 = createMockTable("t1", ImmutableList.of(t1_pk1), ImmutableList.of(t1_pk1.getColumnId()), null);

        Column t2_fk2 = createColumn("fk2", ScalarType.INT, false, true, "t2_c1"); // NOT NULL
        Column t2_val = createColumn("val", ScalarType.VARCHAR, true, false, "t2_c2");
        OlapTable t2 = createMockTable("t2", ImmutableList.of(t2_fk2, t2_val), ImmutableList.of(t2_fk2.getColumnId()), null); // No FK

        LogicalOlapScanOperator scanT1 = createScanOperator(t1, ImmutableList.of("pk1"));
        LogicalOlapScanOperator scanT2 = createScanOperator(t2, ImmutableList.of("fk2", "val"));
        ColumnRefOperator t1_pk1_ref = scanT1.getOutputVariables().get(0);
        ColumnRefOperator t2_fk2_ref = scanT2.getOutputVariables().get(0);
        ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, t2_fk2_ref, t1_pk1_ref);
        LogicalJoinOperator joinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.LEFT_OUTER_JOIN, onPredicate);
        OptExpression joinExpr = OptExpression.create(joinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));

        OptExpression transformedExpr = applyRule(joinExpr);
        assertEquals(JoinOperator.LEFT_OUTER_JOIN, ((LogicalJoinOperator) transformedExpr.getOp()).getJoinType());
    }

    // Test Case 4: Condition Not Met - Join Predicate Mismatch
    @Test
    public void testJoinPredicateMismatch() {
        Column t1_pk1 = createColumn("pk1", ScalarType.INT, false, true, "t1_c1");
        OlapTable t1 = createMockTable("t1", ImmutableList.of(t1_pk1), ImmutableList.of(t1_pk1.getColumnId()), null);

        Column t2_fk2 = createColumn("fk2", ScalarType.INT, false, true, "t2_c1");
        Column t2_other_col = createColumn("other_col", ScalarType.INT, false, false, "t2_c2");
        ForeignKeyConstraint fk = new ForeignKeyConstraint(
                "fk_t2_t1",
                new ForeignKeyConstraint.TableIdentifier(t1.getId(), "test_db", t1.getName()),
                ImmutableList.of(Pair.create(t2_fk2.getColumnId(), t1_pk1.getColumnId()))
        );
        OlapTable t2 = createMockTable("t2", ImmutableList.of(t2_fk2, t2_other_col), ImmutableList.of(t2_fk2.getColumnId()), ImmutableList.of(fk));

        LogicalOlapScanOperator scanT1 = createScanOperator(t1, ImmutableList.of("pk1"));
        LogicalOlapScanOperator scanT2 = createScanOperator(t2, ImmutableList.of("fk2", "other_col"));
        ColumnRefOperator t1_pk1_ref = scanT1.getOutputVariables().get(0);
        ColumnRefOperator t2_other_col_ref = scanT2.getOutputVariables().get(1); // Using other_col

        ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, t2_other_col_ref, t1_pk1_ref);
        LogicalJoinOperator joinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.LEFT_OUTER_JOIN, onPredicate);
        OptExpression joinExpr = OptExpression.create(joinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));

        OptExpression transformedExpr = applyRule(joinExpr);
        assertEquals(JoinOperator.LEFT_OUTER_JOIN, ((LogicalJoinOperator) transformedExpr.getOp()).getJoinType());
    }

    // Test Case 5: Condition Not Met - Join Predicate Incomplete for Composite FK
    @Test
    public void testCompositeFkIncompletePredicate() {
        Column t1_pk1a = createColumn("pk1a", ScalarType.INT, false, true, "t1_c1");
        Column t1_pk1b = createColumn("pk1b", ScalarType.INT, false, true, "t1_c2");
        OlapTable t1 = createMockTable("t1", ImmutableList.of(t1_pk1a, t1_pk1b),
                ImmutableList.of(t1_pk1a.getColumnId(), t1_pk1b.getColumnId()), null);

        Column t2_fk2a = createColumn("fk2a", ScalarType.INT, false, true, "t2_c1");
        Column t2_fk2b = createColumn("fk2b", ScalarType.INT, false, true, "t2_c2");
        ForeignKeyConstraint fk = new ForeignKeyConstraint(
                "fk_t2_t1_comp",
                new ForeignKeyConstraint.TableIdentifier(t1.getId(), "test_db", t1.getName()),
                ImmutableList.of(
                        Pair.create(t2_fk2a.getColumnId(), t1_pk1a.getColumnId()),
                        Pair.create(t2_fk2b.getColumnId(), t1_pk1b.getColumnId())
                )
        );
        OlapTable t2 = createMockTable("t2", ImmutableList.of(t2_fk2a, t2_fk2b),
                ImmutableList.of(t2_fk2a.getColumnId(), t2_fk2b.getColumnId()), ImmutableList.of(fk));

        LogicalOlapScanOperator scanT1 = createScanOperator(t1, ImmutableList.of("pk1a", "pk1b"));
        LogicalOlapScanOperator scanT2 = createScanOperator(t2, ImmutableList.of("fk2a", "fk2b"));
        ColumnRefOperator t1_pk1a_ref = scanT1.getOutputVariables().get(0);
        ColumnRefOperator t2_fk2a_ref = scanT2.getOutputVariables().get(0);

        ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, t2_fk2a_ref, t1_pk1a_ref); // Only one part
        LogicalJoinOperator joinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.LEFT_OUTER_JOIN, onPredicate);
        OptExpression joinExpr = OptExpression.create(joinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));

        OptExpression transformedExpr = applyRule(joinExpr);
        assertEquals(JoinOperator.LEFT_OUTER_JOIN, ((LogicalJoinOperator) transformedExpr.getOp()).getJoinType());
    }

    // Test Case 6: Transformation with Composite FK
    @Test
    public void testCompositeFkCompletePredicate() {
        Column t1_pk1a = createColumn("pk1a", ScalarType.INT, false, true, "t1_c1");
        Column t1_pk1b = createColumn("pk1b", ScalarType.INT, false, true, "t1_c2");
        OlapTable t1 = createMockTable("t1", ImmutableList.of(t1_pk1a, t1_pk1b),
                ImmutableList.of(t1_pk1a.getColumnId(), t1_pk1b.getColumnId()), null);

        Column t2_fk2a = createColumn("fk2a", ScalarType.INT, false, true, "t2_c1");
        Column t2_fk2b = createColumn("fk2b", ScalarType.INT, false, true, "t2_c2");
        ForeignKeyConstraint fk = new ForeignKeyConstraint(
                "fk_t2_t1_comp",
                new ForeignKeyConstraint.TableIdentifier(t1.getId(), "test_db", t1.getName()),
                ImmutableList.of(
                        Pair.create(t2_fk2a.getColumnId(), t1_pk1a.getColumnId()),
                        Pair.create(t2_fk2b.getColumnId(), t1_pk1b.getColumnId())
                )
        );
        OlapTable t2 = createMockTable("t2", ImmutableList.of(t2_fk2a, t2_fk2b),
                ImmutableList.of(t2_fk2a.getColumnId(), t2_fk2b.getColumnId()), ImmutableList.of(fk));

        LogicalOlapScanOperator scanT1 = createScanOperator(t1, ImmutableList.of("pk1a", "pk1b"));
        LogicalOlapScanOperator scanT2 = createScanOperator(t2, ImmutableList.of("fk2a", "fk2b"));
        ColumnRefOperator t1_pk1a_ref = scanT1.getOutputVariables().get(0);
        ColumnRefOperator t1_pk1b_ref = scanT1.getOutputVariables().get(1);
        ColumnRefOperator t2_fk2a_ref = scanT2.getOutputVariables().get(0);
        ColumnRefOperator t2_fk2b_ref = scanT2.getOutputVariables().get(1);

        ScalarOperator pred1 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, t2_fk2a_ref, t1_pk1a_ref);
        ScalarOperator pred2 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, t2_fk2b_ref, t1_pk1b_ref);
        ScalarOperator onPredicate = BinaryPredicateOperator.and(pred1, pred2);

        LogicalJoinOperator joinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.LEFT_OUTER_JOIN, onPredicate);
        OptExpression joinExpr = OptExpression.create(joinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));

        OptExpression transformedExpr = applyRule(joinExpr);
        assertEquals(JoinOperator.INNER_JOIN, ((LogicalJoinOperator) transformedExpr.getOp()).getJoinType());
    }

    // Test Case 7: GitHub Issue Example (Case 1 from issue #59101)
    @Test
    public void testGitHubIssue59101Case1() {
        // t1 (cust_code PK)
        Column t1_cust_code = createColumn("cust_code", ScalarType.VARCHAR, false, true, "t1_c1");
        OlapTable t1 = createMockTable("t1", ImmutableList.of(t1_cust_code), ImmutableList.of(t1_cust_code.getColumnId()), null);

        // t2 (cust_code FK to t1.cust_code, cust_code in t2 is NOT NULL)
        Column t2_cust_code = createColumn("cust_code", ScalarType.VARCHAR, false, true, "t2_c1"); // FK, NOT NULL
        Column t2_ivst_prtcp_id = createColumn("ivst_prtcp_id", ScalarType.VARCHAR, true, false, "t2_c2");
        Column t2_one_id = createColumn("one_id", ScalarType.VARCHAR, true, false, "t2_c3");

        ForeignKeyConstraint fk = new ForeignKeyConstraint(
                "fk_t2_t1_cust_code",
                new ForeignKeyConstraint.TableIdentifier(t1.getId(), "test_db", t1.getName()),
                ImmutableList.of(Pair.create(t2_cust_code.getColumnId(), t1_cust_code.getColumnId()))
        );
        OlapTable t2 = createMockTable("t2", ImmutableList.of(t2_cust_code, t2_ivst_prtcp_id, t2_one_id),
                ImmutableList.of(t2_cust_code.getColumnId()), ImmutableList.of(fk));

        LogicalOlapScanOperator scanT1 = createScanOperator(t1, ImmutableList.of("cust_code"));
        LogicalOlapScanOperator scanT2 = createScanOperator(t2, ImmutableList.of("cust_code", "ivst_prtcp_id", "one_id"));

        ColumnRefOperator t1_cust_code_ref = scanT1.getOutputVariables().get(0); // b.cust_code
        ColumnRefOperator t2_cust_code_ref = scanT2.getOutputVariables().get(0); // a.cust_code

        ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, t2_cust_code_ref, t1_cust_code_ref);
        LogicalJoinOperator joinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.LEFT_OUTER_JOIN, onPredicate);
        OptExpression joinExpr = OptExpression.create(joinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));

        OptExpression transformedExpr = applyRule(joinExpr);
        assertEquals(JoinOperator.INNER_JOIN, ((LogicalJoinOperator) transformedExpr.getOp()).getJoinType());
    }

    // Test Case 8: Table Pruning Interaction (Inspired by Case 2 from issue #59101)
    @Test
    public void testTablePruningInteractionFocusJoinType() {
        // Setup similar to Test Case 7
        Column t1_cust_code = createColumn("cust_code", ScalarType.VARCHAR, false, true, "t1_c1");
        OlapTable t1 = createMockTable("t1", ImmutableList.of(t1_cust_code), ImmutableList.of(t1_cust_code.getColumnId()), null);

        Column t2_cust_code = createColumn("cust_code", ScalarType.VARCHAR, false, true, "t2_c1");
        Column t2_ivst_prtcp_id = createColumn("ivst_prtcp_id", ScalarType.VARCHAR, true, false, "t2_c2");
        ForeignKeyConstraint fk = new ForeignKeyConstraint(
                "fk_t2_t1_cust_code",
                new ForeignKeyConstraint.TableIdentifier(t1.getId(), "test_db", t1.getName()),
                ImmutableList.of(Pair.create(t2_cust_code.getColumnId(), t1_cust_code.getColumnId()))
        );
        OlapTable t2 = createMockTable("t2", ImmutableList.of(t2_cust_code, t2_ivst_prtcp_id),
                ImmutableList.of(t2_cust_code.getColumnId()), ImmutableList.of(fk));

        // Query: SELECT a.ivst_prtcp_id FROM t2 a LEFT JOIN t1 b ON a.cust_code = b.cust_code
        // Scan t1 for cust_code (needed for join), scan t2 for cust_code and ivst_prtcp_id
        LogicalOlapScanOperator scanT1 = createScanOperator(t1, ImmutableList.of("cust_code"));
        LogicalOlapScanOperator scanT2 = createScanOperator(t2, ImmutableList.of("cust_code", "ivst_prtcp_id"));

        ColumnRefOperator t1_cust_code_ref = scanT1.getOutputVariables().get(0);
        ColumnRefOperator t2_cust_code_ref = scanT2.getOutputVariables().get(0);

        ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, t2_cust_code_ref, t1_cust_code_ref);
        LogicalJoinOperator joinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.LEFT_OUTER_JOIN, onPredicate);
        OptExpression joinExpr = OptExpression.create(joinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));

        // Apply RewriteLeftJoinToInnerJoinRule
        OptExpression transformedExpr = applyRule(joinExpr);
        assertEquals(JoinOperator.INNER_JOIN, ((LogicalJoinOperator) transformedExpr.getOp()).getJoinType(),
                "Join should be transformed to INNER JOIN first.");

        // Conceptually, pruning would happen next.
        // For this unit test, we focus on the join type change.
        // A more integrated test would apply PruneJoinColumns (or similar) and check scanT1's outputs.
        // If PruneJoinColumns rule were applied here (and assuming SELECT a.ivst_prtcp_id means only t2_ivst_prtcp_id_ref is needed from join output)
        // then scanT1 might be pruned if it's not needed for filtering or its output columns would be empty.
        // This is hard to test in isolation without running a fuller optimizer sequence.
    }


    // Test Case 9: Materialized View Definition
    @Test
    public void testMaterializedViewDefinition() {
        // Setup similar to Test Case 7
        Column t1_cust_code = createColumn("cust_code", ScalarType.VARCHAR, false, true, "t1_c1_mv");
        OlapTable t1 = createMockTable("t1_mv", ImmutableList.of(t1_cust_code), ImmutableList.of(t1_cust_code.getColumnId()), null);

        Column t2_cust_code = createColumn("cust_code", ScalarType.VARCHAR, false, true, "t2_c1_mv");
        Column t2_ivst_prtcp_id = createColumn("ivst_prtcp_id", ScalarType.VARCHAR, true, false, "t2_c2_mv");
        ForeignKeyConstraint fk = new ForeignKeyConstraint(
                "fk_t2_t1_mv_cust_code",
                new ForeignKeyConstraint.TableIdentifier(t1.getId(), "test_db", t1.getName()),
                ImmutableList.of(Pair.create(t2_cust_code.getColumnId(), t1_cust_code.getColumnId()))
        );
        OlapTable t2 = createMockTable("t2_mv", ImmutableList.of(t2_cust_code, t2_ivst_prtcp_id),
                ImmutableList.of(t2_cust_code.getColumnId()), ImmutableList.of(fk));

        // MV: SELECT b.cust_code, a.ivst_prtcp_id FROM t2 a LEFT JOIN t1 b ON a.cust_code = b.cust_code
        LogicalOlapScanOperator scanT1 = createScanOperator(t1, ImmutableList.of("cust_code"));
        LogicalOlapScanOperator scanT2 = createScanOperator(t2, ImmutableList.of("cust_code", "ivst_prtcp_id"));

        ColumnRefOperator t1_cust_code_ref = scanT1.getOutputVariables().get(0); // b.cust_code
        ColumnRefOperator t2_cust_code_ref = scanT2.getOutputVariables().get(0); // a.cust_code

        ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, t2_cust_code_ref, t1_cust_code_ref);
        LogicalJoinOperator joinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.LEFT_OUTER_JOIN, onPredicate);

        // Simulate projection for MV: b.cust_code, a.ivst_prtcp_id
        Map<ColumnRefOperator, ScalarOperator> mvProjectionMap = Maps.newHashMap();
        mvProjectionMap.put(columnRefFactory.create("output_b_cust_code", t1_cust_code_ref.getType(), t1_cust_code_ref.isNullable()), t1_cust_code_ref);
        mvProjectionMap.put(columnRefFactory.create("output_a_ivst_prtcp_id", scanT2.getOutputVariables().get(1).getType(), scanT2.getOutputVariables().get(1).isNullable()), scanT2.getOutputVariables().get(1));
        joinOp.setProjection(new Projection(mvProjectionMap));

        OptExpression joinExpr = OptExpression.create(joinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));

        OptExpression transformedExpr = applyRule(joinExpr);
        assertTrue(transformedExpr.getOp() instanceof LogicalJoinOperator);
        assertEquals(JoinOperator.INNER_JOIN, ((LogicalJoinOperator) transformedExpr.getOp()).getJoinType());
        assertNotNull(((LogicalJoinOperator) transformedExpr.getOp()).getProjection(), "Projection should be preserved");
    }
    
    @Test
    public void testNotLeftOuterJoin() {
        Column t1_pk1 = createColumn("pk1", ScalarType.INT, false, true, "t1_c1");
        OlapTable t1 = createMockTable("t1", ImmutableList.of(t1_pk1), ImmutableList.of(t1_pk1.getColumnId()), null);

        Column t2_fk2 = createColumn("fk2", ScalarType.INT, false, true, "t2_c1");
        ForeignKeyConstraint fk = new ForeignKeyConstraint(
                "fk_t2_t1",
                new ForeignKeyConstraint.TableIdentifier(t1.getId(), "test_db", t1.getName()),
                ImmutableList.of(Pair.create(t2_fk2.getColumnId(), t1_pk1.getColumnId()))
        );
        OlapTable t2 = createMockTable("t2", ImmutableList.of(t2_fk2), ImmutableList.of(t2_fk2.getColumnId()), ImmutableList.of(fk));

        LogicalOlapScanOperator scanT1 = createScanOperator(t1, ImmutableList.of("pk1"));
        LogicalOlapScanOperator scanT2 = createScanOperator(t2, ImmutableList.of("fk2"));

        ColumnRefOperator t1_pk1_ref = scanT1.getOutputVariables().get(0);
        ColumnRefOperator t2_fk2_ref = scanT2.getOutputVariables().get(0);

        ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, t2_fk2_ref, t1_pk1_ref);
        
        // Test with INNER_JOIN
        LogicalJoinOperator innerJoinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.INNER_JOIN, onPredicate);
        OptExpression innerJoinExpr = OptExpression.create(innerJoinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));
        OptExpression transformedInnerExpr = applyRule(innerJoinExpr);
        assertEquals(JoinOperator.INNER_JOIN, ((LogicalJoinOperator) transformedInnerExpr.getOp()).getJoinType());

        // Test with RIGHT_OUTER_JOIN
        LogicalJoinOperator rightOuterJoinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.RIGHT_OUTER_JOIN, onPredicate);
        OptExpression rightOuterJoinExpr = OptExpression.create(rightOuterJoinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));
        OptExpression transformedRightOuterExpr = applyRule(rightOuterJoinExpr);
        assertEquals(JoinOperator.RIGHT_OUTER_JOIN, ((LogicalJoinOperator) transformedRightOuterExpr.getOp()).getJoinType());
    }

    @Test
    public void testFkToDifferentTable() {
        Column t1_pk1 = createColumn("pk1", ScalarType.INT, false, true, "t1_c1");
        OlapTable t1 = createMockTable("t1", ImmutableList.of(t1_pk1), ImmutableList.of(t1_pk1.getColumnId()), null);

        Column t3_pk3 = createColumn("pk3", ScalarType.INT, false, true, "t3_c1"); // Another table
        OlapTable t3 = createMockTable("t3", ImmutableList.of(t3_pk3), ImmutableList.of(t3_pk3.getColumnId()), null);


        Column t2_fk2 = createColumn("fk2", ScalarType.INT, false, true, "t2_c1");
        ForeignKeyConstraint fkToT3 = new ForeignKeyConstraint( // FK points to t3, not t1
                "fk_t2_t3",
                new ForeignKeyConstraint.TableIdentifier(t3.getId(), "test_db", t3.getName()),
                ImmutableList.of(Pair.create(t2_fk2.getColumnId(), t3_pk3.getColumnId()))
        );
        OlapTable t2 = createMockTable("t2", ImmutableList.of(t2_fk2), ImmutableList.of(t2_fk2.getColumnId()), ImmutableList.of(fkToT3));

        LogicalOlapScanOperator scanT1 = createScanOperator(t1, ImmutableList.of("pk1"));
        LogicalOlapScanOperator scanT2 = createScanOperator(t2, ImmutableList.of("fk2"));

        ColumnRefOperator t1_pk1_ref = scanT1.getOutputVariables().get(0);
        ColumnRefOperator t2_fk2_ref = scanT2.getOutputVariables().get(0);

        ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, t2_fk2_ref, t1_pk1_ref);
        LogicalJoinOperator joinOp = createJoinOperator(OptExpression.create(scanT2), OptExpression.create(scanT1),
                JoinOperator.LEFT_OUTER_JOIN, onPredicate);
        OptExpression joinExpr = OptExpression.create(joinOp, OptExpression.create(scanT2), OptExpression.create(scanT1));

        OptExpression transformedExpr = applyRule(joinExpr);
        assertEquals(JoinOperator.LEFT_OUTER_JOIN, ((LogicalJoinOperator) transformedExpr.getOp()).getJoinType());
    }
}
