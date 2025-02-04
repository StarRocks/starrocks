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

import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DrivingTableSelectionRuleTest {

    @Test
    public void transform() {
        DrivingTableSelection rule = new DrivingTableSelection();
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();

        Column c1 = new Column("c1", ScalarType.INT, true);
        Column c2 = new Column("c2", ScalarType.INT, true);
        Column c3 = new Column("c3", ScalarType.INT, true);
        Column c4 = new Column("c4", ScalarType.INT, true);
        Column c5 = new Column("c4", ScalarType.INT, true);
        Column c6 = new Column("c5", ScalarType.INT, true);
        ColumnRefOperator c1Operator = columnRefFactory.create("c1", ScalarType.INT, true);
        ColumnRefOperator c2Operator = columnRefFactory.create("c2", ScalarType.INT, true);
        ColumnRefOperator c3Operator = columnRefFactory.create("c3", ScalarType.INT, true);
        ColumnRefOperator c4Operator = columnRefFactory.create("c4", ScalarType.INT, true);
        ColumnRefOperator c5Operator = columnRefFactory.create("c5", ScalarType.INT, true);
        ColumnRefOperator c6Operator = columnRefFactory.create("c6", ScalarType.INT, true);

        Table t = new OlapTable(0, "t", List.of(c1, c2, c3), null, null, null);
        Table t1 = new OlapTable(1, "t1", List.of(c4), null, null, null);
        Table t2 = new OlapTable(2, "t2", List.of(c5), null, null, null);
        Table t3 = new OlapTable(3, "t3", List.of(c6), null, null, null);

        columnRefFactory.updateColumnRefToColumns(c1Operator, c1, t);
        columnRefFactory.updateColumnRefToColumns(c2Operator, c2, t);
        columnRefFactory.updateColumnRefToColumns(c3Operator, c3, t);
        columnRefFactory.updateColumnRefToColumns(c4Operator, c4, t1);
        columnRefFactory.updateColumnRefToColumns(c5Operator, c5, t2);
        columnRefFactory.updateColumnRefToColumns(c6Operator, c6, t3);

        Map<ColumnRefOperator, Column> tColumnMap = Maps.newHashMap();
        Map<ColumnRefOperator, Column> t1ColumnMap = Maps.newHashMap();
        Map<ColumnRefOperator, Column> t2ColumnMap = Maps.newHashMap();
        Map<ColumnRefOperator, Column> t3ColumnMap = Maps.newHashMap();
        tColumnMap.put(c1Operator, c1);
        tColumnMap.put(c2Operator, c2);
        tColumnMap.put(c3Operator, c3);
        t1ColumnMap.put(c4Operator, c4);
        t2ColumnMap.put(c5Operator, c5);
        t3ColumnMap.put(c6Operator, c6);

        OptExpression tScan = new OptExpression(new LogicalOlapScanOperator(t, tColumnMap, Maps.newHashMap(), null, -1, null));
        OptExpression t1Scan = new OptExpression(new LogicalOlapScanOperator(t1, t1ColumnMap, Maps.newHashMap(), null, -1, null));
        OptExpression t2Scan = new OptExpression(new LogicalOlapScanOperator(t2, t2ColumnMap, Maps.newHashMap(), null, -1, null));
        OptExpression t3Scan = new OptExpression(new LogicalOlapScanOperator(t3, t3ColumnMap, Maps.newHashMap(), null, -1, null));

        Map<ColumnRefOperator, ScalarOperator> projection1ColumnRefMap = Maps.newHashMap();
        Map<ColumnRefOperator, ScalarOperator> projection2ColumnRefMap = Maps.newHashMap();
        Map<ColumnRefOperator, ScalarOperator> projection3ColumnRefMap = Maps.newHashMap();
        projection1ColumnRefMap.putAll(tScan.getRowOutputInfo().getColumnRefMap());
        projection1ColumnRefMap.putAll(t1Scan.getRowOutputInfo().getColumnRefMap());
        projection2ColumnRefMap.putAll(projection1ColumnRefMap);
        projection2ColumnRefMap.putAll(t2Scan.getRowOutputInfo().getColumnRefMap());
        projection3ColumnRefMap.putAll(t3Scan.getRowOutputInfo().getColumnRefMap());

        ScalarOperator onKeys = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                new BinaryPredicateOperator(BinaryType.EQ, c1Operator, c4Operator),
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                        new BinaryPredicateOperator(BinaryType.EQ, c2Operator, c5Operator),
                        new BinaryPredicateOperator(BinaryType.EQ, c3Operator, c6Operator)));
        OptExpression crossJoin1 = OptExpression.create(new LogicalJoinOperator(JoinOperator.CROSS_JOIN, null), tScan, t1Scan);
        OptExpression projection1 = OptExpression.create(LogicalProjectOperator.builder()
                .setColumnRefMap(projection1ColumnRefMap).build(), crossJoin1);
        OptExpression crossJoin2 =
                OptExpression.create(new LogicalJoinOperator(JoinOperator.CROSS_JOIN, null), projection1, t2Scan);
        OptExpression projection2 = OptExpression.create(LogicalProjectOperator.builder()
                .setColumnRefMap(projection2ColumnRefMap).build(), crossJoin2);
        OptExpression projection3 = OptExpression.create(LogicalProjectOperator.builder()
                .setColumnRefMap(projection3ColumnRefMap).build(), t3Scan);
        OptExpression innerJoin =
                OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN, onKeys), projection2, projection3);

        OptimizerContext context = new OptimizerContext(new Memo(), columnRefFactory);

        System.out.println("Before: " + innerJoin.debugString());
        assertTrue(rule.check(innerJoin, context));
        List<OptExpression> transform = rule.transform(innerJoin, context);

        OptExpression newJoinTree = transform.get(0);
        System.out.println("After: " + newJoinTree.debugString());

        // JOIN
        {
            assertEquals(0, ((LogicalOlapScanOperator) newJoinTree.inputAt(1).getOp()).getTable().getId());
            assertEquals(2,
                    ((LogicalOlapScanOperator) newJoinTree.inputAt(0).inputAt(0).inputAt(0).inputAt(0).inputAt(0).inputAt(0)
                            .getOp()).getTable()
                            .getId());
        }
        // Projection
        {
            Map<ColumnRefOperator, ScalarOperator> map0 = new HashMap<>();
            map0.putAll(t1Scan.getRowOutputInfo().getColumnRefMap());
            map0.putAll(t2Scan.getRowOutputInfo().getColumnRefMap());
            map0.putAll(t3Scan.getRowOutputInfo().getColumnRefMap());

            assertEquals(map0, ((LogicalProjectOperator) newJoinTree.inputAt(0).getOp()).getColumnRefMap());

            Map<ColumnRefOperator, ScalarOperator> map1 = new HashMap<>();
            map1.putAll(t2Scan.getRowOutputInfo().getColumnRefMap());
            map1.putAll(t3Scan.getRowOutputInfo().getColumnRefMap());

            assertEquals(map1, ((LogicalProjectOperator) newJoinTree.inputAt(0).inputAt(0).inputAt(0).getOp()).getColumnRefMap());

            Map<ColumnRefOperator, ScalarOperator> map2 = new HashMap<>(t3Scan.getRowOutputInfo().getColumnRefMap());
            t2Scan.getRowOutputInfo().getColumnRefMap().forEach((k, v) -> map2.put(k, ConstantOperator.createNull(v.getType())));

            assertEquals(map2,
                    ((LogicalProjectOperator) newJoinTree.inputAt(0).inputAt(0).inputAt(0).inputAt(0).inputAt(1)
                            .getOp()).getColumnRefMap());
        }
    }
}
