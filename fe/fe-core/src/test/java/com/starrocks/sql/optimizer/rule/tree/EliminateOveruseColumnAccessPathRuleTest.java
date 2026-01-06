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

package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.thrift.TAccessPathType;
import com.starrocks.type.InvalidType;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link EliminateOveruseColumnAccessPathRule}.
 *
 * These tests focus on the behavior when column name and ColumnId differ
 * (e.g. after a column rename), to ensure that the rule matches access
 * paths using ColumnId instead of the mutable column name.
 */
public class EliminateOveruseColumnAccessPathRuleTest {

    @Test
    public void testOveruseColumnAccessPathMatchedByColumnIdAfterRename() {
        // Simulate a renamed column: ColumnId = "c", current name = "c_new"
        ColumnRefOperator colRef = new ColumnRefOperator(1, TypeFactory.createVarcharType(10), "c_new", true);
        Column columnMeta = new Column("c", TypeFactory.createVarcharType(10), true, "");
        // Rename the column logically, ColumnId should remain "c"
        columnMeta.setName("c_new");

        Map<ColumnRefOperator, Column> colRefToMeta = Map.of(colRef, columnMeta);

        // Minimal table stub, EliminateOveruseColumnAccessPathRule does not touch table metadata
        Table table = new Table(Table.TableType.OLAP) { };
        table.setId(1L);
        table.setName("t");

        PhysicalOlapScanOperator scan = new PhysicalOlapScanOperator(
                table,
                colRefToMeta,
                null,
                Operator.DEFAULT_LIMIT,
                null,
                0L,
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                null,
                false,
                null);

        // Build a subfield-pruning ColumnAccessPath for this columnId "c"
        ColumnAccessPath rootPath = new ColumnAccessPath(
                TAccessPathType.ROOT,
                columnMeta.getColumnId().getId(),
                InvalidType.INVALID);
        ColumnAccessPath childField = new ColumnAccessPath(
                TAccessPathType.FIELD,
                "f",
                InvalidType.INVALID);
        rootPath.addChildPath(childField);
        scan.setColumnAccessPaths(ImmutableList.of(rootPath));

        // Parent project outputs the whole column, so the subfield path should be treated as "overuse"
        Map<ColumnRefOperator, com.starrocks.sql.optimizer.operator.scalar.ScalarOperator> projectMap =
                Map.of(colRef, colRef);
        PhysicalProjectOperator project = new PhysicalProjectOperator(projectMap, Map.of());

        OptExpression scanExpr = OptExpression.create(scan, List.of());
        OptExpression root = OptExpression.create(project, List.of(scanExpr));

        EliminateOveruseColumnAccessPathRule rule = new EliminateOveruseColumnAccessPathRule();
        rule.rewrite(root, null);

        // After the rule, the overused ColumnAccessPath for column "c" should be eliminated.
        Assertions.assertTrue(
                scan.getColumnAccessPaths().isEmpty(),
                "Overused column access path should be eliminated when matching by ColumnId");
    }
}


