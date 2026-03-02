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

package com.starrocks.sql.optimizer.rule.tree.lowcardinality;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.catalog.Table;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.thrift.TAccessPathType;
import com.starrocks.type.InvalidType;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link DecodeCollector} focusing on ColumnId-based access path matching.
 */
public class DecodeCollectorTest {

    @Test
    public void testCheckComplexTypeInvalidUsesColumnIdForRenamedColumn() throws Exception {
        SessionVariable session = new SessionVariable();
        DecodeCollector collector = new DecodeCollector(session, true);

        // Simulate a renamed complex column: ColumnId = "j", current name = "j_new"
        ColumnRefOperator colRef = new ColumnRefOperator(1, TypeFactory.createVarcharType(10), "j_new", true);
        Column columnMeta = new Column("j", TypeFactory.createVarcharType(10), true, "");
        // Rename the logical column name, ColumnId should remain "j"
        columnMeta.setName("j_new");

        Map<ColumnRefOperator, Column> colRefToMeta = Map.of(colRef, columnMeta);

        // Minimal table stub, DecodeCollector does not depend on table metadata in this path
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

        // Build a ColumnAccessPath whose root path is ColumnId "j" and children are all OFFSET,
        // so checkComplexTypeInvalid should return false when matched correctly.
        ColumnAccessPath root = new ColumnAccessPath(
                TAccessPathType.ROOT,
                columnMeta.getColumnId().getId(),
                InvalidType.INVALID);
        ColumnAccessPath offsetChild = new ColumnAccessPath(
                TAccessPathType.OFFSET,
                ColumnAccessPath.PATH_PLACEHOLDER,
                InvalidType.INVALID);
        root.addChildPath(offsetChild);
        scan.setColumnAccessPaths(List.of(root));

        Method method = DecodeCollector.class.getDeclaredMethod(
                "checkComplexTypeInvalid",
                com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator.class,
                ColumnRefOperator.class);
        method.setAccessible(true);
        boolean valid = (boolean) method.invoke(collector, scan, colRef);

        // When matching by ColumnId, the complex column should be considered invalid for dict optimization.
        Assertions.assertFalse(valid,
                "checkComplexTypeInvalid should return false for renamed complex column when matching by ColumnId");
    }
}


