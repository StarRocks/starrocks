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

package com.starrocks.sql;

import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.catalog.Column;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.OptExprBuilder;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Covers {@link InsertPlanner#fillShadowColumns} for a {@code __starrocks_shadow_}-prefixed column with
 * no same-named origin (a genuinely new added column, e.g. a range ADD-key column): it must materialize
 * the column's CONST default instead of crashing, while the pre-existing origin-present (HASH
 * MODIFY-COLUMN type-change) cast path stays byte-identical.
 *
 * <p>{@code fillShadowColumns} is private and reads/writes {@code InsertPlanner}'s own
 * {@code outputBaseSchema}/{@code outputFullSchema} fields, so these tests call it via reflection with a
 * minimal hand-built {@link OptExprBuilder} rather than driving the whole planner pipeline.
 */
public class InsertPlannerShadowAddColumnTest {

    private static void setPrivateField(Object target, String name, Object value) throws Exception {
        Field field = InsertPlanner.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    /**
     * Invokes the private {@code fillShadowColumns} and returns the resulting projection's column-ref map.
     * {@code outputColumns} is mutated in place by the call (columns are appended), mirroring the real
     * pipeline, so the caller can identify a newly materialized column via its new tail entry.
     */
    private static Map<ColumnRefOperator, ScalarOperator> invokeFillShadowColumns(
            InsertPlanner planner, ColumnRefFactory columnRefFactory, List<ColumnRefOperator> outputColumns,
            OptExprBuilder root) throws Exception {
        Method method = InsertPlanner.class.getDeclaredMethod("fillShadowColumns", ColumnRefFactory.class,
                InsertStmt.class, List.class, OptExprBuilder.class, ConnectContext.class);
        method.setAccessible(true);
        OptExprBuilder result = (OptExprBuilder) method.invoke(planner, columnRefFactory, null, outputColumns, root, null);
        LogicalProjectOperator project = (LogicalProjectOperator) result.getRoot().getOp();
        return project.getColumnRefMap();
    }

    private static OptExprBuilder emptyRoot(List<ColumnRefOperator> baseColumnRefs) {
        return new OptExprBuilder(new LogicalValuesOperator(baseColumnRefs), Collections.emptyList(),
                new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields())));
    }

    @Test
    public void testFillShadowColumnsNoOriginMaterializesDefault() throws Exception {
        // outputFullSchema mirrors OlapTable#rebuildFullSchema()'s real layout: base columns (k1, v1)
        // first, then the extra shadow column appended at the end -- __starrocks_shadow_k2 has NO
        // same-named origin (a genuinely new ADD), so it must resolve to its CONST default, not crash.
        Column k1 = new Column("k1", IntegerType.INT);
        Column v1 = new Column("v1", IntegerType.INT);
        Column shadowK2 = new Column(SchemaChangeHandler.SHADOW_NAME_PREFIX + "k2", IntegerType.INT, true, null, "0", "");
        List<Column> outputBaseSchema = List.of(k1, v1);
        List<Column> outputFullSchema = List.of(k1, v1, shadowK2);

        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        ColumnRefOperator colK1 = columnRefFactory.create("k1", IntegerType.INT, true);
        ColumnRefOperator colV1 = columnRefFactory.create("v1", IntegerType.INT, true);
        List<ColumnRefOperator> outputColumns = new ArrayList<>(List.of(colK1, colV1));

        InsertPlanner planner = new InsertPlanner();
        setPrivateField(planner, "outputBaseSchema", outputBaseSchema);
        setPrivateField(planner, "outputFullSchema", outputFullSchema);

        Map<ColumnRefOperator, ScalarOperator> columnRefMap =
                invokeFillShadowColumns(planner, columnRefFactory, outputColumns, emptyRoot(outputColumns));

        // materializeShadowColumnDefault() appended the new column ref for the shadow column at the end.
        Assertions.assertEquals(3, outputColumns.size());
        ScalarOperator materialized = columnRefMap.get(outputColumns.get(2));
        Assertions.assertTrue(materialized instanceof ConstantOperator, "must materialize a constant, not crash");
        Assertions.assertEquals("0", ((ConstantOperator) materialized).getVarchar());
    }

    @Test
    public void testFillShadowColumnsWithOriginUnchanged() throws Exception {
        // A prefixed column WITH a same-named origin (HASH MODIFY-COLUMN type-change) must still cast
        // the origin column's value -- byte-identical to pre-existing behavior.
        Column k1 = new Column("k1", IntegerType.INT);
        Column c = new Column("c", IntegerType.INT);
        Column shadowC = new Column(SchemaChangeHandler.SHADOW_NAME_PREFIX + "c", IntegerType.BIGINT);
        List<Column> outputBaseSchema = List.of(k1, c);
        List<Column> outputFullSchema = List.of(k1, c, shadowC);

        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        ColumnRefOperator colK1 = columnRefFactory.create("k1", IntegerType.INT, true);
        ColumnRefOperator colC = columnRefFactory.create("c", IntegerType.INT, true);
        List<ColumnRefOperator> outputColumns = new ArrayList<>(List.of(colK1, colC));

        InsertPlanner planner = new InsertPlanner();
        setPrivateField(planner, "outputBaseSchema", outputBaseSchema);
        setPrivateField(planner, "outputFullSchema", outputFullSchema);

        Map<ColumnRefOperator, ScalarOperator> columnRefMap =
                invokeFillShadowColumns(planner, columnRefFactory, outputColumns, emptyRoot(outputColumns));

        Assertions.assertEquals(3, outputColumns.size());
        ScalarOperator shadowCValue = columnRefMap.get(outputColumns.get(2));
        Assertions.assertTrue(shadowCValue instanceof CastOperator, "cast(origin c) -- unchanged");
        Assertions.assertSame(colC, ((CastOperator) shadowCValue).getChild(0));
    }
}
