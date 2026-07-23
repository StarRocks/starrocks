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

package com.starrocks.sql.analyzer.mv;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TableName;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Tests for {@link IvmRowIdDeriver}: the row id of a retractable projection/filter/join/derived-table MV is
 * built from its base primary keys (an inner/cross join concatenates both sides; a derived table forwards the
 * inner block's keys through the sub-query alias), and every shape this foundation does not maintain (non-PK
 * base, non-cloud-native base, mixed join, non-inner/cross join op, an aggregate / union inner sub-query)
 * derives null so the analyzer rejects it at CREATE.
 */
public class IvmRowIdDeriverTest {

    private static Column keyColumn(String name) {
        Column column = new Column(name, IntegerType.INT, false);
        column.setIsKey(true);
        return column;
    }

    private static Column valueColumn(String name) {
        Column column = new Column(name, IntegerType.INT, false);
        column.setIsKey(false);
        return column;
    }

    @Test
    public void testCloudNativePrimaryKeyTableYieldsKeyColumns(
            @Mocked TableRelation tableRelation, @Mocked OlapTable table) {
        new Expectations() {
            {
                tableRelation.getTable();
                result = table;
                minTimes = 0;
                tableRelation.getResolveTableName();
                result = new TableName("test_db", "pk_t");
                minTimes = 0;
                table.isCloudNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;
                table.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                minTimes = 0;
                table.getBaseSchema();
                result = List.of(keyColumn("id"), valueColumn("v"));
                minTimes = 0;
            }
        };

        List<Expr> keys = IvmRowIdDeriver.deriveRowIdKeys(tableRelation);
        Assertions.assertNotNull(keys);
        Assertions.assertEquals(1, keys.size(), "only the key column is part of the row id");
        Assertions.assertInstanceOf(SlotRef.class, keys.get(0));
        Assertions.assertEquals("id", ((SlotRef) keys.get(0)).getColumnName());
    }

    @Test
    public void testSelectRelationForwardsInnerTableKeys(
            @Mocked SelectRelation select, @Mocked TableRelation tableRelation, @Mocked OlapTable table) {
        new Expectations() {
            {
                select.getRelation();
                result = tableRelation;
                minTimes = 0;
                tableRelation.getTable();
                result = table;
                minTimes = 0;
                tableRelation.getResolveTableName();
                result = new TableName("test_db", "pk_t");
                minTimes = 0;
                table.isCloudNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;
                table.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                minTimes = 0;
                table.getBaseSchema();
                result = List.of(keyColumn("id"));
                minTimes = 0;
            }
        };

        List<Expr> keys = IvmRowIdDeriver.deriveRowIdKeys(select);
        Assertions.assertNotNull(keys, "a projection/filter forwards its FROM table's identity");
        Assertions.assertEquals("id", ((SlotRef) keys.get(0)).getColumnName());
    }

    @Test
    public void testNonPrimaryKeyTableYieldsNull(@Mocked TableRelation tableRelation, @Mocked OlapTable table) {
        new Expectations() {
            {
                tableRelation.getTable();
                result = table;
                minTimes = 0;
                table.isCloudNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;
                table.getKeysType();
                result = KeysType.DUP_KEYS;
                minTimes = 0;
            }
        };
        Assertions.assertNull(IvmRowIdDeriver.deriveRowIdKeys(tableRelation));
    }

    @Test
    public void testNonCloudNativeTableYieldsNull(@Mocked TableRelation tableRelation, @Mocked OlapTable table) {
        new Expectations() {
            {
                tableRelation.getTable();
                result = table;
                minTimes = 0;
                table.isCloudNativeTableOrMaterializedView();
                result = false;
                minTimes = 0;
                table.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                minTimes = 0;
            }
        };
        Assertions.assertNull(IvmRowIdDeriver.deriveRowIdKeys(tableRelation));
    }

    @Test
    public void testInnerJoinConcatenatesBothSidesKeys(
            @Mocked JoinRelation join,
            @Mocked TableRelation leftRel, @Mocked OlapTable leftTable,
            @Mocked TableRelation rightRel, @Mocked OlapTable rightTable) {
        new Expectations() {
            {
                join.getJoinOp();
                result = JoinOperator.INNER_JOIN;
                minTimes = 0;
                join.getLeft();
                result = leftRel;
                minTimes = 0;
                join.getRight();
                result = rightRel;
                minTimes = 0;
                leftRel.getTable();
                result = leftTable;
                minTimes = 0;
                leftRel.getResolveTableName();
                result = new TableName("test_db", "a");
                minTimes = 0;
                leftTable.isCloudNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;
                leftTable.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                minTimes = 0;
                leftTable.getBaseSchema();
                result = List.of(keyColumn("a_id"));
                minTimes = 0;
                rightRel.getTable();
                result = rightTable;
                minTimes = 0;
                rightRel.getResolveTableName();
                result = new TableName("test_db", "b");
                minTimes = 0;
                rightTable.isCloudNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;
                rightTable.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                minTimes = 0;
                rightTable.getBaseSchema();
                result = List.of(keyColumn("b_id"));
                minTimes = 0;
            }
        };
        List<Expr> keys = IvmRowIdDeriver.deriveRowIdKeys(join);
        Assertions.assertNotNull(keys, "an inner join of two PK bases has a concatenated row id");
        Assertions.assertEquals(2, keys.size(), "both sides' key columns form the join identity");
        Assertions.assertEquals("a_id", ((SlotRef) keys.get(0)).getColumnName());
        Assertions.assertEquals("b_id", ((SlotRef) keys.get(1)).getColumnName());
    }

    @Test
    public void testMixedJoinYieldsNull(
            @Mocked JoinRelation join,
            @Mocked TableRelation leftRel, @Mocked OlapTable leftTable,
            @Mocked TableRelation rightRel, @Mocked OlapTable rightTable) {
        // One side is not a cloud-native PK base, so it has no row id; the whole join is unmaintainable.
        new Expectations() {
            {
                join.getJoinOp();
                result = JoinOperator.INNER_JOIN;
                minTimes = 0;
                join.getLeft();
                result = leftRel;
                minTimes = 0;
                join.getRight();
                result = rightRel;
                minTimes = 0;
                leftRel.getTable();
                result = leftTable;
                minTimes = 0;
                leftRel.getResolveTableName();
                result = new TableName("test_db", "a");
                minTimes = 0;
                leftTable.isCloudNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;
                leftTable.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                minTimes = 0;
                leftTable.getBaseSchema();
                result = List.of(keyColumn("a_id"));
                minTimes = 0;
                rightRel.getTable();
                result = rightTable;
                minTimes = 0;
                rightTable.isCloudNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;
                rightTable.getKeysType();
                result = KeysType.DUP_KEYS;
                minTimes = 0;
            }
        };
        Assertions.assertNull(IvmRowIdDeriver.deriveRowIdKeys(join),
                "a join with a non-PK side is not retractably maintainable");
    }

    @Test
    public void testCrossJoinConcatenatesBothSidesKeys(
            @Mocked JoinRelation join,
            @Mocked TableRelation leftRel, @Mocked OlapTable leftTable,
            @Mocked TableRelation rightRel, @Mocked OlapTable rightTable) {
        new Expectations() {
            {
                join.getJoinOp();
                result = JoinOperator.CROSS_JOIN;
                minTimes = 0;
                join.getLeft();
                result = leftRel;
                minTimes = 0;
                join.getRight();
                result = rightRel;
                minTimes = 0;
                leftRel.getTable();
                result = leftTable;
                minTimes = 0;
                leftRel.getResolveTableName();
                result = new TableName("test_db", "a");
                minTimes = 0;
                leftTable.isCloudNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;
                leftTable.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                minTimes = 0;
                leftTable.getBaseSchema();
                result = List.of(keyColumn("a_id"));
                minTimes = 0;
                rightRel.getTable();
                result = rightTable;
                minTimes = 0;
                rightRel.getResolveTableName();
                result = new TableName("test_db", "b");
                minTimes = 0;
                rightTable.isCloudNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;
                rightTable.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                minTimes = 0;
                rightTable.getBaseSchema();
                result = List.of(keyColumn("b_id"));
                minTimes = 0;
            }
        };
        List<Expr> keys = IvmRowIdDeriver.deriveRowIdKeys(join);
        Assertions.assertNotNull(keys, "a cross join of two PK bases has a concatenated row id");
        Assertions.assertEquals(2, keys.size(), "both sides' key columns form the join identity");
        Assertions.assertEquals("a_id", ((SlotRef) keys.get(0)).getColumnName());
        Assertions.assertEquals("b_id", ((SlotRef) keys.get(1)).getColumnName());
    }

    @Test
    public void testUnsupportedJoinOpYieldsNull(@Mocked JoinRelation join) {
        new Expectations() {
            {
                join.getJoinOp();
                result = JoinOperator.LEFT_OUTER_JOIN;
                minTimes = 0;
            }
        };
        Assertions.assertNull(IvmRowIdDeriver.deriveRowIdKeys(join),
                "a non-inner/cross join op has no maintainable row id");
    }

    @Test
    public void testSubqueryOverAggregateYieldsNull(@Mocked SubqueryRelation subquery, @Mocked QueryStatement qs,
                                                    @Mocked SelectRelation innerSelect) {
        new Expectations() {
            {
                subquery.getQueryStatement();
                result = qs;
                minTimes = 0;
                qs.getQueryRelation();
                result = innerSelect;
                minTimes = 0;
                innerSelect.getGroupBy();
                result = List.of(new SlotRef(new TableName("db", "t"), "id"));
                minTimes = 0;
            }
        };
        Assertions.assertNull(IvmRowIdDeriver.deriveRowIdKeys(subquery),
                "a sub-query over an aggregate inner (identity is the group keys) is a deferred combination");
    }

    @Test
    public void testSubqueryOverNestedSubqueryYieldsNull(@Mocked SubqueryRelation subquery, @Mocked QueryStatement qs,
                                                         @Mocked SelectRelation innerSelect,
                                                         @Mocked SubqueryRelation innerFrom) {
        new Expectations() {
            {
                subquery.getQueryStatement();
                result = qs;
                minTimes = 0;
                qs.getQueryRelation();
                result = innerSelect;
                minTimes = 0;
                innerSelect.getRelation();
                result = innerFrom;
                minTimes = 0;
            }
        };
        Assertions.assertNull(IvmRowIdDeriver.deriveRowIdKeys(subquery),
                "a nested derived table (a sub-query inside the sub-query) is a deferred combination");
    }

    @Test
    public void testSubqueryOverUnionYieldsNull(@Mocked SubqueryRelation subquery, @Mocked QueryStatement qs,
                                                @Mocked UnionRelation innerUnion) {
        new Expectations() {
            {
                subquery.getQueryStatement();
                result = qs;
                minTimes = 0;
                qs.getQueryRelation();
                result = innerUnion;
                minTimes = 0;
            }
        };
        Assertions.assertNull(IvmRowIdDeriver.deriveRowIdKeys(subquery),
                "a sub-query over a union (multi-input) is a deferred combination");
    }

    @Test
    public void testSubqueryForwardsInnerPkKeys(@Mocked SubqueryRelation subquery, @Mocked QueryStatement qs,
                                                @Mocked SelectRelation inner, @Mocked TableRelation tableRelation,
                                                @Mocked OlapTable table) {
        SelectList innerSelectList = new SelectList();
        innerSelectList.addItem(new SelectListItem(new SlotRef(new TableName("db", "pk_t"), "id"), "id"));
        new Expectations() {
            {
                subquery.getQueryStatement();
                result = qs;
                minTimes = 0;
                qs.getQueryRelation();
                result = inner;
                minTimes = 0;
                inner.getRelation();
                result = tableRelation;
                minTimes = 0;
                tableRelation.getTable();
                result = table;
                minTimes = 0;
                tableRelation.getResolveTableName();
                result = new TableName("db", "pk_t");
                minTimes = 0;
                table.isCloudNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;
                table.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                minTimes = 0;
                table.getBaseSchema();
                result = List.of(keyColumn("id"));
                minTimes = 0;
                inner.getSelectList();
                result = innerSelectList;
                minTimes = 0;
                inner.getOutputExpression();
                result = List.of(new SlotRef(new TableName("db", "pk_t"), "id"));
                minTimes = 0;
                subquery.getResolveTableName();
                result = new TableName(null, "t");
                minTimes = 0;
            }
        };

        List<Expr> keys = IvmRowIdDeriver.deriveRowIdKeys(subquery);
        Assertions.assertNotNull(keys, "a derived table over a PK base forwards the inner key");
        Assertions.assertEquals(1, keys.size());
        Assertions.assertInstanceOf(SlotRef.class, keys.get(0));
        Assertions.assertEquals("__rowid_key_0__", ((SlotRef) keys.get(0)).getColumnName(),
                "the forwarded key references the exposed inner output column");
    }
}
