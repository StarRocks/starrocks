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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TableName;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmOpUtils;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Tests for {@link IvmRowIdInjector#discriminateUnionBranchRowIds}: each retractable UNION ALL branch's
 * {@code __ROW_ID__} must be re-keyed to {@code encode(branch ordinal, keys)} so two branches whose primary
 * keys collide stay distinct MV rows under net-collapse.
 */
public class IvmRowIdInjectorTest {

    private static Column keyColumn(String name) {
        Column column = new Column(name, IntegerType.INT, false);
        column.setIsKey(true);
        return column;
    }

    // The shape IVMAnalyzer leaves for a branch: __ROW_ID__ at index 0, then the branch's real outputs.
    private static SelectList branchSelectList() {
        SelectList selectList = new SelectList();
        selectList.addItem(new SelectListItem(new IntLiteral(0), IvmOpUtils.COLUMN_ROW_ID));
        selectList.addItem(new SelectListItem(new IntLiteral(7), "v"));
        return selectList;
    }

    @Test
    public void testDiscriminateUnionBranchRowIdsPrependsBranchOrdinal(
            @Mocked CreateMaterializedViewStatement statement,
            @Mocked SelectRelation branch0, @Mocked TableRelation rel0, @Mocked OlapTable table0,
            @Mocked SelectRelation branch1, @Mocked TableRelation rel1, @Mocked OlapTable table1) {
        SelectList selectList0 = branchSelectList();
        SelectList selectList1 = branchSelectList();
        new Expectations() {
            {
                branch0.getRelation();
                result = rel0;
                minTimes = 0;
                rel0.getTable();
                result = table0;
                minTimes = 0;
                rel0.getResolveTableName();
                result = new TableName("db", "a0");
                minTimes = 0;
                table0.isCloudNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;
                table0.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                minTimes = 0;
                table0.getBaseSchema();
                result = List.of(keyColumn("id"));
                minTimes = 0;
                branch0.getSelectList();
                result = selectList0;
                minTimes = 0;
                branch0.getOutputExpression();
                result = Lists.newArrayList(new IntLiteral(0), new IntLiteral(7));
                minTimes = 0;

                branch1.getRelation();
                result = rel1;
                minTimes = 0;
                rel1.getTable();
                result = table1;
                minTimes = 0;
                rel1.getResolveTableName();
                result = new TableName("db", "a1");
                minTimes = 0;
                table1.isCloudNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;
                table1.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                minTimes = 0;
                table1.getBaseSchema();
                result = List.of(keyColumn("id"));
                minTimes = 0;
                branch1.getSelectList();
                result = selectList1;
                minTimes = 0;
                branch1.getOutputExpression();
                result = Lists.newArrayList(new IntLiteral(0), new IntLiteral(7));
                minTimes = 0;
            }
        };

        List<QueryRelation> branches = Lists.newArrayList(branch0, branch1);
        IvmRowIdInjector.discriminateUnionBranchRowIds(statement, branches);

        assertBranchOrdinal(selectList0, 0);
        assertBranchOrdinal(selectList1, 1);

        new Verifications() {
            {
                statement.setEncodeRowIdVersion(anyInt);
                times = 1;
            }
        };
    }

    private static void assertBranchOrdinal(SelectList selectList, long expectedOrdinal) {
        Expr rowIdExpr = selectList.getItems().get(0).getExpr();
        Assertions.assertInstanceOf(FunctionCallExpr.class, rowIdExpr);
        FunctionCallExpr fromBinary = (FunctionCallExpr) rowIdExpr;
        Assertions.assertTrue(FunctionSet.FROM_BINARY.equalsIgnoreCase(fromBinary.getFunctionName()),
                "the row id is FROM_BINARY(encode(...)), got: " + fromBinary.getFunctionName());
        Assertions.assertInstanceOf(FunctionCallExpr.class, fromBinary.getChild(0));
        FunctionCallExpr encode = (FunctionCallExpr) fromBinary.getChild(0);
        Expr discriminant = encode.getChild(0);
        Assertions.assertInstanceOf(IntLiteral.class, discriminant,
                "the encode must lead with the branch ordinal constant");
        Assertions.assertEquals(expectedOrdinal, ((IntLiteral) discriminant).getValue(),
                "branch " + expectedOrdinal + " must lead its encode with its ordinal");
    }
}
