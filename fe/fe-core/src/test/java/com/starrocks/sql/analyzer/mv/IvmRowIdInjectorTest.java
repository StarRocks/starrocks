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
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.TableName;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmOpUtils;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

/**
 * Tests for {@link IvmRowIdInjector#discriminateUnionBranchRowIds}: each retractable UNION ALL branch's
 * {@code __ROW_ID__} is re-keyed to {@code encode(branch ordinal, keys)} -- reading the keys back out of the
 * branch's own {@code __ROW_ID__} column -- so two branches whose primary keys collide stay distinct MV rows
 * under net-collapse.
 */
public class IvmRowIdInjectorTest {

    // The shape IVMAnalyzer leaves for a retractable branch: __ROW_ID__ = FROM_BINARY(ENCODE_ROW_ID(<keys>))
    // at index 0, then the branch's real outputs. discriminateUnionBranchRowIds reads the keys back out of it.
    private static SelectList branchSelectList(String keyTable, String keyColumn, Type keyType) {
        SlotRef key = new SlotRef(new TableName("db", keyTable), keyColumn);
        key.setType(keyType);
        List<Expr> keys = Lists.<Expr>newArrayList(key);
        FunctionCallExpr rowId = IvmOpUtils.buildRowIdFuncExpr(IvmOpUtils.deduceEncodeRowIdVersion(keys), keys);
        SelectList selectList = new SelectList();
        selectList.addItem(new SelectListItem(rowId, IvmOpUtils.COLUMN_ROW_ID));
        selectList.addItem(new SelectListItem(new IntLiteral(7), "v"));
        return selectList;
    }

    @Test
    public void testDiscriminateUnionBranchRowIdsPrependsBranchOrdinal(
            @Mocked CreateMaterializedViewStatement statement,
            @Mocked SelectRelation branch0, @Mocked SelectRelation branch1) {
        SelectList selectList0 = branchSelectList("a0", "id", IntegerType.BIGINT);
        SelectList selectList1 = branchSelectList("a1", "id", IntegerType.BIGINT);
        new Expectations() {
            {
                branch0.getSelectList();
                result = selectList0;
                minTimes = 0;
                branch0.getOutputExpression();
                result = Lists.newArrayList(selectList0.getItems().get(0).getExpr(), new IntLiteral(7));
                minTimes = 0;

                branch1.getSelectList();
                result = selectList1;
                minTimes = 0;
                branch1.getOutputExpression();
                result = Lists.newArrayList(selectList1.getItems().get(0).getExpr(), new IntLiteral(7));
                minTimes = 0;
            }
        };

        List<QueryRelation> branches = Lists.newArrayList(branch0, branch1);
        IvmRowIdInjector.discriminateUnionBranchRowIds(statement, branches, null);

        assertBranchOrdinal(selectList0, 0, "id");
        assertBranchOrdinal(selectList1, 1, "id");

        new Verifications() {
            {
                statement.setEncodeRowIdVersion(anyInt);
                times = 1;
            }
        };
    }

    @Test
    public void testDiscriminateRejectsBranchMissingRowIdColumn(
            @Mocked CreateMaterializedViewStatement statement, @Mocked SelectRelation branch0) {
        SelectList noRowId = new SelectList();
        noRowId.addItem(new SelectListItem(new IntLiteral(7), "v"));
        new Expectations() {
            {
                branch0.getSelectList();
                result = noRowId;
                minTimes = 0;
            }
        };
        Assertions.assertThrows(IllegalStateException.class,
                () -> IvmRowIdInjector.discriminateUnionBranchRowIds(statement, Lists.newArrayList(branch0), null),
                "a branch whose column 0 is not __ROW_ID__ must be rejected, not silently mis-keyed");
    }


    @ParameterizedTest(name = "{0} branch keys deduce {1}")
    @MethodSource("branchKeyTypes")
    public void testUnionBranchesDeduceFromOrdinalPlusKeys(Type keyType, String expectedEncode,
                                                           @Mocked CreateMaterializedViewStatement statement,
                                                           @Mocked SelectRelation branch0,
                                                           @Mocked SelectRelation branch1) {
        SelectList selectList0 = branchSelectList("a0", "id", keyType);
        SelectList selectList1 = branchSelectList("a1", "id", keyType);
        new Expectations() {
            {
                branch0.getSelectList();
                result = selectList0;
                minTimes = 0;
                branch0.getOutputExpression();
                result = Lists.newArrayList(selectList0.getItems().get(0).getExpr(), new IntLiteral(7));
                minTimes = 0;

                branch1.getSelectList();
                result = selectList1;
                minTimes = 0;
                branch1.getOutputExpression();
                result = Lists.newArrayList(selectList1.getItems().get(0).getExpr(), new IntLiteral(7));
                minTimes = 0;
            }
        };

        IvmRowIdInjector.discriminateUnionBranchRowIds(statement,
                Lists.newArrayList((QueryRelation) branch0, (QueryRelation) branch1), null);

        Assertions.assertEquals(expectedEncode, encodeFunctionName(selectList0));
        Assertions.assertEquals(expectedEncode, encodeFunctionName(selectList1),
                "both branches must share one encoding or their row ids are not comparable");
    }

    /**
     * One encoding has to cover every branch, so a branch the deduction sends to the fingerprint takes the
     * others with it -- row ids built by two different encodings would not be comparable under net-collapse.
     */
    @Test
    public void testOneBranchOnTheFingerprintMovesThemAll(@Mocked CreateMaterializedViewStatement statement,
                                                          @Mocked SelectRelation branch0,
                                                          @Mocked SelectRelation branch1) {
        SelectList narrow = branchSelectList("a0", "id", IntegerType.BIGINT);
        SelectList wide = branchSelectList("a1", "id", TypeFactory.createVarcharType(32));
        new Expectations() {
            {
                branch0.getSelectList();
                result = narrow;
                minTimes = 0;
                branch0.getOutputExpression();
                result = Lists.newArrayList(narrow.getItems().get(0).getExpr(), new IntLiteral(7));
                minTimes = 0;

                branch1.getSelectList();
                result = wide;
                minTimes = 0;
                branch1.getOutputExpression();
                result = Lists.newArrayList(wide.getItems().get(0).getExpr(), new IntLiteral(7));
                minTimes = 0;
            }
        };

        IvmRowIdInjector.discriminateUnionBranchRowIds(statement,
                Lists.newArrayList((QueryRelation) branch0, (QueryRelation) branch1), null);

        Assertions.assertEquals(FunctionSet.ENCODE_FINGERPRINT_SHA256, encodeFunctionName(narrow),
                "the narrow branch must follow the wide one onto the fingerprint");
        Assertions.assertEquals(FunctionSet.ENCODE_FINGERPRINT_SHA256, encodeFunctionName(wide));
    }

    private static Stream<Arguments> branchKeyTypes() {
        return Stream.of(
                // The ordinal is a TINYINT, so it leaves the narrow branch key inside the sort key's budget.
                Arguments.of(IntegerType.BIGINT, FunctionSet.ENCODE_SORT_KEY),
                Arguments.of(TypeFactory.createVarcharType(32), FunctionSet.ENCODE_FINGERPRINT_SHA256));
    }

    private static String encodeFunctionName(SelectList selectList) {
        FunctionCallExpr fromBinary = (FunctionCallExpr) selectList.getItems().get(0).getExpr();
        return ((FunctionCallExpr) fromBinary.getChild(0)).getFunctionName();
    }

    // encode(branch ordinal, <key read back from column 0>) wrapped in FROM_BINARY.
    private static void assertBranchOrdinal(SelectList selectList, long expectedOrdinal, String expectedKeyColumn) {
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
        Expr key = encode.getChild(1);
        Assertions.assertInstanceOf(SlotRef.class, key,
                "the key must be read back from column 0, not re-derived");
        Assertions.assertEquals(expectedKeyColumn, ((SlotRef) key).getColumnName(),
                "the read-back key must be the branch's own primary key column");
    }
}
