// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/CreateMaterializedViewStmtTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

// TODO: refactor these test case with SQL style
public class CreateMaterializedViewStmtTest {

    @Mocked
    private Analyzer analyzer;
    @Mocked
    private ExprSubstitutionMap exprSubstitutionMap;
    @Mocked
    private ConnectContext connectContext;
    @Mocked
    private Config config;
    @Mocked
    private CatalogUtils catalogUtils;

    @Test
    public void testFunctionColumnInSelectClause(@Injectable ArithmeticExpr arithmeticExpr) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem = new SelectListItem(arithmeticExpr, null);
        selectList.addItem(selectListItem);
        FromClause fromClause = new FromClause();
        SelectStmt selectStmt = new SelectStmt(selectList, fromClause, null, null, null, null, LimitElement.NO_LIMIT);

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
            }
        };
        CreateMaterializedViewStmt createMaterializedViewStmt =
                new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testFunctionColumnShouldNullable(@Injectable SlotRef slotRef, @Injectable TableRef tableRef,
                                                 @Injectable SelectStmt selectStmt, @Injectable Column column,
                                                 @Injectable SlotDescriptor slotDescriptor) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem = new SelectListItem(slotRef, null);
        selectList.addItem(selectListItem);

        TableName tableName = new TableName("db", "table");
        SlotRef slotRef2 = new SlotRef(tableName, "v1");
        Deencapsulation.setField(slotRef2, "desc", slotDescriptor);
        slotRef2.setType(Type.INT);
        List<Expr> fnChildren = Lists.newArrayList(slotRef2);
        FunctionCallExpr toBitMapFunc = new FunctionCallExpr(FunctionSet.TO_BITMAP, fnChildren);
        toBitMapFunc.setFn(Expr.getBuiltinFunction(FunctionSet.TO_BITMAP, new Type[] {Type.BIGINT},
                Function.CompareMode.IS_SUPERTYPE_OF));
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(FunctionSet.BITMAP_UNION,
                Lists.newArrayList(toBitMapFunc));
        functionCallExpr.setFn(Expr.getBuiltinFunction(FunctionSet.BITMAP_UNION, new Type[] {Type.BITMAP},
                Function.CompareMode.IS_SUPERTYPE_OF));
        SelectListItem selectListItem2 = new SelectListItem(functionCallExpr, null);
        selectList.addItem(selectListItem2);

        OrderByElement orderByElement1 = new OrderByElement(slotRef, false, false);
        ArrayList<OrderByElement> orderByElementList = Lists.newArrayList(orderByElement1);

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.analyze(analyzer);
                selectStmt.getSelectList();
                result = selectList;
                selectStmt.getTableRefs();
                result = Lists.newArrayList(tableRef);
                selectStmt.getWhereClause();
                result = null;
                selectStmt.getHavingPred();
                result = null;
                selectStmt.getOrderByElements();
                result = orderByElementList;
                slotRef.getColumnName();
                result = "k1";
                slotDescriptor.getColumn();
                result = column;
                column.getType();
                result = Type.INT;
            }
        };
        CreateMaterializedViewStmt createMaterializedViewStmt =
                new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            System.out.print(e.getMessage());
        }
        Assert.assertTrue(createMaterializedViewStmt.getMVColumnItemList().get(1).isAllowNull());
    }

    @Test
    public void testAggregateWithFunctionColumnInSelectClause(@Injectable ArithmeticExpr arithmeticExpr,
                                                              @Injectable SelectStmt selectStmt) throws UserException {
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(FunctionSet.SUM, Lists.newArrayList(arithmeticExpr));
        functionCallExpr.setFn(Expr.getBuiltinFunction(
                FunctionSet.SUM, new Type[] {Type.BIGINT}, Function.CompareMode.IS_SUPERTYPE_OF));
        SelectList selectList = new SelectList();
        SelectListItem selectListItem = new SelectListItem(functionCallExpr, null);
        selectList.addItem(selectListItem);
        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.analyze(analyzer);
                selectStmt.getSelectList();
                result = selectList;
                arithmeticExpr.toString();
                result = "a+b";
            }
        };

        CreateMaterializedViewStmt createMaterializedViewStmt =
                new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testJoinSelectClause(@Injectable SlotRef slotRef,
                                     @Injectable TableRef tableRef1,
                                     @Injectable TableRef tableRef2,
                                     @Injectable SelectStmt selectStmt) throws UserException {
        SelectListItem selectListItem = new SelectListItem(slotRef, null);
        SelectList selectList = new SelectList();
        selectList.addItem(selectListItem);
        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.analyze(analyzer);
                selectStmt.getTableRefs();
                result = Lists.newArrayList(tableRef1, tableRef2);
                selectStmt.getSelectList();
                result = selectList;
                slotRef.getColumnName();
                result = "k1";
            }
        };

        CreateMaterializedViewStmt createMaterializedViewStmt =
                new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testSelectClauseWithWhereClause(@Injectable SlotRef slotRef,
                                                @Injectable TableRef tableRef,
                                                @Injectable Expr whereClause,
                                                @Injectable SelectStmt selectStmt) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem = new SelectListItem(slotRef, null);
        selectList.addItem(selectListItem);
        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.analyze(analyzer);
                selectStmt.getSelectList();
                result = selectList;
                selectStmt.getTableRefs();
                result = Lists.newArrayList(tableRef);
                selectStmt.getWhereClause();
                result = whereClause;
                slotRef.getColumnName();
                result = "k1";
            }
        };

        CreateMaterializedViewStmt createMaterializedViewStmt =
                new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testSelectClauseWithHavingClause(@Injectable SlotRef slotRef,
                                                 @Injectable TableRef tableRef,
                                                 @Injectable Expr havingClause,
                                                 @Injectable SelectStmt selectStmt) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem = new SelectListItem(slotRef, null);
        selectList.addItem(selectListItem);

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.analyze(analyzer);
                selectStmt.getSelectList();
                result = selectList;
                selectStmt.getTableRefs();
                result = Lists.newArrayList(tableRef);
                selectStmt.getWhereClause();
                result = null;
                selectStmt.getHavingPred();
                result = havingClause;
                slotRef.getColumnName();
                result = "k1";
            }
        };
        CreateMaterializedViewStmt createMaterializedViewStmt =
                new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testOrderOfColumn(@Injectable SlotRef slotRef1,
                                  @Injectable SlotRef slotRef2,
                                  @Injectable TableRef tableRef,
                                  @Injectable SelectStmt selectStmt) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem1 = new SelectListItem(slotRef1, null);
        selectList.addItem(selectListItem1);
        SelectListItem selectListItem2 = new SelectListItem(slotRef2, null);
        selectList.addItem(selectListItem2);
        OrderByElement orderByElement1 = new OrderByElement(slotRef2, false, false);
        OrderByElement orderByElement2 = new OrderByElement(slotRef1, false, false);
        ArrayList<OrderByElement> orderByElementList = Lists.newArrayList(orderByElement1, orderByElement2);

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.analyze(analyzer);
                selectStmt.getSelectList();
                result = selectList;
                selectStmt.getTableRefs();
                result = Lists.newArrayList(tableRef);
                selectStmt.getWhereClause();
                result = null;
                selectStmt.getHavingPred();
                result = null;
                selectStmt.getOrderByElements();
                result = orderByElementList;
                slotRef1.getColumnName();
                result = "k1";
                slotRef2.getColumnName();
                result = "k2";
            }
        };
        CreateMaterializedViewStmt createMaterializedViewStmt =
                new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testOrderByAggregateColumn(@Injectable SlotRef slotRef1,
                                           @Injectable TableRef tableRef,
                                           @Injectable SelectStmt selectStmt,
                                           @Injectable Column column2,
                                           @Injectable SlotDescriptor slotDescriptor) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem1 = new SelectListItem(slotRef1, null);
        selectList.addItem(selectListItem1);
        TableName tableName = new TableName("db", "table");
        SlotRef slotRef2 = new SlotRef(tableName, "v1");
        Deencapsulation.setField(slotRef2, "desc", slotDescriptor);
        List<Expr> fnChildren = Lists.newArrayList(slotRef2);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(FunctionSet.SUM, fnChildren);
        functionCallExpr.setFn(Expr.getBuiltinFunction(
                FunctionSet.SUM, new Type[] {Type.BIGINT}, Function.CompareMode.IS_SUPERTYPE_OF));
        SelectListItem selectListItem2 = new SelectListItem(functionCallExpr, null);
        selectList.addItem(selectListItem2);
        OrderByElement orderByElement1 = new OrderByElement(functionCallExpr, false, false);
        OrderByElement orderByElement2 = new OrderByElement(slotRef1, false, false);
        ArrayList<OrderByElement> orderByElementList = Lists.newArrayList(orderByElement1, orderByElement2);

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.analyze(analyzer);
                selectStmt.getSelectList();
                result = selectList;
                selectStmt.getTableRefs();
                result = Lists.newArrayList(tableRef);
                selectStmt.getWhereClause();
                result = null;
                selectStmt.getHavingPred();
                result = null;
                selectStmt.getOrderByElements();
                result = orderByElementList;
                slotRef1.getColumnName();
                result = "k1";
                slotDescriptor.getColumn();
                result = column2;
                column2.getType();
                result = Type.INT;
            }
        };
        CreateMaterializedViewStmt createMaterializedViewStmt =
                new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testDuplicateColumn(@Injectable SelectStmt selectStmt) throws UserException {
        SelectList selectList = new SelectList();
        TableName tableName = new TableName("db", "table");
        SlotRef slotRef1 = new SlotRef(tableName, "k1");
        SelectListItem selectListItem1 = new SelectListItem(slotRef1, null);
        selectList.addItem(selectListItem1);
        List<Expr> fnChildren = Lists.newArrayList(slotRef1);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(FunctionSet.SUM, fnChildren);
        functionCallExpr.setFn(Expr.getBuiltinFunction(
                FunctionSet.SUM, new Type[] {Type.BIGINT}, Function.CompareMode.IS_SUPERTYPE_OF));
        SelectListItem selectListItem2 = new SelectListItem(functionCallExpr, null);
        selectList.addItem(selectListItem2);

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.analyze(analyzer);
                selectStmt.getSelectList();
                result = selectList;
            }
        };
        CreateMaterializedViewStmt createMaterializedViewStmt =
                new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testDuplicateColumn1(@Injectable SlotRef slotRef1,
                                     @Injectable SelectStmt selectStmt,
                                     @Injectable Column column2,
                                     @Injectable SlotDescriptor slotDescriptor) throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem1 = new SelectListItem(slotRef1, null);
        selectList.addItem(selectListItem1);
        TableName tableName = new TableName("db", "table");
        SlotRef slotRef2 = new SlotRef(tableName, "k2");
        Deencapsulation.setField(slotRef2, "desc", slotDescriptor);
        List<Expr> fn1Children = Lists.newArrayList(slotRef2);
        FunctionCallExpr functionCallExpr1 = new FunctionCallExpr(FunctionSet.SUM, fn1Children);
        functionCallExpr1.setFn(Expr.getBuiltinFunction(
                FunctionSet.SUM, new Type[] {Type.BIGINT}, Function.CompareMode.IS_SUPERTYPE_OF));
        SelectListItem selectListItem2 = new SelectListItem(functionCallExpr1, null);
        selectList.addItem(selectListItem2);
        FunctionCallExpr functionCallExpr2 = new FunctionCallExpr(FunctionSet.MAX, fn1Children);
        functionCallExpr2.setFn(
                Expr.getBuiltinFunction(FunctionSet.MAX, new Type[] {Type.BIGINT},
                        Function.CompareMode.IS_SUPERTYPE_OF));
        SelectListItem selectListItem3 = new SelectListItem(functionCallExpr2, null);
        selectList.addItem(selectListItem3);

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.analyze(analyzer);
                selectStmt.getSelectList();
                result = selectList;
                slotRef1.getColumnName();
                result = "k1";
                slotDescriptor.getColumn();
                result = column2;
                column2.getType();
                result = Type.INT;
            }
        };
        CreateMaterializedViewStmt createMaterializedViewStmt =
                new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testOrderByColumnsLessThenGroupByColumns(@Injectable SlotRef slotRef1,
                                                         @Injectable SlotRef slotRef2,
                                                         @Injectable TableRef tableRef,
                                                         @Injectable SelectStmt selectStmt,
                                                         @Injectable Column column3,
                                                         @Injectable SlotDescriptor slotDescriptor)
            throws UserException {
        SelectList selectList = new SelectList();
        SelectListItem selectListItem1 = new SelectListItem(slotRef1, null);
        selectList.addItem(selectListItem1);
        SelectListItem selectListItem2 = new SelectListItem(slotRef2, null);
        selectList.addItem(selectListItem2);
        TableName tableName = new TableName("db", "table");
        SlotRef functionChild0 = new SlotRef(tableName, "v1");
        Deencapsulation.setField(functionChild0, "desc", slotDescriptor);
        List<Expr> fn1Children = Lists.newArrayList(functionChild0);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(FunctionSet.SUM, fn1Children);
        functionCallExpr.setFn(Expr.getBuiltinFunction(
                FunctionSet.SUM, new Type[] {Type.BIGINT}, Function.CompareMode.IS_SUPERTYPE_OF));
        SelectListItem selectListItem3 = new SelectListItem(functionCallExpr, null);
        selectList.addItem(selectListItem3);
        OrderByElement orderByElement1 = new OrderByElement(slotRef1, false, false);
        ArrayList<OrderByElement> orderByElementList = Lists.newArrayList(orderByElement1);

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default";
                selectStmt.analyze(analyzer);
                selectStmt.getSelectList();
                result = selectList;
                selectStmt.getTableRefs();
                result = Lists.newArrayList(tableRef);
                selectStmt.getWhereClause();
                result = null;
                selectStmt.getHavingPred();
                result = null;
                selectStmt.getOrderByElements();
                result = orderByElementList;
                slotRef1.getColumnName();
                result = "k1";
                slotRef2.getColumnName();
                result = "non-k2";
                slotDescriptor.getColumn();
                result = column3;
                column3.getType();
                result = Type.INT;
            }
        };
        CreateMaterializedViewStmt createMaterializedViewStmt =
                new CreateMaterializedViewStmt("test", selectStmt, null);
        try {
            createMaterializedViewStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testKeepScaleAndPrecisionOfType(@Injectable SelectStmt selectStmt,
                                                @Injectable SlotDescriptor slotDescriptor1,
                                                @Injectable Column column1,
                                                @Injectable SlotDescriptor slotDescriptor2,
                                                @Injectable Column column2,
                                                @Injectable SlotDescriptor slotDescriptor3,
                                                @Injectable Column column3) {
        CreateMaterializedViewStmt createMaterializedViewStmt =
                new CreateMaterializedViewStmt("test", selectStmt, null);
        SlotRef slotRef = new SlotRef(new TableName("db", "table"), "a");
        List<Expr> params = Lists.newArrayList();
        params.add(slotRef);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(FunctionSet.MIN, params);
        Deencapsulation.setField(slotRef, "desc", slotDescriptor1);
        new Expectations() {
            {
                slotDescriptor1.getColumn();
                result = column1;
                column1.getType();
                result = ScalarType.createVarchar(50);
            }
        };
        MVColumnItem mvColumnItem =
                Deencapsulation.invoke(createMaterializedViewStmt, "buildMVColumnItem", functionCallExpr);
        Assert.assertEquals(50, ((ScalarType) mvColumnItem.getType()).getLength());

        SlotRef slotRef2 = new SlotRef(new TableName("db", "table"), "a");
        List<Expr> params2 = Lists.newArrayList();
        params2.add(slotRef2);
        FunctionCallExpr functionCallExpr2 = new FunctionCallExpr(FunctionSet.MIN, params2);
        Deencapsulation.setField(slotRef2, "desc", slotDescriptor2);
        new Expectations() {
            {
                slotDescriptor2.getColumn();
                result = column2;
                column2.getType();
                result = ScalarType.createDecimalV2Type(10, 1);
            }
        };
        MVColumnItem mvColumnItem2 =
                Deencapsulation.invoke(createMaterializedViewStmt, "buildMVColumnItem", functionCallExpr2);
        Assert.assertEquals(new Integer(10), ((ScalarType) mvColumnItem2.getType()).getPrecision());
        Assert.assertEquals(1, ((ScalarType) mvColumnItem2.getType()).getScalarScale());

        SlotRef slotRef3 = new SlotRef(new TableName("db", "table"), "a");
        List<Expr> params3 = Lists.newArrayList();
        params3.add(slotRef3);
        FunctionCallExpr functionCallExpr3 = new FunctionCallExpr(FunctionSet.MIN, params3);
        Deencapsulation.setField(slotRef3, "desc", slotDescriptor3);
        new Expectations() {
            {
                slotDescriptor3.getColumn();
                result = column3;
                column3.getType();
                result = ScalarType.createCharType(5);
            }
        };
        MVColumnItem mvColumnItem3 =
                Deencapsulation.invoke(createMaterializedViewStmt, "buildMVColumnItem", functionCallExpr3);
        Assert.assertEquals(5, ((ScalarType) mvColumnItem3.getType()).getLength());
    }
}

