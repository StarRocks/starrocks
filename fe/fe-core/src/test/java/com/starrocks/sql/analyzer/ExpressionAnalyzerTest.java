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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.Function;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.CollectionElementExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprToThriftVisitor;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LikePredicate;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.UserVariableExpr;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExpressionAnalyzerTest extends PlanTestBase {

    @Test
    public void testVariables() throws Exception {
        String sql = "SELECT @@max_allowed_packet, @@SESSION.character_set_client,\n" +
                "        @@GLOBAL.character_set_connection";
        ExecPlan execPlan = getExecPlan(sql);
        Assertions.assertEquals("@@max_allowed_packet", execPlan.getColNames().get(0));
        Assertions.assertEquals("@@SESSION.character_set_client", execPlan.getColNames().get(1));
        Assertions.assertEquals("@@GLOBAL.character_set_connection", execPlan.getColNames().get(2));
    }

    @Test
    public void testMapElementAnalyzer() {
        ExpressionAnalyzer.Visitor visitor = new ExpressionAnalyzer.Visitor(new AnalyzeState(), new ConnectContext());
        SlotRef slot = new SlotRef(null, "col", "col");
        Type keyType = IntegerType.INT;
        Type valueType = TypeFactory.createCharType(10);
        Type mapType = new MapType(keyType, valueType);
        slot.setType(mapType);

        IntLiteral sub = new IntLiteral(10);

        CollectionElementExpr collectionElementExpr = new CollectionElementExpr(slot, sub, false);
        try {
            visitor.visitCollectionElementExpr(collectionElementExpr,
                    new Scope(RelationId.anonymous(), new RelationFields()));
        } catch (Exception e) {
            Assertions.fail();
        }

        StringLiteral subCast = new StringLiteral("10");
        CollectionElementExpr collectionElementExpr1 = new CollectionElementExpr(slot, subCast, false);
        try {
            visitor.visitCollectionElementExpr(collectionElementExpr1,
                    new Scope(RelationId.anonymous(), new RelationFields()));
        } catch (Exception e) {
            Assertions.fail();
        }

        StringLiteral subNoCast = new StringLiteral("aaa");
        CollectionElementExpr collectionElementExpr2 = new CollectionElementExpr(slot, subNoCast, false);
        Assertions.assertThrows(SemanticException.class,
                () -> visitor.visitCollectionElementExpr(collectionElementExpr2,
                        new Scope(RelationId.anonymous(), new RelationFields())));

        Type keyTypeChar = TypeFactory.createCharType(10);
        Type valueTypeInt = IntegerType.INT;
        mapType = new MapType(keyTypeChar, valueTypeInt);
        slot.setType(mapType);
        StringLiteral subString = new StringLiteral("aaa");
        CollectionElementExpr collectionElementExpr3 = new CollectionElementExpr(slot, subString, false);
        try {
            visitor.visitCollectionElementExpr(collectionElementExpr3,
                    new Scope(RelationId.anonymous(), new RelationFields()));
        } catch (Exception e) {
            Assertions.fail();
        }

        Assertions.assertEquals(TExprNodeType.MAP_ELEMENT_EXPR,
                ExprToThriftVisitor
                        .treeToThrift(collectionElementExpr3).getNodes().get(0).getNode_type());
    }

    @Test
    public void testArraySubscriptAnalyzer() {
        ExpressionAnalyzer.Visitor visitor = new ExpressionAnalyzer.Visitor(new AnalyzeState(), new ConnectContext());
        SlotRef slot = new SlotRef(null, "col", "col");
        Type elementType = TypeFactory.createCharType(10);
        Type arrayType = new ArrayType(elementType);
        slot.setType(arrayType);

        IntLiteral sub = new IntLiteral(10);

        CollectionElementExpr collectionElementExpr = new CollectionElementExpr(slot, sub, false);
        try {
            visitor.visitCollectionElementExpr(collectionElementExpr,
                    new Scope(RelationId.anonymous(), new RelationFields()));
        } catch (Exception e) {
            Assertions.fail();
        }

        StringLiteral subCast = new StringLiteral("10");
        CollectionElementExpr collectionElementExpr1 = new CollectionElementExpr(slot, subCast, false);
        Assertions.assertThrows(SemanticException.class,
                () -> visitor.visitCollectionElementExpr(collectionElementExpr1,
                        new Scope(RelationId.anonymous(), new RelationFields())));

        StringLiteral subNoCast = new StringLiteral("aaa");
        CollectionElementExpr collectionElementExpr2 = new CollectionElementExpr(slot, subNoCast, false);
        Assertions.assertThrows(SemanticException.class,
                () -> visitor.visitCollectionElementExpr(collectionElementExpr2,
                        new Scope(RelationId.anonymous(), new RelationFields())));

        Assertions.assertEquals(TExprNodeType.ARRAY_ELEMENT_EXPR,
                ExprToThriftVisitor
                        .treeToThrift(collectionElementExpr2).getNodes().get(0).getNode_type());
    }

    @Test
    public void testNoSubscriptAnalyzer() {
        ExpressionAnalyzer.Visitor visitor = new ExpressionAnalyzer.Visitor(new AnalyzeState(), new ConnectContext());
        SlotRef slot = new SlotRef(null, "col", "col");
        slot.setType(IntegerType.INT);

        IntLiteral sub = new IntLiteral(10);

        CollectionElementExpr collectionElementExpr = new CollectionElementExpr(slot, sub, false);
        Assertions.assertThrows(SemanticException.class,
                () -> visitor.visitCollectionElementExpr(collectionElementExpr,
                        new Scope(RelationId.anonymous(), new RelationFields())));
    }

    @Test
    public void testMapFunctionsAnalyzer() {
        Type keyType = IntegerType.INT;
        Type valueType = TypeFactory.createCharType(10);
        Type mapType = new MapType(keyType, valueType);

        String mapKeys = "map_keys";
        String mapValues = "map_values";
        String mapSize = "map_size";
        Type[] argumentTypes = {mapType};

        Function fnMapKeys =
                ExprUtils.getBuiltinFunction(mapKeys, argumentTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assertions.assertEquals(fnMapKeys.functionName(), "map_keys");
        Assertions.assertTrue(fnMapKeys.getReturnType().isArrayType());
        Assertions.assertEquals(((ArrayType) fnMapKeys.getReturnType()).getItemType(), keyType);

        Function fnMapValues =
                ExprUtils.getBuiltinFunction(mapValues, argumentTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assertions.assertEquals(fnMapValues.functionName(), "map_values");
        Assertions.assertTrue(fnMapValues.getReturnType().isArrayType());
        Assertions.assertEquals(((ArrayType) fnMapValues.getReturnType()).getItemType(), valueType);

        Function fnMapSize =
                ExprUtils.getBuiltinFunction(mapSize, argumentTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assertions.assertEquals(fnMapSize.functionName(), "map_size");
        Assertions.assertEquals(fnMapSize.getReturnType(), IntegerType.INT);

        Type[] argumentTypesErrorNum = {mapType, keyType};
        Function fnKeysErrorNum = ExprUtils.getBuiltinFunction(mapKeys, argumentTypesErrorNum,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assertions.assertNull(fnKeysErrorNum);
        ExprUtils.getBuiltinFunction(mapValues, argumentTypesErrorNum, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assertions.assertNull(fnKeysErrorNum);
        ExprUtils.getBuiltinFunction(mapSize, argumentTypesErrorNum, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assertions.assertNull(fnKeysErrorNum);

        Type[] argumentTypesErrorType = {keyType};
        Function fnKeysErrorType = ExprUtils.getBuiltinFunction(mapKeys, argumentTypesErrorType,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assertions.assertNull(fnKeysErrorType);
        Function fnValuesErrorType = ExprUtils.getBuiltinFunction(mapValues, argumentTypesErrorType,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assertions.assertNull(fnValuesErrorType);
        Function fnSizeErrorType = ExprUtils.getBuiltinFunction(mapSize, argumentTypesErrorType,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assertions.assertNull(fnSizeErrorType);
    }

    @Test
    public void testDateCoalesceAnalyzer() {
        Type dateType = DateType.DATE;
        Type dateTimeType = DateType.DATETIME;

        {
            Type[] argumentTypes = {dateType, dateTimeType};
            String coalesce = "coalesce";
            Function fnCoalesce =
                    ExprUtils.getBuiltinFunction(coalesce, argumentTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            Assertions.assertEquals(fnCoalesce.functionName(), "coalesce");
            Assertions.assertEquals(fnCoalesce.getReturnType(), dateTimeType);
        }
        {
            Type[] argumentTypes = {dateTimeType, dateType};
            String coalesce = "coalesce";
            Function fnCoalesce =
                    ExprUtils.getBuiltinFunction(coalesce, argumentTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            Assertions.assertEquals(fnCoalesce.functionName(), "coalesce");
            Assertions.assertEquals(fnCoalesce.getReturnType(), dateTimeType);
        }
    }

    @Test
    public void testUserVariableExprAnalyzer() {
        Expr expr = SqlParser.parseSqlToExpr("[1, 2, 3]", 32);
        UserVariableExpr userVariableExpr = new UserVariableExpr("test", NodePosition.ZERO);
        userVariableExpr.setValue(expr);
        UserVariableExpr copy = (UserVariableExpr) userVariableExpr.clone();
        Assertions.assertEquals(userVariableExpr, copy);
    }

    @Test
    public void testLikePatternSyntaxException() {
        StringLiteral e1 = new StringLiteral("a");
        e1.setType(VarcharType.VARCHAR);
        StringLiteral e2 = new StringLiteral("([A-Za-z0-9]+[\\u4e00-\\u9fa5]{2}[A-Za-z0-9]+)");
        e2.setType(VarcharType.VARCHAR);
        LikePredicate likePredicate = new LikePredicate(LikePredicate.Operator.REGEXP, e1, e2);
        ExpressionAnalyzer.Visitor visitor = new ExpressionAnalyzer.Visitor(new AnalyzeState(), new ConnectContext());
        Assertions.assertThrows(SemanticException.class, () -> visitor.visitLikePredicate(likePredicate,
                new Scope(RelationId.anonymous(), new RelationFields())));
    }
}