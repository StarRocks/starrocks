// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.CollectionElementExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExprNodeType;
import org.junit.Assert;
import org.junit.Test;

public class ExpressionAnalyzerTest extends PlanTestBase {

    @Test
    public void testVariables() throws Exception {
        String sql = "SELECT @@max_allowed_packet, @@SESSION.character_set_client,\n" +
                "        @@GLOBAL.character_set_connection";
        ExecPlan execPlan = getExecPlan(sql);
        Assert.assertEquals("@@max_allowed_packet", execPlan.getColNames().get(0));
        Assert.assertEquals("@@SESSION.character_set_client", execPlan.getColNames().get(1));
        Assert.assertEquals("@@GLOBAL.character_set_connection", execPlan.getColNames().get(2));
    }

    @Test
    public void testMapElementAnalyzer() throws Exception {
        ExpressionAnalyzer.Visitor visitor = new ExpressionAnalyzer.Visitor(new AnalyzeState(), new ConnectContext());
        SlotRef slot = new SlotRef(null, "col", "col");
        Type keyType = ScalarType.createType(PrimitiveType.INT);
        Type valueType = ScalarType.createCharType(10);
        Type mapType = new MapType(keyType, valueType);
        slot.setType(mapType);

        IntLiteral sub = new IntLiteral(10);

        CollectionElementExpr collectionElementExpr = new CollectionElementExpr(slot, sub);
        try {
            visitor.visitCollectionElementExpr(collectionElementExpr,
                    new Scope(RelationId.anonymous(), new RelationFields()));
        } catch (Exception e) {
            Assert.assertFalse(true);
        }

        StringLiteral subCast = new StringLiteral("10");
        CollectionElementExpr collectionElementExpr1 = new CollectionElementExpr(slot, subCast);
        try {
            visitor.visitCollectionElementExpr(collectionElementExpr1,
                    new Scope(RelationId.anonymous(), new RelationFields()));
        } catch (Exception e) {
            Assert.assertFalse(true);
        }

        StringLiteral subNoCast = new StringLiteral("aaa");
        CollectionElementExpr collectionElementExpr2 = new CollectionElementExpr(slot, subNoCast);
        Assert.assertThrows(SemanticException.class,
                () -> visitor.visitCollectionElementExpr(collectionElementExpr2,
                        new Scope(RelationId.anonymous(), new RelationFields())));

        Type keyTypeChar = ScalarType.createCharType(10);
        Type valueTypeInt = ScalarType.createType(PrimitiveType.INT);
        mapType = new MapType(keyTypeChar, valueTypeInt);
        slot.setType(mapType);
        StringLiteral subString = new StringLiteral("aaa");
        CollectionElementExpr collectionElementExpr3 = new CollectionElementExpr(slot, subString);
        try {
            visitor.visitCollectionElementExpr(collectionElementExpr3,
                    new Scope(RelationId.anonymous(), new RelationFields()));
        } catch (Exception e) {
            Assert.assertFalse(true);
        }

        Assert.assertEquals(TExprNodeType.MAP_ELEMENT_EXPR,
                collectionElementExpr3.treeToThrift().getNodes().get(0).getNode_type());
    }

    @Test
    public void testArraySubscriptAnalyzer() throws Exception {
        ExpressionAnalyzer.Visitor visitor = new ExpressionAnalyzer.Visitor(new AnalyzeState(), new ConnectContext());
        SlotRef slot = new SlotRef(null, "col", "col");
        Type elementType = ScalarType.createCharType(10);
        Type arrayType = new ArrayType(elementType);
        slot.setType(arrayType);

        IntLiteral sub = new IntLiteral(10);

        CollectionElementExpr collectionElementExpr = new CollectionElementExpr(slot, sub);
        try {
            visitor.visitCollectionElementExpr(collectionElementExpr,
                    new Scope(RelationId.anonymous(), new RelationFields()));
        } catch (Exception e) {
            Assert.assertFalse(true);
        }

        StringLiteral subCast = new StringLiteral("10");
        CollectionElementExpr collectionElementExpr1 = new CollectionElementExpr(slot, subCast);
        Assert.assertThrows(SemanticException.class,
                () -> visitor.visitCollectionElementExpr(collectionElementExpr1,
                        new Scope(RelationId.anonymous(), new RelationFields())));

        StringLiteral subNoCast = new StringLiteral("aaa");
        CollectionElementExpr collectionElementExpr2 = new CollectionElementExpr(slot, subNoCast);
        Assert.assertThrows(SemanticException.class,
                () -> visitor.visitCollectionElementExpr(collectionElementExpr2,
                        new Scope(RelationId.anonymous(), new RelationFields())));

        Assert.assertEquals(TExprNodeType.ARRAY_ELEMENT_EXPR,
                collectionElementExpr2.treeToThrift().getNodes().get(0).getNode_type());
    }

    @Test
    public void testNoSubscriptAnalyzer() throws Exception {
        ExpressionAnalyzer.Visitor visitor = new ExpressionAnalyzer.Visitor(new AnalyzeState(), new ConnectContext());
        SlotRef slot = new SlotRef(null, "col", "col");
        slot.setType(ScalarType.createType(PrimitiveType.INT));

        IntLiteral sub = new IntLiteral(10);

        CollectionElementExpr collectionElementExpr = new CollectionElementExpr(slot, sub);
        Assert.assertThrows(SemanticException.class,
                () -> visitor.visitCollectionElementExpr(collectionElementExpr,
                        new Scope(RelationId.anonymous(), new RelationFields())));
    }

    @Test
    public void testMapFunctionsAnalyzer() throws Exception {
        Type keyType = ScalarType.createType(PrimitiveType.INT);
        Type valueType = ScalarType.createCharType(10);
        Type mapType = new MapType(keyType, valueType);

        String mapKeys = "map_keys";
        String mapValues = "map_values";
        String mapSize = "map_size";
        Type[] argumentTypes = { mapType };

        Function fnMapKeys = Expr.getBuiltinFunction(mapKeys, argumentTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assert.assertEquals(fnMapKeys.functionName(), "map_keys");
        Assert.assertTrue(fnMapKeys.getReturnType().isArrayType());
        Assert.assertEquals(((ArrayType) fnMapKeys.getReturnType()).getItemType(), keyType);

        Function fnMapValues = Expr.getBuiltinFunction(mapValues, argumentTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assert.assertEquals(fnMapValues.functionName(), "map_values");
        Assert.assertTrue(fnMapValues.getReturnType().isArrayType());
        Assert.assertEquals(((ArrayType) fnMapValues.getReturnType()).getItemType(), valueType);


        Function fnMapSize = Expr.getBuiltinFunction(mapSize, argumentTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assert.assertEquals(fnMapSize.functionName(), "map_size");
        Assert.assertEquals(fnMapSize.getReturnType(), Type.INT);

        Type[] argumentTypesErrorNum = { mapType, keyType };
        Function fnKeysErrorNum = Expr.getBuiltinFunction(mapKeys, argumentTypesErrorNum,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assert.assertTrue(fnKeysErrorNum == null);
        Function fnValuesErrorNum = Expr.getBuiltinFunction(mapValues, argumentTypesErrorNum,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assert.assertTrue(fnKeysErrorNum == null);
        Function fnSizeErrorNum = Expr.getBuiltinFunction(mapSize, argumentTypesErrorNum,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assert.assertTrue(fnKeysErrorNum == null);

        Type[] argumentTypesErrorType = { keyType };
        Function fnKeysErrorType = Expr.getBuiltinFunction(mapKeys, argumentTypesErrorType,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assert.assertTrue(fnKeysErrorType == null);
        Function fnValuesErrorType = Expr.getBuiltinFunction(mapValues, argumentTypesErrorType,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assert.assertTrue(fnValuesErrorType == null);
        Function fnSizeErrorType = Expr.getBuiltinFunction(mapSize, argumentTypesErrorType,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assert.assertTrue(fnSizeErrorType == null);
    }
}