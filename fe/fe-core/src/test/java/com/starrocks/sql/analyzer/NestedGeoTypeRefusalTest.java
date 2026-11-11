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

import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.ExceptRelation;
import com.starrocks.sql.ast.IntersectRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SetQualifier;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.CaseExpr;
import com.starrocks.sql.ast.expression.CaseWhenClause;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LambdaFunctionExpr;
import com.starrocks.sql.ast.expression.MapExpr;
import com.starrocks.sql.ast.expression.MultiInPredicate;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.spm.SPMFunctions;
import com.starrocks.type.AnyArrayType;
import com.starrocks.type.AnyElementType;
import com.starrocks.type.AnyMapType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.GeoTypeDescriptor;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.NullType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NestedGeoTypeRefusalTest {
    private final ExpressionAnalyzer.Visitor visitor = new ExpressionAnalyzer.Visitor(new AnalyzeState(), new ConnectContext());

    static List<Type> types() {
        List<Type> result = new ArrayList<>();
        for (Type leaf : List.of(IntegerType.INT,
                ScalarType.createGeoType(PrimitiveType.GEOGRAPHY, new GeoTypeDescriptor(
                        GeoTypeDescriptor.LogicalType.GEOGRAPHY, GeoTypeDescriptor.CoordinateSystem.SPHERICAL,
                        GeoTypeDescriptor.EdgeAlgorithm.SPHERICAL, "OGC:CRS84", 4326)),
                ScalarType.createGeoType(PrimitiveType.GEOMETRY, GeoTypeDescriptor.geometry("EPSG:3857")))) {
            result.addAll(List.of(leaf, new ArrayType(leaf), new MapType(IntegerType.INT, leaf),
                    new StructType(List.of(leaf)), new ArrayType(new StructType(List.of(new ArrayType(leaf))))));
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource("types")
    public void testMapKeys(Type type) {
        boolean geo = type.isGeoType() || type.hasNestedGeoType();
        assertEquals(!geo && !type.isComplexType(), type.isValidMapKeyType());
        Function arrays = new ScalarFunction(new FunctionName("map_from_arrays"),
                List.of(AnyArrayType.ANY_ARRAY, AnyArrayType.ANY_ARRAY), AnyMapType.ANY_MAP, false);
        Function agg = new AggregateFunction(new FunctionName("map_agg"),
                List.of(AnyElementType.ANY_ELEMENT, AnyElementType.ANY_ELEMENT), AnyMapType.ANY_MAP, null, false);
        for (Function fn : List.of(arrays, agg)) {
            Type[] args = fn == arrays ? new Type[] {new ArrayType(type), ArrayType.ARRAY_INT} :
                    new Type[] {type, IntegerType.INT};
            if (geo) {
                assertEquals("Map key doesn't support type: " + type, assertThrows(SemanticException.class,
                        () -> PolymorphicFunctionAnalyzer.generatePolymorphicFunction(fn, args)).getDetailMsg());
            } else {
                assertEquals(new MapType(type, IntegerType.INT),
                        PolymorphicFunctionAnalyzer.generatePolymorphicFunction(fn, args).getReturnType());
            }
            Type[] values = fn == arrays ? new Type[] {ArrayType.ARRAY_INT, new ArrayType(type)} :
                    new Type[] {IntegerType.INT, type};
            assertEquals(new MapType(IntegerType.INT, type),
                    PolymorphicFunctionAnalyzer.generatePolymorphicFunction(fn, values).getReturnType());
        }
        MapExpr body = new MapExpr(new MapType(type, IntegerType.INT), List.of(NullLiteral.create(type), new IntLiteral(1)));
        FunctionCallExpr apply = new FunctionCallExpr("map_apply", List.of(
                new LambdaFunctionExpr(List.of(body)), NullLiteral.create(new MapType(IntegerType.INT, IntegerType.INT))));
        if (geo) {
            assertEquals("Map key doesn't support type: " + type, assertThrows(SemanticException.class,
                    () -> FunctionAnalyzer.analyze(apply)).getDetailMsg());
            MapType invalid = new MapType(type, IntegerType.INT);
            Type nested = new ArrayType(new StructType(List.of(new MapType(IntegerType.INT, invalid))));
            assertThrows(SemanticException.class, () -> PolymorphicFunctionAnalyzer.generatePolymorphicFunction(
                    arrays, new Type[] {ArrayType.ARRAY_INT, new ArrayType(nested)}));
        } else {
            FunctionAnalyzer.analyze(apply);
            assertEquals(body.getType(), apply.getType());
        }
        MapType values = new MapType(IntegerType.INT, type);
        apply.getChild(0).setChild(0, new MapExpr(values, List.of(new IntLiteral(1, IntegerType.INT), NullLiteral.create(type))));
        FunctionAnalyzer.analyze(apply);
        assertEquals(values, apply.getType());
    }

    @ParameterizedTest
    @MethodSource("types")
    public void testComparisonAndCase(Type type) {
        boolean geo = type.isGeoType() || type.hasNestedGeoType();
        for (boolean range : new boolean[] {false, true}) {
            assertEquals(geo || range && (type.isMapType() || type.isStructType()),
                    TypeManager.getCompatibleTypeForBinary(range, type, type).isInvalid());
            assertEquals(geo || range && (type.isMapType() || type.isStructType()),
                    TypeManager.getCompatibleTypeForBinary(range, NullType.NULL, type).isInvalid());
            assertEquals(geo, TypeManager.getCompatibleTypeForBetweenAndIn(List.of(type, type), range).isInvalid());
        }
        CaseExpr simple = new CaseExpr(NullLiteral.create(type), List.of(
                new CaseWhenClause(NullLiteral.create(type), new IntLiteral(1))), new IntLiteral(0));
        if (geo) {
            assertEquals("Invalid when type cast " + type.toSql() + " to INVALID_TYPE", assertThrows(SemanticException.class,
                    () -> visitor.visitCaseWhenExpr(simple, null)).getDetailMsg());
        } else {
            visitor.visitCaseWhenExpr(simple, null);
            assertEquals(IntegerType.TINYINT, simple.getType());
        }
        assertEquals(type.isGeoType(), TypeManager.getCompatibleTypeForCaseWhen(List.of(type, type)).isInvalid());
        if (type.hasNestedGeoType() && TypeManager.canCastTo(type, type)) {
            CaseExpr searched = new CaseExpr(null, List.of(
                    new CaseWhenClause(new BoolLiteral(true), NullLiteral.create(type))), NullLiteral.create(type));
            visitor.visitCaseWhenExpr(searched, null);
            assertEquals(type, searched.getType());
        }
        if (type.hasNestedGeoType()) {
            assertFalse(type.canGroupBy());
            assertFalse(type.canOrderBy());
            assertFalse(type.canJoinOn());
            assertFalse(type.canDistinct());
        }
    }

    @ParameterizedTest
    @MethodSource("types")
    public void testSetOperations(Type type) {
        for (int op = 0; op < 4; op++) {
            List<com.starrocks.sql.ast.QueryRelation> inputs = List.of(
                    new ValuesRelation(List.of(), List.of("g"), List.of(type)),
                    new ValuesRelation(List.of(), List.of("g"), List.of(type)));
            SetOperationRelation set = switch (op) {
                case 0 -> new UnionRelation(inputs, SetQualifier.ALL);
                case 1 -> new UnionRelation(inputs, SetQualifier.DISTINCT);
                case 2 -> new IntersectRelation(inputs, SetQualifier.DISTINCT);
                default -> new ExceptRelation(inputs, SetQualifier.DISTINCT);
            };
            QueryAnalyzer analyzer = new QueryAnalyzer(new ConnectContext());
            if (op > 0 && (type.isGeoType() || type.hasNestedGeoType())) {
                assertEquals(type + " not support set operation", assertThrows(SemanticException.class,
                        () -> analyzer.analyze(new QueryStatement(set), null)).getDetailMsg());
            } else {
                analyzer.analyze(new QueryStatement(set), null);
                assertEquals(type, set.getScope().getRelationFields().getFieldByIndex(0).getType());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("types")
    public void testMultiInAndSpm(Type type) {
        ValuesRelation values = new ValuesRelation(List.of(List.of(new IntLiteral(1), NullLiteral.create(type))),
                List.of("id", "g"));
        Subquery subquery = new Subquery(new QueryStatement(values));
        subquery.setType(IntegerType.BIGINT);
        MultiInPredicate in = new MultiInPredicate(List.of(new IntLiteral(1), new NullLiteral()), subquery, false);
        // scalar geo tuple IN is covered by #82211
        if (type.hasNestedGeoType() || type.isMapType() || type.isStructType()) {
            String error = type.isMapType() || type.isStructType() ? "InPredicate of JSON, Map, Struct types is not supported" :
                    "in predicate type " + NullType.NULL.toSql() + " with type " + type.toSql() + " is invalid";
            assertEquals(error, assertThrows(SemanticException.class,
                    () -> visitor.visitMultiInPredicate(in, null)).getDetailMsg());
        } else if (!type.isGeoType()) {
            visitor.visitMultiInPredicate(in, null);
        }
        FunctionCallExpr single = new FunctionCallExpr("_spm_const_enum", List.of(new IntLiteral(1), NullLiteral.create(type)));
        boolean geo = type.isGeoType() || type.hasNestedGeoType();
        if (geo) {
            assertEquals("spm function _spm_const_enum does not support type " + type.toSql(),
                    assertThrows(SemanticException.class,
                            () -> SPMFunctions.getSPMFunction(single, List.of(IntegerType.INT, type))).getDetailMsg());
        } else {
            assertTrue(SPMFunctions.getSPMFunction(single, List.of(IntegerType.INT, type)) != null);
        }
        for (String name : List.of("_spm_const_range", "_spm_const_enum")) {
            FunctionCallExpr call = new FunctionCallExpr(name, List.of(new IntLiteral(1),
                    NullLiteral.create(type), NullLiteral.create(type)));
            if (geo) {
                String error = "spm function " + name + " does not support type " + type.toSql();
                assertEquals(error, assertThrows(SemanticException.class, () -> SPMFunctions.getSPMFunction(call,
                        List.of(IntegerType.BIGINT, type, type))).getDetailMsg());
            } else {
                assertTrue(SPMFunctions.getSPMFunction(call, List.of(IntegerType.BIGINT, type, type)) != null);
            }
        }
    }
}
