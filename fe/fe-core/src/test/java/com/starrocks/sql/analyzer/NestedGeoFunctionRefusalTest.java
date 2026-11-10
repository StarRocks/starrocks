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
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.expression.ArrayExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.FunctionParams;
import com.starrocks.sql.ast.expression.LambdaFunctionExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.type.AnyArrayType;
import com.starrocks.type.AnyElementType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.FunctionType;
import com.starrocks.type.GeoTypeDescriptor;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NestedGeoFunctionRefusalTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

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

    private static SlotRef slot(Type type) {
        SlotRef slot = new SlotRef(null, "v");
        slot.setType(type);
        return slot;
    }

    private static FunctionCallExpr call(String name, Type... args) {
        FunctionCallExpr call = new FunctionCallExpr(name, Arrays.stream(args).map(t -> (Expr) slot(t)).toList());
        call.setFn(new ScalarFunction(new FunctionName(name), Arrays.asList(args), args[0], false));
        call.setType(args[0]);
        return call;
    }

    private static void check(FunctionCallExpr call, boolean refused, String error) {
        if (refused) {
            assertEquals(error, assertThrows(SemanticException.class, () -> FunctionAnalyzer.analyze(call)).getDetailMsg());
        } else {
            FunctionAnalyzer.analyze(call);
        }
    }

    @ParameterizedTest
    @MethodSource("types")
    public void testComparedElements(Type item) {
        Type array = new ArrayType(item);
        for (String name : List.of("array_sort", "array_max", "array_min", "array_distinct", "array_unique_agg",
                "array_contains", "array_position", "array_remove", "array_contains_all", "array_contains_seq",
                "array_intersect", "arrays_overlap", "array_top_n", "array_sort_lambda", "nullif", "crc32_hash")) {
            Type[] args = switch (name) {
                case "array_contains", "array_position", "array_remove" -> new Type[] {array, item};
                case "array_contains_all", "array_contains_seq", "array_intersect", "arrays_overlap", "nullif" ->
                        new Type[] {array, array};
                case "array_top_n" -> new Type[] {array, IntegerType.INT};
                case "array_sort_lambda" -> new Type[] {array, FunctionType.FUNCTION};
                default -> new Type[] {array};
            };
            for (Expr input : List.of(slot(array), NullLiteral.create(array),
                    new ArrayExpr(array, List.of(NullLiteral.create(item))))) {
                FunctionCallExpr call = call(name, args);
                call.setChild(0, input);
                check(call, item.isGeoType() || item.hasNestedGeoType(),
                        name + " does not support geo arguments: " + array.toSql());
            }
        }
        for (String safe : List.of("array_length", "array_append", "array_concat", "array_slice", "reverse",
                "array_repeat", "array_flatten", "arrays_zip", "null_or_empty")) {
            Type[] args = switch (safe) {
                case "array_append" -> new Type[] {array, item};
                case "array_concat", "arrays_zip" -> new Type[] {array, array};
                case "array_slice" -> new Type[] {array, IntegerType.BIGINT};
                case "array_repeat" -> new Type[] {item, IntegerType.INT};
                case "array_flatten" -> new Type[] {new ArrayType(array)};
                default -> new Type[] {array};
            };
            FunctionAnalyzer.analyze(call(safe, args));
        }
    }

    @ParameterizedTest
    @MethodSource("types")
    public void testResolvedEmptyAndNull(Type item) {
        for (String name : List.of("array_contains", "array_position", "array_remove")) {
            for (Expr input : List.of(new ArrayExpr(ArrayType.ARRAY_NULL, List.of()), new NullLiteral())) {
                Function declaration = new ScalarFunction(new FunctionName(name),
                        List.of(AnyArrayType.ANY_ARRAY, AnyElementType.ANY_ELEMENT), BooleanType.BOOLEAN, false);
                Function fn = PolymorphicFunctionAnalyzer.generatePolymorphicFunction(declaration,
                        new Type[] {input.getType(), item});
                FunctionCallExpr call = new FunctionCallExpr(name, List.of(input, slot(item)));
                call.setFn(fn);
                check(call, item.isGeoType() || item.hasNestedGeoType(),
                        name + " does not support geo arguments: " + fn.getArgs()[0].toSql());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("types")
    public void testSortByKeysAndValues(Type item) {
        Type array = new ArrayType(item);
        ExpressionAnalyzer.Visitor visitor = new ExpressionAnalyzer.Visitor(
                new AnalyzeState(), AnalyzeTestUtil.getConnectContext());
        for (int arity : List.of(2, 3)) {
            Type[] values = arity == 2 ? new Type[] {array, ArrayType.ARRAY_INT} :
                    new Type[] {array, ArrayType.ARRAY_INT, ArrayType.ARRAY_INT};
            FunctionCallExpr safe = call("array_sortby", values);
            if (item.isGeoType() || item.hasNestedGeoType() || (arity == 3 && item.isComplexType())) {
                assertThrows(SemanticException.class, () -> visitor.visitFunctionCall(safe, null));
            } else {
                visitor.visitFunctionCall(safe, null);
                assertEquals(array, safe.getType());
            }
            check(call("array_sortby", values), item.isGeoType() || item.hasNestedGeoType(),
                    "array_sortby does not support geo arguments: " + array.toSql());
            Type[] keys = arity == 2 ? new Type[] {ArrayType.ARRAY_INT, array} :
                    new Type[] {ArrayType.ARRAY_INT, ArrayType.ARRAY_INT, array};
            check(call("array_sortby", keys), item.isGeoType() || item.hasNestedGeoType(),
                    "array_sortby does not support geo arguments: " + array.toSql());
        }
        FunctionCallExpr mapped = new FunctionCallExpr("array_map", List.of(
                new LambdaFunctionExpr(List.of(NullLiteral.create(item))), slot(ArrayType.ARRAY_INT)));
        FunctionAnalyzer.analyze(mapped);
        FunctionCallExpr sorted = call("array_sortby", ArrayType.ARRAY_INT, mapped.getType());
        sorted.setChild(1, mapped);
        check(sorted, item.isGeoType() || item.hasNestedGeoType(),
                "array_sortby does not support geo arguments: " + array.toSql());
    }

    @ParameterizedTest
    @MethodSource("types")
    public void testAggregates(Type item) {
        for (String name : List.of("max_by", "min_by", "array_agg", "map_agg")) {
            Type result = name.equals("array_agg") ? new ArrayType(item) :
                    name.equals("map_agg") ? new MapType(IntegerType.INT, item) : item;
            Type[] args = name.equals("array_agg") ? new Type[] {item} :
                    name.equals("map_agg") ? new Type[] {IntegerType.INT, item} : new Type[] {item, IntegerType.INT};
            FunctionCallExpr call = call(name, args);
            call.setFn(new AggregateFunction(new FunctionName(name), Arrays.asList(args), result, null, false));
            call.setType(result);
            check(call, (name.equals("max_by") || name.equals("min_by")) && item.hasNestedGeoType(),
                    name + " does not support nested geo values");
        }
        for (Type result : List.of(item, new ArrayType(item))) {
            FunctionCallExpr ordered = new FunctionCallExpr("array_agg", new FunctionParams(false,
                    List.of(slot(item), slot(IntegerType.INT)), List.of(new OrderByElement(slot(IntegerType.INT), true, null))));
            ordered.setFn(new AggregateFunction(new FunctionName("array_agg"),
                    List.of(item, IntegerType.INT), result, null, false));
            ordered.setType(result);
            check(ordered, item.isGeoType() || item.hasNestedGeoType(),
                    "ORDER BY aggregation is not supported for geo results: " + result.toSql());
        }
    }
}
