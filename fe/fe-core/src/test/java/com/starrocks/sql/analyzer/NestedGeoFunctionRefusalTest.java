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
import com.starrocks.catalog.FunctionSet;
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
import org.junit.jupiter.params.provider.ValueSource;

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
        for (String name : List.of(FunctionSet.ARRAY_SORT, FunctionSet.ARRAY_MAX, FunctionSet.ARRAY_MIN,
                FunctionSet.ARRAY_DISTINCT, FunctionSet.ARRAY_UNIQUE_AGG, FunctionSet.ARRAY_CONTAINS,
                FunctionSet.ARRAY_POSITION, FunctionSet.ARRAY_REMOVE, FunctionSet.ARRAY_CONTAINS_ALL,
                FunctionSet.ARRAY_CONTAINS_SEQ, FunctionSet.ARRAY_INTERSECT, FunctionSet.ARRAYS_OVERLAP,
                FunctionSet.ARRAY_TOP_N, FunctionSet.ARRAY_SORT_LAMBDA, FunctionSet.NULLIF, FunctionSet.CRC32_HASH)) {
            Type[] args = switch (name) {
                case FunctionSet.ARRAY_CONTAINS, FunctionSet.ARRAY_POSITION, FunctionSet.ARRAY_REMOVE -> new Type[] {array, item};
                case FunctionSet.ARRAY_CONTAINS_ALL, FunctionSet.ARRAY_CONTAINS_SEQ, FunctionSet.ARRAY_INTERSECT,
                        FunctionSet.ARRAYS_OVERLAP, FunctionSet.NULLIF -> new Type[] {array, array};
                case FunctionSet.ARRAY_TOP_N -> new Type[] {array, IntegerType.INT};
                case FunctionSet.ARRAY_SORT_LAMBDA -> new Type[] {array, FunctionType.FUNCTION};
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
        for (String safe : List.of(FunctionSet.ARRAY_LENGTH, FunctionSet.ARRAY_APPEND, FunctionSet.ARRAY_CONCAT,
                FunctionSet.ARRAY_SLICE, FunctionSet.REVERSE, FunctionSet.ARRAY_REPEAT, FunctionSet.ARRAY_FLATTEN,
                FunctionSet.ARRAYS_ZIP, FunctionSet.NULL_OR_EMPTY)) {
            Type[] args = switch (safe) {
                case FunctionSet.ARRAY_APPEND -> new Type[] {array, item};
                case FunctionSet.ARRAY_CONCAT, FunctionSet.ARRAYS_ZIP -> new Type[] {array, array};
                case FunctionSet.ARRAY_SLICE -> new Type[] {array, IntegerType.BIGINT};
                case FunctionSet.ARRAY_REPEAT -> new Type[] {item, IntegerType.INT};
                case FunctionSet.ARRAY_FLATTEN -> new Type[] {new ArrayType(array)};
                default -> new Type[] {array};
            };
            FunctionAnalyzer.analyze(call(safe, args));
        }
    }

    @ParameterizedTest
    @MethodSource("types")
    public void testResolvedEmptyAndNull(Type item) {
        for (String name : List.of(FunctionSet.ARRAY_CONTAINS, FunctionSet.ARRAY_POSITION, FunctionSet.ARRAY_REMOVE)) {
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
            FunctionCallExpr safe = call(FunctionSet.ARRAY_SORTBY, values);
            if (item.isGeoType() || item.hasNestedGeoType() || (arity == 3 && item.isComplexType())) {
                assertThrows(SemanticException.class, () -> visitor.visitFunctionCall(safe, null));
            } else {
                visitor.visitFunctionCall(safe, null);
                assertEquals(array, safe.getType());
            }
            check(call(FunctionSet.ARRAY_SORTBY, values), item.isGeoType() || item.hasNestedGeoType(),
                    "array_sortby does not support geo arguments: " + array.toSql());
            Type[] keys = arity == 2 ? new Type[] {ArrayType.ARRAY_INT, array} :
                    new Type[] {ArrayType.ARRAY_INT, ArrayType.ARRAY_INT, array};
            check(call(FunctionSet.ARRAY_SORTBY, keys), item.isGeoType() || item.hasNestedGeoType(),
                    "array_sortby does not support geo arguments: " + array.toSql());
        }
        FunctionCallExpr mapped = new FunctionCallExpr(FunctionSet.ARRAY_MAP, List.of(
                new LambdaFunctionExpr(List.of(NullLiteral.create(item))), slot(ArrayType.ARRAY_INT)));
        FunctionAnalyzer.analyze(mapped);
        FunctionCallExpr sorted = call(FunctionSet.ARRAY_SORTBY, ArrayType.ARRAY_INT, mapped.getType());
        sorted.setChild(1, mapped);
        check(sorted, item.isGeoType() || item.hasNestedGeoType(),
                "array_sortby does not support geo arguments: " + array.toSql());
    }

    @ParameterizedTest
    @ValueSource(strings = {"ST_GeogFromText(%s)", "ST_GeomFromText(%s, 'EPSG:3857')"})
    public void testSumMapAndToJson(String constructor) {
        String geo = constructor.formatted("'POINT (1 2)'");
        String nullGeo = constructor.formatted("CAST(NULL AS VARCHAR)");
        for (String value : List.of(geo, nullGeo)) {
            AnalyzeTestUtil.analyzeFail("SELECT cardinality(sum_map(map{1:" + value + "}))",
                    "sum_map unsupported value type:");
            AnalyzeTestUtil.analyzeFail("SELECT to_json(map{'g':" + value + "})",
                    "to_json does not support geo arguments:");
            AnalyzeTestUtil.analyzeFail("SELECT to_json(named_struct('g'," + value + "))",
                    "to_json does not support geo arguments:");
        }
        AnalyzeTestUtil.analyzeFail("SELECT to_json(named_struct('g',[" + geo + "]))",
                "to_json does not support geo arguments:");
        AnalyzeTestUtil.analyzeSuccess("SELECT sum_map(map{1:2})");
        AnalyzeTestUtil.analyzeSuccess("SELECT to_json(map{'g':1}), to_json(named_struct('g',1))");
        AnalyzeTestUtil.analyzeSuccess("SELECT to_json(named_struct('g',ST_AsText(" + geo + ")))");
    }

    @ParameterizedTest
    @MethodSource("types")
    public void testAggregates(Type item) {
        for (String name : List.of(FunctionSet.MAX_BY, FunctionSet.MIN_BY, FunctionSet.ARRAY_AGG, FunctionSet.MAP_AGG)) {
            Type result = name.equals(FunctionSet.ARRAY_AGG) ? new ArrayType(item) :
                    name.equals(FunctionSet.MAP_AGG) ? new MapType(IntegerType.INT, item) : item;
            Type[] args = name.equals(FunctionSet.ARRAY_AGG) ? new Type[] {item} :
                    name.equals(FunctionSet.MAP_AGG) ? new Type[] {IntegerType.INT, item} : new Type[] {item, IntegerType.INT};
            FunctionCallExpr call = call(name, args);
            call.setFn(new AggregateFunction(new FunctionName(name), Arrays.asList(args), result, null, false));
            call.setType(result);
            check(call, (name.equals(FunctionSet.MAX_BY) || name.equals(FunctionSet.MIN_BY)) && item.hasNestedGeoType(),
                    name + " does not support nested geo values");
        }
        for (Type result : List.of(item, new ArrayType(item))) {
            FunctionCallExpr ordered = new FunctionCallExpr(FunctionSet.ARRAY_AGG, new FunctionParams(false,
                    List.of(slot(item), slot(IntegerType.INT)), List.of(new OrderByElement(slot(IntegerType.INT), true, null))));
            ordered.setFn(new AggregateFunction(new FunctionName(FunctionSet.ARRAY_AGG),
                    List.of(item, IntegerType.INT), result, null, false));
            ordered.setType(result);
            check(ordered, item.isGeoType() || item.hasNestedGeoType(),
                    "ORDER BY aggregation is not supported for geo results: " + result.toSql());
        }
    }
}
