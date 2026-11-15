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

import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.type.AnyElementType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.GeoTypeDescriptor;
import com.starrocks.type.IntegerType;
import com.starrocks.type.JsonType;
import com.starrocks.type.MapType;
import com.starrocks.type.NullType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class GeoEncodingRefusalTest {
    static List<Type> types() {
        List<Type> types = new ArrayList<>();
        for (Type leaf : List.of(IntegerType.INT,
                ScalarType.createGeoType(PrimitiveType.GEOGRAPHY, new GeoTypeDescriptor(
                        GeoTypeDescriptor.LogicalType.GEOGRAPHY, GeoTypeDescriptor.CoordinateSystem.SPHERICAL,
                        GeoTypeDescriptor.EdgeAlgorithm.SPHERICAL, "OGC:CRS84", 4326)),
                ScalarType.createGeoType(PrimitiveType.GEOMETRY, GeoTypeDescriptor.geometry("EPSG:3857")))) {
            types.addAll(List.of(leaf, new ArrayType(leaf), new MapType(IntegerType.INT, leaf),
                    new StructType(List.of(leaf)), new ArrayType(new StructType(List.of(new ArrayType(leaf))))));
        }
        types.addAll(List.of(NullType.NULL, VarcharType.VARCHAR, VarbinaryType.VARBINARY, JsonType.JSON));
        return types;
    }

    private static SlotRef slot(Type type) {
        SlotRef slot = new SlotRef(null, "v");
        slot.setType(type);
        return slot;
    }

    @ParameterizedTest
    @MethodSource("types")
    public void testResolvedArguments(Type type) {
        for (String name : List.of(FunctionSet.ENCODE_SORT_KEY, FunctionSet.ENCODE_FINGERPRINT_SHA256,
                FunctionSet.ENCODE_FINGERPRINT_SHA256_V2)) {
            for (Expr value : List.of(slot(type), NullLiteral.create(type))) {
                for (int position : List.of(0, 1, 2)) {
                    List<Expr> args = new ArrayList<>(List.of(slot(IntegerType.INT), new NullLiteral()));
                    args.add(position, value);
                    FunctionCallExpr call = new FunctionCallExpr(name, args);
                    ScalarFunction declaration = new ScalarFunction(new FunctionName(name),
                            List.of(AnyElementType.ANY_ELEMENT), VarbinaryType.VARBINARY, true);
                    call.setFn(PolymorphicFunctionAnalyzer.generatePolymorphicFunction(declaration,
                            args.stream().map(Expr::getType).toArray(Type[]::new)));
                    if (type.isGeoType() || type.hasNestedGeoType()) {
                        assertEquals(type, call.getFn().getArgs()[position]);
                        assertEquals(name + " does not support geo arguments: " + type.toSql(),
                                assertThrows(SemanticException.class, () -> FunctionAnalyzer.analyze(call)).getDetailMsg());
                    } else {
                        FunctionAnalyzer.analyze(call);
                    }
                }
            }
        }
    }
}
