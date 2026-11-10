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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.MultiInPredicate;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.type.GeoTypeDescriptor;
import com.starrocks.type.IntegerType;
import com.starrocks.type.NullType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ScalarGeoMultiInTest {
    private static final String ERROR = "InPredicate of GEOGRAPHY/GEOMETRY types is not supported";
    private final ExpressionAnalyzer.Visitor visitor = new ExpressionAnalyzer.Visitor(
            new AnalyzeState(), new ConnectContext());

    private MultiInPredicate predicate(Type left, Type right, boolean notIn, boolean geoFirst) {
        var lhs = NullLiteral.create(left);
        var rhs = NullLiteral.create(right);
        var values = new ValuesRelation(List.of(geoFirst ? List.of(rhs, new IntLiteral(1)) :
                List.of(new IntLiteral(1), rhs)), List.of("a", "b"));
        var subquery = new Subquery(new QueryStatement(values));
        subquery.setType(IntegerType.BIGINT);
        return new MultiInPredicate(geoFirst ? List.of(lhs, new IntLiteral(1)) :
                List.of(new IntLiteral(1), lhs), subquery, notIn);
    }

    @Test
    public void testScalarGeoOnEitherSide() {
        var geography = new GeoTypeDescriptor(GeoTypeDescriptor.LogicalType.GEOGRAPHY,
                GeoTypeDescriptor.CoordinateSystem.SPHERICAL, GeoTypeDescriptor.EdgeAlgorithm.SPHERICAL,
                "OGC:CRS84", 4326);
        for (Type geo : List.of(ScalarType.createGeoType(PrimitiveType.GEOGRAPHY, geography),
                ScalarType.createGeoType(PrimitiveType.GEOMETRY, GeoTypeDescriptor.geometry("EPSG:3857")))) {
            for (boolean notIn : List.of(false, true)) {
                for (boolean geoFirst : List.of(false, true)) {
                    for (Type other : List.of(geo, NullType.NULL, IntegerType.INT)) {
                        assertEquals(ERROR, assertThrows(SemanticException.class, () ->
                                visitor.visitMultiInPredicate(predicate(geo, other, notIn, geoFirst), null)).getDetailMsg());
                        assertEquals(ERROR, assertThrows(SemanticException.class, () ->
                                visitor.visitMultiInPredicate(predicate(other, geo, notIn, geoFirst), null)).getDetailMsg());
                    }
                }
            }
        }
    }

    @Test
    public void testOrdinaryTypesAndArity() {
        visitor.visitMultiInPredicate(predicate(IntegerType.INT, IntegerType.BIGINT, false, false), null);
        visitor.visitMultiInPredicate(predicate(NullType.NULL, IntegerType.BIGINT, true, true), null);
        var values = new ValuesRelation(List.of(List.of(new IntLiteral(1))), List.of("id"));
        var subquery = new Subquery(new QueryStatement(values));
        subquery.setType(IntegerType.BIGINT);
        var predicate = new MultiInPredicate(List.of(new IntLiteral(1), new IntLiteral(2)), subquery, false);
        assertEquals("subquery must return the same number of columns as provided by the IN predicate",
                assertThrows(SemanticException.class, () -> visitor.visitMultiInPredicate(predicate, null)).getDetailMsg());
    }
}
