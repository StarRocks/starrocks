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

package com.starrocks.sql;

import com.starrocks.catalog.Column;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.type.ArrayType;
import com.starrocks.type.GeoTypeDescriptor;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class InsertPlannerGeoAssignmentTest {
    private static List<Type> shapes(Type leaf) {
        return List.of(leaf, new ArrayType(leaf), new MapType(VarcharType.VARCHAR, leaf),
                new StructType(List.of(leaf)), new ArrayType(new ArrayType(leaf)));
    }

    private static Stream<Type> geoTypes() {
        return Stream.of(ScalarType.createGeoType(PrimitiveType.GEOGRAPHY,
                new GeoTypeDescriptor(GeoTypeDescriptor.LogicalType.GEOGRAPHY,
                        GeoTypeDescriptor.CoordinateSystem.SPHERICAL, GeoTypeDescriptor.EdgeAlgorithm.SPHERICAL,
                        "OGC:CRS84", 4326)),
                geometry("EPSG:3857"), geometry("EPSG:4326"), geometry("LOCAL:linear"));
    }

    private static Type geometry(String crs) {
        return ScalarType.createGeoType(PrimitiveType.GEOMETRY, GeoTypeDescriptor.geometry(crs));
    }

    private static Stream<Arguments> assignments() {
        Stream.Builder<Arguments> cases = Stream.builder();
        geoTypes().forEach(geo -> {
            for (Type ordinary : List.of(new VarcharType(256), VarbinaryType.VARBINARY)) {
                List<Type> sources = shapes(geo);
                List<Type> targets = shapes(ordinary);
                for (int i = 0; i < sources.size(); i++) {
                    cases.add(Arguments.of(sources.get(i), targets.get(i)));
                }
            }
        });
        return cases.build();
    }

    private static Stream<Arguments> identities() {
        List<Type> sources = geoTypes().flatMap(geo -> shapes(geo).stream()).toList();
        List<Type> targets = geoTypes().flatMap(geo -> shapes(geo).stream()).toList();
        return IntStream.range(0, sources.size()).mapToObj(i -> Arguments.of(sources.get(i), targets.get(i)));
    }

    private static void assign(Type source, Type target) {
        InsertPlanner.checkGeoAssignment(source, new Column("payload", target));
    }

    @ParameterizedTest
    @MethodSource("assignments")
    public void testRejectGeoAssignmentCasts(Type source, Type target) {
        SemanticException error = assertThrows(SemanticException.class, () -> assign(source, target));
        assertEquals("Cannot assign " + source.toSql() + " to column 'payload' of type " + target.toSql() +
                " in INSERT: native geo assignments require identical types (including geo descriptors). " +
                "Use ST_AsText/ST_AsBinary for VARCHAR/VARBINARY targets", error.getDetailMsg());
        assertThrows(SemanticException.class, () -> assign(target, source));
    }

    @ParameterizedTest
    @MethodSource("identities")
    public void testGeoIdentity(Type source, Type target) {
        assertNotSame(source, target);
        assertEquals(source, target);
        assertDoesNotThrow(() -> assign(source, target));
    }

    @Test
    public void testRejectDifferentGeoDescriptors() {
        assertThrows(SemanticException.class, () -> assign(geometry("EPSG:3857"), geometry("EPSG:4326")));
        assertThrows(SemanticException.class, () ->
                assign(new ArrayType(geometry("EPSG:3857")), new ArrayType(geometry("EPSG:4326"))));
    }

    @Test
    public void testIcebergGeographyIdentity() {
        Type source = geoTypes().findFirst().orElseThrow();
        Type target = ColumnTypeConverter.fromIcebergType(Types.GeographyType.crs84());
        assertEquals(source, target);
        assertDoesNotThrow(() -> assign(source, target));
    }

    @Test
    public void testOrdinaryAssignment() {
        assertDoesNotThrow(() -> assign(IntegerType.INT, IntegerType.BIGINT));
    }
}
