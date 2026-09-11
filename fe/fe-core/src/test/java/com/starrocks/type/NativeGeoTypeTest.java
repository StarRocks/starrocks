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

package com.starrocks.type;

import com.baidu.bjf.remoting.protobuf.ProtobufProxy;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.proto.GeoTypeDescPB;
import com.starrocks.proto.PTypeDesc;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.statistic.base.ColumnClassifier;
import com.starrocks.statistic.base.ComplexTypeColumnStats;
import com.starrocks.statistic.base.PrimitiveTypeColumnStats;
import com.starrocks.thrift.TGeoEdgeAlgorithm;
import com.starrocks.thrift.TGeoTypeDesc;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TTypeDesc;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.Test;

import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NativeGeoTypeTest {
    static GeoTypeDescriptor descriptor(PrimitiveType primitive) {
        boolean geography = primitive == PrimitiveType.GEOGRAPHY;
        return new GeoTypeDescriptor(
                geography ? GeoTypeDescriptor.LogicalType.GEOGRAPHY : GeoTypeDescriptor.LogicalType.GEOMETRY,
                geography ? GeoTypeDescriptor.CoordinateSystem.SPHERICAL : GeoTypeDescriptor.CoordinateSystem.CARTESIAN,
                geography ? GeoTypeDescriptor.EdgeAlgorithm.SPHERICAL : GeoTypeDescriptor.EdgeAlgorithm.PLANAR,
                "OGC:CRS84", 4326);
    }

    @Test
    public void testWireRoundTrips() throws Exception {
        for (PrimitiveType primitive : new PrimitiveType[] {PrimitiveType.GEOGRAPHY, PrimitiveType.GEOMETRY}) {
            ScalarType type = ScalarType.createGeoType(primitive, descriptor(primitive));
            TTypeDesc thrift = TypeSerializer.toThrift(type);
            assertEquals(TGeoTypeDesc.class, thrift.types.get(0).scalar_type.geo.getClass());
            assertEquals(primitive, TypeDeserializer.fromThrift(thrift.types.get(0).scalar_type.type));
            TTypeDesc decoded = new TTypeDesc();
            new TDeserializer().deserialize(decoded, new TSerializer().serialize(thrift));
            assertEquals(type, TypeDeserializer.fromThrift(decoded));
            var codec = ProtobufProxy.create(PTypeDesc.class);
            PTypeDesc protobuf = codec.decode(codec.encode(TypeSerializer.toProtobuf(type)));
            assertEquals(GeoTypeDescPB.class, protobuf.types.get(0).scalarType.geo.getClass());
            assertEquals(type, TypeDeserializer.fromProtobuf(protobuf));
            assertFalse(type.isSupported());
            assertSame(type.getGeoDescriptor(), type.clone().getGeoDescriptor());
            Type nested = new ArrayType(type);
            assertEquals(nested, TypeDeserializer.fromThrift(TypeSerializer.toThrift(nested)));
            assertEquals(nested, TypeDeserializer.fromProtobuf(TypeSerializer.toProtobuf(nested)));
        }
    }

    @Test
    public void testRejectConflictingPrimitive() {
        var geography = descriptor(PrimitiveType.GEOGRAPHY);
        assertThrows(IllegalArgumentException.class, () -> ScalarType.createGeoType(PrimitiveType.VARBINARY, geography));
        assertThrows(IllegalArgumentException.class, () -> ScalarType.createGeoType(PrimitiveType.GEOMETRY, geography));
        ScalarType type = ScalarType.createGeoType(PrimitiveType.GEOGRAPHY, geography);
        TTypeDesc thrift = TypeSerializer.toThrift(type);
        thrift.types.get(0).scalar_type.setType(TPrimitiveType.VARBINARY);
        assertThrows(IllegalArgumentException.class, () -> TypeDeserializer.fromThrift(thrift));
        PTypeDesc proto = TypeSerializer.toProtobuf(type);
        proto.types.get(0).scalarType.type = TPrimitiveType.GEOMETRY.getValue();
        assertThrows(IllegalArgumentException.class, () -> TypeDeserializer.fromProtobuf(proto));
    }

    @Test
    public void testRejectMissingMetadataAndWrongEdge() {
        ScalarType type = ScalarType.createGeoType(PrimitiveType.GEOGRAPHY, descriptor(PrimitiveType.GEOGRAPHY));
        TTypeDesc thrift = TypeSerializer.toThrift(type);
        thrift.types.get(0).scalar_type.geo.setEdge_algorithm(TGeoEdgeAlgorithm.PLANAR);
        assertThrows(IllegalArgumentException.class, () -> TypeDeserializer.fromThrift(thrift));
        thrift.types.get(0).scalar_type.unsetGeo();
        assertThrows(IllegalArgumentException.class, () -> TypeDeserializer.fromThrift(thrift));
        PTypeDesc proto = TypeSerializer.toProtobuf(type);
        proto.types.get(0).scalarType.geo = null;
        assertThrows(IllegalArgumentException.class, () -> TypeDeserializer.fromProtobuf(proto));
    }

    @Test
    public void testDescriptorIdentity() {
        var geography = descriptor(PrimitiveType.GEOGRAPHY);
        ScalarType type = ScalarType.createGeoType(PrimitiveType.GEOGRAPHY, geography);
        ScalarType copy = ScalarType.createGeoType(PrimitiveType.GEOGRAPHY, geography);
        assertEquals(type, copy);
        assertEquals(type.hashCode(), copy.hashCode());
        assertNotEquals(type, ScalarType.createGeoType(PrimitiveType.GEOMETRY, descriptor(PrimitiveType.GEOMETRY)));
        assertNotEquals(type, new ScalarType(PrimitiveType.VARBINARY));
        assertNotEquals(type, new ScalarType(PrimitiveType.GEOGRAPHY));
    }

    @Test
    public void testOrdinaryTypeUnchanged() {
        ScalarType type = new ScalarType(PrimitiveType.INT);
        assertEquals(type, TypeDeserializer.fromThrift(TypeSerializer.toThrift(type)));
        assertEquals(type, TypeDeserializer.fromProtobuf(TypeSerializer.toProtobuf(type)));
    }

    @Test
    public void testSemanticWireFixture() throws Exception {
        // Shared with the BE FrontendWireFixture test: no column/storage wrapper.
        String wire = "0a1c08001218081e2a1408011001180122094f47433a435253383428e621";
        var codec = ProtobufProxy.create(PTypeDesc.class);
        ScalarType type = ScalarType.createGeoType(PrimitiveType.GEOGRAPHY, descriptor(PrimitiveType.GEOGRAPHY));
        assertEquals(type, TypeDeserializer.fromProtobuf(codec.decode(HexFormat.of().parseHex(wire))));
        assertEquals(wire, HexFormat.of().formatHex(codec.encode(TypeSerializer.toProtobuf(type))));
    }

    @Test
    public void testBackendSemanticOnlyWireFixtures() throws Exception {
        // Produced by BE TypeDescriptor::to_thrift/to_protobuf with direct geo type metadata,
        // then serialized with Thrift TBinaryProtocol and protobuf SerializeAsString respectively.
        String[] protobufWires = {
                "0a2708001223081e10ffffffffffffffffff012a1408011001180122094f47433a435253383428e621",
                "0a2708001223081f10ffffffffffffffffff012a1408021002180622094f47433a435253383428e621"
        };
        String[] thriftWires = {
                "0f00010c00000001080001000000000c00020800010000001e080002ffffffff0c000608000100000001"
                        + "08000200000001080003000000010b0004000000094f47433a4352533834080005000010e600000000",
                "0f00010c00000001080001000000000c00020800010000001f080002ffffffff0c000608000100000002"
                        + "08000200000002080003000000060b0004000000094f47433a4352533834080005000010e600000000"
        };
        PrimitiveType[] primitives = {PrimitiveType.GEOGRAPHY, PrimitiveType.GEOMETRY};
        var codec = ProtobufProxy.create(PTypeDesc.class);
        for (int i = 0; i < primitives.length; i++) {
            ScalarType expected = ScalarType.createGeoType(primitives[i], descriptor(primitives[i]));
            PTypeDesc proto = codec.decode(HexFormat.of().parseHex(protobufWires[i]));
            TTypeDesc thrift = new TTypeDesc();
            new TDeserializer().deserialize(thrift, HexFormat.of().parseHex(thriftWires[i]));
            assertEquals(expected, TypeDeserializer.fromProtobuf(proto));
            assertEquals(expected, TypeDeserializer.fromThrift(thrift));
            // BE writes the unused scalar length (-1); FE omits it. Compare semantic wire content.
            proto.types.get(0).scalarType.len = null;
            thrift.types.get(0).scalar_type.unsetLen();
            assertEquals(HexFormat.of().formatHex(codec.encode(proto)),
                    HexFormat.of().formatHex(codec.encode(TypeSerializer.toProtobuf(expected))));
            assertEquals(thrift, TypeSerializer.toThrift(expected));
        }
    }

    @Test
    public void testRejectMissingSemanticMetadata() {
        for (PrimitiveType primitive : new PrimitiveType[] {PrimitiveType.GEOGRAPHY, PrimitiveType.GEOMETRY}) {
            ScalarType type = ScalarType.createGeoType(primitive, descriptor(primitive));
            TTypeDesc thrift = TypeSerializer.toThrift(type);
            PTypeDesc proto = TypeSerializer.toProtobuf(type);
            thrift.types.get(0).scalar_type.setGeo(new TGeoTypeDesc());
            proto.types.get(0).scalarType.geo = new GeoTypeDescPB();
            assertThrows(IllegalArgumentException.class, () -> TypeDeserializer.fromThrift(thrift));
            assertThrows(IllegalArgumentException.class, () -> TypeDeserializer.fromProtobuf(proto));
        }
    }

    @Test
    public void testSemanticFieldsPreservedWithoutSrid() throws Exception {
        for (PrimitiveType primitive : new PrimitiveType[] {PrimitiveType.GEOGRAPHY, PrimitiveType.GEOMETRY}) {
            GeoTypeDescriptor metadata = descriptor(primitive);
            ScalarType expected = ScalarType.createGeoType(primitive, new GeoTypeDescriptor(metadata.logicalType(),
                    metadata.coordinateSystem(), metadata.edgeAlgorithm(), metadata.crs(), null));
            TTypeDesc thrift = TypeSerializer.toThrift(expected);
            assertFalse(thrift.types.get(0).scalar_type.geo.isSetSrid());
            TTypeDesc decoded = new TTypeDesc();
            new TDeserializer().deserialize(decoded, new TSerializer().serialize(thrift));
            var codec = ProtobufProxy.create(PTypeDesc.class);
            PTypeDesc proto = codec.decode(codec.encode(TypeSerializer.toProtobuf(expected)));
            assertNull(proto.types.get(0).scalarType.geo.srid);
            assertEquals(expected, TypeDeserializer.fromThrift(decoded));
            assertEquals(expected, TypeDeserializer.fromProtobuf(proto));
        }
    }

    @Test
    public void testCompatibilityMatrixAfterGeoReordering() {
        assertEquals(PrimitiveType.UNKNOWN_TYPE, PrimitiveType.values()[PrimitiveType.values().length - 1]);
        assertTrue(PrimitiveType.GEOGRAPHY.ordinal() < PrimitiveType.UNKNOWN_TYPE.ordinal());
        assertTrue(PrimitiveType.GEOMETRY.ordinal() < PrimitiveType.UNKNOWN_TYPE.ordinal());
        assertEquals(PrimitiveType.BIGINT,
                TypeCompatibilityMatrix.getCompatibleType(PrimitiveType.INT, PrimitiveType.BIGINT));
        assertEquals(PrimitiveType.INVALID_TYPE,
                TypeCompatibilityMatrix.getCompatibleType(PrimitiveType.INT, PrimitiveType.UNKNOWN_TYPE));
        assertEquals(PrimitiveType.UNKNOWN_TYPE,
                TypeCompatibilityMatrix.getCompatibleType(PrimitiveType.UNKNOWN_TYPE, PrimitiveType.UNKNOWN_TYPE));
    }

    @Test
    public void testGeoCapabilities() {
        for (PrimitiveType primitive : new PrimitiveType[] {PrimitiveType.GEOGRAPHY, PrimitiveType.GEOMETRY}) {
            ScalarType type = ScalarType.createGeoType(primitive, descriptor(primitive));
            assertGeoCapabilitiesRejected(new ScalarType(primitive));
            assertGeoCapabilitiesRejected(type);
            assertGeoCapabilitiesRejected(TypeDeserializer.fromThrift(TypeSerializer.toThrift(type)));
            assertGeoCapabilitiesRejected(TypeDeserializer.fromProtobuf(TypeSerializer.toProtobuf(type)));
        }
    }

    @Test
    public void testNestedGeoCapabilities() {
        for (PrimitiveType primitive : new PrimitiveType[] {PrimitiveType.GEOGRAPHY, PrimitiveType.GEOMETRY}) {
            ScalarType geo = ScalarType.createGeoType(primitive, descriptor(primitive));
            for (Type type : List.of(new ArrayType(geo), new MapType(IntegerType.INT, geo),
                    new MapType(geo, IntegerType.INT),
                    new StructType(List.of(IntegerType.INT, geo)),
                    new ArrayType(new StructType(List.of(new ArrayType(geo)))))) {
                assertGeoCapabilitiesRejected(type);
                assertGeoCapabilitiesRejected(TypeDeserializer.fromThrift(TypeSerializer.toThrift(type)));
                assertGeoCapabilitiesRejected(TypeDeserializer.fromProtobuf(TypeSerializer.toProtobuf(type)));
            }
        }
    }

    @Test
    public void testOrdinaryCapabilitiesUnchanged() {
        for (Type type : List.of(IntegerType.INT, VarcharType.VARCHAR, DateType.DATE,
                new ArrayType(IntegerType.INT), new StructType(List.of(IntegerType.INT)))) {
            assertTrue(type.canJoinOn());
            assertTrue(type.canGroupBy());
            assertTrue(type.canOrderBy());
            assertTrue(type.canDistinct());
            assertEquals(!type.isComplexType(), type.canDistributedBy());
        }
        Type map = new MapType(IntegerType.INT, VarcharType.VARCHAR);
        assertTrue(map.canJoinOn());
        assertTrue(map.canGroupBy());
        assertTrue(map.canDistinct());
        assertFalse(map.canOrderBy());
        assertFalse(map.canDistributedBy());
        Type binary = new ScalarType(PrimitiveType.VARBINARY);
        assertTrue(binary.canJoinOn());
        assertTrue(binary.canGroupBy());
        assertTrue(binary.canOrderBy());
        assertTrue(binary.canDistributedBy());
        assertFalse(binary.canDistinct());
        assertGeoCapabilitiesRejected(new ScalarType(PrimitiveType.JSON));
    }

    private static void assertGeoCapabilitiesRejected(Type type) {
        assertFalse(type.canJoinOn(), type.toSql());
        assertFalse(type.canGroupBy(), type.toSql());
        assertFalse(type.canOrderBy(), type.toSql());
        assertFalse(type.canDistinct(), type.toSql());
        assertFalse(type.canDistributedBy(), type.toSql());
        assertFalse(type.canBeMVKey(), type.toSql());
    }

    @Test
    public void testGeoPartitionBy() {
        for (PrimitiveType primitive : new PrimitiveType[] {PrimitiveType.GEOGRAPHY, PrimitiveType.GEOMETRY}) {
            ScalarType geo = ScalarType.createGeoType(primitive, descriptor(primitive));
            for (Type scalar : List.of(new ScalarType(primitive), geo,
                    TypeDeserializer.fromThrift(TypeSerializer.toThrift(geo)),
                    TypeDeserializer.fromProtobuf(TypeSerializer.toProtobuf(geo)))) {
                for (Type type : List.of(scalar, new ArrayType(scalar), new ArrayType(new ArrayType(scalar)),
                        new MapType(IntegerType.INT, scalar), new StructType(List.of(scalar)))) {
                    assertFalse(type.canPartitionBy(), type.toSql());
                }
            }
        }
    }

    @Test
    public void testGeoStatistics() {
        for (PrimitiveType primitive : new PrimitiveType[] {PrimitiveType.GEOGRAPHY, PrimitiveType.GEOMETRY}) {
            ScalarType geo = ScalarType.createGeoType(primitive, descriptor(primitive));
            for (Type type : List.of(new ScalarType(primitive), geo,
                    TypeDeserializer.fromThrift(TypeSerializer.toThrift(geo)),
                    TypeDeserializer.fromProtobuf(TypeSerializer.toProtobuf(geo)))) {
                assertFalse(type.canStatistic(), type.toSql());
                Table table = new Table(1, "geo_stats", Table.TableType.OLAP,
                        List.of(new Column("g", type), new Column("i", IntegerType.INT)));
                for (boolean manual : new boolean[] {false, true}) {
                    ColumnClassifier classifier = ColumnClassifier.of(
                            List.of("g", "i"), List.of(type, IntegerType.INT), table, manual);
                    assertEquals(1, classifier.getColumnStats().size());
                    assertTrue(classifier.getColumnStats().get(0) instanceof PrimitiveTypeColumnStats);
                    assertEquals(1, classifier.getUnSupportCollectColumns().size());
                    var unsupported = classifier.getUnSupportCollectColumns().get(0);
                    assertTrue(unsupported instanceof ComplexTypeColumnStats);
                    assertEquals("''", unsupported.getMax());
                    assertEquals("''", unsupported.getMin());
                    assertEquals("00", unsupported.getNDV());
                }
            }
        }
    }

    @Test
    public void testGeoPseudoTypeMatching() {
        for (PrimitiveType primitive : new PrimitiveType[] {PrimitiveType.GEOGRAPHY, PrimitiveType.GEOMETRY}) {
            ScalarType geo = ScalarType.createGeoType(primitive, descriptor(primitive));
            for (Type type : List.of(new ScalarType(primitive), geo,
                    TypeDeserializer.fromThrift(TypeSerializer.toThrift(geo)),
                    TypeDeserializer.fromProtobuf(TypeSerializer.toProtobuf(geo)))) {
                assertTrue(type.matchesType(AnyElementType.ANY_ELEMENT));
                assertTrue(AnyElementType.ANY_ELEMENT.matchesType(type));
                assertFalse(type.matchesType(AnyArrayType.ANY_ARRAY));
                assertFalse(type.matchesType(AnyMapType.ANY_MAP));
                assertFalse(type.matchesType(AnyStructType.ANY_STRUCT));
                assertFalse(type.matchesType(IntegerType.INT));
                assertFalse(type.matchesType(VarbinaryType.VARBINARY));
                assertEquals(geo.equals(type), geo.matchesType(type));
            }
            GeoTypeDescriptor metadata = descriptor(primitive);
            ScalarType differentSrid = ScalarType.createGeoType(primitive, new GeoTypeDescriptor(
                    metadata.logicalType(), metadata.coordinateSystem(), metadata.edgeAlgorithm(), metadata.crs(), 3857));
            assertFalse(geo.matchesType(differentSrid));
            assertFalse(differentSrid.matchesType(geo));
        }
        assertFalse(ScalarType.createGeoType(PrimitiveType.GEOGRAPHY, descriptor(PrimitiveType.GEOGRAPHY))
                .matchesType(ScalarType.createGeoType(PrimitiveType.GEOMETRY, descriptor(PrimitiveType.GEOMETRY))));
    }

    @Test
    public void testOrdinaryPartitionStatisticsAndPseudoTypes() {
        for (Type type : List.of(IntegerType.INT, VarcharType.VARCHAR, DateType.DATE,
                new ArrayType(IntegerType.INT), new ArrayType(new ArrayType(IntegerType.INT)))) {
            assertTrue(type.canPartitionBy());
            assertTrue(type.canStatistic());
            assertTrue(type.matchesType(AnyElementType.ANY_ELEMENT));
        }
        Type map = new MapType(IntegerType.INT, VarcharType.VARCHAR);
        assertFalse(map.canPartitionBy());
        assertTrue(map.canStatistic());
        for (Type type : List.of(JsonType.JSON, VarbinaryType.VARBINARY,
                new StructType(List.of(IntegerType.INT)))) {
            assertFalse(type.canPartitionBy());
            assertFalse(type.canStatistic());
        }
    }

    @Test
    public void testRejectEmptyCrsEvenWithSrid() throws Exception {
        var codec = ProtobufProxy.create(PTypeDesc.class);
        for (PrimitiveType primitive : new PrimitiveType[] {PrimitiveType.GEOGRAPHY, PrimitiveType.GEOMETRY}) {
            GeoTypeDescriptor metadata = descriptor(primitive);
            for (Integer srid : new Integer[] {null, 4326}) {
                GeoTypeDescriptor emptyCrs = new GeoTypeDescriptor(metadata.logicalType(), metadata.coordinateSystem(),
                        metadata.edgeAlgorithm(), "", srid);
                assertThrows(IllegalArgumentException.class, () -> ScalarType.createGeoType(primitive, emptyCrs));
                ScalarType valid = ScalarType.createGeoType(primitive, new GeoTypeDescriptor(metadata.logicalType(),
                        metadata.coordinateSystem(), metadata.edgeAlgorithm(), metadata.crs(), srid));
                assertEquals(valid, TypeDeserializer.fromThrift(TypeSerializer.toThrift(valid)));
                assertEquals(valid, TypeDeserializer.fromProtobuf(TypeSerializer.toProtobuf(valid)));
                for (boolean missing : new boolean[] {false, true}) {
                    TTypeDesc thrift = TypeSerializer.toThrift(valid);
                    if (missing) {
                        thrift.types.get(0).scalar_type.geo.unsetCrs();
                    } else {
                        thrift.types.get(0).scalar_type.geo.setCrs("");
                    }
                    TTypeDesc decoded = new TTypeDesc();
                    new TDeserializer().deserialize(decoded, new TSerializer().serialize(thrift));
                    assertThrows(IllegalArgumentException.class, () -> TypeDeserializer.fromThrift(decoded));
                    PTypeDesc proto = TypeSerializer.toProtobuf(valid);
                    proto.types.get(0).scalarType.geo.crs = missing ? null : "";
                    PTypeDesc decodedProto = codec.decode(codec.encode(proto));
                    assertThrows(IllegalArgumentException.class, () -> TypeDeserializer.fromProtobuf(decodedProto));
                }
            }
        }
    }

    @Test
    public void testUnsupportedGeoCommonTypes() {
        List<ScalarType> ordinary = List.of(BooleanType.BOOLEAN, IntegerType.INT, IntegerType.BIGINT,
                FloatType.DOUBLE, DateType.DATE, DateType.DATETIME, VarcharType.VARCHAR,
                TypeFactory.createVarcharType(16), TypeFactory.createCharType(8), VarbinaryType.VARBINARY,
                TypeFactory.createDecimalV2Type(10, 2),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 2),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 8),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL256, 76, 8), JsonType.JSON, VariantType.VARIANT);
        for (PrimitiveType primitive : new PrimitiveType[] {PrimitiveType.GEOGRAPHY, PrimitiveType.GEOMETRY}) {
            GeoTypeDescriptor metadata = descriptor(primitive);
            ScalarType geo = ScalarType.createGeoType(primitive, metadata);
            for (ScalarType other : ordinary) {
                assertIncompatibleGeoPair(geo, other);
            }
            assertIncompatibleGeoPair(geo, new ScalarType(primitive));
            assertIncompatibleGeoPair(geo, ScalarType.createGeoType(primitive, new GeoTypeDescriptor(
                    metadata.logicalType(), metadata.coordinateSystem(), metadata.edgeAlgorithm(), "EPSG:4326", 4326)));
            assertIncompatibleGeoPair(geo, ScalarType.createGeoType(primitive, new GeoTypeDescriptor(
                    metadata.logicalType(), metadata.coordinateSystem(), metadata.edgeAlgorithm(), metadata.crs(), 3857)));
        }
        assertIncompatibleGeoPair(
                ScalarType.createGeoType(PrimitiveType.GEOGRAPHY, descriptor(PrimitiveType.GEOGRAPHY)),
                ScalarType.createGeoType(PrimitiveType.GEOMETRY, descriptor(PrimitiveType.GEOMETRY)));
    }

    @Test
    public void testNestedGeoCommonTypeDiagnostic() {
        for (PrimitiveType primitive : new PrimitiveType[] {PrimitiveType.GEOGRAPHY, PrimitiveType.GEOMETRY}) {
            ScalarType geo = ScalarType.createGeoType(primitive, descriptor(primitive));
            List<Type> geoTypes = List.of(geo, new ArrayType(geo), new MapType(IntegerType.INT, geo),
                    new MapType(geo, IntegerType.INT), new StructType(List.of(geo)));
            List<Type> intTypes = List.of(IntegerType.INT, new ArrayType(IntegerType.INT),
                    new MapType(IntegerType.INT, IntegerType.INT), new MapType(IntegerType.INT, IntegerType.INT),
                    new StructType(List.of(IntegerType.INT)));
            for (int i = 0; i < geoTypes.size(); i++) {
                Type left = geoTypes.get(i);
                Type right = intTypes.get(i);
                assertTrue(TypeManager.getCommonSuperType(left, right).isInvalid());
                assertTrue(TypeManager.getCommonSuperType(right, left).isInvalid());
                SemanticException error = assertThrows(SemanticException.class,
                        () -> TypeManager.getCommonSuperType(List.of(left, right)));
                assertTrue(error.getMessage().contains("cannot be matched"));
            }
        }
    }

    @Test
    public void testCommonTypeShortcutsUnchanged() {
        for (PrimitiveType primitive : new PrimitiveType[] {PrimitiveType.GEOGRAPHY, PrimitiveType.GEOMETRY}) {
            ScalarType geo = ScalarType.createGeoType(primitive, descriptor(primitive));
            for (boolean strict : new boolean[] {false, true}) {
                assertSame(geo, TypeManager.getAssignmentCompatibleType(geo, geo.clone(), strict));
                assertSame(geo, TypeManager.getAssignmentCompatibleType(geo, NullType.NULL, strict));
                assertSame(geo, TypeManager.getAssignmentCompatibleType(NullType.NULL, geo, strict));
                assertSame(UnknownType.UNKNOWN_TYPE,
                        TypeManager.getAssignmentCompatibleType(geo, UnknownType.UNKNOWN_TYPE, strict));
                assertSame(InvalidType.INVALID,
                        TypeManager.getAssignmentCompatibleType(geo, InvalidType.INVALID, strict));
            }
        }
        assertEquals(IntegerType.BIGINT, TypeManager.getCommonSuperType(IntegerType.INT, IntegerType.BIGINT));
        assertEquals(VarcharType.VARCHAR, TypeManager.getCommonSuperType(IntegerType.INT, VarcharType.VARCHAR));
        assertEquals(TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2), TypeManager.getCommonSuperType(
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 2),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2)));
    }

    private static void assertIncompatibleGeoPair(ScalarType left, ScalarType right) {
        for (boolean strict : new boolean[] {false, true}) {
            assertSame(InvalidType.INVALID, TypeManager.getAssignmentCompatibleType(left, right, strict));
            assertSame(InvalidType.INVALID, TypeManager.getAssignmentCompatibleType(right, left, strict));
        }
    }
}
