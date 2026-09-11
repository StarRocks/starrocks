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
import com.starrocks.proto.PTypeDesc;
import com.starrocks.thrift.TGeoEdgeAlgorithm;
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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NativeGeoTypeTest {
    static GeoColumnDescriptor descriptor(PrimitiveType primitive) {
        boolean geography = primitive == PrimitiveType.GEOGRAPHY;
        return new GeoColumnDescriptor(
                geography ? GeoColumnDescriptor.LogicalType.GEOGRAPHY : GeoColumnDescriptor.LogicalType.GEOMETRY,
                geography ? GeoColumnDescriptor.CoordinateSystem.SPHERICAL : GeoColumnDescriptor.CoordinateSystem.CARTESIAN,
                geography ? GeoColumnDescriptor.EdgeAlgorithm.SPHERICAL : GeoColumnDescriptor.EdgeAlgorithm.PLANAR,
                "OGC:CRS84", 4326, GeoColumnDescriptor.Encoding.WKB, GeoColumnDescriptor.Dimension.XY,
                GeoColumnDescriptor.ValidationState.UNVALIDATED);
    }

    @Test
    public void testWireRoundTrips() throws Exception {
        for (PrimitiveType primitive : new PrimitiveType[] {PrimitiveType.GEOGRAPHY, PrimitiveType.GEOMETRY}) {
            ScalarType type = ScalarType.createGeoType(primitive, descriptor(primitive));
            TTypeDesc thrift = TypeSerializer.toThrift(type);
            assertEquals(primitive, TypeDeserializer.fromThrift(thrift.types.get(0).scalar_type.type));
            TTypeDesc decoded = new TTypeDesc();
            new TDeserializer().deserialize(decoded, new TSerializer().serialize(thrift));
            assertEquals(type, TypeDeserializer.fromThrift(decoded));
            var codec = ProtobufProxy.create(PTypeDesc.class);
            PTypeDesc protobuf = codec.decode(codec.encode(TypeSerializer.toProtobuf(type)));
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
        thrift.types.get(0).scalar_type.geo.type.setEdge_algorithm(TGeoEdgeAlgorithm.PLANAR);
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
    public void testBackendWireFixture() throws Exception {
        // Shared with native_geo_type_test.cpp: not just a same-language round trip.
        String wire = "0a2608001222081e2a1e0a1408011001180122094f47433a435253383428e6211206080110011801";
        var codec = ProtobufProxy.create(PTypeDesc.class);
        ScalarType type = ScalarType.createGeoType(PrimitiveType.GEOGRAPHY, descriptor(PrimitiveType.GEOGRAPHY));
        assertEquals(type, TypeDeserializer.fromProtobuf(codec.decode(HexFormat.of().parseHex(wire))));
        assertEquals(wire, HexFormat.of().formatHex(codec.encode(TypeSerializer.toProtobuf(type))));
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
}
