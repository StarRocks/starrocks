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

package com.starrocks.connector.iceberg;

import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.thrift.TIcebergGeoKind;
import com.starrocks.thrift.TIcebergGeoMetadata;
import com.starrocks.thrift.TIcebergSchema;
import com.starrocks.thrift.TIcebergSchemaField;
import com.starrocks.type.ArrayType;
import com.starrocks.type.GeoTypeDescriptor;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.UnknownType;
import com.starrocks.type.VarbinaryType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.EdgeAlgorithm;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class IcebergGeoMetadataTest {
    @Test
    public void geographyAlgorithmsSurviveSchemaAndWireRoundTrips() throws Exception {
        for (EdgeAlgorithm edge : EdgeAlgorithm.values()) {
            for (String crs : new String[] {"OGC:CRS84", "EPSG:4326", "srid:4326"}) {
                Types.GeographyType type = Types.GeographyType.of(crs, edge);
                Schema schema = new Schema(Types.NestedField.optional(1, "shape", type));
                Schema restored = SchemaParser.fromJson(SchemaParser.toJson(schema));
                TIcebergSchema wire = IcebergApiConverter.getTIcebergSchema(restored);
                TIcebergSchema decoded = new TIcebergSchema();
                new TDeserializer(new TCompactProtocol.Factory()).deserialize(decoded,
                        new TSerializer(new TCompactProtocol.Factory()).serialize(wire));
                Assertions.assertEquals(wire, decoded);
                TIcebergGeoMetadata geo = decoded.getFields().get(0).getGeo_metadata();
                Assertions.assertEquals(TIcebergGeoKind.GEOGRAPHY, geo.getKind());
                Assertions.assertEquals(crs, geo.getCrs());
                Assertions.assertEquals(edge.name(), geo.getEdge_algorithm());
                boolean canonical = crs.equals("OGC:CRS84") && edge == EdgeAlgorithm.SPHERICAL;
                Assertions.assertEquals(!canonical, ColumnTypeConverter.fromIcebergType(type).isUnknown());
            }
        }
    }

    @Test
    public void defaultsGeometryAndOrdinaryBinaryRemainDistinct() {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "geog", Types.GeographyType.crs84()),
                Types.NestedField.optional(2, "geom", Types.GeometryType.of("EPSG:3857")),
                Types.NestedField.optional(3, "bytes", Types.BinaryType.get()),
                Types.NestedField.required(4, "id", Types.IntegerType.get()));
        TIcebergSchema wire = IcebergApiConverter.getTIcebergSchema(schema);
        Assertions.assertEquals("OGC:CRS84", wire.getFields().get(0).getGeo_metadata().getCrs());
        Assertions.assertEquals("SPHERICAL", wire.getFields().get(0).getGeo_metadata().getEdge_algorithm());
        Assertions.assertEquals(TIcebergGeoKind.GEOMETRY, wire.getFields().get(1).getGeo_metadata().getKind());
        Assertions.assertEquals("EPSG:3857", wire.getFields().get(1).getGeo_metadata().getCrs());
        Assertions.assertEquals("PLANAR", wire.getFields().get(1).getGeo_metadata().getEdge_algorithm());
        Assertions.assertFalse(wire.getFields().get(2).isSetGeo_metadata());
        Assertions.assertFalse(wire.getFields().get(3).isSetGeo_metadata());
        Assertions.assertEquals(VarbinaryType.VARBINARY, ColumnTypeConverter.fromIcebergType(Types.BinaryType.get()));
        Assertions.assertEquals(IntegerType.INT, ColumnTypeConverter.fromIcebergType(Types.IntegerType.get()));
    }

    @ParameterizedTest
    @CsvSource(value = {"OGC:CRS84, OGC:CRS84, 4326", "NULL, OGC:CRS84, 4326", "EPSG:3857, EPSG:3857, 3857",
            "EPSG:4326, EPSG:4326, 4326", "EPSG:1, EPSG:1, 1", "EPSG:2147483647, EPSG:2147483647, 2147483647",
            "EPSG:0004326, EPSG:0004326, 4326"}, nullValues = "NULL")
    public void geometryTypeAndScanMetadataAgree(String crs, String expectedCrs, int expectedSrid) {
        Types.GeometryType geometry = geometryWithCrs(crs);
        var mapped = ColumnTypeConverter.fromIcebergType(geometry);
        Assertions.assertEquals(PrimitiveType.GEOMETRY, mapped.getPrimitiveType());
        GeoTypeDescriptor descriptor = ((ScalarType) mapped).getGeoDescriptor();
        Assertions.assertEquals(new GeoTypeDescriptor(GeoTypeDescriptor.LogicalType.GEOMETRY,
                GeoTypeDescriptor.EdgeAlgorithm.PLANAR,
                expectedCrs, expectedSrid), descriptor);
        var array = Assertions.assertInstanceOf(ArrayType.class,
                ColumnTypeConverter.fromIcebergType(Types.ListType.ofOptional(2, geometry)));
        Assertions.assertEquals(mapped, array.getItemType());

        Schema schema = new Schema(Types.NestedField.optional(1, "g", geometry));
        var field = IcebergApiConverter.getTIcebergSchema(schema).getFields().get(0);
        Assertions.assertEquals(1, field.getField_id());
        TIcebergGeoMetadata geo = field.getGeo_metadata();
        Assertions.assertEquals(descriptor.logicalType().name(), geo.getKind().name());
        Assertions.assertEquals(descriptor.crs(), geo.getCrs());
        Assertions.assertEquals(descriptor.edgeAlgorithm().name(), geo.getEdge_algorithm());
    }

    @ParameterizedTest
    @ValueSource(strings = {"EPSG:0", "EPSG:-1", "EPSG:+1", "EPSG:1a", "EPSG:2147483648", "IGNF:ATI",
            "{\"type\":\"ProjectedCRS\"}", "", "epsg:3857"})
    public void unsupportedGeometryCrsMapsToUnknown(String crs) {
        Types.GeometryType geometry = geometryWithCrs(crs);
        Assertions.assertEquals(UnknownType.UNKNOWN_TYPE, ColumnTypeConverter.fromIcebergType(geometry));
        assertUnsupportedContainers(geometry);
    }

    private static Types.GeometryType geometryWithCrs(String crs) {
        if (crs == null || crs.isEmpty()) {
            // Iceberg substitutes CRS84 for null and rejects empty CRS before type conversion.
            Types.GeometryType geometry = spy(Types.GeometryType.crs84());
            doReturn(crs).when(geometry).crs();
            return geometry;
        }
        return Types.GeometryType.of(crs);
    }

    @Test
    public void unsupportedNestedTypesMapTheWholeColumnToUnknown() {
        assertUnsupportedContainers(Types.FixedType.ofLength(16));
        assertUnsupportedContainers(Types.GeographyType.of("OGC:CRS84", EdgeAlgorithm.VINCENTY));
    }

    private static void assertUnsupportedContainers(org.apache.iceberg.types.Type leaf) {
        var list = Types.ListType.ofOptional(2, leaf);
        var map = Types.MapType.ofOptional(2, 3, Types.StringType.get(), leaf);
        var struct = Types.StructType.of(Types.NestedField.optional(4, "g", leaf),
                Types.NestedField.optional(5, "id", Types.IntegerType.get()));
        for (var type : List.of(list, map, struct, Types.ListType.ofOptional(3, struct))) {
            Assertions.assertEquals(UnknownType.UNKNOWN_TYPE, ColumnTypeConverter.fromIcebergType(type));
        }
    }

    @Test
    public void geoMapKeysRemainUnsupportedAtAnyDepth() {
        for (var geo : List.of(Types.GeometryType.crs84(), Types.GeographyType.crs84())) {
            var directKey = Types.MapType.ofOptional(2, 3, geo, Types.IntegerType.get());
            var structKey = Types.MapType.ofOptional(2, 3,
                    Types.StructType.of(Types.NestedField.optional(4, "g", geo)), Types.IntegerType.get());
            for (var map : List.of(directKey, structKey)) {
                Assertions.assertEquals(UnknownType.UNKNOWN_TYPE, ColumnTypeConverter.fromIcebergType(map));
                Assertions.assertEquals(UnknownType.UNKNOWN_TYPE,
                        ColumnTypeConverter.fromIcebergType(Types.ListType.ofOptional(1, map)));
            }
        }
    }

    @Test
    public void nestedGeometryTypesAndScanMetadataPreserveLeafCrs() {
        var crs84 = Types.GeometryType.crs84();
        var epsg3857 = Types.GeometryType.of("EPSG:3857");
        var list = Types.ListType.ofOptional(2, crs84);
        var map = Types.MapType.ofOptional(4, 5, Types.IntegerType.get(), epsg3857);
        var struct = Types.StructType.of(Types.NestedField.optional(7, "shape", epsg3857),
                Types.NestedField.optional(8, "id", Types.IntegerType.get()));
        var deep = Types.ListType.ofOptional(10, Types.StructType.of(
                Types.NestedField.optional(11, "shape", crs84),
                Types.NestedField.optional(12, "geog", Types.GeographyType.crs84())));
        var schema = new Schema(Types.NestedField.optional(1, "a", list),
                Types.NestedField.optional(3, "m", map), Types.NestedField.optional(6, "s", struct),
                Types.NestedField.optional(9, "d", deep));

        var array = Assertions.assertInstanceOf(ArrayType.class, ColumnTypeConverter.fromIcebergType(list));
        var mappedMap = Assertions.assertInstanceOf(MapType.class, ColumnTypeConverter.fromIcebergType(map));
        var mappedStruct = Assertions.assertInstanceOf(StructType.class, ColumnTypeConverter.fromIcebergType(struct));
        var mappedDeep = Assertions.assertInstanceOf(ArrayType.class, ColumnTypeConverter.fromIcebergType(deep));
        var deepStruct = Assertions.assertInstanceOf(StructType.class, mappedDeep.getItemType());
        Assertions.assertEquals(IntegerType.INT, mappedStruct.getField("id").getType());
        Assertions.assertEquals(IntegerType.INT, mappedMap.getKeyType());
        Assertions.assertEquals(PrimitiveType.GEOGRAPHY, deepStruct.getField("geog").getType().getPrimitiveType());

        var fields = IcebergApiConverter.getTIcebergSchema(schema).getFields();
        Assertions.assertEquals(List.of(1, 3, 6, 9), fields.stream().map(TIcebergSchemaField::getField_id).toList());
        for (var field : fields) {
            Assertions.assertFalse(field.isSetGeo_metadata());
        }
        assertGeometryLeaf(fields.get(0).getChildren().get(0), 2, array.getItemType(), "OGC:CRS84", 4326);
        var key = fields.get(1).getChildren().get(0);
        Assertions.assertEquals(4, key.getField_id());
        Assertions.assertFalse(key.isSetGeo_metadata());
        assertGeometryLeaf(fields.get(1).getChildren().get(1), 5, mappedMap.getValueType(), "EPSG:3857", 3857);
        assertGeometryLeaf(fields.get(2).getChildren().get(0), 7,
                mappedStruct.getField("shape").getType(), "EPSG:3857", 3857);
        var element = fields.get(3).getChildren().get(0);
        Assertions.assertEquals(10, element.getField_id());
        assertGeometryLeaf(element.getChildren().get(0), 11,
                deepStruct.getField("shape").getType(), "OGC:CRS84", 4326);
        Assertions.assertEquals(12, element.getChildren().get(1).getField_id());
        Assertions.assertEquals(TIcebergGeoKind.GEOGRAPHY, element.getChildren().get(1).getGeo_metadata().getKind());
    }

    private static void assertGeometryLeaf(TIcebergSchemaField field, int id, Type type, String crs, int srid) {
        var scalar = Assertions.assertInstanceOf(ScalarType.class, type);
        Assertions.assertEquals(PrimitiveType.GEOMETRY, scalar.getPrimitiveType());
        Assertions.assertEquals(new GeoTypeDescriptor(GeoTypeDescriptor.LogicalType.GEOMETRY,
                GeoTypeDescriptor.EdgeAlgorithm.PLANAR,
                crs, srid), scalar.getGeoDescriptor());
        Assertions.assertEquals(id, field.getField_id());
        Assertions.assertEquals(new TIcebergGeoMetadata().setKind(TIcebergGeoKind.GEOMETRY).setCrs(crs)
                .setEdge_algorithm("PLANAR"), field.getGeo_metadata());
    }

    @Test
    public void geoKindUsesStableEnumWireValues() throws Exception {
        Assertions.assertEquals(0, TIcebergGeoKind.UNKNOWN.getValue());
        Assertions.assertEquals(1, TIcebergGeoKind.GEOGRAPHY.getValue());
        Assertions.assertEquals(2, TIcebergGeoKind.GEOMETRY.getValue());
        for (TIcebergGeoKind kind : TIcebergGeoKind.values()) {
            TIcebergGeoMetadata metadata = new TIcebergGeoMetadata().setKind(kind);
            byte[] bytes = new TSerializer(new TCompactProtocol.Factory()).serialize(metadata);
            TCompactProtocol protocol = new TCompactProtocol(new TMemoryInputTransport(bytes));
            protocol.readStructBegin();
            TField field = protocol.readFieldBegin();
            Assertions.assertEquals(1, field.id);
            Assertions.assertEquals(TType.I32, field.type);
            Assertions.assertEquals(kind.getValue(), protocol.readI32());
            TIcebergGeoMetadata decoded = new TIcebergGeoMetadata();
            new TDeserializer(new TCompactProtocol.Factory()).deserialize(decoded, bytes);
            Assertions.assertEquals(kind, decoded.getKind());
        }
        TIcebergGeoMetadata absent = new TIcebergGeoMetadata();
        byte[] bytes = new TSerializer(new TCompactProtocol.Factory()).serialize(absent);
        TIcebergGeoMetadata decoded = new TIcebergGeoMetadata();
        new TDeserializer(new TCompactProtocol.Factory()).deserialize(decoded, bytes);
        Assertions.assertFalse(decoded.isSetKind());
    }
}
