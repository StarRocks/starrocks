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
import com.starrocks.type.IntegerType;
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
                Assertions.assertTrue(ColumnTypeConverter.fromIcebergType(type).isUnknown());
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
        Assertions.assertTrue(ColumnTypeConverter.fromIcebergType(Types.GeometryType.crs84()).isUnknown());
        Assertions.assertEquals(VarbinaryType.VARBINARY, ColumnTypeConverter.fromIcebergType(Types.BinaryType.get()));
        Assertions.assertEquals(IntegerType.INT, ColumnTypeConverter.fromIcebergType(Types.IntegerType.get()));
    }

    @Test
    public void nestedMetadataIsPreservedWithoutEnablingNestedGeo() {
        Schema schema = new Schema(Types.NestedField.optional(1, "shapes",
                Types.ListType.ofOptional(2, Types.GeometryType.crs84())));
        TIcebergSchema wire = IcebergApiConverter.getTIcebergSchema(schema);
        Assertions.assertFalse(wire.getFields().get(0).isSetGeo_metadata());
        Assertions.assertEquals(TIcebergGeoKind.GEOMETRY,
                wire.getFields().get(0).getChildren().get(0).getGeo_metadata().getKind());
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
