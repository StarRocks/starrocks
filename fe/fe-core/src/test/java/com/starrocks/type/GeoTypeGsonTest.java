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

import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.starrocks.catalog.Column;
import com.starrocks.persist.gson.GsonUtils;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GeoTypeGsonTest {
    @Test
    public void testConcreteRoundTrip() {
        for (PrimitiveType primitive : List.of(PrimitiveType.GEOGRAPHY, PrimitiveType.GEOMETRY)) {
            for (String crs : List.of("OGC:CRS84", "EPSG:4326", "EPSG:3857", "EPSG:004326", "custom:local", " x ")) {
                for (GeoTypeDescriptor.EdgeAlgorithm edge : GeoTypeDescriptor.EdgeAlgorithm.values()) {
                    if (edge == GeoTypeDescriptor.EdgeAlgorithm.UNKNOWN
                            || (primitive == PrimitiveType.GEOMETRY) != (edge == GeoTypeDescriptor.EdgeAlgorithm.PLANAR)) {
                        continue;
                    }
                    Integer derived = GeoTypeDescriptor.geometry(crs).srid();
                    assertScalarRoundTrip(geo(primitive, crs, edge, derived));
                    assertScalarRoundTrip(geo(primitive, crs, edge, null));
                }
            }
        }
        ScalarType present = geo(PrimitiveType.GEOMETRY, "EPSG:3857", GeoTypeDescriptor.EdgeAlgorithm.PLANAR, 3857);
        ScalarType absent = geo(PrimitiveType.GEOMETRY, "EPSG:3857", GeoTypeDescriptor.EdgeAlgorithm.PLANAR, null);
        assertNotEquals(roundTrip(present), roundTrip(absent));
        for (int srid : new int[] {Integer.MIN_VALUE, -1, 0, Integer.MAX_VALUE}) {
            assertScalarRoundTrip(geo(PrimitiveType.GEOMETRY, "custom:local", GeoTypeDescriptor.EdgeAlgorithm.PLANAR, srid));
        }
        ScalarType explicitNull = GsonUtils.GSON.fromJson(json("GEOMETRY",
                "{\"crs\":\"EPSG:3857\",\"edgeAlgorithm\":\"PLANAR\",\"srid\":null}"), ScalarType.class);
        assertEquals(absent, explicitNull);
        assertFalse(GsonUtils.GSON.toJsonTree(explicitNull).getAsJsonObject().getAsJsonObject("geo").has("srid"));
    }

    @Test
    public void testNestedMetadata() {
        for (PrimitiveType primitive : List.of(PrimitiveType.GEOGRAPHY, PrimitiveType.GEOMETRY)) {
            ScalarType leaf = geo(primitive, "OGC:CRS84", primitive == PrimitiveType.GEOGRAPHY
                    ? GeoTypeDescriptor.EdgeAlgorithm.SPHERICAL : GeoTypeDescriptor.EdgeAlgorithm.PLANAR, 4326);
            StructType struct = new StructType(List.of(new StructField("shape", 17, "physical_shape", leaf, "geo"),
                    new StructField("id", IntegerType.INT)), true);
            for (Type type : List.of(new ArrayType(leaf), new MapType(IntegerType.INT, leaf), struct,
                    new ArrayType(new MapType(IntegerType.INT, new StructType(List.of(new ArrayType(leaf))))))) {
                assertEquals(type, roundTrip(type));
                Column column = new Column("shapes", type);
                assertEquals(column, GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(column), Column.class));
            }
            StructType restored = (StructType) roundTrip(struct);
            assertEquals(0, restored.getFieldPos("shape"));
            assertEquals(17, restored.getFields().get(0).getFieldId());
            assertEquals("physical_shape", restored.getFields().get(0).getFieldPhysicalName());
            assertEquals("geo", restored.getFields().get(0).getComment());
            MapType restoredMap = (MapType) roundTrip(new MapType(IntegerType.INT, leaf));
            assertFalse(restoredMap.getSelectedFields()[0]);
            assertFalse(restoredMap.getSelectedFields()[1]);
        }
    }

    @Test
    public void testLegacyMissingDescriptorRemainsMissing() {
        // The former writer persisted only these annotated ScalarType fields, including for VIEW columns.
        for (PrimitiveType primitive : List.of(PrimitiveType.GEOGRAPHY, PrimitiveType.GEOMETRY)) {
            String legacy = "{\"clazz\":\"ScalarType\",\"type\":\"" + primitive
                    + "\",\"len\":-1,\"precision\":0,\"scale\":0}";
            for (Class<? extends Type> declared : List.of(Type.class, ScalarType.class)) {
                Type restored = GsonUtils.GSON.fromJson(legacy, declared);
                assertEquals(primitive, restored.getPrimitiveType());
                assertNull(((ScalarType) restored).getGeoDescriptor());
                String rewritten = GsonUtils.GSON.toJson(restored, declared);
                assertFalse(GsonUtils.GSON.fromJson(rewritten, JsonObject.class).has("geo"));
                assertEquals(restored, GsonUtils.GSON.fromJson(rewritten, declared));
            }
            Column column = new Column("legacy", GsonUtils.GSON.fromJson(legacy, Type.class));
            Column restoredColumn = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(column), Column.class);
            assertNull(((ScalarType) restoredColumn.getType()).getGeoDescriptor());
            ArrayType array = new ArrayType(restoredColumn.getType());
            assertEquals(array, roundTrip(array));
        }
    }

    @Test
    public void testRejectPresentInvalidMetadata() {
        for (String invalid : List.of("null", "[]", "1", "\"geo\"", "true", "{}",
                "{\"crs\":\"EPSG:3857\"}", "{\"edgeAlgorithm\":\"PLANAR\"}",
                "{\"crs\":null,\"edgeAlgorithm\":\"PLANAR\"}",
                "{\"crs\":4326,\"edgeAlgorithm\":\"PLANAR\"}",
                "{\"crs\":{},\"edgeAlgorithm\":\"PLANAR\"}",
                "{\"crs\":\"\",\"edgeAlgorithm\":\"PLANAR\"}")) {
            assertInvalid("GEOMETRY", invalid);
        }
        for (String edge : List.of("null", "1", "true", "{}", "\"UNKNOWN\"", "\"FUTURE\"", "\"planar\"")) {
            assertInvalid("GEOMETRY", "{\"crs\":\"EPSG:3857\",\"edgeAlgorithm\":" + edge + "}");
        }
        for (String srid : List.of("\"3857\"", "true", "[]", "{}", "3857.1", "2147483648", "-2147483649", "4326")) {
            assertInvalid("GEOMETRY", "{\"crs\":\"EPSG:3857\",\"edgeAlgorithm\":\"PLANAR\",\"srid\":" + srid + "}");
        }
        assertInvalid("GEOMETRY", "{\"crs\":\"OGC:CRS84\",\"edgeAlgorithm\":\"SPHERICAL\"}");
        assertInvalid("GEOGRAPHY", "{\"crs\":\"OGC:CRS84\",\"edgeAlgorithm\":\"PLANAR\"}");
        assertInvalid("INT", "{\"crs\":\"EPSG:3857\",\"edgeAlgorithm\":\"PLANAR\"}");
        assertInvalid("NOT_EXIST", "{\"crs\":\"EPSG:3857\",\"edgeAlgorithm\":\"PLANAR\"}");
        assertThrows(JsonParseException.class, () -> GsonUtils.GSON.fromJson(
                "{\"clazz\":\"ScalarType\",\"type\":\"GEOMETRY\",\"geo\":", Type.class));
    }

    @Test
    public void testOrdinaryTypesUnchanged() {
        for (ScalarType type : List.of(IntegerType.INT, new ScalarType(PrimitiveType.INT), TypeFactory.createVarcharType(17),
                TypeFactory.createCharType(5), TypeFactory.createDecimalV2Type(10, 2),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 30, 7))) {
            assertEquals(type, GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(type), Type.class));
            assertFalse(GsonUtils.GSON.toJsonTree(type).getAsJsonObject().has("geo"));
            assertEquals(new ArrayType(type), roundTrip(new ArrayType(type)));
            Column column = new Column("control", type);
            assertEquals(column, GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(column), Column.class));
        }
        // Scalar subclasses use their runtime adapter; explicitly declaring Type has never registered them.
        assertEquals(IntegerType.INT, GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(IntegerType.INT), Type.class));
        for (Class<? extends Type> declared : List.of(Type.class, ScalarType.class)) {
            assertThrows(JsonParseException.class, () -> GsonUtils.GSON.toJson(IntegerType.INT, declared));
        }
        ScalarType invalid = GsonUtils.GSON.fromJson("{\"clazz\":\"ScalarType\",\"type\":\"NOT_EXIST\"}", ScalarType.class);
        assertEquals(PrimitiveType.INVALID_TYPE, invalid.getPrimitiveType());
        assertNull(GsonUtils.GSON.fromJson("null", Type.class));
        assertNull(GsonUtils.GSON.fromJson("null", ScalarType.class));
        // Builtin pseudo-types remain outside the persisted concrete Type subtypes.
        assertThrows(JsonParseException.class, () -> GsonUtils.GSON.toJson(AnyGeographyType.GEOGRAPHY, Type.class));
        assertThrows(JsonParseException.class, () -> GsonUtils.GSON.toJson(AnyGeometryType.GEOMETRY, Type.class));
    }

    private static ScalarType geo(PrimitiveType primitive, String crs, GeoTypeDescriptor.EdgeAlgorithm edge, Integer srid) {
        boolean geography = primitive == PrimitiveType.GEOGRAPHY;
        return ScalarType.createGeoType(primitive, new GeoTypeDescriptor(
                geography ? GeoTypeDescriptor.LogicalType.GEOGRAPHY : GeoTypeDescriptor.LogicalType.GEOMETRY,
                geography ? GeoTypeDescriptor.CoordinateSystem.SPHERICAL : GeoTypeDescriptor.CoordinateSystem.CARTESIAN,
                edge, crs, srid));
    }

    private static void assertScalarRoundTrip(ScalarType type) {
        for (Class<? extends Type> declared : List.of(Type.class, ScalarType.class)) {
            String json = GsonUtils.GSON.toJson(type, declared);
            ScalarType restored = (ScalarType) GsonUtils.GSON.fromJson(json, declared);
            assertEquals(type, restored);
            assertEquals(type.hashCode(), restored.hashCode());
            assertEquals(type.getGeoDescriptor(), restored.getGeoDescriptor());
            JsonObject descriptor = GsonUtils.GSON.fromJson(json, JsonObject.class).getAsJsonObject("geo");
            assertEquals(type.getGeoDescriptor().crs(), descriptor.get("crs").getAsString());
            assertEquals(type.getGeoDescriptor().edgeAlgorithm().name(), descriptor.get("edgeAlgorithm").getAsString());
            assertEquals(type.getGeoDescriptor().srid() != null, descriptor.has("srid"));
        }
        Column column = new Column("shape", type);
        assertEquals(column, GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(column), Column.class));
    }

    private static Type roundTrip(Type type) {
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(type, Type.class), Type.class);
    }

    private static String json(String primitive, String geo) {
        return "{\"clazz\":\"ScalarType\",\"type\":\"" + primitive + "\",\"geo\":" + geo + "}";
    }

    private static void assertInvalid(String primitive, String geo) {
        for (Class<? extends Type> declared : List.of(Type.class, ScalarType.class)) {
            JsonParseException error = assertThrows(JsonParseException.class,
                    () -> GsonUtils.GSON.fromJson(json(primitive, geo), declared));
            assertTrue(error.getMessage().contains("geo"), error.getMessage());
        }
    }
}
