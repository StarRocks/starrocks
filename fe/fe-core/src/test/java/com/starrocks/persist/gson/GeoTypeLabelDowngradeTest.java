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

package com.starrocks.persist.gson;

import com.starrocks.catalog.Column;
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Newer versions persist native GEO types with the "GeoType" label. This version must still load such metadata
 * after a downgrade.
 */
public class GeoTypeLabelDowngradeTest {
    // Exactly what a newer version writes for GEOMETRY and GEOGRAPHY, with and without an SRID.
    private static final List<String> NEWER_GEO_TYPES = List.of(
            "{\"clazz\":\"GeoType\",\"crs\":\"EPSG:3857\",\"edgeAlgorithm\":\"PLANAR\",\"srid\":3857,"
                    + "\"type\":\"GEOMETRY\",\"len\":-1,\"precision\":0,\"scale\":0}",
            "{\"clazz\":\"GeoType\",\"crs\":\"OGC:CRS84\",\"edgeAlgorithm\":\"SPHERICAL\",\"srid\":4326,"
                    + "\"type\":\"GEOGRAPHY\",\"len\":-1,\"precision\":0,\"scale\":0}",
            "{\"clazz\":\"GeoType\",\"crs\":\"custom:local\",\"edgeAlgorithm\":\"PLANAR\","
                    + "\"type\":\"GEOMETRY\",\"len\":-1,\"precision\":0,\"scale\":0}");

    private static final String INVALID_SCALAR =
            "{\"clazz\":\"ScalarType\",\"type\":\"INVALID_TYPE\",\"len\":-1,\"precision\":0,\"scale\":0}";

    @Test
    public void testNewerGeoTypeLoadsAsInvalidScalarType() {
        for (String newer : NEWER_GEO_TYPES) {
            for (Class<? extends Type> declared : List.of(Type.class, ScalarType.class)) {
                Type loaded = GsonUtils.GSON.fromJson(newer, declared);
                assertSame(ScalarType.class, loaded.getClass());
                assertEquals(PrimitiveType.INVALID_TYPE, loaded.getPrimitiveType());
                // Another checkpoint in this version no longer carries the newer label.
                assertEquals(INVALID_SCALAR, GsonUtils.GSON.toJson(loaded, declared));
            }
        }
    }

    @Test
    public void testNewerGeoTypeInsideColumnAndNestedType() {
        for (String newer : NEWER_GEO_TYPES) {
            Column template = new Column("g", IntegerType.INT);
            String columnJson = GsonUtils.GSON.toJson(template)
                    .replace(GsonUtils.GSON.toJson(template.getType()), newer);
            Column column = GsonUtils.GSON.fromJson(columnJson, Column.class);
            assertEquals("g", column.getName());
            assertEquals(PrimitiveType.INVALID_TYPE, column.getType().getPrimitiveType());

            Type nested = GsonUtils.GSON.fromJson("{\"clazz\":\"ArrayType\",\"itemType\":" + newer + "}", Type.class);
            assertEquals(PrimitiveType.INVALID_TYPE, ((ArrayType) nested).getItemType().getPrimitiveType());
        }
    }

    @Test
    public void testOtherLabelsAreUnchanged() {
        assertEquals("{\"clazz\":\"ScalarType\",\"type\":\"INT\",\"len\":-1,\"precision\":0,\"scale\":0}",
                GsonUtils.GSON.toJson(new ScalarType(PrimitiveType.INT), Type.class));
        assertThrows(SubtypeNotFoundException.class,
                () -> GsonUtils.GSON.fromJson("{\"clazz\":\"NoSuchType\",\"type\":\"INT\"}", Type.class));
    }

    @Test
    public void testForwardCompatibleLabelRegistration() {
        RuntimeTypeAdapterFactory<Type> factory = RuntimeTypeAdapterFactory.of(Type.class, "clazz")
                .registerSubtype(ScalarType.class, "ScalarType");
        assertThrows(IllegalArgumentException.class,
                () -> factory.registerForwardCompatibleLabel(ArrayType.class, "Other"));
        assertThrows(IllegalArgumentException.class,
                () -> factory.registerForwardCompatibleLabel(ScalarType.class, "ScalarType"));
        factory.registerForwardCompatibleLabel(ScalarType.class, "GeoType");
        assertThrows(IllegalArgumentException.class,
                () -> factory.registerForwardCompatibleLabel(ScalarType.class, "GeoType"));
        assertThrows(IllegalArgumentException.class, () -> factory.registerSubtype(ArrayType.class, "GeoType"));
    }
}
