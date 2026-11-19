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

import java.util.Objects;

/**
 * A native GEOGRAPHY or GEOMETRY type with its semantic descriptor. Like the other scalar type classes, it only
 * constructs a {@link ScalarType}; the descriptor is still stored on {@link ScalarType}.
 */
public class GeoType extends ScalarType {
    public GeoType(PrimitiveType primitiveType, GeoTypeDescriptor descriptor) {
        super(primitiveType);
        Objects.requireNonNull(descriptor).validate(primitiveType);
        setGeoDescriptor(descriptor);
    }

    /** Creates a GEOGRAPHY type in {@code crs} with spherical edges. */
    public static GeoType createGeographyType(String crs) {
        return new GeoType(PrimitiveType.GEOGRAPHY, GeoTypeDescriptor.geography(crs));
    }

    /** Creates a planar GEOMETRY type in {@code crs}. */
    public static GeoType createGeometryType(String crs) {
        return new GeoType(PrimitiveType.GEOMETRY, GeoTypeDescriptor.geometry(crs));
    }

    /** Creates a GEOGRAPHY type with the given descriptor. */
    public static GeoType createGeographyType(GeoTypeDescriptor descriptor) {
        return new GeoType(PrimitiveType.GEOGRAPHY, descriptor);
    }

    /** Creates a GEOMETRY type with the given descriptor. */
    public static GeoType createGeometryType(GeoTypeDescriptor descriptor) {
        return new GeoType(PrimitiveType.GEOMETRY, descriptor);
    }
}
