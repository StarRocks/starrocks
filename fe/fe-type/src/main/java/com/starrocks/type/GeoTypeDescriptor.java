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

/** Immutable semantic type metadata; column representation belongs to the column/transport layer. */
public record GeoTypeDescriptor(LogicalType logicalType, CoordinateSystem coordinateSystem,
                                EdgeAlgorithm edgeAlgorithm, String crs, Integer srid) {
    public enum LogicalType {
        UNKNOWN, GEOGRAPHY, GEOMETRY
    }

    public enum CoordinateSystem {
        UNKNOWN, SPHERICAL, CARTESIAN
    }

    public enum EdgeAlgorithm {
        UNKNOWN, SPHERICAL, VINCENTY, THOMAS, ANDOYER, KARNEY, PLANAR
    }

    public GeoTypeDescriptor {
        Objects.requireNonNull(logicalType);
        Objects.requireNonNull(coordinateSystem);
        Objects.requireNonNull(edgeAlgorithm);
        Objects.requireNonNull(crs);
    }

    public void validate(PrimitiveType primitive) {
        boolean geography = primitive == PrimitiveType.GEOGRAPHY;
        boolean sphericalEdge = edgeAlgorithm != EdgeAlgorithm.UNKNOWN && edgeAlgorithm != EdgeAlgorithm.PLANAR;
        if ((primitive != PrimitiveType.GEOGRAPHY && primitive != PrimitiveType.GEOMETRY)
                || logicalType != (geography ? LogicalType.GEOGRAPHY : LogicalType.GEOMETRY)
                || coordinateSystem != (geography ? CoordinateSystem.SPHERICAL : CoordinateSystem.CARTESIAN)
                || (geography ? !sphericalEdge : edgeAlgorithm != EdgeAlgorithm.PLANAR)) {
            throw new IllegalArgumentException("Native geo primitive conflicts with its semantic descriptor");
        }
    }
}
