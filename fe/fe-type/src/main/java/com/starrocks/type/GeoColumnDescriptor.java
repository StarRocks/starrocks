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

/** Immutable metadata, independent of generated transport classes. */
public record GeoColumnDescriptor(LogicalType logicalType, CoordinateSystem coordinateSystem,
                                  EdgeAlgorithm edgeAlgorithm, String crs, Integer srid,
                                  Encoding encoding, Dimension dimension, ValidationState validationState) {
    public enum LogicalType {
        UNKNOWN, GEOGRAPHY, GEOMETRY
    }

    public enum CoordinateSystem {
        UNKNOWN, SPHERICAL, CARTESIAN
    }

    public enum EdgeAlgorithm {
        UNKNOWN, SPHERICAL, VINCENTY, THOMAS, ANDOYER, KARNEY, PLANAR
    }

    public enum Encoding {
        UNKNOWN, WKB
    }

    public enum Dimension {
        UNKNOWN, XY, XYZ, XYM, XYZM, MIXED
    }

    public enum ValidationState {
        UNKNOWN, UNVALIDATED, STRUCTURALLY_VALIDATED, SEMANTICALLY_VALIDATED
    }

    public GeoColumnDescriptor {
        Objects.requireNonNull(logicalType);
        Objects.requireNonNull(coordinateSystem);
        Objects.requireNonNull(edgeAlgorithm);
        Objects.requireNonNull(crs);
        Objects.requireNonNull(encoding);
        Objects.requireNonNull(dimension);
        Objects.requireNonNull(validationState);
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
        if (encoding != Encoding.WKB) {
            throw new IllegalArgumentException("Native geo types require WKB encoding");
        }
    }
}
