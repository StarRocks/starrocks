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
 * Immutable semantic type metadata; column representation belongs to the column/transport layer.
 * <p>
 * A plain class rather than a record: FE's GSON version cannot read a record back, which persisting this descriptor
 * in FE metadata will need.
 */
public final class GeoTypeDescriptor {
    private final LogicalType logicalType;
    private final EdgeAlgorithm edgeAlgorithm;
    private final String crs;
    private final Integer srid;

    public enum LogicalType {
        UNKNOWN, GEOGRAPHY, GEOMETRY
    }

    public enum EdgeAlgorithm {
        UNKNOWN, SPHERICAL, VINCENTY, THOMAS, ANDOYER, KARNEY, PLANAR
    }

    public GeoTypeDescriptor(LogicalType logicalType, EdgeAlgorithm edgeAlgorithm, String crs, Integer srid) {
        this.logicalType = Objects.requireNonNull(logicalType);
        this.edgeAlgorithm = Objects.requireNonNull(edgeAlgorithm);
        this.crs = Objects.requireNonNull(crs);
        this.srid = srid;
    }

    public LogicalType logicalType() {
        return logicalType;
    }

    public EdgeAlgorithm edgeAlgorithm() {
        return edgeAlgorithm;
    }

    public String crs() {
        return crs;
    }

    public Integer srid() {
        return srid;
    }

    public static GeoTypeDescriptor geography(String crs) {
        return new GeoTypeDescriptor(LogicalType.GEOGRAPHY, EdgeAlgorithm.SPHERICAL, crs, deriveSrid(crs));
    }

    public static GeoTypeDescriptor geometry(String crs) {
        return new GeoTypeDescriptor(LogicalType.GEOMETRY, EdgeAlgorithm.PLANAR, crs, deriveSrid(crs));
    }

    public void validate(PrimitiveType primitive) {
        if (crs.isEmpty()) {
            throw new IllegalArgumentException("Native geo types require a non-empty CRS");
        }
        Integer derivedSrid = deriveSrid(crs);
        if (srid != null && derivedSrid != null && !srid.equals(derivedSrid)) {
            throw new IllegalArgumentException("Native geo SRID conflicts with its CRS");
        }
        boolean geography = primitive == PrimitiveType.GEOGRAPHY;
        boolean sphericalEdge = edgeAlgorithm != EdgeAlgorithm.UNKNOWN && edgeAlgorithm != EdgeAlgorithm.PLANAR;
        if ((primitive != PrimitiveType.GEOGRAPHY && primitive != PrimitiveType.GEOMETRY)
                || logicalType != (geography ? LogicalType.GEOGRAPHY : LogicalType.GEOMETRY)
                || (geography ? !sphericalEdge : edgeAlgorithm != EdgeAlgorithm.PLANAR)) {
            throw new IllegalArgumentException("Native geo primitive conflicts with its semantic descriptor");
        }
    }

    public boolean isSemanticallyCompatible(GeoTypeDescriptor other) {
        return other != null && !crs.isEmpty() && logicalType == other.logicalType
                && edgeAlgorithm == other.edgeAlgorithm && crs.equals(other.crs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GeoTypeDescriptor)) {
            return false;
        }
        GeoTypeDescriptor other = (GeoTypeDescriptor) o;
        return logicalType == other.logicalType && edgeAlgorithm == other.edgeAlgorithm
                && Objects.equals(crs, other.crs) && Objects.equals(srid, other.srid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logicalType, edgeAlgorithm, crs, srid);
    }

    @Override
    public String toString() {
        return "GeoTypeDescriptor[logicalType=" + logicalType + ", edgeAlgorithm=" + edgeAlgorithm + ", crs=" + crs
                + ", srid=" + srid + "]";
    }

    private static Integer deriveSrid(String crs) {
        if (crs.equals("OGC:CRS84")) {
            return 4326;
        }
        if (!crs.startsWith("EPSG:")) {
            return null;
        }
        // Match BE std::from_chars: an optional minus followed by ASCII digits.
        // Other non-empty CRS identifiers remain opaque, with no derived numeric SRID.
        String suffix = crs.substring("EPSG:".length());
        int start = suffix.startsWith("-") ? 1 : 0;
        if (start == suffix.length()) {
            return null;
        }
        for (int i = start; i < suffix.length(); ++i) {
            if (suffix.charAt(i) < '0' || suffix.charAt(i) > '9') {
                return null;
            }
        }
        try {
            return Integer.valueOf(suffix);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
