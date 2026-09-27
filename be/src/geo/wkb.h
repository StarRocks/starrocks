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

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "base/string/slice.h"
#include "common/status.h"

namespace starrocks {

// Values match the OGC Simple Features WKB geometry type numbers.
enum class WkbGeometryType : uint32_t {
    POINT = 1,
    LINESTRING = 2,
    POLYGON = 3,
    MULTIPOINT = 4,
    MULTILINESTRING = 5,
    MULTIPOLYGON = 6,
    GEOMETRYCOLLECTION = 7,
};

struct WkbCoordinate {
    double x = 0;
    double y = 0;

    bool operator==(const WkbCoordinate& rhs) const { return x == rhs.x && y == rhs.y; }
};

enum class WkbCoordinateSemantics {
    GEOGRAPHY_CRS84,
    GEOMETRY_CARTESIAN,
};

// A bounded geometry tree used at native GEO SQL boundaries.
// S2 and legacy GeoShape bytes never enter the native WKB representation.
struct WkbGeometry {
    WkbGeometryType type = WkbGeometryType::POINT;
    bool empty = false;

    // POINT contains zero or one coordinate; LINESTRING contains its vertices.
    std::vector<WkbCoordinate> coordinates;
    // POLYGON contains one exterior ring followed by zero or more interior rings.
    std::vector<std::vector<WkbCoordinate>> rings;
    // Multi-geometries and GEOMETRYCOLLECTION contain child geometries.
    std::vector<WkbGeometry> children;
};

class WkbCodec {
public:
    // Parse two-dimensional OGC WKT. GEOGRAPHY additionally validates CRS84
    // longitude/latitude bounds; GEOMETRY accepts finite Cartesian coordinates.
    static Status parse_wkt(std::string_view input, WkbGeometry* output,
                            WkbCoordinateSemantics semantics = WkbCoordinateSemantics::GEOGRAPHY_CRS84);

    // Parse two-dimensional OGC WKB in either byte order. EWKB flags and ISO
    // SQL/MM dimensional type offsets are rejected; CRS is an explicit
    // constructor argument at the SQL boundary.
    static Status parse_wkb(const Slice& input, WkbGeometry* output,
                            WkbCoordinateSemantics semantics = WkbCoordinateSemantics::GEOGRAPHY_CRS84);

    // Emit canonical little-endian OGC WKB.
    static Status to_wkb(const WkbGeometry& geometry, std::string* output,
                         WkbCoordinateSemantics semantics = WkbCoordinateSemantics::GEOGRAPHY_CRS84);

    // Emit normalized OGC WKT.
    static Status to_wkt(const WkbGeometry& geometry, std::string* output,
                         WkbCoordinateSemantics semantics = WkbCoordinateSemantics::GEOGRAPHY_CRS84);

    static const char* type_name(WkbGeometryType type);
};

} // namespace starrocks
