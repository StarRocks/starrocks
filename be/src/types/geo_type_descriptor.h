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

#include <optional>
#include <string>

#include "gen_cpp/Types_types.h"
#include "gen_cpp/types.pb.h"

namespace starrocks {

// Normalized internal metadata, not a native SQL type. Missing wire fields become
// UNKNOWN / empty CRS / default subdescriptors; only parsed SRID retains presence.
// Fixed-underlying-type enums preserve unrecognized int32 values. Conversion is
// policy-free: callers decide which kinds, algorithms, CRS and dimensions they support.
struct GeoTypeDescriptor {
    PGeoLogicalType logical_type = GEO_LOGICAL_TYPE_UNKNOWN;
    PGeoCoordinateSystem coordinate_system = GEO_COORDINATE_SYSTEM_UNKNOWN;
    PGeoEdgeAlgorithm edge_algorithm = GEO_EDGE_ALGORITHM_UNKNOWN;
    std::string crs;
    std::optional<int32_t> srid;

    static GeoTypeDescriptor from_thrift(const TGeoTypeDesc& thrift);
    static GeoTypeDescriptor from_protobuf(const PGeoTypeDesc& protobuf);
    TGeoTypeDesc to_thrift() const;
    PGeoTypeDesc to_protobuf() const;

    // Exact normalized metadata equality, not SQL type compatibility.
    bool operator==(const GeoTypeDescriptor& rhs) const = default;
};

struct GeoStorageDescriptor {
    PGeoEncoding encoding = GEO_ENCODING_UNKNOWN;
    PGeoDimension dimension = GEO_DIMENSION_UNKNOWN;
    PGeoValidationState validation_state = GEO_VALIDATION_STATE_UNKNOWN;

    static GeoStorageDescriptor from_thrift(const TGeoStorageDesc& thrift);
    static GeoStorageDescriptor from_protobuf(const PGeoStorageDesc& protobuf);
    TGeoStorageDesc to_thrift() const;
    PGeoStorageDesc to_protobuf() const;

    // Exact normalized metadata equality, not SQL type compatibility.
    bool operator==(const GeoStorageDescriptor& rhs) const = default;
};

struct GeoColumnDescriptor {
    GeoTypeDescriptor type;
    GeoStorageDescriptor storage;

    static GeoColumnDescriptor from_thrift(const TGeoColumnDesc& thrift);
    static GeoColumnDescriptor from_protobuf(const PGeoColumnDesc& protobuf);
    TGeoColumnDesc to_thrift() const;
    PGeoColumnDesc to_protobuf() const;

    bool operator==(const GeoColumnDescriptor& rhs) const = default;
};

// Callers establish supported semantics before using these compatibility predicates.
// CRS identifiers must be nonempty and equal: no alias normalization, reprojection
// or inference from the optional parsed SRID.
bool is_geo_semantically_compatible(const GeoTypeDescriptor& lhs, const GeoTypeDescriptor& rhs);

// Semantic compatibility is not capability validation or a row-dimension guarantee.
// Encoding and producer-validation state do not determine overload identity.
bool is_geo_compute_compatible(const GeoColumnDescriptor& lhs, const GeoColumnDescriptor& rhs);

// No payload conversion: both sides must declare WKB. UNKNOWN/MIXED target dimension
// is unconstrained; otherwise the source must guarantee the same dimension.
bool is_geo_assignment_compatible(const GeoColumnDescriptor& source, const GeoColumnDescriptor& target);

// UNION ALL may combine dimensions; derive the result dimension separately.
bool is_geo_union_all_compatible(const GeoColumnDescriptor& lhs, const GeoColumnDescriptor& rhs);

} // namespace starrocks
