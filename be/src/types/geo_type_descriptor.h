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

#include "common/statusor.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/types.pb.h"

namespace starrocks {

// Standalone metadata, not a native SQL type. Preserve optional-field presence,
// including explicit UNKNOWN. Reject unrecognized enum numbers at conversion
// boundaries; never substitute a supported default. No payload inspection here.
struct GeoTypeDescriptor {
    std::optional<TGeoLogicalType::type> logical_type;
    std::optional<TGeoCoordinateSystem::type> coordinate_system;
    std::optional<TGeoEdgeAlgorithm::type> edge_algorithm;
    std::optional<std::string> crs;
    std::optional<int32_t> srid;

    static StatusOr<GeoTypeDescriptor> from_thrift(const TGeoTypeDesc& thrift);
    static StatusOr<GeoTypeDescriptor> from_protobuf(const PGeoTypeDesc& protobuf);
    StatusOr<TGeoTypeDesc> to_thrift() const;
    StatusOr<PGeoTypeDesc> to_protobuf() const;

    // Exact metadata equality, NOT SQL type compatibility.
    bool operator==(const GeoTypeDescriptor& rhs) const;
    bool operator!=(const GeoTypeDescriptor& rhs) const { return !(*this == rhs); }
};

struct GeoStorageDescriptor {
    std::optional<TGeoEncoding::type> encoding;
    std::optional<TGeoDimension::type> dimension;
    std::optional<TGeoValidationState::type> validation_state;

    static StatusOr<GeoStorageDescriptor> from_thrift(const TGeoStorageDesc& thrift);
    static StatusOr<GeoStorageDescriptor> from_protobuf(const PGeoStorageDesc& protobuf);
    StatusOr<TGeoStorageDesc> to_thrift() const;
    StatusOr<PGeoStorageDesc> to_protobuf() const;

    // Exact metadata equality, NOT SQL type compatibility.
    bool operator==(const GeoStorageDescriptor& rhs) const;
    bool operator!=(const GeoStorageDescriptor& rhs) const { return !(*this == rhs); }
};

struct GeoColumnDescriptor {
    std::optional<GeoTypeDescriptor> type;
    std::optional<GeoStorageDescriptor> storage;

    static StatusOr<GeoColumnDescriptor> from_thrift(const TGeoColumnDesc& thrift);
    static StatusOr<GeoColumnDescriptor> from_protobuf(const PGeoColumnDesc& protobuf);
    StatusOr<TGeoColumnDesc> to_thrift() const;
    StatusOr<PGeoColumnDesc> to_protobuf() const;

    // Exact metadata equality, NOT SQL type compatibility.
    bool operator==(const GeoColumnDescriptor& rhs) const;
    bool operator!=(const GeoColumnDescriptor& rhs) const { return !(*this == rhs); }
};

// Compatibility operates on recognized descriptor enum values (checked at
// conversion boundaries), without repeating wire validation at each call site.
// Conservative CRS comparison: identifiers must be present, nonempty and equal.
// No alias normalization, reprojection or inference from the optional parsed SRID.
bool is_geo_semantically_compatible(const GeoTypeDescriptor& lhs, const GeoTypeDescriptor& rhs);

// Semantic compatibility only: does not enable an algorithm/function or prove
// row-level dimensions. Encoding and producer-validation state are not overload identity.
bool is_geo_compute_compatible(const GeoColumnDescriptor& lhs, const GeoColumnDescriptor& rhs);

// No payload conversion. Both sides must declare WKB; validation state is irrelevant.
// UNKNOWN/MIXED (or absent) target dimension is unconstrained. A constrained target
// requires the same guaranteed source dimension, without dropping coordinates.
bool is_geo_assignment_compatible(const GeoColumnDescriptor& source, const GeoColumnDescriptor& target);

// Unlike assignment, UNION ALL may combine dimensions without relabeling rows.
// Result dimension must be derived separately; compatibility is not an XY guarantee.
bool is_geo_union_all_compatible(const GeoColumnDescriptor& lhs, const GeoColumnDescriptor& rhs);

} // namespace starrocks
