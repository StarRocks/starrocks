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

// Semantic metadata for native geo values. This descriptor, rather than the
// WKB payload, distinguishes planar GEOMETRY from spherical GEOGRAPHY.
struct GeoTypeDescriptor {
    TGeoLogicalType::type logical_type{TGeoLogicalType::UNKNOWN};
    TGeoCoordinateSystem::type coordinate_system{TGeoCoordinateSystem::UNKNOWN};
    TGeoEdgeAlgorithm::type edge_algorithm{TGeoEdgeAlgorithm::UNKNOWN};
    std::string crs;
    std::optional<int32_t> srid;

    static GeoTypeDescriptor from_thrift(const TGeoTypeDesc& thrift);
    static GeoTypeDescriptor from_protobuf(const PGeoTypeDesc& protobuf);

    TGeoTypeDesc to_thrift() const;
    PGeoTypeDesc to_protobuf() const;

    bool operator==(const GeoTypeDescriptor& rhs) const;
    bool operator!=(const GeoTypeDescriptor& rhs) const { return !(*this == rhs); }
};

// Physical metadata for the transported or persisted native geo payload.
struct GeoStorageDescriptor {
    TGeoEncoding::type encoding{TGeoEncoding::UNKNOWN};
    TGeoDimension::type dimension{TGeoDimension::UNKNOWN};
    TGeoValidationState::type validation_state{TGeoValidationState::UNKNOWN};

    static GeoStorageDescriptor from_thrift(const TGeoStorageDesc& thrift);
    static GeoStorageDescriptor from_protobuf(const PGeoStorageDesc& protobuf);

    TGeoStorageDesc to_thrift() const;
    PGeoStorageDesc to_protobuf() const;

    bool operator==(const GeoStorageDescriptor& rhs) const;
    bool operator!=(const GeoStorageDescriptor& rhs) const { return !(*this == rhs); }
};

} // namespace starrocks
