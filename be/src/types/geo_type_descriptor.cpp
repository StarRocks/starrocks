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

#include "types/geo_type_descriptor.h"

namespace starrocks {
namespace {

static_assert(static_cast<int>(TGeoLogicalType::UNKNOWN) == GEO_LOGICAL_TYPE_UNKNOWN);
static_assert(static_cast<int>(TGeoLogicalType::GEOGRAPHY) == GEO_LOGICAL_TYPE_GEOGRAPHY);
static_assert(static_cast<int>(TGeoLogicalType::GEOMETRY) == GEO_LOGICAL_TYPE_GEOMETRY);
static_assert(static_cast<int>(TGeoCoordinateSystem::UNKNOWN) == GEO_COORDINATE_SYSTEM_UNKNOWN);
static_assert(static_cast<int>(TGeoCoordinateSystem::SPHERICAL) == GEO_COORDINATE_SYSTEM_SPHERICAL);
static_assert(static_cast<int>(TGeoCoordinateSystem::CARTESIAN) == GEO_COORDINATE_SYSTEM_CARTESIAN);
static_assert(static_cast<int>(TGeoEdgeAlgorithm::UNKNOWN) == GEO_EDGE_ALGORITHM_UNKNOWN);
static_assert(static_cast<int>(TGeoEdgeAlgorithm::SPHERICAL) == GEO_EDGE_ALGORITHM_SPHERICAL);
static_assert(static_cast<int>(TGeoEdgeAlgorithm::VINCENTY) == GEO_EDGE_ALGORITHM_VINCENTY);
static_assert(static_cast<int>(TGeoEdgeAlgorithm::THOMAS) == GEO_EDGE_ALGORITHM_THOMAS);
static_assert(static_cast<int>(TGeoEdgeAlgorithm::ANDOYER) == GEO_EDGE_ALGORITHM_ANDOYER);
static_assert(static_cast<int>(TGeoEdgeAlgorithm::KARNEY) == GEO_EDGE_ALGORITHM_KARNEY);
static_assert(static_cast<int>(TGeoEdgeAlgorithm::PLANAR) == GEO_EDGE_ALGORITHM_PLANAR);

} // namespace

// Normalize unknown Thrift enum numbers at the input boundary; support policy belongs to callers.
GeoTypeDescriptor GeoTypeDescriptor::from_thrift(const TGeoTypeDesc& thrift) {
    GeoTypeDescriptor result;
    result.logical_type = thrift.__isset.logical_type && GeoLogicalTypePB_IsValid(thrift.logical_type)
                                  ? static_cast<GeoLogicalTypePB>(thrift.logical_type)
                                  : GEO_LOGICAL_TYPE_UNKNOWN;
    result.coordinate_system =
            thrift.__isset.coordinate_system && GeoCoordinateSystemPB_IsValid(thrift.coordinate_system)
                    ? static_cast<GeoCoordinateSystemPB>(thrift.coordinate_system)
                    : GEO_COORDINATE_SYSTEM_UNKNOWN;
    result.edge_algorithm = thrift.__isset.edge_algorithm && GeoEdgeAlgorithmPB_IsValid(thrift.edge_algorithm)
                                    ? static_cast<GeoEdgeAlgorithmPB>(thrift.edge_algorithm)
                                    : GEO_EDGE_ALGORITHM_UNKNOWN;
    if (thrift.__isset.crs) result.crs = thrift.crs;
    if (thrift.__isset.srid) result.srid = thrift.srid;
    return result;
}

GeoTypeDescriptor GeoTypeDescriptor::from_protobuf(const GeoTypeDescPB& protobuf) {
    GeoTypeDescriptor result;
    result.logical_type = protobuf.logical_type();
    result.coordinate_system = protobuf.coordinate_system();
    result.edge_algorithm = protobuf.edge_algorithm();
    result.crs = protobuf.crs();
    if (protobuf.has_srid()) result.srid = protobuf.srid();
    return result;
}

TGeoTypeDesc GeoTypeDescriptor::to_thrift() const {
    TGeoTypeDesc result;
    result.__set_logical_type(static_cast<TGeoLogicalType::type>(logical_type));
    result.__set_coordinate_system(static_cast<TGeoCoordinateSystem::type>(coordinate_system));
    result.__set_edge_algorithm(static_cast<TGeoEdgeAlgorithm::type>(edge_algorithm));
    result.__set_crs(crs);
    if (srid) result.__set_srid(*srid);
    return result;
}

GeoTypeDescPB GeoTypeDescriptor::to_protobuf() const {
    GeoTypeDescPB result;
    result.set_logical_type(logical_type);
    result.set_coordinate_system(coordinate_system);
    result.set_edge_algorithm(edge_algorithm);
    result.set_crs(crs);
    if (srid) result.set_srid(*srid);
    return result;
}

GeoColumnDescriptor GeoColumnDescriptor::from_protobuf(const GeoColumnDescPB& protobuf) {
    return {GeoTypeDescriptor::from_protobuf(protobuf.type())};
}

GeoColumnDescPB GeoColumnDescriptor::to_protobuf() const {
    GeoColumnDescPB result;
    *result.mutable_type() = type.to_protobuf();
    return result;
}

bool is_geo_semantically_compatible(const GeoTypeDescriptor& lhs, const GeoTypeDescriptor& rhs) {
    return lhs.logical_type != GEO_LOGICAL_TYPE_UNKNOWN && lhs.coordinate_system != GEO_COORDINATE_SYSTEM_UNKNOWN &&
           lhs.edge_algorithm != GEO_EDGE_ALGORITHM_UNKNOWN && !lhs.crs.empty() &&
           lhs.logical_type == rhs.logical_type && lhs.coordinate_system == rhs.coordinate_system &&
           lhs.edge_algorithm == rhs.edge_algorithm && lhs.crs == rhs.crs;
}

bool is_geo_compute_compatible(const GeoColumnDescriptor& lhs, const GeoColumnDescriptor& rhs) {
    return is_geo_semantically_compatible(lhs.type, rhs.type);
}

} // namespace starrocks
