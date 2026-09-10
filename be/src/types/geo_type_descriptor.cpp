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
static_assert(static_cast<int>(TGeoEncoding::UNKNOWN) == GEO_ENCODING_UNKNOWN);
static_assert(static_cast<int>(TGeoEncoding::WKB) == GEO_ENCODING_WKB);
static_assert(static_cast<int>(TGeoDimension::UNKNOWN) == GEO_DIMENSION_UNKNOWN);
static_assert(static_cast<int>(TGeoDimension::XY) == GEO_DIMENSION_XY);
static_assert(static_cast<int>(TGeoDimension::XYZ) == GEO_DIMENSION_XYZ);
static_assert(static_cast<int>(TGeoDimension::XYM) == GEO_DIMENSION_XYM);
static_assert(static_cast<int>(TGeoDimension::XYZM) == GEO_DIMENSION_XYZM);
static_assert(static_cast<int>(TGeoDimension::MIXED) == GEO_DIMENSION_MIXED);
static_assert(static_cast<int>(TGeoValidationState::UNKNOWN) == GEO_VALIDATION_STATE_UNKNOWN);
static_assert(static_cast<int>(TGeoValidationState::UNVALIDATED) == GEO_VALIDATION_STATE_UNVALIDATED);
static_assert(static_cast<int>(TGeoValidationState::STRUCTURALLY_VALIDATED) ==
              GEO_VALIDATION_STATE_STRUCTURALLY_VALIDATED);
static_assert(static_cast<int>(TGeoValidationState::SEMANTICALLY_VALIDATED) ==
              GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED);

} // namespace

// Normalize unknown Thrift enum numbers at the input boundary; support policy belongs to callers.
GeoTypeDescriptor GeoTypeDescriptor::from_thrift(const TGeoTypeDesc& thrift) {
    GeoTypeDescriptor result;
    result.logical_type = thrift.__isset.logical_type && PGeoLogicalType_IsValid(thrift.logical_type)
                                  ? static_cast<PGeoLogicalType>(thrift.logical_type)
                                  : GEO_LOGICAL_TYPE_UNKNOWN;
    result.coordinate_system =
            thrift.__isset.coordinate_system && PGeoCoordinateSystem_IsValid(thrift.coordinate_system)
                    ? static_cast<PGeoCoordinateSystem>(thrift.coordinate_system)
                    : GEO_COORDINATE_SYSTEM_UNKNOWN;
    result.edge_algorithm = thrift.__isset.edge_algorithm && PGeoEdgeAlgorithm_IsValid(thrift.edge_algorithm)
                                    ? static_cast<PGeoEdgeAlgorithm>(thrift.edge_algorithm)
                                    : GEO_EDGE_ALGORITHM_UNKNOWN;
    if (thrift.__isset.crs) result.crs = thrift.crs;
    if (thrift.__isset.srid) result.srid = thrift.srid;
    return result;
}

GeoTypeDescriptor GeoTypeDescriptor::from_protobuf(const PGeoTypeDesc& protobuf) {
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

PGeoTypeDesc GeoTypeDescriptor::to_protobuf() const {
    PGeoTypeDesc result;
    result.set_logical_type(logical_type);
    result.set_coordinate_system(coordinate_system);
    result.set_edge_algorithm(edge_algorithm);
    result.set_crs(crs);
    if (srid) result.set_srid(*srid);
    return result;
}

GeoStorageDescriptor GeoStorageDescriptor::from_thrift(const TGeoStorageDesc& thrift) {
    GeoStorageDescriptor result;
    result.encoding = thrift.__isset.encoding && PGeoEncoding_IsValid(thrift.encoding)
                              ? static_cast<PGeoEncoding>(thrift.encoding)
                              : GEO_ENCODING_UNKNOWN;
    result.dimension = thrift.__isset.dimension && PGeoDimension_IsValid(thrift.dimension)
                               ? static_cast<PGeoDimension>(thrift.dimension)
                               : GEO_DIMENSION_UNKNOWN;
    result.validation_state = thrift.__isset.validation_state && PGeoValidationState_IsValid(thrift.validation_state)
                                      ? static_cast<PGeoValidationState>(thrift.validation_state)
                                      : GEO_VALIDATION_STATE_UNKNOWN;
    return result;
}

GeoStorageDescriptor GeoStorageDescriptor::from_protobuf(const PGeoStorageDesc& protobuf) {
    GeoStorageDescriptor result;
    result.encoding = protobuf.encoding();
    result.dimension = protobuf.dimension();
    result.validation_state = protobuf.validation_state();
    return result;
}

TGeoStorageDesc GeoStorageDescriptor::to_thrift() const {
    TGeoStorageDesc result;
    result.__set_encoding(static_cast<TGeoEncoding::type>(encoding));
    result.__set_dimension(static_cast<TGeoDimension::type>(dimension));
    result.__set_validation_state(static_cast<TGeoValidationState::type>(validation_state));
    return result;
}

PGeoStorageDesc GeoStorageDescriptor::to_protobuf() const {
    PGeoStorageDesc result;
    result.set_encoding(encoding);
    result.set_dimension(dimension);
    result.set_validation_state(validation_state);
    return result;
}

GeoColumnDescriptor GeoColumnDescriptor::from_thrift(const TGeoColumnDesc& thrift) {
    return {thrift.__isset.type ? GeoTypeDescriptor::from_thrift(thrift.type) : GeoTypeDescriptor{},
            thrift.__isset.storage ? GeoStorageDescriptor::from_thrift(thrift.storage) : GeoStorageDescriptor{}};
}

GeoColumnDescriptor GeoColumnDescriptor::from_protobuf(const PGeoColumnDesc& protobuf) {
    return {GeoTypeDescriptor::from_protobuf(protobuf.type()), GeoStorageDescriptor::from_protobuf(protobuf.storage())};
}

TGeoColumnDesc GeoColumnDescriptor::to_thrift() const {
    TGeoColumnDesc result;
    result.__set_type(type.to_thrift());
    result.__set_storage(storage.to_thrift());
    return result;
}

PGeoColumnDesc GeoColumnDescriptor::to_protobuf() const {
    PGeoColumnDesc result;
    *result.mutable_type() = type.to_protobuf();
    *result.mutable_storage() = storage.to_protobuf();
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

bool is_geo_assignment_compatible(const GeoColumnDescriptor& source, const GeoColumnDescriptor& target) {
    return is_geo_union_all_compatible(source, target) &&
           (target.storage.dimension == GEO_DIMENSION_UNKNOWN || target.storage.dimension == GEO_DIMENSION_MIXED ||
            source.storage.dimension == target.storage.dimension);
}

bool is_geo_union_all_compatible(const GeoColumnDescriptor& lhs, const GeoColumnDescriptor& rhs) {
    return is_geo_compute_compatible(lhs, rhs) && lhs.storage.encoding == GEO_ENCODING_WKB &&
           rhs.storage.encoding == GEO_ENCODING_WKB;
}

} // namespace starrocks
