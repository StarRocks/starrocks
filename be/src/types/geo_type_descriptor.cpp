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

#define ASSERT_GEO_ENUM_VALUE(thrift_value, protobuf_value) \
    static_assert(static_cast<int>(thrift_value) == static_cast<int>(protobuf_value))

ASSERT_GEO_ENUM_VALUE(TGeoLogicalType::UNKNOWN, GEO_LOGICAL_TYPE_UNKNOWN);
ASSERT_GEO_ENUM_VALUE(TGeoLogicalType::GEOGRAPHY, GEO_LOGICAL_TYPE_GEOGRAPHY);
ASSERT_GEO_ENUM_VALUE(TGeoLogicalType::GEOMETRY, GEO_LOGICAL_TYPE_GEOMETRY);
ASSERT_GEO_ENUM_VALUE(TGeoCoordinateSystem::UNKNOWN, GEO_COORDINATE_SYSTEM_UNKNOWN);
ASSERT_GEO_ENUM_VALUE(TGeoCoordinateSystem::SPHERICAL, GEO_COORDINATE_SYSTEM_SPHERICAL);
ASSERT_GEO_ENUM_VALUE(TGeoCoordinateSystem::CARTESIAN, GEO_COORDINATE_SYSTEM_CARTESIAN);
ASSERT_GEO_ENUM_VALUE(TGeoEdgeAlgorithm::UNKNOWN, GEO_EDGE_ALGORITHM_UNKNOWN);
ASSERT_GEO_ENUM_VALUE(TGeoEdgeAlgorithm::GEODESIC, GEO_EDGE_ALGORITHM_GEODESIC);
ASSERT_GEO_ENUM_VALUE(TGeoEdgeAlgorithm::LINEAR, GEO_EDGE_ALGORITHM_LINEAR);
ASSERT_GEO_ENUM_VALUE(TGeoEncoding::UNKNOWN, GEO_ENCODING_UNKNOWN);
ASSERT_GEO_ENUM_VALUE(TGeoEncoding::WKB, GEO_ENCODING_WKB);
ASSERT_GEO_ENUM_VALUE(TGeoDimension::UNKNOWN, GEO_DIMENSION_UNKNOWN);
ASSERT_GEO_ENUM_VALUE(TGeoDimension::XY, GEO_DIMENSION_XY);
ASSERT_GEO_ENUM_VALUE(TGeoDimension::XYZ, GEO_DIMENSION_XYZ);
ASSERT_GEO_ENUM_VALUE(TGeoDimension::XYM, GEO_DIMENSION_XYM);
ASSERT_GEO_ENUM_VALUE(TGeoDimension::XYZM, GEO_DIMENSION_XYZM);
ASSERT_GEO_ENUM_VALUE(TGeoDimension::MIXED, GEO_DIMENSION_MIXED);
ASSERT_GEO_ENUM_VALUE(TGeoValidationState::UNKNOWN, GEO_VALIDATION_STATE_UNKNOWN);
ASSERT_GEO_ENUM_VALUE(TGeoValidationState::UNVALIDATED, GEO_VALIDATION_STATE_UNVALIDATED);
ASSERT_GEO_ENUM_VALUE(TGeoValidationState::STRUCTURALLY_VALIDATED, GEO_VALIDATION_STATE_STRUCTURALLY_VALIDATED);
ASSERT_GEO_ENUM_VALUE(TGeoValidationState::SEMANTICALLY_VALIDATED, GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED);

#undef ASSERT_GEO_ENUM_VALUE

} // namespace

GeoTypeDescriptor GeoTypeDescriptor::from_thrift(const TGeoTypeDesc& thrift) {
    GeoTypeDescriptor result;
    if (thrift.__isset.logical_type) {
        result.logical_type = thrift.logical_type;
    }
    if (thrift.__isset.coordinate_system) {
        result.coordinate_system = thrift.coordinate_system;
    }
    if (thrift.__isset.edge_algorithm) {
        result.edge_algorithm = thrift.edge_algorithm;
    }
    if (thrift.__isset.crs) {
        result.crs = thrift.crs;
    }
    if (thrift.__isset.srid) {
        result.srid = thrift.srid;
    }
    return result;
}

GeoTypeDescriptor GeoTypeDescriptor::from_protobuf(const PGeoTypeDesc& protobuf) {
    GeoTypeDescriptor result;
    if (protobuf.has_logical_type()) {
        result.logical_type = static_cast<TGeoLogicalType::type>(protobuf.logical_type());
    }
    if (protobuf.has_coordinate_system()) {
        result.coordinate_system = static_cast<TGeoCoordinateSystem::type>(protobuf.coordinate_system());
    }
    if (protobuf.has_edge_algorithm()) {
        result.edge_algorithm = static_cast<TGeoEdgeAlgorithm::type>(protobuf.edge_algorithm());
    }
    if (protobuf.has_crs()) {
        result.crs = protobuf.crs();
    }
    if (protobuf.has_srid()) {
        result.srid = protobuf.srid();
    }
    return result;
}

TGeoTypeDesc GeoTypeDescriptor::to_thrift() const {
    TGeoTypeDesc result;
    result.__set_logical_type(logical_type);
    result.__set_coordinate_system(coordinate_system);
    result.__set_edge_algorithm(edge_algorithm);
    if (!crs.empty()) {
        result.__set_crs(crs);
    }
    if (srid.has_value()) {
        result.__set_srid(*srid);
    }
    return result;
}

PGeoTypeDesc GeoTypeDescriptor::to_protobuf() const {
    PGeoTypeDesc result;
    result.set_logical_type(static_cast<PGeoLogicalType>(logical_type));
    result.set_coordinate_system(static_cast<PGeoCoordinateSystem>(coordinate_system));
    result.set_edge_algorithm(static_cast<PGeoEdgeAlgorithm>(edge_algorithm));
    if (!crs.empty()) {
        result.set_crs(crs);
    }
    if (srid.has_value()) {
        result.set_srid(*srid);
    }
    return result;
}

bool GeoTypeDescriptor::operator==(const GeoTypeDescriptor& rhs) const {
    return logical_type == rhs.logical_type && coordinate_system == rhs.coordinate_system &&
           edge_algorithm == rhs.edge_algorithm && crs == rhs.crs && srid == rhs.srid;
}

GeoStorageDescriptor GeoStorageDescriptor::from_thrift(const TGeoStorageDesc& thrift) {
    GeoStorageDescriptor result;
    if (thrift.__isset.encoding) {
        result.encoding = thrift.encoding;
    }
    if (thrift.__isset.dimension) {
        result.dimension = thrift.dimension;
    }
    if (thrift.__isset.validation_state) {
        result.validation_state = thrift.validation_state;
    }
    return result;
}

GeoStorageDescriptor GeoStorageDescriptor::from_protobuf(const PGeoStorageDesc& protobuf) {
    GeoStorageDescriptor result;
    if (protobuf.has_encoding()) {
        result.encoding = static_cast<TGeoEncoding::type>(protobuf.encoding());
    }
    if (protobuf.has_dimension()) {
        result.dimension = static_cast<TGeoDimension::type>(protobuf.dimension());
    }
    if (protobuf.has_validation_state()) {
        result.validation_state = static_cast<TGeoValidationState::type>(protobuf.validation_state());
    }
    return result;
}

TGeoStorageDesc GeoStorageDescriptor::to_thrift() const {
    TGeoStorageDesc result;
    result.__set_encoding(encoding);
    result.__set_dimension(dimension);
    result.__set_validation_state(validation_state);
    return result;
}

PGeoStorageDesc GeoStorageDescriptor::to_protobuf() const {
    PGeoStorageDesc result;
    result.set_encoding(static_cast<PGeoEncoding>(encoding));
    result.set_dimension(static_cast<PGeoDimension>(dimension));
    result.set_validation_state(static_cast<PGeoValidationState>(validation_state));
    return result;
}

bool GeoStorageDescriptor::operator==(const GeoStorageDescriptor& rhs) const {
    return encoding == rhs.encoding && dimension == rhs.dimension && validation_state == rhs.validation_state;
}

} // namespace starrocks
