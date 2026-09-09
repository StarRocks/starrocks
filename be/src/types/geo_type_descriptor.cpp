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

#include <tuple>

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

template <typename Enum>
Status check_enum(const std::optional<Enum>& value, bool (*is_valid)(int), const char* field) {
    if (value && !is_valid(static_cast<int>(*value))) {
        return Status::NotSupported(std::string("Unknown geo enum value for ") + field + ": " +
                                    std::to_string(static_cast<int>(*value)));
    }
    return Status::OK();
}

// Both descriptor messages use fields 1..3 for enums. A proto2 unknown enum
// can coexist with a known value for the same field, so has_*() is insufficient.
template <typename Message>
Status check_unknown_enums(const Message& message) {
    const auto& fields = message.unknown_fields();
    for (int i = 0; i < fields.field_count(); ++i) {
        const auto& field = fields.field(i);
        if (field.number() >= 1 && field.number() <= 3) {
            return Status::NotSupported("Unknown geo enum wire value at field " + std::to_string(field.number()));
        }
    }
    return Status::OK();
}

Status check_enums(const GeoTypeDescriptor& desc) {
    RETURN_IF_ERROR(check_enum(desc.logical_type, PGeoLogicalType_IsValid, "logical_type"));
    RETURN_IF_ERROR(check_enum(desc.coordinate_system, PGeoCoordinateSystem_IsValid, "coordinate_system"));
    RETURN_IF_ERROR(check_enum(desc.edge_algorithm, PGeoEdgeAlgorithm_IsValid, "edge_algorithm"));
    return Status::OK();
}

Status check_enums(const GeoStorageDescriptor& desc) {
    RETURN_IF_ERROR(check_enum(desc.encoding, PGeoEncoding_IsValid, "encoding"));
    RETURN_IF_ERROR(check_enum(desc.dimension, PGeoDimension_IsValid, "dimension"));
    RETURN_IF_ERROR(check_enum(desc.validation_state, PGeoValidationState_IsValid, "validation_state"));
    return Status::OK();
}

bool has_wkb_storage(const GeoColumnDescriptor& desc) {
    return desc.storage && desc.storage->encoding == TGeoEncoding::WKB;
}

} // namespace

StatusOr<GeoTypeDescriptor> GeoTypeDescriptor::from_thrift(const TGeoTypeDesc& thrift) {
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
    RETURN_IF_ERROR(check_enums(result));
    return result;
}

StatusOr<GeoTypeDescriptor> GeoTypeDescriptor::from_protobuf(const PGeoTypeDesc& protobuf) {
    RETURN_IF_ERROR(check_unknown_enums(protobuf));
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

StatusOr<TGeoTypeDesc> GeoTypeDescriptor::to_thrift() const {
    RETURN_IF_ERROR(check_enums(*this));
    TGeoTypeDesc result;
    if (logical_type) {
        result.__set_logical_type(*logical_type);
    }
    if (coordinate_system) {
        result.__set_coordinate_system(*coordinate_system);
    }
    if (edge_algorithm) {
        result.__set_edge_algorithm(*edge_algorithm);
    }
    if (crs) {
        result.__set_crs(*crs);
    }
    if (srid) {
        result.__set_srid(*srid);
    }
    return result;
}

StatusOr<PGeoTypeDesc> GeoTypeDescriptor::to_protobuf() const {
    RETURN_IF_ERROR(check_enums(*this));
    PGeoTypeDesc result;
    if (logical_type) {
        result.set_logical_type(static_cast<PGeoLogicalType>(*logical_type));
    }
    if (coordinate_system) {
        result.set_coordinate_system(static_cast<PGeoCoordinateSystem>(*coordinate_system));
    }
    if (edge_algorithm) {
        result.set_edge_algorithm(static_cast<PGeoEdgeAlgorithm>(*edge_algorithm));
    }
    if (crs) {
        result.set_crs(*crs);
    }
    if (srid) {
        result.set_srid(*srid);
    }
    return result;
}

bool GeoTypeDescriptor::operator==(const GeoTypeDescriptor& rhs) const {
    return std::tie(logical_type, coordinate_system, edge_algorithm, crs, srid) ==
           std::tie(rhs.logical_type, rhs.coordinate_system, rhs.edge_algorithm, rhs.crs, rhs.srid);
}

StatusOr<GeoStorageDescriptor> GeoStorageDescriptor::from_thrift(const TGeoStorageDesc& thrift) {
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
    RETURN_IF_ERROR(check_enums(result));
    return result;
}

StatusOr<GeoStorageDescriptor> GeoStorageDescriptor::from_protobuf(const PGeoStorageDesc& protobuf) {
    RETURN_IF_ERROR(check_unknown_enums(protobuf));
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

StatusOr<TGeoStorageDesc> GeoStorageDescriptor::to_thrift() const {
    RETURN_IF_ERROR(check_enums(*this));
    TGeoStorageDesc result;
    if (encoding) {
        result.__set_encoding(*encoding);
    }
    if (dimension) {
        result.__set_dimension(*dimension);
    }
    if (validation_state) {
        result.__set_validation_state(*validation_state);
    }
    return result;
}

StatusOr<PGeoStorageDesc> GeoStorageDescriptor::to_protobuf() const {
    RETURN_IF_ERROR(check_enums(*this));
    PGeoStorageDesc result;
    if (encoding) {
        result.set_encoding(static_cast<PGeoEncoding>(*encoding));
    }
    if (dimension) {
        result.set_dimension(static_cast<PGeoDimension>(*dimension));
    }
    if (validation_state) {
        result.set_validation_state(static_cast<PGeoValidationState>(*validation_state));
    }
    return result;
}

bool GeoStorageDescriptor::operator==(const GeoStorageDescriptor& rhs) const {
    return std::tie(encoding, dimension, validation_state) ==
           std::tie(rhs.encoding, rhs.dimension, rhs.validation_state);
}

StatusOr<GeoColumnDescriptor> GeoColumnDescriptor::from_thrift(const TGeoColumnDesc& thrift) {
    GeoColumnDescriptor result;
    if (thrift.__isset.type) {
        ASSIGN_OR_RETURN(result.type, GeoTypeDescriptor::from_thrift(thrift.type));
    }
    if (thrift.__isset.storage) {
        ASSIGN_OR_RETURN(result.storage, GeoStorageDescriptor::from_thrift(thrift.storage));
    }
    return result;
}

StatusOr<GeoColumnDescriptor> GeoColumnDescriptor::from_protobuf(const PGeoColumnDesc& protobuf) {
    GeoColumnDescriptor result;
    if (protobuf.has_type()) {
        ASSIGN_OR_RETURN(result.type, GeoTypeDescriptor::from_protobuf(protobuf.type()));
    }
    if (protobuf.has_storage()) {
        ASSIGN_OR_RETURN(result.storage, GeoStorageDescriptor::from_protobuf(protobuf.storage()));
    }
    return result;
}

StatusOr<TGeoColumnDesc> GeoColumnDescriptor::to_thrift() const {
    TGeoColumnDesc result;
    if (type) {
        ASSIGN_OR_RETURN(auto value, type->to_thrift());
        result.__set_type(value);
    }
    if (storage) {
        ASSIGN_OR_RETURN(auto value, storage->to_thrift());
        result.__set_storage(value);
    }
    return result;
}

StatusOr<PGeoColumnDesc> GeoColumnDescriptor::to_protobuf() const {
    PGeoColumnDesc result;
    if (type) {
        ASSIGN_OR_RETURN(auto value, type->to_protobuf());
        *result.mutable_type() = std::move(value);
    }
    if (storage) {
        ASSIGN_OR_RETURN(auto value, storage->to_protobuf());
        *result.mutable_storage() = std::move(value);
    }
    return result;
}

bool GeoColumnDescriptor::operator==(const GeoColumnDescriptor& rhs) const {
    return std::tie(type, storage) == std::tie(rhs.type, rhs.storage);
}

bool is_geo_semantically_compatible(const GeoTypeDescriptor& lhs, const GeoTypeDescriptor& rhs) {
    return lhs.logical_type && *lhs.logical_type != TGeoLogicalType::UNKNOWN && lhs.coordinate_system &&
           *lhs.coordinate_system != TGeoCoordinateSystem::UNKNOWN && lhs.edge_algorithm &&
           *lhs.edge_algorithm != TGeoEdgeAlgorithm::UNKNOWN && lhs.crs && !lhs.crs->empty() &&
           lhs.logical_type == rhs.logical_type && lhs.coordinate_system == rhs.coordinate_system &&
           lhs.edge_algorithm == rhs.edge_algorithm && lhs.crs == rhs.crs;
}

bool is_geo_compute_compatible(const GeoColumnDescriptor& lhs, const GeoColumnDescriptor& rhs) {
    return lhs.type && rhs.type && is_geo_semantically_compatible(*lhs.type, *rhs.type);
}

bool is_geo_assignment_compatible(const GeoColumnDescriptor& source, const GeoColumnDescriptor& target) {
    if (!is_geo_union_all_compatible(source, target)) {
        return false;
    }
    const auto dimension = target.storage->dimension.value_or(TGeoDimension::UNKNOWN);
    return dimension == TGeoDimension::UNKNOWN || dimension == TGeoDimension::MIXED ||
           source.storage->dimension == dimension;
}

bool is_geo_union_all_compatible(const GeoColumnDescriptor& lhs, const GeoColumnDescriptor& rhs) {
    return is_geo_compute_compatible(lhs, rhs) && has_wkb_storage(lhs) && has_wkb_storage(rhs);
}

} // namespace starrocks
