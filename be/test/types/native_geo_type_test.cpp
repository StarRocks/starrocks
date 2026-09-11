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

#include <gtest/gtest.h>

#include <string_view>

#include "types/type_descriptor.h"

namespace starrocks {
namespace {

GeoColumnDescriptor descriptor(LogicalType primitive) {
    GeoColumnDescriptor result;
    bool geography = primitive == TYPE_GEOGRAPHY;
    result.type.logical_type = geography ? GEO_LOGICAL_TYPE_GEOGRAPHY : GEO_LOGICAL_TYPE_GEOMETRY;
    result.type.coordinate_system = geography ? GEO_COORDINATE_SYSTEM_SPHERICAL : GEO_COORDINATE_SYSTEM_CARTESIAN;
    result.type.edge_algorithm = geography ? GEO_EDGE_ALGORITHM_SPHERICAL : GEO_EDGE_ALGORITHM_PLANAR;
    result.type.crs = "OGC:CRS84";
    result.type.srid = 4326;
    result.storage.encoding = GEO_ENCODING_WKB;
    result.storage.dimension = GEO_DIMENSION_XY;
    result.storage.validation_state = GEO_VALIDATION_STATE_UNVALIDATED;
    return result;
}

TEST(NativeGeoTypeTest, PrimitiveAndMetadataRoundTrip) {
    for (auto primitive : {TYPE_GEOGRAPHY, TYPE_GEOMETRY}) {
        auto type = TypeDescriptor::create_geo_type(primitive, descriptor(primitive).type);
        auto thrift = type.to_thrift();
        EXPECT_EQ(primitive, thrift_to_type(thrift.types[0].scalar_type.type));
        EXPECT_FALSE(thrift.types[0].scalar_type.geo.__isset.storage);
        EXPECT_EQ(type, TypeDescriptor::from_thrift(thrift));
        auto protobuf = type.to_protobuf();
        EXPECT_FALSE(protobuf.types(0).scalar_type().geo().has_storage());
        PTypeDesc decoded;
        ASSERT_TRUE(decoded.ParseFromString(protobuf.SerializeAsString()));
        EXPECT_EQ(type, TypeDescriptor::from_protobuf(decoded));
        EXPECT_FALSE(type.support_join());
        EXPECT_FALSE(type.support_groupby());
        EXPECT_FALSE(type.support_orderby());
        EXPECT_FALSE(type.is_assignable(type));
        auto array = TypeDescriptor::create_array_type(type);
        EXPECT_EQ(array, TypeDescriptor::from_thrift(array.to_thrift()));
        EXPECT_EQ(array, TypeDescriptor::from_protobuf(array.to_protobuf()));
    }
}

TEST(NativeGeoTypeTest, OptionalSemanticMetadata) {
    for (auto primitive : {TYPE_INT, TYPE_GEOGRAPHY, TYPE_GEOMETRY}) {
        TypeDescriptor type(primitive);
        EXPECT_FALSE(type.geo_type.has_value());
        EXPECT_FALSE(type.to_thrift().types[0].scalar_type.__isset.geo);
        EXPECT_FALSE(type.to_protobuf().types(0).scalar_type().has_geo());
        EXPECT_EQ(type, TypeDescriptor::from_thrift(type.to_thrift()));
        EXPECT_EQ(type, TypeDescriptor::from_protobuf(type.to_protobuf()));
        if (type.is_geo_type()) EXPECT_FALSE(type.is_assignable(type));
    }
    auto type = TypeDescriptor::create_geo_type(TYPE_GEOGRAPHY, {});
    EXPECT_TRUE(TypeDescriptor::from_thrift(type.to_thrift()).geo_type.has_value());
    EXPECT_TRUE(TypeDescriptor::from_protobuf(type.to_protobuf()).geo_type.has_value());
    EXPECT_NE(TypeDescriptor(TYPE_GEOGRAPHY), type);
}

TEST(NativeGeoTypeTest, ConversionDoesNotValidateSemantics) {
    // FE validates the type; consumers decide which edge algorithms they support.
    auto semantic = descriptor(TYPE_GEOGRAPHY).type;
    semantic.edge_algorithm = GEO_EDGE_ALGORITHM_UNKNOWN;
    auto type = TypeDescriptor::create_geo_type(TYPE_GEOGRAPHY, semantic);
    EXPECT_EQ(type, TypeDescriptor::from_thrift(type.to_thrift()));
    EXPECT_EQ(type, TypeDescriptor::from_protobuf(type.to_protobuf()));
}

TEST(NativeGeoTypeTest, SemanticMetadataHasValueSemantics) {
    auto type = TypeDescriptor::create_geo_type(TYPE_GEOGRAPHY, descriptor(TYPE_GEOGRAPHY).type);
    auto copy = type;
    EXPECT_EQ(copy, type);
    copy.geo_type->crs = "urn:ogc:def:crs:OGC::CRS84";
    EXPECT_EQ("OGC:CRS84", type.geo_type->crs);
    EXPECT_NE(copy, type);
    copy = type;
    copy.geo_type->srid.reset();
    EXPECT_NE(copy, type);
    EXPECT_EQ(4326, type.geo_type->srid);
}

TEST(NativeGeoTypeTest, ColumnRepresentationIsSeparateFromType) {
    auto column = descriptor(TYPE_GEOGRAPHY);
    auto type = TypeDescriptor::create_geo_type(TYPE_GEOGRAPHY, column.type);
    // A column reuses the semantic value, and transports its representation separately.
    column.type = *type.geo_type;
    column.storage.dimension = GEO_DIMENSION_XYZ;
    column.storage.validation_state = GEO_VALIDATION_STATE_STRUCTURALLY_VALIDATED;
    EXPECT_EQ(column, GeoColumnDescriptor::from_thrift(column.to_thrift()));
    EXPECT_EQ(column, GeoColumnDescriptor::from_protobuf(column.to_protobuf()));
    auto thrift = type.to_thrift();
    thrift.types[0].scalar_type.__set_geo(column.to_thrift());
    EXPECT_EQ(type, TypeDescriptor::from_thrift(thrift));
    auto proto = type.to_protobuf();
    *proto.mutable_types(0)->mutable_scalar_type()->mutable_geo() = column.to_protobuf();
    EXPECT_EQ(type, TypeDescriptor::from_protobuf(proto));

    thrift.types[0].scalar_type.geo.__isset.type = false;
    proto.mutable_types(0)->mutable_scalar_type()->mutable_geo()->clear_type();
    EXPECT_FALSE(TypeDescriptor::from_thrift(thrift).geo_type.has_value());
    EXPECT_FALSE(TypeDescriptor::from_protobuf(proto).geo_type.has_value());
}

TEST(NativeGeoTypeTest, FrontendWireFixture) {
    // Existing FE fixture carries a full column descriptor; only its semantic part belongs here.
    const std::string_view wire(
            "\x0a\x26\x08\x00\x12\x22\x08\x1e\x2a\x1e\x0a\x14\x08\x01\x10\x01\x18\x01\x22\x09\x4f\x47\x43\x3a\x43\x52"
            "\x53\x38\x34\x28\xe6\x21\x12\x06\x08\x01\x10\x01\x18\x01",
            40);
    PTypeDesc proto;
    ASSERT_TRUE(proto.ParseFromArray(wire.data(), wire.size()));
    auto expected = TypeDescriptor::create_geo_type(TYPE_GEOGRAPHY, descriptor(TYPE_GEOGRAPHY).type);
    EXPECT_EQ(expected, TypeDescriptor::from_protobuf(proto));
    auto encoded = expected.to_protobuf();
    encoded.mutable_types(0)->mutable_scalar_type()->clear_len(); // FE omits unused scalar length.
    proto.mutable_types(0)->mutable_scalar_type()->mutable_geo()->clear_storage();
    EXPECT_EQ(proto.SerializeAsString(), encoded.SerializeAsString());
}

TEST(NativeGeoTypeTest, DiagnosticTypeNames) {
    for (auto primitive : {TYPE_GEOGRAPHY, TYPE_GEOMETRY}) {
        const char* expected = primitive == TYPE_GEOGRAPHY ? "GEOGRAPHY" : "GEOMETRY";
        EXPECT_STREQ(expected, logical_type_to_string(primitive));
        EXPECT_EQ(expected, type_to_string(primitive));
        EXPECT_EQ(expected, type_to_string_v2(primitive));
    }
    EXPECT_STREQ("VARIANT", logical_type_to_string(TYPE_VARIANT));
    EXPECT_EQ("VARIANT", type_to_string(TYPE_VARIANT));
    EXPECT_EQ("VARIANT", type_to_string_v2(TYPE_VARIANT));
    EXPECT_EQ(TPrimitiveType::VARIANT, to_thrift(TYPE_VARIANT));
    EXPECT_EQ(TYPE_VARIANT, thrift_to_type(TPrimitiveType::VARIANT));
}

TEST(NativeGeoTypeTest, ExcludedFromZoneMapAndChecksum) {
    for (auto primitive : {TYPE_GEOGRAPHY, TYPE_GEOMETRY}) {
        EXPECT_FALSE(is_zone_map_key_type(primitive));
        EXPECT_FALSE(is_support_checksum_type(primitive));
    }
    EXPECT_TRUE(is_zone_map_key_type(TYPE_INT));
    EXPECT_TRUE(is_support_checksum_type(TYPE_INT));
    EXPECT_FALSE(is_zone_map_key_type(TYPE_JSON));
    EXPECT_FALSE(is_support_checksum_type(TYPE_JSON));
}

} // namespace
} // namespace starrocks
