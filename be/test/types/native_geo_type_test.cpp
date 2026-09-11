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
        auto created = TypeDescriptor::create_geo_type(primitive, descriptor(primitive));
        ASSERT_TRUE(created.ok()) << created.status();
        const auto& type = *created;
        auto thrift = type.to_thrift();
        EXPECT_EQ(primitive, thrift_to_type(thrift.types[0].scalar_type.type));
        EXPECT_EQ(type, TypeDescriptor::from_thrift(thrift));
        auto protobuf = type.to_protobuf();
        PTypeDesc decoded;
        ASSERT_TRUE(decoded.ParseFromString(protobuf.SerializeAsString()));
        EXPECT_EQ(type, TypeDescriptor::from_protobuf(decoded));
        EXPECT_TRUE(type.validate_geo_type(true).ok());
        EXPECT_TRUE(type.validate_geo_type().is_not_supported());
        EXPECT_FALSE(type.support_join());
        EXPECT_FALSE(type.support_groupby());
        EXPECT_FALSE(type.support_orderby());
        EXPECT_FALSE(type.is_assignable(type));
        auto array = TypeDescriptor::create_array_type(type);
        EXPECT_EQ(array, TypeDescriptor::from_thrift(array.to_thrift()));
        EXPECT_EQ(array, TypeDescriptor::from_protobuf(array.to_protobuf()));
        EXPECT_TRUE(array.validate_geo_type().is_not_supported());
    }
}

TEST(NativeGeoTypeTest, RejectInconsistentAuthority) {
    auto geo = descriptor(TYPE_GEOGRAPHY);
    EXPECT_FALSE(TypeDescriptor::create_geo_type(TYPE_VARBINARY, geo).ok());
    EXPECT_FALSE(TypeDescriptor::create_geo_type(TYPE_GEOMETRY, geo).ok());
    geo.type.coordinate_system = GEO_COORDINATE_SYSTEM_CARTESIAN;
    EXPECT_FALSE(TypeDescriptor::create_geo_type(TYPE_GEOGRAPHY, geo).ok());
    geo = descriptor(TYPE_GEOGRAPHY);
    geo.type.edge_algorithm = GEO_EDGE_ALGORITHM_PLANAR;
    EXPECT_FALSE(TypeDescriptor::create_geo_type(TYPE_GEOGRAPHY, geo).ok());
    geo = descriptor(TYPE_GEOMETRY);
    geo.type.edge_algorithm = GEO_EDGE_ALGORITHM_UNKNOWN;
    EXPECT_FALSE(TypeDescriptor::create_geo_type(TYPE_GEOMETRY, geo).ok());
    geo = descriptor(TYPE_GEOGRAPHY);
    geo.storage.encoding = GEO_ENCODING_UNKNOWN;
    EXPECT_FALSE(TypeDescriptor::create_geo_type(TYPE_GEOGRAPHY, geo).ok());
}

TEST(NativeGeoTypeTest, WireMismatchCheckedByCaller) {
    auto created = TypeDescriptor::create_geo_type(TYPE_GEOGRAPHY, descriptor(TYPE_GEOGRAPHY));
    ASSERT_TRUE(created.ok());
    auto thrift = created->to_thrift();
    thrift.types[0].scalar_type.type = TPrimitiveType::VARBINARY;
    EXPECT_FALSE(TypeDescriptor::from_thrift(thrift).validate_geo_type(true).ok());
    auto proto = created->to_protobuf();
    proto.mutable_types(0)->mutable_scalar_type()->set_type(TPrimitiveType::GEOMETRY);
    EXPECT_FALSE(TypeDescriptor::from_protobuf(proto).validate_geo_type(true).ok());
    proto.mutable_types(0)->mutable_scalar_type()->clear_geo();
    EXPECT_FALSE(TypeDescriptor::from_protobuf(proto).validate_geo_type(true).ok());
}

TEST(NativeGeoTypeTest, DescriptorIdentityAndSharing) {
    auto created = TypeDescriptor::create_geo_type(TYPE_GEOGRAPHY, descriptor(TYPE_GEOGRAPHY));
    ASSERT_TRUE(created.ok());
    auto copy = *created;
    EXPECT_EQ(copy.geo.get(), created->geo.get());
    auto changed = descriptor(TYPE_GEOGRAPHY);
    changed.storage.dimension = GEO_DIMENSION_XYZ;
    auto other = TypeDescriptor::create_geo_type(TYPE_GEOGRAPHY, changed);
    ASSERT_TRUE(other.ok());
    EXPECT_NE(copy, *other);
    TypeDescriptor ordinary(TYPE_INT);
    EXPECT_EQ(nullptr, ordinary.geo);
    EXPECT_TRUE(ordinary.validate_geo_type().ok());
    EXPECT_EQ(ordinary, TypeDescriptor::from_thrift(ordinary.to_thrift()));
}

TEST(NativeGeoTypeTest, FrontendWireFixture) {
    // Shared with NativeGeoTypeTest.java: independent Java and C++ codecs agree.
    const std::string_view wire(
            "\x0a\x26\x08\x00\x12\x22\x08\x1e\x2a\x1e\x0a\x14\x08\x01\x10\x01\x18\x01\x22\x09\x4f\x47\x43\x3a\x43\x52"
            "\x53\x38\x34\x28\xe6\x21\x12\x06\x08\x01\x10\x01\x18\x01",
            40);
    PTypeDesc proto;
    ASSERT_TRUE(proto.ParseFromArray(wire.data(), wire.size()));
    auto expected = TypeDescriptor::create_geo_type(TYPE_GEOGRAPHY, descriptor(TYPE_GEOGRAPHY));
    ASSERT_TRUE(expected.ok());
    EXPECT_EQ(*expected, TypeDescriptor::from_protobuf(proto));
    auto encoded = expected->to_protobuf();
    encoded.mutable_types(0)->mutable_scalar_type()->clear_len(); // FE omits unused scalar length.
    EXPECT_EQ(wire, encoded.SerializeAsString());
}

} // namespace
} // namespace starrocks
