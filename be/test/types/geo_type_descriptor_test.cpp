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

#include <gtest/gtest.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <array>
#include <limits>

namespace starrocks {
namespace {

using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::transport::TMemoryBuffer;

GeoColumnDescriptor geography() {
    return {GeoTypeDescriptor{TGeoLogicalType::GEOGRAPHY, TGeoCoordinateSystem::SPHERICAL, TGeoEdgeAlgorithm::SPHERICAL,
                              "OGC:CRS84", std::nullopt},
            GeoStorageDescriptor{TGeoEncoding::WKB, TGeoDimension::XY, TGeoValidationState::UNVALIDATED}};
}

template <typename Thrift>
Thrift thrift_wire_round_trip(const Thrift& input) {
    auto buffer = std::make_shared<TMemoryBuffer>();
    TBinaryProtocol protocol(buffer);
    input.write(&protocol);
    Thrift output;
    output.read(&protocol);
    return output;
}

template <typename Descriptor>
void check_round_trip(const Descriptor& expected) {
    auto thrift = expected.to_thrift();
    ASSERT_TRUE(thrift.ok()) << thrift.status();
    auto decoded_thrift = Descriptor::from_thrift(thrift_wire_round_trip(*thrift));
    ASSERT_TRUE(decoded_thrift.ok()) << decoded_thrift.status();
    EXPECT_EQ(expected, *decoded_thrift);

    auto protobuf = expected.to_protobuf();
    ASSERT_TRUE(protobuf.ok()) << protobuf.status();
    auto parsed = *protobuf;
    parsed.Clear();
    ASSERT_TRUE(parsed.ParseFromString(protobuf->SerializeAsString()));
    auto decoded_protobuf = Descriptor::from_protobuf(parsed);
    ASSERT_TRUE(decoded_protobuf.ok()) << decoded_protobuf.status();
    EXPECT_EQ(expected, *decoded_protobuf);

    auto cross_wire = decoded_thrift->to_protobuf();
    ASSERT_TRUE(cross_wire.ok()) << cross_wire.status();
    auto cross_desc = Descriptor::from_protobuf(*cross_wire);
    ASSERT_TRUE(cross_desc.ok()) << cross_desc.status();
    EXPECT_EQ(expected, *cross_desc);
}

TEST(GeoTypeDescriptorTest, AllAlgorithmsAndStorageStatesRoundTrip) {
    const std::array algorithms = {TGeoEdgeAlgorithm::UNKNOWN,  TGeoEdgeAlgorithm::SPHERICAL,
                                   TGeoEdgeAlgorithm::VINCENTY, TGeoEdgeAlgorithm::THOMAS,
                                   TGeoEdgeAlgorithm::ANDOYER,  TGeoEdgeAlgorithm::KARNEY,
                                   TGeoEdgeAlgorithm::PLANAR};
    for (auto algorithm : algorithms) {
        for (int dim = TGeoDimension::UNKNOWN; dim <= TGeoDimension::MIXED; ++dim) {
            for (int state = TGeoValidationState::UNKNOWN; state <= TGeoValidationState::SEMANTICALLY_VALIDATED;
                 ++state) {
                SCOPED_TRACE(::testing::Message() << algorithm << "/" << dim << "/" << state);
                auto desc = geography();
                desc.type->edge_algorithm = algorithm;
                if (algorithm == TGeoEdgeAlgorithm::PLANAR) {
                    desc.type->logical_type = TGeoLogicalType::GEOMETRY;
                    desc.type->coordinate_system = TGeoCoordinateSystem::CARTESIAN;
                }
                desc.type->srid = 4326;
                desc.storage->dimension = static_cast<TGeoDimension::type>(dim);
                desc.storage->validation_state = static_cast<TGeoValidationState::type>(state);
                check_round_trip(*desc.type);
                check_round_trip(*desc.storage);
                check_round_trip(desc);
            }
        }
    }
}

TEST(GeoTypeDescriptorTest, WireOrdinalsRemainStable) {
    EXPECT_EQ(0, TGeoLogicalType::UNKNOWN);
    EXPECT_EQ(0, GEO_LOGICAL_TYPE_UNKNOWN);
    EXPECT_EQ(1, TGeoLogicalType::GEOGRAPHY);
    EXPECT_EQ(1, GEO_LOGICAL_TYPE_GEOGRAPHY);
    EXPECT_EQ(2, TGeoLogicalType::GEOMETRY);
    EXPECT_EQ(2, GEO_LOGICAL_TYPE_GEOMETRY);
    EXPECT_EQ(0, TGeoCoordinateSystem::UNKNOWN);
    EXPECT_EQ(0, GEO_COORDINATE_SYSTEM_UNKNOWN);
    EXPECT_EQ(1, TGeoCoordinateSystem::SPHERICAL);
    EXPECT_EQ(1, GEO_COORDINATE_SYSTEM_SPHERICAL);
    EXPECT_EQ(2, TGeoCoordinateSystem::CARTESIAN);
    EXPECT_EQ(2, GEO_COORDINATE_SYSTEM_CARTESIAN);
    EXPECT_EQ(0, TGeoEdgeAlgorithm::UNKNOWN);
    EXPECT_EQ(0, GEO_EDGE_ALGORITHM_UNKNOWN);
    EXPECT_EQ(1, TGeoEdgeAlgorithm::SPHERICAL);
    EXPECT_EQ(1, GEO_EDGE_ALGORITHM_SPHERICAL);
    EXPECT_EQ(2, TGeoEdgeAlgorithm::VINCENTY);
    EXPECT_EQ(2, GEO_EDGE_ALGORITHM_VINCENTY);
    EXPECT_EQ(3, TGeoEdgeAlgorithm::THOMAS);
    EXPECT_EQ(3, GEO_EDGE_ALGORITHM_THOMAS);
    EXPECT_EQ(4, TGeoEdgeAlgorithm::ANDOYER);
    EXPECT_EQ(4, GEO_EDGE_ALGORITHM_ANDOYER);
    EXPECT_EQ(5, TGeoEdgeAlgorithm::KARNEY);
    EXPECT_EQ(5, GEO_EDGE_ALGORITHM_KARNEY);
    EXPECT_EQ(6, TGeoEdgeAlgorithm::PLANAR);
    EXPECT_EQ(6, GEO_EDGE_ALGORITHM_PLANAR);
    EXPECT_EQ(0, TGeoEncoding::UNKNOWN);
    EXPECT_EQ(0, GEO_ENCODING_UNKNOWN);
    EXPECT_EQ(1, TGeoEncoding::WKB);
    EXPECT_EQ(1, GEO_ENCODING_WKB);
    EXPECT_EQ(0, TGeoDimension::UNKNOWN);
    EXPECT_EQ(0, GEO_DIMENSION_UNKNOWN);
    EXPECT_EQ(1, TGeoDimension::XY);
    EXPECT_EQ(1, GEO_DIMENSION_XY);
    EXPECT_EQ(2, TGeoDimension::XYZ);
    EXPECT_EQ(2, GEO_DIMENSION_XYZ);
    EXPECT_EQ(3, TGeoDimension::XYM);
    EXPECT_EQ(3, GEO_DIMENSION_XYM);
    EXPECT_EQ(4, TGeoDimension::XYZM);
    EXPECT_EQ(4, GEO_DIMENSION_XYZM);
    EXPECT_EQ(5, TGeoDimension::MIXED);
    EXPECT_EQ(5, GEO_DIMENSION_MIXED);
    EXPECT_EQ(0, TGeoValidationState::UNKNOWN);
    EXPECT_EQ(0, GEO_VALIDATION_STATE_UNKNOWN);
    EXPECT_EQ(1, TGeoValidationState::UNVALIDATED);
    EXPECT_EQ(1, GEO_VALIDATION_STATE_UNVALIDATED);
    EXPECT_EQ(2, TGeoValidationState::STRUCTURALLY_VALIDATED);
    EXPECT_EQ(2, GEO_VALIDATION_STATE_STRUCTURALLY_VALIDATED);
    EXPECT_EQ(3, TGeoValidationState::SEMANTICALLY_VALIDATED);
    EXPECT_EQ(3, GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED);
    EXPECT_EQ(1, PGeoTypeDesc::kLogicalTypeFieldNumber);
    EXPECT_EQ(2, PGeoTypeDesc::kCoordinateSystemFieldNumber);
    EXPECT_EQ(3, PGeoTypeDesc::kEdgeAlgorithmFieldNumber);
    EXPECT_EQ(4, PGeoTypeDesc::kCrsFieldNumber);
    EXPECT_EQ(5, PGeoTypeDesc::kSridFieldNumber);
    EXPECT_EQ(1, PGeoStorageDesc::kEncodingFieldNumber);
    EXPECT_EQ(2, PGeoStorageDesc::kDimensionFieldNumber);
    EXPECT_EQ(3, PGeoStorageDesc::kValidationStateFieldNumber);
    EXPECT_EQ(1, PGeoColumnDesc::kTypeFieldNumber);
    EXPECT_EQ(2, PGeoColumnDesc::kStorageFieldNumber);
}

TEST(GeoTypeDescriptorTest, AlgorithmsRemainDistinctForCompatibility) {
    for (int left = TGeoEdgeAlgorithm::SPHERICAL; left <= TGeoEdgeAlgorithm::PLANAR; ++left) {
        for (int right = TGeoEdgeAlgorithm::SPHERICAL; right <= TGeoEdgeAlgorithm::PLANAR; ++right) {
            auto source = geography();
            auto target = geography();
            source.type->edge_algorithm = static_cast<TGeoEdgeAlgorithm::type>(left);
            target.type->edge_algorithm = static_cast<TGeoEdgeAlgorithm::type>(right);
            EXPECT_EQ(left == right, is_geo_compute_compatible(source, target));
            EXPECT_EQ(left == right, is_geo_assignment_compatible(source, target));
            EXPECT_EQ(left == right, is_geo_union_all_compatible(source, target));
        }
    }
}

TEST(GeoTypeDescriptorTest, OptionalFieldsPreservePresence) {
    check_round_trip(GeoTypeDescriptor{});
    check_round_trip(GeoStorageDescriptor{});
    check_round_trip(GeoColumnDescriptor{});
    check_round_trip(GeoColumnDescriptor{GeoTypeDescriptor{}, GeoStorageDescriptor{}});
    // Each optional field independently, including explicit default values.
    for (int field = 0; field < 5; ++field) {
        GeoTypeDescriptor desc;
        switch (field) {
        case 0:
            desc.logical_type = TGeoLogicalType::UNKNOWN;
            break;
        case 1:
            desc.coordinate_system = TGeoCoordinateSystem::UNKNOWN;
            break;
        case 2:
            desc.edge_algorithm = TGeoEdgeAlgorithm::UNKNOWN;
            break;
        case 3:
            desc.crs = "";
            break;
        case 4:
            desc.srid = 0;
            break;
        }
        check_round_trip(desc);
        EXPECT_NE(desc, GeoTypeDescriptor{});
        check_round_trip(GeoColumnDescriptor{desc, std::nullopt});
    }
    for (int field = 0; field < 3; ++field) {
        GeoStorageDescriptor desc;
        switch (field) {
        case 0:
            desc.encoding = TGeoEncoding::UNKNOWN;
            break;
        case 1:
            desc.dimension = TGeoDimension::UNKNOWN;
            break;
        case 2:
            desc.validation_state = TGeoValidationState::UNKNOWN;
            break;
        }
        check_round_trip(desc);
        EXPECT_NE(desc, GeoStorageDescriptor{});
        check_round_trip(GeoColumnDescriptor{std::nullopt, desc});
    }
}

// Construct actual wire bytes rather than relying on generated enum setters.
template <typename Thrift>
Thrift thrift_enum_wire(int16_t field, int32_t value) {
    auto buffer = std::make_shared<TMemoryBuffer>();
    TBinaryProtocol protocol(buffer);
    protocol.writeStructBegin("geo");
    protocol.writeFieldBegin("enum", apache::thrift::protocol::T_I32, field);
    protocol.writeI32(value);
    protocol.writeFieldEnd();
    protocol.writeFieldStop();
    protocol.writeStructEnd();
    Thrift parsed;
    parsed.read(&protocol);
    return parsed;
}

TEST(GeoTypeDescriptorTest, UnknownThriftEnumsAreRejectedBeforeConversion) {
    for (auto value : {127, -1, std::numeric_limits<int32_t>::max()}) {
        for (int16_t field = 1; field <= 3; ++field) {
            SCOPED_TRACE(::testing::Message() << field << "/" << value);
            const auto type = thrift_enum_wire<TGeoTypeDesc>(field, value);
            EXPECT_TRUE(GeoTypeDescriptor::from_thrift(type).status().is_not_supported());
            const auto storage = thrift_enum_wire<TGeoStorageDesc>(field, value);
            EXPECT_TRUE(GeoStorageDescriptor::from_thrift(storage).status().is_not_supported());
            TGeoColumnDesc column;
            column.__set_type(type);
            EXPECT_TRUE(GeoColumnDescriptor::from_thrift(column).status().is_not_supported());
            column = TGeoColumnDesc{};
            column.__set_storage(storage);
            EXPECT_TRUE(GeoColumnDescriptor::from_thrift(column).status().is_not_supported());
        }
    }
}

template <typename Proto, typename Descriptor>
void check_unknown_protobuf_enum(int field, int32_t value) {
    // Serialization followed by parsing places closed-enum values in unknown_fields().
    Proto input;
    input.mutable_unknown_fields()->AddVarint(field, static_cast<uint64_t>(static_cast<int64_t>(value)));
    for (bool include_known_value : {false, true}) {
        if (include_known_value) {
            const auto* enum_field = input.GetDescriptor()->FindFieldByNumber(field);
            input.GetReflection()->SetEnum(&input, enum_field, enum_field->enum_type()->value(0));
        }
        Proto parsed;
        ASSERT_TRUE(parsed.ParseFromString(input.SerializeAsString()));
        ASSERT_EQ(1, parsed.unknown_fields().field_count());
        EXPECT_EQ(static_cast<uint64_t>(static_cast<int64_t>(value)), parsed.unknown_fields().field(0).varint());
        EXPECT_TRUE(Descriptor::from_protobuf(parsed).status().is_not_supported());
    }
}

TEST(GeoTypeDescriptorTest, Proto2ClosedEnumsAreRejectedEvenAlongsideKnownValues) {
    for (auto value : {127, -1, std::numeric_limits<int32_t>::max()}) {
        for (int field = 1; field <= 3; ++field) {
            SCOPED_TRACE(::testing::Message() << field << "/" << value);
            check_unknown_protobuf_enum<PGeoTypeDesc, GeoTypeDescriptor>(field, value);
            check_unknown_protobuf_enum<PGeoStorageDesc, GeoStorageDescriptor>(field, value);
            PGeoColumnDesc column;
            column.mutable_type()->mutable_unknown_fields()->AddVarint(field, value);
            PGeoColumnDesc parsed;
            ASSERT_TRUE(parsed.ParseFromString(column.SerializeAsString()));
            EXPECT_TRUE(GeoColumnDescriptor::from_protobuf(parsed).status().is_not_supported());
            column.Clear();
            column.mutable_storage()->mutable_unknown_fields()->AddVarint(field, value);
            ASSERT_TRUE(parsed.ParseFromString(column.SerializeAsString()));
            EXPECT_TRUE(GeoColumnDescriptor::from_protobuf(parsed).status().is_not_supported());
        }
    }
}

TEST(GeoTypeDescriptorTest, UnrecognizedEnumsCannotBeSerializedFromLocalDescriptors) {
    {
        GeoTypeDescriptor desc;
        desc.logical_type = static_cast<TGeoLogicalType::type>(127);
        EXPECT_TRUE(desc.to_thrift().status().is_not_supported());
        EXPECT_TRUE(desc.to_protobuf().status().is_not_supported());
    }
    {
        GeoTypeDescriptor desc;
        desc.coordinate_system = static_cast<TGeoCoordinateSystem::type>(127);
        EXPECT_TRUE(desc.to_thrift().status().is_not_supported());
        EXPECT_TRUE(desc.to_protobuf().status().is_not_supported());
    }
    {
        GeoTypeDescriptor desc;
        desc.edge_algorithm = static_cast<TGeoEdgeAlgorithm::type>(127);
        EXPECT_TRUE(desc.to_thrift().status().is_not_supported());
        EXPECT_TRUE(desc.to_protobuf().status().is_not_supported());
    }
    {
        GeoStorageDescriptor desc;
        desc.encoding = static_cast<TGeoEncoding::type>(127);
        EXPECT_TRUE(desc.to_thrift().status().is_not_supported());
        EXPECT_TRUE(desc.to_protobuf().status().is_not_supported());
    }
    {
        GeoStorageDescriptor desc;
        desc.dimension = static_cast<TGeoDimension::type>(127);
        EXPECT_TRUE(desc.to_thrift().status().is_not_supported());
        EXPECT_TRUE(desc.to_protobuf().status().is_not_supported());
    }
    {
        GeoStorageDescriptor desc;
        desc.validation_state = static_cast<TGeoValidationState::type>(127);
        EXPECT_TRUE(desc.to_thrift().status().is_not_supported());
        EXPECT_TRUE(desc.to_protobuf().status().is_not_supported());
    }
}

TEST(GeoTypeDescriptorTest, ValidationStateAndParsedSridAreNotTypeIdentity) {
    auto source = geography();
    for (int state = TGeoValidationState::UNKNOWN; state <= TGeoValidationState::SEMANTICALLY_VALIDATED; ++state) {
        auto target = source;
        target.storage->validation_state = static_cast<TGeoValidationState::type>(state);
        target.type->srid = 4326;
        EXPECT_NE(source, target);
        EXPECT_TRUE(is_geo_semantically_compatible(*source.type, *target.type));
        EXPECT_TRUE(is_geo_compute_compatible(source, target));
        EXPECT_TRUE(is_geo_assignment_compatible(source, target));
        EXPECT_TRUE(is_geo_union_all_compatible(source, target));
    }
    auto target = source;
    target.storage->validation_state.reset();
    EXPECT_TRUE(is_geo_assignment_compatible(source, target));
}

TEST(GeoTypeDescriptorTest, SemanticMismatchIsNotImplicitlyConverted) {
    const auto source = geography();
    for (int field = 0; field < 4; ++field) {
        auto target = source;
        switch (field) {
        case 0:
            target.type->logical_type = TGeoLogicalType::GEOMETRY;
            break;
        case 1:
            target.type->coordinate_system = TGeoCoordinateSystem::CARTESIAN;
            break;
        case 2:
            target.type->edge_algorithm = TGeoEdgeAlgorithm::KARNEY;
            break;
        case 3:
            target.type->crs = "EPSG:4326";
            break; // Not an axis-order-safe CRS84 alias.
        }
        EXPECT_FALSE(is_geo_compute_compatible(source, target));
        EXPECT_FALSE(is_geo_assignment_compatible(source, target));
        EXPECT_FALSE(is_geo_union_all_compatible(source, target));
    }
    EXPECT_FALSE(is_geo_compute_compatible({}, {}));
    auto incomplete = source;
    incomplete.type->crs.reset();
    EXPECT_FALSE(is_geo_compute_compatible(incomplete, incomplete));
    incomplete.type->crs = "";
    EXPECT_FALSE(is_geo_compute_compatible(incomplete, incomplete));
    incomplete = source;
    incomplete.type->edge_algorithm = TGeoEdgeAlgorithm::UNKNOWN;
    EXPECT_FALSE(is_geo_compute_compatible(incomplete, incomplete));
}

TEST(GeoTypeDescriptorTest, EncodingAffectsTransportNotComputeIdentity) {
    const auto source = geography();
    auto target = source;
    target.storage->encoding = TGeoEncoding::UNKNOWN;
    EXPECT_TRUE(is_geo_compute_compatible(source, target));
    EXPECT_FALSE(is_geo_assignment_compatible(source, target));
    EXPECT_FALSE(is_geo_assignment_compatible(target, source));
    EXPECT_FALSE(is_geo_union_all_compatible(source, target));
    target.storage->encoding.reset();
    EXPECT_FALSE(is_geo_union_all_compatible(source, target));
    target.storage.reset();
    EXPECT_TRUE(is_geo_compute_compatible(source, target));
    EXPECT_FALSE(is_geo_assignment_compatible(source, target));
}

TEST(GeoTypeDescriptorTest, DimensionGuaranteesAreDirectional) {
    for (int source_dim = TGeoDimension::UNKNOWN; source_dim <= TGeoDimension::MIXED; ++source_dim) {
        for (int target_dim = TGeoDimension::UNKNOWN; target_dim <= TGeoDimension::MIXED; ++target_dim) {
            auto source = geography();
            auto target = geography();
            source.storage->dimension = static_cast<TGeoDimension::type>(source_dim);
            target.storage->dimension = static_cast<TGeoDimension::type>(target_dim);
            const bool assignable = target_dim == TGeoDimension::UNKNOWN || target_dim == TGeoDimension::MIXED ||
                                    source_dim == target_dim;
            EXPECT_EQ(assignable, is_geo_assignment_compatible(source, target));
            EXPECT_TRUE(is_geo_union_all_compatible(source, target));
            EXPECT_TRUE(is_geo_compute_compatible(source, target)); // Row checks are a later compute contract.
        }
    }
    auto source = geography();
    auto target = geography();
    source.storage->dimension.reset();
    EXPECT_FALSE(is_geo_assignment_compatible(source, target));
    target.storage->dimension.reset();
    EXPECT_TRUE(is_geo_assignment_compatible(source, target));
    EXPECT_TRUE(is_geo_union_all_compatible(source, target));
}

} // namespace
} // namespace starrocks
