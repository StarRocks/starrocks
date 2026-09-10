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

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <gtest/gtest.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <array>
#include <limits>
#include <type_traits>

namespace starrocks {
namespace {

using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::protocol::TCompactProtocol;
using apache::thrift::transport::TMemoryBuffer;

GeoColumnDescriptor geography() {
    return {GeoTypeDescriptor{GEO_LOGICAL_TYPE_GEOGRAPHY, GEO_COORDINATE_SYSTEM_SPHERICAL, GEO_EDGE_ALGORITHM_SPHERICAL,
                              "OGC:CRS84", std::nullopt},
            GeoStorageDescriptor{GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_UNVALIDATED}};
}

template <typename Protocol = TBinaryProtocol, typename Thrift>
Thrift thrift_wire_round_trip(const Thrift& input) {
    auto buffer = std::make_shared<TMemoryBuffer>();
    Protocol protocol(buffer);
    input.write(&protocol);
    Thrift output;
    output.read(&protocol);
    return output;
}

template <typename Descriptor>
void check_round_trip(const Descriptor& expected) {
    EXPECT_EQ(expected, Descriptor::from_thrift(thrift_wire_round_trip(expected.to_thrift())));
    EXPECT_EQ(expected, Descriptor::from_thrift(thrift_wire_round_trip<TCompactProtocol>(expected.to_thrift())));
    auto protobuf = expected.to_protobuf();
    auto parsed = protobuf;
    parsed.Clear();
    ASSERT_TRUE(parsed.ParseFromString(protobuf.SerializeAsString()));
    EXPECT_EQ(expected, Descriptor::from_protobuf(parsed));
    const auto from_thrift = Descriptor::from_thrift(thrift_wire_round_trip(expected.to_thrift()));
    EXPECT_EQ(expected, Descriptor::from_protobuf(from_thrift.to_protobuf()));
    const auto from_proto = Descriptor::from_protobuf(parsed);
    EXPECT_EQ(expected, Descriptor::from_thrift(from_proto.to_thrift()));
}

TEST(GeoTypeDescriptorTest, AllAlgorithmsAndStorageStatesRoundTrip) {
    const std::array algorithms = {GEO_EDGE_ALGORITHM_UNKNOWN,  GEO_EDGE_ALGORITHM_SPHERICAL,
                                   GEO_EDGE_ALGORITHM_VINCENTY, GEO_EDGE_ALGORITHM_THOMAS,
                                   GEO_EDGE_ALGORITHM_ANDOYER,  GEO_EDGE_ALGORITHM_KARNEY,
                                   GEO_EDGE_ALGORITHM_PLANAR};
    for (auto algorithm : algorithms) {
        for (int dim = GEO_DIMENSION_UNKNOWN; dim <= GEO_DIMENSION_MIXED; ++dim) {
            for (int state = GEO_VALIDATION_STATE_UNKNOWN; state <= GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED;
                 ++state) {
                SCOPED_TRACE(::testing::Message() << algorithm << "/" << dim << "/" << state);
                auto desc = geography();
                desc.type.edge_algorithm = algorithm;
                if (algorithm == GEO_EDGE_ALGORITHM_PLANAR) {
                    desc.type.logical_type = GEO_LOGICAL_TYPE_GEOMETRY;
                    desc.type.coordinate_system = GEO_COORDINATE_SYSTEM_CARTESIAN;
                }
                desc.type.srid = 4326;
                desc.storage.dimension = static_cast<PGeoDimension>(dim);
                desc.storage.validation_state = static_cast<PGeoValidationState>(state);
                check_round_trip(desc.type);
                check_round_trip(desc.storage);
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
    EXPECT_EQ(1, GeoTypeDescPB::kLogicalTypeFieldNumber);
    EXPECT_EQ(2, GeoTypeDescPB::kCoordinateSystemFieldNumber);
    EXPECT_EQ(3, GeoTypeDescPB::kEdgeAlgorithmFieldNumber);
    EXPECT_EQ(4, GeoTypeDescPB::kCrsFieldNumber);
    EXPECT_EQ(5, GeoTypeDescPB::kSridFieldNumber);
    EXPECT_EQ(1, GeoStorageDescPB::kEncodingFieldNumber);
    EXPECT_EQ(2, GeoStorageDescPB::kDimensionFieldNumber);
    EXPECT_EQ(3, GeoStorageDescPB::kValidationStateFieldNumber);
    EXPECT_EQ(1, GeoColumnDescPB::kTypeFieldNumber);
    EXPECT_EQ(2, GeoColumnDescPB::kStorageFieldNumber);
}

TEST(GeoTypeDescriptorTest, AlgorithmsRemainDistinctForCompatibility) {
    for (int left = GEO_EDGE_ALGORITHM_SPHERICAL; left <= GEO_EDGE_ALGORITHM_PLANAR; ++left) {
        for (int right = GEO_EDGE_ALGORITHM_SPHERICAL; right <= GEO_EDGE_ALGORITHM_PLANAR; ++right) {
            auto source = geography();
            auto target = geography();
            source.type.edge_algorithm = static_cast<PGeoEdgeAlgorithm>(left);
            target.type.edge_algorithm = static_cast<PGeoEdgeAlgorithm>(right);
            EXPECT_EQ(left == right, is_geo_compute_compatible(source, target));
        }
    }
}

TEST(GeoTypeDescriptorTest, MissingFieldsNormalizeWithoutGuessingSemantics) {
    EXPECT_EQ(GeoTypeDescriptor{}, GeoTypeDescriptor::from_thrift(TGeoTypeDesc{}));
    EXPECT_EQ(GeoTypeDescriptor{}, GeoTypeDescriptor::from_protobuf(GeoTypeDescPB{}));
    EXPECT_EQ(GeoStorageDescriptor{}, GeoStorageDescriptor::from_thrift(TGeoStorageDesc{}));
    EXPECT_EQ(GeoStorageDescriptor{}, GeoStorageDescriptor::from_protobuf(GeoStorageDescPB{}));
    EXPECT_EQ(GeoColumnDescriptor{}, GeoColumnDescriptor::from_thrift(TGeoColumnDesc{}));
    EXPECT_EQ(GeoColumnDescriptor{}, GeoColumnDescriptor::from_protobuf(GeoColumnDescPB{}));
    check_round_trip(GeoColumnDescriptor{});

    TGeoTypeDesc type;
    type.__set_logical_type(TGeoLogicalType::UNKNOWN);
    type.__set_coordinate_system(TGeoCoordinateSystem::UNKNOWN);
    type.__set_edge_algorithm(TGeoEdgeAlgorithm::UNKNOWN);
    type.__set_crs("");
    EXPECT_EQ(GeoTypeDescriptor{}, GeoTypeDescriptor::from_thrift(type));
    const auto normalized = GeoTypeDescriptor{}.to_protobuf();
    EXPECT_EQ(GEO_LOGICAL_TYPE_UNKNOWN, normalized.logical_type());
    EXPECT_EQ(GEO_EDGE_ALGORITHM_UNKNOWN, normalized.edge_algorithm());
    EXPECT_TRUE(normalized.crs().empty());
    EXPECT_FALSE(normalized.has_srid());

    // Unset Thrift fields are not active, even if their backing values were modified.
    TGeoTypeDesc unset;
    unset.logical_type = TGeoLogicalType::GEOGRAPHY;
    unset.crs = "OGC:CRS84";
    EXPECT_EQ(GeoTypeDescriptor{}, GeoTypeDescriptor::from_thrift(unset));
    TGeoColumnDesc column;
    column.__set_type(type);
    column.__set_storage(TGeoStorageDesc{});
    EXPECT_EQ(GeoColumnDescriptor{}, GeoColumnDescriptor::from_thrift(column));
    EXPECT_FALSE(is_geo_compute_compatible({}, {}));
}

TEST(GeoTypeDescriptorTest, SridRetainsMeaningfulPresence) {
    auto desc = geography();
    check_round_trip(desc);
    EXPECT_FALSE(desc.type.to_protobuf().has_srid());
    EXPECT_FALSE(desc.type.to_thrift().__isset.srid);
    desc.type.srid = 0;
    check_round_trip(desc);
    EXPECT_TRUE(desc.type.to_protobuf().has_srid());
    EXPECT_TRUE(desc.type.to_thrift().__isset.srid);
    EXPECT_NE(desc, geography());
}

// Construct an unknown enum on the wire without invoking a generated enum setter.
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

std::string protobuf_enum_wire(int field, int32_t value) {
    std::string bytes;
    {
        google::protobuf::io::StringOutputStream stream(&bytes);
        google::protobuf::io::CodedOutputStream output(&stream);
        output.WriteTag(field << 3);
        output.WriteVarint64(static_cast<uint64_t>(static_cast<int64_t>(value)));
    }
    return bytes;
}

TEST(GeoTypeDescriptorTest, WireFieldsUseDeclaredEnums) {
    static_assert(std::is_same_v<decltype(TGeoTypeDesc{}.logical_type), TGeoLogicalType::type>);
    static_assert(std::is_same_v<decltype(GeoTypeDescPB{}.logical_type()), PGeoLogicalType>);
    static_assert(std::is_same_v<decltype(TGeoTypeDesc{}.coordinate_system), TGeoCoordinateSystem::type>);
    static_assert(std::is_same_v<decltype(GeoTypeDescPB{}.coordinate_system()), PGeoCoordinateSystem>);
    static_assert(std::is_same_v<decltype(TGeoTypeDesc{}.edge_algorithm), TGeoEdgeAlgorithm::type>);
    static_assert(std::is_same_v<decltype(GeoTypeDescPB{}.edge_algorithm()), PGeoEdgeAlgorithm>);
    static_assert(std::is_same_v<decltype(TGeoStorageDesc{}.encoding), TGeoEncoding::type>);
    static_assert(std::is_same_v<decltype(GeoStorageDescPB{}.encoding()), PGeoEncoding>);
    static_assert(std::is_same_v<decltype(TGeoStorageDesc{}.dimension), TGeoDimension::type>);
    static_assert(std::is_same_v<decltype(GeoStorageDescPB{}.dimension()), PGeoDimension>);
    static_assert(std::is_same_v<decltype(TGeoStorageDesc{}.validation_state), TGeoValidationState::type>);
    static_assert(std::is_same_v<decltype(GeoStorageDescPB{}.validation_state()), PGeoValidationState>);
}

constexpr std::array unknown_values = {127, -1, std::numeric_limits<int32_t>::max(),
                                       std::numeric_limits<int32_t>::min()};

TEST(GeoTypeDescriptorTest, UnknownThriftEnumsNormalizeToUnknown) {
    for (int32_t value : unknown_values) {
        for (int16_t field = 1; field <= 3; ++field) {
            SCOPED_TRACE(::testing::Message() << field << "/" << value);
            auto type_wire = thrift_enum_wire<TGeoTypeDesc>(field, value);
            type_wire.__set_crs("OGC:CRS84");
            type_wire.__set_srid(4326);
            const auto storage_wire = thrift_enum_wire<TGeoStorageDesc>(field, value);
            const auto type = GeoTypeDescriptor::from_thrift(type_wire);
            const auto storage = GeoStorageDescriptor::from_thrift(storage_wire);
            GeoTypeDescriptor expected;
            expected.crs = "OGC:CRS84";
            expected.srid = 4326;
            EXPECT_EQ(expected, type);
            EXPECT_EQ(GeoStorageDescriptor{}, storage);
            check_round_trip(type);
            check_round_trip(storage);
            TGeoColumnDesc column;
            column.__set_type(type_wire);
            column.__set_storage(storage_wire);
            const GeoColumnDescriptor normalized{expected, {}};
            EXPECT_EQ(normalized, GeoColumnDescriptor::from_thrift(column));
            check_round_trip(normalized);
            EXPECT_FALSE(is_geo_compute_compatible(normalized, normalized));
        }
    }
}

TEST(GeoTypeDescriptorTest, UnknownProtobufWireEnumsUseUnknownDefault) {
    for (int32_t value : unknown_values) {
        for (int field = 1; field <= 3; ++field) {
            SCOPED_TRACE(::testing::Message() << field << "/" << value);
            GeoTypeDescPB type;
            GeoStorageDescPB storage;
            ASSERT_TRUE(type.ParseFromString(protobuf_enum_wire(field, value)));
            ASSERT_TRUE(storage.ParseFromString(protobuf_enum_wire(field, value)));
            // Closed-enum parsing leaves the field unset; its declared default is UNKNOWN.
            EXPECT_FALSE(type.GetReflection()->HasField(type, type.GetDescriptor()->FindFieldByNumber(field)));
            EXPECT_FALSE(storage.GetReflection()->HasField(storage, storage.GetDescriptor()->FindFieldByNumber(field)));
            EXPECT_EQ(1, type.unknown_fields().field_count());
            EXPECT_EQ(1, storage.unknown_fields().field_count());
            EXPECT_EQ(GeoTypeDescriptor{}, GeoTypeDescriptor::from_protobuf(type));
            EXPECT_EQ(GeoStorageDescriptor{}, GeoStorageDescriptor::from_protobuf(storage));
            GeoColumnDescPB column;
            *column.mutable_type() = type;
            *column.mutable_storage() = storage;
            EXPECT_EQ(GeoColumnDescriptor{}, GeoColumnDescriptor::from_protobuf(column));
            check_round_trip(GeoColumnDescriptor::from_protobuf(column));
            // Internal descriptors are normalized metadata, not an unknown-field relay.
            EXPECT_TRUE(GeoTypeDescriptor::from_protobuf(type).to_protobuf().unknown_fields().empty());
            EXPECT_TRUE(GeoStorageDescriptor::from_protobuf(storage).to_protobuf().unknown_fields().empty());
        }
    }
}

TEST(GeoTypeDescriptorTest, ProtobufKnownEnumsAreNotOverriddenByUnknownFields) {
    const auto expected = geography();
    for (int32_t value : unknown_values) {
        for (int field = 1; field <= 3; ++field) {
            for (bool known_first : {false, true}) {
                SCOPED_TRACE(::testing::Message() << field << "/" << value << "/" << known_first);
                const auto unknown = protobuf_enum_wire(field, value);
                const auto type_bytes = expected.type.to_protobuf().SerializeAsString();
                const auto storage_bytes = expected.storage.to_protobuf().SerializeAsString();
                GeoTypeDescPB type;
                GeoStorageDescPB storage;
                ASSERT_TRUE(type.ParseFromString(known_first ? type_bytes + unknown : unknown + type_bytes));
                ASSERT_TRUE(storage.ParseFromString(known_first ? storage_bytes + unknown : unknown + storage_bytes));
                EXPECT_EQ(1, type.unknown_fields().field_count());
                EXPECT_EQ(1, storage.unknown_fields().field_count());
                EXPECT_EQ(expected.type, GeoTypeDescriptor::from_protobuf(type));
                EXPECT_EQ(expected.storage, GeoStorageDescriptor::from_protobuf(storage));
            }
        }
    }
}

TEST(GeoTypeDescriptorTest, CallerChoosesSupportedAlgorithm) {
    auto input = geography().type.to_protobuf();
    input.set_edge_algorithm(GEO_EDGE_ALGORITHM_KARNEY);
    const auto desc = GeoTypeDescriptor::from_protobuf(input);
    // A declared algorithm survives conversion, even if this caller only supports SPHERICAL.
    const auto supports_algorithm = [](const GeoTypeDescriptor& type) {
        return type.edge_algorithm == GEO_EDGE_ALGORITHM_SPHERICAL;
    };
    EXPECT_FALSE(supports_algorithm(desc));
    EXPECT_EQ(GEO_EDGE_ALGORITHM_KARNEY, desc.to_protobuf().edge_algorithm());
}

TEST(GeoTypeDescriptorTest, ValidationStateAndParsedSridAreNotTypeIdentity) {
    auto source = geography();
    for (int state = GEO_VALIDATION_STATE_UNKNOWN; state <= GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED; ++state) {
        auto target = source;
        target.storage.validation_state = static_cast<PGeoValidationState>(state);
        target.type.srid = 4326;
        EXPECT_NE(source, target);
        EXPECT_TRUE(is_geo_semantically_compatible(source.type, target.type));
        EXPECT_TRUE(is_geo_compute_compatible(source, target));
    }
}

TEST(GeoTypeDescriptorTest, SemanticMismatchIsNotImplicitlyConverted) {
    const auto source = geography();
    for (int field = 0; field < 4; ++field) {
        auto target = source;
        switch (field) {
        case 0:
            target.type.logical_type = GEO_LOGICAL_TYPE_GEOMETRY;
            break;
        case 1:
            target.type.coordinate_system = GEO_COORDINATE_SYSTEM_CARTESIAN;
            break;
        case 2:
            target.type.edge_algorithm = GEO_EDGE_ALGORITHM_KARNEY;
            break;
        case 3:
            target.type.crs = "EPSG:4326";
            break; // Not an axis-order-safe CRS84 alias.
        }
        EXPECT_FALSE(is_geo_compute_compatible(source, target));
    }
    EXPECT_FALSE(is_geo_compute_compatible({}, {}));
    auto incomplete = source;
    incomplete.type.crs.clear();
    EXPECT_FALSE(is_geo_compute_compatible(incomplete, incomplete));
    incomplete = source;
    incomplete.type.edge_algorithm = GEO_EDGE_ALGORITHM_UNKNOWN;
    EXPECT_FALSE(is_geo_compute_compatible(incomplete, incomplete));
}

TEST(GeoTypeDescriptorTest, EncodingDoesNotAffectSemanticComparison) {
    const auto source = geography();
    auto target = source;
    target.storage.encoding = GEO_ENCODING_UNKNOWN;
    EXPECT_NE(source, target);
    EXPECT_TRUE(is_geo_compute_compatible(source, target));
    target.storage = GeoStorageDescriptor{};
    EXPECT_TRUE(is_geo_compute_compatible(source, target));
}

TEST(GeoTypeDescriptorTest, DimensionDoesNotAffectSemanticComparison) {
    for (int source_dim = GEO_DIMENSION_UNKNOWN; source_dim <= GEO_DIMENSION_MIXED; ++source_dim) {
        for (int target_dim = GEO_DIMENSION_UNKNOWN; target_dim <= GEO_DIMENSION_MIXED; ++target_dim) {
            auto source = geography();
            auto target = geography();
            source.storage.dimension = static_cast<PGeoDimension>(source_dim);
            target.storage.dimension = static_cast<PGeoDimension>(target_dim);
            EXPECT_EQ(source_dim == target_dim, source == target);
            EXPECT_TRUE(is_geo_semantically_compatible(source.type, target.type));
            // Semantic comparison neither derives a result dimension nor checks row dimensions.
            EXPECT_TRUE(is_geo_compute_compatible(source, target));
        }
    }
}

} // namespace
} // namespace starrocks
