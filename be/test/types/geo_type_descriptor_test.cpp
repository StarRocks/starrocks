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

#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <gtest/gtest.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <array>
#include <limits>

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
    for (int left = GEO_EDGE_ALGORITHM_SPHERICAL; left <= GEO_EDGE_ALGORITHM_PLANAR; ++left) {
        for (int right = GEO_EDGE_ALGORITHM_SPHERICAL; right <= GEO_EDGE_ALGORITHM_PLANAR; ++right) {
            auto source = geography();
            auto target = geography();
            source.type.edge_algorithm = static_cast<PGeoEdgeAlgorithm>(left);
            target.type.edge_algorithm = static_cast<PGeoEdgeAlgorithm>(right);
            EXPECT_EQ(left == right, is_geo_compute_compatible(source, target));
            EXPECT_EQ(left == right, is_geo_assignment_compatible(source, target));
            EXPECT_EQ(left == right, is_geo_union_all_compatible(source, target));
        }
    }
}

TEST(GeoTypeDescriptorTest, MissingFieldsNormalizeWithoutGuessingSemantics) {
    EXPECT_EQ(GeoTypeDescriptor{}, GeoTypeDescriptor::from_thrift(TGeoTypeDesc{}));
    EXPECT_EQ(GeoTypeDescriptor{}, GeoTypeDescriptor::from_protobuf(PGeoTypeDesc{}));
    EXPECT_EQ(GeoStorageDescriptor{}, GeoStorageDescriptor::from_thrift(TGeoStorageDesc{}));
    EXPECT_EQ(GeoStorageDescriptor{}, GeoStorageDescriptor::from_protobuf(PGeoStorageDesc{}));
    EXPECT_EQ(GeoColumnDescriptor{}, GeoColumnDescriptor::from_thrift(TGeoColumnDesc{}));
    EXPECT_EQ(GeoColumnDescriptor{}, GeoColumnDescriptor::from_protobuf(PGeoColumnDesc{}));
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

// Same numeric wire encoding as the original enum fields, with no generated setter.
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

template <typename Message>
int32_t enum_number(const Message& message, int field) {
    return message.GetReflection()->GetInt32(message, message.GetDescriptor()->FindFieldByNumber(field));
}

constexpr std::array unknown_values = {127, -1, std::numeric_limits<int32_t>::max(),
                                       std::numeric_limits<int32_t>::min()};

TEST(GeoTypeDescriptorTest, UnknownNumbersSurviveBothTransports) {
    for (int32_t value : unknown_values) {
        for (int16_t field = 1; field <= 3; ++field) {
            SCOPED_TRACE(::testing::Message() << field << "/" << value);
            const auto type = GeoTypeDescriptor::from_thrift(thrift_enum_wire<TGeoTypeDesc>(field, value));
            const auto storage = GeoStorageDescriptor::from_thrift(thrift_enum_wire<TGeoStorageDesc>(field, value));
            EXPECT_EQ(value, enum_number(type.to_protobuf(), field));
            EXPECT_EQ(value, enum_number(storage.to_protobuf(), field));
            check_round_trip(type);
            check_round_trip(storage);
            check_round_trip(GeoColumnDescriptor{type, storage});

            PGeoTypeDesc type_pb;
            PGeoStorageDesc storage_pb;
            ASSERT_TRUE(type_pb.ParseFromString(protobuf_enum_wire(field, value)));
            ASSERT_TRUE(storage_pb.ParseFromString(protobuf_enum_wire(field, value)));
            EXPECT_TRUE(type_pb.unknown_fields().empty());
            EXPECT_TRUE(storage_pb.unknown_fields().empty());
            EXPECT_EQ(type, GeoTypeDescriptor::from_protobuf(type_pb));
            EXPECT_EQ(storage, GeoStorageDescriptor::from_protobuf(storage_pb));
        }
    }
}

TEST(GeoTypeDescriptorTest, DirectProtobufSettersPreserveUnknownNumbers) {
    // Unlike closed-enum setters, int32 setters work identically with/without NDEBUG.
    for (int32_t value : unknown_values) {
        PGeoTypeDesc type;
        type.set_logical_type(value);
        type.set_coordinate_system(value);
        type.set_edge_algorithm(value);
        PGeoStorageDesc storage;
        storage.set_encoding(value);
        storage.set_dimension(value);
        storage.set_validation_state(value);
        const auto type_desc = GeoTypeDescriptor::from_protobuf(type);
        const auto storage_desc = GeoStorageDescriptor::from_protobuf(storage);
        for (int field = 1; field <= 3; ++field) {
            EXPECT_EQ(value, enum_number(type_desc.to_protobuf(), field));
            EXPECT_EQ(value, enum_number(storage_desc.to_protobuf(), field));
        }
        check_round_trip(GeoColumnDescriptor{type_desc, storage_desc});
    }
}

TEST(GeoTypeDescriptorTest, LegacyProto2UnknownFieldSetRemainsReadable) {
    // Model a closed-enum producer: unknown values go into its unknown-field set.
    // After serialization the current int32 schema must recover their exact values.
    google::protobuf::FileDescriptorProto file;
    file.set_name("legacy_geo_test.proto");
    file.set_syntax("proto2");
    auto* enumeration = file.add_enum_type();
    enumeration->set_name("LegacyEnum");
    auto* zero = enumeration->add_value();
    zero->set_name("UNKNOWN");
    zero->set_number(0);
    auto* message = file.add_message_type();
    message->set_name("LegacyGeo");
    for (int i = 1; i <= 3; ++i) {
        auto* field = message->add_field();
        field->set_name("field" + std::to_string(i));
        field->set_number(i);
        field->set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
        field->set_type(google::protobuf::FieldDescriptorProto::TYPE_ENUM);
        field->set_type_name(".LegacyEnum");
    }
    google::protobuf::DescriptorPool pool;
    const auto* schema = pool.BuildFile(file);
    ASSERT_NE(nullptr, schema);
    google::protobuf::DynamicMessageFactory factory(&pool);
    std::unique_ptr<google::protobuf::Message> legacy(factory.GetPrototype(schema->message_type(0))->New());
    for (int32_t value : unknown_values) {
        for (int field = 1; field <= 3; ++field) {
            for (bool known_first : {false, true}) {
                const auto bytes =
                        (known_first ? protobuf_enum_wire(field, 0) : std::string{}) + protobuf_enum_wire(field, value);
                ASSERT_TRUE(legacy->ParseFromString(bytes));
                ASSERT_EQ(1, legacy->GetReflection()->GetUnknownFields(*legacy).field_count());
                const auto relayed = legacy->SerializeAsString();
                PGeoTypeDesc type;
                PGeoStorageDesc storage;
                ASSERT_TRUE(type.ParseFromString(relayed));
                ASSERT_TRUE(storage.ParseFromString(relayed));
                EXPECT_EQ(value, enum_number(GeoTypeDescriptor::from_protobuf(type).to_protobuf(), field));
                EXPECT_EQ(value, enum_number(GeoStorageDescriptor::from_protobuf(storage).to_protobuf(), field));
            }
        }
    }
}

TEST(GeoTypeDescriptorTest, CallerChoosesSupportedAlgorithm) {
    auto input = geography().type.to_protobuf();
    input.set_edge_algorithm(127);
    const auto desc = GeoTypeDescriptor::from_protobuf(input);
    // Conversion preserves the value. The consuming caller decides whether to reject.
    EXPECT_FALSE(PGeoEdgeAlgorithm_IsValid(desc.edge_algorithm));
    EXPECT_EQ(127, desc.to_protobuf().edge_algorithm());
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
        EXPECT_TRUE(is_geo_assignment_compatible(source, target));
        EXPECT_TRUE(is_geo_union_all_compatible(source, target));
    }
    auto target = source;
    target.storage.validation_state = GEO_VALIDATION_STATE_UNKNOWN;
    EXPECT_TRUE(is_geo_assignment_compatible(source, target));
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
        EXPECT_FALSE(is_geo_assignment_compatible(source, target));
        EXPECT_FALSE(is_geo_union_all_compatible(source, target));
    }
    EXPECT_FALSE(is_geo_compute_compatible({}, {}));
    auto incomplete = source;
    incomplete.type.crs.clear();
    EXPECT_FALSE(is_geo_compute_compatible(incomplete, incomplete));
    incomplete = source;
    incomplete.type.edge_algorithm = GEO_EDGE_ALGORITHM_UNKNOWN;
    EXPECT_FALSE(is_geo_compute_compatible(incomplete, incomplete));
}

TEST(GeoTypeDescriptorTest, EncodingAffectsTransportNotComputeIdentity) {
    const auto source = geography();
    auto target = source;
    target.storage.encoding = GEO_ENCODING_UNKNOWN;
    EXPECT_TRUE(is_geo_compute_compatible(source, target));
    EXPECT_FALSE(is_geo_assignment_compatible(source, target));
    EXPECT_FALSE(is_geo_assignment_compatible(target, source));
    EXPECT_FALSE(is_geo_union_all_compatible(source, target));
    target.storage = GeoStorageDescriptor{};
    EXPECT_TRUE(is_geo_compute_compatible(source, target));
    EXPECT_FALSE(is_geo_assignment_compatible(source, target));
}

TEST(GeoTypeDescriptorTest, DimensionGuaranteesAreDirectional) {
    for (int source_dim = GEO_DIMENSION_UNKNOWN; source_dim <= GEO_DIMENSION_MIXED; ++source_dim) {
        for (int target_dim = GEO_DIMENSION_UNKNOWN; target_dim <= GEO_DIMENSION_MIXED; ++target_dim) {
            auto source = geography();
            auto target = geography();
            source.storage.dimension = static_cast<PGeoDimension>(source_dim);
            target.storage.dimension = static_cast<PGeoDimension>(target_dim);
            const bool assignable = target_dim == GEO_DIMENSION_UNKNOWN || target_dim == GEO_DIMENSION_MIXED ||
                                    source_dim == target_dim;
            EXPECT_EQ(assignable, is_geo_assignment_compatible(source, target));
            EXPECT_TRUE(is_geo_union_all_compatible(source, target));
            EXPECT_TRUE(is_geo_compute_compatible(source, target)); // Row checks are a later compute contract.
        }
    }
    auto source = geography();
    auto target = geography();
    source.storage.dimension = GEO_DIMENSION_UNKNOWN;
    EXPECT_FALSE(is_geo_assignment_compatible(source, target));
    target.storage.dimension = GEO_DIMENSION_UNKNOWN;
    EXPECT_TRUE(is_geo_assignment_compatible(source, target));
    EXPECT_TRUE(is_geo_union_all_compatible(source, target));
}

} // namespace
} // namespace starrocks
