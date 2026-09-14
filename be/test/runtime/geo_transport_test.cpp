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

#include <chrono>
#include <iostream>

#include "base/coding.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/geo_column.h"
#include "column/nullable_column.h"
#include "column/serde/column_array_serde.h"
#include "runtime/serde/protobuf_chunk_serde.h"

namespace starrocks::serde {
namespace {

GeoColumnDescriptor descriptor() {
    return {{GEO_LOGICAL_TYPE_GEOGRAPHY, GEO_COORDINATE_SYSTEM_SPHERICAL, GEO_EDGE_ALGORITHM_SPHERICAL, "OGC:CRS84",
             4326},
            {GEO_ENCODING_WKB, GEO_DIMENSION_MIXED, GEO_VALIDATION_STATE_UNVALIDATED}};
}

std::string point() {
    const uint8_t bytes[] = {1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xf0, 0x3f, 0, 0, 0, 0, 0, 0, 0, 0x40};
    return {reinterpret_cast<const char*>(bytes), sizeof(bytes)};
}

std::vector<uint8_t> serialize_geo(const GeoColumn& column) {
    std::vector<uint8_t> bytes(column.serialized_column_size());
    auto result = column.serialize_column(bytes.data());
    EXPECT_TRUE(result.ok()) << result.status();
    return bytes;
}

} // namespace

TEST(GeoTransportTest, ChunkRoundTripAndOwnership) {
    const auto desc = descriptor();
    const auto type = TypeDescriptor::create_geo_type(TYPE_GEOGRAPHY, desc.type);
    for (int shape = 0; shape < 5; ++shape) {
        for (int level : {0, 7}) {
            auto geo = GeoColumn::create(desc);
            geo->append_wkb(Slice(point()));
            geo->append_wkb(Slice(std::string("\1\7\0\0\0\0\0\0\0", 9)));
            ASSERT_TRUE(geo->inspect_wkb(0).ok());
            ColumnPtr column = std::move(geo);
            if (shape == 1 || shape == 3 || shape == 4) {
                auto nulls = NullColumn::create(2, shape >= 3 ? 1 : 0);
                nulls->get_data()[0] = 1;
                column = NullableColumn::create(std::move(column), std::move(nulls));
            }
            if (shape == 2 || shape == 3) column = ConstColumn::create(std::move(column), 128);
            Chunk source;
            source.append_column(column, 1);
            ProtobufChunkMeta meta;
            meta.types = {type};
            meta.is_nulls = {column->is_nullable()};
            meta.is_consts = {column->is_constant()};
            meta.slot_id_to_index[1] = 0;
            auto context = EncodeContext::get_encode_context_shared_ptr(1, level);
            // First and subsequent chunks keep the existing chunk protocol version.
            for (bool first : {true, false}) {
                auto encoded = first ? ProtobufChunkSerde::serialize(source, context)
                                     : ProtobufChunkSerde::serialize_without_meta(source, context);
                ASSERT_TRUE(encoded.ok()) << encoded.status();
                context->set_encode_levels_in_pb(&*encoded);
                EXPECT_EQ(1, decode_fixed32_le(reinterpret_cast<const uint8_t*>(encoded->data().data())));
                ProtobufChunkDeserializer reader(meta, &*encoded, level);
                auto decoded = reader.deserialize(encoded->data());
                ASSERT_TRUE(decoded.ok()) << decoded.status();
                auto out = decoded->get_column_by_slot_id(1);
                EXPECT_EQ(column->size(), out->size());
                EXPECT_EQ(column->is_constant(), out->is_constant());
                EXPECT_EQ(column->is_nullable(), out->is_nullable());
                if (shape == 3) {
                    // Constant NULL is the legacy Boolean placeholder; semantic identity is in meta.
                    EXPECT_TRUE(out->only_null());
                    EXPECT_EQ(type, meta.types[0]);
                    continue;
                }
                const auto* restored = down_cast<const GeoColumn*>(ColumnHelper::get_data_column(out.get()));
                EXPECT_EQ(desc, restored->descriptor());
                EXPECT_FALSE(restored->has_wkb_cache());
                encoded->mutable_data()->assign(encoded->data().size(), '\xff');
                EXPECT_EQ(point(), restored->get_wkb(0).to_string());
                for (size_t i = 0; i < out->size(); ++i) EXPECT_EQ(column->is_null(i), out->is_null(i));
            }
        }
    }
}

TEST(GeoTransportTest, RejectsTruncationWithoutChangingDestination) {
    auto source = GeoColumn::create(descriptor());
    source->append_wkb(Slice(point()));
    auto bytes = serialize_geo(*source);
    auto target = GeoColumn::create(descriptor());
    target->append_wkb(Slice("unchanged"));
    for (size_t length = 0; length < bytes.size(); ++length) {
        EXPECT_FALSE(target->deserialize_column(bytes.data(), bytes.data() + length).ok()) << length;
        EXPECT_EQ("unchanged", target->get_wkb(0).to_string());
    }
    for (size_t field : {size_t{4}, size_t{8}, size_t{12}}) {
        auto corrupt = bytes;
        encode_fixed32_le(corrupt.data() + field, UINT32_MAX);
        EXPECT_FALSE(target->deserialize_column(corrupt.data(), corrupt.data() + corrupt.size()).ok());
    }
    auto corrupt = bytes;
    encode_fixed32_le(corrupt.data() + corrupt.size() - 4, UINT32_MAX);
    EXPECT_FALSE(target->deserialize_column(corrupt.data(), corrupt.data() + corrupt.size()).ok());
    EXPECT_EQ("unchanged", target->get_wkb(0).to_string());
}

TEST(GeoTransportTest, RejectsDescriptorMismatchUnknownEnumsAndVersions) {
    auto source = GeoColumn::create(descriptor());
    source->append_wkb(Slice(point()));
    auto bytes = serialize_geo(*source);
    auto other = descriptor();
    other.type.crs = "EPSG:3857";
    auto target = GeoColumn::create(other);
    EXPECT_FALSE(target->deserialize_column(bytes.data(), bytes.data() + bytes.size()).ok());
    target = GeoColumn::create(descriptor());
    auto corrupt = bytes;
    encode_fixed32_le(corrupt.data(), 99);
    EXPECT_TRUE(
            target->deserialize_column(corrupt.data(), corrupt.data() + corrupt.size()).status().is_not_supported());
    // A proto2 unknown enum can hide behind the default accessor. Reject, never normalize it.
    GeoColumnDescPB pb = descriptor().to_protobuf();
    pb.mutable_type()->mutable_unknown_fields()->AddVarint(3, 99);
    auto metadata = pb.SerializeAsString();
    uint32_t old_size = decode_fixed32_le(bytes.data() + 4);
    corrupt.assign(bytes.begin(), bytes.begin() + 16);
    encode_fixed32_le(corrupt.data() + 4, metadata.size());
    corrupt.insert(corrupt.end(), metadata.begin(), metadata.end());
    corrupt.insert(corrupt.end(), bytes.begin() + 16 + old_size, bytes.end());
    EXPECT_FALSE(target->deserialize_column(corrupt.data(), corrupt.data() + corrupt.size()).ok());
    EXPECT_EQ(0, target->size());
}

TEST(GeoTransportTest, OrdinaryWireFormatAndUnsupportedPaths) {
    auto binary = BinaryColumn::create();
    binary->append(Slice(point()));
    Chunk chunk;
    chunk.append_column(std::move(binary), 1);
    auto encoded = ProtobufChunkSerde::serialize(chunk);
    ASSERT_TRUE(encoded.ok()) << encoded.status();
    EXPECT_EQ(1, decode_fixed32_le(reinterpret_cast<const uint8_t*>(encoded->data().data())));

    auto geo = GeoColumn::create(descriptor());
    geo->append_wkb(Slice(point()));
    std::vector<uint8_t> bytes(geo->serialized_column_size());
    EXPECT_TRUE(ColumnArraySerde::serialize(*geo, bytes.data()).status().is_not_supported());
    auto geometry_desc = descriptor();
    geometry_desc.type.logical_type = GEO_LOGICAL_TYPE_GEOMETRY;
    auto geometry = GeoColumn::create(geometry_desc);
    EXPECT_TRUE(geometry->serialize_column(bytes.data()).status().is_not_supported());
    ProtobufChunkMeta meta;
    ProtobufChunkDeserializer reader(meta);
    for (size_t size = 0; size < 8; ++size) EXPECT_FALSE(reader.deserialize(std::string(size, 0)).ok());
    auto invalid = encoded->data();
    encode_fixed32_le(reinterpret_cast<uint8_t*>(invalid.data()), 99);
    EXPECT_FALSE(reader.deserialize(invalid).ok());
}

TEST(GeoTransportTest, StorageMetadataAndOpaquePayload) {
    for (int edge = GEO_EDGE_ALGORITHM_SPHERICAL; edge <= GEO_EDGE_ALGORITHM_KARNEY; ++edge) {
        for (int dimension = GEO_DIMENSION_UNKNOWN; dimension <= GEO_DIMENSION_MIXED; ++dimension) {
            for (int validation = GEO_VALIDATION_STATE_UNKNOWN;
                 validation <= GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED; ++validation) {
                auto desc = descriptor();
                desc.type.edge_algorithm = static_cast<GeoEdgeAlgorithmPB>(edge);
                desc.type.srid.reset();
                desc.storage.dimension = static_cast<GeoDimensionPB>(dimension);
                desc.storage.validation_state = static_cast<GeoValidationStatePB>(validation);
                auto source = GeoColumn::create(desc);
                source->append_wkb(Slice("opaque WKB: transport does not parse"));
                auto bytes = serialize_geo(*source);
                auto type = TypeDescriptor::create_geo_type(TYPE_GEOGRAPHY, desc.type);
                auto target = ColumnHelper::create_column(type, false);
                type.geo_type->crs = "changed after creation";
                auto* geo = down_cast<GeoColumn*>(target.get());
                ASSERT_TRUE(geo->deserialize_column(bytes.data(), bytes.data() + bytes.size()).ok());
                EXPECT_EQ(desc, geo->descriptor());
                EXPECT_EQ(source->get_wkb(0), geo->get_wkb(0));
                EXPECT_FALSE(geo->has_wkb_cache());
            }
        }
    }
}

TEST(GeoTransportTest, EmptyAndAllFamilies) {
    auto source = GeoColumn::create(descriptor());
    auto target = GeoColumn::create(descriptor());
    auto bytes = serialize_geo(*source);
    ASSERT_TRUE(target->deserialize_column(bytes.data(), bytes.data() + bytes.size()).ok());
    EXPECT_EQ(0, target->size());
    source->append_wkb(Slice(point()));
    for (uint8_t family = 2; family <= 7; ++family) {
        std::string empty(9, 0);
        empty[0] = 1;
        empty[1] = family;
        source->append_wkb(Slice(empty));
    }
    bytes = serialize_geo(*source);
    ASSERT_TRUE(target->deserialize_column(bytes.data(), bytes.data() + bytes.size()).ok());
    ASSERT_EQ(7, target->size());
    for (size_t row = 0; row < source->size(); ++row) EXPECT_EQ(source->get_wkb(row), target->get_wkb(row));
}

TEST(GeoTransportTest, NullableEncodingAndMalformedRowCounts) {
    auto desc = descriptor();
    auto geo = GeoColumn::create(desc);
    geo->append_wkb(Slice(point()));
    geo->assign(512, 0);
    auto nulls = NullColumn::create(512, 1);
    nulls->get_data()[0] = 0;
    Chunk chunk;
    chunk.append_column(NullableColumn::create(std::move(geo), std::move(nulls)), 1);
    ProtobufChunkMeta meta;
    meta.types = {TypeDescriptor::create_geo_type(TYPE_GEOGRAPHY, desc.type)};
    meta.is_nulls = {true};
    meta.is_consts = {false};
    meta.slot_id_to_index[1] = 0;
    for (int level : {0, 2, 6}) {
        auto context = EncodeContext::get_encode_context_shared_ptr(1, level);
        auto wire = ProtobufChunkSerde::serialize(chunk, context);
        ASSERT_TRUE(wire.ok()) << wire.status();
        context->set_encode_levels_in_pb(&*wire);
        ProtobufChunkDeserializer reader(meta, &*wire, level);
        auto restored = reader.deserialize(wire->data());
        ASSERT_TRUE(restored.ok()) << restored.status();
        EXPECT_FALSE(restored->columns()[0]->is_null(0));
        EXPECT_TRUE(restored->columns()[0]->is_null(511));
    }
    auto wire = ProtobufChunkSerde::serialize(chunk);
    ASSERT_TRUE(wire.ok());
    auto corrupt = wire->data();
    // Shorten only the null map, retaining an intact two-part column payload.
    encode_fixed32_le(reinterpret_cast<uint8_t*>(corrupt.data()) + 8, 511);
    corrupt.erase(12, 1);
    ProtobufChunkDeserializer reader(meta);
    EXPECT_TRUE(reader.deserialize(corrupt).status().is_corruption());
}

TEST(GeoTransportTest, LegacyConstantNullInOrdinaryAndMixedChunks) {
    for (bool mixed : {false, true}) {
        Chunk chunk;
        chunk.append_column(ColumnHelper::create_const_null_column(16), 1);
        ProtobufChunkMeta meta;
        meta.types = {TypeDescriptor(TYPE_BIGINT)};
        meta.is_nulls = {true};
        meta.is_consts = {true};
        meta.slot_id_to_index[1] = 0;
        if (mixed) {
            auto geo = GeoColumn::create(descriptor());
            geo->append_wkb(Slice(point()));
            geo->assign(16, 0);
            chunk.append_column(std::move(geo), 2);
            meta.types.push_back(TypeDescriptor::create_geo_type(TYPE_GEOGRAPHY, descriptor().type));
            meta.is_nulls.push_back(false);
            meta.is_consts.push_back(false);
            meta.slot_id_to_index[2] = 1;
        }
        auto wire = ProtobufChunkSerde::serialize(chunk);
        ASSERT_TRUE(wire.ok()) << wire.status();
        EXPECT_EQ(1, decode_fixed32_le(reinterpret_cast<const uint8_t*>(wire->data().data())));
        ProtobufChunkDeserializer reader(meta);
        auto restored = reader.deserialize(wire->data());
        ASSERT_TRUE(restored.ok()) << restored.status();
        EXPECT_TRUE(restored->get_column_by_slot_id(1)->only_null());
        EXPECT_EQ(16, restored->num_rows());
    }
}

TEST(GeoTransportTest, ConstantNullRetainsLegacyWireBytes) {
    for (size_t rows : {size_t{0}, size_t{16}}) {
        auto geo = GeoColumn::create(descriptor());
        geo->append_default();
        auto typed = ConstColumn::create(NullableColumn::create(std::move(geo), NullColumn::create(1, 1)), rows);
        auto legacy = ColumnHelper::create_const_null_column(rows);
        Chunk typed_chunk;
        typed_chunk.append_column(std::move(typed), 1);
        Chunk legacy_chunk;
        legacy_chunk.append_column(std::move(legacy), 1);
        auto typed_wire = ProtobufChunkSerde::serialize(typed_chunk);
        auto legacy_wire = ProtobufChunkSerde::serialize(legacy_chunk);
        ASSERT_TRUE(typed_wire.ok()) << typed_wire.status();
        ASSERT_TRUE(legacy_wire.ok()) << legacy_wire.status();
        EXPECT_EQ(legacy_wire->SerializeAsString(), typed_wire->SerializeAsString());
        EXPECT_EQ(rows, decode_fixed64_le(reinterpret_cast<const uint8_t*>(typed_wire->data().data()) + 8));
    }
}

TEST(GeoTransportTest, SchemaReaderKeepsVersionOne) {
    auto data = Int32Column::create();
    data->append(42);
    Chunk chunk;
    chunk.append_column(std::move(data), 1);
    auto wire = ProtobufChunkSerde::serialize_without_meta(chunk);
    ASSERT_TRUE(wire.ok());
    Schema schema;
    schema.append(std::make_shared<Field>(0, "value", TYPE_INT, false));
    auto restored = ProtobufChunkSerde::deserialize_with_schema(schema, wire->data());
    ASSERT_TRUE(restored.ok()) << restored.status();
    EXPECT_EQ(42, restored->columns()[0]->get(0).get_int32());
    auto invalid = wire->data();
    encode_fixed32_le(reinterpret_cast<uint8_t*>(invalid.data()), 2);
    EXPECT_TRUE(ProtobufChunkSerde::deserialize_with_schema(schema, invalid).status().is_corruption());
}

TEST(GeoTransportTest, UnversionedTypedConstantNullRemainsUnchanged) {
    auto data = Int64Column::create(1, 42);
    auto column = ConstColumn::create(NullableColumn::create(std::move(data), NullColumn::create(1, 1)), 16);
    std::vector<uint8_t> bytes(ColumnArraySerde::max_serialized_size(*column));
    auto written = ColumnArraySerde::serialize(*column, bytes.data());
    ASSERT_TRUE(written.ok()) << written.status();
    auto restored = column->clone_empty();
    auto read = ColumnArraySerde::deserialize(bytes.data(), *written, restored.get());
    ASSERT_TRUE(read.ok()) << read.status();
    EXPECT_EQ(16, restored->size());
    EXPECT_TRUE(restored->only_null());
    auto* values = down_cast<const Int64Column*>(ColumnHelper::get_data_column(restored.get()));
    EXPECT_EQ(42, values->get_data()[0]);
}

TEST(GeoTransportTest, RejectsConflictingReceiverPrimitive) {
    auto column = GeoColumn::create(descriptor());
    column->append_wkb(Slice(point()));
    Chunk chunk;
    chunk.append_column(std::move(column), 1);
    auto wire = ProtobufChunkSerde::serialize(chunk);
    ASSERT_TRUE(wire.ok());
    ProtobufChunkMeta meta;
    meta.types = {TypeDescriptor::create_geo_type(TYPE_GEOGRAPHY, descriptor().type)};
    meta.types[0].type = TYPE_GEOMETRY;
    meta.is_nulls = {false};
    meta.is_consts = {false};
    meta.slot_id_to_index[1] = 0;
    ProtobufChunkDeserializer reader(meta);
    EXPECT_TRUE(reader.deserialize(wire->data()).status().is_corruption());
}

// Opt-in reproducible microbenchmark; report timings, never enforce machine-dependent thresholds.
TEST(GeoTransportTest, DISABLED_TransportBenchmark) {
    auto geo = GeoColumn::create(descriptor());
    auto binary = BinaryColumn::create();
    auto payload = point();
    for (int i = 0; i < 4096; ++i) {
        geo->append_wkb(Slice(payload));
        binary->append(Slice(payload));
    }
    for (const Column* column : {static_cast<const Column*>(geo.get()), static_cast<const Column*>(binary.get())}) {
        const auto size = ColumnArraySerde::max_serialized_size(*column);
        std::vector<uint8_t> bytes(size);
        auto target = column->clone_empty();
        constexpr int iterations = 1000;
        auto start = std::chrono::steady_clock::now();
        for (int i = 0; i < iterations; ++i) {
            ASSERT_TRUE(ColumnArraySerde::serialize(*column, bytes.data(), false, 0, true).ok());
        }
        auto serialized = std::chrono::steady_clock::now();
        for (int i = 0; i < iterations; ++i) {
            ASSERT_TRUE(ColumnArraySerde::deserialize(bytes.data(), bytes.data() + bytes.size(), target.get()).ok());
        }
        auto finished = std::chrono::steady_clock::now();
        std::cout << column->get_name() << ": rows=4096 bytes=" << size << " iterations=" << iterations
                  << " serialize_us="
                  << std::chrono::duration_cast<std::chrono::microseconds>(serialized - start).count()
                  << " deserialize_us="
                  << std::chrono::duration_cast<std::chrono::microseconds>(finished - serialized).count() << '\n';
    }
}

} // namespace starrocks::serde
