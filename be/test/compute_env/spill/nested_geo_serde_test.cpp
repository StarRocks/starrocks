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

#include "base/coding.h"
#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/geo_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/serde/column_array_serde.h"
#include "column/struct_column.h"
#include "compute_env/spill/spiller.h"
#include "io/string_input_stream.h"
#include "runtime/serde/protobuf_chunk_serde.h"

namespace starrocks {
namespace {

enum InputState { kValues, kZeroRows, kEmpty, kNullable, kAllNull, kConstant, kConstantNull };
constexpr int kNoEncoding = 0, kAdaptiveEncoding = 7;

// Exercise ColumnarSerde and the real BlockReader with an in-memory spill block.
class GeoSpillBlock final : public spill::Block, public spill::SpillOutputDataStream {
public:
    explicit GeoSpillBlock(std::string bytes = {}) : bytes(std::move(bytes)) { _size = this->bytes.size(); }
    Status append(const std::vector<Slice>& data) override {
        for (const auto& slice : data) bytes.append(slice.data, slice.size);
        _size = bytes.size();
        return Status::OK();
    }
    Status append(RuntimeState*, const std::vector<Slice>& data, size_t, size_t) override { return append(data); }
    Status flush() override { return Status::OK(); }
    bool is_remote() const override { return false; }
    bool try_acquire_sizes(size_t) override { return true; }
    std::string debug_string() const override { return "geo spill block"; }
    StatusOr<std::unique_ptr<io::InputStreamWrapper>> get_readable() const override {
        return std::make_unique<io::InputStreamWrapper>(std::make_unique<io::StringInputStream>(bytes));
    }
    std::shared_ptr<spill::BlockReader> get_reader(const spill::BlockReaderOptions& options) override {
        class Reader final : public spill::BlockReader {
        public:
            Reader(const spill::Block* block, const spill::BlockReaderOptions& options) : BlockReader(block, options) {}
            std::string debug_string() override { return _block->debug_string(); }
            const spill::Block* block() const override { return _block; }
        };
        return std::make_shared<Reader>(this, options);
    }
    std::string bytes;
};

std::vector<TypeDescriptor> nested_geo_types(const TypeDescriptor& leaf) {
    auto wrap = [](const TypeDescriptor& child) {
        return std::vector<TypeDescriptor>{TypeDescriptor::create_array_type(child),
                                           TypeDescriptor::create_map_type(TypeDescriptor(TYPE_INT), child),
                                           TypeDescriptor::create_struct_type({"g"}, {child})};
    };
    auto types = wrap(leaf);
    for (const auto& inner : wrap(leaf))
        for (auto outer : wrap(inner)) types.push_back(std::move(outer));
    return types;
}

MutableColumnPtr nested_geo_column(const TypeDescriptor& type, const GeoColumnDescriptor& desc, size_t rows,
                                   bool empty) {
    if (type.is_geo_type()) {
        auto geo = GeoColumn::create(desc);
        // POINT (1 2) in both WKB byte orders, and LINESTRING EMPTY.
        const uint8_t little[] = {1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xf0, 0x3f, 0, 0, 0, 0, 0, 0, 0, 0x40};
        const uint8_t big[] = {0, 0, 0, 0, 1, 0x3f, 0xf0, 0, 0, 0, 0, 0, 0, 0x40, 0, 0, 0, 0, 0, 0, 0};
        const Slice values[] = {{reinterpret_cast<const char*>(little), sizeof(little)},
                                {reinterpret_cast<const char*>(big), sizeof(big)},
                                {"\1\2\0\0\0\0\0\0\0", 9}};
        for (size_t row = 0; row < rows; ++row) geo->append_wkb(values[empty ? 2 : row % 3]);
        return geo;
    }
    auto column = ColumnHelper::create_column(type, false);
    auto child_rows = type.type == TYPE_STRUCT ? rows : (empty ? 0 : rows);
    auto child = nested_geo_column(type.children.back(), desc, child_rows, empty);
    Column* target;
    if (type.type == TYPE_STRUCT) {
        target = down_cast<StructColumn*>(column.get())->field_column_raw_ptr(0);
    } else {
        auto offsets = UInt32Column::create(1, 0);
        for (size_t row = 0; row < rows; ++row) offsets->append(empty ? 0 : row + 1);
        if (type.type == TYPE_ARRAY) {
            auto* array = down_cast<ArrayColumn*>(column.get());
            array->offsets_column_raw_ptr()->swap_column(*offsets);
            target = array->elements_column_raw_ptr();
        } else {
            auto* map = down_cast<MapColumn*>(column.get());
            map->offsets_column_raw_ptr()->swap_column(*offsets);
            for (size_t row = 0; row < child_rows; ++row)
                map->keys_column_raw_ptr()->append_datum(Datum(static_cast<int32_t>(row + 1)));
            target = map->values_column_raw_ptr();
        }
    }
    target->append(*child, 0, child_rows);
    if (child_rows > 2) {
        auto* nullable = down_cast<NullableColumn*>(target);
        nullable->null_column_data()[2] = 1;
        nullable->update_has_null();
    }
    return column;
}

ChunkPtr nested_geo_chunk(const TypeDescriptor& type, const GeoColumnDescriptor& desc, InputState state) {
    const bool constant = state == kConstant || state == kConstantNull;
    size_t rows = state == kZeroRows ? 0 : (constant ? 1 : 3);
    auto column = nested_geo_column(type, desc, rows, state == kEmpty || state == kAllNull);
    if (state == kNullable || state == kAllNull) {
        auto nulls = NullColumn::create(rows, state == kAllNull ? 1 : 0);
        if (state == kNullable) nulls->get_data()[1] = 1;
        column = NullableColumn::create(std::move(column), std::move(nulls));
    }
    if (state == kConstant) column = ConstColumn::create(std::move(column), 3);
    if (state == kConstantNull) column = ColumnHelper::create_column(type, true, true, 3);
    // ProjectOperator::push_chunk materializes constant results before downstream exchange/spill.
    if (constant) {
        if (column->only_null()) {
            column = ColumnHelper::create_column(type, true);
            column->append_nulls(3);
        } else {
            auto projected = ColumnHelper::create_column(type, false);
            projected->append(*down_cast<const ConstColumn*>(column.get())->data_column(), 0, 1);
            projected->assign(3, 0);
            column = std::move(projected);
        }
    }
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(column), 1);
    return chunk;
}

const GeoColumn* nested_geo_leaf(const Column* column) {
    column = ColumnHelper::get_data_column(column);
    if (column->is_array()) return nested_geo_leaf(down_cast<const ArrayColumn*>(column)->elements_column().get());
    if (column->is_map()) return nested_geo_leaf(down_cast<const MapColumn*>(column)->values_column().get());
    if (column->is_struct()) return nested_geo_leaf(down_cast<const StructColumn*>(column)->field_column_raw_ptr(0));
    return down_cast<const GeoColumn*>(column);
}

StatusOr<std::string> column_wire(const Column& column) {
    std::string bytes(serde::ColumnArraySerde::max_serialized_size(column), '\0');
    auto* start = reinterpret_cast<uint8_t*>(bytes.data());
    ASSIGN_OR_RETURN(auto end, serde::ColumnArraySerde::serialize(column, start));
    bytes.resize(end - start);
    return bytes;
}

StatusOr<spill::SerdePtr> geo_spill_serde(spill::Spiller& spiller, const ChunkPtr& schema) {
    spiller._chunk_builder.chunk_schema() = std::make_shared<spill::SpilledChunkBuildSchema>();
    spiller._chunk_builder.chunk_schema()->set_schema(schema);
    ASSIGN_OR_RETURN(auto codec, spill::Serde::create_serde(&spiller));
    RETURN_IF_ERROR(codec->prepare());
    return codec;
}

StatusOr<ChunkPB> nested_geo_wire(const ChunkPtr& source, bool spill, int level, bool first = true) {
    if (!spill) {
        auto context = serde::EncodeContext::get_encode_context_shared_ptr(1, level);
        auto wire = first ? serde::ProtobufChunkSerde::serialize(*source, context)
                          : serde::ProtobufChunkSerde::serialize_without_meta(*source, context);
        if (wire.ok()) context->set_encode_levels_in_pb(&*wire);
        return wire;
    }
    spill::SpilledOptions options;
    options.encode_level = level;
    spill::Spiller spiller(options, nullptr);
    ASSIGN_OR_RETURN(auto codec, geo_spill_serde(spiller, source));
    auto block = std::make_shared<GeoSpillBlock>();
    spill::SerdeContext context;
    RETURN_IF_ERROR(codec->serialize(nullptr, context, source, block, false));
    ChunkPB wire;
    wire.set_data(block->bytes);
    return wire;
}

StatusOr<ChunkUniquePtr> read_nested_geo(const ChunkPtr& schema, const TypeDescriptor& type, const ChunkPB& wire,
                                         bool spill, int level = kNoEncoding) {
    if (!spill) {
        const auto& column = schema->columns()[0];
        serde::ProtobufChunkMeta meta;
        meta.types = {type};
        meta.is_nulls = {column->is_nullable()};
        meta.is_consts = {column->is_constant()};
        meta.slot_id_to_index[1] = 0;
        serde::ProtobufChunkDeserializer reader(meta, &wire, level);
        ASSIGN_OR_RETURN(auto chunk, reader.deserialize(wire.data()));
        return std::make_unique<Chunk>(std::move(chunk));
    }
    spill::Spiller spiller(spill::SpilledOptions{}, nullptr);
    ASSIGN_OR_RETURN(auto codec, geo_spill_serde(spiller, schema));
    GeoSpillBlock block(wire.data());
    RuntimeProfile::Counter timer(TUnit::TIME_NS), count(TUnit::UNIT), bytes(TUnit::BYTES);
    spill::BlockReaderOptions options{.read_io_timer = &timer, .read_io_count = &count, .read_io_bytes = &bytes};
    auto reader = block.get_reader(options);
    spill::SerdeContext context;
    return codec->deserialize(context, reader.get());
}

const GeoColumnDescriptor geo_descriptors[] = {
        {{GEO_LOGICAL_TYPE_GEOGRAPHY, GEO_COORDINATE_SYSTEM_SPHERICAL, GEO_EDGE_ALGORITHM_SPHERICAL, "OGC:CRS84", 4326},
         {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_STRUCTURALLY_VALIDATED}},
        {{GEO_LOGICAL_TYPE_GEOMETRY, GEO_COORDINATE_SYSTEM_CARTESIAN, GEO_EDGE_ALGORITHM_PLANAR, "EPSG:3857", 3857},
         {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_STRUCTURALLY_VALIDATED}}};

TEST(NestedGeoSerdeTest, ExchangeAndSpillRoundTrip) {
    for (const auto& desc : geo_descriptors) {
        const auto primitive = desc.type.logical_type == GEO_LOGICAL_TYPE_GEOGRAPHY ? TYPE_GEOGRAPHY : TYPE_GEOMETRY;
        for (const auto& type : nested_geo_types(TypeDescriptor::create_geo_type(primitive, desc.type))) {
            for (auto state : {kValues, kZeroRows, kEmpty, kNullable, kAllNull, kConstant, kConstantNull}) {
                auto source = nested_geo_chunk(type, desc, state);
                for (bool spill : {false, true}) {
                    // Direct empty/all-NULL ARRAY exchange is already covered by PR 3a.
                    if (!spill && type.type == TYPE_ARRAY && type.children[0].is_geo_type() &&
                        (state == kEmpty || state == kAllNull))
                        continue;
                    for (int level : {kNoEncoding, kAdaptiveEncoding}) {
                        for (bool first : {true, false}) {
                            if (spill && !first) continue;
                            SCOPED_TRACE(::testing::Message() << type.debug_string() << ':' << state << ':' << spill
                                                              << ':' << level << ':' << first);
                            auto wire = nested_geo_wire(source, spill, level, first);
                            ASSERT_TRUE(wire.ok()) << wire.status();
                            auto restored = read_nested_geo(source, type, *wire, spill, level);
                            ASSERT_TRUE(restored.ok()) << restored.status();
                            const auto& before = source->columns()[0];
                            const auto& after = (*restored)->columns()[0];
                            EXPECT_EQ(before->size(), after->size());
                            EXPECT_EQ(before->is_nullable(), after->is_nullable());
                            EXPECT_FALSE(after->is_constant());
                            auto expected = column_wire(*before), actual = column_wire(*after);
                            ASSERT_TRUE(expected.ok());
                            ASSERT_TRUE(actual.ok());
                            EXPECT_EQ(*expected, *actual); // Includes offsets, keys, null maps, descriptors and WKB.
                            const auto* leaf = nested_geo_leaf(after.get());
                            const auto* input = nested_geo_leaf(before.get());
                            EXPECT_EQ(input->descriptor(), leaf->descriptor());
                            ASSERT_EQ(input->size(), leaf->size());
                            for (size_t row = 0; row < leaf->size(); ++row)
                                EXPECT_EQ(input->get_wkb(row), leaf->get_wkb(row));
                            (*restored)->check_or_die();
                        }
                    }
                }
            }
        }
    }
}

TEST(NestedGeoSerdeTest, RejectsDescriptorMismatch) {
    for (const auto& desc : geo_descriptors) {
        const auto primitive = desc.type.logical_type == GEO_LOGICAL_TYPE_GEOGRAPHY ? TYPE_GEOGRAPHY : TYPE_GEOMETRY;
        for (const auto& type : nested_geo_types(TypeDescriptor::create_geo_type(primitive, desc.type))) {
            auto source = nested_geo_chunk(type, desc, kValues);
            for (bool spill : {false, true}) {
                SCOPED_TRACE(::testing::Message() << type.debug_string() << ':' << spill);
                auto wire = nested_geo_wire(source, spill, kNoEncoding);
                ASSERT_TRUE(wire.ok()) << wire.status();
                auto receiver_type = type;
                auto* receiver_leaf = &receiver_type;
                while (!receiver_leaf->is_geo_type()) receiver_leaf = &receiver_leaf->children.back();
                const auto& other = geo_descriptors[primitive == TYPE_GEOGRAPHY ? 1 : 0];
                *receiver_leaf = TypeDescriptor::create_geo_type(
                        primitive == TYPE_GEOGRAPHY ? TYPE_GEOMETRY : TYPE_GEOGRAPHY, other.type);
                auto receiver = nested_geo_chunk(receiver_type, other, kValues);
                EXPECT_FALSE(read_nested_geo(receiver, receiver_type, *wire, spill).ok());
                if (primitive == TYPE_GEOMETRY) {
                    auto other_crs = desc;
                    other_crs.type.crs = "EPSG:4326";
                    other_crs.type.srid = 4326;
                    *receiver_leaf = TypeDescriptor::create_geo_type(primitive, other_crs.type);
                    receiver = nested_geo_chunk(receiver_type, other_crs, kValues);
                    EXPECT_FALSE(read_nested_geo(receiver, receiver_type, *wire, spill).ok());
                }
            }
        }
    }
}

} // namespace
} // namespace starrocks
