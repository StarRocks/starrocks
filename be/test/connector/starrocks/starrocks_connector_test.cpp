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

#include "connector/starrocks/starrocks_connector.h"

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "runtime/serde/protobuf_chunk_serde.h"
#include "types/type_descriptor.h"

namespace starrocks::connector {

namespace {

ColumnPtr make_int_column(int32_t start) {
    auto column = FixedLengthColumn<int32_t>::create();
    for (int32_t i = 0; i < 8; ++i) {
        column->append(start + i);
    }
    return column;
}

ColumnPtr make_bigint_column(int64_t value) {
    auto column = FixedLengthColumn<int64_t>::create();
    for (int32_t i = 0; i < 8; ++i) {
        column->append(value);
    }
    return column;
}

TStarRocksRemoteScanOutput make_remote_output(
        int32_t output_index, SlotId local_slot_id, const TypeDescriptor& type,
        TStarRocksRemoteScanWireShape::type wire_shape = TStarRocksRemoteScanWireShape::FULL_ROOT) {
    TStarRocksRemoteScanOutput output;
    output.__set_output_index(output_index);
    output.__set_local_slot_id(local_slot_id);
    output.__set_actual_wire_type(type.to_thrift());
    output.__set_nullable(false);
    output.__set_is_const(false);
    output.__set_wire_shape(wire_shape);
    return output;
}

TStarRocksRemoteScanOutput make_remote_output(int32_t output_index, SlotId local_slot_id, LogicalType type) {
    return make_remote_output(output_index, local_slot_id, TypeDescriptor(type));
}

TStarRocksRemoteScanOutput make_remote_output(int32_t output_index, SlotId local_slot_id, LogicalType type,
                                              TStarRocksRemoteScanWireShape::type wire_shape) {
    return make_remote_output(output_index, local_slot_id, TypeDescriptor(type), wire_shape);
}

void assert_invalid_remote_outputs(const std::vector<TStarRocksRemoteScanOutput>& outputs,
                                   const std::string& expected_message) {
    auto meta_or = build_remote_scan_chunk_meta(outputs);
    ASSERT_FALSE(meta_or.ok());
    ASSERT_NE(std::string::npos, meta_or.status().to_string().find(expected_message)) << meta_or.status();
}

} // namespace

TEST(StarRocksConnectorTest, BuildsChunkMetaWithWireOrderAndLocalSlotIds) {
    Chunk wire_chunk;
    wire_chunk.append_column(make_int_column(100), 99);
    wire_chunk.append_column(make_int_column(200), 88);
    auto chunk_pb_or = serde::ProtobufChunkSerde::serialize_without_meta(wire_chunk);
    ASSERT_TRUE(chunk_pb_or.ok()) << chunk_pb_or.status();
    auto chunk_pb = std::move(chunk_pb_or).value();

    std::vector<TStarRocksRemoteScanOutput> outputs;
    outputs.emplace_back(make_remote_output(0, 7, TYPE_INT));
    outputs.emplace_back(make_remote_output(1, 4, TYPE_INT));

    auto meta_or = build_remote_scan_chunk_meta(outputs);
    ASSERT_TRUE(meta_or.ok()) << meta_or.status();
    auto meta = std::move(meta_or).value();
    serde::ProtobufChunkDeserializer deserializer(meta, &chunk_pb);
    auto decoded_or = deserializer.deserialize(chunk_pb.data());
    ASSERT_TRUE(decoded_or.ok()) << decoded_or.status();
    auto decoded = std::move(decoded_or).value();

    ASSERT_EQ(2, decoded.num_columns());
    ASSERT_EQ(8, decoded.num_rows());
    ASSERT_EQ(100, decoded.get_column_by_slot_id(7)->get(0).get_int32());
    ASSERT_EQ(200, decoded.get_column_by_slot_id(4)->get(0).get_int32());
}

TEST(StarRocksConnectorTest, BuildsChunkMetaWithPrunedStructWireType) {
    TypeDescriptor wire_type = TypeDescriptor::create_struct_type(
            {"f2"}, {TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH)});
    std::vector<TStarRocksRemoteScanOutput> outputs;
    outputs.emplace_back(make_remote_output(0, 4, wire_type, TStarRocksRemoteScanWireShape::PRUNED_ROOT_STRUCT));

    auto meta_or = build_remote_scan_chunk_meta(outputs);
    ASSERT_TRUE(meta_or.ok()) << meta_or.status();
    auto meta = std::move(meta_or).value();

    ASSERT_EQ(1, meta.types.size());
    ASSERT_EQ(0, meta.slot_id_to_index[4]);
    ASSERT_EQ(TYPE_STRUCT, meta.types[0].type);
    ASSERT_EQ(std::vector<std::string>({"f2"}), meta.types[0].field_names);
    ASSERT_EQ(1, meta.types[0].children.size());
    ASSERT_EQ(TYPE_VARCHAR, meta.types[0].children[0].type);
}

TEST(StarRocksConnectorTest, BuildsChunkMetaWithRowMarkerOutput) {
    Chunk wire_chunk;
    wire_chunk.append_column(make_bigint_column(1), 0);
    auto chunk_pb_or = serde::ProtobufChunkSerde::serialize_without_meta(wire_chunk);
    ASSERT_TRUE(chunk_pb_or.ok()) << chunk_pb_or.status();
    auto chunk_pb = std::move(chunk_pb_or).value();

    std::vector<TStarRocksRemoteScanOutput> outputs;
    outputs.emplace_back(make_remote_output(0, 9, TYPE_BIGINT, TStarRocksRemoteScanWireShape::ROW_MARKER));

    auto meta_or = build_remote_scan_chunk_meta(outputs);
    ASSERT_TRUE(meta_or.ok()) << meta_or.status();
    auto meta = std::move(meta_or).value();

    ASSERT_EQ(1, meta.types.size());
    ASSERT_EQ(TYPE_BIGINT, meta.types[0].type);
    ASSERT_EQ(0, meta.slot_id_to_index[9]);
    ASSERT_FALSE(meta.is_nulls[0]);
    ASSERT_FALSE(meta.is_consts[0]);

    serde::ProtobufChunkDeserializer deserializer(meta, &chunk_pb);
    auto decoded_or = deserializer.deserialize(chunk_pb.data());
    ASSERT_TRUE(decoded_or.ok()) << decoded_or.status();
    auto decoded = std::move(decoded_or).value();
    ASSERT_EQ(8, decoded.num_rows());
    ASSERT_EQ(1, decoded.get_column_by_slot_id(9)->get(0).get_int64());
}

TEST(StarRocksConnectorTest, RejectsInvalidRemoteOutputMapping) {
    assert_invalid_remote_outputs({make_remote_output(0, 4, TYPE_INT), make_remote_output(0, 5, TYPE_INT)},
                                  "duplicate starrocks remote scan output_index");
    assert_invalid_remote_outputs({make_remote_output(0, 4, TYPE_INT), make_remote_output(2, 5, TYPE_INT)},
                                  "starrocks remote scan output_index 2 out of range");
    assert_invalid_remote_outputs({make_remote_output(0, 4, TYPE_INT), make_remote_output(1, 4, TYPE_INT)},
                                  "duplicate starrocks remote scan local_slot_id");

    auto missing_index = make_remote_output(0, 4, TYPE_INT);
    missing_index.__isset.output_index = false;
    assert_invalid_remote_outputs({missing_index}, "missing output_index");

    auto missing_slot = make_remote_output(0, 4, TYPE_INT);
    missing_slot.__isset.local_slot_id = false;
    assert_invalid_remote_outputs({missing_slot}, "missing local_slot_id");

    auto missing_type = make_remote_output(0, 4, TYPE_INT);
    missing_type.__isset.actual_wire_type = false;
    assert_invalid_remote_outputs({missing_type}, "missing actual_wire_type");

    auto missing_nullable = make_remote_output(0, 4, TYPE_INT);
    missing_nullable.__isset.nullable = false;
    assert_invalid_remote_outputs({missing_nullable}, "missing nullable");

    auto missing_const = make_remote_output(0, 4, TYPE_INT);
    missing_const.__isset.is_const = false;
    assert_invalid_remote_outputs({missing_const}, "missing is_const");
}

} // namespace starrocks::connector
