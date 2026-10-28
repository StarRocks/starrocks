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

#include <arrow/io/memory.h>
#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

#include <algorithm>
#include <cstring>
#include <optional>
#include <vector>

#include "base/testutil/assert.h"
#include "base/types/int256.h"
#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/map_column.h"
#include "common/object_pool.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "connector/hive/scanner/hdfs_scanner_context.h"
#include "formats/parquet/file_reader.h"
#include "fs/fs.h"
#include "io/string_input_stream.h"
#include "parquet_test_util/util.h"
#include "runtime/descriptor_helper.h"
#include "runtime/runtime_state.h"

namespace starrocks::parquet {
namespace {

void check_nested_decimals(bool wide) {
    namespace pq = ::parquet;
    using pq::schema::GroupNode;
    using pq::schema::PrimitiveNode;
    const uint8_t raw[][2] = {{0xff, 0x7f}, {0x00, 0x00}, {0x00, 0x80}, {0x01, 0x00}, {0xff, 0x00}};
    const uint8_t wide_negative[] = {0xe9, 0xe4, 0x33, 0x58, 0xee, 0x66, 0xea, 0x4a, 0xf8, 0x9b, 0x4b,
                                     0x54, 0x17, 0x9a, 0xd6, 0x86, 0x88, 0x8a, 0x5a, 0x0e, 0x8e, 0x6a,
                                     0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01};
    const uint8_t wide_positive[] = {0x16, 0x1b, 0xcc, 0xa7, 0x11, 0x99, 0x15, 0xb5, 0x07, 0x64, 0xb4,
                                     0xab, 0xe8, 0x65, 0x29, 0x79, 0x77, 0x75, 0xa5, 0xf1, 0x71, 0x95,
                                     0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
    const pq::ByteArray values[] = {{wide ? 32U : 2U, wide ? wide_negative : raw[0]},
                                    {1, raw[1]},
                                    {wide ? 32U : 2U, wide ? wide_positive : raw[2]},
                                    {1, raw[3]},
                                    {1, raw[4]}};
    const int256_t max = parse_int256(std::string(76, '9'));
    const int256_t negative = wide ? -max : int256_t(-129);
    const int256_t positive = wide ? max : int256_t(128);
    const int precision = wide ? 76 : 18;
    const int scale = wide ? 0 : 8;
    const uint8_t raw_keys[] = {11, 12, 13, 14, 15, 16, 17};

    for (bool outer_nullable : {false, true}) {
        for (bool value_nullable : {false, true}) {
            for (int list_encoding : {0, 1, 2}) {
                if ((list_encoding != 0 && value_nullable) || (list_encoding == 2 && outer_nullable)) continue;
                for (bool dictionary : {false, true}) {
                    SCOPED_TRACE(::testing::Message()
                                 << "outer_nullable=" << outer_nullable << ", value_nullable=" << value_nullable
                                 << ", list_encoding=" << list_encoding << ", dictionary=" << dictionary);
                    std::vector<int16_t> defs, reps;
                    std::vector<std::vector<std::optional<int256_t>>> expected;
                    std::vector<std::vector<int32_t>> expected_keys;
                    std::vector<int32_t> keys;
                    if (outer_nullable) {
                        defs.push_back(0);
                        reps.push_back(0);
                        expected.push_back({});
                        expected_keys.push_back({});
                    }
                    defs.push_back(outer_nullable ? 1 : 0);
                    reps.push_back(0);
                    expected.push_back({});
                    expected_keys.push_back({});
                    if (value_nullable) {
                        const int16_t levels[] = {1, 2, 2, 2, 2, 1, 2};
                        const int16_t repetitions[] = {0, 1, 1, 1, 0, 1, 0};
                        for (int i = 0; i < 7; ++i) {
                            defs.push_back(levels[i] + outer_nullable);
                            reps.push_back(repetitions[i]);
                        }
                        expected.insert(expected.end(), {{std::nullopt, negative, int256_t(0), positive},
                                                         {int256_t(1), std::nullopt},
                                                         {int256_t(-1)}});
                        expected_keys.insert(expected_keys.end(), {{11, 12, 13, 14}, {15, 16}, {17}});
                        keys = {11, 12, 13, 14, 15, 16, 17};
                    } else {
                        const int16_t repetitions[] = {0, 1, 1, 0, 0};
                        for (int16_t rep : repetitions) {
                            defs.push_back(1 + outer_nullable);
                            reps.push_back(rep);
                        }
                        expected.insert(expected.end(),
                                        {{negative, int256_t(0), positive}, {int256_t(1)}, {int256_t(-1)}});
                        expected_keys.insert(expected_keys.end(), {{12, 13, 14}, {15}, {17}});
                        keys = {12, 13, 14, 15, 17};
                    }
                    std::vector<int16_t> key_defs;
                    for (int16_t def : defs) key_defs.push_back(std::min<int16_t>(def, 1 + outer_nullable));
                    std::vector<pq::ByteArray> decimal_keys;
                    for (int32_t key : keys) decimal_keys.emplace_back(1, &raw_keys[key - 11]);
                    auto outer_repetition = outer_nullable ? pq::Repetition::OPTIONAL : pq::Repetition::REQUIRED;
                    auto value_repetition = value_nullable ? pq::Repetition::OPTIONAL : pq::Repetition::REQUIRED;
                    pq::schema::NodePtr list;
                    if (list_encoding == 0) {
                        auto element =
                                PrimitiveNode::Make("element", value_repetition,
                                                    pq::LogicalType::Decimal(precision, scale), pq::Type::BYTE_ARRAY);
                        auto list_group = GroupNode::Make("list", pq::Repetition::REPEATED, {element});
                        list = GroupNode::Make("items", outer_repetition, {list_group}, pq::ConvertedType::LIST);
                    } else if (list_encoding == 1) {
                        auto element =
                                PrimitiveNode::Make("element", pq::Repetition::REPEATED,
                                                    pq::LogicalType::Decimal(precision, scale), pq::Type::BYTE_ARRAY);
                        list = GroupNode::Make("items", outer_repetition, {element}, pq::ConvertedType::LIST);
                    } else {
                        list = PrimitiveNode::Make("items", pq::Repetition::REPEATED,
                                                   pq::LogicalType::Decimal(precision, scale), pq::Type::BYTE_ARRAY);
                    }
                    auto key = PrimitiveNode::Make("key", pq::Repetition::REQUIRED, pq::Type::INT32);
                    auto value = PrimitiveNode::Make("value", value_repetition,
                                                     pq::LogicalType::Decimal(precision, scale), pq::Type::BYTE_ARRAY);
                    auto entries = GroupNode::Make("key_value", pq::Repetition::REPEATED, {key, value});
                    auto map = GroupNode::Make("prices", outer_repetition, {entries}, pq::ConvertedType::MAP);
                    auto decimal_key =
                            PrimitiveNode::Make("key", pq::Repetition::REQUIRED,
                                                pq::LogicalType::Decimal(precision, scale), pq::Type::BYTE_ARRAY);
                    auto int_value = PrimitiveNode::Make("value", pq::Repetition::REQUIRED, pq::Type::INT32);
                    auto decimal_entries =
                            GroupNode::Make("key_value", pq::Repetition::REPEATED, {decimal_key, int_value});
                    auto decimal_map = GroupNode::Make("decimal_keys", outer_repetition, {decimal_entries},
                                                       pq::ConvertedType::MAP);
                    pq::schema::NodeVector fields = {list};
                    if (list_encoding != 2) {
                        fields.push_back(map);
                        fields.push_back(decimal_map);
                    }
                    auto schema = std::static_pointer_cast<GroupNode>(
                            GroupNode::Make("root", pq::Repetition::REQUIRED, fields));
                    auto sink_result = ::arrow::io::BufferOutputStream::Create();
                    ASSERT_TRUE(sink_result.ok());
                    auto sink = sink_result.ValueOrDie();
                    pq::WriterProperties::Builder properties;
                    if (dictionary)
                        properties.enable_dictionary();
                    else
                        properties.disable_dictionary();
                    auto writer = pq::ParquetFileWriter::Open(sink, schema, properties.build());
                    auto* group = writer->AppendRowGroup();
                    static_cast<pq::ByteArrayWriter*>(group->NextColumn())
                            ->WriteBatch(defs.size(), defs.data(), reps.data(), values);
                    if (list_encoding != 2) {
                        static_cast<pq::Int32Writer*>(group->NextColumn())
                                ->WriteBatch(defs.size(), key_defs.data(), reps.data(), keys.data());
                        static_cast<pq::ByteArrayWriter*>(group->NextColumn())
                                ->WriteBatch(defs.size(), defs.data(), reps.data(), values);
                        static_cast<pq::ByteArrayWriter*>(group->NextColumn())
                                ->WriteBatch(defs.size(), key_defs.data(), reps.data(), decimal_keys.data());
                        static_cast<pq::Int32Writer*>(group->NextColumn())
                                ->WriteBatch(defs.size(), key_defs.data(), reps.data(), keys.data());
                    }
                    writer->Close();
                    auto buffer_result = sink->Finish();
                    ASSERT_TRUE(buffer_result.ok());
                    auto buffer = buffer_result.ValueOrDie();

                    auto apache_reader =
                            pq::ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(buffer));
                    for (int index : (list_encoding == 2 ? std::vector<int>{0} : std::vector<int>{0, 2})) {
                        auto metadata = apache_reader->metadata()->RowGroup(0)->ColumnChunk(index);
                        ASSERT_EQ(pq::Type::BYTE_ARRAY, metadata->type());
                        bool encoding_seen = false;
                        for (const auto& stat : metadata->encoding_stats()) {
                            if (stat.page_type == pq::PageType::DATA_PAGE ||
                                stat.page_type == pq::PageType::DATA_PAGE_V2) {
                                EXPECT_EQ(dictionary ? pq::Encoding::RLE_DICTIONARY : pq::Encoding::PLAIN,
                                          stat.encoding);
                                encoding_seen = true;
                            }
                        }
                        ASSERT_TRUE(encoding_seen);
                        auto column = apache_reader->RowGroup(0)->Column(index);
                        auto* reader = static_cast<pq::ByteArrayReader*>(column.get());
                        int16_t actual_defs[9], actual_reps[9];
                        pq::ByteArray actual_values[9];
                        int64_t values_read = 0;
                        ASSERT_EQ(defs.size(),
                                  reader->ReadBatch(9, actual_defs, actual_reps, actual_values, &values_read));
                        ASSERT_EQ(5, values_read);
                        for (size_t i = 0; i < defs.size(); ++i) {
                            EXPECT_EQ(defs[i], actual_defs[i]);
                            EXPECT_EQ(reps[i], actual_reps[i]);
                        }
                        for (int i = 0; i < 5; ++i) {
                            ASSERT_EQ(values[i].len, actual_values[i].len);
                            EXPECT_EQ(0, std::memcmp(values[i].ptr, actual_values[i].ptr, values[i].len));
                        }
                    }

                    if (list_encoding != 2) {
                        auto metadata = apache_reader->metadata()->RowGroup(0)->ColumnChunk(3);
                        ASSERT_EQ(pq::Type::BYTE_ARRAY, metadata->type());
                        auto column = apache_reader->RowGroup(0)->Column(3);
                        auto* reader = static_cast<pq::ByteArrayReader*>(column.get());
                        int16_t actual_defs[9], actual_reps[9];
                        pq::ByteArray actual_values[9];
                        int64_t values_read = 0;
                        ASSERT_EQ(key_defs.size(),
                                  reader->ReadBatch(9, actual_defs, actual_reps, actual_values, &values_read));
                        ASSERT_EQ(keys.size(), values_read);
                        for (size_t i = 0; i < key_defs.size(); ++i) {
                            EXPECT_EQ(key_defs[i], actual_defs[i]);
                            EXPECT_EQ(reps[i], actual_reps[i]);
                        }
                        for (size_t i = 0; i < keys.size(); ++i) {
                            ASSERT_EQ(1, actual_values[i].len);
                            EXPECT_EQ(keys[i], actual_values[i].ptr[0]);
                        }
                    }

                    ObjectPool pool;
                    auto* state = pool.add(new RuntimeState(TQueryGlobals()));
                    FragmentDictState fragment_dict_state;
                    state->set_fragment_dict_state(&fragment_dict_state);
                    auto* ctx = pool.add(new HdfsScannerContext());
                    FormatScannerStats stats{};
                    std::atomic<int32_t> lazy_counter{0};
                    ctx->format_scan_context.lazy_column_coalesce_counter = &lazy_counter;
                    ctx->format_scan_context.timezone = "UTC";
                    ctx->format_scan_context.stats = &stats;
                    ctx->format_scan_context.predicate_tree = &ctx->predicates.predicate_tree;
                    const auto decimal_type = TypeDescriptor::from_logical_type(wide ? TYPE_DECIMAL256 : TYPE_DECIMAL64,
                                                                                -1, precision, scale);
                    const auto list_type = TypeDescriptor::create_array_type(decimal_type);
                    const auto map_type =
                            TypeDescriptor::create_map_type(TypeDescriptor::from_logical_type(TYPE_INT), decimal_type);
                    const auto key_map_type =
                            TypeDescriptor::create_map_type(decimal_type, TypeDescriptor::from_logical_type(TYPE_INT));
                    Utils::SlotDesc slots[] = {
                            {"items", list_type, 0}, {"prices", map_type, 1}, {"decimal_keys", key_map_type, 2}, {""}};
                    Utils::SlotDesc list_only[] = {{"items", list_type, 0}, {""}};
                    auto* tuple = Utils::create_tuple_descriptor(state, &pool, list_encoding == 2 ? list_only : slots);
                    Utils::make_column_info_vector(tuple, &ctx->format_scan_context.materialized_columns);
                    ctx->slot_descs = tuple->slots();
                    ctx->format_scan_context.scan_range_offset = 0;
                    ctx->format_scan_context.scan_range_length = buffer->size();
                    auto file = std::make_shared<RandomAccessFile>(
                            std::make_shared<io::StringInputStream>(buffer->ToString()), "nested-decimal.parquet");
                    auto reader = std::make_shared<FileReader>(2, file.get(), buffer->size());
                    ASSERT_OK(reader->init(&ctx->format_scan_context));
                    auto chunk = std::make_shared<Chunk>();
                    chunk->append_column(ColumnHelper::create_column(list_type, true), 0);
                    if (list_encoding != 2) {
                        chunk->append_column(ColumnHelper::create_column(map_type, true), 1);
                        chunk->append_column(ColumnHelper::create_column(key_map_type, true), 2);
                    }
                    size_t row_index = 0;
                    for (;;) {
                        chunk->reset();
                        auto status = reader->get_next(&chunk);
                        if (status.is_end_of_file()) break;
                        ASSERT_OK(status);
                        for (size_t row = 0; row < chunk->num_rows(); ++row, ++row_index) {
                            ASSERT_LT(row_index, expected.size());
                            const auto& list_column = chunk->get_column_by_index(0);
                            EXPECT_EQ(outer_nullable && row_index == 0, list_column->is_null(row));
                            auto* array =
                                    down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(list_column.get()));
                            auto array_start = array->offsets().get_data()[row];
                            ASSERT_EQ(expected[row_index].size(), array->offsets().get_data()[row + 1] - array_start);
                            for (size_t i = 0; i < expected[row_index].size(); ++i) {
                                const auto& item = expected[row_index][i];
                                EXPECT_EQ(!item.has_value(), array->elements().is_null(array_start + i));
                                if (item) {
                                    const auto value = array->elements().get(array_start + i);
                                    EXPECT_EQ(*item, wide ? value.get_int256() : int256_t(value.get_int64()));
                                }
                            }
                            if (list_encoding == 2) continue;
                            const auto& map_column = chunk->get_column_by_index(1);
                            const auto& key_map_column = chunk->get_column_by_index(2);
                            EXPECT_EQ(outer_nullable && row_index == 0, map_column->is_null(row));
                            EXPECT_EQ(outer_nullable && row_index == 0, key_map_column->is_null(row));
                            auto* result_map =
                                    down_cast<const MapColumn*>(ColumnHelper::get_data_column(map_column.get()));
                            auto* key_map =
                                    down_cast<const MapColumn*>(ColumnHelper::get_data_column(key_map_column.get()));
                            auto key_map_start = key_map->offsets().get_data()[row];
                            auto map_start = result_map->offsets().get_data()[row];
                            ASSERT_EQ(expected_keys[row_index].size(),
                                      key_map->offsets().get_data()[row + 1] - key_map_start);
                            ASSERT_EQ(expected[row_index].size(),
                                      result_map->offsets().get_data()[row + 1] - map_start);
                            for (size_t i = 0; i < expected[row_index].size(); ++i) {
                                const auto& item = expected[row_index][i];
                                EXPECT_EQ(!item.has_value(), result_map->values().is_null(map_start + i));
                                EXPECT_EQ(expected_keys[row_index][i],
                                          result_map->keys().get(map_start + i).get_int32());
                                const auto decimal_key_value = key_map->keys().get(key_map_start + i);
                                EXPECT_EQ(int256_t(expected_keys[row_index][i]),
                                          wide ? decimal_key_value.get_int256()
                                               : int256_t(decimal_key_value.get_int64()));
                                EXPECT_EQ(expected_keys[row_index][i],
                                          key_map->values().get(key_map_start + i).get_int32());
                                if (item) {
                                    const auto value = result_map->values().get(map_start + i);
                                    EXPECT_EQ(*item, wide ? value.get_int256() : int256_t(value.get_int64()));
                                }
                            }
                        }
                    }
                    EXPECT_EQ(expected.size(), row_index);
                }
            }
        }
    }
}

TEST(ByteArrayDecimalNestedTest, NullableListAndMap) {
    check_nested_decimals(false);
}

TEST(ByteArrayDecimalNestedTest, Decimal256ListAndMap) {
    check_nested_decimals(true);
}

} // namespace
} // namespace starrocks::parquet
