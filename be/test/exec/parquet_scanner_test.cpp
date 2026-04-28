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

#include "exec/parquet_scanner.h"

#include <arrow/builder.h>
#include <arrow/io/file.h>
#include <gtest/gtest.h>
#include <parquet/arrow/writer.h>

#include <filesystem>
#include <memory>
#include <utility>

#include "column/chunk.h"
#include "common/status.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "testutil/assert.h"
#include "testutil/desc_tbl_helper.h"
#include "util/defer_op.h"

namespace starrocks {

class ParquetScannerTest : public ::testing::Test {
public:
    static void SetUpTestSuite() {
        const char* starrocks_home = getenv("STARROCKS_HOME");
        ASSERT_NE(nullptr, starrocks_home);
        _tmp_root_dir = std::filesystem::path(starrocks_home) / "be/test/exec/test_data/parquet_scanner/tmp";
        ASSERT_FALSE(_tmp_root_dir.empty());
        ASSERT_EQ(std::filesystem::path("tmp"), _tmp_root_dir.filename());

        std::error_code ec;
        std::filesystem::create_directories(_tmp_root_dir, ec);
        ASSERT_FALSE(ec) << "failed to create directory " << _tmp_root_dir << ": " << ec.message();
    }

    static void TearDownTestSuite() {
        ASSERT_FALSE(_tmp_root_dir.empty());
        ASSERT_EQ(std::filesystem::path("tmp"), _tmp_root_dir.filename());
        ASSERT_NE(std::filesystem::path(), _tmp_root_dir.parent_path());
        ASSERT_EQ(std::filesystem::path("parquet_scanner"), _tmp_root_dir.parent_path().filename());

        std::error_code ec;
        std::filesystem::remove_all(_tmp_root_dir, ec);
        ASSERT_FALSE(ec) << "failed to remove directory " << _tmp_root_dir << ": " << ec.message();
    }

private:
    std::vector<TBrokerRangeDesc> generate_ranges(const std::vector<std::string>& file_names,
                                                  int32_t num_columns_from_file,
                                                  const std::vector<std::string>& columns_from_path) {
        std::vector<TBrokerRangeDesc> ranges;
        ranges.resize(file_names.size());
        for (auto i = 0; i < file_names.size(); ++i) {
            TBrokerRangeDesc& range = ranges[i];
            range.__set_num_of_columns_from_file(num_columns_from_file);
            range.__set_columns_from_path(columns_from_path);
            range.__set_path(file_names[i]);
            range.start_offset = 0;
            range.size = LONG_MAX;
            range.file_type = TFileType::FILE_LOCAL;
        }
        return ranges;
    }

    std::vector<TBrokerRangeDesc> generate_split_ranges(const std::vector<std::string>& file_names,
                                                        const std::vector<int>& file_sizes,
                                                        int32_t num_columns_from_file,
                                                        const std::vector<std::string>& columns_from_path) {
        std::vector<TBrokerRangeDesc> ranges;
        int split_size = 128 * 1024;

        for (auto i = 0; i < file_names.size(); ++i) {
            TBrokerRangeDesc range;
            range.__set_num_of_columns_from_file(num_columns_from_file);
            range.__set_columns_from_path(columns_from_path);
            range.__set_path(file_names[i]);
            range.file_type = TFileType::FILE_LOCAL;

            for (auto offset = 0; offset < file_sizes[i]; offset += split_size) {
                range.start_offset = offset;
                range.size = split_size < file_sizes[i] - offset ? split_size : file_sizes[i] - offset;
                ranges.push_back(range);
            }
        }
        return ranges;
    }

    starrocks::TExpr create_column_ref(int32_t slot_id, const TypeDescriptor& type_desc, bool is_nullable) {
        starrocks::TExpr e = starrocks::TExpr();
        e.nodes.emplace_back(TExprNode());
        e.nodes[0].__set_type(type_desc.to_thrift());
        e.nodes[0].__set_node_type(TExprNodeType::SLOT_REF);
        e.nodes[0].__set_is_nullable(is_nullable);
        e.nodes[0].__set_slot_ref(TSlotRef());
        e.nodes[0].slot_ref.__set_slot_id((::starrocks::TSlotId)slot_id);
        return e;
    }

    starrocks::TExpr create_cast_expr(const starrocks::TExpr& child, const TypeDescriptor& type_desc) {
        starrocks::TExpr e = starrocks::TExpr();
        e.nodes.emplace_back(TExprNode());
        e.nodes.insert(e.nodes.end(), child.nodes.begin(), child.nodes.end());
        auto& to_expr = e.nodes[0];
        to_expr.__set_type(type_desc.to_thrift());
        to_expr.__set_child_type(child.nodes[0].type.types[0].scalar_type.type);
        to_expr.__set_node_type(TExprNodeType::CAST_EXPR);
        to_expr.__set_is_nullable(true);
        to_expr.__set_num_children(1);
        return e;
    }

    std::unique_ptr<ParquetScanner> create_parquet_scanner(
            const std::string& timezone, DescriptorTbl* desc_tbl,
            const std::unordered_map<size_t, ::starrocks::TExpr>& dst_slot_exprs,
            const std::vector<TBrokerRangeDesc>& ranges, int32_t chunk_size = 0) {
        /// Init RuntimeState
        TQueryOptions query_options;
        if (chunk_size > 0) {
            query_options.__set_batch_size(chunk_size);
        }
        auto query_globals = TQueryGlobals();
        query_globals.time_zone = timezone;
        RuntimeState* state = _obj_pool.add(new RuntimeState(TUniqueId(), query_options, query_globals, nullptr));
        state->set_desc_tbl(desc_tbl);
        state->init_instance_mem_tracker();

        /// TBrokerScanRangeParams
        TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
        params->strict_mode = true;
        std::vector<TupleDescriptor*> tuples;
        desc_tbl->get_tuple_descs(&tuples);
        const auto num_tuples = tuples.size();
        params->src_tuple_id = 0;
        params->dest_tuple_id = num_tuples - 1;
        const auto* src_tuple = desc_tbl->get_tuple_descriptor(params->src_tuple_id);
        const auto* dst_tuple = desc_tbl->get_tuple_descriptor(params->dest_tuple_id);
        for (int i = 0; i < src_tuple->slots().size(); i++) {
            auto& src_slot = src_tuple->slots()[i];
            auto& dst_slot = dst_tuple->slots()[i];
            if (dst_slot_exprs.count(i)) {
                params->expr_of_dest_slot[dst_slot->id()] = dst_slot_exprs.at(i);
            } else {
                params->expr_of_dest_slot[dst_slot->id()] =
                        create_column_ref(src_slot->id(), src_slot->type(), src_slot->is_nullable());
            }
        }

        for (int i = 0; i < src_tuple->slots().size(); i++) {
            params->src_slot_ids.emplace_back(i);
        }

        RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("test_prof", true));
        ScannerCounter* counter = _obj_pool.add(new ScannerCounter());

        TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
        broker_scan_range->params = *params;
        broker_scan_range->ranges = ranges;

        auto scanner = std::make_unique<ParquetScanner>(state, profile, *broker_scan_range, counter);
        EXPECT_EQ("parquet", scanner->file_format());
        // scan_type is not set in TBrokerScanRangeParams, default to LOAD
        EXPECT_EQ("load", scanner->scan_type());
        return scanner;
    }

    void validate(std::unique_ptr<ParquetScanner>& scanner, const size_t expect_num_rows,
                  const std::function<void(const ChunkPtr&)>& check_func) {
        ASSERT_OK(scanner->open());
        size_t num_rows = 0;
        while (true) {
            auto res = scanner->get_next();
            if (!res.ok() && res.status().is_end_of_file()) {
                ASSERT_EQ(expect_num_rows, num_rows);
                break;
            }
            if (!res.ok()) {
                std::cout << "Unexpected status:" << res.status().to_string() << std::endl;
            }
            ChunkPtr chunk = res.value();
            if (chunk == nullptr) {
                ASSERT_EQ(expect_num_rows, num_rows);
                break;
            }

            ASSERT_TRUE(chunk->num_rows() > 0);
            num_rows += chunk->num_rows();
            check_func(chunk);
        }
        ASSERT_GT(scanner->TEST_scanner_counter()->file_read_count, 0);
        ASSERT_GT(scanner->TEST_scanner_counter()->file_read_ns, 0);
        scanner->close();
    }

    SlotTypeDescInfoArray select_columns(const std::vector<std::string>& column_names, bool is_nullable) {
        auto slot_map = std::unordered_map<std::string, TypeDescriptor>{
                {"col_date", TypeDescriptor::from_logical_type(TYPE_DATE)},
                {"col_datetime", TypeDescriptor::from_logical_type(TYPE_DATETIME)},
                {"col_char", TypeDescriptor::from_logical_type(TYPE_CHAR)},
                {"col_varchar", TypeDescriptor::from_logical_type(TYPE_VARCHAR)},
                {"col_boolean", TypeDescriptor::from_logical_type(TYPE_BOOLEAN)},
                {"col_tinyint", TypeDescriptor::from_logical_type(TYPE_TINYINT)},
                {"col_smallint", TypeDescriptor::from_logical_type(TYPE_SMALLINT)},
                {"col_int", TypeDescriptor::from_logical_type(TYPE_INT)},
                {"col_bigint", TypeDescriptor::from_logical_type(TYPE_BIGINT)},
                {"col_decimal_p6s2", TypeDescriptor::from_logical_type(TYPE_DECIMAL32, -1, 6, 2)},
                {"col_decimal_p14s5", TypeDescriptor::from_logical_type(TYPE_DECIMAL64, -1, 14, 5)},
                {"col_decimal_p27s9", TypeDescriptor::from_logical_type(TYPE_DECIMALV2, -1, 27, 9)},

                {"col_int_null", TypeDescriptor::from_logical_type(TYPE_INT)},
                {"col_string_null", TypeDescriptor::from_logical_type(TYPE_VARCHAR)},

                {"col_json_int8", TypeDescriptor::create_json_type()},
                {"col_json_int16", TypeDescriptor::create_json_type()},
                {"col_json_int32", TypeDescriptor::create_json_type()},
                {"col_json_int64", TypeDescriptor::create_json_type()},
                {"col_json_uint8", TypeDescriptor::create_json_type()},
                {"col_json_uint16", TypeDescriptor::create_json_type()},
                {"col_json_uint32", TypeDescriptor::create_json_type()},
                {"col_json_uint64", TypeDescriptor::create_json_type()},
                {"col_json_timestamp", TypeDescriptor::create_json_type()},
                {"col_json_timestamp_not_normalized", TypeDescriptor::create_json_type()},

                {"col_json_float32", TypeDescriptor::create_json_type()},
                {"col_json_float64", TypeDescriptor::create_json_type()},

                {"col_json_bool", TypeDescriptor::create_json_type()},
                {"col_json_string", TypeDescriptor::create_json_type()},

                {"col_json_list", TypeDescriptor::create_json_type()},
                {"col_json_map", TypeDescriptor::create_json_type()},
                {"col_json_map_timestamp", TypeDescriptor::create_json_type()},
                {"col_json_struct", TypeDescriptor::create_json_type()},
                {"col_json_list_list", TypeDescriptor::create_json_type()},
                {"col_json_list_struct", TypeDescriptor::create_json_type()},
                {"col_json_map_list", TypeDescriptor::create_json_type()},
                {"col_json_struct_struct", TypeDescriptor::create_json_type()},

                // Convert struct->JSON->string
                {"col_json_struct_string", TypeDescriptor::from_logical_type(TYPE_VARCHAR)},
                {"col_json_json_string", TypeDescriptor::create_json_type()},
                {"issue_17693_c0", TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_VARCHAR))},
                {"issue_17822_c0", TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_VARCHAR))},
                {"nested_array_c0", TypeDescriptor::create_array_type(TypeDescriptor::create_array_type(
                                            TypeDescriptor::from_logical_type(TYPE_VARCHAR)))},
                {"col_map", TypeDescriptor::create_map_type(TypeDescriptor::create_varchar_type(1048576),
                                                            TypeDescriptor::create_varchar_type(1048576))}};
        SlotTypeDescInfoArray slot_infos;
        slot_infos.reserve(column_names.size());
        for (auto& name : column_names) {
            CHECK_EQ(slot_map.count(name), 1);
            slot_infos.emplace_back(name, slot_map[name], is_nullable);
        }
        return slot_infos;
    }

    template <bool is_nullable>
    void test_column_from_path(const std::vector<std::string>& columns_from_file,
                               const std::vector<std::string>& columns_from_path,
                               const std::vector<std::string>& column_values,
                               const std::unordered_map<size_t, ::starrocks::TExpr>& dst_slot_exprs) {
        std::vector<std::string> file_names;
        if constexpr (is_nullable) {
            file_names = _nullable_file_names;
        } else {
            file_names = _file_names;
        }
        std::vector<std::string> column_names;
        column_names.reserve(columns_from_file.size() + columns_from_path.size());
        column_names.insert(column_names.end(), columns_from_file.begin(), columns_from_file.end());
        column_names.insert(column_names.end(), columns_from_path.begin(), columns_from_path.end());

        auto src_slot_infos = select_columns(columns_from_file, is_nullable);
        for (const auto& i : columns_from_path) {
            src_slot_infos.emplace_back(i, TypeDescriptor::from_logical_type(TYPE_VARCHAR), is_nullable);
        }

        auto dst_slot_infos = select_columns(column_names, is_nullable);

        auto ranges = generate_ranges(file_names, columns_from_file.size(), column_values);
        auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {src_slot_infos, dst_slot_infos});
        auto scanner = create_parquet_scanner("UTC", desc_tbl, dst_slot_exprs, ranges);
        auto check = [](const ChunkPtr& chunk) {
            auto& columns = chunk->columns();
            for (auto& col : columns) {
                if constexpr (is_nullable) {
                    ASSERT_TRUE(!col->only_null() || !col->is_constant());
                } else {
                    ASSERT_TRUE(!col->is_nullable() || !col->is_constant());
                }
            }
        };
        validate(scanner, 36865, check);
        ASSERT_EQ(file_names.size(), scanner->TEST_scanner_counter()->num_files_read);
    }

    template <bool is_nullable>
    ChunkPtr get_chunk(const std::vector<std::string>& columns_from_file,
                       const std::unordered_map<size_t, ::starrocks::TExpr>& dst_slot_exprs, std::string specific_file,
                       size_t expected_rows) {
        std::vector<std::string> file_names{std::move(specific_file)};
        const std::vector<std::string>& column_names = columns_from_file;

        auto src_slot_infos = select_columns(columns_from_file, is_nullable);
        auto dst_slot_infos = select_columns(column_names, is_nullable);

        auto ranges = generate_ranges(file_names, columns_from_file.size(), {});
        auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {src_slot_infos, dst_slot_infos});
        auto scanner = create_parquet_scanner("UTC", desc_tbl, dst_slot_exprs, ranges);

        ChunkPtr result;
        auto check = [&](const ChunkPtr& chunk) {
            auto& columns = chunk->columns();
            for (auto& col : columns) {
                if constexpr (is_nullable) {
                    ASSERT_TRUE(!col->only_null() || !col->is_constant());
                } else {
                    ASSERT_TRUE(!col->is_nullable() || !col->is_constant());
                }
            }
            result = chunk;
        };
        validate(scanner, expected_rows, check);

        return result;
    }

    void create_dictionary_string_parquet(const std::string& file_name, const std::vector<std::string>& values,
                                          std::string* file_path) {
        *file_path = (_tmp_root_dir / file_name).string();

        arrow::StringDictionaryBuilder builder;
        for (const auto& value : values) {
            ASSERT_OK(builder.Append(value));
        }

        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder.Finish(&array));

        auto schema = arrow::schema({arrow::field("zone", array->type(), true)});
        auto table = arrow::Table::Make(schema, {array});

        ASSERT_NE(nullptr, table);
        ASSERT_EQ(values.size(), table->num_rows());

        auto output_res = arrow::io::FileOutputStream::Open(*file_path);
        ASSERT_TRUE(output_res.ok()) << output_res.status().ToString();
        std::shared_ptr<arrow::io::FileOutputStream> output = output_res.ValueOrDie();

        auto arrow_props = ::parquet::ArrowWriterProperties::Builder().store_schema()->build();
        auto status = ::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), output, table->num_rows(),
                                                   ::parquet::default_writer_properties(), arrow_props);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_TRUE(output->Close().ok());
    }

    void create_nested_dictionary_parquet(const std::string& file_name,
                                          const std::vector<std::vector<std::string>>& values, std::string* file_path) {
        *file_path = (_tmp_root_dir / file_name).string();

        arrow::ListBuilder list_builder(arrow::default_memory_pool(),
                                        std::make_unique<arrow::StringDictionaryBuilder>(arrow::default_memory_pool()));
        auto* dict_builder = down_cast<arrow::StringDictionaryBuilder*>(list_builder.value_builder());

        for (const auto& list : values) {
            ASSERT_OK(list_builder.Append());
            for (const auto& val : list) {
                ASSERT_OK(dict_builder->Append(val));
            }
        }

        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(list_builder.Finish(&array));

        auto schema = arrow::schema({arrow::field("nested_dict", array->type(), true)});
        auto table = arrow::Table::Make(schema, {array});

        auto output_res = arrow::io::FileOutputStream::Open(*file_path);
        ASSERT_TRUE(output_res.ok()) << output_res.status().ToString();
        std::shared_ptr<arrow::io::FileOutputStream> output = output_res.ValueOrDie();

        auto arrow_props = ::parquet::ArrowWriterProperties::Builder().store_schema()->build();
        auto status = ::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), output, table->num_rows(),
                                                   ::parquet::default_writer_properties(), arrow_props);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_TRUE(output->Close().ok());
    }

    void create_struct_dictionary_parquet(const std::string& file_name, const std::vector<std::string>& values,
                                          std::string* file_path) {
        *file_path = (_tmp_root_dir / file_name).string();

        auto dict_builder = std::make_shared<arrow::StringDictionaryBuilder>(arrow::default_memory_pool());
        auto struct_type = arrow::struct_({arrow::field("name", dict_builder->type(), true)});
        arrow::StructBuilder struct_builder(struct_type, arrow::default_memory_pool(), {dict_builder});

        for (const auto& value : values) {
            ASSERT_OK(struct_builder.Append());
            ASSERT_OK(dict_builder->Append(value));
        }

        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(struct_builder.Finish(&array));

        auto schema = arrow::schema({arrow::field("struct_dict", array->type(), true)});
        auto table = arrow::Table::Make(schema, {array});

        auto output_res = arrow::io::FileOutputStream::Open(*file_path);
        ASSERT_TRUE(output_res.ok()) << output_res.status().ToString();
        std::shared_ptr<arrow::io::FileOutputStream> output = output_res.ValueOrDie();

        auto arrow_props = ::parquet::ArrowWriterProperties::Builder().store_schema()->build();
        auto status = ::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), output, table->num_rows(),
                                                   ::parquet::default_writer_properties(), arrow_props);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_TRUE(output->Close().ok());
    }

    void create_map_dictionary_parquet(const std::string& file_name,
                                       const std::vector<std::vector<std::pair<std::string, std::string>>>& values,
                                       std::string* file_path) {
        *file_path = (_tmp_root_dir / file_name).string();

        auto key_builder = std::make_shared<arrow::StringBuilder>(arrow::default_memory_pool());
        auto item_builder = std::make_shared<arrow::StringDictionaryBuilder>(arrow::default_memory_pool());
        arrow::MapBuilder map_builder(arrow::default_memory_pool(), key_builder, item_builder, false);

        for (const auto& map_values : values) {
            ASSERT_OK(map_builder.Append());
            for (const auto& [key, value] : map_values) {
                ASSERT_OK(key_builder->Append(key));
                ASSERT_OK(item_builder->Append(value));
            }
        }

        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(map_builder.Finish(&array));

        auto schema = arrow::schema({arrow::field("map_dict", array->type(), true)});
        auto table = arrow::Table::Make(schema, {array});

        auto output_res = arrow::io::FileOutputStream::Open(*file_path);
        ASSERT_TRUE(output_res.ok()) << output_res.status().ToString();
        std::shared_ptr<arrow::io::FileOutputStream> output = output_res.ValueOrDie();

        auto arrow_props = ::parquet::ArrowWriterProperties::Builder().store_schema()->build();
        auto status = ::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), output, table->num_rows(),
                                                   ::parquet::default_writer_properties(), arrow_props);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_TRUE(output->Close().ok());
    }

    void check_schema(const std::string& path,
                      const std::vector<std::pair<std::string, TypeDescriptor>>& expected_schema) {
        RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("test_prof", true));
        ScannerCounter* counter = _obj_pool.add(new ScannerCounter());
        auto query_globals = TQueryGlobals();
        RuntimeState* state = _obj_pool.add(new RuntimeState(TUniqueId(), TQueryOptions(), query_globals, nullptr));

        auto ranges = generate_ranges({path}, 0, {});
        TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
        broker_scan_range->ranges = ranges;

        auto scanner = ParquetScanner(state, profile, *broker_scan_range, counter, true);
        EXPECT_OK(scanner.open());
        DeferOp defer([&scanner] { scanner.close(); });

        std::vector<SlotDescriptor> schema;
        EXPECT_OK(scanner.get_schema(&schema));
        EXPECT_EQ(schema.size(), expected_schema.size());
        for (size_t i = 0; i < expected_schema.size(); ++i) {
            EXPECT_EQ(schema[i].col_name(), expected_schema[i].first);
            EXPECT_TRUE(schema[i].type() == expected_schema[i].second)
                    << schema[i].col_name() << " got: " << schema[i].type().debug_string()
                    << " expect: " << expected_schema[i].second.debug_string();
        }
    }

    void SetUp() override {
        std::string starrocks_home = getenv("STARROCKS_HOME");
        test_exec_dir = starrocks_home + "/be/test/exec";
        _nullable_file_names =
                std::vector<std::string>{test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_0.parquet",
                                         test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_1.parquet",
                                         test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_4095.parquet",
                                         test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_4096.parquet",
                                         test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_4097.parquet",
                                         test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_8191.parquet",
                                         test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_8192.parquet",
                                         test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_8193.parquet"};
        _file_names = std::vector<std::string>{test_exec_dir + "/test_data/parquet_data/data_0.parquet",
                                               test_exec_dir + "/test_data/parquet_data/data_1.parquet",
                                               test_exec_dir + "/test_data/parquet_data/data_4095.parquet",
                                               test_exec_dir + "/test_data/parquet_data/data_4096.parquet",
                                               test_exec_dir + "/test_data/parquet_data/data_4097.parquet",
                                               test_exec_dir + "/test_data/parquet_data/data_8191.parquet",
                                               test_exec_dir + "/test_data/parquet_data/data_8192.parquet",
                                               test_exec_dir + "/test_data/parquet_data/data_8193.parquet"};
        _file_sizes = std::vector<int>{404,    /*"/test_data/parquet_data/data_0.parquet",   */
                                       2012,   /*"/test_data/parquet_data/data_1.parquet",*/
                                       386707, /*"/test_data/parquet_data/data_4095.parquet",*/
                                       388341, /*"/test_data/parquet_data/data_4096.parquet",*/
                                       388199, /*"/test_data/parquet_data/data_4097.parquet",*/
                                       773729, /*"/test_data/parquet_data/data_8191.parquet",*/
                                       772472, /*"/test_data/parquet_data/data_8192.parquet",*/
                                       775318 /*"/test_data/parquet_data/data_8193.parquet"*/};
        _runtime_state = _obj_pool.add(new RuntimeState(TQueryGlobals()));
        _issue_16475_file_names =
                std::vector<std::string>{test_exec_dir + "/test_data/parquet_data/issue_17693_1.parquet",
                                         test_exec_dir + "/test_data/parquet_data/issue_17693_2.parquet"};
        _issue_17822_file_names =
                std::vector<std::string>{test_exec_dir + "/test_data/parquet_data/issue_17822.parquet"};
        _nested_array_file_names =
                std::vector<std::string>{test_exec_dir + "/test_data/parquet_data/nested_array_test1.parquet",
                                         test_exec_dir + "/test_data/parquet_data/nested_array_test2.parquet"};
    }

private:
    inline static std::filesystem::path _tmp_root_dir;
    std::string test_exec_dir;
    RuntimeState* _runtime_state;
    ObjectPool _obj_pool;
    std::vector<std::string> _file_names;
    std::vector<std::string> _nullable_file_names;
    std::vector<int> _file_sizes;
    std::vector<std::string> _issue_16475_file_names;
    std::vector<std::string> _issue_17822_file_names;
    std::vector<std::string> _nested_array_file_names;
};

TEST_F(ParquetScannerTest, test_nullable_parquet_data) {
    auto column_names = std::vector<std::string>{
            "col_date",     "col_datetime", "col_char",   "col_varchar",      "col_boolean",       "col_tinyint",
            "col_smallint", "col_int",      "col_bigint", "col_decimal_p6s2", "col_decimal_p14s5", "col_decimal_p27s9",
    };
    auto slot_infos = select_columns(column_names, true);
    auto ranges = generate_ranges(_nullable_file_names, slot_infos.size(), {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {slot_infos, {}});
    auto scanner = create_parquet_scanner("UTC", desc_tbl, {}, ranges);
    auto check = [](const ChunkPtr& chunk) {
        auto& columns = chunk->columns();
        for (auto& col : columns) {
            ASSERT_TRUE(!col->only_null() && col->is_nullable());
        }
    };
    validate(scanner, 36865, check);
}

TEST_F(ParquetScannerTest, test_issue_17693) {
    auto column_names = std::vector<std::string>{
            "issue_17693_c0",
    };
    auto slot_infos = select_columns(column_names, true);
    auto ranges = generate_ranges(_issue_16475_file_names, slot_infos.size(), {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {slot_infos, {}});
    auto scanner = create_parquet_scanner("UTC", desc_tbl, {}, ranges);
    auto check = [](const ChunkPtr& chunk) {
        auto& columns = chunk->columns();
        for (auto& col : columns) {
            ASSERT_TRUE(!col->only_null() && col->is_nullable());
        }
    };
    validate(scanner, 2000, check);
}

TEST_F(ParquetScannerTest, test_issue_17822) {
    auto column_names = std::vector<std::string>{
            "issue_17822_c0",
    };
    auto slot_infos = select_columns(column_names, true);
    auto ranges = generate_ranges(_issue_17822_file_names, slot_infos.size(), {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {slot_infos, {}});
    auto scanner = create_parquet_scanner("UTC", desc_tbl, {}, ranges);
    auto check = [](const ChunkPtr& chunk) {
        auto& columns = chunk->columns();
        for (auto& col : columns) {
            ASSERT_TRUE(!col->only_null() && col->is_nullable());
        }
    };
    validate(scanner, 506, check);
}

TEST_F(ParquetScannerTest, test_nested_array) {
    auto column_names = std::vector<std::string>{
            "nested_array_c0",
    };
    auto slot_infos = select_columns(column_names, true);
    auto ranges = generate_ranges(_nested_array_file_names, slot_infos.size(), {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {slot_infos, {}});
    auto scanner = create_parquet_scanner("UTC", desc_tbl, {}, ranges);
    auto check = [](const ChunkPtr& chunk) {
        auto& columns = chunk->columns();
        for (auto& col : columns) {
            ASSERT_TRUE(!col->only_null() && col->is_nullable());
        }
    };
    validate(scanner, 1003, check);
}

TEST_F(ParquetScannerTest, test_parquet_data) {
    auto column_names = std::vector<std::string>{
            "col_date",     "col_datetime", "col_char",   "col_varchar",      "col_boolean",       "col_tinyint",
            "col_smallint", "col_int",      "col_bigint", "col_decimal_p6s2", "col_decimal_p14s5", "col_decimal_p27s9",
    };
    auto slot_infos = select_columns(column_names, false);
    auto ranges = generate_ranges(_file_names, slot_infos.size(), {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {slot_infos, {}});
    auto scanner = create_parquet_scanner("UTC", desc_tbl, {}, ranges);
    auto check = [](const ChunkPtr& chunk) {
        auto& columns = chunk->columns();
        for (auto& col : columns) {
            ASSERT_TRUE(col->is_nullable() && !col->is_constant());
        }
    };
    validate(scanner, 36865, check);
    ASSERT_EQ(_file_names.size(), scanner->TEST_scanner_counter()->num_files_read);
}

TEST_F(ParquetScannerTest, test_parquet_data_with_1_column_from_path) {
    std::vector<std::string> columns_from_file = {
            "col_datetime", "col_char",   "col_varchar",      "col_boolean",       "col_tinyint",      "col_smallint",
            "col_int",      "col_bigint", "col_decimal_p6s2", "col_decimal_p14s5", "col_decimal_p27s9"};
    auto varchar_type = TypeDescriptor::from_logical_type(TYPE_VARCHAR);
    auto date_type = TypeDescriptor::from_logical_type(TYPE_DATE);
    std::vector<std::string> columns_from_path = {"col_date"};
    std::vector<std::string> column_values = {"2021-03-22"};
    auto column_ref_expr = create_column_ref(11, varchar_type, true);
    auto cast_expr = create_cast_expr(column_ref_expr, date_type);

    auto dst_slot_exprs = std::unordered_map<size_t, starrocks::TExpr>{{11, cast_expr}};
    test_column_from_path<true>(columns_from_file, columns_from_path, column_values, dst_slot_exprs);
    test_column_from_path<false>(columns_from_file, columns_from_path, column_values, dst_slot_exprs);
}

TEST_F(ParquetScannerTest, test_parquet_data_with_2_column_from_path) {
    std::vector<std::string> columns_from_file = {
            "col_char", "col_varchar", "col_boolean",      "col_tinyint",       "col_smallint",
            "col_int",  "col_bigint",  "col_decimal_p6s2", "col_decimal_p14s5", "col_decimal_p27s9"};
    std::vector<std::string> columns_from_path = {"col_date", "col_datetime"};
    std::vector<std::string> column_values = {"2021-02-22", "2020-12-20 22:56:04"};

    auto varchar_type = TypeDescriptor::from_logical_type(TYPE_VARCHAR);
    auto date_type = TypeDescriptor::from_logical_type(TYPE_DATE);
    auto datetime_type = TypeDescriptor::from_logical_type(TYPE_DATETIME);

    auto column_ref_expr10 = create_column_ref(10, varchar_type, true);
    auto column_ref_expr11 = create_column_ref(11, varchar_type, true);

    auto cast_expr10 = create_cast_expr(column_ref_expr10, date_type);
    auto cast_expr11 = create_cast_expr(column_ref_expr11, datetime_type);

    auto dst_slot_exprs = std::unordered_map<size_t, starrocks::TExpr>{
            {10, cast_expr10},
            {11, cast_expr11},
    };

    test_column_from_path<true>(columns_from_file, columns_from_path, column_values, dst_slot_exprs);
    test_column_from_path<false>(columns_from_file, columns_from_path, column_values, dst_slot_exprs);
}

TEST_F(ParquetScannerTest, test_parquet_data_with_3_column_from_path) {
    std::vector<std::string> columns_from_file = {"col_varchar",      "col_boolean",       "col_tinyint",
                                                  "col_smallint",     "col_int",           "col_bigint",
                                                  "col_decimal_p6s2", "col_decimal_p14s5", "col_decimal_p27s9"};
    std::vector<std::string> columns_from_path = {"col_date", "col_datetime", "col_char"};

    auto varchar_type = TypeDescriptor::from_logical_type(TYPE_VARCHAR);
    auto date_type = TypeDescriptor::from_logical_type(TYPE_DATE);
    auto datetime_type = TypeDescriptor::from_logical_type(TYPE_DATETIME);

    auto column_ref_expr9 = create_column_ref(9, varchar_type, true);
    auto column_ref_expr10 = create_column_ref(10, varchar_type, true);
    auto column_ref_expr11 = create_column_ref(11, varchar_type, true);

    auto cast_expr9 = create_cast_expr(column_ref_expr9, date_type);
    auto cast_expr10 = create_cast_expr(column_ref_expr10, datetime_type);

    auto dst_slot_exprs = std::unordered_map<size_t, starrocks::TExpr>{
            {9, cast_expr9},
            {10, cast_expr10},
            {11, column_ref_expr11},
    };

    std::vector<std::string> column_values = {"2021-02-22", "2020-12-20 22:56:04", "beijing"};
    test_column_from_path<true>(columns_from_file, columns_from_path, column_values, dst_slot_exprs);
    test_column_from_path<false>(columns_from_file, columns_from_path, column_values, dst_slot_exprs);
}

TEST_F(ParquetScannerTest, test_to_json) {
    // std::vector<std::string> columns = {"col_int", "col_bool", "col_double", "col_string", "col_null", "col_map", "col_list"};
    const std::string parquet_file_name = test_exec_dir + "/test_data/parquet_data/data_json.parquet";
    // TODO(mofei) read struct-type field from parquet has some issues related with FileReader::GetRecordBatchReader,
    // which does not return correct column data
    std::vector<std::tuple<std::string, std::vector<std::string>>> test_cases = {
            {"col_json_int8", {"1", "2", "3"}},
            {"col_json_int16", {"1", "2", "3"}},
            {"col_json_int32", {"1", "2", "3"}},
            {"col_json_int64", {"1", "2", "3"}},
            {"col_json_uint8", {"1", "2", "3"}},
            {"col_json_uint16", {"1", "2", "3"}},
            {"col_json_int32", {"1", "2", "3"}},
            {"col_json_uint64", {"1", "2", "3"}},
            {"col_json_timestamp", {"1659962123000", "1659962124000", "1659962125000"}},
            {"col_json_timestamp_not_normalized", {"1659962123000", "1659962124000", "1659962125000"}},

            {"col_json_float32", {"1.100000023841858", "2.0999999046325684", "3.0999999046325684"}},
            {"col_json_float64", {"1.1", "2.1", "3.1"}},

            {"col_json_bool", {"true", "false", "true"}},
            {"col_json_string", {"\"s1\"", "\"s2\"", "\"s3\""}},
            {"col_json_list", {"[1, 2]", "[3, 4]", "[5, 6]"}},
            {"col_json_map", {R"({"s1": 1, "s2": 3})", "{\"s2\": 2}", "{\"s3\": 3}"}},
            {"col_json_map_timestamp", {"{\"1659962123000\": 1}", "{\"1659962124000\": 2}", "{\"1659962125000\": 3}"}},
            {"col_json_struct",
             {R"({ "s0": 1, "s1": "string1" }                                                    )",
              R"( {"s0": 2, "s1": "string2"}                                                     )",
              R"({ "s0": 3, "s1": "string3" }                                                    )"}},
            {"col_json_list_list",
             {"[[1,2,3], [7,8,9], [10,11,12]]                                                    ",
              "[[4,5,6], [7,8,9], [12,13,14]]                                                    ",
              "[[4,5,6], [7,8,9], [12,13,14]]                                                    "}},
            {"col_json_list_struct",
             {R"([{"s0": 1, "s1": "string1"}, {"s0": 2, "s1": "string2" } ]                     )",
              R"( [{"s0": 1, "s1": "string1"} ]                                                 )",
              R"( [{"s0": 1, "s1": "string3"} ]                                                 )"}},
            {"col_json_map_list",
             {R"({"s1": [1,2], "s2": [3,4]}                                                     )",
              R"({"s1": [5,6]}                                                                  )",
              R"({"s1": [5,6]}                                                                  )"}},
            {"col_json_struct_struct",
             {R"({"s0": 1, "s1": {"s2": 3}}                                                     )",
              R"({"s0": 2, "s1": {"s2": 4}}                                                     )",
              R"({ "s0": 3, "s1": {"s2": 5}}                                                    )"}},

            {"col_json_struct_string",
             {R"('{"s0": 1, "s1": "string1"}'                                                    )",
              R"('{"s0": 2, "s1": "string2"}'                                                     )",
              R"('{"s0": 3, "s1": "string3"}'                                                    )"}},

            {"col_json_json_string",
             {R"({"s1": 1}                                                    )",
              R"({"s2": 2}                                                     )",
              R"({"s3": 3}                                                    )"}},

    };
    std::vector<std::string> columns_from_path;
    std::vector<std::string> path_values;
    std::unordered_map<size_t, TExpr> slot_map;

    for (auto& [column_name, expected] : test_cases) {
        std::vector<std::string> column_names{column_name};
        std::cerr << "test " << column_name << std::endl;

        ChunkPtr chunk = get_chunk<true>(column_names, slot_map, parquet_file_name, 3);
        ASSERT_EQ(1, chunk->num_columns());

        auto col = chunk->columns()[0];
        for (int i = 0; i < col->size(); i++) {
            std::string result = col->debug_item(i);
            std::string expect = expected[i];
            expect.erase(std::remove(expect.begin(), expect.end(), ' '), expect.end());
            result.erase(std::remove(result.begin(), result.end(), ' '), result.end());
            EXPECT_EQ(expect, result);
        }
    }
}

TEST_F(ParquetScannerTest, test_selected_parquet_data) {
    auto column_names = std::vector<std::string>{
            "col_date",     "col_datetime", "col_char",   "col_varchar",      "col_boolean",       "col_tinyint",
            "col_smallint", "col_int",      "col_bigint", "col_decimal_p6s2", "col_decimal_p14s5", "col_decimal_p27s9",
    };
    auto slot_infos = select_columns(column_names, false);
    auto ranges = generate_split_ranges(_file_names, _file_sizes, slot_infos.size(), {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {slot_infos, {}});
    auto scanner = create_parquet_scanner("UTC", desc_tbl, {}, ranges);
    auto check = [](const ChunkPtr& chunk) {
        auto& columns = chunk->columns();
        for (auto& col : columns) {
            ASSERT_TRUE(col->is_nullable() && !col->is_constant());
        }
    };
    validate(scanner, 36865, check);
}

TEST_F(ParquetScannerTest, test_dictionary_string_parquet_data) {
    std::string parquet_file_name;
    create_dictionary_string_parquet("dictionary_string.parquet", {"Center", "Edge", "Middle", "Center"},
                                     &parquet_file_name);
    DeferOp defer([&]() { std::filesystem::remove(parquet_file_name); });

    SlotTypeDescInfoArray slot_infos = {{"zone", TypeDescriptor::create_varchar_type(1048576), true}};
    auto ranges = generate_ranges({parquet_file_name}, slot_infos.size(), {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {slot_infos, {}});
    auto scanner = create_parquet_scanner("UTC", desc_tbl, {}, ranges);
    auto check = [](const ChunkPtr& chunk) {
        ASSERT_EQ(1, chunk->num_columns());
        ASSERT_EQ(4, chunk->num_rows());
        auto col = chunk->columns()[0];
        EXPECT_EQ("'Center'", col->debug_item(0));
        EXPECT_EQ("'Edge'", col->debug_item(1));
        EXPECT_EQ("'Middle'", col->debug_item(2));
        EXPECT_EQ("'Center'", col->debug_item(3));
    };
    validate(scanner, 4, check);
}

TEST_F(ParquetScannerTest, test_dictionary_string_parquet_data_with_small_chunk_size) {
    std::string parquet_file_name;
    create_dictionary_string_parquet("dictionary_string_small_chunk.parquet",
                                     {"Center", "Edge", "Middle", "Center", "North"}, &parquet_file_name);
    DeferOp defer([&]() { std::filesystem::remove(parquet_file_name); });

    SlotTypeDescInfoArray slot_infos = {{"zone", TypeDescriptor::create_varchar_type(1048576), true}};
    auto ranges = generate_ranges({parquet_file_name}, slot_infos.size(), {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {slot_infos, {}});
    auto scanner = create_parquet_scanner("UTC", desc_tbl, {}, ranges, 2);

    std::vector<std::string> actual_values;
    std::vector<size_t> chunk_rows;
    auto check = [&](const ChunkPtr& chunk) {
        ASSERT_EQ(1, chunk->num_columns());
        chunk_rows.emplace_back(chunk->num_rows());
        auto col = chunk->columns()[0];
        for (size_t i = 0; i < col->size(); ++i) {
            actual_values.emplace_back(col->debug_item(i));
        }
    };
    validate(scanner, 5, check);

    ASSERT_EQ(std::vector<size_t>({2, 2, 1}), chunk_rows);
    ASSERT_EQ((std::vector<std::string>{"'Center'", "'Edge'", "'Middle'", "'Center'", "'North'"}), actual_values);
}

TEST_F(ParquetScannerTest, test_nested_dictionary_string_parquet) {
    std::string parquet_file_name;
    create_nested_dictionary_parquet("nested_dictionary.parquet", {{"a", "b"}, {"c"}}, &parquet_file_name);
    DeferOp defer([&]() { std::filesystem::remove(parquet_file_name); });

    TypeDescriptor type_list(TYPE_ARRAY);
    type_list.children.emplace_back(TYPE_VARCHAR);
    type_list.children.back().len = 1048576;

    SlotTypeDescInfoArray slot_infos = {{"nested_dict", type_list, true}};
    auto ranges = generate_ranges({parquet_file_name}, slot_infos.size(), {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {slot_infos, {}});
    auto scanner = create_parquet_scanner("UTC", desc_tbl, {}, ranges);

    auto check = [](const ChunkPtr& chunk) {
        ASSERT_EQ(1, chunk->num_columns());
        ASSERT_EQ(2, chunk->num_rows());
        auto col = chunk->columns()[0];
        EXPECT_EQ("['a','b']", col->debug_item(0));
        EXPECT_EQ("['c']", col->debug_item(1));
    };
    validate(scanner, 2, check);
}

TEST_F(ParquetScannerTest, test_struct_dictionary_string_parquet) {
    std::string parquet_file_name;
    create_struct_dictionary_parquet("struct_dictionary.parquet", {"a", "b"}, &parquet_file_name);
    DeferOp defer([&]() { std::filesystem::remove(parquet_file_name); });

    auto type_struct = TypeDescriptor::create_struct_type({"name"}, {TypeDescriptor::create_varchar_type(1048576)});
    SlotTypeDescInfoArray slot_infos = {{"struct_dict", type_struct, true}};
    auto ranges = generate_ranges({parquet_file_name}, slot_infos.size(), {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {slot_infos, {}});
    auto scanner = create_parquet_scanner("UTC", desc_tbl, {}, ranges);

    auto check = [](const ChunkPtr& chunk) {
        ASSERT_EQ(1, chunk->num_columns());
        ASSERT_EQ(2, chunk->num_rows());
        auto col = chunk->columns()[0];
        EXPECT_EQ("{name:'a'}", col->debug_item(0));
        EXPECT_EQ("{name:'b'}", col->debug_item(1));
    };
    validate(scanner, 2, check);
}

TEST_F(ParquetScannerTest, test_map_dictionary_string_parquet) {
    std::string parquet_file_name;
    create_map_dictionary_parquet("map_dictionary.parquet", {{{"k1", "a"}, {"k2", "b"}}, {{"k3", "c"}}},
                                  &parquet_file_name);
    DeferOp defer([&]() { std::filesystem::remove(parquet_file_name); });

    auto type_map = TypeDescriptor::create_map_type(TypeDescriptor::create_varchar_type(1048576),
                                                    TypeDescriptor::create_varchar_type(1048576));
    SlotTypeDescInfoArray slot_infos = {{"map_dict", type_map, true}};
    auto ranges = generate_ranges({parquet_file_name}, slot_infos.size(), {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {slot_infos, {}});
    auto scanner = create_parquet_scanner("UTC", desc_tbl, {}, ranges);

    auto check = [](const ChunkPtr& chunk) {
        ASSERT_EQ(1, chunk->num_columns());
        ASSERT_EQ(2, chunk->num_rows());
        auto col = chunk->columns()[0];
        EXPECT_EQ("{'k1':'a','k2':'b'}", col->debug_item(0));
        EXPECT_EQ("{'k3':'c'}", col->debug_item(1));
    };
    validate(scanner, 2, check);
}

TEST_F(ParquetScannerTest, test_arrow_null) {
    std::vector<std::string> column_names{"col_int_null", "col_string_null"};
    std::string parquet_file_name = test_exec_dir + "/test_data/parquet_data/data_null.parquet";
    std::vector<std::string> file_names{parquet_file_name};

    auto slot_infos = select_columns(column_names, true);
    auto ranges = generate_ranges(file_names, column_names.size(), {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {slot_infos, slot_infos});
    auto scanner = create_parquet_scanner("UTC", desc_tbl, {}, ranges);
    auto check = [](const ChunkPtr& chunk) {};
    validate(scanner, 3, check);
    // single file split
    ASSERT_EQ(file_names.size(), scanner->TEST_scanner_counter()->num_files_read);
}

TEST_F(ParquetScannerTest, int96_timestamp) {
    const std::string parquet_file_name = test_exec_dir + "/test_data/parquet_data/int96_timestamp.parquet";
    std::vector<std::tuple<std::string, std::vector<std::string>>> test_cases = {
            {"col_datetime", {"9999-12-31 23:59:59.009999", "2006-01-02 15:04:05"}}};

    std::vector<std::string> columns_from_path;
    std::vector<std::string> path_values;
    std::unordered_map<size_t, TExpr> slot_map;

    for (auto& [column_name, expected] : test_cases) {
        std::vector<std::string> column_names{column_name};

        ChunkPtr chunk = get_chunk<true>(column_names, slot_map, parquet_file_name, 2);
        ASSERT_EQ(1, chunk->num_columns());

        auto col = chunk->columns()[0];
        for (int i = 0; i < col->size(); i++) {
            std::string result = col->debug_item(i);
            std::string expect = expected[i];
            EXPECT_EQ(expect, result);
        }
    }
}

TEST_F(ParquetScannerTest, get_file_schema) {
    const std::vector<std::pair<std::string, std::vector<std::pair<std::string, TypeDescriptor>>>> test_cases = {
            {test_exec_dir + "/test_data/parquet_data/int96_timestamp.parquet",
             {{"col_datetime", TypeDescriptor::from_logical_type(TYPE_DATETIME)}}},
            {test_exec_dir + "/test_data/parquet_data/data_json.parquet",
             {{"col_json_int8", TypeDescriptor::from_logical_type(TYPE_INT)},
              {"col_json_int16", TypeDescriptor::from_logical_type(TYPE_INT)},
              {"col_json_int32", TypeDescriptor::from_logical_type(TYPE_INT)},
              {"col_json_int64", TypeDescriptor::from_logical_type(TYPE_BIGINT)},
              {"col_json_uint8", TypeDescriptor::from_logical_type(TYPE_INT)},
              {"col_json_uint16", TypeDescriptor::from_logical_type(TYPE_INT)},
              {"col_json_uint32", TypeDescriptor::from_logical_type(TYPE_INT)},
              {"col_json_uint64", TypeDescriptor::from_logical_type(TYPE_BIGINT)},
              {"col_json_timestamp", TypeDescriptor::from_logical_type(TYPE_DATETIME)},
              {"col_json_timestamp_not_normalized", TypeDescriptor::from_logical_type(TYPE_DATETIME)},
              {"col_json_float32", TypeDescriptor::from_logical_type(TYPE_FLOAT)},
              {"col_json_float64", TypeDescriptor::from_logical_type(TYPE_DOUBLE)},
              {"col_json_bool", TypeDescriptor::from_logical_type(TYPE_BOOLEAN)},
              {"col_json_string", TypeDescriptor::create_varchar_type(1048576)},
              {"col_json_list", TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_INT))},
              {"col_json_map", TypeDescriptor::create_map_type(TypeDescriptor::from_logical_type(TYPE_VARCHAR),
                                                               TypeDescriptor::from_logical_type(TYPE_INT))},
              {"col_json_map_timestamp",
               TypeDescriptor::create_map_type(TypeDescriptor::from_logical_type(TYPE_DATETIME),
                                               TypeDescriptor::from_logical_type(TYPE_INT))},
              {"col_json_struct",
               TypeDescriptor::create_struct_type({"s0", "s1"}, {TypeDescriptor::from_logical_type(TYPE_INT),
                                                                 TypeDescriptor::create_varchar_type(1048576)})},
              {"col_json_list_list", TypeDescriptor::create_array_type(TypeDescriptor::create_array_type(
                                             TypeDescriptor::from_logical_type(TYPE_INT)))},
              {"col_json_map_list",
               TypeDescriptor::create_map_type(
                       TypeDescriptor::create_varchar_type(1048576),
                       TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_INT)))},
              {"col_json_list_struct", TypeDescriptor::create_array_type(TypeDescriptor::create_struct_type(
                                               {"s0", "s1"}, {TypeDescriptor::from_logical_type(TYPE_INT),
                                                              TypeDescriptor::create_varchar_type(1048576)}))},
              {"col_json_struct_struct",
               TypeDescriptor::create_struct_type(
                       {"s0", "s1"},
                       {TypeDescriptor::from_logical_type(TYPE_INT),
                        TypeDescriptor::create_struct_type({"s2"}, {TypeDescriptor::from_logical_type(TYPE_INT)})})},
              {"col_json_struct_string",
               TypeDescriptor::create_struct_type({"s0", "s1"}, {TypeDescriptor::from_logical_type(TYPE_INT),
                                                                 TypeDescriptor::create_varchar_type(1048576)})},
              {"col_json_json_string", TypeDescriptor::create_varchar_type(1048576)}}},
            {test_exec_dir + "/test_data/parquet_data/decimal.parquet",
             {{"col_decimal32", TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 9, 2)},
              {"col_decimal64", TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 2)},
              {"col_decimal128_byte_array", TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 2)},
              {"col_decimal128_fixed_len_byte_array", TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 2)}}},
            {test_exec_dir + "/test_data/parquet_data/nested.parquet",
             {{"col_int", TypeDescriptor::from_logical_type(TYPE_BIGINT)},
              {"col_list_int", TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_INT))},
              {"col_list_list_int", TypeDescriptor::create_array_type(TypeDescriptor::create_array_type(
                                            TypeDescriptor::from_logical_type(TYPE_INT)))},
              {"col_map_string_int", TypeDescriptor::create_map_type(TypeDescriptor::create_varchar_type(1048576),
                                                                     TypeDescriptor::from_logical_type(TYPE_INT))},
              {"col_map_map_string_int",
               TypeDescriptor::create_map_type(
                       TypeDescriptor::create_varchar_type(1048576),
                       TypeDescriptor::create_map_type(TypeDescriptor::create_varchar_type(1048576),
                                                       TypeDescriptor::from_logical_type(TYPE_INT)))},
              {"col_list_map_string_int",
               TypeDescriptor::create_array_type(TypeDescriptor::create_map_type(
                       TypeDescriptor::create_varchar_type(1048576), TypeDescriptor::from_logical_type(TYPE_INT)))},
              {"col_map_string_list_int",
               TypeDescriptor::create_map_type(
                       TypeDescriptor::create_varchar_type(1048576),
                       TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_INT)))}}}};

    for (const auto& test_case : test_cases) {
        check_schema(test_case.first, test_case.second);
    }
}

TEST_F(ParquetScannerTest, datetime) {
    const std::string parquet_file_name = test_exec_dir + "/test_data/parquet_data/datetime.parquet";
    std::vector<std::tuple<std::string, std::vector<std::string>>> test_cases = {
            {"col_datetime",
             {"2006-01-02 15:04:05", "2006-01-02 15:04:05.900000", "2006-01-02 15:04:05.999900",
              "2006-01-02 15:04:05.999990", "2006-01-02 15:04:05.999999"}}};

    std::vector<std::string> columns_from_path;
    std::vector<std::string> path_values;
    std::unordered_map<size_t, TExpr> slot_map;

    for (auto& [column_name, expected] : test_cases) {
        std::vector<std::string> column_names{column_name};

        ChunkPtr chunk = get_chunk<true>(column_names, slot_map, parquet_file_name, 5);
        ASSERT_EQ(1, chunk->num_columns());

        auto col = chunk->columns()[0];
        for (int i = 0; i < col->size(); i++) {
            std::string result = col->debug_item(i);
            std::string expect = expected[i];
            EXPECT_EQ(expect, result);
        }
    }
}

TEST_F(ParquetScannerTest, optional_map_key) {
    const std::string parquet_file_name = test_exec_dir + "/test_data/parquet_data/optional_map_key.parquet";
    std::vector<std::tuple<std::string, std::vector<std::string>>> test_cases = {
            {"col_int", {"1", "2", "6", "3", "4", "5", "7", "8", "9", "1", "2", "3", "4", "5", "7", "8", "9", "6"}},
            {"col_map",
             {"{' ':' '}",
              "{'                                            aAbBcC':'                                            "
              "aAbBcC'}",
              "{'你好，中国！':NULL}",
              "{'aAbBcC                                            ':'aAbBcC                                           "
              " '}",
              "{'                    aAbBcCdDeE                    ':'                    aAbBcCdDeE                   "
              " '}",
              "{'null':NULL}",
              "{'                                                  ':'                                                 "
              " '}",
              "{'Hello, world!你好':'Hello, world!你好'}",
              "{'Total MapReduce CPU Time Spent: 2 seconds 120 msec':'Total MapReduce CPU Time Spent: 2 seconds 120 "
              "msec'}",
              "{' ':' '}",
              "{'                                            aAbBcC':'                                            "
              "aAbBcC'}",
              "{'aAbBcC                                            ':'aAbBcC                                           "
              " '}",
              "{'                    aAbBcCdDeE                    ':'                    aAbBcCdDeE                   "
              " '}",
              "{'null':NULL}",
              "{'                                                  ':'                                                 "
              " '}",
              "{'Hello, world!你好':'Hello, world!你好'}",
              "{'Total MapReduce CPU Time Spent: 2 seconds 120 msec':'Total MapReduce CPU Time Spent: 2 seconds 120 "
              "msec'}",
              "{'你好，中国！':NULL}"}}};

    std::vector<std::string> columns_from_path;
    std::vector<std::string> path_values;
    std::unordered_map<size_t, TExpr> slot_map;

    for (auto& [column_name, expected] : test_cases) {
        std::vector<std::string> column_names{column_name};

        ChunkPtr chunk = get_chunk<true>(column_names, slot_map, parquet_file_name, 18);
        ASSERT_EQ(1, chunk->num_columns());

        auto col = chunk->columns()[0];
        for (int i = 0; i < col->size(); i++) {
            std::string result = col->debug_item(i);
            std::string expect = expected[i];
            EXPECT_EQ(expect, result);
        }
    }
}

} // namespace starrocks
