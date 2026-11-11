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

#include "connector/file/scanner/json_scanner.h"

#include <gtest/gtest.h>
#include <zlib.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <random>
#include <sstream>
#include <utility>

#include "base/testutil/assert.h"
#include "base/testutil/parallel_test.h"
#include "base/uid_util.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "common/config_exec_flow_fwd.h"
#include "common/config_exec_fwd.h"
#include "compute_env/load/load_stream_mgr.h"
#include "compute_env/load/stream_load_pipe.h"
#include "compute_env/load_path/base_load_path_mgr.h"
#include "fs/fs_util.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "rapidjson/document.h"
#include "runtime/byte_buffer.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/service_contexts.h"
#include "testutil/type_test_helper.h"

namespace starrocks {

class JsonScannerTest : public ::testing::Test {
protected:
    std::unique_ptr<JsonScanner> create_json_scanner(const std::vector<TypeDescriptor>& types,
                                                     const std::vector<TBrokerRangeDesc>& ranges,
                                                     const std::vector<std::string>& col_names,
                                                     size_t file_size_limit = 1024 * 1024,
                                                     const std::vector<TRoutineLoadMetaColumn>& meta_cols = {}) {
        /// Init DescriptorTable
        TDescriptorTableBuilder desc_tbl_builder;
        TTupleDescriptorBuilder tuple_desc_builder;
        for (int i = 0; i < types.size(); ++i) {
            TSlotDescriptorBuilder slot_desc_builder;
            slot_desc_builder.type(types[i]).column_name(col_names[i]).length(types[i].len).nullable(true);
            tuple_desc_builder.add_slot(slot_desc_builder.build());
        }
        tuple_desc_builder.build(&desc_tbl_builder);

        DescriptorTbl* desc_tbl = nullptr;
        Status st = DescriptorTbl::create(_state.get(), &_pool, desc_tbl_builder.desc_tbl(), &desc_tbl,
                                          config::vector_chunk_size);
        CHECK(st.ok()) << st.to_string();

        /// Init RuntimeState
        _state->set_desc_tbl(desc_tbl);
        _state->init_instance_mem_tracker();

        /// TBrokerScanRangeParams
        TBrokerScanRangeParams* params = _pool.add(new TBrokerScanRangeParams());
        params->strict_mode = true;
        params->dest_tuple_id = 0;
        params->src_tuple_id = 0;
        params->json_file_size_limit = file_size_limit;
        for (int i = 0; i < types.size(); i++) {
            params->expr_of_dest_slot[i] = TExpr();
            params->expr_of_dest_slot[i].nodes.emplace_back(TExprNode());
            params->expr_of_dest_slot[i].nodes[0].__set_type(types[i].to_thrift());
            params->expr_of_dest_slot[i].nodes[0].__set_node_type(TExprNodeType::SLOT_REF);
            params->expr_of_dest_slot[i].nodes[0].__set_is_nullable(true);
            params->expr_of_dest_slot[i].nodes[0].__set_slot_ref(TSlotRef());
            params->expr_of_dest_slot[i].nodes[0].slot_ref.__set_slot_id(i);
            params->expr_of_dest_slot[i].nodes[0].__set_type(types[i].to_thrift());
        }

        for (int i = 0; i < types.size(); i++) {
            params->src_slot_ids.emplace_back(i);
        }

        if (!meta_cols.empty()) {
            params->__set_stream_source_meta_columns(meta_cols);
        }

        TBrokerScanRange* broker_scan_range = _pool.add(new TBrokerScanRange());
        broker_scan_range->params = *params;
        broker_scan_range->ranges = ranges;
        auto scanner = std::make_unique<JsonScanner>(_state.get(), _profile, *broker_scan_range, _counter);
        EXPECT_EQ("json", scanner->file_format());
        // scan_type is not set in TBrokerScanRangeParams, default to LOAD
        EXPECT_EQ("load", scanner->scan_type());
        return scanner;
    }

    ChunkPtr test_whole_row_json(int columns, const std::string& input_data, std::string jsonpath,
                                 const std::vector<std::string>& jsonpaths) {
        std::vector<TypeDescriptor> types;
        for (int i = 0; i < columns; i++) {
            types.emplace_back(TYPE_VARCHAR_DESC);
        }

        std::vector<TBrokerRangeDesc> ranges;
        TBrokerRangeDesc range;
        range.format_type = TFileFormatType::FORMAT_JSON;
        range.file_type = TFileType::FILE_LOCAL;
        range.strip_outer_array = false;
        range.__isset.strip_outer_array = false;
        range.__isset.jsonpaths = true;
        range.jsonpaths = std::move(jsonpath);
        range.__isset.json_root = false;
        range.__set_path(input_data);
        ranges.emplace_back(range);

        auto scanner = create_json_scanner(types, ranges, jsonpaths);

        EXPECT_OK(scanner->open());

        ChunkPtr chunk = scanner->get_next().value();
        return chunk;
    }

    ChunkPtr test_with_jsonpath(int columns, const std::string& input_data, std::string jsonpath,
                                const std::vector<std::string>& jsonpaths) {
        std::vector<TypeDescriptor> types;
        for (int i = 0; i < columns; i++) {
            types.emplace_back(TypeDescriptor::create_json_type());
        }

        std::vector<TBrokerRangeDesc> ranges;
        TBrokerRangeDesc range;
        range.format_type = TFileFormatType::FORMAT_JSON;
        range.file_type = TFileType::FILE_LOCAL;
        range.strip_outer_array = false;
        range.__isset.strip_outer_array = false;
        range.__isset.jsonpaths = true;
        range.jsonpaths = std::move(jsonpath);
        range.__isset.json_root = false;
        range.__set_path(input_data);
        ranges.emplace_back(range);

        auto scanner = create_json_scanner(types, ranges, jsonpaths);

        EXPECT_OK(scanner->open());

        ChunkPtr chunk = scanner->get_next().value();
        return chunk;
    }

    std::shared_ptr<RuntimeState> create_runtime_state() {
        TQueryOptions query_options;
        TUniqueId fragment_id;
        TQueryGlobals query_globals;
        std::shared_ptr<RuntimeState> runtime_state = std::make_shared<RuntimeState>(
                fragment_id, query_options, query_globals, &_query_execution_services, nullptr);
        TUniqueId id;
        runtime_state->init_mem_trackers(id);
        return runtime_state;
    }

    static void write_json_to_file(std::string filename, std::string json_data) {
        std::ofstream ofs;
        ofs.open(filename, std::ofstream::out);
        ofs << json_data;
        ofs.close();
    }

    // Helper function to create a large JSON array with random data
    static std::string generate_large_json_array(size_t num_records, size_t target_size_bytes) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> int_dist(1, 1000000);
        std::uniform_real_distribution<> double_dist(1.0, 1000000.0);

        std::vector<std::string> words = {"apple", "banana", "cherry",   "date", "elderberry",
                                          "fig",   "grape",  "honeydew", "kiwi", "lemon"};
        std::uniform_int_distribution<> word_dist(0, words.size() - 1);

        std::ostringstream json_stream;
        json_stream << "[";

        size_t current_size = 1; // Start with "["

        for (size_t i = 0; i < num_records && current_size < target_size_bytes; ++i) {
            if (i > 0) json_stream << ",";

            // Create a record with multiple fields to increase size
            json_stream << "{";
            json_stream << R"("id":)" << int_dist(gen) << ",";
            json_stream << R"("name":")" << words[word_dist(gen)] << "_" << int_dist(gen) << R"(",)";
            json_stream << R"("value":)" << double_dist(gen) << ",";
            json_stream << R"("timestamp":)" << int_dist(gen) << ",";
            json_stream << R"("category":")" << words[word_dist(gen)] << R"(",)";
            json_stream << R"("score":)" << double_dist(gen) << ",";
            json_stream << R"("active":)" << (int_dist(gen) % 2 ? "true" : "false") << ",";
            json_stream << R"("tags":[")" << words[word_dist(gen)] << R"(",")" << words[word_dist(gen)] << R"("])";
            json_stream << "}";

            current_size = json_stream.str().length();
        }

        json_stream << "]";
        return json_stream.str();
    }

    // Helper function to compress data using gzip
    static std::string compress_gzip(const std::string& data) {
        z_stream strm;
        strm.zalloc = Z_NULL;
        strm.zfree = Z_NULL;
        strm.opaque = Z_NULL;

        if (deflateInit2(&strm, Z_NO_COMPRESSION, Z_DEFLATED, 31, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
            return "";
        }

        strm.avail_in = data.size();
        strm.next_in = (Bytef*)data.data();

        std::string compressed;
        char out_buffer[4096];

        do {
            strm.avail_out = sizeof(out_buffer);
            strm.next_out = (Bytef*)out_buffer;

            int ret = deflate(&strm, Z_FINISH);
            if (ret == Z_STREAM_ERROR) {
                deflateEnd(&strm);
                return "";
            }

            compressed.append(out_buffer, sizeof(out_buffer) - strm.avail_out);
        } while (strm.avail_out == 0);

        deflateEnd(&strm);
        return compressed;
    }

    // Helper function to create a temporary large compressed JSON file
    static std::string create_temp_large_compressed_json(size_t target_size_mb) {
        size_t target_size_bytes = target_size_mb * 1024 * 1024;

        // Generate large JSON data
        std::string json_data = generate_large_json_array(1000000, target_size_bytes);

        // Compress the data
        std::string compressed_data = compress_gzip(json_data);

        // Create temporary file path
        std::string temp_file = "/tmp/large_test_" + std::to_string(target_size_mb) + "MB.json.gz";

        // Write compressed data to temporary file
        std::ofstream ofs(temp_file, std::ios::binary);
        ofs.write(compressed_data.data(), compressed_data.size());
        ofs.close();

        return temp_file;
    }

    // Helper function to clean up temporary files
    static void cleanup_temp_file(const std::string& filepath) { std::remove(filepath.c_str()); }

    void SetUp() override {
        config::vector_chunk_size = 4096;
        _runtime_services.load_stream_mgr = &_load_stream_mgr;
        _query_execution_services.runtime = &_runtime_services;
        _profile = _pool.add(new RuntimeProfile("test"));
        _counter = _pool.add(new ScannerCounter());
        _state = create_runtime_state();
        std::string starrocks_home = getenv("STARROCKS_HOME");
    }

    void TearDown() override {}

private:
    RuntimeProfile* _profile = nullptr;
    ScannerCounter* _counter = nullptr;
    std::shared_ptr<RuntimeState> _state = nullptr;
    LoadStreamMgr _load_stream_mgr;
    RuntimeServices _runtime_services;
    QueryExecutionServices _query_execution_services;
    ObjectPool _pool;
};

// NOLINTNEXTLINE
TEST_F(JsonScannerTest, test_array_json) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);

    TypeDescriptor array_type(TYPE_ARRAY);
    array_type.children.emplace_back(TypeDescriptor::create_json_type());
    types.emplace_back(array_type);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test9.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"c1", "c2"});

    Status st = scanner->open();
    ASSERT_TRUE(st.ok());

    auto st2 = scanner->get_next();
    ASSERT_TRUE(st2.ok());

    ChunkPtr chunk = st2.value();
    ASSERT_EQ(chunk->columns()[0]->debug_string(), "[1]");
    ASSERT_EQ(chunk->columns()[1]->debug_string(), "[[{\"k2\": \"v2\"}]]");

    ASSERT_GT(scanner->TEST_scanner_counter()->file_read_count, 0);
    ASSERT_GT(scanner->TEST_scanner_counter()->file_read_ns, 0);
    ASSERT_EQ(ranges.size(), scanner->TEST_scanner_counter()->num_files_read);
}

TEST_F(JsonScannerTest, test_json_without_path) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_DOUBLE);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test1.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"category", "author", "title", "price"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(2, chunk->num_rows());

    EXPECT_EQ("['reference', 'NigelRees', 'SayingsoftheCentury', 8.95]", chunk->debug_row(0));
    EXPECT_EQ("['fiction', 'EvelynWaugh', 'SwordofHonour', 12.99]", chunk->debug_row(1));
    ASSERT_EQ(ranges.size(), scanner->TEST_scanner_counter()->num_files_read);
}

TEST_F(JsonScannerTest, test_debezium_json_load) {
    std::string filename = "./be/test/exec/test_data/json_scanner/debezium_test.json";
    std::string json_data = R"(
        {"payload": {"before": null, "after": {"id": 1, "name": "a"}, "op": "c"}}
        {"payload": {"before": {"id": 2, "name": "b"}, "after": null, "op": "d"}}
    )";
    write_json_to_file(filename, json_data);
    DeferOp defer([&] { std::remove(filename.c_str()); });

    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_TINYINT); // for __op

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.__isset.envelope = true;
    range.envelope = TEnvelopeType::DEBEZIUM;
    range.__set_path(filename);
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"id", "name", "__op"});

    ASSERT_OK(scanner->open());

    auto st2 = scanner->get_next();
    ASSERT_OK(st2);

    ChunkPtr chunk = st2.value();
    ASSERT_EQ(2, chunk->num_rows());
    ASSERT_EQ(3, chunk->num_columns());

    // Row 1: id=1, name=a, __op=0 (upsert)
    EXPECT_EQ("[1, 'a', 0]", chunk->debug_row(0));
    // Row 2: id=2, name=b, __op=1 (delete)
    EXPECT_EQ("[2, 'b', 1]", chunk->debug_row(1));
}

TEST_F(JsonScannerTest, test_debezium_json_load_with_jsonpath_shorter_than_slots) {
    std::string filename = "./be/test/exec/test_data/json_scanner/debezium_test_jsonpath_shorter.json";
    std::string json_data = R"({"payload": {"before": null, "after": {"id": 1, "name": "a"}, "op": "c"}})";
    write_json_to_file(filename, json_data);
    DeferOp defer([&] { std::remove(filename.c_str()); });

    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_TINYINT); // for __op

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.__isset.envelope = true;
    range.envelope = TEnvelopeType::DEBEZIUM;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.id", "$.name"])";
    range.__set_path(filename);
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"id", "name", "__op"});

    ASSERT_OK(scanner->open());
    auto st = scanner->get_next();
    ASSERT_OK(st);
    ChunkPtr chunk = st.value();

    ASSERT_EQ(1, chunk->num_rows());
    EXPECT_EQ("[1, 'a', 0]", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_debezium_json_load_with_missing_op_jsonpath) {
    std::string filename = "./be/test/exec/test_data/json_scanner/debezium_test_jsonpath_missing_op.json";
    std::string json_data = R"({"payload": {"before": {"id": 2, "name": "b"}, "after": null, "op": "d"}})";
    write_json_to_file(filename, json_data);
    DeferOp defer([&] { std::remove(filename.c_str()); });

    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_TINYINT); // for __op

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.__isset.envelope = true;
    range.envelope = TEnvelopeType::DEBEZIUM;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.id", "$.name", "$.missing_op"])";
    range.__set_path(filename);
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"id", "name", "__op"});

    ASSERT_OK(scanner->open());
    auto st = scanner->get_next();
    ASSERT_OK(st);
    ChunkPtr chunk = st.value();

    ASSERT_EQ(1, chunk->num_rows());
    EXPECT_EQ("[2, 'b', 1]", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_json_path_with_asterisk_basic) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.keyname.*", "$.k1", "$.kind"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test10.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"keyname", "k1", "kind"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(3, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[20, 'v2', 'server']", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_json_path_with_asterisk_null) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.keyname.*", "$.k1", "$.kind"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test11.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"keyname", "k1", "kind"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(3, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[NULL, 'v2', 'server']", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_json_path_with_asterisk_record) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.keyname.*.a", "$.k1", "$.kind"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test12.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"keyname", "k1", "kind"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(3, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[1, 'v2', 'server']", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_json_path_with_asterisk_array) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.keyname.*[0]", "$.k1", "$.kind"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test13.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"keyname", "k1", "kind"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(3, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[25, 'v2', 'server']", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_json_with_path) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.k1", "$.kind", "$.keyname.ip", "$.keyname.value"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test2.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(2, chunk->num_rows());

    EXPECT_EQ("['v1', 'server', '10.10.0.1', 20]", chunk->debug_row(0));
    EXPECT_EQ("['v2', 'server', '10.20.1.1', 20]", chunk->debug_row(1));
}

TEST_F(JsonScannerTest, test_invalid_json_path) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(invalid path)";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test2.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "ip", "value"});

    Status st = scanner->open();
    ASSERT_TRUE(st.is_data_quality_error());
    ASSERT_TRUE(st.message().find("parse error. Invalid json path") != std::string::npos);
}

TEST_F(JsonScannerTest, test_one_level_array) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_ARRAY_VARCHAR_DESC);

    types.emplace_back(TYPE_ARRAY_INT_DESC);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.keyname.ip", "$.keyname.value"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test3.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(2, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[['10.10.0.1','10.20.1.1'], [10,20]]", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_two_level_array) {
    std::vector<TypeDescriptor> types;
    TypeDescriptor t1(TYPE_ARRAY);
    t1.children.emplace_back(TYPE_ARRAY);
    t1.children.back().children.emplace_back(TYPE_BIGINT);
    types.emplace_back(t1);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test4.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(1, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[[[10,20],[30,40]]]", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_invalid_column_in_array) {
    std::vector<TypeDescriptor> types;
    TypeDescriptor t1(TYPE_ARRAY);
    t1.children.emplace_back(TYPE_ARRAY);
    t1.children.back().children.emplace_back(TYPE_SMALLINT);
    types.emplace_back(t1);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test5.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(1, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[[[NULL,20],[30,40]]]", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_invalid_nested_level1) {
    // the nested level in schema is larger than json
    std::vector<TypeDescriptor> types;
    TypeDescriptor t1(TYPE_ARRAY);
    t1.children.emplace_back(TYPE_ARRAY);
    t1.children.back().children.emplace_back(TYPE_TINYINT);
    types.emplace_back(t1);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test6.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();

    EXPECT_EQ(1, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[[NULL,NULL,NULL,NULL]]", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_invalid_nested_level2) {
    // the nested level in schema is less than json
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_ARRAY_LARGEINT_DESC);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test7.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"value"});

    ASSERT_OK(scanner->open());
    ASSIGN_OR_ASSERT_FAIL(auto chunk, scanner->get_next());
    EXPECT_EQ(1, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[[NULL,NULL]]", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_json_with_long_string) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test8.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"request", "ids"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(2, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("['{\"area\":\"beijing\",\"country\":\"china\"}', '[\"478472290\",\"478473274\"]']", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_ndjson) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test_ndjson.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ("['v1', 'server', NULL, NULL]", chunk->debug_row(0));
    EXPECT_EQ("['v2', 'server', NULL, NULL]", chunk->debug_row(1));
    EXPECT_EQ("['v3', 'server', NULL, NULL]", chunk->debug_row(2));
    EXPECT_EQ("['v4', 'server', NULL, NULL]", chunk->debug_row(3));
    EXPECT_EQ("['v5', 'server', NULL, NULL]", chunk->debug_row(4));
}

TEST_F(JsonScannerTest, test_ndjson_with_jsonpath) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.k1", "$.kind", "$.keyname.ip", "$.keyname.value"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test_ndjson.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ("['v1', 'server', '10.10.0.1', 10]", chunk->debug_row(0));
    EXPECT_EQ("['v2', 'server', '10.10.0.2', 20]", chunk->debug_row(1));
    EXPECT_EQ("['v3', 'server', '10.10.0.3', 30]", chunk->debug_row(2));
    EXPECT_EQ("['v4', 'server', '10.10.0.4', 40]", chunk->debug_row(3));
    EXPECT_EQ("['v5', 'server', '10.10.0.5', 50]", chunk->debug_row(4));
}

TEST_F(JsonScannerTest, test_json_new_parser) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.k1", "$.kind", "$.ip", "$.value"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test_json_new_parser.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(12, chunk->num_rows());

    EXPECT_EQ("['v1', 'server', '10.10.0.1', NULL]", chunk->debug_row(0));
    EXPECT_EQ("['v2', NULL, '10.10.0.2', 20]", chunk->debug_row(1));
    EXPECT_EQ("[NULL, 'server', '10.10.0.3', 30]", chunk->debug_row(2));
    EXPECT_EQ("[NULL, 'server', NULL, NULL]", chunk->debug_row(3));
    EXPECT_EQ("[NULL, NULL, NULL, 50]", chunk->debug_row(4));
    EXPECT_EQ("['v1', 'server', '10.10.0.1', 10]", chunk->debug_row(5));
    EXPECT_EQ("[NULL, NULL, NULL, 50]", chunk->debug_row(6));
    EXPECT_EQ("['v34', 'server2', '10.10.0.22', NULL]", chunk->debug_row(7));
    EXPECT_EQ("['v3', NULL, '10.10.0.33', 20]", chunk->debug_row(8));
    EXPECT_EQ("[NULL, 'server', '10.10.0.3', 30]", chunk->debug_row(9));
    EXPECT_EQ("[NULL, 'server', NULL, NULL]", chunk->debug_row(10));
    EXPECT_EQ("['v1', 'server', '10.10.0.1', 10]", chunk->debug_row(11));
}

TEST_F(JsonScannerTest, test_adaptive_nullable_column) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.k1", "$.kind", "$.ip", "$.value"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test_adaptive_nullable_column.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(6, chunk->num_rows());

    EXPECT_EQ("[NULL, NULL, NULL, NULL]", chunk->debug_row(0));
    EXPECT_EQ("['v2', NULL, NULL, NULL]", chunk->debug_row(1));
    EXPECT_EQ("[NULL, 'server', NULL, NULL]", chunk->debug_row(2));
    EXPECT_EQ("[NULL, NULL, '10.10.0.1', NULL]", chunk->debug_row(3));
    EXPECT_EQ("[NULL, NULL, NULL, 50]", chunk->debug_row(4));
    EXPECT_EQ("['v1', 'server', '10.10.0.1', 10]", chunk->debug_row(5));
}

TEST_F(JsonScannerTest, test_adaptive_nullable_column_2) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.k1", "$.kind", "$.ip", "$.value"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test_adaptive_nullable_column.json");
    ranges.emplace_back(range);
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(6, chunk->num_rows());

    EXPECT_EQ("[NULL, NULL, NULL, NULL]", chunk->debug_row(0));
    EXPECT_EQ("['v2', NULL, NULL, NULL]", chunk->debug_row(1));
    EXPECT_EQ("[NULL, 'server', NULL, NULL]", chunk->debug_row(2));
    EXPECT_EQ("[NULL, NULL, '10.10.0.1', NULL]", chunk->debug_row(3));
    EXPECT_EQ("[NULL, NULL, NULL, 50]", chunk->debug_row(4));
    EXPECT_EQ("['v1', 'server', '10.10.0.1', 10]", chunk->debug_row(5));

    chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(6, chunk->num_rows());
    EXPECT_EQ("[NULL, NULL, NULL, NULL]", chunk->debug_row(0));
    EXPECT_EQ("['v2', NULL, NULL, NULL]", chunk->debug_row(1));
    EXPECT_EQ("[NULL, 'server', NULL, NULL]", chunk->debug_row(2));
    EXPECT_EQ("[NULL, NULL, '10.10.0.1', NULL]", chunk->debug_row(3));
    EXPECT_EQ("[NULL, NULL, NULL, 50]", chunk->debug_row(4));
    EXPECT_EQ("['v1', 'server', '10.10.0.1', 10]", chunk->debug_row(5));
}

TEST_F(JsonScannerTest, test_multi_type) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_BOOLEAN);

    types.emplace_back(TYPE_TINYINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_BIGINT);
    // Numbers beyond range of uint64_t is not supported by json scanner.
    // Hence, we skip the test of LARGEINT.

    types.emplace_back(TYPE_FLOAT);
    types.emplace_back(TYPE_DOUBLE);

    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_DATE);
    types.emplace_back(TYPE_DATETIME);
    types.emplace_back(TYPE_ARRAY_INT_DESC);

    types.emplace_back(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 27, 9));
    types.emplace_back(TypeDescriptor::create_char_type(20));
    types.emplace_back(TYPE_TIME);
    types.emplace_back(TypeDescriptor::create_struct_type({"f_int", "f_string"}, {TYPE_INT_DESC, TYPE_VARCHAR_DESC}));
    types.emplace_back(TypeDescriptor::create_map_type(TYPE_VARCHAR_DESC, TYPE_DOUBLE_DESC));

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test_multi_type.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(
            types, ranges,
            {"f_bool", "f_tinyint", "f_smallint", "f_int", "f_bigint", "f_float", "f_double", "f_varchar", "f_date",
             "f_datetime", "f_array", "f_decimal", "f_char", "f_time", "f_struct", "f_map"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(16, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    auto expected =
            "[1, 127, 32767, 2147483647, 9223372036854775807, 3.14, 3.14, 'starrocks', 2021-12-09, 2021-12-09 "
            "10:00:00, [1,3,5], 1234565789012345678901234567.123456789, 'starrocks', 36000, {f_int:1,f_string:'a'}, "
            "{'f_double1':3.14,'f_double2':3.141}]";

    EXPECT_EQ(expected, chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_cast_type) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test_cast_type.json");
    ranges.emplace_back(range);

    auto scanner =
            create_json_scanner(types, ranges, {"f_float", "f_bool", "f_int", "f_float_in_string", "f_int_in_string"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(5, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("['3.14', '1', '123', 3.14, 123]", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_load_native_json) {
    std::vector<TypeDescriptor> types;
    constexpr int kNumColumns = 16;
    for (int i = 0; i < kNumColumns; i++) {
        types.emplace_back(TypeDescriptor::create_json_type());
    }

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test_json_load.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(
            types, ranges,
            {"f_bool", "f_tinyint", "f_smallint", "f_int", "f_bigint", "f_float", "f_double", "f_varchar", "f_date",
             "f_datetime", "f_array", "f_decimal", "f_char", "f_time", "f_nestarray", "f_object"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(kNumColumns, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    auto expected =
            "[true, 127, 32767, 2147483647, 9223372036854775807, 3.14, 3.14, \"starrocks\", "
            "\"2021-12-09\", \"2021-12-09 10:00:00\", [1, 3, 5], "
            "\"1234565789012345678901234567.123456789\", \"starrocks\", \"10:00:00\", "
            "[[{\"k1\": 1, \"k2\": 2}, {\"k3\": 3}], [{\"k1\": 4, \"k2\": 5}, {\"k3\": 6}]], "
            "{\"n1\": {\"n2\": {\"n3\": 1.1}}}]";

    EXPECT_EQ(expected, chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_native_json_ndjson_with_jsonpath) {
    std::vector<TypeDescriptor> types;
    constexpr int kColumns = 4;
    for (int i = 0; i < kColumns; i++) {
        types.emplace_back(TypeDescriptor::create_json_type());
    }

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.k1", "$.kind", "$.keyname.ip", "$.keyname.value"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test_ndjson.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ(R"(["v1", "server", "10.10.0.1", "10"])", chunk->debug_row(0));
    EXPECT_EQ(R"(["v2", "server", "10.10.0.2", "20"])", chunk->debug_row(1));
    EXPECT_EQ(R"(["v3", "server", "10.10.0.3", "30"])", chunk->debug_row(2));
    EXPECT_EQ(R"(["v4", "server", "10.10.0.4", "40"])", chunk->debug_row(3));
    EXPECT_EQ(R"(["v5", "server", "10.10.0.5", "50"])", chunk->debug_row(4));
}

TEST_F(JsonScannerTest, test_string_single_column) {
    std::string datapath = "./be/test/exec/test_data/json_scanner/test_ndjson_chinese.json";
    {
        std::string jsonpath1 = R"(["$"])";
        ChunkPtr result1 = test_whole_row_json(1, datapath, jsonpath1, {"$"});
        EXPECT_EQ(1, result1->num_columns());
        EXPECT_EQ(5, result1->num_rows());

        EXPECT_EQ(R"(['{"k1":"v1",   "kind":"中文", "keyname":{"ip地址":"10.10.0.1", "value":"10"}}'])",
                  result1->debug_row(0));
        EXPECT_EQ(R"(['{"k1":"v2", "kind":"英文", "keyname":{"ip地址":"10.10.0.2", "value":"20"}}'])",
                  result1->debug_row(1));
        EXPECT_EQ(R"(['{"k1":"v3", "kind":"法文", "keyname":{"ip地址":"10.10.0.3", "value":"30"}}'])",
                  result1->debug_row(2));
        EXPECT_EQ(R"(['{"k1":"v4",   "kind":"德文", "keyname":{"ip地址":"10.10.0.4", "value":"40"}}'])",
                  result1->debug_row(3));
        EXPECT_EQ(R"(['{"k1":"v5",   "kind":"西班牙", "keyname":{"ip地址":"10.10.0.5", "value":"50"}}'])",
                  result1->debug_row(4));
    }
}

TEST_F(JsonScannerTest, test_native_json_single_column) {
    std::string datapath = "./be/test/exec/test_data/json_scanner/test_ndjson_chinese.json";

    {
        std::string jsonpath1 = R"(["$"])";
        ChunkPtr result1 = test_with_jsonpath(1, datapath, jsonpath1, {"$"});
        EXPECT_EQ(1, result1->num_columns());
        EXPECT_EQ(5, result1->num_rows());

        EXPECT_EQ(R"([{"k1": "v1", "keyname": {"ip地址": "10.10.0.1", "value": "10"}, "kind": "中文"}])",
                  result1->debug_row(0));
        EXPECT_EQ(R"([{"k1": "v2", "keyname": {"ip地址": "10.10.0.2", "value": "20"}, "kind": "英文"}])",
                  result1->debug_row(1));
        EXPECT_EQ(R"([{"k1": "v3", "keyname": {"ip地址": "10.10.0.3", "value": "30"}, "kind": "法文"}])",
                  result1->debug_row(2));
        EXPECT_EQ(R"([{"k1": "v4", "keyname": {"ip地址": "10.10.0.4", "value": "40"}, "kind": "德文"}])",
                  result1->debug_row(3));
        EXPECT_EQ(R"([{"k1": "v5", "keyname": {"ip地址": "10.10.0.5", "value": "50"}, "kind": "西班牙"}])",
                  result1->debug_row(4));
    }
    {
        std::string jsonpath1 = R"(["$", "$.k1"])";
        ChunkPtr result1 = test_with_jsonpath(2, datapath, jsonpath1, {"$", "$.k1"});
        EXPECT_EQ(2, result1->num_columns());
        EXPECT_EQ(5, result1->num_rows());

        EXPECT_EQ(R"([{"k1": "v1", "keyname": {"ip地址": "10.10.0.1", "value": "10"}, "kind": "中文"}, "v1"])",
                  result1->debug_row(0));
        EXPECT_EQ(R"([{"k1": "v2", "keyname": {"ip地址": "10.10.0.2", "value": "20"}, "kind": "英文"}, "v2"])",
                  result1->debug_row(1));
        EXPECT_EQ(R"([{"k1": "v3", "keyname": {"ip地址": "10.10.0.3", "value": "30"}, "kind": "法文"}, "v3"])",
                  result1->debug_row(2));
        EXPECT_EQ(R"([{"k1": "v4", "keyname": {"ip地址": "10.10.0.4", "value": "40"}, "kind": "德文"}, "v4"])",
                  result1->debug_row(3));
        EXPECT_EQ(R"([{"k1": "v5", "keyname": {"ip地址": "10.10.0.5", "value": "50"}, "kind": "西班牙"}, "v5"])",
                  result1->debug_row(4));
    }
    {
        std::string jsonpath1 = R"(["$.k1","$"])";
        ChunkPtr result1 = test_with_jsonpath(2, datapath, jsonpath1, {"$.k1", "$"});
        EXPECT_EQ(2, result1->num_columns());
        EXPECT_EQ(5, result1->num_rows());

        EXPECT_EQ(R"(["v1", {"k1": "v1", "keyname": {"ip地址": "10.10.0.1", "value": "10"}, "kind": "中文"}])",
                  result1->debug_row(0));
        EXPECT_EQ(R"(["v2", {"k1": "v2", "keyname": {"ip地址": "10.10.0.2", "value": "20"}, "kind": "英文"}])",
                  result1->debug_row(1));
        EXPECT_EQ(R"(["v3", {"k1": "v3", "keyname": {"ip地址": "10.10.0.3", "value": "30"}, "kind": "法文"}])",
                  result1->debug_row(2));
        EXPECT_EQ(R"(["v4", {"k1": "v4", "keyname": {"ip地址": "10.10.0.4", "value": "40"}, "kind": "德文"}])",
                  result1->debug_row(3));
        EXPECT_EQ(R"(["v5", {"k1": "v5", "keyname": {"ip地址": "10.10.0.5", "value": "50"}, "kind": "西班牙"}])",
                  result1->debug_row(4));
    }
    {
        std::string jsonpath1 = R"(["$.k1", "$.kind", "$"])";
        ChunkPtr result1 = test_with_jsonpath(3, datapath, jsonpath1, {"$.k1", "$.kind", "$"});
        EXPECT_EQ(3, result1->num_columns());
        EXPECT_EQ(5, result1->num_rows());

        EXPECT_EQ(R"(["v1", "中文", {"k1": "v1", "keyname": {"ip地址": "10.10.0.1", "value": "10"}, "kind": "中文"}])",
                  result1->debug_row(0));
        EXPECT_EQ(R"(["v2", "英文", {"k1": "v2", "keyname": {"ip地址": "10.10.0.2", "value": "20"}, "kind": "英文"}])",
                  result1->debug_row(1));
        EXPECT_EQ(R"(["v3", "法文", {"k1": "v3", "keyname": {"ip地址": "10.10.0.3", "value": "30"}, "kind": "法文"}])",
                  result1->debug_row(2));
        EXPECT_EQ(R"(["v4", "德文", {"k1": "v4", "keyname": {"ip地址": "10.10.0.4", "value": "40"}, "kind": "德文"}])",
                  result1->debug_row(3));
        EXPECT_EQ(
                R"(["v5", "西班牙", {"k1": "v5", "keyname": {"ip地址": "10.10.0.5", "value": "50"}, "kind": "西班牙"}])",
                result1->debug_row(4));
    }
    {
        std::string jsonpath1 = R"(["$.k1", "$.kind", "$", "$.k2"])";
        ChunkPtr result1 = test_with_jsonpath(4, datapath, jsonpath1, {"$.k1", "$.kind", "$", "$.k2"});
        EXPECT_EQ(4, result1->num_columns());
        EXPECT_EQ(5, result1->num_rows());

        EXPECT_EQ(
                R"(["v1", "中文", {"k1": "v1", "keyname": {"ip地址": "10.10.0.1", "value": "10"}, "kind": "中文"}, NULL])",
                result1->debug_row(0));
        EXPECT_EQ(
                R"(["v2", "英文", {"k1": "v2", "keyname": {"ip地址": "10.10.0.2", "value": "20"}, "kind": "英文"}, NULL])",
                result1->debug_row(1));
        EXPECT_EQ(
                R"(["v3", "法文", {"k1": "v3", "keyname": {"ip地址": "10.10.0.3", "value": "30"}, "kind": "法文"}, NULL])",
                result1->debug_row(2));
        EXPECT_EQ(
                R"(["v4", "德文", {"k1": "v4", "keyname": {"ip地址": "10.10.0.4", "value": "40"}, "kind": "德文"}, NULL])",
                result1->debug_row(3));
        EXPECT_EQ(
                R"(["v5", "西班牙", {"k1": "v5", "keyname": {"ip地址": "10.10.0.5", "value": "50"}, "kind": "西班牙"}, NULL])",
                result1->debug_row(4));
    }
}

TEST_F(JsonScannerTest, test_expanded_with_json_root) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TypeDescriptor::create_json_type());

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.jsonpaths = "";
    range.json_root = "$.data";

    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = true;

    range.__set_path("./be/test/exec/test_data/json_scanner/test_expanded_array.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "keyname"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(3, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ("['v1', 'server', {\"ip\": \"10.10.0.1\", \"value\": \"10\"}]", chunk->debug_row(0));
    EXPECT_EQ("['v2', 'server', {\"ip\": \"10.10.0.2\", \"value\": \"20\"}]", chunk->debug_row(1));
    EXPECT_EQ("['v3', 'server', {\"ip\": \"10.10.0.3\", \"value\": \"30\"}]", chunk->debug_row(2));
    EXPECT_EQ("['v4', 'server', {\"ip\": \"10.10.0.4\", \"value\": \"40\"}]", chunk->debug_row(3));
    EXPECT_EQ("['v5', 'server', {\"ip\": \"10.10.0.5\", \"value\": \"50\"}]", chunk->debug_row(4));
}

TEST_F(JsonScannerTest, test_ndjson_expanded_with_json_root) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TypeDescriptor::create_json_type());

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.jsonpaths = "";
    range.json_root = "$.data";

    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = true;

    range.__set_path("./be/test/exec/test_data/json_scanner/test_ndjson_expanded_array.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "keyname"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(3, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ("['v1', 'server', {\"ip\": \"10.10.0.1\", \"value\": \"10\"}]", chunk->debug_row(0));
    EXPECT_EQ("['v2', 'server', {\"ip\": \"10.10.0.2\", \"value\": \"20\"}]", chunk->debug_row(1));
    EXPECT_EQ("['v3', 'server', {\"ip\": \"10.10.0.3\", \"value\": \"30\"}]", chunk->debug_row(2));
    EXPECT_EQ("['v4', 'server', {\"ip\": \"10.10.0.4\", \"value\": \"40\"}]", chunk->debug_row(3));
    EXPECT_EQ("['v5', 'server', {\"ip\": \"10.10.0.5\", \"value\": \"50\"}]", chunk->debug_row(4));
}

// this test covers json_scanner.cpp:_construct_row_in_object_order.
TEST_F(JsonScannerTest, test_construct_row_in_object_order) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test_cast_type.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges,
                                       {"f_dummy_0", "f_float", "f_dummy_1", "f_bool", "f_dummy_2", "f_int",
                                        "f_dummy_3", "f_float_in_string", "f_dummy_4", "f_int_in_string", "f_dummy_5"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(11, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[NULL, '3.14', NULL, '1', NULL, '123', NULL, 3.14, NULL, 123, NULL]", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_jsonroot_with_jsonpath) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = true;
    range.__isset.json_root = true;

    range.jsonpaths = R"(["$.ip", "$.value"])";
    range.json_root = "$.keyname";

    range.__set_path("./be/test/exec/test_data/json_scanner/test_ndjson.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(2, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ("['10.10.0.1', '10']", chunk->debug_row(0));
    EXPECT_EQ("['10.10.0.2', '20']", chunk->debug_row(1));
    EXPECT_EQ("['10.10.0.3', '30']", chunk->debug_row(2));
    EXPECT_EQ("['10.10.0.4', '40']", chunk->debug_row(3));
    EXPECT_EQ("['10.10.0.5', '50']", chunk->debug_row(4));
}

TEST_F(JsonScannerTest, test_expanded_with_jsonroot_and_extracted_by_jsonpath) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.jsonpaths = R"(["$.keyname.ip", "$.keyname.value"])";
    range.json_root = "$.data";

    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = true;
    range.__isset.json_root = true;

    range.__set_path("./be/test/exec/test_data/json_scanner/test_expanded_array.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(2, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ("['10.10.0.1', '10']", chunk->debug_row(0));
    EXPECT_EQ("['10.10.0.2', '20']", chunk->debug_row(1));
    EXPECT_EQ("['10.10.0.3', '30']", chunk->debug_row(2));
    EXPECT_EQ("['10.10.0.4', '40']", chunk->debug_row(3));
    EXPECT_EQ("['10.10.0.5', '50']", chunk->debug_row(4));
}

TEST_F(JsonScannerTest, test_ndjson_expaned_with_jsonroot_and_extracted_by_jsonpath) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.jsonpaths = R"(["$.keyname.ip", "$.keyname.value"])";
    range.json_root = "$.data";

    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = true;
    range.__isset.json_root = true;

    range.__set_path("./be/test/exec/test_data/json_scanner/test_ndjson_expanded_array.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(2, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ("['10.10.0.1', '10']", chunk->debug_row(0));
    EXPECT_EQ("['10.10.0.2', '20']", chunk->debug_row(1));
    EXPECT_EQ("['10.10.0.3', '30']", chunk->debug_row(2));
    EXPECT_EQ("['10.10.0.4', '40']", chunk->debug_row(3));
    EXPECT_EQ("['10.10.0.5', '50']", chunk->debug_row(4));
}

TEST_F(JsonScannerTest, test_illegal_input) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/illegal.json");
    ranges.emplace_back(range);

    auto scanner =
            create_json_scanner(types, ranges, {"f_float", "f_bool", "f_int", "f_float_in_string", "f_int_in_string"});

    ASSERT_OK(scanner->open());
    auto st = scanner->get_next().status();
    ASSERT_TRUE(st.is_data_quality_error());
    ASSERT_TRUE(st.message().find("parse error. illegal json started with") != std::string::npos);
}

TEST_F(JsonScannerTest, test_illegal_input_with_jsonpath) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.f_float", "$.f_bool", "$.f_int", "$.f_float_in_string", "$.f_int_in_string"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/illegal.json");
    ranges.emplace_back(range);

    auto scanner =
            create_json_scanner(types, ranges, {"f_float", "f_bool", "f_int", "f_float_in_string", "f_int_in_string"});

    ASSERT_OK(scanner->open());
    ASSERT_TRUE(scanner->get_next().status().is_data_quality_error());
}

// See more: https://github.com/StarRocks/starrocks/issues/11054
TEST_F(JsonScannerTest, test_column_2x_than_columns_with_json_root) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TypeDescriptor::create_json_type());
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.jsonpaths = "";
    range.json_root = "$.data";

    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = true;

    range.__set_path("./be/test/exec/test_data/json_scanner/test_ndjson_expanded_array.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges,
                                       {"k1", "kind", "keyname", "null_col1", "null_col2", "null_col3", "null_col4"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(7, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ("['v1', 'server', {\"ip\": \"10.10.0.1\", \"value\": \"10\"}, NULL, NULL, NULL, NULL]",
              chunk->debug_row(0));
    EXPECT_EQ("['v2', 'server', {\"ip\": \"10.10.0.2\", \"value\": \"20\"}, NULL, NULL, NULL, NULL]",
              chunk->debug_row(1));
    EXPECT_EQ("['v3', 'server', {\"ip\": \"10.10.0.3\", \"value\": \"30\"}, NULL, NULL, NULL, NULL]",
              chunk->debug_row(2));
    EXPECT_EQ("['v4', 'server', {\"ip\": \"10.10.0.4\", \"value\": \"40\"}, NULL, NULL, NULL, NULL]",
              chunk->debug_row(3));
    EXPECT_EQ("['v5', 'server', {\"ip\": \"10.10.0.5\", \"value\": \"50\"}, NULL, NULL, NULL, NULL]",
              chunk->debug_row(4));
}

TEST_F(JsonScannerTest, test_null_with_jsonpath) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.k1", "$.kind", "$.keyname.ip", "$.keyname.value"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/null.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "ip", "value"});

    ASSERT_OK(scanner->open());
    ASSERT_TRUE(scanner->get_next().status().is_data_quality_error());
}

TEST_F(JsonScannerTest, test_repeated_jsonpaths) {
    std::string json_data =
            R"({"level1":{"level2":"long long long long long long long long long long long long long long string"}})";
    std::string json_filename = "be/test/exec/test_data/json_scanner/test_repeated_jsonpaths.json";

    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_JSON);
    types.emplace_back(TYPE_JSON);
    types.emplace_back(TYPE_JSON);
    types.emplace_back(TYPE_JSON);
    types.emplace_back(TYPE_JSON);
    types.emplace_back(TYPE_JSON);

    write_json_to_file(json_filename, json_data);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.level1.level2", "$", "$.level1.level2", "$", "$.level1.level2", "$"])";
    range.__isset.json_root = false;
    range.__set_path(json_filename);
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(
            types, ranges, {"level1_level2_1", "root1", "level1_level2_2", "root2", "level1_level2_3", "root3"});

    ASSERT_OK(scanner->open());
    auto st = scanner->get_next().status();
    ASSERT_TRUE(st.ok()) << st;

    (void)fs::delete_file(json_filename);
}

TEST_F(JsonScannerTest, file_stream) {
    // 1. create StreamLoadPipe
    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<StreamLoadPipe>(1024 * 1024, 64 * 1024);
    DeferOp remove_pipe([&]() { _load_stream_mgr.remove(load_id); });
    ASSERT_OK(_load_stream_mgr.put(load_id, pipe));

    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_STREAM;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_load_id(load_id.to_thrift());
    ranges.emplace_back(range);

    std::string data = R"({"key1": 1, "key2": 2 }{"key1": 3, "key2": 4 })";
    EXPECT_OK(pipe->append(data.c_str(), data.size()));
    EXPECT_OK(pipe->finish());

    auto scanner = create_json_scanner(types, ranges, {"key1", "key2"});
    Status st;
    st = scanner->open();
    EXPECT_OK(st);

    auto res = scanner->get_next();
    EXPECT_OK(res.status());

    ChunkPtr chunk = res.value();
    EXPECT_EQ(2, chunk->num_columns());
    EXPECT_EQ(2, chunk->num_rows());

    EXPECT_EQ("[1, 2]", chunk->debug_row(0));
    EXPECT_EQ("[3, 4]", chunk->debug_row(1));
}

// Regression test for #11412: a gzip-compressed JSON stream load (FILE_STREAM + FORMAT_JSON
// + compression_type) must be decompressed by CompressedStreamLoadPipeReader, NOT wrapped in a
// CompressedInputStream by create_sequential_file. Wrapping it caused a double-decompress and a
// type-confused down_cast<StreamLoadPipeInputStream*> in _read_file_stream(), crashing the BE.
TEST_F(JsonScannerTest, file_stream_gzip_compressed) {
    // 1. create StreamLoadPipe
    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<StreamLoadPipe>(1024 * 1024, 64 * 1024);
    DeferOp remove_pipe([&]() { _load_stream_mgr.remove(load_id); });
    ASSERT_OK(_load_stream_mgr.put(load_id, pipe));

    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_STREAM;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_load_id(load_id.to_thrift());
    // The FE sets compression_type when the user requests a compressed stream load.
    range.__set_compression_type(TCompressionType::GZIP);
    ranges.emplace_back(range);

    std::string data = R"({"key1": 1, "key2": 2 }{"key1": 3, "key2": 4 })";
    std::string compressed = compress_gzip(data);
    ASSERT_FALSE(compressed.empty());
    EXPECT_OK(pipe->append(compressed.c_str(), compressed.size()));
    EXPECT_OK(pipe->finish());

    auto scanner = create_json_scanner(types, ranges, {"key1", "key2"});
    Status st;
    st = scanner->open();
    EXPECT_OK(st);

    auto res = scanner->get_next();
    EXPECT_OK(res.status());

    ChunkPtr chunk = res.value();
    EXPECT_EQ(2, chunk->num_columns());
    EXPECT_EQ(2, chunk->num_rows());

    EXPECT_EQ("[1, 2]", chunk->debug_row(0));
    EXPECT_EQ("[3, 4]", chunk->debug_row(1));
}

TEST_F(JsonScannerTest, test_duplicate_key) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test_duplicate_key.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "k2"});

    Status st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(2, chunk->num_columns());
    EXPECT_EQ(2, chunk->num_rows());

    EXPECT_EQ("[1, 1]", chunk->debug_row(0));
    EXPECT_EQ("[2, 2]", chunk->debug_row(1));
}

TEST_F(JsonScannerTest, test_gzip_compressed_json_with_compression_type) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_DOUBLE);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__isset.compression_type = true;
    range.compression_type = TCompressionType::GZIP;
    range.__set_path("./be/test/exec/test_data/json_scanner/books.json.gz");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"category", "author", "title", "price"});
    Status st = scanner->open();
    ASSERT_OK(st);
    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_GT(chunk->num_rows(), 0);
}

TEST_F(JsonScannerTest, test_gzip_compressed_json_with_suffix_inference) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_VARCHAR_DESC);
    types.emplace_back(TYPE_DOUBLE);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/books.json.gz");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"category", "author", "title", "price"});
    Status st = scanner->open();
    ASSERT_OK(st);
    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_GT(chunk->num_rows(), 0);
}

TEST_F(JsonScannerTest, test_large_compressed_json_large) {
    // Create a temporary large compressed JSON file (>1MB)
    std::string temp_file = create_temp_large_compressed_json(3); // 3MB target
    std::vector<std::string> column_names{"id", "name", "value", "timestamp", "category", "score", "active", "tags"};

    // Ensure cleanup happens even if test fails
    // auto cleanup = [temp_file]() { cleanup_temp_file(temp_file); };
    auto cleanup = [temp_file]() { cleanup_temp_file(temp_file); };
    DeferOp defer_cleanup(cleanup);

    // Verify file was created and has reasonable size
    std::ifstream file_check(temp_file, std::ios::binary | std::ios::ate);
    ASSERT_TRUE(file_check.is_open());
    size_t file_size = file_check.tellg();
    file_check.close();

    // File should be at least 1MB (since we're targeting 3MB)
    EXPECT_GE(file_size, 1 * 1024 * 1024);

    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_VARCHAR_DESC);                  // id
    types.emplace_back(TYPE_VARCHAR_DESC);                  // name
    types.emplace_back(TYPE_DOUBLE);                        // value
    types.emplace_back(TYPE_BIGINT);                        // timestamp
    types.emplace_back(TYPE_VARCHAR_DESC);                  // category
    types.emplace_back(TYPE_DOUBLE);                        // score
    types.emplace_back(TYPE_BOOLEAN);                       // active
    types.emplace_back(TypeDescriptor::create_json_type()); // tags

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_compression_type(TCompressionType::GZIP);
    range.__set_path(temp_file);
    ranges.emplace_back(range);

    // small size limit
    {
        size_t file_size_limit = 1024 * 1024;
        auto scanner = create_json_scanner(types, ranges, column_names, file_size_limit);
        Status st = scanner->open();
        ASSERT_OK(st);
        DeferOp close_scanner([&]() { scanner->close(); });
        auto res = scanner->get_next();
        EXPECT_TRUE(res.status().is_mem_limit_exceeded()) << res.status();
    }

    size_t file_size_limit = 10 * 1024 * 1024;
    auto scanner = create_json_scanner(types, ranges, column_names, file_size_limit);
    Status st = scanner->open();
    ASSERT_OK(st);
    DeferOp close_scanner([&]() { scanner->close(); });

    // Test that we can read multiple chunks from the large file
    int total_rows = 0;
    int chunk_count = 0;
    const int max_chunks = 10; // Limit to avoid excessive memory usage in tests

    while (chunk_count < max_chunks) {
        auto res = scanner->get_next();
        if (!res.ok()) {
            EXPECT_TRUE(res.status().is_end_of_file()) << res.status();
            break; // End of file or error
        }

        ChunkPtr chunk = std::move(res.value());
        if (chunk->num_rows() == 0) {
            break; // End of file
        }

        total_rows += chunk->num_rows();
        chunk_count++;

        // Verify chunk structure
        EXPECT_EQ(8, chunk->num_columns());
        EXPECT_GT(chunk->num_rows(), 0);
    }

    // Should have read some data
    EXPECT_GT(total_rows, 0);
    EXPECT_GT(chunk_count, 0);
}

static TRoutineLoadMetaColumn json_meta_desc(int32_t slot_id, TStreamSourceMetaKind::type kind) {
    TRoutineLoadMetaColumn d;
    d.__set_slot_id(slot_id);
    d.__set_kind(kind);
    return d;
}

TEST_F(JsonScannerTest, test_source_metadata_fill) {
    // Hidden source-metadata slots follow (or interleave with) the payload columns; the routine-load FE
    // derives them from the INCLUDE METADATA clause. BE_TEST reads json from a file rather than the
    // Kafka pipe, so the per-message metadata is injected via set_test_stream_meta(). Metadata-value
    // formatting itself is covered by stream_source_meta_test; this covers the JsonReader integration:
    // slot interception (object-order and jsonpath), positional jsonpath alignment, and the
    // payload-field-name-collides-with-a-metadata-alias skip.
    std::string filename = "./be/test/exec/test_data/json_scanner/meta_test.json";
    write_json_to_file(filename, R"({"id": 42, "payload": "hello"})");
    DeferOp defer([&] { std::remove(filename.c_str()); });

    StreamMessageMeta meta(ByteBufferMetaType::KAFKA);
    meta.set_topic("orders");
    meta.set_partition(7);
    meta.set_offset(100);
    meta.add_header("trace-id", "xyz");

    // by-name path (no jsonpath): metadata slots follow the two payload columns, filled from meta.
    {
        std::vector<TypeDescriptor> types;
        types.emplace_back(TYPE_BIGINT);       // id (payload)
        types.emplace_back(TYPE_VARCHAR_DESC); // payload
        types.emplace_back(TYPE_VARCHAR_DESC); // topic (meta, slot 2)
        types.emplace_back(TYPE_INT);          // partition (meta, slot 3)
        types.emplace_back(TYPE_BIGINT);       // offset (meta, slot 4)
        types.emplace_back(TypeDescriptor::create_map_type(TYPE_VARCHAR_DESC,
                                                           TYPE_VARCHAR_DESC)); // headers (slot 5)

        std::vector<TRoutineLoadMetaColumn> meta_cols = {
                json_meta_desc(2, TStreamSourceMetaKind::TOPIC), json_meta_desc(3, TStreamSourceMetaKind::PARTITION),
                json_meta_desc(4, TStreamSourceMetaKind::OFFSET), json_meta_desc(5, TStreamSourceMetaKind::HEADERS)};

        std::vector<TBrokerRangeDesc> ranges;
        TBrokerRangeDesc range;
        range.format_type = TFileFormatType::FORMAT_JSON;
        range.file_type = TFileType::FILE_LOCAL;
        range.__isset.jsonpaths = false;
        range.__set_path(filename);
        ranges.emplace_back(range);

        auto scanner = create_json_scanner(types, ranges, {"id", "payload", "mt_topic", "mt_part", "mt_off", "mt_hdr"},
                                           1024 * 1024, meta_cols);
        scanner->set_test_stream_meta(&meta);
        ASSERT_OK(scanner->open());
        ChunkPtr chunk = scanner->get_next().value();
        ASSERT_EQ(6, chunk->num_columns());
        ASSERT_EQ(1, chunk->num_rows());
        EXPECT_EQ(42, chunk->get(0)[0].get_int64());
        EXPECT_EQ("hello", chunk->get(0)[1].get_slice());
        EXPECT_EQ("orders", chunk->get(0)[2].get_slice());
        EXPECT_EQ(7, chunk->get(0)[3].get_int32());
        EXPECT_EQ(100, chunk->get(0)[4].get_int64());
        EXPECT_EQ(1, chunk->get(0)[5].get_map().size());
    }

    // jsonpath path: a metadata slot sits BETWEEN the two payload columns; the positional
    // slot->jsonpath mapping must skip it (meta_count shift) for the payload columns to stay aligned.
    {
        std::vector<TypeDescriptor> types;
        types.emplace_back(TYPE_BIGINT);       // id (payload, jsonpath[0])
        types.emplace_back(TYPE_VARCHAR_DESC); // topic (meta, slot 1)
        types.emplace_back(TYPE_VARCHAR_DESC); // payload (payload, jsonpath[1])

        std::vector<TRoutineLoadMetaColumn> meta_cols = {json_meta_desc(1, TStreamSourceMetaKind::TOPIC)};

        std::vector<TBrokerRangeDesc> ranges;
        TBrokerRangeDesc range;
        range.format_type = TFileFormatType::FORMAT_JSON;
        range.file_type = TFileType::FILE_LOCAL;
        range.__isset.jsonpaths = true;
        range.jsonpaths = R"(["$.id", "$.payload"])";
        range.__set_path(filename);
        ranges.emplace_back(range);

        auto scanner = create_json_scanner(types, ranges, {"id", "mt_topic", "payload"}, 1024 * 1024, meta_cols);
        scanner->set_test_stream_meta(&meta);
        ASSERT_OK(scanner->open());
        ChunkPtr chunk = scanner->get_next().value();
        ASSERT_EQ(3, chunk->num_columns());
        ASSERT_EQ(1, chunk->num_rows());
        EXPECT_EQ(42, chunk->get(0)[0].get_int64());       // jsonpath[0] -> $.id
        EXPECT_EQ("orders", chunk->get(0)[1].get_slice()); // metadata, transparent to jsonpath mapping
        EXPECT_EQ("hello", chunk->get(0)[2].get_slice());  // jsonpath[1] -> $.payload, via meta_count shift
    }

    // collision: a payload field ("payload") shares its name with a metadata alias; the metadata value
    // must win and the payload field must not overwrite it (object-order skip path).
    {
        std::vector<TypeDescriptor> types;
        types.emplace_back(TYPE_BIGINT);       // id (payload)
        types.emplace_back(TYPE_VARCHAR_DESC); // payload (meta TOPIC, slot 1)

        std::vector<TRoutineLoadMetaColumn> meta_cols = {json_meta_desc(1, TStreamSourceMetaKind::TOPIC)};

        std::vector<TBrokerRangeDesc> ranges;
        TBrokerRangeDesc range;
        range.format_type = TFileFormatType::FORMAT_JSON;
        range.file_type = TFileType::FILE_LOCAL;
        range.__isset.jsonpaths = false;
        range.__set_path(filename);
        ranges.emplace_back(range);

        auto scanner = create_json_scanner(types, ranges, {"id", "payload"}, 1024 * 1024, meta_cols);
        scanner->set_test_stream_meta(&meta);
        ASSERT_OK(scanner->open());
        ChunkPtr chunk = scanner->get_next().value();
        ASSERT_EQ(2, chunk->num_columns());
        ASSERT_EQ(1, chunk->num_rows());
        EXPECT_EQ(42, chunk->get(0)[0].get_int64());
        EXPECT_EQ("orders", chunk->get(0)[1].get_slice()); // metadata wins over the "payload" json field
    }
}

namespace {

// Routine load appends every Kafka message as its own buffer, see KafkaConsumerPipe::append_json.
Status append_json_buffer(StreamLoadPipe* pipe, const std::string& data, const StreamMessageMeta* meta) {
    ByteBufferMetaType meta_type = meta != nullptr ? meta->type() : ByteBufferMetaType::NONE;
    ASSIGN_OR_RETURN(auto buf, ByteBuffer::allocate_with_tracker(data.size(), simdjson::SIMDJSON_PADDING, meta_type));
    buf->put_bytes(data.data(), data.size());
    buf->flip_to_read();
    if (meta != nullptr) {
        RETURN_IF_ERROR(buf->meta()->copy_from(const_cast<StreamMessageMeta*>(meta)));
    }
    return pipe->append(std::move(buf));
}

const std::vector<std::string> kK13Names{"k1", "k2", "k3",  "k4",  "k5",  "k6", "k7",
                                         "k8", "k9", "k10", "k11", "k12", "k13"};
// The closing '}' is missing.
const std::string kTruncatedDocument =
        R"({"k1":"2020-06-28","k2":"2020-06-25 00:00:09","k3":"beijing","k4":"haidian","k5":0,"k6":-118,)"
        R"("k7":-32758,"k8":-2147483638,"k9":-9223372036854775798,"k10":"-18446744073709551608","k11":-2.1,)"
        R"("k12":-3.04,"k13":-3.122)";
// The value of k13 is missing, but the document is complete.
const std::string kMissingValueDocument =
        R"({"k1":"2020-06-28","k2":"2020-06-25 00:00:09","k3":"beijing","k4":"haidian","k5":0,"k6":-118,)"
        R"("k7":-32758,"k8":-2147483638,"k9":-9223372036854775798,"k10":"-18446744073709551608","k11":-2.1,)"
        R"("k12":-3.04,"k13":})";

// Writes the rejected records of a load to one file.
class TestLoadPathMgr : public BaseLoadPathMgr {
public:
    explicit TestLoadPathMgr(std::string rejected_record_path)
            : _rejected_record_path(std::move(rejected_record_path)) {}
    Status init() override { return Status::OK(); }
    Status allocate_dir(const std::string&, const std::string&, std::string*) override {
        return Status::NotSupported("");
    }
    void get_load_data_path(std::vector<std::string>*) override {}
    Status get_load_error_file_name(const TUniqueId&, std::string*) override { return Status::NotSupported(""); }
    std::string get_load_error_absolute_path(const std::string& file_path) override { return file_path; }
    std::string get_load_rejected_record_absolute_path(const std::string&, const std::string&, const std::string&,
                                                       const int64_t, const TUniqueId&) override {
        return _rejected_record_path;
    }

private:
    std::string _rejected_record_path;
};

} // namespace

// A json document stream (ndjson) can end with an incomplete document: a truncated Kafka message, or the cut-off
// last line of a stream load body. simdjson iterate_many stops before it with a plain EOF, so it has to be reported
// as an error row by the reader, and the scan goes on with the next buffer.
class JsonScannerTruncatedDocTest : public JsonScannerTest {
protected:
    void SetUp() override {
        JsonScannerTest::SetUp();
        _old_batch_size = config::json_parse_many_batch_size;
        _old_dynamic_batch_size = config::enable_dynamic_batch_size_for_json_parse_many;
        config::json_parse_many_batch_size = 1000000;
        config::enable_dynamic_batch_size_for_json_parse_many = true;
        // Only a load writes the error log.
        _state->_query_options.query_type = TQueryType::LOAD;
        _error_log.open(_error_log_path, std::ofstream::out);
        _state->_error_log_file = &_error_log;
    }

    void TearDown() override {
        _state->_error_log_file = nullptr;
        _error_log.close();
        std::remove(_error_log_path.c_str());
        config::json_parse_many_batch_size = _old_batch_size;
        config::enable_dynamic_batch_size_for_json_parse_many = _old_dynamic_batch_size;
        JsonScannerTest::TearDown();
    }

    struct ScanResult {
        size_t rows = 0;
        int64_t filtered = 0;
        // The status that ends the scan, EOF if all buffers are read.
        Status status;
    };

    // Scans a stream load pipe filled with `buffers`, each one appended as a buffer of its own.
    ScanResult scan(TBrokerRangeDesc range, const std::vector<std::string>& names,
                    const std::vector<std::string>& buffers, const StreamMessageMeta* meta = nullptr) {
        auto load_id = UniqueId::gen_uid();
        auto pipe = std::make_shared<StreamLoadPipe>(1024 * 1024, 64 * 1024);
        DeferOp remove_pipe([&]() { _load_stream_mgr.remove(load_id); });
        EXPECT_OK(_load_stream_mgr.put(load_id, pipe));
        for (const auto& buffer : buffers) {
            EXPECT_OK(append_json_buffer(pipe.get(), buffer, meta));
        }
        EXPECT_OK(pipe->finish());

        range.format_type = TFileFormatType::FORMAT_JSON;
        range.file_type = TFileType::FILE_STREAM;
        range.__set_load_id(load_id.to_thrift());
        return scan(range, names);
    }

    ScanResult scan(const TBrokerRangeDesc& range, const std::vector<std::string>& names) {
        std::vector<TypeDescriptor> types(names.size(), TypeDescriptor::create_varchar_type(1024));
        auto scanner = create_json_scanner(types, {range}, names);
        _counter->num_rows_filtered = 0;

        ScanResult result;
        result.status = scanner->open();
        while (result.status.ok()) {
            auto chunk = scanner->get_next();
            result.status = chunk.status();
            if (chunk.ok()) {
                result.rows += chunk.value()->num_rows();
            }
        }
        result.filtered = _counter->num_rows_filtered;
        return result;
    }

    ScanResult scan(const std::vector<std::string>& buffers) { return scan(TBrokerRangeDesc(), {"k1", "k2"}, buffers); }

    std::string error_log() {
        _error_log.flush();
        std::ifstream in(_error_log_path);
        return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
    }

private:
    const std::string _error_log_path = "./json_scanner_truncated_doc_test_error_log";
    std::ofstream _error_log;
    int32_t _old_batch_size = 0;
    bool _old_dynamic_batch_size = true;
};

// Routine load: the only message of the batch is a truncated document.
TEST_F(JsonScannerTruncatedDocTest, kafka_message_with_only_truncated_document) {
    StreamMessageMeta meta(ByteBufferMetaType::KAFKA);
    meta.set_partition(3);
    meta.set_offset(42);
    auto result = scan(TBrokerRangeDesc(), kK13Names, {kTruncatedDocument}, &meta);
    EXPECT_TRUE(result.status.is_end_of_file()) << result.status;
    EXPECT_EQ(0, result.rows);
    EXPECT_EQ(1, result.filtered);

    // The error row carries the size, the beginning of the dropped text within the error log length limit, and
    // the message meta.
    std::string log = error_log();
    EXPECT_NE(std::string::npos,
              log.find("parse error. Incomplete or malformed json document at the end of the input (" +
                       std::to_string(kTruncatedDocument.size()) + " bytes)"))
            << log;
    EXPECT_NE(std::string::npos,
              log.find("Row: " + kTruncatedDocument.substr(0, 64) + " [meta: kafka partition: 3, offset: 42]"))
            << log;
}

// Routine load: a document followed by a truncated one in the same message. The message has a loaded row, so the
// truncated document was lost even when empty batches were not committed.
TEST_F(JsonScannerTruncatedDocTest, kafka_message_with_trailing_truncated_document) {
    auto result = scan({"{\"k1\":\"a\",\"k2\":1}\n{\"k1\":\"b\",\"k2\":"});
    EXPECT_TRUE(result.status.is_end_of_file()) << result.status;
    EXPECT_EQ(1, result.rows);
    EXPECT_EQ(1, result.filtered);
    EXPECT_NE(std::string::npos, error_log().find(R"(Row: {"k1":"b","k2":)"));
}

// Routine load: the scan goes on with the messages after a truncated one.
TEST_F(JsonScannerTruncatedDocTest, kafka_truncated_messages_between_good_messages) {
    auto result = scan({R"({"k1":"a","k2":)", R"({"k1":"b","k2":2})", R"({"k1":"c")", R"({"k1":"d","k2":4})"});
    EXPECT_TRUE(result.status.is_end_of_file()) << result.status;
    EXPECT_EQ(2, result.rows);
    EXPECT_EQ(2, result.filtered);
}

// Every truncated message is an error row, while the error log keeps its limit.
TEST_F(JsonScannerTruncatedDocTest, many_truncated_messages) {
    std::vector<std::string> messages(MAX_ERROR_LINES_IN_FILE + 10, R"({"k1":"a","k2":)");
    messages.emplace_back(R"({"k1":"b","k2":2})");
    auto result = scan(messages);
    EXPECT_TRUE(result.status.is_end_of_file()) << result.status;
    EXPECT_EQ(1, result.rows);
    EXPECT_EQ(MAX_ERROR_LINES_IN_FILE + 10, result.filtered);

    std::stringstream log(error_log());
    int64_t logged = 0;
    for (std::string line; std::getline(log, line);) {
        logged += line.find("Incomplete or malformed json document") != std::string::npos;
    }
    EXPECT_EQ(MAX_ERROR_LINES_IN_FILE, logged);
}

// A dropped document is an error row of the chunk being read, like a malformed one, so a run of truncated messages
// does not keep a single read going through all of them.
TEST_F(JsonScannerTruncatedDocTest, truncated_messages_count_toward_the_chunk) {
    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<StreamLoadPipe>(1024 * 1024, 64 * 1024);
    DeferOp remove_pipe([&]() { _load_stream_mgr.remove(load_id); });
    ASSERT_OK(_load_stream_mgr.put(load_id, pipe));
    for (int i = 0; i < 10; i++) {
        ASSERT_OK(append_json_buffer(pipe.get(), R"({"k1":"a","k2":)", nullptr));
    }
    ASSERT_OK(append_json_buffer(pipe.get(), R"({"k1":"b","k2":2})", nullptr));
    ASSERT_OK(pipe->finish());

    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_STREAM;
    range.__set_load_id(load_id.to_thrift());
    _state->set_chunk_size(4);
    std::vector<TypeDescriptor> types(2, TypeDescriptor::create_varchar_type(1024));
    auto scanner = create_json_scanner(types, {range}, {"k1", "k2"});
    ASSERT_OK(scanner->open());

    auto chunk = scanner->get_next();
    ASSERT_OK(chunk.status());
    EXPECT_EQ(0, chunk.value()->num_rows());
    EXPECT_EQ(4, _counter->num_rows_filtered);

    size_t rows = 0;
    while (chunk.ok()) {
        rows += chunk.value()->num_rows();
        chunk = scanner->get_next();
    }
    EXPECT_TRUE(chunk.status().is_end_of_file()) << chunk.status();
    EXPECT_EQ(1, rows);
    EXPECT_EQ(10, _counter->num_rows_filtered);
}

// Stream load: the whole body is one buffer, and its last line is cut off.
TEST_F(JsonScannerTruncatedDocTest, stream_load_body_with_trailing_truncated_document) {
    auto result = scan({"{\"k1\":\"a\",\"k2\":1}\n{\"k1\":\"b\",\"k2\":2}\n{\"k1\":\"c\",\"k2\":3"});
    EXPECT_TRUE(result.status.is_end_of_file()) << result.status;
    EXPECT_EQ(2, result.rows);
    EXPECT_EQ(1, result.filtered);
}

// simdjson tells an incomplete document by the balance of its brackets, so it drops a last document with an extra '}'
// the same way.
TEST_F(JsonScannerTruncatedDocTest, trailing_document_with_extra_closing_brace) {
    auto result = scan({"{\"k1\":\"a\",\"k2\":1}\n{\"k1\":\"b\",\"k2\":2}}"});
    EXPECT_TRUE(result.status.is_end_of_file()) << result.status;
    EXPECT_EQ(1, result.rows);
    EXPECT_EQ(1, result.filtered);
}

// A complete but malformed document is one error row as well.
TEST_F(JsonScannerTruncatedDocTest, kafka_message_with_missing_value) {
    auto result = scan(TBrokerRangeDesc(), kK13Names, {kMissingValueDocument});
    EXPECT_TRUE(result.status.is_end_of_file()) << result.status;
    EXPECT_EQ(0, result.rows);
    EXPECT_EQ(1, result.filtered);
}

// Neither trailing whitespace nor a whitespace-only message is an incomplete document.
TEST_F(JsonScannerTruncatedDocTest, trailing_whitespace) {
    auto result = scan({"{\"k1\":\"a\",\"k2\":1}\n \t\r\n", " \n", R"({"k1":"b","k2":2})"});
    EXPECT_TRUE(result.status.is_end_of_file()) << result.status;
    EXPECT_EQ(2, result.rows);
    EXPECT_EQ(0, result.filtered);
    EXPECT_EQ("", error_log());
}

// A body larger than json_parse_many_batch_size is parsed in several batches, and only the last one tells whether
// the input ends with an incomplete document.
TEST_F(JsonScannerTruncatedDocTest, multi_batch) {
    // Read when the parser is created, and only honoured above simdjson::dom::MINIMAL_BATCH_SIZE.
    config::json_parse_many_batch_size = 64;
    std::string body;
    for (int i = 0; i < 200; i++) {
        body += "{\"k1\":\"" + std::to_string(i) + "\",\"k2\":" + std::to_string(i) + "}\n";
    }
    for (const auto& [tail, filtered] :
         std::vector<std::pair<std::string, int64_t>>{{"", 0},
                                                      {"\n \n", 0},
                                                      {R"({"k1":"200","k2":)", 1},
                                                      // Longer than the batch.
                                                      {R"({"k1":"200","k2":")" + std::string(200, 'x'), 1}}) {
        auto result = scan({body + tail});
        EXPECT_TRUE(result.status.is_end_of_file()) << result.status;
        EXPECT_EQ(200, result.rows);
        EXPECT_EQ(filtered, result.filtered) << tail;
    }
}

// A document larger than the batch makes the parser iterate the rest of the input again, from the last parsed
// document and with a larger batch size.
TEST_F(JsonScannerTruncatedDocTest, multi_batch_with_document_larger_than_batch) {
    config::json_parse_many_batch_size = 64;
    std::string body;
    for (int i = 0; i < 100; i++) {
        std::string doc = "{\"k1\":\"" + (i == 60 ? std::string(200, 'x') : std::to_string(i)) +
                          "\",\"k2\":" + std::to_string(i) + "}";
        // Documents at multiples of 32 bytes.
        doc.resize((doc.size() + 32) / 32 * 32 - 1, ' ');
        body += doc + "\n";
    }
    for (const auto& [tail, filtered] :
         std::vector<std::pair<std::string, int64_t>>{{"", 0}, {R"({"k1":"100","k2":)", 1}}) {
        auto result = scan({body + tail});
        EXPECT_TRUE(result.status.is_end_of_file()) << result.status;
        EXPECT_EQ(100, result.rows) << tail;
        EXPECT_EQ(filtered, result.filtered) << tail;
    }
}

TEST_F(JsonScannerTruncatedDocTest, json_root) {
    TBrokerRangeDesc range;
    range.__set_json_root("$.data");
    auto result = scan(range, {"k1", "k2"},
                       {R"({"data":{"k1":"a","k2":)", R"({"data":{"k1":"b","k2":2}})",
                        R"({"data":{"k1":"c","k2":3}} {"data":{"k1")"});
    EXPECT_TRUE(result.status.is_end_of_file()) << result.status;
    EXPECT_EQ(2, result.rows);
    EXPECT_EQ(2, result.filtered);
}

// The expanded parser reads the first document while parsing the buffer, so a buffer without any complete document
// must not end the scan.
TEST_F(JsonScannerTruncatedDocTest, json_root_with_strip_outer_array) {
    TBrokerRangeDesc range;
    range.__set_json_root("$.data");
    range.__set_strip_outer_array(true);
    auto result = scan(range, {"k1", "k2"},
                       {R"({"data":[{"k1":"a","k2":1},{"k1":"b")", R"({"data":[{"k1":"c","k2":3},{"k1":"d","k2":4}]})",
                        R"({"data":[{"k1":"e","k2":5}]} {"data":[{"k1":"f")", R"({"data":[{"k1":"g","k2":)",
                        R"({"data":[{"k1":"h","k2":8}]})"});
    EXPECT_TRUE(result.status.is_end_of_file()) << result.status;
    EXPECT_EQ(4, result.rows);
    EXPECT_EQ(3, result.filtered);
}

// A file is read as a single buffer, so a file without any complete document ends the scan with one error row.
TEST_F(JsonScannerTruncatedDocTest, file_without_complete_document) {
    std::string filename = "./be/test/exec/test_data/json_scanner/truncated_doc_test.json";
    write_json_to_file(filename, R"({"data":[{"k1":"a","k2":1},{"k1":"b")");
    DeferOp defer([&] { std::remove(filename.c_str()); });

    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.__set_json_root("$.data");
    range.__set_strip_outer_array(true);
    range.__set_path(filename);
    auto result = scan(range, {"k1", "k2"});
    EXPECT_TRUE(result.status.is_end_of_file()) << result.status;
    EXPECT_EQ(0, result.rows);
    EXPECT_EQ(1, result.filtered);
    // One error row, one line in the error log: the EOF that ends the file is not another error.
    std::string log = error_log();
    EXPECT_EQ(1, std::count(log.begin(), log.end(), '\n')) << log;
    EXPECT_NE(std::string::npos, log.find("Incomplete or malformed json document")) << log;
}

TEST_F(JsonScannerTruncatedDocTest, debezium) {
    TBrokerRangeDesc range;
    range.__set_envelope(TEnvelopeType::DEBEZIUM);
    auto result = scan(range, {"k1", "k2"},
                       {R"({"payload":{"before":null,"after":{"k1":"a","k2":1},"op":"c"}})",
                        R"({"payload":{"before":null,"after":{"k1":"b","k2":2},"op":"c"}} {"payload":{"after")"});
    EXPECT_TRUE(result.status.is_end_of_file()) << result.status;
    EXPECT_EQ(2, result.rows);
    EXPECT_EQ(1, result.filtered);
}

// With strip_outer_array the buffer is parsed as one array, which does not drop anything silently: a malformed
// element closed by ']' is an error row, and a truncated array fails the scan.
TEST_F(JsonScannerTruncatedDocTest, strip_outer_array) {
    TBrokerRangeDesc range;
    range.__set_strip_outer_array(true);
    auto result = scan(range, kK13Names, {"[" + kTruncatedDocument + "]"});
    EXPECT_TRUE(result.status.is_end_of_file()) << result.status;
    EXPECT_EQ(0, result.rows);
    EXPECT_EQ(1, result.filtered);

    result = scan(range, {"k1", "k2"}, {R"([{"k1":"a","k2":1},{"k1":"b","k2":2)"});
    EXPECT_TRUE(result.status.is_data_quality_error()) << result.status;
    EXPECT_NE(std::string::npos, result.status.message().find("parse error"));
}

// With log_rejected_record_num set, the whole dropped text also goes to the rejected record log.
TEST_F(JsonScannerTruncatedDocTest, rejected_record) {
    const std::string dir = "./json_scanner_truncated_doc_test_rejected_record";
    TestLoadPathMgr load_path_mgr(dir + "/records");
    _runtime_services.load_path_mgr = &load_path_mgr;
    DeferOp defer([&] {
        _runtime_services.load_path_mgr = nullptr;
        std::filesystem::remove_all(dir);
    });
    _state->_query_options.log_rejected_record_num = -1;

    auto result = scan({R"({"k1":"a","k2":1} {"k1":"b")", kTruncatedDocument});
    EXPECT_TRUE(result.status.is_end_of_file()) << result.status;
    EXPECT_EQ(1, result.rows);
    EXPECT_EQ(2, result.filtered);
    EXPECT_EQ(2, _state->_num_log_rejected_rows.load());

    std::vector<std::string> raw_records;
    std::ifstream in(dir + "/records.jsonl");
    for (std::string line; std::getline(in, line);) {
        rapidjson::Document record;
        record.Parse(line.data(), line.size());
        ASSERT_FALSE(record.HasParseError()) << line;
        raw_records.emplace_back(record["raw_record"]["_raw"].GetString());
        EXPECT_NE(std::string::npos, std::string(record["error_message"].GetString())
                                             .find("Incomplete or malformed json document at the end of the input"))
                << line;
    }
    EXPECT_EQ((std::vector<std::string>{R"({"k1":"b")", kTruncatedDocument}), raw_records);
}

} // namespace starrocks
