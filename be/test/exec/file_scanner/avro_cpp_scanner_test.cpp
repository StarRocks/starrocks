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

#include "exec/file_scanner/avro_cpp_scanner.h"

#include <gtest/gtest.h>

#include "exec/file_scanner/file_scanner.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

namespace starrocks {

class AvroCppScannerTest : public ::testing::Test {
public:
    void SetUp() override {
        std::string starrocks_home = getenv("STARROCKS_HOME");
        _test_exec_dir = starrocks_home + "/be/test/formats/test_data/avro/cpp/";
        _counter = _obj_pool.add(new ScannerCounter());
        _profile = _obj_pool.add(new RuntimeProfile("test"));
        _state = create_runtime_state();
        _timezone = cctz::utc_time_zone();
    }

    AvroReaderUniquePtr create_avro_reader(const std::string& filename) {
        auto file_or = FileSystem::Default()->new_random_access_file(_test_exec_dir + filename);
        CHECK_OK(file_or.status());

        auto avro_reader = std::make_unique<AvroReader>();
        CHECK_OK(avro_reader->init(std::make_unique<AvroBufferInputStream>(
                                           std::move(file_or.value()), config::avro_reader_buffer_size_bytes, _counter),
                                   filename, _state.get(), _counter, nullptr, &_column_readers, true));
        return avro_reader;
    }

    std::vector<TBrokerRangeDesc> create_range_descs(const std::vector<std::string>& file_names,
                                                     int32_t num_of_columns_from_file) {
        std::vector<TBrokerRangeDesc> ranges;
        ranges.resize(file_names.size());
        for (auto i = 0; i < file_names.size(); ++i) {
            TBrokerRangeDesc& range = ranges[i];
            range.__set_path(_test_exec_dir + file_names[i]);
            range.start_offset = 0;
            range.size = std::numeric_limits<int64_t>::max();
            range.file_type = TFileType::FILE_LOCAL;
            range.__set_format_type(TFileFormatType::FORMAT_AVRO);
            range.__set_num_of_columns_from_file(num_of_columns_from_file);
        }
        return ranges;
    }

    std::unique_ptr<AvroCppScanner> create_avro_scanner(const std::vector<std::string>& col_names,
                                                        const std::vector<TypeDescriptor>& type_descs,
                                                        const std::vector<TBrokerRangeDesc>& ranges) {
        // init DescriptorTable
        TDescriptorTableBuilder desc_tbl_builder;
        TTupleDescriptorBuilder tuple_desc_builder;
        for (int i = 0; i < type_descs.size(); ++i) {
            TSlotDescriptorBuilder slot_desc_builder;
            slot_desc_builder.type(type_descs[i]).column_name(col_names[i]).length(type_descs[i].len).nullable(true);
            tuple_desc_builder.add_slot(slot_desc_builder.build());
        }
        tuple_desc_builder.build(&desc_tbl_builder);

        DescriptorTbl* desc_tbl = nullptr;
        Status st = DescriptorTbl::create(_state.get(), &_obj_pool, desc_tbl_builder.desc_tbl(), &desc_tbl,
                                          config::vector_chunk_size);
        CHECK(st.ok()) << st.to_string();

        // init RuntimeState
        _state->set_desc_tbl(desc_tbl);
        _state->init_instance_mem_tracker();

        // init TBrokerScanRangeParams
        TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
        params->strict_mode = true;
        params->dest_tuple_id = 0;
        params->src_tuple_id = 0;
        for (int i = 0; i < type_descs.size(); i++) {
            params->expr_of_dest_slot[i] = TExpr();
            params->expr_of_dest_slot[i].nodes.emplace_back(TExprNode());
            params->expr_of_dest_slot[i].nodes[0].__set_type(type_descs[i].to_thrift());
            params->expr_of_dest_slot[i].nodes[0].__set_node_type(TExprNodeType::SLOT_REF);
            params->expr_of_dest_slot[i].nodes[0].__set_is_nullable(true);
            params->expr_of_dest_slot[i].nodes[0].__set_slot_ref(TSlotRef());
            params->expr_of_dest_slot[i].nodes[0].slot_ref.__set_slot_id(i);
            params->expr_of_dest_slot[i].nodes[0].__set_type(type_descs[i].to_thrift());
        }

        for (int i = 0; i < type_descs.size(); i++) {
            params->src_slot_ids.emplace_back(i);
        }

        TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
        broker_scan_range->params = *params;
        broker_scan_range->ranges = ranges;

        return std::make_unique<AvroCppScanner>(_state.get(), _profile, *broker_scan_range, _counter);
    }

private:
    std::shared_ptr<RuntimeState> create_runtime_state() {
        TQueryOptions query_options;
        TUniqueId fragment_id;
        TQueryGlobals query_globals;
        query_globals.__set_time_zone("Etc/UTC");
        std::shared_ptr<RuntimeState> state =
                std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, ExecEnv::GetInstance());
        TUniqueId id;
        state->init_mem_trackers(id);
        return state;
    }

    std::string _test_exec_dir;
    ObjectPool _obj_pool;
    ScannerCounter* _counter;
    RuntimeProfile* _profile;
    std::shared_ptr<RuntimeState> _state;
    cctz::time_zone _timezone;
    std::vector<avrocpp::ColumnReaderUniquePtr> _column_readers;
};

TEST_F(AvroCppScannerTest, test_read_primitive_types) {
    std::string filename = "primitive.avro";
    std::vector<std::string> col_names;
    std::vector<TypeDescriptor> type_descs;
    std::vector<TBrokerRangeDesc> ranges;

    // init reader for read schema
    auto reader = create_avro_reader(filename);

    std::vector<SlotDescriptor> slot_descs;
    ASSERT_OK(reader->get_schema(&slot_descs));

    for (auto& slot_desc : slot_descs) {
        col_names.emplace_back(slot_desc.col_name());
        type_descs.emplace_back(slot_desc.type());
    }

    // init scan range descs
    ranges = create_range_descs({filename, filename}, slot_descs.size());

    // init scanner for read data
    auto scanner = create_avro_scanner(col_names, type_descs, ranges);
    CHECK_OK(scanner->open());

    // read data
    auto chunk_or = scanner->get_next();
    ASSERT_TRUE(chunk_or.ok());
    auto chunk = chunk_or.value();
    ASSERT_EQ(1, chunk->num_rows());
    ASSERT_EQ("[NULL, 1, 123, 1234567890123, 3.14, 2.71828, 'abc', 'hello avro']", chunk->debug_row(0));

    chunk_or = scanner->get_next();
    ASSERT_TRUE(chunk_or.ok());
    chunk = chunk_or.value();
    ASSERT_EQ(1, chunk->num_rows());
    ASSERT_EQ("[NULL, 1, 123, 1234567890123, 3.14, 2.71828, 'abc', 'hello avro']", chunk->debug_row(0));

    chunk_or = scanner->get_next();
    ASSERT_TRUE(chunk_or.status().is_end_of_file());

    scanner->close();
}

TEST_F(AvroCppScannerTest, test_read_complex_types) {
    std::string filename = "complex.avro";
    std::vector<std::string> col_names;
    std::vector<TypeDescriptor> type_descs;
    std::vector<TBrokerRangeDesc> ranges;

    // init reader for read schema
    auto reader = create_avro_reader(filename);

    std::vector<SlotDescriptor> slot_descs;
    ASSERT_OK(reader->get_schema(&slot_descs));

    for (auto& slot_desc : slot_descs) {
        col_names.emplace_back(slot_desc.col_name());
        type_descs.emplace_back(slot_desc.type());
    }

    // init scan range descs
    ranges = create_range_descs({filename}, slot_descs.size());

    // init scanner for read data
    auto scanner = create_avro_scanner(col_names, type_descs, ranges);
    CHECK_OK(scanner->open());

    // read data
    auto chunk_or = scanner->get_next();
    ASSERT_TRUE(chunk_or.ok());
    auto chunk = chunk_or.value();
    ASSERT_EQ(1, chunk->num_rows());
    ASSERT_EQ("[{id:1,name:'avro'}, 'HEARTS', ['one','two','three'], {'a':1,'b':2}, 100, 'abababababababab']",
              chunk->debug_row(0));

    chunk_or = scanner->get_next();
    ASSERT_TRUE(chunk_or.status().is_end_of_file());

    scanner->close();
}

TEST_F(AvroCppScannerTest, test_read_complex_nest_types) {
    std::string filename = "complex_nest.avro";
    std::vector<std::string> col_names;
    std::vector<TypeDescriptor> type_descs;
    std::vector<TBrokerRangeDesc> ranges;

    // init reader for read schema
    auto reader = create_avro_reader(filename);

    std::vector<SlotDescriptor> slot_descs;
    ASSERT_OK(reader->get_schema(&slot_descs));

    for (auto& slot_desc : slot_descs) {
        col_names.emplace_back(slot_desc.col_name());
        type_descs.emplace_back(slot_desc.type());
    }

    // init scan range descs
    ranges = create_range_descs({filename}, slot_descs.size());

    // init scanner for read data
    auto scanner = create_avro_scanner(col_names, type_descs, ranges);
    CHECK_OK(scanner->open());

    // read data
    auto chunk_or = scanner->get_next();
    ASSERT_TRUE(chunk_or.ok());
    auto chunk = chunk_or.value();
    ASSERT_EQ(1, chunk->num_rows());
    ASSERT_EQ(
            "[{inner:{val:123}}, {list:[1,2,3]}, {dict:{'a':'x','b':'y'}}, [{value:'one'},{value:'two'}], "
            "[[1,2],[3,4]], [{'a':1,'b':2},{'c':3}], {'r1':{id:10},'r2':{id:20}}, {'nums':[5,6,7]}, "
            "{'outer':{'inner':'val'}}, {entries:[{flag:1},{flag:0}]}, {entries:[{'x':'1'},{'y':'2'}]}, "
            "[{'one':{ok:1}},{'two':{ok:0}}], {'group':[{score:99.9},{score:88.8}]}]",
            chunk->debug_row(0));

    chunk_or = scanner->get_next();
    ASSERT_TRUE(chunk_or.status().is_end_of_file());

    scanner->close();
}

TEST_F(AvroCppScannerTest, test_read_logical_types) {
    std::string filename = "logical.avro";
    std::vector<std::string> col_names;
    std::vector<TypeDescriptor> type_descs;
    std::vector<TBrokerRangeDesc> ranges;

    // init reader for read schema
    auto reader = create_avro_reader(filename);

    std::vector<SlotDescriptor> slot_descs;
    ASSERT_OK(reader->get_schema(&slot_descs));

    // add the last duration type column ut later
    for (int i = 0; i < slot_descs.size() - 1; ++i) {
        col_names.emplace_back(slot_descs[i].col_name());
        type_descs.emplace_back(slot_descs[i].type());
    }

    // init scan range descs
    ranges = create_range_descs({filename}, slot_descs.size() - 1);

    // init scanner for read data
    auto scanner = create_avro_scanner(col_names, type_descs, ranges);
    CHECK_OK(scanner->open());

    // read data
    auto chunk_or = scanner->get_next();
    ASSERT_TRUE(chunk_or.ok());
    auto chunk = chunk_or.value();
    ASSERT_EQ(1, chunk->num_rows());
    ASSERT_EQ(
            "[1234.56, 1234.56, '61ed1775-2ce2-4f88-8352-1da6847512d6', 2025-04-11, 55543806, 55543806481, "
            "2025-04-11 07:25:43.806000, 2025-04-11 07:25:43.806481, 1744356343806, 1744356343806481]",
            chunk->debug_row(0));

    chunk_or = scanner->get_next();
    ASSERT_TRUE(chunk_or.status().is_end_of_file());

    scanner->close();
}

} // namespace starrocks
