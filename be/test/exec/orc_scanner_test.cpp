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

#include <exec/orc_scanner.h>
#include <gtest/gtest.h>

#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

namespace starrocks {

class ORCScannerTest : public ::testing::Test {
public:
    TBrokerScanRange create_scan_range(const std::vector<std::string>& file_names) {
        TBrokerScanRange scan_range;

        std::vector<TBrokerRangeDesc> ranges;
        ranges.resize(file_names.size());
        for (auto i = 0; i < file_names.size(); ++i) {
            TBrokerRangeDesc& range = ranges[i];
            range.__set_path(file_names[i]);
            range.start_offset = 0;
            range.size = std::numeric_limits<int64_t>::max();
            range.file_type = TFileType::FILE_LOCAL;
            range.__set_format_type(TFileFormatType::FORMAT_ORC);
        }
        scan_range.ranges = ranges;
        return scan_range;
    }

    std::unique_ptr<ORCScanner> create_orc_scanner(const std::vector<TypeDescriptor>& types,
                                                   const std::vector<std::string>& col_names,
                                                   const std::vector<TBrokerRangeDesc>& ranges) {
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
        Status st = DescriptorTbl::create(_runtime_state.get(), &_obj_pool, desc_tbl_builder.desc_tbl(), &desc_tbl,
                                          config::vector_chunk_size);
        CHECK(st.ok()) << st.to_string();

        /// Init RuntimeState
        _runtime_state->set_desc_tbl(desc_tbl);
        _runtime_state->init_instance_mem_tracker();

        /// TBrokerScanRangeParams
        TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
        params->strict_mode = true;
        params->dest_tuple_id = 0;
        params->src_tuple_id = 0;
        params->json_file_size_limit = 1024 * 1024;
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

        TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
        broker_scan_range->params = *params;
        broker_scan_range->ranges = ranges;
        return std::make_unique<ORCScanner>(_runtime_state.get(), _profile, *broker_scan_range, _counter);
    }

    std::shared_ptr<RuntimeState> create_runtime_state() {
        TQueryOptions query_options;
        TUniqueId fragment_id;
        TQueryGlobals query_globals;
        std::shared_ptr<RuntimeState> runtime_state =
                std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, ExecEnv::GetInstance());
        TUniqueId id;
        runtime_state->init_mem_trackers(id);
        return runtime_state;
    }

    void SetUp() override {
        std::string starrocks_home = getenv("STARROCKS_HOME");
        _test_exec_dir = starrocks_home + "/be/test/exec";

        _runtime_state = create_runtime_state();

        _counter = _obj_pool.add(new ScannerCounter());
        _profile = _obj_pool.add(new RuntimeProfile("test"));
    }

private:
    std::string _test_exec_dir;
    ObjectPool _obj_pool;
    std::shared_ptr<RuntimeState> _runtime_state;
    RuntimeProfile* _profile = nullptr;
    ScannerCounter* _counter = nullptr;
};

TEST_F(ORCScannerTest, get_schema) {
    RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    auto scan_range = create_scan_range({_test_exec_dir + "/test_data/orc_scanner/type_mismatch.orc"});

    scan_range.params.__set_schema_sample_file_count(1);

    ScannerCounter counter{};
    RuntimeProfile profile{"test"};
    std::unique_ptr<ORCScanner> scanner = std::make_unique<ORCScanner>(&state, &profile, scan_range, &counter, true);

    auto st = scanner->open();
    EXPECT_TRUE(st.ok());

    std::vector<SlotDescriptor> schemas;
    st = scanner->get_schema(&schemas);
    EXPECT_TRUE(st.ok());

    EXPECT_EQ("VARCHAR(1048576)", schemas[0].type().debug_string());

    ASSERT_GT(counter.file_read_count, 0);
    ASSERT_GT(counter.file_read_ns, 0);
}

TEST_F(ORCScannerTest, implicit_cast) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(32));
    types.emplace_back(TypeDescriptor::create_varchar_type(32));

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_ORC;
    range.file_type = TFileType::FILE_LOCAL;
    range.__set_path(_test_exec_dir + "/test_data/orc_scanner/boolean_type.orc");
    ranges.push_back(range);
    range.__set_path(_test_exec_dir + "/test_data/orc_scanner/date_type.orc");
    ranges.push_back(range);

    auto scanner = create_orc_scanner(types, {"col_0", "col_1"}, ranges);

    EXPECT_OK(scanner->open());

    auto result = scanner->get_next();
    EXPECT_OK(result.status());
    auto chunk = result.value();
    EXPECT_EQ(2, chunk->num_columns());
    EXPECT_EQ(2, chunk->num_rows());
    EXPECT_EQ("['16', '1']", chunk->debug_row(0));
    EXPECT_EQ("['16', '0']", chunk->debug_row(1));

    result = scanner->get_next();
    EXPECT_OK(result.status());
    chunk = result.value();
    EXPECT_EQ(2, chunk->num_columns());
    EXPECT_EQ(2, chunk->num_rows());
    EXPECT_EQ("['11', '2020-01-01']", chunk->debug_row(0));
    EXPECT_EQ("['11', '2020-01-02']", chunk->debug_row(1));
}

} // namespace starrocks
