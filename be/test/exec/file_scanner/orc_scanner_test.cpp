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

#include "exec/file_scanner/orc_scanner.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "glog/logging.h"
#include "orc/OrcFile.hh"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

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
                                                   const std::vector<TBrokerRangeDesc>& ranges,
                                                   ScannerCounter* counter = nullptr) {
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
        ScannerCounter* counter_ptr = counter != nullptr ? counter : _counter;
        auto scanner = std::make_unique<ORCScanner>(_runtime_state.get(), _profile, *broker_scan_range, counter_ptr);
        EXPECT_EQ("orc", scanner->file_format());
        // scan_type is not set in TBrokerScanRangeParams, default to LOAD
        EXPECT_EQ("load", scanner->scan_type());
        return scanner;
    }

    std::string data_file_path(const std::string& file_name) const {
        return _test_exec_dir + "/test_data/orc_scanner/" + file_name;
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

protected:
    struct MultiStripeContext {
        struct StripeInfo {
            uint64_t offset;
            uint64_t length;
            uint64_t rows;
            size_t row_start;
        };

        std::string file_path;
        uint64_t file_length = 0;
        size_t total_rows = 0;
        std::vector<StripeInfo> stripes;
        std::vector<std::string> baseline_rows;
    };

    Status _init_multi_stripe_context(MultiStripeContext* ctx);
    Status _scan_multi_stripes(const std::vector<TBrokerRangeDesc>& ranges, std::vector<std::string>* out_rows,
                               ScannerCounter* out_counter = nullptr);
    TBrokerRangeDesc _build_range(const std::string& file_path, uint64_t file_length, uint64_t start,
                                  uint64_t end) const;

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
    range.__set_start_offset(0);
    range.__set_size(-1);
    ranges.push_back(range);
    range.__set_path(_test_exec_dir + "/test_data/orc_scanner/date_type.orc");
    range.__set_start_offset(0);
    range.__set_size(-1);
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
    EXPECT_EQ(ranges.size(), scanner->TEST_scanner_counter()->num_files_read);
}

Status ORCScannerTest::_init_multi_stripe_context(MultiStripeContext* ctx) {
    DCHECK(ctx != nullptr);
    ctx->file_path = data_file_path("multi_stripes.orc");

    try {
        auto reader = orc::createReader(orc::readLocalFile(ctx->file_path), orc::ReaderOptions{});
        ctx->file_length = reader->getFileLength();
        ctx->stripes.clear();
        size_t row_index = 0;
        uint64_t stripe_count = reader->getNumberOfStripes();
        ctx->stripes.reserve(stripe_count);
        for (uint64_t i = 0; i < stripe_count; ++i) {
            auto stripe = reader->getStripe(i);
            MultiStripeContext::StripeInfo info;
            info.offset = stripe->getOffset();
            info.length = stripe->getLength();
            info.rows = stripe->getNumberOfRows();
            info.row_start = row_index;
            row_index += static_cast<size_t>(info.rows);
            ctx->stripes.emplace_back(std::move(info));
        }
        ctx->total_rows = row_index;
        RETURN_IF_ERROR(_scan_multi_stripes({_build_range(ctx->file_path, ctx->file_length, 0, ctx->file_length)},
                                            &ctx->baseline_rows));
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError(
                strings::Substitute("Failed to load ORC metadata for $0: $1", ctx->file_path, e.what()));
    }
}

Status ORCScannerTest::_scan_multi_stripes(const std::vector<TBrokerRangeDesc>& ranges,
                                           std::vector<std::string>* out_rows, ScannerCounter* out_counter) {
    DCHECK(out_rows != nullptr);
    out_rows->clear();

    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor(TYPE_INT));
    types.emplace_back(TypeDescriptor::create_varchar_type(1048576));
    std::vector<std::string> col_names = {"c0", "c1"};

    ScannerCounter local_counter{};
    ScannerCounter* counter_ptr = &local_counter;
    if (out_counter != nullptr) {
        *out_counter = ScannerCounter{};
        counter_ptr = out_counter;
    }

    auto scanner = create_orc_scanner(types, col_names, ranges, counter_ptr);
    RETURN_IF_ERROR(scanner->open());
    DeferOp close_scanner([&]() { scanner->close(); });

    while (true) {
        auto result = scanner->get_next();
        if (result.status().is_end_of_file()) {
            break;
        }
        RETURN_IF_ERROR(result.status());
        ChunkPtr chunk = result.value();
        for (size_t i = 0; i < chunk->num_rows(); ++i) {
            out_rows->emplace_back(chunk->debug_row(i));
        }
    }
    return Status::OK();
}

TBrokerRangeDesc ORCScannerTest::_build_range(const std::string& file_path, uint64_t file_length, uint64_t start,
                                              uint64_t end) const {
    TBrokerRangeDesc range;
    range.file_type = TFileType::FILE_LOCAL;
    range.__set_format_type(TFileFormatType::FORMAT_ORC);
    range.__set_path(file_path);
    range.start_offset = static_cast<int64_t>(start);
    range.size = static_cast<int64_t>(end - start);
    range.__set_splittable(true);
    range.__set_file_size(static_cast<int64_t>(file_length));
    return range;
}

TEST_F(ORCScannerTest, selective_ranges_return_expected_rows) {
    MultiStripeContext ctx;
    ASSERT_OK(_init_multi_stripe_context(&ctx));
    ASSERT_GE(ctx.stripes.size(), 2);
    const auto& first = ctx.stripes[0];
    const auto& second = ctx.stripes[1];

    struct {
        const char* label;
        uint64_t start;
        uint64_t end;
        size_t expected_row_start;
        size_t expected_row_count;
    } cases[] = {{"first_stripe", first.offset, first.offset + first.length, first.row_start,
                  static_cast<size_t>(first.rows)},
                 {"second_stripe", second.offset, second.offset + second.length, second.row_start,
                  static_cast<size_t>(second.rows)},
                 {"interior_start", first.offset + std::max<uint64_t>(uint64_t{1}, first.length / 2),
                  second.offset + second.length, second.row_start, static_cast<size_t>(second.rows)}};

    for (const auto& c : cases) {
        SCOPED_TRACE(c.label);
        std::vector<std::string> actual_rows;
        ScannerCounter counter;
        std::vector<TBrokerRangeDesc> ranges = {
                _build_range(ctx.file_path, ctx.file_length, c.start, c.end),
        };
        ASSERT_OK(_scan_multi_stripes(ranges, &actual_rows, &counter));

        std::vector<std::string> expected_rows(ctx.baseline_rows.begin() + c.expected_row_start,
                                               ctx.baseline_rows.begin() + c.expected_row_start + c.expected_row_count);
        EXPECT_EQ(expected_rows, actual_rows);
        EXPECT_EQ(0, counter.num_rows_filtered);
    }
    EXPECT_GT(second.offset, first.offset);
}

TEST_F(ORCScannerTest, zero_length_range_returns_empty_scan) {
    MultiStripeContext ctx;
    ASSERT_OK(_init_multi_stripe_context(&ctx));
    ASSERT_FALSE(ctx.stripes.empty());
    const auto& first = ctx.stripes.front();

    std::vector<std::string> actual_rows;
    ScannerCounter counter;
    std::vector<TBrokerRangeDesc> ranges = {
            _build_range(ctx.file_path, ctx.file_length, first.offset, first.offset),
    };
    ASSERT_OK(_scan_multi_stripes(ranges, &actual_rows, &counter));

    EXPECT_TRUE(actual_rows.empty());
    EXPECT_EQ(0, counter.num_rows_filtered);
}

TEST_F(ORCScannerTest, adjacent_ranges_cover_entire_file) {
    MultiStripeContext ctx;
    ASSERT_OK(_init_multi_stripe_context(&ctx));
    ASSERT_GE(ctx.stripes.size(), 2);
    const auto& first = ctx.stripes[0];
    const auto& second = ctx.stripes[1];

    std::vector<TBrokerRangeDesc> ranges = {
            _build_range(ctx.file_path, ctx.file_length, first.offset, first.offset + first.length),
            _build_range(ctx.file_path, ctx.file_length, second.offset, ctx.file_length),
    };

    std::vector<std::string> actual_rows;
    ScannerCounter counter;
    ASSERT_OK(_scan_multi_stripes(ranges, &actual_rows, &counter));

    EXPECT_EQ(ctx.baseline_rows, actual_rows);
    EXPECT_EQ(0, counter.num_rows_filtered);
}

} // namespace starrocks
