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

#include "connector/file/scanner/arrow_scanner.h"

#include <arrow/builder.h>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/compression.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <utility>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/arrow/arrow_memory_pool.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/config_ingest_fwd.h"
#include "common/status.h"
#include "compute_env/load/load_stream_mgr.h"
#include "compute_env/load_path/load_path_mgr.h"
#include "compute_env/load_path/load_path_state_helper.h"
#include "compute_env/load_path/rejected_record_writer.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/service_contexts.h"
#include "testutil/desc_tbl_helper.h"
#include "types/type_descriptor.h"

namespace starrocks {

#define ASSERT_ARROW_OK(status)                                                   \
    do {                                                                          \
        auto&& _status = (status);                                                \
        ASSERT_TRUE(_status.ok()) << "Arrow call failed: " << _status.ToString(); \
    } while (0)

class ArrowScannerTest : public ::testing::Test {
public:
    static void SetUpTestSuite() {
        const char* starrocks_home = getenv("STARROCKS_HOME");
        ASSERT_NE(nullptr, starrocks_home);
        _tmp_root_dir = std::filesystem::path(starrocks_home) / "be/test/exec/test_data/arrow_scanner/tmp";
        ASSERT_FALSE(_tmp_root_dir.empty());

        std::error_code ec;
        std::filesystem::create_directories(_tmp_root_dir, ec);
        ASSERT_FALSE(ec) << "failed to create directory " << _tmp_root_dir << ": " << ec.message();
    }

    static void TearDownTestSuite() {
        std::error_code ec;
        std::filesystem::remove_all(_tmp_root_dir, ec);
        ASSERT_FALSE(ec) << "failed to remove directory " << _tmp_root_dir << ": " << ec.message();
    }

protected:
    void SetUp() override {
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        _runtime_state = new RuntimeState(TUniqueId(), query_options, query_globals, nullptr);
    }

    void TearDown() override {
        delete _runtime_state;
        _obj_pool.clear();
    }

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
            range.__set_format_type(TFileFormatType::FORMAT_ARROW);
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

    std::unique_ptr<ArrowScanner> create_arrow_scanner(
            const std::string& timezone, DescriptorTbl* desc_tbl,
            const std::unordered_map<size_t, ::starrocks::TExpr>& dst_slot_exprs,
            const std::vector<TBrokerRangeDesc>& ranges, int32_t batch_size = 0) {
        TQueryOptions query_options;
        if (batch_size > 0) {
            query_options.__set_batch_size(batch_size);
        }
        auto query_globals = TQueryGlobals();
        query_globals.time_zone = timezone;
        RuntimeState* state = _obj_pool.add(new RuntimeState(TUniqueId(), query_options, query_globals, nullptr));
        state->set_desc_tbl(desc_tbl);
        state->init_instance_mem_tracker();

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

        auto scanner = std::make_unique<ArrowScanner>(state, profile, *broker_scan_range, counter);
        EXPECT_EQ("arrow", scanner->file_format());
        EXPECT_EQ("load", scanner->scan_type());
        return scanner;
    }

    void create_arrow_stream_file(const std::string& file_name, std::string* file_path) {
        *file_path = (_tmp_root_dir / file_name).string();

        arrow::Int32Builder int_builder;
        arrow::StringBuilder str_builder;
        arrow::DoubleBuilder double_builder;

        ASSERT_ARROW_OK(int_builder.AppendValues({1, 2, 3, 4, 5}));
        ASSERT_ARROW_OK(str_builder.AppendValues({"a", "b", "c", "d", "e"}));
        ASSERT_ARROW_OK(double_builder.AppendValues({1.1, 2.2, 3.3, 4.4, 5.5}));

        std::shared_ptr<arrow::Array> int_array;
        std::shared_ptr<arrow::Array> str_array;
        std::shared_ptr<arrow::Array> double_array;

        ASSERT_ARROW_OK(int_builder.Finish(&int_array));
        ASSERT_ARROW_OK(str_builder.Finish(&str_array));
        ASSERT_ARROW_OK(double_builder.Finish(&double_array));

        auto schema = arrow::schema({arrow::field("c0_int", arrow::int32()), arrow::field("c1_str", arrow::utf8()),
                                     arrow::field("c2_double", arrow::float64())});

        auto batch = arrow::RecordBatch::Make(schema, 5, {int_array, str_array, double_array});

        auto out_file_res = arrow::io::FileOutputStream::Open(*file_path);
        ASSERT_ARROW_OK(out_file_res.status());
        auto out_file = out_file_res.ValueOrDie();

        auto writer_res = arrow::ipc::MakeStreamWriter(out_file, schema);
        ASSERT_ARROW_OK(writer_res.status());
        auto writer = writer_res.ValueOrDie();

        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        ASSERT_ARROW_OK(out_file->Close());
    }

    inline static std::filesystem::path _tmp_root_dir;
    RuntimeState* _runtime_state = nullptr;
    ObjectPool _obj_pool;
};

TEST_F(ArrowScannerTest, TestScanArrowStream) {
    std::string file_path;
    create_arrow_stream_file("test_stream.arrow", &file_path);

    std::vector<std::string> file_names{file_path};
    std::vector<std::string> columns{"c0_int", "c1_str", "c2_double"};

    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    src_slot_infos.emplace_back("c1_str", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);
    src_slot_infos.emplace_back("c2_double", TypeDescriptor::from_logical_type(TYPE_DOUBLE), true);

    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;

    auto ranges = generate_ranges(file_names, columns.size(), {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {src_slot_infos, dst_slot_infos});
    auto scanner = create_arrow_scanner("UTC", desc_tbl, {}, ranges);

    ASSERT_OK(scanner->open());
    auto res = scanner->get_next();
    ASSERT_OK(res.status());
    auto chunk = res.value();
    ASSERT_NE(nullptr, chunk);
    ASSERT_EQ(5, chunk->num_rows());

    auto c0 = chunk->columns()[0];
    auto c1 = chunk->columns()[1];
    auto c2 = chunk->columns()[2];

    ASSERT_EQ(1, c0->get(0).get_int32());
    ASSERT_EQ("a", c1->get(0).get_slice());
    ASSERT_DOUBLE_EQ(1.1, c2->get(0).get_double());

    ASSERT_EQ(5, c0->get(4).get_int32());
    ASSERT_EQ("e", c1->get(4).get_slice());
    ASSERT_DOUBLE_EQ(5.5, c2->get(4).get_double());

    // Next get_next should return EOF
    auto res2 = scanner->get_next();
    ASSERT_TRUE(res2.status().is_end_of_file());

    scanner->close();
}

TEST_F(ArrowScannerTest, TestScanArrowStreamMismatchAndCast) {
    std::string file_path = (_tmp_root_dir / "test_mismatch_cast.arrow").string();

    arrow::Int32Builder int_builder;
    arrow::StringBuilder str_builder;
    arrow::Int32Builder extra_int_builder;

    ASSERT_ARROW_OK(int_builder.AppendValues({100, 200, 300, 400, 500}));
    ASSERT_ARROW_OK(str_builder.AppendValues({"a", "b", "c", "d", "e"}));
    ASSERT_ARROW_OK(extra_int_builder.AppendValues({10, 20, 30, 40, 50}));

    std::shared_ptr<arrow::Array> int_array;
    std::shared_ptr<arrow::Array> str_array;
    std::shared_ptr<arrow::Array> extra_array;

    ASSERT_ARROW_OK(int_builder.Finish(&int_array));
    ASSERT_ARROW_OK(str_builder.Finish(&str_array));
    ASSERT_ARROW_OK(extra_int_builder.Finish(&extra_array));

    // File schema contains extra column c3_extra, and cols are in different order
    auto schema = arrow::schema({arrow::field("c3_extra", arrow::int32()), arrow::field("c1_str", arrow::utf8()),
                                 arrow::field("c0_bigint", arrow::int32())});

    auto batch = arrow::RecordBatch::Make(schema, 5, {extra_array, str_array, int_array});

    auto out_file_res = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_ARROW_OK(out_file_res.status());
    auto out_file = out_file_res.ValueOrDie();

    auto writer_res = arrow::ipc::MakeStreamWriter(out_file, schema);
    ASSERT_ARROW_OK(writer_res.status());
    auto writer = writer_res.ValueOrDie();

    ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
    ASSERT_ARROW_OK(writer->Close());
    ASSERT_ARROW_OK(out_file->Close());

    std::vector<std::string> file_names{file_path};
    std::vector<std::string> columns{"c0_bigint", "c1_str", "c2_null"};

    // c0_bigint is TYPE_BIGINT in StarRocks, but arrow file had int32 (requires cast)
    // c1_str is TYPE_VARCHAR in StarRocks, and arrow file has utf8
    // c2_null is TYPE_INT in StarRocks, but not present in arrow file (should be NULL)
    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_bigint", TypeDescriptor::from_logical_type(TYPE_BIGINT), true);
    src_slot_infos.emplace_back("c1_str", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);
    src_slot_infos.emplace_back("c2_null", TypeDescriptor::from_logical_type(TYPE_INT), true);

    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;

    auto ranges = generate_ranges(file_names, columns.size(), {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {src_slot_infos, dst_slot_infos});
    auto scanner = create_arrow_scanner("UTC", desc_tbl, {}, ranges);

    ASSERT_OK(scanner->open());
    auto res = scanner->get_next();
    ASSERT_OK(res.status());
    auto chunk = res.value();
    ASSERT_NE(nullptr, chunk);
    ASSERT_EQ(5, chunk->num_rows());

    auto c0 = chunk->columns()[0];
    auto c1 = chunk->columns()[1];
    auto c2 = chunk->columns()[2];

    ASSERT_EQ(100, c0->get(0).get_int64());
    ASSERT_EQ("a", c1->get(0).get_slice());
    ASSERT_TRUE(c2->is_null(0));

    ASSERT_EQ(500, c0->get(4).get_int64());
    ASSERT_EQ("e", c1->get(4).get_slice());
    ASSERT_TRUE(c2->is_null(4));

    // Next get_next should return EOF
    auto res2 = scanner->get_next();
    ASSERT_TRUE(res2.status().is_end_of_file());

    scanner->close();
}

TEST_F(ArrowScannerTest, TestScanArrowStreamNullColumnChunkBoundary) {
    std::string file_path = (_tmp_root_dir / "test_null_column_boundary.arrow").string();

    arrow::Int32Builder int_builder;
    arrow::StringBuilder str_builder;

    ASSERT_ARROW_OK(int_builder.AppendValues({100, 200, 300, 400, 500}));
    ASSERT_ARROW_OK(str_builder.AppendValues({"a", "b", "c", "d", "e"}));

    std::shared_ptr<arrow::Array> int_array;
    std::shared_ptr<arrow::Array> str_array;

    ASSERT_ARROW_OK(int_builder.Finish(&int_array));
    ASSERT_ARROW_OK(str_builder.Finish(&str_array));

    auto schema = arrow::schema({arrow::field("c1_str", arrow::utf8()), arrow::field("c0_bigint", arrow::int32())});

    auto batch = arrow::RecordBatch::Make(schema, 5, {str_array, int_array});

    auto out_file_res = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_ARROW_OK(out_file_res.status());
    auto out_file = out_file_res.ValueOrDie();

    auto writer_res = arrow::ipc::MakeStreamWriter(out_file, schema);
    ASSERT_ARROW_OK(writer_res.status());
    auto writer = writer_res.ValueOrDie();

    ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
    ASSERT_ARROW_OK(writer->Close());
    ASSERT_ARROW_OK(out_file->Close());

    std::vector<std::string> file_names{file_path};
    std::vector<std::string> columns{"c0_bigint", "c1_str", "c2_null"};

    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_bigint", TypeDescriptor::from_logical_type(TYPE_BIGINT), true);
    src_slot_infos.emplace_back("c1_str", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);
    src_slot_infos.emplace_back("c2_null", TypeDescriptor::from_logical_type(TYPE_INT), true);

    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;

    auto ranges = generate_ranges(file_names, columns.size(), {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {src_slot_infos, dst_slot_infos});
    // Create the scanner with batch_size = 2
    auto scanner = create_arrow_scanner("UTC", desc_tbl, {}, ranges, 2);

    ASSERT_OK(scanner->open());

    // 1st chunk: should have 2 rows
    {
        auto res = scanner->get_next();
        ASSERT_OK(res.status());
        auto chunk = res.value();
        ASSERT_NE(nullptr, chunk);
        ASSERT_EQ(2, chunk->num_rows());

        auto c0 = chunk->columns()[0];
        auto c1 = chunk->columns()[1];
        auto c2 = chunk->columns()[2];

        ASSERT_EQ(100, c0->get(0).get_int64());
        ASSERT_EQ("a", c1->get(0).get_slice());
        ASSERT_TRUE(c2->is_null(0));

        ASSERT_EQ(200, c0->get(1).get_int64());
        ASSERT_EQ("b", c1->get(1).get_slice());
        ASSERT_TRUE(c2->is_null(1));
    }

    // 2nd chunk: should have 2 rows
    {
        auto res = scanner->get_next();
        ASSERT_OK(res.status());
        auto chunk = res.value();
        ASSERT_NE(nullptr, chunk);
        ASSERT_EQ(2, chunk->num_rows());

        auto c0 = chunk->columns()[0];
        auto c1 = chunk->columns()[1];
        auto c2 = chunk->columns()[2];

        ASSERT_EQ(300, c0->get(0).get_int64());
        ASSERT_EQ("c", c1->get(0).get_slice());
        ASSERT_TRUE(c2->is_null(0));

        ASSERT_EQ(400, c0->get(1).get_int64());
        ASSERT_EQ("d", c1->get(1).get_slice());
        ASSERT_TRUE(c2->is_null(1));
    }

    // 3rd chunk: should have 1 row
    {
        auto res = scanner->get_next();
        ASSERT_OK(res.status());
        auto chunk = res.value();
        ASSERT_NE(nullptr, chunk);
        ASSERT_EQ(1, chunk->num_rows());

        auto c0 = chunk->columns()[0];
        auto c1 = chunk->columns()[1];
        auto c2 = chunk->columns()[2];

        ASSERT_EQ(500, c0->get(0).get_int64());
        ASSERT_EQ("e", c1->get(0).get_slice());
        ASSERT_TRUE(c2->is_null(0));
    }

    // Next get_next should return EOF
    auto res2 = scanner->get_next();
    ASSERT_TRUE(res2.status().is_end_of_file());

    scanner->close();
}

TEST_F(ArrowScannerTest, TestScanArrowStreamStrictModeQualityError) {
    std::string file_path = (_tmp_root_dir / "test_strict_mode.arrow").string();

    arrow::Int32Builder int_builder;
    arrow::StringBuilder str_builder;

    ASSERT_ARROW_OK(int_builder.AppendValues({100, 200, 300}));
    ASSERT_ARROW_OK(str_builder.AppendValues({"a", "too_long_string", "b"}));

    auto int_field = std::make_shared<arrow::Field>("c0_bigint", arrow::int32());
    auto str_field = std::make_shared<arrow::Field>("c1_str", arrow::utf8());
    auto schema = arrow::schema({int_field, str_field});

    std::shared_ptr<arrow::Array> int_array;
    ASSERT_ARROW_OK(int_builder.Finish(&int_array));
    std::shared_ptr<arrow::Array> str_array;
    ASSERT_ARROW_OK(str_builder.Finish(&str_array));

    auto batch = arrow::RecordBatch::Make(schema, 3, {int_array, str_array});

    auto out_file_res = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_ARROW_OK(out_file_res.status());
    auto out_file = out_file_res.ValueOrDie();

    auto writer_res = arrow::ipc::MakeStreamWriter(out_file, schema);
    ASSERT_ARROW_OK(writer_res.status());
    auto writer = writer_res.ValueOrDie();

    ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
    ASSERT_ARROW_OK(writer->Close());
    ASSERT_ARROW_OK(out_file->Close());

    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_bigint", TypeDescriptor::from_logical_type(TYPE_BIGINT), true);
    src_slot_infos.emplace_back("c1_str", TypeDescriptor::create_char_type(2), true);

    SlotTypeDescInfoArray dst_slot_infos;
    dst_slot_infos.emplace_back("c0_bigint", TypeDescriptor::from_logical_type(TYPE_BIGINT), true);
    dst_slot_infos.emplace_back("c1_str", TypeDescriptor::create_char_type(2), true); // CHAR(2)

    std::vector<std::string> file_names = {file_path};
    auto ranges = generate_ranges(file_names, 2, {});

    TQueryOptions query_options;
    query_options.query_type = TQueryType::LOAD;
    query_options.log_rejected_record_num = 10;
    TQueryGlobals query_globals;
    query_globals.time_zone = "UTC";
    LoadPathMgr load_path_mgr({(_tmp_root_dir / "load_path").string()});
    ASSERT_OK(load_path_mgr.init());
    RuntimeServices runtime_services;
    runtime_services.load_path_mgr = &load_path_mgr;
    QueryExecutionServices query_execution_services;
    query_execution_services.runtime = &runtime_services;

    RuntimeState* state = _obj_pool.add(
            new RuntimeState(TUniqueId(), query_options, query_globals, &query_execution_services, nullptr));

    DescriptorTbl* desc_tbl = DescTblHelper::generate_desc_tbl(state, _obj_pool, {src_slot_infos, dst_slot_infos});
    state->set_desc_tbl(desc_tbl);
    state->init_instance_mem_tracker();
    state->set_db("test_db");
    state->set_load_label("test_label");
    state->set_txn_id(12345);

    TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
    params->strict_mode = true;
    params->__isset.strict_mode = true;
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
        params->expr_of_dest_slot[dst_slot->id()] =
                create_column_ref(src_slot->id(), src_slot->type(), src_slot->is_nullable());
        params->dest_sid_to_src_sid_without_trans[dst_slot->id()] = src_slot->id();
    }
    params->__isset.dest_sid_to_src_sid_without_trans = true;

    for (int i = 0; i < src_tuple->slots().size(); i++) {
        params->src_slot_ids.emplace_back(i);
    }

    RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("test_prof", true));
    ScannerCounter* counter = _obj_pool.add(new ScannerCounter());

    TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
    broker_scan_range->params = *params;
    broker_scan_range->ranges = ranges;

    auto scanner = std::make_unique<ArrowScanner>(state, profile, *broker_scan_range, counter);
    ASSERT_OK(scanner->open());

    auto res = scanner->get_next();
    ASSERT_OK(res.status());
    auto chunk = res.value();
    ASSERT_NE(nullptr, chunk);

    // Expecting 2 rows since the middle row failed quality checks and was filtered out
    ASSERT_EQ(2, chunk->num_rows());

    auto c0 = chunk->columns()[0];
    auto c1 = chunk->columns()[1];

    ASSERT_EQ(100, c0->get(0).get_int64());
    ASSERT_EQ("a", c1->get(0).get_slice());

    ASSERT_EQ(300, c0->get(1).get_int64());
    ASSERT_EQ("b", c1->get(1).get_slice());

    auto res2 = scanner->get_next();
    ASSERT_TRUE(res2.status().is_end_of_file());

    std::string error_log_path = state->get_error_log_file_path();
    scanner->close();

    std::string absolute_path = load_path_mgr.get_load_error_absolute_path(error_log_path);
    std::ifstream file(absolute_path);
    ASSERT_TRUE(file.is_open());
    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string error_log_content = buffer.str();

    ASSERT_FALSE(error_log_content.empty());
    ASSERT_NE(error_log_content.find("too_long_string"), std::string::npos);
}

TEST_F(ArrowScannerTest, TestScanArrowStreamEmptyFile) {
    std::string file_path = (_tmp_root_dir / "test_empty.arrow").string();

    arrow::Int32Builder int_builder;
    arrow::StringBuilder str_builder;

    auto int_field = std::make_shared<arrow::Field>("c0_int", arrow::int32());
    auto str_field = std::make_shared<arrow::Field>("c1_str", arrow::utf8());
    auto schema = arrow::schema({int_field, str_field});

    std::shared_ptr<arrow::Array> int_array;
    ASSERT_ARROW_OK(int_builder.Finish(&int_array));
    std::shared_ptr<arrow::Array> str_array;
    ASSERT_ARROW_OK(str_builder.Finish(&str_array));

    // Create record batch with 0 rows
    auto batch = arrow::RecordBatch::Make(schema, 0, {int_array, str_array});

    auto out_file_res = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_ARROW_OK(out_file_res.status());
    auto out_file = out_file_res.ValueOrDie();

    auto writer_res = arrow::ipc::MakeStreamWriter(out_file, schema);
    ASSERT_ARROW_OK(writer_res.status());
    auto writer = writer_res.ValueOrDie();

    ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
    ASSERT_ARROW_OK(writer->Close());
    ASSERT_ARROW_OK(out_file->Close());

    std::vector<std::string> file_names{file_path};
    std::vector<std::string> columns{"c0_int", "c1_str"};

    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    src_slot_infos.emplace_back("c1_str", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);

    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;

    auto ranges = generate_ranges(file_names, columns.size(), {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {src_slot_infos, dst_slot_infos});
    auto scanner = create_arrow_scanner("UTC", desc_tbl, {}, ranges);

    ASSERT_OK(scanner->open());
    auto res = scanner->get_next();
    // Since there are no rows, the first get_next should return EOF (end of file)
    ASSERT_TRUE(res.status().is_end_of_file());

    scanner->close();
}

TEST_F(ArrowScannerTest, TestScanArrowStreamNullable) {
    std::string file_path = (_tmp_root_dir / "test_nullable.arrow").string();

    arrow::Int32Builder int_builder;
    arrow::StringBuilder str_builder;

    ASSERT_ARROW_OK(int_builder.Append(10));
    ASSERT_ARROW_OK(int_builder.AppendNull());
    ASSERT_ARROW_OK(int_builder.Append(30));

    ASSERT_ARROW_OK(str_builder.AppendNull());
    ASSERT_ARROW_OK(str_builder.Append("hello"));
    ASSERT_ARROW_OK(str_builder.AppendNull());

    auto int_field = std::make_shared<arrow::Field>("c0_int", arrow::int32());
    auto str_field = std::make_shared<arrow::Field>("c1_str", arrow::utf8());
    auto schema = arrow::schema({int_field, str_field});

    std::shared_ptr<arrow::Array> int_array;
    ASSERT_ARROW_OK(int_builder.Finish(&int_array));
    std::shared_ptr<arrow::Array> str_array;
    ASSERT_ARROW_OK(str_builder.Finish(&str_array));

    auto batch = arrow::RecordBatch::Make(schema, 3, {int_array, str_array});

    auto out_file_res = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_ARROW_OK(out_file_res.status());
    auto out_file = out_file_res.ValueOrDie();

    auto writer_res = arrow::ipc::MakeStreamWriter(out_file, schema);
    ASSERT_ARROW_OK(writer_res.status());
    auto writer = writer_res.ValueOrDie();

    ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
    ASSERT_ARROW_OK(writer->Close());
    ASSERT_ARROW_OK(out_file->Close());

    std::vector<std::string> file_names{file_path};
    std::vector<std::string> columns{"c0_int", "c1_str"};

    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    src_slot_infos.emplace_back("c1_str", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);

    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;

    auto ranges = generate_ranges(file_names, columns.size(), {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {src_slot_infos, dst_slot_infos});
    auto scanner = create_arrow_scanner("UTC", desc_tbl, {}, ranges);

    ASSERT_OK(scanner->open());
    auto res = scanner->get_next();
    ASSERT_OK(res.status());
    auto chunk = res.value();
    ASSERT_NE(nullptr, chunk);
    ASSERT_EQ(3, chunk->num_rows());

    auto c0 = chunk->columns()[0];
    auto c1 = chunk->columns()[1];

    ASSERT_FALSE(c0->is_null(0));
    ASSERT_TRUE(c0->is_null(1));
    ASSERT_FALSE(c0->is_null(2));

    ASSERT_TRUE(c1->is_null(0));
    ASSERT_FALSE(c1->is_null(1));
    ASSERT_TRUE(c1->is_null(2));

    ASSERT_EQ(10, c0->get(0).get_int32());
    ASSERT_EQ(30, c0->get(2).get_int32());
    ASSERT_EQ("hello", c1->get(1).get_slice());

    auto res2 = scanner->get_next();
    ASSERT_TRUE(res2.status().is_end_of_file());

    scanner->close();
}

TEST_F(ArrowScannerTest, TestScanArrowStreamMultiBatch) {
    std::string file_path = (_tmp_root_dir / "test_multi_batch.arrow").string();

    auto int_field = std::make_shared<arrow::Field>("c0_int", arrow::int32());
    auto schema = arrow::schema({int_field});

    auto out_file_res = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_ARROW_OK(out_file_res.status());
    auto out_file = out_file_res.ValueOrDie();

    auto writer_res = arrow::ipc::MakeStreamWriter(out_file, schema);
    ASSERT_ARROW_OK(writer_res.status());
    auto writer = writer_res.ValueOrDie();

    // Write batch 1: [1, 2]
    {
        arrow::Int32Builder int_builder;
        ASSERT_ARROW_OK(int_builder.AppendValues({1, 2}));
        std::shared_ptr<arrow::Array> int_array;
        ASSERT_ARROW_OK(int_builder.Finish(&int_array));
        auto batch = arrow::RecordBatch::Make(schema, 2, {int_array});
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
    }

    // Write batch 2: [3, 4, 5]
    {
        arrow::Int32Builder int_builder;
        ASSERT_ARROW_OK(int_builder.AppendValues({3, 4, 5}));
        std::shared_ptr<arrow::Array> int_array;
        ASSERT_ARROW_OK(int_builder.Finish(&int_array));
        auto batch = arrow::RecordBatch::Make(schema, 3, {int_array});
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
    }

    ASSERT_ARROW_OK(writer->Close());
    ASSERT_ARROW_OK(out_file->Close());

    std::vector<std::string> file_names{file_path};
    std::vector<std::string> columns{"c0_int"};

    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);

    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;

    auto ranges = generate_ranges(file_names, columns.size(), {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {src_slot_infos, dst_slot_infos});
    auto scanner = create_arrow_scanner("UTC", desc_tbl, {}, ranges, 3);

    ASSERT_OK(scanner->open());
    {
        auto res = scanner->get_next();
        ASSERT_OK(res.status());
        auto chunk = res.value();
        ASSERT_NE(nullptr, chunk);
        ASSERT_EQ(3, chunk->num_rows());
        auto c0 = chunk->columns()[0];
        ASSERT_EQ(1, c0->get(0).get_int32());
        ASSERT_EQ(2, c0->get(1).get_int32());
        ASSERT_EQ(3, c0->get(2).get_int32());
    }

    {
        auto res = scanner->get_next();
        ASSERT_OK(res.status());
        auto chunk = res.value();
        ASSERT_NE(nullptr, chunk);
        ASSERT_EQ(2, chunk->num_rows());
        auto c0 = chunk->columns()[0];
        ASSERT_EQ(4, c0->get(0).get_int32());
        ASSERT_EQ(5, c0->get(1).get_int32());
    }

    auto res2 = scanner->get_next();
    ASSERT_TRUE(res2.status().is_end_of_file());

    scanner->close();
}

class MockDiscreteStreamLoadPipe : public StreamLoadPipe {
public:
    using StreamLoadPipe::StreamLoadPipe;
    bool is_discrete_message_pipe() const override { return true; }
};
using DiscreteStreamLoadPipe = MockDiscreteStreamLoadPipe;

TEST_F(ArrowScannerTest, TestScanArrowStreamDiscrete) {
    LoadStreamMgr load_stream_mgr;
    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    DeferOp remove_pipe([&]() { load_stream_mgr.remove(load_id); });
    ASSERT_OK(load_stream_mgr.put(load_id, pipe));

    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_VARCHAR);

    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    src_slot_infos.emplace_back("c1_str", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);

    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_ARROW;
    range.file_type = TFileType::FILE_STREAM;
    range.__set_load_id(load_id.to_thrift());
    ranges.emplace_back(range);

    TQueryOptions query_options;
    query_options.query_type = TQueryType::LOAD;
    TQueryGlobals query_globals;
    query_globals.time_zone = "UTC";
    RuntimeServices runtime_services;
    runtime_services.load_stream_mgr = &load_stream_mgr;
    QueryExecutionServices query_execution_services;
    query_execution_services.runtime = &runtime_services;

    RuntimeState* state = _obj_pool.add(
            new RuntimeState(TUniqueId(), query_options, query_globals, &query_execution_services, nullptr));

    DescriptorTbl* desc_tbl = DescTblHelper::generate_desc_tbl(state, _obj_pool, {src_slot_infos, dst_slot_infos});
    state->set_desc_tbl(desc_tbl);
    state->init_instance_mem_tracker();
    state->set_db("test_db");
    state->set_load_label("test_label");

    TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
    params->strict_mode = true;
    params->__isset.strict_mode = true;
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
        params->expr_of_dest_slot[dst_slot->id()] =
                create_column_ref(src_slot->id(), src_slot->type(), src_slot->is_nullable());
        params->dest_sid_to_src_sid_without_trans[dst_slot->id()] = src_slot->id();
    }
    params->__isset.dest_sid_to_src_sid_without_trans = true;

    for (int i = 0; i < src_tuple->slots().size(); i++) {
        params->src_slot_ids.emplace_back(i);
    }

    RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("test_prof", true));
    ScannerCounter* counter = _obj_pool.add(new ScannerCounter());

    TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
    broker_scan_range->params = *params;
    broker_scan_range->ranges = ranges;

    // Create First Arrow stream
    arrow::Int32Builder int_builder1;
    arrow::StringBuilder str_builder1;
    ASSERT_ARROW_OK(int_builder1.AppendValues({1, 2}));
    ASSERT_ARROW_OK(str_builder1.AppendValues({"a", "b"}));
    std::shared_ptr<arrow::Array> int_array1, str_array1;
    ASSERT_ARROW_OK(int_builder1.Finish(&int_array1));
    ASSERT_ARROW_OK(str_builder1.Finish(&str_array1));

    auto int_field = std::make_shared<arrow::Field>("c0_int", arrow::int32());
    auto str_field = std::make_shared<arrow::Field>("c1_str", arrow::utf8());
    auto schema = arrow::schema({int_field, str_field});
    auto batch1 = arrow::RecordBatch::Make(schema, 2, {int_array1, str_array1});

    auto stream1 = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto writer1 = arrow::ipc::MakeStreamWriter(stream1, schema).ValueOrDie();
    ASSERT_ARROW_OK(writer1->WriteRecordBatch(*batch1));
    ASSERT_ARROW_OK(writer1->Close());
    auto buf1 = stream1->Finish().ValueOrDie();

    // Create Second Arrow stream
    arrow::Int32Builder int_builder2;
    arrow::StringBuilder str_builder2;
    ASSERT_ARROW_OK(int_builder2.AppendValues({3, 4}));
    ASSERT_ARROW_OK(str_builder2.AppendValues({"c", "d"}));
    std::shared_ptr<arrow::Array> int_array2, str_array2;
    ASSERT_ARROW_OK(int_builder2.Finish(&int_array2));
    ASSERT_ARROW_OK(str_builder2.Finish(&str_array2));

    auto batch2 = arrow::RecordBatch::Make(schema, 2, {int_array2, str_array2});
    auto stream2 = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto writer2 = arrow::ipc::MakeStreamWriter(stream2, schema).ValueOrDie();
    ASSERT_ARROW_OK(writer2->WriteRecordBatch(*batch2));
    ASSERT_ARROW_OK(writer2->Close());
    auto buf2 = stream2->Finish().ValueOrDie();

    // Append both to pipe as separate buffers
    ByteBufferPtr bb1 = ByteBuffer::allocate_with_tracker(buf1->size()).value();
    bb1->put_bytes((const char*)buf1->data(), buf1->size());
    bb1->flip_to_read();
    EXPECT_OK(pipe->append(std::move(bb1)));

    ByteBufferPtr bb2 = ByteBuffer::allocate_with_tracker(buf2->size()).value();
    bb2->put_bytes((const char*)buf2->data(), buf2->size());
    bb2->flip_to_read();
    EXPECT_OK(pipe->append(std::move(bb2)));
    EXPECT_OK(pipe->finish());

    auto scanner = std::make_unique<ArrowScanner>(state, profile, *broker_scan_range, counter);
    ASSERT_OK(scanner->open());

    auto res = scanner->get_next();
    ASSERT_OK(res.status());
    auto chunk = res.value();
    ASSERT_NE(nullptr, chunk);
    ASSERT_EQ(2, chunk->num_rows());
    ASSERT_EQ(1, chunk->columns()[0]->get(0).get_int32());
    ASSERT_EQ("a", chunk->columns()[1]->get(0).get_slice());
    ASSERT_EQ(2, chunk->columns()[0]->get(1).get_int32());
    ASSERT_EQ("b", chunk->columns()[1]->get(1).get_slice());

    // Pull next buffer seamlessly
    auto res2 = scanner->get_next();
    ASSERT_OK(res2.status());
    auto chunk2 = res2.value();
    ASSERT_NE(nullptr, chunk2);
    ASSERT_EQ(2, chunk2->num_rows());
    ASSERT_EQ(3, chunk2->columns()[0]->get(0).get_int32());
    ASSERT_EQ("c", chunk2->columns()[1]->get(0).get_slice());
    ASSERT_EQ(4, chunk2->columns()[0]->get(1).get_int32());
    ASSERT_EQ("d", chunk2->columns()[1]->get(1).get_slice());

    auto res3 = scanner->get_next();
    ASSERT_TRUE(res3.status().is_end_of_file());

    EXPECT_EQ(buf1->size() + buf2->size(), state->num_bytes_scan_from_source());

    scanner->close();
}

TEST_F(ArrowScannerTest, test_multi_file_scan) {
    std::string file1_path = (_tmp_root_dir / "multi_file_1.arrow").string();
    std::string file2_path = (_tmp_root_dir / "multi_file_2.arrow").string();

    auto schema = arrow::schema({arrow::field("c0_int", arrow::int32())});

    // File 1: 3 rows
    {
        arrow::Int32Builder builder;
        ASSERT_ARROW_OK(builder.AppendValues({1, 2, 3}));
        std::shared_ptr<arrow::Array> array;
        ASSERT_ARROW_OK(builder.Finish(&array));
        auto batch = arrow::RecordBatch::Make(schema, 3, {array});
        auto out_file = arrow::io::FileOutputStream::Open(file1_path).ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(out_file, schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        ASSERT_ARROW_OK(out_file->Close());
    }

    // File 2: 4 rows
    {
        arrow::Int32Builder builder;
        ASSERT_ARROW_OK(builder.AppendValues({4, 5, 6, 7}));
        std::shared_ptr<arrow::Array> array;
        ASSERT_ARROW_OK(builder.Finish(&array));
        auto batch = arrow::RecordBatch::Make(schema, 4, {array});
        auto out_file = arrow::io::FileOutputStream::Open(file2_path).ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(out_file, schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        ASSERT_ARROW_OK(out_file->Close());
    }

    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;

    auto ranges = generate_ranges({file1_path, file2_path}, 1, {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {src_slot_infos, dst_slot_infos});
    auto scanner = create_arrow_scanner("UTC", desc_tbl, {}, ranges);

    ASSERT_OK(scanner->open());
    int total_rows = 0;
    while (true) {
        auto res = scanner->get_next();
        if (res.status().is_end_of_file()) {
            break;
        }
        ASSERT_OK(res.status());
        auto chunk = res.value();
        if (chunk != nullptr) {
            total_rows += chunk->num_rows();
        }
    }
    scanner->close();
    ASSERT_EQ(7, total_rows);
}

// Regression test for r3648381347: when two Arrow files are scanned and the
// first file ends before the StarRocks chunk fills, rows from each file must
// receive the correct per-file partition/path column values.  Before the fix,
// finalize_src_chunk() always stamped ALL rows in the mixed-file chunk with the
// most recently opened file's columns_from_path, so file-1 rows received
// file-2's partition value.
//
// The fix breaks at file boundaries so that finalize_src_chunk() always sees a
// single-file chunk and can safely use _scan_range.ranges.at(_next_file - 1)
// to stamp path columns.
TEST_F(ArrowScannerTest, TestMultiFileCrossFileBoundaryPathValues) {
    std::string file1_path = (_tmp_root_dir / "path_boundary_file1.arrow").string();
    std::string file2_path = (_tmp_root_dir / "path_boundary_file2.arrow").string();

    auto schema = arrow::schema({arrow::field("c0_int", arrow::int32())});

    // File 1: 3 rows [1, 2, 3]
    {
        arrow::Int32Builder builder;
        ASSERT_ARROW_OK(builder.AppendValues({1, 2, 3}));
        std::shared_ptr<arrow::Array> array;
        ASSERT_ARROW_OK(builder.Finish(&array));
        auto batch = arrow::RecordBatch::Make(schema, 3, {array});
        auto out_file = arrow::io::FileOutputStream::Open(file1_path).ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(out_file, schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        ASSERT_ARROW_OK(out_file->Close());
    }

    // File 2: 3 rows [4, 5, 6]
    {
        arrow::Int32Builder builder;
        ASSERT_ARROW_OK(builder.AppendValues({4, 5, 6}));
        std::shared_ptr<arrow::Array> array;
        ASSERT_ARROW_OK(builder.Finish(&array));
        auto batch = arrow::RecordBatch::Make(schema, 3, {array});
        auto out_file = arrow::io::FileOutputStream::Open(file2_path).ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(out_file, schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        ASSERT_ARROW_OK(out_file->Close());
    }

    // Schema: c0_int from file, partition from path.
    // num_of_columns_from_file = 1 (only c0_int); partition is appended by
    // fill_columns_from_path.
    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    src_slot_infos.emplace_back("partition", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);

    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;

    // Build ranges manually so each file carries a different columns_from_path.
    std::vector<TBrokerRangeDesc> ranges(2);
    for (int i = 0; i < 2; ++i) {
        ranges[i].__set_num_of_columns_from_file(1); // only c0_int comes from file
        ranges[i].start_offset = 0;
        ranges[i].size = LONG_MAX;
        ranges[i].file_type = TFileType::FILE_LOCAL;
        ranges[i].__set_format_type(TFileFormatType::FORMAT_ARROW);
    }
    ranges[0].__set_path(file1_path);
    ranges[0].__set_columns_from_path({"p1"}); // file 1 partition value
    ranges[1].__set_path(file2_path);
    ranges[1].__set_columns_from_path({"p2"}); // file 2 partition value

    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {src_slot_infos, dst_slot_infos});

    // chunk_size=4 is larger than both files (3 rows each). Without the fix,
    // get_next() would cross the file boundary mid-chunk and stamp all 4 rows
    // (3 from file 1 + 1 from file 2) with file-2's partition "p2".
    // With the fix, the scanner breaks at the file boundary, so chunk 1 has
    // only file-1 rows (stamped "p1") and chunk 2 has only file-2 rows ("p2").
    auto scanner = create_arrow_scanner("UTC", desc_tbl, {}, ranges, /*batch_size=*/4);

    ASSERT_OK(scanner->open());

    // Chunk 1: all 3 rows from file 1, partition must be "p1".
    {
        auto res = scanner->get_next();
        ASSERT_OK(res.status());
        auto chunk = res.value();
        ASSERT_NE(nullptr, chunk);
        ASSERT_EQ(3, chunk->num_rows());

        auto c0 = chunk->columns()[0];
        auto part = chunk->columns()[1];

        EXPECT_EQ(1, c0->get(0).get_int32());
        EXPECT_EQ("p1", part->get(0).get_slice());

        EXPECT_EQ(2, c0->get(1).get_int32());
        EXPECT_EQ("p1", part->get(1).get_slice());

        EXPECT_EQ(3, c0->get(2).get_int32());
        EXPECT_EQ("p1", part->get(2).get_slice());
    }

    // Chunk 2: all 3 rows from file 2, partition must be "p2".
    {
        auto res = scanner->get_next();
        ASSERT_OK(res.status());
        auto chunk = res.value();
        ASSERT_NE(nullptr, chunk);
        ASSERT_EQ(3, chunk->num_rows());

        auto c0 = chunk->columns()[0];
        auto part = chunk->columns()[1];

        EXPECT_EQ(4, c0->get(0).get_int32());
        EXPECT_EQ("p2", part->get(0).get_slice());

        EXPECT_EQ(5, c0->get(1).get_int32());
        EXPECT_EQ("p2", part->get(1).get_slice());

        EXPECT_EQ(6, c0->get(2).get_int32());
        EXPECT_EQ("p2", part->get(2).get_slice());
    }

    auto res_eof = scanner->get_next();
    ASSERT_TRUE(res_eof.status().is_end_of_file());

    scanner->close();
}

TEST_F(ArrowScannerTest, TestGetSchemaNotSupported) {
    std::vector<SlotDescriptor> schema;
    TBrokerScanRange broker_scan_range;
    ScannerCounter counter;
    RuntimeProfile profile("test_prof", true);
    ArrowScanner scanner(_runtime_state, &profile, broker_scan_range, &counter);
    auto status = scanner.get_schema(&schema);
    ASSERT_TRUE(status.is_not_supported());
}

TEST_F(ArrowScannerTest, TestOpenDifferentColumnCounts) {
    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {src_slot_infos, dst_slot_infos});

    std::vector<TBrokerRangeDesc> ranges(2);
    ranges[0].__set_num_of_columns_from_file(1);
    ranges[0].__set_columns_from_path({"p1"});
    ranges[1].__set_num_of_columns_from_file(1);
    ranges[1].__set_columns_from_path({"p1", "p2"}); // Different count

    auto scanner = create_arrow_scanner("UTC", desc_tbl, {}, ranges);
    auto status = scanner->open();
    ASSERT_TRUE(status.is_internal_error());
}

TEST_F(ArrowScannerTest, TestStreamNullOrEmptyBuffer) {
    LoadStreamMgr load_stream_mgr;
    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    DeferOp remove_pipe([&]() { load_stream_mgr.remove(load_id); });
    ASSERT_OK(load_stream_mgr.put(load_id, pipe));

    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_ARROW;
    range.file_type = TFileType::FILE_STREAM;
    range.__set_load_id(load_id.to_thrift());
    ranges.emplace_back(range);

    TQueryOptions query_options;
    query_options.query_type = TQueryType::LOAD;
    TQueryGlobals query_globals;
    query_globals.time_zone = "UTC";
    RuntimeServices runtime_services;
    runtime_services.load_stream_mgr = &load_stream_mgr;
    QueryExecutionServices query_execution_services;
    query_execution_services.runtime = &runtime_services;

    RuntimeState* state = _obj_pool.add(
            new RuntimeState(TUniqueId(), query_options, query_globals, &query_execution_services, nullptr));

    DescriptorTbl* desc_tbl = DescTblHelper::generate_desc_tbl(state, _obj_pool, {src_slot_infos, dst_slot_infos});
    state->set_desc_tbl(desc_tbl);
    state->init_instance_mem_tracker();
    state->set_db("test_db");
    state->set_load_label("test_label");

    TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
    params->strict_mode = true;
    params->__isset.strict_mode = true;
    std::vector<TupleDescriptor*> tuples;
    desc_tbl->get_tuple_descs(&tuples);
    params->src_tuple_id = 0;
    params->dest_tuple_id = tuples.size() - 1;
    const auto* src_tuple = desc_tbl->get_tuple_descriptor(params->src_tuple_id);
    const auto* dst_tuple = desc_tbl->get_tuple_descriptor(params->dest_tuple_id);
    for (int i = 0; i < src_tuple->slots().size(); i++) {
        auto& src_slot = src_tuple->slots()[i];
        auto& dst_slot = dst_tuple->slots()[i];
        params->expr_of_dest_slot[dst_slot->id()] =
                create_column_ref(src_slot->id(), src_slot->type(), src_slot->is_nullable());
        params->dest_sid_to_src_sid_without_trans[dst_slot->id()] = src_slot->id();
    }
    params->__isset.dest_sid_to_src_sid_without_trans = true;
    for (int i = 0; i < src_tuple->slots().size(); i++) {
        params->src_slot_ids.emplace_back(i);
    }

    RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("test_prof", true));
    ScannerCounter* counter = _obj_pool.add(new ScannerCounter());

    TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
    broker_scan_range->params = *params;
    broker_scan_range->ranges = ranges;

    // Append empty buffer first, then valid arrow stream
    ByteBufferPtr empty_bb = ByteBuffer::allocate_with_tracker(0).value();
    EXPECT_OK(pipe->append(std::move(empty_bb)));

    arrow::Int32Builder int_builder;
    ASSERT_ARROW_OK(int_builder.AppendValues({42}));
    std::shared_ptr<arrow::Array> int_array;
    ASSERT_ARROW_OK(int_builder.Finish(&int_array));
    auto schema = arrow::schema({arrow::field("c0_int", arrow::int32())});
    auto batch = arrow::RecordBatch::Make(schema, 1, {int_array});

    auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto writer = arrow::ipc::MakeStreamWriter(stream, schema).ValueOrDie();
    ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
    ASSERT_ARROW_OK(writer->Close());
    auto buf = stream->Finish().ValueOrDie();

    ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
    bb->put_bytes((const char*)buf->data(), buf->size());
    bb->flip_to_read();
    EXPECT_OK(pipe->append(std::move(bb)));
    EXPECT_OK(pipe->finish());

    auto scanner = std::make_unique<ArrowScanner>(state, profile, *broker_scan_range, counter);
    ASSERT_OK(scanner->open());

    auto res = scanner->get_next();
    ASSERT_OK(res.status());
    auto chunk = res.value();
    ASSERT_NE(nullptr, chunk);
    ASSERT_EQ(1, chunk->num_rows());
    ASSERT_EQ(42, chunk->columns()[0]->get(0).get_int32());

    auto res_eof = scanner->get_next();
    ASSERT_TRUE(res_eof.status().is_end_of_file());

    scanner->close();
}

TEST_F(ArrowScannerTest, TestStreamMalformedBufferFiltering) {
    LoadStreamMgr load_stream_mgr;
    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    DeferOp remove_pipe([&]() { load_stream_mgr.remove(load_id); });
    ASSERT_OK(load_stream_mgr.put(load_id, pipe));

    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_ARROW;
    range.file_type = TFileType::FILE_STREAM;
    range.__set_load_id(load_id.to_thrift());
    ranges.emplace_back(range);

    TQueryOptions query_options;
    query_options.query_type = TQueryType::LOAD;
    TQueryGlobals query_globals;
    query_globals.time_zone = "UTC";
    RuntimeServices runtime_services;
    runtime_services.load_stream_mgr = &load_stream_mgr;
    QueryExecutionServices query_execution_services;
    query_execution_services.runtime = &runtime_services;

    RuntimeState* state = _obj_pool.add(
            new RuntimeState(TUniqueId(), query_options, query_globals, &query_execution_services, nullptr));

    DescriptorTbl* desc_tbl = DescTblHelper::generate_desc_tbl(state, _obj_pool, {src_slot_infos, dst_slot_infos});
    state->set_desc_tbl(desc_tbl);
    state->init_instance_mem_tracker();
    state->set_db("test_db");
    state->set_load_label("test_label");

    TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
    params->strict_mode = true;
    params->__isset.strict_mode = true;
    std::vector<TupleDescriptor*> tuples;
    desc_tbl->get_tuple_descs(&tuples);
    params->src_tuple_id = 0;
    params->dest_tuple_id = tuples.size() - 1;
    const auto* src_tuple = desc_tbl->get_tuple_descriptor(params->src_tuple_id);
    const auto* dst_tuple = desc_tbl->get_tuple_descriptor(params->dest_tuple_id);
    for (int i = 0; i < src_tuple->slots().size(); i++) {
        auto& src_slot = src_tuple->slots()[i];
        auto& dst_slot = dst_tuple->slots()[i];
        params->expr_of_dest_slot[dst_slot->id()] =
                create_column_ref(src_slot->id(), src_slot->type(), src_slot->is_nullable());
        params->dest_sid_to_src_sid_without_trans[dst_slot->id()] = src_slot->id();
    }
    params->__isset.dest_sid_to_src_sid_without_trans = true;
    for (int i = 0; i < src_tuple->slots().size(); i++) {
        params->src_slot_ids.emplace_back(i);
    }

    RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("test_prof", true));
    ScannerCounter* counter = _obj_pool.add(new ScannerCounter());

    TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
    broker_scan_range->params = *params;
    broker_scan_range->ranges = ranges;

    // Append 15 malformed buffers — more than the old kMaxConsecutiveErrors=10
    // to verify the scanner now processes all of them without aborting early.
    constexpr int kNumMalformed = 15;
    std::string junk = "not_arrow_ipc_format_data";
    for (int i = 0; i < kNumMalformed; ++i) {
        ByteBufferPtr junk_bb = ByteBuffer::allocate_with_tracker(junk.size()).value();
        junk_bb->put_bytes(junk.data(), junk.size());
        junk_bb->flip_to_read();
        EXPECT_OK(pipe->append(std::move(junk_bb)));
    }
    EXPECT_OK(pipe->finish());

    auto scanner = std::make_unique<ArrowScanner>(state, profile, *broker_scan_range, counter);
    ASSERT_OK(scanner->open());

    // The scanner should reach EOF (not InternalError) after filtering all messages.
    auto res = scanner->get_next();
    ASSERT_TRUE(res.status().is_end_of_file()) << "Expected EOF, got: " << res.status();

    // All 15 malformed messages should be counted as filtered rows.
    ASSERT_EQ(counter->num_rows_filtered, kNumMalformed);

    scanner->close();
}

TEST_F(ArrowScannerTest, TestStreamMessageMetaExtraction) {
    LoadStreamMgr load_stream_mgr;
    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    DeferOp remove_pipe([&]() { load_stream_mgr.remove(load_id); });
    ASSERT_OK(load_stream_mgr.put(load_id, pipe));

    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_ARROW;
    range.file_type = TFileType::FILE_STREAM;
    range.__set_load_id(load_id.to_thrift());
    ranges.emplace_back(range);

    TQueryOptions query_options;
    query_options.query_type = TQueryType::LOAD;
    TQueryGlobals query_globals;
    query_globals.time_zone = "UTC";
    RuntimeServices runtime_services;
    runtime_services.load_stream_mgr = &load_stream_mgr;
    QueryExecutionServices query_execution_services;
    query_execution_services.runtime = &runtime_services;

    RuntimeState* state = _obj_pool.add(
            new RuntimeState(TUniqueId(), query_options, query_globals, &query_execution_services, nullptr));

    DescriptorTbl* desc_tbl = DescTblHelper::generate_desc_tbl(state, _obj_pool, {src_slot_infos, dst_slot_infos});
    state->set_desc_tbl(desc_tbl);
    state->init_instance_mem_tracker();
    state->set_db("test_db");
    state->set_load_label("test_label");

    TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
    params->strict_mode = true;
    params->__isset.strict_mode = true;
    std::vector<TupleDescriptor*> tuples;
    desc_tbl->get_tuple_descs(&tuples);
    params->src_tuple_id = 0;
    params->dest_tuple_id = tuples.size() - 1;
    const auto* src_tuple = desc_tbl->get_tuple_descriptor(params->src_tuple_id);
    const auto* dst_tuple = desc_tbl->get_tuple_descriptor(params->dest_tuple_id);
    for (int i = 0; i < src_tuple->slots().size(); i++) {
        auto& src_slot = src_tuple->slots()[i];
        auto& dst_slot = dst_tuple->slots()[i];
        params->expr_of_dest_slot[dst_slot->id()] =
                create_column_ref(src_slot->id(), src_slot->type(), src_slot->is_nullable());
        params->dest_sid_to_src_sid_without_trans[dst_slot->id()] = src_slot->id();
    }
    params->__isset.dest_sid_to_src_sid_without_trans = true;
    for (int i = 0; i < src_tuple->slots().size(); i++) {
        params->src_slot_ids.emplace_back(i);
    }

    RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("test_prof", true));
    ScannerCounter* counter = _obj_pool.add(new ScannerCounter());

    TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
    broker_scan_range->params = *params;
    broker_scan_range->ranges = ranges;

    std::string junk = "invalid_arrow_stream_data";
    ByteBufferPtr junk_bb = ByteBuffer::allocate_with_tracker(junk.size(), 0, ByteBufferMetaType::KAFKA).value();
    junk_bb->put_bytes(junk.data(), junk.size());
    junk_bb->flip_to_read();

    auto* msg_meta = static_cast<StreamMessageMeta*>(junk_bb->meta());
    msg_meta->set_partition(2);
    msg_meta->set_offset(100);

    EXPECT_OK(pipe->append(std::move(junk_bb)));
    EXPECT_OK(pipe->finish());

    auto scanner = std::make_unique<ArrowScanner>(state, profile, *broker_scan_range, counter);
    ASSERT_OK(scanner->open());

    auto res = scanner->get_next();
    ASSERT_TRUE(res.status().is_end_of_file());

    scanner->close();
}

TEST_F(ArrowScannerTest, TestStreamFileEmptyBuffer) {
    LoadStreamMgr load_stream_mgr;
    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    DeferOp remove_pipe([&]() { load_stream_mgr.remove(load_id); });
    ASSERT_OK(load_stream_mgr.put(load_id, pipe));

    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_ARROW;
    range.file_type = TFileType::FILE_STREAM;
    range.__set_load_id(load_id.to_thrift());
    ranges.emplace_back(range);

    TQueryOptions query_options;
    query_options.query_type = TQueryType::LOAD;
    TQueryGlobals query_globals;
    query_globals.time_zone = "UTC";
    RuntimeServices runtime_services;
    runtime_services.load_stream_mgr = &load_stream_mgr;
    QueryExecutionServices query_execution_services;
    query_execution_services.runtime = &runtime_services;

    RuntimeState* state = _obj_pool.add(
            new RuntimeState(TUniqueId(), query_options, query_globals, &query_execution_services, nullptr));

    DescriptorTbl* desc_tbl = DescTblHelper::generate_desc_tbl(state, _obj_pool, {src_slot_infos, dst_slot_infos});
    state->set_desc_tbl(desc_tbl);
    state->init_instance_mem_tracker();
    state->set_db("test_db");
    state->set_load_label("test_label");

    TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
    params->strict_mode = true;
    params->__isset.strict_mode = true;
    std::vector<TupleDescriptor*> tuples;
    desc_tbl->get_tuple_descs(&tuples);
    params->src_tuple_id = 0;
    params->dest_tuple_id = tuples.size() - 1;
    const auto* src_tuple = desc_tbl->get_tuple_descriptor(params->src_tuple_id);
    const auto* dst_tuple = desc_tbl->get_tuple_descriptor(params->dest_tuple_id);
    for (int i = 0; i < src_tuple->slots().size(); i++) {
        auto& src_slot = src_tuple->slots()[i];
        auto& dst_slot = dst_tuple->slots()[i];
        params->expr_of_dest_slot[dst_slot->id()] =
                create_column_ref(src_slot->id(), src_slot->type(), src_slot->is_nullable());
        params->dest_sid_to_src_sid_without_trans[dst_slot->id()] = src_slot->id();
    }
    params->__isset.dest_sid_to_src_sid_without_trans = true;
    for (int i = 0; i < src_tuple->slots().size(); i++) {
        params->src_slot_ids.emplace_back(i);
    }

    RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("test_prof", true));
    ScannerCounter* counter = _obj_pool.add(new ScannerCounter());

    TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
    broker_scan_range->params = *params;
    broker_scan_range->ranges = ranges;

    // Append an empty buffer (0 remaining bytes)
    ByteBufferPtr empty_bb = ByteBuffer::allocate_with_tracker(0, 0, ByteBufferMetaType::KAFKA).value();
    EXPECT_OK(pipe->append(std::move(empty_bb)));

    // Append a valid record batch message buffer
    arrow::Int32Builder int_builder;
    ASSERT_ARROW_OK(int_builder.AppendValues({10, 20, 30}));
    std::shared_ptr<arrow::Array> int_array;
    ASSERT_ARROW_OK(int_builder.Finish(&int_array));
    auto schema = arrow::schema({arrow::field("c0_int", arrow::int32())});
    auto batch = arrow::RecordBatch::Make(schema, 3, {int_array});

    auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto writer = arrow::ipc::MakeStreamWriter(stream, schema).ValueOrDie();
    ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
    ASSERT_ARROW_OK(writer->Close());
    auto buf = stream->Finish().ValueOrDie();

    ByteBufferPtr valid_bb = ByteBuffer::allocate_with_tracker(buf->size(), 0, ByteBufferMetaType::KAFKA).value();
    valid_bb->put_bytes((const char*)buf->data(), buf->size());
    valid_bb->flip_to_read();

    auto* msg_meta = static_cast<StreamMessageMeta*>(valid_bb->meta());
    msg_meta->set_partition(0);
    msg_meta->set_offset(0);

    EXPECT_OK(pipe->append(std::move(valid_bb)));
    EXPECT_OK(pipe->finish());

    auto scanner = std::make_unique<ArrowScanner>(state, profile, *broker_scan_range, counter);
    ASSERT_OK(scanner->open());

    auto res = scanner->get_next();
    ASSERT_OK(res.status());
    ASSERT_EQ(3, res.value()->num_rows());

    auto res_eof = scanner->get_next();
    ASSERT_TRUE(res_eof.status().is_end_of_file());

    scanner->close();
}

TEST_F(ArrowScannerTest, TestLocalFileReadNextFailure) {
    std::string file_path = (_tmp_root_dir / "test_corrupt_batch.arrow").string();
    {
        std::ofstream file(file_path, std::ios::binary);
        // Write invalid data that acts as a truncated file
        file << "ARROW1_invalid_header_and_body_data";
    }

    std::vector<std::string> file_names{file_path};
    std::vector<std::string> columns{"c0_int"};

    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;

    auto ranges = generate_ranges(file_names, columns.size(), {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {src_slot_infos, dst_slot_infos});
    auto scanner = create_arrow_scanner("UTC", desc_tbl, {}, ranges);

    auto open_st = scanner->open();
    if (open_st.ok()) {
        auto res = scanner->get_next();
        // Should fail to read record batch
        ASSERT_FALSE(res.status().ok());
    }
    scanner->close();
}

// ---------------------------------------------------------------------------
// Helpers to build IPC buffers with specific truncation for coverage testing.
// ---------------------------------------------------------------------------

// Build a schema-only IPC stream (schema message + EOS). When
// RecordBatchStreamReader::Open is called on this, it succeeds (covers line 293).
// ReadNext then returns a null batch (EOS), not an error (covers lines 327-340).
static std::vector<uint8_t> make_schema_only_ipc_stream(const std::shared_ptr<arrow::Schema>& schema) {
    auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto writer = arrow::ipc::MakeStreamWriter(stream, schema).ValueOrDie();
    // Close immediately without writing any batches → schema + EOS only
    (void)writer->Close();
    auto buf = stream->Finish().ValueOrDie();
    return std::vector<uint8_t>(buf->data(), buf->data() + buf->size());
}

// Build a valid schema message followed by junk bytes (no valid EOS / batch).
// RecordBatchStreamReader::Open reads the schema and succeeds (covers line 293).
// ReadNext then reads the junk bytes and returns an error (covers lines 303-324).
//
// Strategy: write schema-only stream (schema + EOS), then strip the EOS
// (last 8 bytes: 0xFFFFFFFF 0x00000000) and append random non-zero bytes.
static std::vector<uint8_t> make_schema_plus_junk_ipc_stream(const std::shared_ptr<arrow::Schema>& schema) {
    auto schema_only = make_schema_only_ipc_stream(schema);
    // Remove the EOS marker (8 bytes: continuation token -1 + length 0)
    constexpr size_t kEosSize = 8;
    if (schema_only.size() <= kEosSize) {
        // Degenerate: just return junk
        return {0x41, 0x52, 0x52, 0x4f, 0x57, 0x31, 0x00, 0x00, 0xFF, 0xFE, 0xFD, 0xFC};
    }
    std::vector<uint8_t> result(schema_only.begin(), schema_only.end() - kEosSize);
    // Append junk bytes that are not a valid Arrow IPC message
    for (int i = 0; i < 32; i++) {
        result.push_back(static_cast<uint8_t>(0xDE + i));
    }
    return result;
}

// Build a stream with one valid int32 batch followed by junk. The writer's EOS
// marker is removed so Open succeeds, the first ReadNext succeeds, and the
// second ReadNext fails while reading the corrupt continuation.
static std::vector<uint8_t> make_batch_plus_junk_ipc_stream(const std::shared_ptr<arrow::Schema>& schema,
                                                            const std::vector<int32_t>& values) {
    arrow::Int32Builder builder;
    auto append_status = builder.AppendValues(values);
    EXPECT_TRUE(append_status.ok()) << append_status.ToString();
    if (!append_status.ok()) return {};
    std::shared_ptr<arrow::Array> array;
    auto finish_status = builder.Finish(&array);
    EXPECT_TRUE(finish_status.ok()) << finish_status.ToString();
    if (!finish_status.ok()) return {};
    auto batch = arrow::RecordBatch::Make(schema, values.size(), {array});

    auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto writer = arrow::ipc::MakeStreamWriter(stream, schema).ValueOrDie();
    auto write_status = writer->WriteRecordBatch(*batch);
    EXPECT_TRUE(write_status.ok()) << write_status.ToString();
    if (!write_status.ok()) return {};
    auto close_status = writer->Close();
    EXPECT_TRUE(close_status.ok()) << close_status.ToString();
    if (!close_status.ok()) return {};
    auto buf = stream->Finish().ValueOrDie();

    constexpr size_t kEosSize = 8;
    EXPECT_GT(buf->size(), kEosSize);
    if (buf->size() <= kEosSize) return {};
    std::vector<uint8_t> result(buf->data(), buf->data() + buf->size() - kEosSize);
    for (int i = 0; i < 32; i++) {
        result.push_back(static_cast<uint8_t>(0xDE + i));
    }
    return result;
}

// Helper: build a stream scanner context backed by a StreamLoadPipe with one slot.
// RuntimeServices and QueryExecutionServices are members (not stack-locals) so they
// outlive the helper function and the RuntimeState pointer to them stays valid.
struct StreamScannerContext {
    LoadStreamMgr load_stream_mgr;
    UniqueId load_id;
    std::shared_ptr<StreamLoadPipe> pipe;
    ObjectPool obj_pool;

    RuntimeServices runtime_services;
    QueryExecutionServices qes;

    RuntimeState* state = nullptr;
    RuntimeProfile* profile = nullptr;
    ScannerCounter* counter = nullptr;
    TBrokerScanRange broker_scan_range;

    ~StreamScannerContext() { load_stream_mgr.remove(load_id); }
};

static StatusOr<std::unique_ptr<StreamScannerContext>> make_stream_scanner_context(
        const SlotTypeDescInfoArray& slot_infos, UniqueId load_id, std::shared_ptr<StreamLoadPipe> pipe,
        int32_t batch_size = 0, LoadPathMgr* load_path_mgr = nullptr, int64_t log_rejected_record_num = 0) {
    auto ctx = std::make_unique<StreamScannerContext>();
    ctx->load_id = load_id;
    ctx->pipe = pipe;
    RETURN_IF_ERROR(ctx->load_stream_mgr.put(load_id, pipe));

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_ARROW;
    range.file_type = TFileType::FILE_STREAM;
    range.__set_load_id(load_id.to_thrift());
    ranges.emplace_back(range);

    TQueryOptions query_options;
    query_options.query_type = TQueryType::LOAD;
    if (batch_size != 0) {
        query_options.__set_batch_size(batch_size);
    }
    if (log_rejected_record_num != 0) {
        query_options.__set_log_rejected_record_num(log_rejected_record_num);
    }
    TQueryGlobals query_globals;
    query_globals.time_zone = "UTC";
    ctx->runtime_services.load_stream_mgr = &ctx->load_stream_mgr;
    ctx->runtime_services.load_path_mgr = load_path_mgr;
    ctx->qes.runtime = &ctx->runtime_services;

    ctx->state = ctx->obj_pool.add(new RuntimeState(TUniqueId(), query_options, query_globals, &ctx->qes, nullptr));

    DescriptorTbl* desc_tbl = DescTblHelper::generate_desc_tbl(ctx->state, ctx->obj_pool, {slot_infos, slot_infos});
    ctx->state->set_desc_tbl(desc_tbl);
    ctx->state->init_instance_mem_tracker();
    ctx->state->set_db("test_db");
    ctx->state->set_load_label("test_label");

    TBrokerScanRangeParams* params = ctx->obj_pool.add(new TBrokerScanRangeParams());
    params->strict_mode = true;
    params->__isset.strict_mode = true;
    std::vector<TupleDescriptor*> tuples;
    desc_tbl->get_tuple_descs(&tuples);
    params->src_tuple_id = 0;
    params->dest_tuple_id = static_cast<int>(tuples.size()) - 1;
    const auto* src_tuple = desc_tbl->get_tuple_descriptor(params->src_tuple_id);
    const auto* dst_tuple = desc_tbl->get_tuple_descriptor(params->dest_tuple_id);
    for (int i = 0; i < (int)src_tuple->slots().size(); i++) {
        auto& ss = src_tuple->slots()[i];
        auto& ds = dst_tuple->slots()[i];
        TExpr e;
        e.nodes.emplace_back();
        e.nodes[0].__set_type(ss->type().to_thrift());
        e.nodes[0].__set_node_type(TExprNodeType::SLOT_REF);
        e.nodes[0].__set_is_nullable(ss->is_nullable());
        e.nodes[0].__set_slot_ref(TSlotRef());
        e.nodes[0].slot_ref.__set_slot_id((TSlotId)ss->id());
        params->expr_of_dest_slot[ds->id()] = e;
        params->dest_sid_to_src_sid_without_trans[ds->id()] = ss->id();
        params->src_slot_ids.emplace_back(i);
    }
    params->__isset.dest_sid_to_src_sid_without_trans = true;

    ctx->profile = ctx->obj_pool.add(new RuntimeProfile("prof", true));
    ctx->counter = ctx->obj_pool.add(new ScannerCounter());
    ctx->broker_scan_range.params = *params;
    ctx->broker_scan_range.ranges = ranges;
    return ctx;
}

// Exercises the stream Open-success path (line 293) and the null-batch EOS path
// (lines 327-340): schema-only IPC stream → Open OK → ReadNext → null batch → EOS.
TEST_F(ArrowScannerTest, TestStreamOpenSuccessReadNextEOS) {
    SlotTypeDescInfoArray slots;
    slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);

    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();

    // Build schema-only IPC: Open will succeed, ReadNext will get null (EOS).
    auto schema = arrow::schema({arrow::field("c0_int", arrow::int32())});
    auto buf = make_schema_only_ipc_stream(schema);
    ASSERT_GT(buf.size(), 0u);

    ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf.size()).value();
    bb->put_bytes(reinterpret_cast<const char*>(buf.data()), buf.size());
    bb->flip_to_read();
    EXPECT_OK(pipe->append(std::move(bb)));
    EXPECT_OK(pipe->finish());

    auto scanner = std::make_unique<ArrowScanner>(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
    ASSERT_OK(scanner->open());

    // Schema-only: no batch data → scanner should reach EOS immediately.
    // This exercises the Open-success path (line 293) then null-batch path (327-340).
    auto res = scanner->get_next();
    EXPECT_TRUE(res.status().is_end_of_file() || res.status().ok());

    scanner->close();
}

// Exercises the ReadNext failure path (lines 303-324) including the r3716853595 fix:
// schema section is valid (Open succeeds → line 293), batch section is junk
// (ReadNext fails → lines 303-324, including reset of _conv_funcs and _message_boundary).
// A valid second message is then sent to verify recovery.
TEST_F(ArrowScannerTest, TestStreamOpenSuccessReadNextFailure) {
    SlotTypeDescInfoArray slots;
    slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);

    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();

    auto schema = arrow::schema({arrow::field("c0_int", arrow::int32())});

    // Message 1: valid schema + junk batch body.
    // Open succeeds (covers line 293), ReadNext fails (covers lines 303-324).
    {
        auto buf = make_schema_plus_junk_ipc_stream(schema);
        ASSERT_GT(buf.size(), 0u);
        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf.size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf.data()), buf.size());
        bb->flip_to_read();
        EXPECT_OK(pipe->append(std::move(bb)));
    }

    // Message 2: valid full IPC stream for recovery verification.
    {
        arrow::Int32Builder b;
        ASSERT_ARROW_OK(b.AppendValues({7, 8, 9}));
        std::shared_ptr<arrow::Array> arr;
        ASSERT_ARROW_OK(b.Finish(&arr));
        auto batch = arrow::RecordBatch::Make(schema, 3, {arr});
        auto os = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(os, schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = os->Finish().ValueOrDie();
        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        EXPECT_OK(pipe->append(std::move(bb)));
    }

    EXPECT_OK(pipe->finish());

    auto scanner = std::make_unique<ArrowScanner>(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
    ASSERT_OK(scanner->open());

    // The malformed first message is filtered, then the scanner must recover
    // and return all rows from the valid second message.
    std::vector<int32_t> recovered_values;
    for (int i = 0; i < 10; i++) {
        auto res = scanner->get_next();
        if (res.status().is_end_of_file()) break;
        ASSERT_OK(res.status());
        ASSERT_NE(nullptr, res.value());
        const auto& column = res.value()->columns()[0];
        for (size_t row = 0; row < res.value()->num_rows(); ++row) {
            recovered_values.emplace_back(column->get(row).get_int32());
        }
    }
    ASSERT_EQ((std::vector<int32_t>{7, 8, 9}), recovered_values);
    EXPECT_EQ(1, ctx->counter->num_rows_filtered);
    scanner->close();
}

TEST_F(ArrowScannerTest, TestStreamRejectsMessageWithLateReadNextFailure) {
    SlotTypeDescInfoArray slots;
    slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);

    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, load_id, pipe, /*batch_size=*/1);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();

    auto schema = arrow::schema({arrow::field("c0_int", arrow::int32())});

    // The first message has a valid batch followed by a corrupt continuation.
    {
        auto buf = make_batch_plus_junk_ipc_stream(schema, {1, 2, 3});
        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf.size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf.data()), buf.size());
        bb->flip_to_read();
        EXPECT_OK(pipe->append(std::move(bb)));
    }

    // The second message is complete and valid.
    {
        arrow::Int32Builder builder;
        ASSERT_ARROW_OK(builder.AppendValues({7, 8}));
        std::shared_ptr<arrow::Array> array;
        ASSERT_ARROW_OK(builder.Finish(&array));
        auto batch = arrow::RecordBatch::Make(schema, 2, {array});
        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();
        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        EXPECT_OK(pipe->append(std::move(bb)));
    }
    EXPECT_OK(pipe->finish());

    auto scanner = std::make_unique<ArrowScanner>(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
    ASSERT_OK(scanner->open());

    std::vector<int32_t> values;
    while (true) {
        auto result = scanner->get_next();
        if (result.status().is_end_of_file()) {
            break;
        }
        ASSERT_OK(result.status());
        ASSERT_NE(nullptr, result.value());
        const auto& column = result.value()->columns()[0];
        for (size_t row = 0; row < result.value()->num_rows(); ++row) {
            values.emplace_back(column->get(row).get_int32());
        }
    }
    EXPECT_EQ((std::vector<int32_t>{7, 8}), values);
    EXPECT_EQ(4, ctx->counter->num_rows_filtered);
    scanner->close();
}

// Exercises the pipe read non-EOF error path (line 255):
// use a non-blocking StreamLoadPipe, cancel it before calling get_next().
// In non-blocking mode, cancel causes pipe->read() (no_block_read) to return
struct TestErrorDiscretePipe : public MockDiscreteStreamLoadPipe {
    using MockDiscreteStreamLoadPipe::MockDiscreteStreamLoadPipe;
    void cancel(const Status& status) override {
        _cancel_st = status;
        MockDiscreteStreamLoadPipe::cancel(status);
    }
    StatusOr<ByteBufferPtr> read() override {
        if (!_cancel_st.ok()) {
            return _cancel_st;
        }
        return MockDiscreteStreamLoadPipe::read();
    }
    Status _cancel_st = Status::OK();
};

// Exercises the pipe->read() error path in next_batch():
// when consumer_pipe->read() returns a non-OK status that is not EOF (e.g. Cancelled),
// _parser_buf and _arrow_stream are reset and the status is returned.
TEST_F(ArrowScannerTest, TestStreamPipeReadError) {
    SlotTypeDescInfoArray slots;
    slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);

    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<TestErrorDiscretePipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();

    // Cancel the pipe — read() returns the cancelled status, not EOF.
    pipe->cancel(Status::Cancelled("test cancellation"));

    auto scanner = std::make_unique<ArrowScanner>(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
    ASSERT_OK(scanner->open());

    // The scanner should see the non-EOF cancelled status from consumer_pipe->read().
    auto res = scanner->get_next();
    EXPECT_FALSE(res.status().is_end_of_file());

    scanner->close();
}

TEST_F(ArrowScannerTest, TestScanArrowStreamContinuousMultiChunk) {
    SlotTypeDescInfoArray slots;
    slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    slots.emplace_back("c1_str", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);

    auto load_id = UniqueId::gen_uid();
    // Plain StreamLoadPipe (HTTP continuous stream load, not KafkaConsumerPipe)
    auto pipe = std::make_shared<StreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();

    // Create an Arrow RecordBatch stream serialized to a buffer
    arrow::Int32Builder int_builder;
    arrow::StringBuilder str_builder;
    ASSERT_ARROW_OK(int_builder.AppendValues({1, 2, 3, 4, 5}));
    ASSERT_ARROW_OK(str_builder.AppendValues({"a", "b", "c", "d", "e"}));
    std::shared_ptr<arrow::Array> int_array, str_array;
    ASSERT_ARROW_OK(int_builder.Finish(&int_array));
    ASSERT_ARROW_OK(str_builder.Finish(&str_array));

    auto int_field = std::make_shared<arrow::Field>("c0_int", arrow::int32());
    auto str_field = std::make_shared<arrow::Field>("c1_str", arrow::utf8());
    auto schema = arrow::schema({int_field, str_field});
    auto batch = arrow::RecordBatch::Make(schema, 5, {int_array, str_array});

    auto os = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto writer = arrow::ipc::MakeStreamWriter(os, schema).ValueOrDie();
    ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
    ASSERT_ARROW_OK(writer->Close());
    auto buf = os->Finish().ValueOrDie();

    // Split the serialized buffer into two arbitrary halves (chunk1 and chunk2)
    ASSERT_GT(buf->size(), 10u);
    size_t split_point = buf->size() / 2;

    ByteBufferPtr bb1 = ByteBuffer::allocate_with_tracker(split_point).value();
    bb1->put_bytes(reinterpret_cast<const char*>(buf->data()), split_point);
    bb1->flip_to_read();
    EXPECT_OK(pipe->append(std::move(bb1)));

    size_t chunk2_size = buf->size() - split_point;
    ByteBufferPtr bb2 = ByteBuffer::allocate_with_tracker(chunk2_size).value();
    bb2->put_bytes(reinterpret_cast<const char*>(buf->data() + split_point), chunk2_size);
    bb2->flip_to_read();
    EXPECT_OK(pipe->append(std::move(bb2)));

    EXPECT_OK(pipe->finish());

    auto scanner = std::make_unique<ArrowScanner>(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
    ASSERT_OK(scanner->open());

    auto res = scanner->get_next();
    ASSERT_OK(res.status());
    auto chunk = res.value();
    ASSERT_NE(nullptr, chunk);
    ASSERT_EQ(5, chunk->num_rows());
    ASSERT_EQ(1, chunk->columns()[0]->get(0).get_int32());
    ASSERT_EQ("a", chunk->columns()[1]->get(0).get_slice());
    ASSERT_EQ(5, chunk->columns()[0]->get(4).get_int32());
    ASSERT_EQ("e", chunk->columns()[1]->get(4).get_slice());

    auto eof_res = scanner->get_next();
    EXPECT_TRUE(eof_res.status().is_end_of_file());

    scanner->close();
}

// Exercises the _file.reset() path in get_next() (arrow_scanner.cpp line 469):
// a local Arrow IPC file that contains only a schema + EOS (no batches).
// next_batch() returns EndOfFile → get_next() hits line 467-469 (_file.reset()).
TEST_F(ArrowScannerTest, TestLocalFileEmptyIpcStream) {
    std::string file_path = (_tmp_root_dir / "empty_ipc_stream.arrow").string();
    {
        // Write an Arrow IPC stream with a schema but no record batches.
        auto out_res = arrow::io::FileOutputStream::Open(file_path);
        ASSERT_ARROW_OK(out_res.status());
        auto out = out_res.ValueOrDie();
        auto schema = arrow::schema({arrow::field("c0_int", arrow::int32())});
        auto writer_res = arrow::ipc::MakeStreamWriter(out, schema);
        ASSERT_ARROW_OK(writer_res.status());
        // Close immediately without writing any batches → schema + EOS.
        ASSERT_ARROW_OK(writer_res.ValueOrDie()->Close());
        ASSERT_ARROW_OK(out->Close());
    }

    std::vector<std::string> file_names{file_path};
    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;

    auto ranges = generate_ranges(file_names, 1, {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {src_slot_infos, dst_slot_infos});
    auto scanner = create_arrow_scanner("UTC", desc_tbl, {}, ranges);

    ASSERT_OK(scanner->open());

    // The file has a schema but no batches. next_batch() will get null from ReadNext
    // (EOS), loop back with _file reset, then call open_next_reader() which returns
    // EndOfFile. The get_next() loop then hits line 467-469 and resets _file.
    auto res = scanner->get_next();
    // Should be EOF (no rows to return).
    EXPECT_TRUE(res.status().is_end_of_file() || (res.status().ok() && res.value()->num_rows() == 0));

    scanner->close();
}

// Rejecting a whole IPC message must count every batch and must not destroy
// the conversion expressions belonging to a preceding, unfinished chunk.
TEST_F(ArrowScannerTest, TestStreamSchemaInvalidMultiBatchAccounting) {
    for (auto [leading_valid_message, trailing_valid_message] :
         {std::pair{false, false}, std::pair{false, true}, std::pair{true, false}, std::pair{true, true}}) {
        SCOPED_TRACE(::testing::Message()
                     << "leading=" << leading_valid_message << " trailing=" << trailing_valid_message);
        auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
        SlotTypeDescInfoArray slots;
        slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), false);
        auto context = make_stream_scanner_context(slots, UniqueId::gen_uid(), pipe);
        ASSERT_OK(context.status());
        auto ctx = std::move(context).value();

        auto append_message = [&](const std::string& column_name, const std::vector<int>& batch_rows) {
            auto schema = arrow::schema({arrow::field(column_name, arrow::int32())});
            auto output = arrow::io::BufferOutputStream::Create().ValueOrDie();
            auto writer = arrow::ipc::MakeStreamWriter(output, schema).ValueOrDie();
            for (int rows : batch_rows) {
                arrow::Int32Builder builder;
                for (int row = 0; row < rows; ++row) {
                    ASSERT_ARROW_OK(builder.Append(42));
                }
                std::shared_ptr<arrow::Array> array;
                ASSERT_ARROW_OK(builder.Finish(&array));
                auto batch = arrow::RecordBatch::Make(schema, rows, {array});
                ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
            }
            ASSERT_ARROW_OK(writer->Close());
            auto buffer = output->Finish().ValueOrDie();
            auto bytes = ByteBuffer::allocate_with_tracker(buffer->size()).value();
            bytes->put_bytes(reinterpret_cast<const char*>(buffer->data()), buffer->size());
            bytes->flip_to_read();
            ASSERT_OK(pipe->append(std::move(bytes)));
        };
        if (leading_valid_message) {
            append_message("c0_int", {1});
        }
        // The missing required column invalidates all five rows, even though
        // validation first sees an empty batch.
        append_message("other", {0, 2, 3});
        if (trailing_valid_message) {
            append_message("c0_int", {1});
        }
        ASSERT_OK(pipe->finish());

        ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
        ASSERT_OK(scanner.open());
        int64_t loaded = 0;
        while (true) {
            auto result = scanner.get_next();
            if (result.status().is_end_of_file()) {
                break;
            }
            ASSERT_OK(result.status());
            auto chunk = result.value();
            for (size_t row = 0; row < chunk->num_rows(); ++row) {
                ASSERT_EQ(42, chunk->columns()[0]->get(row).get_int32());
            }
            loaded += chunk->num_rows();
        }
        EXPECT_EQ(static_cast<int>(leading_valid_message) + static_cast<int>(trailing_valid_message), loaded);
        EXPECT_EQ(5, ctx->counter->num_rows_filtered);
        scanner.close();
    }
}

TEST_F(ArrowScannerTest, TestStreamSchemaInvalidMissingNonNullableColumnFiltered) {
    LoadStreamMgr load_stream_mgr;
    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    DeferOp remove_pipe([&]() { load_stream_mgr.remove(load_id); });
    ASSERT_OK(load_stream_mgr.put(load_id, pipe));

    SlotTypeDescInfoArray src_slot_infos;
    // c0_int is non-nullable; c1_str is nullable.
    src_slot_infos.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), false);
    src_slot_infos.emplace_back("c1_str", TypeDescriptor::create_varchar_type(32), true);
    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_ARROW;
    range.file_type = TFileType::FILE_STREAM;
    range.__set_load_id(load_id.to_thrift());
    ranges.emplace_back(range);

    TQueryOptions query_options;
    query_options.query_type = TQueryType::LOAD;
    TQueryGlobals query_globals;
    query_globals.time_zone = "UTC";
    RuntimeServices runtime_services;
    runtime_services.load_stream_mgr = &load_stream_mgr;
    QueryExecutionServices query_execution_services;
    query_execution_services.runtime = &runtime_services;

    RuntimeState* state = _obj_pool.add(
            new RuntimeState(TUniqueId(), query_options, query_globals, &query_execution_services, nullptr));

    DescriptorTbl* desc_tbl = DescTblHelper::generate_desc_tbl(state, _obj_pool, {src_slot_infos, dst_slot_infos});
    state->set_desc_tbl(desc_tbl);
    state->init_instance_mem_tracker();
    state->set_db("test_db");
    state->set_load_label("test_label");

    TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
    params->strict_mode = true;
    params->__isset.strict_mode = true;
    std::vector<TupleDescriptor*> tuples;
    desc_tbl->get_tuple_descs(&tuples);
    params->src_tuple_id = 0;
    params->dest_tuple_id = tuples.size() - 1;
    const auto* src_tuple = desc_tbl->get_tuple_descriptor(params->src_tuple_id);
    const auto* dst_tuple = desc_tbl->get_tuple_descriptor(params->dest_tuple_id);
    for (int i = 0; i < src_tuple->slots().size(); i++) {
        auto& src_slot = src_tuple->slots()[i];
        auto& dst_slot = dst_tuple->slots()[i];
        params->expr_of_dest_slot[dst_slot->id()] =
                create_column_ref(src_slot->id(), src_slot->type(), src_slot->is_nullable());
        params->dest_sid_to_src_sid_without_trans[dst_slot->id()] = src_slot->id();
    }
    params->__isset.dest_sid_to_src_sid_without_trans = true;
    for (int i = 0; i < src_tuple->slots().size(); i++) {
        params->src_slot_ids.emplace_back(i);
    }

    RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("test_prof", true));
    ScannerCounter* counter = _obj_pool.add(new ScannerCounter());

    TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
    broker_scan_range->params = *params;
    broker_scan_range->ranges = ranges;

    // Message 1: Missing the non-nullable c0_int column (only contains c1_str).
    {
        arrow::StringBuilder str_builder;
        ASSERT_ARROW_OK(str_builder.AppendValues({"bad_msg"}));
        std::shared_ptr<arrow::Array> str_array;
        ASSERT_ARROW_OK(str_builder.Finish(&str_array));
        auto schema_bad = arrow::schema({arrow::field("c1_str", arrow::utf8())});
        auto batch_bad = arrow::RecordBatch::Make(schema_bad, 1, {str_array});

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, schema_bad).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch_bad));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes((const char*)buf->data(), buf->size());
        bb->flip_to_read();
        EXPECT_OK(pipe->append(std::move(bb)));
    }

    // Message 2: Contains both c0_int and c1_str (valid).
    {
        arrow::Int32Builder int_builder;
        ASSERT_ARROW_OK(int_builder.AppendValues({100}));
        std::shared_ptr<arrow::Array> int_array;
        ASSERT_ARROW_OK(int_builder.Finish(&int_array));

        arrow::StringBuilder str_builder;
        ASSERT_ARROW_OK(str_builder.AppendValues({"good_msg"}));
        std::shared_ptr<arrow::Array> str_array;
        ASSERT_ARROW_OK(str_builder.Finish(&str_array));

        auto schema_good =
                arrow::schema({arrow::field("c0_int", arrow::int32()), arrow::field("c1_str", arrow::utf8())});
        auto batch_good = arrow::RecordBatch::Make(schema_good, 1, {int_array, str_array});

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, schema_good).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch_good));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes((const char*)buf->data(), buf->size());
        bb->flip_to_read();
        EXPECT_OK(pipe->append(std::move(bb)));
    }
    EXPECT_OK(pipe->finish());

    auto scanner = std::make_unique<ArrowScanner>(state, profile, *broker_scan_range, counter);
    ASSERT_OK(scanner->open());

    // Scanner should filter the first message and return the valid second message.
    auto res = scanner->get_next();
    ASSERT_OK(res.status());
    auto chunk = res.value();
    ASSERT_NE(nullptr, chunk);
    ASSERT_EQ(1, chunk->num_rows());
    ASSERT_EQ(100, chunk->columns()[0]->get(0).get_int32());
    ASSERT_EQ("good_msg", chunk->columns()[1]->get(0).get_slice());

    // Filtered count should record the bad message.
    ASSERT_EQ(1, counter->num_rows_filtered);

    auto res_eof = scanner->get_next();
    ASSERT_TRUE(res_eof.status().is_end_of_file());

    scanner->close();
}

TEST_F(ArrowScannerTest, TestStreamSchemaInvalidIncompatibleTypeFiltered) {
    LoadStreamMgr load_stream_mgr;
    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    DeferOp remove_pipe([&]() { load_stream_mgr.remove(load_id); });
    ASSERT_OK(load_stream_mgr.put(load_id, pipe));

    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_ARROW;
    range.file_type = TFileType::FILE_STREAM;
    range.__set_load_id(load_id.to_thrift());
    ranges.emplace_back(range);

    TQueryOptions query_options;
    query_options.query_type = TQueryType::LOAD;
    TQueryGlobals query_globals;
    query_globals.time_zone = "UTC";
    RuntimeServices runtime_services;
    runtime_services.load_stream_mgr = &load_stream_mgr;
    QueryExecutionServices query_execution_services;
    query_execution_services.runtime = &runtime_services;

    RuntimeState* state = _obj_pool.add(
            new RuntimeState(TUniqueId(), query_options, query_globals, &query_execution_services, nullptr));

    DescriptorTbl* desc_tbl = DescTblHelper::generate_desc_tbl(state, _obj_pool, {src_slot_infos, dst_slot_infos});
    state->set_desc_tbl(desc_tbl);
    state->init_instance_mem_tracker();
    state->set_db("test_db");
    state->set_load_label("test_label");

    TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
    params->strict_mode = true;
    params->__isset.strict_mode = true;
    std::vector<TupleDescriptor*> tuples;
    desc_tbl->get_tuple_descs(&tuples);
    params->src_tuple_id = 0;
    params->dest_tuple_id = tuples.size() - 1;
    const auto* src_tuple = desc_tbl->get_tuple_descriptor(params->src_tuple_id);
    const auto* dst_tuple = desc_tbl->get_tuple_descriptor(params->dest_tuple_id);
    for (int i = 0; i < src_tuple->slots().size(); i++) {
        auto& src_slot = src_tuple->slots()[i];
        auto& dst_slot = dst_tuple->slots()[i];
        params->expr_of_dest_slot[dst_slot->id()] =
                create_column_ref(src_slot->id(), src_slot->type(), src_slot->is_nullable());
        params->dest_sid_to_src_sid_without_trans[dst_slot->id()] = src_slot->id();
    }
    params->__isset.dest_sid_to_src_sid_without_trans = true;
    for (int i = 0; i < src_tuple->slots().size(); i++) {
        params->src_slot_ids.emplace_back(i);
    }

    RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("test_prof", true));
    ScannerCounter* counter = _obj_pool.add(new ScannerCounter());

    TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
    broker_scan_range->params = *params;
    broker_scan_range->ranges = ranges;

    // Message 1: c0_int is provided as a Struct type (unusable schema for INT).
    {
        arrow::Int32Builder inner_builder;
        ASSERT_ARROW_OK(inner_builder.AppendValues({999}));
        std::shared_ptr<arrow::Array> inner_array;
        ASSERT_ARROW_OK(inner_builder.Finish(&inner_array));

        auto struct_type = arrow::struct_({arrow::field("sub_f", arrow::int32())});
        auto struct_array = std::make_shared<arrow::StructArray>(struct_type, 1, arrow::ArrayVector{inner_array});
        auto schema_bad = arrow::schema({arrow::field("c0_int", struct_type)});
        auto batch_bad = arrow::RecordBatch::Make(schema_bad, 1, {struct_array});

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, schema_bad).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch_bad));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes((const char*)buf->data(), buf->size());
        bb->flip_to_read();
        EXPECT_OK(pipe->append(std::move(bb)));
    }

    // Message 2: c0_int is valid INT32.
    {
        arrow::Int32Builder int_builder;
        ASSERT_ARROW_OK(int_builder.AppendValues({200}));
        std::shared_ptr<arrow::Array> int_array;
        ASSERT_ARROW_OK(int_builder.Finish(&int_array));

        auto schema_good = arrow::schema({arrow::field("c0_int", arrow::int32())});
        auto batch_good = arrow::RecordBatch::Make(schema_good, 1, {int_array});

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, schema_good).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch_good));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes((const char*)buf->data(), buf->size());
        bb->flip_to_read();
        EXPECT_OK(pipe->append(std::move(bb)));
    }
    EXPECT_OK(pipe->finish());

    auto scanner = std::make_unique<ArrowScanner>(state, profile, *broker_scan_range, counter);
    ASSERT_OK(scanner->open());

    auto res = scanner->get_next();
    ASSERT_OK(res.status());
    auto chunk = res.value();
    ASSERT_NE(nullptr, chunk);
    ASSERT_EQ(1, chunk->num_rows());
    ASSERT_EQ(200, chunk->columns()[0]->get(0).get_int32());

    ASSERT_EQ(1, counter->num_rows_filtered);

    auto res_eof = scanner->get_next();
    ASSERT_TRUE(res_eof.status().is_end_of_file());

    scanner->close();
}

TEST_F(ArrowScannerTest, TestStreamPulsarMessageMetaExtractionWithSourceId) {
    LoadStreamMgr load_stream_mgr;
    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    DeferOp remove_pipe([&]() { load_stream_mgr.remove(load_id); });
    ASSERT_OK(load_stream_mgr.put(load_id, pipe));

    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_ARROW;
    range.file_type = TFileType::FILE_STREAM;
    range.__set_load_id(load_id.to_thrift());
    ranges.emplace_back(range);

    TQueryOptions query_options;
    query_options.query_type = TQueryType::LOAD;
    TQueryGlobals query_globals;
    query_globals.time_zone = "UTC";
    LoadPathMgr load_path_mgr({(_tmp_root_dir / "pulsar_load_path").string()});
    ASSERT_OK(load_path_mgr.init());
    RuntimeServices runtime_services;
    runtime_services.load_path_mgr = &load_path_mgr;
    runtime_services.load_stream_mgr = &load_stream_mgr;
    QueryExecutionServices query_execution_services;
    query_execution_services.runtime = &runtime_services;

    RuntimeState* state = _obj_pool.add(
            new RuntimeState(TUniqueId(), query_options, query_globals, &query_execution_services, nullptr));

    DescriptorTbl* desc_tbl = DescTblHelper::generate_desc_tbl(state, _obj_pool, {src_slot_infos, dst_slot_infos});
    state->set_desc_tbl(desc_tbl);
    state->init_instance_mem_tracker();
    state->set_db("test_db");
    state->set_load_label("test_label");

    TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
    params->strict_mode = true;
    params->__isset.strict_mode = true;
    std::vector<TupleDescriptor*> tuples;
    desc_tbl->get_tuple_descs(&tuples);
    params->src_tuple_id = 0;
    params->dest_tuple_id = tuples.size() - 1;
    const auto* src_tuple = desc_tbl->get_tuple_descriptor(params->src_tuple_id);
    const auto* dst_tuple = desc_tbl->get_tuple_descriptor(params->dest_tuple_id);
    for (int i = 0; i < src_tuple->slots().size(); i++) {
        auto& src_slot = src_tuple->slots()[i];
        auto& dst_slot = dst_tuple->slots()[i];
        params->expr_of_dest_slot[dst_slot->id()] =
                create_column_ref(src_slot->id(), src_slot->type(), src_slot->is_nullable());
        params->dest_sid_to_src_sid_without_trans[dst_slot->id()] = src_slot->id();
    }
    params->__isset.dest_sid_to_src_sid_without_trans = true;
    for (int i = 0; i < src_tuple->slots().size(); i++) {
        params->src_slot_ids.emplace_back(i);
    }

    RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("test_prof", true));
    ScannerCounter* counter = _obj_pool.add(new ScannerCounter());

    TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
    broker_scan_range->params = *params;
    broker_scan_range->ranges = ranges;

    std::string junk = "invalid_arrow_stream_data_for_pulsar";
    ByteBufferPtr junk_bb = ByteBuffer::allocate_with_tracker(junk.size(), 0, ByteBufferMetaType::PULSAR).value();
    junk_bb->put_bytes(junk.data(), junk.size());
    junk_bb->flip_to_read();

    auto* msg_meta = static_cast<StreamMessageMeta*>(junk_bb->meta());
    msg_meta->set_partition(7);
    msg_meta->set_message_id("100:2:0");

    EXPECT_OK(pipe->append(std::move(junk_bb)));

    // Non-partitioned Pulsar topics have a message ID but no partition number.
    ByteBufferPtr nonpartitioned_bb =
            ByteBuffer::allocate_with_tracker(junk.size(), 0, ByteBufferMetaType::PULSAR).value();
    nonpartitioned_bb->put_bytes(junk.data(), junk.size());
    nonpartitioned_bb->flip_to_read();
    auto* nonpartitioned_meta = static_cast<StreamMessageMeta*>(nonpartitioned_bb->meta());
    nonpartitioned_meta->set_message_id("200:3:-1");
    EXPECT_OK(pipe->append(std::move(nonpartitioned_bb)));
    EXPECT_OK(pipe->finish());

    auto scanner = std::make_unique<ArrowScanner>(state, profile, *broker_scan_range, counter);
    ASSERT_OK(scanner->open());

    auto res = scanner->get_next();
    ASSERT_TRUE(res.status().is_end_of_file());
    ASSERT_EQ(2, counter->num_rows_filtered);

    scanner->close();

    std::ifstream error_file(load_path_mgr.get_load_error_absolute_path(state->get_error_log_file_path()));
    ASSERT_TRUE(error_file.is_open());
    std::stringstream error_buffer;
    error_buffer << error_file.rdbuf();
    const std::string errors = error_buffer.str();
    ASSERT_NE(std::string::npos, errors.find("100:2:0"));
    ASSERT_NE(std::string::npos, errors.find("200:3:-1"));
}

TEST_F(ArrowScannerTest, TestStreamIterativeEmptyChunkSkipping) {
    LoadStreamMgr load_stream_mgr;
    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    DeferOp remove_pipe([&]() { load_stream_mgr.remove(load_id); });
    ASSERT_OK(load_stream_mgr.put(load_id, pipe));

    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_ARROW;
    range.file_type = TFileType::FILE_STREAM;
    range.__set_load_id(load_id.to_thrift());
    ranges.emplace_back(range);

    TQueryOptions query_options;
    query_options.query_type = TQueryType::LOAD;
    TQueryGlobals query_globals;
    query_globals.time_zone = "UTC";
    RuntimeServices runtime_services;
    runtime_services.load_stream_mgr = &load_stream_mgr;
    QueryExecutionServices query_execution_services;
    query_execution_services.runtime = &runtime_services;

    RuntimeState* state = _obj_pool.add(
            new RuntimeState(TUniqueId(), query_options, query_globals, &query_execution_services, nullptr));

    DescriptorTbl* desc_tbl = DescTblHelper::generate_desc_tbl(state, _obj_pool, {src_slot_infos, dst_slot_infos});
    state->set_desc_tbl(desc_tbl);
    state->init_instance_mem_tracker();
    state->set_db("test_db");
    state->set_load_label("test_label");

    TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
    params->strict_mode = true;
    params->__isset.strict_mode = true;
    std::vector<TupleDescriptor*> tuples;
    desc_tbl->get_tuple_descs(&tuples);
    params->src_tuple_id = 0;
    params->dest_tuple_id = tuples.size() - 1;
    const auto* src_tuple = desc_tbl->get_tuple_descriptor(params->src_tuple_id);
    const auto* dst_tuple = desc_tbl->get_tuple_descriptor(params->dest_tuple_id);
    for (int i = 0; i < src_tuple->slots().size(); i++) {
        auto& src_slot = src_tuple->slots()[i];
        auto& dst_slot = dst_tuple->slots()[i];
        params->expr_of_dest_slot[dst_slot->id()] =
                create_column_ref(src_slot->id(), src_slot->type(), src_slot->is_nullable());
        params->dest_sid_to_src_sid_without_trans[dst_slot->id()] = src_slot->id();
    }
    params->__isset.dest_sid_to_src_sid_without_trans = true;
    for (int i = 0; i < src_tuple->slots().size(); i++) {
        params->src_slot_ids.emplace_back(i);
    }

    RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("test_prof", true));
    ScannerCounter* counter = _obj_pool.add(new ScannerCounter());

    TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
    broker_scan_range->params = *params;
    broker_scan_range->ranges = ranges;

    // Create a 0-row Arrow RecordBatch buffer
    arrow::Int32Builder empty_builder;
    std::shared_ptr<arrow::Array> empty_array;
    ASSERT_ARROW_OK(empty_builder.Finish(&empty_array));
    auto schema = arrow::schema({arrow::field("c0_int", arrow::int32())});
    auto batch_empty = arrow::RecordBatch::Make(schema, 0, {empty_array});

    auto stream_empty = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto writer_empty = arrow::ipc::MakeStreamWriter(stream_empty, schema).ValueOrDie();
    ASSERT_ARROW_OK(writer_empty->WriteRecordBatch(*batch_empty));
    ASSERT_ARROW_OK(writer_empty->Close());
    auto buf_empty = stream_empty->Finish().ValueOrDie();

    // Create a valid 1-row Arrow RecordBatch buffer
    arrow::Int32Builder valid_builder;
    ASSERT_ARROW_OK(valid_builder.Append(42));
    std::shared_ptr<arrow::Array> valid_array;
    ASSERT_ARROW_OK(valid_builder.Finish(&valid_array));
    auto batch_valid = arrow::RecordBatch::Make(schema, 1, {valid_array});

    auto stream_valid = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto writer_valid = arrow::ipc::MakeStreamWriter(stream_valid, schema).ValueOrDie();
    ASSERT_ARROW_OK(writer_valid->WriteRecordBatch(*batch_valid));
    ASSERT_ARROW_OK(writer_valid->Close());
    auto buf_valid = stream_valid->Finish().ValueOrDie();

    // Append 100 empty Arrow messages (would cause deep recursion if not iterative)
    constexpr int kEmptyBatches = 100;
    for (int i = 0; i < kEmptyBatches; ++i) {
        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf_empty->size()).value();
        bb->put_bytes((const char*)buf_empty->data(), buf_empty->size());
        bb->flip_to_read();
        EXPECT_OK(pipe->append(std::move(bb)));
    }

    // Append 1 valid Arrow message
    {
        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf_valid->size()).value();
        bb->put_bytes((const char*)buf_valid->data(), buf_valid->size());
        bb->flip_to_read();
        EXPECT_OK(pipe->append(std::move(bb)));
    }

    // Append another 50 empty Arrow messages
    constexpr int kTrailingEmptyBatches = 50;
    for (int i = 0; i < kTrailingEmptyBatches; ++i) {
        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf_empty->size()).value();
        bb->put_bytes((const char*)buf_empty->data(), buf_empty->size());
        bb->flip_to_read();
        EXPECT_OK(pipe->append(std::move(bb)));
    }

    EXPECT_OK(pipe->finish());

    auto scanner = std::make_unique<ArrowScanner>(state, profile, *broker_scan_range, counter);
    ASSERT_OK(scanner->open());

    // First get_next should iteratively skip the 100 empty batches and return the 1-row chunk
    auto res = scanner->get_next();
    ASSERT_OK(res.status());
    ASSERT_NE(nullptr, res.value());
    ASSERT_EQ(1, res.value()->num_rows());
    ASSERT_EQ(42, res.value()->columns()[0]->get(0).get_int32());

    // Second get_next should iteratively skip the remaining 50 empty batches and return EOF
    auto res_eof = scanner->get_next();
    ASSERT_TRUE(res_eof.status().is_end_of_file());

    scanner->close();
}

// ---------------------------------------------------------------------------
// A discrete (Kafka/Pulsar) message is decoded, validated and converted as a unit:
//   * a rejected message never exposes any of its rows,
//   * every row it declares is counted as filtered,
//   * every message is accounted for exactly once (read XOR rejected).
// ---------------------------------------------------------------------------
namespace {

class ArrowScannerLogSink final : public google::LogSink {
public:
    void send(google::LogSeverity severity, const char* full_filename, const char* base_filename, int line,
              const google::LogMessageTime& time, const char* message, size_t message_len) override {
        std::lock_guard lock(_mutex);
        _messages.emplace_back(severity, std::string(message, message_len));
    }

    size_t count(google::LogSeverity severity, const std::string& needle) const {
        std::lock_guard lock(_mutex);
        return std::count_if(_messages.begin(), _messages.end(), [&](const auto& entry) {
            return entry.first == severity && entry.second.find(needle) != std::string::npos;
        });
    }

private:
    mutable std::mutex _mutex;
    std::vector<std::pair<google::LogSeverity, std::string>> _messages;
};

void append_raw_message(const std::shared_ptr<StreamLoadPipe>& pipe, const uint8_t* data, size_t size) {
    ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(size).value();
    bb->put_bytes(reinterpret_cast<const char*>(data), size);
    bb->flip_to_read();
    ASSERT_OK(pipe->append(std::move(bb)));
}

std::vector<uint8_t> serialize_ipc_stream(const std::shared_ptr<arrow::Schema>& schema,
                                          const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) {
    auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto writer = arrow::ipc::MakeStreamWriter(stream, schema).ValueOrDie();
    for (const auto& batch : batches) {
        EXPECT_TRUE(writer->WriteRecordBatch(*batch).ok());
    }
    EXPECT_TRUE(writer->Close().ok());
    auto buf = stream->Finish().ValueOrDie();
    return std::vector<uint8_t>(buf->data(), buf->data() + buf->size());
}

void append_ipc_message(const std::shared_ptr<StreamLoadPipe>& pipe, const std::shared_ptr<arrow::Schema>& schema,
                        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) {
    auto bytes = serialize_ipc_stream(schema, batches);
    append_raw_message(pipe, bytes.data(), bytes.size());
}

std::shared_ptr<arrow::RecordBatch> make_int32_batch(const std::shared_ptr<arrow::Schema>& schema,
                                                     const std::vector<int32_t>& values) {
    arrow::Int32Builder builder;
    EXPECT_TRUE(builder.AppendValues(values).ok());
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return arrow::RecordBatch::Make(schema, static_cast<int64_t>(values.size()), {array});
}

std::shared_ptr<arrow::RecordBatch> make_string_batch(const std::shared_ptr<arrow::Schema>& schema,
                                                      const std::vector<std::string>& values) {
    arrow::StringBuilder builder;
    EXPECT_TRUE(builder.AppendValues(values).ok());
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return arrow::RecordBatch::Make(schema, static_cast<int64_t>(values.size()), {array});
}

// Deliberately unchecked: tests use it to build out-of-range dictionary indices.
std::shared_ptr<arrow::RecordBatch> make_dictionary_string_batch(const std::shared_ptr<arrow::Schema>& schema,
                                                                 const std::vector<std::string>& dictionary_values,
                                                                 const std::vector<int8_t>& indices) {
    arrow::StringBuilder dict_builder;
    EXPECT_TRUE(dict_builder.AppendValues(dictionary_values).ok());
    std::shared_ptr<arrow::Array> dict_array;
    EXPECT_TRUE(dict_builder.Finish(&dict_array).ok());

    arrow::Int8Builder idx_builder;
    EXPECT_TRUE(idx_builder.AppendValues(indices).ok());
    std::shared_ptr<arrow::Array> idx_array;
    EXPECT_TRUE(idx_builder.Finish(&idx_array).ok());

    auto dict_type = arrow::dictionary(arrow::int8(), arrow::utf8());
    std::shared_ptr<arrow::Array> column = std::make_shared<arrow::DictionaryArray>(dict_type, idx_array, dict_array);
    return arrow::RecordBatch::Make(schema, static_cast<int64_t>(indices.size()), {column});
}

// A list<null> column whose offsets describe `offsets.size() - 1` rows over a NullArray child of
// `offsets.back()` elements. The child has no buffers, so the message stays tiny on the wire.
std::shared_ptr<arrow::RecordBatch> make_null_list_batch(const std::shared_ptr<arrow::Schema>& schema,
                                                         const std::vector<int32_t>& offsets) {
    arrow::Int32Builder offsets_builder;
    EXPECT_TRUE(offsets_builder.AppendValues(offsets).ok());
    std::shared_ptr<arrow::Array> offsets_array;
    EXPECT_TRUE(offsets_builder.Finish(&offsets_array).ok());
    auto child = std::make_shared<arrow::NullArray>(offsets.back());
    auto list = arrow::ListArray::FromArrays(*offsets_array, *child);
    EXPECT_TRUE(list.ok()) << list.status().ToString();
    std::shared_ptr<arrow::Array> list_array = list.ValueOrDie();
    return arrow::RecordBatch::Make(schema, static_cast<int64_t>(offsets.size()) - 1, {list_array});
}

struct HostileMessageCase {
    SlotTypeDescInfoArray slots;
    std::shared_ptr<arrow::Schema> hostile_schema;
    std::vector<std::shared_ptr<arrow::RecordBatch>> hostile_batches;
    std::shared_ptr<arrow::Schema> good_schema;
    std::shared_ptr<arrow::RecordBatch> good_batch;
    int64_t expected_filtered_rows = 0;
    int64_t decoded_bytes_cap = 64 * 1024 * 1024;
};

// Feeds `hostile` followed by one valid message and verifies the scanner rejects the hostile one atomically.
void expect_hostile_message_rejected_atomically(const HostileMessageCase& c) {
    const int64_t old_limit = config::routine_load_arrow_max_decoded_bytes_per_message;
    config::routine_load_arrow_max_decoded_bytes_per_message = c.decoded_bytes_cap;
    DeferOp restore_limit([&]() { config::routine_load_arrow_max_decoded_bytes_per_message = old_limit; });

    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(c.slots, UniqueId::gen_uid(), pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();

    append_ipc_message(pipe, c.hostile_schema, c.hostile_batches);
    append_ipc_message(pipe, c.good_schema, {c.good_batch});
    ASSERT_OK(pipe->finish());

    ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
    ASSERT_OK(scanner.open());

    auto res = scanner.get_next();
    ASSERT_OK(res.status());
    ASSERT_NE(nullptr, res.value());
    EXPECT_EQ(c.good_batch->num_rows(), res.value()->num_rows())
            << "no row of the rejected message may be exposed before the valid message";

    auto eof_res = scanner.get_next();
    EXPECT_TRUE(eof_res.status().is_end_of_file()) << eof_res.status();

    EXPECT_EQ(c.expected_filtered_rows, ctx->counter->num_rows_filtered);
    EXPECT_EQ(1, ctx->counter->arrow_messages_rejected);
    EXPECT_EQ(1, ctx->counter->arrow_messages_read) << "a rejected message must not also be counted as read";
    scanner.close();
}

HostileMessageCase make_int_slot_case() {
    HostileMessageCase c;
    c.slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    c.good_schema = arrow::schema({arrow::field("c0_int", arrow::int32())});
    c.good_batch = make_int32_batch(c.good_schema, {101, 202});
    return c;
}

HostileMessageCase make_string_slot_case() {
    HostileMessageCase c;
    c.slots.emplace_back("c0_str", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);
    c.good_schema = arrow::schema({arrow::field("c0_str", arrow::utf8())});
    c.good_batch = make_string_batch(c.good_schema, {"ok1", "ok2"});
    return c;
}

} // namespace

TEST_F(ArrowScannerTest, TestStreamDecompressionBombFilteredByMemoryCap) {
    const int64_t old_limit = config::routine_load_arrow_max_decoded_bytes_per_message;
    config::routine_load_arrow_max_decoded_bytes_per_message = 8 * 1024;
    DeferOp restore_limit([&]() { config::routine_load_arrow_max_decoded_bytes_per_message = old_limit; });

    SlotTypeDescInfoArray slots;
    slots.emplace_back("c0_bigint", TypeDescriptor::from_logical_type(TYPE_BIGINT), true);

    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();
    auto* counter = ctx->counter;

    auto schema = arrow::schema({arrow::field("c0_bigint", arrow::int64())});

    // Message 1: LZ4-compressed batch with 4096 int64 rows (32 KB uncompressed body > 8 KB cap).
    {
        arrow::Int64Builder builder;
        for (int64_t i = 0; i < 4096; ++i) {
            ASSERT_ARROW_OK(builder.Append(42));
        }
        std::shared_ptr<arrow::Array> array;
        ASSERT_ARROW_OK(builder.Finish(&array));
        auto batch = arrow::RecordBatch::Make(schema, 4096, {array});

        arrow::ipc::IpcWriteOptions write_options = arrow::ipc::IpcWriteOptions::Defaults();
        auto codec_res = arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME);
        ASSERT_ARROW_OK(codec_res.status());
        write_options.codec = std::move(codec_res).ValueOrDie();

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, schema, write_options).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        EXPECT_OK(pipe->append(std::move(bb)));
    }

    // Message 2: Valid small uncompressed message (2 rows, well under 8 KB cap).
    {
        arrow::Int64Builder builder;
        ASSERT_ARROW_OK(builder.AppendValues({101, 202}));
        std::shared_ptr<arrow::Array> array;
        ASSERT_ARROW_OK(builder.Finish(&array));
        auto batch = arrow::RecordBatch::Make(schema, 2, {array});

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        EXPECT_OK(pipe->append(std::move(bb)));
    }
    EXPECT_OK(pipe->finish());

    ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, counter);
    ASSERT_OK(scanner.open());

    auto res = scanner.get_next();
    ASSERT_OK(res.status());
    auto chunk = res.value();
    ASSERT_NE(nullptr, chunk);
    ASSERT_EQ(2, chunk->num_rows());
    EXPECT_EQ(101, chunk->columns()[0]->get(0).get_int64());
    EXPECT_EQ(202, chunk->columns()[0]->get(1).get_int64());

    auto eof_res = scanner.get_next();
    EXPECT_TRUE(eof_res.status().is_end_of_file());

    EXPECT_EQ(1, counter->arrow_messages_rejected);
    EXPECT_EQ(4096, counter->num_rows_filtered);
    EXPECT_EQ(1, counter->arrow_messages_read);

    scanner.close();
}

TEST_F(ArrowScannerTest, TestStreamRejectedCompressedMessageStillCountsDecompressedBytes) {
    auto c = make_string_slot_case();
    SlotTypeDescInfoArray slots = c.slots;
    auto field_name = c.good_schema->field(0)->name();
    auto schema = arrow::schema({arrow::field(field_name, arrow::utf8())});

    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();
    auto* counter = ctx->counter;

    // Message 1: LZ4_FRAME-compressed utf8 batch holding 256 values of std::string(64, 'a')
    // plus one final value "\xff" (invalid UTF-8: ValidateFull() rejects it after ReadNext
    // has already allocated the decompressed buffers).
    {
        arrow::StringBuilder builder;
        for (int i = 0; i < 256; ++i) {
            ASSERT_ARROW_OK(builder.Append(std::string(64, 'a')));
        }
        ASSERT_ARROW_OK(builder.Append(std::string("\xff", 1)));
        std::shared_ptr<arrow::Array> array;
        ASSERT_ARROW_OK(builder.Finish(&array));
        auto batch = arrow::RecordBatch::Make(schema, 257, {array});

        arrow::ipc::IpcWriteOptions write_options = arrow::ipc::IpcWriteOptions::Defaults();
        auto codec_res = arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME);
        ASSERT_ARROW_OK(codec_res.status());
        write_options.codec = std::move(codec_res).ValueOrDie();

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, schema, write_options).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        EXPECT_OK(pipe->append(std::move(bb)));
    }

    // Message 2: Valid small uncompressed message.
    {
        arrow::StringBuilder builder;
        ASSERT_ARROW_OK(builder.AppendValues({"valid1", "valid2"}));
        std::shared_ptr<arrow::Array> array;
        ASSERT_ARROW_OK(builder.Finish(&array));
        auto batch = arrow::RecordBatch::Make(schema, 2, {array});

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        EXPECT_OK(pipe->append(std::move(bb)));
    }
    EXPECT_OK(pipe->finish());

    ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, counter);
    ASSERT_OK(scanner.open());

    auto res = scanner.get_next();
    ASSERT_OK(res.status());
    auto chunk = res.value();
    ASSERT_NE(nullptr, chunk);
    ASSERT_EQ(2, chunk->num_rows());
    EXPECT_EQ("valid1", chunk->columns()[0]->get(0).get_slice().to_string());
    EXPECT_EQ("valid2", chunk->columns()[0]->get(1).get_slice().to_string());

    auto eof_res = scanner.get_next();
    EXPECT_TRUE(eof_res.status().is_end_of_file());

    EXPECT_EQ(1, counter->arrow_messages_rejected);
    EXPECT_EQ(1, counter->arrow_messages_read); // success-only
    EXPECT_GT(counter->arrow_decompressed_bytes, 0);
    auto* profile_counter = scanner._profile->get_counter("ArrowDecompressedBytes");
    ASSERT_NE(nullptr, profile_counter);
    EXPECT_EQ(counter->arrow_decompressed_bytes, profile_counter->value());

    scanner.close();
}

TEST_F(ArrowScannerTest, TestStreamUnparseableMessageCountsZeroDecompressedBytes) {
    auto c = make_string_slot_case();
    SlotTypeDescInfoArray slots = c.slots;

    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();
    auto* counter = ctx->counter;

    std::string junk = "not an arrow stream";
    ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(junk.size()).value();
    bb->put_bytes(junk.data(), junk.size());
    bb->flip_to_read();
    EXPECT_OK(pipe->append(std::move(bb)));
    EXPECT_OK(pipe->finish());

    ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, counter);
    ASSERT_OK(scanner.open());

    auto res = scanner.get_next();
    EXPECT_TRUE(res.status().is_end_of_file());

    EXPECT_EQ(1, counter->arrow_messages_rejected);
    EXPECT_EQ(0, counter->arrow_decompressed_bytes);
    auto* profile_counter = scanner._profile->get_counter("ArrowDecompressedBytes");
    if (profile_counter != nullptr) {
        EXPECT_EQ(0, profile_counter->value());
    }

    scanner.close();
}

TEST_F(ArrowScannerTest, TestStreamMultiBatchDecompressionBombCountsAllDeclaredRows) {
    const int64_t old_limit = config::routine_load_arrow_max_decoded_bytes_per_message;
    config::routine_load_arrow_max_decoded_bytes_per_message = 8 * 1024;
    DeferOp restore_limit([&]() { config::routine_load_arrow_max_decoded_bytes_per_message = old_limit; });

    SlotTypeDescInfoArray slots;
    slots.emplace_back("c0_bigint", TypeDescriptor::from_logical_type(TYPE_BIGINT), true);

    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();
    auto* counter = ctx->counter;

    auto schema = arrow::schema({arrow::field("c0_bigint", arrow::int64())});

    // Message 1: Single Arrow IPC stream containing two LZ4-compressed RecordBatches:
    // Batch 1 has 64 int64 rows (512 bytes, decodes fine), Batch 2 has 4096 int64 rows (32 KB > 8 KB cap).
    {
        arrow::Int64Builder builder1;
        for (int64_t i = 0; i < 64; ++i) {
            ASSERT_ARROW_OK(builder1.Append(i));
        }
        std::shared_ptr<arrow::Array> array1;
        ASSERT_ARROW_OK(builder1.Finish(&array1));
        auto batch1 = arrow::RecordBatch::Make(schema, 64, {array1});

        arrow::Int64Builder builder2;
        for (int64_t i = 0; i < 4096; ++i) {
            ASSERT_ARROW_OK(builder2.Append(42));
        }
        std::shared_ptr<arrow::Array> array2;
        ASSERT_ARROW_OK(builder2.Finish(&array2));
        auto batch2 = arrow::RecordBatch::Make(schema, 4096, {array2});

        arrow::ipc::IpcWriteOptions write_options = arrow::ipc::IpcWriteOptions::Defaults();
        auto codec_res = arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME);
        ASSERT_ARROW_OK(codec_res.status());
        write_options.codec = std::move(codec_res).ValueOrDie();

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, schema, write_options).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch1));
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch2));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        EXPECT_OK(pipe->append(std::move(bb)));
    }

    // Message 2: Valid small uncompressed message (2 rows, well under 8 KB cap).
    {
        arrow::Int64Builder builder;
        ASSERT_ARROW_OK(builder.AppendValues({101, 202}));
        std::shared_ptr<arrow::Array> array;
        ASSERT_ARROW_OK(builder.Finish(&array));
        auto batch = arrow::RecordBatch::Make(schema, 2, {array});

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        EXPECT_OK(pipe->append(std::move(bb)));
    }
    EXPECT_OK(pipe->finish());

    ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, counter);
    ASSERT_OK(scanner.open());

    auto res = scanner.get_next();
    ASSERT_OK(res.status());
    auto chunk = res.value();
    ASSERT_NE(nullptr, chunk);
    ASSERT_EQ(2, chunk->num_rows());
    EXPECT_EQ(101, chunk->columns()[0]->get(0).get_int64());
    EXPECT_EQ(202, chunk->columns()[0]->get(1).get_int64());

    auto eof_res = scanner.get_next();
    EXPECT_TRUE(eof_res.status().is_end_of_file());

    EXPECT_EQ(1, counter->arrow_messages_rejected);
    EXPECT_EQ(64 + 4096, counter->num_rows_filtered);
    EXPECT_EQ(1, counter->arrow_messages_read);

    scanner.close();
}

TEST_F(ArrowScannerTest, TestStreamCompressedLz4AndZstdIpcBatches) {
    SlotTypeDescInfoArray slots;
    slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);

    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();

    auto schema = arrow::schema({arrow::field("c0_int", arrow::int32())});

    auto append_compressed_message = [&](arrow::Compression::type compression_type,
                                         const std::vector<int32_t>& values) {
        arrow::Int32Builder builder;
        ASSERT_ARROW_OK(builder.AppendValues(values));
        std::shared_ptr<arrow::Array> array;
        ASSERT_ARROW_OK(builder.Finish(&array));
        auto batch = arrow::RecordBatch::Make(schema, static_cast<int64_t>(values.size()), {array});

        arrow::ipc::IpcWriteOptions write_options = arrow::ipc::IpcWriteOptions::Defaults();
        auto codec_res = arrow::util::Codec::Create(compression_type);
        ASSERT_ARROW_OK(codec_res.status());
        write_options.codec = std::move(codec_res).ValueOrDie();

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, schema, write_options).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bb)));
    };

    append_compressed_message(arrow::Compression::LZ4_FRAME, {10, 20, 30});
    append_compressed_message(arrow::Compression::ZSTD, {40, 50});
    ASSERT_OK(pipe->finish());

    ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
    ASSERT_OK(scanner.open());

    auto res1 = scanner.get_next();
    ASSERT_OK(res1.status());
    auto chunk1 = res1.value();
    ASSERT_NE(nullptr, chunk1);
    ASSERT_EQ(3, chunk1->num_rows());
    EXPECT_EQ(10, chunk1->columns()[0]->get(0).get_int32());
    EXPECT_EQ(20, chunk1->columns()[0]->get(1).get_int32());
    EXPECT_EQ(30, chunk1->columns()[0]->get(2).get_int32());

    auto res2 = scanner.get_next();
    ASSERT_OK(res2.status());
    auto chunk2 = res2.value();
    ASSERT_NE(nullptr, chunk2);
    ASSERT_EQ(2, chunk2->num_rows());
    EXPECT_EQ(40, chunk2->columns()[0]->get(0).get_int32());
    EXPECT_EQ(50, chunk2->columns()[0]->get(1).get_int32());

    auto eof_res = scanner.get_next();
    EXPECT_TRUE(eof_res.status().is_end_of_file());

    EXPECT_EQ(2, ctx->counter->arrow_messages_read);
    EXPECT_EQ(0, ctx->counter->arrow_messages_rejected);
    EXPECT_GT(ctx->counter->arrow_decompressed_bytes, 0);
    EXPECT_GT(ctx->counter->arrow_decode_ns, 0);

    scanner.close();
}

TEST_F(ArrowScannerTest, TestStreamDictionaryEncodedComplexTypeFiltered) {
    SlotTypeDescInfoArray slots;
    slots.emplace_back("c1", TypeDescriptor::from_logical_type(TYPE_INT), true);

    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();

    // Message 1: 2 rows where column "c1" is dictionary(int8, struct<a: int32>) targeting INT slot "c1".
    {
        auto struct_type = arrow::struct_({arrow::field("a", arrow::int32())});
        arrow::Int32Builder field_builder;
        ASSERT_ARROW_OK(field_builder.AppendValues({100, 200}));
        std::shared_ptr<arrow::Array> field_array;
        ASSERT_ARROW_OK(field_builder.Finish(&field_array));
        auto dict_values = std::make_shared<arrow::StructArray>(struct_type, 2, arrow::ArrayVector{field_array});

        arrow::Int8Builder indices_builder;
        ASSERT_ARROW_OK(indices_builder.AppendValues({0, 1}));
        std::shared_ptr<arrow::Array> indices_array;
        ASSERT_ARROW_OK(indices_builder.Finish(&indices_array));

        auto dict_type = arrow::dictionary(arrow::int8(), struct_type);
        auto dict_array_res = arrow::DictionaryArray::FromArrays(dict_type, indices_array, dict_values);
        ASSERT_ARROW_OK(dict_array_res.status());
        auto dict_array = dict_array_res.ValueOrDie();

        auto bad_schema = arrow::schema({arrow::field("c1", dict_type)});
        auto bad_batch = arrow::RecordBatch::Make(bad_schema, 2, {dict_array});

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, bad_schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*bad_batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bb)));
    }

    // Message 2: Valid 2-row INT message.
    {
        auto good_schema = arrow::schema({arrow::field("c1", arrow::int32())});
        arrow::Int32Builder builder;
        ASSERT_ARROW_OK(builder.AppendValues({77, 88}));
        std::shared_ptr<arrow::Array> array;
        ASSERT_ARROW_OK(builder.Finish(&array));
        auto good_batch = arrow::RecordBatch::Make(good_schema, 2, {array});

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, good_schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*good_batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bb)));
    }
    ASSERT_OK(pipe->finish());

    ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
    ASSERT_OK(scanner.open());

    auto res = scanner.get_next();
    ASSERT_OK(res.status());
    auto chunk = res.value();
    ASSERT_NE(nullptr, chunk);
    ASSERT_EQ(2, chunk->num_rows());
    EXPECT_EQ(77, chunk->columns()[0]->get(0).get_int32());
    EXPECT_EQ(88, chunk->columns()[0]->get(1).get_int32());

    auto eof_res = scanner.get_next();
    EXPECT_TRUE(eof_res.status().is_end_of_file());

    EXPECT_EQ(2, ctx->counter->num_rows_filtered);
    EXPECT_EQ(1, ctx->counter->arrow_messages_rejected);
    EXPECT_EQ(1, ctx->counter->arrow_messages_read);

    scanner.close();
}

TEST_F(ArrowScannerTest, TestStreamDiscreteMessageReturnsBeforeNextMessageArrives) {
    SlotTypeDescInfoArray slots;
    slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);

    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();

    auto schema = arrow::schema({arrow::field("c0_int", arrow::int32())});
    auto append_message = [&](const std::vector<int32_t>& values) {
        arrow::Int32Builder builder;
        ASSERT_ARROW_OK(builder.AppendValues(values));
        std::shared_ptr<arrow::Array> array;
        ASSERT_ARROW_OK(builder.Finish(&array));
        auto batch = arrow::RecordBatch::Make(schema, static_cast<int64_t>(values.size()), {array});

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bb)));
    };

    // Append Message 1 (2 rows) WITHOUT calling pipe->finish().
    append_message({11, 22});

    ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
    ASSERT_OK(scanner.open());

    auto future = std::async(std::launch::async, [&]() { return scanner.get_next(); });
    auto wait_status = future.wait_for(std::chrono::milliseconds(500));
    if (wait_status != std::future_status::ready) {
        pipe->cancel(Status::Cancelled("timed out waiting for first discrete message"));
    }
    ASSERT_EQ(std::future_status::ready, wait_status);

    auto res1 = future.get();
    ASSERT_OK(res1.status());
    auto chunk1 = res1.value();
    ASSERT_NE(nullptr, chunk1);
    ASSERT_EQ(2, chunk1->num_rows());
    EXPECT_EQ(11, chunk1->columns()[0]->get(0).get_int32());
    EXPECT_EQ(22, chunk1->columns()[0]->get(1).get_int32());

    // Now append Message 2 (3 rows) and finish the pipe.
    append_message({33, 44, 55});
    ASSERT_OK(pipe->finish());

    auto res2 = scanner.get_next();
    ASSERT_OK(res2.status());
    auto chunk2 = res2.value();
    ASSERT_NE(nullptr, chunk2);
    ASSERT_EQ(3, chunk2->num_rows());
    EXPECT_EQ(33, chunk2->columns()[0]->get(0).get_int32());
    EXPECT_EQ(44, chunk2->columns()[0]->get(1).get_int32());
    EXPECT_EQ(55, chunk2->columns()[0]->get(2).get_int32());

    auto eof_res = scanner.get_next();
    EXPECT_TRUE(eof_res.status().is_end_of_file());

    scanner.close();
}

TEST_F(ArrowScannerTest, TestStreamRowInFileResetsPerDiscreteMessage) {
    LoadStreamMgr load_stream_mgr;
    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    DeferOp remove_pipe([&]() { load_stream_mgr.remove(load_id); });
    ASSERT_OK(load_stream_mgr.put(load_id, pipe));

    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_str", TypeDescriptor::create_char_type(2), true);
    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_ARROW;
    range.file_type = TFileType::FILE_STREAM;
    range.__set_load_id(load_id.to_thrift());
    ranges.emplace_back(range);

    TQueryOptions query_options;
    query_options.query_type = TQueryType::LOAD;
    query_options.__set_log_rejected_record_num(10);
    TQueryGlobals query_globals;
    query_globals.time_zone = "UTC";
    LoadPathMgr load_path_mgr({(_tmp_root_dir / "row_in_file_load_path").string()});
    ASSERT_OK(load_path_mgr.init());
    RuntimeServices runtime_services;
    runtime_services.load_path_mgr = &load_path_mgr;
    runtime_services.load_stream_mgr = &load_stream_mgr;
    QueryExecutionServices query_execution_services;
    query_execution_services.runtime = &runtime_services;

    RuntimeState* state = _obj_pool.add(
            new RuntimeState(TUniqueId(), query_options, query_globals, &query_execution_services, nullptr));

    DescriptorTbl* desc_tbl = DescTblHelper::generate_desc_tbl(state, _obj_pool, {src_slot_infos, dst_slot_infos});
    state->set_desc_tbl(desc_tbl);
    state->init_instance_mem_tracker();
    state->set_db("test_db");
    state->set_load_label("test_row_in_file_label");
    state->set_txn_id(54321);

    TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
    params->strict_mode = true;
    params->__isset.strict_mode = true;
    std::vector<TupleDescriptor*> tuples;
    desc_tbl->get_tuple_descs(&tuples);
    params->src_tuple_id = 0;
    params->dest_tuple_id = static_cast<int>(tuples.size()) - 1;
    const auto* src_tuple = desc_tbl->get_tuple_descriptor(params->src_tuple_id);
    const auto* dst_tuple = desc_tbl->get_tuple_descriptor(params->dest_tuple_id);
    for (int i = 0; i < static_cast<int>(src_tuple->slots().size()); i++) {
        auto& src_slot = src_tuple->slots()[i];
        auto& dst_slot = dst_tuple->slots()[i];
        params->expr_of_dest_slot[dst_slot->id()] =
                create_column_ref(src_slot->id(), src_slot->type(), src_slot->is_nullable());
        params->dest_sid_to_src_sid_without_trans[dst_slot->id()] = src_slot->id();
        params->src_slot_ids.emplace_back(i);
    }
    params->__isset.dest_sid_to_src_sid_without_trans = true;

    RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("test_prof", true));
    ScannerCounter* counter = _obj_pool.add(new ScannerCounter());

    TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
    broker_scan_range->params = *params;
    broker_scan_range->ranges = ranges;

    auto schema = arrow::schema({arrow::field("c0_str", arrow::utf8())});
    auto append_str_message = [&](const std::vector<std::string>& values) {
        arrow::StringBuilder builder;
        ASSERT_ARROW_OK(builder.AppendValues(values));
        std::shared_ptr<arrow::Array> array;
        ASSERT_ARROW_OK(builder.Finish(&array));
        auto batch = arrow::RecordBatch::Make(schema, static_cast<int64_t>(values.size()), {array});

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bb)));
    };

    // Message 1: 5 valid rows.
    append_str_message({"a", "b", "c", "d", "e"});
    // Message 2: row index 2 ("too_long") exceeds CHAR(2) in strict mode.
    append_str_message({"u", "v", "too_long", "w"});
    ASSERT_OK(pipe->finish());

    ArrowScanner scanner(state, profile, *broker_scan_range, counter);
    ASSERT_OK(scanner.open());

    auto res1 = scanner.get_next();
    ASSERT_OK(res1.status());
    ASSERT_NE(nullptr, res1.value());
    EXPECT_EQ(5, res1.value()->num_rows());

    auto res2 = scanner.get_next();
    ASSERT_OK(res2.status());
    ASSERT_NE(nullptr, res2.value());
    EXPECT_EQ(3, res2.value()->num_rows());

    auto eof_res = scanner.get_next();
    EXPECT_TRUE(eof_res.status().is_end_of_file());

    scanner.close();

    auto* writer = LoadPathStateHelper::rejected_record_writer(state);
    ASSERT_NE(nullptr, writer);
    ASSERT_FALSE(writer->file_path().empty());
    std::ifstream rejected_file(writer->file_path());
    ASSERT_TRUE(rejected_file.is_open());
    std::stringstream rejected_buf;
    rejected_buf << rejected_file.rdbuf();
    const std::string rejected_json = rejected_buf.str();

    EXPECT_NE(std::string::npos, rejected_json.find("\"row_in_file\":2")) << rejected_json;
    EXPECT_EQ(std::string::npos, rejected_json.find("\"row_in_file\":7")) << rejected_json;
}

TEST_F(ArrowScannerTest, TestStreamLeadingZeroRowBatchDoesNotSplitNextMessage) {
    SlotTypeDescInfoArray slots;
    slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);

    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();

    auto schema = arrow::schema({arrow::field("c0_int", arrow::int32())});
    auto append_multi_batch_message = [&](const std::vector<std::vector<int32_t>>& batches) {
        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, schema).ValueOrDie();
        for (const auto& values : batches) {
            arrow::Int32Builder builder;
            ASSERT_ARROW_OK(builder.AppendValues(values));
            std::shared_ptr<arrow::Array> array;
            ASSERT_ARROW_OK(builder.Finish(&array));
            auto batch = arrow::RecordBatch::Make(schema, static_cast<int64_t>(values.size()), {array});
            ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        }
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bb)));
    };

    // Message 1: 2 rows.
    append_multi_batch_message({{1, 2}});
    // Message 2: two 0-row batches in one IPC stream.
    append_multi_batch_message({{}, {}});
    // Message 3: two batches of 2 rows and 3 rows in one IPC stream.
    append_multi_batch_message({{10, 20}, {30, 40, 50}});
    ASSERT_OK(pipe->finish());

    ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
    ASSERT_OK(scanner.open());

    auto res1 = scanner.get_next();
    ASSERT_OK(res1.status());
    auto chunk1 = res1.value();
    ASSERT_NE(nullptr, chunk1);
    ASSERT_EQ(2, chunk1->num_rows());
    EXPECT_EQ(1, chunk1->columns()[0]->get(0).get_int32());
    EXPECT_EQ(2, chunk1->columns()[0]->get(1).get_int32());

    // Second get_next() must skip Message 2's empty batches without crashing and
    // coalesce both batches of Message 3 into a single 5-row Chunk.
    auto res2 = scanner.get_next();
    ASSERT_OK(res2.status());
    auto chunk2 = res2.value();
    ASSERT_NE(nullptr, chunk2);
    ASSERT_EQ(5, chunk2->num_rows());
    EXPECT_EQ(10, chunk2->columns()[0]->get(0).get_int32());
    EXPECT_EQ(20, chunk2->columns()[0]->get(1).get_int32());
    EXPECT_EQ(30, chunk2->columns()[0]->get(2).get_int32());
    EXPECT_EQ(40, chunk2->columns()[0]->get(3).get_int32());
    EXPECT_EQ(50, chunk2->columns()[0]->get(4).get_int32());

    auto eof_res = scanner.get_next();
    EXPECT_TRUE(eof_res.status().is_end_of_file());

    EXPECT_EQ(3, ctx->counter->arrow_messages_read);
    EXPECT_EQ(0, ctx->counter->arrow_messages_rejected);

    scanner.close();
}

TEST_F(ArrowScannerTest, TestStreamMessageErrorDoesNotInheritPreviousColumnContext) {
    LoadStreamMgr load_stream_mgr;
    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    DeferOp remove_pipe([&]() { load_stream_mgr.remove(load_id); });
    ASSERT_OK(load_stream_mgr.put(load_id, pipe));

    SlotTypeDescInfoArray src_slot_infos;
    src_slot_infos.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), false);
    src_slot_infos.emplace_back("c1_str", TypeDescriptor::create_varchar_type(32), true);
    SlotTypeDescInfoArray dst_slot_infos = src_slot_infos;

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_ARROW;
    range.file_type = TFileType::FILE_STREAM;
    range.__set_load_id(load_id.to_thrift());
    ranges.emplace_back(range);

    TQueryOptions query_options;
    query_options.query_type = TQueryType::LOAD;
    query_options.__set_log_rejected_record_num(10);
    TQueryGlobals query_globals;
    query_globals.time_zone = "UTC";
    LoadPathMgr load_path_mgr({(_tmp_root_dir / "msg_error_col_ctx_load_path").string()});
    ASSERT_OK(load_path_mgr.init());
    RuntimeServices runtime_services;
    runtime_services.load_path_mgr = &load_path_mgr;
    runtime_services.load_stream_mgr = &load_stream_mgr;
    QueryExecutionServices query_execution_services;
    query_execution_services.runtime = &runtime_services;

    RuntimeState* state = _obj_pool.add(
            new RuntimeState(TUniqueId(), query_options, query_globals, &query_execution_services, nullptr));

    DescriptorTbl* desc_tbl = DescTblHelper::generate_desc_tbl(state, _obj_pool, {src_slot_infos, dst_slot_infos});
    state->set_desc_tbl(desc_tbl);
    state->init_instance_mem_tracker();
    state->set_db("test_db");
    state->set_load_label("test_msg_error_col_ctx_label");
    state->set_txn_id(54322);

    TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
    params->strict_mode = true;
    params->__isset.strict_mode = true;
    std::vector<TupleDescriptor*> tuples;
    desc_tbl->get_tuple_descs(&tuples);
    params->src_tuple_id = 0;
    params->dest_tuple_id = static_cast<int>(tuples.size()) - 1;
    const auto* src_tuple = desc_tbl->get_tuple_descriptor(params->src_tuple_id);
    const auto* dst_tuple = desc_tbl->get_tuple_descriptor(params->dest_tuple_id);
    for (int i = 0; i < static_cast<int>(src_tuple->slots().size()); i++) {
        auto& src_slot = src_tuple->slots()[i];
        auto& dst_slot = dst_tuple->slots()[i];
        params->expr_of_dest_slot[dst_slot->id()] =
                create_column_ref(src_slot->id(), src_slot->type(), src_slot->is_nullable());
        params->dest_sid_to_src_sid_without_trans[dst_slot->id()] = src_slot->id();
        params->src_slot_ids.emplace_back(i);
    }
    params->__isset.dest_sid_to_src_sid_without_trans = true;

    RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("test_prof", true));
    ScannerCounter* counter = _obj_pool.add(new ScannerCounter());

    TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
    broker_scan_range->params = *params;
    broker_scan_range->ranges = ranges;

    auto good_schema = arrow::schema({arrow::field("c0_int", arrow::int32()), arrow::field("c1_str", arrow::utf8())});
    auto append_good_message = [&](const std::vector<int32_t>& ints, const std::vector<std::string>& strs) {
        arrow::Int32Builder int_builder;
        ASSERT_ARROW_OK(int_builder.AppendValues(ints));
        std::shared_ptr<arrow::Array> int_array;
        ASSERT_ARROW_OK(int_builder.Finish(&int_array));

        arrow::StringBuilder str_builder;
        ASSERT_ARROW_OK(str_builder.AppendValues(strs));
        std::shared_ptr<arrow::Array> str_array;
        ASSERT_ARROW_OK(str_builder.Finish(&str_array));

        auto batch = arrow::RecordBatch::Make(good_schema, static_cast<int64_t>(ints.size()), {int_array, str_array});
        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, good_schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bb)));
    };

    // Message 1: Valid 2-row Arrow IPC stream with c0_int and c1_str.
    append_good_message({10, 20}, {"alpha", "beta"});

    // Message 2: Invalid Arrow IPC stream (corrupt bytes).
    {
        std::string junk = "corrupt_arrow_ipc_stream_after_valid_message";
        ByteBufferPtr bad_bb = ByteBuffer::allocate_with_tracker(junk.size()).value();
        bad_bb->put_bytes(junk.data(), junk.size());
        bad_bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bad_bb)));
    }

    // Message 3: Valid 1-row Arrow IPC stream with c0_int and c1_str.
    append_good_message({30}, {"gamma"});
    ASSERT_OK(pipe->finish());

    ArrowScanner scanner(state, profile, *broker_scan_range, counter);
    ASSERT_OK(scanner.open());

    auto res1 = scanner.get_next();
    ASSERT_OK(res1.status());
    ASSERT_NE(nullptr, res1.value());
    ASSERT_EQ(2, res1.value()->num_rows());
    EXPECT_EQ(10, res1.value()->columns()[0]->get(0).get_int32());
    EXPECT_EQ("alpha", res1.value()->columns()[1]->get(0).get_slice());
    EXPECT_EQ(20, res1.value()->columns()[0]->get(1).get_int32());
    EXPECT_EQ("beta", res1.value()->columns()[1]->get(1).get_slice());

    auto res2 = scanner.get_next();
    ASSERT_OK(res2.status());
    ASSERT_NE(nullptr, res2.value());
    ASSERT_EQ(1, res2.value()->num_rows());
    EXPECT_EQ(30, res2.value()->columns()[0]->get(0).get_int32());
    EXPECT_EQ("gamma", res2.value()->columns()[1]->get(0).get_slice());

    auto eof_res = scanner.get_next();
    EXPECT_TRUE(eof_res.status().is_end_of_file());

    scanner.close();

    std::ifstream error_file(load_path_mgr.get_load_error_absolute_path(state->get_error_log_file_path()));
    ASSERT_TRUE(error_file.is_open());
    std::stringstream error_buffer;
    error_buffer << error_file.rdbuf();
    const std::string errors = error_buffer.str();
    EXPECT_NE(std::string::npos, errors.find("column = null")) << errors;
    EXPECT_EQ(std::string::npos, errors.find("column = c1_str")) << errors;

    auto* writer = LoadPathStateHelper::rejected_record_writer(state);
    ASSERT_NE(nullptr, writer);
    ASSERT_FALSE(writer->file_path().empty());
    std::ifstream rejected_file(writer->file_path());
    ASSERT_TRUE(rejected_file.is_open());
    std::stringstream rejected_buf;
    rejected_buf << rejected_file.rdbuf();
    const std::string rejected_json = rejected_buf.str();
    EXPECT_NE(std::string::npos, rejected_json.find("\"_raw\"")) << rejected_json;
    EXPECT_NE(std::string::npos, rejected_json.find("\"error_column\":\"\"")) << rejected_json;
    EXPECT_EQ(std::string::npos, rejected_json.find("c1_str")) << rejected_json;
}

TEST_F(ArrowScannerTest, TestStreamZeroBatchSchemaValidationFailure) {
    SlotTypeDescInfoArray slots;
    slots.emplace_back("c1_str", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);
    slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), false);

    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();
    auto* counter = ctx->counter;

    auto append_zero_batch_stream = [&](const std::shared_ptr<arrow::Schema>& schema) {
        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bb)));
    };

    // Message 1: 0-batch Arrow IPC stream (Schema + EOS only) missing non-nullable c0_int.
    append_zero_batch_stream(arrow::schema({arrow::field("c1_str", arrow::utf8())}));

    // Message 2: 0-batch Arrow IPC stream whose c0_int column has incompatible complex type struct<a: int32>.
    append_zero_batch_stream(
            arrow::schema({arrow::field("c0_int", arrow::struct_({arrow::field("a", arrow::int32())}))}));

    // Message 3: 0-batch Arrow IPC stream with valid schema (verifies 0-batch valid stream succeeds and is skipped).
    auto valid_schema = arrow::schema({arrow::field("c1_str", arrow::utf8()), arrow::field("c0_int", arrow::int32())});
    append_zero_batch_stream(valid_schema);

    // Message 4: Valid 2-row Arrow IPC stream.
    {
        arrow::StringBuilder str_builder;
        ASSERT_ARROW_OK(str_builder.AppendValues({"row0", "row1"}));
        std::shared_ptr<arrow::Array> str_array;
        ASSERT_ARROW_OK(str_builder.Finish(&str_array));

        arrow::Int32Builder int_builder;
        ASSERT_ARROW_OK(int_builder.AppendValues({11, 22}));
        std::shared_ptr<arrow::Array> int_array;
        ASSERT_ARROW_OK(int_builder.Finish(&int_array));

        auto batch = arrow::RecordBatch::Make(valid_schema, 2, {str_array, int_array});
        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, valid_schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bb)));
    }
    ASSERT_OK(pipe->finish());

    ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, counter);
    ASSERT_OK(scanner.open());

    auto res = scanner.get_next();
    ASSERT_OK(res.status());
    auto chunk = res.value();
    ASSERT_NE(nullptr, chunk);
    ASSERT_EQ(2, chunk->num_rows());
    EXPECT_EQ("row0", chunk->columns()[0]->get(0).get_slice());
    EXPECT_EQ("row1", chunk->columns()[0]->get(1).get_slice());
    EXPECT_EQ(11, chunk->columns()[1]->get(0).get_int32());
    EXPECT_EQ(22, chunk->columns()[1]->get(1).get_int32());

    auto eof_res = scanner.get_next();
    EXPECT_TRUE(eof_res.status().is_end_of_file());

    EXPECT_EQ(2, counter->arrow_messages_rejected);
    EXPECT_EQ(2, counter->num_rows_filtered);

    scanner.close();
}

TEST_F(ArrowScannerTest, TestStreamMultiBatchRuntimeAppendFailureCountsRemainingBatches) {
    SlotTypeDescInfoArray slots;
    slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);

    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();
    auto* counter = ctx->counter;

    auto dict_type = arrow::dictionary(arrow::int8(), arrow::int32());
    auto dict_schema = arrow::schema({arrow::field("c0_int", dict_type)});

    arrow::Int32Builder dict_builder;
    ASSERT_ARROW_OK(dict_builder.AppendValues({10, 20}));
    std::shared_ptr<arrow::Array> dict_values;
    ASSERT_ARROW_OK(dict_builder.Finish(&dict_values));

    auto make_dict_batch = [&](const std::vector<int8_t>& indices) {
        arrow::Int8Builder idx_builder;
        EXPECT_TRUE(idx_builder.AppendValues(indices).ok());
        std::shared_ptr<arrow::Array> idx_array;
        EXPECT_TRUE(idx_builder.Finish(&idx_array).ok());
        auto dict_array = std::make_shared<arrow::DictionaryArray>(dict_type, idx_array, dict_values);
        return arrow::RecordBatch::Make(dict_schema, static_cast<int64_t>(indices.size()), {dict_array});
    };

    // Message 1: 2-batch discrete Arrow IPC stream.
    // Batch 1 (2 rows) has an out-of-bounds dictionary index (5 >= 2), which ValidateFull() rejects at decode
    // time. The whole message, including the valid Batch 2 (3 rows), is rejected and all 5 rows are counted.
    {
        auto batch1 = make_dict_batch({0, 5});
        auto batch2 = make_dict_batch({0, 1, 0});

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, dict_schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch1));
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch2));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bb)));
    }

    // Message 2: Valid 2-row message.
    {
        auto good_schema = arrow::schema({arrow::field("c0_int", arrow::int32())});
        arrow::Int32Builder builder;
        ASSERT_ARROW_OK(builder.AppendValues({77, 88}));
        std::shared_ptr<arrow::Array> array;
        ASSERT_ARROW_OK(builder.Finish(&array));
        auto valid_batch = arrow::RecordBatch::Make(good_schema, 2, {array});

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, good_schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*valid_batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bb)));
    }
    ASSERT_OK(pipe->finish());

    ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, counter);
    ASSERT_OK(scanner.open());

    auto res = scanner.get_next();
    ASSERT_OK(res.status());
    auto chunk = res.value();
    ASSERT_NE(nullptr, chunk);
    ASSERT_EQ(2, chunk->num_rows());
    EXPECT_EQ(77, chunk->columns()[0]->get(0).get_int32());
    EXPECT_EQ(88, chunk->columns()[0]->get(1).get_int32());

    auto eof_res = scanner.get_next();
    EXPECT_TRUE(eof_res.status().is_end_of_file());

    EXPECT_EQ(5, counter->num_rows_filtered);
    EXPECT_EQ(1, counter->arrow_messages_rejected);

    scanner.close();
}

TEST_F(ArrowScannerTest, TestArrowBoundedMemoryPoolOperationsInScannerSuite) {
    ArrowBoundedMemoryPool pool(1024);
    EXPECT_EQ("starrocks_bounded_arrow_pool", pool.backend_name());
    EXPECT_EQ(0, pool.bytes_allocated());
    EXPECT_EQ(0, pool.max_memory());
    EXPECT_EQ(0, pool.total_bytes_allocated());
    EXPECT_EQ(0, pool.num_allocations());

    uint8_t* buf = nullptr;
    EXPECT_TRUE(pool.Allocate(-1, &buf).IsInvalid());

    ASSERT_ARROW_OK(pool.Allocate(256, &buf));
    ASSERT_NE(nullptr, buf);
    EXPECT_EQ(256, pool.bytes_allocated());
    EXPECT_EQ(256, pool.max_memory());
    EXPECT_EQ(256, pool.total_bytes_allocated());
    EXPECT_EQ(1, pool.num_allocations());

    uint8_t* over_buf = nullptr;
    EXPECT_TRUE(pool.Allocate(1024, &over_buf).IsCapacityError());
    EXPECT_EQ(256, pool.bytes_allocated());

    EXPECT_TRUE(pool.Reallocate(256, -1, &buf).IsInvalid());
    ASSERT_ARROW_OK(pool.Reallocate(256, 512, &buf));
    EXPECT_EQ(512, pool.bytes_allocated());
    EXPECT_EQ(512, pool.max_memory());
    EXPECT_EQ(512, pool.total_bytes_allocated());

    ASSERT_ARROW_OK(pool.Reallocate(512, 128, &buf));
    EXPECT_EQ(128, pool.bytes_allocated());
    EXPECT_EQ(512, pool.max_memory());
    EXPECT_EQ(512, pool.total_bytes_allocated());

    EXPECT_TRUE(pool.Reallocate(128, 2048, &buf).IsCapacityError());
    EXPECT_EQ(128, pool.bytes_allocated());

    pool.Free(buf, 128);
    EXPECT_EQ(0, pool.bytes_allocated());

    ArrowBoundedMemoryPool inner_pool(16);
    ArrowBoundedMemoryPool delegate_fail_pool(1024, &inner_pool);
    uint8_t* fail_buf = nullptr;
    EXPECT_TRUE(delegate_fail_pool.Allocate(64, &fail_buf).IsCapacityError());
    EXPECT_EQ(0, delegate_fail_pool.bytes_allocated());
    EXPECT_TRUE(delegate_fail_pool.Reallocate(0, 64, &fail_buf).IsCapacityError());
    EXPECT_EQ(0, delegate_fail_pool.bytes_allocated());

    // Also exercise batch_count metadata overhead cap in decode_discrete_message() when uncompressed batches
    // exceed routine_load_arrow_max_decoded_bytes_per_message via batch_count * 1024.
    const int64_t old_limit = config::routine_load_arrow_max_decoded_bytes_per_message;
    config::routine_load_arrow_max_decoded_bytes_per_message = 2048;
    DeferOp restore_limit([&]() { config::routine_load_arrow_max_decoded_bytes_per_message = old_limit; });

    SlotTypeDescInfoArray slots;
    slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();

    auto schema = arrow::schema({arrow::field("c0_int", arrow::int32())});
    {
        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, schema).ValueOrDie();
        for (int b = 0; b < 3; ++b) {
            arrow::Int32Builder builder;
            ASSERT_ARROW_OK(builder.Append(b + 1));
            std::shared_ptr<arrow::Array> array;
            ASSERT_ARROW_OK(builder.Finish(&array));
            auto batch = arrow::RecordBatch::Make(schema, 1, {array});
            ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        }
        ASSERT_ARROW_OK(writer->Close());
        auto ipc_buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(ipc_buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(ipc_buf->data()), ipc_buf->size());
        bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bb)));
    }
    ASSERT_OK(pipe->finish());

    ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
    ASSERT_OK(scanner.open());
    auto eof_res = scanner.get_next();
    EXPECT_TRUE(eof_res.status().is_end_of_file());
    EXPECT_EQ(1, ctx->counter->arrow_messages_rejected);
    EXPECT_EQ(3, ctx->counter->num_rows_filtered);
    scanner.close();
}

TEST_F(ArrowScannerTest, TestRealKafkaConsumerPipeAndConverterNullGuards) {
    EXPECT_TRUE(validate_arrow_column_type(nullptr, TypeDescriptor::from_logical_type(TYPE_INT), "c0")
                        .is_invalid_argument());
    EXPECT_TRUE(
            validate_arrow_column_type(arrow::int32().get(), TypeDescriptor::from_logical_type(TYPE_INT), "c0").ok());

    SlotTypeDescInfoArray slots;
    slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);

    {
        auto unique_pipe = std::make_unique<DiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
        EXPECT_TRUE(unique_pipe->is_discrete_message_pipe());
    }
    auto load_id = UniqueId::gen_uid();
    std::shared_ptr<StreamLoadPipe> pipe = std::make_shared<DiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    EXPECT_TRUE(pipe->is_discrete_message_pipe());
    auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();

    auto schema = arrow::schema({arrow::field("c0_int", arrow::int32())});
    {
        arrow::Int32Builder builder;
        ASSERT_ARROW_OK(builder.AppendValues({101, 202}));
        std::shared_ptr<arrow::Array> array;
        ASSERT_ARROW_OK(builder.Finish(&array));
        auto batch = arrow::RecordBatch::Make(schema, 2, {array});

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bb)));
    }
    ASSERT_OK(pipe->finish());

    ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
    ASSERT_OK(scanner.open());

    auto res = scanner.get_next();
    ASSERT_OK(res.status());
    ASSERT_NE(nullptr, res.value());
    ASSERT_EQ(2, res.value()->num_rows());
    EXPECT_EQ(101, res.value()->columns()[0]->get(0).get_int32());
    EXPECT_EQ(202, res.value()->columns()[0]->get(1).get_int32());

    auto eof_res = scanner.get_next();
    EXPECT_TRUE(eof_res.status().is_end_of_file());
    scanner.close();
}

TEST_F(ArrowScannerTest, TestNonDiscreteStreamErrorAndEofPaths) {
    auto schema = arrow::schema({arrow::field("c0_int", arrow::int32())});

    // Subcase A: Non-discrete StreamLoadPipe with valid schema followed by junk batch bytes.
    {
        SlotTypeDescInfoArray slots;
        slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
        auto load_id = UniqueId::gen_uid();
        auto pipe = std::make_shared<StreamLoadPipe>(1024 * 1024, 64 * 1024);
        auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
        ASSERT_OK(ctx_res.status());
        auto& ctx = ctx_res.value();

        auto buf = make_schema_plus_junk_ipc_stream(schema);
        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf.size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf.data()), buf.size());
        bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bb)));
        ASSERT_OK(pipe->finish());

        ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
        ASSERT_OK(scanner.open());
        auto res = scanner.get_next();
        EXPECT_TRUE(res.status().is_internal_error());
        // Calling next_batch() when _curr_file_reader was reset to nullptr hits lines 375-376.
        auto next_st = scanner.next_batch();
        EXPECT_TRUE(next_st.is_end_of_file());
        scanner.close();
    }

    // Subcase B: Non-discrete StreamLoadPipe with day_time_interval targeting TYPE_INT fails in initialize_src_chunk.
    {
        SlotTypeDescInfoArray slots;
        slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
        auto load_id = UniqueId::gen_uid();
        auto pipe = std::make_shared<StreamLoadPipe>(1024 * 1024, 64 * 1024);
        auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
        ASSERT_OK(ctx_res.status());
        auto& ctx = ctx_res.value();

        arrow::DayTimeIntervalBuilder interval_builder;
        ASSERT_ARROW_OK(interval_builder.Append(arrow::DayTimeIntervalType::DayMilliseconds{1, 100}));
        std::shared_ptr<arrow::Array> interval_array;
        ASSERT_ARROW_OK(interval_builder.Finish(&interval_array));
        auto bad_schema = arrow::schema({arrow::field("c0_int", arrow::day_time_interval())});
        auto bad_batch = arrow::RecordBatch::Make(bad_schema, 1, {interval_array});

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, bad_schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*bad_batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bb)));
        ASSERT_OK(pipe->finish());

        ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
        ASSERT_OK(scanner.open());
        auto res = scanner.get_next();
        EXPECT_FALSE(res.status().ok());
        EXPECT_FALSE(res.status().is_end_of_file());
        scanner.close();
    }

    // Subcase C: Non-discrete StreamLoadPipe missing non-nullable c0_int fails in append_batch_to_src_chunk (lines 648-649, 750).
    {
        SlotTypeDescInfoArray slots;
        slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), false);
        slots.emplace_back("c1_str", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);
        auto load_id = UniqueId::gen_uid();
        auto pipe = std::make_shared<StreamLoadPipe>(1024 * 1024, 64 * 1024);
        auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
        ASSERT_OK(ctx_res.status());
        auto& ctx = ctx_res.value();

        arrow::StringBuilder str_builder;
        ASSERT_ARROW_OK(str_builder.AppendValues({"only_str"}));
        std::shared_ptr<arrow::Array> str_array;
        ASSERT_ARROW_OK(str_builder.Finish(&str_array));
        auto str_schema = arrow::schema({arrow::field("c1_str", arrow::utf8())});
        auto batch = arrow::RecordBatch::Make(str_schema, 1, {str_array});

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, str_schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bb)));
        ASSERT_OK(pipe->finish());

        ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
        ASSERT_OK(scanner.open());
        auto res = scanner.get_next();
        EXPECT_TRUE(res.status().is_invalid_argument());
        scanner.close();
    }

    // Subcase D: Non-discrete StreamLoadPipe with valid Batch 1 followed by junk Batch 2 fails in inner loop (line 777).
    {
        SlotTypeDescInfoArray slots;
        slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
        auto load_id = UniqueId::gen_uid();
        auto pipe = std::make_shared<StreamLoadPipe>(1024 * 1024, 64 * 1024);
        auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
        ASSERT_OK(ctx_res.status());
        auto& ctx = ctx_res.value();

        auto buf = make_batch_plus_junk_ipc_stream(schema, {10, 20});
        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf.size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf.data()), buf.size());
        bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bb)));
        ASSERT_OK(pipe->finish());

        ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
        ASSERT_OK(scanner.open());
        auto res = scanner.get_next();
        EXPECT_TRUE(res.status().is_internal_error());
        scanner.close();
    }
}

TEST_F(ArrowScannerTest, TestNonDiscreteMultiRangeTransitionAndAllRowsFiltered) {
    // Subcase A: Multi-range transition where Range 1 has a 0-row RecordBatch, Range 2 hits EOF on an empty discrete
    // pipe while chunk->num_rows() == 0, and Range 3 has an unsupported day_time_interval column targeting TYPE_INT
    // (covering lines 785, 786, 787, 793).
    {
        std::string file1_path = (_tmp_root_dir / "transition_zero_row.arrow").string();
        std::string file3_path = (_tmp_root_dir / "transition_bad_interval.arrow").string();

        auto good_schema = arrow::schema({arrow::field("c0_int", arrow::int32())});
        {
            arrow::Int32Builder empty_builder;
            std::shared_ptr<arrow::Array> empty_array;
            ASSERT_ARROW_OK(empty_builder.Finish(&empty_array));
            auto zero_batch = arrow::RecordBatch::Make(good_schema, 0, {empty_array});
            auto out_file = arrow::io::FileOutputStream::Open(file1_path).ValueOrDie();
            auto writer = arrow::ipc::MakeStreamWriter(out_file, good_schema).ValueOrDie();
            ASSERT_ARROW_OK(writer->WriteRecordBatch(*zero_batch));
            ASSERT_ARROW_OK(writer->Close());
            ASSERT_ARROW_OK(out_file->Close());
        }
        {
            arrow::DayTimeIntervalBuilder interval_builder;
            ASSERT_ARROW_OK(interval_builder.Append(arrow::DayTimeIntervalType::DayMilliseconds{2, 500}));
            std::shared_ptr<arrow::Array> interval_array;
            ASSERT_ARROW_OK(interval_builder.Finish(&interval_array));
            auto bad_schema = arrow::schema({arrow::field("c0_int", arrow::day_time_interval())});
            auto bad_batch = arrow::RecordBatch::Make(bad_schema, 1, {interval_array});
            auto out_file = arrow::io::FileOutputStream::Open(file3_path).ValueOrDie();
            auto writer = arrow::ipc::MakeStreamWriter(out_file, bad_schema).ValueOrDie();
            ASSERT_ARROW_OK(writer->WriteRecordBatch(*bad_batch));
            ASSERT_ARROW_OK(writer->Close());
            ASSERT_ARROW_OK(out_file->Close());
        }

        SlotTypeDescInfoArray slots;
        slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
        auto load_id = UniqueId::gen_uid();
        auto empty_pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
        ASSERT_OK(empty_pipe->finish());
        auto ctx_res = make_stream_scanner_context(slots, load_id, empty_pipe);
        ASSERT_OK(ctx_res.status());
        auto& ctx = ctx_res.value();

        TBrokerRangeDesc range1;
        range1.__set_path(file1_path);
        range1.start_offset = 0;
        range1.size = LONG_MAX;
        range1.file_type = TFileType::FILE_LOCAL;
        range1.__set_format_type(TFileFormatType::FORMAT_ARROW);

        TBrokerRangeDesc range2 = ctx->broker_scan_range.ranges[0];

        TBrokerRangeDesc range3;
        range3.__set_path(file3_path);
        range3.start_offset = 0;
        range3.size = LONG_MAX;
        range3.file_type = TFileType::FILE_LOCAL;
        range3.__set_format_type(TFileFormatType::FORMAT_ARROW);

        ctx->broker_scan_range.ranges = {range1, range2, range3};

        ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
        ASSERT_OK(scanner.open());
        auto res = scanner.get_next();
        EXPECT_FALSE(res.status().ok());
        EXPECT_FALSE(res.status().is_end_of_file());
        scanner.close();
    }

    // Subcase B & C: Strict-mode CHAR(2) filtering where Batch 1 (2 rows) fills chunk (batch_size = 2) and is
    // completely filtered out while !_scanner_eof (covering line 804 continue), and Batch 2 (1 row) hits EOF and is
    // completely filtered out while _scanner_eof == true (covering lines 801-802).
    {
        SlotTypeDescInfoArray slots;
        slots.emplace_back("c0_str", TypeDescriptor::create_char_type(2), true);
        auto load_id = UniqueId::gen_uid();
        auto pipe = std::make_shared<StreamLoadPipe>(1024 * 1024, 64 * 1024);
        auto ctx_res = make_stream_scanner_context(slots, load_id, pipe, /*batch_size=*/2);
        ASSERT_OK(ctx_res.status());
        auto& ctx = ctx_res.value();

        auto str_schema = arrow::schema({arrow::field("c0_str", arrow::utf8())});
        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, str_schema).ValueOrDie();

        // Batch 1: 2 rows, both exceed CHAR(2) in strict mode -> chunk_is_full() breaks while !_scanner_eof -> line 804
        {
            arrow::StringBuilder builder;
            ASSERT_ARROW_OK(builder.AppendValues({"too_long_1", "too_long_2"}));
            std::shared_ptr<arrow::Array> array;
            ASSERT_ARROW_OK(builder.Finish(&array));
            auto batch1 = arrow::RecordBatch::Make(str_schema, 2, {array});
            ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch1));
        }
        // Batch 2: 1 row, exceeds CHAR(2) in strict mode -> next_batch() hits EOF (_scanner_eof = true) -> lines 801-802
        {
            arrow::StringBuilder builder;
            ASSERT_ARROW_OK(builder.AppendValues({"too_long_3"}));
            std::shared_ptr<arrow::Array> array;
            ASSERT_ARROW_OK(builder.Finish(&array));
            auto batch2 = arrow::RecordBatch::Make(str_schema, 1, {array});
            ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch2));
        }
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bb)));
        ASSERT_OK(pipe->finish());

        ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
        ASSERT_OK(scanner.open());
        auto res = scanner.get_next();
        EXPECT_TRUE(res.status().is_end_of_file());
        EXPECT_EQ(3, ctx->counter->num_rows_filtered);

        // Call get_next() one extra time after _scanner_eof is already true.
        auto res_after_eof = scanner.get_next();
        EXPECT_TRUE(res_after_eof.status().is_end_of_file());
        scanner.close();
    }
}

struct EmptyOnceDiscretePipe : public MockDiscreteStreamLoadPipe {
    using MockDiscreteStreamLoadPipe::MockDiscreteStreamLoadPipe;
    StatusOr<ByteBufferPtr> read() override {
        if (!returned_empty) {
            returned_empty = true;
            return ByteBuffer::allocate_with_tracker(0).value();
        }
        return MockDiscreteStreamLoadPipe::read();
    }
    bool returned_empty = false;
};

TEST_F(ArrowScannerTest, TestDiscreteHelperDefensiveBranches) {
    // Subcase A: 2-batch discrete stream where Batch 1 has a schema mismatch (missing non-nullable c0_int)
    // and Batch 2 fails ReadNext (truncated tail), hitting line 503 (!schema_error.empty() + read_error),
    // plus a message whose RecordBatch BodyCompression codec is corrupted so ReadRecordBatch in
    // count_ipc_stream_declared_rows fails and hits line 137.
    {
        SlotTypeDescInfoArray slots;
        slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), false);
        slots.emplace_back("c1_str", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);

        auto load_id = UniqueId::gen_uid();
        auto pipe = std::make_shared<EmptyOnceDiscretePipe>(1024 * 1024, 64 * 1024);
        auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
        ASSERT_OK(ctx_res.status());
        auto& ctx = ctx_res.value();

        // Message 1: Batch 1 missing c0_int, followed by junk bytes for Batch 2 -> hits line 503.
        {
            auto str_schema = arrow::schema({arrow::field("c1_str", arrow::utf8())});
            arrow::StringBuilder str_builder;
            ASSERT_ARROW_OK(str_builder.AppendValues({"r1", "r2"}));
            std::shared_ptr<arrow::Array> str_array;
            ASSERT_ARROW_OK(str_builder.Finish(&str_array));
            auto batch1 = arrow::RecordBatch::Make(str_schema, 2, {str_array});

            auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
            auto writer = arrow::ipc::MakeStreamWriter(stream, str_schema).ValueOrDie();
            ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch1));
            ASSERT_ARROW_OK(writer->Close());
            auto buf = stream->Finish().ValueOrDie();

            constexpr size_t kEosSize = 8;
            ASSERT_GT(buf->size(), kEosSize);
            std::vector<uint8_t> corrupt(buf->data(), buf->data() + buf->size() - kEosSize);
            for (int i = 0; i < 32; ++i) {
                corrupt.push_back(static_cast<uint8_t>(0xDE + i));
            }

            ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(corrupt.size()).value();
            bb->put_bytes(reinterpret_cast<const char*>(corrupt.data()), corrupt.size());
            bb->flip_to_read();
            ASSERT_OK(pipe->append(std::move(bb)));
        }

        // Message 2: Valid outer Message Flatbuffer for RECORD_BATCH with ZSTD BodyCompression, then corrupt the
        // BodyCompression codec byte in the RecordBatch Flatbuffer table so ReadRecordBatch inside
        // count_ipc_stream_declared_rows(empty_schema) fails before inspecting fields and hits line 137.
        {
            auto good_schema =
                    arrow::schema({arrow::field("c0_int", arrow::int32()), arrow::field("c1_str", arrow::utf8())});
            auto schema_only = make_schema_only_ipc_stream(good_schema);
            const size_t rb_offset = schema_only.size() - 8;

            arrow::Int32Builder int_builder;
            arrow::StringBuilder str_builder;
            for (int i = 0; i < 4; ++i) {
                ASSERT_ARROW_OK(int_builder.Append(i + 1));
                ASSERT_ARROW_OK(str_builder.Append("v"));
            }
            std::shared_ptr<arrow::Array> int_array, str_array;
            ASSERT_ARROW_OK(int_builder.Finish(&int_array));
            ASSERT_ARROW_OK(str_builder.Finish(&str_array));
            auto batch = arrow::RecordBatch::Make(good_schema, 4, {int_array, str_array});

            arrow::ipc::IpcWriteOptions write_options = arrow::ipc::IpcWriteOptions::Defaults();
            write_options.codec = arrow::util::Codec::Create(arrow::Compression::ZSTD).ValueOrDie();

            auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
            auto writer = arrow::ipc::MakeStreamWriter(stream, good_schema, write_options).ValueOrDie();
            ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
            ASSERT_ARROW_OK(writer->Close());
            auto buf = stream->Finish().ValueOrDie();

            std::vector<uint8_t> bytes(buf->data(), buf->data() + buf->size());
            const uint8_t* msg_buf = bytes.data() + rb_offset + 8;
            const uint8_t* msg_root = msg_buf + *reinterpret_cast<const uint32_t*>(msg_buf);
            const uint8_t* msg_vtable = msg_root - *reinterpret_cast<const int32_t*>(msg_root);
            uint16_t header_field_off = *reinterpret_cast<const uint16_t*>(msg_vtable + 8);
            const uint8_t* rb_uoffset_ptr = msg_root + header_field_off;
            const uint8_t* rb_root = rb_uoffset_ptr + *reinterpret_cast<const uint32_t*>(rb_uoffset_ptr);
            const uint8_t* rb_vtable = rb_root - *reinterpret_cast<const int32_t*>(rb_root);
            uint16_t comp_field_off = *reinterpret_cast<const uint16_t*>(rb_vtable + 10);
            ASSERT_NE(0, comp_field_off);
            const uint8_t* comp_uoffset_ptr = rb_root + comp_field_off;
            uint8_t* comp_root =
                    const_cast<uint8_t*>(comp_uoffset_ptr) + *reinterpret_cast<const uint32_t*>(comp_uoffset_ptr);
            const uint8_t* comp_vtable = comp_root - *reinterpret_cast<const int32_t*>(comp_root);
            uint16_t codec_field_off = *reinterpret_cast<const uint16_t*>(comp_vtable + 4);
            ASSERT_NE(0, codec_field_off);
            ASSERT_EQ(1, comp_root[codec_field_off]);
            comp_root[codec_field_off] = 0x7F;

            ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(bytes.size()).value();
            bb->put_bytes(reinterpret_cast<const char*>(bytes.data()), bytes.size());
            bb->flip_to_read();
            ASSERT_OK(pipe->append(std::move(bb)));
        }

        ASSERT_OK(pipe->finish());

        ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
        ASSERT_OK(scanner.open());
        auto eof_res = scanner.get_next();
        EXPECT_TRUE(eof_res.status().is_end_of_file());
        EXPECT_TRUE(pipe->returned_empty);
        EXPECT_EQ(2, ctx->counter->arrow_messages_rejected);
        scanner.close();
    }

    // Subcase B: Direct calls via -fno-access-control on an opened ArrowScanner.
    {
        SlotTypeDescInfoArray slots;
        slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);

        auto load_id = UniqueId::gen_uid();
        auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
        auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
        ASSERT_OK(ctx_res.status());
        auto& ctx = ctx_res.value();

        auto schema = arrow::schema({arrow::field("c0_int", arrow::int32())});
        arrow::Int32Builder builder;
        ASSERT_ARROW_OK(builder.AppendValues({55, 66}));
        std::shared_ptr<arrow::Array> array;
        ASSERT_ARROW_OK(builder.Finish(&array));
        auto valid_batch = arrow::RecordBatch::Make(schema, 2, {array});

        {
            auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
            auto writer = arrow::ipc::MakeStreamWriter(stream, schema).ValueOrDie();
            ASSERT_ARROW_OK(writer->WriteRecordBatch(*valid_batch));
            ASSERT_ARROW_OK(writer->Close());
            auto buf = stream->Finish().ValueOrDie();

            ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
            bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
            bb->flip_to_read();
            ASSERT_OK(pipe->append(std::move(bb)));
        }
        ASSERT_OK(pipe->finish());

        ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
        ASSERT_OK(scanner.open());

        // 1. validate_ipc_schema(nullptr) -> line 434.
        EXPECT_TRUE(scanner.validate_ipc_schema(nullptr).is_invalid_argument());

        // 2. Append a nullptr slot descriptor to exercise nullptr slot guards in validate_ipc_schema (440),
        // validate_batch_schema (412), initialize_src_chunk (616), append_batch_to_src_chunk (641),
        // and finalize_src_chunk (685).
        scanner._src_slot_descriptors.push_back(nullptr);
        scanner._conv_funcs.emplace_back(std::make_unique<ConvertFuncTree>());
        scanner._cast_exprs.push_back(nullptr);
        scanner._num_of_columns_from_file = static_cast<int>(scanner._src_slot_descriptors.size());

        EXPECT_OK(scanner.validate_ipc_schema(schema));
        EXPECT_OK(scanner.validate_batch_schema(valid_batch));

        // 3. Exercise lazy _conv_funcs rebuild (lines 653-659) in append_batch_to_src_chunk.
        ASSERT_OK(scanner.open_next_reader());
        scanner._batch = valid_batch;
        scanner._batch_start_idx = 0;
        scanner._chunk_start_idx = 0;
        ChunkPtr manual_chunk;
        ASSERT_OK(scanner.initialize_src_chunk(&manual_chunk));
        scanner._conv_funcs[0]->func = nullptr;
        ASSERT_OK(scanner.append_batch_to_src_chunk(&manual_chunk));
        ASSERT_OK(scanner.finalize_src_chunk(&manual_chunk));
        ASSERT_NE(nullptr, manual_chunk);
        EXPECT_EQ(2, manual_chunk->num_rows());

        // Also exercise error return in lazy _conv_funcs rebuild (line 658) when batch column has unsupported type.
        arrow::DayTimeIntervalBuilder interval_b;
        ASSERT_ARROW_OK(interval_b.Append(arrow::DayTimeIntervalType::DayMilliseconds{1, 0}));
        std::shared_ptr<arrow::Array> interval_arr;
        ASSERT_ARROW_OK(interval_b.Finish(&interval_arr));
        auto bad_schema = arrow::schema({arrow::field("c0_int", arrow::day_time_interval())});
        auto bad_batch = arrow::RecordBatch::Make(bad_schema, 1, {interval_arr});

        scanner._batch = valid_batch;
        scanner._batch_start_idx = 0;
        scanner._chunk_start_idx = 0;
        ChunkPtr err_chunk;
        ASSERT_OK(scanner.initialize_src_chunk(&err_chunk));
        scanner._batch = bad_batch;
        scanner._conv_funcs[0]->func = nullptr;
        EXPECT_FALSE(scanner.append_batch_to_src_chunk(&err_chunk).ok());

        // 4. With the manually opened discrete reader and stale per-batch state cleared, get_next() reads and
        // converts the valid message from the pipe.
        scanner._batch.reset();
        auto res = scanner.get_next();
        ASSERT_OK(res.status());
        ASSERT_NE(nullptr, res.value());
        EXPECT_EQ(2, res.value()->num_rows());
        EXPECT_EQ(55, res.value()->columns()[0]->get(0).get_int32());
        EXPECT_EQ(66, res.value()->columns()[0]->get(1).get_int32());

        auto eof_res = scanner.get_next();
        EXPECT_TRUE(eof_res.status().is_end_of_file());
        scanner.close();
    }
}

TEST_F(ArrowScannerTest, TestArrowInputStreamAndFileMetadataBranches) {
    // Subcase A: Empty scan ranges in ArrowScanner::open() (covers line 224).
    {
        SlotTypeDescInfoArray slots;
        slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
        auto load_id = UniqueId::gen_uid();
        auto pipe = std::make_shared<StreamLoadPipe>(1024 * 1024, 64 * 1024);
        auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
        ASSERT_OK(ctx_res.status());
        auto& ctx = ctx_res.value();
        ctx->broker_scan_range.ranges.clear();

        ArrowScanner empty_scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
        ASSERT_OK(empty_scanner.open());
        empty_scanner.close();
    }

    // Subcase B: Cancelled non-discrete StreamLoadPipe causes _file->read() in StarRocksArrowInputStream::Read()
    // to return non-OK status (covers line 66).
    {
        SlotTypeDescInfoArray slots;
        slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
        auto load_id = UniqueId::gen_uid();
        auto pipe = std::make_shared<StreamLoadPipe>(1024 * 1024, 64 * 1024);
        auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
        ASSERT_OK(ctx_res.status());
        auto& ctx = ctx_res.value();

        pipe->cancel(Status::InternalError("cancelled stream"));

        ArrowScanner cancelled_scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
        ASSERT_OK(cancelled_scanner.open());
        auto res = cancelled_scanner.get_next();
        EXPECT_TRUE(res.status().is_internal_error());
        cancelled_scanner.close();
    }

    // Subcase C: Scan with modification_time set (line 206), broker_addresses non-empty (line 260),
    // VLOG_ROW enabled (lines 699, 703), and direct exercise of StarRocksArrowInputStream Tell(), Close(),
    // closed(), and Read() when closed (lines 62, 85-97).
    {
        SlotTypeDescInfoArray slots;
        slots.emplace_back("c0_str", TypeDescriptor::create_char_type(2), true);

        auto load_id = UniqueId::gen_uid();
        auto pipe = std::make_shared<StreamLoadPipe>(1024 * 1024, 64 * 1024);
        LoadStreamMgr load_stream_mgr;
        ASSERT_OK(load_stream_mgr.put(load_id, pipe));
        DeferOp remove_pipe([&]() { load_stream_mgr.remove(load_id); });

        TBrokerRangeDesc range;
        range.format_type = TFileFormatType::FORMAT_ARROW;
        range.file_type = TFileType::FILE_STREAM;
        range.__set_load_id(load_id.to_thrift());
        range.__set_path("stream_with_mtime.arrow");
        range.__set_size(4096);
        range.__set_modification_time(1700000000000L);

        TQueryOptions query_options;
        query_options.query_type = TQueryType::LOAD;
        query_options.__set_log_rejected_record_num(10);
        TQueryGlobals query_globals;
        query_globals.time_zone = "UTC";
        LoadPathMgr load_path_mgr({(_tmp_root_dir / "mtime_rejected_load_path").string()});
        ASSERT_OK(load_path_mgr.init());
        RuntimeServices runtime_services;
        runtime_services.load_path_mgr = &load_path_mgr;
        runtime_services.load_stream_mgr = &load_stream_mgr;
        QueryExecutionServices qes;
        qes.runtime = &runtime_services;

        RuntimeState* state = _obj_pool.add(new RuntimeState(TUniqueId(), query_options, query_globals, &qes, nullptr));
        DescriptorTbl* desc_tbl = DescTblHelper::generate_desc_tbl(state, _obj_pool, {slots, slots});
        state->set_desc_tbl(desc_tbl);
        state->init_instance_mem_tracker();
        state->set_db("test_db");
        state->set_load_label("test_mtime_label");
        state->set_txn_id(98765);

        TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
        params->strict_mode = true;
        params->__isset.strict_mode = true;
        std::vector<TupleDescriptor*> tuples;
        desc_tbl->get_tuple_descs(&tuples);
        params->src_tuple_id = 0;
        params->dest_tuple_id = static_cast<int>(tuples.size()) - 1;
        const auto* src_tuple = desc_tbl->get_tuple_descriptor(params->src_tuple_id);
        const auto* dst_tuple = desc_tbl->get_tuple_descriptor(params->dest_tuple_id);
        for (int i = 0; i < static_cast<int>(src_tuple->slots().size()); i++) {
            auto& src_slot = src_tuple->slots()[i];
            auto& dst_slot = dst_tuple->slots()[i];
            params->expr_of_dest_slot[dst_slot->id()] =
                    create_column_ref(src_slot->id(), src_slot->type(), src_slot->is_nullable());
            params->dest_sid_to_src_sid_without_trans[dst_slot->id()] = src_slot->id();
            params->src_slot_ids.emplace_back(i);
        }
        params->__isset.dest_sid_to_src_sid_without_trans = true;

        TBrokerScanRange broker_scan_range;
        broker_scan_range.params = *params;
        broker_scan_range.ranges.push_back(range);
        TNetworkAddress broker_addr;
        broker_addr.hostname = "127.0.0.1";
        broker_addr.port = 8000;
        broker_scan_range.broker_addresses.push_back(broker_addr);

        auto schema = arrow::schema({arrow::field("c0_str", arrow::utf8())});
        arrow::StringBuilder builder;
        ASSERT_ARROW_OK(builder.AppendValues({"ok", "too_long_value"}));
        std::shared_ptr<arrow::Array> array;
        ASSERT_ARROW_OK(builder.Finish(&array));
        auto batch = arrow::RecordBatch::Make(schema, 2, {array});

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bb)));
        ASSERT_OK(pipe->finish());

        RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("mtime_prof", true));
        ScannerCounter* counter = _obj_pool.add(new ScannerCounter());
        ArrowScanner scanner(state, profile, broker_scan_range, counter);
        ASSERT_OK(scanner.open());
        ASSERT_OK(scanner.open_next_reader());
        ASSERT_NE(nullptr, scanner._curr_file_reader);

        // Extract the owned arrow::io::InputStream (StarRocksArrowInputStream) from InputStreamMessageReader
        // inside RecordBatchStreamReaderImpl to exercise Tell(), Close(), closed(), and Read() after Close().
        auto* msg_reader = *reinterpret_cast<arrow::ipc::MessageReader* const*>(
                reinterpret_cast<const char*>(scanner._curr_file_reader.get()) + 0x108);
        ASSERT_NE(nullptr, msg_reader);
        auto owned_arrow_stream = *reinterpret_cast<const std::shared_ptr<arrow::io::InputStream>*>(
                reinterpret_cast<const char*>(msg_reader) + 0x18);
        ASSERT_NE(nullptr, owned_arrow_stream);
        EXPECT_FALSE(owned_arrow_stream->closed());
        auto tell_open = owned_arrow_stream->Tell();
        ASSERT_ARROW_OK(tell_open.status());
        EXPECT_GT(tell_open.ValueOrDie(), 0);

        const int prev_vlog = FLAGS_v;
        FLAGS_v = 10;
        DeferOp restore_vlog([&]() { FLAGS_v = prev_vlog; });

        auto chunk_res = scanner.get_next();
        ASSERT_OK(chunk_res.status());
        ASSERT_NE(nullptr, chunk_res.value());
        EXPECT_EQ(1, chunk_res.value()->num_rows());
        EXPECT_EQ("ok", chunk_res.value()->columns()[0]->get(0).get_slice());
        EXPECT_EQ(1, counter->num_rows_filtered);

        // Now close the StarRocksArrowInputStream and verify Tell() and Read() on a closed stream.
        ASSERT_ARROW_OK(owned_arrow_stream->Close());
        EXPECT_TRUE(owned_arrow_stream->closed());
        auto tell_closed = owned_arrow_stream->Tell();
        EXPECT_TRUE(tell_closed.status().IsInvalid());
        uint8_t dummy_byte = 0;
        auto read_closed = owned_arrow_stream->Read(1, &dummy_byte);
        EXPECT_TRUE(read_closed.status().IsInvalid());

        scanner.close();
    }
}

TEST_F(ArrowScannerTest, TestTruncatedBodyRowCountAndDictionaryBombAndProfileCounters) {
    // Subcase A: Truncated RECORD_BATCH body preserves declared rows from intact metadata header ([P1] 4305383608)
    // and publishes Arrow scanner counters on RuntimeProfile ([P2] 4305383612).
    {
        SlotTypeDescInfoArray slots;
        slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);

        auto load_id = UniqueId::gen_uid();
        auto pipe = std::make_shared<DiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
        auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
        ASSERT_OK(ctx_res.status());
        auto& ctx = ctx_res.value();

        auto schema = arrow::schema({arrow::field("c0_int", arrow::int32())});

        // Message 1: 1-batch Arrow IPC stream with 4096 rows, truncated by 64 bytes at the tail so the Schema
        // message and RECORD_BATCH Flatbuffer metadata header are intact, but the RECORD_BATCH body is truncated.
        {
            arrow::Int32Builder builder;
            for (int32_t i = 0; i < 4096; ++i) {
                ASSERT_ARROW_OK(builder.Append(i + 1));
            }
            std::shared_ptr<arrow::Array> array;
            ASSERT_ARROW_OK(builder.Finish(&array));
            auto batch = arrow::RecordBatch::Make(schema, 4096, {array});

            auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
            auto writer = arrow::ipc::MakeStreamWriter(stream, schema).ValueOrDie();
            ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
            ASSERT_ARROW_OK(writer->Close());
            auto buf = stream->Finish().ValueOrDie();
            ASSERT_GT(buf->size(), 128);

            const int64_t truncated_size = buf->size() - 64;
            ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(truncated_size).value();
            bb->put_bytes(reinterpret_cast<const char*>(buf->data()), truncated_size);
            bb->flip_to_read();
            ASSERT_OK(pipe->append(std::move(bb)));
        }

        // Message 2: Valid 2-row message.
        {
            arrow::Int32Builder builder;
            ASSERT_ARROW_OK(builder.AppendValues({101, 202}));
            std::shared_ptr<arrow::Array> array;
            ASSERT_ARROW_OK(builder.Finish(&array));
            auto batch = arrow::RecordBatch::Make(schema, 2, {array});

            auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
            arrow::ipc::IpcWriteOptions write_options = arrow::ipc::IpcWriteOptions::Defaults();
            write_options.codec = arrow::util::Codec::Create(arrow::Compression::ZSTD).ValueOrDie();
            auto writer = arrow::ipc::MakeStreamWriter(stream, schema, write_options).ValueOrDie();
            ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
            ASSERT_ARROW_OK(writer->Close());
            auto buf = stream->Finish().ValueOrDie();

            ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
            bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
            bb->flip_to_read();
            ASSERT_OK(pipe->append(std::move(bb)));
        }
        ASSERT_OK(pipe->finish());

        ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
        ASSERT_OK(scanner.open());

        auto res = scanner.get_next();
        ASSERT_OK(res.status());
        ASSERT_NE(nullptr, res.value());
        ASSERT_EQ(2, res.value()->num_rows());
        EXPECT_EQ(101, res.value()->columns()[0]->get(0).get_int32());
        EXPECT_EQ(202, res.value()->columns()[0]->get(1).get_int32());

        auto eof_res = scanner.get_next();
        EXPECT_TRUE(eof_res.status().is_end_of_file());

        EXPECT_EQ(4096, ctx->counter->num_rows_filtered);
        EXPECT_EQ(1, ctx->counter->arrow_messages_rejected);
        EXPECT_EQ(1, ctx->counter->arrow_messages_read);
        EXPECT_GT(ctx->counter->arrow_decompressed_bytes, 0);
        EXPECT_GT(ctx->counter->arrow_decode_ns, 0);

        auto* read_counter = scanner._profile->get_counter("ArrowMessagesRead");
        auto* rejected_counter = scanner._profile->get_counter("ArrowMessagesRejected");
        auto* decompressed_counter = scanner._profile->get_counter("ArrowDecompressedBytes");
        auto* decode_timer = scanner._profile->get_counter("ArrowDecodeTime");
        ASSERT_NE(nullptr, read_counter);
        ASSERT_NE(nullptr, rejected_counter);
        ASSERT_NE(nullptr, decompressed_counter);
        ASSERT_NE(nullptr, decode_timer);
        EXPECT_EQ(ctx->counter->arrow_messages_read, read_counter->value());
        EXPECT_EQ(ctx->counter->arrow_messages_rejected, rejected_counter->value());
        EXPECT_EQ(ctx->counter->arrow_decompressed_bytes, decompressed_counter->value());
        EXPECT_EQ(ctx->counter->arrow_decode_ns, decode_timer->value());

        scanner.close();
    }

    // Subcase B: Dictionary expansion bomb filtered by memory cap ([P1] 4305383609).
    {
        const int64_t old_limit = config::routine_load_arrow_max_decoded_bytes_per_message;
        config::routine_load_arrow_max_decoded_bytes_per_message = 16 * 1024;
        DeferOp restore_limit([&]() { config::routine_load_arrow_max_decoded_bytes_per_message = old_limit; });

        SlotTypeDescInfoArray slots;
        slots.emplace_back("c0_str", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);

        auto load_id = UniqueId::gen_uid();
        auto pipe = std::make_shared<DiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
        auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
        ASSERT_OK(ctx_res.status());
        auto& ctx = ctx_res.value();

        // Message 1: dictionary(int8, utf8) with 1 string of 128 bytes and 512 rows referencing index 0.
        // ReadNext succeeds (< 2 KB allocated), but arrow::compute::Cast during convert_arrow_array_to_column
        // attempts to allocate > 64 KB of expanded string data and fails with CapacityError from ArrowBoundedMemoryPool.
        {
            auto dict_type = arrow::dictionary(arrow::int8(), arrow::utf8());
            auto dict_schema = arrow::schema({arrow::field("c0_str", dict_type)});

            arrow::StringBuilder dict_builder;
            ASSERT_ARROW_OK(dict_builder.Append(std::string(128, 'x')));
            std::shared_ptr<arrow::Array> dict_values;
            ASSERT_ARROW_OK(dict_builder.Finish(&dict_values));

            arrow::Int8Builder idx_builder;
            for (int i = 0; i < 512; ++i) {
                ASSERT_ARROW_OK(idx_builder.Append(0));
            }
            std::shared_ptr<arrow::Array> idx_array;
            ASSERT_ARROW_OK(idx_builder.Finish(&idx_array));

            auto dict_array = arrow::DictionaryArray::FromArrays(dict_type, idx_array, dict_values).ValueOrDie();
            auto bomb_batch = arrow::RecordBatch::Make(dict_schema, 512, {dict_array});

            auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
            auto writer = arrow::ipc::MakeStreamWriter(stream, dict_schema).ValueOrDie();
            ASSERT_ARROW_OK(writer->WriteRecordBatch(*bomb_batch));
            ASSERT_ARROW_OK(writer->Close());
            auto buf = stream->Finish().ValueOrDie();
            ASSERT_LT(buf->size(), 4096);

            ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
            bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
            bb->flip_to_read();
            ASSERT_OK(pipe->append(std::move(bb)));
        }

        // Message 2: Valid 2-row VARCHAR message.
        {
            auto good_schema = arrow::schema({arrow::field("c0_str", arrow::utf8())});
            arrow::StringBuilder str_builder;
            ASSERT_ARROW_OK(str_builder.AppendValues({"ok1", "ok2"}));
            std::shared_ptr<arrow::Array> str_array;
            ASSERT_ARROW_OK(str_builder.Finish(&str_array));
            auto good_batch = arrow::RecordBatch::Make(good_schema, 2, {str_array});

            auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
            auto writer = arrow::ipc::MakeStreamWriter(stream, good_schema).ValueOrDie();
            ASSERT_ARROW_OK(writer->WriteRecordBatch(*good_batch));
            ASSERT_ARROW_OK(writer->Close());
            auto buf = stream->Finish().ValueOrDie();

            ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
            bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
            bb->flip_to_read();
            ASSERT_OK(pipe->append(std::move(bb)));
        }
        ASSERT_OK(pipe->finish());

        ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
        ASSERT_OK(scanner.open());

        auto res = scanner.get_next();
        ASSERT_OK(res.status());
        ASSERT_NE(nullptr, res.value());
        ASSERT_EQ(2, res.value()->num_rows());
        EXPECT_EQ("ok1", res.value()->columns()[0]->get(0).get_slice());
        EXPECT_EQ("ok2", res.value()->columns()[0]->get(1).get_slice());

        auto eof_res = scanner.get_next();
        EXPECT_TRUE(eof_res.status().is_end_of_file());

        EXPECT_EQ(512, ctx->counter->num_rows_filtered);
        EXPECT_EQ(1, ctx->counter->arrow_messages_rejected);
        EXPECT_EQ(ctx->counter->arrow_messages_rejected,
                  scanner._profile->get_counter("ArrowMessagesRejected")->value());

        scanner.close();
    }

    // Subcase C: Framing edge cases in count_ipc_stream_declared_rows (corrupt Flatbuffer metadata after valid
    // continuation + metadata_len, and truncated DICTIONARY_BATCH body).
    {
        SlotTypeDescInfoArray slots;
        slots.emplace_back("c0_str", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);

        auto load_id = UniqueId::gen_uid();
        auto pipe = std::make_shared<DiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
        auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
        ASSERT_OK(ctx_res.status());
        auto& ctx = ctx_res.value();

        auto dict_type = arrow::dictionary(arrow::int8(), arrow::utf8());
        auto dict_schema = arrow::schema({arrow::field("c0_str", dict_type)});
        auto schema_only = make_schema_only_ipc_stream(dict_schema);
        ASSERT_GT(schema_only.size(), 8u);

        // Message 1: Valid Schema message followed by 0xFFFFFFFF (-1), metadata_len = 16, and 16 invalid Flatbuffer
        // bytes so Message::Open(metadata_buf, nullptr) returns !msg_res.ok().
        {
            std::vector<uint8_t> corrupt_meta(schema_only.begin(), schema_only.end() - 8);
            int32_t cont = -1;
            int32_t meta_len = 16;
            const auto* cont_bytes = reinterpret_cast<const uint8_t*>(&cont);
            const auto* len_bytes = reinterpret_cast<const uint8_t*>(&meta_len);
            corrupt_meta.insert(corrupt_meta.end(), cont_bytes, cont_bytes + 4);
            corrupt_meta.insert(corrupt_meta.end(), len_bytes, len_bytes + 4);
            corrupt_meta.insert(corrupt_meta.end(), 16, 0xAB);

            ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(corrupt_meta.size()).value();
            bb->put_bytes(reinterpret_cast<const char*>(corrupt_meta.data()), corrupt_meta.size());
            bb->flip_to_read();
            ASSERT_OK(pipe->append(std::move(bb)));
        }

        // Message 2: Dictionary-encoded stream truncated inside the DICTIONARY_BATCH body so msg->type() is
        // DICTIONARY_BATCH (!= RECORD_BATCH) when total_size - offset < body_len.
        {
            arrow::StringBuilder dict_builder;
            ASSERT_ARROW_OK(dict_builder.Append(std::string(512, 'd')));
            std::shared_ptr<arrow::Array> dict_values;
            ASSERT_ARROW_OK(dict_builder.Finish(&dict_values));

            arrow::Int8Builder idx_builder;
            ASSERT_ARROW_OK(idx_builder.AppendValues({0, 0}));
            std::shared_ptr<arrow::Array> idx_array;
            ASSERT_ARROW_OK(idx_builder.Finish(&idx_array));

            auto dict_array = arrow::DictionaryArray::FromArrays(dict_type, idx_array, dict_values).ValueOrDie();
            auto batch = arrow::RecordBatch::Make(dict_schema, 2, {dict_array});

            auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
            auto writer = arrow::ipc::MakeStreamWriter(stream, dict_schema).ValueOrDie();
            ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
            ASSERT_ARROW_OK(writer->Close());
            auto buf = stream->Finish().ValueOrDie();

            // Truncate inside the 512-byte DICTIONARY_BATCH body (before the RECORD_BATCH message starts).
            const size_t dict_body_cut = schema_only.size() + 256;
            ASSERT_LT(dict_body_cut, static_cast<size_t>(buf->size()));
            ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(dict_body_cut).value();
            bb->put_bytes(reinterpret_cast<const char*>(buf->data()), dict_body_cut);
            bb->flip_to_read();
            ASSERT_OK(pipe->append(std::move(bb)));
        }
        ASSERT_OK(pipe->finish());

        ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
        ASSERT_OK(scanner.open());
        auto eof_res = scanner.get_next();
        EXPECT_TRUE(eof_res.status().is_end_of_file());
        EXPECT_EQ(2, ctx->counter->arrow_messages_rejected);
        EXPECT_EQ(2, ctx->counter->num_rows_filtered);
        scanner.close();
    }
}

// ---------------------------------------------------------------------------
// Hostile Routine Load messages.
// ---------------------------------------------------------------------------

TEST_F(ArrowScannerTest, TestStreamRejectsEmptySchemaMessageDeclaringManyRows) {
    constexpr int64_t kDeclaredRows = 2'000'000;
    auto c = make_int_slot_case();
    // Every projected slot is nullable, so the schema validates, yet the batch declares millions of rows
    // while carrying no buffers at all.
    c.hostile_schema = arrow::schema(std::vector<std::shared_ptr<arrow::Field>>{});
    c.hostile_batches = {
            arrow::RecordBatch::Make(c.hostile_schema, kDeclaredRows, std::vector<std::shared_ptr<arrow::Array>>{})};
    c.expected_filtered_rows = kDeclaredRows;
    c.decoded_bytes_cap = 1024 * 1024;
    expect_hostile_message_rejected_atomically(c);
}

TEST_F(ArrowScannerTest, TestStreamClampsRowsDeclaredByHostileMessages) {
    // Declared row counts come from untrusted IPC metadata. Summing them must saturate instead of wrapping, or the
    // filtered-rows counter (which drives max_filter_ratio) could go negative.
    constexpr int64_t kDeclaredRows = std::numeric_limits<int64_t>::max() / 2;
    auto c = make_int_slot_case();
    auto empty_schema = arrow::schema(std::vector<std::shared_ptr<arrow::Field>>{});

    SlotTypeDescInfoArray slots = c.slots;
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, UniqueId::gen_uid(), pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();
    for (int i = 0; i < 3; ++i) {
        append_ipc_message(
                pipe, empty_schema,
                {arrow::RecordBatch::Make(empty_schema, kDeclaredRows, std::vector<std::shared_ptr<arrow::Array>>{})});
    }
    append_ipc_message(pipe, c.good_schema, {c.good_batch});
    ASSERT_OK(pipe->finish());

    ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
    ASSERT_OK(scanner.open());
    auto res = scanner.get_next();
    ASSERT_OK(res.status());
    ASSERT_EQ(2, res.value()->num_rows());
    EXPECT_TRUE(scanner.get_next().status().is_end_of_file());
    constexpr int64_t kClamp = std::numeric_limits<int32_t>::max();
    EXPECT_EQ(3, ctx->counter->arrow_messages_rejected);
    EXPECT_EQ(3 * kClamp, ctx->counter->num_rows_filtered);
    // The orchestrator and the FE add sink rows on top of this counter; that sum must not wrap.
    int64_t total = 0;
    EXPECT_FALSE(__builtin_add_overflow(ctx->counter->num_rows_filtered, int64_t{2}, &total));
    scanner.close();
}

TEST_F(ArrowScannerTest, TestStreamClampsSingleMessageWhoseBatchesDeclareOverflowingRowCount) {
    constexpr int64_t kHalfPlusOne = std::numeric_limits<int64_t>::max() / 2 + 1;
    auto c = make_int_slot_case();
    auto empty_schema = arrow::schema(std::vector<std::shared_ptr<arrow::Field>>{});

    SlotTypeDescInfoArray slots = c.slots;
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, UniqueId::gen_uid(), pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();
    append_ipc_message(
            pipe, empty_schema,
            {arrow::RecordBatch::Make(empty_schema, kHalfPlusOne, std::vector<std::shared_ptr<arrow::Array>>{}),
             arrow::RecordBatch::Make(empty_schema, kHalfPlusOne, std::vector<std::shared_ptr<arrow::Array>>{})});
    append_ipc_message(pipe, c.good_schema, {c.good_batch});
    ASSERT_OK(pipe->finish());

    ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
    ASSERT_OK(scanner.open());
    auto res = scanner.get_next();
    ASSERT_OK(res.status());
    ASSERT_EQ(2, res.value()->num_rows());
    EXPECT_TRUE(scanner.get_next().status().is_end_of_file());
    EXPECT_EQ(1, ctx->counter->arrow_messages_rejected);
    EXPECT_EQ(std::numeric_limits<int32_t>::max(), ctx->counter->num_rows_filtered);
    scanner.close();
}

TEST_F(ArrowScannerTest, TestStreamRejectsNullTypeColumnDeclaringManyRows) {
    constexpr int64_t kDeclaredRows = 2'000'000;
    auto c = make_int_slot_case();
    c.hostile_schema = arrow::schema({arrow::field("c0_int", arrow::null())});
    c.hostile_batches = {arrow::RecordBatch::Make(c.hostile_schema, kDeclaredRows,
                                                  {std::make_shared<arrow::NullArray>(kDeclaredRows)})};
    c.expected_filtered_rows = kDeclaredRows;
    c.decoded_bytes_cap = 1024 * 1024;
    expect_hostile_message_rejected_atomically(c);
}

TEST_F(ArrowScannerTest, TestStreamRejectsNestedNullChildDeclaringManyElements) {
    constexpr int32_t kChildElements = 2'000'000;
    HostileMessageCase c;
    c.slots.emplace_back("c0_arr", TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_INT)),
                         true);
    c.hostile_schema = arrow::schema({arrow::field("c0_arr", arrow::list(arrow::null()))});
    // One top-level row whose list holds two million NULL elements.
    c.hostile_batches = {make_null_list_batch(c.hostile_schema, {0, kChildElements})};
    c.good_schema = c.hostile_schema;
    c.good_batch = make_null_list_batch(c.good_schema, {0, 1, 2});
    c.expected_filtered_rows = 1;
    c.decoded_bytes_cap = 1024 * 1024;
    expect_hostile_message_rejected_atomically(c);
}

TEST_F(ArrowScannerTest, TestStreamRejectsDictionaryExpansionBeyondOutputBudget) {
    auto c = make_string_slot_case();
    auto dict_schema = arrow::schema({arrow::field("c0_str", arrow::dictionary(arrow::int8(), arrow::utf8()))});
    // 4096 indices over a single 1 KiB value: tiny on the wire, 4 MiB once expanded.
    c.hostile_schema = dict_schema;
    c.hostile_batches = {
            make_dictionary_string_batch(dict_schema, {std::string(1024, 'x')}, std::vector<int8_t>(4096, 0))};
    c.expected_filtered_rows = 4096;
    c.decoded_bytes_cap = 1024 * 1024;
    expect_hostile_message_rejected_atomically(c);
}

TEST_F(ArrowScannerTest, TestStreamRejectsMessageWhenLaterBatchHasOutOfRangeDictionaryIndex) {
    auto c = make_string_slot_case();
    auto dict_schema = arrow::schema({arrow::field("c0_str", arrow::dictionary(arrow::int8(), arrow::utf8()))});
    // The first batch alone fills more than one chunk; the defect is only in the second batch.
    c.hostile_schema = dict_schema;
    c.hostile_batches = {make_dictionary_string_batch(dict_schema, {"a", "b"}, std::vector<int8_t>(5000, 0)),
                         make_dictionary_string_batch(dict_schema, {"a", "b"}, {0, 7})};
    c.expected_filtered_rows = 5002;
    expect_hostile_message_rejected_atomically(c);
}

TEST_F(ArrowScannerTest, TestStreamRejectsMessageWithInvalidUtf8) {
    auto c = make_string_slot_case();
    const int32_t offsets[] = {0, 1, 2, 3};
    auto offsets_buf = arrow::Buffer::FromString(std::string(reinterpret_cast<const char*>(offsets), sizeof(offsets)));
    auto data_buf = arrow::Buffer::FromString(std::string("\xff\xfe\xfd", 3));
    std::shared_ptr<arrow::Array> bad_strings = std::make_shared<arrow::StringArray>(3, offsets_buf, data_buf);
    c.hostile_schema = c.good_schema;
    c.hostile_batches = {arrow::RecordBatch::Make(c.hostile_schema, 3, {bad_strings})};
    c.expected_filtered_rows = 3;
    expect_hostile_message_rejected_atomically(c);
}

TEST_F(ArrowScannerTest, TestStreamRejectsWholeMessageWhenConvertedOutputExceedsBudget) {
    auto c = make_int_slot_case();
    // Estimated output is 4 bytes per row (800,000 B) which fits the 900,000 B budget, but a nullable INT
    // column really materializes 5 bytes per row, so the cap is only crossed while converting later chunks.
    // Chunks converted before that point must not leak out of the rejected message.
    c.hostile_schema = c.good_schema;
    c.hostile_batches = {make_int32_batch(c.hostile_schema, std::vector<int32_t>(200'000, 7))};
    c.expected_filtered_rows = 200'000;
    c.decoded_bytes_cap = 900'000;
    expect_hostile_message_rejected_atomically(c);
}

TEST_F(ArrowScannerTest, TestStreamMissingNullableColumnLoadsNulls) {
    // Matches JSON/Avro/CSV: a column absent from the payload is NULL when the destination allows it.
    SlotTypeDescInfoArray slots;
    slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, UniqueId::gen_uid(), pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();

    auto other_schema = arrow::schema({arrow::field("unrelated", arrow::int32())});
    append_ipc_message(pipe, other_schema, {make_int32_batch(other_schema, {1, 2, 3})});
    ASSERT_OK(pipe->finish());

    ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
    ASSERT_OK(scanner.open());
    auto res = scanner.get_next();
    ASSERT_OK(res.status());
    ASSERT_NE(nullptr, res.value());
    ASSERT_EQ(3, res.value()->num_rows());
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_TRUE(res.value()->columns()[0]->is_null(i));
    }
    EXPECT_TRUE(scanner.get_next().status().is_end_of_file());
    EXPECT_EQ(1, ctx->counter->arrow_messages_read);
    EXPECT_EQ(0, ctx->counter->arrow_messages_rejected);
    EXPECT_EQ(0, ctx->counter->num_rows_filtered);
    scanner.close();
}

TEST_F(ArrowScannerTest, TestStreamRejectedMessageWarningsAreCapped) {
    constexpr int kCorruptMessages = 150;
    SlotTypeDescInfoArray slots;
    slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, UniqueId::gen_uid(), pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();

    const std::string junk = "corrupt_arrow_ipc_stream";
    for (int i = 0; i < kCorruptMessages; ++i) {
        append_raw_message(pipe, reinterpret_cast<const uint8_t*>(junk.data()), junk.size());
    }
    auto good_schema = arrow::schema({arrow::field("c0_int", arrow::int32())});
    append_ipc_message(pipe, good_schema, {make_int32_batch(good_schema, {1, 2})});
    ASSERT_OK(pipe->finish());

    ArrowScannerLogSink sink;
    google::AddLogSink(&sink);
    DeferOp remove_sink([&] { google::RemoveLogSink(&sink); });

    ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
    ASSERT_OK(scanner.open());
    auto res = scanner.get_next();
    ASSERT_OK(res.status());
    ASSERT_EQ(2, res.value()->num_rows());
    EXPECT_TRUE(scanner.get_next().status().is_end_of_file());
    scanner.close();

    // Counters stay exact even though the log is capped.
    EXPECT_EQ(kCorruptMessages, ctx->counter->arrow_messages_rejected);
    EXPECT_EQ(kCorruptMessages, ctx->counter->num_rows_filtered);
    // Same cap as the error log (101 entries) plus a single "suppressed" notice.
    const size_t warnings = sink.count(google::GLOG_WARNING, "Arrow routine load:");
    EXPECT_GE(warnings, 1u);
    EXPECT_LE(warnings, 102u);
    EXPECT_EQ(1u, sink.count(google::GLOG_WARNING, "suppressed"));
}

TEST_F(ArrowScannerTest, TestStreamRejectedRecordsHonorUserCapBeyondErrorLogCap) {
    constexpr int kCorruptMessages = 150;
    LoadPathMgr load_path_mgr({(_tmp_root_dir / "hostile_rejected_record_cap").string()});
    ASSERT_OK(load_path_mgr.init());

    SlotTypeDescInfoArray slots;
    slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, UniqueId::gen_uid(), pipe, /*batch_size=*/0, &load_path_mgr,
                                               /*log_rejected_record_num=*/1000);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();
    ctx->state->set_db("test_db");
    ctx->state->set_load_label("test_hostile_rejected_record_cap");
    ctx->state->set_txn_id(54323);

    const std::string junk = "corrupt_arrow_ipc_stream";
    for (int i = 0; i < kCorruptMessages; ++i) {
        append_raw_message(pipe, reinterpret_cast<const uint8_t*>(junk.data()), junk.size());
    }
    ASSERT_OK(pipe->finish());

    ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
    ASSERT_OK(scanner.open());
    EXPECT_TRUE(scanner.get_next().status().is_end_of_file());
    scanner.close();

    // The error log is capped (~100 entries) but rejected records follow log_rejected_record_num only.
    auto* writer = LoadPathStateHelper::rejected_record_writer(ctx->state);
    ASSERT_NE(nullptr, writer);
    std::ifstream rejected_file(writer->file_path());
    ASSERT_TRUE(rejected_file.is_open());
    std::string line;
    int rejected_records = 0;
    while (std::getline(rejected_file, line)) {
        if (!line.empty()) ++rejected_records;
    }
    EXPECT_EQ(kCorruptMessages, rejected_records);
}

namespace {

std::vector<uint8_t> mutate_ipc_bytes(const std::vector<uint8_t>& base, std::mt19937& rng) {
    std::vector<uint8_t> out = base;
    switch (rng() % 5) {
    case 0: { // flip up to four random bits
        const size_t flips = 1 + rng() % 4;
        for (size_t i = 0; i < flips; ++i) {
            out[rng() % out.size()] ^= static_cast<uint8_t>(1u << (rng() % 8));
        }
        break;
    }
    case 1: // truncate
        out.resize(1 + rng() % out.size());
        break;
    case 2: { // overwrite an 8-byte window with random bytes
        const size_t pos = rng() % out.size();
        for (size_t i = pos; i < std::min(out.size(), pos + 8); ++i) {
            out[i] = static_cast<uint8_t>(rng());
        }
        break;
    }
    case 3: { // delete a short range
        const size_t pos = rng() % out.size();
        const size_t len = 1 + rng() % 16;
        out.erase(out.begin() + pos, out.begin() + std::min(out.size(), pos + len));
        if (out.empty()) out.push_back(0);
        break;
    }
    default: { // write an extreme 4-byte value (lengths, offsets, counts)
        const size_t pos = rng() % (out.size() - 3);
        const uint8_t fill = (rng() % 2) ? 0xFF : 0x7F;
        for (size_t i = pos; i < pos + 4; ++i) out[i] = fill;
        break;
    }
    }
    return out;
}

} // namespace

TEST_F(ArrowScannerTest, TestStreamMutatedMessagesAreAccountedForExactlyOnce) {
    SlotTypeDescInfoArray slots;
    slots.emplace_back("c0_int", TypeDescriptor::from_logical_type(TYPE_INT), true);
    slots.emplace_back("c1_str", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);
    slots.emplace_back("c2_dict", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);

    auto schema = arrow::schema({arrow::field("c0_int", arrow::int32()), arrow::field("c1_str", arrow::utf8()),
                                 arrow::field("c2_dict", arrow::dictionary(arrow::int8(), arrow::utf8()))});
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    for (int b = 0; b < 2; ++b) {
        arrow::Int32Builder int_builder;
        arrow::StringBuilder str_builder;
        for (int i = 0; i < 40; ++i) {
            ASSERT_ARROW_OK(int_builder.Append(b * 100 + i));
            ASSERT_ARROW_OK(str_builder.Append("row_" + std::to_string(b * 100 + i)));
        }
        std::shared_ptr<arrow::Array> int_array, str_array;
        ASSERT_ARROW_OK(int_builder.Finish(&int_array));
        ASSERT_ARROW_OK(str_builder.Finish(&str_array));

        arrow::StringBuilder dict_builder;
        ASSERT_ARROW_OK(dict_builder.AppendValues({"red", "green", "blue"}));
        std::shared_ptr<arrow::Array> dict_values;
        ASSERT_ARROW_OK(dict_builder.Finish(&dict_values));
        arrow::Int8Builder idx_builder;
        for (int i = 0; i < 40; ++i) ASSERT_ARROW_OK(idx_builder.Append(static_cast<int8_t>(i % 3)));
        std::shared_ptr<arrow::Array> idx_array;
        ASSERT_ARROW_OK(idx_builder.Finish(&idx_array));
        std::shared_ptr<arrow::Array> dict_array =
                std::make_shared<arrow::DictionaryArray>(schema->field(2)->type(), idx_array, dict_values);
        batches.push_back(arrow::RecordBatch::Make(schema, 40, {int_array, str_array, dict_array}));
    }
    const auto base = serialize_ipc_stream(schema, batches);
    ASSERT_GT(base.size(), 16u);

    std::mt19937 rng(20261022);
    constexpr int kGroups = 8;
    constexpr int kMessagesPerGroup = 150;
    for (int group = 0; group < kGroups; ++group) {
        auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
        auto ctx_res = make_stream_scanner_context(slots, UniqueId::gen_uid(), pipe);
        ASSERT_OK(ctx_res.status());
        auto& ctx = ctx_res.value();

        for (int m = 0; m < kMessagesPerGroup; ++m) {
            auto mutated = mutate_ipc_bytes(base, rng);
            append_raw_message(pipe, mutated.data(), mutated.size());
        }
        ASSERT_OK(pipe->finish());

        ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
        ASSERT_OK(scanner.open());
        while (true) {
            auto res = scanner.get_next();
            if (res.status().is_end_of_file()) break;
            ASSERT_TRUE(res.status().ok()) << "group " << group << ": " << res.status();
            ASSERT_NE(nullptr, res.value());
        }
        EXPECT_EQ(kMessagesPerGroup, ctx->counter->arrow_messages_read + ctx->counter->arrow_messages_rejected)
                << "group " << group << ": every message must be read or rejected exactly once";
        EXPECT_GE(ctx->counter->num_rows_filtered, ctx->counter->arrow_messages_rejected) << "group " << group;
        scanner.close();
    }
}

TEST_F(ArrowScannerTest, TestStreamPropagatesDelegateOutOfMemoryDuringDecompression) {
    class AlwaysFailingPool final : public arrow::MemoryPool {
    public:
        using arrow::MemoryPool::Allocate;
        using arrow::MemoryPool::Free;
        using arrow::MemoryPool::Reallocate;

        arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
            return arrow::Status::OutOfMemory("injected");
        }

        arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t** ptr) override {
            return arrow::Status::OutOfMemory("injected");
        }

        void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
            arrow::default_memory_pool()->Free(buffer, size, alignment);
        }

        int64_t bytes_allocated() const override { return 0; }
        int64_t max_memory() const override { return 0; }
        int64_t total_bytes_allocated() const override { return 0; }
        int64_t num_allocations() const override { return 0; }
        std::string backend_name() const override { return "always_failing"; }
    };

    SlotTypeDescInfoArray slots;
    slots.emplace_back("c0_bigint", TypeDescriptor::from_logical_type(TYPE_BIGINT), true);

    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();

    auto schema = arrow::schema({arrow::field("c0_bigint", arrow::int64())});

    // Message 1: LZ4-compressed batch with 4096 int64 rows
    {
        arrow::Int64Builder builder;
        for (int64_t i = 0; i < 4096; ++i) {
            ASSERT_ARROW_OK(builder.Append(42));
        }
        std::shared_ptr<arrow::Array> array;
        ASSERT_ARROW_OK(builder.Finish(&array));
        auto batch = arrow::RecordBatch::Make(schema, 4096, {array});

        arrow::ipc::IpcWriteOptions write_options = arrow::ipc::IpcWriteOptions::Defaults();
        auto codec_res = arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME);
        ASSERT_ARROW_OK(codec_res.status());
        write_options.codec = std::move(codec_res).ValueOrDie();

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, schema, write_options).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        EXPECT_OK(pipe->append(std::move(bb)));
    }

    // Message 2: Valid small uncompressed message (2 rows)
    {
        arrow::Int64Builder builder;
        ASSERT_ARROW_OK(builder.AppendValues({101, 202}));
        std::shared_ptr<arrow::Array> array;
        ASSERT_ARROW_OK(builder.Finish(&array));
        auto batch = arrow::RecordBatch::Make(schema, 2, {array});

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        EXPECT_OK(pipe->append(std::move(bb)));
    }
    EXPECT_OK(pipe->finish());

    ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
    ASSERT_OK(scanner.open());
    AlwaysFailingPool failing;
    scanner._decode_delegate_pool = &failing;

    auto res = scanner.get_next();
    EXPECT_TRUE(res.status().is_mem_limit_exceeded()) << res.status().to_string();
    EXPECT_EQ(0, ctx->counter->arrow_messages_rejected);
    EXPECT_EQ(0, ctx->counter->num_rows_filtered);
    EXPECT_EQ(0, ctx->counter->arrow_messages_read);
    scanner.close();
}

TEST_F(ArrowScannerTest, TestStreamPropagatesDelegateOutOfMemoryDuringDictionaryDecode) {
    class AlwaysFailingPool final : public arrow::MemoryPool {
    public:
        using arrow::MemoryPool::Allocate;
        using arrow::MemoryPool::Free;
        using arrow::MemoryPool::Reallocate;

        arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
            return arrow::Status::OutOfMemory("injected");
        }

        arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t** ptr) override {
            return arrow::Status::OutOfMemory("injected");
        }

        void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
            arrow::default_memory_pool()->Free(buffer, size, alignment);
        }

        int64_t bytes_allocated() const override { return 0; }
        int64_t max_memory() const override { return 0; }
        int64_t total_bytes_allocated() const override { return 0; }
        int64_t num_allocations() const override { return 0; }
        std::string backend_name() const override { return "always_failing"; }
    };

    SlotTypeDescInfoArray slots;
    slots.emplace_back("c0_str", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);

    auto load_id = UniqueId::gen_uid();
    auto pipe = std::make_shared<MockDiscreteStreamLoadPipe>(1024 * 1024, 64 * 1024);
    auto ctx_res = make_stream_scanner_context(slots, load_id, pipe);
    ASSERT_OK(ctx_res.status());
    auto& ctx = ctx_res.value();

    // Message 1: dictionary(int8, utf8) with 1 string of 128 bytes and 512 rows referencing index 0
    {
        auto dict_type = arrow::dictionary(arrow::int8(), arrow::utf8());
        auto dict_schema = arrow::schema({arrow::field("c0_str", dict_type)});

        arrow::StringBuilder dict_builder;
        ASSERT_ARROW_OK(dict_builder.Append(std::string(128, 'x')));
        std::shared_ptr<arrow::Array> dict_values;
        ASSERT_ARROW_OK(dict_builder.Finish(&dict_values));

        arrow::Int8Builder idx_builder;
        for (int i = 0; i < 512; ++i) {
            ASSERT_ARROW_OK(idx_builder.Append(0));
        }
        std::shared_ptr<arrow::Array> idx_array;
        ASSERT_ARROW_OK(idx_builder.Finish(&idx_array));

        std::shared_ptr<arrow::Array> dict_array =
                std::make_shared<arrow::DictionaryArray>(dict_type, idx_array, dict_values);
        auto batch = arrow::RecordBatch::Make(dict_schema, 512, {dict_array});

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, dict_schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bb)));
    }

    // Message 2: Valid small uncompressed message
    {
        auto str_schema = arrow::schema({arrow::field("c0_str", arrow::utf8())});
        arrow::StringBuilder builder;
        ASSERT_ARROW_OK(builder.AppendValues({"hello", "world"}));
        std::shared_ptr<arrow::Array> array;
        ASSERT_ARROW_OK(builder.Finish(&array));
        auto batch = arrow::RecordBatch::Make(str_schema, 2, {array});

        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(stream, str_schema).ValueOrDie();
        ASSERT_ARROW_OK(writer->WriteRecordBatch(*batch));
        ASSERT_ARROW_OK(writer->Close());
        auto buf = stream->Finish().ValueOrDie();

        ByteBufferPtr bb = ByteBuffer::allocate_with_tracker(buf->size()).value();
        bb->put_bytes(reinterpret_cast<const char*>(buf->data()), buf->size());
        bb->flip_to_read();
        ASSERT_OK(pipe->append(std::move(bb)));
    }
    ASSERT_OK(pipe->finish());

    ArrowScanner scanner(ctx->state, ctx->profile, ctx->broker_scan_range, ctx->counter);
    ASSERT_OK(scanner.open());
    AlwaysFailingPool failing;
    scanner._decode_delegate_pool = &failing;

    auto res = scanner.get_next();
    EXPECT_TRUE(res.status().is_mem_limit_exceeded()) << res.status().to_string();
    EXPECT_EQ(0, ctx->counter->arrow_messages_rejected);
    EXPECT_EQ(0, ctx->counter->num_rows_filtered);
    EXPECT_EQ(0, ctx->counter->arrow_messages_read);
    scanner.close();
}

} // namespace starrocks
