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

#include "exec/file_scanner/arrow_scanner.h"

#include <arrow/builder.h>
#include <arrow/io/file.h>
#include <arrow/ipc/writer.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <utility>

#include "base/testutil/assert.h"
#include "runtime/exec_env.h"
#include "runtime/load_path_mgr.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/status.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "testutil/desc_tbl_helper.h"
#include "types/type_descriptor.h"

namespace starrocks {

#define ASSERT_ARROW_OK(status)                                                    \
    do {                                                                           \
        auto&& _status = (status);                                                 \
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
            const std::vector<TBrokerRangeDesc>& ranges,
            int32_t batch_size = 0) {
        TQueryOptions query_options;
        if (batch_size > 0) {
            query_options.__set_batch_size(batch_size);
        }
        auto query_globals = TQueryGlobals();
        query_globals.time_zone = timezone;
        RuntimeState* state = _obj_pool.add(
                new RuntimeState(TUniqueId(), query_options, query_globals, static_cast<ExecEnv*>(nullptr)));
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

        auto schema = arrow::schema({
            arrow::field("c0_int", arrow::int32()),
            arrow::field("c1_str", arrow::utf8()),
            arrow::field("c2_double", arrow::float64())
        });

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
    auto schema = arrow::schema({
        arrow::field("c3_extra", arrow::int32()),
        arrow::field("c1_str", arrow::utf8()),
        arrow::field("c0_bigint", arrow::int32())
    });

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

    auto schema = arrow::schema({
        arrow::field("c1_str", arrow::utf8()),
        arrow::field("c0_bigint", arrow::int32())
    });

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
    ExecEnv* exec_env = ExecEnv::GetInstance();
    ASSERT_NE(nullptr, exec_env);
    ASSERT_NE(nullptr, exec_env->load_path_mgr());

    RuntimeState* state = _obj_pool.add(
            new RuntimeState(TUniqueId(), query_options, query_globals, &exec_env->query_execution_services(), exec_env));

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

    std::string absolute_path = exec_env->load_path_mgr()->get_load_error_absolute_path(error_log_path);
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

} // namespace starrocks
