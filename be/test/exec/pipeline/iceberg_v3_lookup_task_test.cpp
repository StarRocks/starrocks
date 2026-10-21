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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/config_exec_fwd.h"
#include "common/object_pool.h"
#include "common/runtime_profile.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "compute_env/query/global_late_materialization_context.h"
#include "compute_env/query/query_runtime_state.h"
#include "connector/hive/iceberg_global_late_materialization_context.h"
#include "exec/exec_env.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/lookup_operator.h"
#include "exec/pipeline/lookup_request.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors_ext.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

namespace {

// Collects the response columns of a single lookup request.
class CollectingLookUpRequestContext final : public LookUpRequestContext {
public:
    explicit CollectingLookUpRequestContext(size_t num_rows) : _num_rows(num_rows) {}

    TupleId request_tuple_id() const override { return 0; }
    Status collect_input_columns(ChunkPtr chunk) override { return Status::OK(); }
    StatusOr<size_t> fill_response(const ChunkPtr& result_chunk, const std::vector<SlotDescriptor*>& slots,
                                   size_t start_offset) override {
        response = std::make_shared<Chunk>();
        for (const auto* slot : slots) {
            auto src_col = result_chunk->get_column_by_slot_id(slot->id());
            auto dst_col = src_col->clone_empty();
            dst_col->append(*src_col, start_offset, _num_rows);
            response->append_column(std::move(dst_col), slot->id());
        }
        return _num_rows;
    }
    void callback(const Status& status) override {}

    ChunkPtr response;

private:
    size_t _num_rows;
};

} // namespace

class IcebergV3LookUpTaskTest : public ::testing::Test {
public:
    void SetUp() override {
        _exec_env = ExecEnv::GetInstance();
        TUniqueId fragment_id;
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals,
                                                        &_exec_env->query_execution_services(), _exec_env);
        _fragment_dict_state = std::make_unique<FragmentDictState>();
        _runtime_state->set_fragment_dict_state(_fragment_dict_state.get());
        TUniqueId id;
        _runtime_state->init_mem_trackers(id);
        _pool = _runtime_state->obj_pool();
        auto* fragment_context = _pool->add(new FragmentContext());
        fragment_context->set_pred_tree_params({true, true});
        _runtime_state->set_fragment_ctx(fragment_context, &fragment_context->fragment_runtime_state());

        _runtime_state->set_desc_tbl(_create_table_desc());

        _query_runtime_state.set_object_pool(_pool);
        _query_runtime_state.set_global_late_materialization_ctx_mgr(&_glm_mgr);
        _runtime_state->set_query_runtime_state(&_query_runtime_state);

        // Two scan ranges over the same 4-row file, with different first row ids. The lookup has to pair
        // every fetched row with its request by (scan_range_id, _row_id).
        auto* glm_ctx = _pool->add(new IcebergGlobalLateMaterilizationContext());
        glm_ctx->hdfs_scan_node.__set_tuple_id(0);
        ASSERT_EQ(0, glm_ctx->assign_scan_range_id(_create_scan_range(0)));
        ASSERT_EQ(1, glm_ctx->assign_scan_range_id(_create_scan_range(kSecondFirstRowId)));
        _glm_mgr.get_or_create_ctx(kScanId, [&]() { return glm_ctx; });

        _op_factory = std::make_unique<LookUpOperatorFactory>(0, 0, _row_pos_descs, nullptr, 0);
        _op = std::make_unique<LookUpOperator>(_op_factory.get(), 0, 0, 0, _row_pos_descs, nullptr, 0);
    }

protected:
    // (scan_range_id, _row_id) of one lookup request.
    using Request = std::pair<int32_t, int64_t>;

    static constexpr int64_t kScanId = 1;
    static constexpr int64_t kSecondFirstRowId = 100;
    // _scan_range_id is not a slot of the request tuple; _row_id is.
    static constexpr SlotId kScanRangeIdRefSlot = 5;
    static constexpr SlotId kRowIdRefSlot = 2;
    static constexpr SlotId kFetchScanRangeIdSlot = 10;
    static constexpr SlotId kFetchRowIdSlot = 11;

    DescriptorTbl* _create_table_desc() {
        TDescriptorTableBuilder table_desc_builder;
        TSlotDescriptorBuilder slot_desc_builder;
        auto slot1 =
                slot_desc_builder.type(LogicalType::TYPE_INT).column_name("col1").column_pos(0).nullable(true).build();
        auto slot2 = slot_desc_builder.type(LogicalType::TYPE_BIGINT)
                             .column_name("col2")
                             .column_pos(1)
                             .nullable(true)
                             .build();
        auto slot3 = slot_desc_builder.type(LogicalType::TYPE_BIGINT)
                             .column_name("_row_id")
                             .column_pos(2)
                             .nullable(true)
                             .build();
        TTupleDescriptorBuilder tuple_desc_builder;
        tuple_desc_builder.add_slot(slot1);
        tuple_desc_builder.add_slot(slot2);
        tuple_desc_builder.add_slot(slot3);
        tuple_desc_builder.build(&table_desc_builder);
        DescriptorTbl* tbl = nullptr;
        CHECK(DescriptorTbl::create(_runtime_state.get(), _pool, table_desc_builder.desc_tbl(), &tbl,
                                    config::vector_chunk_size)
                      .ok());
        CHECK_EQ(kRowIdRefSlot, tbl->get_tuple_descriptor(0)->slots()[2]->id());

        std::map<int64_t, THdfsPartition> p_map;
        p_map[0] = THdfsPartition();
        THdfsTable t_hdfs_table;
        t_hdfs_table.__set_partitions(p_map);
        TTableDescriptor tdesc;
        tdesc.__set_hdfsTable(t_hdfs_table);
        auto* table_desc = _pool->add(new HdfsTableDescriptor(tdesc, _pool));
        tbl->get_tuple_descriptor(0)->set_table_desc(table_desc);
        return tbl;
    }

    THdfsScanRange _create_scan_range(int64_t first_row_id) {
        THdfsScanRange scan_range;
        scan_range.__set_file_length(_file_size);
        scan_range.__set_offset(4);
        scan_range.__set_length(_file_size);
        scan_range.__set_partition_id(0);
        scan_range.__set_relative_path(_file);
        scan_range.__set_file_format(THdfsFileFormat::PARQUET);
        scan_range.__set_first_row_id(first_row_id);
        return scan_range;
    }

    static ChunkPtr _create_request_chunk(const std::vector<Request>& requests) {
        auto scan_range_id_col = ColumnHelper::create_column(TypeDescriptor(LogicalType::TYPE_INT), true);
        auto row_id_col = ColumnHelper::create_column(TypeDescriptor(LogicalType::TYPE_BIGINT), true);
        for (const auto& [scan_range_id, row_id] : requests) {
            scan_range_id_col->append_datum(Datum(scan_range_id));
            row_id_col->append_datum(Datum(row_id));
        }
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(std::move(scan_range_id_col), kFetchScanRangeIdSlot);
        chunk->append_column(std::move(row_id_col), kFetchRowIdSlot);
        return chunk;
    }

    // Runs one lookup over the given (scan_range_id, row_id) requests and returns its status and response.
    std::pair<Status, ChunkPtr> _lookup(const std::vector<Request>& requests) {
        auto request_ctx = std::make_shared<CollectingLookUpRequestContext>(requests.size());
        auto ctx = std::make_shared<LookUpTaskContext>();
        ctx->request_tuple_id = 0;
        ctx->scan_id = kScanId;
        ctx->lookup_ref_slot_ids = {kScanRangeIdRefSlot, kRowIdRefSlot};
        ctx->fetch_ref_slot_ids = {kFetchScanRangeIdSlot, kFetchRowIdSlot};
        ctx->request_ctxs = {request_ctx};
        ctx->profile = &_profile;
        ctx->parent = _op.get();

        IcebergV3LookUpTask task(ctx);
        auto status = task.process(_runtime_state.get(), _create_request_chunk(requests));
        return {status, request_ctx->response};
    }

    ExecEnv* _exec_env = nullptr;
    std::shared_ptr<RuntimeState> _runtime_state;
    std::unique_ptr<FragmentDictState> _fragment_dict_state;
    ObjectPool* _pool = nullptr;
    GlobalLateMaterilizationContextMgr _glm_mgr;
    QueryRuntimeState _query_runtime_state;
    RuntimeProfile _profile{"IcebergV3LookUpTaskTest"};
    phmap::flat_hash_map<TupleId, RowPositionDescriptor*> _row_pos_descs;
    std::unique_ptr<LookUpOperatorFactory> _op_factory;
    std::unique_ptr<LookUpOperator> _op;

    // num rows: 4
    std::string _file = "./be/test/exec/test_data/parquet_scanner/file_reader_test.parquet1";
    int64_t _file_size = 729;
};

// Requests arrive out of order, span two scan ranges and repeat a row id. Every response row must be the file row
// that its (scan_range_id, _row_id) points to.
TEST_F(IcebergV3LookUpTaskTest, test_pair_rows_by_scan_range_and_row_id) {
    // Baseline: the file rows in file order, read through the first scan range.
    auto [baseline_status, baseline] = _lookup({{0, 0}, {0, 1}, {0, 2}, {0, 3}});
    ASSERT_TRUE(baseline_status.ok()) << baseline_status;
    ASSERT_NE(nullptr, baseline);
    ASSERT_EQ(4, baseline->num_rows());
    ASSERT_EQ(2, baseline->num_columns());

    // The second scan range reads the same rows under row ids starting at kSecondFirstRowId.
    const int64_t base = kSecondFirstRowId;
    std::vector<Request> requests = {{1, base + 2}, {0, 3}, {1, base}, {0, 0}, {0, 3}, {1, base + 1}, {0, 1}};
    std::vector<size_t> expected_file_rows = {2, 3, 0, 0, 3, 1, 1};

    auto [status, response] = _lookup(requests);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NE(nullptr, response);
    ASSERT_EQ(requests.size(), response->num_rows());
    for (size_t i = 0; i < requests.size(); i++) {
        EXPECT_EQ(baseline->debug_row(expected_file_rows[i]), response->debug_row(i)) << "request " << i;
    }
}

// A row id the file does not have fetches fewer rows than requested; the lookup must fail instead of pairing the
// requests with the wrong rows.
TEST_F(IcebergV3LookUpTaskTest, test_missing_row_id_fails) {
    auto [status, response] = _lookup({{0, 1}, {0, 1000}});
    ASSERT_FALSE(status.ok());
    EXPECT_TRUE(status.is_internal_error()) << status;
    EXPECT_NE(std::string::npos, status.message().find("fetched 1 rows for 2 requested row ids")) << status;

    auto [empty_status, empty_response] = _lookup({{1, 1000}});
    ASSERT_FALSE(empty_status.ok());
    EXPECT_NE(std::string::npos, empty_status.message().find("fetched 0 rows for 1 requested row ids"))
            << empty_status;
}

} // namespace starrocks::pipeline
