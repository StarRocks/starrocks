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

#pragma once

#include <gtest/gtest.h>

#include "exec/stream/aggregate/stream_aggregator.h"
#include "testutil/column_test_helper.h"
#include "testutil/desc_tbl_helper.h"
#include "testutil/exprs_test_helper.h"

namespace starrocks::stream {

using SlotTypeInfo = SlotTypeInfo;
using ExprsTestHelper = ExprsTestHelper;
using StreamRowOp = StreamRowOp;
using GroupByKeyInfo = SlotId;
using AggInfo = std::tuple<SlotId, std::string, LogicalType, LogicalType>;

class StreamTestBase : public testing::Test {
public:
    StreamTestBase() = default;
    ~StreamTestBase() = default;

protected:
    void SetUp() override {}
    void TearDown() override {}

protected:
    DescriptorTbl* GenerateDescTbl(RuntimeState* state, ObjectPool& obj_pool,
                                   const std::vector<std::vector<SlotTypeInfo>>& slot_info_arrays) {
        return DescTblHelper::generate_desc_tbl(state, obj_pool,
                                                DescTblHelper::create_slot_type_desc_info_arrays(slot_info_arrays));
    }

    std::shared_ptr<StreamAggregator> _create_stream_aggregator(
            const std::vector<std::vector<SlotTypeInfo>>& slot_infos, const std::vector<GroupByKeyInfo>& group_by_infos,
            const std::vector<AggInfo>& agg_infos, bool is_generate_retract, int32_t count_agg_idx) {
        auto params = std::make_shared<AggregatorParams>();
        params->needs_finalize = false;
        params->has_outer_join_child = false;
        params->streaming_preaggregation_mode = TStreamingPreaggregationMode::AUTO;
        params->intermediate_tuple_id = 1;
        params->output_tuple_id = 2;
        params->count_agg_idx = count_agg_idx;
        params->sql_grouping_keys = "";
        params->sql_aggregate_functions = "";
        params->conjuncts = {};
        params->is_testing = true;
        // TODO: test more cases.
        params->is_append_only = false;
        params->is_generate_retract = is_generate_retract;
        params->grouping_exprs = _create_group_by_exprs(slot_infos[0], group_by_infos);
        params->intermediate_aggr_exprs = {};
        params->aggregate_functions = _create_agg_exprs(slot_infos[0], agg_infos);
        params->init();
        return std::make_shared<StreamAggregator>(std::move(params));
    }

    std::vector<TExpr> _create_group_by_exprs(std::vector<SlotTypeInfo> slot_infos, std::vector<GroupByKeyInfo> infos) {
        std::vector<TExpr> exprs;
        for (auto& slot_id : infos) {
            auto info = slot_infos[slot_id];
            auto type = ExprsTestHelper::create_scalar_type_desc(to_thrift(std::get<1>(info)));
            auto t_expr_node = ExprsTestHelper::create_slot_expr_node(0, slot_id, type, false);
            exprs.emplace_back(ExprsTestHelper::create_slot_expr(t_expr_node));
        }
        return exprs;
    }
    std::vector<TExpr> _create_agg_exprs(std::vector<SlotTypeInfo> slot_infos, std::vector<AggInfo> infos) {
        std::vector<TExpr> exprs;
        for (auto& agg_info : infos) {
            auto slot_id = std::get<0>(agg_info);
            auto agg_name = std::get<1>(agg_info);
            auto slot_info = slot_infos[slot_id];
            // agg input type
            auto t_type = ExprsTestHelper::create_scalar_type_desc(to_thrift(std::get<1>(slot_info)));
            // agg intermediate type
            auto intermediate_type = ExprsTestHelper::create_scalar_type_desc(to_thrift(std::get<2>(agg_info)));
            // agg result type
            auto ret_type = ExprsTestHelper::create_scalar_type_desc(to_thrift(std::get<3>(agg_info)));

            auto child_node = ExprsTestHelper::create_slot_expr_node(0, slot_id, t_type, false);

            auto f_fn = ExprsTestHelper::create_builtin_function(agg_name, {t_type}, intermediate_type, ret_type);
            auto agg_expr = ExprsTestHelper::create_aggregate_expr(f_fn, {child_node});
            exprs.emplace_back(agg_expr);
        }
        return exprs;
    }

    template <typename T>
    StreamChunkPtr MakeStreamChunk(const std::vector<std::vector<T>>& cols, const std::vector<int8_t>& ops) {
        auto chunk_ptr = std::make_shared<Chunk>();
        for (size_t i = 0; i < cols.size(); i++) {
            auto col = ColumnTestHelper::build_column<T>(cols[i]);
            chunk_ptr->append_column(std::move(col), i);
        }
        Int8ColumnPtr ops_col = Int8Column::create();
        ops_col->append_numbers(ops.data(), ops.size() * sizeof(int8_t));
        return StreamChunkConverter::make_stream_chunk(std::move(chunk_ptr), std::move(ops_col));
    }

    template <class T>
    void CheckChunk(ChunkPtr chunk, std::vector<LogicalType> types, std::vector<std::vector<T>> ans,
                    std::vector<int8_t> ops) {
        auto chunk_size = chunk->num_rows();
        auto num_col = chunk->num_columns();
        DCHECK_EQ(types.size(), num_col);
        // Check data except ops.
        DCHECK_EQ(chunk_size, ans[0].size());

        for (size_t col_idx = 0; col_idx < ans.size(); ++col_idx) {
            auto& col = chunk->get_column_by_index(col_idx);
            auto exp_col = ans[col_idx];
            CheckColumn<T>(col, exp_col);
        }
        // check ops.
        if (ops.size() > 0) {
            DCHECK_EQ(chunk_size, ops.size());
            StreamChunk* stream_chunk = dynamic_cast<StreamChunk*>(chunk.get());
            auto col = StreamChunkConverter::ops(stream_chunk);
            CheckColumn(col, ops);
        }
    }

    template <typename T>
    void CheckColumn(const ColumnPtr& real_col, std::vector<T> exp_col) {
        VLOG_ROW << "Start to check column";
        DCHECK_EQ(real_col->size(), exp_col.size());
        auto num_row = real_col->size();
        for (size_t i = 0; i < num_row; i++) {
            CheckDatum<T>(real_col->get(i), exp_col[i]);
        }
    }

    void CheckColumn(const StreamRowOp* ops, std::vector<int8_t> exp_col) {
        for (size_t i = 0; i < exp_col.size(); i++) {
            DCHECK_EQ(ops[i], exp_col[i]);
        }
    }

    template <typename T>
    void CheckDatum(Datum datum, T data) {
        DCHECK_EQ(datum.get<T>(), data);
    }
};
} // namespace starrocks::stream
