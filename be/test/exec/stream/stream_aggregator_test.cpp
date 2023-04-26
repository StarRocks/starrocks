// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#include <gtest/gtest.h>

#include <vector>

#include "exec/stream/stream_test.h"
#include "testutil/desc_tbl_helper.h"

namespace starrocks::stream {

class StreamAggregateTestBase : public StreamTestBase {
public:
    template <typename T>
    struct StreamRowData {
        std::vector<std::vector<T>> rows;
        std::vector<int8_t> ops;
    };

    StreamAggregateTestBase(bool is_generate_retract) : _is_generate_retract(is_generate_retract) {}
    ~StreamAggregateTestBase() override = default;

    std::vector<LogicalType> get_slot_types(std::vector<SlotTypeInfo> slot_infos) {
        std::vector<LogicalType> types;
        for (auto& info : slot_infos) {
            types.push_back(std::get<1>(info));
        }
        return types;
    }

    template <typename T>
    void RunBatch(size_t run_id, const StreamRowData<T>& input_rows, StreamChunkPtr* result_chunk,
                  ChunkPtr* intermediate_chunk, std::vector<ChunkPtr>& detail_chunks) {
        VLOG_ROW << "[RunBatchAndCheck] >>>>>>>>>>>>>>> Run: " << run_id;

        auto input_chunk_ptr = MakeStreamChunk<T>(input_rows.rows, input_rows.ops);
        auto chunk_size = input_chunk_ptr->num_rows();
        DCHECK_IF_ERROR(_stream_aggregator->process_chunk(input_chunk_ptr.get()));
        DCHECK_IF_ERROR(
                _stream_aggregator->output_changes(chunk_size, result_chunk, intermediate_chunk, detail_chunks));
        for (auto& column : (*result_chunk)->columns()) {
            VLOG_ROW << "[RunBatchAndCheck] result column:" << column->debug_string();
        }

        // intermediate data may not exist
        for (auto& column : (*intermediate_chunk)->columns()) {
            VLOG_ROW << "[RunBatchAndCheck] intermediate column:" << column->debug_string();
        }
        if (!detail_chunks.empty()) {
            for (auto& detail_chunk : detail_chunks) {
                VLOG_ROW << "[RunBatchAndCheck] detail chunks...";
                for (auto& column : detail_chunk->columns()) {
                    VLOG_ROW << "[RunBatchAndCheck] detail column:" << column->debug_string();
                }
            }
        }
        DCHECK_IF_ERROR(_stream_aggregator->reset_state(_runtime_state));
    }

    template <typename T>
    void RunBatchAndCheck(size_t run_id, const StreamRowData<T>& input_rows,
                          const StreamRowData<T>& expect_result_data) {
        return RunBatchAndCheck(run_id, input_rows, expect_result_data, StreamRowData<T>{{}, {}});
    }

    template <typename T>
    void RunBatchAndCheck(size_t run_id, const StreamRowData<T>& input_rows, const StreamRowData<T>& expect_result_data,
                          const StreamRowData<T>& expect_intermediate_data) {
        auto result_chunk_ptr = std::make_shared<StreamChunk>();
        auto intermediate_chunk_ptr = std::make_shared<Chunk>();
        std::vector<ChunkPtr> detail_chunks;
        RunBatch(run_id, input_rows, &result_chunk_ptr, &intermediate_chunk_ptr, detail_chunks);
        CheckChunk(result_chunk_ptr, get_slot_types(_slot_infos[2]), expect_result_data.rows, expect_result_data.ops);
        if (!expect_intermediate_data.rows.empty()) {
            CheckChunk(intermediate_chunk_ptr, get_slot_types(_slot_infos[1]), expect_intermediate_data.rows,
                       expect_intermediate_data.ops);
        }
    }

    void SetUp() override {
        _runtime_state = _obj_pool.add(new RuntimeState(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr));
        _runtime_profile = _runtime_state->runtime_profile();
        _mem_tracker = std::make_unique<MemTracker>();
    }

    void TearDown() override {}

protected:
    bool _is_generate_retract;
    RuntimeState* _runtime_state;
    ObjectPool _obj_pool;
    DescriptorTbl* _tbl;
    RuntimeProfile* _runtime_profile;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::shared_ptr<StreamAggregator> _stream_aggregator;
    std::shared_ptr<StreamAggregator> _stream_aggregator_generate_retract;
    std::vector<std::vector<SlotTypeInfo>> _slot_infos;
    std::vector<GroupByKeyInfo> _group_by_infos;
    std::vector<AggInfo> _agg_infos;
};

///////////////  Count Aggregate Function ///////////////
class CountStreamAggregateTestBase : public StreamAggregateTestBase {
public:
    CountStreamAggregateTestBase(bool is_generate_retract) : StreamAggregateTestBase(is_generate_retract) {}

    void SetUp() override {
        StreamAggregateTestBase::SetUp();

        _slot_infos = std::vector<std::vector<SlotTypeInfo>>{
                // input slots
                {
                        {"col1", TYPE_BIGINT, false},
                        {"col2", TYPE_BIGINT, false},
                },
                // intermediate slots
                {
                        {"col1", TYPE_BIGINT, false},
                        {"count_agg", TYPE_BIGINT, false},
                },
                // result slots
                {
                        {"col1", TYPE_BIGINT, false},
                        {"count_agg", TYPE_BIGINT, false},
                },
        };
        _group_by_infos = {0};
        _agg_infos = std::vector<AggInfo>{// slot_index, agg_name, agg_intermediate_type, agg_result_type
                                          {1, "count", TYPE_BIGINT, TYPE_BIGINT}};

        _tbl = GenerateDescTbl(_runtime_state, _obj_pool, _slot_infos);
        _runtime_state->set_desc_tbl(_tbl);
        _stream_aggregator =
                _create_stream_aggregator(_slot_infos, _group_by_infos, _agg_infos, _is_generate_retract, 0);
    }
};

class CountStreamAggregateTest : public CountStreamAggregateTestBase {
public:
    CountStreamAggregateTest() : CountStreamAggregateTestBase(false) {}
};

class CountStreamAggregateTestWithGenerateRetracts : public CountStreamAggregateTestBase {
public:
    CountStreamAggregateTestWithGenerateRetracts() : CountStreamAggregateTestBase(true) {}
};

TEST_F(CountStreamAggregateTest, TestNoRetracts_NoGenerateRetracts) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_runtime_state, &_obj_pool, _runtime_profile));
    DCHECK_IF_ERROR(_stream_aggregator->open(_runtime_state));

    // 0: StreamRowOp::INSERT,
    // 1: StreamRowOp::DELETE
    // Data:
    // c0 c1 op
    //  1 1 INSERT
    //  2 2 INSERT
    //  1 2 INSERT
    // group by c0 count(c1)
    // no  retract
    // c0, count(c1), op
    // 1 2 0
    // 2 1 0
    RunBatchAndCheck(1, StreamRowData<int64_t>{{{1, 2, 1}, {1, 2, 2}}, {0, 0, 0}},
                     StreamRowData<int64_t>{{{1, 2}, {2, 1}}, {0, 0}});
    _stream_aggregator->close(_runtime_state);
}

TEST_F(CountStreamAggregateTest, TestWithRetracts_NoGenerateRetracts) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_runtime_state, &_obj_pool, _runtime_profile));
    DCHECK_IF_ERROR(_stream_aggregator->open(_runtime_state));

    // 0: StreamRowOp::INSERT,
    // 1: StreamRowOp::DELETE
    // 0: StreamRowOp::INSERT,
    // 1: StreamRowOp::DELETE
    // Data:
    // c0 c1 op
    //  1 1 INSERT
    //  2 2 INSERT
    //  1 1 DELETE
    // group by c0 count(c1)
    // no  retract
    // c0, count(c1), op
    // 1 0 0 <---- It's Confused If No generated Retracts.
    // 2 1 0
    RunBatchAndCheck(1, StreamRowData<int64_t>{{{1, 2, 1}, {1, 2, 2}}, {0, 0, 1}},
                     StreamRowData<int64_t>{{{1, 2}, {0, 1}}, {0, 0}});
    _stream_aggregator->close(_runtime_state);
}

TEST_F(CountStreamAggregateTest, TestNoRetracts_MultiRuns) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_runtime_state, &_obj_pool, _runtime_profile));
    DCHECK_IF_ERROR(_stream_aggregator->open(_runtime_state));

    // 0: StreamRowOp::INSERT,
    // 1: StreamRowOp::DELETE
    // RUN 1
    // Data:
    // c0 c1 op
    //  1 1 INSERT
    //  2 2 INSERT
    //  1 2 INSERT
    // group by c0 count(c1)
    // no  retract
    // c0, count(c1), op
    // 1 2 0
    // 2 1 0
    RunBatchAndCheck(1, StreamRowData<int64_t>{{{1, 2, 1}, {1, 2, 2}}, {0, 0, 0}},
                     StreamRowData<int64_t>{{{1, 2}, {2, 1}}, {0, 0}});

    // RUN 2
    // Data:
    // c0 c1 op
    //  1 1 INSERT
    //  2 2 INSERT
    //  1 2 INSERT
    //
    // last run(intermediate) data
    // c0, count(c1), op
    // 1 2 0
    // 2 1 0
    // group by c0 count(c1)
    // no  retract
    // c0, count(c1), op
    // 1 4 0
    // 2 2 0
    RunBatchAndCheck(2, StreamRowData<int64_t>{{{1, 2, 1}, {1, 2, 2}}, {0, 0, 0}},
                     StreamRowData<int64_t>{{{1, 2}, {4, 2}}, {0, 0}});

    // RUN 3
    // RUN 2
    // Data:
    // c0 c1 op
    //  1 1 INSERT
    //  2 2 INSERT
    //  3 2 INSERT
    //
    // last run(intermediate) data
    // c0, count(c1), op
    // 1 4 0
    // 2 2 0
    // group by c0 count(c1)
    // no  retract
    // c0, count(c1), op
    // 1 5 0
    // 2 3 0
    // 3 1 0
    RunBatchAndCheck(3, StreamRowData<int64_t>{{{1, 2, 3}, {1, 2, 2}}, {0, 0, 0}},
                     StreamRowData<int64_t>{{{1, 2, 3}, {5, 3, 1}}, {0, 0, 0}});

    // Final Close
    _stream_aggregator->close(_runtime_state);
}

TEST_F(CountStreamAggregateTestWithGenerateRetracts, TestWithRetracts_GenerateRetracts) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_runtime_state, &_obj_pool, _runtime_profile));
    DCHECK_IF_ERROR(_stream_aggregator->open(_runtime_state));

    // 0: StreamRowOp::INSERT,
    // 1: StreamRowOp::DELETE
    // 0: StreamRowOp::INSERT,
    // 1: StreamRowOp::DELETE
    // Data:
    // c0 c1 op
    //  1 1 INSERT
    //  2 2 INSERT
    //  1 1 DELETE
    // group by c0 count(c1)
    // with retract
    // c0, count(c1), op
    // 2 1 0 <--- group by key 0 will not be output, because it's retracted.
    RunBatchAndCheck(1, StreamRowData<int64_t>{{{1, 2, 1}, {1, 2, 2}}, {0, 0, 1}},
                     StreamRowData<int64_t>{{{2}, {1}}, {0}});
    _stream_aggregator->close(_runtime_state);
}

TEST_F(CountStreamAggregateTestWithGenerateRetracts, TestWithRetracts_MultiRuns) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_runtime_state, &_obj_pool, _runtime_profile));
    DCHECK_IF_ERROR(_stream_aggregator->open(_runtime_state));

    // 0: StreamRowOp::INSERT,
    // 1: StreamRowOp::DELETE
    // Run 1
    // Data:
    // c0 c1 op
    //  1 1 INSERT
    //  2 2 INSERT
    //  1 1 DELETE
    // group by c0 count(c1)
    // with retract
    // c0, count(c1), op
    // 2 1 INSERT <--- group by key 0 will not be output, because it's retracted.
    RunBatchAndCheck(1, StreamRowData<int64_t>{{{1, 2, 1}, {1, 2, 2}}, {0, 0, 1}},
                     StreamRowData<int64_t>{{{2}, {1}}, {0}});

    // Run 2
    // Data:
    // c0 c1 op
    //  2 2 DELETE
    //  1 1 INSERT
    //  3 1 INSERT
    // (last run) data
    // 2 1 INSERT
    // group by c0 count(c1)
    // with retract
    // c0, count(c1), op
    // 2 1 DELETE
    // 1 1 INSERT
    // 3 1 INSERT
    RunBatchAndCheck(
            2, StreamRowData<int64_t>{{{2, 1, 3}, {2, 1, 1}}, {1, 0, 0}},
            StreamRowData<int64_t>{{{2, 1, 3}, {1, 1, 1}}, {1, 0, 0}}); // TODO: make sure state table is also cleaned.

    // Run 3
    // Data:
    // c0 c1 op
    //  1 1 INSERT
    //  3 1 INSERT
    //  1 1 INSERT
    // (last run) data
    // 2 1 INSERT
    // 1 1 INSERT
    // 3 1 INSERT
    // group by c0 count(c1)
    // with retract
    // c0, count(c1), op
    // 2 1 INSERT
    // 1 1 UPDATE_BEFORE
    // 1 3 UPDATE_AFTER
    // 3 1 UPDATE_BEFORE
    // 3 1 UPDATE_AFTER
    RunBatchAndCheck(3, StreamRowData<int64_t>{{{2, 1, 3}, {2, 1, 1}}, {0, 0, 0}},
                     StreamRowData<int64_t>{{{2, 1, 1, 3, 3}, {1, 1, 2, 1, 2}},
                                            {0, 2, 3, 2, 3}}); // make sure state table is also cleaned.

    _stream_aggregator->close(_runtime_state);
}

///////////////  Min/Max Aggregate Function ///////////////
class SumAvgCountStreamAggregateTest : public StreamAggregateTestBase {
public:
    SumAvgCountStreamAggregateTest(bool is_generate_retract) : StreamAggregateTestBase(is_generate_retract) {}

    void SetUp() override {
        StreamAggregateTestBase::SetUp();
        _slot_infos = std::vector<std::vector<SlotTypeInfo>>{
                // input slots
                {
                        {"col1", TYPE_BIGINT, false},
                        {"col2", TYPE_BIGINT, false},
                },
                // intermediate slots
                {
                        {"col1", TYPE_BIGINT, false},
                        {"sum_agg", TYPE_BIGINT, false},
                        {"avg_agg", TYPE_VARBINARY, false},
                        {"count_agg", TYPE_BIGINT, false},
                },
                // result slots
                {
                        {"col1", TYPE_BIGINT, false},
                        {"sum_agg", TYPE_BIGINT, false},
                        {"avg_agg", TYPE_DOUBLE, false},
                        {"count_agg", TYPE_BIGINT, false},
                },
        };
        _group_by_infos = {0};

        // sum(col2), avg(col2), count(col2) group by col1
        _agg_infos = std::vector<AggInfo>{// slot_index, agg_name, agg_intermediate_type, agg_result_type
                                          {1, "sum", TYPE_BIGINT, TYPE_BIGINT},
                                          {1, "avg", TYPE_VARBINARY, TYPE_DOUBLE},
                                          {1, "count", TYPE_BIGINT, TYPE_BIGINT}};

        _tbl = GenerateDescTbl(_runtime_state, _obj_pool, _slot_infos);
        _runtime_state->set_desc_tbl(_tbl);
        _stream_aggregator =
                _create_stream_aggregator(_slot_infos, _group_by_infos, _agg_infos, _is_generate_retract, 2);
    }
};

class SumAvgStreamAggregateTestWithoutRetract : public SumAvgCountStreamAggregateTest {
public:
    SumAvgStreamAggregateTestWithoutRetract() : SumAvgCountStreamAggregateTest(false) {}
};

TEST_F(SumAvgStreamAggregateTestWithoutRetract, TestNoRetracts_OneRun) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_runtime_state, &_obj_pool, _runtime_profile));
    DCHECK_IF_ERROR(_stream_aggregator->open(_runtime_state));

    auto result_chunk_ptr = std::make_shared<StreamChunk>();
    auto intermediate_chunk_ptr = std::make_shared<Chunk>();
    std::vector<ChunkPtr> detail_chunks;
    RunBatch(1, StreamRowData<int64_t>{{{1, 2, 1}, {1, 2, 2}}, {0, 0, 0}}, &result_chunk_ptr, &intermediate_chunk_ptr,
             detail_chunks);

    // group by key: col1
    CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(0), {1, 2});
    // sum(col2)
    CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(1), {3, 2});
    // avg(col2)
    CheckColumn<double>(result_chunk_ptr->get_column_by_index(2), {1.5, 2});
    // count(col2)
    CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(3), {2, 1});
    // ops
    CheckColumn(StreamChunkConverter::ops(result_chunk_ptr), {0, 0});

    DCHECK_EQ(intermediate_chunk_ptr->num_columns(), 2);
    DCHECK_EQ(intermediate_chunk_ptr->num_rows(), 2);

    DCHECK(detail_chunks.empty());
    _stream_aggregator->close(_runtime_state);
}

TEST_F(SumAvgStreamAggregateTestWithoutRetract, TestNoRetracts_MultiRun) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_runtime_state, &_obj_pool, _runtime_profile));
    DCHECK_IF_ERROR(_stream_aggregator->open(_runtime_state));
    // Run 1
    auto result_chunk_ptr = std::make_shared<StreamChunk>();
    auto intermediate_chunk_ptr = std::make_shared<Chunk>();
    std::vector<ChunkPtr> detail_chunks;
    RunBatch(1, StreamRowData<int64_t>{{{1, 2, 1}, {1, 2, 2}}, {0, 0, 0}}, &result_chunk_ptr, &intermediate_chunk_ptr,
             detail_chunks);
    // group by key: col1
    CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(0), {1, 2});
    // sum(col2)
    CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(1), {3, 2});
    // avg(col2)
    CheckColumn<double>(result_chunk_ptr->get_column_by_index(2), {1.5, 2});
    // count(col2)
    CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(3), {2, 1});
    // ops
    CheckColumn(StreamChunkConverter::ops(result_chunk_ptr), {0, 0});
    DCHECK_EQ(intermediate_chunk_ptr->num_columns(), 2);
    DCHECK_EQ(intermediate_chunk_ptr->num_rows(), 2);
    DCHECK(detail_chunks.empty());

    // Run 2
    RunBatch(2, StreamRowData<int64_t>{{{1, 2, 3}, {3, 4, 3}}, {0, 0, 0}}, &result_chunk_ptr, &intermediate_chunk_ptr,
             detail_chunks);
    // group by key: col1
    CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(0), {1, 2, 3});
    // sum(col2)
    CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(1), {6, 6, 3});
    // avg(col2)
    CheckColumn<double>(result_chunk_ptr->get_column_by_index(2), {2, 3, 3});
    // count(col2)
    CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(3), {3, 2, 1});
    // ops
    CheckColumn(StreamChunkConverter::ops(result_chunk_ptr), {0, 0, 0});
    DCHECK_EQ(intermediate_chunk_ptr->num_columns(), 2);
    DCHECK_EQ(intermediate_chunk_ptr->num_rows(), 3);
    DCHECK(detail_chunks.empty());

    // Run 3
    RunBatch(3, StreamRowData<int64_t>{{{2, 3}, {3, 4}}, {0, 0}}, &result_chunk_ptr, &intermediate_chunk_ptr,
             detail_chunks);
    // group by key: col1
    CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(0), {2, 3});
    // sum(col2)
    CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(1), {9, 7});
    // avg(col2)
    CheckColumn<double>(result_chunk_ptr->get_column_by_index(2), {3, 3.5});
    // count(col2)
    CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(3), {3, 2});
    // ops
    CheckColumn(StreamChunkConverter::ops(result_chunk_ptr), {0, 0});
    DCHECK_EQ(intermediate_chunk_ptr->num_columns(), 2);
    DCHECK_EQ(intermediate_chunk_ptr->num_rows(), 2);
    DCHECK(detail_chunks.empty());

    _stream_aggregator->close(_runtime_state);
}

///////////////  Min/Max Aggregate Function ///////////////
class MinMaxCountStreamAggregateTest : public StreamAggregateTestBase {
public:
    MinMaxCountStreamAggregateTest(bool is_generate_retract) : StreamAggregateTestBase(is_generate_retract) {}

    void SetUp() override {
        StreamAggregateTestBase::SetUp();

        _slot_infos = std::vector<std::vector<SlotTypeInfo>>{
                // input slots
                {
                        {"col1", TYPE_BIGINT, false},
                        {"col2", TYPE_BIGINT, false},
                },
                // intermediate slots
                {
                        {"col1", TYPE_BIGINT, false},
                        {"min_agg", TYPE_BIGINT, false},
                        {"max_agg", TYPE_BIGINT, false},
                        {"count_agg", TYPE_BIGINT, false},
                },
                // result slots
                {
                        {"col1", TYPE_BIGINT, false},
                        {"min_agg", TYPE_BIGINT, false},
                        {"max_agg", TYPE_BIGINT, false},
                        {"count_agg", TYPE_BIGINT, false},
                },
        };
        _group_by_infos = {0};
        _agg_infos = std::vector<AggInfo>{// slot_index, agg_name, agg_intermediate_type, agg_result_type
                                          {1, "retract_min", TYPE_BIGINT, TYPE_BIGINT},
                                          {1, "retract_max", TYPE_BIGINT, TYPE_BIGINT},

                                          {1, "count", TYPE_BIGINT, TYPE_BIGINT}};

        _tbl = GenerateDescTbl(_runtime_state, _obj_pool, _slot_infos);
        _runtime_state->set_desc_tbl(_tbl);
        _stream_aggregator =
                _create_stream_aggregator(_slot_infos, _group_by_infos, _agg_infos, _is_generate_retract, 2);
    }
};

class MinMaxCountStreamAggregateTestWithoutRetract : public MinMaxCountStreamAggregateTest {
public:
    MinMaxCountStreamAggregateTestWithoutRetract() : MinMaxCountStreamAggregateTest(false) {}
};

TEST_F(MinMaxCountStreamAggregateTestWithoutRetract, TestNoRetracts_OneRun) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_runtime_state, &_obj_pool, _runtime_profile));
    DCHECK_IF_ERROR(_stream_aggregator->open(_runtime_state));

    RunBatchAndCheck(1, StreamRowData<int64_t>{{{1, 2, 1}, {1, 2, 2}}, {0, 0, 0}},
                     StreamRowData<int64_t>{{{1, 2}, {1, 2}, {2, 2}}, {0, 0}});
    _stream_aggregator->close(_runtime_state);
}

TEST_F(MinMaxCountStreamAggregateTestWithoutRetract, TestNoRetracts_MultiRun) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_runtime_state, &_obj_pool, _runtime_profile));
    DCHECK_IF_ERROR(_stream_aggregator->open(_runtime_state));
    // Run 1
    RunBatchAndCheck(1, StreamRowData<int64_t>{{{1, 2, 1}, {1, 2, 2}}, {0, 0, 0}},
                     StreamRowData<int64_t>{{{1, 2}, {1, 2}, {2, 2}, {2, 1}}, {0, 0}});
    // Run 2
    RunBatchAndCheck(2, StreamRowData<int64_t>{{{1, 2, 3}, {1, 3, 3}}, {0, 0, 0}},
                     StreamRowData<int64_t>{{{1, 2, 3}, {1, 2, 3}, {2, 3, 3}, {3, 2, 1}}, {0, 0, 0}});
    // Run 3
    RunBatchAndCheck(3, StreamRowData<int64_t>{{{2, 3}, {1, 4}}, {0, 0}},
                     StreamRowData<int64_t>{{{2, 3}, {1, 3}, {3, 4}, {3, 2}}, {0, 0}});
    _stream_aggregator->close(_runtime_state);
}

class MinMaxCountStreamAggregateTestWithRetract : public MinMaxCountStreamAggregateTest {
public:
    MinMaxCountStreamAggregateTestWithRetract() : MinMaxCountStreamAggregateTest(true) {}
};

TEST_F(MinMaxCountStreamAggregateTestWithRetract, TestWihRetracts_OneRun) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_runtime_state, &_obj_pool, _runtime_profile));
    DCHECK_IF_ERROR(_stream_aggregator->open(_runtime_state));

    RunBatchAndCheck(1, StreamRowData<int64_t>{{{1, 2, 1}, {1, 2, 1}}, {0, 0, 1}},
                     StreamRowData<int64_t>{{{2}, {2}, {2}, {1}}, {0}});
    _stream_aggregator->close(_runtime_state);
}

TEST_F(MinMaxCountStreamAggregateTestWithRetract, TestWihRetracts_MultiRun1) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_runtime_state, &_obj_pool, _runtime_profile));
    DCHECK_IF_ERROR(_stream_aggregator->open(_runtime_state));
    // Run 1
    // Input:
    // key  value
    // 1    0
    // 2    2
    // 1    2
    // key  min max count op
    // 1    0   2   2   0
    // 2    2   2   1   0
    RunBatchAndCheck(1, StreamRowData<int64_t>{{{1, 2, 1}, {0, 2, 2}}, {0, 0, 0}},
                     StreamRowData<int64_t>{{{1, 2}, {0, 2}, {2, 2}, {2, 1}}, {0, 0}});
    // Run 2
    // Input:
    // key  value
    // 1    1
    // 2    3
    // 3    3
    // key  min max count op
    // 1    0   2   2    UPDATE_BEFORE
    // 1    0   2   3    UPDATE_AFTER
    // 2    2   2   1    UPDATE_BEFORE
    // 2    2   3   2    UPDATE_AFTER
    // 3    3   3   1    INSERT
    RunBatchAndCheck(2, StreamRowData<int64_t>{{{1, 2, 3}, {1, 3, 3}}, {0, 0, 0}},
                     StreamRowData<int64_t>{{{1, 1, 2, 2, 3}, {0, 0, 2, 2, 3}, {2, 2, 2, 3, 3}, {2, 3, 1, 2, 1}},
                                            {2, 3, 2, 3, 0}});
    // Run 3
    // Input:
    // key  value
    // 2    1
    // 3    4
    // key  min max count op
    // 2    2   3   2    UPDATE_BEFORE
    // 2    1   3   3    UPDATE_AFTER
    // 3    3   3   1    UPDATE_BEFORE
    // 3    3   4   2    UPDATE_AFTER
    RunBatchAndCheck(3, StreamRowData<int64_t>{{{2, 3}, {1, 4}}, {0, 0}},
                     StreamRowData<int64_t>{{{2, 2, 3, 3}, {2, 1, 3, 3}, {3, 3, 3, 4}, {2, 3, 1, 2}}, {2, 3, 2, 3}});
    _stream_aggregator->close(_runtime_state);
}

TEST_F(MinMaxCountStreamAggregateTestWithRetract, TestWihRetracts_MultiRun2) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_runtime_state, &_obj_pool, _runtime_profile));
    DCHECK_IF_ERROR(_stream_aggregator->open(_runtime_state));

    // key <-> min <-> max <-> count
    // Run 1
    // Input:
    // key  value
    // 1    +1
    // 2    +2
    // 1    +2
    // key  min max count op
    // 1    1   2   2   0
    // 2    2   2   1   0
    RunBatchAndCheck(1, StreamRowData<int64_t>{{{1, 2, 1}, {1, 2, 2}}, {0, 0, 0}},
                     StreamRowData<int64_t>{{{1, 2}, {1, 2}, {2, 2}, {2, 1}}, {0, 0}});
    // Run 2
    // Input:
    // key  value
    // 1    -1
    // 2    -2
    // 3    +3
    // key  min max count op
    // 1    1   2   2   2
    // 2    2   2   1   1
    // 1    2   2   1   3
    // 3    3   3   1   0
    RunBatchAndCheck(2, StreamRowData<int64_t>{{{1, 2, 3}, {1, 2, 3}}, {1, 1, 0}},
                     StreamRowData<int64_t>{{{1, 1, 2, 3}, {1, 2, 2, 3}, {2, 2, 2, 3}, {2, 1, 1, 1}}, {2, 3, 1, 0}});
    // Run 3
    // Input:
    // key  value
    // 1    -2
    // 2    +2
    // 3    -3
    // key  min max count op
    // 1    2   2   1   1
    // 2    2   2   1   0
    // 3    3   3   1   1
    RunBatchAndCheck(3, StreamRowData<int64_t>{{{1, 2, 3}, {2, 2, 3}}, {1, 0, 1}},
                     StreamRowData<int64_t>{{{1, 2, 3}, {2, 2, 3}, {2, 2, 3}, {1, 1, 1}}, {1, 0, 1}});
    _stream_aggregator->close(_runtime_state);
}

///////////////  All Aggregate Functions ///////////////
class AllStreamAggregateFunctionsTestBase : public StreamAggregateTestBase {
public:
    AllStreamAggregateFunctionsTestBase(bool is_generate_retract) : StreamAggregateTestBase(is_generate_retract) {}

    void SetUp() override {
        StreamAggregateTestBase::SetUp();
        _slot_infos = std::vector<std::vector<SlotTypeInfo>>{
                // input slots
                {
                        {"col1", TYPE_BIGINT, false},
                        {"col2", TYPE_BIGINT, false},
                        {"col3", TYPE_BIGINT, false},
                },
                // intermediate slots
                {
                        {"col1", TYPE_BIGINT, false},
                        {"sum_agg", TYPE_BIGINT, false},
                        {"min_agg", TYPE_BIGINT, false},
                        {"avg_agg", TYPE_VARBINARY, false},
                        {"max_agg", TYPE_BIGINT, false},
                        {"count_agg", TYPE_BIGINT, false},
                },
                // result slots
                {
                        {"col1", TYPE_BIGINT, false},
                        {"sum_agg", TYPE_BIGINT, false},
                        {"min_agg", TYPE_BIGINT, false},
                        {"avg_agg", TYPE_DOUBLE, false},
                        {"max_agg", TYPE_BIGINT, false},
                        {"count_agg", TYPE_BIGINT, false},
                },
        };
        _group_by_infos = {0};

        // sum(col2), min(col3), avg(col3), max(col3), count(col2) group by col1
        _agg_infos = std::vector<AggInfo>{// slot_index, agg_name, agg_intermediate_type, agg_result_type
                                          {1, "sum", TYPE_BIGINT, TYPE_BIGINT},
                                          {2, "retract_min", TYPE_BIGINT, TYPE_BIGINT},
                                          {2, "avg", TYPE_VARBINARY, TYPE_DOUBLE},
                                          {1, "retract_max", TYPE_BIGINT, TYPE_BIGINT},
                                          {1, "count", TYPE_BIGINT, TYPE_BIGINT}};

        _tbl = GenerateDescTbl(_runtime_state, _obj_pool, _slot_infos);
        _runtime_state->set_desc_tbl(_tbl);
        _stream_aggregator =
                _create_stream_aggregator(_slot_infos, _group_by_infos, _agg_infos, _is_generate_retract, 4);
    }
};

class AllStreamAggregateFunctionsTestWithoutRetract : public AllStreamAggregateFunctionsTestBase {
public:
    AllStreamAggregateFunctionsTestWithoutRetract() : AllStreamAggregateFunctionsTestBase(false) {}
};

TEST_F(AllStreamAggregateFunctionsTestWithoutRetract, TestNoRetracts_OneRun) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_runtime_state, &_obj_pool, _runtime_profile));
    DCHECK_IF_ERROR(_stream_aggregator->open(_runtime_state));

    auto result_chunk_ptr = std::make_shared<StreamChunk>();
    auto intermediate_chunk_ptr = std::make_shared<Chunk>();
    std::vector<ChunkPtr> detail_chunks;
    RunBatch(1, StreamRowData<int64_t>{{{1, 2, 1}, {1, 2, 2}, {1, 1, 3}}, {0, 0, 0}}, &result_chunk_ptr,
             &intermediate_chunk_ptr, detail_chunks);

    // group by key: col1
    CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(0), {1, 2});
    // sum(col2)
    CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(1), {3, 2});
    // min(col3)
    CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(2), {1, 1});
    // avg(col3)
    CheckColumn<double>(result_chunk_ptr->get_column_by_index(3), {2, 1});
    // max(col2)
    CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(4), {2, 2});
    // count(col2)
    CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(5), {2, 1});
    // ops
    CheckColumn(StreamChunkConverter::ops(result_chunk_ptr), {0, 0});
    DCHECK_EQ(intermediate_chunk_ptr->num_columns(), 2);
    DCHECK_EQ(intermediate_chunk_ptr->num_rows(), 2);
    DCHECK_EQ(detail_chunks.size(), 2);

    _stream_aggregator->close(_runtime_state);
}

TEST_F(AllStreamAggregateFunctionsTestWithoutRetract, TestNoRetracts_MultiRun) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_runtime_state, &_obj_pool, _runtime_profile));
    DCHECK_IF_ERROR(_stream_aggregator->open(_runtime_state));
    auto result_chunk_ptr = std::make_shared<StreamChunk>();
    auto intermediate_chunk_ptr = std::make_shared<Chunk>();

    // NOTE: binary/slice is not copied into datum, need this vector to track all chunk of state' life .
    std::vector<ChunkPtr> result_chunks;
    {
        std::vector<ChunkPtr> detail_chunks;
        // Run 1
        RunBatch(1, StreamRowData<int64_t>{{{1, 2, 1}, {1, 2, 2}, {1, 1, 3}}, {0, 0, 0}}, &result_chunk_ptr,
                 &intermediate_chunk_ptr, detail_chunks);
        result_chunks.push_back(intermediate_chunk_ptr);

        // group by key: col1
        CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(0), {1, 2});
        // sum(col2)
        CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(1), {3, 2});
        // min(col3)
        CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(2), {1, 1});
        // avg(col3)
        CheckColumn<double>(result_chunk_ptr->get_column_by_index(3), {2, 1});
        // max(col2)
        CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(4), {2, 2});
        // count(col2)
        CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(5), {2, 1});
        // ops
        CheckColumn(StreamChunkConverter::ops(result_chunk_ptr), {0, 0});
    }

    // Run 2
    {
        std::vector<ChunkPtr> detail_chunks;
        RunBatch(2, StreamRowData<int64_t>{{{2, 3}, {2, 2}, {1, 3}}, {0, 0}}, &result_chunk_ptr,
                 &intermediate_chunk_ptr, detail_chunks);
        result_chunks.push_back(intermediate_chunk_ptr);
        // group by key: col1
        CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(0), {2, 3});
        // sum(col2)
        CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(1), {4, 2});
        // min(col3)
        CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(2), {1, 3});
        // avg(col3)
        CheckColumn<double>(result_chunk_ptr->get_column_by_index(3), {1, 3});
        // max(col2)
        CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(4), {2, 2});
        // count(col2)
        CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(5), {2, 1});
        // ops
        CheckColumn(StreamChunkConverter::ops(result_chunk_ptr), {0, 0});
    }

    // Run 3
    {
        std::vector<ChunkPtr> detail_chunks;
        RunBatch(3, StreamRowData<int64_t>{{{1, 3}, {2, 2}, {2, 3}}, {0, 0}}, &result_chunk_ptr,
                 &intermediate_chunk_ptr, detail_chunks);
        result_chunks.push_back(intermediate_chunk_ptr);
        // group by key: col1
        CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(0), {1, 3});
        // sum(col2)
        CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(1), {5, 4});
        // min(col3)
        CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(2), {1, 3});
        // avg(col3)
        CheckColumn<double>(result_chunk_ptr->get_column_by_index(3), {2, 3});
        // max(col2)
        CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(4), {2, 2});
        // count(col2)
        CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(5), {3, 2});
        // ops
        CheckColumn(StreamChunkConverter::ops(result_chunk_ptr), {0, 0});
    }

    _stream_aggregator->close(_runtime_state);
}

// String
class AllStreamAggregateFunctionsTestBaseWithString : public StreamAggregateTestBase {
public:
    AllStreamAggregateFunctionsTestBaseWithString(bool is_generate_retract)
            : StreamAggregateTestBase(is_generate_retract) {}

    void SetUp() override {
        StreamAggregateTestBase::SetUp();
        _slot_infos = std::vector<std::vector<SlotTypeInfo>>{
                // input slots
                {
                        {"col1", TYPE_VARCHAR, false},
                        {"col2", TYPE_VARCHAR, false},
                        {"col3", TYPE_VARCHAR, false},
                },
                // intermediate slots
                {
                        {"col1", TYPE_VARCHAR, false},
                        {"min_agg", TYPE_VARCHAR, false},
                        {"max_agg", TYPE_VARCHAR, false},
                        {"count_agg", TYPE_BIGINT, false},
                },
                // result slots
                {
                        {"col1", TYPE_VARCHAR, false},
                        {"min_agg", TYPE_VARCHAR, false},
                        {"max_agg", TYPE_VARCHAR, false},
                        {"count_agg", TYPE_BIGINT, false},
                },
        };
        _group_by_infos = {0};

        // sum(col2), min(col3), avg(col3), max(col3), count(col2) group by col1
        _agg_infos = std::vector<AggInfo>{// slot_index, agg_name, agg_intermediate_type, agg_result_type
                                          {2, "retract_min", TYPE_VARCHAR, TYPE_VARCHAR},
                                          {1, "retract_max", TYPE_VARCHAR, TYPE_VARCHAR},
                                          {1, "count", TYPE_BIGINT, TYPE_BIGINT}};

        _tbl = GenerateDescTbl(_runtime_state, _obj_pool, _slot_infos);
        _runtime_state->set_desc_tbl(_tbl);
        _stream_aggregator =
                _create_stream_aggregator(_slot_infos, _group_by_infos, _agg_infos, _is_generate_retract, 2);
    }
};

class AllStreamAggregateFunctionsTestWithoutRetractWithString : public AllStreamAggregateFunctionsTestBaseWithString {
public:
    AllStreamAggregateFunctionsTestWithoutRetractWithString() : AllStreamAggregateFunctionsTestBaseWithString(false) {}
};

TEST_F(AllStreamAggregateFunctionsTestWithoutRetractWithString, TestNoRetracts_OneRun) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_runtime_state, &_obj_pool, _runtime_profile));
    DCHECK_IF_ERROR(_stream_aggregator->open(_runtime_state));

    auto result_chunk_ptr = std::make_shared<StreamChunk>();
    auto intermediate_chunk_ptr = std::make_shared<Chunk>();
    std::vector<ChunkPtr> detail_chunks;
    RunBatch(1, StreamRowData<Slice>{{{"a", "b", "a"}, {"aa", "bb", "cc"}, {"ax", "bx", "cx"}}, {0, 0, 0}},
             &result_chunk_ptr, &intermediate_chunk_ptr, detail_chunks);

    // group by key: col1
    CheckColumn<Slice>(result_chunk_ptr->get_column_by_index(0), {"a", "b"});
    // min(col3)
    CheckColumn<Slice>(result_chunk_ptr->get_column_by_index(1), {"ax", "bx"});
    // max(col2)
    CheckColumn<Slice>(result_chunk_ptr->get_column_by_index(2), {"cc", "bb"});
    // count(col2)
    CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(3), {2, 1});
    // ops
    CheckColumn(StreamChunkConverter::ops(result_chunk_ptr), {0, 0});
    DCHECK_EQ(intermediate_chunk_ptr->num_columns(), 0);
    DCHECK_EQ(detail_chunks.size(), 2);

    _stream_aggregator->close(_runtime_state);
}

TEST_F(AllStreamAggregateFunctionsTestWithoutRetractWithString, TestNoRetracts_MultiRun) {
    DCHECK_IF_ERROR(_stream_aggregator->prepare(_runtime_state, &_obj_pool, _runtime_profile));
    DCHECK_IF_ERROR(_stream_aggregator->open(_runtime_state));

    // NOTE: binary/slice is not copied into datum, need this vector to track all chunk of state' life .
    std::vector<std::vector<ChunkPtr>> detail_chunks_buffer;
    std::vector<ChunkPtr> result_chunks;
    std::vector<ChunkPtr> intermediate_chunks;
    {
        auto result_chunk_ptr = std::make_shared<StreamChunk>();
        auto intermediate_chunk_ptr = std::make_shared<Chunk>();
        std::vector<ChunkPtr> detail_chunks;
        // Run 1
        RunBatch(1, StreamRowData<Slice>{{{"a", "b", "a"}, {"aa", "bb", "cc"}, {"ax", "bx", "cx"}}, {0, 0, 0}},
                 &result_chunk_ptr, &intermediate_chunk_ptr, detail_chunks);

        // group by key: col1
        CheckColumn<Slice>(result_chunk_ptr->get_column_by_index(0), {"a", "b"});
        // min(col3)
        CheckColumn<Slice>(result_chunk_ptr->get_column_by_index(1), {"ax", "bx"});
        // max(col2)
        CheckColumn<Slice>(result_chunk_ptr->get_column_by_index(2), {"cc", "bb"});
        // count(col2)
        CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(3), {2, 1});
        detail_chunks_buffer.emplace_back(detail_chunks);
        result_chunks.emplace_back(intermediate_chunk_ptr);
        intermediate_chunks.emplace_back(result_chunk_ptr);
    }

    {
        // Run 2
        auto result_chunk_ptr = std::make_shared<StreamChunk>();
        auto intermediate_chunk_ptr = std::make_shared<Chunk>();
        std::vector<ChunkPtr> detail_chunks;
        RunBatch(2, StreamRowData<Slice>{{{"a", "b", "c"}, {"ee", "bb", "dd"}, {"ex", "bx", "fx"}}, {0, 0, 0}},
                 &result_chunk_ptr, &intermediate_chunk_ptr, detail_chunks);
        // group by key: col1
        CheckColumn<Slice>(result_chunk_ptr->get_column_by_index(0), {"a", "b", "c"});
        // min(col3)
        CheckColumn<Slice>(result_chunk_ptr->get_column_by_index(1), {"ax", "bx", "fx"});
        // max(col2)
        CheckColumn<Slice>(result_chunk_ptr->get_column_by_index(2), {"ee", "bb", "dd"});
        // count(col2)
        CheckColumn<int64_t>(result_chunk_ptr->get_column_by_index(3), {3, 2, 1});
        intermediate_chunks.emplace_back(intermediate_chunk_ptr);
        result_chunks.push_back(result_chunk_ptr);
        detail_chunks_buffer.push_back(detail_chunks);
    }

    _stream_aggregator->close(_runtime_state);
}

} // namespace starrocks::stream
