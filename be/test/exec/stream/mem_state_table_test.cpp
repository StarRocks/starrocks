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

#include "exec/stream/state/mem_state_table.h"

#include <gtest/gtest.h>

#include <vector>

#include "exec/stream/stream_test.h"
#include "testutil/desc_tbl_helper.h"

namespace starrocks::stream {

class MemStateTableTest : public StreamTestBase {
public:
    MemStateTableTest() = default;
    ~MemStateTableTest() override = default;

    void SetUp() override {
        _runtime_state = _obj_pool.add(new RuntimeState(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr));
        _runtime_profile = _runtime_state->runtime_profile();
        _mem_tracker = std::make_unique<MemTracker>();
        std::vector<SlotTypeInfo> src_slots = std::vector<SlotTypeInfo>{
                {"col1", TYPE_INT, false},
                {"col2", TYPE_INT, false},
                {"col3", TYPE_INT, false},
                {"agg1", TYPE_INT, false},
        };
        auto slot_type_info_arrays = DescTblHelper::create_slot_type_desc_info_arrays({src_slots});
        _tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, slot_type_info_arrays);
        _runtime_state->set_desc_tbl(_tbl);
    }
    void TearDown() override {}

protected:
    void check_seek(StateTable* state_table, const std::vector<int32_t>& keys, const std::vector<int32_t>& ans) {
        auto key_cols = _make_key_columnss(keys);
        StateTableResult result;
        auto status = state_table->seek(key_cols, result);
        DCHECK(status.ok());
        auto& found = result.found;
        auto& chunk = result.result_chunk;
        DCHECK_EQ(found.size(), 1);
        DCHECK_EQ(chunk->num_rows(), 1);
        _check_result(chunk, ans, 0);
    }

    void check_prefix_scan(StateTable* state_table, const std::vector<int32_t>& keys,
                           const std::vector<std::vector<int32_t>>& expect_rows) {
        auto key_cols = _make_key_columnss(keys);
        auto chunk_iter_or = state_table->prefix_scan(key_cols, 0);
        DCHECK(chunk_iter_or.ok());
        auto chunk_iter = chunk_iter_or.value();

        ChunkPtr chunk = ChunkHelper::new_chunk(chunk_iter->schema(), 1);
        auto status = chunk_iter->get_next(chunk.get());
        DCHECK(status.ok());
        chunk_iter->close();
        DCHECK_EQ(chunk->num_rows(), expect_rows.size());
        for (auto i = 0; i < chunk->num_rows(); i++) {
            _check_result(chunk, expect_rows[i], i);
        }
        {
            // iterator should reach the end of file.
            status = chunk_iter->get_next(chunk.get());
            DCHECK(status.is_end_of_file());
        }
    }

    void check_seek_not_found(StateTable* state_table, const std::vector<int32_t>& keys) {
        auto key_cols = _make_key_columnss(keys);
        StateTableResult result;
        auto status = state_table->seek(key_cols, result);
        DCHECK(status.ok());
        _check_not_found(result);
    }

    void check_prefix_scan_not_found(StateTable* state_table, const std::vector<int32_t>& keys,
                                     const Status& expect_status) {
        auto key_cols = _make_key_columnss(keys);
        auto iter_or = state_table->prefix_scan(key_cols, 0);
        DCHECK(!iter_or.ok());
        DCHECK(iter_or.status().code() == expect_status.code());
    }

private:
    void _check_not_found(const StateTableResult& result) {
        DCHECK_EQ(result.found.size(), 1);
        DCHECK_EQ(result.found[0], 0);
        DCHECK_EQ(result.result_chunk->num_rows(), 0);
    }

    void _check_result(ChunkPtr chunk, const std::vector<int32_t>& ans, int32_t row_idx) {
        auto num_cols = ans.size();
        DCHECK_EQ(chunk->num_columns(), num_cols);
        for (size_t i = 0; i < num_cols; i++) {
            auto col = chunk->get_column_by_index(i);
            DCHECK_EQ((col->get(row_idx)).get_int32(), ans[i]);
        }
    }

    Columns _make_key_columnss(const std::vector<int32_t>& keys) {
        Columns cols;
        for (auto& key : keys) {
            cols.push_back(ColumnTestHelper::build_column<int32_t>({key}));
        }
        return cols;
    }

protected:
    RuntimeState* _runtime_state;
    ObjectPool _obj_pool;
    DescriptorTbl* _tbl;
    RuntimeProfile* _runtime_profile;
    std::unique_ptr<MemTracker> _mem_tracker;
};

TEST_F(MemStateTableTest, TestSeekKey) {
    auto tuple_desc = _tbl->get_tuple_descriptor(0);
    auto state_table = std::make_unique<MemStateTable>(tuple_desc->slots(), 1);
    // test not exists
    check_seek_not_found(state_table.get(), {1});

    auto chunk_ptr = MakeStreamChunk<int32_t>({{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {11, 12, 13}}, {0, 0, 0});
    // write table
    state_table->write(_runtime_state, chunk_ptr);
    // read table
    check_seek(state_table.get(), {1}, {1, 1, 11});
    check_seek(state_table.get(), {2}, {2, 2, 12});
    check_seek(state_table.get(), {3}, {3, 3, 13});

    // UPDATE keys
    auto chunk_ptr2 = MakeStreamChunk<int32_t>({{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {21, 22, 23}}, {0, 0, 0});
    // write table
    state_table->write(_runtime_state, chunk_ptr2);
    // read table
    check_seek(state_table.get(), {1}, {1, 1, 21});
    check_seek(state_table.get(), {2}, {2, 2, 22});
    check_seek(state_table.get(), {3}, {3, 3, 23});
}

TEST_F(MemStateTableTest, TestPrefixSeek) {
    auto tuple_desc = _tbl->get_tuple_descriptor(0);
    auto state_table = std::make_unique<MemStateTable>(tuple_desc->slots(), 3);
    auto chunk_ptr = MakeStreamChunk<int32_t>({{1, 1, 1}, {1, 1, 1}, {1, 2, 3}, {11, 12, 13}}, {0, 0, 0});
    // test not exists
    check_prefix_scan_not_found(state_table.get(), {1, 1}, Status::EndOfFile(""));

    // write table
    state_table->write(_runtime_state, chunk_ptr);
    // read table
    check_prefix_scan(state_table.get(), {1, 1},
                      {
                              {1, 11},
                              {2, 12},
                              {3, 13},
                      });

    // UPDATE keys
    auto chunk_ptr2 = MakeStreamChunk<int32_t>({{1, 1, 1}, {1, 1, 1}, {1, 2, 3}, {21, 22, 23}}, {0, 0, 0});
    // write table
    state_table->write(_runtime_state, chunk_ptr2);
    // read table
    check_prefix_scan(state_table.get(), {1, 1},
                      {
                              {1, 21},
                              {2, 22},
                              {3, 23},
                      });
}

} // namespace starrocks::stream
