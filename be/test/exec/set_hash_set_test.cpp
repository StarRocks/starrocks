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

#include <algorithm>

#include "base/failpoint/fail_point.h"
#include "base/testutil/assert.h"
#include "column/column_helper.h"
#include "common/object_pool.h"
#include "exec/except_hash_set.h"
#include "exec/exec_env.h"
#include "exec/intersect_hash_set.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"

namespace starrocks {

// EXCEPT/INTERSECT serialize a chunk's key columns into a fixed-stride buffer sized
// max_one_row_size * state->chunk_size(). One very wide row prices the whole chunk, so past
// INT32_MAX the operators serialize row by row into a single-row buffer instead. The two paths
// must agree byte for byte, because one query can take both -- a chunk holding a wide value goes
// by rows while the narrow chunks around it stay vectorized.
class SetHashSetTest : public ::testing::Test {
public:
    void SetUp() override {
        // A default-constructed RuntimeState has no mem trackers, and RETURN_IF_LIMIT_EXCEEDED on
        // the probe paths dereferences them.
        TUniqueId fragment_id;
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        auto* exec_env = ExecEnv::GetInstance();
        _runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals,
                                                        &exec_env->query_execution_services(), exec_env);
        _runtime_state->init_mem_trackers(TUniqueId());
        _runtime_state->set_chunk_size(kChunkSize);
        _varchar_type = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        _slot_ref = _pool.add(new ColumnRef(_varchar_type, 0));
        _exprs.emplace_back(_pool.add(new ExprContext(_slot_ref)));
        ASSERT_OK(ExprExecutor::prepare(_exprs, _runtime_state.get()));
        ASSERT_OK(ExprExecutor::open(_exprs, _runtime_state.get()));
    }

    void TearDown() override { ExprExecutor::close(_exprs, _runtime_state.get()); }

protected:
    // A stride above this forces the by-rows path: kWideValueSize * kChunkSize > INT32_MAX.
    static constexpr size_t kChunkSize = 4096;
    static constexpr size_t kWideValueSize = 1024 * 1024;

    ChunkPtr make_chunk(const std::vector<std::string>& values, bool nullable) {
        auto column = ColumnHelper::create_column(_varchar_type, nullable);
        for (const auto& value : values) {
            column->append_datum(Datum(Slice(value)));
        }
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(std::move(column), 0);
        return chunk;
    }

    static std::string wide_value(char fill) { return std::string(kWideValueSize, fill); }

    // Turning the failpoint on lets ordinary narrow data drive the by-rows path, so the two
    // serializations can be compared on identical input instead of only on half-megabyte rows.
    class ForceByRows {
    public:
        explicit ForceByRows(const std::string& name) : _name(name) {
            auto* fp = failpoint::FailPointRegistry::GetInstance()->get(_name);
            EXPECT_NE(nullptr, fp) << "failpoint " << _name << " is not registered";
            set_enabled(fp, true);
            _fp = fp;
        }
        ~ForceByRows() {
            if (_fp != nullptr) {
                set_enabled(_fp, false);
            }
        }

    private:
        static void set_enabled(failpoint::FailPoint* fp, bool enable) {
            PFailPointTriggerMode mode;
            mode.set_mode(enable ? FailPointTriggerModeType::ENABLE : FailPointTriggerModeType::DISABLE);
            fp->setMode(mode);
        }

        std::string _name;
        failpoint::FailPoint* _fp = nullptr;
    };

    // The set of keys the hash set ended up holding, so a by-rows build can be compared against a
    // batch build of the same rows.
    static std::vector<std::string> keys_of(IntersectHashSerializeSet& hash_set) {
        std::vector<std::string> keys;
        for (auto it = hash_set.begin(); it != hash_set.end(); ++it) {
            keys.emplace_back(it->slice.data, it->slice.size);
        }
        std::sort(keys.begin(), keys.end());
        return keys;
    }

    static std::vector<std::string> keys_of(ExceptHashSerializeSet& hash_set) {
        std::vector<std::string> keys;
        for (auto it = hash_set.begin(); it != hash_set.end(); ++it) {
            keys.emplace_back(it->slice.data, it->slice.size);
        }
        std::sort(keys.begin(), keys.end());
        return keys;
    }

    std::shared_ptr<RuntimeState> _runtime_state;
    ObjectPool _pool;
    TypeDescriptor _varchar_type;
    ColumnRef* _slot_ref = nullptr;
    std::vector<ExprContext*> _exprs;
};

// A chunk holding a single 1MB row would ask for 1MB * 4096 = 4GB of buffer if it were serialized
// in one batch. It must still be inserted, and found again by a later probe.
TEST_F(SetHashSetTest, intersect_wide_row_takes_by_rows_path) {
    IntersectHashSerializeSet hash_set;
    ASSERT_OK(hash_set.init(_runtime_state.get()));
    MemPool pool;

    auto wide_chunk = make_chunk({wide_value('a')}, false);
    hash_set.build_set(_runtime_state.get(), wide_chunk, _exprs, &pool);
    ASSERT_FALSE(hash_set.empty());

    // Probing the same value marks it as hit; a different value must not match.
    ASSERT_OK(hash_set.refine_intersect_row(_runtime_state.get(), wide_chunk, _exprs, 1));
    size_t hit = 0;
    for (auto it = hash_set.begin(); it != hash_set.end(); ++it) {
        hit += (it->hit_times == 1);
    }
    ASSERT_EQ(1, hit);

    auto other_chunk = make_chunk({wide_value('b')}, false);
    ASSERT_OK(hash_set.refine_intersect_row(_runtime_state.get(), other_chunk, _exprs, 2));
    for (auto it = hash_set.begin(); it != hash_set.end(); ++it) {
        ASSERT_NE(2, it->hit_times);
    }
}

// The regression this guards: a wide chunk is serialized by rows and a narrow one in a batch, so a
// value inserted through one path has to be found through the other. If the two layouts disagreed
// the lookup would silently miss.
TEST_F(SetHashSetTest, intersect_by_rows_and_batch_layouts_agree) {
    IntersectHashSerializeSet hash_set;
    ASSERT_OK(hash_set.init(_runtime_state.get()));
    MemPool pool;

    // Built by rows (1MB stride), probed in a batch (narrow stride) and vice versa.
    const std::string wide = wide_value('a');
    hash_set.build_set(_runtime_state.get(), make_chunk({wide}, false), _exprs, &pool);
    hash_set.build_set(_runtime_state.get(), make_chunk({"narrow"}, false), _exprs, &pool);

    ASSERT_OK(hash_set.refine_intersect_row(_runtime_state.get(), make_chunk({"narrow"}, false), _exprs, 1));
    ASSERT_OK(hash_set.refine_intersect_row(_runtime_state.get(), make_chunk({wide}, false), _exprs, 1));

    size_t hit = 0;
    for (auto it = hash_set.begin(); it != hash_set.end(); ++it) {
        hit += (it->hit_times == 1);
    }
    ASSERT_EQ(2, hit);
}

// A nullable column serializes its own null byte while a non-nullable one gets a false byte
// prepended, so both paths have to reproduce that for the keys to compare equal.
TEST_F(SetHashSetTest, intersect_nullable_wide_row_round_trips) {
    IntersectHashSerializeSet hash_set;
    ASSERT_OK(hash_set.init(_runtime_state.get()));
    MemPool pool;

    auto nullable_wide = make_chunk({wide_value('a')}, true);
    hash_set.build_set(_runtime_state.get(), nullable_wide, _exprs, &pool);
    ASSERT_OK(hash_set.refine_intersect_row(_runtime_state.get(), nullable_wide, _exprs, 1));

    size_t hit = 0;
    for (auto it = hash_set.begin(); it != hash_set.end(); ++it) {
        hit += (it->hit_times == 1);
    }
    ASSERT_EQ(1, hit);
}

TEST_F(SetHashSetTest, except_wide_row_takes_by_rows_path) {
    ExceptHashSerializeSet hash_set;
    ASSERT_OK(hash_set.init(_runtime_state.get()));
    ExceptBufferState buffer_state;
    ASSERT_OK(buffer_state.init(_runtime_state.get()));
    MemPool pool;

    auto wide_chunk = make_chunk({wide_value('a')}, false);
    hash_set.build_set(_runtime_state.get(), wide_chunk, _exprs, &pool, &buffer_state);
    ASSERT_EQ(1, hash_set.size());

    // Erasing with the same value marks the row deleted; an unrelated value leaves it alone.
    auto other_chunk = make_chunk({wide_value('b')}, false);
    ASSERT_OK(hash_set.erase_duplicate_row(_runtime_state.get(), other_chunk, _exprs, &buffer_state));
    for (auto it = hash_set.begin(); it != hash_set.end(); ++it) {
        ASSERT_FALSE(it->deleted);
    }

    ASSERT_OK(hash_set.erase_duplicate_row(_runtime_state.get(), wide_chunk, _exprs, &buffer_state));
    for (auto it = hash_set.begin(); it != hash_set.end(); ++it) {
        ASSERT_TRUE(it->deleted);
    }
}

TEST_F(SetHashSetTest, except_by_rows_and_batch_layouts_agree) {
    ExceptHashSerializeSet hash_set;
    ASSERT_OK(hash_set.init(_runtime_state.get()));
    ExceptBufferState buffer_state;
    ASSERT_OK(buffer_state.init(_runtime_state.get()));
    MemPool pool;

    const std::string wide = wide_value('a');
    hash_set.build_set(_runtime_state.get(), make_chunk({wide}, false), _exprs, &pool, &buffer_state);
    hash_set.build_set(_runtime_state.get(), make_chunk({"narrow"}, false), _exprs, &pool, &buffer_state);
    ASSERT_EQ(2, hash_set.size());

    // Built by rows, erased in a batch -- and the other way round.
    ASSERT_OK(hash_set.erase_duplicate_row(_runtime_state.get(), make_chunk({"narrow"}, false), _exprs, &buffer_state));
    ASSERT_OK(hash_set.erase_duplicate_row(_runtime_state.get(), make_chunk({wide}, false), _exprs, &buffer_state));

    size_t deleted = 0;
    for (auto it = hash_set.begin(); it != hash_set.end(); ++it) {
        deleted += it->deleted;
    }
    ASSERT_EQ(2, deleted);
}

// Once a wide chunk has driven the stride up, a following narrow chunk must not inherit it: that
// would keep every later chunk on the by-rows path for the rest of the query.
TEST_F(SetHashSetTest, narrow_chunk_after_wide_chunk_still_matches) {
    IntersectHashSerializeSet hash_set;
    ASSERT_OK(hash_set.init(_runtime_state.get()));
    MemPool pool;

    hash_set.build_set(_runtime_state.get(), make_chunk({wide_value('a')}, false), _exprs, &pool);

    std::vector<std::string> narrow_values;
    narrow_values.reserve(kChunkSize);
    for (size_t i = 0; i < kChunkSize; ++i) {
        narrow_values.emplace_back("v" + std::to_string(i));
    }
    hash_set.build_set(_runtime_state.get(), make_chunk(narrow_values, false), _exprs, &pool);

    ASSERT_OK(hash_set.refine_intersect_row(_runtime_state.get(), make_chunk(narrow_values, false), _exprs, 1));
    size_t hit = 0;
    for (auto it = hash_set.begin(); it != hash_set.end(); ++it) {
        hit += (it->hit_times == 1);
    }
    ASSERT_EQ(kChunkSize, hit);
}

// The two serializations must produce the same keys for the same rows. Comparing them on ordinary
// narrow data is only possible with the failpoint: naturally reaching the by-rows path takes a row
// of half a megabyte, and at that size the interesting cases (nulls, empty strings, many rows in
// one chunk) become impractical to build.
TEST_F(SetHashSetTest, intersect_failpoint_by_rows_matches_batch) {
    const std::vector<std::string> values = {"a", "", "bb", "ccc", std::string(1, '\0'), "a"};

    IntersectHashSerializeSet batch_set;
    ASSERT_OK(batch_set.init(_runtime_state.get()));
    MemPool batch_pool;
    batch_set.build_set(_runtime_state.get(), make_chunk(values, false), _exprs, &batch_pool);

    IntersectHashSerializeSet by_rows_set;
    ASSERT_OK(by_rows_set.init(_runtime_state.get()));
    MemPool by_rows_pool;
    {
        ForceByRows guard("intersect_hash_set_force_by_rows");
        by_rows_set.build_set(_runtime_state.get(), make_chunk(values, false), _exprs, &by_rows_pool);
    }

    ASSERT_EQ(keys_of(batch_set), keys_of(by_rows_set));

    // And a key built one way has to be found the other way.
    {
        ForceByRows guard("intersect_hash_set_force_by_rows");
        ASSERT_OK(batch_set.refine_intersect_row(_runtime_state.get(), make_chunk(values, false), _exprs, 1));
    }
    size_t hit = 0;
    for (auto it = batch_set.begin(); it != batch_set.end(); ++it) {
        hit += (it->hit_times == 1);
    }
    ASSERT_EQ(keys_of(batch_set).size(), hit);
}

TEST_F(SetHashSetTest, intersect_failpoint_by_rows_matches_batch_nullable) {
    auto nullable_chunk = [this]() {
        auto column = ColumnHelper::create_column(_varchar_type, true);
        column->append_datum(Datum(Slice("a")));
        column->append_nulls(1);
        column->append_datum(Datum(Slice("")));
        column->append_nulls(1);
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(std::move(column), 0);
        return chunk;
    };

    IntersectHashSerializeSet batch_set;
    ASSERT_OK(batch_set.init(_runtime_state.get()));
    MemPool batch_pool;
    batch_set.build_set(_runtime_state.get(), nullable_chunk(), _exprs, &batch_pool);

    IntersectHashSerializeSet by_rows_set;
    ASSERT_OK(by_rows_set.init(_runtime_state.get()));
    MemPool by_rows_pool;
    {
        ForceByRows guard("intersect_hash_set_force_by_rows");
        by_rows_set.build_set(_runtime_state.get(), nullable_chunk(), _exprs, &by_rows_pool);
    }

    ASSERT_EQ(keys_of(batch_set), keys_of(by_rows_set));
}

TEST_F(SetHashSetTest, except_failpoint_by_rows_matches_batch) {
    const std::vector<std::string> values = {"a", "", "bb", "ccc", std::string(1, '\0'), "a"};

    ExceptHashSerializeSet batch_set;
    ASSERT_OK(batch_set.init(_runtime_state.get()));
    ExceptBufferState batch_state;
    ASSERT_OK(batch_state.init(_runtime_state.get()));
    MemPool batch_pool;
    batch_set.build_set(_runtime_state.get(), make_chunk(values, false), _exprs, &batch_pool, &batch_state);

    ExceptHashSerializeSet by_rows_set;
    ASSERT_OK(by_rows_set.init(_runtime_state.get()));
    ExceptBufferState by_rows_state;
    ASSERT_OK(by_rows_state.init(_runtime_state.get()));
    MemPool by_rows_pool;
    {
        ForceByRows guard("except_hash_set_force_by_rows");
        by_rows_set.build_set(_runtime_state.get(), make_chunk(values, false), _exprs, &by_rows_pool, &by_rows_state);
    }

    ASSERT_EQ(keys_of(batch_set), keys_of(by_rows_set));

    // Built in a batch, erased by rows: every key must still be located.
    {
        ForceByRows guard("except_hash_set_force_by_rows");
        ASSERT_OK(batch_set.erase_duplicate_row(_runtime_state.get(), make_chunk(values, false), _exprs, &batch_state));
    }
    size_t deleted = 0;
    for (auto it = batch_set.begin(); it != batch_set.end(); ++it) {
        deleted += it->deleted;
    }
    ASSERT_EQ(batch_set.size(), deleted);
}

// A full chunk through the by-rows path: the single-row buffer is rewritten for every row, so a
// stale pointer or a missing reset would show up as lost or duplicated keys.
TEST_F(SetHashSetTest, intersect_failpoint_by_rows_full_chunk) {
    std::vector<std::string> values;
    values.reserve(kChunkSize);
    for (size_t i = 0; i < kChunkSize; ++i) {
        values.emplace_back("value_" + std::to_string(i));
    }

    IntersectHashSerializeSet hash_set;
    ASSERT_OK(hash_set.init(_runtime_state.get()));
    MemPool pool;
    {
        ForceByRows guard("intersect_hash_set_force_by_rows");
        hash_set.build_set(_runtime_state.get(), make_chunk(values, false), _exprs, &pool);
        ASSERT_OK(hash_set.refine_intersect_row(_runtime_state.get(), make_chunk(values, false), _exprs, 1));
    }

    size_t hit = 0;
    for (auto it = hash_set.begin(); it != hash_set.end(); ++it) {
        hit += (it->hit_times == 1);
    }
    ASSERT_EQ(kChunkSize, hit);
}

} // namespace starrocks
