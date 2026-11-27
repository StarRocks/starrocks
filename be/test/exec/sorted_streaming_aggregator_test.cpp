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

#include "exec/sorted_streaming_aggregator.h"

#include <gtest/gtest.h>

#include <map>
#include <new>
#include <set>

#include "base/testutil/assert.h"
#include "column/array_column.h"
#include "column/fixed_length_column.h"
#include "common/configbase.h"
#include "common/memory/column_allocator.h"
#include "exprs/agg/array_agg.h"
#include "runtime/runtime_state.h"
#include "testutil/desc_tbl_builder.h"

namespace starrocks {

class TrackingColumnAllocator final : public MemHookAllocator {
public:
    void* checked_alloc(size_t size) override {
        void* ptr = MemHookAllocator::checked_alloc(size);
        _allocations.emplace(ptr, size);
        bytes += size;
        return ptr;
    }

    void free(void* ptr) override {
        auto it = _allocations.find(ptr);
        if (it != _allocations.end()) {
            bytes -= it->second;
            _allocations.erase(it);
        }
        MemHookAllocator::free(ptr);
    }

    size_t bytes = 0;

private:
    std::map<void*, size_t> _allocations;
};

// Use the native ARRAY_AGG state and operations. Count state lifetimes outside the memory pool.
class CountingArrayAgg final
        : public AggregateFunctionBatchHelper<ArrayAggAggregateState<TYPE_BIGINT, false>, CountingArrayAgg> {
public:
    void create(FunctionContext* ctx, AggDataPtr ptr) const override {
        if (created == fail_create_after) {
            throw std::bad_alloc();
        }
        _native.create(ctx, ptr);
        EXPECT_TRUE(live.insert(ptr).second);
        ++created;
    }

    void destroy(FunctionContext* ctx, AggDataPtr ptr) const override {
        auto erased = live.erase(ptr);
        EXPECT_EQ(1, erased);
        if (erased == 1) {
            _native.destroy(ctx, ptr);
            ++destroyed;
        }
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr ptr, size_t row) const override {
        _native.update(ctx, columns, ptr, row);
        EXPECT_GT(data(ptr).data_column.capacity(), 0);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr ptr, size_t row) const override {
        _native.merge(ctx, column, ptr, row);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr ptr, Column* to) const override {
        if (finalized == fail_finalize_after) {
            throw std::bad_alloc();
        }
        _native.serialize_to_column(ctx, ptr, to);
        ++finalized;
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr ptr, Column* to) const override {
        serialize_to_column(ctx, ptr, to);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t rows,
                                     MutableColumnPtr& dst) const override {
        _native.convert_to_serialize_format(ctx, src, rows, dst);
    }

    std::string get_name() const override { return "array_agg"; }

    mutable std::set<AggDataPtr> live;
    mutable size_t created = 0;
    mutable size_t destroyed = 0;
    mutable size_t finalized = 0;
    size_t fail_create_after = SIZE_MAX;
    size_t fail_finalize_after = SIZE_MAX;

private:
    ArrayAggAggregateFunction<TYPE_BIGINT, false> _native;
};

class SortedStreamingAggregatorTest : public ::testing::Test {
public:
    static void SetUpTestSuite() { ASSERT_TRUE(config::init(nullptr)); }

protected:
    void init(size_t functions = 1) {
        _runtime.set_chunk_size(16);
        _aggregator = std::make_unique<SortedStreamingAggregator>(std::make_shared<AggregatorParams>());
        auto& agg = *_aggregator;
        agg._state = &_runtime;
        agg._mem_pool = std::make_unique<MemPool>();
        agg._agg_stat = &_statistics;
        agg._needs_finalize = true;
        // Bypass expression preparation. The fixture supplies columns and native function contexts.
        agg._is_opened = true;
        auto array_type = TypeDescriptor::create_array_type(TYPE_BIGINT_DESC);
        DescriptorTblBuilder builder(&_runtime, &_objects);
        auto& tuple = builder.declare_tuple();
        tuple << TYPE_BIGINT_DESC;
        agg._group_by_types.push_back({TYPE_BIGINT_DESC, false});
        for (size_t i = 0; i < functions; ++i) {
            _functions.emplace_back(std::make_unique<CountingArrayAgg>());
            _contexts.emplace_back(FunctionContext::create_test_context({TYPE_BIGINT_DESC}, array_type));
            agg._agg_functions.push_back(_functions.back().get());
            agg._agg_fn_ctxs.push_back(_contexts.back().get());
            agg._agg_states_offsets.push_back(i * _functions.back()->size());
            agg._agg_states_total_size += _functions.back()->size();
            AggFunctionTypes types{};
            types.result_type = array_type;
            types.serde_type = array_type;
            agg._agg_fn_types.push_back(types);
            agg._is_merge_funcs.push_back(false);
            tuple << array_type;
        }
        agg._fns.resize(functions);
        agg._agg_expr_ctxs.resize(functions);
        agg._agg_input_columns.resize(functions);
        agg._agg_input_raw_columns.resize(functions);
        agg._output_tuple_desc = builder.build()->get_tuple_descriptor(0);
        agg._intermediate_tuple_desc = agg._output_tuple_desc;
        ASSERT_OK(agg.open(&_runtime));
    }

    void input(std::initializer_list<int64_t> keys) {
        auto key_column = Int64Column::create();
        auto values = Int64Column::create();
        for (auto key : keys) {
            key_column->append(key);
            values->append(key * 10);
        }
        _aggregator->_group_by_columns = {key_column};
        for (size_t i = 0; i < _functions.size(); ++i) {
            _aggregator->_agg_input_columns[i] = {values};
            _aggregator->_agg_input_raw_columns[i] = {values.get()};
        }
    }

    StatusOr<ChunkPtr> compute(std::initializer_list<int64_t> keys) {
        input(keys);
        return _aggregator->streaming_compute_agg_state(keys.size());
    }

    void close_and_check() {
        _aggregator->close(&_runtime);
        _aggregator->close(&_runtime);
        for (auto& fn : _functions) {
            EXPECT_EQ(fn->created, fn->destroyed);
            EXPECT_TRUE(fn->live.empty());
        }
        EXPECT_EQ(0, _aggregator->mem_pool()->total_allocated_bytes());
    }

    void TearDown() override { _aggregator.reset(); }

    RuntimeState _runtime;
    ObjectPool _objects;
    RuntimeProfile _profile{"sorted aggregation"};
    AggStatistics _statistics{&_profile};
    std::vector<std::unique_ptr<CountingArrayAgg>> _functions;
    std::vector<std::unique_ptr<FunctionContext>> _contexts;
    std::unique_ptr<SortedStreamingAggregator> _aggregator;
};

TEST_F(SortedStreamingAggregatorTest, EosFinalizeFailure) {
    init();
    input({1, 1});
    TrackingColumnAllocator allocator;
    ThreadLocalColumnAllocatorSetter setter(&allocator);
    ASSERT_TRUE(_aggregator->streaming_compute_agg_state(2).ok());
    EXPECT_GT(allocator.bytes, 0);
    _functions[0]->fail_finalize_after = 0;
    auto result = _aggregator->pull_eos_chunk();
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_mem_limit_exceeded());
    close_and_check();
    _aggregator.reset();
    EXPECT_EQ(0, allocator.bytes);
}

TEST_F(SortedStreamingAggregatorTest, BatchFinalizeFailure) {
    init();
    ASSERT_TRUE(compute({1, 1}).ok());
    _functions[0]->fail_finalize_after = 1;
    EXPECT_FALSE(compute({1, 1, 2, 2, 3, 4, 4}).ok());
    EXPECT_EQ(4, _functions[0]->created);
    EXPECT_EQ(1, _functions[0]->finalized);
    auto eos = _aggregator->pull_eos_chunk();
    ASSERT_TRUE(eos.ok());
    EXPECT_EQ(nullptr, eos.value());
    close_and_check();
}

TEST_F(SortedStreamingAggregatorTest, PartialCreateFailureWithTwoFunctions) {
    init(2);
    ASSERT_TRUE(compute({1, 1}).ok());
    _functions[1]->fail_create_after = 2;
    EXPECT_THROW((void)compute({1, 1, 2, 2, 3, 4}), std::bad_alloc);
    EXPECT_EQ(4, _functions[0]->created);
    EXPECT_EQ(2, _functions[1]->created);
    close_and_check();
}

TEST_F(SortedStreamingAggregatorTest, NormalOutputAndBufferReuse) {
    init(2);
    for (int64_t key = 1; key <= 5; ++key) {
        auto result = compute({key, key});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(key == 1 ? 0 : 1, result.value()->num_rows());
        if (key > 1) {
            EXPECT_EQ(key - 1, result.value()->get_column_by_index(0)->get(0).get_int64());
            auto array = result.value()->get_column_by_index(1)->get(0).get_array();
            ASSERT_EQ(2, array.size());
            EXPECT_EQ((key - 1) * 10, array[0].get_int64());
            EXPECT_EQ((key - 1) * 10, array[1].get_int64());
            auto second_array = result.value()->get_column_by_index(2)->get(0).get_array();
            ASSERT_EQ(2, second_array.size());
            EXPECT_EQ((key - 1) * 10, second_array[0].get_int64());
            EXPECT_EQ((key - 1) * 10, second_array[1].get_int64());
        }
    }
    auto result = _aggregator->pull_eos_chunk();
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->num_rows());
    EXPECT_EQ(5, result.value()->get_column_by_index(0)->get(0).get_int64());
    EXPECT_EQ(2, result.value()->get_column_by_index(1)->get(0).get_array().size());
    ASSERT_TRUE(_aggregator->pull_eos_chunk().ok());
    EXPECT_EQ(nullptr, _aggregator->pull_eos_chunk().value());
    close_and_check();
}

} // namespace starrocks
