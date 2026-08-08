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

#include <benchmark/benchmark.h>

#include <cstdint>
#include <memory>
#include <random>
#include <string>

#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "exprs/agg/aggregate_state_allocator.h"
#include "exprs/agg/maxmin_by.h"
#include "exprs/function_context.h"
#include "runtime/mem_pool.h"
#include "runtime/memory/counting_allocator.h"

namespace starrocks {

enum class KeyOrder {
    ASCENDING,
    RANDOM,
    DESCENDING,
};

class ManagedAggregateState {
public:
    ManagedAggregateState(FunctionContext* context, const AggregateFunction* function)
            : _context(context), _function(function) {
        _state = _mem_pool.allocate_aligned(function->size(), function->alignof_size());
        _function->create(_context, _state);
    }

    ~ManagedAggregateState() { _function->destroy(_context, _state); }

    AggDataPtr state() const { return _state; }

    void reset() {
        static const Columns empty_args;
        _function->reset(_context, empty_args, _state);
    }

private:
    FunctionContext* _context;
    const AggregateFunction* _function;
    MemPool _mem_pool;
    AggDataPtr _state;
};

static Int32Column::MutablePtr create_keys(size_t row_count, KeyOrder order) {
    auto keys = Int32Column::create();
    auto& data = keys->get_data();
    data.resize(row_count);

    if (order == KeyOrder::ASCENDING) {
        for (size_t i = 0; i < row_count; ++i) {
            data[i] = static_cast<int32_t>(i);
        }
    } else if (order == KeyOrder::DESCENDING) {
        for (size_t i = 0; i < row_count; ++i) {
            data[i] = static_cast<int32_t>(row_count - i);
        }
    } else {
        std::mt19937 random(0xC0FFEE);
        std::uniform_int_distribution<int32_t> distribution(0, static_cast<int32_t>(row_count * 4));
        for (auto& key : data) {
            key = distribution(random);
        }
    }
    return keys;
}

static BinaryColumn::MutablePtr create_values(size_t row_count, size_t value_size) {
    auto values = BinaryColumn::create();
    std::string payload(value_size, 'x');
    for (size_t i = 0; i < row_count; ++i) {
        values->append(Slice(payload));
    }
    return values;
}

template <bool use_batch, KeyOrder key_order>
static void BM_MaxBySingleState(benchmark::State& state) {
    const size_t value_size = state.range(0);
    const size_t row_count = state.range(1);
    auto values = create_values(row_count, value_size);
    auto keys = create_keys(row_count, key_order);
    const Column* columns[] = {values.get(), keys.get()};

    std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context());
    using AggregateState = MaxByAggregateData<TYPE_INT, false>;
    using AggregateFunction =
            MaxMinByAggregateFunction<TYPE_INT, AggregateState, MaxByElement<TYPE_INT, AggregateState>>;
    AggregateFunction function;

    CountingAllocatorWithHook allocator;
    SCOPED_THREAD_LOCAL_AGG_STATE_ALLOCATOR_SETTER(&allocator);
    ManagedAggregateState aggregate_state(context.get(), &function);

    for (auto _ : state) {
        aggregate_state.reset();
        if constexpr (use_batch) {
            function.update_batch_single_state(context.get(), row_count, columns, aggregate_state.state());
        } else {
            for (size_t i = 0; i < row_count; ++i) {
                function.update(context.get(), columns, aggregate_state.state(), i);
            }
        }
        benchmark::DoNotOptimize(aggregate_state.state());
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations() * row_count);
    state.SetBytesProcessed(state.iterations() * row_count * (value_size + sizeof(int32_t)));
}

BENCHMARK_TEMPLATE(BM_MaxBySingleState, false, KeyOrder::ASCENDING)
        ->Args({16, 4096})
        ->Args({128, 4096})
        ->Args({1024, 4096})
        ->ArgNames({"value_bytes", "rows"});
BENCHMARK_TEMPLATE(BM_MaxBySingleState, true, KeyOrder::ASCENDING)
        ->Args({16, 4096})
        ->Args({128, 4096})
        ->Args({1024, 4096})
        ->ArgNames({"value_bytes", "rows"});

BENCHMARK_TEMPLATE(BM_MaxBySingleState, false, KeyOrder::RANDOM)
        ->Args({16, 4096})
        ->Args({128, 4096})
        ->Args({1024, 4096})
        ->ArgNames({"value_bytes", "rows"});
BENCHMARK_TEMPLATE(BM_MaxBySingleState, true, KeyOrder::RANDOM)
        ->Args({16, 4096})
        ->Args({128, 4096})
        ->Args({1024, 4096})
        ->ArgNames({"value_bytes", "rows"});

BENCHMARK_TEMPLATE(BM_MaxBySingleState, false, KeyOrder::DESCENDING)
        ->Args({16, 4096})
        ->Args({128, 4096})
        ->Args({1024, 4096})
        ->ArgNames({"value_bytes", "rows"});
BENCHMARK_TEMPLATE(BM_MaxBySingleState, true, KeyOrder::DESCENDING)
        ->Args({16, 4096})
        ->Args({128, 4096})
        ->Args({1024, 4096})
        ->ArgNames({"value_bytes", "rows"});

} // namespace starrocks
