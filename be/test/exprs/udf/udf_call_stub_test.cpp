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

#include "exprs/udf/udf_call_stub.h"

#include <arrow/record_batch.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "base/testutil/assert.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "exprs/function_context.h"
#include "types/type_descriptor.h"

namespace starrocks {

namespace {

// Returns its input, and records how many calls were inside do_evaluate at once.
class EchoArrowFuncCallStub final : public AbstractArrowFuncCallStub {
public:
    explicit EchoArrowFuncCallStub(FunctionContext* ctx) : AbstractArrowFuncCallStub(ctx) {}

    int max_in_flight() const { return _max_in_flight.load(); }

protected:
    StatusOr<std::shared_ptr<RecordBatch>> do_evaluate(RecordBatch&& batch) override {
        int now = ++_in_flight;
        int prev = _max_in_flight.load();
        while (now > prev && !_max_in_flight.compare_exchange_weak(prev, now)) {
        }
        // Widen the window so that overlapping calls are observed.
        std::this_thread::sleep_for(std::chrono::microseconds(200));
        auto result = arrow::RecordBatch::Make(batch.schema(), batch.num_rows(), batch.columns());
        --_in_flight;
        return result;
    }

private:
    std::atomic<int> _in_flight{0};
    std::atomic<int> _max_in_flight{0};
};

ColumnPtr make_bigint_column(int64_t start, size_t num_rows) {
    auto column = Int64Column::create();
    for (size_t i = 0; i < num_rows; ++i) {
        column->append(start + static_cast<int64_t>(i));
    }
    return column;
}

void assert_echoed(const ColumnPtr& result, int64_t start, size_t num_rows) {
    ASSERT_EQ(num_rows, result->size());
    const auto* data = down_cast<const Int64Column*>(ColumnHelper::get_data_column(result.get()));
    for (size_t i = 0; i < num_rows; ++i) {
        ASSERT_FALSE(result->is_null(i));
        ASSERT_EQ(start + static_cast<int64_t>(i), data->get_data()[i]);
    }
}

} // namespace

// The stub is cached across ExprContext clones, so it must not depend on the FunctionContext it was
// built from: a scan data source frees its cloned conjunct contexts while the stub stays cached.
TEST(ArrowFuncCallStubTest, OutlivesFunctionContext) {
    auto ctx = std::unique_ptr<FunctionContext>(
            FunctionContext::create_test_context({TypeDescriptor(TYPE_BIGINT)}, TypeDescriptor(TYPE_BIGINT)));
    EchoArrowFuncCallStub stub(ctx.get());
    ctx.reset();

    ASSIGN_OR_ABORT(auto result, stub.evaluate({make_bigint_column(100, 16)}, 16));
    assert_echoed(result, 100, 16);
}

// A pipeline driver's scan IO tasks run on different threads but share the driver's stub; each call
// must see its own batch's reply.
TEST(ArrowFuncCallStubTest, ConcurrentCallsDoNotOverlap) {
    auto ctx = std::unique_ptr<FunctionContext>(
            FunctionContext::create_test_context({TypeDescriptor(TYPE_BIGINT)}, TypeDescriptor(TYPE_BIGINT)));
    EchoArrowFuncCallStub stub(ctx.get());

    constexpr int kThreads = 8;
    constexpr int kCallsPerThread = 50;
    std::atomic<int> failures{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < kCallsPerThread; ++i) {
                int64_t start = (t * kCallsPerThread + i) * 1000L;
                size_t num_rows = 1 + (t * 7 + i) % 64;
                auto result = stub.evaluate({make_bigint_column(start, num_rows)}, num_rows);
                if (!result.ok() || result.value()->size() != num_rows) {
                    ++failures;
                    continue;
                }
                const auto* data = down_cast<const Int64Column*>(ColumnHelper::get_data_column(result.value().get()));
                if (data->get_data()[0] != start) {
                    ++failures;
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    ASSERT_EQ(0, failures.load());
    ASSERT_EQ(1, stub.max_in_flight());
}

} // namespace starrocks
