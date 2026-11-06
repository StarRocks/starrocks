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

// Returns its input, and records the most calls that were inside do_evaluate of one stub at once.
class EchoArrowFuncCallStub final : public AbstractArrowFuncCallStub {
public:
    EchoArrowFuncCallStub(FunctionContext* ctx, std::atomic<int>* max_in_flight_per_stub,
                          std::atomic<int>* max_in_flight_total, std::atomic<int>* in_flight_total)
            : AbstractArrowFuncCallStub(ctx),
              _max_in_flight_per_stub(max_in_flight_per_stub),
              _max_in_flight_total(max_in_flight_total),
              _in_flight_total(in_flight_total) {}

protected:
    StatusOr<std::shared_ptr<RecordBatch>> do_evaluate(RecordBatch&& batch) override {
        record_max(_max_in_flight_per_stub, ++_in_flight);
        record_max(_max_in_flight_total, ++*_in_flight_total);
        // Widen the window so that overlapping calls are observed.
        std::this_thread::sleep_for(std::chrono::microseconds(200));
        auto result = arrow::RecordBatch::Make(batch.schema(), batch.num_rows(), batch.columns());
        --*_in_flight_total;
        --_in_flight;
        return result;
    }

private:
    static void record_max(std::atomic<int>* max, int now) {
        int prev = max->load();
        while (now > prev && !max->compare_exchange_weak(prev, now)) {
        }
    }

    std::atomic<int> _in_flight{0};
    std::atomic<int>* _max_in_flight_per_stub;
    std::atomic<int>* _max_in_flight_total;
    std::atomic<int>* _in_flight_total;
};

struct EchoStubFactory {
    std::unique_ptr<FunctionContext> ctx{
            FunctionContext::create_test_context({TypeDescriptor(TYPE_BIGINT)}, TypeDescriptor(TYPE_BIGINT))};
    std::atomic<int> max_in_flight_per_stub{0};
    std::atomic<int> max_in_flight_total{0};
    std::atomic<int> in_flight_total{0};

    UDFCallStubPool::StubBuilder builder() {
        return [this]() -> std::unique_ptr<UDFCallStub> {
            return std::make_unique<EchoArrowFuncCallStub>(ctx.get(), &max_in_flight_per_stub, &max_in_flight_total,
                                                           &in_flight_total);
        };
    }
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

// The stub is pooled across ExprContext clones, so it must not depend on the FunctionContext it was
// built from: a scan data source frees its cloned conjunct contexts while the stub stays pooled.
TEST(ArrowFuncCallStubTest, OutlivesFunctionContext) {
    EchoStubFactory factory;
    auto stub = factory.builder()();
    factory.ctx.reset();

    ASSIGN_OR_ABORT(auto result, stub->evaluate({make_bigint_column(100, 16)}, 16));
    assert_echoed(result, 100, 16);
}

TEST(UDFCallStubPoolTest, SequentialCallsReuseOneStub) {
    EchoStubFactory factory;
    UDFCallStubPool pool;
    for (int i = 0; i < 10; ++i) {
        ASSIGN_OR_ABORT(auto result, pool.evaluate(factory.builder(), {make_bigint_column(i * 100, 8)}, 8));
        assert_echoed(result, i * 100, 8);
    }
    ASSERT_EQ(1, pool.num_built());
    ASSERT_EQ(1, pool.num_idle());
}

// A pipeline driver's scan IO tasks run on different threads and evaluate the same conjunct: they
// must each get a stub of their own, run in parallel, and each see its own batch's reply.
TEST(UDFCallStubPoolTest, ConcurrentCallsUseSeparateStubs) {
    EchoStubFactory factory;
    UDFCallStubPool pool;

    constexpr int kThreads = 8;
    constexpr int kCallsPerThread = 50;
    std::atomic<int> failures{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < kCallsPerThread; ++i) {
                int64_t start = (t * kCallsPerThread + i) * 1000L;
                size_t num_rows = 1 + (t * 7 + i) % 64;
                auto result = pool.evaluate(factory.builder(), {make_bigint_column(start, num_rows)}, num_rows);
                if (!result.ok() || result.value()->size() != num_rows) {
                    ++failures;
                    continue;
                }
                const auto* data = down_cast<const Int64Column*>(ColumnHelper::get_data_column(result.value().get()));
                if (data->get_data()[0] != start || data->get_data()[num_rows - 1] != start + num_rows - 1) {
                    ++failures;
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    ASSERT_EQ(0, failures.load());
    // No stub is ever used by two calls at once ...
    ASSERT_EQ(1, factory.max_in_flight_per_stub.load());
    // ... yet calls did run in parallel, on more than one stub, never more than there were callers.
    ASSERT_GT(factory.max_in_flight_total.load(), 1);
    ASSERT_GT(pool.num_built(), 1);
    ASSERT_LE(pool.num_built(), kThreads);
    ASSERT_EQ(pool.num_built(), pool.num_idle());
}

// A failed call may leave an unread reply on the stub's stream; the next call must not inherit it.
TEST(UDFCallStubPoolTest, FailedStubIsNotReused) {
    UDFCallStubPool pool;
    int builds = 0;
    auto failing = [&]() -> std::unique_ptr<UDFCallStub> {
        ++builds;
        return create_error_call_stub(Status::InternalError("worker died"));
    };
    for (int i = 0; i < 3; ++i) {
        auto result = pool.evaluate(failing, {make_bigint_column(0, 4)}, 4);
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(0, pool.num_idle());
    }
    ASSERT_EQ(3, builds);
    ASSERT_EQ(3, pool.num_built());

    EchoStubFactory factory;
    ASSIGN_OR_ABORT(auto result, pool.evaluate(factory.builder(), {make_bigint_column(7, 4)}, 4));
    assert_echoed(result, 7, 4);
    ASSERT_EQ(1, pool.num_idle());
}

} // namespace starrocks
