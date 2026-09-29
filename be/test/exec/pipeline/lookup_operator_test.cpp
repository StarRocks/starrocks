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

#include "exec/pipeline/lookup_operator.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/query_context.h"
#include "runtime/lookup_stream_mgr.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

namespace starrocks::pipeline {

class LookUpOperatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        _state.set_query_ctx(_query_ctx.get());
        auto dispatcher = std::make_shared<LookUpDispatcher>(TUniqueId(), 1, std::vector<TupleId>{});
        _factory = std::make_unique<LookUpOperatorFactory>(1, 1, _row_pos_descs, std::move(dispatcher), 1);
    }

    void TearDown() override {
        if (_op != nullptr) {
            _op->close(&_state);
        }
    }

    // The IO task's completion callback calls defer_notify() from a scan thread; call it directly here.
    void notify_from_io_task() { down_cast<LookUpOperator*>(_op.get())->defer_notify(); }

    std::shared_ptr<QueryContext> _query_ctx = std::make_shared<QueryContext>();
    RuntimeState _state;
    phmap::flat_hash_map<TupleId, RowPositionDescriptor*> _row_pos_descs;
    std::unique_ptr<LookUpOperatorFactory> _factory;
    OperatorPtr _op;
    std::unique_ptr<PipelineDriver> _driver;
};

// Under the poll scheduler (e.g. exec_mode='etl' enables wait-dependent events), drivers never get an observer.
// Finishing an IO task used to dereference the null observer and crash the BE.
TEST_F(LookUpOperatorTest, io_task_notify_without_event_scheduler) {
    _state.set_enable_event_scheduler(false);
    _op = _factory->create(1, 0);
    ASSERT_EQ(nullptr, _op->observer());
    ASSERT_OK(_op->prepare(&_state));

    notify_from_io_task();
}

// Under the event scheduler the finished IO task must still wake the driver, or the lookup source hangs.
TEST_F(LookUpOperatorTest, io_task_notify_wakes_driver_with_event_scheduler) {
    _state.set_enable_event_scheduler(true);
    _op = _factory->create(1, 0);
    // In 4.1 the observer is concrete. Verify its reschedule signal on a real, unblocked driver.
    _driver = std::make_unique<PipelineDriver>(Operators{_op}, _query_ctx.get(), nullptr, nullptr, -1);
    _op->set_observer(_driver->observer());
    ASSERT_OK(_op->prepare(&_state));

    ASSERT_FALSE(_driver->need_check_reschedule());
    notify_from_io_task();
    EXPECT_TRUE(_driver->need_check_reschedule());
}

} // namespace starrocks::pipeline
