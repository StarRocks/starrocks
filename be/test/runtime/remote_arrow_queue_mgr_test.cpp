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

#include "runtime/remote_arrow_queue_mgr.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include <memory>

namespace starrocks {

class RemoteArrowQueueMgrTest : public testing::Test {
protected:
    static TUniqueId fragment_id(int64_t hi, int64_t lo) {
        TUniqueId id;
        id.hi = hi;
        id.lo = lo;
        return id;
    }

    static std::shared_ptr<arrow::Schema> int32_schema() {
        return arrow::schema({arrow::field("k1", arrow::int32(), true)});
    }

    static std::shared_ptr<arrow::RecordBatch> make_record_batch(int32_t value) {
        std::shared_ptr<arrow::Array> col;
        arrow::NumericBuilder<arrow::Int32Type> builder;
        EXPECT_TRUE(builder.Reserve(1).ok());
        EXPECT_TRUE(builder.Append(value).ok());
        EXPECT_TRUE(builder.Finish(&col).ok());
        return arrow::RecordBatch::Make(int32_schema(), 1, {col});
    }
};

TEST_F(RemoteArrowQueueMgrTest, create_same_queue) {
    RemoteArrowQueueMgr mgr;
    TUniqueId id = fragment_id(10, 100);
    RemoteArrowQueueSharedPtr q1;
    mgr.create_queue(id, &q1);
    ASSERT_TRUE(q1 != nullptr);
    RemoteArrowQueueSharedPtr q2;
    mgr.create_queue(id, &q2);
    ASSERT_EQ(q1.get(), q2.get());
}

TEST_F(RemoteArrowQueueMgrTest, fetch_result_normal) {
    RemoteArrowQueueMgr mgr;
    TUniqueId id = fragment_id(10, 100);
    RemoteArrowQueueSharedPtr queue;
    mgr.create_queue(id, &queue);
    ASSERT_TRUE(queue->put(make_record_batch(20)));
    ASSERT_TRUE(queue->put(nullptr));

    std::shared_ptr<arrow::RecordBatch> result;
    bool eos = true;
    ASSERT_TRUE(mgr.fetch_result(id, &result, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(1, result->num_rows());
    ASSERT_EQ(1, result->num_columns());
}

TEST_F(RemoteArrowQueueMgrTest, fetch_result_end) {
    RemoteArrowQueueMgr mgr;
    TUniqueId id = fragment_id(10, 100);
    RemoteArrowQueueSharedPtr queue;
    mgr.create_queue(id, &queue);
    ASSERT_TRUE(queue->put(nullptr));

    std::shared_ptr<arrow::RecordBatch> result;
    bool eos = false;
    ASSERT_TRUE(mgr.fetch_result(id, &result, &eos).ok());
    ASSERT_TRUE(eos);
    ASSERT_TRUE(result == nullptr);
}

TEST_F(RemoteArrowQueueMgrTest, fetch_missing_returns_error) {
    RemoteArrowQueueMgr mgr;
    std::shared_ptr<arrow::RecordBatch> result;
    bool eos = false;
    ASSERT_FALSE(mgr.fetch_result(fragment_id(1, 1), &result, &eos).ok());
}

TEST_F(RemoteArrowQueueMgrTest, terminal_status_is_returned_before_eos) {
    RemoteArrowQueueMgr mgr;
    TUniqueId id = fragment_id(3, 4);
    RemoteArrowQueueSharedPtr queue;
    mgr.create_queue(id, &queue);
    mgr.update_queue_status(id, Status::Cancelled("cancelled for test"));
    ASSERT_TRUE(queue->put(nullptr));

    std::shared_ptr<arrow::RecordBatch> result;
    bool eos = false;
    Status status = mgr.fetch_result(id, &result, &eos);
    ASSERT_TRUE(status.is_cancelled());
}

TEST_F(RemoteArrowQueueMgrTest, schema_round_trip_and_cancel_clears_it) {
    RemoteArrowQueueMgr mgr;
    TUniqueId id = fragment_id(2, 2);
    auto schema = int32_schema();
    mgr.set_arrow_schema(id, schema);
    ASSERT_EQ(schema.get(), mgr.get_arrow_schema(id).get());
    ASSERT_TRUE(mgr.cancel(id).ok());
    ASSERT_TRUE(mgr.get_arrow_schema(id) == nullptr);
}

TEST_F(RemoteArrowQueueMgrTest, shutdown_does_not_resurface_stale_batch) {
    RemoteArrowQueueMgr mgr;
    TUniqueId id = fragment_id(5, 6);
    RemoteArrowQueueSharedPtr queue;
    mgr.create_queue(id, &queue);
    ASSERT_TRUE(queue->put(make_record_batch(42)));

    // First fetch hands back a real batch. The Arrow Flight reader reuses the SAME shared_ptr
    // across ReadNext() calls, so `result` still points at this batch on the next fetch.
    std::shared_ptr<arrow::RecordBatch> result;
    bool eos = true;
    ASSERT_TRUE(mgr.fetch_result(id, &result, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_NE(nullptr, result);

    // Producer stops (queue shut down) with no more data: blocking_get() returns false WITHOUT
    // overwriting *result. The stale batch must be cleared so the reader sees a clean EOS instead
    // of the previous batch being re-emitted.
    queue->shutdown();
    ASSERT_TRUE(mgr.fetch_result(id, &result, &eos).ok());
    ASSERT_TRUE(eos);
    ASSERT_EQ(nullptr, result);
}

TEST_F(RemoteArrowQueueMgrTest, put_allows_overshoot_after_soft_watermark) {
    RemoteArrowQueue queue(2);
    ASSERT_FALSE(queue.is_full());

    ASSERT_TRUE(queue.put(make_record_batch(1)));
    ASSERT_FALSE(queue.is_full());
    ASSERT_TRUE(queue.put(make_record_batch(2)));
    ASSERT_TRUE(queue.is_full());

    ASSERT_TRUE(queue.put(make_record_batch(3)));
    ASSERT_TRUE(queue.put(nullptr));

    std::shared_ptr<arrow::RecordBatch> result;
    ASSERT_TRUE(queue.blocking_get(&result));
    ASSERT_NE(nullptr, result);
    ASSERT_TRUE(queue.blocking_get(&result));
    ASSERT_NE(nullptr, result);
    ASSERT_TRUE(queue.blocking_get(&result));
    ASSERT_NE(nullptr, result);
    ASSERT_TRUE(queue.blocking_get(&result));
    ASSERT_EQ(nullptr, result);
    ASSERT_FALSE(queue.is_full());
}

// The queue is full once buffered RecordBatch BYTES reach the limit, even when the item count is
// far below its watermark; draining releases the bytes. The nullptr EOS marker counts as 0.
TEST_F(RemoteArrowQueueMgrTest, is_full_on_memory_watermark) {
    RemoteArrowQueue queue(/*max_elements=*/1000, /*max_bytes=*/1);
    ASSERT_FALSE(queue.is_full());

    ASSERT_TRUE(queue.put(make_record_batch(7))); // a non-empty batch exceeds the 1-byte cap
    ASSERT_TRUE(queue.is_full());

    std::shared_ptr<arrow::RecordBatch> result;
    ASSERT_TRUE(queue.blocking_get(&result)); // releases the batch's bytes -> below the cap
    ASSERT_NE(nullptr, result);
    ASSERT_FALSE(queue.is_full());

    ASSERT_TRUE(queue.put(nullptr)); // EOS marker: 0 bytes
    ASSERT_TRUE(queue.blocking_get(&result));
    ASSERT_EQ(nullptr, result);
    ASSERT_FALSE(queue.is_full());
}

} // namespace starrocks
