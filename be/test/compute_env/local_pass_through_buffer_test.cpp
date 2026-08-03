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

#include "compute_env/data_stream/local_pass_through_buffer.h"

#include <gtest/gtest.h>

namespace starrocks {

static TUniqueId make_id(int64_t hi, int64_t lo) {
    TUniqueId id;
    id.hi = hi;
    id.lo = lo;
    return id;
}

// A PassThroughContext must keep working after the query's PassThroughChunkBuffer is released.
//
// The buffer is refcounted per fragment and released by PassThroughChunkBufferGuard. Both
// FragmentExecutor::_fail_cleanup() and FragmentContext::count_down_execution_group() drop that
// reference *before* the fragment's pipelines are destroyed, and pipeline teardown runs
// ~ExchangeSourceOperatorFactory -> DataStreamRecvr::close() -> PassThroughContext::set_cancelled().
// If the context only held a raw pointer into the buffer, that is a use-after-free.
TEST(LocalPassThroughBufferTest, context_outlives_chunk_buffer) {
    PassThroughChunkBufferManager mgr;
    const TUniqueId query_id = make_id(2023, 1121);
    const TUniqueId fragment_instance_id = make_id(2023, 1122);
    const PlanNodeId dest_node_id = 7;

    mgr.open_fragment_instance(query_id);
    PassThroughChunkBuffer* buffer = mgr.get(query_id);
    ASSERT_NE(nullptr, buffer);

    // The local sink and the local receiver of the same [instance, dest node] share one channel.
    PassThroughContext recvr_ctx(buffer, fragment_instance_id, dest_node_id);
    recvr_ctx.init();
    PassThroughContext sink_ctx(buffer, fragment_instance_id, dest_node_id);
    sink_ctx.init();

    ASSERT_FALSE(recvr_ctx.is_cancelled());
    ASSERT_FALSE(sink_ctx.is_cancelled());

    // The fragment fails: the last reference to the query's buffer goes away, so the buffer and
    // everything it published is destroyed, while the pipelines holding these contexts are not.
    mgr.close_fragment_instance(query_id);
    ASSERT_EQ(nullptr, mgr.get(query_id));

    // Pipeline teardown happens afterwards and cancels the receiver.
    recvr_ctx.set_cancelled();
    EXPECT_TRUE(recvr_ctx.is_cancelled());
    EXPECT_EQ(0, recvr_ctx.total_bytes());

    // The sink still observes the cancellation through the same shared channel.
    EXPECT_TRUE(sink_ctx.is_cancelled());
    EXPECT_EQ(0, sink_ctx.total_bytes());
}

// Two fragments of the same query share the buffer; releasing one reference must not invalidate
// the contexts resolved by the other, nor the channel identity they share.
TEST(LocalPassThroughBufferTest, channel_shared_across_fragment_references) {
    PassThroughChunkBufferManager mgr;
    const TUniqueId query_id = make_id(2023, 2121);
    const TUniqueId fragment_instance_id = make_id(2023, 2122);
    const PlanNodeId dest_node_id = 9;

    mgr.open_fragment_instance(query_id);
    mgr.open_fragment_instance(query_id);
    PassThroughChunkBuffer* buffer = mgr.get(query_id);
    ASSERT_NE(nullptr, buffer);

    PassThroughContext sink_ctx(buffer, fragment_instance_id, dest_node_id);
    sink_ctx.init();

    // The sender fragment finishes and drops its reference.
    mgr.close_fragment_instance(query_id);
    ASSERT_NE(nullptr, mgr.get(query_id));

    PassThroughContext recvr_ctx(buffer, fragment_instance_id, dest_node_id);
    recvr_ctx.init();

    // The receiver fragment fails and drops the last reference.
    mgr.close_fragment_instance(query_id);
    ASSERT_EQ(nullptr, mgr.get(query_id));

    sink_ctx.set_cancelled();
    EXPECT_TRUE(recvr_ctx.is_cancelled());
}

} // namespace starrocks
