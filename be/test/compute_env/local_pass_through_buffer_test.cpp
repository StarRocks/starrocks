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

#include <thread>
#include <unordered_map>

#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "common/memory/column_allocator.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_env.h"

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

namespace {

// BE unit tests do not install the malloc hooks. Charge the real column allocations explicitly
// so append_chunk() measures nonzero physical bytes, and freeing a cloned column releases them
// under the consuming/cleanup thread's tracker just as it does in production.
class TrackedColumnAllocator final : public MemHookAllocator {
public:
    void* alloc(size_t size) override {
        void* ptr = MemHookAllocator::alloc(size);
        if (ptr != nullptr) {
            _sizes.emplace(ptr, size);
            CurrentThread::current().mem_consume(size);
        }
        return ptr;
    }

    void* checked_alloc(size_t size) override {
        void* ptr = alloc(size);
        if (ptr == nullptr) {
            throw std::bad_alloc();
        }
        return ptr;
    }

    void free(void* ptr) override {
        if (ptr == nullptr) {
            return;
        }
        auto it = _sizes.find(ptr);
        ASSERT_NE(_sizes.end(), it);
        CurrentThread::current().mem_release(it->second);
        _sizes.erase(it);
        MemHookAllocator::free(ptr);
    }

    bool empty() const { return _sizes.empty(); }

private:
    std::unordered_map<void*, size_t> _sizes;
};

class LocalPassThroughMemoryTest : public testing::Test {
protected:
    void SetUp() override {
        _process = RuntimeEnv::GetInstance()->process_mem_tracker();
        _passthrough = RuntimeEnv::GetInstance()->passthrough_mem_tracker();
        ASSERT_NE(nullptr, _process);
        ASSERT_NE(nullptr, _passthrough);
        flush();
        _initial_process_bytes = _process->consumption();
        _initial_passthrough_bytes = _passthrough->consumption();

        // Both instances share the query tracker, as fragment instances of one query do on a BE. Give it
        // a limit at construction so it is one of its children's limit trackers; tests lower it later.
        _query = std::make_shared<MemTracker>(kUnboundedLimit, "passthrough_query", _process);
        _sender = std::make_unique<MemTracker>(-1, "passthrough_sender", _query.get());
        _receiver = std::make_unique<MemTracker>(-1, "passthrough_receiver", _query.get());
        _sender_scope = std::make_unique<CurrentThreadMemTrackerSetter>(_sender.get());
        _column_scope = std::make_unique<ThreadLocalColumnAllocatorSetter>(&_allocator);

        _manager.open_fragment_instance(_query_id);
        auto* buffer = _manager.get(_query_id);
        _sink = std::make_unique<PassThroughContext>(buffer, make_id(2026, 2), 7);
        _source = std::make_unique<PassThroughContext>(buffer, make_id(2026, 2), 7);
        _sink->init(_query);
        _source->init(_query);

        _input = std::make_unique<Chunk>();
        auto column = Int64Column::create();
        column->append(42);
        _input->append_column(std::move(column), 0);
        flush();
        _before_buffering = _process->consumption();
        _sender_input_bytes = _sender->consumption();
        _query_before_buffering = _query->consumption();
    }

    void TearDown() override {
        _manager.close_fragment_instance(_query_id);
        _sink.reset();
        _source.reset();
        _input.reset();
        flush();
        EXPECT_TRUE(_allocator.empty());
        EXPECT_EQ(_initial_passthrough_bytes, _passthrough->consumption());
        EXPECT_EQ(_initial_process_bytes, _process->consumption());
        EXPECT_EQ(0, _sender->consumption());
        EXPECT_EQ(0, _receiver->consumption());
        EXPECT_EQ(0, _query->consumption());
        _column_scope.reset();
        _sender_scope.reset();
    }

    static void flush() { CurrentThread::current().mem_tracker_ctx_shift(); }

    int64_t append(int sender_id = 0) {
        _sink->append_chunk(sender_id, _input.get(), _input->memory_usage(), 0);
        flush();
        return _sink->total_bytes();
    }

    int64_t append_large_chunk() {
        // Exceed CurrentThread's 2 MiB accounting batch so cleanup commits its temporary
        // charge immediately, rather than cancelling it against the subsequent free.
        _input->set_num_rows(4 * 1024 * 1024 / sizeof(int64_t));
        flush();
        _before_buffering = _process->consumption();
        _sender_input_bytes = _sender->consumption();
        _query_before_buffering = _query->consumption();
        return append();
    }

    MemTracker* _process = nullptr;
    MemTracker* _passthrough = nullptr;
    int64_t _initial_process_bytes = 0;
    int64_t _initial_passthrough_bytes = 0;
    int64_t _before_buffering = 0;
    int64_t _sender_input_bytes = 0;
    int64_t _query_before_buffering = 0;
    static constexpr int64_t kUnboundedLimit = int64_t{1} << 60;
    // Declared before the instance trackers so it outlives them.
    std::shared_ptr<MemTracker> _query;
    std::unique_ptr<MemTracker> _sender;
    std::unique_ptr<MemTracker> _receiver;
    TrackedColumnAllocator _allocator;
    std::unique_ptr<CurrentThreadMemTrackerSetter> _sender_scope;
    std::unique_ptr<ThreadLocalColumnAllocatorSetter> _column_scope;
    const TUniqueId _query_id = make_id(2026, 1);
    PassThroughChunkBufferManager _manager;
    std::unique_ptr<PassThroughContext> _sink;
    std::unique_ptr<PassThroughContext> _source;
    ChunkUniquePtr _input;
};

TEST_F(LocalPassThroughMemoryTest, buffered_chunks_remain_in_query_and_process_budget) {
    // The passthrough tracker only mirrors the parked bytes; it must stay out of the tracker tree.
    EXPECT_EQ(nullptr, _passthrough->parent());
    EXPECT_EQ(2, _passthrough->get_level());
    const int64_t first_bytes = append();
    ASSERT_GT(first_bytes, 0);
    const int64_t buffered_bytes = append(1);
    EXPECT_GT(buffered_bytes, first_bytes);
    EXPECT_EQ(_initial_passthrough_bytes + buffered_bytes, _passthrough->consumption());
    EXPECT_EQ(_query_before_buffering + buffered_bytes, _query->consumption());
    EXPECT_EQ(_before_buffering + buffered_bytes, _process->consumption());
    EXPECT_EQ(_sender_input_bytes, _sender->consumption());
    EXPECT_EQ(0, _receiver->consumption());
}

TEST_F(LocalPassThroughMemoryTest, append_with_pending_tls_charge_does_not_double_count) {
    _input->set_num_rows(128 * 1024 / sizeof(int64_t));
    flush();
    auto& thread = CurrentThread::current();
    int64_t clone_bytes = 0;
    {
        const int64_t before_clone = thread.get_consumed_bytes();
        auto clone = _input->clone_unique();
        clone_bytes = thread.get_consumed_bytes() - before_clone;
    }
    flush();
    constexpr int64_t batch_size = 2 * 1024 * 1024;
    ASSERT_GT(clone_bytes, 1);
    ASSERT_LT(clone_bytes, batch_size);

    // An earlier test's high-water mark must not hide a duplicate append charge.
    const int64_t peak_gap = _process->peak_consumption() - _process->consumption();
    _process->consume(peak_gap);
    DeferOp release_padding([&] { _process->release(peak_gap); });
    const int64_t process_before = _process->consumption();
    const int64_t sender_before = _sender->consumption();
    const int64_t pending_bytes = batch_size - clone_bytes / 2;
    thread.mem_consume(pending_bytes);
    DeferOp release_pending([&] {
        thread.mem_release(pending_bytes);
        flush();
    });
    const int64_t expected_process = process_before + pending_bytes + clone_bytes;
    const int64_t old_limit = _process->limit();
    DeferOp restore_limit([&] { _process->set_limit(old_limit); });
    _process->set_limit(expected_process + clone_bytes / 2);

    // Bypass append(): its immediate flush would hide the false limit-exceeded window.
    // The clone crosses the positive batching threshold; its transfer debit is smaller than a batch.
    _sink->append_chunk(0, _input.get(), _input->memory_usage(), 0);
    EXPECT_EQ(clone_bytes, _sink->total_bytes());
    EXPECT_EQ(_initial_passthrough_bytes + clone_bytes, _passthrough->consumption());
    EXPECT_EQ(sender_before + pending_bytes, _sender->consumption());
    EXPECT_EQ(sender_before + pending_bytes + clone_bytes, _query->consumption());
    EXPECT_EQ(expected_process, _process->consumption());
    EXPECT_EQ(expected_process, _process->peak_consumption());
    EXPECT_EQ(nullptr, _sender->find_limit_exceeded_tracker());

    flush();
    EXPECT_EQ(expected_process, _process->consumption());
    EXPECT_EQ(expected_process, _process->peak_consumption());
    _source->set_cancelled();
    flush();
    EXPECT_EQ(process_before + pending_bytes, _process->consumption());
    EXPECT_EQ(sender_before + pending_bytes, _query->consumption());
    EXPECT_EQ(_initial_passthrough_bytes, _passthrough->consumption());
}

TEST_F(LocalPassThroughMemoryTest, pull_transfers_ownership_without_changing_query_or_process_usage) {
    const int64_t buffered_bytes = append();
    ASSERT_GT(buffered_bytes, 0);

    std::thread consumer([&] {
        CurrentThreadMemTrackerSetter receiver_scope(_receiver.get());
        ThreadLocalColumnAllocatorSetter column_scope(&_allocator);
        ChunkUniquePtrVector chunks;
        std::vector<size_t> bytes;
        _source->pull_chunks(0, &chunks, &bytes);
        // Ownership transfer must be visible without flushing the receiver TLS cache.

        EXPECT_EQ(1, chunks.size());
        EXPECT_EQ(0, _source->total_bytes());
        EXPECT_EQ(_initial_passthrough_bytes, _passthrough->consumption());
        EXPECT_EQ(buffered_bytes, _receiver->consumption());
        EXPECT_EQ(_query_before_buffering + buffered_bytes, _query->consumption());
        EXPECT_EQ(_before_buffering + buffered_bytes, _process->consumption());

        chunks.clear();
        flush();
        EXPECT_EQ(0, _receiver->consumption());
        EXPECT_EQ(_query_before_buffering, _query->consumption());
        EXPECT_EQ(_before_buffering, _process->consumption());
    });
    consumer.join();
}

TEST_F(LocalPassThroughMemoryTest, cancellation_releases_buffered_memory_once) {
    ASSERT_GT(append(), 0);
    ASSERT_GT(append(1), 0);
    CurrentThreadMemTrackerSetter receiver_scope(_receiver.get());
    _source->set_cancelled();
    flush();
    EXPECT_EQ(0, _source->total_bytes());
    EXPECT_EQ(_initial_passthrough_bytes, _passthrough->consumption());
    EXPECT_EQ(_query_before_buffering, _query->consumption());
    EXPECT_EQ(_before_buffering, _process->consumption());
    EXPECT_EQ(0, _receiver->consumption());

    // Repeated cancellation and a later append must not release/charge the same bytes again.
    _source->set_cancelled();
    EXPECT_EQ(0, append());
    EXPECT_EQ(_before_buffering, _process->consumption());
    EXPECT_EQ(_initial_passthrough_bytes, _passthrough->consumption());
}

TEST_F(LocalPassThroughMemoryTest, last_context_releases_unconsumed_memory) {
    const int64_t buffered_bytes = append();
    ASSERT_GT(buffered_bytes, 0);
    _manager.close_fragment_instance(_query_id);
    _sink.reset();
    EXPECT_EQ(_initial_passthrough_bytes + buffered_bytes, _passthrough->consumption());
    EXPECT_EQ(_query_before_buffering + buffered_bytes, _query->consumption());
    EXPECT_EQ(_before_buffering + buffered_bytes, _process->consumption());

    _source.reset();
    flush();
    EXPECT_EQ(_initial_passthrough_bytes, _passthrough->consumption());
    EXPECT_EQ(_query_before_buffering, _query->consumption());
    EXPECT_EQ(_before_buffering, _process->consumption());
}

TEST_F(LocalPassThroughMemoryTest, large_cancellation_does_not_inflate_process_peak) {
    const int64_t buffered_bytes = append_large_chunk();
    ASSERT_GE(buffered_bytes, 4 * 1024 * 1024);

    // The process tracker is shared across tests and retains its high-water mark. Pad current
    // consumption to that mark so an earlier test cannot mask a duplicate cleanup charge.
    const int64_t peak_gap = _process->peak_consumption() - _process->consumption();
    _process->consume(peak_gap);
    DeferOp release_padding([&] { _process->release(peak_gap); });
    const int64_t peak_before_cleanup = _process->peak_consumption();

    CurrentThreadMemTrackerSetter receiver_scope(_receiver.get());
    _source->set_cancelled();
    flush();
    EXPECT_EQ(peak_before_cleanup, _process->peak_consumption());
    EXPECT_EQ(_before_buffering + peak_gap, _process->consumption());
    EXPECT_EQ(_initial_passthrough_bytes, _passthrough->consumption());
    EXPECT_EQ(0, _source->total_bytes());
    EXPECT_EQ(0, _receiver->consumption());
}

TEST_F(LocalPassThroughMemoryTest, large_destruction_does_not_inflate_process_peak) {
    const int64_t buffered_bytes = append_large_chunk();
    ASSERT_GE(buffered_bytes, 4 * 1024 * 1024);

    // Keep this check independent of the process high-water mark left by other tests.
    const int64_t peak_gap = _process->peak_consumption() - _process->consumption();
    _process->consume(peak_gap);
    DeferOp release_padding([&] { _process->release(peak_gap); });
    const int64_t peak_before_cleanup = _process->peak_consumption();

    _manager.close_fragment_instance(_query_id);
    _sink.reset();
    EXPECT_EQ(_initial_passthrough_bytes + buffered_bytes, _passthrough->consumption());
    CurrentThreadMemTrackerSetter receiver_scope(_receiver.get());
    _source.reset();
    flush();
    EXPECT_EQ(peak_before_cleanup, _process->peak_consumption());
    EXPECT_EQ(_before_buffering + peak_gap, _process->consumption());
    EXPECT_EQ(_initial_passthrough_bytes, _passthrough->consumption());
    EXPECT_EQ(0, _receiver->consumption());
}

TEST_F(LocalPassThroughMemoryTest, buffered_memory_triggers_process_limit_checks) {
    const int64_t buffered_bytes = append();
    ASSERT_GT(buffered_bytes, 1);
    const int64_t old_limit = _process->limit();
    DeferOp restore_limit([&] { _process->set_limit(old_limit); });
    _process->set_limit(_before_buffering + buffered_bytes / 2);

    EXPECT_EQ(_process, _sender->find_limit_exceeded_tracker());
    auto* exceeded_tracker = _sender->try_consume(1);
    EXPECT_EQ(_process, exceeded_tracker);
    if (exceeded_tracker == nullptr) {
        // Keep the unfixed baseline balanced when demonstrating the regression.
        _sender->release(1);
    }
    EXPECT_EQ(_before_buffering + buffered_bytes, _process->consumption());

    _source->set_cancelled();
    flush();
    EXPECT_FALSE(_process->limit_exceeded());
    ASSERT_EQ(nullptr, _sender->try_consume(1));
    _sender->release(1);
    EXPECT_EQ(_before_buffering, _process->consumption());
}

// The parked bytes count against the query limit, so a query whose pass-through backlog grows without
// bound fails on its own limit instead of exhausting the process budget for every query on the BE.
TEST_F(LocalPassThroughMemoryTest, buffered_memory_triggers_query_limit_checks) {
    const int64_t buffered_bytes = append();
    ASSERT_GT(buffered_bytes, 1);
    _query->set_limit(_query_before_buffering + buffered_bytes / 2);
    DeferOp restore_limit([&] { _query->set_limit(kUnboundedLimit); });

    EXPECT_EQ(_query.get(), _sender->find_limit_exceeded_tracker());
    EXPECT_EQ(_query.get(), _receiver->find_limit_exceeded_tracker());
    auto* exceeded_tracker = _sender->try_consume(1);
    EXPECT_EQ(_query.get(), exceeded_tracker);
    if (exceeded_tracker == nullptr) {
        _sender->release(1);
    }

    _source->set_cancelled();
    flush();
    EXPECT_FALSE(_query->limit_exceeded());
    ASSERT_EQ(nullptr, _sender->try_consume(1));
    _sender->release(1);
    EXPECT_EQ(_query_before_buffering, _query->consumption());
}

// Without a query tracker (e.g. a context initialized without one), the bytes are parked on the process
// tracker: the instance trackers stay balanced, the process tracker keeps counting the parked bytes, and
// the passthrough tracker mirrors them.
TEST(LocalPassThroughBufferTest, falls_back_to_process_tracker_without_query_tracker) {
    MemTracker* process = RuntimeEnv::GetInstance()->process_mem_tracker();
    MemTracker* passthrough = RuntimeEnv::GetInstance()->passthrough_mem_tracker();
    TrackedColumnAllocator allocator;
    auto sender = std::make_unique<MemTracker>(-1, "passthrough_fallback_sender", process);
    CurrentThread::current().mem_tracker_ctx_shift();
    const int64_t passthrough_before = passthrough->consumption();
    {
        CurrentThreadMemTrackerSetter sender_scope(sender.get());
        ThreadLocalColumnAllocatorSetter column_scope(&allocator);
        PassThroughChunkBufferManager mgr;
        const TUniqueId query_id = make_id(2026, 11);
        mgr.open_fragment_instance(query_id);
        PassThroughContext sink(mgr.get(query_id), make_id(2026, 12), 3);
        PassThroughContext source(mgr.get(query_id), make_id(2026, 12), 3);
        sink.init();
        source.init();

        auto input = std::make_unique<Chunk>();
        auto column = Int64Column::create();
        column->append(42);
        input->append_column(std::move(column), 0);
        CurrentThread::current().mem_tracker_ctx_shift();
        const int64_t sender_before = sender->consumption();
        const int64_t process_before = process->consumption();

        sink.append_chunk(0, input.get(), input->memory_usage(), 0);
        CurrentThread::current().mem_tracker_ctx_shift();
        const int64_t buffered_bytes = sink.total_bytes();
        ASSERT_GT(buffered_bytes, 0);
        EXPECT_EQ(process_before + buffered_bytes, process->consumption());
        EXPECT_EQ(passthrough_before + buffered_bytes, passthrough->consumption());
        EXPECT_EQ(sender_before, sender->consumption());

        source.set_cancelled();
        CurrentThread::current().mem_tracker_ctx_shift();
        EXPECT_EQ(process_before, process->consumption());
        EXPECT_EQ(passthrough_before, passthrough->consumption());
        EXPECT_EQ(sender_before, sender->consumption());
        mgr.close_fragment_instance(query_id);
        input.reset();
        CurrentThread::current().mem_tracker_ctx_shift();
    }
    EXPECT_EQ(0, sender->consumption());
    EXPECT_TRUE(allocator.empty());
}

} // namespace

} // namespace starrocks
