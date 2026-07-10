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

#include "runtime/remote_chunk_queue_mgr.h"

#include <gtest/gtest.h>

namespace starrocks {

class RemoteChunkQueueMgrTest : public testing::Test {
protected:
    static TUniqueId fragment_id(int64_t hi, int64_t lo) {
        TUniqueId id;
        id.hi = hi;
        id.lo = lo;
        return id;
    }
};

TEST_F(RemoteChunkQueueMgrTest, eos_removes_queue) {
    RemoteChunkQueueMgr mgr;
    RemoteChunkQueueSharedPtr queue;
    TUniqueId id = fragment_id(1, 2);
    mgr.create_queue(id, &queue);
    ASSERT_TRUE(queue->put(nullptr));

    ChunkPB chunk;
    bool eos = false;
    ASSERT_TRUE(mgr.fetch_chunk(id, 0, &chunk, &eos).ok());
    ASSERT_TRUE(eos);
    ASSERT_TRUE(mgr.fetch_chunk(id, 1, &chunk, &eos).is_not_found());
}

TEST_F(RemoteChunkQueueMgrTest, eos_fetch_erases_queue) {
    RemoteChunkQueueMgr mgr;
    RemoteChunkQueueSharedPtr queue;
    TUniqueId id = fragment_id(5, 6);
    mgr.create_queue(id, &queue);
    ASSERT_TRUE(queue->put(nullptr));

    ChunkPB chunk;
    bool eos = false;
    ASSERT_TRUE(mgr.fetch_chunk(id, 0, &chunk, &eos).ok());
    ASSERT_TRUE(eos);
    ASSERT_TRUE(mgr.fetch_chunk(id, 1, &chunk, &eos).is_not_found());
}

TEST_F(RemoteChunkQueueMgrTest, terminal_status_is_returned_on_eos) {
    RemoteChunkQueueMgr mgr;
    RemoteChunkQueueSharedPtr queue;
    TUniqueId id = fragment_id(3, 4);
    mgr.create_queue(id, &queue);
    mgr.update_queue_status(id, Status::Cancelled("cancelled for test"));
    ASSERT_TRUE(queue->put(nullptr));

    ChunkPB chunk;
    bool eos = false;
    Status status = mgr.fetch_chunk(id, 0, &chunk, &eos);
    ASSERT_TRUE(eos);
    ASSERT_TRUE(status.is_cancelled());
}

TEST_F(RemoteChunkQueueMgrTest, cancelled_queue_returns_cancelled_instead_of_clean_eos) {
    RemoteChunkQueueMgr mgr;
    RemoteChunkQueueSharedPtr queue;
    TUniqueId id = fragment_id(7, 8);
    mgr.create_queue(id, &queue);
    queue->update_status(Status::Cancelled("cancelled for test"));
    queue->shutdown();

    ChunkPB chunk;
    bool eos = false;
    Status status = mgr.fetch_chunk(id, 0, &chunk, &eos);
    ASSERT_TRUE(eos);
    ASSERT_TRUE(status.is_cancelled());
    ASSERT_TRUE(mgr.fetch_chunk(id, 1, &chunk, &eos).is_not_found());
}

TEST_F(RemoteChunkQueueMgrTest, put_allows_overshoot_after_soft_watermark) {
    RemoteChunkQueue queue(2);
    ASSERT_FALSE(queue.is_full());

    ASSERT_TRUE(queue.put(std::make_shared<ChunkPB>()));
    ASSERT_FALSE(queue.is_full());
    ASSERT_TRUE(queue.put(std::make_shared<ChunkPB>()));
    ASSERT_TRUE(queue.is_full());

    ASSERT_TRUE(queue.put(std::make_shared<ChunkPB>()));
    ASSERT_TRUE(queue.put(nullptr));

    std::shared_ptr<ChunkPB> result;
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

// A retried fetch (same packet_seq, e.g. from brpc auto-retry after a lost response) must replay
// the last served chunk, not destructively pop the next one — otherwise rows are silently lost.
TEST_F(RemoteChunkQueueMgrTest, duplicate_packet_seq_replays_last_chunk) {
    RemoteChunkQueueMgr mgr;
    RemoteChunkQueueSharedPtr queue;
    TUniqueId id = fragment_id(9, 10);
    mgr.create_queue(id, &queue);

    auto a = std::make_shared<ChunkPB>();
    a->set_data("A");
    auto b = std::make_shared<ChunkPB>();
    b->set_data("B");
    ASSERT_TRUE(queue->put(a));
    ASSERT_TRUE(queue->put(b));
    ASSERT_TRUE(queue->put(nullptr)); // eos

    ChunkPB chunk;
    bool eos = false;
    // First fetch of seq 0 -> A.
    ASSERT_TRUE(mgr.fetch_chunk(id, 0, &chunk, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ("A", chunk.data());

    // Retry of seq 0 -> replays A, does NOT advance to B.
    ChunkPB retry;
    eos = false;
    ASSERT_TRUE(mgr.fetch_chunk(id, 0, &retry, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ("A", retry.data());

    // Advancing to seq 1 -> B (A was not skipped by the retry).
    ChunkPB second;
    eos = false;
    ASSERT_TRUE(mgr.fetch_chunk(id, 1, &second, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ("B", second.data());

    // Retry of seq 1 -> replays B.
    ChunkPB second_retry;
    eos = false;
    ASSERT_TRUE(mgr.fetch_chunk(id, 1, &second_retry, &eos).ok());
    ASSERT_EQ("B", second_retry.data());

    // Seq 2 -> clean EOS.
    ChunkPB tail;
    eos = false;
    ASSERT_TRUE(mgr.fetch_chunk(id, 2, &tail, &eos).ok());
    ASSERT_TRUE(eos);
}

// A stale/out-of-order retry of an OLDER packet_seq (arriving after the stream already advanced)
// must be rejected, not served the newer sequence's cached chunk — replaying the wrong chunk would
// silently corrupt the result.
TEST_F(RemoteChunkQueueMgrTest, stale_older_packet_seq_is_rejected) {
    RemoteChunkQueueMgr mgr;
    RemoteChunkQueueSharedPtr queue;
    TUniqueId id = fragment_id(13, 14);
    mgr.create_queue(id, &queue);

    auto a = std::make_shared<ChunkPB>();
    a->set_data("A");
    auto b = std::make_shared<ChunkPB>();
    b->set_data("B");
    ASSERT_TRUE(queue->put(a));
    ASSERT_TRUE(queue->put(b));
    ASSERT_TRUE(queue->put(nullptr)); // eos

    ChunkPB chunk;
    bool eos = false;
    ASSERT_TRUE(mgr.fetch_chunk(id, 0, &chunk, &eos).ok()); // A, last served seq = 0
    ASSERT_EQ("A", chunk.data());
    ChunkPB c1;
    eos = false;
    ASSERT_TRUE(mgr.fetch_chunk(id, 1, &c1, &eos).ok()); // B, last served seq = 1
    ASSERT_EQ("B", c1.data());

    // Retry of seq 0 (older than last served seq 1) must be rejected as InvalidArgument, NOT
    // returned B's chunk and NOT treated as shutdown (which would erase the queue).
    ChunkPB stale;
    eos = false;
    Status stale_status = mgr.fetch_chunk(id, 0, &stale, &eos);
    ASSERT_TRUE(stale_status.is_invalid_argument()) << stale_status;

    // The healthy queue must NOT have been torn down by the stale retry: the next in-order fetch
    // still reaches it (clean EOS) instead of NotFound.
    ChunkPB tail;
    eos = false;
    ASSERT_TRUE(mgr.fetch_chunk(id, 2, &tail, &eos).ok());
    ASSERT_TRUE(eos);
}

// The queue is full once buffered serialized-chunk BYTES reach the limit, even when the item count
// is far below its watermark; draining releases the bytes. The nullptr EOS marker counts as 0.
TEST_F(RemoteChunkQueueMgrTest, is_full_on_memory_watermark) {
    RemoteChunkQueue queue(/*max_elements=*/1000, /*max_bytes=*/4);
    ASSERT_FALSE(queue.is_full());

    auto make = [](const std::string& data) {
        auto c = std::make_shared<ChunkPB>();
        c->set_data(data);
        return c;
    };
    ASSERT_TRUE(queue.put(make("ab"))); // 2 bytes buffered, below the 4-byte cap
    ASSERT_FALSE(queue.is_full());
    ASSERT_TRUE(queue.put(make("cd"))); // 4 bytes buffered, reaches the cap
    ASSERT_TRUE(queue.is_full());

    std::shared_ptr<ChunkPB> result;
    ASSERT_TRUE(queue.blocking_get(&result)); // releases 2 bytes -> below the cap
    ASSERT_NE(nullptr, result);
    ASSERT_FALSE(queue.is_full());

    ASSERT_TRUE(queue.put(nullptr)); // EOS marker: 0 bytes
    ASSERT_TRUE(queue.blocking_get(&result));
    ASSERT_NE(nullptr, result);
    ASSERT_TRUE(queue.blocking_get(&result));
    ASSERT_EQ(nullptr, result);
    ASSERT_FALSE(queue.is_full());
}

} // namespace starrocks
