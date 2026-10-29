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

#include "compute_env/load/stream_context_mgr.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>

#include "base/testutil/assert.h"
#include "base/uid_util.h"
#include "base/utility/defer_op.h"
#include "compute_env/load/load_stream_mgr.h"
#include "compute_env/load/stream_load_context.h"
#include "compute_env/load/stream_load_pipe.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/byte_buffer.h"

namespace starrocks {

// A lone bool used to bind to StreamLoadPipe(size_t max_buffered_bytes, ...): a 1-byte buffer with blocking reads.
static_assert(!std::is_constructible_v<StreamLoadPipe, bool>);
static_assert(std::is_constructible_v<StreamLoadPipe, size_t>);

namespace {

ByteBufferPtr make_buffer(size_t size, char fill) {
    auto buf = ByteBuffer::allocate_with_tracker(size).value();
    const std::string data(size, fill);
    buf->put_bytes(data.data(), data.size());
    buf->flip_to_read();
    return buf;
}

// Runs `fn` on another thread and waits up to 10 s for it. A pipe call that blocks would never return, so on a
// timeout the pipe is cancelled to release the thread, and nullopt lets the caller fail instead of hanging.
std::optional<Status> call_with_deadline(StreamLoadPipe* pipe, std::function<Status()> fn) {
    auto future = std::async(std::launch::async, std::move(fn));
    if (future.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
        pipe->cancel(Status::Cancelled("blocked past the deadline"));
        future.wait();
        return std::nullopt;
    }
    return future.get();
}

} // namespace

// A channel stays open between the requests of a transaction and until it is committed. If its pipe blocked a
// reader that has no data, every open channel would hold a connector scan thread; if the pipe could not buffer a
// request while the fragment is not reading, the HTTP worker delivering it would wait for the fragment. Together
// they deadlocked compute nodes with more open channels than connector scan threads until the transactions timed
// out.
TEST(StreamContextMgrTest, channel_pipe_reads_without_blocking_and_buffers_requests) {
    // Declared first: a context removes its pipe from the load stream manager when it is released.
    LoadStreamMgr load_stream_mgr;
    StreamContextMgr stream_context_mgr(&load_stream_mgr);
    const TUniqueId load_id = UniqueId::gen_uid().to_thrift();
    StreamLoadContext* ctx = nullptr;
    ASSERT_OK(stream_context_mgr.create_channel_context("label", 0, "db", "tbl", TFileFormatType::FORMAT_CSV_PLAIN, ctx,
                                                        load_id, 1));
    DeferOp release_ctx([&] { StreamLoadContext::release(ctx); });

    auto pipe = std::dynamic_pointer_cast<StreamLoadPipe>(ctx->body_sink);
    ASSERT_NE(nullptr, pipe);
    ASSERT_EQ(pipe, load_stream_mgr.get(load_id));

    // The fragment starts reading before any request arrives: the read times out instead of blocking.
    auto read_st = call_with_deadline(pipe.get(), [&pipe] { return pipe->read().status(); });
    ASSERT_TRUE(read_st.has_value()) << "a read with no data blocked";
    ASSERT_TRUE(read_st->is_time_out()) << *read_st;

    // The scanners read through StreamLoadPipeInputStream, which uses the byte read.
    auto byte_read_st = call_with_deadline(pipe.get(), [&pipe] {
        uint8_t data[16];
        size_t size = sizeof(data);
        bool eof = false;
        return pipe->read(data, &size, &eof);
    });
    ASSERT_TRUE(byte_read_st.has_value()) << "a byte read with no data blocked";
    ASSERT_TRUE(byte_read_st->is_time_out()) << *byte_read_st;

    // Two requests arrive while the fragment is not reading. Both fit in the default buffer, so the second one
    // does not wait for the fragment to consume the first.
    const size_t request_bytes = DEFAULT_STREAM_LOAD_PIPE_BUFFERED_BYTES / 2;
    ASSERT_OK(pipe->append(make_buffer(request_bytes, 'a')));
    auto second_request = make_buffer(request_bytes, 'b');
    auto append_st = call_with_deadline(pipe.get(), [&] { return pipe->append(std::move(second_request)); });
    ASSERT_TRUE(append_st.has_value()) << "the second request waited for the reader";
    ASSERT_OK(*append_st);

    // The fragment reads both requests in order, then times out again until the producer finishes.
    for (const char fill : {'a', 'b'}) {
        auto buf_or = pipe->read();
        ASSERT_OK(buf_or.status());
        const ByteBufferPtr& buf = buf_or.value();
        ASSERT_EQ(request_bytes, buf->remaining());
        ASSERT_EQ(fill, buf->ptr[0]);
        ASSERT_EQ(fill, buf->ptr[request_bytes - 1]);
    }
    ASSERT_TRUE(pipe->read().status().is_time_out());
    ASSERT_OK(pipe->finish());
    ASSERT_TRUE(pipe->read().status().is_end_of_file());
}

} // namespace starrocks
