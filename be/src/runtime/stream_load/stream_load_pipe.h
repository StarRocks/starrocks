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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/stream_load/stream_load_pipe.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>

#include "io/input_stream.h"
#include "runtime/message_body_sink.h"
#include "util/bit_util.h"
#include "util/byte_buffer.h"

namespace starrocks {

// StreamLoadPipe use to transfer data from producer to consumer
// Data in pip is stored in chunks.
class StreamLoadPipe : public MessageBodySink {
public:
    StreamLoadPipe(size_t max_buffered_bytes = 1024 * 1024, size_t min_chunk_size = 64 * 1024)
            : _max_buffered_bytes(max_buffered_bytes), _min_chunk_size(min_chunk_size) {}
    ~StreamLoadPipe() override = default;

    Status append(ByteBufferPtr&& buf) override;

    Status append(const char* data, size_t size) override;

    bool exhausted() override {
        std::unique_lock<std::mutex> l(_lock);
        return _buf_queue.empty();
    }

    StatusOr<ByteBufferPtr> read();

    StatusOr<ByteBufferPtr> no_block_read();

    Status read(uint8_t* data, size_t* data_size, bool* eof);

    Status no_block_read(uint8_t* data, size_t* data_size, bool* eof);

    // called when consumer finished
    void close() { cancel(Status::OK()); }

    // called when producer finished
    Status finish() override;

    // called when producer/consumer failed
    void cancel(const Status& status) override;

    void set_non_blocking_read() { _non_blocking_read = true; }

private:
    Status _append(const ByteBufferPtr& buf);

    // Called when `no_block_read(uint8_t* data, size_t* data_size, bool* eof)`
    // timeout in the mid of read, we will push the read data back to the _buf_queue.
    // The lock is already acquired before calling this function
    // and there is no need to acquire the lock again in the function
    Status _push_front_unlocked(const ByteBufferPtr& buf);

    // Blocking queue
    std::mutex _lock;
    size_t _buffered_bytes{0};
    size_t _max_buffered_bytes;
    size_t _min_chunk_size;
    std::deque<ByteBufferPtr> _buf_queue;
    std::condition_variable _put_cond;
    std::condition_variable _get_cond;
    bool _non_blocking_read{false};

    bool _finished{false};
    bool _cancelled{false};

    ByteBufferPtr _write_buf;
    ByteBufferPtr _read_buf;
    Status _err_st = Status::OK();
};

// TODO: Make `StreamLoadPipe` as a derived class of `io::InputStream`.
class StreamLoadPipeInputStream : public io::InputStream {
public:
    explicit StreamLoadPipeInputStream(std::shared_ptr<StreamLoadPipe> file, bool non_blocking_read);
    ~StreamLoadPipeInputStream() override;

    StatusOr<int64_t> read(void* data, int64_t size) override;

    Status skip(int64_t n) override;

    std::shared_ptr<StreamLoadPipe> pipe() { return _pipe; }

private:
    std::shared_ptr<StreamLoadPipe> _pipe;
};

} // namespace starrocks
