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

#include "gen_cpp/Types_types.h"
#include "io/input_stream.h"
#include "runtime/message_body_sink.h"
#include "util/bit_util.h"
#include "util/byte_buffer.h"
#include "util/compression/stream_compression.h"

namespace starrocks {

static constexpr size_t DEFAULT_STREAM_LOAD_PIPE_BUFFERED_BYTES = 1024 * 1024;
static constexpr size_t DEFAULT_STREAM_LOAD_PIPE_CHUNK_SIZE = 64 * 1024;

// StreamLoadPipe use to transfer data from producer to consumer
// Data in pip is stored in chunks.
class StreamLoadPipe : public MessageBodySink {
public:
    StreamLoadPipe(size_t max_buffered_bytes = DEFAULT_STREAM_LOAD_PIPE_BUFFERED_BYTES,
                   size_t min_chunk_size = DEFAULT_STREAM_LOAD_PIPE_CHUNK_SIZE)
            : StreamLoadPipe(false, -1, max_buffered_bytes, min_chunk_size) {}

    StreamLoadPipe(bool non_blocking_read, int32_t non_blocking_wait_us, size_t max_buffered_bytes,
                   size_t min_chunk_size)
            : _non_blocking_read(non_blocking_read),
              _non_blocking_wait_us(non_blocking_wait_us),
              _max_buffered_bytes(max_buffered_bytes),
              _min_chunk_size(min_chunk_size) {}

    ~StreamLoadPipe() override = default;

    Status append(ByteBufferPtr&& buf) override;

    Status append(const char* data, size_t size) override;

    bool exhausted() override {
        std::unique_lock<std::mutex> l(_lock);
        return _buf_queue.empty();
    }

    virtual StatusOr<ByteBufferPtr> read();

    StatusOr<ByteBufferPtr> no_block_read();

    virtual Status read(uint8_t* data, size_t* data_size, bool* eof);

    Status no_block_read(uint8_t* data, size_t* data_size, bool* eof);

    // called when consumer finished
    void close() { cancel(Status::Cancelled("Close the pipe")); }

    // called when producer finished
    Status finish() override;

    bool finished() {
        std::unique_lock<std::mutex> l(_lock);
        return _finished;
    }

    // called when producer/consumer failed
    void cancel(const Status& status) override;

    int32_t num_waiting_append_buffer() {
        std::unique_lock<std::mutex> l(_lock);
        return _num_waiting_append_buffer;
    }

    int32_t num_append_buffers() {
        std::unique_lock<std::mutex> l(_lock);
        return _num_append_buffers;
    }

    int64_t append_buffer_bytes() {
        std::unique_lock<std::mutex> l(_lock);
        return _append_buffer_bytes;
    }

private:
    Status _append(const ByteBufferPtr& buf);

    // Blocking queue
    std::mutex _lock;
    size_t _buffered_bytes{0};
    bool _non_blocking_read{false};
    int32_t _non_blocking_wait_us;
    size_t _max_buffered_bytes;
    size_t _min_chunk_size;
    std::deque<ByteBufferPtr> _buf_queue;
    std::condition_variable _put_cond;
    std::condition_variable _get_cond;
    int32_t _num_waiting_append_buffer{0};
    int32_t _num_append_buffers{0};
    int64_t _append_buffer_bytes{0};

    bool _finished{false};
    bool _cancelled{false};

    ByteBufferPtr _write_buf;
    ByteBufferPtr _read_buf;
    Status _err_st = Status::OK();
};

class StreamLoadPipeReader {
public:
    StreamLoadPipeReader(std::shared_ptr<StreamLoadPipe> pipe) : _pipe(pipe) {}
    virtual ~StreamLoadPipeReader() {}
    virtual StatusOr<ByteBufferPtr> read() { return _pipe->read(); }

private:
    std::shared_ptr<StreamLoadPipe> _pipe;
};

class CompressedStreamLoadPipeReader : public StreamLoadPipeReader {
public:
    CompressedStreamLoadPipeReader(std::shared_ptr<StreamLoadPipe> pipe, TCompressionType::type compression_type);
    ~CompressedStreamLoadPipeReader() override = default;
    StatusOr<ByteBufferPtr> read() override;

private:
    const size_t DEFAULT_DECOMPRESS_BUFFER_SIZE = 1024 * 1024;
    const size_t MAX_DECOMPRESS_BUFFER_SIZE = 128 * 1024 * 1024;
    TCompressionType::type _compression_type;
    ByteBufferPtr _decompressed_buffer;
    std::unique_ptr<StreamCompression> _decompressor;
};

// TODO: Make `StreamLoadPipe` as a derived class of `io::InputStream`.
class StreamLoadPipeInputStream : public io::InputStream {
public:
    explicit StreamLoadPipeInputStream(std::shared_ptr<StreamLoadPipe> file);
    ~StreamLoadPipeInputStream() override;

    StatusOr<int64_t> read(void* data, int64_t size) override;

    Status skip(int64_t n) override;

    std::shared_ptr<StreamLoadPipe> pipe() { return _pipe; }

private:
    std::shared_ptr<StreamLoadPipe> _pipe;
};

} // namespace starrocks
