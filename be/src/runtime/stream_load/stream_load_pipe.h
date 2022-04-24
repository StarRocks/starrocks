// This file is made available under Elastic License 2.0.
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

#include "common/statusor.h"
#include "io/input_stream.h"
#include "runtime/message_body_sink.h"
#include "util/byte_buffer.h"

namespace starrocks {

// StreamLoadPipe use to transfer data from producer to consumer
// Data in pip is stored in chunks.
class StreamLoadPipe : public MessageBodySink {
public:
    explicit StreamLoadPipe(size_t max_buffered_bytes = 1024 * 1024, size_t min_chunk_size = 64 * 1024)
            : _max_buffered_bytes(max_buffered_bytes), _min_chunk_size(min_chunk_size) {}

    ~StreamLoadPipe() override = default;

    Status append(const char* data, size_t size) override;

    Status append_and_flush(const char* data, size_t size);

    Status flush();

    // called when consumer finished
    void close() { cancel(Status::OK()); }

    // called when producer finished
    Status finish() override;

    // called when producer/consumer failed
    void cancel(const Status& status) override;

    // Return nullptr on end of stream.
    StatusOr<ByteBufferPtr> read();

private:
    size_t _max_buffered_bytes;
    size_t _min_chunk_size;

    // Blocking queue
    std::mutex _lock;
    size_t _buffered_bytes{0};
    std::deque<ByteBufferPtr> _buf_queue;
    std::condition_variable _put_cond;
    std::condition_variable _get_cond;

    ByteBufferPtr _write_buf;
    Status _err_st;

    bool _finished{false};
    bool _cancelled{false};
};

std::unique_ptr<io::InputStream> new_pipe_read_stream(std::shared_ptr<StreamLoadPipe> pipe);

} // namespace starrocks
