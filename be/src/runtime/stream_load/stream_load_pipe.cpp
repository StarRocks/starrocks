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

#include "runtime/stream_load/stream_load_pipe.h"

#include <simdjson/common_defs.h>

#include "util/bit_util.h"

namespace starrocks {

class StreamLoadPipeInputStream : public io::InputStream {
public:
    explicit StreamLoadPipeInputStream(std::shared_ptr<StreamLoadPipe> pipe) : _pipe(std::move(pipe)) {}
    ~StreamLoadPipeInputStream() override = default;

    StatusOr<int64_t> read(void* data, int64_t size) override;

    Status skip(int64_t n) override;

    bool allows_peek() const override { return true; }

    StatusOr<std::string_view> peek(int64_t nbytes) override;

private:
    Status fill_buffer_if_need();

    std::shared_ptr<StreamLoadPipe> _pipe;
    ByteBufferPtr _buff;
};

inline Status StreamLoadPipeInputStream::fill_buffer_if_need() {
    if (_buff == nullptr || !_buff->has_remaining()) {
        ASSIGN_OR_RETURN(_buff, _pipe->read());
    }
    return Status::OK();
}

StatusOr<int64_t> StreamLoadPipeInputStream::read(void* data, int64_t size) {
    RETURN_IF_ERROR(fill_buffer_if_need());
    if (_buff == nullptr) return 0;
    auto n = std::min<int64_t>(size, _buff->remaining());
    _buff->get_bytes(static_cast<char*>(data), n);
    return n;
}

StatusOr<std::string_view> StreamLoadPipeInputStream::peek(int64_t nbytes) {
    RETURN_IF_ERROR(fill_buffer_if_need());
    if (_buff == nullptr) {
        return std::string_view();
    }
    nbytes = std::min<int64_t>(nbytes, _buff->remaining());
    return std::string_view(reinterpret_cast<const char*>(_buff->data()), nbytes);
}

Status StreamLoadPipeInputStream::skip(int64_t n) {
    while (n > 0) {
        RETURN_IF_ERROR(fill_buffer_if_need());
        if (_buff == nullptr) {
            break;
        }
        auto x = std::min<size_t>(n, _buff->remaining());
        _buff->skip(x);
        n -= x;
    }
    return Status::OK();
}

Status StreamLoadPipe::append_and_flush(const char* data, size_t size) {
    auto save = _min_chunk_size;
    _min_chunk_size = 0;
    RETURN_IF_ERROR(append(data, size));
    _min_chunk_size = save;
    return flush();
}

Status StreamLoadPipe::append(const char* data, size_t size) {
    size_t pos = 0;
    if (_write_buf != nullptr) {
        if (size <= _write_buf->remaining()) {
            _write_buf->put_bytes(data, size);
            return Status::OK();
        } else {
            pos = _write_buf->remaining();
            _write_buf->put_bytes(data, pos);
            RETURN_IF_ERROR(flush());
        }
    }
    // need to allocate a new chunk
    size_t chunk_size = std::max(_min_chunk_size, size - pos);
    chunk_size = BitUtil::RoundUpToPowerOfTwo(chunk_size);
    _write_buf = ByteBuffer::allocate(chunk_size + simdjson::SIMDJSON_PADDING);
    _write_buf->limit = chunk_size;
    _write_buf->capacity = chunk_size;
    _write_buf->put_bytes(data + pos, size - pos);
    return Status::OK();
}

StatusOr<ByteBufferPtr> StreamLoadPipe::read() {
    std::unique_lock l(_lock);
    _get_cond.wait(l, [this]() -> bool { return _cancelled || _finished || !_buf_queue.empty(); });
    if (!_buf_queue.empty()) {
        ByteBufferPtr ret = std::move(_buf_queue.front());
        _buffered_bytes -= ret->remaining();
        _buf_queue.pop_front();
        _put_cond.notify_one();
        return std::move(ret);
    } else if (!_err_st.ok()) {
        return _err_st;
    } else {
        return nullptr;
    }
}

Status StreamLoadPipe::flush() {
    if (_write_buf != nullptr && _write_buf->pos > 0) {
        _write_buf->flip();
        std::unique_lock<std::mutex> l(_lock);
        // if _buf_queue is empty, we append this buf without size check
        _put_cond.wait(l, [this]() -> bool {
            return _cancelled || _buf_queue.empty() || _buffered_bytes + _write_buf->remaining() <= _max_buffered_bytes;
        });
        if (_cancelled) {
            return _err_st;
        }
        _buffered_bytes += _write_buf->remaining();
        _buf_queue.emplace_back(std::move(_write_buf));
        _get_cond.notify_one();
    }
    _write_buf.reset();
    return Status::OK();
}

Status StreamLoadPipe::finish() {
    RETURN_IF_ERROR(flush());
    std::lock_guard<std::mutex> l(_lock);
    _finished = true;
    _get_cond.notify_all();
    return Status::OK();
}

void StreamLoadPipe::cancel(const Status& status) {
    std::lock_guard<std::mutex> l(_lock);
    _cancelled = true;
    if (_err_st.ok()) {
        _err_st = status;
    }
    _get_cond.notify_all();
    _put_cond.notify_all();
}

std::unique_ptr<io::InputStream> new_pipe_read_stream(std::shared_ptr<StreamLoadPipe> pipe) {
    return std::make_unique<StreamLoadPipeInputStream>(std::move(pipe));
}

} // namespace starrocks
