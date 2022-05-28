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

#include "io/input_stream.h"
#include "runtime/message_body_sink.h"
#include "util/bit_util.h"
#include "util/byte_buffer.h"
#include "util/stack_util.h"

namespace starrocks {

// StreamLoadPipe use to transfer data from producer to consumer
// Data in pip is stored in chunks.
class StreamLoadPipe : public MessageBodySink {
public:
    StreamLoadPipe(size_t max_buffered_bytes = 1024 * 1024, size_t min_chunk_size = 64 * 1024)
            : _max_buffered_bytes(max_buffered_bytes), _min_chunk_size(min_chunk_size) {}
    ~StreamLoadPipe() override = default;

    Status append(ByteBufferPtr&& buf) {
        if (buf != nullptr && buf->has_remaining()) {
            std::unique_lock<std::mutex> l(_lock);
            // if _buf_queue is empty, we append this buf without size check
            while (!_cancelled && !_buf_queue.empty() && _buffered_bytes + buf->remaining() > _max_buffered_bytes) {
                _put_cond.wait(l);
            }
            if (_cancelled) {
                return _err_st;
            }
           _buffered_bytes += buf->remaining();
            _buf_queue.emplace_back(std::move(buf));
            _get_cond.notify_one();
        }
        return Status::OK();
    }

    Status append(const char* data, size_t size) override {
        size_t pos = 0;
        if (_write_buf != nullptr) {
            if (size < _write_buf->remaining()) {
                _write_buf->put_bytes(data, size);
                return Status::OK();
            } else {
                pos = _write_buf->remaining();
                _write_buf->put_bytes(data, pos);

                _write_buf->flip();
                RETURN_IF_ERROR(_append(_write_buf));
                _write_buf.reset();
            }
        }
        // need to allocate a new chunk, min chunk is 64k
        size_t chunk_size = std::max(_min_chunk_size, size - pos);
        chunk_size = BitUtil::RoundUpToPowerOfTwo(chunk_size);
        _write_buf = ByteBuffer::allocate(chunk_size);
        _write_buf->put_bytes(data + pos, size - pos);
        return Status::OK();
    }

    bool exhausted() override {
        std::unique_lock<std::mutex> l(_lock);
        return _buf_queue.empty();
    }

    /* read_one_messages returns data that is written by append in one time.
    * buf: the buffer to return data, and would be expaneded if the capacity is not enough.
    * buf_cap: the capacity of buffer, and would be reset if the capacity is not enough.
    * buf_sz: the actual size of data to return.
    * padding: the extra space reserved in the buffer capacity.
    */
    StatusOr<ByteBufferPtr> read() {
        std::unique_lock<std::mutex> l(_lock);
        while (!_cancelled && !_finished && _buf_queue.empty()) {
            _get_cond.wait(l);
        }
        // cancelled
        if (_cancelled) {
            return _err_st;
        }

        // finished
        if (_buf_queue.empty()) {
            DCHECK(_finished);
            return Status::EndOfFile("all data has been read");
        }
        auto buf = _buf_queue.front();
        _buf_queue.pop_front();
        _buffered_bytes -= buf->limit;
        _put_cond.notify_one();
        return buf;
    }

    Status read(uint8_t* data, size_t* data_size, bool* eof) {
        size_t bytes_read = 0;
        while (bytes_read < *data_size) {
            if (_read_buf == nullptr || !_read_buf->has_remaining()) {
                std::unique_lock<std::mutex> l(_lock);
                while (!_cancelled && !_finished && _buf_queue.empty()) {
                    _get_cond.wait(l);
                }
                // cancelled
                if (_cancelled) {
                    return _err_st;
                }
                // finished
                if (_buf_queue.empty()) {
                    DCHECK(_finished);
                    *data_size = bytes_read;
                    *eof = (bytes_read == 0);
                    return Status::OK();
                }
                _read_buf = _buf_queue.front();
                _buf_queue.pop_front();
            }

            size_t copy_size = std::min(*data_size - bytes_read, _read_buf->remaining());
            _read_buf->get_bytes((char*)data + bytes_read, copy_size);
            bytes_read += copy_size;
            if (!_read_buf->has_remaining()) {
                _buffered_bytes -= _read_buf->limit;
                _put_cond.notify_one();
            }
        }
        DCHECK(bytes_read == *data_size) << "bytes_read=" << bytes_read << ", *data_size=" << *data_size;
        *eof = false;
        return Status::OK();
    }

    // called when consumer finished
    void close() { cancel(Status::OK()); }

    // called when producer finished
    Status finish() override {
        if (_write_buf != nullptr) {
            _write_buf->flip();
            _append(_write_buf);
            _write_buf.reset();
        }
        {
            std::lock_guard<std::mutex> l(_lock);
            _finished = true;
        }
        _get_cond.notify_all();
        return Status::OK();
    }

    // called when producer/consumer failed
    void cancel(const Status& status) override {
        {
            std::lock_guard<std::mutex> l(_lock);
            _cancelled = true;
            if (_err_st.ok()) {
                _err_st = status;
            }
        }
        _get_cond.notify_all();
        _put_cond.notify_all();
    }

private:
    Status _append(const ByteBufferPtr& buf) {
        if (buf != nullptr && buf->has_remaining()) {
            std::unique_lock<std::mutex> l(_lock);
            // if _buf_queue is empty, we append this buf without size check
            while (!_cancelled && !_buf_queue.empty() && _buffered_bytes + buf->remaining() > _max_buffered_bytes) {
                _put_cond.wait(l);
            }
            if (_cancelled) {
                return _err_st;
            }
            _buf_queue.push_back(buf);
            _buffered_bytes += buf->remaining();
            _get_cond.notify_one();
        }
        return Status::OK();
    }

    // Blocking queue
    std::mutex _lock;
    size_t _buffered_bytes{0};
    size_t _max_buffered_bytes;
    size_t _min_chunk_size;
    std::deque<ByteBufferPtr> _buf_queue;
    std::condition_variable _put_cond;
    std::condition_variable _get_cond;

    bool _finished{false};
    bool _cancelled{false};

    ByteBufferPtr _write_buf;
    ByteBufferPtr _read_buf;
    Status _err_st = Status::OK();
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
