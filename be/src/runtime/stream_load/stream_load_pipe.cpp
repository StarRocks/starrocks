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

#include "runtime/stream_load/stream_load_pipe.h"

#include "util/alignment.h"
#include "util/compression/compression_utils.h"

namespace starrocks {

Status StreamLoadPipe::append(ByteBufferPtr&& buf) {
    if (buf != nullptr && buf->has_remaining()) {
        std::unique_lock<std::mutex> l(_lock);

        _num_waiting_append_buffer += 1;
        // if _buf_queue is empty, we append this buf without size check
        _put_cond.wait(l, [&]() {
            return _cancelled || _finished || _buf_queue.empty() ||
                   _buffered_bytes + buf->remaining() <= _max_buffered_bytes;
        });
        _num_waiting_append_buffer -= 1;

        if (_finished) {
            return Status::CapacityLimitExceed("Stream load pipe is finished");
        }

        if (_cancelled) {
            return _err_st;
        }
        _num_append_buffers += 1;
        _append_buffer_bytes += buf->remaining();
        _buffered_bytes += buf->remaining();
        _buf_queue.emplace_back(std::move(buf));
        _get_cond.notify_one();
    }
    return Status::OK();
}

Status StreamLoadPipe::append(const char* data, size_t size) {
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
    ASSIGN_OR_RETURN(_write_buf, ByteBuffer::allocate_with_tracker(chunk_size));
    _write_buf->put_bytes(data + pos, size - pos);
    return Status::OK();
}

StatusOr<ByteBufferPtr> StreamLoadPipe::read() {
    if (_non_blocking_read) {
        return no_block_read();
    }
    std::unique_lock<std::mutex> l(_lock);
    _get_cond.wait(l, [&]() { return _cancelled || _finished || !_buf_queue.empty(); });

    // cancelled
    if (_cancelled) {
        return Status::EndOfFile("all data has been read");
    }

    // finished
    if (_buf_queue.empty()) {
        DCHECK(_finished);
        return Status::EndOfFile("all data has been read");
    }
    auto buf = std::move(_buf_queue.front());
    _buf_queue.pop_front();
    _buffered_bytes -= buf->limit;
    _put_cond.notify_one();
    return buf;
}

StatusOr<ByteBufferPtr> StreamLoadPipe::no_block_read() {
    std::unique_lock<std::mutex> l(_lock);

    _get_cond.wait_for(l, std::chrono::microseconds(_non_blocking_wait_us),
                       [&]() { return _cancelled || _finished || !_buf_queue.empty(); });

    // cancelled
    if (_cancelled) {
        return _err_st;
    }

    // finished
    if (_buf_queue.empty()) {
        if (_finished) {
            return Status::EndOfFile("all data has been read");
        } else {
            return Status::TimedOut("stream load pipe time out");
        }
    }
    auto buf = std::move(_buf_queue.front());
    _buf_queue.pop_front();
    _buffered_bytes -= buf->limit;
    _put_cond.notify_one();
    return buf;
}

Status StreamLoadPipe::read(uint8_t* data, size_t* data_size, bool* eof) {
    if (_non_blocking_read) {
        return no_block_read(data, data_size, eof);
    }
    size_t bytes_read = 0;
    while (bytes_read < *data_size) {
        if (_read_buf == nullptr || !_read_buf->has_remaining()) {
            std::unique_lock<std::mutex> l(_lock);

            _get_cond.wait(l, [&]() { return _cancelled || _finished || !_buf_queue.empty(); });
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

Status StreamLoadPipe::no_block_read(uint8_t* data, size_t* data_size, bool* eof) {
    size_t bytes_read = 0;
    while (bytes_read < *data_size) {
        if (_read_buf == nullptr || !_read_buf->has_remaining()) {
            std::unique_lock<std::mutex> l(_lock);

            _get_cond.wait_for(l, std::chrono::microseconds(_non_blocking_wait_us),
                               [&]() { return _cancelled || _finished || !_buf_queue.empty(); });

            // cancelled
            if (_cancelled) {
                return _err_st;
            }
            if (_buf_queue.empty()) {
                *data_size = bytes_read;
                *eof = _finished && (bytes_read == 0);
                bool timeout = (bytes_read == 0) && !_finished;
                return timeout ? Status::TimedOut("stream load pipe time out") : Status::OK();
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

Status StreamLoadPipe::finish() {
    if (_write_buf != nullptr) {
        _write_buf->flip();
        RETURN_IF_ERROR(_append(_write_buf));
        _write_buf.reset();
    }
    {
        std::lock_guard<std::mutex> l(_lock);
        _finished = true;
    }
    _put_cond.notify_all();
    _get_cond.notify_all();
    return Status::OK();
}

void StreamLoadPipe::cancel(const Status& status) {
    {
        std::lock_guard<std::mutex> l(_lock);
        _cancelled = true;
        if (_err_st.ok()) {
            _err_st = status.ok() ? Status::Cancelled("Cancelled with ok status") : status;
        }
    }
    _get_cond.notify_all();
    _put_cond.notify_all();
}

Status StreamLoadPipe::_append(const ByteBufferPtr& buf) {
    if (buf != nullptr && buf->has_remaining()) {
        std::unique_lock<std::mutex> l(_lock);
        // if _buf_queue is empty, we append this buf without size check
        _put_cond.wait(l, [&]() {
            return _cancelled || _buf_queue.empty() || _buffered_bytes + buf->remaining() <= _max_buffered_bytes;
        });

        if (_cancelled) {
            return _err_st;
        }
        _buf_queue.push_back(buf);
        _buffered_bytes += buf->remaining();
        _get_cond.notify_one();
    }
    return Status::OK();
}

CompressedStreamLoadPipeReader::CompressedStreamLoadPipeReader(std::shared_ptr<StreamLoadPipe> pipe,
                                                               TCompressionType::type compression_type)
        : StreamLoadPipeReader(std::move(pipe)), _compression_type(compression_type) {}

StatusOr<ByteBufferPtr> CompressedStreamLoadPipeReader::read() {
    size_t buffer_size = DEFAULT_DECOMPRESS_BUFFER_SIZE;
    if (_decompressor == nullptr) {
        auto compression = CompressionUtils::to_compression_pb(_compression_type);
        if (compression == CompressionTypePB::UNKNOWN_COMPRESSION) {
            return Status::NotSupported("Unsupported compression algorithm: " + std::to_string(_compression_type));
        }
        RETURN_IF_ERROR(StreamCompression::create_decompressor(compression, &_decompressor));
    }

    if (_decompressed_buffer == nullptr) {
        ASSIGN_OR_RETURN(_decompressed_buffer, ByteBuffer::allocate_with_tracker(buffer_size));
    }

    ASSIGN_OR_RETURN(auto buf, StreamLoadPipeReader::read());

    // try to read all compressed data into _decompressed_buffer
    bool stream_end = false;
    size_t total_bytes_read = 0;
    size_t bytes_read = 0;
    size_t bytes_written = 0;
    RETURN_IF_ERROR(_decompressor->decompress(reinterpret_cast<uint8_t*>(buf->ptr), buf->remaining(), &bytes_read,
                                              reinterpret_cast<uint8_t*>(_decompressed_buffer->ptr),
                                              _decompressed_buffer->capacity, &bytes_written, &stream_end));
    _decompressed_buffer->pos = bytes_written;
    bytes_written = 0;
    total_bytes_read += bytes_read;

    std::list<ByteBufferPtr> pieces;
    size_t pieces_size = 0;
    // read all pieces
    while (!stream_end) {
        // buffer size grows exponentially
        buffer_size = buffer_size < MAX_DECOMPRESS_BUFFER_SIZE ? buffer_size * 2 : MAX_DECOMPRESS_BUFFER_SIZE;
        ASSIGN_OR_RETURN(auto piece, ByteBuffer::allocate_with_tracker(buffer_size));
        RETURN_IF_ERROR(_decompressor->decompress(
                reinterpret_cast<uint8_t*>(buf->ptr) + total_bytes_read, buf->remaining() - total_bytes_read,
                &bytes_read, reinterpret_cast<uint8_t*>(piece->ptr), piece->capacity, &bytes_written, &stream_end));
        piece->pos = bytes_written;
        pieces.emplace_back(std::move(piece));
        pieces_size += bytes_written;
        total_bytes_read += bytes_read;
        bytes_read = 0;
        bytes_written = 0;
    }

    if (pieces_size > 0) {
        if (_decompressed_buffer->remaining() < pieces_size) {
            // align to 1024 bytes.
            auto sz = ALIGN_UP(_decompressed_buffer->pos + pieces_size, 1024);
            ASSIGN_OR_RETURN(_decompressed_buffer, ByteBuffer::reallocate_with_tracker(_decompressed_buffer, sz));
        }
        for (const auto& piece : pieces) {
            _decompressed_buffer->put_bytes(piece->ptr, piece->pos);
        }
    }
    _decompressed_buffer->flip();
    return _decompressed_buffer;
}

StreamLoadPipeInputStream::StreamLoadPipeInputStream(std::shared_ptr<StreamLoadPipe> file) : _pipe(std::move(file)) {}

StreamLoadPipeInputStream::~StreamLoadPipeInputStream() {
    _pipe->close();
}

StatusOr<int64_t> StreamLoadPipeInputStream::read(void* data, int64_t size) {
    bool eof = false;
    size_t nread = size;
    RETURN_IF_ERROR(_pipe->read(static_cast<uint8_t*>(data), &nread, &eof));
    return nread;
}

Status StreamLoadPipeInputStream::skip(int64_t n) {
    std::unique_ptr<char[]> buf(new char[n]);
    do {
        ASSIGN_OR_RETURN(auto r, read(buf.get(), n));
        if (r == 0) {
            break;
        }
        n -= r;
    } while (n > 0);
    return Status::OK();
}

} // namespace starrocks
