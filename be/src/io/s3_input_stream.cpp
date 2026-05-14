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

#include "io/s3_input_stream.h"

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <fmt/format.h>

#include "base/concurrency/stopwatch.hpp"
#include "io/io_profiler.h"
#include "io/s3_global_throttle.h"
#include "io/s3_zero_copy_iostream.h"

#ifdef USE_STAROS
#include "fslib/metric_key.h"
#include "metrics/metrics.h"
#endif

namespace starrocks::io {

inline Status make_error_status(const Aws::S3::S3Error& error) {
    return Status::IOError(fmt::format(
            "BE access S3 file failed, SdkResponseCode={}, SdkErrorType={}, SdkErrorMessage={}",
            static_cast<int>(error.GetResponseCode()), static_cast<int>(error.GetErrorType()), error.GetMessage()));
}

StatusOr<int64_t> S3InputStream::read(void* out, int64_t count) {
    int64_t size = _size.load(std::memory_order_acquire);
    if (UNLIKELY(size == -1)) {
        ASSIGN_OR_RETURN(size, S3InputStream::get_size());
    }
    if (_offset >= size) {
        return 0;
    }
    MonotonicStopWatch watch;
    watch.start();
    count = std::min(count, size - _offset);

    // prefetch case:
    // case1: pretech is disable: _read_ahead_size = -1     -> direct read from s3
    // case2: read size greater than _read_ahead_size       -> direct read from s3
    // case3: read range is in buffer                       -> copy from buffer
    // case4: read start is in buffer, end is outof buffer  -> copy part data from buffer, load data from s3 to buffer, copy remain from buffer
    // case5: read start is greater than buffer end         -> load data from s3 to buffer, copy from buffer
    // case6: read start is lower than buffer start         -> load data from s3 to buffer, copy from buffer
    if (count > _read_ahead_size) {
        auto real_length = std::min<int64_t>(_offset + count, size) - _offset;

        // https://www.rfc-editor.org/rfc/rfc9110.html#name-range
        auto range = fmt::format("bytes={}-{}", _offset, _offset + real_length - 1);
        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket(_bucket);
        request.SetKey(_object);
        request.SetRange(std::move(range));
        request.SetResponseStreamFactory([out, real_length]() {
            return Aws::New<S3ZeroCopyIOStream>(AWS_ALLOCATE_TAG, reinterpret_cast<char*>(out), real_length);
        });

        Aws::S3::Model::GetObjectOutcome outcome = _s3client->GetObject(request);
        if (outcome.IsSuccess()) {
            if (UNLIKELY(outcome.GetResult().GetContentLength() != real_length)) {
                return Status::InternalError("The response length is different from request length for io stream!");
            }
            _offset += real_length;
            IOProfiler::add_read(count, watch.elapsed_time());
            return real_length;
        } else {
            return make_error_status(outcome.GetError());
        }
    } else {
        int64_t remain_to_read_length = count;
        int64_t copy_length = 0;
        if (_offset >= _buffer_start_offset && _offset < _buffer_start_offset + _buffer_data_length) {
            // case 3: read range is in buffer, copy from buffer and remain_to_read_length will be zero.
            // case 4: read start is in buffer, end is outof buffer,
            //         copy partial from buffer and remain_to_read_length will be > 0.
            copy_length = std::min<int64_t>(count, _buffer_data_length - (_offset - _buffer_start_offset));
            memcpy(static_cast<char*>(out), _read_buffer.get() + (_offset - _buffer_start_offset), copy_length);
            remain_to_read_length = remain_to_read_length - copy_length;
        }
        if (remain_to_read_length > 0) {
            // case 4,5,6, load data from s3
            // case 4: load from s3 to buffer from offset: _buffer_start_offset + _buffer_data_length
            // case 5,6: load from s3 to buffer from offset: _offset
            int64_t read_start_offset = _offset;
            if (_offset >= _buffer_start_offset && _offset < _buffer_start_offset + _buffer_data_length) {
                read_start_offset = _buffer_start_offset + _buffer_data_length;
            }
            int64_t read_end_offset = std::min<int64_t>(read_start_offset + _read_ahead_size, size);
            auto range = fmt::format("bytes={}-{}", read_start_offset, read_end_offset);
            Aws::S3::Model::GetObjectRequest request;
            request.SetBucket(_bucket);
            request.SetKey(_object);
            request.SetRange(std::move(range));

            Aws::S3::Model::GetObjectOutcome outcome = _s3client->GetObject(request);
            if (outcome.IsSuccess()) {
                Aws::IOStream& body = outcome.GetResult().GetBody();
                int64_t read_length = read_end_offset - read_start_offset;
                body.read(reinterpret_cast<char*>(_read_buffer.get()), read_length);
                _buffer_start_offset = read_start_offset;
                _buffer_data_length = body.gcount();
            } else {
                return make_error_status(outcome.GetError());
            }
            memcpy(static_cast<char*>(out) + copy_length, _read_buffer.get(), remain_to_read_length);
            copy_length += remain_to_read_length;
        }
        _offset += copy_length;
        IOProfiler::add_read(count, watch.elapsed_time());
        return copy_length;
    }
}

Status S3InputStream::seek(int64_t offset) {
    if (offset < 0) return Status::InvalidArgument(fmt::format("Invalid offset {}", offset));
    _offset = offset;
    return Status::OK();
}

StatusOr<int64_t> S3InputStream::position() {
    return _offset;
}

StatusOr<int64_t> S3InputStream::get_size() {
    int64_t size = _size.load(std::memory_order_acquire);
    if (size != -1) return size;

    // Double-checked init under _size_init_mu so the HEAD request runs once even when
    // parallel workers race here concurrently with the consumer thread.
    std::lock_guard<std::mutex> L(_size_init_mu);
    size = _size.load(std::memory_order_relaxed);
    if (size != -1) return size;

    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(_bucket);
    request.SetKey(_object);
    Aws::S3::Model::HeadObjectOutcome outcome = _s3client->HeadObject(request);
    if (!outcome.IsSuccess()) {
        return make_error_status(outcome.GetError());
    }
    size = outcome.GetResult().GetContentLength();
    _size.store(size, std::memory_order_release);
    return size;
}

void S3InputStream::set_size(int64_t value) {
    _size.store(value, std::memory_order_release);
}

StatusOr<std::string> S3InputStream::read_all() {
    MonotonicStopWatch watch;
    watch.start();
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(_bucket);
    request.SetKey(_object);
    Aws::S3::Model::GetObjectOutcome outcome = _s3client->GetObject(request);
    if (outcome.IsSuccess()) {
        Aws::IOStream& body = outcome.GetResult().GetBody();
        IOProfiler::add_read(body.gcount(), watch.elapsed_time());
        return std::string(std::istreambuf_iterator<char>(body), {});
    } else {
        return make_error_status(outcome.GetError());
    }
}

// Thread-safe positional read for parallel I/O.
// This method does NOT modify internal stream state (_offset, _read_buffer).
// S3 GetObject with Range header is inherently thread-safe - each call creates a new HTTP request.
Status S3InputStream::read_at_fully(int64_t offset, void* out, int64_t count) {
    if (count == 0) return Status::OK();

    // Get size if not known. Concurrent callers may race here; get_size() serializes.
    int64_t size = _size.load(std::memory_order_acquire);
    if (size == -1) {
        ASSIGN_OR_RETURN(size, S3InputStream::get_size());
    }

    if (offset < 0 || offset + count > size) {
        return Status::IOError(
                fmt::format("S3 read_at_fully out of bounds: offset={}, count={}, size={}", offset, count, size));
    }

    // Wait for global throttle if another thread received 429/503
    S3GlobalThrottle::wait();

    MonotonicStopWatch watch;
    watch.start();

    // https://www.rfc-editor.org/rfc/rfc9110.html#name-range
    auto range = fmt::format("bytes={}-{}", offset, offset + count - 1);

    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(_bucket);
    request.SetKey(_object);
    request.SetRange(std::move(range));
    request.SetResponseStreamFactory([out, count]() {
        return Aws::New<S3ZeroCopyIOStream>(AWS_ALLOCATE_TAG, reinterpret_cast<char*>(out), count);
    });

    Aws::S3::Model::GetObjectOutcome outcome = _s3client->GetObject(request);
    if (outcome.IsSuccess()) {
        if (UNLIKELY(outcome.GetResult().GetContentLength() != count)) {
            return Status::InternalError(fmt::format("S3 response length mismatch: expected={}, got={}", count,
                                                     outcome.GetResult().GetContentLength()));
        }
        // Note: Do NOT modify _offset here - this is a thread-safe positional read
        IOProfiler::add_read(count, watch.elapsed_time());
        return Status::OK();
    } else {
        return make_error_status(outcome.GetError());
    }
}

} // namespace starrocks::io
