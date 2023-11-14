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

#pragma once

#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/s3/S3Client.h>

#include "io/output_stream.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks::io {

class S3OutputStream : public OutputStream {
public:
    explicit S3OutputStream(std::shared_ptr<Aws::S3::S3Client> client, std::string bucket, std::string object,
                            int64_t max_single_part_size, int64_t min_upload_part_size, bool background_write = true);

    ~S3OutputStream() override = default;

    // Disallow copy and assignment
    S3OutputStream(const S3OutputStream&) = delete;
    void operator=(const S3OutputStream&) = delete;

    // Disallow move, because no usage now
    S3OutputStream(S3OutputStream&&) = delete;
    void operator=(S3OutputStream&&) = delete;

    Status write(const void* data, int64_t size) override;

    [[nodiscard]] bool allows_aliasing() const override { return false; }

    Status write_aliased(const void* data, int64_t size) override;

    Status skip(int64_t count) override;

    StatusOr<Buffer> get_direct_buffer() override;

    StatusOr<Position> get_direct_buffer_and_advance(int64_t size) override;

    Status close() override;

private:
    Status create_multipart_upload();
    Status complete_multipart_upload();

    Status upload_part(std::vector<uint8_t> buffer);

    std::shared_ptr<Aws::S3::S3Client> _client;
    const Aws::String _bucket;
    const Aws::String _object;
    const int64_t _max_single_part_size;
    const int64_t _min_upload_part_size;
    std::vector<uint8_t> _buffer;
    Aws::String _upload_id;
    int _part_id{1}; // starts at 1

    const bool _background_write;
    PriorityThreadPool* _io_executor;

    std::mutex _mutex; // guards following
    std::vector<Aws::String> _etags;
    int _completed_parts{0};
    Status _io_status;
    std::condition_variable _cv;
};

// A non-copying iostream.
// See https://stackoverflow.com/questions/35322033/aws-c-sdk-uploadpart-times-out
// https://stackoverflow.com/questions/13059091/creating-an-input-stream-from-constant-memory
class StringViewStream : Aws::Utils::Stream::PreallocatedStreamBuf, public std::iostream {
public:
    StringViewStream(const void* data, int64_t nbytes)
            : Aws::Utils::Stream::PreallocatedStreamBuf(reinterpret_cast<unsigned char*>(const_cast<void*>(data)),
                                                        static_cast<size_t>(nbytes)),
              std::iostream(this) {}
};

} // namespace starrocks::io
