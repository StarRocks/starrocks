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

#include <aws/s3/S3Client.h>

#include "io/output_stream.h"

namespace starrocks::io {

/// a specialized version of s3 output stream
/// 1. no internal buffering
/// 2. use multi-part upload by default
class DirectS3OutputStream : public OutputStream {
public:
    explicit DirectS3OutputStream(std::shared_ptr<Aws::S3::S3Client> client, std::string bucket, std::string object);

    ~DirectS3OutputStream() override = default;

    DirectS3OutputStream(const DirectS3OutputStream&) = delete;
    DirectS3OutputStream(DirectS3OutputStream&&) = delete;
    void operator=(const DirectS3OutputStream&) = delete;
    void operator=(DirectS3OutputStream&&) = delete;

    Status write(const void* data, int64_t size) override;

    bool allows_aliasing() const override { return false; }

    Status write_aliased(const void* data, int64_t size) override {
        return Status::NotSupported("DirectS3OutputStream::write_aliased");
    };

    Status skip(int64_t count) override { return Status::NotSupported("DirectS3OutputStream::skip"); }

    StatusOr<Buffer> get_direct_buffer() override {
        return Status::NotSupported("DirectS3OutputStream::get_direct_buffer");
    }

    StatusOr<Position> get_direct_buffer_and_advance(int64_t size) override {
        return Status::NotSupported("DirectS3OutputStream::get_direct_buffer_and_advance");
    }

    Status close() override;

private:
    Status create_multipart_upload();
    Status complete_multipart_upload();

    std::shared_ptr<Aws::S3::S3Client> _client;
    const Aws::String _bucket;
    const Aws::String _object;

    Aws::String _upload_id;
    std::vector<Aws::String> _etags;
};

} // namespace starrocks::io
