// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <string>

#include "io/seekable_input_stream.h"

namespace Aws::S3 {
class S3Client;
}

namespace starrocks::io {

class S3InputStream final : public SeekableInputStream {
public:
    explicit S3InputStream(std::shared_ptr<Aws::S3::S3Client> client, std::string bucket, std::string object)
            : _s3client(std::move(client)),
              _bucket(std::move(bucket)),
              _object(std::move(object)),
              _offset(0),
              _size(-1) {}

    ~S3InputStream() override = default;

    // Disallow copy and assignment
    S3InputStream(const S3InputStream&) = delete;
    void operator=(const S3InputStream&) = delete;

    // Disallow move ctor and move assignment, because no usage now
    S3InputStream(S3InputStream&&) = delete;
    void operator=(S3InputStream&&) = delete;

    StatusOr<int64_t> read(void* data, int64_t count) override;

    Status seek(int64_t offset) override;

    StatusOr<int64_t> position() override;

    StatusOr<int64_t> get_size() override;

    void set_size(int64_t size) override;

private:
    std::shared_ptr<Aws::S3::S3Client> _s3client;
    std::string _bucket;
    std::string _object;
    int64_t _offset;
    int64_t _size;
};

} // namespace starrocks::io
