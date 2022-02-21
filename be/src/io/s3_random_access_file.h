// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#ifdef STARROCKS_WITH_AWS

#pragma once

#include "io/random_access_file.h"

namespace Aws::S3 {
class S3Client;
}

namespace starrocks::io {

class S3RandomAccessFile final : public RandomAccessFile {
public:
    explicit S3RandomAccessFile(std::shared_ptr<Aws::S3::S3Client> client, std::string bucket, std::string object)
            : _s3client(std::move(client)),
              _bucket(std::move(bucket)),
              _object(std::move(object)),
              _offset(0),
              _size(-1) {}

    StatusOr<int64_t> read(void* data, int64_t count) override;

    StatusOr<int64_t> read_at(int64_t offset, void* out, int64_t count) override;

    Status skip(int64_t count) override;

    StatusOr<int64_t> seek(int64_t offset, int whence) override;

    StatusOr<int64_t> position() override;

    StatusOr<int64_t> get_size() override;

private:
    std::shared_ptr<Aws::S3::S3Client> _s3client;
    std::string _bucket;
    std::string _object;
    int64_t _offset;
    int64_t _size;
};

} // namespace starrocks::io

#endif
