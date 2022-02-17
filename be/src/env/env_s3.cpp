// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "env/env_s3.h"

#include <fmt/core.h>

#include "common/config.h"
#include "gutil/strings/substitute.h"
#include "object_store/s3_client.h"
#include "util/hdfs_util.h"

namespace starrocks {

// ==================================  S3RandomAccessFile  ==========================================

// class for remote read hdfs file
// Now this is not thread-safe.
class S3RandomAccessFile : public RandomAccessFile {
public:
    S3RandomAccessFile(std::unique_ptr<ObjectStoreClient> object_store, std::string bucket, std::string object)
            : _object_store(std::move(object_store)),
              _bucket(std::move(bucket)),
              _object(std::move(object)),
              _object_path(fmt::format("s3://{}/{}", _bucket, _object)),
              _object_size(0) {}

    ~S3RandomAccessFile() override = default;

    Status read(uint64_t offset, Slice* res) const override;
    Status read_at(uint64_t offset, const Slice& res) const override;
    Status readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const override;
    Status size(uint64_t* size) const override;

    const std::string& file_name() const override { return _object_path; }

private:
    std::unique_ptr<ObjectStoreClient> _object_store;
    std::string _bucket;
    std::string _object;
    std::string _object_path;
    mutable uint64_t _object_size;
};

S3RandomAccessFile::S3RandomAccessFile(hdfsFS fs, const std::string& file_name, size_t file_size, bool usePread)
        : _opened(false),
          _fs(fs),
          _file(nullptr),
          _object_path(file_name),
          _object_size(file_size),
          _usePread(usePread) {}

Status S3RandomAccessFile::read(uint64_t offset, Slice* res) const {
    size_t read_size = 0;
    RETURN_IF_ERROR(_client->get_object_range(_bucket, _object, offset, res->size, res->data, &read_size));
    res->size = read_size;
    return Status::OK();
}

Status S3RandomAccessFile::read_at(uint64_t offset, const Slice& res) const {
    size_t read_size = 0;
    RETURN_IF_ERROR(_client->get_object_range(_bucket, _object, offset, res.size, res.data, &read_size));
    if (read_size != res.size) {
        return Status::InternalError(fmt::format("fail to read enough bytes from {} at offset {}, required={} real={}",
                                                 _object_path, offset, res.size, read_size));
    }
    return Status::OK();
}

Status S3RandomAccessFile::readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const {
    for (size_t i = 0; i < res_cnt; i++) {
        RETURN_IF_ERROR(S3RandomAccessFile::read_at(offset, res[i]));
        offset += res[i].size;
    }
    return Status::OK();
}

Status S3RandomAccessFile::size(uint64_t* size) const {
    if (_object_size == 0) {
        RETURN_IF_ERROR(_object_store->get_object_size(_bucket, _object, &_object_size));
    }
    *size = _object_size;
    return Status::OK();
}

Status S3RandomAccessFile::readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const {
    for (size_t i = 0; i < res_cnt; i++) {
        RETURN_IF_ERROR(S3RandomAccessFile::read_at(offset, res[i]));
        offset += res[i].size;
    }
    return Status::OK();
}

StatusOr<std::unique_ptr<RandomAccessFile>> EnvS3::new_random_access_file(const std::string& path) {
    return EnvS3::new_random_access_file(RandomAccessFileOptions(), path);
}

StatusOr<std::unique_ptr<RandomAccessFile>> EnvS3::new_random_access_file(const RandomAccessFileOptions& opts,
                                                                          const std::string& path) {
    if (!is_object_storage_path(path)) {
        return Status::InvalidArgument(fmt::format("Invalid S3 path {}", path));
    }
    std::string bucket = get_bucket_from_namenode(get_namenode_from_path(path));
    std::string object = file_path.substr(nn.size(), file_path.size() - nn.size());
    Aws::Client::ClientConfiguration config{.scheme = Aws::Http::Scheme::HTTP,
                                            .endpointOverride = config::aws_s3_endpoint,
                                            .maxConnections = config::aws_s3_max_connection};
    S3Credential cred{.access_key_id = config::aws_access_key_id, .secret_access_key = config::aws_secret_access_key};
    auto s3_client = std::make_unique<S3Client>(config, &cred, false);
    return std::make_unique<S3RandomAccessFile>(std::move(s3_client), std::move(bucket), std::move(object));
}

} // namespace starrocks
