// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#ifdef STARROCKS_WITH_AWS

#include "env/env_s3.h"

#include <fmt/core.h>

#include "common/config.h"
#include "gutil/strings/substitute.h"
#include "object_store/s3_object_store.h"
#include "util/hdfs_util.h"

namespace starrocks {

// ==================================  S3RandomAccessFile  ==========================================

// class for remote read S3 object file.
// Now this is not thread-safe.
class S3RandomAccessFile : public RandomAccessFile {
public:
    S3RandomAccessFile(std::unique_ptr<ObjectStore> client, std::string bucket, std::string object)
            : _client(std::move(client)),
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
    std::unique_ptr<ObjectStore> _client;
    std::string _bucket;
    std::string _object;
    std::string _object_path;
    mutable uint64_t _object_size;
};

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
        RETURN_IF_ERROR(_client->get_object_size(_bucket, _object, &_object_size));
    }
    *size = _object_size;
    return Status::OK();
}

StatusOr<std::unique_ptr<RandomAccessFile>> EnvS3::new_random_access_file(const std::string& path) {
    return EnvS3::new_random_access_file(RandomAccessFileOptions(), path);
}

StatusOr<std::unique_ptr<RandomAccessFile>> EnvS3::new_random_access_file(const RandomAccessFileOptions& opts,
                                                                          const std::string& path) {
    if (!is_object_storage_path(path.c_str())) {
        return Status::InvalidArgument(fmt::format("Invalid S3 path {}", path));
    }
    std::string namenode;
    RETURN_IF_ERROR(get_namenode_from_path(path, &namenode));
    std::string bucket = get_bucket_from_namenode(namenode);
    std::string object = path.substr(namenode.size(), path.size() - namenode.size());
    Aws::Client::ClientConfiguration config;
    config.scheme = Aws::Http::Scheme::HTTP;
    config.endpointOverride = config::aws_s3_endpoint;
    config.maxConnections = config::aws_s3_max_connection;
    S3Credential cred;
    cred.access_key_id = config::aws_access_key_id;
    cred.secret_access_key = config::aws_secret_access_key;
    auto store = std::make_unique<S3ObjectStore>(config, &cred, false);
    return std::make_unique<S3RandomAccessFile>(std::move(store), std::move(bucket), std::move(object));
}

} // namespace starrocks

#endif
