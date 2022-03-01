// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#ifdef STARROCKS_WITH_AWS

#include "env/env_s3.h"

#include <fmt/core.h>

#include <limits>

#include "common/config.h"
#include "gutil/strings/substitute.h"
#include "object_store/s3_object_store.h"
#include "util/hdfs_util.h"

namespace starrocks {

// Wrap a `starrocks::io::RandomAccessFile` into `starrocks::RandomAccessFile`.
class RandomAccessFileWrapper : public RandomAccessFile {
public:
    explicit RandomAccessFileWrapper(std::unique_ptr<io::RandomAccessFile> input, std::string path)
            : _input(std::move(input)), _object_path(std::move(path)), _object_size(0) {}

    ~RandomAccessFileWrapper() override = default;

    Status read(uint64_t offset, Slice* res) const override;
    Status read_at(uint64_t offset, const Slice& res) const override;
    Status readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const override;
    Status size(uint64_t* size) const override;
    const std::string& file_name() const override { return _object_path; }

private:
    std::unique_ptr<io::RandomAccessFile> _input;
    std::string _object_path;
    mutable uint64_t _object_size;
};

Status RandomAccessFileWrapper::read(uint64_t offset, Slice* res) const {
    if (UNLIKELY(offset > std::numeric_limits<int64_t>::max())) return Status::NotSupported("offset overflow");
    ASSIGN_OR_RETURN(res->size, _input->read_at(static_cast<int64_t>(offset), res->data, res->size));
    return Status::OK();
}

Status RandomAccessFileWrapper::read_at(uint64_t offset, const Slice& res) const {
    if (UNLIKELY(offset > std::numeric_limits<int64_t>::max())) return Status::NotSupported("offset overflow");
    return _input->read_at_fully(static_cast<int64_t>(offset), res.data, res.size);
}

Status RandomAccessFileWrapper::readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const {
    for (size_t i = 0; i < res_cnt; i++) {
        RETURN_IF_ERROR(RandomAccessFileWrapper::read_at(offset, res[i]));
        offset += res[i].size;
    }
    return Status::OK();
}

Status RandomAccessFileWrapper::size(uint64_t* size) const {
    if (_object_size == 0) {
        ASSIGN_OR_RETURN(_object_size, _input->get_size());
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
    if (is_oss_path(namenode.c_str())) {
        if (config::object_storage_access_key_id.empty() || config::object_storage_secret_access_key.empty() ||
            config::object_storage_endpoint.empty()) {
            return Status::RuntimeError(
                    "Configuration object_storage_access_key_id, object_storage_secret_access_key "
                    "and object_storage_endpoint is required for OSS.");
        }
        config.endpointOverride = get_endpoint_from_oss_bucket(config::object_storage_endpoint, &bucket);
    } else if (!config::object_storage_endpoint.empty()) {
        config.endpointOverride = config::object_storage_endpoint;
    }
    config.maxConnections = config::object_storage_max_connection;
    S3ObjectStore store(config);
    RETURN_IF_ERROR(store.init(false));
    ASSIGN_OR_RETURN(auto input_file, store.get_object(bucket, object));
    return std::make_unique<RandomAccessFileWrapper>(std::move(input_file), path);
}

} // namespace starrocks

#endif
