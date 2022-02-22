// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#ifdef STARROCKS_WITH_AWS

#include "env/env_s3.h"

#include <fmt/core.h>

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
    ASSIGN_OR_RETURN(res->size, _input->read_at(offset, res->data, res->size));
    return Status::OK();
}

Status RandomAccessFileWrapper::read_at(uint64_t offset, const Slice& res) const {
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
    if (!config::aws_s3_endpoint.empty()) {
        config.endpointOverride = config::aws_s3_endpoint;
    }
    config.maxConnections = config::aws_s3_max_connection;
    S3Credential cred;
    cred.access_key_id = config::aws_access_key_id;
    cred.secret_access_key = config::aws_secret_access_key;
    S3ObjectStore store(config);
    RETURN_IF_ERROR(store->init(&cred, false));
    ASSIGN_OR_RETURN(auto input_file, store.get_object(bucket, object));
    return std::make_unique<RandomAccessFileWrapper>(std::move(input_file), path);
}

} // namespace starrocks

#endif
