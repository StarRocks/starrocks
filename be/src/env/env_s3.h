// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "env/env.h"

namespace starrocks {

// NOTE: Many methods in this class are unimplemented
// TODO: Do NOT export the class definition, i.e, move the class
// definition to cpp file.
class EnvS3 : public Env {
public:
    EnvS3() {}
    ~EnvS3() override = default;

    EnvS3(const EnvS3&) = delete;
    void operator=(const EnvS3&) = delete;
    EnvS3(EnvS3&&) = delete;
    void operator=(EnvS3&&) = delete;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const std::string& path) override;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& path) override;

    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const std::string& path) override {
        return Status::NotSupported("EnvS3::new_sequential_file");
    }

    // FIXME: `new_writable_file()` will not truncate an already-exist file/object, which does not satisfy
    // the API requirement.
    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const std::string& path) override;

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& opts,
                                                              const std::string& path) override;

    StatusOr<std::unique_ptr<RandomRWFile>> new_random_rw_file(const std::string& path) override {
        return Status::NotSupported("EnvS3::new_random_rw_file");
    }

    StatusOr<std::unique_ptr<RandomRWFile>> new_random_rw_file(const RandomRWFileOptions& opts,
                                                               const std::string& path) override {
        return Status::NotSupported("EnvS3::new_random_rw_file");
    }

    Status path_exists(const std::string& path) override { return Status::NotSupported("EnvS3::path_exists"); }

    Status get_children(const std::string& dir, std::vector<std::string>* file) override {
        return Status::NotSupported("EnvS3::get_children");
    }

    Status iterate_dir(const std::string& dir, const std::function<bool(const char*)>& cb) override {
        return Status::NotSupported("EnvS3::iterate_dir");
    }

    Status delete_file(const std::string& path) override { return Status::NotSupported("EnvS3::delete_file"); }

    Status create_dir(const std::string& dirname) override { return Status::NotSupported("EnvS3::create_dir"); }

    Status create_dir_if_missing(const std::string& dirname, bool* created) override {
        return Status::NotSupported("EnvS3::create_dir_if_missing");
    }

    Status delete_dir(const std::string& dirname) override { return Status::NotSupported("EnvS3::delete_dir"); }

    Status sync_dir(const std::string& dirname) override { return Status::NotSupported("EnvS3::sync_dir"); }

    Status is_directory(const std::string& path, bool* is_dir) override {
        return Status::NotSupported("EnvS3::is_directory");
    }

    Status canonicalize(const std::string& path, std::string* file) override {
        return Status::NotSupported("EnvS3::canonicalize");
    }

    Status get_file_size(const std::string& path, uint64_t* size) override {
        return Status::NotSupported("EnvS3::get_file_size");
    }

    Status get_file_modified_time(const std::string& path, uint64_t* file_mtime) override {
        return Status::NotSupported("EnvS3::get_file_modified_time");
    }

    Status rename_file(const std::string& src, const std::string& target) override {
        return Status::NotSupported("EnvS3::rename_file");
    }

    Status link_file(const std::string& old_path, const std::string& new_path) override {
        return Status::NotSupported("EnvS3::link_file");
    }
};

} // namespace starrocks
