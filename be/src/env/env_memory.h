// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <algorithm>

#include "env/env.h"

namespace starrocks {

class EnvMemoryImpl;

class EnvMemory : public Env {
public:
    EnvMemory();

    ~EnvMemory() override;

    EnvMemory(const EnvMemory&) = delete;
    void operator=(const EnvMemory&) = delete;

    Status new_sequential_file(const std::string& url, std::unique_ptr<SequentialFile>* file) override;

    StatusOr<std::unique_ptr<io::RandomAccessFile>> new_random_access_file(const std::string& url) override;

    StatusOr<std::unique_ptr<io::RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                           const std::string& url) override;

    Status new_writable_file(const std::string& url, std::unique_ptr<WritableFile>* file) override;

    Status new_writable_file(const WritableFileOptions& opts, const std::string& url,
                             std::unique_ptr<WritableFile>* file) override;

    Status new_random_rw_file(const std::string& url, std::unique_ptr<RandomRWFile>* file) override;

    Status new_random_rw_file(const RandomRWFileOptions& opts, const std::string& url,
                              std::unique_ptr<RandomRWFile>* file) override;

    Status path_exists(const std::string& url) override;

    Status get_children(const std::string& dir, std::vector<std::string>* file) override;

    Status iterate_dir(const std::string& dir, const std::function<bool(const char*)>& cb) override;

    Status delete_file(const std::string& url) override;

    Status create_dir(const std::string& dirname) override;

    Status create_dir_if_missing(const std::string& dirname, bool* created) override;

    Status delete_dir(const std::string& dirname) override;

    Status sync_dir(const std::string& dirname) override;

    Status is_directory(const std::string& url, bool* is_dir) override;

    Status canonicalize(const std::string& path, std::string* file) override;

    Status get_file_size(const std::string& url, uint64_t* size) override;

    Status get_file_modified_time(const std::string& url, uint64_t* file_mtime) override;

    Status rename_file(const std::string& src, const std::string& target) override;

    Status link_file(const std::string& old_path, const std::string& new_path) override;

    Status create_file(const std::string& path);
    Status append_file(const std::string& path, const Slice& content);
    Status read_file(const std::string& path, std::string* content);

private:
    EnvMemoryImpl* _impl;
};

} // namespace starrocks
