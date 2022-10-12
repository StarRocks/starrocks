// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <algorithm>

#include "fs/fs.h"

namespace starrocks {

class EnvMemoryImpl;

class MemoryFileSystem : public FileSystem {
public:
    MemoryFileSystem();

    ~MemoryFileSystem() override;

    MemoryFileSystem(const MemoryFileSystem&) = delete;
    void operator=(const MemoryFileSystem&) = delete;

    Type type() const override { return MEMORY; }

    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const std::string& url) override;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const std::string& url) override;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& url) override;

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const std::string& url) override;

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& opts,
                                                              const std::string& url) override;

    Status path_exists(const std::string& url) override;

    Status get_children(const std::string& dir, std::vector<std::string>* file) override;

    Status iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) override;

    Status delete_file(const std::string& url) override;

    Status create_dir(const std::string& dirname) override;

    Status create_dir_if_missing(const std::string& dirname, bool* created) override;

    Status create_dir_recursive(const std::string& dirname) override;

    Status delete_dir(const std::string& dirname) override;

    Status delete_dir_recursive(const std::string& dirname) override;

    Status sync_dir(const std::string& dirname) override;

    StatusOr<bool> is_directory(const std::string& url) override;

    Status canonicalize(const std::string& path, std::string* file) override;

    StatusOr<uint64_t> get_file_size(const std::string& url) override;

    StatusOr<uint64_t> get_file_modified_time(const std::string& url) override;

    Status rename_file(const std::string& src, const std::string& target) override;

    Status link_file(const std::string& old_path, const std::string& new_path) override;

    Status create_file(const std::string& path);
    Status append_file(const std::string& path, const Slice& content);
    Status read_file(const std::string& path, std::string* content);

private:
    EnvMemoryImpl* _impl;
};

} // namespace starrocks
