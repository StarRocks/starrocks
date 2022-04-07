// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <algorithm>

#include "env/env.h"

namespace starrocks {

// NOTE(): Careful to use it. Now this is only used to do UT now.
class StringRandomAccessFile final : public RandomAccessFile {
public:
    explicit StringRandomAccessFile(std::string str) : _str(std::move(str)) {}
    ~StringRandomAccessFile() override = default;

    StatusOr<int64_t> read_at(int64_t offset, void* data, int64_t size) const override {
        if (offset >= _str.size()) {
            return 0;
        }
        size_t to_read = std::min<size_t>(size, _str.size() - offset);
        memcpy(data, _str.data() + offset, to_read);
        return to_read;
    }

    Status read_at_fully(int64_t offset, void* data, int64_t size) const override {
        if (offset + size > _str.size()) {
            return Status::InternalError("");
        }
        memcpy(data, _str.data() + offset, size);
        return Status::OK();
    }

    Status readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const override {
        size_t total_size = 0;
        for (int i = 0; i < res_cnt; ++i) {
            total_size += res[i].size;
        }
        if (offset + total_size > _str.size()) {
            return Status::InternalError("");
        }
        for (int i = 0; i < res_cnt; ++i) {
            memcpy(res[i].data, _str.data() + offset, res[i].size);
            offset += res[i].size;
        }
        return Status::OK();
    }

    StatusOr<uint64_t> get_size() const override { return _str.size(); }

    const std::string& filename() const override {
        static std::string s_name = "StringRandomAccessFile";
        return s_name;
    }

private:
    std::string _str;
};

class StringSequentialFile final : public SequentialFile {
public:
    explicit StringSequentialFile(std::string str) : _random_file(std::move(str)) {}
    ~StringSequentialFile() override = default;

    StatusOr<int64_t> read(void* data, int64_t size) override {
        ASSIGN_OR_RETURN(auto nread, _random_file.read_at(_offset, data, size));
        _offset += nread;
        return nread;
    }

    const std::string& filename() const override {
        static std::string s_name = "StringSequentialFile";
        return s_name;
    }

    Status skip(uint64_t n) override {
        ASSIGN_OR_RETURN(const uint64_t size, _random_file.get_size());
        _offset = std::min(_offset + n, size);
        return Status::OK();
    }

    StatusOr<uint64_t> get_size() const { return _random_file.get_size(); }

private:
    uint64_t _offset = 0;
    StringRandomAccessFile _random_file;
};

class EnvMemoryImpl;

class EnvMemory : public Env {
public:
    EnvMemory();

    ~EnvMemory() override;

    EnvMemory(const EnvMemory&) = delete;
    void operator=(const EnvMemory&) = delete;

    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const std::string& url) override;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const std::string& url) override;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& url) override;

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const std::string& url) override;

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& opts,
                                                              const std::string& url) override;

    StatusOr<std::unique_ptr<RandomRWFile>> new_random_rw_file(const std::string& url) override;

    StatusOr<std::unique_ptr<RandomRWFile>> new_random_rw_file(const RandomRWFileOptions& opts,
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
