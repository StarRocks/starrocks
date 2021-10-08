// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <algorithm>

#include "env/env.h"

namespace starrocks {

// NOTE(): Careful to use it. Now this is only used to do UT now.
class StringRandomAccessFile final : public RandomAccessFile {
public:
    explicit StringRandomAccessFile(std::string str) : _str(std::move(str)) {}
    ~StringRandomAccessFile() override = default;

    Status read(uint64_t offset, Slice* res) const override {
        if (offset >= _str.size()) {
            res->size = 0;
            return Status::OK();
        }
        size_t to_read = std::min<size_t>(res->size, _str.size() - offset);
        memcpy(res->data, _str.data() + offset, to_read);
        res->size = to_read;
        return Status::OK();
    }

    Status read_at(uint64_t offset, const Slice& result) const override {
        if (offset + result.size > _str.size()) {
            return Status::InternalError("");
        }
        memcpy(result.data, _str.data() + offset, result.size);
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

    Status size(uint64_t* size) const override {
        *size = _str.size();
        return Status::OK();
    }

    const std::string& file_name() const override {
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

    Status read(Slice* res) override {
        Status st = _random_file.read(_offset, res);
        if (st.ok()) {
            _offset += res->size;
        }
        return st;
    }

    const std::string& filename() const override {
        static std::string s_name = "StringSequentialFile";
        return s_name;
    }

    Status skip(uint64_t n) override {
        uint64_t size = 0;
        CHECK(_random_file.size(&size).ok());
        _offset = std::min(_offset + n, size);
        return Status::OK();
    }

    uint64_t size() const {
        uint64_t sz;
        (void)_random_file.size(&sz);
        return sz;
    }

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

    Status new_sequential_file(const std::string& url, std::unique_ptr<SequentialFile>* file) override;

    Status new_random_access_file(const std::string& url, std::unique_ptr<RandomAccessFile>* file) override;

    Status new_random_access_file(const RandomAccessFileOptions& opts, const std::string& url,
                                  std::unique_ptr<RandomAccessFile>* file) override;

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
