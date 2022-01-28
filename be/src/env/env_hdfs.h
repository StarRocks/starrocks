// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <hdfs/hdfs.h>

#include "env/env.h"
#include "env/s3_client.h"

namespace starrocks {

struct HdfsFsHandle {
    enum class Type { LOCAL, HDFS, S3 };
    Type type;
    std::string namenode;
    hdfsFS hdfs_fs;
    S3Client* s3_client;
};

// class for remote read hdfs file
// Now this is not thread-safe.
class HdfsRandomAccessFile : public RandomAccessFile {
public:
    HdfsRandomAccessFile(hdfsFS fs, const std::string& file_name, size_t file_size, bool usePread);
    virtual ~HdfsRandomAccessFile() noexcept;

    Status open() override;
    void close() override;
    Status read(uint64_t offset, Slice* res) const override;
    Status read_at(uint64_t offset, const Slice& res) const override;
    Status readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const override;

    Status size(uint64_t* size) const override {
        *size = _file_size;
        return Status::OK();
    }
    const std::string& file_name() const override { return _file_name; }

    hdfsFile hdfs_file() const { return _file; }

private:
    Status _read_at(int64_t offset, char* data, size_t size, size_t* read_size) const;
    bool _opened;
    hdfsFS _fs;
    hdfsFile _file;
    std::string _file_name;
    size_t _file_size;
    bool _usePread;
};

class S3RandomAccessFile : public RandomAccessFile {
public:
    S3RandomAccessFile(S3Client* client, const std::string& bucket, const std::string& object, size_t object_size = 0);
    virtual ~S3RandomAccessFile() noexcept;

    Status open() override;
    void close() override;
    Status read(uint64_t offset, Slice* res) const override;
    Status read_at(uint64_t offset, const Slice& res) const override;
    Status readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const override;

    Status size(uint64_t* size) const override {
        *size = _object_size;
        return Status::OK();
    }
    const std::string& file_name() const override { return _file_name; }

private:
    void _init(const HdfsFsHandle& handle);
    S3Client* _client;
    std::string _file_name;
    std::string _bucket;
    std::string _object;
    size_t _object_size;
};

std::shared_ptr<RandomAccessFile> create_random_access_hdfs_file(const HdfsFsHandle& handle,
                                                                 const std::string& file_path, size_t file_size,
                                                                 bool usePread);
} // namespace starrocks
