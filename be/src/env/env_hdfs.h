// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <hdfs/hdfs.h>

#include "env/env.h"

namespace starrocks {

// class for remote read hdfs file
// Now this is not thread-safe.
class HdfsRandomAccessFile : public RandomAccessFile {
public:
    HdfsRandomAccessFile(hdfsFS fs, std::string filename);
    virtual ~HdfsRandomAccessFile() noexcept;

    Status open();
    void close() noexcept;
    Status read(uint64_t offset, Slice* res) const override;
    Status read_at(uint64_t offset, const Slice& res) const override;
    Status readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const override;

    Status size(uint64_t* size) const override;
    const std::string& file_name() const override { return _filename; }

    hdfsFile hdfs_file() const { return _file; }

private:
    bool _opened;
    hdfsFS _fs;
    hdfsFile _file;
    std::string _filename;
};

} // namespace starrocks
