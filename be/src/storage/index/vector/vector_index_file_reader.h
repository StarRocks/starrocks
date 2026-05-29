// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#ifdef WITH_TENANN

#include <memory>
#include <string>

#include "fs/fs.h"
#include "tenann/store/index_file_reader.h"

namespace starrocks {

// Bridge StarRocks RandomAccessFile to TenANN IndexFileReader interface,
// enabling TenANN to read vector index files from remote storage (S3/HDFS/OSS).
class VectorIndexFileReader : public tenann::IndexFileReader {
public:
    VectorIndexFileReader(std::unique_ptr<RandomAccessFile> file, int64_t file_size)
            : _file(std::move(file)), _file_size(file_size), _filename(_file->filename()) {}

    ~VectorIndexFileReader() override = default;

    int64_t Read(void* data, int64_t count) override {
        auto st = _file->read_at_fully(_position, data, count);
        if (st.ok()) {
            _position += count;
            return count;
        }
        return -1;
    }

    int64_t ReadAt(int64_t offset, void* data, int64_t count) override {
        return _file->read_at_fully(offset, data, count).ok() ? count : -1;
    }

    void Seek(int64_t pos) override { _position = pos; }

    int64_t GetSize() override { return _file_size; }

    const std::string& filename() const override { return _filename; }

private:
    std::unique_ptr<RandomAccessFile> _file;
    int64_t _file_size = 0;
    int64_t _position = 0;
    std::string _filename;
};

} // namespace starrocks

#endif
