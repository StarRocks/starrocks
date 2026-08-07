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

#include "common/statusor.h"
#include "fs/fs.h"
#include "tenann/store/index_file_reader.h"

namespace starrocks {

// Bridge StarRocks RandomAccessFile to TenANN IndexFileReader interface,
// enabling TenANN to read vector index files from remote storage (S3/HDFS/OSS).
//
// TenANN may call ReadAt() concurrently for different IVF lists. Each call opens an
// independent RandomAccessFile because StarRocks streams are stateful and not thread-safe.
class VectorIndexFileReader : public tenann::IndexFileReader {
public:
    // Opens the `.vi` described by `file_info` (path + owning FileSystem; size optional).
    // Fails Status::NotFound if the file is missing, propagated from the filesystem layer.
    // `file_info.fs` must be non-null and is retained by shared_ptr: this reader is stored
    // in the tenann index cache entry and outlives the SegmentIterator that opened it.
    static StatusOr<std::unique_ptr<VectorIndexFileReader>> open(const FileInfo& file_info);

    VectorIndexFileReader(std::unique_ptr<RandomAccessFile> load_file, FileInfo file_info,
                          RandomAccessFileOptions block_read_options, int64_t file_size)
            : _load_file(std::move(load_file)),
              _file_info(std::move(file_info)),
              _block_read_options(std::move(block_read_options)),
              _file_size(file_size),
              _filename(_file_info.path) {}

    ~VectorIndexFileReader() override = default;

    // Used only during the initial index load.
    int64_t Read(void* data, int64_t count) override;

    // Positioned reads for different inverted lists may run concurrently.
    int64_t ReadAt(int64_t offset, void* data, int64_t count) override;

    void Seek(int64_t pos) override { _position = pos; }

    int64_t GetSize() override { return _file_size; }

    const std::string& filename() const override { return _filename; }

    // Block reads open independent files, so the load stream can be released afterward.
    void release_load_file() { _load_file.reset(); }

private:
    std::unique_ptr<RandomAccessFile> _load_file;
    // Retains the FileSystem and resolved size for future block-cache misses.
    FileInfo _file_info;
    // Disables readahead for independent block reads.
    RandomAccessFileOptions _block_read_options;
    int64_t _file_size = 0;
    int64_t _position = 0;
    std::string _filename;
};

} // namespace starrocks

#endif
