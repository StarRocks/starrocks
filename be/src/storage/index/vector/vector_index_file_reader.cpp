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

#ifdef WITH_TENANN

#include "storage/index/vector/vector_index_file_reader.h"

#include <memory>
#include <utility>

#include "common/logging.h"
#include "common/status.h"
#include "common/statusor.h"
#include "fs/fs.h"

namespace starrocks {

StatusOr<std::unique_ptr<VectorIndexFileReader>> VectorIndexFileReader::open(const FileInfo& file_info) {
    if (file_info.fs == nullptr) {
        return Status::InvalidArgument("VectorIndexFileReader needs a FileSystem for " + file_info.path);
    }
    FileInfo resolved = file_info;

    RandomAccessFileOptions block_read_opts;
    block_read_opts.buffer_size = 0;

    ASSIGN_OR_RETURN(auto load_file, resolved.fs->new_random_access_file(resolved));
    if (!resolved.size.has_value()) {
        // Cold path only: the caller normally hands the size down from the factory's get_size().
        ASSIGN_OR_RETURN(auto size, load_file->get_size());
        resolved.size = size;
    }

    const int64_t file_size = resolved.size.value();
    return std::make_unique<VectorIndexFileReader>(std::move(load_file), std::move(resolved),
                                                   std::move(block_read_opts), file_size);
}

int64_t VectorIndexFileReader::Read(void* data, int64_t count) {
    if (_load_file == nullptr) {
        LOG(WARNING) << "sequential read on a released vector index load stream, path=" << _file_info.path
                     << ", position=" << _position << ", count=" << count;
        return -1;
    }
    auto st = _load_file->read_at_fully(_position, data, count);
    if (!st.ok()) {
        LOG(WARNING) << "sequential read of vector index file failed, path=" << _file_info.path
                     << ", position=" << _position << ", count=" << count << ", status=" << st;
        return -1;
    }
    _position += count;
    return count;
}

int64_t VectorIndexFileReader::ReadAt(int64_t offset, void* data, int64_t count) {
    // Each call needs an independent stream because RandomAccessFile is not thread-safe.
    auto file_or = _file_info.fs->new_random_access_file(_block_read_options, _file_info);
    if (!file_or.ok()) {
        LOG(WARNING) << "failed to open vector index file for block read, path=" << _file_info.path
                     << ", offset=" << offset << ", count=" << count << ", status=" << file_or.status();
        return -1;
    }
    auto st = (*file_or)->read_at_fully(offset, data, count);
    if (!st.ok()) {
        LOG(WARNING) << "block read of vector index file failed, path=" << _file_info.path << ", offset=" << offset
                     << ", count=" << count << ", status=" << st;
        return -1;
    }
    return count;
}

} // namespace starrocks

#endif
