// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/fs/file_block_manager.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "storage/fs/block_manager.h"

namespace starrocks {

class BlockId;
class FileSystem;
class MemTracker;
class RandomAccessFile;

namespace fs {
namespace internal {

class FileReadableBlock;
class FileWritableBlock;
struct BlockManagerMetrics;

} // namespace internal

// TODO(lingbin): When we create a batch of blocks(blocks are created one by one),
// eg, when we do a compaction,  multiple files will be generated in sequence.
// For this scenario, we should have a mechanism that can give the Operating System
// more opportunities to perform IO merge.

// A file-backed block storage implementation.
//
// This is a naive block implementation which maps each block to its own
// file on disk.
//
// The block manager can take advantage of multiple filesystem paths.
//
// When creating blocks, the block manager will place blocks based on the
// provided CreateBlockOptions.

// The file-backed block manager.
class FileBlockManager : public BlockManager {
public:
    explicit FileBlockManager(std::shared_ptr<FileSystem> fs, BlockManagerOptions opts);
    ~FileBlockManager() override;

    Status open() override;

    Status create_block(const CreateBlockOptions& opts, std::unique_ptr<WritableBlock>* block) override;

    Status open_block(const std::string& path, std::unique_ptr<ReadableBlock>* block) override;

private:
    friend class internal::FileReadableBlock;
    friend class internal::FileWritableBlock;

    // Deletes an existing block, allowing its space to be reclaimed by the
    // filesystem. The change is immediately made durable.
    //
    // Blocks may be deleted while they are open for reading or writing;
    // the actual deletion will take place after the last open reader or
    // writer is closed.
    Status _delete_block(const std::string& path);

    FileSystem* fs() const { return _fs.get(); }

    // For manipulating files.
    std::shared_ptr<FileSystem> _fs;

    // The options that the FileBlockManager was created with.
    const BlockManagerOptions _opts;
};

} // namespace fs
} // namespace starrocks
