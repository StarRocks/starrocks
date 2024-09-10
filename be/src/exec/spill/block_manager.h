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

#include <memory>

#include "common/status.h"
#include "common/statusor.h"
#include "gen_cpp/Types_types.h"
#include "io/input_stream.h"
#include "util/runtime_profile.h"
#include "util/slice.h"

namespace starrocks::spill {

class BlockReader;
class BlockReaderOptions;
// Block represents a continuous storage space and is the smallest storage unit of flush and restore in spill task.
// Block only supports append writing and sequential reading, and neither writing nor reading of Block is guaranteed to be thread-safe.
class Block {
public:
    virtual ~Block() = default;

    // append data into Block
    virtual Status append(const std::vector<Slice>& data) = 0;

    // flush block to somewhere
    virtual Status flush() = 0;

    virtual StatusOr<std::unique_ptr<io::InputStreamWrapper>> get_readable() const = 0;

    virtual std::shared_ptr<BlockReader> get_reader(const BlockReaderOptions& options) = 0;

    virtual std::string debug_string() const = 0;

    size_t size() const { return _size; }
    size_t num_rows() const { return _num_rows; }
    bool is_remote() const { return _is_remote; }
    void set_is_remote(bool is_remote) { _is_remote = is_remote; }

    virtual bool preallocate(size_t write_size) = 0;

    bool exclusive() const { return _exclusive; }
    void set_exclusive(bool exclusive) { _exclusive = exclusive; }

    void inc_num_rows(size_t num_rows) { _num_rows += num_rows; }

protected:
    size_t _num_rows{};
    size_t _size{};
    bool _is_remote = false;
    bool _exclusive{};
};

using BlockPtr = std::shared_ptr<Block>;

struct BlockReaderOptions {
    bool enable_buffer_read = false;
    size_t max_buffer_bytes = std::numeric_limits<size_t>::max();

    RuntimeProfile::Counter* read_io_timer = nullptr;
    RuntimeProfile::Counter* read_io_count = nullptr;
    RuntimeProfile::Counter* read_io_bytes = nullptr;
};

class BlockReader {
public:
    BlockReader(const Block* block, const BlockReaderOptions& options)
            : _block(block), _length(block->size()), _options(options) {}

    virtual ~BlockReader() = default;
    // read exacly the specified length of data from Block,
    // if the Block has reached the end, should return EndOfFile status
    virtual Status read_fully(void* data, int64_t count);

    virtual std::string debug_string() = 0;

    virtual const Block* block() const = 0;

protected:
    const Block* _block = nullptr;
    std::unique_ptr<io::InputStreamWrapper> _readable;
    size_t _length = 0;
    size_t _offset = 0;

    // used for buffer read
    std::unique_ptr<uint8_t[]> _buffer;
    Slice _slice;
    BlockReaderOptions _options;
};

struct AcquireBlockOptions {
    TUniqueId query_id;
    TUniqueId fragment_instance_id;
    int32_t plan_node_id;
    std::string name;
    bool direct_io = false;
    // The block will occupy the entire container, making it easier to remove the block.
    bool exclusive = false;
    size_t block_size = 0;
};

// BlockManager is used to manage the life cycle of the Block.
// All flush tasks need to apply for a Block from BlockManager through `acuire_block` before writing,
// and need to return Block to BlockManager through `release_block` after writing.
// The allocation strategy of Block is determined by BlockManager.
class BlockManager {
public:
    virtual ~BlockManager() = default;
    virtual Status open() = 0;
    virtual void close() = 0;
    // acquire a block from BlockManager, return error if BlockManager can't allocate one.
    virtual StatusOr<BlockPtr> acquire_block(const AcquireBlockOptions& opts) = 0;
    // return Block to BlockManager
    virtual Status release_block(BlockPtr block) = 0;
};
} // namespace starrocks::spill