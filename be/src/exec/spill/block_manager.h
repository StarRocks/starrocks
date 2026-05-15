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
#include <optional>

#include "base/metrics.h"
#include "base/string/slice.h"
#include "common/runtime_profile.h"
#include "common/status.h"
#include "common/statusor.h"
#include "gen_cpp/Types_types.h"
#include "io/core/input_stream.h"

namespace starrocks::spill {

using BlockAffinityGroup = uint64_t;
static const BlockAffinityGroup kDefaultBlockAffinityGroup = UINT64_MAX;

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

    // Physical path of the underlying storage object, if applicable.
    // - FileBlock (remote, S3-like): returns "<container>/<file>" so callers can
    //   issue batch deletes / vacuum lookups.
    // - LogBlock (local, multi-block-per-file): returns nullopt; multiple blocks
    //   share one container file and per-block deletion is meaningless.
    // Default returns nullopt so subclasses without a single backing file (or
    // future block types) can opt out without code changes.
    virtual std::optional<std::string> path() const { return std::nullopt; }

    virtual bool try_acquire_sizes(size_t size) = 0;

    size_t size() const { return _size; }
    size_t num_rows() const { return _num_rows; }
    bool is_remote() const { return _is_remote; }
    void set_is_remote(bool is_remote) { _is_remote = is_remote; }

    void inc_num_rows(size_t num_rows) { _num_rows += num_rows; }

    void set_affinity_group(BlockAffinityGroup affinity_group) { _affinity_group = affinity_group; }
    BlockAffinityGroup affinity_group() const { return _affinity_group; }

protected:
    size_t _num_rows{};
    size_t _size{};
    bool _is_remote = false;
    BlockAffinityGroup _affinity_group = kDefaultBlockAffinityGroup;
};

using BlockPtr = std::shared_ptr<Block>;

struct BlockReaderOptions {
    bool enable_buffer_read = false;
    size_t max_buffer_bytes = std::numeric_limits<size_t>::max();

    RuntimeProfile::Counter* read_io_timer = nullptr;
    RuntimeProfile::Counter* read_io_count = nullptr;
    RuntimeProfile::Counter* read_io_bytes = nullptr;

    // Global per-(operator_type, storage_type) counters mirrored for
    // server-level observability. May be null if the caller has no
    // global metrics registered.
    IntCounter* global_read_io_duration_ns = nullptr;
    IntCounter* global_read_bytes = nullptr;
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
    BlockAffinityGroup affinity_group = kDefaultBlockAffinityGroup;
    // force to use remote block
    bool force_remote = false;
    // When true, the container will not attempt to delete the parent directory (e.g. query_id dir)
    // in its destructor. This is used by LoadSpillBlockManager which defers parent path cleanup
    // to its own destructor via clear_parent_path(), ensuring all spill files are removed first.
    bool skip_parent_path_deletion = false;
    // When true, the FileBlockContainer destructor will skip calling `delete_file(path())`.
    // This is used by LoadSpillBlockManager in the Lake write path (DeltaWriterImpl), where
    // per-file deletion is moved off the write hot path. Combined with skip_parent_path_deletion,
    // the container becomes a pure in-memory handle: it neither deletes files nor parent dirs.
    //
    // Deletion now happens via two paths:
    //   1. Primary: TabletInternalParallelMergeTask hot-deletes spill files via
    //      lake::delete_files_async after a successful merge.
    //   2. Safety net: vacuum_full (60s cycle) reclaims any leftovers (BE crash, hot-delete
    //      failure, etc.) by scanning load_spill_txns/ and parsing the leading hex txn_id.
    //
    // Default `false` preserves the original behavior for query spill / connector write paths.
    //
    // Scope: Only consumed by FileBlockManager (remote / S3-like block storage). LogBlockManager
    // (local disk, multi-block-per-file) ignores this flag because its files are local and the
    // motivating S3 rate-limit problem does not apply. Callers targeting a HyBirdBlockManager
    // should therefore understand that this flag only takes effect for blocks routed to the
    // remote FileBlockManager.
    bool skip_file_deletion = false;

    // ----- Flat layout options (Lake load-spill, vacuum-cleanup mode) -----
    //
    // When true, the FileBlockManager creates each block as a flat file directly under
    // the dir root (no per-query subdirectory). The file name is composed by the caller
    // via `flat_name_prefix` plus the container `_id` (sequence). The vacuum side
    // recognizes expired files purely by parsing the leading hex `txn_id` from the file
    // name, so no list-by-subdir is required.
    //
    // Invariant (enforced by FileBlockContainer ctor DCHECK):
    //   flat_layout = true  ==>  skip_parent_path_deletion = true && skip_file_deletion = true
    //
    // Scope: Only consumed by FileBlockManager. LogBlockManager ignores this field.
    bool flat_layout = false;

    // Required when `flat_layout = true`. Composed by the caller as
    //   "<txn_id_hex>_<load_id_uuid>_<frag_id_uuid>"
    // The final on-disk filename will be "<flat_name_prefix>_<container_id>".
    // The caller owns uniqueness of the prefix; FileBlockContainer simply concatenates.
    std::string flat_name_prefix;
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

    BlockAffinityGroup acquire_affinity_group() { return _next_affinity_group++; }
    virtual Status release_affinity_group(const BlockAffinityGroup affinity_group) { return Status::OK(); }

protected:
    std::atomic<BlockAffinityGroup> _next_affinity_group = 0;
};
} // namespace starrocks::spill