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

<<<<<<< HEAD
#include <atomic>
#include <utility>

=======
#include <algorithm>
#include <atomic>
#include <mutex>
#include <utility>

#include "block_manager.h"
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/sort_exec_exprs.h"
#include "exec/sorting/sorting.h"
#include "exec/spill/block_manager.h"
#include "exec/spill/serde.h"
<<<<<<< HEAD
=======
#include "exec/workgroup/scan_task_queue.h"
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

namespace starrocks::spill {

// InputStream is used in restore phase to represent an input stream of a restore task.
// InputStream reads multiple Blocks and returns the deserialized Chunks.
<<<<<<< HEAD
class SpillInputStream;
using InputStreamPtr = std::shared_ptr<SpillInputStream>;
class Spiller;
=======
using InputStreamPtr = std::shared_ptr<SpillInputStream>;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

class SpillInputStream {
public:
    SpillInputStream() = default;
    virtual ~SpillInputStream() = default;

<<<<<<< HEAD
    virtual StatusOr<ChunkUniquePtr> get_next(SerdeContext& ctx) = 0;
    virtual bool is_ready() = 0;
    virtual void close() = 0;

    virtual bool enable_prefetch() const { return false; }
    virtual Status prefetch(SerdeContext& ctx) { return Status::NotSupported("input stream doesn't support prefetch"); }
=======
    virtual StatusOr<ChunkUniquePtr> get_next(workgroup::YieldContext& yield_ctx, SerdeContext& ctx) = 0;
    virtual bool is_ready() = 0;
    virtual void close() = 0;

    virtual void get_io_stream(std::vector<SpillInputStream*>* io_stream) {}

    virtual bool enable_prefetch() const { return false; }
    virtual Status prefetch(workgroup::YieldContext& yield_ctx, SerdeContext& ctx) {
        return Status::NotSupported("input stream doesn't support prefetch");
    }
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    void mark_is_eof() { _eof = true; }

    bool eof() { return _eof; }

    static InputStreamPtr union_all(const InputStreamPtr& left, const InputStreamPtr& right);
    static InputStreamPtr union_all(std::vector<InputStreamPtr>& _streams);
<<<<<<< HEAD
    static InputStreamPtr as_stream(std::vector<ChunkPtr> chunks, Spiller* spiller);
=======
    static InputStreamPtr as_stream(const std::vector<ChunkPtr>& chunks, Spiller* spiller);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

private:
    std::atomic_bool _eof = false;
};

<<<<<<< HEAD
// Note: not thread safe
class BlockGroup {
public:
    BlockGroup() = default;

    void append(BlockPtr block) { _blocks.emplace_back(std::move(block)); }
=======
class YieldableRestoreTask {
public:
    YieldableRestoreTask(InputStreamPtr input_stream) : _input_stream(std::move(input_stream)) {
        _input_stream->get_io_stream(&_sub_stream);
    }
    Status do_read(workgroup::YieldContext& ctx, SerdeContext& context);

private:
    InputStreamPtr _input_stream;
    std::vector<SpillInputStream*> _sub_stream;
};

// Group of Blocks.
// If spill needs sorted block output. Data within a block group is usually ordered.
// Multiple block groups need to be merged and sorted.
// Not thread safe.
//
class BlockGroup {
public:
    BlockGroup() = default;
    BlockGroup(BlockAffinityGroup affinity_group) : _affinity_group(affinity_group) {}

    void append(BlockPtr block) {
        DCHECK(block != nullptr);
        _data_size = std::nullopt;
        _blocks.emplace_back(std::move(block));
    }

    const std::vector<BlockPtr>& blocks() { return _blocks; }

    size_t data_size() const {
        if (_data_size.has_value()) {
            return _data_size.value();
        }
        size_t data_size = 0;
        for (const auto& block : _blocks) {
            data_size += block->size();
        }
        _data_size = data_size;
        return _data_size.value();
    }
    size_t num_rows() const {
        size_t num_rows = 0;
        for (const auto& block : _blocks) {
            num_rows += block->num_rows();
        }
        return num_rows;
    }
    BlockAffinityGroup get_affinity_group() const { return _affinity_group; }

private:
    // used to cache data_size in blocks
    mutable std::optional<size_t> _data_size;
    std::vector<BlockPtr> _blocks;
    BlockAffinityGroup _affinity_group = kDefaultBlockAffinityGroup;
};
using BlockGroupPtr = std::shared_ptr<BlockGroup>;

class BlockGroupSet {
public:
    void add_block_group(BlockGroupPtr block_group) {
        std::lock_guard guard(_mutex);
        _groups.emplace_back(std::move(block_group));
    }

    size_t size() const {
        std::lock_guard guard(_mutex);
        return _groups.size();
    }

    size_t num_rows() const {
        std::lock_guard guard(_mutex);
        size_t num_rows = 0;
        for (const auto& group : _groups) {
            num_rows += group->num_rows();
        }
        return num_rows;
    }

    // choose the two smallest chunks for the compaction
    std::vector<BlockGroupPtr> select_compaction_block_groups();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    StatusOr<InputStreamPtr> as_unordered_stream(const SerdePtr& serde, Spiller* spiller);

    StatusOr<InputStreamPtr> as_ordered_stream(RuntimeState* state, const SerdePtr& serde, Spiller* spiller,
                                               const SortExecExprs* sort_exprs, const SortDescs* sort_descs);

<<<<<<< HEAD
private:
    std::vector<BlockPtr> _blocks;
=======
    static StatusOr<InputStreamPtr> build_ordered_stream(std::vector<BlockGroupPtr>& block_groups, RuntimeState* state,
                                                         const SerdePtr& serde, Spiller* spiller,
                                                         const SortExecExprs* sort_exprs, const SortDescs* sort_descs);

private:
    mutable std::mutex _mutex;
    std::vector<BlockGroupPtr> _groups;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
};

} // namespace starrocks::spill