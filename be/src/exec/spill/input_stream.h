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

#include <atomic>

#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/sort_exec_exprs.h"
#include "exec/sorting/sorting.h"
#include "exec/spill/block_manager.h"
#include "exec/spill/serde.h"

namespace starrocks::spill {

// InputStream is used in restore phase to represent an input stream of a restore task.
// InputStream reads multiple Blocks and returns the deserialized Chunks.
class InputStream {
public:
    InputStream() = default;
    InputStream(const std::vector<BlockPtr>& input_blocks) : _input_blocks(input_blocks) {}

    virtual ~InputStream() = default;
    virtual StatusOr<ChunkUniquePtr> get_next(SerdeContext& ctx) = 0;
    virtual bool is_ready() = 0;
    virtual void close() = 0;

    virtual bool enable_prefetch() const { return false; }
    virtual Status prefetch(SerdeContext& ctx) { return Status::NotSupported("input stream doesn't support prefetch"); }

    void mark_is_eof() { _eof = true; }

    bool eof() { return _eof; }

protected:
    std::atomic_bool _eof = false;
    std::vector<BlockPtr> _input_blocks;
};

using InputStreamPtr = std::shared_ptr<InputStream>;

class BlockGroup {
public:
    BlockGroup(SerdePtr serde) : _serde(std::move(serde)) {}

    void append(BlockPtr block) { _blocks.emplace_back(std::move(block)); }

    StatusOr<InputStreamPtr> as_unordered_stream();

    StatusOr<InputStreamPtr> as_ordered_stream(RuntimeState* state, const SortExecExprs* sort_exprs,
                                               const SortDescs* sort_descs);

private:
    SerdePtr _serde;
    std::vector<BlockPtr> _blocks;
};

} // namespace starrocks::spill