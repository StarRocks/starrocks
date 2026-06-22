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
#include <memory>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "compute_env/spill/serde.h"
#include "compute_env/spill/spill_fwd.h"

namespace starrocks::spill {

// InputStream is used in restore phase to represent an input stream of a restore task.
// InputStream reads multiple Blocks and returns the deserialized Chunks.
using InputStreamPtr = std::shared_ptr<SpillInputStream>;

class SpillInputStream {
public:
    SpillInputStream() = default;
    virtual ~SpillInputStream() = default;

    virtual StatusOr<ChunkUniquePtr> get_next(workgroup::YieldContext& yield_ctx, SerdeContext& ctx) = 0;
    virtual bool is_ready() = 0;
    virtual void close() = 0;

    virtual void get_io_stream(std::vector<SpillInputStream*>* io_stream) {}

    virtual bool enable_prefetch() const { return false; }
    virtual Status prefetch(workgroup::YieldContext& yield_ctx, SerdeContext& ctx) {
        return Status::NotSupported("input stream doesn't support prefetch");
    }

    void mark_is_eof() { _eof = true; }

    bool eof() { return _eof; }

    static InputStreamPtr union_all(const InputStreamPtr& left, const InputStreamPtr& right);
    static InputStreamPtr union_all(std::vector<InputStreamPtr>& _streams);
    static InputStreamPtr as_stream(const std::vector<ChunkPtr>& chunks, Spiller* spiller);

private:
    std::atomic_bool _eof = false;
};

} // namespace starrocks::spill
