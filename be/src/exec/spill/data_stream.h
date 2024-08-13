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

#include "common/status.h"
#include "exec/spill/spill_fwd.h"
#include "runtime/runtime_state.h"
#include "util/slice.h"

namespace starrocks::spill {

class SpillOutputDataStream {
public:
    virtual ~SpillOutputDataStream() = default;
    virtual Status append(RuntimeState* state, const std::vector<Slice>& data, size_t total_write_size,
                          size_t write_num_rows) = 0;
    virtual Status flush() = 0;
    virtual bool is_remote() const = 0;
    // only used for DCHECK
    size_t append_rows() const { return _append_rows; }

protected:
    size_t _append_rows{};
};
using SpillOutputDataStreamPtr = std::shared_ptr<SpillOutputDataStream>;
SpillOutputDataStreamPtr create_spill_output_stream(Spiller* spiller, BlockGroup* block_group,
                                                    BlockManager* block_manager);

using InputStreamPtr = std::shared_ptr<SpillInputStream>;

// Transfer data from input_stream to output stream
// This class will execution io tasks. make sure call it in io threads
class DataTranster {
public:
    static Status transfer(workgroup::YieldContext& yield_ctx, RuntimeState* state, Serde* serde,
                           const SpillOutputDataStreamPtr& output, const InputStreamPtr& input_stream);
};

} // namespace starrocks::spill