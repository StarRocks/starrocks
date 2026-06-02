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

#include <vector>

#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "compute_env/spill/block_manager.h"
#include "compute_env/spill/input_stream.h"
#include "compute_env/spill/serde.h"

namespace starrocks {

class RuntimeState;
class SortExecExprs;
struct SortDescs;

namespace spill {

class Spiller;

namespace detail {

InputStreamPtr make_raw_chunk_input_stream(std::vector<ChunkPtr> chunks, Spiller* spiller);
InputStreamPtr make_buffered_input_stream(int capacity, InputStreamPtr stream, Spiller* spiller);
InputStreamPtr make_sequence_input_stream(std::vector<BlockPtr> input_blocks, SerdePtr serde,
                                          BlockReaderOptions options);
StatusOr<InputStreamPtr> make_ordered_input_stream(std::vector<InputStreamPtr> input_streams, RuntimeState* state,
                                                   const SerdePtr& serde, const SortExecExprs* sort_exprs,
                                                   const SortDescs* descs, Spiller* spiller);

} // namespace detail
} // namespace spill
} // namespace starrocks
