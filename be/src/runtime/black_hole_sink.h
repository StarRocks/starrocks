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

#include "column/chunk.h"
#include "common/statusor.h"
#include "runtime/buffer_control_block.h"
#include "runtime/result_writer.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class BlackHoleSink : public ResultWriter {
public:
    BlackHoleSink(BufferControlBlock* sinker, RuntimeProfile* parent_profile);

    Status init(RuntimeState* state) override;

    Status append_chunk(Chunk* chunk) override;

    StatusOr<TFetchDataResultPtrs> process_chunk(Chunk* chunk) override;

    StatusOr<bool> try_add_batch(TFetchDataResultPtrs& results) override;

    Status close() override;

private:
    BufferControlBlock* _sinker;
    RuntimeProfile* _parent_profile; // parent profile from result sink. not owned
    // number of rows eat
    RuntimeProfile::Counter* _eat_rows_counter = nullptr;
    int64_t _eaten_rows_num;
};

} // namespace starrocks