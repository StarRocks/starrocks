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

#include "runtime/result_writer.h"
#include "util/runtime_profile.h"

namespace starrocks {
class BufferControlBlock;
class BufferControlResultWriter : public ResultWriter {
public:
    BufferControlResultWriter(BufferControlBlock* sinker, RuntimeProfile* parent_profile);

    Status add_to_write_buffer(Chunk* chunk) override;
    bool is_full() const override;
    void cancel() override;
    Status close() override;

protected:
    virtual void _init_profile();

    BufferControlBlock* _sinker;

    // parent profile from result sink. not owned
    RuntimeProfile* _parent_profile;

    // total time cost on append chunk operation
    RuntimeProfile::Counter* _append_chunk_timer = nullptr;
    // tuple convert timer, child timer of _append_chunk_timer
    RuntimeProfile::Counter* _convert_tuple_timer = nullptr;
    // file write timer, child timer of _append_chunk_timer
    RuntimeProfile::Counter* _result_send_timer = nullptr;
    // number of sent rows
    RuntimeProfile::Counter* _sent_rows_counter = nullptr;
};
} // namespace starrocks