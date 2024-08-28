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

#include "common/statusor.h"
#include "runtime/result_writer.h"
#include "runtime/runtime_state.h"
#include "util/raw_container.h"

namespace arrow {
class Schema;
}

namespace starrocks {

class ExprContext;
class BufferControlBlock;
class RuntimeProfile;
using TFetchDataResultPtr = std::unique_ptr<TFetchDataResult>;
using TFetchDataResultPtrs = std::vector<TFetchDataResultPtr>;

// convert the row batch to arrow protocol row
class ArrowResultWriter final : public ResultWriter {
public:
    ArrowResultWriter(BufferControlBlock* sinker, std::vector<ExprContext*>& output_expr_ctxs,
                      RuntimeProfile* parent_profile, const RowDescriptor& row_desc);

    Status init(RuntimeState* state) override;

    Status append_chunk(Chunk* chunk) override;

    Status close() override;

    StatusOr<TFetchDataResultPtrs> process_chunk(Chunk* chunk) override;

    StatusOr<bool> try_add_batch(TFetchDataResultPtrs& results) override;

private:
    void _init_profile();

    void _prepare_id_to_col_name_map();

    BufferControlBlock* _sinker;

    std::vector<ExprContext*>& _output_expr_ctxs;

    std::shared_ptr<arrow::Schema> _arrow_schema;

    RuntimeProfile* _parent_profile; // parent profile from result sink. not owned

    const RowDescriptor& _row_desc;

    // total time cost on append chunk operation
    RuntimeProfile::Counter* _append_chunk_timer = nullptr;
    // tuple convert timer, child timer of _append_chunk_timer
    RuntimeProfile::Counter* _convert_tuple_timer = nullptr;
    // file write timer, child timer of _append_chunk_timer
    RuntimeProfile::Counter* _result_send_timer = nullptr;
    // number of sent rows
    RuntimeProfile::Counter* _sent_rows_counter = nullptr;

    const size_t _max_row_buffer_size = 1024 * 1024 * 1024;

    std::unordered_map<int64_t, std::string> _id_to_col_name;
};

} // namespace starrocks