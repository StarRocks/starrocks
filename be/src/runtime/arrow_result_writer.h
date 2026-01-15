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
#include "runtime/buffer_control_result_writer.h"
#include "runtime/result_writer.h"
#include "runtime/runtime_state.h"

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
class ArrowResultWriter final : public BufferControlResultWriter {
public:
    ArrowResultWriter(BufferControlBlock* sinker, std::vector<ExprContext*>& output_expr_ctxs,
                      const std::vector<std::string>& output_column_names, RuntimeProfile* parent_profile,
                      const RowDescriptor& row_desc);

    Status init(RuntimeState* state) override;

    Status append_chunk(Chunk* chunk) override;

    Status close() override;

    StatusOr<TFetchDataResultPtrs> process_chunk(Chunk* chunk) override;

private:
    void _init_profile() override;

    std::vector<ExprContext*>& _output_expr_ctxs;
    const std::vector<std::string>& _output_column_names;

    const RowDescriptor& _row_desc;

    std::shared_ptr<arrow::Schema> _arrow_schema;
};

} // namespace starrocks