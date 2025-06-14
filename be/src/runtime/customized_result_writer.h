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

#include "runtime/buffer_control_result_writer.h"
#include "runtime/type_pack.h"

namespace starrocks {

class ExprContext;
class MysqlRowBuffer;
class BufferControlBlock;
class RuntimeProfile;

class CustomizedResultWriter final : public BufferControlResultWriter {
public:
    CustomizedResultWriter(BufferControlBlock* sinker, const std::vector<ExprContext*>& output_expr_ctxs,
                           RuntimeProfile* parent_profile);

    ~CustomizedResultWriter() override;

    Status init(RuntimeState* state) override;

    Status append_chunk(Chunk* chunk) override;

    StatusOr<TFetchDataResultPtrs> process_chunk(Chunk* chunk) override;

private:
    StatusOr<TFetchDataResultPtr> _process_chunk(Chunk* chunk);

private:
    const std::vector<ExprContext*>& _output_expr_ctxs;
    std::vector<PackFunc> _pack_funcs;
};

} // namespace starrocks
