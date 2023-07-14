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

#include <unordered_map>

#include "column/chunk.h"
#include "common/global_types.h"
#include "exec/exec_node.h"
#include "exec/olap_common.h"
#include "runtime/global_dict/decoder.h"
#include "runtime/global_dict/parser.h"

namespace starrocks {

class DictDecodeNode final : public ExecNode {
public:
    DictDecodeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    ~DictDecodeNode() override {
        if (runtime_state() != nullptr) {
            close(runtime_state());
        }
    }

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;

    void close(RuntimeState* state) override;

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;

protected:
    void debug_string(int indentation_level, std::stringstream* out) const override { *out << "DictDecodeNode"; }

private:
    void _init_counter();

    std::shared_ptr<Chunk> _input_chunk;
    std::vector<int32_t> _encode_column_cids;
    std::vector<int32_t> _decode_column_cids;
    std::vector<GlobalDictDecoderPtr> _decoders;

    std::vector<ExprContext*> _expr_ctxs;
    std::map<SlotId, std::pair<ExprContext*, DictOptimizeContext>> _string_functions;
    DictOptimizeParser _dict_optimize_parser;

    // profile
    RuntimeProfile::Counter* _decode_timer = nullptr;
};

} // namespace starrocks
