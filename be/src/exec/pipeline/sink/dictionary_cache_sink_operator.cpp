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

#include "exec/pipeline/sink/dictionary_cache_sink_operator.h"

#include "exec/pipeline/pipeline_driver_executor.h"

namespace starrocks::pipeline {

Status DictionaryCacheSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    if (_writer == nullptr) {
        _writer = std::make_unique<DictionaryCacheWriter>(_t_dictionary_cache_sink, state, _fragment_ctx);
    }
    return _writer->prepare();
}

void DictionaryCacheSinkOperator::close(RuntimeState* state) {
    Operator::close(state);
}

bool DictionaryCacheSinkOperator::need_input() const {
    return _writer->need_input();
}

bool DictionaryCacheSinkOperator::is_finished() const {
    return _writer->is_finished();
}

Status DictionaryCacheSinkOperator::set_finishing(RuntimeState* state) {
    return _writer->set_finishing();
}

bool DictionaryCacheSinkOperator::pending_finish() const {
    return !_writer->is_finished();
}

Status DictionaryCacheSinkOperator::set_cancelled(RuntimeState* state) {
    return _writer->cancel();
}

StatusOr<ChunkPtr> DictionaryCacheSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from dictionary cache sink operator");
}

Status DictionaryCacheSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    return _writer->append_chunk(chunk);
}

Status DictionaryCacheSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    return Status::OK();
}

void DictionaryCacheSinkOperatorFactory::close(RuntimeState* state) {
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
