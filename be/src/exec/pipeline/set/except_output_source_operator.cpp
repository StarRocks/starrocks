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

#include "exec/pipeline/set/except_output_source_operator.h"

namespace starrocks::pipeline {

StatusOr<ChunkPtr> ExceptOutputSourceOperator::pull_chunk(RuntimeState* state) {
    return _except_ctx->pull_chunk(state);
}

void ExceptOutputSourceOperator::close(RuntimeState* state) {
    _except_ctx->unref(state);
    Operator::close(state);
}

void ExceptOutputSourceOperatorFactory::close(RuntimeState* state) {
    SourceOperatorFactory::close(state);
}

} // namespace starrocks::pipeline
