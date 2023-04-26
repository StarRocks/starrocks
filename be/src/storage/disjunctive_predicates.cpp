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

#include "storage/disjunctive_predicates.h"

#include "column/chunk.h"

namespace starrocks {

Status DisjunctivePredicates::evaluate(const Chunk* chunk, uint8_t* selection) const {
    return evaluate(chunk, selection, 0, static_cast<uint16_t>(chunk->num_rows()));
}

Status DisjunctivePredicates::evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    RETURN_IF_ERROR(_preds[0].evaluate(chunk, selection, from, to));
    for (size_t i = 1; i < _preds.size(); i++) {
        _preds[i].evaluate_or(chunk, selection, from, to);
    }
    return Status::OK();
}

} // namespace starrocks
