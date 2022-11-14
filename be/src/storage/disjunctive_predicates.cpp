// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/disjunctive_predicates.h"

#include "column/chunk.h"

namespace starrocks::vectorized {

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

} // namespace starrocks::vectorized
