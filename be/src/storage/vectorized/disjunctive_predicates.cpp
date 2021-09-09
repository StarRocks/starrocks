// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/disjunctive_predicates.h"

#include "column/chunk.h"

namespace starrocks::vectorized {

void DisjunctivePredicates::evaluate(const Chunk* chunk, uint8_t* selection) const {
    evaluate(chunk, selection, 0, chunk->num_rows());
}

void DisjunctivePredicates::evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    _preds[0].evaluate(chunk, selection, from, to);
    for (size_t i = 1; i < _preds.size(); i++) {
        _preds[i].evaluate_or(chunk, selection, from, to);
    }
}

} // namespace starrocks::vectorized
