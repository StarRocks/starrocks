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

#include "storage/conjunctive_predicates.h"

#include "column/chunk.h"
#include "util/failpoint/fail_point.h"

namespace starrocks {

// NOTE: No short-circuit.
Status ConjunctivePredicates::evaluate(const Chunk* chunk, uint8_t* selection) const {
    return evaluate(chunk, selection, 0, static_cast<uint16_t>(chunk->num_rows()));
}

Status ConjunctivePredicates::evaluate_and(const Chunk* chunk, uint8_t* selection) const {
    return evaluate_and(chunk, selection, 0, static_cast<uint16_t>(chunk->num_rows()));
}

Status ConjunctivePredicates::evaluate_or(const Chunk* chunk, uint8_t* selection) const {
    return evaluate_or(chunk, selection, 0, static_cast<uint16_t>(chunk->num_rows()));
}

Status ConjunctivePredicates::evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    FAIL_POINT_TRIGGER_RETURN_ERROR(random_error);
    DCHECK_LE(to, chunk->num_rows());
    if (!_vec_preds.empty()) {
        const ColumnPredicate* pred = _vec_preds[0];
        const Column* col = chunk->get_column_by_id(pred->column_id()).get();
        RETURN_IF_ERROR(pred->evaluate(col, selection, from, to));
        for (size_t i = 1; i < _vec_preds.size(); i++) {
            pred = _vec_preds[i];
            col = chunk->get_column_by_id(pred->column_id()).get();
            RETURN_IF_ERROR(pred->evaluate_and(col, selection, from, to));
        }
    }
    return _evaluate_non_vec(chunk, selection, from, to);
}

Status ConjunctivePredicates::evaluate_or(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    DCHECK_LE(to, chunk->num_rows());
    std::unique_ptr<uint8_t[]> buff(new uint8_t[chunk->num_rows()]);
    RETURN_IF_ERROR(evaluate(chunk, buff.get(), from, to));
    const uint8_t* p = buff.get();
    for (uint16_t i = from; i < to; i++) {
        DCHECK((bool)(selection[i] | p[i]) == (selection[i] || p[i]));
        selection[i] |= p[i];
    }
    return Status::OK();
}

Status ConjunctivePredicates::evaluate_and(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    DCHECK_LE(to, chunk->num_rows());
    return _evaluate_and(chunk, selection, from, to);
}

inline Status ConjunctivePredicates::_evaluate_and(const Chunk* chunk, uint8_t* selection, uint16_t from,
                                                   uint16_t to) const {
    for (auto pred : _vec_preds) {
        const Column* col = chunk->get_column_by_id(pred->column_id()).get();
        RETURN_IF_ERROR(pred->evaluate_and(col, selection, from, to));
    }
    return _evaluate_non_vec(chunk, selection, from, to);
}

inline Status ConjunctivePredicates::_evaluate_non_vec(const Chunk* chunk, uint8_t* selection, uint16_t from,
                                                       uint16_t to) const {
    if (!_non_vec_preds.empty()) {
        _selected_idx.resize(to - from);
        uint16_t selected_size = 0;
        if (!_vec_preds.empty()) {
            for (uint16_t i = from; i < to; ++i) {
                _selected_idx[selected_size] = i;
                selected_size += selection[i];
            }
        } else {
            // when there is no vectorized predicates, should initialize _selected_idx
            // in a vectorized way.
            selected_size = to - from;
            for (uint16_t i = from, j = 0; i < to; ++i, ++j) {
                _selected_idx[j] = i;
            }
        }

        for (size_t i = 0; selected_size > 0 && i < _non_vec_preds.size(); ++i) {
            const ColumnPredicate* pred = _non_vec_preds[i];
            const ColumnPtr& c = chunk->get_column_by_id(pred->column_id());
            ASSIGN_OR_RETURN(selected_size, pred->evaluate_branchless(c.get(), _selected_idx.data(), selected_size));
        }

        memset(&selection[from], 0, to - from);
        for (uint16_t i = 0; i < selected_size; ++i) {
            selection[_selected_idx[i]] = 1;
        }
    }
    return Status::OK();
}

std::string ConjunctivePredicates::debug_string() const {
    std::stringstream ss;
    ss << "ConjunctivePredicates(";
    if (!_non_vec_preds.empty()) {
        ss << "non_vec_pred = [";
        for (auto* pred : _non_vec_preds) {
            ss << pred->debug_string();
        }
        ss << "]";
    }
    if (!_vec_preds.empty()) {
        ss << "vec_pred = [";
        for (auto* pred : _vec_preds) {
            ss << pred->debug_string();
        }
        ss << "]";
    }
    ss << ")";
    return ss.str();
}

} // namespace starrocks
