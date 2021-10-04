// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/conjunctive_predicates.h"

#include "column/chunk.h"

namespace starrocks::vectorized {

// NOTE: No short-circuit.
void ConjunctivePredicates::evaluate(const Chunk* chunk, uint8_t* selection) const {
    evaluate(chunk, selection, 0, chunk->num_rows());
}

void ConjunctivePredicates::evaluate_and(const Chunk* chunk, uint8_t* selection) const {
    evaluate_and(chunk, selection, 0, chunk->num_rows());
}

void ConjunctivePredicates::evaluate_or(const Chunk* chunk, uint8_t* selection) const {
    evaluate_or(chunk, selection, 0, chunk->num_rows());
}

void ConjunctivePredicates::evaluate(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    DCHECK_LE(to, chunk->num_rows());
    if (!_vec_preds.empty()) {
        const ColumnPredicate* pred = _vec_preds[0];
        const Column* col = chunk->get_column_by_id(pred->column_id()).get();
        pred->evaluate(col, selection, from, to);
        for (size_t i = 1; i < _vec_preds.size(); i++) {
            pred = _vec_preds[i];
            col = chunk->get_column_by_id(pred->column_id()).get();
            pred->evaluate_and(col, selection, from, to);
        }
    }
    _evaluate_non_vec(chunk, selection, from, to);
}

void ConjunctivePredicates::evaluate_or(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    DCHECK_LE(to, chunk->num_rows());
    std::unique_ptr<uint8_t[]> buff(new uint8_t[chunk->num_rows()]);
    evaluate(chunk, buff.get(), from, to);
    const uint8_t* p = buff.get();
    for (uint16_t i = from; i < to; i++) {
        DCHECK((bool)(selection[i] | p[i]) == (selection[i] || p[i]));
        selection[i] |= p[i];
    }
}

void ConjunctivePredicates::evaluate_and(const Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    DCHECK_LE(to, chunk->num_rows());
    _evaluate_and(chunk, selection, from, to);
}

inline void ConjunctivePredicates::_evaluate_and(const Chunk* chunk, uint8_t* selection, uint16_t from,
                                                 uint16_t to) const {
    for (auto pred : _vec_preds) {
        const Column* col = chunk->get_column_by_id(pred->column_id()).get();
        pred->evaluate_and(col, selection, from, to);
    }
    _evaluate_non_vec(chunk, selection, from, to);
}

inline void ConjunctivePredicates::_evaluate_non_vec(const Chunk* chunk, uint8_t* selection, uint16_t from,
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
            selected_size = pred->evaluate_branchless(c.get(), _selected_idx.data(), selected_size);
        }

        memset(&selection[from], 0, to - from);
        for (uint16_t i = 0; i < selected_size; ++i) {
            selection[_selected_idx[i]] = 1;
        }
    }
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

} // namespace starrocks::vectorized
