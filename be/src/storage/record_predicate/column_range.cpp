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

#include "storage/record_predicate/column_range.h"

#include <fmt/format.h>

#include "column/chunk.h"
#include "storage/record_predicate/record_predicate_helper.h"

namespace starrocks {

ColumnRange::ColumnRange(const RecordPredicatePB& record_predicate_pb) :
        RecordPredicate(record_predicate_pb), _predicate_type(PredicateType::kUnknown) {}

Status ColumnRange::evaluate(Chunk* chunk, uint8_t* selection) const {
    return evaluate(chunk, selection, 0, chunk->num_rows());
}

Status ColumnRange::evaluate(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    RETURN_IF_ERROR(RecordPredicateHelper::check_valid_schema(*this, *chunk->schema()));
    RETURN_IF_ERROR(_init_predicates(*chunk->schema()));
    DCHECK_EQ(_column_names.size(), _selection_vectors.size());
    std::memset(selection + from, 0, to - from);
    for (int i = 0; i < _column_names.size(); i++) {
        if (_selection_vectors[i].size() < to) {
            _selection_vectors[i].resize(to);
        }
        if (_eq_selection_vectors[i].size() < to) {
            _eq_selection_vectors[i].resize(to);
        }
        const auto& column_name = _column_names[i];
        const auto& column = chunk->get_column_by_name(column_name);
        RETURN_IF_ERROR(_predicates[i]->evaluate(column.get(), _selection_vectors[i].data(), from, to));
        RETURN_IF_ERROR(_eq_predicates[i]->evaluate(column.get(), _eq_selection_vectors[i].data(), from, to));
    }

    for (int i = from; i < to; i++) {
        for (int j = 0; j < _column_names.size(); j++) {
            if (_selection_vectors[j][i]) {
                // For kGE or kLE, if the value is equal to the bound when satisfies the predicate,
                // we need to check the next column.
                if (_eq_selection_vectors[j][i]) {
                    if (j < _column_names.size() - 1) {
                        continue;
                    }
                }
                selection[i] = 1;
                break;
            } else if (!_eq_selection_vectors[j][i]) {
                // For all type, if the value is not equal to the bound, we need to break.
                break;
            }

            // If we reach here, it means the value equals the bound but strictly Range predicate evaluated to false.
            // This happens for kLT or kGT where equality implies we should check the next column.
        }
    }
    return Status::OK();
}

Status ColumnRange::init(const RecordPredicatePB& record_predicate_pb) {
    const auto& column_range_meta = record_predicate_pb.column_range();
    RETURN_IF_ERROR(_check_valid_pb(column_range_meta));
    _range = column_range_meta.range();
    _column_names.insert(_column_names.end(), column_range_meta.column_names().begin(),
                         column_range_meta.column_names().end());
    for (int i = 0; i < _column_names.size(); i++) {
        _selection_vectors.emplace_back(_APPROXIMATE_EVAL_ROWS, 0);
        _eq_selection_vectors.emplace_back(_APPROXIMATE_EVAL_ROWS, 0);
    }
    return Status::OK();
}

Status ColumnRange::_check_valid_pb(const ColumnRangeMeta& column_range_meta) const {
    if (column_range_meta.column_names().empty()) {
        return Status::InternalError("column range predicate has no range columns defined");
    }
    for (const auto& column_name : column_range_meta.column_names()) {
        if (column_name.empty()) {
            return Status::InternalError("column range predicate has empty range column name");
        }
    }

    const TabletRangePB& range = column_range_meta.range();
    // currently only support the following range types: <, <=, >, >=
    bool upper_excluded = range.has_upper_bound() && !range.has_lower_bound() && !range.upper_bound_included(); // <
    bool upper_included = range.has_upper_bound() && !range.has_lower_bound() && range.upper_bound_included();  // <=
    bool lower_excluded = range.has_lower_bound() && !range.has_upper_bound() && !range.lower_bound_included(); // >
    bool lower_included = range.has_lower_bound() && !range.has_upper_bound() && range.lower_bound_included();  // >=

    RETURN_IF(!upper_excluded && !upper_included && !lower_excluded && !lower_included,
              Status::InternalError("column range predicate has invalid range bounds"));

    return Status::OK();
}

Status ColumnRange::_init_predicates(const Schema& chunk_schema) const {
    if (!_predicates.empty() && !_eq_predicates.empty() && _predicates.size() == _eq_predicates.size() && 
        _predicates.size() == _column_names.size()) {
        return Status::OK();
    }
    _predicates.clear();
    _eq_predicates.clear();

    // Decide predicate type on first use; this is logically const because it depends
    // only on the protobuf range meta.
    if (_predicate_type == PredicateType::kUnknown) {
        const TabletRangePB& range = _range;
        bool upper_excluded = range.has_upper_bound() && !range.has_lower_bound() && !range.upper_bound_included(); // <
        bool upper_included = range.has_upper_bound() && !range.has_lower_bound() && range.upper_bound_included();  // <=
        bool lower_excluded = range.has_lower_bound() && !range.has_upper_bound() && !range.lower_bound_included(); // >
        bool lower_included = range.has_lower_bound() && !range.has_upper_bound() && range.lower_bound_included();  // >=

        if (upper_excluded) {
            _predicate_type = PredicateType::kLT;
        } else if (upper_included) {
            _predicate_type = PredicateType::kLE;
        } else if (lower_excluded) {
            _predicate_type = PredicateType::kGT;
        } else if (lower_included) {
            _predicate_type = PredicateType::kGE;
        } else {
            return Status::InternalError("column range predicate has invalid range bounds");
        }
    }

    const auto& bounds = (_predicate_type == PredicateType::kLT || _predicate_type == PredicateType::kLE)
                                 ? _range.upper_bound()
                                 : _range.lower_bound();
    DCHECK(bounds.values_size() == static_cast<int>(_column_names.size()));
    for (int i = 0; i < bounds.values_size(); ++i) {
        const auto& bound = bounds.values(i);
        const auto& column_name = _column_names[i];
        auto field = chunk_schema.get_field_by_name(column_name);
        DCHECK(field != nullptr);
        DCHECK(bound.has_int_value() || bound.has_string_value());

        std::string bound_str;
        if (bound.has_int_value()) {
            bound_str = std::to_string(bound.int_value());
        } else if (bound.has_string_value()) {
            bound_str = bound.string_value();
        }
        Slice slice(bound_str);
        ColumnPredicate* pred = new_column_cmp_predicate(_predicate_type, field->type(), field->id(), slice);

        if (pred == nullptr) {
            return Status::InternalError(
                    fmt::format("failed to create column predicate for column range predicate, {}", column_name));
        }
        _predicates.emplace_back(pred);

        ColumnPredicate* eq_pred = new_column_cmp_predicate(PredicateType::kEQ, field->type(), field->id(), slice);
        if (eq_pred == nullptr) {
            return Status::InternalError(
                    fmt::format("failed to create column eq predicate for column range predicate, {}", column_name));
        }
        _eq_predicates.emplace_back(eq_pred);
    }
    return Status::OK();
}

} // namespace starrocks
