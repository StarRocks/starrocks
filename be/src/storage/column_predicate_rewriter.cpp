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

#include "storage/column_predicate_rewriter.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <utility>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/datum.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/object_pool.h"
#include "common/statusor.h"
#include "exprs/expr_context.h"
#include "exprs/in_const_predicate.hpp"
#include "exprs/runtime_filter_bank.h"
#include "gutil/casts.h"
#include "runtime/global_dict/config.h"
#include "runtime/global_dict/miscs.h"
#include "simd/simd.h"
#include "storage/column_expr_predicate.h"
#include "storage/column_predicate.h"
#include "storage/range.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/scalar_column_iterator.h"

namespace starrocks {

// ------------------------------------------------------------------------------------
// ColumnPredicateRewriter
// ------------------------------------------------------------------------------------

constexpr static const LogicalType kDictCodeType = TYPE_INT;

Status ColumnPredicateRewriter::rewrite_predicate(ObjectPool* pool) {
    // because schema has reordered
    // so we only need to check the first `predicate_column_size` fields
    for (size_t i = 0; i < _column_size; i++) {
        const FieldPtr& field = _schema.field(i);
        const auto cid = field->id();

        auto iter = _pred_map.find(cid);
        if (iter == _pred_map.end()) {
            continue;
        }
        auto& preds = iter->second;

        std::vector<const ColumnPredicate*> remove_list;
        for (auto& pred : preds) {
            ColumnPredicate* new_pred = nullptr;
            ASSIGN_OR_RETURN(auto rewrite_status, _rewrite_predicate(pool, field, pred, &new_pred));

            switch (rewrite_status) {
            case RewriteStatus::ALWAYS_TRUE:
                remove_list.emplace_back(pred);
                break;
            case RewriteStatus::ALWAYS_FALSE:
                // predicate always false, clear scan range, this will make `get_next` return EOF directly.
                _scan_range = _scan_range.intersection(SparseRange<>());
                return Status::OK();
            case RewriteStatus::CHANGED:
                pred = pool->add(new_pred);
                break;
            case RewriteStatus::UNCHANGED:
                [[fallthrough]];
            default:
                break; // Do nothing.
            }
        }

        for (const auto pred_will_remove : remove_list) {
            auto willrm = std::find(preds.begin(), preds.end(), pred_will_remove);
            preds.erase(willrm);
        }
    }
    return Status::OK();
}

StatusOr<ColumnPredicateRewriter::RewriteStatus> ColumnPredicateRewriter::_rewrite_predicate(
        ObjectPool* pool, const FieldPtr& field, const ColumnPredicate* pred, ColumnPredicate** dest_pred) {
    const auto cid = field->id();
    if (!_need_rewrite[cid]) {
        return RewriteStatus::UNCHANGED;
    }
    DCHECK(_column_iterators[cid]->all_page_dict_encoded());

    if (PredicateType::kEQ == pred->type()) {
        Datum value = pred->value();
        int code = _column_iterators[cid]->dict_lookup(value.get_slice());
        if (code < 0) {
            return RewriteStatus::ALWAYS_FALSE;
        }
        *dest_pred = new_column_eq_predicate(get_type_info(kDictCodeType), cid, std::to_string(code));
        return RewriteStatus::CHANGED;
    }

    if (PredicateType::kNE == pred->type()) {
        Datum value = pred->value();
        int code = _column_iterators[cid]->dict_lookup(value.get_slice());
        if (code < 0) {
            if (!field->is_nullable()) {
                return RewriteStatus::ALWAYS_TRUE;
            } else {
                // convert this predicate to `not null` predicate.
                *dest_pred = new_column_null_predicate(get_type_info(kDictCodeType), cid, false);
                return RewriteStatus::CHANGED;
            }
        }
        *dest_pred = new_column_ne_predicate(get_type_info(kDictCodeType), cid, std::to_string(code));
        return RewriteStatus::CHANGED;
    }

    if (PredicateType::kInList == pred->type()) {
        std::vector<Datum> values = pred->values();
        std::vector<int> codewords;
        for (const auto& value : values) {
            if (int code = _column_iterators[cid]->dict_lookup(value.get_slice()); code >= 0) {
                codewords.emplace_back(code);
            }
        }
        if (codewords.empty()) {
            return RewriteStatus::ALWAYS_FALSE;
        }
        std::vector<std::string> str_codewords;
        str_codewords.reserve(codewords.size());
        for (int code : codewords) {
            str_codewords.emplace_back(std::to_string(code));
        }
        *dest_pred = new_column_in_predicate(get_type_info(kDictCodeType), cid, str_codewords);
        return RewriteStatus::CHANGED;
    }

    if (PredicateType::kNotInList == pred->type()) {
        std::vector<Datum> values = pred->values();
        std::vector<int> codewords;
        for (const auto& value : values) {
            if (int code = _column_iterators[cid]->dict_lookup(value.get_slice()); code >= 0) {
                codewords.emplace_back(code);
            }
        }
        if (codewords.empty()) {
            if (!field->is_nullable()) {
                return RewriteStatus::ALWAYS_TRUE;
            } else {
                // convert this predicate to `not null` predicate.
                *dest_pred = new_column_null_predicate(get_type_info(kDictCodeType), cid, false);
                return RewriteStatus::CHANGED;
            }
        }
        std::vector<std::string> str_codewords;
        str_codewords.reserve(codewords.size());
        for (int code : codewords) {
            str_codewords.emplace_back(std::to_string(code));
        }
        *dest_pred = new_column_not_in_predicate(get_type_info(kDictCodeType), cid, str_codewords);
        return RewriteStatus::CHANGED;
    }

    if (PredicateType::kGE == pred->type() || PredicateType::kGT == pred->type()) {
        ASSIGN_OR_RETURN(const auto* sorted_dicts_ptr, _get_or_load_segment_dict(cid));
        const auto& sorted_dicts = *sorted_dicts_ptr;

        // use non-padding string value.
        auto value = pred->values()[0].get_slice().to_string();
        auto iter =
                std::lower_bound(sorted_dicts.begin(), sorted_dicts.end(), value,
                                 [](const auto& entity, const auto& value) { return entity.first.compare(value) < 0; });
        std::vector<std::string> str_codewords;
        // X > 3.5 find 4, range(4, inf)
        // X > 3 find 3, range(3, inf)
        // X >= 3.5 find 4, range(4, inf)
        // X >= 3 find 3, range(3, inf)
        if (PredicateType::kGT == pred->type() && iter != sorted_dicts.end() && iter->first == value) {
            iter++;
        }
        while (iter != sorted_dicts.end()) {
            str_codewords.push_back(std::to_string(iter->second));
            iter++;
        }
        if (!str_codewords.empty()) {
            *dest_pred = new_column_in_predicate(get_type_info(kDictCodeType), cid, str_codewords);
            return RewriteStatus::CHANGED;
        } else {
            return RewriteStatus::ALWAYS_FALSE;
        }
    }

    if (PredicateType::kLE == pred->type() || PredicateType::kLT == pred->type()) {
        ASSIGN_OR_RETURN(const auto* sorted_dicts_ptr, _get_or_load_segment_dict(cid));
        const auto& sorted_dicts = *sorted_dicts_ptr;

        // use non-padding string value.
        auto value = pred->values()[0].get_slice().to_string();
        auto iter =
                std::lower_bound(sorted_dicts.begin(), sorted_dicts.end(), value,
                                 [](const auto& entity, const auto& value) { return entity.first.compare(value) < 0; });
        std::vector<std::string> str_codewords;
        auto begin_iter = sorted_dicts.begin();
        // X < 3.5 find 4, range(-inf, 3)
        // X < 3 find 3, range(-inf, 2)
        // X <= 3.5 find 4, range(-inf, 3)
        // X <= 3 find 3, range(-inf, 3)
        if (!(PredicateType::kLE == pred->type() && iter != sorted_dicts.end() && iter->first == value)) {
            iter--;
        }
        while (begin_iter <= iter && begin_iter != sorted_dicts.end()) {
            str_codewords.push_back(std::to_string(begin_iter->second));
            begin_iter++;
        }
        if (!str_codewords.empty()) {
            *dest_pred = new_column_in_predicate(get_type_info(kDictCodeType), cid, str_codewords);
            return RewriteStatus::CHANGED;
        } else {
            return RewriteStatus::ALWAYS_FALSE;
        }
    }

    if (PredicateType::kExpr == pred->type()) {
        ASSIGN_OR_RETURN(const auto* dict_and_codes_ptr, _get_or_load_segment_dict_vec(cid, field));
        const auto& [dict_column, code_column] = *dict_and_codes_ptr;

        return _rewrite_expr_predicate(pool, dict_column, code_column, field->is_nullable(), pred, dest_pred);
    }

    return RewriteStatus::UNCHANGED;
}

StatusOr<const ColumnPredicateRewriter::SortedDicts*> ColumnPredicateRewriter::_get_or_load_segment_dict(ColumnId cid) {
    auto it = _cid_to_sorted_dicts.find(cid);
    if (it == _cid_to_sorted_dicts.end()) {
        it = _cid_to_sorted_dicts.emplace(cid, SortedDicts{}).first;
        RETURN_IF_ERROR(_load_segment_dict(&it->second, _column_iterators[cid].get()));
    }

    return &it->second;
}

// This function is only used to rewrite the LE/LT/GE/GT condition.
// For the greater than or less than condition,
// you need to get the values of all ordered dictionaries and rewrite them as `InList` expressions
Status ColumnPredicateRewriter::_load_segment_dict(std::vector<std::pair<std::string, int>>* dicts,
                                                   ColumnIterator* iter) {
    // We already loaded dicts, no need to do once more.
    if (!dicts->empty()) {
        return Status::OK();
    }
    auto column_iterator = down_cast<ScalarColumnIterator*>(iter);
    auto dict_size = column_iterator->dict_size();
    int dict_codes[dict_size];
    std::iota(dict_codes, dict_codes + dict_size, 0);

    auto column = BinaryColumn::create();
    RETURN_IF_ERROR(column_iterator->decode_dict_codes(dict_codes, dict_size, column.get()));

    for (int i = 0; i < dict_size; ++i) {
        dicts->emplace_back(column->get_slice(i).to_string(), dict_codes[i]);
    }

    std::sort(dicts->begin(), dicts->end(),
              [](const auto& e1, const auto& e2) { return e1.first.compare(e2.first) < 0; });
    return Status::OK();
}

StatusOr<const ColumnPredicateRewriter::DictAndCodes*> ColumnPredicateRewriter::_get_or_load_segment_dict_vec(
        ColumnId cid, const FieldPtr& field) {
    auto it = _cid_to_vec_sorted_dicts.find(cid);
    if (it == _cid_to_vec_sorted_dicts.end()) {
        it = _cid_to_vec_sorted_dicts.emplace(cid, std::make_pair(nullptr, nullptr)).first;
        auto& [dict_column, code_column] = it->second;
        RETURN_IF_ERROR(
                _load_segment_dict_vec(_column_iterators[cid].get(), &dict_column, &code_column, field->is_nullable()));
    }

    return &it->second;
}

Status ColumnPredicateRewriter::_load_segment_dict_vec(ColumnIterator* iter, ColumnPtr* dict_column,
                                                       ColumnPtr* code_column, bool field_nullable) {
    auto column_iterator = down_cast<ScalarColumnIterator*>(iter);
    auto dict_size = column_iterator->dict_size();
    int dict_codes[dict_size];
    std::iota(dict_codes, dict_codes + dict_size, 0);

    auto dict_col = BinaryColumn::create();
    RETURN_IF_ERROR(column_iterator->decode_dict_codes(dict_codes, dict_size, dict_col.get()));

    if (field_nullable) {
        // create nullable column with NULL at last.
        NullColumnPtr null_col = NullColumn::create();
        null_col->resize(dict_size);
        auto null_column = NullableColumn::create(dict_col, null_col);
        null_column->append_default();
        *dict_column = null_column;
    } else {
        // otherwise we just give binary column.
        *dict_column = dict_col;
    }

    auto code_col = Int32Column::create();
    code_col->resize(dict_size);
    auto& code_buf = code_col->get_data();
    for (int i = 0; i < dict_size; i++) {
        code_buf[i] = dict_codes[i];
    }
    *code_column = code_col;
    return Status::OK();
}

StatusOr<ColumnPredicateRewriter::RewriteStatus> ColumnPredicateRewriter::_rewrite_expr_predicate(
        ObjectPool* pool, const ColumnPtr& raw_dict_column, const ColumnPtr& raw_code_column, bool field_nullable,
        const ColumnPredicate* src_pred, ColumnPredicate** dest_pred) {
    *dest_pred = nullptr;
    size_t value_size = raw_dict_column->size();
    std::vector<uint8_t> selection(value_size);
    const auto* pred = down_cast<const ColumnExprPredicate*>(src_pred);
    size_t chunk_size = std::min<size_t>(pred->runtime_state()->chunk_size(), std::numeric_limits<uint16_t>::max());

    if (value_size <= chunk_size) {
        RETURN_IF_ERROR(pred->evaluate(raw_dict_column.get(), selection.data(), 0, value_size));
    } else {
        auto dict_column = raw_dict_column->clone_empty();
        SparseRange<> range(0, value_size);
        auto iter = range.new_iterator();
        auto selection_cursor = selection.data();
        while (iter.has_more()) {
            auto next_range = iter.next(chunk_size);
            size_t num_rows = next_range.span_size();
            DCHECK_LE(next_range.begin() + num_rows, raw_dict_column->size());
            dict_column->append(*raw_dict_column, next_range.begin(), num_rows);
            RETURN_IF_ERROR(pred->evaluate(dict_column.get(), selection_cursor, 0, num_rows));
            dict_column->reset_column();
            selection_cursor += num_rows;
        }
    }

    size_t code_size = raw_code_column->size();
    const auto& code_column = ColumnHelper::cast_to<TYPE_INT>(raw_code_column);
    const auto& code_values = code_column->get_data();
    if (field_nullable) {
        DCHECK((code_size + 1) == value_size);
    } else {
        DCHECK(code_size == value_size);
    }

    size_t false_count = SIMD::count_zero(selection);
    size_t true_count = (value_size - false_count);
    if (true_count == 0) {
        return RewriteStatus::ALWAYS_FALSE;
    }

    if (false_count == 0) {
        // always true.
        return RewriteStatus::ALWAYS_TRUE;
    }

    // TODO(yan): use eq/ne predicates when only one item, but it's very very hard to construct ne/eq expr.
    auto used_values = Int32Column::create();
    for (int i = 0; i < code_size; i++) {
        if (selection[i]) {
            used_values->append(code_values[i]);
        }
    }
    bool eq_null = true;
    bool null_in_set = false;
    if (field_nullable && selection[code_size]) {
        null_in_set = true;
    }
    bool is_not_in = false;

    // construct in filter.
    RuntimeState* state = pred->runtime_state();
    ColumnRef column_ref(pred->slot_desc());
    // change column input type from binary to int(code)
    TypeDescriptor type_desc = TypeDescriptor::from_logical_type(TYPE_INT);
    column_ref._type = type_desc;
    Expr* probe_expr = &column_ref;

    // probe_expr will be copied into filter, so we don't need to allocate it.
    VectorizedInConstPredicateBuilder builder(state, pool, probe_expr);
    builder.set_eq_null(eq_null);
    builder.set_null_in_set(null_in_set);
    builder.set_is_not_in(is_not_in);
    builder.use_array_set(code_size);
    DCHECK_IF_ERROR(builder.create());
    (void)builder.add_values(used_values, 0);
    ExprContext* filter = builder.get_in_const_predicate();

    DCHECK_IF_ERROR(filter->prepare(state));
    DCHECK_IF_ERROR(filter->open(state));
    ASSIGN_OR_RETURN(*dest_pred,
                     ColumnExprPredicate::make_column_expr_predicate(get_type_info(kDictCodeType), pred->column_id(),
                                                                     state, filter, pred->slot_desc()))
    filter->close(state);

    return RewriteStatus::CHANGED;
}

// ------------------------------------------------------------------------------------
// GlobalDictPredicatesRewriter
// ------------------------------------------------------------------------------------

StatusOr<ColumnPredicatePtr> GlobalDictPredicatesRewriter::_rewrite_predicate(const ColumnPredicate* pred,
                                                                              std::vector<uint8_t>& selection) {
    if (!_column_need_rewrite(pred->column_id())) {
        return nullptr;
    }

    const auto& dict = _dict_maps.at(pred->column_id());
    ChunkPtr temp_chunk = std::make_shared<Chunk>();

    auto [binary_column, codes] = extract_column_with_codes(*dict);

    size_t dict_rows = codes.size();
    selection.resize(dict_rows);

    RETURN_IF_ERROR(pred->evaluate(binary_column.get(), selection.data(), 0, dict_rows));

    std::vector<uint8_t> code_mapping;
    code_mapping.resize(DICT_DECODE_MAX_SIZE + 1);
    for (size_t i = 0; i < codes.size(); ++i) {
        code_mapping[codes[i]] = selection[i];
    }

    auto* new_pred =
            new_column_dict_conjuct_predicate(get_type_info(kDictCodeType), pred->column_id(), std::move(code_mapping));
    new_pred->set_index_filter_only(pred->is_index_filter_only());
    return std::unique_ptr<ColumnPredicate>(new_pred);
}

Status GlobalDictPredicatesRewriter::rewrite_predicate(ObjectPool* pool, ConjunctivePredicates& predicates) {
    std::vector<uint8_t> selection;
    auto preds_rewrite = [&](std::vector<const ColumnPredicate*>& preds) {
        for (auto& pred : preds) {
            ASSIGN_OR_RETURN(auto new_pred, _rewrite_predicate(pred, selection));
            if (new_pred != nullptr) {
                pred = pool->add(new_pred.release());
            }
        }
        return Status::OK();
    };

    RETURN_IF_ERROR(preds_rewrite(predicates.non_vec_preds()));
    RETURN_IF_ERROR(preds_rewrite(predicates.vec_preds()));

    return Status::OK();
}

// ------------------------------------------------------------------------------------
// ZonemapPredicatesRewriter
// ------------------------------------------------------------------------------------

Status ZonemapPredicatesRewriter::rewrite_predicate_map(ObjectPool* pool, const ColumnPredicateMap& src_pred_map,
                                                        ColumnPredicateMap* dst_pred_map) {
    DCHECK(dst_pred_map != nullptr);
    for (auto& [cid, src_preds] : src_pred_map) {
        auto& dst_preds = dst_pred_map->insert({cid, {}}).first->second;

        for (const auto* src_pred : src_preds) {
            RETURN_IF_ERROR(_rewrite_predicate(pool, src_pred, dst_preds));
        }
    }
    return Status::OK();
}

Status ZonemapPredicatesRewriter::_rewrite_predicate(ObjectPool* pool, const ColumnPredicate* src_pred,
                                                     ColumnPredicates& dst_preds) {
    if (!src_pred->is_expr_predicate()) {
        dst_preds.emplace_back(src_pred);
    } else {
        std::vector<const ColumnExprPredicate*> new_preds;
        RETURN_IF_ERROR(_rewrite_column_expr_predicate(pool, src_pred, new_preds));
        if (!new_preds.empty()) {
            dst_preds.insert(dst_preds.end(), new_preds.begin(), new_preds.end());
        } else {
            dst_preds.emplace_back(src_pred);
        }
    }
    return Status::OK();
}

Status ZonemapPredicatesRewriter::_rewrite_column_expr_predicate(ObjectPool* pool, const ColumnPredicate* src_pred,
                                                                 std::vector<const ColumnExprPredicate*>& dst_preds) {
    DCHECK(src_pred != nullptr);
    const auto* column_expr_pred = down_cast<const ColumnExprPredicate*>(src_pred);
    return column_expr_pred->try_to_rewrite_for_zone_map_filter(pool, &dst_preds);
}

} // namespace starrocks
