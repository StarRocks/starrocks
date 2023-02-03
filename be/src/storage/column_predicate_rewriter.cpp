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
#include "storage/rowset/column_reader.h"
#include "storage/rowset/scalar_column_iterator.h"

namespace starrocks {
constexpr static const LogicalType kDictCodeType = TYPE_INT;

Status ColumnPredicateRewriter::rewrite_predicate(ObjectPool* pool) {
    // because schema has reordered
    // so we only need to check the first `predicate_column_size` fields
    for (size_t i = 0; i < _column_size; i++) {
        const FieldPtr& field = _schema.field(i);
        ColumnId cid = field->id();
        if (_need_rewrite[cid]) {
            RETURN_IF_ERROR(_rewrite_predicate(pool, field));
        }
    }
    return Status::OK();
}

StatusOr<bool> ColumnPredicateRewriter::_rewrite_predicate(ObjectPool* pool, const FieldPtr& field) {
    auto cid = field->id();
    DCHECK(_column_iterators[cid]->all_page_dict_encoded());
    auto iter = _predicates.find(cid);
    if (iter == _predicates.end()) {
        return false;
    }
    PredicateList& preds = iter->second;
    // the predicate has been erased, because of bitmap index filter.
    RETURN_IF(preds.empty(), false);

    // TODO: use pair<slice,int>
    std::vector<std::pair<std::string, int>> sorted_dicts{};

    std::vector<const ColumnPredicate*> remove_list;

    for (auto& i : preds) {
        const ColumnPredicate* pred = i;
        if (PredicateType::kEQ == pred->type()) {
            Datum value = pred->value();
            int code = _column_iterators[cid]->dict_lookup(value.get_slice());
            if (code < 0) {
                // predicate always false, clear scan range, this will make `get_next` return EOF directly.
                _scan_range = _scan_range.intersection(SparseRange());
                continue;
            }
            auto ptr = new_column_eq_predicate(get_type_info(kDictCodeType), cid, std::to_string(code));
            i = pool->add(ptr);
            continue;
        }
        if (PredicateType::kNE == pred->type()) {
            Datum value = pred->value();
            int code = _column_iterators[cid]->dict_lookup(value.get_slice());
            if (code < 0) {
                if (!field->is_nullable()) {
                    // predicate always true, clear this predicate.
                    remove_list.push_back(i);
                    continue;
                } else {
                    // convert this predicate to `not null` predicate.
                    auto ptr = new_column_null_predicate(get_type_info(kDictCodeType), cid, false);
                    i = pool->add(ptr);
                    continue;
                }
            }
            auto ptr = new_column_ne_predicate(get_type_info(kDictCodeType), cid, std::to_string(code));
            i = pool->add(ptr);
            continue;
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
                // predicate always false, clear scan range.
                _scan_range = _scan_range.intersection(SparseRange());
                continue;
            }
            std::vector<std::string> str_codewords;
            str_codewords.reserve(codewords.size());
            for (int code : codewords) {
                str_codewords.emplace_back(std::to_string(code));
            }
            auto ptr = new_column_in_predicate(get_type_info(kDictCodeType), cid, str_codewords);
            i = pool->add(ptr);
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
                    // predicate always true, clear this predicate.
                    remove_list.push_back(i);
                    continue;
                } else {
                    // convert this predicate to `not null` predicate.
                    auto ptr = new_column_null_predicate(get_type_info(kDictCodeType), cid, false);
                    i = pool->add(ptr);
                    continue;
                }
            }
            std::vector<std::string> str_codewords;
            str_codewords.reserve(codewords.size());
            for (int code : codewords) {
                str_codewords.emplace_back(std::to_string(code));
            }
            auto ptr = new_column_not_in_predicate(get_type_info(kDictCodeType), cid, str_codewords);
            i = pool->add(ptr);
        }
        if (PredicateType::kGE == pred->type() || PredicateType::kGT == pred->type()) {
            _get_segment_dict(&sorted_dicts, _column_iterators[cid].get());
            // use non-padding string value.
            auto value = pred->values()[0].get_slice().to_string();
            auto iter = std::lower_bound(
                    sorted_dicts.begin(), sorted_dicts.end(), value,
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
                auto ptr = new_column_in_predicate(get_type_info(kDictCodeType), cid, str_codewords);
                i = pool->add(ptr);
            } else {
                _scan_range = _scan_range.intersection(SparseRange());
                continue;
            }
        }
        if (PredicateType::kLE == pred->type() || PredicateType::kLT == pred->type()) {
            _get_segment_dict(&sorted_dicts, _column_iterators[cid].get());
            // use non-padding string value.
            auto value = pred->values()[0].get_slice().to_string();
            auto iter = std::lower_bound(
                    sorted_dicts.begin(), sorted_dicts.end(), value,
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
                auto ptr = new_column_in_predicate(get_type_info(kDictCodeType), cid, str_codewords);
                i = pool->add(ptr);
            } else {
                _scan_range = _scan_range.intersection(SparseRange());
                continue;
            }
        }
    }

    bool load_seg_dict_vec = false;
    ColumnPtr dict_column;
    ColumnPtr code_column;
    for (auto& i : preds) {
        const ColumnPredicate* pred = i;
        if (PredicateType::kExpr == pred->type()) {
            if (!load_seg_dict_vec) {
                load_seg_dict_vec = true;
                _get_segment_dict_vec(_column_iterators[cid].get(), &dict_column, &code_column, field->is_nullable());
            }

            ColumnPredicate* ptr;
            ASSIGN_OR_RETURN(bool non_empty,
                             _rewrite_expr_predicate(pool, pred, dict_column, code_column, field->is_nullable(), &ptr));
            if (!non_empty) {
                _scan_range = _scan_range.intersection(SparseRange());
            } else {
                i = pool->add(ptr);
            }
        }
    }

    for (const auto pred_will_remove : remove_list) {
        auto willrm = std::find(preds.begin(), preds.end(), pred_will_remove);
        preds.erase(willrm);
    }

    return true;
}

// This function is only used to rewrite the LE/LT/GE/GT condition.
// For the greater than or less than condition,
// you need to get the values of all ordered dictionaries and rewrite them as `InList` expressions
void ColumnPredicateRewriter::_get_segment_dict(std::vector<std::pair<std::string, int>>* dicts, ColumnIterator* iter) {
    // We already loaded dicts, no need to do once more.
    if (!dicts->empty()) {
        return;
    }
    auto column_iterator = down_cast<ScalarColumnIterator*>(iter);
    auto dict_size = column_iterator->dict_size();
    int dict_codes[dict_size];
    std::iota(dict_codes, dict_codes + dict_size, 0);

    auto column = BinaryColumn::create();
    column_iterator->decode_dict_codes(dict_codes, dict_size, column.get());

    for (int i = 0; i < dict_size; ++i) {
        dicts->emplace_back(column->get_slice(i).to_string(), dict_codes[i]);
    }

    std::sort(dicts->begin(), dicts->end(),
              [](const auto& e1, const auto& e2) { return e1.first.compare(e2.first) < 0; });
}

void ColumnPredicateRewriter::_get_segment_dict_vec(ColumnIterator* iter, ColumnPtr* dict_column,
                                                    ColumnPtr* code_column, bool field_nullable) {
    auto column_iterator = down_cast<ScalarColumnIterator*>(iter);
    auto dict_size = column_iterator->dict_size();
    int dict_codes[dict_size];
    std::iota(dict_codes, dict_codes + dict_size, 0);

    auto dict_col = BinaryColumn::create();
    column_iterator->decode_dict_codes(dict_codes, dict_size, dict_col.get());

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
}

StatusOr<bool> ColumnPredicateRewriter::_rewrite_expr_predicate(ObjectPool* pool, const ColumnPredicate* raw_pred,
                                                                const ColumnPtr& raw_dict_column,
                                                                const ColumnPtr& raw_code_column, bool field_nullable,
                                                                ColumnPredicate** ptr) {
    *ptr = nullptr;
    size_t value_size = raw_dict_column->size();
    std::vector<uint8_t> selection(value_size);
    const auto* pred = down_cast<const ColumnExprPredicate*>(raw_pred);
    RETURN_IF_ERROR(pred->evaluate(raw_dict_column.get(), selection.data(), 0, value_size));

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
        return false;
    }

    if (false_count == 0) {
        // always true.
        *ptr = new ColumnTruePredicate(get_type_info(kDictCodeType), pred->column_id());
        return true;
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
    TypeDescriptor type_desc = TypeDescriptor::from_primtive_type(TYPE_INT);
    column_ref._type = type_desc;
    Expr* probe_expr = &column_ref;

    // probe_expr will be copied into filter, so we don't need to allocate it.
    VectorizedInConstPredicateBuilder builder(state, pool, probe_expr);
    builder.set_eq_null(eq_null);
    builder.set_null_in_set(null_in_set);
    builder.set_is_not_in(is_not_in);
    builder.use_array_set(code_size);
    DCHECK_IF_ERROR(builder.create());
    builder.add_values(used_values, 0);
    ExprContext* filter = builder.get_in_const_predicate();

    DCHECK_IF_ERROR(filter->prepare(state));
    DCHECK_IF_ERROR(filter->open(state));
    ASSIGN_OR_RETURN(*ptr, ColumnExprPredicate::make_column_expr_predicate(
                                   get_type_info(kDictCodeType), pred->column_id(), state, filter, pred->slot_desc()))
    filter->close(state);

    return true;
}

// member function for ConjunctivePredicatesRewriter

Status ConjunctivePredicatesRewriter::rewrite_predicate(ObjectPool* pool) {
    std::vector<uint8_t> selection;
    auto pred_rewrite = [&](std::vector<const ColumnPredicate*>& preds) {
        for (auto& pred : preds) {
            if (column_need_rewrite(pred->column_id())) {
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

                pred = new_column_dict_conjuct_predicate(get_type_info(kDictCodeType), pred->column_id(),
                                                         std::move(code_mapping));
                pool->add(const_cast<ColumnPredicate*>(pred));
            }
        }
        return Status::OK();
    };

    RETURN_IF_ERROR(pred_rewrite(_predicates.non_vec_preds()));
    RETURN_IF_ERROR(pred_rewrite(_predicates.vec_preds()));

    return Status::OK();
}

Status ZonemapPredicatesRewriter::rewrite_predicate_map(ObjectPool* pool,
                                                        const std::unordered_map<ColumnId, PredicateList>& src,
                                                        std::unordered_map<ColumnId, PredicateList>* dst) {
    DCHECK(dst != nullptr);
    for (auto& [cid, preds] : src) {
        dst->insert({cid, {}});
        auto& preds_after_rewrite = dst->at(cid);
        RETURN_IF_ERROR(rewrite_predicate_list(pool, preds, &preds_after_rewrite));
    }
    return Status::OK();
}

Status ZonemapPredicatesRewriter::rewrite_predicate_list(ObjectPool* pool, const PredicateList& src,
                                                         PredicateList* dst) {
    DCHECK(dst != nullptr);
    for (auto& pred : src) {
        if (pred->is_expr_predicate()) {
            std::vector<const ColumnExprPredicate*> new_preds;
            RETURN_IF_ERROR(_rewrite_column_expr_predicates(pool, pred, &new_preds));
            if (!new_preds.empty()) {
                dst->insert(dst->end(), new_preds.begin(), new_preds.end());
            } else {
                dst->emplace_back(pred);
            }
        } else {
            dst->emplace_back(pred);
        }
    }
    return Status::OK();
}

Status ZonemapPredicatesRewriter::_rewrite_column_expr_predicates(ObjectPool* pool, const ColumnPredicate* pred,
                                                                  std::vector<const ColumnExprPredicate*>* new_preds) {
    DCHECK(new_preds != nullptr);
    const auto* column_expr_pred = static_cast<const ColumnExprPredicate*>(pred);
    return column_expr_pred->try_to_rewrite_for_zone_map_filter(pool, new_preds);
}

} // namespace starrocks
