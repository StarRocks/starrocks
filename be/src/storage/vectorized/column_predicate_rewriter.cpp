// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/column_predicate_rewriter.h"

#include <algorithm>
#include <cstdint>
#include <utility>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/datum.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exprs/expr_context.h"
#include "exprs/vectorized/in_const_predicate.hpp"
#include "exprs/vectorized/runtime_filter_bank.h"
#include "runtime/global_dicts.h"
#include "simd/simd.h"
#include "storage/rowset/segment_v2/column_reader.h"
#include "storage/rowset/segment_v2/scalar_column_iterator.h"
#include "storage/vectorized/column_expr_predicate.h"
#include "storage/vectorized/column_predicate.h"

namespace starrocks::vectorized {
constexpr static const FieldType kDictCodeType = OLAP_FIELD_TYPE_INT;

void ColumnPredicateRewriter::rewrite_predicate(ObjectPool* pool) {
    // because schema has reordered
    // so we only need to check the first `predicate_column_size` fields
    for (size_t i = 0; i < _column_size; i++) {
        const FieldPtr& field = _schema.field(i);
        ColumnId cid = field->id();
        if (_need_rewrite[cid]) {
            _rewrite_predicate(pool, field);
        }
    }
}

bool ColumnPredicateRewriter::_rewrite_predicate(ObjectPool* pool, const FieldPtr& field) {
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

    for (int i = 0; i < preds.size(); ++i) {
        const ColumnPredicate* pred = preds[i];
        if (PredicateType::kEQ == pred->type()) {
            Datum value = pred->value();
            int code = _column_iterators[cid]->dict_lookup(value.get_slice());
            if (code < 0) {
                // predicate always false, clear scan range, this will make `get_next` return EOF directly.
                _scan_range = _scan_range.intersection(SparseRange());
                continue;
            }
            auto ptr = new_column_eq_predicate(get_type_info(kDictCodeType), cid, std::to_string(code));
            preds[i] = pool->add(ptr);
            continue;
        }
        if (PredicateType::kNE == pred->type()) {
            Datum value = pred->value();
            int code = _column_iterators[cid]->dict_lookup(value.get_slice());
            if (code < 0) {
                if (!field->is_nullable()) {
                    // predicate always true, clear this predicate.
                    remove_list.push_back(preds[i]);
                    continue;
                } else {
                    // convert this predicate to `not null` predicate.
                    auto ptr = new_column_null_predicate(get_type_info(kDictCodeType), cid, false);
                    preds[i] = pool->add(ptr);
                    continue;
                }
            }
            auto ptr = new_column_ne_predicate(get_type_info(kDictCodeType), cid, std::to_string(code));
            preds[i] = pool->add(ptr);
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
            preds[i] = pool->add(ptr);
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
                    remove_list.push_back(preds[i]);
                    continue;
                } else {
                    // convert this predicate to `not null` predicate.
                    auto ptr = new_column_null_predicate(get_type_info(kDictCodeType), cid, false);
                    preds[i] = pool->add(ptr);
                    continue;
                }
            }
            std::vector<std::string> str_codewords;
            str_codewords.reserve(codewords.size());
            for (int code : codewords) {
                str_codewords.emplace_back(std::to_string(code));
            }
            auto ptr = new_column_not_in_predicate(get_type_info(kDictCodeType), cid, str_codewords);
            preds[i] = pool->add(ptr);
        }
        if (PredicateType::kGE == pred->type() || PredicateType::kGT == pred->type()) {
            _get_segment_dict(&sorted_dicts, _column_iterators[cid]);
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
            while (iter < sorted_dicts.end()) {
                str_codewords.push_back(std::to_string(iter->second));
                iter++;
            }
            if (!str_codewords.empty()) {
                auto ptr = new_column_in_predicate(get_type_info(kDictCodeType), cid, str_codewords);
                preds[i] = pool->add(ptr);
            } else {
                _scan_range = _scan_range.intersection(SparseRange());
                continue;
            }
        }
        if (PredicateType::kLE == pred->type() || PredicateType::kLT == pred->type()) {
            _get_segment_dict(&sorted_dicts, _column_iterators[cid]);
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
                preds[i] = pool->add(ptr);
            } else {
                _scan_range = _scan_range.intersection(SparseRange());
                continue;
            }
        }
    }

    bool load_seg_dict_vec = false;
    ColumnPtr dict_column;
    ColumnPtr code_column;
    for (int i = 0; i < preds.size(); ++i) {
        const ColumnPredicate* pred = preds[i];
        if (PredicateType::kExpr == pred->type()) {
            if (!load_seg_dict_vec) {
                load_seg_dict_vec = true;
                _get_segment_dict_vec(_column_iterators[cid], &dict_column, &code_column, field->is_nullable());
            }

            ColumnPredicate* ptr;
            bool non_empty = _rewrite_expr_predicate(pool, pred, dict_column, code_column, field->is_nullable(), &ptr);
            if (!non_empty) {
                _scan_range = _scan_range.intersection(SparseRange());
            } else {
                preds[i] = pool->add(ptr);
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
void ColumnPredicateRewriter::_get_segment_dict(std::vector<std::pair<std::string, int>>* dicts,
                                                segment_v2::ColumnIterator* iter) {
    // We already loaded dicts, no need to do once more.
    if (!dicts->empty()) {
        return;
    }
    auto column_iterator = down_cast<segment_v2::ScalarColumnIterator*>(iter);
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

void ColumnPredicateRewriter::_get_segment_dict_vec(segment_v2::ColumnIterator* iter, ColumnPtr* dict_column,
                                                    ColumnPtr* code_column, bool field_nullable) {
    auto column_iterator = down_cast<segment_v2::ScalarColumnIterator*>(iter);
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

bool ColumnPredicateRewriter::_rewrite_expr_predicate(ObjectPool* pool, const ColumnPredicate* raw_pred,
                                                      const ColumnPtr& raw_dict_column,
                                                      const ColumnPtr& raw_code_column, bool field_nullable,
                                                      ColumnPredicate** ptr) {
    *ptr = nullptr;
    size_t value_size = raw_dict_column->size();
    std::vector<uint8_t> selection(value_size);
    const ColumnExprPredicate* pred = down_cast<const ColumnExprPredicate*>(raw_pred);
    pred->evaluate(raw_dict_column.get(), selection.data(), 0, value_size);

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
    uint8_t sel_value = (true_count < false_count) ? 1 : 0;
    for (int i = 0; i < code_size; i++) {
        if (selection[i] == sel_value) {
            used_values->append(code_values[i]);
        }
    }
    bool eq_null = true;
    bool null_in_set = false;
    if (field_nullable) {
        if (selection[code_size] == sel_value) {
            null_in_set = true;
        }
    }
    bool is_not_in = false;
    if (sel_value == 0) {
        is_not_in = true;
    }

    // construct in filter.
    RuntimeState* state = pred->runtime_state();
    ColumnRef column_ref(pred->slot_desc());
    // change column input type from binary to int(code)
    TypeDescriptor type_desc = TypeDescriptor::from_primtive_type(TYPE_INT);
    column_ref._type = type_desc;
    Expr* probe_expr = &column_ref;
    // probe_expr will be copied into filter, so we don't need to allocate it.
    ExprContext* filter =
            RuntimeFilterHelper::create_runtime_in_filter(state, pool, probe_expr, eq_null, null_in_set, is_not_in);
    DCHECK_IF_ERROR(RuntimeFilterHelper::fill_runtime_in_filter(used_values, probe_expr, filter, 0));

    RowDescriptor row_desc; // I think we don't need to use it at all.
    DCHECK_IF_ERROR(filter->prepare(state, row_desc));
    DCHECK_IF_ERROR(filter->open(state));
    *ptr = new ColumnExprPredicate(get_type_info(kDictCodeType), pred->column_id(), state, filter, pred->slot_desc());
    filter->close(state);

    return true;
}

// member function for ConjunctivePredicatesRewriter

void ConjunctivePredicatesRewriter::rewrite_predicate(ObjectPool* pool) {
    std::vector<uint8_t> selection;
    auto pred_rewrite = [&](std::vector<const ColumnPredicate*>& preds) {
        for (auto& pred : preds) {
            if (column_need_rewrite(pred->column_id())) {
                const auto& dict = _dict_maps.at(pred->column_id());
                ChunkPtr temp_chunk = std::make_shared<Chunk>();

                auto [binary_column, codes] = extract_column_with_codes(*dict);

                int dict_rows = codes.size();
                selection.resize(dict_rows);

                pred->evaluate(binary_column.get(), selection.data(), 0, dict_rows);

                std::vector<uint8_t> code_mapping;
                code_mapping.resize(DICT_DECODE_MAX_SIZE + 1);
                for (int i = 0; i < codes.size(); ++i) {
                    code_mapping[codes[i]] = selection[i];
                }

                pred = new_column_dict_conjuct_predicate(get_type_info(kDictCodeType), pred->column_id(),
                                                         std::move(code_mapping));
                pool->add(const_cast<ColumnPredicate*>(pred));
            }
        }
    };

    pred_rewrite(_predicates.non_vec_preds());
    pred_rewrite(_predicates.vec_preds());
}

} // namespace starrocks::vectorized
