// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/column_predicate_rewriter.h"

#include <algorithm>
#include <utility>

#include "column/binary_column.h"
#include "column/datum.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exprs/expr_context.h"
#include "storage/rowset/segment_v2/column_reader.h"

namespace starrocks::vectorized {
constexpr static const FieldType kDictCodeType = OLAP_FIELD_TYPE_INT;

void ColumnPredicateRewriter::rewrite_predicate(ObjectPool* pool) {
    // because schema has reordered
    // so we only need to check the first `predicate_column_size` fields
    for (size_t i = 0; i < _predicate_column_size; i++) {
        const FieldPtr& field = _schema.field(i);
        ColumnId cid = field->id();
        if (_predicate_need_rewrite[cid]) {
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
    std::vector<std::pair<std::string, int>> sorted_dicts;

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
            get_segment_dict(&sorted_dicts, _column_iterators[cid]);
            auto value = pred->value().get_slice().to_string();
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
            get_segment_dict(&sorted_dicts, _column_iterators[cid]);
            std::vector<std::string> str_codewords;
            auto value = pred->value().get_slice().to_string();
            auto iter = std::lower_bound(
                    sorted_dicts.begin(), sorted_dicts.end(), value,
                    [](const auto& entity, const auto& value) { return entity.first.compare(value) < 0; });
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

    for (const auto pred_will_remove : remove_list) {
        auto willrm = std::find(preds.begin(), preds.end(), pred_will_remove);
        preds.erase(willrm);
    }

    return true;
}

// This function is only used to rewrite the LE/LT/GE/GT condition.
// For the greater than or less than condition,
// you need to get the values of all ordered dictionaries and rewrite them as `InList` expressions
void ColumnPredicateRewriter::get_segment_dict(std::vector<std::pair<std::string, int>>* dicts,
                                               segment_v2::ColumnIterator* iter) {
    auto column_iterator = down_cast<segment_v2::FileColumnIterator*>(iter);
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

} // namespace starrocks::vectorized