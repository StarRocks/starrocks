// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <cstdint>
#include <sstream>
#include <vector>

#include "column/datum.h"
#include "runtime/global_dict/config.h"
#include "storage/column_operator_predicate.h"
#include "storage/olap_common.h"
#include "storage/vectorized_column_predicate.h"

namespace starrocks::vectorized {

// DictConjuctPredicateOperator for global dictionary optimization.
// It converts all predicates into code mappings.
// eg: where key = 'SR' will convert to
// [0] "SR" -> true
// [1] "RK" -> false
//
template <FieldType field_type, class ColumnType>
class DictConjuctPredicateOperator {
public:
    DictConjuctPredicateOperator(std::vector<uint8_t> code_mapping) : _code_mapping(std::move(code_mapping)) {}

    uint8_t eval_at(const ColumnType* lowcard_column, int idx) const {
        return _code_mapping[lowcard_column->get_data()[idx]];
    }

    bool zone_map_filter(const ZoneMapDetail& detail) const { return true; }

    static constexpr PredicateType type() { return PredicateType::kMap; }
    static constexpr bool support_bloom_filter() { return false; }

    static constexpr bool can_vectorized() { return true; }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange* range) const {
        return Status::Cancelled("not implemented");
    }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info, ObjectPool* obj_pool) const {
        return Status::NotSupported("Not support Convert Dict Conjuct Predicate");
    }

    std::string debug_string() const {
        std::stringstream ss;
        for (int i = 0; i < _code_mapping.size(); ++i) {
            ss << i << ":" << int(_code_mapping[i]) << "\n";
        }
        return ss.str();
    }

    bool padding_zeros(size_t len) const { return false; }

    Datum value() const { return Datum(); }

    std::vector<Datum> values() const {
        std::vector<Datum> res;
        for (int i = 0; i < _code_mapping.size(); ++i) {
            res.emplace_back(int(_code_mapping[i]));
        }
        return res;
    }

private:
    std::vector<uint8_t> _code_mapping;
};

// used in low_card dict code
ColumnPredicate* new_column_dict_conjuct_predicate(const TypeInfoPtr& type_info, ColumnId id,
                                                   std::vector<uint8_t> dict_mapping) {
    DCHECK(type_info->type() == OLAP_FIELD_TYPE_INT || type_info->type() == OLAP_FIELD_TYPE_SMALLINT);
    if (type_info->type() == OLAP_FIELD_TYPE_SMALLINT) {
        return new ColumnOperatorPredicate<OLAP_FIELD_TYPE_SMALLINT, Int16Column, DictConjuctPredicateOperator,
                                           decltype(dict_mapping)>(type_info, id, std::move(dict_mapping));
    } else if (type_info->type() == OLAP_FIELD_TYPE_INT) {
        return new ColumnOperatorPredicate<OLAP_FIELD_TYPE_INT, Int32Column, DictConjuctPredicateOperator,
                                           decltype(dict_mapping)>(type_info, id, std::move(dict_mapping));
    }

    return nullptr;
}
} // namespace starrocks::vectorized
