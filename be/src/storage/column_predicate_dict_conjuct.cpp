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

#include <cstdint>
#include <sstream>
#include <vector>

#include "column/datum.h"
#include "runtime/global_dict/dict_column.h"
#include "storage/column_operator_predicate.h"
#include "storage/column_predicate.h"

namespace starrocks {

// DictConjuctPredicateOperator for global dictionary optimization.
// It converts all predicates into code mappings.
// the null input will deal with 0
// eg: where key = 'SR' will convert to
// [0] NULL -> false
// [1] "SR" -> true
// [2] "RK" -> false
//

template <LogicalType field_type>
class DictConjuctPredicateOperator {
public:
    static constexpr bool skip_null = false;
    DictConjuctPredicateOperator(std::vector<uint8_t> code_mapping) : _code_mapping(std::move(code_mapping)) {}

    uint8_t eval_at(const LowCardDictColumn* lowcard_column, int idx) const {
        return _code_mapping[lowcard_column->get_data()[idx]];
    }

    bool zone_map_filter(const ZoneMapDetail& detail) const { return true; }

    static constexpr PredicateType type() { return PredicateType::kMap; }
    static constexpr bool support_bloom_filter() { return false; }

    static constexpr bool can_vectorized() { return true; }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const {
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

    Datum value() const { return {}; }

    std::vector<Datum> values() const {
        std::vector<Datum> res;
        res.reserve(_code_mapping.size());
        for (unsigned char i : _code_mapping) {
            res.emplace_back(int(i));
        }
        return res;
    }

private:
    std::vector<uint8_t> _code_mapping;
};

// used in low_card dict code
ColumnPredicate* new_column_dict_conjuct_predicate(const TypeInfoPtr& type_info, ColumnId id,
                                                   std::vector<uint8_t> dict_mapping) {
    DCHECK(type_info->type() == TYPE_INT);
    if (type_info->type() == TYPE_INT) {
        return new ColumnOperatorPredicate<TYPE_INT, LowCardDictColumn, DictConjuctPredicateOperator,
                                           decltype(dict_mapping)>(type_info, id, std::move(dict_mapping));
    }

    return nullptr;
}

} // namespace starrocks
