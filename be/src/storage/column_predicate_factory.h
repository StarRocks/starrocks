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

#pragma once

#include <cstdint>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "base/string/slice.h"
#include "base/string/string_parser.hpp"
#include "column/runtime_type_traits.h"
#include "common/status.h"
#include "storage/primitive/column_predicate.h"
#include "storage/types.h"
#include "types/decimalv3.h"
#include "types/json_value.h"
#include "types/logical_type.h"
#include "types/storage_type_traits.h"

namespace starrocks {

template <LogicalType ftype>
struct PredicateCmpTypeForField {
    using ValueType = StorageCppType<ftype>;
};

template <>
struct PredicateCmpTypeForField<TYPE_JSON> {
    using ValueType = JsonValue;
};

std::ostream& operator<<(std::ostream& os, PredicateType p);

template <typename T>
static inline T string_to_int(const Slice& s) {
    StringParser::ParseResult r;
    T v = StringParser::string_to_int<T>(s.data, s.size, &r);
    DCHECK_EQ(StringParser::PARSE_SUCCESS, r);
    return v;
}

template <typename T>
static inline T string_to_float(const Slice& s) {
    StringParser::ParseResult r;
    T v = StringParser::string_to_float<T>(s.data, s.size, &r);
    DCHECK_EQ(StringParser::PARSE_SUCCESS, r);
    return v;
}

class ColumnPredicateAssignOp {
public:
    static uint8_t apply(uint8_t a, uint8_t b) { return b; }
};

class ColumnPredicateAndOp {
public:
    static uint8_t apply(uint8_t a, uint8_t b) { return a & b; }
};

class ColumnPredicateOrOp {
public:
    static uint8_t apply(uint8_t a, uint8_t b) { return a | b; }
};

ColumnPredicate* new_column_eq_predicate(const TypeInfoPtr& type, ColumnId id, const Slice& operand);
ColumnPredicate* new_column_ne_predicate(const TypeInfoPtr& type, ColumnId id, const Slice& operand);
ColumnPredicate* new_column_lt_predicate(const TypeInfoPtr& type, ColumnId id, const Slice& operand);
ColumnPredicate* new_column_le_predicate(const TypeInfoPtr& type, ColumnId id, const Slice& operand);
ColumnPredicate* new_column_gt_predicate(const TypeInfoPtr& type, ColumnId id, const Slice& operand);
ColumnPredicate* new_column_ge_predicate(const TypeInfoPtr& type, ColumnId id, const Slice& operand);
ColumnPredicate* new_column_cmp_predicate(PredicateType predicate, const TypeInfoPtr& type, ColumnId id,
                                          const Slice& operand);

ColumnPredicate* new_column_in_predicate(const TypeInfoPtr& type, ColumnId id,
                                         const std::vector<std::string>& operands);

ColumnPredicate* new_dictionary_code_in_predicate(const TypeInfoPtr& type, ColumnId id,
                                                  const std::vector<int32_t>& operands, size_t size);
ColumnPredicate* new_column_not_in_predicate(const TypeInfoPtr& type, ColumnId id,
                                             const std::vector<std::string>& operands);
ColumnPredicate* new_column_null_predicate(const TypeInfoPtr& type, ColumnId, bool is_null);

ColumnPredicate* new_column_dict_conjuct_predicate(const TypeInfoPtr& type_info, ColumnId id,
                                                   std::vector<uint8_t> dict_mapping);

ColumnPredicate* new_column_placeholder_predicate(const TypeInfoPtr& type_info, ColumnId id);

ColumnPredicate* new_column_eq_predicate_from_datum(const TypeInfoPtr& type, ColumnId id, const Datum& operand);
ColumnPredicate* new_column_ne_predicate_from_datum(const TypeInfoPtr& type, ColumnId id, const Datum& operand);
ColumnPredicate* new_column_lt_predicate_from_datum(const TypeInfoPtr& type, ColumnId id, const Datum& operand);
ColumnPredicate* new_column_le_predicate_from_datum(const TypeInfoPtr& type, ColumnId id, const Datum& operand);
ColumnPredicate* new_column_gt_predicate_from_datum(const TypeInfoPtr& type, ColumnId id, const Datum& operand);
ColumnPredicate* new_column_ge_predicate_from_datum(const TypeInfoPtr& type, ColumnId id, const Datum& operand);
ColumnPredicate* new_column_in_predicate_from_datum(const TypeInfoPtr& type, ColumnId id,
                                                    const std::vector<Datum>& operands);
ColumnPredicate* new_column_not_in_predicate_from_datum(const TypeInfoPtr& type, ColumnId id,
                                                        const std::vector<Datum>& operands);

Status compound_and_predicates_evaluate(const std::vector<const ColumnPredicate*>& predicates, const Column* col,
                                        uint8_t* selection, uint16_t* selected_idx, uint16_t from, uint16_t to);

template <LogicalType LT>
class Bitset;
template <LogicalType LT>
ColumnPredicate* new_bitset_in_predicate(const TypeInfoPtr& type_info, ColumnId id, const Bitset<LT>& bitset);

} //namespace starrocks
