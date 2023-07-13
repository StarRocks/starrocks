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
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "column/column.h" // Column
#include "column/datum.h"
#include "column/type_traits.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "gen_cpp/Opcodes_types.h"
#include "runtime/decimalv3.h"
#include "storage/olap_common.h" // ColumnId
#include "storage/range.h"
#include "storage/type_traits.h"
#include "storage/types.h"
#include "storage/zone_map_detail.h"
#include "types/logical_type.h"
#include "util/json.h"
#include "util/string_parser.hpp"

class Roaring;

namespace starrocks {
class BloomFilter;
class Slice;
class ObjectPool;
class ExprContext;
class RuntimeState;
class SlotDescriptor;
class BitmapIndexIterator;
class BloomFilter;
} // namespace starrocks

namespace starrocks {

template <LogicalType ftype>
struct PredicateCmpTypeForField {
    using ValueType = typename CppTypeTraits<ftype>::CppType;
};

template <>
struct PredicateCmpTypeForField<TYPE_JSON> {
    using ValueType = JsonValue;
};

enum class PredicateType {
    kUnknown = 0,
    kEQ = 1,
    kNE = 2,
    kGT = 3,
    kGE = 4,
    kLT = 5,
    kLE = 6,
    kInList = 7,
    kNotInList = 8,
    kIsNull = 9,
    kNotNull = 10,
    kAnd = 11,
    kOr = 12,
    kExpr = 13,
    kTrue = 14,
    kMap = 15,
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

// ColumnPredicate represents a predicate that can only be applied to a column.
class ColumnPredicate {
public:
    explicit ColumnPredicate(TypeInfoPtr type_info, ColumnId column_id)
            : _type_info(std::move(type_info)), _column_id(column_id) {}

    virtual ~ColumnPredicate() = default;

    uint32_t column_id() const { return _column_id; }

    Status evaluate(const Column* column, uint8_t* selection) const {
        return evaluate(column, selection, 0, column->size());
    }

    Status evaluate_and(const Column* column, uint8_t* selection) const {
        return evaluate_and(column, selection, 0, column->size());
    }

    Status evaluate_or(const Column* column, uint8_t* selection) const {
        return evaluate_or(column, selection, 0, column->size());
    }

    virtual Status evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const = 0;

    virtual Status evaluate_and(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const = 0;

    virtual Status evaluate_or(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const = 0;

    virtual StatusOr<uint16_t> evaluate_branchless(const Column* column, uint16_t* sel, uint16_t sel_size) const {
        CHECK(false) << "not supported";
        return 0;
    }

    virtual bool filter(const BloomFilter& bf) const { return true; }

    // Return false to filter out a data page.
    virtual bool zone_map_filter(const ZoneMapDetail& detail) const { return true; }

    virtual bool support_bloom_filter() const { return false; }

    // Return false to filter out a data page.
    virtual bool bloom_filter(const BloomFilter* bf) const { return true; }

    virtual Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const {
        return Status::Cancelled("not implemented");
    }

    // Indicate whether or not the evaluate can be vectorized.
    // If this function return true, evaluate function will be vectorized and can achieve
    // good performance.
    // If this function return false, prefer using evaluate_branless to get a better performance
    virtual bool can_vectorized() const = 0;

    // Indicate if this predicate uses ExprContext*. The predicates of this kind has one major limitation
    // that it does not support `evaluate` range. In another word, `from` must be zero.
    bool is_expr_predicate() const { return _is_expr_predicate; }

    bool is_index_filter_only() const { return _is_index_filter_only; }

    void set_index_filter_only(bool is_index_only) { _is_index_filter_only = is_index_only; }

    virtual PredicateType type() const = 0;

    // Constant value in the predicate. And this constant value might be adjusted according to schema.
    // For example, if column type is char(20), then this constant value might be zero-padded to 20 chars.
    virtual Datum value() const { return {}; }

    // Constant value in the predicate in vector form. In contrast to `value()`, these value are un-modified.
    virtual std::vector<Datum> values() const { return std::vector<Datum>{}; }

    virtual Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                              ObjectPool* obj_pool) const = 0;

    virtual std::string debug_string() const {
        std::stringstream ss;
        ss << "(column_id=" << _column_id << ")";
        return ss.str();
    }

    // Padding the operand value with zeros('\0') for index filter.
    //
    // When `CHAR` values are stored, for some historical reason, they ar right-padded with
    // '\0' to the specified length. When `CHAR` values are retrieved, in vectorized engine,
    // trailing zeros are removed, except for index column. So, when a CHAR column has either
    // bitmap index or bloom filter, the predicate operand should be right-padded with '\0'.
    virtual bool padding_zeros(size_t column_length) { return false; }
    const TypeInfo* type_info() const { return _type_info.get(); }

protected:
    constexpr static const char* kMsgTooManyItems = "too many bitmap filter items";
    constexpr static const char* kMsgLowCardinality = "low bitmap index cardinality";

    TypeInfoPtr _type_info;
    ColumnId _column_id;
    // Whether this predicate only used to filter index, not filter chunk row
    bool _is_index_filter_only = false;
    // If this predicate uses ExprContext*
    bool _is_expr_predicate = false;
};

using PredicateList = std::vector<const ColumnPredicate*>;

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
ColumnPredicate* new_column_not_in_predicate(const TypeInfoPtr& type, ColumnId id,
                                             const std::vector<std::string>& operands);
ColumnPredicate* new_column_null_predicate(const TypeInfoPtr& type, ColumnId, bool is_null);

ColumnPredicate* new_column_dict_conjuct_predicate(const TypeInfoPtr& type_info, ColumnId id,
                                                   std::vector<uint8_t> dict_mapping);

} //namespace starrocks
