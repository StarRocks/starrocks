// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <runtime/decimalv3.h>

#include <cstdint>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "column/column.h" // Column
#include "column/datum.h"
#include "common/object_pool.h"
#include "storage/olap_common.h" // ColumnId
#include "storage/vectorized/range.h"
#include "storage/vectorized/zone_map_detail.h"
#include "util/string_parser.hpp"

class Roaring;

namespace starrocks {
class BloomFilter;
class Slice;
class ObjectPool;
class ExprContext;
class RuntimeState;
class SlotDescriptor;
} // namespace starrocks

namespace starrocks::segment_v2 {
class BitmapIndexIterator;
class BloomFilter;
} // namespace starrocks::segment_v2

namespace starrocks::vectorized {

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
    inline static uint8_t apply(uint8_t a, uint8_t b) { return b; }
};

class ColumnPredicateAndOp {
public:
    inline static uint8_t apply(uint8_t a, uint8_t b) { return a & b; }
};

class ColumnPredicateOrOp {
public:
    inline static uint8_t apply(uint8_t a, uint8_t b) { return a | b; }
};

// ColumnPredicate represents a predicate that can only be applied to a column.
class ColumnPredicate {
public:
    explicit ColumnPredicate(TypeInfoPtr type_info, ColumnId column_id)
            : _type_info(std::move(type_info)), _column_id(column_id) {}

    virtual ~ColumnPredicate() = default;

    uint32_t column_id() const { return _column_id; }

    void evaluate(const Column* column, uint8_t* selection) const { evaluate(column, selection, 0, column->size()); }

    void evaluate_and(const Column* column, uint8_t* selection) const {
        evaluate_and(column, selection, 0, column->size());
    }

    void evaluate_or(const Column* column, uint8_t* selection) const {
        evaluate_or(column, selection, 0, column->size());
    }

    virtual void evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const = 0;

    virtual void evaluate_and(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const = 0;

    virtual void evaluate_or(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const = 0;

    virtual uint16_t evaluate_branchless(const Column* column, uint16_t* sel, uint16_t sel_size) const {
        CHECK(false) << "not supported";
        return 0;
    }

    virtual bool filter(const BloomFilter& bf) const { return true; }

    // Return false to filter out a data page.
    virtual bool zone_map_filter(const ZoneMapDetail& detail) const { return true; }

    virtual bool support_bloom_filter() const { return false; }

    // Return false to filter out a data page.
    virtual bool bloom_filter(const segment_v2::BloomFilter* bf) const { return true; }

    virtual Status seek_bitmap_dictionary(segment_v2::BitmapIndexIterator* iter, SparseRange* range) const {
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
    virtual Datum value() const { return Datum(); }

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

template <template <FieldType> typename Predicate, template <FieldType> typename BinaryPredicate>
ColumnPredicate* new_column_predicate(const TypeInfoPtr& type_info, ColumnId id, const Slice& operand) {
    auto type = type_info->type();
    switch (type) {
    case OLAP_FIELD_TYPE_BOOL:
        return new Predicate<OLAP_FIELD_TYPE_BOOL>(type_info, id, string_to_int<int>(operand) != 0);
    case OLAP_FIELD_TYPE_TINYINT:
        return new Predicate<OLAP_FIELD_TYPE_TINYINT>(type_info, id, string_to_int<int8_t>(operand));
    case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
        return nullptr;
    case OLAP_FIELD_TYPE_SMALLINT:
        return new Predicate<OLAP_FIELD_TYPE_SMALLINT>(type_info, id, string_to_int<int16_t>(operand));
    case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
        return nullptr;
    case OLAP_FIELD_TYPE_INT:
        return new Predicate<OLAP_FIELD_TYPE_INT>(type_info, id, string_to_int<int32_t>(operand));
    case OLAP_FIELD_TYPE_UNSIGNED_INT:
        return nullptr;
    case OLAP_FIELD_TYPE_BIGINT:
        return new Predicate<OLAP_FIELD_TYPE_BIGINT>(type_info, id, string_to_int<int64_t>(operand));
    case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        return nullptr;
    case OLAP_FIELD_TYPE_LARGEINT:
        return new Predicate<OLAP_FIELD_TYPE_LARGEINT>(type_info, id, string_to_int<int128_t>(operand));
    case OLAP_FIELD_TYPE_FLOAT:
        return new Predicate<OLAP_FIELD_TYPE_FLOAT>(type_info, id, string_to_float<float>(operand));
    case OLAP_FIELD_TYPE_DOUBLE:
        return new Predicate<OLAP_FIELD_TYPE_DOUBLE>(type_info, id, string_to_float<double>(operand));
    case OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
        return nullptr;
    case OLAP_FIELD_TYPE_DECIMAL: {
        decimal12_t value;
        auto st = type_info->from_string(&value, operand.to_string());
        DCHECK_EQ(OLAP_SUCCESS, st);
        return new Predicate<OLAP_FIELD_TYPE_DECIMAL>(type_info, id, value);
    }
    case OLAP_FIELD_TYPE_DECIMAL_V2: {
        DecimalV2Value value;
        auto st = type_info->from_string(&value, operand.to_string());
        DCHECK_EQ(OLAP_SUCCESS, st);
        return new Predicate<OLAP_FIELD_TYPE_DECIMAL_V2>(type_info, id, value);
    }
    case OLAP_FIELD_TYPE_DECIMAL32: {
        int32_t value;
        auto st = type_info->from_string(&value, operand.to_string());
        DCHECK_EQ(OLAP_SUCCESS, st);
        return new Predicate<OLAP_FIELD_TYPE_DECIMAL32>(type_info, id, value);
    }
    case OLAP_FIELD_TYPE_DECIMAL64: {
        int64_t value;
        auto st = type_info->from_string(&value, operand.to_string());
        DCHECK_EQ(OLAP_SUCCESS, st);
        return new Predicate<OLAP_FIELD_TYPE_DECIMAL64>(type_info, id, value);
    }
    case OLAP_FIELD_TYPE_DECIMAL128: {
        int128_t value;
        auto st = type_info->from_string(&value, operand.to_string());
        DCHECK_EQ(OLAP_SUCCESS, st);
        return new Predicate<OLAP_FIELD_TYPE_DECIMAL128>(type_info, id, value);
    }
    case OLAP_FIELD_TYPE_DATE: {
        uint24_t value = 0;
        auto st = type_info->from_string(&value, operand.to_string());
        DCHECK_EQ(OLAP_SUCCESS, st);
        return new Predicate<OLAP_FIELD_TYPE_DATE>(type_info, id, value);
    }
    case OLAP_FIELD_TYPE_DATE_V2: {
        int32_t value = 0;
        auto st = type_info->from_string(&value, operand.to_string());
        DCHECK_EQ(OLAP_SUCCESS, st);
        return new Predicate<OLAP_FIELD_TYPE_DATE_V2>(type_info, id, value);
    }
    case OLAP_FIELD_TYPE_DATETIME: {
        uint64_t value = 0;
        auto st = type_info->from_string(&value, operand.to_string());
        DCHECK_EQ(OLAP_SUCCESS, st);
        return new Predicate<OLAP_FIELD_TYPE_DATETIME>(type_info, id, value);
    }
    case OLAP_FIELD_TYPE_TIMESTAMP: {
        int64_t value = 0;
        auto st = type_info->from_string(&value, operand.to_string());
        DCHECK_EQ(OLAP_SUCCESS, st);
        return new Predicate<OLAP_FIELD_TYPE_TIMESTAMP>(type_info, id, value);
    }
    case OLAP_FIELD_TYPE_CHAR:
        return new BinaryPredicate<OLAP_FIELD_TYPE_CHAR>(type_info, id, operand);
    case OLAP_FIELD_TYPE_VARCHAR:
        return new BinaryPredicate<OLAP_FIELD_TYPE_VARCHAR>(type_info, id, operand);
    case OLAP_FIELD_TYPE_STRUCT:
    case OLAP_FIELD_TYPE_ARRAY:
    case OLAP_FIELD_TYPE_MAP:
    case OLAP_FIELD_TYPE_UNKNOWN:
    case OLAP_FIELD_TYPE_NONE:
    case OLAP_FIELD_TYPE_HLL:
    case OLAP_FIELD_TYPE_OBJECT:
    case OLAP_FIELD_TYPE_PERCENTILE:
    case OLAP_FIELD_TYPE_MAX_VALUE:
        return nullptr;
        // No default to ensure newly added enumerator will be handled.
    }
    return nullptr;
}

ColumnPredicate* new_column_eq_predicate(const TypeInfoPtr& type, ColumnId id, const Slice& operand);
ColumnPredicate* new_column_ne_predicate(const TypeInfoPtr& type, ColumnId id, const Slice& operand);
ColumnPredicate* new_column_lt_predicate(const TypeInfoPtr& type, ColumnId id, const Slice& operand);
ColumnPredicate* new_column_le_predicate(const TypeInfoPtr& type, ColumnId id, const Slice& operand);
ColumnPredicate* new_column_gt_predicate(const TypeInfoPtr& type, ColumnId id, const Slice& operand);
ColumnPredicate* new_column_ge_predicate(const TypeInfoPtr& type, ColumnId id, const Slice& operand);

ColumnPredicate* new_column_in_predicate(const TypeInfoPtr& type, ColumnId id,
                                         const std::vector<std::string>& operands);
ColumnPredicate* new_column_not_in_predicate(const TypeInfoPtr& type, ColumnId id,
                                             const std::vector<std::string>& operands);
ColumnPredicate* new_column_null_predicate(const TypeInfoPtr& type, ColumnId, bool is_null);

ColumnPredicate* new_column_dict_conjuct_predicate(const TypeInfoPtr& type_info, ColumnId id,
                                                   std::vector<uint8_t> dict_mapping);

template <FieldType field_type, template <FieldType> typename Predicate, typename NewColumnPredicateFunc>
Status predicate_convert_to(Predicate<field_type> const& input_predicate,
                            typename CppTypeTraits<field_type>::CppType const& value,
                            NewColumnPredicateFunc new_column_predicate_func, const ColumnPredicate** output,
                            const TypeInfoPtr& target_type_info, ObjectPool* obj_pool) {
    using ValueType = typename CppTypeTraits<field_type>::CppType;
    const auto to_type = target_type_info->type();
    if (to_type == field_type) {
        *output = &input_predicate;
        return Status::OK();
    }
    auto cid = input_predicate.column_id();
    const auto type_info = input_predicate.type_info();
    std::string str = type_info->to_string(&value);
    *output = obj_pool->add(new_column_predicate_func(target_type_info, cid, str));
    return Status::OK();
}

} //namespace starrocks::vectorized
