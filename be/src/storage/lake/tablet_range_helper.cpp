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

#include "storage/lake/tablet_range_helper.h"

#include <google/protobuf/util/message_differencer.h>

#include <memory>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/datum_convert.h"
#include "column/schema.h"
#include "common/logging.h"
#include "fmt/format.h"
#include "runtime/types.h"
#include "storage/chunk_helper.h"
#include "storage/datum_variant.h"
#include "storage/primary_key_encoder.h"
#include "storage/types.h"
#include "storage/variant_tuple.h"

namespace starrocks::lake {

// Produce a Datum holding the minimum value for the given logical type.
static StatusOr<Datum> datum_from_type_min(LogicalType type) {
    switch (type) {
    case TYPE_BOOLEAN: {
        bool v;
        get_type_info(TYPE_BOOLEAN)->set_to_min(&v);
        Datum d;
        d.set_int8(v);
        return d;
    }
    case TYPE_TINYINT: {
        int8_t v{};
        get_type_info(TYPE_TINYINT)->set_to_min(&v);
        Datum d;
        d.set_int8(v);
        return d;
    }
    case TYPE_SMALLINT: {
        int16_t v{};
        get_type_info(TYPE_SMALLINT)->set_to_min(&v);
        Datum d;
        d.set_int16(v);
        return d;
    }
    case TYPE_INT: {
        int32_t v{};
        get_type_info(TYPE_INT)->set_to_min(&v);
        Datum d;
        d.set_int32(v);
        return d;
    }
    case TYPE_BIGINT: {
        int64_t v{};
        get_type_info(TYPE_BIGINT)->set_to_min(&v);
        Datum d;
        d.set_int64(v);
        return d;
    }
    case TYPE_LARGEINT: {
        int128_t v{};
        get_type_info(TYPE_LARGEINT)->set_to_min(&v);
        Datum d;
        d.set_int128(v);
        return d;
    }
    case TYPE_DATE: {
        int32_t v{};
        get_type_info(TYPE_DATE)->set_to_min(&v);
        Datum d;
        d.set_int32(v);
        return d;
    }
    case TYPE_DATETIME: {
        int64_t v{};
        get_type_info(TYPE_DATETIME)->set_to_min(&v);
        Datum d;
        d.set_int64(v);
        return d;
    }
    case TYPE_VARCHAR: {
        Datum d;
        d.set_slice(Slice("", 0));
        return d;
    }
    default:
        return Status::NotSupported(fmt::format("unsupported type for PK min datum: {}", type));
    }
}

Status TabletRangeHelper::validate_tablet_range(const TabletRangePB& tablet_range_pb) {
    if (!tablet_range_pb.has_lower_bound() && !tablet_range_pb.has_upper_bound()) {
        return Status::OK();
    }

    if (tablet_range_pb.has_upper_bound()) {
        if (!tablet_range_pb.has_upper_bound_included()) {
            return Status::Corruption("upper_bound_included is required");
        }
        if (tablet_range_pb.upper_bound_included()) {
            return Status::Corruption("Upper bound is inclusive");
        }
    }

    if (tablet_range_pb.has_lower_bound()) {
        if (!tablet_range_pb.has_lower_bound_included()) {
            return Status::Corruption("lower_bound_included is required");
        }
        if (!tablet_range_pb.lower_bound_included()) {
            return Status::Corruption("Lower bound is exclusive");
        }
    }

    return Status::OK();
}

// The value old rows of a segment read for a sort-key column added later by a metadata-only fast schema
// evolution, mirroring DefaultValueColumnIterator::init: a declared default of the literal "NULL", or no
// declared default on a nullable column, reads as NULL; otherwise the declared default parsed to its type.
static StatusOr<DatumVariant> read_time_default(const TabletColumn& column) {
    const TypeInfoPtr& type_info = get_type_info(column);
    Datum datum;
    if (column.has_default_value()) {
        if (column.default_value() == "NULL") {
            datum.set_null();
        } else {
            RETURN_IF_ERROR(datum_from_string(type_info.get(), &datum, column.default_value(), nullptr));
        }
    } else if (column.is_nullable()) {
        datum.set_null();
    } else {
        return Status::Corruption(
                fmt::format("added sort-key column {} has no default and is not nullable", column.name()));
    }
    return DatumVariant(type_info, datum);
}

StatusOr<SeekRange> TabletRangeHelper::create_seek_range_from(const TabletRangePB& tablet_range_pb,
                                                              const TabletSchemaCSPtr& tablet_schema, MemPool* mem_pool,
                                                              const TabletSchemaCSPtr& current_schema) {
    SeekRange tablet_range;
    RETURN_IF_ERROR(validate_tablet_range(tablet_range_pb));
    if (!tablet_range_pb.has_lower_bound() && !tablet_range_pb.has_upper_bound()) {
        // (-inf, +inf)
        return tablet_range;
    }
    const auto& sort_key_idxes = tablet_schema->sort_key_idxes();
    DCHECK(!sort_key_idxes.empty());
    const size_t seg_arity = sort_key_idxes.size();

    // Decode the leading `seg_arity` values of a bound with `tablet_schema` so the SeekTuple's positional
    // field ids align with the target segment.
    auto decode_leading = [&](const TuplePB& tuple) -> StatusOr<SeekTuple> {
        Schema schema;
        std::vector<Datum> values;
        values.reserve(seg_arity);
        for (size_t i = 0; i < seg_arity; i++) {
            const int idx = sort_key_idxes[i];
            schema.append_sort_key_idx(idx);
            auto f = std::make_shared<Field>(ChunkHelper::convert_field(idx, tablet_schema->column(idx)));
            schema.append(std::move(f));

            Datum datum;
            RETURN_IF_ERROR(DatumVariant::from_proto(tuple.values(i), &datum, nullptr, nullptr, mem_pool));
            values.emplace_back(std::move(datum));
        }
        return SeekTuple(std::move(schema), std::move(values));
    };

    // Sign of (added-column defaults D) minus (a bound's dropped trailing values at [seg_arity, m)), a
    // lexicographic NULL-aware comparison. D comes from `current_schema`'s sort-key columns [seg_arity, m):
    // for an old segment every row reads those columns as their default, so D vs the trailing decides how a
    // boundary-prefix row routes.
    auto compare_default_to_trailing = [&](const TuplePB& tuple) -> StatusOr<int> {
        const int m = tuple.values_size();
        if (current_schema == nullptr) {
            return Status::Corruption("current schema is required to project a wider range bound");
        }
        const auto& cur_sort_key_idxes = current_schema->sort_key_idxes();
        if (static_cast<int>(cur_sort_key_idxes.size()) < m) {
            return Status::Corruption(fmt::format("current schema sort-key arity {} < range bound arity {}",
                                                  cur_sort_key_idxes.size(), m));
        }
        VariantTuple default_trailing;
        VariantTuple bound_trailing;
        default_trailing.reserve(m - seg_arity);
        bound_trailing.reserve(m - seg_arity);
        for (int i = static_cast<int>(seg_arity); i < m; i++) {
            ASSIGN_OR_RETURN(auto d, read_time_default(current_schema->column(cur_sort_key_idxes[i])));
            default_trailing.append(std::move(d));
            DatumVariant bound_value;
            RETURN_IF_ERROR(bound_value.from_proto(tuple.values(i)));
            bound_trailing.append(std::move(bound_value));
        }
        return default_trailing.compare(bound_trailing);
    };

    // Decode a bound into a SeekTuple and its (possibly projected) inclusivity.
    auto build_bound = [&](const TuplePB& tuple, bool pb_included, bool is_lower, SeekTuple* out_tuple,
                           bool* out_included) -> Status {
        const int m = tuple.values_size();
        if (m < static_cast<int>(seg_arity)) {
            return Status::Corruption(fmt::format(
                    "Unexpected number of values in TabletRangePB bound value, expected at least: {}, actual: {}",
                    seg_arity, m));
        }
        ASSIGN_OR_RETURN(*out_tuple, decode_leading(tuple));
        if (m == static_cast<int>(seg_arity)) {
            *out_included = pb_included;
            return Status::OK();
        }
        // The bound is wider than this segment's sort key: project it onto the leading `seg_arity` columns
        // and derive inclusivity from D (what old rows read for the dropped columns) vs the trailing values.
        // A boundary-prefix row (prefix, D) is on the >= side of a lower bound iff D >= trailing, and on the
        // < side of an upper bound iff D < trailing.
        ASSIGN_OR_RETURN(const int cmp, compare_default_to_trailing(tuple));
        if (is_lower) {
            *out_included = pb_included ? (cmp >= 0) : (cmp > 0);
        } else {
            *out_included = pb_included ? (cmp <= 0) : (cmp < 0);
        }
        return Status::OK();
    };

    SeekTuple lower;
    SeekTuple upper;
    bool inclusive_lower = tablet_range_pb.lower_bound_included();
    bool inclusive_upper = tablet_range_pb.upper_bound_included();
    if (tablet_range_pb.has_lower_bound()) {
        RETURN_IF_ERROR(build_bound(tablet_range_pb.lower_bound(), tablet_range_pb.lower_bound_included(),
                                    /*is_lower=*/true, &lower, &inclusive_lower));
    }
    if (tablet_range_pb.has_upper_bound()) {
        RETURN_IF_ERROR(build_bound(tablet_range_pb.upper_bound(), tablet_range_pb.upper_bound_included(),
                                    /*is_lower=*/false, &upper, &inclusive_upper));
    }

    tablet_range = SeekRange(std::move(lower), std::move(upper));
    tablet_range.set_inclusive_lower(inclusive_lower);
    tablet_range.set_inclusive_upper(inclusive_upper);
    return tablet_range;
}

// TabletRangePB should always be [lower_bound, upper_bound) which is consistent with the SstSeekRange.
StatusOr<SstSeekRange> TabletRangeHelper::create_sst_seek_range_from(const TabletRangePB& tablet_range_pb,
                                                                     const TabletSchemaCSPtr& tablet_schema) {
    SstSeekRange sst_seek_range;
    RETURN_IF_ERROR(validate_tablet_range(tablet_range_pb));
    if (!tablet_range_pb.has_lower_bound() && !tablet_range_pb.has_upper_bound()) {
        // (-inf, +inf)
        return sst_seek_range;
    }

    const auto& sort_key_idxes = tablet_schema->sort_key_idxes();
    DCHECK(!sort_key_idxes.empty());
    // sort key must the same as pk key
    RETURN_IF(sort_key_idxes.size() != tablet_schema->num_key_columns(),
              Status::InternalError(fmt::format("Sort key index size {} must be the same as pk key size {}",
                                                sort_key_idxes.size(), tablet_schema->num_key_columns())));
    for (int i = 0; i < tablet_schema->num_key_columns(); i++) {
        if (sort_key_idxes[i] != i) {
            return Status::InternalError(
                    fmt::format("Sort key index {} must be {}, but is {}", i, i, sort_key_idxes[i]));
        }
    }

    auto parse_bound_to_seek_string = [&](const TuplePB& tuple) -> StatusOr<std::string> {
        DCHECK_EQ(tuple.values_size(), static_cast<int>(sort_key_idxes.size()));
        if (tuple.values_size() != sort_key_idxes.size()) {
            return Status::Corruption(
                    fmt::format("Unexpected number of values in TabletRangePB bound value, expected: {}, actual: {}",
                                sort_key_idxes.size(), tuple.values_size()));
        }

        auto chunk = std::make_unique<Chunk>();
        for (int i = 0; i < tuple.values_size(); i++) {
            const int idx = sort_key_idxes[i];

            Datum datum;
            TypeDescriptor type_desc;
            RETURN_IF_ERROR(DatumVariant::from_proto(tuple.values(i), &datum, &type_desc));
            const bool is_nullable = tablet_schema->column(idx).is_nullable();
            if (!is_nullable && datum.is_null()) {
                // PK columns are non-nullable, so a NULL variant in the range boundary
                // can only represent a MIN sentinel (FE maps MIN → NULL_VALUE).
                // Fill with the per-type minimum value for correct PK encoding.
                ASSIGN_OR_RETURN(datum, datum_from_type_min(type_desc.type));
                auto column = ColumnHelper::create_column(type_desc, false);
                column->append_datum(datum);
                chunk->append_column(std::move(column), (SlotId)idx);
            } else {
                auto column = ColumnHelper::create_column(type_desc, is_nullable);
                column->append_datum(datum);
                chunk->append_column(std::move(column), (SlotId)idx);
            }
        }

        std::vector<ColumnId> pk_columns(tablet_schema->num_key_columns());
        for (int i = 0; i < tablet_schema->num_key_columns(); i++) {
            pk_columns[i] = (ColumnId)i;
        }
        auto pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);
        MutableColumnPtr pk_column;
        ASSIGN_OR_RETURN(auto pk_encoding_type, tablet_schema->primary_key_encoding_type_or_error());
        RETURN_IF(pk_encoding_type != PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2,
                  Status::InvalidArgument(
                          "Big-endian encoding is required for range-distribution table in share data mode"));
        RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(pkey_schema, &pk_column, pk_encoding_type));
        PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, 1, pk_column.get(), pk_encoding_type);
        if (pk_column->is_binary()) {
            return down_cast<BinaryColumn*>(pk_column.get())->get_slice(0).to_string();
        } else {
            return std::string(reinterpret_cast<const char*>(pk_column->raw_data()), pk_column->type_size());
        }
    };

    if (tablet_range_pb.has_lower_bound()) {
        ASSIGN_OR_RETURN(sst_seek_range.seek_key, parse_bound_to_seek_string(tablet_range_pb.lower_bound()));
    }
    if (tablet_range_pb.has_upper_bound()) {
        ASSIGN_OR_RETURN(sst_seek_range.stop_key, parse_bound_to_seek_string(tablet_range_pb.upper_bound()));
    }

    return sst_seek_range;
}

StatusOr<TabletRangePB> TabletRangeHelper::convert_t_range_to_pb_range(const TTabletRange& t_range) {
    TabletRangePB pb_range;
    auto convert_bound = [](const auto& t_tuple, auto* pb_tuple) -> Status {
        for (const auto& t_val : t_tuple.values) {
            auto* pb_val = pb_tuple->add_values();
            if (t_val.__isset.type) {
                if (!t_val.type.__isset.types || t_val.type.types.empty()) {
                    return Status::InvalidArgument("TVariant type is set but types list is empty");
                }
                *pb_val->mutable_type() = TypeDescriptor::from_thrift(t_val.type).to_protobuf();
            } else {
                return Status::InvalidArgument("TVariant type is required");
            }

            if (t_val.__isset.value) {
                pb_val->set_value(t_val.value);
            } else if (!t_val.__isset.variant_type || t_val.variant_type == TVariantType::NORMAL_VALUE) {
                return Status::InvalidArgument("TVariant value is required for NORMAL_VALUE variant");
            }

            if (!t_val.__isset.variant_type) {
                return Status::InvalidArgument("TVariant variant_type is required");
            }
            if (t_val.variant_type == TVariantType::MINIMUM || t_val.variant_type == TVariantType::MAXIMUM) {
                return Status::InvalidArgument("MINIMUM/MAXIMUM variant is not supported in tablet range");
            }
            pb_val->set_variant_type(static_cast<VariantTypePB>(t_val.variant_type));
        }
        return Status::OK();
    };
    if (t_range.__isset.lower_bound) {
        RETURN_IF_ERROR(convert_bound(t_range.lower_bound, pb_range.mutable_lower_bound()));
    }
    if (t_range.__isset.upper_bound) {
        RETURN_IF_ERROR(convert_bound(t_range.upper_bound, pb_range.mutable_upper_bound()));
    }
    if (t_range.__isset.lower_bound_included) {
        pb_range.set_lower_bound_included(t_range.lower_bound_included);
    }
    if (t_range.__isset.upper_bound_included) {
        pb_range.set_upper_bound_included(t_range.upper_bound_included);
    }
    return pb_range;
}

// Structural (proto byte-for-byte) equality of two TuplePB messages. Used by
// validate_new_tablet_ranges to detect zero-width and adjacency-tile mismatches.
namespace {
bool tuple_pb_equal(const TuplePB& lhs, const TuplePB& rhs) {
    return google::protobuf::util::MessageDifferencer::Equals(lhs, rhs);
}

// Same as tuple_pb_equal but treats "neither side has the bound" as equal —
// used at the first.lower / last.upper endpoint checks where the parent range
// may be unbounded.
bool tuple_bound_equal(bool lhs_has, const TuplePB& lhs, bool rhs_has, const TuplePB& rhs) {
    if (lhs_has != rhs_has) return false;
    if (!lhs_has) return true;
    return tuple_pb_equal(lhs, rhs);
}
} // namespace

Status TabletRangeHelper::validate_new_tablet_ranges(
        const TabletRangePB& old_tablet_range,
        const google::protobuf::RepeatedPtrField<TabletRangePB>& new_tablet_ranges) {
    if (new_tablet_ranges.empty()) {
        return Status::InvalidArgument("validate_new_tablet_ranges: new_tablet_ranges is empty");
    }

    // 0. The old tablet's own range must be well-formed too — otherwise our
    //    "first.lower matches old.lower" / "last.upper matches old.upper"
    //    checks below could be comparing against a malformed reference.
    RETURN_IF_ERROR(validate_tablet_range(old_tablet_range));

    // 1. Each new-tablet range must be individually well-formed, and the two
    //    bounds (when both set) must not be byte-equal (catches zero-width
    //    children). Strict semantic ordering (lower < upper) requires a
    //    schema for type-aware comparison and is the caller's responsibility
    //    (e.g., compute_split_ranges_from_external_boundaries compares via
    //    the tablet schema).
    for (const auto& r : new_tablet_ranges) {
        RETURN_IF_ERROR(validate_tablet_range(r));
        if (r.has_lower_bound() && r.has_upper_bound() && tuple_pb_equal(r.lower_bound(), r.upper_bound())) {
            return Status::InvalidArgument(
                    "validate_new_tablet_ranges: range with lower_bound == upper_bound (zero-width)");
        }
    }

    // 2. First range's lower bound must match the old tablet's lower bound.
    const auto& first = new_tablet_ranges[0];
    if (!tuple_bound_equal(first.has_lower_bound(), first.lower_bound(), old_tablet_range.has_lower_bound(),
                           old_tablet_range.lower_bound())) {
        return Status::InvalidArgument("validate_new_tablet_ranges: first.lower_bound != old_tablet_range.lower_bound");
    }
    if (first.has_lower_bound() && !first.lower_bound_included()) {
        return Status::InvalidArgument("validate_new_tablet_ranges: first.lower_bound must be inclusive when set");
    }

    // 3. Last range's upper bound must match the old tablet's upper bound.
    const auto& last = new_tablet_ranges[new_tablet_ranges.size() - 1];
    if (!tuple_bound_equal(last.has_upper_bound(), last.upper_bound(), old_tablet_range.has_upper_bound(),
                           old_tablet_range.upper_bound())) {
        return Status::InvalidArgument("validate_new_tablet_ranges: last.upper_bound != old_tablet_range.upper_bound");
    }
    if (last.has_upper_bound() && last.upper_bound_included()) {
        return Status::InvalidArgument("validate_new_tablet_ranges: last.upper_bound must be exclusive when set");
    }

    // 4. Adjacent ranges must tile exactly: ranges[i].upper == ranges[i+1].lower,
    //    upper exclusive on the left, lower inclusive on the right.
    for (int i = 0; i + 1 < new_tablet_ranges.size(); ++i) {
        const auto& current_range = new_tablet_ranges[i];
        const auto& next_range = new_tablet_ranges[i + 1];
        if (!current_range.has_upper_bound() || !next_range.has_lower_bound()) {
            return Status::InvalidArgument(
                    fmt::format("validate_new_tablet_ranges: gap at boundary {} (interior bounds must be set)", i));
        }
        if (current_range.upper_bound_included() || !next_range.lower_bound_included()) {
            return Status::InvalidArgument(
                    fmt::format("validate_new_tablet_ranges: invalid bound flags at boundary {} "
                                "(left must be exclusive, right must be inclusive)",
                                i));
        }
        if (!tuple_pb_equal(current_range.upper_bound(), next_range.lower_bound())) {
            return Status::InvalidArgument(
                    fmt::format("validate_new_tablet_ranges: gap or overlap at boundary {} "
                                "(ranges[i].upper_bound != ranges[i+1].lower_bound)",
                                i));
        }
    }

    return Status::OK();
}

} // namespace starrocks::lake
