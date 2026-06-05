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
#include "column/raw_data_visitor.h"
#include "column/schema.h"
#include "common/logging.h"
#include "fmt/format.h"
#include "storage/chunk_helper.h"
#include "storage/datum_variant.h"
#include "storage/primary_key_encoder.h"
#include "storage/types.h"
#include "types/storage_type_traits.h"
#include "types/type_descriptor.h"

namespace starrocks::lake {

// Produce a Datum holding the minimum value for the given logical type.
// Uses TypeInfo::set_to_min() which calls std::numeric_limits<CppType>::lowest().
// Covers all PK-supported types (APPLY_FOR_ALL_PK_SUPPORT_TYPE in logical_type_infra.h).
template <LogicalType TYPE>
static Datum datum_from_type_min_impl() {
    using CppType = StorageCppType<TYPE>;
    CppType value{};
    get_type_info(TYPE)->set_to_min(&value);
    Datum d;
    d.set(value);
    return d;
}

static StatusOr<Datum> datum_from_type_min(LogicalType type) {
    switch (type) {
    case TYPE_BOOLEAN: {
        bool v;
        get_type_info(TYPE_BOOLEAN)->set_to_min(&v);
        Datum d;
        d.set_int8(v);
        return d;
    }
    case TYPE_TINYINT:
        return datum_from_type_min_impl<TYPE_TINYINT>();
    case TYPE_SMALLINT:
        return datum_from_type_min_impl<TYPE_SMALLINT>();
    case TYPE_INT:
        return datum_from_type_min_impl<TYPE_INT>();
    case TYPE_BIGINT:
        return datum_from_type_min_impl<TYPE_BIGINT>();
    case TYPE_LARGEINT:
        return datum_from_type_min_impl<TYPE_LARGEINT>();
    case TYPE_DATE:
        return datum_from_type_min_impl<TYPE_DATE>();
    case TYPE_DATETIME:
        return datum_from_type_min_impl<TYPE_DATETIME>();
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

StatusOr<SeekRange> TabletRangeHelper::create_seek_range_from(const TabletRangePB& tablet_range_pb,
                                                              const TabletSchemaCSPtr& tablet_schema,
                                                              MemPool* mem_pool) {
    SeekRange tablet_range;
    RETURN_IF_ERROR(validate_tablet_range(tablet_range_pb));
    if (!tablet_range_pb.has_lower_bound() && !tablet_range_pb.has_upper_bound()) {
        // (-inf, +inf)
        return tablet_range;
    }
    const auto& sort_key_idxes = tablet_schema->sort_key_idxes();
    DCHECK(!sort_key_idxes.empty());
    auto parse_bound_to_seek_tuple = [&](const TuplePB& tuple) -> StatusOr<SeekTuple> {
        const int n = tuple.values_size();
        if (n != sort_key_idxes.size()) {
            return Status::Corruption(
                    fmt::format("Unexpected number of values in TabletRangePB bound value, expected: {}, actual: {}",
                                sort_key_idxes.size(), n));
        }

        Schema schema;
        std::vector<Datum> values;
        values.reserve(n);
        for (int i = 0; i < n; i++) {
            const int idx = sort_key_idxes[i];
            schema.append_sort_key_idx(idx);
            auto f = std::make_shared<Field>(ChunkHelper::convert_field(idx, tablet_schema->column(idx)));
            schema.append(f);

            Datum datum;
            RETURN_IF_ERROR(DatumVariant::from_proto(tuple.values(i), &datum, nullptr, nullptr, mem_pool));
            values.emplace_back(std::move(datum));
        }
        return SeekTuple(std::move(schema), std::move(values));
    };

    SeekTuple lower;
    SeekTuple upper;
    if (tablet_range_pb.has_lower_bound()) {
        ASSIGN_OR_RETURN(lower, parse_bound_to_seek_tuple(tablet_range_pb.lower_bound()));
    }
    if (tablet_range_pb.has_upper_bound()) {
        ASSIGN_OR_RETURN(upper, parse_bound_to_seek_tuple(tablet_range_pb.upper_bound()));
    }

    tablet_range = SeekRange(std::move(lower), std::move(upper));
    tablet_range.set_inclusive_lower(tablet_range_pb.lower_bound_included());
    tablet_range.set_inclusive_upper(tablet_range_pb.upper_bound_included());
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
            RawDataVisitor visitor;
            RETURN_IF_ERROR(pk_column->accept(&visitor));
            return std::string(reinterpret_cast<const char*>(visitor.result()), pk_column->type_size());
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
