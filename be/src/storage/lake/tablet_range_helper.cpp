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

namespace starrocks::lake {

Status TabletRangeHelper::_validate_tablet_range(const TabletRangePB& tablet_range_pb) {
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
    RETURN_IF_ERROR(_validate_tablet_range(tablet_range_pb));
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
            schema.append(std::move(f));

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
    RETURN_IF_ERROR(_validate_tablet_range(tablet_range_pb));
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
                return Status::InvalidArgument("Non-nullable primary key contains NULL in tablet range");
            }
            auto column = ColumnHelper::create_column(type_desc, is_nullable);
            column->append_datum(datum);
            chunk->append_column(std::move(column), (SlotId)idx);
        }

        std::vector<ColumnId> pk_columns(tablet_schema->num_key_columns());
        for (int i = 0; i < tablet_schema->num_key_columns(); i++) {
            pk_columns[i] = (ColumnId)i;
        }
        auto pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);
        MutableColumnPtr pk_column;
        RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(pkey_schema, &pk_column));
        PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, 1, pk_column.get());
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
            } else {
                return Status::InvalidArgument("TVariant value is required");
            }

            if (t_val.__isset.variant_type) {
                pb_val->set_variant_type(static_cast<VariantTypePB>(t_val.variant_type));
            } else {
                return Status::InvalidArgument("TVariant variant_type is required");
            }
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

} // namespace starrocks::lake
