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
#include "storage/chunk_helper.h"
#include "storage/primary_key_encoder.h"
#include "storage/types.h"

namespace starrocks::lake {

Status TabletRangeHelper::_parse_string_to_datum(const TypeDescriptor& type_desc, const std::string& value_str,
                                                 Datum* datum) {
    auto type_info = get_type_info(type_desc);
    if (type_info == nullptr) {
        return Status::InternalError(fmt::format("Unsupported type: {}", type_desc.type));
    }
    return datum_from_string(type_info.get(), datum, value_str, nullptr);
}

StatusOr<MutableColumns> TabletRangeHelper::get_lower_boundaries_from(const std::vector<TTabletRange>& tablet_ranges,
                                                                      size_t num_columns) {
    if (tablet_ranges.empty()) {
        return MutableColumns{};
    }
    const size_t num_ranges = tablet_ranges.size();

    // Infer TypeDescriptor for each range-distributed column from tablet ranges.
    // Caller should guarantee that for every column for range distribution, at least one
    // tablet range provides a concrete type information for lower bound or upper bound.
    std::vector<std::unique_ptr<TypeDescriptor>> type_descs(num_columns);
    for (size_t i = 0; i < num_columns; ++i) {
        for (const auto& tablet_range : tablet_ranges) {
            const TTuple* lower = tablet_range.__isset.lower_bound ? &tablet_range.lower_bound : nullptr;
            const TTuple* upper = tablet_range.__isset.upper_bound ? &tablet_range.upper_bound : nullptr;
            DCHECK(lower != nullptr || upper != nullptr);

            if (lower != nullptr) {
                DCHECK(i < lower->values.size());
                type_descs[i] = std::make_unique<TypeDescriptor>(TypeDescriptor::from_thrift(lower->values[i].type));
                break;
            }

            if (upper != nullptr) {
                DCHECK(i < upper->values.size());
                type_descs[i] = std::make_unique<TypeDescriptor>(TypeDescriptor::from_thrift(upper->values[i].type));
                break;
            }
        }
    }

    MutableColumns boundaries(num_columns);
    auto parse_bound = [&](const TTuple& bound) {
        for (size_t i = 0; i < num_columns; ++i) {
            const auto& type_desc = *type_descs[i];
            MutableColumnPtr& column = boundaries[i];
            if (!column) {
                column = ColumnHelper::create_column(type_desc, false);
                column->reserve(num_ranges);
            }

            const auto& value = bound.values[i];
            if (!value.__isset.value) {
                return Status::InternalError("Missing value in range bound");
            }

            Datum datum;
            RETURN_IF_ERROR(_parse_string_to_datum(type_desc, value.value, &datum));
            column->append_datum(datum);
        }
        return Status::OK();
    };

    for (size_t range_idx = 1; range_idx < num_ranges; ++range_idx) {
        const auto& tablet_range = tablet_ranges[range_idx];
        RETURN_IF_ERROR(parse_bound(tablet_range.lower_bound));
    }

    return boundaries;
}

StatusOr<SeekRange> TabletRangeHelper::create_seek_range_from(const TabletRangePB& tablet_range_pb,
                                                              const TabletSchemaCSPtr& tablet_schema) {
    SeekRange tablet_range;
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

            const auto& v = tuple.values(i);
            if (!v.has_type()) {
                return Status::Corruption("Missing type in TabletRangePB bound value");
            }
            if (!v.has_value()) {
                return Status::Corruption("Missing value in TabletRangePB_VariantPB bound value");
            }

            Datum datum;
            auto type_desc = TypeDescriptor::from_protobuf(v.type());
            RETURN_IF_ERROR(_parse_string_to_datum(type_desc, v.value(), &datum));
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

// TabletRangePB is always should be [lower_bound, upper_bound) which is consistent with the SstSeekRange.
StatusOr<SstSeekRange> TabletRangeHelper::create_sst_seek_range_from(const TabletRangePB& tablet_range_pb,
                                                                     const TabletSchemaCSPtr& tablet_schema) {
    SstSeekRange sst_seek_range;
    if (!tablet_range_pb.has_lower_bound() && !tablet_range_pb.has_upper_bound()) {
        // (-inf, +inf)
        return sst_seek_range;
    }

    const auto& sort_key_idxes = tablet_schema->sort_key_idxes();
    DCHECK(!sort_key_idxes.empty());
    DCHECK_EQ(sort_key_idxes.size(), tablet_schema->num_key_columns());

    // sort key must the same as pk key
    for (int i = 0; i < tablet_schema->num_key_columns(); i++) {
        if (sort_key_idxes[i] != i) {
            return Status::InternalError(
                    fmt::format("Sort key index {} must be the same as pk key index {}", i, sort_key_idxes[i]));
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
            const auto& v = tuple.values(i);
            if (!v.has_type()) {
                return Status::Corruption("Missing type in TabletRangePB bound value");
            }
            if (!v.has_value()) {
                return Status::Corruption("Missing value in TabletRangePB_VariantPB bound value");
            }

            Datum datum;
            auto type_desc = TypeDescriptor::from_protobuf(v.type());
            RETURN_IF_ERROR(_parse_string_to_datum(type_desc, v.value(), &datum));
            auto column = ColumnHelper::create_column(type_desc, false);
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

} // namespace starrocks::lake
