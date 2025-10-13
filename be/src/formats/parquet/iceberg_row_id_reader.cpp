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

#include "formats/parquet/iceberg_row_id_reader.h"

#include <limits>

#include "column/datum.h"
#include "formats/parquet/predicate_filter_evaluator.h"
#include "storage/range.h"

namespace starrocks::parquet {

Status IcebergRowIdReader::read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) {
    if (filter == nullptr) {
        // No filter, generate row ids for all rows in the range
        for (uint64_t i = range.begin(); i < range.end(); ++i) {
            // Generate row id based on the first row id and the current row index.
            int64_t row_id = _first_row_id + i;
            dst->append_datum(Datum(row_id));
        }
    } else {
        // Apply filter, only generate row ids for selected rows
        DCHECK_EQ(filter->size(), range.span_size()) << "Filter size must match range size";
        for (uint64_t i = range.begin(); i < range.end(); ++i) {
            size_t filter_index = i - range.begin();
            if ((*filter)[filter_index]) {
                // Generate row id based on the first row id and the current row index.
                int64_t row_id = _first_row_id + i;
                dst->append_datum(Datum(row_id));
            }
        }
    }
    return Status::OK();
}

Status IcebergRowIdReader::fill_dst_column(ColumnPtr& dst, ColumnPtr& src) {
    dst->swap_column(*src);
    return Status::OK();
}

void IcebergRowIdReader::collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges,
                                                  int64_t* end_offset, ColumnIOTypeFlags types, bool active) {
    // No IO ranges to collect for row id reader.
}

void IcebergRowIdReader::select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) {
    // No offset index selection needed for row id reader.
}

StatusOr<bool> IcebergRowIdReader::row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                             CompoundNodeType pred_relation,
                                                             const uint64_t rg_first_row,
                                                             const uint64_t rg_num_rows) const {
    DLOG(INFO) << "IcebergRowIdReader::row_group_zone_map_filter, predicates size: " << predicates.size()
               << ", rg_first_row: " << rg_first_row << ", rg_num_rows: " << rg_num_rows
               << ", rg_first_row_id: " << _first_row_id;

    ZoneMapDetail zone_map{Datum(_first_row_id + rg_first_row), Datum(_first_row_id + rg_first_row + rg_num_rows - 1),
                           false};
    bool ret = !PredicateFilterEvaluatorUtils::zonemap_satisfy(predicates, zone_map, pred_relation);
    if (ret == false) {
        DLOG(INFO) << "IcebergRowIdReader: row group zone map filter passed, no filtering applied."
                   << " row_id range(" << (_first_row_id + rg_first_row) << ", "
                   << (_first_row_id + rg_first_row + rg_num_rows - 1) << ")";
    } else {
        DLOG(INFO) << "IcebergRowIdReader: row group zone map filter applied, "
                   << "filtering happened, row_id range(" << (_first_row_id + rg_first_row) << ", "
                   << (_first_row_id + rg_first_row + rg_num_rows - 1) << ")";
    }
    return ret;
}

StatusOr<bool> IcebergRowIdReader::page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                              SparseRange<uint64_t>* row_ranges,
                                                              CompoundNodeType pred_relation,
                                                              const uint64_t rg_first_row, const uint64_t rg_num_rows) {
    SparseRange<int64_t> row_id_range(_first_row_id + rg_first_row, _first_row_id + rg_first_row + rg_num_rows);
    DLOG(INFO) << "original range: " << row_id_range.to_string();

    if (pred_relation == CompoundNodeType::AND) {
        // For AND relation, apply all predicates sequentially with intersection
        for (const auto& pred : predicates) {
            SparseRange<int64_t> pred_range;
            StatusOr<bool> result = _apply_single_predicate(pred, pred_range);
            if (!result.ok()) {
                return result.status();
            }
            if (!result.value()) {
                // Predicate not supported, can't apply filtering
                return false;
            }
            row_id_range &= pred_range;
            DLOG(INFO) << "after apply " << pred->debug_string() << ", ret: " << row_id_range.to_string();

            // Early exit if range becomes empty
            if (row_id_range.empty()) {
                DLOG(INFO) << "range becomes empty after applying " << pred->debug_string();
                break;
            }
        }
    } else if (pred_relation == CompoundNodeType::OR) {
        // For OR relation, apply all predicates and take union
        SparseRange<int64_t> union_range;
        bool has_valid_predicate = false;

        for (const auto& pred : predicates) {
            SparseRange<int64_t> pred_range;
            StatusOr<bool> result = _apply_single_predicate(pred, pred_range);
            if (!result.ok()) {
                return result.status();
            }
            if (!result.value()) {
                return false;
            }
            has_valid_predicate = true;
            union_range |= pred_range;
        }

        if (!has_valid_predicate) {
            // No supported predicates, can't apply filtering
            return false;
        }

        row_id_range &= union_range;
        DLOG(INFO) << "after applying OR predicates, ret: " << row_id_range.to_string();
    } else {
        return false;
    }

    if (row_id_range.span_size() == rg_num_rows) {
        DLOG(INFO) << "no filtering applied";
        return false;
    }

    // Convert row_id range to row ranges
    for (size_t i = 0; i < row_id_range.size(); i++) {
        Range<int64_t> range = row_id_range[i];
        DLOG(INFO) << "add range: " << range.to_string();
        row_ranges->add(Range<uint64_t>(range.begin() - _first_row_id, range.end() - _first_row_id));
    }

    return true;
}

StatusOr<bool> IcebergRowIdReader::_apply_single_predicate(const ColumnPredicate* pred,
                                                           SparseRange<int64_t>& result_range) {
    switch (pred->type()) {
    case PredicateType::kEQ: {
        // Equal: [value, value]
        int64_t value = pred->value().get_int64();
        result_range = SparseRange<int64_t>(value, value + 1);
        return true;
    }
    case PredicateType::kNE: {
        // Not Equal: [0, value) | (value, max]
        int64_t value = pred->value().get_int64();
        if (value > 0) {
            result_range.add(Range<int64_t>(0, value));
        }
        if (value < std::numeric_limits<int64_t>::max()) {
            result_range.add(Range<int64_t>(value + 1, std::numeric_limits<int64_t>::max()));
        }
        return true;
    }
    case PredicateType::kGT: {
        // Greater Than: (value, max]
        int64_t value = pred->value().get_int64();
        result_range = SparseRange<int64_t>(value + 1, std::numeric_limits<int64_t>::max());
        return true;
    }
    case PredicateType::kGE: {
        // Greater Equal: [value, max]
        int64_t value = pred->value().get_int64();
        result_range = SparseRange<int64_t>(value, std::numeric_limits<int64_t>::max());
        return true;
    }
    case PredicateType::kLT: {
        // Less Than: [0, value)
        int64_t value = pred->value().get_int64();
        result_range = SparseRange<int64_t>(0, value);
        return true;
    }
    case PredicateType::kLE: {
        // Less Equal: [0, value]
        int64_t value = pred->value().get_int64();
        result_range = SparseRange<int64_t>(0, value + 1);
        return true;
    }
    case PredicateType::kInList: {
        // In List: union of all values
        const auto& values = pred->values();
        if (values.empty()) {
            // Empty IN list means no rows match
            result_range = SparseRange<int64_t>();
            return true;
        }
        for (const auto& value : values) {
            int64_t int_value = value.get_int64();
            result_range.add(Range<int64_t>(int_value, int_value + 1));
        }
        return true;
    }
    case PredicateType::kNotInList: {
        // Not In List: complement of union of all values
        const auto& values = pred->values();
        if (values.empty()) {
            // Empty NOT IN list means all rows match
            result_range = SparseRange<int64_t>(0, std::numeric_limits<int64_t>::max());
            return true;
        }
        // Start with full range
        result_range.add(Range<int64_t>(0, std::numeric_limits<int64_t>::max()));

        // Subtract each value from the range
        for (const auto& value : values) {
            int64_t int_value = value.get_int64();
            SparseRange<int64_t> temp_range;

            // For each range in result_range, subtract the excluded value
            for (size_t i = 0; i < result_range.size(); i++) {
                const auto& range = result_range[i];
                if (range.begin() < int_value && range.end() > int_value + 1) {
                    // Range spans the excluded value, split into two parts
                    temp_range.add(Range<int64_t>(range.begin(), int_value));
                    temp_range.add(Range<int64_t>(int_value + 1, range.end()));
                } else if (range.begin() < int_value && range.end() > int_value) {
                    // Range ends at or after excluded value
                    temp_range.add(Range<int64_t>(range.begin(), int_value));
                } else if (range.begin() < int_value + 1 && range.end() > int_value + 1) {
                    // Range starts before excluded value ends
                    temp_range.add(Range<int64_t>(int_value + 1, range.end()));
                } else if (range.begin() >= int_value && range.end() <= int_value + 1) {
                    // Range is completely within excluded value, skip it
                    continue;
                } else {
                    // Range is outside excluded value, keep it
                    temp_range.add(range);
                }
            }
            result_range = temp_range;
        }
        return true;
    }
    case PredicateType::kIsNull: {
        // IS NULL: no rows match for row_id column (it's always not null)
        result_range = SparseRange<int64_t>();
        return true;
    }
    case PredicateType::kNotNull: {
        // IS NOT NULL: all rows match for row_id column (it's always not null)
        result_range = SparseRange<int64_t>(0, std::numeric_limits<int64_t>::max());
        return true;
    }
    default: {
        DLOG(WARNING) << "Unsupported predicate type for row_id filtering: " << pred->type();
        // For unsupported types, we can't apply filtering
        return false;
    }
    }
}

} // namespace starrocks::parquet
