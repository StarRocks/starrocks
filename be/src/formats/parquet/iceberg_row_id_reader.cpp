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

#include "column/column_helper.h"
#include "formats/parquet/predicate_filter_evaluator.h"
#include "formats/parquet/scalar_column_reader.h"
#include "storage/range.h"
#include "types/datum.h"
#include "types/type_descriptor.h"

namespace starrocks::parquet {

IcebergRowLineageReader::IcebergRowLineageReader(ColumnReaderPtr delegate)
        : ColumnReader(delegate == nullptr ? nullptr : delegate->get_column_parquet_field()),
          _delegate(std::move(delegate)) {}

Status IcebergRowLineageReader::prepare() {
    return _delegate == nullptr ? Status::OK() : _delegate->prepare();
}

void IcebergRowLineageReader::get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) {
    if (_delegate != nullptr) {
        _delegate->get_levels(def_levels, rep_levels, num_levels);
    }
}

void IcebergRowLineageReader::set_need_parse_levels(bool need_parse_levels) {
    if (_delegate != nullptr) {
        _delegate->set_need_parse_levels(need_parse_levels);
    }
}

void IcebergRowLineageReader::collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges,
                                                      int64_t* end_offset, ColumnIOTypeFlags types, bool active) {
    if (_delegate != nullptr) {
        _delegate->collect_column_io_range(ranges, end_offset, types, active);
    }
}

void IcebergRowLineageReader::select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) {
    if (_delegate != nullptr) {
        _delegate->select_offset_index(range, rg_first_row);
    }
}

StatusOr<ColumnPtr> IcebergRowLineageReader::read_physical_bigint_range(const Range<uint64_t>& range,
                                                                        const Filter* filter) const {
    DCHECK(_delegate != nullptr);
    ColumnPtr physical = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true);
    RETURN_IF_ERROR(_delegate->read_range(range, filter, physical));
    return physical;
}

IcebergRowIdReader::IcebergRowIdReader(std::optional<int64_t> first_row_id)
        : IcebergRowLineageReader(nullptr), _first_row_id(first_row_id) {}

IcebergRowIdReader::IcebergRowIdReader(ColumnReaderPtr delegate, std::optional<int64_t> first_row_id)
        : IcebergRowLineageReader(std::move(delegate)), _first_row_id(first_row_id) {}

Status IcebergRowIdReader::read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) {
    Column* dst_col = dst->as_mutable_raw_ptr();
    if (has_physical_reader()) {
        ASSIGN_OR_RETURN(ColumnPtr physical, read_physical_bigint_range(range, filter));
        for (size_t i = 0; i < physical->size(); ++i) {
            Datum datum = physical->get(i);
            if (!datum.is_null()) {
                dst_col->append_datum(datum);
            } else if (_first_row_id.has_value()) {
                dst_col->append_datum(Datum(_first_row_id.value() + range.begin() + i));
            } else {
                dst_col->append_datum(kNullDatum);
            }
        }
        return Status::OK();
    }

    // Ignore filter and output all rows in range, consistent with other reserved column readers.
    if (!_first_row_id.has_value()) {
        for (uint64_t i = range.begin(); i < range.end(); ++i) {
            dst_col->append_datum(kNullDatum);
        }
        return Status::OK();
    }
    for (uint64_t i = range.begin(); i < range.end(); ++i) {
        dst_col->append_datum(Datum(_first_row_id.value() + i));
    }
    return Status::OK();
}

Status IcebergRowIdReader::fill_dst_column(ColumnPtr& dst, ColumnPtr& src) {
    dst->as_mutable_raw_ptr()->swap_column(*(src->as_mutable_raw_ptr()));
    return Status::OK();
}

StatusOr<bool> IcebergRowIdReader::row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                             CompoundNodeType pred_relation,
                                                             const uint64_t rg_first_row,
                                                             const uint64_t rg_num_rows) const {
    if (has_physical_reader()) {
        if (!_fallback_can_change_values()) {
            return _delegate->row_group_zone_map_filter(predicates, pred_relation, rg_first_row, rg_num_rows);
        }
        return false;
    }
    if (!_first_row_id.has_value()) {
        return false;
    }
    ZoneMapDetail zone_map{Datum(_first_row_id.value() + rg_first_row),
                           Datum(_first_row_id.value() + rg_first_row + rg_num_rows - 1), false};
    bool ret = !PredicateFilterEvaluatorUtils::zonemap_satisfy(predicates, zone_map, pred_relation);
    return ret;
}

StatusOr<bool> IcebergRowIdReader::page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                              SparseRange<uint64_t>* row_ranges,
                                                              CompoundNodeType pred_relation,
                                                              const uint64_t rg_first_row, const uint64_t rg_num_rows) {
    if (has_physical_reader()) {
        if (!_fallback_can_change_values()) {
            return _delegate->page_index_zone_map_filter(predicates, row_ranges, pred_relation, rg_first_row,
                                                         rg_num_rows);
        }
        // Physical page stats only describe stored values, but this reader may replace NULLs with
        // inherited row-lineage values. Pruning on physical stats would be unsound here.
        return false;
    }
    if (!_first_row_id.has_value()) {
        return false;
    }
    SparseRange<int64_t> row_id_range(_first_row_id.value() + rg_first_row,
                                      _first_row_id.value() + rg_first_row + rg_num_rows);

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

            // Early exit if range becomes empty
            if (row_id_range.empty()) {
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
    } else {
        return false;
    }

    if (row_id_range.span_size() == rg_num_rows) {
        return false;
    }

    // Convert row_id range to row ranges
    for (size_t i = 0; i < row_id_range.size(); i++) {
        Range<int64_t> range = row_id_range[i];
        row_ranges->add(Range<uint64_t>(range.begin() - _first_row_id.value(), range.end() - _first_row_id.value()));
    }

    return true;
}

// Convert a single predicate on _row_id into a SparseRange of matching row_id values.
// Returns true if the predicate was converted, false if unsupported.
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

IcebergLastUpdatedSequenceNumberReader::IcebergLastUpdatedSequenceNumberReader(Datum fallback_value)
        : IcebergRowLineageReader(nullptr),
          _can_use_fallback(!fallback_value.is_null()),
          _fallback_value(std::move(fallback_value)) {}

IcebergLastUpdatedSequenceNumberReader::IcebergLastUpdatedSequenceNumberReader(ColumnReaderPtr delegate,
                                                                               bool can_use_fallback,
                                                                               Datum fallback_value)
        : IcebergRowLineageReader(std::move(delegate)),
          _can_use_fallback(can_use_fallback),
          _fallback_value(std::move(fallback_value)) {}

Status IcebergLastUpdatedSequenceNumberReader::read_range(const Range<uint64_t>& range, const Filter* filter,
                                                          ColumnPtr& dst) {
    if (!has_physical_reader()) {
        FixedValueColumnReader fallback_reader(_can_use_fallback ? _fallback_value : kNullDatum);
        return fallback_reader.read_range(range, filter, dst);
    }

    ASSIGN_OR_RETURN(ColumnPtr physical, read_physical_bigint_range(range, filter));
    Column* dst_col = dst->as_mutable_raw_ptr();
    for (size_t i = 0; i < physical->size(); ++i) {
        Datum datum = physical->get(i);
        if (!datum.is_null()) {
            dst_col->append_datum(datum);
        } else if (_can_use_fallback && !_fallback_value.is_null()) {
            dst_col->append_datum(_fallback_value);
        } else {
            dst_col->append_datum(kNullDatum);
        }
    }
    return Status::OK();
}

StatusOr<bool> IcebergLastUpdatedSequenceNumberReader::row_group_zone_map_filter(
        const std::vector<const ColumnPredicate*>& predicates, CompoundNodeType pred_relation,
        const uint64_t rg_first_row, const uint64_t rg_num_rows) const {
    if (has_physical_reader()) {
        if (!_fallback_can_change_values()) {
            return _delegate->row_group_zone_map_filter(predicates, pred_relation, rg_first_row, rg_num_rows);
        }
        return false;
    }
    FixedValueColumnReader fallback_reader(_can_use_fallback ? _fallback_value : kNullDatum);
    return fallback_reader.row_group_zone_map_filter(predicates, pred_relation, rg_first_row, rg_num_rows);
}

StatusOr<bool> IcebergLastUpdatedSequenceNumberReader::page_index_zone_map_filter(
        const std::vector<const ColumnPredicate*>& predicates, SparseRange<uint64_t>* row_ranges,
        CompoundNodeType pred_relation, const uint64_t rg_first_row, const uint64_t rg_num_rows) {
    if (has_physical_reader()) {
        if (!_fallback_can_change_values()) {
            return _delegate->page_index_zone_map_filter(predicates, row_ranges, pred_relation, rg_first_row,
                                                         rg_num_rows);
        }
        // Physical page stats only describe stored values, but this reader may replace NULLs with
        // file-level fallback values. Pruning on physical stats would be unsound here.
        return false;
    }
    FixedValueColumnReader fallback_reader(_can_use_fallback ? _fallback_value : kNullDatum);
    return fallback_reader.page_index_zone_map_filter(predicates, row_ranges, pred_relation, rg_first_row, rg_num_rows);
}

} // namespace starrocks::parquet
