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

#include <optional>

#include "formats/parquet/column_reader.h"

namespace starrocks::parquet {

class IcebergRowLineageReader : public ColumnReader {
public:
    explicit IcebergRowLineageReader(ColumnReaderPtr delegate);

    Status prepare() override;

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override;

    void set_need_parse_levels(bool need_parse_levels) override;

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOTypeFlags types, bool active) override;

    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override;

protected:
    bool has_physical_reader() const { return _delegate != nullptr; }

    StatusOr<ColumnPtr> read_physical_bigint_range(const Range<uint64_t>& range, const Filter* filter) const;

    ColumnReaderPtr _delegate;
};

class IcebergRowIdReader final : public IcebergRowLineageReader {
public:
    explicit IcebergRowIdReader(std::optional<int64_t> first_row_id);

    IcebergRowIdReader(ColumnReaderPtr delegate, std::optional<int64_t> first_row_id);

    ~IcebergRowIdReader() override = default;

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) override;

    Status fill_dst_column(ColumnPtr& dst, ColumnPtr& src) override;

    StatusOr<bool> row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                             CompoundNodeType pred_relation, const uint64_t rg_first_row,
                                             const uint64_t rg_num_rows) const override;

    StatusOr<bool> page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                              SparseRange<uint64_t>* row_ranges, CompoundNodeType pred_relation,
                                              const uint64_t rg_first_row, const uint64_t rg_num_rows) override;

private:
    // Helper method to apply a single predicate and return the resulting range
    StatusOr<bool> _apply_single_predicate(const ColumnPredicate* pred, SparseRange<int64_t>& result_range);

    bool _fallback_can_change_values() const { return _first_row_id.has_value(); }

    std::optional<int64_t> _first_row_id;
};

class IcebergLastUpdatedSequenceNumberReader final : public IcebergRowLineageReader {
public:
    explicit IcebergLastUpdatedSequenceNumberReader(Datum fallback_value);

    IcebergLastUpdatedSequenceNumberReader(ColumnReaderPtr delegate, bool can_use_fallback, Datum fallback_value);

    ~IcebergLastUpdatedSequenceNumberReader() override = default;

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) override;

    StatusOr<bool> row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                             CompoundNodeType pred_relation, const uint64_t rg_first_row,
                                             const uint64_t rg_num_rows) const override;

    StatusOr<bool> page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                              SparseRange<uint64_t>* row_ranges, CompoundNodeType pred_relation,
                                              const uint64_t rg_first_row, const uint64_t rg_num_rows) override;

private:
    bool _fallback_can_change_values() const { return _can_use_fallback && !_fallback_value.is_null(); }

    bool _can_use_fallback = false;
    Datum _fallback_value;
};
} // namespace starrocks::parquet
