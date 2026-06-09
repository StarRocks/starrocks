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

#include <atomic>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cache/scan/shared_buffered_input_stream.h"
#include "column/column_access_path.h"
#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/expr_context.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/column_reader_factory.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/utils.h"
#include "gen_cpp/parquet_types.h"
#include "runtime/descriptors.h"
#include "storage/primitive/range.h"

namespace starrocks {
class RandomAccessFile;
struct HdfsScanStats;
struct HdfsScannerContext;
class ExprContext;
class TIcebergSchemaField;
class THdfsScanRange;

namespace parquet {
class ColumnMaterializer;
class FileMetaData;
class VariantProjectionHandler;
} // namespace parquet
struct TypeDescriptor;
} // namespace starrocks

namespace starrocks::parquet {

struct GroupReaderParam {
    struct Column {
        // parquet field index in root node's children
        int32_t idx_in_parquet;

        // column type in parquet file
        tparquet::Type::type type_in_parquet;

        SlotDescriptor* slot_desc = nullptr;

        const TIcebergSchemaField* t_lake_schema_field = nullptr;

        bool decode_needed;
        bool is_extended_variant_virtual = false;
        std::string source_variant_column_name;
        std::string variant_virtual_leaf_path;

        const TypeDescriptor& slot_type() const { return slot_desc->type(); }
        const SlotId slot_id() const { return slot_desc->id(); }
    };

    // Non-owning pointer to the scanner context. Provides access to all
    // context-derived fields (partition columns, not-existed slots, options,
    // global dicts, etc.) without copying them into every GroupReaderParam.
    // Always non-null when used from FileReader; may be null in unit tests.
    const HdfsScannerContext* scanner_ctx = nullptr;

    // conjunct_ctxs that column is materialized in group reader
    // Mutable per-group-reader shallow copy of the scanner context's by_slot map;
    // update_with_none_existed_slot() erases entries for absent columns.
    std::unordered_map<SlotId, std::vector<ExprContext*>> conjunct_ctxs_by_slot;

    // columns
    std::vector<Column> read_cols;

    HdfsScanStats* stats = nullptr;

    SharedBufferedInputStream* sb_stream = nullptr;

    int chunk_size = 0;

    RandomAccessFile* file = nullptr;

    const FileMetaData* file_metadata = nullptr;

    int64_t modification_time = 0;
    uint64_t file_size = 0;
    const DataCacheOptions* datacache_options;

    // used to identify io coalesce
    std::atomic<int32_t>* lazy_column_coalesce_counter = nullptr;

    // Kept directly in GroupReaderParam for test-compatibility; also used by
    // _get_extended_bigint_value() to read extended_columns from the scan range.
    int32_t scan_range_id = -1;
    const THdfsScanRange* scan_range = nullptr;
};

class GroupReader {
    friend class VariantProjectionHandler;

public:
    GroupReader(GroupReaderParam& param, int row_group_number, SkipRowsContextPtr skip_rows_ctx,
                int64_t row_group_first_row);
    GroupReader(GroupReaderParam& param, int row_group_number, SkipRowsContextPtr skip_rows_ctx,
                int64_t row_group_first_row, int64_t row_group_first_row_id);
    ~GroupReader();

    Status init();
    Status prepare();
    const tparquet::ColumnChunk* get_chunk_metadata(SlotId slot_id);
    const ParquetField* get_column_parquet_field(SlotId slot_id);
    ColumnReader* get_column_reader(SlotId slot_id);
    uint64_t get_row_group_first_row() const { return _row_group_first_row; }
    const tparquet::RowGroup* get_row_group_metadata() const;
    Status get_next(ChunkPtr* chunk, size_t* row_count);
    void collect_io_ranges(std::vector<SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                           ColumnIOTypeFlags types = ColumnIOType::PAGES);

    SparseRange<uint64_t> get_range() const { return _range; }
    SparseRange<uint64_t>& get_range() { return _range; }
    const bool get_is_group_filtered() const { return _is_group_filtered; }
    bool& get_is_group_filtered() { return _is_group_filtered; }

private:
    // ── Initialization ───────────────────────────────────────────────────────
    void _set_end_offset(int64_t value) { _end_offset = value; }
    Status _create_column_readers();
    StatusOr<ColumnReaderPtr> _create_reserved_iceberg_column_reader(const SlotDescriptor* slot, int32_t field_id);
    StatusOr<Datum> _get_extended_bigint_value(SlotId slot_id) const;
    StatusOr<ColumnReaderPtr> _create_column_reader(const GroupReaderParam::Column& column);
    void _process_columns_and_conjunct_ctxs();
    bool _try_to_use_dict_filter(const GroupReaderParam::Column& column, ExprContext* ctx,
                                 std::vector<std::string>& sub_field_path, bool is_decode_needed);
    Status _prepare_column_readers() const;

    // ── get_next() pipeline phases ───────────────────────────────────────────
    //
    // 1. Prune deleted rows: applies deletion bitmap to produce chunk_filter.
    //    Returns true if rows survive; false to skip this range entirely.
    StatusOr<bool> _prune_deleted_rows(const Range<uint64_t>& r, Filter& chunk_filter, bool& has_filter, size_t count);

    // 2. Read & filter active columns: reads active physical columns and
    //    evaluates dict / expression filters.  Populates chunk_filter and
    //    fills active_chunk.  Returns true if rows survive.
    StatusOr<bool> _read_and_filter_active_columns(const Range<uint64_t>& r, Filter& chunk_filter,
                                                   ChunkPtr& active_chunk, bool& has_filter, size_t count);

    // ── Member variables ─────────────────────────────────────────────────────

    // row group meta
    const tparquet::RowGroup* _row_group_metadata = nullptr;
    int64_t _row_group_first_row = 0;
    int64_t _row_group_first_row_id = 0;
    SkipRowsContextPtr _skip_rows_ctx;

    // column readers for column chunk in row group
    std::unordered_map<SlotId, std::unique_ptr<ColumnReader>> _column_readers;

    // Column materialization layer over ColumnReaders. Predicate classification
    // still lives in GroupReader until the next refactor steps.
    std::unique_ptr<ColumnMaterializer> _column_materializer;

    // ── Variant handler (always present; empty() when no variant columns) ───
    std::unique_ptr<VariantProjectionHandler> _variant;

    // dict value is empty after conjunct eval, file group can be skipped
    bool _is_group_filtered = false;

    // param for read row group
    const GroupReaderParam& _param;

    ObjectPool _obj_pool;

    ColumnReaderOptions _column_reader_opts;

    int64_t _end_offset = 0;

    bool _global_dict_applied_in_group = false;

    SparseRange<uint64_t> _range;
    SparseRangeIterator<uint64_t> _range_iter;

    // a flag to reflect prepare() is called
    bool _has_prepared = false;
};

using GroupReaderPtr = std::shared_ptr<GroupReader>;

} // namespace starrocks::parquet
