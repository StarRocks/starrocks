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

#include <cctz/time_zone.h>

#include <atomic>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "column/column_access_path.h"
#include "column/variant_path_parser.h"
#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/expr_context.h"
#include "formats/parquet/column_read_order_ctx.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/column_reader_factory.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/utils.h"
#include "gen_cpp/parquet_types.h"
#include "io/shared_buffered_input_stream.h"
#include "runtime/descriptors.h"
#include "storage/range.h"

namespace starrocks {
class RandomAccessFile;
struct HdfsScanStats;
class ExprContext;
class TIcebergSchemaField;
class THdfsScanRange;

namespace parquet {
class FileMetaData;
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

    // conjunct_ctxs that column is materialized in group reader
    std::unordered_map<SlotId, std::vector<ExprContext*>> conjunct_ctxs_by_slot;

    // columns
    std::vector<Column> read_cols;

    std::string timezone;

    HdfsScanStats* stats = nullptr;

    io::SharedBufferedInputStream* sb_stream = nullptr;

    int chunk_size = 0;

    RandomAccessFile* file = nullptr;

    const FileMetaData* file_metadata = nullptr;

    bool case_sensitive = false;

    bool use_file_pagecache = false;
    int64_t modification_time = 0;
    uint64_t file_size = 0;
    const DataCacheOptions* datacache_options;

    // used to identify io coalesce
    std::atomic<int32_t>* lazy_column_coalesce_counter = nullptr;

    // used for pageIndex
    std::vector<ExprContext*> min_max_conjunct_ctxs;
    const PredicateTree* predicate_tree = nullptr;

    // partition column
    const std::vector<HdfsScannerContext::ColumnInfo>* partition_columns = nullptr;
    // partition column value which read from hdfs file path
    const Columns* partition_values = nullptr;
    // not existed column
    const std::vector<SlotDescriptor*>* not_existed_slots = nullptr;
    // reserved field slots
    const std::vector<SlotDescriptor*>* reserved_field_slots = nullptr;
    // used for global low cardinality optimization
    ColumnIdToGlobalDictMap* global_dictmaps = &EMPTY_GLOBAL_DICTMAPS;
    const std::vector<ColumnAccessPathPtr>* column_access_paths = nullptr;

    int32_t scan_range_id = -1;
    const THdfsScanRange* scan_range = nullptr;
};

class GroupReader {
public:
    GroupReader(GroupReaderParam& param, int row_group_number, SkipRowsContextPtr skip_rows_ctx,
                int64_t row_group_first_row);
    GroupReader(GroupReaderParam& param, int row_group_number, SkipRowsContextPtr skip_rows_ctx,
                int64_t row_group_first_row, int64_t row_group_first_row_id);
    ~GroupReader();

    // init used to init column reader, and devide active/lazy
    // then we can use inited column collect io range.
    Status init();
    Status prepare();
    const tparquet::ColumnChunk* get_chunk_metadata(SlotId slot_id);
    const ParquetField* get_column_parquet_field(SlotId slot_id);
    ColumnReader* get_column_reader(SlotId slot_id);
    uint64_t get_row_group_first_row() const { return _row_group_first_row; }
    const tparquet::RowGroup* get_row_group_metadata() const;
    Status get_next(ChunkPtr* chunk, size_t* row_count);
    void collect_io_ranges(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                           ColumnIOTypeFlags types = ColumnIOType::PAGES);

    SparseRange<uint64_t> get_range() const { return _range; }
    SparseRange<uint64_t>& get_range() { return _range; }
    const bool get_is_group_filtered() const { return _is_group_filtered; }
    bool& get_is_group_filtered() { return _is_group_filtered; }

private:
    // Describes a virtual sub-path projection from a variant column, e.g. "data.id" in
    // "SELECT data.id FROM t" where data is a VARIANT column.  The virtual column has no
    // physical counterpart in the parquet file; it is computed by extracting parsed_path
    // from the source variant column identified by source_slot_id.
    struct VariantVirtualProjection {
        VariantPath parsed_path;    // sub-path to extract, e.g. ["id"]
        TypeDescriptor target_type; // expected output type of the projected sub-field
        SlotId source_slot_id;      // slot that holds the source VARIANT column data
                                    // (either a physical slot or a hidden slot with a negative id)
    };

    // A VARIANT column that must be read to serve virtual projections but is not itself
    // present in the output chunk (i.e. not in the user's SELECT list).
    // A temporary column is allocated in _read_chunk under a synthetic negative slot_id so
    // that _variant_virtual_projections can reference it without colliding with real slots.
    // One HiddenVariantSource is created per unique source column name; multiple virtual
    // columns referencing the same VARIANT source share the same hidden entry.
    //
    // is_active: true  → backs at least one virtual column that has a conjunct; must be read
    //                    before _apply_deferred_variant_conjuncts (included in active_chunk).
    // is_active: false → backs only projection-only virtual columns; can be read after
    //                    conjunct evaluation, avoiding reads for rows that are filtered out.
    struct HiddenVariantSource {
        SlotId slot_id;                       // synthetic negative id (-1, -2, ...) that identifies this source
        std::unique_ptr<ColumnReader> reader; // reader for the underlying VARIANT column
        bool is_active = false;               // active (pre-conjunct) vs lazy (post-conjunct)
    };

    void _set_end_offset(int64_t value) { _end_offset = value; }

    // deal_with_pageindex need collect pageindex io range first, it will collect all row groups' io together,
    // so it should be done in file reader. when reading the current row group, we need first deal_with_pageindex,
    // and then we can collect io range based on pageindex.
    Status _deal_with_pageindex();

    void _use_as_dict_filter_column(int col_idx, SlotId slot_id, std::vector<std::string>& sub_field_path);
    Status _rewrite_conjunct_ctxs_to_predicates(bool* is_group_filtered);

    StatusOr<bool> _filter_chunk_with_dict_filter(ChunkPtr* chunk, Filter* filter);
    // Evaluates deferred variant virtual conjuncts against active_chunk.
    // Returns the filter to be applied by the caller; active_chunk is NOT filtered
    // by this method — the caller merges it with chunk_filter and applies the combined
    // result once after Phase 4 (in get_next).
    // An empty filter means there were no deferred conjuncts (all rows pass).
    // A non-empty all-zero filter means every row was filtered out.
    // Evaluates deferred variant virtual conjuncts and optionally returns the projected
    // virtual columns used by those conjuncts.
    //
    // projected_chunk:
    //   - Output chunk keyed by virtual slot id, containing only slots that have deferred
    //     conjuncts and were projected in this phase.
    //   - Row order/row count matches `active_chunk` BEFORE Phase-4 combined filter is applied.
    //     Caller must apply the exact same filter to projected_chunk before passing it to
    //     _fill_dst_chunk.
    StatusOr<Filter> _apply_deferred_variant_conjuncts(ChunkPtr& active_chunk, size_t raw_count,
                                                       ChunkPtr* projected_chunk);
    Status _align_deferred_projected_chunk_after_filter(const ChunkPtr& active_chunk,
                                                        const ChunkPtr& deferred_projected_chunk,
                                                        const Filter& chunk_filter, size_t pre_filter_rows);
    // Fills output chunk and computes virtual projections. For slots present in
    // `projected_chunk`, reuse precomputed columns (from deferred conjunct evaluation) to
    // avoid re-projecting and re-seeking variant paths.
    Status _fill_dst_chunk(ChunkPtr& active_chunk, const ChunkPtr& projected_chunk, ChunkPtr* chunk);
    const cctz::time_zone& _get_variant_projection_timezone();

    Status _create_column_readers();
    StatusOr<ColumnReaderPtr> _create_reserved_iceberg_column_reader(const SlotDescriptor* slot, int32_t field_id);
    StatusOr<Datum> _get_extended_bigint_value(SlotId slot_id) const;
    StatusOr<ColumnReaderPtr> _create_column_reader(const GroupReaderParam::Column& column);
    VariantShreddedReadHints _get_variant_shredded_hints(std::string_view column_name) const;
    Status _prepare_column_readers() const;
    // Creates a lightweight VIEW chunk of _read_chunk containing the given slot_ids.
    // Each entry in slot_ids must already exist as a column in _read_chunk. _init_read_chunk()
    // pre-allocates physical slots and active hidden variant source slots; lazy hidden sources
    // are allocated separately during Phase 6 of the variant read pipeline.
    // When include_reserved_fields is true, reserved_field_slots are appended after slot_ids.
    ChunkPtr _create_read_chunk(const std::vector<SlotId>& slot_ids, bool include_reserved_fields = true);
    // Extract dict filter columns and conjuncts
    void _process_columns_and_conjunct_ctxs();

    bool _try_to_use_dict_filter(const GroupReaderParam::Column& column, ExprContext* ctx,
                                 std::vector<std::string>& sub_field_path, bool is_decode_needed);

    Status _init_read_chunk();

    Status _read_range(const std::vector<int>& read_columns, const Range<uint64_t>& range, const Filter* filter,
                       ChunkPtr* chunk, bool ignore_reserved_field = false);

    StatusOr<size_t> _read_range_round_by_round(const Range<uint64_t>& range, Filter* filter, ChunkPtr* chunk);

    // row group meta
    const tparquet::RowGroup* _row_group_metadata = nullptr;
    int64_t _row_group_first_row = 0;
    int64_t _row_group_first_row_id = 0;
    SkipRowsContextPtr _skip_rows_ctx;

    // column readers for column chunk in row group
    std::unordered_map<SlotId, std::unique_ptr<ColumnReader>> _column_readers;

    // Maps each virtual-column slot id to its projection descriptor.
    // Virtual columns (e.g. "data.id") are skipped in the normal fill path and handled
    // separately in _fill_dst_chunk by extracting the sub-path from the source variant column.
    std::unordered_map<SlotId, VariantVirtualProjection> _variant_virtual_projections;

    // Maps source variant column name to its hidden reader and temporary slot.
    // Only populated when the source VARIANT column is not itself in the output (no physical slot).
    // If the source column IS selected by the user, its physical slot is used directly and no
    // hidden entry is created, avoiding a double read.
    std::unordered_map<std::string, HiddenVariantSource> _hidden_variant_sources;

    // Fast reverse-lookup: slot_id → HiddenVariantSource* (pointer into _hidden_variant_sources).
    // Built in _create_column_readers() after _hidden_variant_sources is fully populated.
    std::unordered_map<SlotId, HiddenVariantSource*> _hidden_slot_index;

    // Counter for assigning synthetic slot ids to hidden variant sources.
    // Starts at -1 and decrements so as not to collide with real (non-negative) slot ids.
    SlotId _next_hidden_slot_id = -1;

    // Slot ids of hidden sources classified as active (needed before conjunct eval) and
    // lazy (projection-only, deferred until after conjunct eval).
    // Populated in _process_columns_and_conjunct_ctxs().
    std::vector<SlotId> _active_hidden_slot_ids;
    std::vector<SlotId> _lazy_hidden_slot_ids;

    // Unified slot id lists for _create_read_chunk.  Populated by
    // _process_columns_and_conjunct_ctxs() after the column/conjunct classification.
    //   _active_slot_ids = physical active slots  +  active hidden variant sources
    //   _lazy_slot_ids   = physical lazy slots   (hidden lazy sources are read into a
    //                      temporary chunk in Phase 6, not via _create_read_chunk)
    std::vector<SlotId> _active_slot_ids;
    std::vector<SlotId> _lazy_slot_ids;

    // conjunct ctxs that eval after chunk is dict decoded
    std::vector<ExprContext*> _left_conjunct_ctxs;
    // variant virtual-column conjuncts can only be evaluated after post-read projection.
    std::vector<ExprContext*> _deferred_variant_virtual_conjunct_ctxs;
    // slot ids of virtual columns that have at least one deferred conjunct.
    // used in _apply_deferred_variant_conjuncts to detect invariant violations.
    std::unordered_set<SlotId> _deferred_conjunct_slot_ids;

    // active columns that hold read_col index
    std::vector<int> _active_column_indices;
    // lazy conlumns that hold read_col index
    std::vector<int> _lazy_column_indices;
    // load lazy column or not
    bool _lazy_column_needed = false;

    // dict value is empty after conjunct eval, file group can be skipped
    bool _is_group_filtered = false;

    // Column backing store for each get_next() call.  Initialized once in _init_read_chunk()
    // and reset at the start of every range iteration.  Holds three categories of columns:
    //   • Physical slots – one column per _param.read_cols entry plus reserved_field_slots.
    //     The same ColumnPtr objects are SHARED with active_chunk / lazy_chunk (created via
    //     _create_read_chunk), so filtering those view-chunks also modifies _read_chunk.
    //   • Active hidden variant sources – included in active_chunk via _active_slot_ids.
    //     Filtered automatically by active_chunk->filter() in Phase 4.
    //   • Lazy hidden variant sources – read into a temporary chunk in Phase 6 (after
    //     variant conjunct eval) and merged into active_chunk.  _fill_dst_chunk then
    //     consumes them from active_chunk; they are not accessed via _read_chunk directly.
    ChunkPtr _read_chunk;

    // param for read row group
    const GroupReaderParam& _param;

    ObjectPool _obj_pool;

    ColumnReaderOptions _column_reader_opts;

    int64_t _end_offset = 0;

    // columns(index) use as dict filter column
    std::vector<int> _dict_column_indices;
    std::unordered_map<int, std::vector<std::vector<std::string>>> _dict_column_sub_field_paths;
    std::unordered_map<SlotId, std::vector<ExprContext*>> _left_no_dict_filter_conjuncts_by_slot;

    SparseRange<uint64_t> _range;
    SparseRangeIterator<uint64_t> _range_iter;

    // round by round ctx
    std::unique_ptr<ColumnReadOrderCtx> _column_read_order_ctx;

    // a flag to reflect prepare() is called
    bool _has_prepared = false;

    // Parsed lazily for VARIANT virtual projections only. Most Parquet readers do not need
    // this value, so avoid failing unrelated scans on malformed session timezone strings.
    cctz::time_zone _timezone_obj = cctz::utc_time_zone();
    bool _timezone_resolved = false;
};

using GroupReaderPtr = std::shared_ptr<GroupReader>;

} // namespace starrocks::parquet
