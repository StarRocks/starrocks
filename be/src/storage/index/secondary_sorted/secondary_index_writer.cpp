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

#include "storage/index/secondary_sorted/secondary_index_writer.h"

#include <atomic>
#include <memory>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/config.h"
#include "common/logging.h"
#include "exec/sorting/sorting.h"
#include "fs/fs.h"
#include "gen_cpp/segment.pb.h"
#include "storage/chunk_helper.h"
#include "storage/index/secondary_sorted/types.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/tablet_manager.h"
#include "storage/olap_common.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_schema.h"

namespace starrocks::secondary_sorted {

namespace {

constexpr size_t kAppendChunkSize = 4096;

// Build the path basename for a secondary index file.
// Format: sidx_<txn>_<index_name>.idx
std::string make_index_filename(int64_t txn_id, const std::string& index_name) {
    return fmt::format("sidx_{}_{}.idx", txn_id, index_name);
}

// Resolve the absolute file path inside the Lake fileset's data directory.
std::string make_index_file_path(lake::TabletManager* tablet_mgr, int64_t tablet_id, const std::string& basename) {
    return tablet_mgr->segment_location(tablet_id, basename);
}

// Materialize a Chunk holding `index_col_columns + encoded_pos_column` for a
// row sub-range [start, end). We slice using cloned columns so the SegmentWriter
// can consume incremental chunks without aliasing.
ChunkPtr slice_chunk(const Columns& all_columns, const Schema& output_schema, size_t start, size_t end) {
    Columns sliced;
    sliced.reserve(all_columns.size());
    for (const auto& col : all_columns) {
        auto out = col->clone_empty();
        out->append(*col, start, end - start);
        sliced.push_back(std::move(out));
    }
    return std::make_shared<Chunk>(std::move(sliced), std::make_shared<Schema>(output_schema));
}

} // namespace

StatusOr<std::vector<uint32_t>> SecondaryIndexWriter::resolve_index_col_ids(
        const TabletSchema& source_schema, const std::vector<std::string>& col_names) {
    std::vector<uint32_t> ids;
    ids.reserve(col_names.size());
    for (const auto& name : col_names) {
        size_t idx = source_schema.field_index(name);
        if (idx == static_cast<size_t>(-1)) {
            return Status::NotFound(fmt::format("secondary index column '{}' not found in tablet schema", name));
        }
        ids.push_back(static_cast<uint32_t>(idx));
    }
    return ids;
}

TabletSchemaSPtr SecondaryIndexWriter::build_index_schema(const TabletSchema& source_schema,
                                                         const std::vector<uint32_t>& index_col_ids) {
    auto schema = std::make_shared<TabletSchema>();
    schema->set_id(TabletSchema::invalid_id());

    // Clone the index columns. Mark each as key + sort_key so SegmentWriter
    // arranges the short-key index and sorted layout we depend on for fast
    // prefix lookup.
    std::vector<ColumnId> sort_key_idxes;
    sort_key_idxes.reserve(index_col_ids.size());
    int32_t next_unique_id = 1; // synthetic IDs; not joined with source
    for (size_t i = 0; i < index_col_ids.size(); ++i) {
        TabletColumn col(source_schema.column(index_col_ids[i]));
        col.set_unique_id(next_unique_id++);
        col.set_is_key(true);
        col.set_is_sort_key(true);
        col.set_aggregation(STORAGE_AGGREGATE_NONE);
        // Defensive: secondary index files do not carry bitmap / bloom side
        // indexes; turn them off on the synthetic columns so SegmentWriter
        // does not try to emit them.
        col.set_is_bf_column(false);
        col.set_has_bitmap_index(false);
        schema->append_column(std::move(col));
        sort_key_idxes.push_back(static_cast<ColumnId>(i));
    }

    // Append the encoded-position column (BIGINT, non-key, non-null).
    TabletColumn pos_col(STORAGE_AGGREGATE_NONE, TYPE_BIGINT, /*is_nullable=*/false, next_unique_id++,
                        sizeof(int64_t));
    pos_col.set_name(kEncodedPositionColumnName);
    pos_col.set_is_key(false);
    pos_col.set_is_sort_key(false);
    schema->append_column(std::move(pos_col));

    schema->set_sort_key_idxes(std::move(sort_key_idxes));
    schema->set_num_short_key_columns(static_cast<uint16_t>(index_col_ids.size()));
    return schema;
}

StatusOr<SecondaryIndexFilePB> SecondaryIndexWriter::build(const BuildInput& input) {
    if (input.tablet_mgr == nullptr || input.fs == nullptr) {
        return Status::InvalidArgument("SecondaryIndexWriter::build: missing tablet_mgr/fs");
    }
    if (input.source_schema == nullptr) {
        return Status::InvalidArgument("SecondaryIndexWriter::build: null source_schema");
    }
    if (input.index_col_names.empty()) {
        return Status::InvalidArgument("SecondaryIndexWriter::build: empty index_col_names");
    }

    ASSIGN_OR_RETURN(auto index_col_ids, resolve_index_col_ids(*input.source_schema, input.index_col_names));
    auto index_schema = build_index_schema(*input.source_schema, index_col_ids);
    TabletSchemaCSPtr index_schema_csptr = index_schema;

    // Read schema used to pull idx_cols out of source segments. Built from
    // the source schema (not the synthetic one) so column ids match the
    // segments on disk.
    auto read_tablet_schema = TabletSchema::create(*input.source_schema,
                                                   std::vector<int32_t>(index_col_ids.begin(), index_col_ids.end()));
    Schema read_schema = ChunkHelper::convert_schema(read_tablet_schema);

    // Accumulate one Column per index col + one Int64Column for the encoded
    // (seg_id, rowid) position. All allocated to fit the rowset row count.
    Columns acc_index_cols;
    acc_index_cols.reserve(index_col_ids.size());
    for (size_t i = 0; i < index_col_ids.size(); ++i) {
        const TabletColumn& tc = index_schema->column(i);
        acc_index_cols.push_back(ChunkHelper::column_from_field_type(tc.type(), tc.is_nullable()));
    }
    auto encoded_pos_col = Int64Column::create();

    int64_t total_rows = 0;
    const int64_t mem_limit_bytes =
            static_cast<int64_t>(config::secondary_index_build_mem_limit_mb) * 1024L * 1024L;

    // Scan each source segment. The segment id we record is the segment's
    // ordinal in the rowset, which the caller has already assigned 0..N-1.
    OlapReaderStatistics stats;
    SegmentReadOptions read_opts;
    read_opts.fs = input.fs;
    read_opts.stats = &stats;
    // We intentionally do not apply the table's delete predicate or any
    // version filter here -- index entries that point to deleted/superseded
    // rows are filtered at query time via the existing DelVec pipeline.

    for (size_t seg_id = 0; seg_id < input.segments.size(); ++seg_id) {
        auto& segment = input.segments[seg_id];
        if (segment == nullptr) continue;
        ASSIGN_OR_RETURN(auto iter, segment->new_iterator(read_schema, read_opts));
        if (iter == nullptr) continue;

        // Track per-segment local rowid so the encoded position is correct
        // even when a segment yields multiple chunks.
        uint32_t seg_local_rowid = 0;
        auto chunk = ChunkHelper::new_chunk(read_schema, kAppendChunkSize);
        while (true) {
            chunk->reset();
            Status s = iter->get_next(chunk.get());
            if (s.is_end_of_file()) break;
            RETURN_IF_ERROR(s);
            const size_t n = chunk->num_rows();
            if (n == 0) continue;

            for (size_t c = 0; c < index_col_ids.size(); ++c) {
                acc_index_cols[c]->append(*chunk->get_column_by_index(c), 0, n);
            }
            // emit encoded_pos for each row, using a per-segment local rowid.
            auto& pos_data = encoded_pos_col->get_data();
            pos_data.reserve(pos_data.size() + n);
            for (size_t r = 0; r < n; ++r) {
                pos_data.push_back(encode_position(static_cast<uint32_t>(seg_id), seg_local_rowid + r));
            }
            seg_local_rowid += n;
            total_rows += n;

            int64_t approx_bytes = 0;
            for (auto& col : acc_index_cols) approx_bytes += col->byte_size();
            approx_bytes += encoded_pos_col->byte_size();
            if (approx_bytes > mem_limit_bytes) {
                return Status::MemoryLimitExceeded(fmt::format(
                        "secondary index build exceeded memory limit: {} bytes > {} bytes (config={} MB)",
                        approx_bytes, mem_limit_bytes, config::secondary_index_build_mem_limit_mb));
            }
        }
    }

    if (total_rows == 0) {
        LOG(INFO) << "SecondaryIndexWriter: rowset is empty, skipping index file '" << input.index_name << "'";
        SecondaryIndexFilePB pb;
        pb.set_index_name(input.index_name);
        pb.set_file_size(0);
        pb.set_file_name("");
        for (auto& n : input.index_col_names) pb.add_index_col_names(n);
        return pb;
    }

    // Sort columns by the index column prefix.
    SortDescs sort_descs(std::vector<int>(index_col_ids.size(), 1) /* asc */,
                        std::vector<int>(index_col_ids.size(), -1) /* nulls first */);
    Permutation perm;
    std::atomic<bool> cancel{false};
    RETURN_IF_ERROR(sort_and_tie_columns(cancel, acc_index_cols, sort_descs, &perm));

    // Apply permutation to all columns (index cols + encoded_pos). The
    // resulting Columns are sorted by the index prefix; ties broken by
    // input order. SegmentWriter requires data fed in sort-key order.
    Columns sorted_cols;
    sorted_cols.reserve(acc_index_cols.size() + 1);
    for (auto& col : acc_index_cols) {
        auto reordered = col->clone_empty();
        for (auto& p : perm) reordered->append(*col, p.index_in_chunk, 1);
        sorted_cols.push_back(std::move(reordered));
    }
    {
        auto reordered = encoded_pos_col->clone_empty();
        for (auto& p : perm) reordered->append(*encoded_pos_col, p.index_in_chunk, 1);
        sorted_cols.push_back(std::move(reordered));
    }

    // Allocate the output WritableFile in the Lake fileset.
    const std::string basename = make_index_filename(input.txn_id, input.index_name);
    const std::string full_path = make_index_file_path(input.tablet_mgr, input.tablet_id, basename);
    WritableFileOptions wopts;
    ASSIGN_OR_RETURN(auto wfile, input.fs->new_writable_file(wopts, full_path));

    SegmentWriterOptions seg_opts;
    seg_opts.is_compaction = false;
    seg_opts.skip_vector_index = true;
    auto seg_writer = std::make_unique<SegmentWriter>(std::move(wfile), /*segment_id=*/0, index_schema_csptr, seg_opts);
    RETURN_IF_ERROR(seg_writer->init());

    Schema output_schema = ChunkHelper::convert_schema(index_schema_csptr);
    const size_t total = sorted_cols.front()->size();
    for (size_t start = 0; start < total; start += kAppendChunkSize) {
        size_t end = std::min(total, start + kAppendChunkSize);
        auto chunk = slice_chunk(sorted_cols, output_schema, start, end);
        RETURN_IF_ERROR(seg_writer->append_chunk(*chunk));
    }

    uint64_t segment_size = 0;
    uint64_t idx_size = 0;
    uint64_t footer_position = 0;
    RETURN_IF_ERROR(seg_writer->finalize(&segment_size, &idx_size, &footer_position));

    SecondaryIndexFilePB pb;
    pb.set_index_name(input.index_name);
    pb.set_file_name(basename);
    pb.set_file_size(static_cast<int64_t>(segment_size));
    for (auto& n : input.index_col_names) pb.add_index_col_names(n);

    LOG(INFO) << "SecondaryIndexWriter: built '" << input.index_name << "' for tablet=" << input.tablet_id
              << " txn=" << input.txn_id << " rows=" << total << " size=" << segment_size << " path=" << basename;
    return pb;
}

} // namespace starrocks::secondary_sorted
