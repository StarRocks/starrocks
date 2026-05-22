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
#include "runtime/chunk_helper.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
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

} // namespace

StatusOr<std::vector<uint32_t>> SecondaryIndexWriter::resolve_index_col_ids(const TabletSchema& source_schema,
                                                                            const std::vector<std::string>& col_names) {
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
    // Delegate to the shared helper so the writer and reader cannot drift in
    // synthetic-schema shape. Bloom on by default: an equality lookup on an
    // indexed column hits the per-page filter first and short-circuits the
    // ordinal scan when the value is absent.
    return build_index_tablet_schema(source_schema, index_col_ids, /*enable_bloom_filter=*/true);
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

    // Read schema used to pull idx_cols out of source segments. We pass the
    // source schema shared_ptr directly because TabletSchema::create() takes
    // it by const ref to the smart pointer.
    std::vector<int32_t> read_cids(index_col_ids.begin(), index_col_ids.end());
    auto read_tablet_schema = TabletSchema::create(input.source_schema, read_cids);
    Schema read_schema = ChunkHelper::convert_schema(read_tablet_schema);

    // Lazily-initialised accumulator columns. Stored as MutableColumns so we
    // can append into them; the COW pattern of regular Columns/ColumnPtr does
    // not allow in-place mutation through an immutable Ptr. We move them into
    // a `Columns` view at sort time.
    MutableColumns acc_index_cols;
    auto encoded_pos_col = Int64Column::create();

    int64_t total_rows = 0;
    const int64_t mem_limit_bytes = static_cast<int64_t>(config::secondary_index_build_mem_limit_mb) * 1024L * 1024L;

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
        ASSIGN_OR_RETURN(auto chunk, RuntimeChunkHelper::new_chunk_checked(read_schema, kAppendChunkSize));
        while (true) {
            chunk->reset();
            Status s = iter->get_next(chunk.get());
            if (s.is_end_of_file()) break;
            RETURN_IF_ERROR(s);
            const size_t n = chunk->num_rows();
            if (n == 0) continue;

            // First time we see real data: clone empty columns from the
            // chunk so the accumulator has matching types.
            if (acc_index_cols.empty()) {
                acc_index_cols.reserve(index_col_ids.size());
                for (size_t c = 0; c < index_col_ids.size(); ++c) {
                    acc_index_cols.push_back(chunk->get_column_by_index(c)->clone_empty());
                }
            }

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
                return Status::MemoryLimitExceeded(
                        fmt::format("secondary index build exceeded memory limit: {} bytes > {} bytes (config={} MB)",
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

    // Move MutableColumns into a Columns view for sorting. acc_index_cols is
    // unusable after this conversion.
    Columns sort_view;
    sort_view.reserve(acc_index_cols.size());
    for (auto& mcol : acc_index_cols) {
        sort_view.emplace_back(std::move(mcol));
    }
    acc_index_cols.clear();

    Permutation perm;
    std::atomic<bool> cancel{false};
    RETURN_IF_ERROR(sort_and_tie_columns(cancel, sort_view, SortDescs::asc_null_first(sort_view.size()), &perm));

    // Build a source chunk holding all columns (idx + encoded_pos) and use
    // Chunk::append_selective with the permutation to produce sorted output.
    Schema output_schema = ChunkHelper::convert_schema(index_schema_csptr);
    auto schema_ptr = std::make_shared<Schema>(output_schema);
    Columns src_columns = std::move(sort_view);
    // ColumnPtr is COW-immutable; we explicitly construct the int64 column
    // and add it to the trailing slot.
    src_columns.emplace_back(std::move(encoded_pos_col));
    auto src_chunk = std::make_shared<Chunk>(std::move(src_columns), schema_ptr);

    std::vector<uint32_t> indexes(perm.size());
    for (size_t i = 0; i < perm.size(); ++i) {
        indexes[i] = perm[i].index_in_chunk;
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

    const size_t total = perm.size();
    for (size_t start = 0; start < total; start += kAppendChunkSize) {
        size_t end = std::min(total, start + kAppendChunkSize);
        ASSIGN_OR_RETURN(auto out_chunk, RuntimeChunkHelper::new_chunk_checked(output_schema, end - start));
        out_chunk->append_selective(*src_chunk, indexes.data() + start, 0, end - start);
        RETURN_IF_ERROR(seg_writer->append_chunk(*out_chunk));
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
