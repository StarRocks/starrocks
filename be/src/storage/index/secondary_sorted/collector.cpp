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

#include "storage/index/secondary_sorted/collector.h"

#include <atomic>

#include "column/chunk.h"
#include "column/schema.h"
#include "common/config.h"
#include "common/logging.h"
#include "exec/sorting/sorting.h"
#include "fs/fs_factory.h"
#include "runtime/chunk_helper.h"
#include "storage/chunk_helper.h"
#include "storage/index/secondary_sorted/types.h"
#include "storage/lake/tablet_manager.h"
#include "storage/rowset/segment_writer.h"

namespace starrocks::secondary_sorted {

namespace {

constexpr size_t kAppendChunkSize = 4096;

std::string make_index_filename(int64_t txn_id, const std::string& index_name) {
    return fmt::format("sidx_{}_{}.idx", txn_id, index_name);
}

} // namespace

StatusOr<std::unique_ptr<SecondaryIndexCollector>> SecondaryIndexCollector::create(
        int64_t tablet_id, int64_t txn_id, const std::vector<SecondaryIndexDef>& defs,
        const TabletSchemaCSPtr& source_schema) {
    if (defs.empty() || source_schema == nullptr) {
        return std::unique_ptr<SecondaryIndexCollector>{};
    }
    auto collector = std::unique_ptr<SecondaryIndexCollector>(
            new SecondaryIndexCollector(tablet_id, txn_id, source_schema));

    for (const auto& def : defs) {
        PerIndexState st;
        st.name = def.index_name;
        st.col_names = def.index_col_names;
        st.source_col_ids.reserve(def.index_col_names.size());
        for (const auto& name : def.index_col_names) {
            size_t idx = source_schema->field_index(name);
            if (idx == static_cast<size_t>(-1)) {
                return Status::NotFound(
                        fmt::format("secondary index column '{}' not found in tablet schema (index '{}')", name,
                                     def.index_name));
            }
            st.source_col_ids.push_back(static_cast<uint32_t>(idx));
        }
        st.pos_col = Int64Column::create();
        collector->_indexes.push_back(std::move(st));
    }
    return collector;
}

SecondaryIndexCollector::SecondaryIndexCollector(int64_t tablet_id, int64_t txn_id, TabletSchemaCSPtr source_schema)
        : _tablet_id(tablet_id), _txn_id(txn_id), _source_schema(std::move(source_schema)) {}

Status SecondaryIndexCollector::add_chunk(const Chunk& chunk, uint32_t seg_id, uint32_t base_rowid) {
    const size_t n = chunk.num_rows();
    if (n == 0) return Status::OK();
    for (auto& st : _indexes) {
        RETURN_IF_ERROR(_add_chunk_to_index(st, chunk, seg_id, base_rowid));
    }
    return Status::OK();
}

Status SecondaryIndexCollector::_add_chunk_to_index(PerIndexState& st, const Chunk& chunk, uint32_t seg_id,
                                                   uint32_t base_rowid) {
    const size_t n = chunk.num_rows();

    // First time we see real data: clone empty columns from the source chunk
    // so the accumulator has matching types for each index column.
    if (st.idx_cols.empty()) {
        st.idx_cols.reserve(st.source_col_ids.size());
        for (uint32_t cid : st.source_col_ids) {
            // chunk may have been built from a Schema covering only the
            // write columns. Map source-schema column id through
            // schema()->get_field_index_by_name() when present.
            const auto& column = chunk.get_column_by_index(cid);
            st.idx_cols.push_back(column->clone_empty());
        }
    }

    for (size_t c = 0; c < st.source_col_ids.size(); ++c) {
        const auto& column = chunk.get_column_by_index(st.source_col_ids[c]);
        st.idx_cols[c]->append(*column, 0, n);
    }
    auto& pos_data = st.pos_col->get_data();
    pos_data.reserve(pos_data.size() + n);
    for (size_t r = 0; r < n; ++r) {
        pos_data.push_back(encode_position(seg_id, base_rowid + static_cast<uint32_t>(r)));
    }
    return Status::OK();
}

StatusOr<std::vector<SecondaryIndexFilePB>> SecondaryIndexCollector::finalize(
        std::shared_ptr<FileSystem> fs, lake::TabletManager* tablet_mgr) {
    std::vector<SecondaryIndexFilePB> out;
    if (tablet_mgr == nullptr) {
        return Status::InvalidArgument("SecondaryIndexCollector::finalize: missing tablet_mgr");
    }
    if (fs == nullptr) {
        // The tablet writer may not have been handed a FileSystem; resolve one
        // from the tablet's storage root, same fallback the writer uses for
        // segment files.
        const std::string root = tablet_mgr->tablet_root_location(_tablet_id);
        ASSIGN_OR_RETURN(fs, FileSystemFactory::CreateSharedFromString(root));
    }
    for (auto& st : _indexes) {
        ASSIGN_OR_RETURN(auto pb, _write_one_index(st, fs, tablet_mgr));
        if (!pb.file_name().empty()) {
            out.push_back(std::move(pb));
        }
    }
    return out;
}

StatusOr<SecondaryIndexFilePB> SecondaryIndexCollector::_write_one_index(PerIndexState& st,
                                                                       std::shared_ptr<FileSystem> fs,
                                                                       lake::TabletManager* tablet_mgr) {
    if (st.idx_cols.empty()) {
        // Nothing was ever written through this collector (empty rowset).
        SecondaryIndexFilePB pb;
        pb.set_index_name(st.name);
        for (auto& n : st.col_names) pb.add_index_col_names(n);
        return pb;
    }

    auto index_schema = build_index_tablet_schema(*_source_schema, st.source_col_ids, /*enable_bloom_filter=*/true);
    TabletSchemaCSPtr index_schema_csptr = index_schema;

    // Move accumulator MutableColumns into an immutable Columns view for
    // sort_and_tie. acc_index_cols becomes empty afterwards.
    Columns sort_view;
    sort_view.reserve(st.idx_cols.size());
    for (auto& mcol : st.idx_cols) {
        sort_view.emplace_back(std::move(mcol));
    }
    st.idx_cols.clear();

    Permutation perm;
    std::atomic<bool> cancel{false};
    RETURN_IF_ERROR(sort_and_tie_columns(cancel, sort_view, SortDescs::asc_null_first(sort_view.size()), &perm));

    Schema output_schema = ChunkHelper::convert_schema(index_schema_csptr);
    auto schema_ptr = std::make_shared<Schema>(output_schema);

    Columns src_columns = std::move(sort_view);
    src_columns.emplace_back(std::move(st.pos_col));
    auto src_chunk = std::make_shared<Chunk>(std::move(src_columns), schema_ptr);

    std::vector<uint32_t> indexes(perm.size());
    for (size_t i = 0; i < perm.size(); ++i) {
        indexes[i] = perm[i].index_in_chunk;
    }

    const std::string basename = make_index_filename(_txn_id, st.name);
    const std::string full_path = tablet_mgr->segment_location(_tablet_id, basename);
    WritableFileOptions wopts;
    ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(wopts, full_path));

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
    pb.set_index_name(st.name);
    pb.set_file_name(basename);
    pb.set_file_size(static_cast<int64_t>(segment_size));
    for (auto& n : st.col_names) pb.add_index_col_names(n);

    LOG(INFO) << "secondary_index_collector: built '" << st.name << "' for tablet=" << _tablet_id
              << " txn=" << _txn_id << " rows=" << total << " size=" << segment_size;
    return pb;
}

} // namespace starrocks::secondary_sorted
