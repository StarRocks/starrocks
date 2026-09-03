// Copyright (c) 2021 Beijing Dingshi Zongheng Technology Co., Ltd. All rights reserved.

#include "segment_rewriter.h"

#include "base/container/raw_container.h"
#include "base/path/filesystem_util.h"
#include "base/string/slice.h"
#include "base/testutil/sync_point.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/column.h"
#include "column/schema.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "gen_cpp/segment.pb.h"
#include "platform/key_cache.h"
#include "storage/chunk_helper.h"
#include "storage/lake/types_fwd.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"

namespace starrocks {

SegmentRewriter::SegmentRewriter() = default;

// Append to |out_ids| the ids of vector indexes whose .vi the dest segment will have. Shared-nothing
// callers pass out_ids == nullptr (empty path map), so nothing is recorded.
static void record_rewrite_vector_index_ids(const SegmentWriter& writer, std::vector<int64_t>* out_ids) {
    if (out_ids == nullptr) {
        return;
    }
    if (writer.defer_vector_index_build()) {
        if (writer.num_rows() < writer.vector_index_build_threshold()) {
            return;
        }
    } else if (!writer.has_vector_index_written()) {
        return;
    }
    for (const auto& [index_id, _] : writer.vector_index_file_paths()) {
        out_ids->push_back(index_id);
    }
}

Status SegmentRewriter::rewrite_partial_update(const FileInfo& src, FileInfo* dest,
                                               const std::shared_ptr<const TabletSchema>& tschema,
                                               std::vector<uint32_t>& column_ids, MutableColumns& columns,
                                               uint32_t segment_id, const FooterPointerPB& partial_rowset_footer,
                                               SegmentFileMark segment_file_mark,
                                               RewriteVectorIndexOptions vector_index_opts,
                                               std::vector<int64_t>* out_vector_index_ids) {
    constexpr size_t kBufferSize = 1024 * 1024; // 1 MB
    if (UNLIKELY(column_ids.empty())) {
        // In shared-nothing mode, this size can be null, and we don't need it so it's ok to return zero;
        dest->size = src.size.value_or(0);
        return fs::copy_file(src.path, dest->path, kBufferSize).status();
    }
    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(dest->path));
    RandomAccessFileOptions ropts;
    WritableFileOptions wopts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    if (!src.encryption_meta.empty()) {
        ASSIGN_OR_RETURN(ropts.encryption_info, KeyCache::instance().unwrap_encryption_meta(src.encryption_meta));
        wopts.encryption_info = ropts.encryption_info;
        dest->encryption_meta = src.encryption_meta;
    }
    ASSIGN_OR_RETURN(auto rfile, fs->new_random_access_file_with_bundling(ropts, src));
    ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(wopts, dest->path));

    SegmentFooterPB footer;
    RETURN_IF_ERROR(Segment::parse_segment_footer(rfile.get(), &footer, nullptr, &partial_rowset_footer));
    // keep the partial rowset footer in dest file
    // because be may be crash during update rowset meta
    uint64_t remaining = partial_rowset_footer.position() + partial_rowset_footer.size();
    std::string read_buffer;
    raw::stl_string_resize_uninitialized(&read_buffer, kBufferSize);
    uint64_t offset = 0;
    while (remaining > 0) {
        if (remaining < kBufferSize) {
            raw::stl_string_resize_uninitialized(&read_buffer, remaining);
        }

        // TODO(cbl): data is decrypted from rfile, then copy to wfile re-encrypted,
        // possible optimization opportunity to eliminate some decryption/encryption
        RETURN_IF_ERROR(rfile->read_at_fully(offset, read_buffer.data(), read_buffer.size()));
        RETURN_IF_ERROR(wfile->append(read_buffer));

        offset += read_buffer.size();
        remaining -= read_buffer.size();
    }

    SegmentWriterOptions opts;
    opts.segment_file_mark = std::move(segment_file_mark);
    // Direct how vector indexes on the rewritten columns are produced. Shared-data sync mode
    // passes location-provider-resolved .vi paths so the SegmentWriter writes them at the
    // reader-visible path (instead of the empty-segment_file_mark IndexDescriptor fallback, which
    // is unreachable via the location provider); async mode sets defer so .vi generation is left
    // to the FE-scheduled VectorIndexBuildTask. Shared-nothing leaves all at their defaults.
    opts.vector_index_file_paths = std::move(vector_index_opts.file_paths);
    opts.defer_vector_index_build = vector_index_opts.defer_build;
    opts.vector_index_build_threshold = vector_index_opts.build_threshold;
    SegmentWriter writer(std::move(wfile), segment_id, tschema, opts);
    RETURN_IF_ERROR(writer.init(column_ids, false, &footer));

    auto schema = ChunkHelper::convert_schema(tschema, column_ids);
    auto chunk = ChunkFactory::new_chunk(schema, columns[0]->size());
    for (int i = 0; i < columns.size(); ++i) {
        chunk->get_column_by_index(i).reset(std::move(columns[i]));
    }
    uint64_t index_size = 0;
    uint64_t segment_file_size;
    RETURN_IF_ERROR(writer.append_chunk(*chunk));
    RETURN_IF_ERROR(writer.finalize_columns(&index_size));
    TEST_ERROR_POINT("SegmentRewriter::rewrite1");
    RETURN_IF_ERROR(writer.finalize_footer(&segment_file_size));

    record_rewrite_vector_index_ids(writer, out_vector_index_ids);
    dest->size = segment_file_size;
    return Status::OK();
}

// This function is used when the auto-increment column is not specified in partial update.
// In this function, we use the segment iterator to read the old data, replace the old auto
// increment column, and rewrite the full segment file through SegmentWriter.
Status SegmentRewriter::rewrite_partial_update_owned_only(
        const FileInfo& src, FileInfo* dest, const std::shared_ptr<const TabletSchema>& tschema,
        const std::vector<uint32_t>& resolved_column_ids, MutableColumns& resolved_columns, const Filter& owned,
        uint32_t emitted_rowid_base, uint32_t segment_id, const FooterPointerPB& partial_rowset_footer,
        SegmentFileMark segment_file_mark, RewriteVectorIndexOptions vector_index_opts,
        std::vector<int64_t>* out_vector_index_ids) {
    RETURN_ERROR_IF_FALSE(resolved_column_ids.size() == resolved_columns.size(),
                          "resolved column ids and columns disagree");
    RETURN_ERROR_IF_FALSE(!owned.empty(), "owned-only rewrite needs an ownership mask");
    for (const auto& column : resolved_columns) {
        RETURN_ERROR_IF_FALSE(column->size() == owned.size(),
                              "a resolved column does not span the rows the iterator emitted");
    }
    // Whatever the resolved set does not cover is what the load actually wrote, and those are the only
    // columns physically present in the source segment.
    std::set<uint32_t> resolved(resolved_column_ids.begin(), resolved_column_ids.end());
    std::vector<uint32_t> written_column_ids;
    written_column_ids.reserve(tschema->num_columns() - resolved.size());
    for (uint32_t i = 0, n = tschema->num_columns(); i < n; i++) {
        if (resolved.count(i) == 0) {
            written_column_ids.emplace_back(i);
        }
    }
    RETURN_ERROR_IF_FALSE(!written_column_ids.empty(), "a partial segment with no written column");

    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(dest->path));
    RandomAccessFileOptions ropts;
    WritableFileOptions wopts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    if (!src.encryption_meta.empty()) {
        ASSIGN_OR_RETURN(ropts.encryption_info, KeyCache::instance().unwrap_encryption_meta(src.encryption_meta));
        wopts.encryption_info = ropts.encryption_info;
        dest->encryption_meta = src.encryption_meta;
    }

    // Segment keys its column readers on unique id, so the full tablet schema is safe for a partial
    // segment: the columns the load did not write simply get no reader. The iterator asks for the
    // written ones only, which is exactly what the file holds.
    size_t footer_length_hint = 16 * 1024;
    ASSIGN_OR_RETURN(auto segment,
                     Segment::open(fs, src, segment_id, tschema, &footer_length_hint, &partial_rowset_footer));

    auto written_schema = ChunkHelper::convert_schema(tschema, written_column_ids);
    OlapReaderStatistics stats;
    SegmentReadOptions read_opts;
    read_opts.fs = fs;
    read_opts.stats = &stats;
    ASSIGN_OR_RETURN(auto iter, segment->new_iterator(written_schema, read_opts));

    // Read the source through, keeping only the rows this tablet owns. |owned| covers the run of rows
    // the publish iterator emitted, which starts at |emitted_rowid_base| -- a rowid-narrowed read on a
    // sort-key == PK tablet emits a slice, not the whole file -- so a source row outside that run is
    // not this tablet's either and is dropped with the rest.
    auto kept = ChunkFactory::new_chunk(written_schema, owned.size());
    auto chunk = ChunkFactory::new_chunk(written_schema, DEFAULT_CHUNK_SIZE);
    size_t source_row = 0;
    while (true) {
        chunk->reset();
        auto st = iter->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        }
        RETURN_IF_ERROR(st);
        const size_t chunk_rows = chunk->num_rows();
        if (chunk_rows == 0) {
            continue;
        }
        Filter selection(chunk_rows, 0);
        for (size_t i = 0; i < chunk_rows; i++) {
            const size_t abs = source_row + i;
            if (abs >= emitted_rowid_base && abs - emitted_rowid_base < owned.size()) {
                selection[i] = owned[abs - emitted_rowid_base];
            }
        }
        source_row += chunk_rows;
        chunk->filter(selection);
        kept->append(*chunk);
    }
    iter->close();

    // The resolved columns are indexed like |owned|, so the same mask leaves the two halves paired.
    auto resolved_schema = ChunkHelper::convert_schema(tschema, resolved_column_ids);
    auto resolved_chunk = ChunkFactory::new_chunk(resolved_schema, owned.size());
    for (size_t i = 0; i < resolved_columns.size(); i++) {
        resolved_chunk->get_column_by_index(i).reset(std::move(resolved_columns[i]));
    }
    resolved_chunk->filter(owned);
    RETURN_ERROR_IF_FALSE(kept->num_rows() == resolved_chunk->num_rows(),
                          "written and resolved halves disagree after filtering");

    auto full_schema = ChunkHelper::convert_schema(tschema);
    auto out = ChunkFactory::new_chunk(full_schema, kept->num_rows());
    for (size_t i = 0; i < written_column_ids.size(); i++) {
        out->get_column_by_index(written_column_ids[i]) = kept->get_column_by_index(i);
    }
    for (size_t i = 0; i < resolved_column_ids.size(); i++) {
        out->get_column_by_index(resolved_column_ids[i]) = resolved_chunk->get_column_by_index(i);
    }

    ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(wopts, dest->path));
    SegmentWriterOptions opts;
    opts.segment_file_mark = std::move(segment_file_mark);
    opts.vector_index_file_paths = std::move(vector_index_opts.file_paths);
    opts.defer_vector_index_build = vector_index_opts.defer_build;
    opts.vector_index_build_threshold = vector_index_opts.build_threshold;
    SegmentWriter writer(std::move(wfile), segment_id, tschema, opts);
    // Every column is present, so unlike the copy-and-append rewrite this never has to satisfy the
    // sort key out of a value-only column set.
    RETURN_IF_ERROR(writer.init());
    RETURN_IF_ERROR(writer.append_chunk(*out));
    uint64_t index_size = 0;
    uint64_t segment_file_size = 0;
    uint64_t footer_position = 0;
    RETURN_IF_ERROR(writer.finalize(&segment_file_size, &index_size, &footer_position));

    record_rewrite_vector_index_ids(writer, out_vector_index_ids);
    dest->size = segment_file_size;
    return Status::OK();
}

Status SegmentRewriter::rewrite_auto_increment(const std::string& src_path, const std::string& dest_path,
                                               const TabletSchemaCSPtr& tschema,
                                               AutoIncrementPartialUpdateState& auto_increment_partial_update_state,
                                               std::vector<uint32_t>& column_ids, MutableColumns* columns,
                                               SegmentFileMark segment_file_mark) {
    if (column_ids.size() == 0) {
        DCHECK_EQ(columns, nullptr);
    }

    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(dest_path));

    uint32_t auto_increment_column_id = 0;
    for (const auto& col : tschema->columns()) {
        if (col.is_auto_increment()) {
            break;
        }
        ++auto_increment_column_id;
    }
    uint32_t segment_id = auto_increment_partial_update_state.segment_id;
    Rowset* rowset = auto_increment_partial_update_state.rowset;
    RETURN_IF_ERROR(rowset->load());

    uint32_t num_rows = rowset->segments()[segment_id]->num_rows();

    std::vector<uint32_t> src_column_ids;
    std::set<uint32_t> update_columns_set(column_ids.begin(), column_ids.end());

    for (auto i = 0; i < tschema->num_columns(); ++i) {
        if (i != auto_increment_column_id && update_columns_set.find(i) == update_columns_set.end()) {
            src_column_ids.emplace_back(i);
        }
    }
    Schema src_schema = ChunkHelper::convert_schema(tschema, src_column_ids);

    auto chunk_shared_ptr = ChunkFactory::new_chunk(src_schema, num_rows);
    auto read_chunk = chunk_shared_ptr.get();

    SegmentReadOptions seg_options;
    OlapReaderStatistics stats;
    seg_options.fs = fs;
    seg_options.stats = &stats;
    seg_options.chunk_size = num_rows;
    seg_options.temporary_data = true;

    ASSIGN_OR_RETURN(auto itr, rowset->segments()[segment_id]->new_iterator(src_schema, seg_options));

    if (itr) {
        auto st = itr->get_next(read_chunk);
        itr->close();
        // Do NOT swallow the read error: a transient read failure (page crc / decompress / io) here would
        // otherwise leave read_chunk short, and the downstream append_chunk would silently emit a segment whose
        // key columns have fewer rows than the value columns (segment num_rows=0 while value columns=N).
        TEST_SYNC_POINT_CALLBACK("SegmentRewriter::rewrite_auto_increment:get_next", &st);
        RETURN_IF_ERROR(st);
        TEST_SYNC_POINT_CALLBACK("SegmentRewriter::rewrite_auto_increment:read_chunk", read_chunk);
        if (UNLIKELY(read_chunk->num_rows() != num_rows)) {
            auto msg = "rewrite_auto_increment: read " + std::to_string(read_chunk->num_rows()) +
                       " rows from partial segment " + src_path + " but expected " + std::to_string(num_rows) +
                       " rows (partial-update/auto-increment segment read inconsistency)";
            LOG(ERROR) << msg;
            return Status::InternalError(msg);
        }
    }

    WritableFileOptions wopts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(wopts, dest_path));

    std::vector<uint32_t> full_column_ids(tschema->num_columns());
    std::iota(full_column_ids.begin(), full_column_ids.end(), 0);
    auto schema = ChunkHelper::convert_schema(tschema, full_column_ids);
    auto chunk = ChunkFactory::new_chunk(schema, full_column_ids.size());

    size_t update_columns_index = 0;
    size_t read_columns_index = 0;
    for (int i = 0; i < tschema->num_columns(); ++i) {
        if (i == auto_increment_column_id) {
            chunk->get_column_by_index(i).reset(std::move(auto_increment_partial_update_state.write_column));
        } else if (update_columns_set.find(i) != update_columns_set.end()) {
            chunk->get_column_by_index(i).reset(std::move((*columns)[update_columns_index]));
            ++update_columns_index;
        } else {
            chunk->get_column_by_index(i).swap(read_chunk->get_column_by_index(read_columns_index));
            read_columns_index++;
        }
    }

    SegmentWriterOptions opts;
    opts.segment_file_mark = std::move(segment_file_mark);
    SegmentWriter writer(std::move(wfile), segment_id, tschema, opts);
    RETURN_IF_ERROR(writer.init(full_column_ids, true));

    uint64_t index_size = 0;
    uint64_t segment_file_size;
    RETURN_IF_ERROR(writer.append_chunk(*chunk));
    RETURN_IF_ERROR(writer.finalize_columns(&index_size));
    TEST_ERROR_POINT("SegmentRewriter::rewrite2");
    RETURN_IF_ERROR(writer.finalize_footer(&segment_file_size));

    return Status::OK();
}

// This function is used when the auto-increment column is not specified in partial update.
// In this function, we use the segment iterator to read the old data, replace the old auto
// increment column, and rewrite the full segment file through SegmentWriter.
Status SegmentRewriter::rewrite_auto_increment_lake(
        const FileInfo& src, FileInfo* dest, const TabletSchemaCSPtr& tschema,
        starrocks::lake::AutoIncrementPartialUpdateState& auto_increment_partial_update_state,
        const std::vector<uint32_t>& unmodified_column_ids, MutableColumns* unmodified_column_data,
        const starrocks::lake::Tablet* tablet, RewriteVectorIndexOptions vector_index_opts,
        std::vector<int64_t>* out_vector_index_ids, const Filter& owned, uint32_t emitted_rowid_base) {
    if (unmodified_column_ids.size() == 0) {
        DCHECK_EQ(unmodified_column_data, nullptr);
    }

    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(dest->path));

    ColumnId auto_increment_column_id = 0;
    for (const auto& col : tschema->columns()) {
        if (col.is_auto_increment()) {
            break;
        }
        ++auto_increment_column_id;
    }
    uint32_t segment_id = auto_increment_partial_update_state.segment_id;

    std::vector<ColumnId> modified_column_ids;
    std::set<ColumnId> unmodified_column_id_set(unmodified_column_ids.begin(), unmodified_column_ids.end());

    for (auto i = 0; i < tschema->num_columns(); ++i) {
        if (i != auto_increment_column_id && unmodified_column_id_set.count(i) == 0) {
            modified_column_ids.emplace_back(i);
        }
    }
    Schema src_schema = ChunkHelper::convert_schema(tschema, modified_column_ids);

    size_t footer_sine_hint = 16 * 1024;
    auto tablet_mgr = tablet->tablet_mgr();
    // not fill data and meta cache
    auto fill_cache = false;
    LakeIOOptions lake_io_opts{.fill_data_cache = fill_cache, .buffer_size = -1};
    ASSIGN_OR_RETURN(auto segment,
                     tablet_mgr->load_segment(src, segment_id, &footer_sine_hint, lake_io_opts, fill_cache, tschema));
    uint32_t num_rows = segment->num_rows();

    auto chunk_shared_ptr = ChunkFactory::new_chunk(src_schema, num_rows);
    auto read_chunk = chunk_shared_ptr.get();

    SegmentReadOptions seg_options;
    OlapReaderStatistics stats;
    seg_options.fs = fs;
    seg_options.stats = &stats;
    seg_options.chunk_size = num_rows;
    seg_options.temporary_data = true;

    // Read data from the (partial) segment file generated by this import task
    ASSIGN_OR_RETURN(auto itr, segment->new_iterator(src_schema, seg_options));
    RETURN_IF_ERROR(itr->get_next(read_chunk));
    if (UNLIKELY(read_chunk->num_rows() != num_rows)) {
        LOG(ERROR) << "Unexpected row number. expected=" << num_rows << " real=" << read_chunk->num_rows();
        return Status::InternalError("Unexpected row count");
    }
    itr->close();

    // On a split cross publish the source segment holds the siblings' rows too. Drop them here so the
    // output is private with no foreign rows in it. |owned| covers the run of rows the publish
    // iterator EMITTED, starting at |emitted_rowid_base| -- a rowid-narrowed read emits a slice, not
    // the whole file -- so a source row outside that run is not this tablet's either, and the columns
    // the caller supplies are indexed like |owned| and take the same mask.
    if (!owned.empty()) {
        Filter selection(num_rows, 0);
        for (uint32_t i = 0; i < num_rows; i++) {
            if (i >= emitted_rowid_base && i - emitted_rowid_base < owned.size()) {
                selection[i] = owned[i - emitted_rowid_base];
            }
        }
        const size_t kept_rows = read_chunk->filter(selection);
        if (unmodified_column_data != nullptr) {
            for (auto& column : *unmodified_column_data) {
                RETURN_ERROR_IF_FALSE(column->size() == owned.size(),
                                      "an unmodified column does not span the rows the iterator emitted");
                (void)column->filter(owned);
            }
        }
        auto& ai_column = auto_increment_partial_update_state.write_column;
        if (ai_column != nullptr) {
            RETURN_ERROR_IF_FALSE(ai_column->size() == owned.size(),
                                  "the auto-increment column does not span the rows the iterator emitted");
            (void)ai_column->filter(owned);
        }
        RETURN_ERROR_IF_FALSE(ai_column == nullptr || ai_column->size() == kept_rows,
                              "auto-increment and written halves disagree after filtering");
        num_rows = static_cast<uint32_t>(kept_rows);
    }

    WritableFileOptions wopts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    if (!src.encryption_meta.empty()) {
        ASSIGN_OR_RETURN(wopts.encryption_info, KeyCache::instance().unwrap_encryption_meta(src.encryption_meta));
        dest->encryption_meta = src.encryption_meta;
    }
    ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(wopts, dest->path));

    auto schema = tschema->schema();
    auto chunk = ChunkFactory::new_chunk(*schema, num_rows);

    // Fill in the values of columns that have not been modified
    size_t unmodified_column_index = 0;
    size_t modified_column_index = 0;
    for (ColumnId i = 0, sz = tschema->num_columns(); i < sz; ++i) {
        if (i == auto_increment_column_id) {
            chunk->get_column_by_index(i).reset(std::move(auto_increment_partial_update_state.write_column));
        } else if (unmodified_column_id_set.count(i) > 0) {
            chunk->get_column_by_index(i).reset(std::move(unmodified_column_data->at(unmodified_column_index)));
            ++unmodified_column_index;
        } else {
            chunk->get_column_by_index(i).swap(read_chunk->get_column_by_index(modified_column_index));
            ++modified_column_index;
        }
    }

    // Write a complete segment file. All columns (including any vector-indexed one) go through
    // column writers here, so unlike rewrite_partial_update the inline (sync) vector index build
    // covers the whole schema; see RewriteVectorIndexOptions for the shared-data/-nothing split.
    SegmentWriterOptions opts;
    opts.vector_index_file_paths = std::move(vector_index_opts.file_paths);
    opts.defer_vector_index_build = vector_index_opts.defer_build;
    opts.vector_index_build_threshold = vector_index_opts.build_threshold;
    SegmentWriter writer(std::move(wfile), segment_id, tschema, opts);
    RETURN_IF_ERROR(writer.init());

    uint64_t index_size = 0;
    uint64_t segment_file_size;
    RETURN_IF_ERROR(writer.append_chunk(*chunk));
    RETURN_IF_ERROR(writer.finalize_columns(&index_size));
    TEST_ERROR_POINT("SegmentRewriter::rewrite3");
    RETURN_IF_ERROR(writer.finalize_footer(&segment_file_size));

    record_rewrite_vector_index_ids(writer, out_vector_index_ids);
    dest->size = segment_file_size;
    return Status::OK();
}

} // namespace starrocks
