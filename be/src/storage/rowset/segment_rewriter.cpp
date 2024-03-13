// Copyright (c) 2021 Beijing Dingshi Zongheng Technology Co., Ltd. All rights reserved.

#include "segment_rewriter.h"

#include "column/chunk.h"
#include "column/column.h"
#include "column/schema.h"
#include "fs/fs.h"
#include "gen_cpp/segment.pb.h"
#include "storage/chunk_helper.h"
#include "storage/lake/types_fwd.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "testutil/sync_point.h"
#include "util/filesystem_util.h"
#include "util/raw_container.h"
#include "util/slice.h"

namespace starrocks {

SegmentRewriter::SegmentRewriter() = default;

Status SegmentRewriter::rewrite(const std::string& src_path, FileInfo* dest_path,
                                const std::shared_ptr<const TabletSchema>& tschema, std::vector<uint32_t>& column_ids,
                                std::vector<std::unique_ptr<Column>>& columns, uint32_t segment_id,
                                const FooterPointerPB& partial_rowset_footer) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(dest_path->path));
    WritableFileOptions wopts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(wopts, dest_path->path));
    ASSIGN_OR_RETURN(auto rfile, fs->new_random_access_file(src_path));

    SegmentFooterPB footer;
    RETURN_IF_ERROR(Segment::parse_segment_footer(rfile.get(), &footer, nullptr, &partial_rowset_footer));
    // keep the partial rowset footer in dest file
    // because be may be crash during update rowset meta
    uint64_t remaining = partial_rowset_footer.position() + partial_rowset_footer.size();
    std::string read_buffer;
    raw::stl_string_resize_uninitialized(&read_buffer, 4096);
    uint64_t offset = 0;
    while (remaining > 0) {
        if (remaining < 4096) {
            raw::stl_string_resize_uninitialized(&read_buffer, remaining);
        }

        RETURN_IF_ERROR(rfile->read_at_fully(offset, read_buffer.data(), read_buffer.size()));
        RETURN_IF_ERROR(wfile->append(read_buffer));

        offset += read_buffer.size();
        remaining -= read_buffer.size();
    }

    SegmentWriterOptions opts;
    SegmentWriter writer(std::move(wfile), segment_id, tschema, opts);
    RETURN_IF_ERROR(writer.init(column_ids, false, &footer));

    auto schema = ChunkHelper::convert_schema(tschema, column_ids);
    auto chunk = ChunkHelper::new_chunk(schema, columns[0]->size());
    for (int i = 0; i < columns.size(); ++i) {
        chunk->get_column_by_index(i).reset(columns[i].release());
    }
    uint64_t index_size = 0;
    uint64_t segment_file_size;
    RETURN_IF_ERROR(writer.append_chunk(*chunk));
    RETURN_IF_ERROR(writer.finalize_columns(&index_size));
    TEST_ERROR_POINT("SegmentRewriter::rewrite1");
    RETURN_IF_ERROR(writer.finalize_footer(&segment_file_size));

    dest_path->size = segment_file_size;
    return Status::OK();
}

// This function is used when the auto-increment column is not specified in partial update.
// In this function, we use the segment iterator to read the old data, replace the old auto
// increment column, and rewrite the full segment file through SegmentWriter.
Status SegmentRewriter::rewrite(const std::string& src_path, const std::string& dest_path,
                                const TabletSchemaCSPtr& tschema,
                                AutoIncrementPartialUpdateState& auto_increment_partial_update_state,
                                std::vector<uint32_t>& column_ids, std::vector<std::unique_ptr<Column>>* columns) {
    if (column_ids.size() == 0) {
        DCHECK_EQ(columns, nullptr);
    }

    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(dest_path));

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

    auto chunk_shared_ptr = ChunkHelper::new_chunk(src_schema, num_rows);
    auto read_chunk = chunk_shared_ptr.get();

    SegmentReadOptions seg_options;
    OlapReaderStatistics stats;
    seg_options.fs = fs;
    seg_options.stats = &stats;
    seg_options.chunk_size = num_rows;

    auto res = rowset->segments()[segment_id]->new_iterator(src_schema, seg_options);
    auto& itr = res.value();

    if (itr) {
        auto st = itr->get_next(read_chunk);
        DCHECK_EQ(read_chunk->num_rows(), num_rows);
    }
    itr->close();

    WritableFileOptions wopts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(wopts, dest_path));

    std::vector<uint32_t> full_column_ids(tschema->num_columns());
    std::iota(full_column_ids.begin(), full_column_ids.end(), 0);
    auto schema = ChunkHelper::convert_schema(tschema, full_column_ids);
    auto chunk = ChunkHelper::new_chunk(schema, full_column_ids.size());

    size_t update_columns_index = 0;
    size_t read_columns_index = 0;
    for (int i = 0; i < tschema->num_columns(); ++i) {
        if (i == auto_increment_column_id) {
            chunk->get_column_by_index(i).reset(auto_increment_partial_update_state.write_column.release());
        } else if (update_columns_set.find(i) != update_columns_set.end()) {
            chunk->get_column_by_index(i).reset((*columns)[update_columns_index].release());
            ++update_columns_index;
        } else {
            chunk->get_column_by_index(i).swap(read_chunk->get_column_by_index(read_columns_index));
            read_columns_index++;
        }
    }

    SegmentWriterOptions opts;
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
Status SegmentRewriter::rewrite(const std::string& src_path, FileInfo* dest_path, const TabletSchemaCSPtr& tschema,
                                starrocks::lake::AutoIncrementPartialUpdateState& auto_increment_partial_update_state,
                                std::vector<uint32_t>& column_ids, std::vector<std::unique_ptr<Column>>* columns,
                                const starrocks::lake::TxnLogPB_OpWrite& op_write, starrocks::lake::Tablet* tablet) {
    if (column_ids.size() == 0) {
        DCHECK_EQ(columns, nullptr);
    }

    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(dest_path->path));

    uint32_t auto_increment_column_id = 0;
    for (const auto& col : tschema->columns()) {
        if (col.is_auto_increment()) {
            break;
        }
        ++auto_increment_column_id;
    }
    uint32_t segment_id = auto_increment_partial_update_state.segment_id;

    std::vector<uint32_t> src_column_ids;
    std::set<uint32_t> update_columns_set(column_ids.begin(), column_ids.end());

    for (auto i = 0; i < tschema->num_columns(); ++i) {
        if (i != auto_increment_column_id && update_columns_set.find(i) == update_columns_set.end()) {
            src_column_ids.emplace_back(i);
        }
    }
    Schema src_schema = ChunkHelper::convert_schema(tschema, src_column_ids);

    size_t footer_sine_hint = 16 * 1024;
    auto tablet_mgr = tablet->tablet_mgr();
    auto segment_path = tablet->segment_location(op_write.rowset().segments(segment_id));
    auto segment_info = FileInfo{.path = segment_path};
    // not fill data and meta cache
    auto fill_cache = false;
    LakeIOOptions lake_io_opts{fill_cache, -1};
    ASSIGN_OR_RETURN(auto segment, tablet_mgr->load_segment(segment_info, segment_id, &footer_sine_hint, lake_io_opts,
                                                            fill_cache, tschema));
    uint32_t num_rows = segment->num_rows();

    auto chunk_shared_ptr = ChunkHelper::new_chunk(src_schema, num_rows);
    auto read_chunk = chunk_shared_ptr.get();

    SegmentReadOptions seg_options;
    OlapReaderStatistics stats;
    seg_options.fs = fs;
    seg_options.stats = &stats;
    seg_options.chunk_size = num_rows;

    auto res = segment->new_iterator(src_schema, seg_options);
    auto& itr = res.value();

    if (itr) {
        auto st = itr->get_next(read_chunk);
        DCHECK_EQ(read_chunk->num_rows(), num_rows);
    }
    itr->close();

    WritableFileOptions wopts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(wopts, dest_path->path));

    std::vector<uint32_t> full_column_ids(tschema->num_columns());
    std::iota(full_column_ids.begin(), full_column_ids.end(), 0);
    auto schema = ChunkHelper::convert_schema(tschema, full_column_ids);
    auto chunk = ChunkHelper::new_chunk(schema, full_column_ids.size());

    size_t update_columns_index = 0;
    size_t read_columns_index = 0;
    for (int i = 0; i < tschema->num_columns(); ++i) {
        if (i == auto_increment_column_id) {
            chunk->get_column_by_index(i).reset(auto_increment_partial_update_state.write_column.release());
        } else if (update_columns_set.find(i) != update_columns_set.end()) {
            chunk->get_column_by_index(i).reset((*columns)[update_columns_index].release());
            ++update_columns_index;
        } else {
            chunk->get_column_by_index(i).swap(read_chunk->get_column_by_index(read_columns_index));
            read_columns_index++;
        }
    }

    SegmentWriterOptions opts;
    SegmentWriter writer(std::move(wfile), segment_id, tschema, opts);
    RETURN_IF_ERROR(writer.init(full_column_ids, true));

    uint64_t index_size = 0;
    uint64_t segment_file_size;
    RETURN_IF_ERROR(writer.append_chunk(*chunk));
    RETURN_IF_ERROR(writer.finalize_columns(&index_size));
    TEST_ERROR_POINT("SegmentRewriter::rewrite3");
    RETURN_IF_ERROR(writer.finalize_footer(&segment_file_size));

    dest_path->size = segment_file_size;
    return Status::OK();
}

Status SegmentRewriter::rewrite(const std::string& src_path, const TabletSchemaCSPtr& tschema,
                                std::vector<uint32_t>& column_ids, std::vector<std::unique_ptr<Column>>& columns,
                                uint32_t segment_id, const FooterPointerPB& partial_rowset_footer) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(src_path));
    ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file(src_path));

    SegmentFooterPB footer;
    RETURN_IF_ERROR(Segment::parse_segment_footer(read_file.get(), &footer, nullptr, &partial_rowset_footer));

    int64_t trunc_len = partial_rowset_footer.position() + partial_rowset_footer.size();
    RETURN_IF_ERROR(FileSystemUtil::resize_file(src_path, trunc_len));

    WritableFileOptions fopts{.sync_on_close = true, .mode = FileSystem::MUST_EXIST};
    ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(fopts, src_path));

    SegmentWriterOptions opts;
    SegmentWriter writer(std::move(wfile), segment_id, tschema, opts);
    RETURN_IF_ERROR(writer.init(column_ids, false, &footer));

    auto schema = ChunkHelper::convert_schema(tschema, column_ids);
    auto chunk = ChunkHelper::new_chunk(schema, columns[0]->size());
    for (int i = 0; i < columns.size(); ++i) {
        chunk->get_column_by_index(i).reset(columns[i].release());
    }
    uint64_t index_size = 0;
    uint64_t segment_file_size;
    RETURN_IF_ERROR(writer.append_chunk(*chunk));
    RETURN_IF_ERROR(writer.finalize_columns(&index_size));
    TEST_ERROR_POINT("SegmentRewriter::rewrite4");
    RETURN_IF_ERROR(writer.finalize_footer(&segment_file_size));

    return Status::OK();
}

} // namespace starrocks
