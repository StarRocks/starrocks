// Copyright (c) 2021 Beijing Dingshi Zongheng Technology Co., Ltd. All rights reserved.

#include "segment_rewriter.h"

#include "column/chunk.h"
#include "column/column.h"
#include "column/schema.h"
#include "fs/fs.h"
#include "gen_cpp/segment.pb.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_writer.h"
#include "util/filesystem_util.h"
#include "util/raw_container.h"
#include "util/slice.h"

namespace starrocks {

SegmentRewriter::SegmentRewriter() {}

SegmentRewriter::~SegmentRewriter() {}

Status SegmentRewriter::rewrite(const std::string& src_path, const std::string& dest_path, const TabletSchema& tschema,
                                std::vector<uint32_t>& column_ids,
                                std::vector<std::unique_ptr<vectorized::Column>>& columns, uint32_t segment_id,
                                const FooterPointerPB& partial_rowset_footer) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(dest_path));
    WritableFileOptions wopts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(wopts, dest_path));
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
    SegmentWriter writer(std::move(wfile), segment_id, &tschema, opts);
    RETURN_IF_ERROR(writer.init(column_ids, false, &footer));

    auto schema = ChunkHelper::convert_schema_to_format_v2(tschema, column_ids);
    auto chunk = ChunkHelper::new_chunk(schema, columns[0]->size());
    for (int i = 0; i < columns.size(); ++i) {
        chunk->get_column_by_index(i).reset(columns[i].release());
    }
    uint64_t index_size = 0;
    uint64_t segment_file_size;
    RETURN_IF_ERROR(writer.append_chunk(*chunk));
    RETURN_IF_ERROR(writer.finalize_columns(&index_size));
    RETURN_IF_ERROR(writer.finalize_footer(&segment_file_size));

    return Status::OK();
}

Status SegmentRewriter::rewrite(const std::string& src_path, const TabletSchema& tschema,
                                std::vector<uint32_t>& column_ids,
                                std::vector<std::unique_ptr<vectorized::Column>>& columns, uint32_t segment_id,
                                const FooterPointerPB& partial_rowset_footer) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(src_path));
    ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file(src_path));

    SegmentFooterPB footer;
    RETURN_IF_ERROR(Segment::parse_segment_footer(read_file.get(), &footer, nullptr, &partial_rowset_footer));

    int64_t trunc_len = partial_rowset_footer.position() + partial_rowset_footer.size();
    RETURN_IF_ERROR(FileSystemUtil::resize_file(src_path, trunc_len));

    WritableFileOptions fopts{.sync_on_close = true, .mode = FileSystem::MUST_EXIST};
    ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(fopts, src_path));

    SegmentWriterOptions opts;
    SegmentWriter writer(std::move(wfile), segment_id, &tschema, opts);
    RETURN_IF_ERROR(writer.init(column_ids, false, &footer));

    auto schema = ChunkHelper::convert_schema_to_format_v2(tschema, column_ids);
    auto chunk = ChunkHelper::new_chunk(schema, columns[0]->size());
    for (int i = 0; i < columns.size(); ++i) {
        chunk->get_column_by_index(i).reset(columns[i].release());
    }
    uint64_t index_size = 0;
    uint64_t segment_file_size;
    RETURN_IF_ERROR(writer.append_chunk(*chunk));
    RETURN_IF_ERROR(writer.finalize_columns(&index_size));
    RETURN_IF_ERROR(writer.finalize_footer(&segment_file_size));

    return Status::OK();
}

} // namespace starrocks
