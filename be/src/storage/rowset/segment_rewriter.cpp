// Copyright (c) 2021 Beijing Dingshi Zongheng Technology Co., Ltd. All rights reserved.

#include "segment_rewriter.h"

#include "env/env.h"
#include "gen_cpp/segment.pb.h"
#include "storage/fs/block_manager.h"
#include "storage/fs/fs_util.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_writer.h"
#include "storage/vectorized/chunk_helper.h"
#include "util/filesystem_util.h"
#include "util/slice.h"

namespace starrocks {

SegmentRewriter::SegmentRewriter() {}

SegmentRewriter::~SegmentRewriter() {}

Status SegmentRewriter::rewrite(const std::string& src_path, const std::string& dest_path, const TabletSchema& tschema,
                                std::vector<uint32_t>& column_ids,
                                std::vector<std::unique_ptr<vectorized::Column>>& columns, size_t segment_id,
                                const FooterPointerPB& partial_rowset_footer) {
    ASSIGN_OR_RETURN(auto block_mgr, fs::fs_util::block_manager(dest_path));
    std::unique_ptr<fs::WritableBlock> wblock;
    fs::CreateBlockOptions wblock_opts({dest_path});
    wblock_opts.mode = Env::CREATE_OR_OPEN_WITH_TRUNCATE;
    RETURN_IF_ERROR(block_mgr->create_block(wblock_opts, &wblock));

    std::unique_ptr<fs::ReadableBlock> rblock;
    RETURN_IF_ERROR(block_mgr->open_block(src_path, &rblock));

    SegmentFooterPB footer;
    RETURN_IF_ERROR(Segment::parse_segment_footer(rblock.get(), &footer, nullptr, &partial_rowset_footer));
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

        RETURN_IF_ERROR(rblock->read(offset, read_buffer));
        RETURN_IF_ERROR(wblock->append(read_buffer));

        offset += read_buffer.size();
        remaining -= read_buffer.size();
    }

    SegmentWriterOptions opts;
    opts.storage_format_version = config::storage_format_version;
    SegmentWriter writer(std::move(wblock), segment_id, &tschema, opts);
    RETURN_IF_ERROR(writer.init(column_ids, false, &footer));

    auto schema = vectorized::ChunkHelper::convert_schema_to_format_v2(tschema, column_ids);
    auto chunk = vectorized::ChunkHelper::new_chunk(schema, columns[0]->size());
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
                                std::vector<std::unique_ptr<vectorized::Column>>& columns, size_t segment_id,
                                const FooterPointerPB& partial_rowset_footer) {
    ASSIGN_OR_RETURN(auto block_mgr, fs::fs_util::block_manager(src_path));
    std::unique_ptr<fs::ReadableBlock> rblock;
    RETURN_IF_ERROR(block_mgr->open_block(src_path, &rblock));

    SegmentFooterPB footer;
    RETURN_IF_ERROR(Segment::parse_segment_footer(rblock.get(), &footer, nullptr, &partial_rowset_footer));

    int64_t trunc_len = partial_rowset_footer.position() + partial_rowset_footer.size();
    RETURN_IF_ERROR(FileSystemUtil::resize_file(src_path, trunc_len));
    block_mgr->erase_block_cache(src_path);

    std::unique_ptr<fs::WritableBlock> wblock;
    fs::CreateBlockOptions wblock_opts({src_path});
    wblock_opts.mode = Env::MUST_EXIST;
    RETURN_IF_ERROR(block_mgr->create_block(wblock_opts, &wblock));
    // set bytes_appended to src file size to ensure the correctness of new_segment_footer
    wblock->set_bytes_appended(trunc_len);

    SegmentWriterOptions opts;
    opts.storage_format_version = config::storage_format_version;
    SegmentWriter writer(std::move(wblock), segment_id, &tschema, opts);
    RETURN_IF_ERROR(writer.init(column_ids, false, &footer));

    auto schema = vectorized::ChunkHelper::convert_schema_to_format_v2(tschema, column_ids);
    auto chunk = vectorized::ChunkHelper::new_chunk(schema, columns[0]->size());
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
