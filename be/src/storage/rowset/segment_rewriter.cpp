// Copyright (c) 2021 Beijing Dingshi Zongheng Technology Co., Ltd. All rights reserved.

#include "segment_rewriter.h"

#include "gen_cpp/segment.pb.h"
#include "gutil/strings/substitute.h"
#include "storage/fs/block_manager.h"
#include "storage/fs/fs_util.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_writer.h"
#include "storage/vectorized/chunk_helper.h"
#include "util/crc32c.h"
#include "util/slice.h"

namespace starrocks {
namespace segment_v2 {

SegmentRewriter::SegmentRewriter() {}

SegmentRewriter::~SegmentRewriter() {}

Status SegmentRewriter::rewrite(const std::string& src, const std::string& dest, const TabletSchema& tschema,
                                std::vector<uint32_t>& column_ids,
                                std::vector<std::unique_ptr<vectorized::Column>>& columns, size_t segment_id) {
    fs::BlockManager* block_mgr = fs::fs_util::block_manager();
    std::unique_ptr<fs::WritableBlock> wblock;
    fs::CreateBlockOptions wblock_opts({dest});
    RETURN_IF_ERROR(block_mgr->create_block(wblock_opts, &wblock));

    std::unique_ptr<fs::ReadableBlock> rblock;
    RETURN_IF_ERROR(block_mgr->open_block(src, &rblock));

    SegmentFooterPB footer;
    uint64_t remaining = 0;
    RETURN_IF_ERROR(Segment::parse_segment_footer(&rblock, &footer, nullptr, &remaining));

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
        vectorized::ColumnPtr& dst_col = chunk->get_column_by_index(i);
        vectorized::Column* src_col = columns[i].release();
        dst_col.reset(src_col);
    }
    uint64_t index_size = 0;
    uint64_t segment_file_size;
    RETURN_IF_ERROR(writer.append_chunk(*chunk));
    RETURN_IF_ERROR(writer.finalize_columns(&index_size));
    RETURN_IF_ERROR(writer.finalize_footer(&segment_file_size));

    return Status::OK();
}

} // namespace segment_v2

} // namespace starrocks
