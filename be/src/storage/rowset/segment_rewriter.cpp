// Copyright (c) 2021 Beijing Dingshi Zongheng Technology Co., Ltd. All rights reserved.

#include "segment_rewriter.h"

#include "gen_cpp/segment.pb.h"
#include "gutil/strings/substitute.h"
#include "storage/fs/block_manager.h"
#include "storage/fs/fs_util.h"
#include "storage/rowset/segment_writer.h"
#include "storage/vectorized/chunk_helper.h"
#include "util/crc32c.h"
#include "util/slice.h"

namespace starrocks {
namespace segment_v2 {

SegmentRewriter::SegmentRewriter() {}

SegmentRewriter::~SegmentRewriter() {}

Status parse_segment_footer(std::unique_ptr<fs::ReadableBlock>* rblock, SegmentFooterPB* footer,
                            uint64_t* segment_data_size) {
    std::string buff;
    raw::stl_string_resize_uninitialized(&buff, 12);

    uint64_t file_size;
    RETURN_IF_ERROR((*rblock)->size(&file_size));

    if (file_size < 12) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: file size $1 < 12", (*rblock)->path(), file_size));
    }

    RETURN_IF_ERROR((*rblock)->read(file_size - buff.size(), buff));

    const uint32_t footer_length = UNALIGNED_LOAD32(buff.data() + buff.size() - 12);
    const uint32_t checksum = UNALIGNED_LOAD32(buff.data() + buff.size() - 8);
    const uint32_t magic_number = UNALIGNED_LOAD32(buff.data() + buff.size() - 4);

    if (magic_number != UNALIGNED_LOAD32(k_segment_magic)) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: magic number not match", (*rblock)->path()));
    }

    if (file_size < 12 + footer_length) {
        return Status::Corruption(strings::Substitute("Bad segment file $0: file size $1 < $2", (*rblock)->path(),
                                                      file_size, 12 + footer_length));
    }

    raw::stl_string_resize_uninitialized(&buff, footer_length);
    RETURN_IF_ERROR((*rblock)->read(file_size - buff.size() - 12, buff));
    uint32_t actual_checksum = crc32c::Value(buff.data(), buff.size());
    if (actual_checksum != checksum) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: footer checksum not match, actual=$1 vs expect=$2",
                                    (*rblock)->path(), actual_checksum, checksum));
    }

    if (!footer->ParseFromArray(buff.data(), buff.size())) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: failed to parse footer", (*rblock)->path()));
    }

    *segment_data_size = file_size - footer_length - 12;
    return Status::OK();
}

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
    RETURN_IF_ERROR(parse_segment_footer(&rblock, &footer, &remaining));

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
