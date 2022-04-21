// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/parquet/page_reader.h"

#include "common/config.h"
#include "env/env.h"
#include "gutil/strings/substitute.h"
#include "util/thrift_util.h"

namespace starrocks::parquet {

static constexpr size_t kHeaderBufSize = 1024;
static constexpr size_t kHeaderBufMaxSize = 16 * 1024;

PageReader::PageReader(RandomAccessFile* file, uint64_t start_offset, uint64_t length)
        : _stream(file, start_offset, length), _start_offset(start_offset), _finish_offset(start_offset + length) {
    _stream.reserve(config::parquet_buffer_stream_reserve_size);
}

Status PageReader::next_header() {
    if (_offset != _next_header_pos) {
        return Status::InternalError(
                strings::Substitute("Try to parse parquet column header in wrong position, offset=$0 vs expect=$1",
                                    _offset, _next_header_pos));
    }
    if (_offset >= _finish_offset) {
        return Status::EndOfFile("");
    }

    const uint8_t* page_buf = nullptr;

    uint32_t header_length = 0;
    size_t nbytes = kHeaderBufSize;

    do {
        RETURN_IF_ERROR(_stream.get_bytes(&page_buf, &nbytes, true));

        header_length = nbytes;
        auto st = deserialize_thrift_msg(page_buf, &header_length, TProtocolType::COMPACT, &_cur_header);
        if (st.ok()) {
            break;
        }
        nbytes <<= 2;
        if (nbytes > kHeaderBufMaxSize) {
            return Status::Corruption("Failed to decode parquet page header");
        }
    } while (true);

    _offset += header_length;
    _next_header_pos = _offset + _cur_header.compressed_page_size;
    _stream.skip(header_length);

    return Status::OK();
}

Status PageReader::read_bytes(const uint8_t** buffer, size_t size) {
    if (_offset + size > _next_header_pos) {
        return Status::InternalError("Size to read exceed page size");
    }
    uint64_t nbytes = size;
    RETURN_IF_ERROR(_stream.get_bytes(buffer, &nbytes));
    DCHECK_EQ(nbytes, size);
    _offset += nbytes;
    return Status::OK();
}

} // namespace starrocks::parquet
