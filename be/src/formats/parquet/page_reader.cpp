// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "formats/parquet/page_reader.h"

#include "common/config.h"
#include "gutil/strings/substitute.h"
#include "util/thrift_util.h"

namespace starrocks::parquet {

static constexpr size_t kHeaderInitSize = 1024;

PageReader::PageReader(io::SeekableInputStream* stream, uint64_t start_offset, uint64_t length, uint64_t num_values)
        : _stream(stream), _finish_offset(start_offset + length), _num_values_total(num_values) {}

Status PageReader::next_header() {
    if (_offset != _next_header_pos) {
        return Status::InternalError(
                strings::Substitute("Try to parse parquet column header in wrong position, offset=$0 vs expect=$1",
                                    _offset, _next_header_pos));
    }

    DCHECK(_num_values_read <= _num_values_total);
    if (_num_values_read >= _num_values_total) {
        LOG_IF(WARNING, _num_values_read > _num_values_total)
                << "Read more values than expected, read=" << _num_values_read << ", expect=" << _num_values_total;
        return Status::EndOfFile("");
    }

    std::vector<uint8_t> page_buffer;
    page_buffer.reserve(config::parquet_header_max_size);
    uint8_t* page_buf = page_buffer.data();

    size_t nbytes = kHeaderInitSize;
    size_t remaining = _finish_offset - _offset;
    uint32_t header_length = 0;

    do {
        nbytes = std::min(nbytes, remaining);
        RETURN_IF_ERROR(_stream->read_at_fully(_offset, page_buf, nbytes));

        header_length = nbytes;
        auto st = deserialize_thrift_msg(page_buf, &header_length, TProtocolType::COMPACT, &_cur_header);
        if (st.ok()) {
            break;
        }

        if ((nbytes > config::parquet_header_max_size) || (_offset + nbytes) >= _finish_offset) {
            return Status::Corruption("Failed to decode parquet page header");
        }
        nbytes <<= 2;
    } while (true);

    _offset += header_length;
    _next_header_pos = _offset + _cur_header.compressed_page_size;
    _num_values_read += _cur_header.data_page_header.num_values;
    return Status::OK();
}

Status PageReader::read_bytes(void* buffer, size_t size) {
    if (_offset + size > _next_header_pos) {
        return Status::InternalError("Size to read exceed page size");
    }
    RETURN_IF_ERROR(_stream->read_at_fully(_offset, buffer, size));
    _offset += size;
    return Status::OK();
}

Status PageReader::skip_bytes(size_t size) {
    if (_offset + size > _next_header_pos) {
        return Status::InternalError("Size to skip exceed page size");
    }
    _offset += size;
    _stream->skip(size);
    return Status::OK();
}

StatusOr<std::string_view> PageReader::peek(size_t size) {
    if (_offset + size > _next_header_pos) {
        return Status::InternalError("Size to read exceed page size");
    }
    _stream->seek(_offset);
    ASSIGN_OR_RETURN(auto ret, _stream->peek(size));
    return ret;
}

} // namespace starrocks::parquet
