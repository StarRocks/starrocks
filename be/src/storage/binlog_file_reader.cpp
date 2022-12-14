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

#include "storage/binlog_file_reader.h"

#include "fs/fs.h"
#include "storage/binlog_file_writer.h"
#include "storage/rowset/page_io.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/raw_container.h"

namespace starrocks {

BinlogFileReader::BinlogFileReader(std::string file_name, std::shared_ptr<BinlogFileMetaPB> file_meta)
        : _file_path(std::move(file_name)), _file_meta(file_meta) {}

Status BinlogFileReader::seek(int64_t version, int64_t seq_id) {
    std::shared_ptr<FileSystem> fs;
    ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(_file_path))
    ASSIGN_OR_RETURN(_file, fs->new_random_access_file(_file_path))
    ASSIGN_OR_RETURN(_file_size, _file->get_size())
    Status status = _read_file_header();
    if (!status.ok()) {
        LOG(WARNING) << "Fail to parse file header " << _file_path << ", " << status;
        return Status::InternalError("Fail to parse file header " + _file_path);
    }
    _next_page_index = 0;
    _current_page_context = std::make_unique<PageContext>();
    _current_log_entry = std::make_unique<LogEntryInfo>();

    return _seek(version, seq_id);
}

Status BinlogFileReader::next() {
    if (_is_end_of_file()) {
        return Status::EndOfFile("There is no left entries");
    }

    PageContext* page_context = _current_page_context.get();
    if (page_context->next_log_entry_index >= page_context->page_header.num_log_entries()) {
        // end of current page and read the next
        RETURN_IF_ERROR(_read_next_page());
    }
    _advance_log_entry();
    return Status::OK();
}

LogEntryInfo* BinlogFileReader::log_entry() {
   return _current_log_entry.get();
}

Status BinlogFileReader::_seek(int64_t version, int64_t seq_id) {
    RETURN_IF_ERROR(_seek_to_page(version, seq_id));
    RETURN_IF_ERROR(_seek_to_log_entry(seq_id));
    return Status::OK();
}

Status BinlogFileReader::_seek_to_page(int64_t version, int64_t seq_id) {
    _reset_current_page_context();
    do {
        _reset_current_page_context();
        PageHeaderPB& page_header_pb = _current_page_context->page_header;
        RETURN_IF_ERROR(_read_page_header(_next_page_index, &page_header_pb));
        if (page_header_pb.version() > version) {
            return Status::NotFound(strings::Substitute("Can't find version $0", version));
        }

        // find the page containing the target change event
        if (page_header_pb.version() == version && page_header_pb.end_seq_id() >= seq_id) {
            RETURN_IF_ERROR(_read_page_content(_next_page_index, &page_header_pb, &(_current_page_context->page_content)));
            break;
        }

        // reach the last page of the file
        if (_next_page_index == _file_meta->num_pages()) {
            return Status::NotFound(strings::Substitute("Can't find version $0, changelog_id $1 "
                                    "in file $2, largest version $3, seq_id $4, and changelog_id $5",
                                    version, seq_id, _file_path, _file_meta->end_version(),
                                    _file_meta->end_seq_id(), _file_meta->end_seq_id()));
        }

        // skip to read page content
        _current_file_pos += page_header_pb.compressed_size();
        _next_page_index++;
    } while (true);
    _init_current_log_entry();
    return Status::OK();
}

Status BinlogFileReader::_read_next_page() {
    _reset_current_page_context();
    RETURN_IF_ERROR(_read_page_header(_next_page_index, &page_header_pb));
    RETURN_IF_ERROR(_read_page_content(_next_page_index, &page_header_pb, &(_current_page_context->page_content)));
    _next_page_index++;
    _init_current_log_entry();
    return Status::OK();
}

Status BinlogFileReader::_seek_to_log_entry(int64_t seq_id) {
    PageContentPB& page_content = _current_page_context->page_content;
    int32_t num_log_entries = page_content.entries_size();
    while (_current_page_context->next_log_entry_index < num_log_entries) {
        _advance_log_entry();
        if (_current_log_entry->end_seq_id >= seq_id) {
            return Status::OK();
        }
    }

    return Status::NotFound(strings::Substitute("Can't find log entry containing the change event <$0, $1>",
                        _current_page_context->page_header.version(), seq_id));
}

void BinlogFileReader::_advance_log_entry() {
    int next_log_entry_index = _current_page_context->next_log_entry_index;
    LogEntryPB* next_log_entry = _current_page_context->page_content.mutable_entries(next_log_entry_index);
    int64_t next_start_seq_id =  _current_log_entry->end_seq_id + 1;
    int num_change_events = 0;
    // assume next log entry share file id with the current first
    FileIdPB* next_file_id = _current_log_entry->file_id;
    // assume rows in next log entry are continuous with current log entry first
    int next_start_row_id = _current_log_entry->start_row_id + _current_log_entry->num_rows;
    int num_rows = 0;
    switch (next_log_entry->entry_type()) {
        case INSERT_RANGE: {
            InsertRangePB* entry_data = next_log_entry->mutable_insert_range_data();
            num_rows = entry_data->num_rows();
            num_change_events = num_rows;
            if (entry_data->has_file_id()) {
                next_file_id = entry_data->mutable_file_id();
            }
            if (entry_data->has_start_row_id()) {
                next_start_row_id = entry_data->start_row_id();
            }
            break;
        }
        case UPDATE: {
            UpdatePB* entry_data = next_log_entry->mutable_update_data();
            num_rows = 1;
            num_change_events = 2;
            if (entry_data->has_after_file_id()) {
                next_file_id = entry_data->mutable_after_file_id();
            }
            if (entry_data->has_after_row_id()) {
                next_start_row_id = entry_data->after_row_id();
            }
            break;
        }
        case DELETE:
            num_change_events = 1;
            break;
        case EMPTY:
        default:
            break;
    }
    _current_log_entry->log_entry = next_log_entry;
    _current_log_entry->start_seq_id = next_start_seq_id;
    _current_log_entry->end_seq_id = next_start_seq_id + num_change_events - 1;
    _current_log_entry->file_id = next_file_id;
    _current_log_entry->start_row_id = next_start_row_id;
    _current_log_entry->num_rows = num_rows;
    _current_log_entry->end_of_version = _current_page_context->page_header.end_of_version()
                && (next_log_entry_index + 1 == _current_page_context->page_header.num_log_entries());
    _current_page_context->next_log_entry_index += 1;
}

Status BinlogFileReader::_read_file_header() {
    // Header: magic_number + header_pb_size(uint32_t) + header_pb_checksum(uint32_t) + header_pb
    size_t header_base_size = k_binlog_magic_number_length + 8;
    if (_file_size < header_base_size) {
        return Status::Corruption(
                strings::Substitute("Bad binlog file $0: file size $1 < $2", _file_path, _file_size, header_base_size));
    }

    // currently header is small
    size_t header_buffer_size = std::min<size_t>(_file_size, 512);
    std::string header_buff;
    raw::stl_string_resize_uninitialized(&header_buff, header_buffer_size);
    RETURN_IF_ERROR(_file->read_at_fully(0, header_buff.data(), header_buff.size()));

    // validate magic number
    if (memcmp(header_buff.data(), k_binlog_magic_number, k_binlog_magic_number_length) != 0) {
        return Status::Corruption(strings::Substitute("Bad binlog file $0: magic number not match", _file_path));
    }

    // TODO why not use decode_fixed32_le
    const uint32_t header_pb_size = UNALIGNED_LOAD32(header_buff.data() + k_binlog_magic_number_length);
    const uint32_t checksum = UNALIGNED_LOAD32(header_buff.data() + k_binlog_magic_number_length + 4);

    size_t header_size = header_base_size + header_pb_size;
    if (_file_size < header_size) {
        return Status::Corruption(
                strings::Substitute("Bad binlog file $0: file size $1 < $2", _file_path, _file_size, header_size));
    }

    std::string_view header_pb_buffer;
    _file_header = std::make_unique<BinlogFileHeaderPB>();
    if (header_size > header_buffer_size) {
        // TODO use the left data in the header_buffer to reduce bytes to read
        header_buff.resize(header_pb_size);
        RETURN_IF_ERROR(_file->read_at_fully(header_base_size, header_buff.data(), header_buff.size()));
        header_pb_buffer = std::string_view(header_buff.data(), header_buff.size());
    } else {
        header_pb_buffer = std::string_view(header_buff.data() + header_base_size, header_pb_size);
    }
    uint32_t actual_checksum = crc32c::Value(header_pb_buffer.data(), header_pb_buffer.size());
    if (actual_checksum != checksum) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: header checksum not match, actual=$1 vs expect=$2",
                                    _file_path, actual_checksum, checksum));
    }

    if (!_file_header->ParseFromArray(header_pb_buffer.data(), header_pb_buffer.size())) {
        return Status::Corruption(
                strings::Substitute("Bad binlog file $0: failed to parse file header pb ", _file_path));
    }

    if (k_binlog_format_version != _file_header->format_version()) {
        return Status::Corruption(
                strings::Substitute("Bad binlog file $0: unknown format version $1, current version $2 ", _file_path,
                                    _file_header->format_version(), k_binlog_format_version));
    }
    _current_file_pos = header_size;

    return Status::OK();
}

Status BinlogFileReader::_read_page_header(int page_index, PageHeaderPB* page_header_pb) {
    // Page Header: page_header_pb_size(int32_t) + checksum(int32_t)
    size_t header_base_size = 8;
    if (_file_size < _current_file_pos + header_base_size) {
        return Status::Corruption(
                strings::Substitute("Bad binlog file page header $0: file size $1 < $2, page index $3", _file_path,
                                    _file_size, _current_file_pos + header_base_size, page_index));
    }

    uint8_t fixed_buf[8];
    // TODO use global buffer to optimize sequentially read
    RETURN_IF_ERROR(_file->read_at_fully(_current_file_pos, fixed_buf, 8));
    uint32_t page_header_pb_size = decode_fixed32_le(fixed_buf);
    uint32_t page_header_pb_checksum = decode_fixed32_le(fixed_buf + 4);
    _current_file_pos += 8;

    if (_file_size < _current_file_pos + page_header_pb_size) {
        int64_t expect_size = _current_file_pos + page_header_pb_size;
        return Status::Corruption(strings::Substitute("Bad binlog file page $0: file size $1 < $2, page index $3",
                                                      _file_path, _file_size, expect_size, page_index));
    }

    std::string page_header_pb_buffer;
    raw::stl_string_resize_uninitialized(&page_header_pb_buffer, page_header_pb_size);
    RETURN_IF_ERROR(
            _file->read_at_fully(_current_file_pos, page_header_pb_buffer.data(), page_header_pb_buffer.size()));
    _current_file_pos += page_header_pb_size;

    uint32_t actual_checksum = crc32c::Value(page_header_pb_buffer.data(), page_header_pb_buffer.size());
    if (actual_checksum != page_header_pb_checksum) {
        return Status::Corruption(strings::Substitute(
                "Bad binlog file $0: page header checksum not match, actual=$1 vs expect=$2, page index $3",
                    _file_path, actual_checksum, page_header_pb_checksum, page_index));
    }

    page_header_pb->Clear();
    if (!page_header_pb->ParseFromString(page_header_pb_buffer)) {
        return Status::Corruption(
                strings::Substitute("Bad binlog file page $0: failed to parse page header pb, page index $1",
                                    _file_path, page_index));
    }

    return Status::OK();
}

Status BinlogFileReader::_read_page_content(int page_index, PageHeaderPB* page_header_pb, PageContentPB* page_content_pb) {
    int32_t compressed_size = page_header_pb->compressed_size();
    int32_t uncompressed_size = page_header_pb->uncompressed_size();

    // TODO Refer to PageIO::read_and_decompress_page, but why allocate APPEND_OVERFLOW_MAX_SIZE
    // hold compressed page at first, reset to decompressed page later
    // Allocate APPEND_OVERFLOW_MAX_SIZE more bytes to make append_strings_overflow work
    std::unique_ptr<char[]> page(new char[compressed_size + vectorized::Column::APPEND_OVERFLOW_MAX_SIZE]);
    Slice page_slice(page.get(), compressed_size);
    RETURN_IF_ERROR(_file->read_at_fully(_current_file_pos, page_slice.get_data(), page_slice.get_size()));
    _current_file_pos += page_slice.get_size();

    uint32_t actual_checksum = crc32c::Value(page_slice.get_data(), page_slice.get_size());
    if (page_header_pb->compressed_page_crc() != actual_checksum) {
        return Status::Corruption(strings::Substitute(
                "Bad binlog file $0: page content checksum not match, actual=$1 vs expect=$2, page index $3",
                _file_path, actual_checksum, page_header_pb->compressed_page_crc(), page_index));
    }

    CompressionTypePB compress_type = page_header_pb->compress_type();
    if (compress_type != NO_COMPRESSION) {
        const BlockCompressionCodec* compress_codec;
        RETURN_IF_ERROR(get_block_compression_codec(compress_type, &compress_codec));
        std::unique_ptr<char[]> decompressed_page(
                new char[uncompressed_size + vectorized::Column::APPEND_OVERFLOW_MAX_SIZE]);
        Slice decompress_page_slice(page.get(), uncompressed_size);
        RETURN_IF_ERROR(compress_codec->decompress(page_slice, &decompress_page_slice));
        if (decompress_page_slice.get_size() != uncompressed_size) {
            return Status::Corruption(strings::Substitute(
                    "Bad binlog file $0: page content decompress failed, expect uncompressed size=$1"
                    " vs real decompressed size=$2, page index $3",
                    _file_path, uncompressed_size, decompress_page_slice.get_size(), page_index));
        }
        page = std::move(decompressed_page);
        page_slice = Slice(page.get(), uncompressed_size);
    }

    page_content_pb->Clear();
    if (!page_content_pb->ParseFromArray(page_slice.get_data(), page_slice.get_size())) {
        return Status::Corruption(
                strings::Substitute("Bad binlog file page $0: failed to parse page content pb, page index $1",
                                    _file_path, page_index));
    }

    return Status::OK();
}

} // namespace starrocks
