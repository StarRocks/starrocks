// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/binlog_file_reader.h"

#include "fs/fs.h"
#include "storage/binlog_file_writer.h"
#include "storage/rowset/page_io.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/raw_container.h"

namespace starrocks {

BinlogFileReader::BinlogFileReader(std::string file_name, std::shared_ptr<BinlogFileMetaPB> file_meta)
        : _file_name(std::move(file_name)), _file_meta(file_meta) {}

Status BinlogFileReader::seek(int64_t version, int64_t changelog_id) {
    std::shared_ptr<FileSystem> fs;
    ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(_file_name))
    ASSIGN_OR_RETURN(_file, fs->new_random_access_file(_file_name));
    Status status = _parse_file_header();
    if (!status.ok()) {
        LOG(WARNING) << "Fail to parse file header " << _file_name << ", " << status;
        return Status::InternalError("Fail to parse file header " + _file_name);
    }
    _current_log_entry_info = std::make_unique<LogEntryInfo>();

    return _seek(version, changelog_id);
}

Status BinlogFileReader::next() {
    if (_is_last_page && _log_entry_index + 1 >= _current_page_content->entries_size()) {
        return Status::EndOfFile("There is no left entries");
    }

    if (_log_entry_index + 1 >= _current_page_content->entries_size()) {
        RETURN_IF_ERROR(_read_page_header());
        RETURN_IF_ERROR(_read_page());
    }

    _advance_log_entry();
    return Status::OK();
}

LogEntryInfo* BinlogFileReader::log_entry() {
    LogEntryInfo* log_entry_info = _current_log_entry_info.get();
    log_entry_info->log_entry = _current_page_content->mutable_entries(_log_entry_index);
    log_entry_info->version = _current_page_header->tablet_version();
    log_entry_info->start_changelog_id = _log_entry_start_changelog_id;
    log_entry_info->end_changelog_id = _log_entry_start_changelog_id + _log_entry_num_changelogs - 1;
    log_entry_info->file_id = _log_entry_file_id;
    log_entry_info->start_row_id = _log_entry_start_row_id;
    log_entry_info->last_log_entry_in_version =
            _current_page_header->end_page() && _log_entry_index + 1 == _current_page_content->entries_size();
    return log_entry_info;
}

Status BinlogFileReader::_seek_to_page(int64_t version, int64_t changelog_id) {
    _current_page_index = 0;
    _current_page_header = std::make_unique<PageHeaderPB>();
    _current_page_content = std::make_unique<PageContentPB>();
    do {
        RETURN_IF_ERROR(_read_page_header());
        if (_current_page_header->tablet_version() > version) {
            return Status::NotFound(strings::Substitute("Can't find version $0", version));
        }

        // find the page contains the target changelog
        if (_current_page_header->tablet_version() == version &&
            _current_page_header->end_change_log_id() >= changelog_id) {
            RETURN_IF_ERROR(_read_page());
            break;
        }

        // reach the last page of the file
        if (_is_last_page) {
            return Status::NotFound(
                    strings::Substitute("Can't find version $0, changelog_id $1 "
                                        "in file $2, largest version $3, seq_id $4, and changelog_id $5",
                                        version, changelog_id, _file_name, _file_meta->end_tablet_version(),
                                        _file_meta->end_seq_id(), _file_meta->end_changelog_id()));
        }

        // skip to read page content
        _current_file_pos += _current_page_header->compressed_size();
    } while (true);
    return Status::OK();
}

Status BinlogFileReader::_seek_to_log_entry(int64_t changelog_id) {
    int32_t num_log_entries = _current_page_content->entries_size();
    while (_log_entry_index < num_log_entries) {
        _advance_log_entry();
        int32_t next_changelog_id = _log_entry_start_changelog_id + _log_entry_num_changelogs;
        if (changelog_id < next_changelog_id) {
            return Status::OK();
        }
        _log_entry_index++;
    }

    return Status::NotFound("Can't find log entry containing changelog " + changelog_id);
}

void BinlogFileReader::_advance_log_entry() {
    _log_entry_index++;
    _log_entry_start_changelog_id += _log_entry_num_changelogs;
    _log_entry_start_row_id += _log_entry_num_rows;

    LogEntryPB* next_log_entry = _current_page_content->mutable_entries(_log_entry_index);
    switch (next_log_entry->entry_type()) {
    case INSERT_RANGE: {
        InsertRangeEntryDataPB* entry_data = next_log_entry->mutable_insert_range_entry_data();
        _log_entry_num_changelogs = entry_data->num_rows();
        if (entry_data->has_file_id()) {
            _log_entry_file_id = entry_data->mutable_file_id();
        }
        if (entry_data->has_start_row_id()) {
            _log_entry_start_row_id = entry_data->start_row_id();
        }
        _log_entry_num_rows = entry_data->num_rows();
        break;
    }
    case UPDATE: {
        UpdateEntryDataPB* entry_data = next_log_entry->mutable_update_entry_data();
        _log_entry_num_changelogs = 2;
        if (entry_data->has_after_file_id()) {
            _log_entry_file_id = entry_data->mutable_after_file_id();
        }
        if (entry_data->has_after_row_id()) {
            _log_entry_start_row_id = entry_data->after_row_id();
        }
        _log_entry_num_rows = 1;
        break;
    }
    case DELETE:
        _log_entry_num_changelogs = 1;
        _log_entry_num_rows = 1;
        break;
    case EMPTY:
    default:
        break;
    }
}

Status BinlogFileReader::_seek(int64_t version, int64_t changelog_id) {
    RETURN_IF_ERROR(_seek_to_page(version, changelog_id));
    RETURN_IF_ERROR(_seek_to_log_entry(version, changelog_id));
    return Status::OK();
}

Status BinlogFileReader::_parse_file_header() {
    // Header: magic_number + header_pb_size(uint32_t) + header_pb_checksum(uint32_t) + header_pb
    size_t header_base_size = k_binlog_magic_number_length + 8;
    ASSIGN_OR_RETURN(_file_size, _file->get_size());
    if (_file_size < header_base_size) {
        return Status::Corruption(
                strings::Substitute("Bad binlog file $0: file size $1 < $2", _file_name, _file_size, header_base_size));
    }

    // currently header is small
    size_t header_buffer_size = std::min<size_t>(_file_size, 512);
    std::string header_buff;
    raw::stl_string_resize_uninitialized(&header_buff, header_buffer_size);
    RETURN_IF_ERROR(_file->read_at_fully(0, header_buff.data(), header_buff.size()));

    // validate magic number
    if (memcmp(header_buff.data(), k_binlog_magic_number, k_binlog_magic_number_length) != 0) {
        return Status::Corruption(strings::Substitute("Bad binlog file $0: magic number not match", _file_name));
    }

    // TODO why not use decode_fixed32_le
    const uint32_t header_pb_size = UNALIGNED_LOAD32(header_buff.data() + k_binlog_magic_number_length);
    const uint32_t checksum = UNALIGNED_LOAD32(header_buff.data() + k_binlog_magic_number_length + 4);

    size_t header_size = header_base_size + header_pb_size;
    if (_file_size < header_size) {
        return Status::Corruption(
                strings::Substitute("Bad binlog file $0: file size $1 < $2", _file_name, _file_size, header_size));
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
                                    _file_name, actual_checksum, checksum));
    }

    if (!_file_header->ParseFromArray(header_pb_buffer.data(), header_pb_buffer.size())) {
        return Status::Corruption(
                strings::Substitute("Bad binlog file $0: failed to parse file header pb ", _file_name));
    }

    if (k_binlog_format_version != _file_header->format_version()) {
        return Status::Corruption(
                strings::Substitute("Bad binlog file $0: unknown format version $1, current version $2 ", _file_name,
                                    _file_header->format_version(), k_binlog_format_version));
    }

    _current_file_pos = header_size;

    return Status::OK();
}

Status BinlogFileReader::_read_page_header() {
    _current_page_index += 1;
    // Page Header: page_header_pb_size(int32_t) + checksum(int32_t)
    size_t header_base_size = 8;
    if (_file_size < _current_file_pos + header_base_size) {
        return Status::Corruption(
                strings::Substitute("Bad binlog file page header $0: file size $1 < $2, page index $3", _file_name,
                                    _file_size, _current_file_pos + header_base_size, _current_page_index));
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
                                                      _file_name, _file_size, expect_size, _current_page_index));
    }

    std::string page_header_pb_buffer;
    raw::stl_string_resize_uninitialized(&page_header_pb_buffer, page_header_pb_size);
    RETURN_IF_ERROR(
            _file->read_at_fully(_current_file_pos, page_header_pb_buffer.data(), page_header_pb_buffer.size()));
    _current_file_pos += page_header_pb_size;

    uint32_t actual_checksum = crc32c::Value(page_header_pb_buffer.data(), page_header_pb_buffer.size());
    if (actual_checksum != page_header_pb_checksum) {
        return Status::Corruption(strings::Substitute(
                "Bad binlog file $0: page header checksum not match, actual=$1 vs expect=$2, page index $3", _file_name,
                actual_checksum, page_header_pb_checksum, _current_page_index));
    }

    _current_page_header->Clear();
    if (!_current_page_header->ParseFromString(page_header_pb_buffer)) {
        return Status::Corruption(
                strings::Substitute("Bad binlog file page $0: failed to parse page header pb, page index $1",
                                    _file_name, _current_page_index));
    }

    if (_current_page_header->tablet_version() == _file_meta->end_tablet_version() &&
        _current_page_header->end_change_log_id() == _file_meta->end_changelog_id()) {
        _is_last_page = true;
    }

    return Status::OK();
}

Status BinlogFileReader::_read_page() {
    int32_t compressed_size = _current_page_header->compressed_size();
    int32_t uncompressed_size = _current_page_header->uncompressed_size();

    // TODO Refer to PageIO::read_and_decompress_page, but why allocate APPEND_OVERFLOW_MAX_SIZE
    // hold compressed page at first, reset to decompressed page later
    // Allocate APPEND_OVERFLOW_MAX_SIZE more bytes to make append_strings_overflow work
    std::unique_ptr<char[]> page(new char[compressed_size + vectorized::Column::APPEND_OVERFLOW_MAX_SIZE]);
    Slice page_slice(page.get(), compressed_size);
    RETURN_IF_ERROR(_file->read_at_fully(_current_file_pos, page_slice.data, page_slice.size));

    uint32_t actual_checksum = crc32c::Value(page_slice.data, page_slice.size);
    if (_current_page_header->compressed_page_crc() != actual_checksum) {
        return Status::Corruption(strings::Substitute(
                "Bad binlog file $0: page content checksum not match, actual=$1 vs expect=$2, page index $3",
                _file_name, actual_checksum, _current_page_header->compressed_page_crc(), _current_page_index));
    }

    CompressionTypePB compress_type = _current_page_header->compress_type();
    if (compress_type != NO_COMPRESSION) {
        const BlockCompressionCodec* compress_codec;
        RETURN_IF_ERROR(get_block_compression_codec(compress_type, &compress_codec));
        std::unique_ptr<char[]> decompressed_page(
                new char[uncompressed_size + vectorized::Column::APPEND_OVERFLOW_MAX_SIZE]);
        Slice decompress_page_slice(page.get(), uncompressed_size);
        RETURN_IF_ERROR(compress_codec->decompress(page_slice, &decompress_page_slice));
        if (decompress_page_slice.size != uncompressed_size) {
            return Status::Corruption(strings::Substitute(
                    "Bad binlog file $0: page content decompress failed, expect uncompressed size=$1"
                    " vs real decompressed size=$2, page index $3",
                    _file_name, uncompressed_size, decompress_page_slice.size, _current_page_index));
        }
        page = std::move(decompressed_page);
        page_slice = Slice(page.get(), uncompressed_size);
    }

    _current_page_content->Clear();
    if (!_current_page_content->ParseFromArray(page_slice.data, page_slice.size)) {
        return Status::Corruption(
                strings::Substitute("Bad binlog file page $0: failed to parse page content pb, page index $1",
                                    _file_name, _current_page_index));
    }

    _log_entry_index = -1;
    _log_entry_start_changelog_id = _current_page_header->start_changelog_id();
    _log_entry_num_changelogs = 0;
    _log_entry_file_id = nullptr;
    _log_entry_start_row_id = -1;
    _log_entry_num_rows = 0;

    return Status::OK();
}

} // namespace starrocks
