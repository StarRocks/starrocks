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
#include "storage/binlog_util.h"
#include "storage/rowset/page_io.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/raw_container.h"

namespace starrocks {

BinlogFileReader::BinlogFileReader(std::string file_name, std::shared_ptr<BinlogFileMetaPB> file_meta)
        : _file_path(std::move(file_name)),
          _file_meta(std::move(file_meta)),
          _file_size(0),
          _current_file_pos(0),
          _next_page_index(0) {}

Status BinlogFileReader::seek(int64_t version, int64_t seq_id) {
    VLOG(3) << "Seek binlog file reader: " << _file_path << ", version: " << version << ", seq_id: " << seq_id;
    std::shared_ptr<FileSystem> fs;
    ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(_file_path))
    ASSIGN_OR_RETURN(_file, fs->new_random_access_file(_file_path))
    ASSIGN_OR_RETURN(_file_size, _file->get_size())
    _file_header = std::make_unique<BinlogFileHeaderPB>();
    Status status = parse_file_header(_file.get(), _file_size, _file_header.get(), &_current_file_pos);
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
    do {
        _reset_current_page_context();
        PageHeaderPB& page_header_pb = _current_page_context->page_header;
        RETURN_IF_ERROR(_read_page_header(_next_page_index, &page_header_pb));
        if (page_header_pb.version() > version) {
            return Status::NotFound(strings::Substitute("Can't find version $0", version));
        }

        if (page_header_pb.version() == version) {
            if (seq_id < page_header_pb.start_seq_id()) {
                return Status::NotFound(strings::Substitute("Can't find version $0, seq_id $1", version, seq_id));
            }

            int64_t end_seq_id = page_header_pb.end_seq_id();
            // end_seq_id == -1 is a special case for an empty ingestion which will
            // generate a page with an EMPTY log entry, and only allow to seek with
            // <version, 0> to get the log entry
            if (end_seq_id == -1 || seq_id <= end_seq_id) {
                DCHECK((end_seq_id != -1) || (seq_id == 0));
                RETURN_IF_ERROR(
                        _read_page_content(_next_page_index, &page_header_pb, &(_current_page_context->page_content)));
                break;
            }
        }

        // skip to read page content
        _current_file_pos += page_header_pb.compressed_size();
        _next_page_index++;
        // reach the end of the file
        if (_next_page_index >= _file_meta->num_pages()) {
            return Status::NotFound(
                    strings::Substitute("Can't find version $0, changelog_id $1 "
                                        "in file $2, largest version $3, seq_id $4, and changelog_id $5",
                                        version, seq_id, _file_path, _file_meta->end_version(),
                                        _file_meta->end_seq_id(), _file_meta->end_seq_id()));
        }
    } while (true);
    _next_page_index++;
    _init_current_log_entry();
    return Status::OK();
}

Status BinlogFileReader::_read_next_page() {
    if (_next_page_index >= _file_meta->num_pages()) {
        return Status::EndOfFile("There is no more pages");
    }

    _reset_current_page_context();
    PageHeaderPB& page_header_pb = _current_page_context->page_header;
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
        int64_t end_seq_id = _current_log_entry->end_seq_id;
        // end_seq_id == -1 is for an empty ingestion with only one EMPTY log entry
        if (end_seq_id == -1 || end_seq_id >= seq_id) {
            return Status::OK();
        }
    }

    return Status::NotFound(strings::Substitute("Can't find log entry containing the change event <$0, $1>",
                                                _current_page_context->page_header.version(), seq_id));
}

void BinlogFileReader::_advance_log_entry() {
    PageContext* page_context = _current_page_context.get();
    LogEntryInfo* log_entry_info = _current_log_entry.get();

    int next_log_entry_index = page_context->next_log_entry_index;
    LogEntryPB* next_log_entry = page_context->page_content.mutable_entries(next_log_entry_index);
    int64_t next_start_seq_id = log_entry_info->end_seq_id + 1;
    int num_change_events = 0;
    // assume next log entry share file id with the current first
    FileIdPB* next_file_id = log_entry_info->file_id;
    // assume rows in next log entry are continuous with current log entry first
    int next_start_row_id = log_entry_info->start_row_id + log_entry_info->num_rows;
    int num_rows = 0;
    switch (next_log_entry->entry_type()) {
    case INSERT_RANGE_PB: {
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
    case UPDATE_PB: {
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
    case DELETE_PB:
        num_change_events = 1;
        break;
    case EMPTY_PB:
    default:
        break;
    }
    log_entry_info->log_entry = next_log_entry;
    log_entry_info->start_seq_id = next_start_seq_id;
    log_entry_info->end_seq_id = next_start_seq_id + num_change_events - 1;
    log_entry_info->file_id = next_file_id;
    log_entry_info->start_row_id = next_start_row_id;
    log_entry_info->num_rows = num_rows;
    log_entry_info->end_of_version = _current_page_context->page_header.end_of_version() &&
                                     (next_log_entry_index == page_context->page_header.num_log_entries() - 1);
    log_entry_info->timestamp_in_us = _current_page_context->page_header.timestamp_in_us();
    page_context->next_log_entry_index += 1;
}

Status BinlogFileReader::_read_page_header(int page_index, PageHeaderPB* page_header_pb) {
    int64_t read_file_size;
    RETURN_IF_ERROR(
            parse_page_header(_file.get(), _file_size, _current_file_pos, page_index, page_header_pb, &read_file_size));
    _current_file_pos += read_file_size;
    return Status::OK();
}

Status BinlogFileReader::_read_page_content(int page_index, PageHeaderPB* page_header_pb,
                                            PageContentPB* page_content_pb) {
    int32_t compressed_size = page_header_pb->compressed_size();
    int32_t uncompressed_size = page_header_pb->uncompressed_size();

    // TODO Refer to PageIO::read_and_decompress_page, but why allocate APPEND_OVERFLOW_MAX_SIZE
    // hold compressed page at first, reset to decompressed page later
    // Allocate APPEND_OVERFLOW_MAX_SIZE more bytes to make append_strings_overflow work
    std::unique_ptr<char[]> page(new char[compressed_size + Column::APPEND_OVERFLOW_MAX_SIZE]);
    Slice page_slice(page.get(), compressed_size);
    RETURN_IF_ERROR(_file->read_at_fully(_current_file_pos, page_slice.data, page_slice.get_size()));
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
        std::unique_ptr<char[]> decompressed_page(new char[uncompressed_size + Column::APPEND_OVERFLOW_MAX_SIZE]);
        Slice decompress_page_slice(decompressed_page.get(), uncompressed_size);
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
        return Status::Corruption(strings::Substitute(
                "Bad binlog file page $0: failed to parse page content pb, page index $1", _file_path, page_index));
    }

    return Status::OK();
}

Status BinlogFileReader::parse_file_header(RandomAccessFile* read_file, int64_t file_size,
                                           BinlogFileHeaderPB* file_header, int64_t* read_file_size) {
    // Header base size: magic_number + header_pb_size(uint32_t) + header_pb_checksum(uint32_t)
    size_t header_base_size = k_binlog_magic_number_length + 8;
    if (file_size < header_base_size) {
        return Status::Corruption(
                fmt::format("Bad binlog file header, file size {} < {}", file_size, header_base_size));
    }

    // TODO use header hint size
    size_t header_buffer_size = std::min<size_t>(file_size, 512);
    std::string header_buff;
    raw::stl_string_resize_uninitialized(&header_buff, header_buffer_size);
    RETURN_IF_ERROR(read_file->read_at_fully(0, header_buff.data(), header_buff.size()));

    // validate magic number
    if (memcmp(header_buff.data(), k_binlog_magic_number, k_binlog_magic_number_length) != 0) {
        return Status::Corruption(fmt::format("Bad binlog file header, magic number not match"));
    }

    const uint32_t header_pb_size = UNALIGNED_LOAD32(header_buff.data() + k_binlog_magic_number_length);
    const uint32_t checksum = UNALIGNED_LOAD32(header_buff.data() + k_binlog_magic_number_length + 4);

    size_t header_size = header_base_size + header_pb_size;
    if (file_size < header_size) {
        return Status::Corruption(fmt::format("Bad binlog file header, file size {} < {}", file_size, header_size));
    }

    std::string_view header_pb_buffer;
    if (header_size > header_buffer_size) {
        // TODO use the left data in the header_buffer to reduce IO
        header_buff.resize(header_pb_size);
        RETURN_IF_ERROR(read_file->read_at_fully(header_base_size, header_buff.data(), header_buff.size()));
        header_pb_buffer = std::string_view(header_buff.data(), header_buff.size());
    } else {
        header_pb_buffer = std::string_view(header_buff.data() + header_base_size, header_pb_size);
    }
    uint32_t actual_checksum = crc32c::Value(header_pb_buffer.data(), header_pb_buffer.size());
    if (actual_checksum != checksum) {
        return Status::Corruption(
                fmt::format("Bad binlog file header, header checksum not match, actual={} vs expect={}",
                            actual_checksum, checksum));
    }

    file_header->Clear();
    if (!file_header->ParseFromArray(header_pb_buffer.data(), header_pb_buffer.size())) {
        return Status::Corruption(fmt::format("Bad binlog file header, failed to parse file header pb "));
    }

    if (k_binlog_format_version != file_header->format_version()) {
        return Status::Corruption(fmt::format("Bad binlog file header: unknown format version {}, current version {}",
                                              file_header->format_version(), k_binlog_format_version));
    }
    *read_file_size = header_size;

    return Status::OK();
}

Status BinlogFileReader::parse_file_footer(RandomAccessFile* read_file, int64_t file_size,
                                           BinlogFileMetaPB* file_meta) {
    if (file_size < 8) {
        return Status::Corruption(fmt::format("Bad binlog file footer, file size {} less than 8", file_size));
    }

    // TODO use footer hint size
    size_t footer_read_size = 512;
    std::string buff;
    raw::stl_string_resize_uninitialized(&buff, footer_read_size);
    size_t read_pos = file_size - buff.size();
    RETURN_IF_ERROR(read_file->read_at_fully(read_pos, buff.data(), buff.size()));

    const uint32_t footer_pb_length = UNALIGNED_LOAD32(buff.data() + buff.size() - 8);
    const uint32_t checksum = UNALIGNED_LOAD32(buff.data() + buff.size() - 4);

    if (file_size < 8 + footer_pb_length) {
        return Status::Corruption(
                fmt::format("Bad binlog file footer, file size {} less than {}", file_size, 8 + footer_pb_length));
    }

    // remove the footer_pb_length and checksum
    buff.resize(buff.size() - 8);
    std::string_view footer_pb_buffer;
    if (footer_pb_length <= buff.size()) {
        uint32_t offset = buff.size() - footer_pb_length;
        footer_pb_buffer = std::string_view(buff.data() + offset, footer_pb_length);
    } else {
        // TODO use the left data in the buff to reduce IO
        buff.resize(footer_pb_length);
        RETURN_IF_ERROR(read_file->read_at_fully(file_size - footer_pb_length - 8, buff.data(), buff.size()));
        footer_pb_buffer = std::string_view(buff.data(), buff.size());
    }
    uint32_t actual_checksum = crc32c::Value(footer_pb_buffer.data(), footer_pb_buffer.size());
    if (actual_checksum != checksum) {
        return Status::Corruption(fmt::format("Bad binlog file footer, checksum not match, actual={} vs expect={}",
                                              actual_checksum, checksum));
    }

    file_meta->Clear();
    if (!file_meta->ParseFromArray(footer_pb_buffer.data(), footer_pb_buffer.size())) {
        return Status::Corruption("Bad binlog file footer: failed to parse footer");
    }

    return Status::OK();
}

Status BinlogFileReader::parse_page_header(RandomAccessFile* read_file, int64_t file_size, int64_t file_pos,
                                           int64_t page_index, PageHeaderPB* page_header_pb, int64_t* read_file_size) {
    int64_t current_file_pos = file_pos;
    // Page Header: page_header_pb_size(int32_t) + checksum(int32_t)
    size_t header_base_size = 8;
    if (file_size < file_pos + header_base_size) {
        return Status::Corruption(fmt::format("Bad binlog file page header, file size {} < {}, page index {}",
                                              file_size, current_file_pos + header_base_size, page_index));
    }

    uint8_t fixed_buf[8];
    // TODO use global buffer to optimize sequentially read
    RETURN_IF_ERROR(read_file->read_at_fully(current_file_pos, fixed_buf, 8));
    uint32_t page_header_pb_size = decode_fixed32_le(fixed_buf);
    uint32_t page_header_pb_checksum = decode_fixed32_le(fixed_buf + 4);
    current_file_pos += 8;

    if (file_size < current_file_pos + page_header_pb_size) {
        return Status::Corruption(fmt::format("Bad binlog file page header pb, file size {} < {}, page index {}",
                                              file_size, current_file_pos + page_header_pb_size, page_index));
    }

    std::string page_header_pb_buffer;
    raw::stl_string_resize_uninitialized(&page_header_pb_buffer, page_header_pb_size);
    RETURN_IF_ERROR(
            read_file->read_at_fully(current_file_pos, page_header_pb_buffer.data(), page_header_pb_buffer.size()));
    current_file_pos += page_header_pb_size;

    uint32_t actual_checksum = crc32c::Value(page_header_pb_buffer.data(), page_header_pb_buffer.size());
    if (actual_checksum != page_header_pb_checksum) {
        return Status::Corruption(
                fmt::format("Bad binlog file page header, checksum not match, actual={} vs expect={}, page index {}",
                            actual_checksum, page_header_pb_checksum, page_index));
    }

    page_header_pb->Clear();
    if (!page_header_pb->ParseFromString(page_header_pb_buffer)) {
        return Status::Corruption(
                fmt::format("Bad binlog file page header, failed to parse pb, page index {}", page_index));
    }
    *read_file_size = current_file_pos - file_pos;

    VLOG(3) << "Read page header, file path: " << read_file->filename() << ", file pos: " << file_pos
            << ", page header size: " << page_header_pb_size
            << ", page header: " << BinlogUtil::page_header_to_string(page_header_pb);

    return Status::OK();
}

StatusOr<BinlogFileMetaPBPtr> BinlogFileReader::load_meta_by_scan_pages(int64_t file_id, RandomAccessFile* read_file,
                                                                        int64_t file_size,
                                                                        BinlogLsn& max_lsn_exclusive) {
    BinlogFileMetaPBPtr file_meta = std::make_shared<BinlogFileMetaPB>();
    file_meta->set_id(file_id);
    file_meta->set_num_pages(0);
    Status status;
    std::unique_ptr<BinlogFileHeaderPB> file_header = std::make_unique<BinlogFileHeaderPB>();
    int64_t read_file_size;
    status = parse_file_header(read_file, file_size, file_header.get(), &read_file_size);
    if (!status.ok()) {
        VLOG(3) << "Failed to parse binlog file header when loading meta, file path: " << read_file->filename() << ", "
                << status;
        return Status::NotFound("Failed to parse file header, file path: " + read_file->filename());
    }

    std::unordered_set<int64_t> rowsets;
    int64_t file_pos = read_file_size;
    int64_t num_pages = 0;
    std::unique_ptr<PageHeaderPB> page_header = std::make_unique<PageHeaderPB>();
    while (true) {
        status = parse_page_header(read_file, file_size, file_pos, num_pages, page_header.get(), &read_file_size);
        if (!status.ok()) {
            VLOG(3) << "Stop to scan pages because of parsing page header failure, file path: " << read_file->filename()
                    << ", file_pos: " << file_pos << ", page_index: " << num_pages << ", " << status;
            break;
        }

        // for empty version, use seq_id 0 for max lsn
        int64_t end_seq_id = page_header->end_seq_id() == -1 ? 0 : page_header->end_seq_id();
        BinlogLsn pageMaxLsn(page_header->version(), end_seq_id);
        if (!(pageMaxLsn < max_lsn_exclusive)) {
            VLOG(3) << "Stop to scan pages because page max lsn exceeds, file path: " << read_file->filename()
                    << ", file_pos: " << file_pos << ", page_index: " << num_pages
                    << ", max_lsn_exclusive: " << max_lsn_exclusive << ", pageMaxLsn" << pageMaxLsn;
            break;
        }

        if (num_pages == 0) {
            file_meta->set_start_version(page_header->version());
            file_meta->set_start_seq_id(page_header->start_seq_id());
            file_meta->set_start_timestamp_in_us(page_header->timestamp_in_us());
        }

        file_meta->set_end_version(page_header->version());
        file_meta->set_end_seq_id(page_header->end_seq_id());
        file_meta->set_end_timestamp_in_us(page_header->timestamp_in_us());
        file_meta->set_version_eof(page_header->end_of_version());

        for (auto rowset_id : page_header->rowsets()) {
            rowsets.insert(rowset_id);
        }

        num_pages += 1;
        file_pos += read_file_size + page_header->compressed_size();
    }

    if (num_pages == 0) {
        VLOG(3) << "There is no valid data found, file path: " << read_file->filename()
                << ", max_lsn_exclusive: " << max_lsn_exclusive;
        return Status::NotFound("There is no valid data in binlog file, path: " + read_file->filename());
    }

    file_meta->set_num_pages(num_pages);
    file_meta->set_file_size(file_pos);
    for (auto rid : rowsets) {
        file_meta->add_rowsets(rid);
    }

    VLOG(3) << "Load binlog file meta from scanning pages, file path: " << read_file->filename()
            << ", meta: " << BinlogUtil::file_meta_to_string(file_meta.get());

    return file_meta;
}

StatusOr<BinlogFileMetaPBPtr> BinlogFileReader::load_meta(int64_t file_id, std::string& file_path,
                                                          BinlogLsn& max_lsn_exclusive) {
    std::shared_ptr<FileSystem> fs;
    ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(file_path))
    ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file(file_path))
    ASSIGN_OR_RETURN(auto file_size, read_file->get_size())

    std::shared_ptr<BinlogFileMetaPB> file_meta = std::make_shared<BinlogFileMetaPB>();
    Status st = parse_file_footer(read_file.get(), file_size, file_meta.get());
    if (!st.ok()) {
        VLOG(3) << "Failed to parse binlog file footer, file path: " << file_path << ", " << st;
    } else {
        BinlogLsn fileMaxLsn(file_meta->end_version(), file_meta->end_seq_id());
        if (fileMaxLsn < max_lsn_exclusive) {
            VLOG(3) << "Load binlog file meta from footer, file path: " << file_path
                    << ", meta: " << BinlogUtil::file_meta_to_string(file_meta.get());
            return file_meta;
        }
        VLOG(3) << "Binlog file footer is not valid, file path: " << file_path
                << ", max_lsn_exclusive: " << max_lsn_exclusive << ", fileMaxLsn: " << fileMaxLsn;
    }

    return load_meta_by_scan_pages(file_id, read_file.get(), file_size, max_lsn_exclusive);
}

} // namespace starrocks
