// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/binlog_file_writer.h"

#include "storage/rowset/page_io.h"
#include "util/crc32c.h"
#include "util/filesystem_util.h"

namespace starrocks {

const char* const k_binlog_magic_number = "BINLOG";
const uint32_t k_binlog_magic_number_length = 6;
const int32_t k_binlog_format_version = 1;

BinlogFileWriter::BinlogFileWriter(int64_t file_id, std::string file_name, int32_t page_size,
                                   CompressionTypePB compression_type)
        : _file_id(file_id),
          _file_name(std::move(file_name)),
          _max_page_size(page_size),
          _compression_type(compression_type),
          _writer_state(WAIT_INIT) {}

Status BinlogFileWriter::init() {
    CHECK(_writer_state == WAIT_INIT);
    _writer_state = WAIT_WRITE;
    // 1. create file
    std::shared_ptr<FileSystem> fs;
    ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(_file_name))
    WritableFileOptions write_option;
    write_option.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    ASSIGN_OR_RETURN(_file, fs->new_writable_file(write_option, _file_name))

    // 2. write file header
    BinlogFileHeaderPB header;
    header.set_format_version(k_binlog_format_version);
    std::string header_pb_buf;
    if (!header.SerializeToString(&header_pb_buf)) {
        LOG(WARNING) << "Failed to serialize binlog file header, file id " << _file_id << ", file name " << _file_name;
        return Status::InternalError("Failed to serialize binlog file header");
    }

    faststring header_fixed_buf;
    // magic number
    header_fixed_buf.append(k_binlog_magic_number, k_binlog_magic_number_length);
    // header pb size
    put_fixed32_le(&header_fixed_buf, header_pb_buf.size());
    // header pb checksum
    uint32_t checksum = crc32c::Value(header_pb_buf.data(), header_pb_buf.size());
    put_fixed32_le(&header_fixed_buf, checksum);
    std::vector<Slice> slices{header_fixed_buf, header_pb_buf};
    // delay to sync file until there is data to write
    Status st = _file->appendv(&slices[0], slices.size());
    if (!st.ok()) {
        LOG(WARNING) << "Failed to write header, file id " << _file_id << ", file name " << _file_name << ", " << st;
        return st;
    }

    // 3. decide compression codec
    RETURN_IF_ERROR(get_block_compression_codec(_compression_type, &_compress_codec));

    // 4. init file meta and pending context
    _file_meta = std::make_unique<BinlogFileMetaPB>();
    _file_meta->set_id(_file_id);
    _file_meta->set_file_size(_file->size());
    _pending_write_context = std::make_unique<PendingWriteContext>();
    _pending_page_context = std::make_unique<PendingPageContext>();

    LOG(INFO) << "Init binlog file writer, file id " << _file_id << ", file name " << _file_name;
}

Status BinlogFileWriter::prepare(int64_t tablet_version, const RowsetId& rowset_id, int64_t start_seq_id,
                                 int64_t start_changelog_id, int64_t rowset_creation_time_in_sec) {
    CHECK(_writer_state == WAIT_WRITE);
    _writer_state = PREPARE;

    PendingWriteContext* write_context = _pending_write_context.get();
    write_context->tablet_version = tablet_version;
    write_context->rowset_id.init(rowset_id.to_string());
    write_context->start_seq_id = start_seq_id;
    write_context->start_changelog_id = start_changelog_id;
    write_context->rowset_creation_time_in_sec = rowset_creation_time_in_sec;
    write_context->rowsets.clear();
    write_context->rowsets.emplace(rowset_id);

    PendingPageContext* page_context = _pending_page_context.get();
    page_context->start_seq_id = start_seq_id;
    page_context->start_changelog_id = start_changelog_id;
    // start_seq_id > end_seq_id means there is no log entry
    page_context->end_seq_id = start_seq_id - 1;
    page_context->end_changelog_id = start_changelog_id - 1;
    page_context->num_log_entries = 0;
    page_context->last_segment_index = -1;
    page_context->last_row_id = -1;
    page_context->estimated_page_size = 0;
    page_context->page_header.Clear();
    page_context->page_content.Clear();
    page_context->rowsets.emplace(rowset_id);

    return Status::OK();
}

Status BinlogFileWriter::add_empty() {
    CHECK(_writer_state == PREPARE);
    CHECK(_pending_page_context->num_log_entries == 0)
            << "Empty rowset should only have one empty log entry,"
            << ", tablet version " << _pending_write_context->tablet_version << ", file id " << _file_id
            << ", file name " << _file_name << ", actual number of entries " << _pending_page_context->num_log_entries;

    LogEntryPB* log_entry = _pending_page_context->page_content.add_entries();
    log_entry->set_entry_type(EMPTY);

    PendingPageContext* page_context = _pending_page_context.get();
    page_context->num_log_entries += 1;
    // TODO reduce estimation cost
    page_context->estimated_page_size += log_entry->ByteSizeLong();
}

Status BinlogFileWriter::add_insert_range(int32_t seg_index, int32_t start_row_id, int32_t end_row_id) {
    CHECK(_writer_state == PREPARE);
    RETURN_IF_ERROR(_switch_page_if_full());

    PendingPageContext* page_context = _pending_page_context.get();
    LogEntryPB* log_entry = page_context->page_content.add_entries();
    log_entry->set_entry_type(INSERT_RANGE);
    InsertRangeEntryDataPB* entry_data = log_entry->mutable_insert_range_entry_data();
    bool in_one_segment = false;
    // if the last and current log entries are in the same segment, no need to set file id.
    // When reading and iterating the page, we can get the file id from the last log entry
    if (page_context->last_segment_index != seg_index) {
        _set_file_id_pb(_pending_write_context->rowset_id, seg_index, entry_data->mutable_file_id());
    } else {
        in_one_segment = true;
    }
    // if rows in current log entry is continuous with that in last entry,
    // not need to set start_row_id, and we can get it from the last entry
    // when iterating the page
    if (!in_one_segment || page_context->last_row_id + 1 < start_row_id) {
        entry_data->set_start_row_id(start_row_id);
    }
    int num_rows = end_row_id - start_row_id + 1;
    entry_data->set_num_rows(num_rows);

    page_context->end_seq_id += num_rows;
    page_context->end_changelog_id += num_rows;
    page_context->num_log_entries += 1;
    page_context->last_segment_index = seg_index;
    page_context->last_row_id = end_row_id;
    // TODO reduce estimation cost
    page_context->estimated_page_size += log_entry->ByteSizeLong();
}

Status BinlogFileWriter::add_update(const RowsetSegInfo& before_info, int32_t before_row_id, int32_t after_seg_index,
                                    int after_row_id) {
    CHECK(_writer_state == PREPARE);
    RETURN_IF_ERROR(_switch_page_if_full());

    PendingPageContext* page_context = _pending_page_context.get();
    LogEntryPB* log_entry = page_context->page_content.add_entries();
    log_entry->set_entry_type(UPDATE);
    UpdateEntryDataPB* entry_data = log_entry->mutable_update_entry_data();

    // set update before
    _set_file_id_pb(before_info.rowset_id, before_info.seg_index, entry_data->mutable_before_file_id());
    entry_data->set_before_row_id(before_row_id);

    // set update after
    bool in_one_segment = false;
    if (page_context->last_segment_index != after_seg_index) {
        _set_file_id_pb(_pending_write_context->rowset_id, after_seg_index, entry_data->mutable_after_file_id());
    } else {
        in_one_segment = true;
    }
    if (!in_one_segment || page_context->last_row_id + 1 < after_row_id) {
        entry_data->set_after_row_id(after_row_id);
    }

    page_context->end_seq_id += 1;
    page_context->end_changelog_id += 2;
    page_context->num_log_entries += 1;
    page_context->last_segment_index = after_seg_index;
    page_context->last_row_id = after_row_id;
    // TODO reduce estimation cost
    page_context->estimated_page_size += log_entry->ByteSizeLong();
    page_context->rowsets.emplace(before_info.rowset_id);
    _pending_write_context->rowsets.emplace(before_info.rowset_id);
}

Status BinlogFileWriter::add_delete(const RowsetSegInfo& delete_info, int32_t row_id) {
    CHECK(_writer_state == PREPARE);
    RETURN_IF_ERROR(_switch_page_if_full());

    PendingPageContext* page_context = _pending_page_context.get();
    LogEntryPB* log_entry = page_context->page_content.add_entries();
    log_entry->set_entry_type(DELETE);
    DeleteEntryDataPB* entry_data = log_entry->mutable_delete_entry_data();
    _set_file_id_pb(delete_info.rowset_id, delete_info.seg_index, entry_data->mutable_file_id());
    entry_data->set_row_id(row_id);

    page_context->end_seq_id += 1;
    page_context->end_changelog_id += 1;
    page_context->num_log_entries += 1;
    // TODO reduce estimation cost
    page_context->estimated_page_size += log_entry->ByteSizeLong();
    page_context->rowsets.emplace(delete_info.rowset_id);
    _pending_write_context->rowsets.emplace(delete_info.rowset_id);
}

Status BinlogFileWriter::commit(bool end_of_tablet_version) {
    CHECK(_writer_state == PREPARE);
    _writer_state = WAIT_WRITE;
    Status status;
    if (_pending_page_context->num_log_entries > 0) {
        status = _flush_page(end_of_tablet_version);
        LOG(WARNING) << "Failed to flush page when committing"
                     << ", tablet version " << _pending_write_context->tablet_version << ", file id " << _file_id
                     << ", file name " << _file_name << ", " << status;
        return status;
    }
    status = _file->sync();
    if (!status.ok()) {
        LOG(WARNING) << "Failed to sync when committing"
                     << ", tablet version " << _pending_write_context->tablet_version << ", file id " << _file_id
                     << ", file name " << _file_name << ", " << status;
        return status;
    }

    PendingWriteContext* write_context = _pending_write_context.get();
    PendingPageContext* page_context = _pending_page_context.get();
    BinlogFileMetaPB* file_meta = _file_meta.get();
    if (!file_meta->has_start_tablet_version()) {
        file_meta->set_start_tablet_version(write_context->tablet_version);
        file_meta->set_start_seq_id(write_context->start_seq_id);
        file_meta->set_start_changelog_id(write_context->start_changelog_id);
        file_meta->set_creation_time_in_sec(write_context->rowset_creation_time_in_sec);
    }
    file_meta->set_end_creation_time_in_sec(write_context->rowset_creation_time_in_sec);
    file_meta->set_end_seq_id(page_context->end_seq_id);
    file_meta->set_end_changelog_id(page_context->end_changelog_id);
    file_meta->set_end_creation_time_in_sec(write_context->rowset_creation_time_in_sec);
    file_meta->set_file_size(_file->size());
    for (auto& rowset_id : write_context->rowsets) {
        auto pair = _rowsets.emplace(rowset_id);
        if (pair.second) {
            RowsetIdPB* rowset_id_pb = file_meta->add_rowsets();
            _set_row_id_pb(rowset_id, rowset_id_pb);
        }
    }
    _reset_pending_context();
    return Status::OK();
}

Status BinlogFileWriter::abort() {
    CHECK(_writer_state == PREPARE);
    _writer_state = WAIT_WRITE;

    int64_t tablet_version = _pending_write_context->tablet_version;
    _reset_pending_context();
    CHECK(_file_meta->file_size() <= _file->size())
            << "File size in meta is larger than the actual size,"
            << ", tablet version " << tablet_version << ", file id " << _file_id << ", file name " << _file_name
            << ", meta size " << _file_meta->file_size() << ", actual size " << _file->size();
    if (_file_meta->file_size() == _file->size()) {
        return Status::OK();
    }

    // try to truncate the file to remove useless data
    Status status = FileSystemUtil::resize_file(_file_name, _file_meta->file_size());
    if (!status.ok()) {
        LOG(WARNING) << "Failed to resize file, tablet version " << _pending_write_context->tablet_version
                     << ", file id " << _file_id << ", file name " << _file_name << ", current size " << _file->size()
                     << ", target size " << _file_meta->file_size() << ", " << status;
        return status;
    }

    // reopen the file to append to the truncated file
    status = _file->close();
    if (!status.ok()) {
        LOG(WARNING) << "Failed to abort, tablet version " << _pending_write_context->tablet_version << ", file id "
                     << _file_id << ", file name " << _file_name << ", " << status;
        return status;
    }

    std::shared_ptr<FileSystem> fs;
    ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(_file_name))
    WritableFileOptions write_option;
    write_option.mode = FileSystem::CREATE_OR_OPEN;
    ASSIGN_OR_RETURN(_file, fs->new_writable_file(write_option, _file_name))
}

Status BinlogFileWriter::rollback(int64_t tablet_version) {
    // TODO rollback to the tablet version when loading
    return Status::OK();
}

Status BinlogFileWriter::close() {
    if (_writer_state == CLOSED) {
        return Status::OK();
    }
    _writer_state = CLOSED;

    if (_file != nullptr) {
        _try_append_file_meta();
        return _file->close();
    }

    return Status::OK();
}

Status BinlogFileWriter::_switch_page_if_full() {
    PendingPageContext* page_context = _pending_page_context.get();
    if (page_context->estimated_page_size < _max_page_size) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_flush_page(false));

    page_context->start_seq_id = page_context->end_seq_id + 1;
    page_context->start_changelog_id = page_context->end_changelog_id + 1;
    // no need to set end_seq_id and end_changelog_id
    page_context->num_log_entries = 0;
    page_context->last_segment_index = -1;
    page_context->last_row_id = -1;
    page_context->estimated_page_size = 0;
    page_context->rowsets.clear();
    page_context->rowsets.emplace(_pending_write_context->rowset_id);
    page_context->page_header.Clear();
    page_context->page_content.Clear();

    return Status::OK();
}

Status BinlogFileWriter::_flush_page(bool is_end_page) {
    CHECK(_pending_page_context->num_log_entries > 0) << "Can't flush an empty page"
                                                      << ", tablet version " << _pending_write_context->tablet_version
                                                      << ", file id " << _file_id << ", file name " << _file_name;
    // 1. compress page content
    PageContentPB& page_content = _pending_page_context->page_content;
    // TODO reuse serialized_page_content
    std::string serialized_page_content;
    if (!page_content.SerializeToString(&serialized_page_content)) {
        LOG(WARNING) << "Failed to serialize page content for version " << _pending_write_context->tablet_version
                     << ", rowset " << _pending_write_context->rowset_id << ", num entries "
                     << _pending_page_context->num_log_entries << ", file id " << _file_id << ", file name "
                     << _file_name;
        return Status::InternalError("Failed to serialize page content");
    }
    Status status;
    std::vector<Slice> slices{serialized_page_content};
    // TODO reuse compressed_body
    faststring compressed_body;
    status = PageIO::compress_page_body(_compress_codec, 0.1, slices, &compressed_body);
    if (!status.ok()) {
        LOG(WARNING) << "Failed to compress page content for version " << _pending_write_context->tablet_version
                     << ", rowset " << _pending_write_context->rowset_id << ", num entries "
                     << page_content.entries_size() << ", file id " << _file_id << ", file name " << _file_name << ", "
                     << status;
        return status;
    }

    // 2. build PageHeaderPB
    PageHeaderPB& page_header = _pending_page_context->page_header;
    page_header.set_page_type(NORMAL);
    page_header.set_uncompressed_size(serialized_page_content.size());
    if (compressed_body.size() == 0) {
        page_header.set_compress_type(NO_COMPRESSION);
        page_header.set_compressed_size(serialized_page_content.size());
        uint32_t crc = crc32c::Value(serialized_page_content.c_str(), serialized_page_content.size());
        page_header.set_compressed_page_crc(crc);
    } else {
        page_header.set_compress_type(_compression_type);
        page_header.set_compressed_size(compressed_body.size());
        Slice slice(compressed_body);
        uint32_t crc = crc32c::Value(slice.get_data(), slice.get_size());
        page_header.set_compressed_page_crc(crc);
    }

    page_header.set_tablet_version(_pending_write_context->tablet_version);
    page_header.set_num_log_entries(_pending_page_context->num_log_entries);
    page_header.set_start_changelog_id(_pending_page_context->start_seq_id);
    page_header.set_start_changelog_id(_pending_page_context->start_changelog_id);
    page_header.set_end_seq_id(_pending_page_context->end_seq_id);
    page_header.set_end_change_log_id(_pending_page_context->end_changelog_id);
    page_header.set_creation_time_in_sec(_pending_write_context->rowset_creation_time_in_sec);
    page_header.set_end_page(is_end_page);
    for (auto& rowset_id : _pending_page_context->rowsets) {
        RowsetIdPB* rowset_id_pb = page_header.add_rowsets();
        _set_row_id_pb(rowset_id, rowset_id_pb);
    }

    VLOG(3) << "Estimated page content size " << _pending_page_context->estimated_page_size
            << ", actual page content size " << page_header.uncompressed_size() << ", compressed page content size "
            << page_header.compressed_size() << ", file id " << _file_id << ", file name " << _file_name;

    // TODO reuse serialized_page_header
    std::string serialized_page_header;
    if (!page_header.SerializeToString(&serialized_page_header)) {
        LOG(WARNING) << "Failed to serialize page header for version " << _pending_write_context->tablet_version
                     << ", rowset " << _pending_write_context->rowset_id << ", num entries "
                     << _pending_page_context->num_log_entries << ", file id " << _file_id << ", file name "
                     << _file_name;
        return Status::InternalError("Failed to serialize page header");
    }

    // 3. write page header and content
    faststring header_fixed_buf;
    // header pb size
    put_fixed32_le(&header_fixed_buf, serialized_page_header.size());
    // header pb checksum
    uint32_t checksum = crc32c::Value(serialized_page_header.data(), serialized_page_header.size());
    put_fixed32_le(&header_fixed_buf, checksum);
    std::vector<Slice> data{header_fixed_buf, serialized_page_header, serialized_page_content};
    // sync file when commit
    status = _file->appendv(&data[0], data.size());
    if (!status.ok()) {
        LOG(WARNING) << "Failed to write page for version " << _pending_write_context->tablet_version << ", rowset "
                     << _pending_write_context->rowset_id << ", num entries " << _pending_page_context->num_log_entries
                     << ", file id " << _file_id << ", file name " << _file_name << ", " << status;
    }

    return status;
}

void BinlogFileWriter::_try_append_file_meta() {
    if (_file_meta == nullptr || !_file_meta->has_start_tablet_version()) {
        return;
    }

    std::string serialized_file_meta;
    if (!_file_meta->SerializeToString(&serialized_file_meta)) {
        LOG(WARNING) << "Failed to serialize file meta, file id " << _file_id << ", file name " << _file_name;
        return;
    }

    faststring footer_fix_buf;
    put_fixed32_le(&footer_fix_buf, serialized_file_meta.size());
    uint32_t checksum = crc32c::Value(serialized_file_meta.data(), serialized_file_meta.size());
    put_fixed32_le(&footer_fix_buf, checksum);
    std::vector<Slice> slices{serialized_file_meta, footer_fix_buf};
    Status st = _file->appendv(&slices[0], slices.size());
    if (!st.ok()) {
        LOG(WARNING) << "Failed to write footer, file id " << _file_id << ", file name " << _file_name << ", " << st;
    }
}

void BinlogFileWriter::_reset_pending_context() {
    _pending_write_context->rowsets.clear();
    _pending_page_context->rowsets.clear();
    _pending_page_context->page_header.Clear();
    _pending_page_context->page_content.Clear();
}

} // namespace starrocks