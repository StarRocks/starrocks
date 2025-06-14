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

#include "storage/binlog_reader.h"

#include <utility>

#include "column/datum.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet.h"

namespace starrocks {

BinlogReader::BinlogReader(TabletSharedPtr tablet, BinlogReaderParams reader_params)
        : _tablet(std::move(tablet)), _reader_params(std::move(reader_params)) {}

Status BinlogReader::init() {
    _binlog_manager = _tablet->binlog_manager();
    if (_binlog_manager == nullptr) {
        return Status::InternalError(
                fmt::format("Fail to initialize binlog reader because there is no binlog manager."
                            " Tablet: {}, keys type: {}",
                            _tablet->full_name(), KeysType_Name(_tablet->keys_type())));
    }
    StatusOr<int64_t> status_or = _binlog_manager->register_reader(shared_from_this());
    if (!status_or.ok()) {
        LOG(ERROR) << "Fail to initialize binlog reader for tablet " << _tablet->full_name() << ", "
                   << status_or.status();
        return status_or.status();
    }
    _reader_id = status_or.value();

    Schema& schema = _reader_params.output_schema;
    for (uint32_t i = 0; i < schema.num_fields(); i++) {
        std::string_view cname = schema.field(i)->name();
        if (cname == BINLOG_OP) {
            _binlog_op_column_index = i;
        } else if (cname == BINLOG_VERSION) {
            _binlog_version_column_index = i;
        } else if (cname == BINLOG_SEQ_ID) {
            _binlog_seq_id_column_index = i;
        } else if (cname == BINLOG_TIMESTAMP) {
            _binlog_timestamp_column_index = i;
        } else {
            _data_column_index.emplace_back(i);
        }
    }
    _data_schema = Schema(&_reader_params.output_schema, _data_column_index);
    _data_chunk = ChunkHelper::new_chunk(_data_schema, 0);

    _initialized = true;

    VLOG(3) << "Initialize binlog reader " << _reader_id << " for tablet " << _tablet->full_name();
    return Status::OK();
}

Status BinlogReader::seek(int64_t version, int64_t seq_id) {
    if (_next_version == version && _next_seq_id == seq_id) {
        return Status::OK();
    }
    _reset();
    RETURN_IF_ERROR(_seek_binlog_file_reader(version, seq_id));
    _log_entry_info = _binlog_file_reader->log_entry();
    _next_version = version;
    _next_seq_id = seq_id;
    if (_log_entry_info->log_entry->entry_type() != EMPTY_PB) {
        RETURN_IF_ERROR(_init_segment_iterator());
    }
    VLOG(3) << "Binlog reader " << _reader_id << " for tablet " << _tablet->full_name() << ", seek to version "
            << version << ", seq_id " << seq_id;
    return Status::OK();
}

Status BinlogReader::get_next(ChunkPtr* chunk, int64_t max_version_exclusive) {
    // Invariant: if _log_entry_info is not nullptr, change event with
    // <_next_version, _next_seq_id> must be in this log entry, otherwise
    // need to find the log entry first
    if (_log_entry_info != nullptr && _log_entry_info->log_entry->entry_type() == EMPTY_PB) {
        _next_version += 1;
        _next_seq_id = 0;
        _log_entry_info = nullptr;
    }
    Status status;
    while (_log_entry_info == nullptr && _next_version < max_version_exclusive) {
        if (_binlog_file_reader != nullptr) {
            status = _binlog_file_reader->next();
            if (!status.ok() && !status.is_end_of_file()) {
                return status;
            }

            if (status.is_end_of_file()) {
                _release_binlog_file();
            }
        }
        if (_binlog_file_reader == nullptr) {
            RETURN_IF_ERROR(_seek_binlog_file_reader(_next_version, _next_seq_id));
        }
        _log_entry_info = _binlog_file_reader->log_entry();
        // versions may be not continuous because binlog on a tablet replica
        // does not clone missing version from other replicas
        if (_log_entry_info->version > _next_version) {
            return Status::NotFound(
                    fmt::format("Can't find next version {}, next seq_id {}", _next_version, _next_seq_id));
        }

        // sanity check
        if (_log_entry_info->version != _next_version || _log_entry_info->start_seq_id != _next_seq_id) {
            std::string err_msg = fmt::format(
                    "Unexpected entry version/seq_id, reader: {}, tablet: {}, "
                    "expect version/seq_id {}/{}, actual version/seq_id {}/{}",
                    _reader_id, _tablet->full_name(), _next_version, _next_seq_id, _log_entry_info->version,
                    _log_entry_info->start_seq_id);
            LOG(ERROR) << err_msg;
            return Status::InternalError(err_msg);
        }
        if (_log_entry_info->log_entry->entry_type() != EMPTY_PB) {
            RETURN_IF_ERROR(_init_segment_iterator());
            break;
        }
        // continue to find a non-empty log entry
        _next_version += 1;
        _next_seq_id = 0;
        _log_entry_info = nullptr;
    }

    if (_next_version >= max_version_exclusive) {
        return Status::EndOfFile(fmt::format("End of max version {}", max_version_exclusive));
    }

    LogEntryInfo* log_entry_info = _log_entry_info;
    _swap_output_and_data_chunk(chunk->get());
    status = _segment_iterator->get_next(_data_chunk.get());
    int32_t num_rows = _data_chunk->num_rows();
    _swap_output_and_data_chunk(chunk->get());
    // sanity check: should not meet the end of file
    if (status.is_end_of_file()) {
        std::string err_msg = fmt::format(
                "Segment should not meet the end of file, reader {}, tablet {}, "
                "next version/seq_id {}/{}, expect end seq_id {}, rowset {}, segment index {}",
                _reader_id, _tablet->full_name(), _next_version, _next_seq_id, log_entry_info->end_seq_id,
                _rowset->rowset_id().to_string(), _log_entry_info->file_id->segment_index());
        LOG(ERROR) << err_msg;
        return Status::InternalError(err_msg);
    }
    if (!status.ok()) {
        return status;
    }
    _append_meta_column(chunk->get(), num_rows, log_entry_info->version, log_entry_info->timestamp_in_us, _next_seq_id);
    _next_seq_id += num_rows;
    // read all change events in this log entry
    if (_next_seq_id > log_entry_info->end_seq_id) {
        bool release_rowset = false;
        // it's also the last log entry in this version, and
        // should move to the next version
        if (log_entry_info->end_of_version) {
            _next_version += 1;
            _next_seq_id = 0;
            release_rowset = true;
        }
        // for duplicate key, the log entry and segment are one-to-one,
        // and release the iterator as soon as possible
        _release_segment_iterator(release_rowset);
        _log_entry_info = nullptr;
    }

    return Status::OK();
}

void BinlogReader::_append_meta_column(Chunk* output_chunk, int32_t num_rows, int64_t version, int64_t timestamp,
                                       int64_t start_seq_id) {
    // for duplicate key table, a chunk only contains rows from a segment, and they have same versions,
    // timestamps, and ops are INSERT
    if (_binlog_op_column_index > -1) {
        ColumnPtr& column = output_chunk->get_column_by_index(_binlog_op_column_index);
        int8_t insert = 0;
        column->append_value_multiple_times(&insert, num_rows);
    }

    if (_binlog_version_column_index > -1) {
        ColumnPtr& column = output_chunk->get_column_by_index(_binlog_version_column_index);
        column->append_value_multiple_times(&version, num_rows);
    }

    if (_binlog_seq_id_column_index > -1) {
        ColumnPtr& column = output_chunk->get_column_by_index(_binlog_seq_id_column_index);
        int64_t end_seq_id = start_seq_id + num_rows - 1;
        for (int64_t seq_id = start_seq_id; seq_id <= end_seq_id; seq_id++) {
            Datum datum(seq_id);
            column->append_datum(datum);
        }
    }

    if (_binlog_timestamp_column_index > -1) {
        ColumnPtr& column = output_chunk->get_column_by_index(_binlog_timestamp_column_index);
        column->append_value_multiple_times(&timestamp, num_rows);
    }
}

void BinlogReader::_swap_output_and_data_chunk(Chunk* output_chunk) {
    Columns& output_columns = output_chunk->columns();
    for (size_t i = 0; i < _data_column_index.size(); i++) {
        uint32_t index = _data_column_index[i];
        _data_chunk->get_column_by_index(i).swap(output_columns[index]);
    }
    _data_chunk->reset();
}

Status BinlogReader::_seek_binlog_file_reader(int64_t version, int64_t seq_id) {
    auto status_or = _binlog_manager->find_binlog_file(version, seq_id);
    if (!status_or.ok()) {
        return status_or.status();
    }
    _binlog_file_holder = status_or.value();
    BinlogFileMetaPBPtr& file_meta = _binlog_file_holder->file_meta();
    std::string file_path = _binlog_manager->get_binlog_file_path(file_meta->id());
    _binlog_file_reader = std::make_shared<BinlogFileReader>(file_path, file_meta);
    RETURN_IF_ERROR(_binlog_file_reader->seek(version, seq_id));
    return Status::OK();
}

Status BinlogReader::_init_segment_iterator() {
    LogEntryInfo* log_entry_info = _log_entry_info;
    int64_t rowset_id = _log_entry_info->file_id->rowset_id();
    if (_rowset != nullptr && rowset_id != _rowset->start_version()) {
        _rowset->release();
        _rowset.reset();
    }
    if (_rowset == nullptr) {
        {
            std::shared_lock lock(_tablet->get_header_lock());
            _rowset = _tablet->get_inc_rowset_by_version(Version(rowset_id, rowset_id));
        }
        _rowset->acquire();
        RETURN_IF_ERROR(_rowset->load());
        VLOG(3) << "Load rowset for reader: " << _reader_id << ", tablet: " << _tablet->full_name()
                << ", rowset: " << _rowset->rowset_id() << ", version: " << _rowset->version();
    }

    int segment_index = _log_entry_info->file_id->segment_index();
    if (segment_index < 0 || segment_index >= _rowset->num_segments()) {
        std::string errMsg = fmt::format(
                "Invalid segment index for reader: {}, tablet: {}, rowset: {}, "
                "number segments: {}, segment index: {}",
                _reader_id, _tablet->full_name(), _rowset->rowset_id().to_string(), _rowset->num_segments(),
                segment_index);
        LOG(ERROR) << errMsg;
        return Status::InternalError(errMsg);
    }
    SegmentSharedPtr seg_ptr = _rowset->segments()[segment_index];
    SegmentReadOptions seg_options;
    ASSIGN_OR_RETURN(seg_options.fs, FileSystem::CreateSharedFromString(_rowset->rowset_path()))
    seg_options.chunk_size = _reader_params.chunk_size;
    seg_options.stats = &_stats;
    // set start row to read if next change event is not the first row in the segment
    if (log_entry_info->start_seq_id < _next_seq_id) {
        // for duplicate key, LogEntryInfo#start_row_id is 0
        rowid_t start_row_id = _next_seq_id - log_entry_info->start_seq_id;
        seg_options.rowid_range_option = std::make_shared<SparseRange<>>(start_row_id, seg_ptr->num_rows());
    }
    ASSIGN_OR_RETURN(_segment_iterator, seg_ptr->new_iterator(_data_schema, seg_options));
    VLOG(3) << "Create segment iterator for reader: " << _reader_id << ", tablet: " << _tablet->full_name()
            << ", rowset: " << _rowset->rowset_id() << ", version: " << _rowset->version()
            << ", segment index: " << segment_index;
    return Status::OK();
}

void BinlogReader::_release_segment_iterator(bool release_rowset) {
    if (_segment_iterator != nullptr) {
        _segment_iterator->close();
        _segment_iterator.reset();
    }
    if (release_rowset && _rowset != nullptr) {
        _rowset->release();
        _rowset.reset();
    }
}

void BinlogReader::_release_binlog_file() {
    if (_binlog_file_reader != nullptr) {
        _binlog_file_reader.reset();
    }

    if (_binlog_file_holder != nullptr) {
        _binlog_file_holder.reset();
    }
}

void BinlogReader::_reset() {
    _release_segment_iterator(true);
    _release_binlog_file();
    _log_entry_info = nullptr;
}

void BinlogReader::close() {
    if (_closed) {
        return;
    }
    _closed = true;
    if (_initialized) {
        _reset();
        _binlog_manager->unregister_reader(_reader_id);
    }
}

} // namespace starrocks
