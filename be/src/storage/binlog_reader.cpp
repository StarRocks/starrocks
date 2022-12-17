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
#include "storage/binlog_manager.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/segment_options.h"

namespace starrocks {

std::unordered_set<std::string_view> BINLOG_META_SET{BINLOG_OP, BINLOG_VERSION, BINLOG_SEQ_ID, BINLOG_TIMESTAMP};

BinlogReader::BinlogReader(std::shared_ptr<BinlogManager> binlog_manager, int64_t reader_id,
                           BinlogReaderParams reader_params)
        : _binlog_manager(std::move(binlog_manager)), _reader_id(reader_id), _reader_params(std::move(reader_params)) {}

Status BinlogReader::init() {
    vectorized::VectorizedSchema& schema = _reader_params.output_schema;
    for (uint32_t i = 0; i < schema.num_fields(); i++) {
        std::string_view cname = schema.field(i)->name();
        if (BINLOG_META_SET.count(cname) == 0) {
            _data_column_index.emplace_back(i);
            continue;
        }

        if (cname == BINLOG_OP) {
            _binlog_op_column_index = i;
        } else if (cname == BINLOG_VERSION) {
            _binlog_version_column_index = i;
        } else if (cname == BINLOG_SEQ_ID) {
            _binlog_seq_id_column_index = i;
        } else if (cname == BINLOG_TIMESTAMP) {
            _binlog_timestamp_column_index = i;
        }
    }
    _data_schema = vectorized::VectorizedSchema(&_reader_params.output_schema, _data_column_index);
    _data_chunk = ChunkHelper::new_chunk(_data_schema, 0);
    return Status::OK();
}

Status BinlogReader::seek(int64_t version, int64_t seq_id) {
    if (version < _next_version || (version == _next_version && seq_id > _next_seq_id)) {
        return Status::InternalError(
                strings::Substitute("Binlog can only be read forward, next position"
                                    " <$0, $1>, seek to <$2, $3>",
                                    _next_version, _next_seq_id, version, seq_id));
    }

    if (_next_version == version && _next_seq_id == seq_id) {
        return Status::OK();
    }
    _reset();
    RETURN_IF_ERROR(_seek_binlog_file_reader(version, seq_id));
    _log_entry_info = _binlog_file_reader->log_entry();
    if (_log_entry_info->log_entry->entry_type() == EMPTY_PB) {
        _next_version = version + 1;
        _next_seq_id = 0;
        _log_entry_info = nullptr;
    } else {
        _next_version = version;
        _next_seq_id = seq_id;
        RETURN_IF_ERROR(_init_segment_iterator());
    }
    LOG(INFO) << "Binlog reader " << _reader_id << ", seek to version " << version << ", seq_id " << seq_id;
    return Status::OK();
}

Status BinlogReader::get_next(vectorized::ChunkPtr* chunk, int64_t max_version_exclusive) {
    // Invariant: if _log_entry_info is not nullptr, change event with
    // <_next_version, _next_seq_id> must be in this log entry, otherwise
    // need to find the log entry first
    Status status;
    while (_log_entry_info == nullptr && _next_version < max_version_exclusive) {
        if (_binlog_file_reader != nullptr) {
            status = _binlog_file_reader->next();
            if (!status.ok() && !status.is_end_of_file()) {
                return status;
            }

            if (status.is_end_of_file()) {
                _binlog_file_reader.reset();
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
        CHECK((_log_entry_info->version == _next_version) && (_log_entry_info->start_seq_id == _next_seq_id));
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

    // TODO append binlog meta columns
    LogEntryInfo* log_entry_info = _log_entry_info;
    _swap_output_and_data_chunk(chunk->get());
    status = _segment_iterator->get_next(_data_chunk.get());
    int32_t num_rows = _data_chunk->num_rows();
    _swap_output_and_data_chunk(chunk->get());
    // should not meet the end of file
    CHECK(!status.is_end_of_file());
    if (!status.ok()) {
        return status;
    }
    _append_meta_column(chunk->get(), num_rows, log_entry_info->version, log_entry_info->timestamp_in_us, _next_seq_id);
    _next_seq_id += num_rows;
    // read all change events in this loge entry
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
        // and release the iterator ASAP
        _release_segment_iterator(release_rowset);
        _log_entry_info = nullptr;
    }

    return Status::OK();
}

void BinlogReader::_append_meta_column(vectorized::Chunk* output_chunk, int32_t num_rows, int64_t version,
                                       int64_t timestamp, int64_t start_seq_id) {
    // for duplicate key table, a chunk only contains rows from a segment, and they have same versions,
    // timestamps, and ops are INSERT
    if (_binlog_op_column_index > -1) {
        vectorized::ColumnPtr& column = output_chunk->get_column_by_index(_binlog_op_column_index);
        int8_t insert = 0;
        column->append_value_multiple_times(&insert, num_rows);
    }

    if (_binlog_version_column_index > -1) {
        vectorized::ColumnPtr& column = output_chunk->get_column_by_index(_binlog_version_column_index);
        column->append_value_multiple_times(&version, num_rows);
    }

    if (_binlog_seq_id_column_index > -1) {
        vectorized::ColumnPtr& column = output_chunk->get_column_by_index(_binlog_seq_id_column_index);
        int64_t end_seq_id = start_seq_id + num_rows - 1;
        for (int64_t seq_id = start_seq_id; seq_id <= end_seq_id; seq_id++) {
            vectorized::Datum datum(seq_id);
            column->append_datum(datum);
        }
    }

    if (_binlog_timestamp_column_index > -1) {
        vectorized::ColumnPtr& column = output_chunk->get_column_by_index(_binlog_timestamp_column_index);
        column->append_value_multiple_times(&timestamp, num_rows);
    }
}

void BinlogReader::_swap_output_and_data_chunk(vectorized::Chunk* output_chunk) {
    vectorized::Columns& output_columns = output_chunk->columns();
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
    _file_meta = status_or.value();
    std::string file_name = _binlog_manager->binlog_file_name(_file_meta->id());
    _binlog_file_reader = std::make_shared<BinlogFileReader>(file_name, _file_meta);
    RETURN_IF_ERROR(_binlog_file_reader->seek(version, seq_id));
    return Status::OK();
}

Status BinlogReader::_init_segment_iterator() {
    LogEntryInfo* log_entry_info = _log_entry_info;
    CHECK_EQ(INSERT_RANGE_PB, log_entry_info->log_entry->entry_type()) << "currently only support INSERT_RANGE";
    CHECK_EQ(0, log_entry_info->start_row_id)
            << "currently only support duplicate key table whose start row id should be 0";
    RowsetIdPB* rowset_id_pb = _log_entry_info->file_id->mutable_rowset_id();
    RowsetId rowset_id;
    rowset_id.init(rowset_id_pb->hi(), rowset_id_pb->mi(), rowset_id_pb->lo());
    if (_rowset != nullptr && rowset_id != _rowset->rowset_id()) {
        _rowset->release();
        _rowset.reset();
    }
    if (_rowset == nullptr) {
        _rowset = _binlog_manager->get_rowset(rowset_id);
        _rowset->acquire();
        RETURN_IF_ERROR(_rowset->load());
    }

    int segment_index = _log_entry_info->file_id->segment_index();
    SegmentSharedPtr seg_ptr = _rowset->segments()[segment_index];
    // TODO only read needed columns
    vectorized::SegmentReadOptions seg_options;
    ASSIGN_OR_RETURN(seg_options.fs, FileSystem::CreateSharedFromString(_rowset->rowset_path()))
    seg_options.chunk_size = _reader_params.chunk_size;
    seg_options.stats = &_stats;
    // set start row to read if next change event is not the first row in the segment
    if (log_entry_info->start_seq_id < _next_seq_id) {
        // for duplicate key, LogEntryInfo#start_row_id is 0
        rowid_t start_row_id = _next_seq_id - log_entry_info->start_seq_id;
        vectorized::SparseRange range(start_row_id, seg_ptr->num_rows());
        seg_options.rowid_range_option =
                std::make_shared<vectorized::RowidRangeOption>(_rowset->rowset_id(), segment_index, range);
    }
    auto res = seg_ptr->new_iterator(_data_schema, seg_options);
    if (!res.status().ok()) {
        return res.status();
    }
    _segment_iterator = res.value();
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

void BinlogReader::_reset() {
    if (_segment_iterator != nullptr) {
        _segment_iterator->close();
        _segment_iterator.reset();
    }

    if (_rowset != nullptr) {
        _rowset->release();
        _rowset.reset();
    }

    if (_binlog_file_reader != nullptr) {
        _binlog_file_reader.reset();
    }

    if (_file_meta != nullptr) {
        _file_meta.reset();
    }

    _log_entry_info = nullptr;
}

void BinlogReader::close() {
    if (_closed) {
        return;
    }
    _closed = true;
    _reset();
}

} // namespace starrocks
