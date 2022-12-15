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

#include "storage/binlog_manager.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/segment_options.h"

namespace starrocks {

BinlogReader::BinlogReader(int64_t reader_id, std::shared_ptr<BinlogManager> binlog_manager,
                           vectorized::VectorizedSchema& schema, int chunk_size)
        : _reader_id(reader_id),
          _binlog_manager(std::move(binlog_manager)),
          _schema(std::move(schema)),
          _chunk_size(chunk_size) {}

BinlogReader::~BinlogReader() {
    _reset();
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

    RETURN_IF_ERROR(_seek_to_file_meta(version, seq_id));
    RETURN_IF_ERROR(_seek_to_segment_row(seq_id));
    LOG(INFO) << "Binlog reader " << _reader_id << ", seek to version " << version << ", seq_id " << seq_id;
    return Status::OK();
}

// TODO what if there are missing versions in the binlog
Status BinlogReader::get_next(vectorized::ChunkPtr* chunk, int64_t max_version_exclusive) {
    if (_next_version >= max_version_exclusive) {
        return Status::EndOfFile("End of max version " + max_version_exclusive);
    }

    chunk->get()->reset();
    if (_log_entry_info->log_entry->entry_type() == EMPTY) {
        _next_version += 1;
        _next_seq_id = 0;
        return Status::OK();
    }

    Status status = _segment_iterator->get_next(chunk->get());
    if (!status.ok() && !status.is_end_of_file()) {
        return status;
    }

    // TODO append binlog meta columns
    if (status.ok()) {
        int num_rows = chunk->get()->num_rows();
        _next_seq_id += num_rows;
        if (_next_seq_id == _log_entry_info->end_seq_id + 1 && _log_entry_info->end_of_version) {
            _next_version += 1;
            _next_seq_id = 0;
        }
        return status;
    }

    // End of segment file, and switch to next log entry
    status = _binlog_file_reader->next();
    if (!status.ok() && !status.is_end_of_file()) {
        return status;
    }

    if (status.is_end_of_file()) {
        status = _seek_to_file_meta(_next_version, _next_seq_id);
        if (status.is_not_found()) {
            return Status::EndOfFile("Read out of all binlog");
        }
        RETURN_IF_ERROR(status);
    }
    RETURN_IF_ERROR(_seek_to_segment_row(_next_seq_id));

    return get_next(chunk, max_version_exclusive);
}

Status BinlogReader::_seek_to_file_meta(int64_t version, int64_t seq_id) {
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

Status BinlogReader::_seek_to_segment_row(int64_t seq_id) {
    _log_entry_info = _binlog_file_reader->log_entry();
    LogEntryTypePB log_entry_type = _log_entry_info->log_entry->entry_type();
    if (log_entry_type == EMPTY) {
        return Status::OK();
    }
    CHECK_EQ(log_entry_type, INSERT_RANGE) << "currently only support INSERT_RANGE";
    _next_seq_id = seq_id;
    CHECK(_log_entry_info->start_seq_id <= _next_seq_id)
            << "Seek to invalid seq, start_seq_id " << _log_entry_info->start_seq_id << ", target seq_id "
            << _next_seq_id;
    int32_t start_row_id = seq_id - _log_entry_info->start_seq_id + _log_entry_info->start_row_id;
    return _init_segment_iterator(start_row_id);
}

Status BinlogReader::_init_segment_iterator(int32_t start_row_id) {
    if (_segment_iterator != nullptr) {
        _segment_iterator->close();
        _segment_iterator.reset();
    }

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

    int seg_index = _log_entry_info->file_id->segment_index();
    SegmentSharedPtr seg_ptr = _rowset->segments()[seg_index];
    vectorized::SegmentReadOptions seg_options;
    ASSIGN_OR_RETURN(seg_options.fs, FileSystem::CreateSharedFromString(_rowset->rowset_path()));
    seg_options.chunk_size = _chunk_size;
    vectorized::SparseRange range(start_row_id, seg_ptr->num_rows());
    seg_options.rowid_range_option =
            std::make_shared<vectorized::RowidRangeOption>(_rowset->rowset_id(), seg_index, range);
    auto res = seg_ptr->new_iterator(_schema, seg_options);
    if (!res.status().ok()) {
        return res.status();
    }
    _segment_iterator = res.value();
    return Status::OK();
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
}

} // namespace starrocks
