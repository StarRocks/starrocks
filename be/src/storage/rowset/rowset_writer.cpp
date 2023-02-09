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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/rowset_writer.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/rowset/rowset_writer.h"

#include <butil/iobuf.h>
#include <butil/reader_writer.h>
#include <fmt/format.h>

#include <ctime>
#include <memory>

#include "column/chunk.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/tracer.h"
#include "fs/fs.h"
#include "io/io_error.h"
#include "runtime/exec_env.h"
#include "segment_options.h"
#include "serde/column_array_serde.h"
#include "storage/aggregate_iterator.h"
#include "storage/chunk_helper.h"
#include "storage/merge_iterator.h"
#include "storage/metadata_util.h"
#include "storage/olap_define.h"
#include "storage/row_source_mask.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/type_utils.h"
#include "util/pretty_printer.h"

namespace starrocks {

class SegmentFileWriter : public butil::IWriter {
public:
    SegmentFileWriter(WritableFile* wfile) : _wfile(wfile) {}

    ssize_t WriteV(const iovec* iov, int iovcnt) override;

private:
    WritableFile* _wfile;
};

ssize_t SegmentFileWriter::WriteV(const iovec* iov, int iovcnt) {
    size_t bytes_written = 0;
    auto st = _wfile->appendv(reinterpret_cast<const Slice*>(iov), iovcnt);
    if (st.ok()) {
        for (int i = 0; i < iovcnt; ++i) {
            bytes_written += iov[i].iov_len;
        }
        return bytes_written;
    } else {
        LOG(WARNING) << "Failed to write " << _wfile->filename() << " err=" << st;
        return -1;
    }
}

RowsetWriter::RowsetWriter(const RowsetWriterContext& context)
        : _context(context), _num_rows_written(0), _total_row_size(0), _total_data_size(0), _total_index_size(0) {}

Status RowsetWriter::init() {
    _rowset_meta_pb = std::make_unique<RowsetMetaPB>();
    _rowset_meta_pb->set_deprecated_rowset_id(0);
    _rowset_meta_pb->set_rowset_id(_context.rowset_id.to_string());
    _rowset_meta_pb->set_partition_id(_context.partition_id);
    _rowset_meta_pb->set_tablet_id(_context.tablet_id);
    _rowset_meta_pb->set_tablet_schema_hash(_context.tablet_schema_hash);
    _rowset_meta_pb->set_rowset_type(BETA_ROWSET);
    _rowset_meta_pb->set_rowset_state(_context.rowset_state);
    _rowset_meta_pb->set_segments_overlap_pb(_context.segments_overlap);

    if (_context.rowset_state == PREPARED || _context.rowset_state == COMMITTED) {
        _is_pending = true;
        _rowset_meta_pb->set_txn_id(_context.txn_id);
        PUniqueId* new_load_id = _rowset_meta_pb->mutable_load_id();
        new_load_id->set_hi(_context.load_id.hi());
        new_load_id->set_lo(_context.load_id.lo());
    } else {
        _rowset_meta_pb->set_start_version(_context.version.first);
        _rowset_meta_pb->set_end_version(_context.version.second);
    }
    *(_rowset_meta_pb->mutable_tablet_uid()) = _context.tablet_uid.to_proto();

    _writer_options.global_dicts = _context.global_dicts != nullptr ? _context.global_dicts : nullptr;
    _writer_options.referenced_column_ids = _context.referenced_column_ids;

    if (_context.tablet_schema->keys_type() == KeysType::PRIMARY_KEYS &&
        (_context.partial_update_tablet_schema || !_context.merge_condition.empty())) {
        _rowset_txn_meta_pb = std::make_unique<RowsetTxnMetaPB>();
    }

    ASSIGN_OR_RETURN(_fs, FileSystem::CreateSharedFromString(_context.rowset_path_prefix));
    return Status::OK();
}

StatusOr<RowsetSharedPtr> RowsetWriter::build() {
    if (_num_rows_written > 0 || (_context.tablet_schema->keys_type() == KeysType::PRIMARY_KEYS && _num_delfile > 0)) {
        RETURN_IF_ERROR(_fs->sync_dir(_context.rowset_path_prefix));
    }
    _rowset_meta_pb->set_num_rows(_num_rows_written);
    _rowset_meta_pb->set_total_row_size(_total_row_size);
    _rowset_meta_pb->set_total_disk_size(_total_data_size);
    _rowset_meta_pb->set_data_disk_size(_total_data_size);
    _rowset_meta_pb->set_index_disk_size(_total_index_size);
    // TODO write zonemap to meta
    _rowset_meta_pb->set_empty(_num_rows_written == 0);
    _rowset_meta_pb->set_creation_time(time(nullptr));
    _rowset_meta_pb->set_num_segments(_num_segment);
    // newly created rowset do not have rowset_id yet, use 0 instead
    _rowset_meta_pb->set_rowset_seg_id(0);
    // updatable tablet require extra processing
    if (_context.tablet_schema->keys_type() == KeysType::PRIMARY_KEYS) {
        DCHECK(_delfile_idxes.size() == _num_delfile);
        if (!_delfile_idxes.empty()) {
            _rowset_meta_pb->mutable_delfile_idxes()->Add(_delfile_idxes.begin(), _delfile_idxes.end());
        }
        _rowset_meta_pb->set_num_delete_files(_num_delfile);
        if (_num_segment <= 1) {
            _rowset_meta_pb->set_segments_overlap_pb(NONOVERLAPPING);
        }
        // if load only has delete, we can skip the partial update logic
        if (_context.partial_update_tablet_schema && _flush_chunk_state != FlushChunkState::DELETE) {
            DCHECK(_context.referenced_column_ids.size() == _context.partial_update_tablet_schema->columns().size());
            RETURN_IF(_num_segment != _rowset_txn_meta_pb->partial_rowset_footers().size(),
                      Status::InternalError(fmt::format("segment number {} not equal to partial_rowset_footers size {}",
                                                        _num_segment,
                                                        _rowset_txn_meta_pb->partial_rowset_footers().size())));
            for (auto i = 0; i < _context.partial_update_tablet_schema->columns().size(); ++i) {
                const auto& tablet_column = _context.partial_update_tablet_schema->column(i);
                _rowset_txn_meta_pb->add_partial_update_column_ids(_context.referenced_column_ids[i]);
                _rowset_txn_meta_pb->add_partial_update_column_unique_ids(tablet_column.unique_id());
            }
            if (!_context.merge_condition.empty()) {
                _rowset_txn_meta_pb->set_merge_condition(_context.merge_condition);
            }
            *_rowset_meta_pb->mutable_txn_meta() = *_rowset_txn_meta_pb;
        } else if (!_context.merge_condition.empty()) {
            _rowset_txn_meta_pb->set_merge_condition(_context.merge_condition);
            *_rowset_meta_pb->mutable_txn_meta() = *_rowset_txn_meta_pb;
        }
    } else {
        if (_num_segment <= 1) {
            _rowset_meta_pb->set_segments_overlap_pb(NONOVERLAPPING);
        }
    }
    if (_is_pending) {
        _rowset_meta_pb->set_rowset_state(COMMITTED);
    } else {
        _rowset_meta_pb->set_rowset_state(VISIBLE);
    }
    auto rowset_meta = std::make_shared<RowsetMeta>(_rowset_meta_pb);
    RowsetSharedPtr rowset;
    RETURN_IF_ERROR(
            RowsetFactory::create_rowset(_context.tablet_schema, _context.rowset_path_prefix, rowset_meta, &rowset));
    _already_built = true;
    return rowset;
}

Status RowsetWriter::flush_segment(const SegmentPB& segment_pb, butil::IOBuf& data) {
    if (data.size() != segment_pb.data_size() + segment_pb.delete_data_size()) {
        return Status::InternalError(fmt::format("segment size {} + delete file size {} not equal attachment size {}",
                                                 segment_pb.data_size(), segment_pb.delete_data_size(), data.size()));
    }

    if (segment_pb.has_path()) {
        // 1. create segment file
        auto path = Rowset::segment_file_path(_context.rowset_path_prefix, _context.rowset_id, segment_pb.segment_id());
        // use MUST_CREATE make sure atomic
        ASSIGN_OR_RETURN(auto wfile, _fs->new_writable_file(path));

        // 2. flush segment file
        auto writer = std::make_unique<SegmentFileWriter>(wfile.get());

        butil::IOBuf segment_data;
        int64_t remaining_bytes = data.cutn(&segment_data, segment_pb.data_size());
        if (remaining_bytes != segment_pb.data_size()) {
            return Status::InternalError(fmt::format("segment {} file size {} not equal attachment size {}",
                                                     segment_pb.DebugString(), remaining_bytes,
                                                     segment_pb.data_size()));
        }
        while (remaining_bytes > 0) {
            auto written_bytes = segment_data.cut_into_writer(writer.get(), remaining_bytes);
            if (written_bytes < 0) {
                return io::io_error(wfile->filename(), errno);
            }
            remaining_bytes -= written_bytes;
        }
        if (remaining_bytes != 0) {
            return Status::InternalError(fmt::format("segment {} write size {} not equal expected size {}",
                                                     wfile->filename(), segment_pb.data_size() - remaining_bytes,
                                                     segment_pb.data_size()));
        }
        if (config::sync_tablet_meta) {
            RETURN_IF_ERROR(wfile->sync());
        }
        RETURN_IF_ERROR(wfile->close());

        if (_context.tablet_schema->keys_type() == KeysType::PRIMARY_KEYS && _context.partial_update_tablet_schema) {
            auto* partial_rowset_footer = _rowset_txn_meta_pb->add_partial_rowset_footers();
            partial_rowset_footer->set_position(segment_pb.partial_footer_position());
            partial_rowset_footer->set_size(segment_pb.partial_footer_size());
        }

        // 3. update statistic
        {
            std::lock_guard<std::mutex> l(_lock);
            _total_data_size += segment_pb.data_size();
            _total_index_size += segment_pb.index_size();
            _num_rows_written += segment_pb.num_rows();
            _total_row_size += segment_pb.row_size();
            _num_segment++;
        }

        VLOG(2) << "Flush segment to " << path << " size " << segment_pb.data_size();
    }

    if (segment_pb.has_delete_path()) {
        // 1. create delete file
        auto path =
                Rowset::segment_del_file_path(_context.rowset_path_prefix, _context.rowset_id, segment_pb.delete_id());
        // use MUST_CREATE make sure atomic
        ASSIGN_OR_RETURN(auto wfile, _fs->new_writable_file(path));

        // 2. flush delete file
        auto writer = std::make_unique<SegmentFileWriter>(wfile.get());

        butil::IOBuf delete_data;
        int64_t remaining_bytes = data.cutn(&delete_data, segment_pb.delete_data_size());
        if (remaining_bytes != segment_pb.delete_data_size()) {
            return Status::InternalError(fmt::format("segment {} delete size {} not equal attachment size {}",
                                                     segment_pb.DebugString(), remaining_bytes,
                                                     segment_pb.delete_data_size()));
        }
        while (remaining_bytes > 0) {
            auto written_bytes = delete_data.cut_into_writer(writer.get(), remaining_bytes);
            if (written_bytes < 0) {
                return io::io_error(wfile->filename(), errno);
            }
            remaining_bytes -= written_bytes;
        }
        if (remaining_bytes != 0) {
            return Status::InternalError(fmt::format("segment delete file {} write size {} not equal expected size {}",
                                                     wfile->filename(), segment_pb.delete_data_size() - remaining_bytes,
                                                     segment_pb.delete_data_size()));
        }

        if (config::sync_tablet_meta) {
            RETURN_IF_ERROR(wfile->sync());
        }
        RETURN_IF_ERROR(wfile->close());

        _num_delfile++;
        _num_rows_del += segment_pb.delete_num_rows();

        VLOG(2) << "Flush delete file to " << path << " size " << segment_pb.delete_data_size();
    }

    return Status::OK();
}

HorizontalRowsetWriter::HorizontalRowsetWriter(const RowsetWriterContext& context)
        : RowsetWriter(context), _segment_writer(nullptr) {}

HorizontalRowsetWriter::~HorizontalRowsetWriter() {
    // TODO(lingbin): Should wrapper exception logic, no need to know file ops directly.
    if (!_already_built) {       // abnormal exit, remove all files generated
        _segment_writer.reset(); // ensure all files are closed
        if (_context.tablet_schema->keys_type() == KeysType::PRIMARY_KEYS) {
            for (const auto& tmp_segment_file : _tmp_segment_files) {
                // these files will be cleaned up with error handling by the GC background even though
                // they have not been deleted here after an error occurred, so we just go ahead here and
                // print the error message only when we encounter an error except Status::NotFound().
                // The following similar scenarios are same.
                auto st = _fs->delete_file(tmp_segment_file);
                LOG_IF(WARNING, !(st.ok() || st.is_not_found()))
                        << "Fail to delete file=" << tmp_segment_file << ", " << st.to_string();
            }
            _tmp_segment_files.clear();
            for (auto i = 0; i < _num_segment; ++i) {
                auto path = Rowset::segment_file_path(_context.rowset_path_prefix, _context.rowset_id, i);
                auto st = _fs->delete_file(path);
                LOG_IF(WARNING, !(st.ok() || st.is_not_found()))
                        << "Fail to delete file=" << path << ", " << st.to_string();
            }
            for (auto i = 0; i < _num_delfile; ++i) {
                auto path = Rowset::segment_del_file_path(_context.rowset_path_prefix, _context.rowset_id, i);
                auto st = _fs->delete_file(path);
                LOG_IF(WARNING, !(st.ok() || st.is_not_found()))
                        << "Fail to delete file=" << path << ", " << st.to_string();
            }
            //// only deleting segment files may not delete all delete files or temporary files
            //// during final merge, should iterate directory and delete all files with rowset_id prefix
            //std::vector<std::string> files;
            //Status st = _context.env->get_children(_context.rowset_path_prefix, &files);
            //if (!st.ok()) {
            //    LOG(WARNING) << "list dir failed: " << _context.rowset_path_prefix << " "
            //                 << st.to_string();
            //}
            //string prefix = _context.rowset_id.to_string();
            //for (auto& f : files) {
            //    if (strncmp(f.c_str(), prefix.c_str(), prefix.size()) == 0) {
            //        string path = _context.rowset_path_prefix + "/" + f;
            //        // Even if an error is encountered, these files that have not been cleaned up
            //        // will be cleaned up by the GC background. So here we only print the error
            //        // message when we encounter an error.
            //        Status st = _context.env->delete_file(path);
            //        LOG_IF(WARNING, !st.ok())
            //                << "Fail to delete file=" << path << ", " << st.to_string();
            //    }
            //}
        } else {
            for (int i = 0; i < _num_segment; ++i) {
                auto path = Rowset::segment_file_path(_context.rowset_path_prefix, _context.rowset_id, i);
                auto st = _fs->delete_file(path);
                LOG_IF(WARNING, !(st.ok() || st.is_not_found()))
                        << "Fail to delete file=" << path << ", " << st.to_string();
            }
        }
        // if _already_built is false, we need to release rowset_id to avoid rowset_id leak
        StorageEngine::instance()->release_rowset_id(_context.rowset_id);
    }
}

StatusOr<std::unique_ptr<SegmentWriter>> HorizontalRowsetWriter::_create_segment_writer() {
    std::lock_guard<std::mutex> l(_lock);
    std::string path;
    if (_context.schema_change_sorting) {
        path = Rowset::segment_temp_file_path(_context.rowset_path_prefix, _context.rowset_id, _num_segment);
        _tmp_segment_files.emplace_back(path);
    } else {
        // for update final merge scenario, we marked segments_overlap to NONOVERLAPPING in
        // function _final_merge, so we create segment data file here, rather than
        // temporary segment files.
        path = Rowset::segment_file_path(_context.rowset_path_prefix, _context.rowset_id, _num_segment);
    }
    ASSIGN_OR_RETURN(auto wfile, _fs->new_writable_file(path));
    const auto* schema = _context.tablet_schema;
    auto segment_writer = std::make_unique<SegmentWriter>(std::move(wfile), _num_segment, schema, _writer_options);
    RETURN_IF_ERROR(segment_writer->init());
    ++_num_segment;
    return std::move(segment_writer);
}

Status HorizontalRowsetWriter::add_chunk(const Chunk& chunk) {
    if (_segment_writer == nullptr) {
        ASSIGN_OR_RETURN(_segment_writer, _create_segment_writer());
    } else if (_segment_writer->estimate_segment_size() >= config::max_segment_file_size ||
               _segment_writer->num_rows_written() + chunk.num_rows() >= _context.max_rows_per_segment) {
        RETURN_IF_ERROR(_flush_segment_writer(&_segment_writer));
        ASSIGN_OR_RETURN(_segment_writer, _create_segment_writer());
    }

    RETURN_IF_ERROR(_segment_writer->append_chunk(chunk));
    _num_rows_written += static_cast<int64_t>(chunk.num_rows());
    _total_row_size += static_cast<int64_t>(chunk.bytes_usage());
    return Status::OK();
}

std::string HorizontalRowsetWriter::_flush_state_to_string() {
    switch (_flush_chunk_state) {
    case FlushChunkState::UNKNOWN:
        return "UNKNOWN";
    case FlushChunkState::UPSERT:
        return "UPSERT";
    case FlushChunkState::DELETE:
        return "DELETE";
    case FlushChunkState::MIXED:
        return "MIXED";
    default:
        return "ERROR FLUSH STATE";
    }
}

std::string HorizontalRowsetWriter::_error_msg() {
    std::string msg =
            strings::Substitute("UNKNOWN flush chunk state:$0, tablet:$1 txn:$2 #seg:$3 #delfile:$4 #upsert:$5 #del:$6",
                                _flush_state_to_string(), _context.tablet_id, _context.txn_id, _num_segment,
                                _num_delfile, _num_rows_written, _num_rows_del);
    LOG(WARNING) << msg;
    return msg;
}

Status HorizontalRowsetWriter::flush_chunk(const Chunk& chunk, SegmentPB* seg_info) {
    // 1. pure upsert
    // once upsert, subsequent flush can only do upsert
    switch (_flush_chunk_state) {
    case FlushChunkState::UNKNOWN:
        _flush_chunk_state = FlushChunkState::UPSERT;
        break;
    case FlushChunkState::UPSERT:
        break;
    case FlushChunkState::DELETE:
        _flush_chunk_state = FlushChunkState::MIXED;
        break;
    case FlushChunkState::MIXED:
        break;
    default:
        return Status::Cancelled(_error_msg());
    }
    return _flush_chunk(chunk, seg_info);
}

Status HorizontalRowsetWriter::_flush_chunk(const Chunk& chunk, SegmentPB* seg_info) {
    auto segment_writer = _create_segment_writer();
    if (!segment_writer.ok()) {
        return segment_writer.status();
    }
    RETURN_IF_ERROR((*segment_writer)->append_chunk(chunk));
    {
        std::lock_guard<std::mutex> l(_lock);
        _num_rows_written += static_cast<int64_t>(chunk.num_rows());
        _total_row_size += static_cast<int64_t>(chunk.bytes_usage());
    }
    if (seg_info) {
        seg_info->set_num_rows(static_cast<int64_t>(chunk.num_rows()));
        seg_info->set_row_size(static_cast<int64_t>(chunk.bytes_usage()));
    }
    return _flush_segment_writer(&segment_writer.value(), seg_info);
}

Status HorizontalRowsetWriter::flush_chunk_with_deletes(const Chunk& upserts, const Column& deletes,
                                                        SegmentPB* seg_info) {
    auto flush_del_file = [&](const Column& deletes, SegmentPB* seg_info) {
        ASSIGN_OR_RETURN(auto wfile, _fs->new_writable_file(Rowset::segment_del_file_path(
                                             _context.rowset_path_prefix, _context.rowset_id, _num_delfile)));
        size_t sz = serde::ColumnArraySerde::max_serialized_size(deletes);
        std::vector<uint8_t> content(sz);
        if (serde::ColumnArraySerde::serialize(deletes, content.data()) == nullptr) {
            return Status::InternalError("deletes column serialize failed");
        }
        RETURN_IF_ERROR(wfile->append(Slice(content.data(), content.size())));
        if (config::sync_tablet_meta) {
            RETURN_IF_ERROR(wfile->sync());
        }
        RETURN_IF_ERROR(wfile->close());
        if (seg_info) {
            seg_info->set_delete_num_rows(deletes.size());
            seg_info->set_delete_id(_num_delfile);
            seg_info->set_delete_data_size(content.size());
            seg_info->set_delete_path(wfile->filename());
        }
        _delfile_idxes.emplace_back(_num_segment + _num_delfile);
        _num_delfile++;
        _num_rows_del += deletes.size();
        return Status::OK();
    };
    // three flush states
    // 1. pure upsert, support multi-segment
    // 2. pure delete, support multi-segment
    // 3. mixed upsert/delete, do not support multi-segment
    if (!upserts.is_empty() && deletes.empty()) {
        return flush_chunk(upserts, seg_info);
    } else if (upserts.is_empty() && !deletes.empty()) {
        // 2. pure delete
        // once delete, subsequent flush can only do delete
        switch (_flush_chunk_state) {
        case FlushChunkState::UNKNOWN:
            _flush_chunk_state = FlushChunkState::DELETE;
            break;
        case FlushChunkState::DELETE:
            break;
        case FlushChunkState::UPSERT:
            _flush_chunk_state = FlushChunkState::MIXED;
        case FlushChunkState::MIXED:
            break;
        default:
            return Status::Cancelled(_error_msg());
        }
        RETURN_IF_ERROR(flush_del_file(deletes, seg_info));
        return Status::OK();
    } else if (!upserts.is_empty() && !deletes.empty()) {
        // 3. mixed upsert/delete, do not support multi-segment, check will there be multi-segment in the following _final_merge
        switch (_flush_chunk_state) {
        case FlushChunkState::UNKNOWN:
            _flush_chunk_state = FlushChunkState::MIXED;
            break;
        default:
            break;
        }
        RETURN_IF_ERROR(_flush_chunk(upserts, seg_info));
        RETURN_IF_ERROR(flush_del_file(deletes, seg_info));
        return Status::OK();
    } else {
        return Status::OK();
    }
}

Status HorizontalRowsetWriter::add_rowset(RowsetSharedPtr rowset) {
    RETURN_IF_ERROR(rowset->link_files_to(_context.rowset_path_prefix, _context.rowset_id));
    _num_rows_written += rowset->num_rows();
    _total_row_size += static_cast<int64_t>(rowset->total_row_size());
    _total_data_size += static_cast<int64_t>(rowset->rowset_meta()->data_disk_size());
    _total_index_size += static_cast<int64_t>(rowset->rowset_meta()->index_disk_size());
    _num_segment += static_cast<int>(rowset->num_segments());
    // TODO update zonemap
    if (rowset->rowset_meta()->has_delete_predicate()) {
        *_rowset_meta_pb->mutable_delete_predicate() = rowset->rowset_meta()->delete_predicate();
    }
    return Status::OK();
}

Status HorizontalRowsetWriter::add_rowset_for_linked_schema_change(RowsetSharedPtr rowset,
                                                                   const SchemaMapping& schema_mapping) {
    // TODO use schema_mapping to transfer zonemap
    return add_rowset(rowset);
}

Status HorizontalRowsetWriter::flush() {
    if (_segment_writer != nullptr) {
        return _flush_segment_writer(&_segment_writer);
    }
    return Status::OK();
}

StatusOr<RowsetSharedPtr> HorizontalRowsetWriter::build() {
    if (!_tmp_segment_files.empty()) {
        RETURN_IF_ERROR(_final_merge());
    }
    if (_vertical_rowset_writer) {
        return _vertical_rowset_writer->build();
    } else {
        // When building a rowset, we must ensure that the current _segment_writer has been
        // flushed, that is, the current _segment_writer is nullptr
        DCHECK(_segment_writer == nullptr) << "segment must be null when build rowset";
        return RowsetWriter::build();
    }
}

// final merge is still used for sorting schema change right now, so we keep the logic in RowsetWriter
// we may move this logic to `schema change` in near future, we can remove the following logic at that time
Status HorizontalRowsetWriter::_final_merge() {
    DCHECK(_context.schema_change_sorting);
    auto span = Tracer::Instance().start_trace_txn_tablet("final_merge", _context.txn_id, _context.tablet_id);
    auto scoped = trace::Scope(span);
    MonotonicStopWatch timer;
    timer.start();

    std::vector<std::shared_ptr<Segment>> segments;

    SegmentReadOptions seg_options;
    seg_options.fs = _fs;

    OlapReaderStatistics stats;
    seg_options.stats = &stats;

    for (int seg_id = 0; seg_id < _num_segment; ++seg_id) {
        if (_num_rows_of_tmp_segment_files[seg_id] == 0) {
            continue;
        }
        std::string tmp_segment_file =
                Rowset::segment_temp_file_path(_context.rowset_path_prefix, _context.rowset_id, seg_id);
        auto segment_ptr = Segment::open(_fs, tmp_segment_file, seg_id, _context.tablet_schema);
        if (!segment_ptr.ok()) {
            LOG(WARNING) << "Fail to open " << tmp_segment_file << ": " << segment_ptr.status();
            return segment_ptr.status();
        }
        segments.emplace_back(segment_ptr.value());
    }

    std::vector<ChunkIteratorPtr> seg_iterators;
    seg_iterators.reserve(segments.size());

    if (CompactionUtils::choose_compaction_algorithm(_context.tablet_schema->num_columns(),
                                                     config::vertical_compaction_max_columns_per_group,
                                                     segments.size()) == VERTICAL_COMPACTION) {
        std::vector<std::vector<uint32_t>> column_groups;
        if (_context.tablet_schema->sort_key_idxes().empty()) {
            std::vector<ColumnId> primary_key_iota_idxes(_context.tablet_schema->num_key_columns());
            std::iota(primary_key_iota_idxes.begin(), primary_key_iota_idxes.end(), 0);
            CompactionUtils::split_column_into_groups(_context.tablet_schema->num_columns(), primary_key_iota_idxes,
                                                      config::vertical_compaction_max_columns_per_group,
                                                      &column_groups);
        } else {
            CompactionUtils::split_column_into_groups(
                    _context.tablet_schema->num_columns(), _context.tablet_schema->sort_key_idxes(),
                    config::vertical_compaction_max_columns_per_group, &column_groups);
        }
        auto schema = ChunkHelper::convert_schema(*_context.tablet_schema, column_groups[0]);
        if (!_context.merge_condition.empty()) {
            for (int i = _context.tablet_schema->num_key_columns(); i < _context.tablet_schema->num_columns(); ++i) {
                if (_context.tablet_schema->schema()->field(i)->name() == _context.merge_condition) {
                    schema.append(_context.tablet_schema->schema()->field(i));
                    break;
                }
            }
        }

        for (const auto& segment : segments) {
            auto res = segment->new_iterator(schema, seg_options);
            if (!res.ok()) {
                return res.status();
            }
            seg_iterators.emplace_back(res.value());
        }

        TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(_context.tablet_id);
        RETURN_IF(tablet == nullptr, Status::InvalidArgument(fmt::format("Not Found tablet:{}", _context.tablet_id)));
        auto mask_buffer = std::make_unique<RowSourceMaskBuffer>(_context.tablet_id, tablet->data_dir()->path());
        auto source_masks = std::make_unique<std::vector<RowSourceMask>>();

        ChunkIteratorPtr itr;
        // create temporary segment files at first, then merge them and create final segment files if schema change with sorting
        if (_context.schema_change_sorting) {
            if (_context.tablet_schema->keys_type() == KeysType::DUP_KEYS ||
                _context.tablet_schema->keys_type() == KeysType::PRIMARY_KEYS) {
                itr = new_heap_merge_iterator(seg_iterators);
            } else if (_context.tablet_schema->keys_type() == KeysType::UNIQUE_KEYS ||
                       _context.tablet_schema->keys_type() == KeysType::AGG_KEYS) {
                itr = new_aggregate_iterator(new_heap_merge_iterator(seg_iterators), true);
            } else {
                return Status::NotSupported(
                        fmt::format("final merge: schema change with sorting do not support {} type",
                                    KeysType_Name(_context.tablet_schema->keys_type())));
            }
        } else {
            itr = new_aggregate_iterator(new_heap_merge_iterator(seg_iterators, _context.merge_condition), true);
        }
        itr->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS);

        _context.max_rows_per_segment = CompactionUtils::get_segment_max_rows(config::max_segment_file_size,
                                                                              _num_rows_written, _total_data_size);

        auto chunk_shared_ptr = ChunkHelper::new_chunk(schema, config::vector_chunk_size);
        auto chunk = chunk_shared_ptr.get();

        _num_segment = 0;
        _num_delfile = 0;
        _num_rows_written = 0;
        _num_rows_del = 0;
        _total_row_size = 0;
        _total_data_size = 0;
        _total_index_size = 0;

        // If RowsetWriter has final merge, it will produce new partial rowset footers and append them to partial_rowset_footers array,
        // but this array already have old entries, should clear those entries before write new segments for final merge.
        if (_rowset_txn_meta_pb) {
            _rowset_txn_meta_pb->clear_partial_rowset_footers();
        }

        // since the segment already NONOVERLAPPING here, make the _create_segment_writer
        // method to create segment data files, rather than temporary segment files.
        _context.segments_overlap = NONOVERLAPPING;

        _vertical_rowset_writer = std::make_unique<VerticalRowsetWriter>(_context);
        if (auto st = _vertical_rowset_writer->init(); !st.ok()) {
            std::stringstream ss;
            ss << "Fail to create rowset writer. tablet_id=" << _context.tablet_id << " err=" << st;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        auto char_field_indexes = ChunkHelper::get_char_field_indexes(schema);

        size_t total_rows = 0;
        size_t total_chunk = 0;
        while (true) {
            chunk->reset();
            auto st = itr->get_next(chunk, source_masks.get());
            if (st.is_end_of_file()) {
                break;
            } else if (st.ok()) {
                ChunkHelper::padding_char_columns(char_field_indexes, schema, *_context.tablet_schema, chunk);
                total_rows += chunk->num_rows();
                total_chunk++;
                if (auto st = _vertical_rowset_writer->add_columns(*chunk, column_groups[0], true); !st.ok()) {
                    LOG(WARNING) << "writer add_columns error. tablet=" << _context.tablet_id << ", err=" << st;
                    return st;
                }
            } else {
                return st;
            }
            if (!source_masks->empty()) {
                RETURN_IF_ERROR(mask_buffer->write(*source_masks));
                source_masks->clear();
            }
        }
        itr->close();
        if (auto st = _vertical_rowset_writer->flush_columns(); !st.ok()) {
            LOG(WARNING) << "failed to flush_columns group, tablet=" << _context.tablet_id << ", err=" << st;
            return st;
        }
        RETURN_IF_ERROR(mask_buffer->flush());

        for (size_t i = 1; i < column_groups.size(); ++i) {
            mask_buffer->flip_to_read();

            seg_iterators.clear();

            auto schema = ChunkHelper::convert_schema(*_context.tablet_schema, column_groups[i]);

            for (const auto& segment : segments) {
                auto res = segment->new_iterator(schema, seg_options);
                if (!res.ok()) {
                    return res.status();
                }
                seg_iterators.emplace_back(res.value());
            }

            ChunkIteratorPtr itr;
            // create temporary segment files at first, then merge them and create final segment files if schema change with sorting
            if (_context.schema_change_sorting) {
                if (_context.tablet_schema->keys_type() == KeysType::DUP_KEYS ||
                    _context.tablet_schema->keys_type() == KeysType::PRIMARY_KEYS) {
                    itr = new_mask_merge_iterator(seg_iterators, mask_buffer.get());
                } else if (_context.tablet_schema->keys_type() == KeysType::UNIQUE_KEYS ||
                           _context.tablet_schema->keys_type() == KeysType::AGG_KEYS) {
                    itr = new_aggregate_iterator(new_mask_merge_iterator(seg_iterators, mask_buffer.get()), false);
                } else {
                    return Status::NotSupported(
                            fmt::format("final merge: schema change with sorting do not support {} type",
                                        KeysType_Name(_context.tablet_schema->keys_type())));
                }
            } else {
                itr = new_aggregate_iterator(new_mask_merge_iterator(seg_iterators, mask_buffer.get()), false);
            }
            itr->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS);

            auto chunk_shared_ptr = ChunkHelper::new_chunk(schema, config::vector_chunk_size);
            auto chunk = chunk_shared_ptr.get();

            auto char_field_indexes = ChunkHelper::get_char_field_indexes(schema);

            while (true) {
                chunk->reset();
                auto st = itr->get_next(chunk, source_masks.get());
                if (st.is_end_of_file()) {
                    break;
                } else if (st.ok()) {
                    ChunkHelper::padding_char_columns(char_field_indexes, schema, *_context.tablet_schema, chunk);
                    if (auto st = _vertical_rowset_writer->add_columns(*chunk, column_groups[i], false); !st.ok()) {
                        LOG(WARNING) << "writer add_columns error. tablet=" << _context.tablet_id << ", err=" << st;
                        return st;
                    }
                } else {
                    return st;
                }
                if (!source_masks->empty()) {
                    source_masks->clear();
                }
            }
            itr->close();
            if (auto st = _vertical_rowset_writer->flush_columns(); !st.ok()) {
                LOG(WARNING) << "failed to flush_columns group, tablet=" << _context.tablet_id << ", err=" << st;
                return st;
            }
        }

        if (auto st = _vertical_rowset_writer->final_flush(); !st.ok()) {
            LOG(WARNING) << "failed to final flush rowset when final merge " << _context.tablet_id << ", err=" << st;
            return st;
        }
        timer.stop();
        LOG(INFO) << "rowset writer vertical final merge finished. tablet:" << _context.tablet_id
                  << " #key:" << _context.tablet_schema->num_key_columns() << " input("
                  << "entry=" << seg_iterators.size() << " rows=" << stats.raw_rows_read
                  << " bytes=" << PrettyPrinter::print(stats.bytes_read, TUnit::UNIT) << ") output(rows=" << total_rows
                  << " chunk=" << total_chunk << " bytes=" << PrettyPrinter::print(total_data_size(), TUnit::UNIT)
                  << ") duration: " << timer.elapsed_time() / 1000000 << "ms";
    } else {
        auto schema = ChunkHelper::convert_schema(*_context.tablet_schema);

        for (const auto& segment : segments) {
            auto res = segment->new_iterator(schema, seg_options);
            if (!res.ok()) {
                return res.status();
            }
            seg_iterators.emplace_back(res.value());
        }

        ChunkIteratorPtr itr;
        // create temporary segment files at first, then merge them and create final segment files if schema change with sorting
        if (_context.schema_change_sorting) {
            if (_context.tablet_schema->keys_type() == KeysType::DUP_KEYS ||
                _context.tablet_schema->keys_type() == KeysType::PRIMARY_KEYS) {
                itr = new_heap_merge_iterator(seg_iterators);
            } else if (_context.tablet_schema->keys_type() == KeysType::UNIQUE_KEYS ||
                       _context.tablet_schema->keys_type() == KeysType::AGG_KEYS) {
                itr = new_aggregate_iterator(new_heap_merge_iterator(seg_iterators), 0);
            } else {
                return Status::NotSupported(
                        fmt::format("final merge: schema change with sorting do not support {} type",
                                    KeysType_Name(_context.tablet_schema->keys_type())));
            }
            _context.schema_change_sorting = false;
        } else {
            itr = new_aggregate_iterator(new_heap_merge_iterator(seg_iterators, _context.merge_condition), 0);
        }
        itr->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS);

        auto chunk_shared_ptr = ChunkHelper::new_chunk(schema, config::vector_chunk_size);
        auto chunk = chunk_shared_ptr.get();

        _num_segment = 0;
        _num_delfile = 0;
        _num_rows_written = 0;
        _num_rows_del = 0;
        _total_row_size = 0;
        _total_data_size = 0;
        _total_index_size = 0;

        // If RowsetWriter has final merge, it will produce new partial rowset footers and append them to partial_rowset_footers array,
        // but this array already have old entries, should clear those entries before write new segments for final merge.
        if (_rowset_txn_meta_pb) {
            _rowset_txn_meta_pb->clear_partial_rowset_footers();
        }

        // since the segment already NONOVERLAPPING here, make the _create_segment_writer
        // method to create segment data files, rather than temporary segment files.
        _context.segments_overlap = NONOVERLAPPING;

        auto char_field_indexes = ChunkHelper::get_char_field_indexes(schema);

        size_t total_rows = 0;
        size_t total_chunk = 0;
        while (true) {
            chunk->reset();
            auto st = itr->get_next(chunk);
            if (st.is_end_of_file()) {
                break;
            } else if (st.ok()) {
                ChunkHelper::padding_char_columns(char_field_indexes, schema, *_context.tablet_schema, chunk);
                total_rows += chunk->num_rows();
                total_chunk++;
                if (auto st = add_chunk(*chunk); !st.ok()) {
                    LOG(WARNING) << "writer add_chunk error: " << st;
                    return st;
                }
            } else {
                return st;
            }
        }
        itr->close();
        if (auto st = flush(); !st.ok()) {
            LOG(WARNING) << "failed to flush, tablet=" << _context.tablet_id << ", err=" << st;
            return st;
        }

        timer.stop();
        LOG(INFO) << "rowset writer horizontal final merge finished. tablet:" << _context.tablet_id
                  << " #key:" << _context.tablet_schema->num_key_columns() << " input("
                  << "entry=" << seg_iterators.size() << " rows=" << stats.raw_rows_read
                  << " bytes=" << PrettyPrinter::print(stats.bytes_read, TUnit::UNIT) << ") output(rows=" << total_rows
                  << " chunk=" << total_chunk << " bytes=" << PrettyPrinter::print(total_data_size(), TUnit::UNIT)
                  << ") duration: " << timer.elapsed_time() / 1000000 << "ms";
    }

    span->SetAttribute("output_bytes", total_data_size());

    for (const auto& tmp_segment_file : _tmp_segment_files) {
        auto st = _fs->delete_file(tmp_segment_file);
        LOG_IF(WARNING, !(st.ok() || st.is_not_found()))
                << "Fail to delete segment temp file=" << tmp_segment_file << ", " << st.to_string();
    }
    _tmp_segment_files.clear();

    return Status::OK();
}

Status HorizontalRowsetWriter::_flush_segment_writer(std::unique_ptr<SegmentWriter>* segment_writer,
                                                     SegmentPB* seg_info) {
    uint64_t segment_size = 0;
    uint64_t index_size = 0;
    uint64_t footer_position = 0;
    RETURN_IF_ERROR((*segment_writer)->finalize(&segment_size, &index_size, &footer_position));
    _num_rows_of_tmp_segment_files.push_back(_num_rows_written - _num_rows_flushed);
    _num_rows_flushed = _num_rows_written;
    if (_context.tablet_schema->keys_type() == KeysType::PRIMARY_KEYS && _context.partial_update_tablet_schema) {
        uint64_t footer_size = segment_size - footer_position;
        auto* partial_rowset_footer = _rowset_txn_meta_pb->add_partial_rowset_footers();
        partial_rowset_footer->set_position(footer_position);
        partial_rowset_footer->set_size(footer_size);
        if (seg_info) {
            seg_info->set_partial_footer_position(footer_position);
            seg_info->set_partial_footer_size(footer_size);
        }
    }
    {
        std::lock_guard<std::mutex> l(_lock);
        _total_data_size += static_cast<int64_t>(segment_size);
        _total_index_size += static_cast<int64_t>(index_size);
    }

    // check global_dict efficacy
    const auto& seg_global_dict_columns_valid_info = (*segment_writer)->global_dict_columns_valid_info();
    for (const auto& it : seg_global_dict_columns_valid_info) {
        if (!it.second) {
            _global_dict_columns_valid_info[it.first] = false;
        } else {
            if (const auto& iter = _global_dict_columns_valid_info.find(it.first);
                iter == _global_dict_columns_valid_info.end()) {
                _global_dict_columns_valid_info[it.first] = true;
            }
        }
    }

    if (seg_info) {
        seg_info->set_data_size(segment_size);
        seg_info->set_index_size(index_size);
        seg_info->set_segment_id((*segment_writer)->segment_id());
        seg_info->set_path((*segment_writer)->segment_path());
    }

    (*segment_writer).reset();
    return Status::OK();
}

VerticalRowsetWriter::VerticalRowsetWriter(const RowsetWriterContext& context) : RowsetWriter(context) {}

VerticalRowsetWriter::~VerticalRowsetWriter() {
    if (!_already_built) {
        for (auto& segment_writer : _segment_writers) {
            segment_writer.reset();
        }

        for (int i = 0; i < _num_segment; ++i) {
            auto path = Rowset::segment_file_path(_context.rowset_path_prefix, _context.rowset_id, i);
            auto st = _fs->delete_file(path);
            LOG_IF(WARNING, !(st.ok() || st.is_not_found()))
                    << "Fail to delete file=" << path << ", " << st.to_string();
        }
        // if _already_built is false, we need to release rowset_id to avoid rowset_id leak
        StorageEngine::instance()->release_rowset_id(_context.rowset_id);
    }
}

Status VerticalRowsetWriter::add_columns(const Chunk& chunk, const std::vector<uint32_t>& column_indexes, bool is_key) {
    const size_t chunk_num_rows = chunk.num_rows();
    if (_segment_writers.empty()) {
        DCHECK(is_key);
        auto segment_writer = _create_segment_writer(column_indexes, is_key);
        if (!segment_writer.ok()) return segment_writer.status();
        _segment_writers.emplace_back(std::move(segment_writer).value());
        _current_writer_index = 0;
        RETURN_IF_ERROR(_segment_writers[_current_writer_index]->append_chunk(chunk));
    } else if (is_key) {
        // key columns
        if (_segment_writers[_current_writer_index]->num_rows_written() + chunk_num_rows >=
            _context.max_rows_per_segment) {
            RETURN_IF_ERROR(_flush_columns(&_segment_writers[_current_writer_index]));
            auto segment_writer = _create_segment_writer(column_indexes, is_key);
            if (!segment_writer.ok()) return segment_writer.status();
            _segment_writers.emplace_back(std::move(segment_writer).value());
            ++_current_writer_index;
        }
        RETURN_IF_ERROR(_segment_writers[_current_writer_index]->append_chunk(chunk));
    } else {
        // non key columns
        uint32_t num_rows_written = _segment_writers[_current_writer_index]->num_rows_written();
        uint32_t segment_num_rows = _segment_writers[_current_writer_index]->num_rows();
        DCHECK_LE(num_rows_written, segment_num_rows);

        if (_current_writer_index == 0 && num_rows_written == 0) {
            RETURN_IF_ERROR(_segment_writers[_current_writer_index]->init(column_indexes, is_key));
        }

        if (num_rows_written + chunk_num_rows <= segment_num_rows) {
            RETURN_IF_ERROR(_segment_writers[_current_writer_index]->append_chunk(chunk));
        } else {
            // split into multi chunks and write into multi segments
            auto write_chunk = chunk.clone_empty();
            size_t num_left_rows = chunk_num_rows;
            size_t offset = 0;
            while (num_left_rows > 0) {
                if (segment_num_rows == num_rows_written) {
                    RETURN_IF_ERROR(_flush_columns(&_segment_writers[_current_writer_index]));
                    ++_current_writer_index;
                    RETURN_IF_ERROR(_segment_writers[_current_writer_index]->init(column_indexes, is_key));
                    num_rows_written = _segment_writers[_current_writer_index]->num_rows_written();
                    segment_num_rows = _segment_writers[_current_writer_index]->num_rows();
                }

                size_t write_size = segment_num_rows - num_rows_written;
                if (write_size > num_left_rows) {
                    write_size = num_left_rows;
                }
                write_chunk->append(chunk, offset, write_size);
                RETURN_IF_ERROR(_segment_writers[_current_writer_index]->append_chunk(*write_chunk));
                write_chunk->reset();
                num_left_rows -= write_size;
                offset += write_size;
                num_rows_written = _segment_writers[_current_writer_index]->num_rows_written();
            }
            DCHECK_EQ(0, num_left_rows);
            DCHECK_EQ(offset, chunk_num_rows);
        }
    }

    if (is_key) {
        _num_rows_written += static_cast<int64_t>(chunk_num_rows);
    }
    _total_row_size += static_cast<int64_t>(chunk.bytes_usage());
    return Status::OK();
}

Status VerticalRowsetWriter::flush_columns() {
    if (_segment_writers.empty()) {
        return Status::OK();
    }

    DCHECK(_segment_writers[_current_writer_index]);
    RETURN_IF_ERROR(_flush_columns(&_segment_writers[_current_writer_index]));
    _current_writer_index = 0;
    return Status::OK();
}

Status VerticalRowsetWriter::final_flush() {
    for (auto& segment_writer : _segment_writers) {
        uint64_t segment_size = 0;
        uint64_t footer_position = 0;
        if (auto st = segment_writer->finalize_footer(&segment_size, &footer_position); !st.ok()) {
            LOG(WARNING) << "Fail to finalize segment footer, " << st;
            return st;
        }
        if (_context.tablet_schema->keys_type() == KeysType::PRIMARY_KEYS && _context.partial_update_tablet_schema) {
            auto* partial_rowset_footer = _rowset_txn_meta_pb->add_partial_rowset_footers();
            partial_rowset_footer->set_position(footer_position);
            partial_rowset_footer->set_size(segment_size - footer_position);
        }
        {
            std::lock_guard<std::mutex> l(_lock);
            _total_data_size += static_cast<int64_t>(segment_size);
        }

        // check global_dict efficacy
        const auto& seg_global_dict_columns_valid_info = segment_writer->global_dict_columns_valid_info();
        for (const auto& it : seg_global_dict_columns_valid_info) {
            if (!it.second) {
                _global_dict_columns_valid_info[it.first] = false;
            } else {
                if (const auto& iter = _global_dict_columns_valid_info.find(it.first);
                    iter == _global_dict_columns_valid_info.end()) {
                    _global_dict_columns_valid_info[it.first] = true;
                }
            }
        }

        segment_writer.reset();
    }
    return Status::OK();
}

StatusOr<std::unique_ptr<SegmentWriter>> VerticalRowsetWriter::_create_segment_writer(
        const std::vector<uint32_t>& column_indexes, bool is_key) {
    std::lock_guard<std::mutex> l(_lock);
    ASSIGN_OR_RETURN(auto wfile, _fs->new_writable_file(Rowset::segment_file_path(_context.rowset_path_prefix,
                                                                                  _context.rowset_id, _num_segment)));
    const auto* schema = _context.tablet_schema;
    auto segment_writer = std::make_unique<SegmentWriter>(std::move(wfile), _num_segment, schema, _writer_options);
    RETURN_IF_ERROR(segment_writer->init(column_indexes, is_key));
    ++_num_segment;
    return std::move(segment_writer);
}

Status VerticalRowsetWriter::_flush_columns(std::unique_ptr<SegmentWriter>* segment_writer) {
    uint64_t index_size = 0;
    RETURN_IF_ERROR((*segment_writer)->finalize_columns(&index_size));
    {
        std::lock_guard<std::mutex> l(_lock);
        _total_index_size += static_cast<int64_t>(index_size);
    }
    return Status::OK();
}

} // namespace starrocks
