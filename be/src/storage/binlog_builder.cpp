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

#include "storage/binlog_builder.h"

#include <utility>

#include "util/filesystem_util.h"

namespace starrocks {

BinlogBuilder::BinlogBuilder(int64_t tablet_id, int64_t version, int64_t change_event_timestamp,
                             BinlogBuilderParamsPtr& params)
        : _tablet_id(tablet_id),
          _version(version),
          _change_event_timestamp(change_event_timestamp),
          _params(params),
          _next_file_id(params->start_file_id),
          _init_writer(false),
          _next_seq_id(0) {}

Status BinlogBuilder::add_empty() {
    RETURN_IF_ERROR(_switch_writer_if_full());
    return _current_writer->add_empty();
}

Status BinlogBuilder::add_insert_range(const RowsetSegInfo& seg_info, int32_t start_row_id, int32_t num_rows) {
    RETURN_IF_ERROR(_switch_writer_if_full());
    return _current_writer->add_insert_range(seg_info, 0, num_rows);
}

Status BinlogBuilder::commit(BinlogBuildResult* result) {
    // TODO sync the parent dir if new files are created
    RETURN_IF_ERROR(_commit_current_writer(true));
    if (_current_writer->file_size() > _params->max_file_size) {
        // ignore close failure for the last writer because
        // all binlog data has been committed (persisted)
        (void)_close_current_writer();
        _current_writer.reset();
    }

    result->params = _params;
    result->next_file_id = _next_file_id;
    result->active_writer = std::move(_current_writer);
    result->metas = std::move(_new_metas);

    return Status::OK();
}

void BinlogBuilder::abort(BinlogBuildResult* result) {
    // 1. cleanup resource including abort/close the current writer, and
    //    deleting newly created binlog files
    bool fail_delete_new_files = false;
    if (!_new_files.empty()) {
        WARN_IF_ERROR(_close_current_writer(), "close writer failed");
        Status st = BinlogBuilder::delete_binlog_files(_new_files);
        fail_delete_new_files = !st.ok();
    } else if (_current_writer != nullptr) {
        // current writer must be from _params.active_writer
        DCHECK_EQ(_params->active_file_meta->id(), _current_writer->file_id());
        // fail to add_xxx/build, and should abort it first
        if (_current_writer->is_writing()) {
            Status status = _abort_current_writer();
            if (!status.ok()) {
                WARN_IF_ERROR(_close_current_writer(), "close writer failed");
            }
        }
    }

    // 2. decide whether the binlog file of _params.active_file_meta can be used for the next ingestion
    //   2.1 _params.active_writer is not closed which indicates it's in normal state, and can be reused
    //   2.2 _params.active_writer is closed
    //     2.2.1 if fail to delete newly created files, just truncate the active file, but not reuse
    //       it. Publish process for a version of ingestion may be retried, and the same binlog data can
    //       be written repeatedly. If failed to delete files here, there may be duplicate data in multiple
    //       binlog files after next publish retry is successful. Not reusing the active file will ensure
    //       the newest and valid data is always located in the file with larger file id, so that we can
    //       filter the duplicated data if already find it in the file with a larger file id when loading
    //       binlog file metas from disk
    //     2.2.2 if newly created files are deleted successfully, can reopen the writer, and reuse it in
    //       the next ingestion. reopen includes the truncate operation
    if (_params->active_file_writer != nullptr) {
        DCHECK(_params->active_file_meta != nullptr);
        if (!_params->active_file_writer->is_closed()) {
            // step 2.1
            result->active_writer = _params->active_file_writer;
        } else if (fail_delete_new_files) {
            // step 2.2.1
            Status status = FileSystemUtil::resize_file(_params->active_file_writer->file_path(),
                                                        _params->active_file_meta->file_size());
            LOG_IF(WARNING, !status.ok())
                    << "Fail to resize the active file when aborting binlog builder, tablet: " << _tablet_id
                    << ", version: " << _version << ", the active file: " << _params->active_file_writer->file_path()
                    << ", " << status;
        } else {
            // step 2.2.2
            BinlogFileWriter* old_writer = _params->active_file_writer.get();
            StatusOr<BinlogFileWriterPtr> status_or =
                    BinlogFileWriter::reopen(old_writer->file_id(), old_writer->file_path(), _params->max_page_size,
                                             _params->compression_type, _params->active_file_meta.get());
            if (status_or.ok()) {
                result->active_writer = status_or.value();
            } else {
                LOG(WARNING) << "Fail to reopen the active file when aborting binlog builder, tablet: " << _tablet_id
                             << ", version: " << _version
                             << ", the active file: " << _params->active_file_writer->file_path() << ", "
                             << status_or.status();
            }
        }
    }

    // 3. set other members
    result->params = _params;
    result->next_file_id = _next_file_id;
}

Status BinlogBuilder::_switch_writer_if_full() {
    if (_current_writer != nullptr && _current_writer->file_size() < _params->max_file_size) {
        return Status::OK();
    }

    if (_current_writer != nullptr) {
        RETURN_IF_ERROR(_commit_current_writer(false));
        // for immediate writers, not tolerate close failure so that the
        // binlog file meta is appended as the footer
        RETURN_IF_ERROR(_close_current_writer());
    }

    BinlogFileWriterPtr writer;
    if (!_init_writer && _params->active_file_writer != nullptr) {
        _init_writer = true;
        writer = _params->active_file_writer;
        VLOG(3) << "Init and reuse active writer, tablet: " << _tablet_id << ", file path: " << writer->file_path();
    } else {
        ASSIGN_OR_RETURN(writer, _create_binlog_writer());
        _new_files.push_back(writer->file_path());
        VLOG(3) << "Reuse active writer, tablet: " << _tablet_id << ", file path: " << writer->file_path();
    }
    _current_writer = std::move(writer);
    RETURN_IF_ERROR(_current_writer->begin(_version, _next_seq_id, _change_event_timestamp));
    return Status::OK();
}

Status BinlogBuilder::_commit_current_writer(bool end_of_version) {
    DCHECK(_current_writer != nullptr && _current_writer->is_writing());
    VLOG(3) << "Commit current writer, tablet: " << _tablet_id << ", file path: " << _current_writer->file_path()
            << ", end_of_version: " << end_of_version;
    Status status = _current_writer->commit(end_of_version);
    if (!status.ok()) {
        LOG(WARNING) << "Fail to commit binlog writer, tablet: " << _tablet_id
                     << ", file path: " << _current_writer->file_path() << ", version " << _version << ", " << status;
        return status;
    }
    std::shared_ptr<BinlogFileMetaPB> file_meta = std::make_shared<BinlogFileMetaPB>();
    _current_writer->copy_file_meta(file_meta.get());
    _new_metas.push_back(file_meta);
    _next_seq_id = file_meta->end_seq_id() + 1;
    return Status::OK();
}

Status BinlogBuilder::_abort_current_writer() {
    DCHECK(_current_writer != nullptr && _current_writer->is_writing());
    VLOG(3) << "Abort current writer, tablet: " << _tablet_id << ", file path: " << _current_writer->file_path();
    Status status = _current_writer->abort();
    LOG_IF(WARNING, !status.ok()) << "Fail to abort binlog writer, tablet: " << _tablet_id
                                  << ", file path: " << _current_writer->file_path() << ", " << status;
    return status;
}

Status BinlogBuilder::_close_current_writer() {
    DCHECK(_current_writer != nullptr);
    VLOG(3) << "Close current writer, tablet: " << _tablet_id << ", file path: " << _current_writer->file_path();
    Status status = _current_writer->close(true);
    LOG_IF(WARNING, !status.ok()) << "Fail to close binlog writer after committed, tablet: " << _tablet_id
                                  << ", file path: " << _current_writer->file_path() << ", " << status;
    return status;
}

StatusOr<BinlogFileWriterPtr> BinlogBuilder::_create_binlog_writer() {
    int64_t file_id = _next_file_id++;
    std::string file_path = BinlogUtil::binlog_file_path(_params->binlog_storage_path, file_id);
    BinlogFileWriterPtr binlog_writer =
            std::make_shared<BinlogFileWriter>(file_id, file_path, _params->max_page_size, _params->compression_type);
    Status status = binlog_writer->init();
    if (status.ok()) {
        return binlog_writer;
    }
    LOG(WARNING) << "Fail to initialize binlog writer, tablet: " << _tablet_id << ", file path: " << file_path << ", "
                 << status;
    Status st = binlog_writer->close(false);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to close binlog writer, tablet: " << _tablet_id << ", file path: " << file_path << ", "
                     << status;
    }

    std::shared_ptr<FileSystem> fs;
    ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(file_path))
    st = fs->delete_file(file_path);
    if (st.ok()) {
        LOG(INFO) << "Delete binlog file after creating failed, tablet: " << _tablet_id << ", file path: " << file_path;
    } else {
        LOG(WARNING) << "Fail to delete binlog file after creating failed, tablet: " << _tablet_id
                     << ", file path: " << file_path << ", " << st;
    }

    return status;
}

Status BinlogBuilder::delete_binlog_files(std::vector<std::string>& file_paths) {
    if (file_paths.empty()) {
        return Status::OK();
    }

    std::shared_ptr<FileSystem> fs;
    ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(file_paths[0]))
    int32_t total_num = 0;
    int32_t fail_num = 0;
    for (auto& path : file_paths) {
        total_num += 1;
        // TODO use path gc to clean up binlog files if fail to delete
        Status st = fs->delete_file(path);
        if (st.ok()) {
            VLOG(3) << "Delete binlog file " << path;
        } else {
            LOG(WARNING) << "Fail to delete binlog file " << path << ", " << st;
            fail_num += 1;
        }
    }

    if (fail_num > 0) {
        return Status::InternalError(
                fmt::format("Fail to delete some of new binlog files, total num: {},"
                            " failed num: {}",
                            total_num, fail_num));
    } else {
        return Status::OK();
    }
}

BinlogFileWriterPtr BinlogBuilder::discard_binlog_build_result(int64_t version, BinlogBuildResult& result) {
    BinlogBuilderParams* params = result.params.get();
    BinlogFileWriter* prev_active_writer = params->active_file_writer.get();
    BinlogFileWriter* current_active_writer = result.active_writer.get();

    bool prev_writer_still_active = prev_active_writer != nullptr && current_active_writer != nullptr &&
                                    (prev_active_writer->file_id() == current_active_writer->file_id());

    // 1. deal with new files created in this ingestion

    // 1.1 close the active writer if it's newly created in this ingestion
    if (current_active_writer != nullptr && !prev_writer_still_active) {
        Status st = current_active_writer->close(false);
        LOG_IF(WARNING, !st.ok()) << "Fail to close newly created active writer for version " << version
                                  << ", when discarding binlog, " << current_active_writer->file_path() << ", " << st;
    }

    // 1.2 delete newly created files
    std::vector<std::string> files_to_delete;
    int32_t start_index = prev_active_writer == nullptr ? 0 : 1;
    for (int i = start_index; i < result.metas.size(); i++) {
        files_to_delete.push_back(BinlogUtil::binlog_file_path(params->binlog_storage_path, result.metas[i]->id()));
    }
    Status st = BinlogBuilder::delete_binlog_files(files_to_delete);
    LOG_IF(WARNING, !st.ok()) << "Fail to delete binlog files for version " << version
                              << " when discarding binlog under " << params->binlog_storage_path << ", " << st;
    bool fail_delete_new_files = !st.ok();

    // 2. deal with the previous active writer, and decide whether the binlog fie can be reused
    std::shared_ptr<BinlogFileWriter> reused_writer;
    if (prev_writer_still_active) {
        // 2.1 reset the writer to the previous meta for reuse
        st = current_active_writer->reset(params->active_file_meta.get());
        if (!st.ok()) {
            WARN_IF_ERROR(current_active_writer->close(false), "Fail to close active writer");
            LOG(WARNING) << "Fail to reset active writer when discarding data for version " << version
                         << ", file path: " << current_active_writer->file_path() << ", " << st;
        } else {
            // reuse the writer
            reused_writer = result.active_writer;
        }
    } else if (prev_active_writer != nullptr) {
        if (fail_delete_new_files) {
            // 2.2 just resize the file but not reuse it, and refer to BinlogBuilder#abort
            // step 2.2.1 for the reason
            st = FileSystemUtil::resize_file(prev_active_writer->file_path(), params->active_file_meta->file_size());
            LOG_IF(WARNING, !st.ok()) << "Fail to resize the active file when discarding data for version: " << version
                                      << ", file path: " << prev_active_writer->file_path() << ", " << st;
        } else {
            // 2.3 previous writer already closed, and reopen it for reuse
            StatusOr<BinlogFileWriterPtr> status_or = BinlogFileWriter::reopen(
                    prev_active_writer->file_id(), prev_active_writer->file_path(), params->max_page_size,
                    params->compression_type, params->active_file_meta.get());
            if (!status_or.ok()) {
                LOG(WARNING) << "Fail to reopen writer when discarding data for version " << version
                             << ", file path: " << prev_active_writer->file_path() << ", " << st;
            } else {
                // reuse the writer
                reused_writer = status_or.value();
            }
        }
    }

    return reused_writer;
}

Status BinlogBuilder::build_duplicate_key(int64_t tablet_id, int64_t version, const RowsetSharedPtr& rowset,
                                          BinlogBuilderParamsPtr& builder_params, BinlogBuildResult* result) {
    std::shared_ptr<BinlogBuilder> builder =
            std::make_shared<BinlogBuilder>(tablet_id, version, rowset->creation_time() * 1000000, builder_params);
    Status status;
    if (rowset->num_rows() == 0) {
        status = builder->add_empty();
    } else {
        RowsetSegInfo seg_info(version, 0);
        std::vector<SegmentSharedPtr>& segments = rowset->segments();
        for (int32_t seg_index = 0; seg_index < rowset->num_segments(); seg_index++) {
            int num_rows = segments[seg_index]->num_rows();
            if (num_rows == 0) {
                continue;
            }
            seg_info.seg_index = seg_index;
            status = builder->add_insert_range(seg_info, 0, num_rows);
            if (!status.ok()) {
                LOG(WARNING) << "Fail to add_insert_range for duplicate key, tablet: " << tablet_id
                             << ", rowset: " << rowset->rowset_id() << ", segment index: " << seg_index
                             << ", number of rows " << num_rows << ", version: " << version << ", " << status;
                break;
            }
        }
    }

    if (status.ok()) {
        status = builder->commit(result);
    }

    if (!status.ok()) {
        builder->abort(result);
        LOG(WARNING) << "Abort the binlog builder for duplicate key, tablet: " << tablet_id << ", version: " << version
                     << ", " << status;
    }

    return status;
}

} // namespace starrocks