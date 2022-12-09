// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "binlog_manager.h"

#include "storage/rowset/page_io.h"
#include "storage/tablet.h"
#include "util/crc32c.h"

namespace starrocks {

Status BinlogManager::create_and_init(Tablet& tablet, std::shared_ptr<BinlogManager>* binlog_manager) {
    *binlog_manager =
            std::make_shared<BinlogManager>(tablet.schema_hash_path(), config::binlog_file_max_size,
                                            config::binlog_page_max_size, tablet.tablet_schema().compression_type());
    Status st = (*binlog_manager)->init(tablet);
    LOG_IF(WARNING, !st.ok()) << "Failed to init binlog for tablet " << tablet.full_name() << " : " << st;
    return st;
}

BinlogManager::BinlogManager(std::string path, int64_t max_file_size, int32_t max_page_size,
                             CompressionTypePB compression_type)
        : _path(std::move(path)),
          _max_file_size(max_file_size),
          _max_page_size(max_page_size),
          _compression_type(compression_type) {}

Status BinlogManager::init(Tablet& tablet) {
    // TODO init binlog manager
    // 1. load wal mate and binlog file metas
    // 2. initialize _shared_rowsets and _private_rowsets
    //    2.1 for duplicate model, iterate _rs_version_map,
    //        _inc_rs_version_map and _stale_rs_version_map in Tablet
    //    2.2 for primary key model, iterate _rowsets, _pending_commits
    //        and _unused_rowsets in TabletUpdates
    // 3. load binlog metas
    // 4. rollback unused tablet version
    // 5. reuse last binlog file
}

Status BinlogManager::add_insert_rowset(RowsetSharedPtr rowset) {
    // TODO maybe block publish RPC
    std::lock_guard lock(_write_lock);
    if (!_binlog_file_metas.empty()) {
        BinlogFileMetaPBSharedPtr file_meta = _binlog_file_metas.rbegin()->second;
        if (file_meta->end_tablet_version() >= rowset->start_version()) {
            LOG(INFO) << "Skip to generate binlog for rowset " << rowset->rowset_id() << ", version "
                      << rowset->start_version() << ", binlog max version " << file_meta->end_tablet_version()
                      << ". This maybe happen if versions are published out of order.";
            return Status::OK();
        }
    }

    std::vector<std::shared_ptr<BinlogFileWriter>> pending_writers;
    shared_ptr<BinlogFileWriter> current_writer;
    if (_active_binlog_writer != nullptr && _active_binlog_writer->committed_file_size() < _max_file_size) {
        current_writer = _active_binlog_writer;
        pending_writers.emplace_back(current_writer);
    } else if (_active_binlog_writer != nullptr) {
        Status status = _active_binlog_writer->close();
        // close failure does not affect new binlog, so ignore it
        if (!status.ok()) {
            LOG(WARNING) << "Fail to close file writer when inserting new rowset " << rowset->rowset_id()
                         << ", version " << rowset->version() << ", binlog file id " << _active_binlog_writer->file_id()
                         << ", binlog file name " << _active_binlog_writer->file_name() << ", " << status;
        }
        _active_binlog_writer.reset();
    }

    int64_t next_file_id = _binlog_file_metas.empty() ? 0 : _binlog_file_metas.rbegin()->second->id() + 1;
    Status status = Status::OK();
    if (current_writer != nullptr) {
        status = current_writer->prepare(rowset->start_version(), rowset->rowset_id(), 0, 0, rowset->creation_time());
        if (!status.ok()) {
            current_writer->abort();
            LOG(WARNING) << "Fail to prepare binlog writer for rowset " << rowset->rowset_id() << ", version "
                         << rowset->start_version() << ", binlog writer " << current_writer->file_name() << ", "
                         << status;
            return Status::InternalError(fmt::format("Fail to generate binlog for rowset {}, version {}",
                                                     rowset->rowset_id().to_string(), rowset->start_version()));
        }
    }

    std::vector<SegmentSharedPtr>& segments = rowset->segments();
    for (int32_t seg_index = 0; seg_index < rowset->num_segments(); seg_index++) {
        int num_rows = segments[seg_index]->num_rows();
        if (num_rows == 0) {
            continue;
        }

        if (current_writer == nullptr || current_writer->pending_file_size() > _max_file_size) {
            StatusOr<std::shared_ptr<BinlogFileWriter>> status_or = _create_binlog_writer(next_file_id);
            status = status_or.status();
            if (!status.ok()) {
                break;
            }
            int64_t next_seq_id = current_writer == nullptr ? 0 : current_writer->pending_end_seq_id() + 1;
            int64_t next_changelog_id = current_writer == nullptr ? 0 : current_writer->pending_end_changelog_id() + 1;
            current_writer = status_or.value();
            pending_writers.emplace_back(current_writer);
            next_file_id += 1;
            status = current_writer->prepare(rowset->start_version(), rowset->rowset_id(), next_seq_id,
                                             next_changelog_id, rowset->creation_time());
            if (!status.ok()) {
                LOG(WARNING) << "Fail to prepare binlog writer for rowset " << rowset->rowset_id() << ", segment index "
                             << seg_index << ", number of rows " << num_rows << ", version " << rowset->start_version()
                             << ", binlog writer " << current_writer->file_name() << ", " << status;
                break;
            }
        }

        status = current_writer->add_insert_range(seg_index, 0, num_rows - 1);
        if (!status.ok()) {
            LOG(WARNING) << "Fail to add_insert_range for rowset " << rowset->rowset_id() << ", segment index "
                         << seg_index << ", number of rows " << num_rows << ", version " << rowset->start_version()
                         << ", binlog writer " << current_writer->file_name() << ", " << status;
            break;
        }
        if (VLOG_IS_ON(3)) {
            VLOG(3) << "add_insert_range for rowset " << rowset->rowset_id() << ", segment index " << seg_index
                    << ", number of rows " << num_rows << ", version " << rowset->start_version() << ", binlog writer "
                    << current_writer->file_name() << ", " << status;
        }
    }

    if (status.ok() && rowset->num_rows() == 0) {
        status = current_writer->add_empty();
        if (!status.ok()) {
            LOG(WARNING) << "Fail to add_empty for rowset " << rowset->rowset_id() << ", version "
                         << rowset->start_version() << ", binlog writer " << current_writer->file_name() << ", "
                         << status;
        }
        if (VLOG_IS_ON(3)) {
            VLOG(3) << "add_empty for rowset " << rowset->rowset_id() << ", version " << rowset->start_version()
                    << ", binlog writer " << current_writer->file_name() << ", " << status;
        }
    }

    int committed_writer_index = pending_writers.size();
    if (status.ok()) {
        bool last_writer = true;
        // commit writers backward, so that we can abort the first writer rather than
        // rollback it if it is shared with the previous version, and other writers
        // commit failed
        for (int i = pending_writers.size() - 1; i >= 0; i--) {
            std::shared_ptr<BinlogFileWriter> writer = pending_writers[i];
            status = writer->commit(last_writer);
            if (!status.ok()) {
                LOG(WARNING) << "Fail to commit binlog writer " << writer->file_name() << ", rowset "
                             << rowset->rowset_id() << ", version " << rowset->start_version() << ", " << status;
                break;
            }
            committed_writer_index = i;
            last_writer = false;
        }

        // update file metas if all writers commit successfully
        if (committed_writer_index == 0) {
            std::vector<BinlogFileMetaPBSharedPtr> new_file_metas;
            for (int i = 0; i < pending_writers.size(); i++) {
                std::shared_ptr<BinlogFileWriter> writer = pending_writers[i];
                // close writers except the last one
                if (i < pending_writers.size() - 1) {
                    // just ignore close failure because data has been committed
                    writer->close();
                }
                BinlogFileMetaPBSharedPtr file_meta = std::make_shared<BinlogFileMetaPB>();
                writer->copy_file_meta(file_meta.get());
                new_file_metas.emplace_back(file_meta);
            }
            _update_metas_after_new_commit(new_file_metas);
            for (int i = 0; i < pending_writers.size() - 1; i++) {
                Status st = pending_writers[i]->close();
                if (!st.ok()) {
                    LOG(WARNING) << "Fail to close binlog writer after committed " << pending_writers[i]->file_name();
                }
            }
            // reuse last writer
            _active_binlog_writer = pending_writers.back();
            // finish all of work
            return Status::OK();
        }
    }

    // abort the pending writer, and delete useless files
    bool reuse_first_writer = _active_binlog_writer != nullptr;
    std::vector<std::string> files_to_delete;
    for (int i = 0; i < pending_writers.size(); i++) {
        std::shared_ptr<BinlogFileWriter> writer = pending_writers[i];
        if (i < committed_writer_index) {
            Status st = writer->abort();
            if (!st.ok()) {
                LOG(WARNING) << "Fail to abort binlog writer " << writer->file_name();
                // if the first writer fails to abort, not reuse it
                if (i == 0) {
                    reuse_first_writer = false;
                }
            }
        }
        if (i > 0 || !reuse_first_writer) {
            Status st = writer->close();
            if (!st.ok()) {
                LOG(WARNING) << "Fail to close binlog writer " << writer->file_name();
            }
        }
        if (i > 0 || _active_binlog_writer == nullptr) {
            files_to_delete.emplace_back(writer->file_name());
        }
    }
    if (!reuse_first_writer) {
        _active_binlog_writer.reset();
    }
    _delete_binlog_files(files_to_delete);

    LOG(WARNING) << "Fail to add_insert_rowset to binlog for rowset " << rowset->rowset_id() << ", version "
                 << rowset->start_version() << ", " << status;
    return Status::InternalError(fmt::format("Fail to generate binlog for rowset {}, version {}",
                                             rowset->rowset_id().to_string(), rowset->start_version()));
}

bool BinlogManager::is_rowset_used(const RowsetId& rowset_id) {
    std::lock_guard lock(_meta_lock);
    return _rowset_count_map.count(rowset_id) >= 1;
}

void BinlogManager::delete_expired_binlog() {
    // TODO remove expired binlog
    // 1. check expired binlog file and referenced rowset
    // 2. if binlog file and rowset is used by some readers, skip to delete
    // 3. delete binlog files
    // 4. if rowset use_count == 1, delete rowset, otherwise just remove from meta
}

StatusOr<BinlogFileMetaPBSharedPtr> BinlogManager::seek_binlog_file(int64_t version, int64_t changelog_id) {
    std::shared_lock lock(_meta_lock);
    int128_t lsn = _changelog_lsn(version, changelog_id);
    auto upper = _binlog_file_metas.upper_bound(lsn);
    if (upper == _binlog_file_metas.begin()) {
        return Status::NotFound(
                strings::Substitute("Can't find file meta for version $0, changelog_id $1", version, changelog_id));
    }

    BinlogFileMetaPBSharedPtr file_meta;
    if (upper == _binlog_file_metas.end()) {
        file_meta = _binlog_file_metas.rbegin()->second;
    } else {
        file_meta = (--upper)->second;
    }

    if (file_meta->end_tablet_version() < version) {
        return Status::NotFound(
                strings::Substitute("Can't find file meta for version $0, changelog_id $1", version, changelog_id));
    }

    return file_meta;
}

StatusOr<std::shared_ptr<BinlogFileWriter>> BinlogManager::_create_binlog_writer(int64_t file_id) {
    // TODO sync parent dir after create new file writer
    std::string file_name = _binlog_file_name(file_id);
    std::shared_ptr<BinlogFileWriter> binlog_writer =
            std::make_shared<BinlogFileWriter>(file_id, file_name, _max_page_size, _compression_type);
    Status status = binlog_writer->init();
    if (status.ok()) {
        return binlog_writer;
    }
    LOG(WARNING) << "Fail to initialize binlog writer, file id " << file_id << ", file name " << file_name << ", "
                 << status;
    Status st = binlog_writer->close();
    if (!st.ok()) {
        LOG(WARNING) << "Fail to close binlog writer, file id " << file_id << ", file name " << file_name << ", "
                     << status;
    }

    std::shared_ptr<FileSystem> fs;
    ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(file_name))
    st = fs->delete_file(file_name);
    if (st.ok()) {
        LOG(INFO) << "Delete binlog file after creating failed " << file_name;
    } else {
        LOG(WARNING) << "Fail to delete binlog file after creating failed " << file_name << ", " << st;
    }

    return status;
}

Status BinlogManager::_delete_binlog_files(std::vector<std::string>& file_names) {
    if (file_names.empty()) {
        return Status::OK();
    }

    std::shared_ptr<FileSystem> fs;
    ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(file_names[0]))
    for (auto& file_name : file_names) {
        Status st = fs->delete_file(file_name);
        if (st.ok()) {
            LOG(INFO) << "Delete binlog file " << file_name;
        } else {
            LOG(WARNING) << "Fail to delete binlog file " << file_name << ", " << st;
        }
    }
    return Status::OK();
}

void BinlogManager::_update_metas_after_new_commit(std::vector<BinlogFileMetaPBSharedPtr> new_file_metas) {
    std::shared_lock lock(_meta_lock);

    RowsetId reused_rowset_id;
    // remove the last file meta that is updated
    if (!_binlog_file_metas.empty()) {
        BinlogFileMetaPBSharedPtr old_file_meta = _binlog_file_metas.rbegin()->second;
        if (old_file_meta->id() == new_file_metas.front()->id()) {
            for (auto& rowset_id_pb : old_file_meta->rowsets()) {
                _convert_rowset_id_pb(rowset_id_pb, &reused_rowset_id);
                _rowset_count_map[reused_rowset_id]--;
            }
        }
    }

    for (BinlogFileMetaPBSharedPtr& file_meta : new_file_metas) {
        int128_t lsn = _changelog_lsn(file_meta->start_tablet_version(), file_meta->start_changelog_id());
        _binlog_file_metas.emplace(lsn, file_meta);
        for (auto& rowset_id_pb : file_meta->rowsets()) {
            _convert_rowset_id_pb(rowset_id_pb, &reused_rowset_id);
            _rowset_count_map[reused_rowset_id]++;
        }
    }
}

} // namespace starrocks