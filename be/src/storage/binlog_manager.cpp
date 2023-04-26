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

#include "storage/binlog_manager.h"

#include "storage/rowset/rowset.h"
#include "storage/tablet.h"

namespace starrocks {

DupKeyRowsetFetcher::DupKeyRowsetFetcher(Tablet& tablet) : _tablet(tablet) {}

RowsetSharedPtr DupKeyRowsetFetcher::get_rowset(int64_t rowset_id) {
    return _tablet.get_inc_rowset_by_version(Version(rowset_id, rowset_id));
}

BinlogManager::BinlogManager(int64_t tablet_id, std::string path, int64_t max_file_size, int32_t max_page_size,
                             CompressionTypePB compression_type, std::shared_ptr<RowsetFetcher> rowset_fetcher)
        : _tablet_id(tablet_id),
          _path(std::move(path)),
          _max_file_size(max_file_size),
          _max_page_size(max_page_size),
          _compression_type(compression_type),
          _rowset_fetcher(rowset_fetcher),
          _unused_binlog_file_ids(UINT64_MAX) {}

BinlogManager::~BinlogManager() {
    std::unique_lock lock(_meta_lock);
    if (_active_binlog_writer != nullptr) {
        _active_binlog_writer->close(true);
        _active_binlog_writer.reset();
    }
}

StatusOr<BinlogBuilderParamsPtr> BinlogManager::begin_ingestion(int64_t version) {
    VLOG(3) << "Begin ingestion, tablet: " << _tablet_id << ", version: " << version << ", path: " << _path;
    DCHECK_EQ(-1, _ingestion_version);

    std::shared_lock meta_lock(_meta_lock);
    if (!_alive_binlog_files.empty()) {
        BinlogFilePtr binlog_file = _alive_binlog_files.rbegin()->second;
        int64_t max_version = binlog_file->file_meta()->end_version();
        if (max_version >= version) {
            VLOG(3) << "The version already existed in binlog, tablet: " << _tablet_id
                    << ", max version: " << max_version << ", new version: " << version;
            return Status::AlreadyExist(fmt::format("Version already exists in binlog"));
        }
    }

    _ingestion_version = version;
    std::shared_ptr<BinlogBuilderParams> params = std::make_shared<BinlogBuilderParams>();
    params->binlog_storage_path = _path;
    params->max_file_size = _max_file_size;
    params->max_page_size = _max_page_size;
    params->compression_type = _compression_type;
    params->start_file_id = _next_file_id;
    if (_active_binlog_writer != nullptr) {
        params->active_file_meta = _alive_binlog_files.rbegin()->second->file_meta();
        params->active_file_writer = _active_binlog_writer;
    }
    return params;
}

void BinlogManager::precommit_ingestion(int64_t version, BinlogBuildResultPtr result) {
    VLOG(3) << "Pre-commit ingestion, tablet: " << _tablet_id << ", version: " << version << ", path: " << _path;
    DCHECK_EQ(version, _ingestion_version);
    DCHECK(_build_result == nullptr);
    _build_result = result;
}

void BinlogManager::abort_ingestion(int64_t version, BinlogBuildResultPtr result) {
    VLOG(3) << "Abort ingestion, tablet: " << _tablet_id << ", version: " << version << ", path: " << _path;
    DCHECK_EQ(version, _ingestion_version);
    DCHECK(_build_result == nullptr);
    _apply_build_result(result.get());
    _ingestion_version = -1;
}

void BinlogManager::delete_ingestion(int64_t version) {
    VLOG(3) << "Delete ingestion, tablet: " << _tablet_id << ", version: " << version << ", path: " << _path;
    DCHECK_EQ(version, _ingestion_version);
    DCHECK(_build_result != nullptr);
    int next_file_id = _build_result->next_file_id;
    BinlogFileWriterPtr writer = BinlogBuilder::discard_binlog_build_result(version, *_build_result);
    {
        std::unique_lock meta_lock(_meta_lock);
        _active_binlog_writer = writer;
        _next_file_id = next_file_id;
    }
    _ingestion_version = -1;
    _build_result.reset();
}

void BinlogManager::commit_ingestion(int64_t version) {
    VLOG(3) << "Commit ingestion, tablet: " << _tablet_id << ", version: " << version << ", path: " << _path;
    DCHECK_EQ(version, _ingestion_version);
    DCHECK(_build_result != nullptr);
    _apply_build_result(_build_result.get());
    _ingestion_version = -1;
    _build_result.reset();
}

void BinlogManager::_apply_build_result(BinlogBuildResult* result) {
    std::unique_lock lock(_meta_lock);
    for (auto& meta : result->metas) {
        int128_t lsn = BinlogUtil::get_lsn(meta->start_version(), meta->start_seq_id());
        auto it = _alive_binlog_files.find(lsn);
        bool override_meta = it != _alive_binlog_files.end();
        if (override_meta) {
            BinlogFilePtr binlog_file = it->second;
            BinlogFileMetaPBPtr& old_file_meta = binlog_file->file_meta();
            std::unordered_set<int64_t> old_rowsets;
            for (auto rowset_id : old_file_meta->rowsets()) {
                old_rowsets.emplace(rowset_id);
            }

            for (auto rowset_id : meta->rowsets()) {
                if (old_rowsets.count(rowset_id) == 0) {
                    if (_alive_rowset_count_map.count(rowset_id) == 0) {
                        _total_alive_rowset_data_size += _rowset_fetcher->get_rowset(rowset_id)->data_disk_size();
                    }
                    _alive_rowset_count_map[rowset_id] += 1;
                }
            }

            _total_alive_binlog_file_size += meta->file_size() - old_file_meta->file_size();
            binlog_file->update_file_meta(meta);
        } else {
            _alive_binlog_files[lsn] = std::make_shared<BinlogFile>(meta);
            for (auto rowset_id : meta->rowsets()) {
                if (_alive_rowset_count_map.count(rowset_id) == 0) {
                    _total_alive_rowset_data_size += _rowset_fetcher->get_rowset(rowset_id)->data_disk_size();
                }
                _alive_rowset_count_map[rowset_id] += 1;
            }
            _total_alive_binlog_file_size += meta->file_size();
        }
    }

    _next_file_id = result->next_file_id;
    _active_binlog_writer = result->active_writer;
}

void BinlogManager::check_expire_and_capacity(int64_t current_second, int64_t binlog_ttl_second,
                                              int64_t binlog_max_size) {
    std::unique_lock lock(_meta_lock);
    _check_wait_reader_binlog_files();
    _check_alive_binlog_files(current_second, binlog_ttl_second, binlog_max_size);

    LOG(INFO) << "Check binlog expire and capacity, tablet: " << _tablet_id
              << ", num alive binlog files: " << _alive_binlog_files.size()
              << ", num alive rowset: " << _alive_rowset_count_map.size()
              << ", total_alive_binlog_file_size: " << _total_alive_binlog_file_size
              << ", total_alive_rowset_data_size: " << _total_alive_rowset_data_size << ", "
              << ", num wait reader binlog files: " << _wait_reader_binlog_files.size()
              << ", num wait reader rowset: " << _wait_reader_rowset_count_map.size()
              << ", total_wait_reader_binlog_file_size: " << _total_wait_reader_binlog_file_size
              << ", total_wait_reader_rowset_data_size: " << _total_wait_reader_rowset_data_size;
}

void BinlogManager::_check_wait_reader_binlog_files() {
    int64_t first_file_id = -1;
    int64_t last_file_id = -1;
    int64_t num_files = 0;
    while (!_wait_reader_binlog_files.empty()) {
        auto& binlog_file = _wait_reader_binlog_files.front();
        if (binlog_file->reader_count() > 0) {
            break;
        }
        auto& file_meta = binlog_file->file_meta();
        _total_wait_reader_binlog_file_size -= file_meta->file_size();
        for (int64_t rowset_id : file_meta->rowsets()) {
            int32_t count = --_wait_reader_rowset_count_map[rowset_id];
            if (count == 0) {
                _wait_reader_rowset_count_map.erase(rowset_id);
                _total_wait_reader_rowset_data_size -= _rowset_fetcher->get_rowset(rowset_id)->data_disk_size();
            }
        }
        _unused_binlog_file_ids.blocking_put(file_meta->id());
        if (first_file_id == -1) {
            first_file_id = file_meta->id();
        }
        last_file_id = file_meta->id();
        num_files += 1;

        _wait_reader_binlog_files.pop_front();

        VLOG(3) << "Binlog file finished to wait readers, tablet: " << _tablet_id << ", file id: " << file_meta->id();
    }
    LOG(INFO) << "Check wait reader binlog files, tablet: " << _tablet_id << ", num unused files: " << num_files
              << ", first_file_id: " << first_file_id << ", last_file_id: " << last_file_id
              << ", num of still wait files: " << _wait_reader_binlog_files.size();
}

void BinlogManager::_check_alive_binlog_files(int64_t current_second, int64_t binlog_ttl_second,
                                              int64_t binlog_max_size) {
    int64_t expiration_time = current_second - binlog_ttl_second;
    int64_t active_file_id = _active_binlog_writer != nullptr ? _active_binlog_writer->file_id() : -1;
    // binlog files should be deleted in the order by the file id, and if there are previous
    // binlog files waiting for readers, the latter should also wait for the readers
    bool need_wait_reader = !_wait_reader_binlog_files.empty();
    int64_t first_file_id = -1;
    int64_t last_file_id = -1;
    int64_t num_files = 0;
    int64_t num_unused_files = 0;
    for (auto it = _alive_binlog_files.begin(); it != _alive_binlog_files.end();) {
        auto& binlog_file = it->second;
        auto& meta = binlog_file->file_meta();
        // skip to clean up the active file
        if (meta->id() == active_file_id) {
            break;
        }
        bool expired = meta->end_timestamp_in_us() / 1000000 < expiration_time;
        bool overcapacity = _total_alive_binlog_file_size + _total_alive_rowset_data_size > binlog_max_size;
        if (!expired && !overcapacity) {
            break;
        }

        if (binlog_file->reader_count() > 0) {
            need_wait_reader = true;
        }

        _total_alive_binlog_file_size -= meta->file_size();
        if (need_wait_reader) {
            _wait_reader_binlog_files.push_back(binlog_file);
            _total_wait_reader_binlog_file_size += meta->file_size();
        } else {
            _unused_binlog_file_ids.blocking_put(meta->id());
        }

        for (int64_t rowset_id : meta->rowsets()) {
            auto rit = _alive_rowset_count_map.find(rowset_id);
            DCHECK(rit != _alive_rowset_count_map.end());
            rit->second--;
            if (rit->second == 0) {
                _alive_rowset_count_map.erase(rowset_id);
                _total_alive_rowset_data_size -= _rowset_fetcher->get_rowset(rowset_id)->data_disk_size();
            }
            if (need_wait_reader) {
                int32_t wait_count = ++_wait_reader_rowset_count_map[rowset_id];
                if (wait_count == 1) {
                    _total_wait_reader_rowset_data_size += _rowset_fetcher->get_rowset(rowset_id)->data_disk_size();
                }
            }
        }

        if (first_file_id == -1) {
            first_file_id = meta->id();
        }
        last_file_id = meta->id();
        num_files += 1;
        num_unused_files += need_wait_reader ? 0 : 1;

        it = _alive_binlog_files.erase(it);

        VLOG(3) << "Binlog file is not alive, tablet: " << _tablet_id << ", file_id: " << meta->id()
                << ", expired: " << expired << ", overcapacity: " << overcapacity
                << ", need_wait_reader: " << need_wait_reader;
    }

    LOG(INFO) << "Check alive binlog files, tablet: " << _tablet_id << ", num files: " << num_files
              << ", first file id: " << first_file_id << ", last file id: " << last_file_id
              << ", num unused files: " << num_unused_files;
}

void BinlogManager::delete_unused_binlog() {
    StatusOr<std::shared_ptr<FileSystem>> status_or = FileSystem::CreateSharedFromString(_path);
    if (!status_or.ok()) {
        LOG(ERROR) << "Failed to delete unused binlog, tablet: " << _tablet_id << ", " << status_or.status();
        return;
    }
    std::shared_ptr<FileSystem> fs = status_or.value();
    int64_t file_id;
    int32_t total_num = 0;
    int32_t fail_num = 0;
    while (_unused_binlog_file_ids.try_get(&file_id) == 1) {
        total_num += 1;
        std::string file_path = BinlogUtil::binlog_file_path(_path, file_id);
        // TODO use path gc to clean up binlog files if fail to delete
        Status st = fs->delete_file(file_path);
        if (st.ok()) {
            VLOG(3) << "Delete binlog file, tablet: " << _tablet_id << ", file path: " << file_path;
        } else {
            LOG(WARNING) << "Fail to delete binlog file, tablet:  " << _tablet_id << ", file path: " << file_path
                         << ", " << st;
            fail_num += 1;
        }
    }

    LOG(INFO) << "Delete unused binlog files, tablet: " << _tablet_id << ", total files: " << total_num
              << ", failed to delete: " << fail_num;
}

void BinlogManager::delete_all_binlog() {
    {
        std::unique_lock lock(_meta_lock);
        while (!_wait_reader_binlog_files.empty()) {
            auto& binlog_file = _wait_reader_binlog_files.front();
            _unused_binlog_file_ids.blocking_put(binlog_file->file_meta()->id());
            _wait_reader_binlog_files.pop_front();
        }
        for (auto it = _alive_binlog_files.begin(); it != _alive_binlog_files.end(); it++) {
            _unused_binlog_file_ids.blocking_put(it->second->file_meta()->id());
        }
        _alive_binlog_files.clear();
        _alive_rowset_count_map.clear();
        _total_alive_binlog_file_size = 0;
        _total_alive_rowset_data_size = 0;
        _wait_reader_rowset_count_map.clear();
        _total_wait_reader_binlog_file_size = 0;
        _total_wait_reader_rowset_data_size = 0;
    }
    delete_unused_binlog();
}

bool BinlogManager::is_rowset_used(int64_t rowset_id) {
    std::shared_lock lock(_meta_lock);
    return _alive_rowset_count_map.count(rowset_id) >= 1 || _wait_reader_rowset_count_map.count(rowset_id) >= 1;
}

StatusOr<int64_t> BinlogManager::register_reader(std::shared_ptr<BinlogReader> reader) {
    std::unique_lock lock(_meta_lock);
    int64_t reader_id = _next_reader_id++;
    _binlog_readers.emplace(reader_id, reader);
    VLOG(3) << "Register reader " << reader_id << " in binlog manager " << _path;
    return reader_id;
}

void BinlogManager::unregister_reader(int64_t reader_id) {
    std::unique_lock lock(_meta_lock);
    VLOG(3) << "Unregister reader " << reader_id << " in binlog manager " << _path;
    _binlog_readers.erase(reader_id);
}

StatusOr<BinlogFileReadHolderPtr> BinlogManager::find_binlog_file(int64_t version, int64_t seq_id) {
    std::shared_lock lock(_meta_lock);
    int128_t lsn = BinlogUtil::get_lsn(version, seq_id);
    // find the first file whose start lsn is greater than the target
    auto upper = _alive_binlog_files.upper_bound(lsn);
    if (upper == _alive_binlog_files.begin()) {
        return Status::NotFound(fmt::format("Can't find file meta for version {}, seq_id {}", version, seq_id));
    }

    BinlogFilePtr binlog_file;
    if (upper == _alive_binlog_files.end()) {
        binlog_file = _alive_binlog_files.rbegin()->second;
    } else {
        binlog_file = (--upper)->second;
    }

    if (binlog_file->file_meta()->end_version() < version) {
        return Status::NotFound(fmt::format("Can't find file meta for version {}, seq_id {}", version, seq_id));
    }

    return binlog_file->new_read_holder();
}

BinlogRange BinlogManager::current_binlog_range() {
    std::shared_lock lock(_meta_lock);
    if (_alive_binlog_files.empty()) {
        return {0, 0, -1, -1};
    }

    BinlogFilePtr& start = _alive_binlog_files.begin()->second;
    BinlogFilePtr& end = _alive_binlog_files.rbegin()->second;
    return BinlogRange(start->file_meta()->start_version(), start->file_meta()->start_seq_id(),
                       end->file_meta()->end_version(), end->file_meta()->end_seq_id());
}

void BinlogManager::close_active_writer() {
    if (_active_binlog_writer != nullptr) {
        _active_binlog_writer->close(true);
        _active_binlog_writer.reset();
    }
}

} // namespace starrocks