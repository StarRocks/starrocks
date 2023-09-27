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
          _rowset_fetcher(std::move(rowset_fetcher)),
          _unused_binlog_file_ids(UINT64_MAX) {}

BinlogManager::~BinlogManager() {
    std::unique_lock lock(_meta_lock);
    if (_active_binlog_writer != nullptr) {
        WARN_IF_ERROR(_active_binlog_writer->close(true), "Close binlog writer failed");
        _active_binlog_writer.reset();
    }
}

Status BinlogManager::init(BinlogLsn min_valid_lsn, std::vector<int64_t>& sorted_valid_versions) {
    // 1. list all of binlog files
    std::list<int64_t> binlog_file_ids;
    Status status = BinlogUtil::list_binlog_file_ids(_path, &binlog_file_ids);
    if (!status.ok()) {
        std::string err_msg = fmt::format("Failed to init binlog because of list error, tablet {}, status: {}",
                                          _tablet_id, status.to_string());
        LOG(ERROR) << err_msg;
        _init_failure.store(true);
        return Status::InternalError(err_msg);
    }
    binlog_file_ids.sort();

    // 2. recover binlog from the largest version to the smallest version
    std::vector<int64_t> useless_file_ids;
    std::vector<BinlogFileMetaPBPtr> recovered_file_metas;
    recovered_file_metas.reserve(binlog_file_ids.size());
    auto file_id_it = binlog_file_ids.rbegin();
    int64_t next_version_index = sorted_valid_versions.size() - 1;
    while (next_version_index >= 0) {
        int64_t version = sorted_valid_versions[next_version_index];
        BinlogLsn min_lsn = version == min_valid_lsn.version() ? min_valid_lsn : BinlogLsn(version, 0);
        Status recover_status = _recover_version(version, min_lsn, binlog_file_ids, file_id_it, recovered_file_metas,
                                                 &useless_file_ids);
        if (!recover_status.ok()) {
            LOG(ERROR) << "Failed to recover version: " << version << " for tablet: " << _tablet_id
                       << ", status: " << recover_status.to_string();
            break;
        }
        next_version_index--;
    }

    if (next_version_index >= 0) {
        std::string err_msg = fmt::format(
                "Failed to init binlog because can't find binlog for all of versions, "
                "tablet: {}, min_valid_lsn: {}, num expected versions: {}, num recovered version: {}, last failed "
                "version: {}",
                _tablet_id, min_valid_lsn.to_string(), sorted_valid_versions.size(),
                sorted_valid_versions.size() - next_version_index, sorted_valid_versions[next_version_index]);
        LOG(ERROR) << err_msg << ". Details of valid versions for tablet: "
                   << fmt::format("{}", fmt::join(sorted_valid_versions, ", "));
        _init_failure.store(true);
        return Status::InternalError(err_msg);
    }

    // 3. construct metas according to the recovery result

    // file metas are pushed back to recovered_file_metas in the descending order of file ids,
    // and we want to recover them in the ascending order, so reverse the vector first
    std::reverse(recovered_file_metas.begin(), recovered_file_metas.end());
    BinlogBuildResult build_result;
    build_result.next_file_id = binlog_file_ids.empty() ? BINLOG_MIN_FILE_ID : (*binlog_file_ids.rbegin() + 1);
    build_result.metas = recovered_file_metas;
    _apply_build_result(&build_result);

    // 4. cleanup useless binlog files
    while (file_id_it != binlog_file_ids.rend()) {
        useless_file_ids.push_back(*file_id_it);
        file_id_it++;
    }

    // file ids are pushed back to useless_file_ids in descending order, and we want to delete
    // them in ascending order, so traverse the vector in the reverse order
    for (auto it = useless_file_ids.rbegin(); it != useless_file_ids.rend(); it++) {
        _unused_binlog_file_ids.blocking_put(*it);
    }

    std::string version_details;
    if (!sorted_valid_versions.empty()) {
        version_details = fmt::format(", min valid lsn: {}, min/max valid version: {}/{}", min_valid_lsn.to_string(),
                                      sorted_valid_versions.front(), sorted_valid_versions.back());
    }
    LOG(INFO) << "Init binlog successfully, tablet: " << _tablet_id
              << ", num valid versions: " << sorted_valid_versions.size() << version_details;
    VLOG(3) << "Details of recovered versions for tablet: " << _tablet_id
            << ", versions: " << fmt::format("{}", fmt::join(sorted_valid_versions, ", "));

    return Status::OK();
}

Status BinlogManager::_recover_version(int64_t version, BinlogLsn& min_lsn, std::list<int64_t>& file_ids,
                                       std::list<int64_t>::reverse_iterator& file_id_it,
                                       std::vector<BinlogFileMetaPBPtr>& recovered_file_metas,
                                       std::vector<int64_t>* useless_file_ids) {
    // the last file meta that contains the binlog of the version
    BinlogFileMetaPBPtr last_file_meta;
    if (!recovered_file_metas.empty()) {
        auto& file_meta = recovered_file_metas.back();
        BinlogLsn start_lsn(file_meta->start_version(), file_meta->start_seq_id());
        if (start_lsn <= min_lsn) {
            // the binlog for the version is only in one file,
            // and finish to recover the version
            return Status::OK();
        } else if (start_lsn.version() == version) {
            // the last recovered file only contains part of binlog
            last_file_meta = file_meta;
        } else {
            // the last file is just the start of the last recovered version
            DCHECK(start_lsn.seq_id() == 0);
        }
    }

    bool find_all_binlog = false;
    while (file_id_it != file_ids.rend()) {
        int64_t file_id = *file_id_it;
        file_id_it++;
        StatusOr<BinlogFileMetaPBPtr> status_or =
                _recover_file_meta_for_version(version, file_id, last_file_meta.get());
        if (!status_or.ok() && !status_or.status().is_not_found()) {
            return status_or.status();
        }
        if (status_or.status().is_not_found()) {
            useless_file_ids->push_back(file_id);
            continue;
        }
        BinlogFileMetaPBPtr file_meta = status_or.value();
        recovered_file_metas.push_back(file_meta);
        BinlogLsn lsn(file_meta->start_version(), file_meta->start_seq_id());
        last_file_meta = file_meta;
        if (lsn <= min_lsn) {
            find_all_binlog = true;
            break;
        }
    }

    if (!find_all_binlog) {
        std::string err_msg = fmt::format("Failed to recover version {} because of incomplete binlog, tablet {}",
                                          version, _tablet_id);
        std::string meta_msg =
                last_file_meta == nullptr ? "null" : BinlogUtil::file_meta_to_string(last_file_meta.get());
        LOG(ERROR) << err_msg << ", last file meta: " << meta_msg;
        return Status::InternalError(err_msg);
    }

    return Status::OK();
}

StatusOr<BinlogFileMetaPBPtr> BinlogManager::_recover_file_meta_for_version(int64_t version, int64_t file_id,
                                                                            BinlogFileMetaPB* last_file_meta) {
    BinlogLsn lsn_upper_bound;
    if (last_file_meta == nullptr) {
        lsn_upper_bound = BinlogLsn(version + 1, 0);
    } else {
        lsn_upper_bound = BinlogLsn(last_file_meta->start_version(), last_file_meta->start_seq_id());
    }
    std::string file_path = BinlogUtil::binlog_file_path(_path, file_id);
    StatusOr status_or = BinlogFileReader::load_meta(file_id, file_path, lsn_upper_bound);
    if (!status_or.ok() && !status_or.status().is_not_found()) {
        std::string err_msg =
                fmt::format("Failed to recover version {} because of load error, tablet {}, file_path: {}", version,
                            _tablet_id, file_path);
        LOG(ERROR) << err_msg << ", " << status_or.status();
        return Status::InternalError(err_msg);
    }

    if (status_or.status().is_not_found()) {
        return status_or;
    }

    BinlogFileMetaPBPtr file_meta = status_or.value();
    DCHECK(file_meta->num_pages() > 0);

    // check the lsn is continuous between the file meta and the last file meta
    if (file_meta->end_version() == version) {
        if (last_file_meta == nullptr && file_meta->version_eof()) {
            return file_meta;
        } else if (last_file_meta != nullptr && file_meta->end_seq_id() + 1 == last_file_meta->start_seq_id()) {
            return file_meta;
        }
    }

    std::string err_msg = fmt::format(
            "Failed to recover version {} because of discontinuous lsn, "
            "tablet: {}, file_path: {}",
            version, _tablet_id, file_path);
    LOG(ERROR) << err_msg << ", current file meta: " << BinlogUtil::file_meta_to_string(file_meta.get())
               << ", last file meta: "
               << (last_file_meta == nullptr ? "null" : BinlogUtil::file_meta_to_string(last_file_meta));

    return Status::InternalError(err_msg);
}

StatusOr<BinlogBuilderParamsPtr> BinlogManager::begin_ingestion(int64_t version) {
    VLOG(3) << "Begin ingestion, tablet: " << _tablet_id << ", version: " << version << ", path: " << _path;
    DCHECK_EQ(-1, _ingestion_version);
    RETURN_IF_ERROR(_check_init_failure());

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

void BinlogManager::precommit_ingestion(int64_t version, const BinlogBuildResultPtr& result) {
    VLOG(3) << "Pre-commit ingestion, tablet: " << _tablet_id << ", version: " << version << ", path: " << _path;
    DCHECK_EQ(version, _ingestion_version);
    DCHECK(_build_result == nullptr);
    _build_result = std::move(result);
}

void BinlogManager::abort_ingestion(int64_t version, const BinlogBuildResultPtr& result) {
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
        BinlogLsn lsn(meta->start_version(), meta->start_seq_id());
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

bool BinlogManager::check_expire_and_capacity(int64_t current_second, int64_t binlog_ttl_second,
                                              int64_t binlog_max_size) {
    std::unique_lock lock(_meta_lock);
    _check_wait_reader_binlog_files();
    bool expired_or_overcapacity = _check_alive_binlog_files(current_second, binlog_ttl_second, binlog_max_size);

    LOG(INFO) << "Check binlog expire and capacity, tablet: " << _tablet_id
              << ", num alive binlog files: " << _alive_binlog_files.size()
              << ", num alive rowset: " << _alive_rowset_count_map.size()
              << ", total_alive_binlog_file_size: " << _total_alive_binlog_file_size
              << ", total_alive_rowset_data_size: " << _total_alive_rowset_data_size << ", "
              << ", num wait reader binlog files: " << _wait_reader_binlog_files.size()
              << ", num wait reader rowset: " << _wait_reader_rowset_count_map.size()
              << ", total_wait_reader_binlog_file_size: " << _total_wait_reader_binlog_file_size
              << ", total_wait_reader_rowset_data_size: " << _total_wait_reader_rowset_data_size;

    return expired_or_overcapacity;
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
        auto file_meta = binlog_file->file_meta();
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

bool BinlogManager::_check_alive_binlog_files(int64_t current_second, int64_t binlog_ttl_second,
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
    bool expired_or_overcapacity = false;
    for (auto it = _alive_binlog_files.begin(); it != _alive_binlog_files.end();) {
        auto& binlog_file = it->second;
        auto meta = binlog_file->file_meta();
        // skip to clean up the active file
        if (meta->id() == active_file_id) {
            break;
        }
        bool expired = meta->end_timestamp_in_us() / 1000000 < expiration_time;
        bool overcapacity = _total_alive_binlog_file_size + _total_alive_rowset_data_size > binlog_max_size;
        if (!expired && !overcapacity) {
            break;
        }

        expired_or_overcapacity = true;
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

    return expired_or_overcapacity;
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
        for (const auto& iter : _alive_binlog_files) {
            _unused_binlog_file_ids.blocking_put((iter.second)->file_meta()->id());
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

StatusOr<int64_t> BinlogManager::register_reader(const std::shared_ptr<BinlogReader>& reader) {
    RETURN_IF_ERROR(_check_init_failure());
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
    RETURN_IF_ERROR(_check_init_failure());
    std::shared_lock lock(_meta_lock);
    BinlogLsn lsn(version, seq_id);
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
    return {start->file_meta()->start_version(), start->file_meta()->start_seq_id(), end->file_meta()->end_version(),
            end->file_meta()->end_seq_id()};
}

// close_active_writer only used for UT
void BinlogManager::close_active_writer() {
    if (_active_binlog_writer != nullptr) {
        (void)_active_binlog_writer->close(true);
        _active_binlog_writer.reset();
    }
}

Status BinlogManager::_check_init_failure() {
    if (_init_failure.load()) {
        std::string msg = fmt::format(
                "Can't provide binlog read/write because tablet {} fails to init binlog. "
                "See the BE log for the reason of init failure",
                _tablet_id);
        return Status::InternalError(msg);
    }
    return Status::OK();
}

} // namespace starrocks