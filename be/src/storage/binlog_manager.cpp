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

BinlogManager::BinlogManager(int64_t _tablet_id, std::string path, int64_t max_file_size, int32_t max_page_size,
                             CompressionTypePB compression_type, std::shared_ptr<RowsetFetcher> rowset_fetcher)
        : _tablet_id(_tablet_id),
          _path(std::move(path)),
          _max_file_size(max_file_size),
          _max_page_size(max_page_size),
          _compression_type(compression_type),
          _rowset_fetcher(rowset_fetcher) {}

BinlogManager::~BinlogManager() {
    std::lock_guard lock(_meta_lock);
    if (_active_binlog_writer != nullptr) {
        _active_binlog_writer->close(true);
        _active_binlog_writer.reset();
    }
}

StatusOr<BinlogBuilderParamsPtr> BinlogManager::begin_ingestion(int64_t version) {
    VLOG(3) << "Begin ingestion, tablet: " << _tablet_id << ", version: " << version << ", path: " << _path;
    DCHECK_EQ(-1, _ingestion_version);

    std::shared_lock meta_lock(_meta_lock);
    if (!_binlog_file_metas.empty()) {
        BinlogFileMetaPBPtr file_meta = _binlog_file_metas.rbegin()->second;
        int64_t max_version = file_meta->end_version();
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
        params->active_file_meta = _binlog_file_metas.rbegin()->second;
        params->active_file_writer.swap(_active_binlog_writer);
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
        auto it = _binlog_file_metas.find(lsn);
        bool override_meta = it != _binlog_file_metas.end();
        if (override_meta) {
            BinlogFileMetaPBPtr& old_file_meta = it->second;
            std::unordered_set<int64_t> old_rowsets;
            for (auto rowset_id : old_file_meta->rowsets()) {
                old_rowsets.emplace(rowset_id);
            }

            for (auto rowset_id : meta->rowsets()) {
                if (old_rowsets.count(rowset_id) == 0) {
                    if (_rowset_count_map.count(rowset_id) == 0) {
                        _total_rowset_disk_size += _rowset_fetcher->get_rowset(rowset_id)->data_disk_size();
                    }
                    _rowset_count_map[rowset_id] += 1;
                }
            }

            _total_binlog_file_disk_size += meta->file_size() - old_file_meta->file_size();
            _binlog_file_metas[lsn] = meta;
        } else {
            _binlog_file_metas[lsn] = meta;
            for (auto rowset_id : meta->rowsets()) {
                if (_rowset_count_map.count(rowset_id) == 0) {
                    _total_rowset_disk_size += _rowset_fetcher->get_rowset(rowset_id)->data_disk_size();
                }
                _rowset_count_map[rowset_id] += 1;
            }
            _total_binlog_file_disk_size += meta->file_size();
        }
    }

    _next_file_id = result->next_file_id;
    _active_binlog_writer = result->active_writer;
}

} // namespace starrocks