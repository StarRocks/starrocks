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

#include "storage/lake/transactions.h"

#include "fs/fs_util.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/txn_log_applier.h"
#include "storage/lake/vacuum.h" // delete_files_async
#include "util/lru_cache.h"

namespace starrocks::lake {

// check whether all txn log exists, for versions may be published repeatedly and
// txn log has been deleted when converting from single publish to batch,
// there we find the latest txnLog as base_version to skip repeated version.

// for example:
// the mode of publish is single,
// txn2 has been published successfully and visible version in FE is updated to 2,
// then txn3 is published successfully in BE and the txn_log of txn3 has been deleted, but FE do not get the response for some reason,
// turn the mode of publish to batch,
// txn3 ,txn4, txn5 will be published in one publish batch task, so txn3 should be skipped and should return 1, just apply txn_log of txn4 and txn5.
Status get_base_tablet_metadat_index(TabletManager* tablet_mgr, int64_t tablet_id, int64_t base_version,
                                     int64_t new_version, std::span<const int64_t>& txn_ids, int& result_base_index) {
    result_base_index = -1;
    for (int i = 0; i < txn_ids.size(); i++) {
        auto txn_id = txn_ids[i];
        auto log_path = tablet_mgr->txn_log_location(tablet_id, txn_id);
        auto txn_log_st = tablet_mgr->get_txn_log(log_path, false);

        if (txn_log_st.status().is_not_found()) {
            auto missig_txn_log_meta = tablet_mgr->get_tablet_metadata()(tablet_id, base_version + i + 1);
            if (missig_txn_log_meta.status().is_not_found()) {
                // this should't happen
                LOG(WARNING) << "txn_log of txn: " << txn_id << " not found, and can not find the tablet_meta";
                return Status::InternalError("Both txn_log and corresponding tablet_meta missing");
            } else {
                result_base_index = i;
                break;
            }
        }
    }

    // all txnlog are not found
    if (UNLIKELY(result_base_index == -1)) {
        return Status::NotFound("all txn_log missing");
    }
    return Status::OK();
}

StatusOr<TabletMetadataPtr> publish_version(TabletManager* tablet_mgr, int64_t tablet_id, int64_t base_version,
                                            int64_t new_version, std::span<const int64_t> txn_ids,
                                            int64_t commit_time) {
    VLOG(1) << "publish version tablet_id: " << tablet_id << ", txns: " << txn_ids << ", base_version: " << base_version
            << ", new_version: " << new_version;

    auto new_version_metadata_or_error = [=](Status error) -> StatusOr<TabletMetadataPtr> {
        auto res = tablet_mgr->get_tablet_metadata(tablet_id, new_version);
        if (res.ok()) return res;
        return error;
    };

    // Read base version metadata
    auto base_version_path = tablet_mgr->tablet_metadata_location(tablet_id, base_version);
    auto base_metadata_or = tablet_mgr->get_tablet_metadata(base_version_path, false);
    if (base_metadata_or.status().is_not_found()) {
        return new_version_metadata_or_error(base_metadata_or.status());
    }

    if (!base_metadata_or.ok()) {
        LOG(WARNING) << "Fail to get " << base_version_path << ": " << base_metadata_or.status();
        return base_metadata_or.status();
    }

    int base_version_index = 0;
    auto index_status = get_base_tablet_metadat_index(tablet_mgr, tablet_id, base_version, new_version, txn_ids,
                                                      base_version_index);
    if (index_status.is_not_found()) {
        LOG(WARNING) << "all txn_log missing, txn_ids: " << txn_ids;
    }

    if (!index_status.ok()) {
        return new_version_metadata_or_error(index_status);
    }

    auto base_metadata = std::move(base_metadata_or).value();
    if (UNLIKELY(base_version_index != 0)) {
        auto latest_published_metadata = tablet_mgr->get_tablet_metadata(tablet_id, base_version + base_version_index);
        base_metadata = std::move(latest_published_metadata).value();
    }

    auto new_metadata = std::make_shared<TabletMetadataPB>(*base_metadata);
    auto log_applier = new_txn_log_applier(Tablet(tablet_mgr, tablet_id), new_metadata, new_version);

    if (new_metadata->compaction_inputs_size() > 0) {
        new_metadata->mutable_compaction_inputs()->Clear();
    }

    if (new_metadata->orphan_files_size() > 0) {
        new_metadata->mutable_orphan_files()->Clear();
    }

    if (base_metadata->compaction_inputs_size() > 0 || base_metadata->orphan_files_size() > 0) {
        new_metadata->set_prev_garbage_version(base_metadata->version());
    }

    new_metadata->set_commit_time(commit_time);

    auto init_st = log_applier->init();
    if (!init_st.ok()) {
        if (init_st.is_already_exist()) {
            return new_version_metadata_or_error(init_st);
        } else {
            return init_st;
        }
    }

    std::vector<std::string> files_to_delete;

    // Apply txn logs
    int64_t alter_version = -1;
    // Do not delete txn logs if txns_size != 1, let gc do the work
    // If the txn logs are deleted, it will be tricky to handle the situation of batch publish switching to single.

    // for example:
    // 1. the mode of publish is batch,
    // 2. txn2 and txn3 have been published successfully and visible version in FE is updated to 3,
    // 3. then txn4 and txn5 are published successfully in BE and the txn_log of txn4 and txn5 have been deleted,
    // but FE do not get the response for some reason,
    // 4. turn the mode of publish to single,
    // 5. txn4 will be published in later publish task, but we can't judge what's the latest_version in BE and we can not reapply txn_log if
    // txn logs have been deleted.
    bool delete_txn_log = (txn_ids.size() == 1);
    for (int i = base_version_index; i < txn_ids.size(); i++) {
        auto txn_id = txn_ids[i];
        auto log_path = tablet_mgr->txn_log_location(tablet_id, txn_id);
        auto txn_log_st = tablet_mgr->get_txn_log(log_path, false);

        if (txn_log_st.status().is_not_found()) {
            return new_version_metadata_or_error(txn_log_st.status());
        }

        if (!txn_log_st.ok()) {
            LOG(WARNING) << "Fail to get " << log_path << ": " << txn_log_st.status();
            return txn_log_st.status();
        }

        auto& txn_log = txn_log_st.value();
        if (txn_log->has_op_schema_change()) {
            alter_version = txn_log->op_schema_change().alter_version();
        }

        auto st = log_applier->apply(*txn_log);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to apply " << log_path << ": " << st;
            return st;
        }

        if (delete_txn_log) {
            files_to_delete.emplace_back(log_path);
        }

        tablet_mgr->metacache()->erase(log_path);
    }

    // Apply vtxn logs for schema change
    // Should firstly apply schema change txn log, then apply txn version logs,
    // because the rowsets in txn log are older.
    if (alter_version != -1 && alter_version + 1 < new_version) {
        DCHECK(base_version == 1 && txn_ids.size() == 1);
        for (int64_t v = alter_version + 1; v < new_version; ++v) {
            auto vlog_path = tablet_mgr->txn_vlog_location(tablet_id, v);
            auto txn_vlog = tablet_mgr->get_txn_vlog(vlog_path, false);
            if (txn_vlog.status().is_not_found()) {
                return new_version_metadata_or_error(txn_vlog.status());
            }

            if (!txn_vlog.ok()) {
                LOG(WARNING) << "Fail to get " << vlog_path << ": " << txn_vlog.status();
                return txn_vlog.status();
            }

            auto st = log_applier->apply(**txn_vlog);
            if (!st.ok()) {
                LOG(WARNING) << "Fail to apply " << vlog_path << ": " << st;
                return st;
            }

            files_to_delete.emplace_back(vlog_path);

            tablet_mgr->metacache()->erase(vlog_path);
        }
    }

    // Save new metadata
    RETURN_IF_ERROR(log_applier->finish());

    // collect trash files, and remove them by background threads
    auto trash_files = log_applier->trash_files();
    if (trash_files != nullptr) {
        files_to_delete.insert(files_to_delete.end(), trash_files->begin(), trash_files->end());
    }

    delete_files_async(std::move(files_to_delete));

    return new_metadata;
}

Status publish_log_version(TabletManager* tablet_mgr, int64_t tablet_id, const int64_t* txn_ids,
                           const int64* log_versions, int txns_size) {
    std::vector<std::string> files_to_delete;
    for (int i = 0; i < txns_size; i++) {
        auto txn_id = txn_ids[i];
        auto log_version = log_versions[i];
        auto txn_log_path = tablet_mgr->txn_log_location(tablet_id, txn_id);
        auto txn_vlog_path = tablet_mgr->txn_vlog_location(tablet_id, log_version);
        // TODO: use rename() API if supported by the underlying filesystem.
        auto st = fs::copy_file(txn_log_path, txn_vlog_path);
        if (st.is_not_found()) {
            ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(txn_vlog_path));
            auto check_st = fs->path_exists(txn_vlog_path);
            if (check_st.ok()) {
                continue;
            } else {
                LOG_IF(WARNING, !check_st.is_not_found())
                        << "Fail to check the existance of " << txn_vlog_path << ": " << check_st;
                return st;
            }
        } else if (!st.ok()) {
            return st;
        } else {
            files_to_delete.emplace_back(txn_log_path);
            tablet_mgr->metacache()->erase(txn_log_path);
        }
    }
    delete_files_async(std::move(files_to_delete));
    return Status::OK();
}

void abort_txn(TabletManager* tablet_mgr, int64_t tablet_id, std::span<const int64_t> txn_ids) {
    std::vector<std::string> files_to_delete;
    for (auto txn_id : txn_ids) {
        auto log_path = tablet_mgr->txn_log_location(tablet_id, txn_id);
        auto txn_log_or = tablet_mgr->get_txn_log(log_path, false);
        if (!txn_log_or.ok()) {
            LOG_IF(WARNING, !txn_log_or.status().is_not_found())
                    << "Fail to get txn log " << log_path << ": " << txn_log_or.status();
            continue;
        }

        TxnLogPtr txn_log = std::move(txn_log_or).value();
        if (txn_log->has_op_write()) {
            for (const auto& segment : txn_log->op_write().rowset().segments()) {
                files_to_delete.emplace_back(tablet_mgr->segment_location(tablet_id, segment));
            }
            for (const auto& del_file : txn_log->op_write().dels()) {
                files_to_delete.emplace_back(tablet_mgr->del_location(tablet_id, del_file));
            }
        }
        if (txn_log->has_op_compaction()) {
            for (const auto& segment : txn_log->op_compaction().output_rowset().segments()) {
                files_to_delete.emplace_back(tablet_mgr->segment_location(tablet_id, segment));
            }
        }
        if (txn_log->has_op_schema_change() && !txn_log->op_schema_change().linked_segment()) {
            for (const auto& rowset : txn_log->op_schema_change().rowsets()) {
                for (const auto& segment : rowset.segments()) {
                    files_to_delete.emplace_back(tablet_mgr->segment_location(tablet_id, segment));
                }
            }
        }

        files_to_delete.emplace_back(log_path);

        tablet_mgr->metacache()->erase(log_path);
    }

    delete_files_async(std::move(files_to_delete));
}

} // namespace starrocks::lake
