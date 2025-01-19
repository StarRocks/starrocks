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
#include "gen_cpp/lake_types.pb.h"
#include "gutil/strings/join.h"
#include "runtime/exec_env.h"
#include "storage/lake/metacache.h"
#include "storage/lake/replication_txn_manager.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/txn_log_applier.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/vacuum.h" // delete_files_async
#include "util/lru_cache.h"

namespace {

template <class T>
using ParallelSet = phmap::parallel_flat_hash_set<T, phmap::priv::hash_default_hash<T>, phmap::priv::hash_default_eq<T>,
                                                  phmap::priv::Allocator<T>, 4, std::mutex, true>;
ParallelSet<int64_t> tablet_txns;

// publish version with EMPTY_TXNLOG_TXNID means there is no txnlog
// and need to increase version number of the tablet,
// the situation happens in create rollup.
const int64_t EMPTY_TXNLOG_TXNID = -1;

bool add_tablet(int64_t tablet_id) {
    auto [_, ok] = tablet_txns.insert(tablet_id);
    return ok;
}

void remove_tablet(int64_t tablet_id) {
    tablet_txns.erase(tablet_id);
}

} // namespace

namespace starrocks::lake {

static void clear_remote_snapshot_async(TabletManager* tablet_mgr, int64_t tablet_id, int64_t txn_id,
                                        std::vector<std::string>* files_to_delete) {
    auto slog_path = tablet_mgr->txn_slog_location(tablet_id, txn_id);
    auto txn_slog_or = tablet_mgr->get_txn_log(slog_path, false);

    if (!txn_slog_or.ok()) {
        // Not found is ok
        if (!txn_slog_or.status().is_not_found()) {
            LOG(WARNING) << "Fail to get txn slog " << slog_path << ": " << txn_slog_or.status();

            tablet_mgr->metacache()->erase(slog_path);
            files_to_delete->emplace_back(std::move(slog_path));
        }
        return;
    }

    run_clear_task_async([txn_slog = std::move(txn_slog_or.value())]() {
        (void)ExecEnv::GetInstance()->lake_replication_txn_manager()->clear_snapshots(txn_slog);
    });

    tablet_mgr->metacache()->erase(slog_path);
    files_to_delete->emplace_back(std::move(slog_path));
}

int64_t cal_new_base_version(int64_t tablet_id, TabletManager* tablet_mgr, int64_t base_version, int64_t new_version,
                             const std::span<const TxnInfoPB>& txns) {
    int64_t version = base_version;
    auto metadata = tablet_mgr->get_latest_cached_tablet_metadata(tablet_id);
    if (metadata != nullptr && metadata->version() <= new_version) {
        version = std::max(version, metadata->version());
    }

    auto index_version = tablet_mgr->update_mgr()->get_primary_index_data_version(tablet_id);
    if (index_version > new_version) {
        tablet_mgr->update_mgr()->unload_and_remove_primary_index(tablet_id);
        return version;
    }
    if (index_version > version) {
        // There is a possibility that the index version is newer than the version in remote storage.
        // Check whether the index version exists in remote storage. If not, clear and rebuild the index.
        auto res = tablet_mgr->get_tablet_metadata(tablet_id, index_version, true,
                                                   txns[index_version - base_version - 1].gtid());
        if (res.ok()) {
            version = index_version;
        } else {
            tablet_mgr->update_mgr()->unload_and_remove_primary_index(tablet_id);
        }
    }

    return version;
}

namespace {
std::ostream& operator<<(std::ostream& os, std::span<const TxnInfoPB>& txns) {
    os << "[";
    for (auto i = size_t(0), sz = txns.size(); i < sz; i++) {
        if (i > 0) {
            os << ", ";
        }
        os << txns[i].DebugString();
    }
    os << "]";
    return os;
}

StatusOr<TxnLogPtr> load_txn_log(TabletManager* tablet_mgr, int64_t tablet_id, const TxnInfoPB& txn_info) {
    if (!txn_info.combined_txn_log()) {
        auto log_path = tablet_mgr->txn_log_location(tablet_id, txn_info.txn_id());
        return tablet_mgr->get_txn_log(log_path, false);
    } else {
        auto cache_key = tablet_mgr->txn_log_location(tablet_id, txn_info.txn_id());
        auto ptr = tablet_mgr->metacache()->lookup_txn_log(cache_key);
        if (ptr) {
            return ptr;
        }
        auto log_path = tablet_mgr->combined_txn_log_location(tablet_id, txn_info.txn_id());
        ASSIGN_OR_RETURN(auto combined_log, tablet_mgr->get_combined_txn_log(log_path, true));
        for (const auto& log : combined_log->txn_logs()) {
            if (log.tablet_id() == tablet_id) {
                return std::make_shared<TxnLogPB>(log);
            }
        }
        return Status::InternalError(fmt::format("txn log list does not contain txn log of tablet {}", tablet_id));
    }
}

} // namespace

StatusOr<TabletMetadataPtr> publish_version(TabletManager* tablet_mgr, int64_t tablet_id, int64_t base_version,
                                            int64_t new_version, std::span<const TxnInfoPB> txns) {
    if (txns.size() == 1 && txns[0].txn_id() == EMPTY_TXNLOG_TXNID) {
        LOG(INFO) << "publish version tablet_id: " << tablet_id << ", txn: " << EMPTY_TXNLOG_TXNID
                  << ", base_version: " << base_version << ", new_version: " << new_version;
        // means there is no txnlog and need to increase version number,
        // just return tablet metadata of base_version.
        DCHECK_EQ(new_version, base_version + 1);
        ASSIGN_OR_RETURN(auto metadata, tablet_mgr->get_tablet_metadata(tablet_id, base_version));

        auto new_metadata = std::make_shared<TabletMetadataPB>(*metadata);
        new_metadata->set_version(new_version);
        new_metadata->set_gtid(txns[0].gtid());

        RETURN_IF_ERROR(tablet_mgr->put_tablet_metadata(new_metadata));
        return new_metadata;
    }

    if (!add_tablet(tablet_id)) {
        return Status::ResourceBusy(
                fmt::format("The previous publish version task for tablet {} has not finished. You can ignore this "
                            "error and the task will retry later.",
                            tablet_id));
    }
    DeferOp remove_tablet_txn([&] { remove_tablet(tablet_id); });

    if (txns.size() > 1) {
        CHECK_EQ(new_version, base_version + txns.size());
    }

    VLOG(2) << "publish version tablet_id: " << tablet_id << ", txns: " << txns << ", base_version: " << base_version
            << ", new_version: " << new_version;

    auto new_metadata_path = tablet_mgr->tablet_metadata_location(tablet_id, new_version);
    auto cached_new_metadata = tablet_mgr->metacache()->lookup_tablet_metadata(new_metadata_path);
    if (cached_new_metadata != nullptr) {
        // The retries may be caused by some tablets failing to publish in a partition
        // set the following log as debug log to prevent excessive logging
        VLOG(1) << "Skipped publish version because target metadata found in cache. tablet_id=" << tablet_id
                << " base_version=" << base_version << " new_version=" << new_version << " txns=" << txns;
        return std::move(cached_new_metadata);
    }

    auto new_version_metadata_or_error = [=](const Status& error) -> StatusOr<TabletMetadataPtr> {
        auto res = tablet_mgr->get_tablet_metadata(tablet_id, new_version, txns.back().gtid());
        if (res.ok()) return res;
        return error;
    };

    int64_t ori_base_version = base_version;
    int64_t new_base_version = cal_new_base_version(tablet_id, tablet_mgr, base_version, new_version, txns);
    if (new_base_version > base_version) {
        LOG(INFO) << "Base version has been adjusted. tablet_id=" << tablet_id << " base_version=" << base_version
                  << " new_base_version=" << new_base_version << " new_version=" << new_version << " txns=" << txns;
        base_version = new_base_version;
    }

    if (base_version > new_version) {
        LOG(ERROR) << "base version should be less than or equal to new version, "
                   << "base version=" << base_version << ", new version=" << new_version << ", tablet_id=" << tablet_id;
        return Status::InternalError("base version is larger than new version");
    }

    if (base_version == new_version) {
        return tablet_mgr->get_tablet_metadata(tablet_id, new_version, true, txns.back().gtid());
    }

    // Read base version metadata
    auto base_metadata_or = tablet_mgr->get_tablet_metadata(tablet_id, base_version, false);
    if (base_metadata_or.status().is_not_found()) {
        return new_version_metadata_or_error(base_metadata_or.status());
    }

    if (!base_metadata_or.ok()) {
        LOG(WARNING) << "Fail to get tablet=" << tablet_id << ", version=" << base_version << ": "
                     << base_metadata_or.status();
        return base_metadata_or.status();
    }

    auto base_metadata = std::move(base_metadata_or).value();
    std::unique_ptr<TxnLogApplier> log_applier;
    std::shared_ptr<TabletMetadataPB> new_metadata;
    std::vector<std::string> files_to_delete;
    auto commit_time = txns.back().commit_time();
    auto gtid = txns.back().gtid();

    // Apply txn logs
    int64_t alter_version = -1;
    // Do not delete txn logs if txns_size != 1, let vacuum do the work
    // If the txn logs are deleted, it will be tricky to handle the situation of batch publish switching to single.

    // for example:
    // 1. the mode of publish is batch,
    // 2. txn2 and txn3 have been published successfully and visible version in FE is updated to 3,
    // 3. then txn4 and txn5 are published successfully in BE and the txn_log of txn4 and txn5 have been deleted,
    // but FE do not get the response for some reason,
    // 4. turn the mode of publish to single,
    // 5. txn4 will be published in later publish task, but we can't judge what's the latest_version in BE and we can not reapply txn_log if
    // txn logs have been deleted.
    int txn_offset = base_version - ori_base_version;
    for (size_t i = txn_offset, sz = txns.size(); i < sz; i++) {
        bool ignore_txn_log = false;
        auto txn_log_st = load_txn_log(tablet_mgr, tablet_id, txns[i]);

        if (txn_log_st.status().is_not_found()) {
            if (i == 0) {
                // this may happen in two situations, in every situation,
                // needs take compaction(force_publish=true) into consideration
                // 1. duplicate publish in mode single
                if (txns.size() == 1) {
                    auto res = tablet_mgr->get_tablet_metadata(tablet_id, new_version, true, txns.back().gtid());
                    if (!res.ok() && res.status().is_not_found()) {
                        if (!txns[i].force_publish()) {
                            return txn_log_st.status();
                        } else {
                            ignore_txn_log = true;
                        }
                    } else if (res.ok()) {
                        return res;
                    } else {
                        return txn_log_st.status();
                    }
                }

                // 2. when converting from single publish to batch for txn log has been deleted,
                // for example:
                // the current mode of publish is single,
                // txn2 has been published successfully and visible version in FE is updated to 2,
                // then txn3 is published successfully in BE and the txn_log of txn3 has been deleted, but FE do not get the response for some reason,
                // turn the mode of publish to batch,
                // txn3 ,txn4, txn5 will be published in one publish batch task, so txn3 should be skipped just apply txn_log of txn4 and txn5.
                auto missig_txn_log_meta =
                        tablet_mgr->get_tablet_metadata(tablet_id, base_version + 1, true, txns[0].gtid());
                if (missig_txn_log_meta.status().is_not_found()) {
                    if (txns[i].force_publish()) {
                        // can not change `base_metadata` below, just use old one
                        ignore_txn_log = true;
                    } else {
                        LOG(WARNING) << "txn_log of txn: " << txns[i].txn_id()
                                     << " not found, and can not find the tablet_meta";
                        return Status::InternalError("Both txn_log and corresponding tablet_meta missing");
                    }
                } else if (!missig_txn_log_meta.status().ok()) {
                    LOG(WARNING) << "txn_log of txn: " << txns[i].txn_id() << " not found, find the tablet_meta error: "
                                 << missig_txn_log_meta.status().to_string();
                    return new_version_metadata_or_error(missig_txn_log_meta.status());
                } else {
                    base_metadata = std::move(missig_txn_log_meta).value();
                    continue;
                }
            } else if (txns[i].force_publish()) {
                ignore_txn_log = true;
            } else {
                return new_version_metadata_or_error(txn_log_st.status());
            }
        }

        if (!txn_log_st.ok() && !ignore_txn_log) {
            LOG(WARNING) << "Fail to get txn log: " << txn_log_st.status() << " tablet_id=" << tablet_id
                         << " txn=" << txns[i].DebugString();
            return txn_log_st.status();
        }

        if (log_applier == nullptr) {
            // init log_applier
            new_metadata = std::make_shared<TabletMetadataPB>(*base_metadata);
            log_applier = new_txn_log_applier(Tablet(tablet_mgr, tablet_id), new_metadata, new_version,
                                              txns[i].rebuild_pindex());

            if (new_metadata->compaction_inputs_size() > 0) {
                new_metadata->mutable_compaction_inputs()->Clear();
            }

            if (new_metadata->orphan_files_size() > 0) {
                new_metadata->mutable_orphan_files()->Clear();
            }

            // force update prev_garbage_version at most config::lake_max_garbage_version_distance,
            // prevent prev_garbage_version from not being updated for a long time and affecting tablet meta vacuum
            if (base_metadata->compaction_inputs_size() > 0 || base_metadata->orphan_files_size() > 0 ||
                base_metadata->version() - base_metadata->prev_garbage_version() >=
                        config::lake_max_garbage_version_distance) {
                new_metadata->set_prev_garbage_version(base_metadata->version());
            }

            new_metadata->set_commit_time(commit_time);
            new_metadata->set_gtid(gtid);

            auto init_st = log_applier->init();
            if (!init_st.ok()) {
                if (init_st.is_already_exist()) {
                    return new_version_metadata_or_error(init_st);
                } else {
                    return init_st;
                }
            }
        }

        // txn log not found and can be ignored, only compaction will reach here, do nothing
        if (ignore_txn_log) {
            LOG(INFO) << "txn_log of txn: " << txns[i].txn_id() << " for tablet: " << tablet_id
                      << " not found, force publish is on, ignore txn log";
            log_applier->observe_empty_compaction(); // record empty compaction
            continue;
        }

        auto& txn_log = txn_log_st.value();
        if (txn_log->has_op_schema_change()) {
            alter_version = txn_log->op_schema_change().alter_version();
        }

        auto st = log_applier->apply(*txn_log);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to apply txn log : " << st << " tablet_id=" << tablet_id
                         << " txn=" << txns[i].DebugString();
            return st;
        }

        auto tablet_log_path = tablet_mgr->txn_log_location(tablet_id, txns[i].txn_id());
        if (txns.size() == 1 && !txns[i].combined_txn_log()) {
            files_to_delete.emplace_back(tablet_log_path);
        }

        tablet_mgr->metacache()->erase(tablet_log_path);

        // Clear remote snapshot and slog for replication txn
        if (txn_log->has_op_replication()) {
            clear_remote_snapshot_async(tablet_mgr, tablet_id, txns[i].txn_id(), &files_to_delete);
        }
    }

    // Apply vtxn logs for schema change
    // Should firstly apply schema change txn log, then apply txn version logs,
    // because the rowsets in txn log are older.
    if (alter_version != -1 && alter_version + 1 < new_version) {
        DCHECK(base_version == 1 && txns.size() == 1);
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

    delete_files_async(std::move(files_to_delete));

    return new_metadata;
}

Status publish_log_version(TabletManager* tablet_mgr, int64_t tablet_id, std::span<const TxnInfoPB> txn_infos,
                           const int64_t* log_versions) {
    auto files_to_delete = std::vector<std::string>{};
    for (int i = 0; i < txn_infos.size(); i++) {
        if (!txn_infos[i].combined_txn_log()) {
            auto txn_id = txn_infos[i].txn_id();
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
        } else {
            ASSIGN_OR_RETURN(auto txn_log, load_txn_log(tablet_mgr, tablet_id, txn_infos[i]));
            auto log_version = log_versions[i];
            RETURN_IF_ERROR(tablet_mgr->put_txn_vlog(txn_log, log_version));
        }
    }
    delete_files_async(std::move(files_to_delete));
    return Status::OK();
}

static void collect_files_in_log(TabletManager* tablet_mgr, const TxnLog& txn_log,
                                 std::vector<std::string>* files_to_delete) {
    auto tablet_id = txn_log.tablet_id();
    if (txn_log.has_op_write()) {
        for (const auto& segment : txn_log.op_write().rowset().segments()) {
            files_to_delete->emplace_back(tablet_mgr->segment_location(tablet_id, segment));
        }
        for (const auto& del_file : txn_log.op_write().dels()) {
            files_to_delete->emplace_back(tablet_mgr->del_location(tablet_id, del_file));
        }
    }
    if (txn_log.has_op_compaction()) {
        for (const auto& segment : txn_log.op_compaction().output_rowset().segments()) {
            files_to_delete->emplace_back(tablet_mgr->segment_location(tablet_id, segment));
        }
    }
    if (txn_log.has_op_schema_change() && !txn_log.op_schema_change().linked_segment()) {
        for (const auto& rowset : txn_log.op_schema_change().rowsets()) {
            for (const auto& segment : rowset.segments()) {
                files_to_delete->emplace_back(tablet_mgr->segment_location(tablet_id, segment));
            }
        }
    }
    if (txn_log.has_op_replication()) {
        for (const auto& op_write : txn_log.op_replication().op_writes()) {
            for (const auto& segment : op_write.rowset().segments()) {
                files_to_delete->emplace_back(tablet_mgr->segment_location(tablet_id, segment));
            }
            for (const auto& del_file : op_write.dels()) {
                files_to_delete->emplace_back(tablet_mgr->del_location(tablet_id, del_file));
            }
        }
    }

    tablet_mgr->metacache()->erase(tablet_mgr->txn_log_location(tablet_id, txn_log.txn_id()));
    tablet_mgr->update_mgr()->try_remove_cache(tablet_id, txn_log.txn_id());
}

void abort_txn(TabletManager* tablet_mgr, int64_t tablet_id, std::span<const TxnInfoPB> txns) {
    TEST_SYNC_POINT("transactions::abort_txn:enter");
    std::vector<std::string> files_to_delete;
    for (size_t i = 0, sz = txns.size(); i < sz; ++i) {
        auto txn_id = txns[i].txn_id();
        auto txn_type = txns[i].txn_type();

        // Clear remote snapshot and slog for replication txn
        if (txn_type == TxnTypePB::TXN_REPLICATION) {
            clear_remote_snapshot_async(tablet_mgr, tablet_id, txn_id, &files_to_delete);
        }

        if (!txns[i].combined_txn_log()) {
            auto log_path = tablet_mgr->txn_log_location(tablet_id, txn_id);
            auto txn_log_or = tablet_mgr->get_txn_log(log_path, false);
            if (!txn_log_or.ok()) {
                LOG_IF(WARNING, !txn_log_or.status().is_not_found())
                        << "Fail to get txn log " << log_path << ": " << txn_log_or.status();
                continue;
            }

            TxnLogPtr txn_log = std::move(txn_log_or).value();
            collect_files_in_log(tablet_mgr, *txn_log, &files_to_delete);
            files_to_delete.emplace_back(log_path);
        } else {
            auto combined_log_path = tablet_mgr->combined_txn_log_location(tablet_id, txn_id);
            auto combined_log_or = tablet_mgr->get_combined_txn_log(combined_log_path, false);
            if (!combined_log_or.ok()) {
                LOG_IF(WARNING, !combined_log_or.status().is_not_found())
                        << "Fail to get txn log " << combined_log_path << ": " << combined_log_or.status();
                continue;
            }
            for (const auto& log : combined_log_or.value()->txn_logs()) {
                collect_files_in_log(tablet_mgr, log, &files_to_delete);
            }
            files_to_delete.emplace_back(combined_log_path);
        }
    }

    delete_files_async(std::move(files_to_delete));
}

} // namespace starrocks::lake
