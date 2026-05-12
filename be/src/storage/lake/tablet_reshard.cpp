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

#include "storage/lake/tablet_reshard.h"

#include <butil/time.h>
#include <bvar/bvar.h>

#include <unordered_map>

#include "base/utility/defer_op.h"
#include "common/logging.h"
#include "runtime/exec_env.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_merger.h"
#include "storage/lake/tablet_reshard_helper.h"
#include "storage/lake/tablet_splitter.h"
#include "storage/lake/transactions.h"
#include "storage/lake/vacuum.h" // delete_files_async

// Layer 1: Reshard operation overall metrics
bvar::Adder<int64_t> g_tablet_reshard_total("tablet_reshard_total");
bvar::Adder<int64_t> g_tablet_reshard_failed("tablet_reshard_failed");
bvar::LatencyRecorder g_tablet_reshard_latency("tablet_reshard");

// Layer 2: Split metrics
bvar::Adder<int64_t> g_tablet_reshard_split_total("tablet_reshard_split_total");
bvar::Adder<int64_t> g_tablet_reshard_split_failed("tablet_reshard_split_failed");
bvar::LatencyRecorder g_tablet_reshard_split_latency("tablet_reshard_split");
bvar::Adder<int64_t> g_tablet_reshard_split_output_tablet_count("tablet_reshard_split_output_tablet_count");
bvar::Adder<int64_t> g_tablet_reshard_split_fallback_total("tablet_reshard_split_fallback_total");

// Layer 2: Merge metrics
bvar::Adder<int64_t> g_tablet_reshard_merge_total("tablet_reshard_merge_total");
bvar::Adder<int64_t> g_tablet_reshard_merge_failed("tablet_reshard_merge_failed");
bvar::LatencyRecorder g_tablet_reshard_merge_latency("tablet_reshard_merge");
bvar::Adder<int64_t> g_tablet_reshard_merge_input_tablet_count("tablet_reshard_merge_input_tablet_count");

// Layer 2: Identical metrics
bvar::Adder<int64_t> g_tablet_reshard_identical_total("tablet_reshard_identical_total");
bvar::Adder<int64_t> g_tablet_reshard_identical_failed("tablet_reshard_identical_failed");

// Layer 3: Cross publish metrics
bvar::Adder<int64_t> g_tablet_reshard_cross_publish_total("tablet_reshard_cross_publish_total");
bvar::Adder<int64_t> g_tablet_reshard_cross_publish_splitting_total("tablet_reshard_cross_publish_splitting_total");
bvar::Adder<int64_t> g_tablet_reshard_cross_publish_merging_total("tablet_reshard_cross_publish_merging_total");
bvar::Adder<int64_t> g_tablet_reshard_cross_publish_identical_total("tablet_reshard_cross_publish_identical_total");

namespace starrocks::lake {

namespace {

std::ostream& operator<<(std::ostream& out, const std::vector<int64_t>& tablet_ids) {
    out << '[';
    for (size_t i = 0; i < tablet_ids.size(); ++i) {
        if (i > 0) {
            out << ", ";
        }
        out << tablet_ids[i];
    }
    out << ']';
    return out;
}

Status handle_splitting_tablet(TabletManager* tablet_manager, const SplittingTabletInfoPB& splitting_tablet,
                               int64_t base_version, int64_t new_version, const TxnInfoPB& txn_info,
                               std::unordered_map<int64_t, TabletMetadataPtr>& new_metadatas,
                               std::unordered_map<int64_t, TabletRangePB>& tablet_ranges) {
    {
        auto old_tablet_new_metadata_location =
                tablet_manager->tablet_metadata_location(splitting_tablet.old_tablet_id(), new_version);
        auto cached_old_tablet_new_metadata =
                tablet_manager->metacache()->lookup_tablet_metadata(old_tablet_new_metadata_location);
        if (cached_old_tablet_new_metadata == nullptr) {
            goto CONTINUE_HANDLE_SPLITTING_TABLET;
        }

        new_metadatas.emplace(splitting_tablet.old_tablet_id(), std::move(cached_old_tablet_new_metadata));
        for (auto new_tablet_id : splitting_tablet.new_tablet_ids()) {
            auto new_tablet_new_metadata_location =
                    tablet_manager->tablet_metadata_location(new_tablet_id, new_version);
            auto cached_new_tablet_new_metadata =
                    tablet_manager->metacache()->lookup_tablet_metadata(new_tablet_new_metadata_location);
            if (cached_new_tablet_new_metadata == nullptr) {
                new_metadatas.clear();
                tablet_ranges.clear();
                goto CONTINUE_HANDLE_SPLITTING_TABLET;
            }
            tablet_ranges.emplace(new_tablet_id, cached_new_tablet_new_metadata->range());
            new_metadatas.emplace(new_tablet_id, std::move(cached_new_tablet_new_metadata));
        }
        return Status::OK();
    }

CONTINUE_HANDLE_SPLITTING_TABLET:
    g_tablet_reshard_split_total << 1;

    auto old_tablet_old_metadata_or =
            tablet_manager->get_tablet_metadata(splitting_tablet.old_tablet_id(), base_version, false);
    if (old_tablet_old_metadata_or.status().is_not_found()) {
        auto old_tablet_new_metadata_or =
                tablet_manager->get_tablet_metadata(splitting_tablet.old_tablet_id(), new_version, txn_info.gtid());
        if (!old_tablet_new_metadata_or.ok()) {
            g_tablet_reshard_split_failed << 1;
            return old_tablet_old_metadata_or.status();
        }
        new_metadatas.emplace(splitting_tablet.old_tablet_id(), std::move(old_tablet_new_metadata_or.value()));

        for (auto new_tablet_id : splitting_tablet.new_tablet_ids()) {
            auto new_tablet_new_metadata_or =
                    tablet_manager->get_tablet_metadata(new_tablet_id, new_version, txn_info.gtid());
            if (!new_tablet_new_metadata_or.ok()) {
                g_tablet_reshard_split_failed << 1;
                return old_tablet_old_metadata_or.status();
            }
            auto& new_tablet_new_metadata = new_tablet_new_metadata_or.value();
            tablet_ranges.emplace(new_tablet_id, new_tablet_new_metadata->range());
            new_metadatas.emplace(new_tablet_id, std::move(new_tablet_new_metadata));
        }
        return Status::OK();
    }

    if (!old_tablet_old_metadata_or.ok()) {
        LOG(WARNING) << "Failed to get tablet: " << splitting_tablet.old_tablet_id() << ", version: " << base_version
                     << ", txn_id: " << txn_info.txn_id() << ", status: " << old_tablet_old_metadata_or.status();
        g_tablet_reshard_split_failed << 1;
        return old_tablet_old_metadata_or.status();
    }

    const auto& old_tablet_old_metadata = old_tablet_old_metadata_or.value();

    auto old_tablet_new_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_old_metadata);
    old_tablet_new_metadata->set_version(new_version);
    old_tablet_new_metadata->set_commit_time(txn_info.commit_time());
    old_tablet_new_metadata->set_gtid(txn_info.gtid());
    tablet_reshard_helper::set_all_data_files_shared(old_tablet_new_metadata.get());
    new_metadatas.emplace(splitting_tablet.old_tablet_id(), std::move(old_tablet_new_metadata));

    auto split_start_ts = butil::gettimeofday_us();
    auto split_new_metadatas_or =
            split_tablet(tablet_manager, old_tablet_old_metadata, splitting_tablet, new_version, txn_info);
    if (!split_new_metadatas_or.ok()) {
        g_tablet_reshard_split_failed << 1;
        return split_new_metadatas_or.status();
    }
    g_tablet_reshard_split_latency << (butil::gettimeofday_us() - split_start_ts);
    auto split_new_metadatas = std::move(split_new_metadatas_or.value());
    g_tablet_reshard_split_output_tablet_count << split_new_metadatas.size();
    for (auto& [tablet_id, tablet_metadata] : split_new_metadatas) {
        tablet_ranges.emplace(tablet_id, tablet_metadata->range());
        new_metadatas.emplace(tablet_id, std::move(tablet_metadata));
    }
    return Status::OK();
}

Status handle_merging_tablet(TabletManager* tablet_manager, const MergingTabletInfoPB& merging_tablet,
                             int64_t base_version, int64_t new_version, const TxnInfoPB& txn_info,
                             std::unordered_map<int64_t, TabletMetadataPtr>& new_metadatas,
                             std::unordered_map<int64_t, TabletRangePB>& tablet_ranges) {
    {
        for (auto old_tablet_id : merging_tablet.old_tablet_ids()) {
            auto old_tablet_new_metadata_location =
                    tablet_manager->tablet_metadata_location(old_tablet_id, new_version);
            auto cached_old_tablet_new_metadata =
                    tablet_manager->metacache()->lookup_tablet_metadata(old_tablet_new_metadata_location);
            if (cached_old_tablet_new_metadata == nullptr) {
                goto CONTINUE_HANDLE_MERGING_TABLET;
            }
            new_metadatas.emplace(old_tablet_id, std::move(cached_old_tablet_new_metadata));
        }

        auto new_tablet_new_metadata_location =
                tablet_manager->tablet_metadata_location(merging_tablet.new_tablet_id(), new_version);
        auto cached_new_tablet_new_metadata =
                tablet_manager->metacache()->lookup_tablet_metadata(new_tablet_new_metadata_location);
        if (cached_new_tablet_new_metadata == nullptr) {
            new_metadatas.clear();
            goto CONTINUE_HANDLE_MERGING_TABLET;
        }
        tablet_ranges.emplace(merging_tablet.new_tablet_id(), cached_new_tablet_new_metadata->range());
        new_metadatas.emplace(merging_tablet.new_tablet_id(), std::move(cached_new_tablet_new_metadata));
        return Status::OK();
    }

CONTINUE_HANDLE_MERGING_TABLET:
    g_tablet_reshard_merge_total << 1;
    g_tablet_reshard_merge_input_tablet_count << merging_tablet.old_tablet_ids_size();

    std::vector<TabletMetadataPtr> old_tablet_metadatas;
    old_tablet_metadatas.reserve(merging_tablet.old_tablet_ids_size());
    for (auto old_tablet_id : merging_tablet.old_tablet_ids()) {
        auto old_tablet_old_metadata_or = tablet_manager->get_tablet_metadata(old_tablet_id, base_version, false);
        if (old_tablet_old_metadata_or.status().is_not_found()) {
            new_metadatas.clear();
            for (auto retry_tablet_id : merging_tablet.old_tablet_ids()) {
                auto old_tablet_new_metadata_or =
                        tablet_manager->get_tablet_metadata(retry_tablet_id, new_version, txn_info.gtid());
                if (!old_tablet_new_metadata_or.ok()) {
                    g_tablet_reshard_merge_failed << 1;
                    return old_tablet_old_metadata_or.status();
                }
                new_metadatas.emplace(retry_tablet_id, std::move(old_tablet_new_metadata_or.value()));
            }
            auto new_tablet_new_metadata_or =
                    tablet_manager->get_tablet_metadata(merging_tablet.new_tablet_id(), new_version, txn_info.gtid());
            if (!new_tablet_new_metadata_or.ok()) {
                g_tablet_reshard_merge_failed << 1;
                return old_tablet_old_metadata_or.status();
            }
            auto& new_tablet_new_metadata = new_tablet_new_metadata_or.value();
            tablet_ranges.emplace(merging_tablet.new_tablet_id(), new_tablet_new_metadata->range());
            new_metadatas.emplace(merging_tablet.new_tablet_id(), std::move(new_tablet_new_metadata));
            return Status::OK();
        }
        if (!old_tablet_old_metadata_or.ok()) {
            LOG(WARNING) << "Failed to get tablet: " << old_tablet_id << ", version: " << base_version
                         << ", txn_id: " << txn_info.txn_id() << ", status: " << old_tablet_old_metadata_or.status();
            g_tablet_reshard_merge_failed << 1;
            return old_tablet_old_metadata_or.status();
        }
        old_tablet_metadatas.emplace_back(std::move(old_tablet_old_metadata_or.value()));
    }

    for (const auto& old_tablet_old_metadata : old_tablet_metadatas) {
        auto old_tablet_new_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_old_metadata);
        old_tablet_new_metadata->set_version(new_version);
        old_tablet_new_metadata->set_commit_time(txn_info.commit_time());
        old_tablet_new_metadata->set_gtid(txn_info.gtid());
        tablet_reshard_helper::set_all_data_files_shared(old_tablet_new_metadata.get(), true);
        new_metadatas.emplace(old_tablet_old_metadata->id(), std::move(old_tablet_new_metadata));
    }

    auto merge_start_ts = butil::gettimeofday_us();
    auto new_tablet_metadata_or =
            merge_tablet(tablet_manager, old_tablet_metadatas, merging_tablet, new_version, txn_info);
    if (!new_tablet_metadata_or.ok()) {
        g_tablet_reshard_merge_failed << 1;
        return new_tablet_metadata_or.status();
    }
    g_tablet_reshard_merge_latency << (butil::gettimeofday_us() - merge_start_ts);
    auto new_tablet_metadata = std::move(new_tablet_metadata_or.value());

    tablet_ranges.emplace(merging_tablet.new_tablet_id(), new_tablet_metadata->range());
    new_metadatas.emplace(merging_tablet.new_tablet_id(), std::move(new_tablet_metadata));
    return Status::OK();
}

Status handle_identical_tablet(TabletManager* tablet_manager, const IdenticalTabletInfoPB& identical_tablet,
                               int64_t base_version, int64_t new_version, const TxnInfoPB& txn_info,
                               std::unordered_map<int64_t, TabletMetadataPtr>& new_metadatas) {
    {
        auto old_tablet_new_metadata_location =
                tablet_manager->tablet_metadata_location(identical_tablet.old_tablet_id(), new_version);
        auto cached_old_tablet_new_metadata =
                tablet_manager->metacache()->lookup_tablet_metadata(old_tablet_new_metadata_location);
        if (cached_old_tablet_new_metadata == nullptr) {
            goto CONTINUE_HANDLE_IDENTICAL_TABLET;
        }

        new_metadatas.emplace(identical_tablet.old_tablet_id(), std::move(cached_old_tablet_new_metadata));

        auto new_tablet_new_metadata_location =
                tablet_manager->tablet_metadata_location(identical_tablet.new_tablet_id(), new_version);
        auto cached_new_tablet_new_metadata =
                tablet_manager->metacache()->lookup_tablet_metadata(new_tablet_new_metadata_location);
        if (cached_new_tablet_new_metadata == nullptr) {
            new_metadatas.clear();
            goto CONTINUE_HANDLE_IDENTICAL_TABLET;
        }

        new_metadatas.emplace(identical_tablet.new_tablet_id(), std::move(cached_new_tablet_new_metadata));
        return Status::OK();
    }

CONTINUE_HANDLE_IDENTICAL_TABLET:
    g_tablet_reshard_identical_total << 1;

    auto old_tablet_old_metadata_or =
            tablet_manager->get_tablet_metadata(identical_tablet.old_tablet_id(), base_version, false);
    if (old_tablet_old_metadata_or.status().is_not_found()) {
        auto old_tablet_new_metadata_or =
                tablet_manager->get_tablet_metadata(identical_tablet.old_tablet_id(), new_version, txn_info.gtid());
        if (!old_tablet_new_metadata_or.ok()) {
            g_tablet_reshard_identical_failed << 1;
            return old_tablet_old_metadata_or.status();
        }
        new_metadatas.emplace(identical_tablet.old_tablet_id(), std::move(old_tablet_new_metadata_or.value()));

        auto new_tablet_new_metadata_or =
                tablet_manager->get_tablet_metadata(identical_tablet.new_tablet_id(), new_version, txn_info.gtid());
        if (!new_tablet_new_metadata_or.ok()) {
            g_tablet_reshard_identical_failed << 1;
            return old_tablet_old_metadata_or.status();
        }
        new_metadatas.emplace(identical_tablet.new_tablet_id(), std::move(new_tablet_new_metadata_or.value()));
        return Status::OK();
    }

    if (!old_tablet_old_metadata_or.ok()) {
        LOG(WARNING) << "Failed to get tablet: " << identical_tablet.old_tablet_id() << ", version: " << base_version
                     << ", txn_id: " << txn_info.txn_id() << ", status: " << old_tablet_old_metadata_or.status();
        g_tablet_reshard_identical_failed << 1;
        return old_tablet_old_metadata_or.status();
    }

    const auto& old_tablet_old_metadata = old_tablet_old_metadata_or.value();

    auto old_tablet_new_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_old_metadata);
    old_tablet_new_metadata->set_version(new_version);
    old_tablet_new_metadata->set_commit_time(txn_info.commit_time());
    old_tablet_new_metadata->set_gtid(txn_info.gtid());
    tablet_reshard_helper::set_all_data_files_shared(old_tablet_new_metadata.get());
    new_metadatas.emplace(identical_tablet.old_tablet_id(), std::move(old_tablet_new_metadata));

    auto new_tablet_new_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_old_metadata);
    new_tablet_new_metadata->set_id(identical_tablet.new_tablet_id());
    new_tablet_new_metadata->set_version(new_version);
    new_tablet_new_metadata->set_commit_time(txn_info.commit_time());
    new_tablet_new_metadata->set_gtid(txn_info.gtid());
    new_tablet_new_metadata->clear_compaction_inputs();
    new_tablet_new_metadata->clear_orphan_files();
    new_tablet_new_metadata->clear_prev_garbage_version();
    new_metadatas.emplace(identical_tablet.new_tablet_id(), std::move(new_tablet_new_metadata));
    return Status::OK();
}

// Transform |txn_log| (which still carries the source tablet id) for publish
// on the merged tablet. Drops compaction as a no-op (background compaction
// will rerun it on the merged tablet) and asynchronously deletes the output
// files that the compaction had already written under the source tablet's path.
// Other op shapes (op_write only, or empty log) pass through unchanged.
Status convert_txn_log_for_merging(TxnLogPB* txn_log) {
    if (!txn_log->has_op_compaction() && !txn_log->has_op_parallel_compaction()) {
        return Status::OK();
    }
    delete_files_async(tablet_reshard_helper::collect_compaction_output_file_paths(
            *txn_log, ExecEnv::GetInstance()->lake_tablet_manager()));
    txn_log->clear_op_compaction();
    txn_log->clear_op_parallel_compaction();
    return Status::OK();
}

// Transform |txn_log| for publish on one of the split child tablets.
//
// Compaction ops are dropped on cross-publish, mirroring the merging-side
// handling (see convert_txn_log_for_merging above). The reason is the same in
// the other direction: a compaction transaction committed on the parent before
// the split has its rows-mapper file (.lcrm) and output rowset built against
// the parent tablet's full key range. Each split child only owns a subrange,
// so when the conflict resolver runs for its op_compaction publish on the
// child it iterates fewer segment rows than the mapper's stored row_count and
// `RowsMapperIterator::status()` rejects the publish with
//   "Chunk vs rows mapper's row count mismatch. <N> vs <total>"
// (see storage/rows_mapper.cpp:155, storage/primary_key_compaction_conflict_resolver.cpp:124,175).
// Because the compaction's input rowsets are still present in every child's
// metadata (shared via set_all_data_files_shared), it is safe to drop the
// compaction here — background compaction on the child will rerun it. The
// compaction's output files were written under the parent tablet's path and
// are deleted async so they do not leak. The async cleanup is gated to a
// single child (split_index == 0) because publish runs convert_txn_log once
// per split child against the same parent txn_log; without the gate the
// identical output paths would be queued for deletion split_count times.
Status convert_txn_log_for_splitting(TxnLogPB* txn_log, const TabletMetadataPtr& base_tablet_metadata,
                                     const PublishTabletInfo& publish_tablet_info) {
    if (txn_log->has_op_compaction() || txn_log->has_op_parallel_compaction()) {
        if (publish_tablet_info.get_split_index() == 0) {
            delete_files_async(tablet_reshard_helper::collect_compaction_output_file_paths(
                    *txn_log, ExecEnv::GetInstance()->lake_tablet_manager()));
        }
        txn_log->clear_op_compaction();
        txn_log->clear_op_parallel_compaction();
    }
    tablet_reshard_helper::set_all_data_files_shared(txn_log);
    RETURN_IF_ERROR(tablet_reshard_helper::update_rowset_ranges(txn_log, base_tablet_metadata->range()));
    tablet_reshard_helper::update_txn_log_data_stats(txn_log, publish_tablet_info.get_split_count(),
                                                     publish_tablet_info.get_split_index());
    return Status::OK();
}

// Pick a single stable tablet id from |info| to serve as the BE-side publish
// slot for this reshard. We always anchor on the old side — SPLIT has one
// old tablet, MERGE picks the first of its old_tablet_ids, IDENTICAL uses its
// old_tablet_id. This keeps three properties:
//
//   1. Retry dedup: all retries of the same reshard carry the same
//      ReshardingTabletInfoPB, so they lock the same id.
//   2. BE-level fail-safe against DML: the same id is what a DML
//      publish_version with PUBLISH_NORMAL would acquire, so any DML that
//      isn't routed through cross-publish also serializes against reshard.
//   3. Deterministic convergence: a single CAS has no partial-acquire /
//      rollback window, so concurrent retries cannot convoy each other the
//      way a multi-tablet acquire-in-loop could.
int64_t reshard_serialization_id(const ReshardingTabletInfoPB& info) {
    if (info.has_splitting_tablet_info()) {
        return info.splitting_tablet_info().old_tablet_id();
    }
    if (info.has_merging_tablet_info()) {
        DCHECK(!info.merging_tablet_info().old_tablet_ids().empty());
        return info.merging_tablet_info().old_tablet_ids(0);
    }
    if (info.has_identical_tablet_info()) {
        return info.identical_tablet_info().old_tablet_id();
    }
    return 0;
}

Status acquire_publish_tablets(const ReshardingTabletInfoPB& info) {
    const int64_t id = reshard_serialization_id(info);
    if (!acquire_publish_tablet(id)) {
        return Status::ResourceBusy(
                fmt::format("The previous publish task for tablet {} has not finished. You can ignore this "
                            "error and the task will retry later.",
                            id));
    }
    return Status::OK();
}

void release_publish_tablets(const ReshardingTabletInfoPB& info) {
    release_publish_tablet(reshard_serialization_id(info));
}

} // namespace

std::ostream& operator<<(std::ostream& out, const PublishTabletInfo& tablet_info) {
    if (tablet_info.get_publish_tablet_type() == PublishTabletInfo::PUBLISH_NORMAL) {
        return out << "{tablet_id: " << tablet_info.get_tablet_id_in_metadata() << '}';
    }
    return out << "{publish_tablet_type: " << static_cast<int>(tablet_info.get_publish_tablet_type())
               << ", tablet_id_in_metadata: " << tablet_info.get_tablet_id_in_metadata()
               << ", tablet_id_in_txn_log: " << tablet_info.get_tablet_ids_in_txn_logs() << '}';
}

StatusOr<TxnLogPtr> convert_txn_log(const TxnLogPtr& txn_log, const TabletMetadataPtr& base_tablet_metadata,
                                    const PublishTabletInfo& publish_tablet_info) {
    const auto type = publish_tablet_info.get_publish_tablet_type();
    if (type == PublishTabletInfo::PUBLISH_NORMAL) {
        return txn_log;
    }

    g_tablet_reshard_cross_publish_total << 1;
    auto new_txn_log = std::make_shared<TxnLogPB>(*txn_log);

    // Each case increments its per-type metric and applies any op-level
    // transform while the log still carries the source tablet id (critical for
    // MERGING, benign for others). Final tablet_id rewrite happens uniformly.
    switch (type) {
    case PublishTabletInfo::SPLITTING_TABLET:
        g_tablet_reshard_cross_publish_splitting_total << 1;
        RETURN_IF_ERROR(convert_txn_log_for_splitting(new_txn_log.get(), base_tablet_metadata, publish_tablet_info));
        break;
    case PublishTabletInfo::MERGING_TABLET:
        g_tablet_reshard_cross_publish_merging_total << 1;
        RETURN_IF_ERROR(convert_txn_log_for_merging(new_txn_log.get()));
        break;
    case PublishTabletInfo::IDENTICAL_TABLET:
        g_tablet_reshard_cross_publish_identical_total << 1;
        break;
    default:
        return Status::InternalError(fmt::format("unknown publish tablet type: {}", static_cast<int>(type)));
    }

    new_txn_log->set_tablet_id(publish_tablet_info.get_tablet_id_in_metadata());
    return new_txn_log;
}

Status publish_resharding_tablet(TabletManager* tablet_manager, const ReshardingTabletInfoPB& resharding_tablet,
                                 int64_t base_version, int64_t new_version, const TxnInfoPB& txn_info,
                                 bool skip_write_tablet_metadata,
                                 std::unordered_map<int64_t, TabletMetadataPtr>& tablet_metadatas,
                                 std::unordered_map<int64_t, TabletRangePB>& tablet_ranges) {
    g_tablet_reshard_total << 1;
    auto reshard_start_ts = butil::gettimeofday_us();

    // Reserve the per-reshard publish slot. All retries of the same reshard
    // resolve to the same id (see reshard_serialization_id), so this
    // single-CAS acquire dedups FE's 10 ms resubmits. Mutual exclusion with
    // concurrent DML publish on the old-side tablet falls out for free: the
    // chosen id matches what a PUBLISH_NORMAL DML would acquire in
    // publish_version. Cross-tablet correctness for the rest of the reshard
    // (all N inputs + the new tablet) is provided by FE commitVersion
    // ordering and convert_txn_log routing, not by this slot.
    if (auto st = acquire_publish_tablets(resharding_tablet); !st.ok()) {
        g_tablet_reshard_failed << 1;
        return st;
    }
    DeferOp release_tablets([&] { release_publish_tablets(resharding_tablet); });

    LOG(INFO) << "Start publish resharding tablet"
              << ", resharding_tablet=" << resharding_tablet.DebugString() << ", txn_info=" << txn_info.DebugString()
              << ", base_version=" << base_version << ", new_version=" << new_version;

    auto handle_status = Status::OK();
    if (resharding_tablet.has_splitting_tablet_info()) {
        handle_status = handle_splitting_tablet(tablet_manager, resharding_tablet.splitting_tablet_info(), base_version,
                                                new_version, txn_info, tablet_metadatas, tablet_ranges);
    } else if (resharding_tablet.has_merging_tablet_info()) {
        handle_status = handle_merging_tablet(tablet_manager, resharding_tablet.merging_tablet_info(), base_version,
                                              new_version, txn_info, tablet_metadatas, tablet_ranges);
    } else if (resharding_tablet.has_identical_tablet_info()) {
        handle_status = handle_identical_tablet(tablet_manager, resharding_tablet.identical_tablet_info(), base_version,
                                                new_version, txn_info, tablet_metadatas);
    }

    if (!handle_status.ok()) {
        g_tablet_reshard_failed << 1;
        return handle_status;
    }

    for (const auto& [tablet_id, new_metadata] : tablet_metadatas) {
        if (!skip_write_tablet_metadata) {
            auto st = tablet_manager->put_tablet_metadata(new_metadata);
            if (!st.ok()) {
                g_tablet_reshard_failed << 1;
                return st;
            }
        } else {
            auto st = tablet_manager->cache_tablet_metadata(new_metadata);
            if (!st.ok()) {
                g_tablet_reshard_failed << 1;
                return st;
            }
            tablet_manager->metacache()->cache_aggregation_partition(
                    tablet_manager->tablet_metadata_root_location(tablet_id), true);
        }
    }

    g_tablet_reshard_latency << (butil::gettimeofday_us() - reshard_start_ts);

    LOG(INFO) << "Finish publish resharding tablet"
              << ", resharding_tablet=" << resharding_tablet.DebugString() << ", txn_info=" << txn_info.DebugString()
              << ", base_version=" << base_version << ", new_version=" << new_version;
    return Status::OK();
}

} // namespace starrocks::lake
