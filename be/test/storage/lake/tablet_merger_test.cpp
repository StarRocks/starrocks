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

#include "storage/lake/tablet_merger.h"

#include "exec/exec_env.h"
#include "storage/lake/tablet_reshard_test_base.h"

namespace starrocks {

// Merge tests run on the reshard fixture under their own suite name.
class LakeTabletMergerTest : public LakeTabletReshardTest {
protected:
    static int64_t merge_rejected_total() {
        const std::string value = bvar::Variable::describe_exposed("tablet_reshard_merge_rejected_total");
        return value.empty() ? -1 : std::stoll(value);
    }

    struct RepeatedLifecycleResult {
        TabletMetadataPtr metadata;
        std::vector<TabletMetadataPtr> final_sources;
        std::map<int32_t, int32_t> expected;
        std::vector<int32_t> deleted_keys;
        std::set<std::string> final_sst_orphans;
    };

    // A primary-key tablet merged again and again with a fresh neighbor. Three lifecycle sources are merged into one,
    // then four times: DML, a full compaction, and a merge with a new private source to the right of the tablet that
    // carries a delete vector, a DCG and an IDG on a sparse segment index. Every merge must give the cursor the
    // packing rule predicts, carry each source's del files and sidecars, carry or drop the source SSTs as their
    // watermarks and owners decide, and keep the rows and the index exact through a cold reopen. With
    // |fall_back_last_merge| the last fresh source also lists a tombstone SST whose watermark lies past its RSSIDs,
    // so the last merge drops every source SST.
    StatusOr<RepeatedLifecycleResult> run_repeated_four_cycle_lifecycle(bool enable_tde, bool fall_back_last_merge) {
        const bool old_tde = config::enable_transparent_data_encryption;
        const int32_t old_min_segments = config::lake_pk_compaction_min_input_segments;
        config::enable_transparent_data_encryption = enable_tde;
        config::lake_pk_compaction_min_input_segments = 1;
        DeferOp restore_config([&] {
            config::enable_transparent_data_encryption = old_tde;
            config::lake_pk_compaction_min_input_segments = old_min_segments;
        });
        if (enable_tde) ensure_kek_in_key_cache();
        DeferOp wait_flush_pool(
                [] { ExecEnv::GetInstance()->lake_services().pk_index_memtable_flush_thread_pool->wait(); });
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
        DeferOp restore_failpoints(
                [&] { set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE); });

        auto expect_cold_reopen = [&](const TabletMetadataPtr& metadata, const std::map<int32_t, int32_t>& expected,
                                      const std::set<int32_t>& deleted) {
            const std::vector<int32_t> deleted_keys(deleted.begin(), deleted.end());
            expect_lifecycle_oracle(metadata, repeated_expected_rows(expected), deleted_keys);
            _primary_key_manager->unload_and_remove_primary_index(metadata->id());
            _tablet_manager->prune_metacache();
            ASSIGN_OR_ABORT(auto cold, _tablet_manager->get_tablet_metadata(metadata->id(), metadata->version()));
            expect_lifecycle_oracle(cold, repeated_expected_rows(expected), deleted_keys);
        };

        std::map<int32_t, int32_t> expected = {{10, 100}, {110, 1100}, {210, 2100}};
        std::set<int32_t> deleted;
        std::vector<TabletMetadataPtr> initial_sources;
        for (int child_index = 0; child_index < 3; ++child_index) {
            const int32_t key = 10 + child_index * 100;
            ASSIGN_OR_RETURN(auto source, create_lifecycle_source(next_id(), child_index * 100, (child_index + 1) * 100,
                                                                  key, expected.at(key), /*include_delete=*/false));
            initial_sources.emplace_back(std::move(source));
        }
        const int64_t initial_target = next_id();
        prepare_tablet_dirs(initial_target);
        ASSIGN_OR_RETURN(const uint32_t initial_cursor, repeated_merge_cursor_oracle(initial_sources));
        const bool initial_falls_back = merge_drops_source_sstables(initial_sources);
        std::unordered_map<int64_t, TabletMetadataPtr> initial_published;
        RETURN_IF_ERROR(publish_resharding_merge(initial_sources, initial_target, initial_sources.front()->version(),
                                                 initial_sources.front()->version() + 1, next_id(), initial_published));
        auto current = initial_published.at(initial_target);
        expect_repeated_merge_pb(current, initial_sources, initial_cursor, initial_falls_back, enable_tde);
        expect_cold_reopen(current, expected, deleted);

        std::vector<TabletMetadataPtr> final_sources;
        std::set<std::string> final_sst_orphans;
        int carried_merges = 0;
        for (int cycle = 0; cycle < 4; ++cycle) {
            SCOPED_TRACE(fmt::format("repeated lifecycle cycle {}", cycle));
            const uint32_t prior_cursor = current->next_rowset_id();
            const int32_t upsert_key = 40 + cycle;
            const int32_t upsert_value = 4000 + cycle;
            const int32_t delete_key = cycle < 3 ? 10 + cycle * 100 : 40;
            _primary_key_manager->unload_and_remove_primary_index(current->id());
            ASSIGN_OR_RETURN(current, publish_followup_upsert_delete(current->id(), current->version(), upsert_key,
                                                                     upsert_value, delete_key));
            expected[upsert_key] = upsert_value;
            expected.erase(delete_key);
            deleted.insert(delete_key);
            ASSIGN_OR_RETURN(current, compact_tablet(current->id(), current->version(), /*force_base=*/true));

            // A fresh private source to the right of everything merged so far, at the tablet's version. Its rowset is
            // moved to a sparse segment index, so any index it has flushed is dropped and rebuilt from its rowsets.
            const int32_t lower = 300 + cycle * 100;
            const int32_t key = lower + 20;
            const int32_t value = (cycle + 1) * 10000 + key;
            ASSIGN_OR_RETURN(auto fresh, create_lifecycle_source(next_id(), lower, lower + 100, key, value,
                                                                 /*include_delete=*/false));
            expected[key] = value;
            auto mutable_fresh = std::make_shared<TabletMetadataPB>(*fresh);
            mutable_fresh->set_version(current->version());
            mutable_fresh->clear_sstable_meta();
            add_repeated_sparse_sidecars(mutable_fresh.get(), cycle, /*child_index=*/0, value, enable_tde);
            const bool force_fallback = fall_back_last_merge && cycle == 3;
            if (force_fallback) {
                // Persist the tablet's index first, so the SSTs the last merge drops include the tablet's own.
                ASSIGN_OR_RETURN(current, _primary_key_manager->flush_pk_memtable(current, current->version()));
                RETURN_IF_ERROR(put_tablet_metadata(current));
                if (current->sstable_meta().sstables_size() == 0) {
                    return Status::InternalError("repeated lifecycle flush emitted no SST");
                }
                append_tombstone_sstable(mutable_fresh.get(), lower + 50,
                                         fmt::format("repeated_tombstone_{}.sst", mutable_fresh->id()),
                                         /*shared=*/false);
            }
            RETURN_IF_ERROR(put_tablet_metadata(mutable_fresh));
            _primary_key_manager->unload_and_remove_primary_index(current->id());
            _primary_key_manager->unload_and_remove_primary_index(mutable_fresh->id());

            const std::vector<TabletMetadataPtr> merge_sources = {current, mutable_fresh};
            ASSIGN_OR_RETURN(const uint32_t expected_cursor, repeated_merge_cursor_oracle(merge_sources));
            const bool falls_back = merge_drops_source_sstables(merge_sources);
            if (force_fallback) EXPECT_TRUE(falls_back);
            const int64_t target_id = next_id();
            prepare_tablet_dirs(target_id);
            std::unordered_map<int64_t, TabletMetadataPtr> published;
            RETURN_IF_ERROR(publish_resharding_merge(merge_sources, target_id, current->version(),
                                                     current->version() + 1, next_id(), published));
            current = published.at(target_id);
            EXPECT_LT(current->next_rowset_id(), uint64_t{prior_cursor} + 4096) << "bounded RSSID growth";
            expect_repeated_merge_pb(current, merge_sources, expected_cursor, falls_back, enable_tde);
            carried_merges += falls_back ? 0 : 1;
            expect_cold_reopen(current, expected, deleted);
            if (cycle == 3) {
                for (const auto& source : merge_sources) {
                    final_sources.emplace_back(published.at(source->id()));
                    if (!falls_back) continue;
                    for (const auto& sst : source->sstable_meta().sstables()) final_sst_orphans.insert(sst.filename());
                }
            }
        }
        LOG(INFO) << "repeated lifecycle: " << carried_merges << " of 4 merges after a compaction carried the SSTs";
        EXPECT_GT(carried_merges, 0) << "at least one merge must carry the sources' SSTs";

        RepeatedLifecycleResult result;
        result.metadata = std::move(current);
        result.final_sources = std::move(final_sources);
        result.expected = std::move(expected);
        result.deleted_keys.assign(deleted.begin(), deleted.end());
        result.final_sst_orphans = std::move(final_sst_orphans);
        return result;
    }
};

TEST_F(LakeTabletMergerTest, test_tablet_merging_interval_projection_repeated_four_cycle_lifecycle) {
    ASSIGN_OR_ABORT(auto result,
                    run_repeated_four_cycle_lifecycle(/*enable_tde=*/false, /*fall_back_last_merge=*/false));
    EXPECT_EQ(result.final_sst_orphans.size(), static_cast<size_t>(result.metadata->orphan_files_size()));
    for (const auto& orphan : result.metadata->orphan_files()) EXPECT_TRUE(orphan.encryption_meta().empty());
}

// The TDE lifecycle with a last merge that drops the source SSTs: each one becomes an encrypted shared orphan of the
// merged tablet, which a vacuum that does not cover the sources keeps and a covering vacuum reclaims.
TEST_F(LakeTabletMergerTest, test_tablet_merging_interval_projection_repeated_tde_sidecar_vacuum_lifecycle) {
    ASSIGN_OR_ABORT(auto result, run_repeated_four_cycle_lifecycle(/*enable_tde=*/true, /*fall_back_last_merge=*/true));
    ASSERT_GE(result.final_sst_orphans.size(), 2);

    const bool old_tde = config::enable_transparent_data_encryption;
    config::enable_transparent_data_encryption = true;
    DeferOp restore_config([&] { config::enable_transparent_data_encryption = old_tde; });
    ensure_kek_in_key_cache();
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    DeferOp restore_flush([&] { set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE); });

    _primary_key_manager->unload_and_remove_primary_index(result.metadata->id());
    ASSIGN_OR_ABORT(auto reopened,
                    _tablet_manager->get_tablet_metadata(result.metadata->id(), result.metadata->version()));
    expect_lifecycle_oracle(reopened, repeated_expected_rows(result.expected), result.deleted_keys);

    constexpr int32_t kTailUpsertKey = 77;
    constexpr int32_t kTailDeleteKey = 120;
    ASSIGN_OR_ABORT(auto after_dml, publish_followup_upsert_delete(reopened->id(), reopened->version(), kTailUpsertKey,
                                                                   /*upsert_value=*/7700, kTailDeleteKey));
    result.expected[kTailUpsertKey] = 7700;
    result.expected.erase(kTailDeleteKey);
    result.deleted_keys.push_back(kTailDeleteKey);
    expect_lifecycle_oracle(after_dml, repeated_expected_rows(result.expected), result.deleted_keys);

    _primary_key_manager->unload_and_remove_primary_index(after_dml->id());
    ASSIGN_OR_ABORT(auto reopened_after_dml,
                    _tablet_manager->get_tablet_metadata(after_dml->id(), after_dml->version()));
    expect_lifecycle_oracle(reopened_after_dml, repeated_expected_rows(result.expected), result.deleted_keys);
    ASSIGN_OR_ABORT(auto compacted,
                    compact_tablet(reopened_after_dml->id(), reopened_after_dml->version(), /*force_base=*/true));
    EXPECT_EQ(reopened_after_dml->version() + 1, compacted->version());
    for (const auto& rowset : compacted->rowsets()) {
        for (const auto& segment : rowset.segment_metas()) EXPECT_FALSE(segment.encryption_meta().empty());
    }
    expect_lifecycle_oracle(compacted, repeated_expected_rows(result.expected), result.deleted_keys);

    _primary_key_manager->unload_and_remove_primary_index(compacted->id());
    ASSIGN_OR_ABORT(auto reopened_compacted,
                    _tablet_manager->get_tablet_metadata(compacted->id(), compacted->version()));
    expect_lifecycle_oracle(reopened_compacted, repeated_expected_rows(result.expected), result.deleted_keys);

    std::map<std::string, FileMetaPB> orphan_declarations;
    int64_t orphan_bytes = 0;
    for (const auto& orphan : result.metadata->orphan_files()) {
        if (!result.final_sst_orphans.contains(orphan.name())) continue;
        orphan_declarations.emplace(orphan.name(), orphan);
        orphan_bytes += orphan.size();
        EXPECT_FALSE(orphan.encryption_meta().empty());
        ASSERT_OK(FileSystem::Default()->path_exists(
                _tablet_manager->sst_location(result.metadata->id(), orphan.name())));
    }
    ASSERT_EQ(result.final_sst_orphans.size(), orphan_declarations.size());

    auto run_vacuum = [&](const std::vector<std::pair<int64_t, int64_t>>& tablet_versions, int64_t min_retain_version) {
        VacuumRequest request;
        VacuumResponse response;
        for (const auto& [tablet_id, min_version] : tablet_versions) {
            auto* info = request.add_tablet_infos();
            info->set_tablet_id(tablet_id);
            info->set_min_version(min_version);
        }
        request.set_min_retain_version(min_retain_version);
        request.set_grace_timestamp(::time(nullptr) + 3600);
        request.set_min_active_txn_id(std::numeric_limits<int64_t>::max());
        request.set_enable_file_bundling(false);
        request.set_enable_shared_file_cleanup(true);
        request.set_delete_txn_log(false);
        lake::vacuum(_tablet_manager.get(), request, &response);
        EXPECT_TRUE(response.has_status());
        EXPECT_EQ(0, response.status().status_code())
                << (response.status().error_msgs_size() > 0 ? response.status().error_msgs(0) : "");
        return response;
    };

    std::vector<TabletMetadataPB> live_sources;
    std::vector<std::pair<int64_t, int64_t>> live_versions = {{compacted->id(), compacted->version()}};
    for (const auto& source : result.final_sources) {
        TabletMetadataPB live(*source);
        live.set_version(compacted->version());
        live.set_prev_garbage_version(source->version());
        live.clear_orphan_files();
        ASSERT_GT(live.sstable_meta().sstables_size(), 0);
        ASSERT_OK(put_tablet_metadata(live));
        live_versions.emplace_back(source->id(), compacted->version());
        live_sources.emplace_back(std::move(live));
    }
    _tablet_manager->prune_metacache();
    auto noncovering = run_vacuum(live_versions, compacted->version());
    EXPECT_TRUE(noncovering.has_status());
    for (const auto& filename : result.final_sst_orphans) {
        const auto status =
                FileSystem::Default()->path_exists(_tablet_manager->sst_location(compacted->id(), filename));
        EXPECT_OK(status);
    }

    const int64_t retirement_version = compacted->version() + 1;
    TabletMetadataPB retired_target(*compacted);
    retired_target.set_version(retirement_version);
    retired_target.set_prev_garbage_version(compacted->version());
    retired_target.clear_orphan_files();
    ASSERT_OK(put_tablet_metadata(retired_target));
    for (const auto& live : live_sources) {
        TabletMetadataPB retired(live);
        retired.set_version(retirement_version);
        retired.set_prev_garbage_version(compacted->version());
        retired.clear_sstable_meta();
        retired.clear_orphan_files();
        for (const auto& sstable : live.sstable_meta().sstables()) {
            auto declaration = orphan_declarations.find(sstable.filename());
            ASSERT_NE(orphan_declarations.end(), declaration);
            retired.add_orphan_files()->CopyFrom(declaration->second);
        }
        ASSERT_OK(put_tablet_metadata(retired));
    }
    _tablet_manager->prune_metacache();

    std::vector<std::pair<int64_t, int64_t>> retired_versions = {{compacted->id(), retirement_version}};
    for (const auto& source : live_sources) retired_versions.emplace_back(source.id(), retirement_version);
    auto covering = run_vacuum(retired_versions, retirement_version);
    EXPECT_GE(covering.vacuumed_file_size(), orphan_bytes);
    for (const auto& filename : result.final_sst_orphans) {
        EXPECT_TRUE(FileSystem::Default()
                            ->path_exists(_tablet_manager->sst_location(compacted->id(), filename))
                            .is_not_found())
                << "covering vacuum must reclaim a retired shared SST";
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_metadata_only_private_complete_reuse) {
    int sstable_open_count = 0;
    SyncPoint::GetInstance()->SetCallBack("PersistentIndexSstable::init:table_open_error",
                                          [&](void*) { ++sstable_open_count; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp clear_open_counter([&] {
        SyncPoint::GetInstance()->ClearCallBack("PersistentIndexSstable::init:table_open_error");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    ASSIGN_OR_ABORT(auto result,
                    publish_metadata_only_merge_fixture(MetadataOnlyMergeShape::kPrivate, /*enable_tde=*/false,
                                                        /*with_del_file=*/true, /*skip_source_flush=*/false));
    const auto& target = result.published.at(result.target_tablet_id);
    ASSERT_EQ(result.source_sst_filenames.size(), static_cast<size_t>(target.sstable_meta().sstables_size()));

    std::set<std::string> output_filenames;
    std::set<std::string> fileset_ids;
    int64_t previous_max = std::numeric_limits<int64_t>::min();
    const PersistentIndexSstablePB* modern = nullptr;
    const PersistentIndexSstablePB* legacy = nullptr;
    for (const auto& sstable : target.sstable_meta().sstables()) {
        output_filenames.insert(sstable.filename());
        EXPECT_FALSE(sstable.shared());
        ASSERT_TRUE(sstable.has_fileset_id());
        fileset_ids.insert(sstable.fileset_id().SerializeAsString());
        EXPECT_LE(previous_max, static_cast<int64_t>(sstable.max_rss_rowid()));
        previous_max = static_cast<int64_t>(sstable.max_rss_rowid());
        if (sstable.has_shared_rssid()) {
            modern = &sstable;
        } else {
            legacy = &sstable;
        }
    }
    EXPECT_EQ(result.source_sst_filenames, output_filenames);
    EXPECT_EQ(output_filenames.size(), fileset_ids.size()) << "metadata reuse gives every SST a singleton fileset";
    EXPECT_EQ(result.source_sst_filenames.size(), static_cast<size_t>(sstable_open_count))
            << "the mandatory source flush is the only phase allowed to open an SST";
    for (const auto& [tablet_id, source] : result.published) {
        if (tablet_id == result.target_tablet_id) continue;
        for (const auto& source_sst : source.sstable_meta().sstables()) {
            auto output =
                    std::find_if(target.sstable_meta().sstables().begin(), target.sstable_meta().sstables().end(),
                                 [&](const auto& candidate) { return candidate.filename() == source_sst.filename(); });
            ASSERT_NE(target.sstable_meta().sstables().end(), output);
            EXPECT_NE(source_sst.fileset_id().SerializeAsString(), output->fileset_id().SerializeAsString());
        }
    }
    ASSERT_NE(nullptr, modern);
    EXPECT_EQ(1, modern->shared_rssid());
    EXPECT_EQ(0, modern->rssid_offset());
    EXPECT_EQ(static_cast<uint64_t>(1) << 32, modern->max_rss_rowid());
    ASSERT_TRUE(modern->has_delvec());
    ASSERT_TRUE(target.delvec_meta().delvecs().contains(1));
    EXPECT_EQ(target.delvec_meta().delvecs().at(1).SerializeAsString(), modern->delvec().SerializeAsString());
    ASSERT_NE(nullptr, legacy);
    EXPECT_EQ(2, legacy->rssid_offset());
    EXPECT_EQ(static_cast<uint64_t>(2) << 32, legacy->max_rss_rowid());

    ASSIGN_OR_ABORT(auto inventory, sst_inventory(result.target_tablet_id));
    EXPECT_EQ(result.source_sst_filenames, inventory) << "post-flush classification must not write a target SST";

    auto target_ptr = std::make_shared<TabletMetadataPB>(target);
    auto index = std::make_unique<lake::LakePersistentIndex>(_tablet_manager.get(), result.target_tablet_id);
    ASSERT_OK(index->init(target_ptr));
    const std::string key_a = encode_int_primary_key(10);
    const std::string key_b = encode_int_primary_key(60);
    Slice keys[] = {Slice(key_a), Slice(key_b)};
    IndexValue values[2];
    ASSERT_OK(index->get(2, keys, values));
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(1) << 32), values[0]);
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(2) << 32), values[1]);
}

// Two siblings of one split that still share their segment and their primary-key index SST. A merge takes every
// file of its sources over by reference, so it refuses them, before the source index flush would open the SST.
TEST_F(LakeTabletMergerTest, test_tablet_merging_metadata_only_identical_rejected_before_flush) {
    int sstable_open_count = 0;
    SyncPoint::GetInstance()->SetCallBack("PersistentIndexSstable::init:table_open_error",
                                          [&](void*) { ++sstable_open_count; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp clear_open_counter([&] {
        SyncPoint::GetInstance()->ClearCallBack("PersistentIndexSstable::init:table_open_error");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    auto status = expect_metadata_only_merge_rejected_without_writes(MetadataOnlyMergeShape::kIdentical);
    EXPECT_TRUE(status.message().contains("still holds shared data files")) << status;
    EXPECT_EQ(0, sstable_open_count);
}

// What the merge does with a primary-key index SST it cannot carry over unchanged. An SST that is wrong metadata --
// shared, listed twice, without a key range or with one outside its source, beside a segment two sources declare, or
// with a watermark outside the signed domain -- is refused before anything is written. An SST that is well formed but
// cannot be translated -- its embedded delete vector names a version with no file, its watermark lies outside its
// source's RSSIDs, or its translated offset does not fit int32 -- makes the merged tablet drop every source SST and
// rebuild its index, each source SST recorded once as a shared orphan.
TEST_F(LakeTabletMergerTest, test_tablet_merging_metadata_only_private_sst_rejects_or_falls_back) {
    struct Case {
        const char* name;
        const char* diagnostic;
        std::function<void(std::vector<std::shared_ptr<TabletMetadataPB>>&)> mutate;
    };
    const std::vector<Case> rejected = {
            {"one shared PB", "still holds shared data files",
             [](auto& sources) { sources[0]->mutable_sstable_meta()->mutable_sstables(0)->set_shared(true); }},
            {"duplicate filename", "twice",
             [](auto& sources) {
                 auto* duplicate = sources[1]->mutable_sstable_meta()->add_sstables();
                 duplicate->CopyFrom(sources[1]->sstable_meta().sstables(0));
             }},
            {"absent SST range", "declares no key range",
             [](auto& sources) { sources[0]->mutable_sstable_meta()->mutable_sstables(0)->clear_range(); }},
            {"range outside source tablet", "outside the tablet range",
             [](auto& sources) {
                 auto* range = sources[0]->mutable_sstable_meta()->mutable_sstables(0)->mutable_range();
                 range->set_start_key("\xff\xff");
                 range->set_end_key("\xff\xff\xff");
             }},
            {"segment declared by both sources", "both declare segment file",
             [](auto& sources) {
                 const auto shared_uid = UniqueId::gen_uid().to_proto();
                 sources[0]->mutable_rowsets(0)->mutable_uid()->CopyFrom(shared_uid);
                 sources[1]->mutable_rowsets(0)->mutable_uid()->CopyFrom(shared_uid);
                 sources[1]->mutable_rowsets(0)->mutable_segment_metas(0)->CopyFrom(
                         sources[0]->rowsets(0).segment_metas(0));
             }},
    };
    for (const auto& test_case : rejected) {
        SCOPED_TRACE(test_case.name);
        auto status =
                expect_metadata_only_merge_rejected_without_writes(MetadataOnlyMergeShape::kPrivate, test_case.mutate);
        EXPECT_TRUE(status.message().contains(test_case.diagnostic)) << status;
    }
    {
        SCOPED_TRACE("signed ordering domain");
        int64_t target_id = 0;
        int64_t target_version = 0;
        auto result_or = publish_metadata_only_merge_fixture(
                MetadataOnlyMergeShape::kPrivate, /*enable_tde=*/false, /*with_del_file=*/false,
                /*skip_source_flush=*/true,
                [](auto& sources) {
                    sources[0]->mutable_sstable_meta()->mutable_sstables(0)->set_max_rss_rowid(static_cast<uint64_t>(1)
                                                                                               << 63);
                },
                &target_id, &target_version);
        EXPECT_TRUE(result_or.status().is_invalid_argument()) << result_or.status();
        EXPECT_TRUE(result_or.status().message().starts_with("MERGE_REJECTED: ")) << result_or.status();
        expect_target_version_not_published(target_id, target_version);
    }

    const std::vector<Case> fall_back = {
            {"unresolved embedded delvec", "",
             [](auto& sources) {
                 auto* delvec = sources[0]->mutable_sstable_meta()->mutable_sstables(0)->mutable_delvec();
                 delvec->set_version(99);
                 delvec->set_size(1);
             }},
            {"watermark below the source", "",
             [](auto& sources) {
                 auto* sstable = sources[1]->mutable_sstable_meta()->mutable_sstables(0);
                 sstable->set_rssid_offset(-100);
                 sstable->set_max_rss_rowid(0);
             }},
            // The second source's rowset 5 is packed to 2, so the offset moves by -3 and leaves int32.
            {"translated offset outside int32", "",
             [](auto& sources) {
                 sources[1]->mutable_sstable_meta()->mutable_sstables(0)->set_rssid_offset(
                         std::numeric_limits<int32_t>::min() + 1);
             }},
    };
    for (const auto& test_case : fall_back) {
        SCOPED_TRACE(test_case.name);
        auto result_or = publish_metadata_only_merge_fixture(MetadataOnlyMergeShape::kPrivate, /*enable_tde=*/false,
                                                             /*with_del_file=*/false, /*skip_source_flush=*/true,
                                                             test_case.mutate);
        if (!result_or.ok()) {
            ADD_FAILURE() << result_or.status();
            continue;
        }
        auto result = std::move(result_or).value();
        const auto& target = result.published.at(result.target_tablet_id);
        EXPECT_EQ(0, target.sstable_meta().sstables_size());
        std::map<std::string, int> orphan_counts;
        for (const auto& orphan : target.orphan_files()) {
            if (result.source_sst_filenames.contains(orphan.name())) {
                ++orphan_counts[orphan.name()];
                EXPECT_TRUE(orphan.shared());
            }
        }
        for (const auto& filename : result.source_sst_filenames) {
            EXPECT_EQ(1, orphan_counts[filename]) << filename;
        }
    }

    for (bool encryption_conflict : {false, true}) {
        SCOPED_TRACE(encryption_conflict ? "conflicting encryption" : "conflicting filesize");
        if (encryption_conflict) ensure_kek_in_key_cache();
        const size_t key_cache_size_before = KeyCache::instance().size();
        const bool tde_before = config::enable_transparent_data_encryption;
        auto conflict = [=](std::vector<std::shared_ptr<TabletMetadataPB>>& sources) {
            auto* right = sources[1]->mutable_sstable_meta()->mutable_sstables(0);
            right->set_filename(sources[0]->sstable_meta().sstables(0).filename());
            if (encryption_conflict) {
                right->set_encryption_meta("private conflicting encryption metadata");
            } else {
                right->set_filesize(right->filesize() + 1);
            }
        };
        int64_t target_id = 0;
        int64_t target_version = 0;
        auto result_or = publish_metadata_only_merge_fixture(
                MetadataOnlyMergeShape::kPrivate, /*enable_tde=*/encryption_conflict,
                /*with_del_file=*/false, /*skip_source_flush=*/true, conflict, &target_id, &target_version);
        EXPECT_TRUE(result_or.status().is_corruption()) << result_or.status();
        EXPECT_TRUE(result_or.status().message().starts_with("MERGE_REJECTED: ")) << result_or.status();
        expect_target_version_not_published(target_id, target_version);
        EXPECT_EQ(tde_before, config::enable_transparent_data_encryption);
        EXPECT_EQ(key_cache_size_before, KeyCache::instance().size());
    }
}

// The second source declares a DCG on RSSID 0, where it has no segment, and the first source's rowset sits at the
// top of the RSSID domain. The merge packs both sources down, so the stale DCG is the one thing wrong with the input:
// it is refused before any delete vector, DCG or source index is written, and the target index is never loaded.
TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_stale_dcg_before_any_write) {
    constexpr uint32_t kFirstRowset = std::numeric_limits<int32_t>::max() - 1;
    constexpr uint32_t kFirstNextRowset = std::numeric_limits<int32_t>::max();
    constexpr uint32_t kSecondRowset = 1;
    auto stale_dcg = [=](std::vector<std::shared_ptr<TabletMetadataPB>>& sources) {
        auto* first = sources[0].get();
        auto* second = sources[1].get();
        first->mutable_rowsets(0)->set_id(kFirstRowset);
        first->set_next_rowset_id(kFirstNextRowset);
        first->mutable_sstable_meta()->mutable_sstables(0)->set_max_rss_rowid(static_cast<uint64_t>(kFirstRowset)
                                                                              << 32);
        second->mutable_rowsets(0)->set_id(kSecondRowset);
        second->set_next_rowset_id(kSecondRowset + 1);
        second->mutable_sstable_meta()->mutable_sstables(0)->set_rssid_offset(kSecondRowset);
        second->mutable_sstable_meta()->mutable_sstables(0)->set_max_rss_rowid(static_cast<uint64_t>(kSecondRowset)
                                                                               << 32);

        const std::string cols_name = "target_domain_overflow.cols";
        write_c1_only_cols_file(first->id(), cols_name, /*num_rows=*/1, [](int) { return 100; });
        auto& dcg = (*first->mutable_dcg_meta()->mutable_dcgs())[kFirstRowset];
        dcg.add_column_files(cols_name);
        dcg.add_unique_column_ids()->add_column_ids(first->schema().column(1).unique_id());
        dcg.add_versions(first->version());
        dcg.add_shared_files(false);

        // The second source has no rowset 0, so this DCG has no segment to apply to.
        const std::string stale_cols_name = "target_domain_overflow_conflict.cols";
        write_c1_only_cols_file(second->id(), stale_cols_name, /*num_rows=*/1, [](int) { return 600; });
        auto& stale = (*second->mutable_dcg_meta()->mutable_dcgs())[0];
        stale.add_column_files(stale_cols_name);
        stale.add_unique_column_ids()->add_column_ids(second->schema().column(1).unique_id());
        stale.add_versions(second->version());
        stale.add_shared_files(false);
    };

    int64_t target_id = 0;
    auto status = expect_metadata_only_merge_rejected_without_writes(MetadataOnlyMergeShape::kPrivate, stale_dcg,
                                                                     /*with_del_file=*/true, &target_id);
    EXPECT_TRUE(status.message().contains("stale DCG RSSID 0")) << status;
    auto* target_cache_entry = _primary_key_manager->index_cache().get(target_id);
    EXPECT_EQ(nullptr, target_cache_entry);
    if (target_cache_entry != nullptr) _primary_key_manager->index_cache().release(target_cache_entry);
}

// When the merged tablet rebuilds its index, every source SST becomes a shared orphan of it, with the source's size
// and encryption and a version vacuum can trust: the SST's generation if it was written at or before the merge, 0
// (unknown) otherwise. Vacuum keeps the file while a source or a sibling still references it, reclaims it once they
// all have retired it, and never deletes it twice.
TEST_F(LakeTabletMergerTest, test_tablet_merging_metadata_only_fallback_folds_source_sst_orphans) {
    enum class Generation { kUnknown, kNegative, kFuture, kWrittenBeforeMerge };
    for (auto generation :
         {Generation::kUnknown, Generation::kNegative, Generation::kFuture, Generation::kWrittenBeforeMerge}) {
        SCOPED_TRACE(static_cast<int>(generation));
        auto mutate = [&](std::vector<std::shared_ptr<TabletMetadataPB>>& sources) {
            const int64_t base_version = sources[0]->version();
            int64_t value = 0;
            switch (generation) {
            case Generation::kUnknown:
                value = 0;
                break;
            case Generation::kNegative:
                value = -1;
                break;
            case Generation::kFuture:
                value = base_version + 100;
                break;
            case Generation::kWrittenBeforeMerge:
                value = base_version;
                break;
            }
            for (auto& source : sources)
                source->mutable_sstable_meta()->mutable_sstables(0)->set_generation_version(value);
            // A watermark past the first source's only rowset cannot be translated, so the index is rebuilt.
            sources[0]->mutable_sstable_meta()->mutable_sstables(0)->set_max_rss_rowid(static_cast<uint64_t>(2) << 32);
        };
        auto result_or = publish_metadata_only_merge_fixture(MetadataOnlyMergeShape::kPrivate,
                                                             /*enable_tde=*/true, /*with_del_file=*/false,
                                                             /*skip_source_flush=*/true, mutate);
        if (!result_or.ok()) {
            ADD_FAILURE() << result_or.status();
            continue;
        }
        auto result = std::move(result_or).value();
        const auto& target = result.published.at(result.target_tablet_id);
        const int64_t expected_version = generation == Generation::kWrittenBeforeMerge ? result.target_version - 1 : 0;
        EXPECT_EQ(0, target.sstable_meta().sstables_size());
        std::map<std::string, const FileMetaPB*> orphans;
        for (const auto& orphan : target.orphan_files()) {
            if (result.source_sst_filenames.contains(orphan.name())) {
                EXPECT_TRUE(orphans.emplace(orphan.name(), &orphan).second) << orphan.name();
            }
        }
        ASSERT_EQ(result.source_sst_filenames.size(), orphans.size());
        std::vector<int64_t> source_tablet_ids;
        for (const auto& [tablet_id, metadata] : result.published) {
            if (tablet_id == result.target_tablet_id) continue;
            source_tablet_ids.push_back(tablet_id);
            for (const auto& sstable : metadata.sstable_meta().sstables()) {
                const auto* orphan = orphans.at(sstable.filename());
                EXPECT_TRUE(orphan->shared());
                EXPECT_EQ(expected_version, orphan->version());
                EXPECT_EQ(sstable.filesize(), orphan->size());
                EXPECT_EQ(sstable.encryption_meta(), orphan->encryption_meta());
            }
        }

        if (generation != Generation::kWrittenBeforeMerge) continue;

        // The first source's SST, referenced by the first source and by a sibling copied from it, through the normal
        // shared-orphan vacuum path.
        const FileMetaPB& orphan =
                *orphans.at(result.published.at(source_tablet_ids.front()).sstable_meta().sstables(0).filename());
        const int64_t sibling_id = next_id();
        prepare_tablet_dirs(sibling_id);
        TabletMetadataPB sibling = result.published.at(source_tablet_ids.front());
        sibling.set_id(sibling_id);
        ASSERT_OK(put_tablet_metadata(sibling));
        source_tablet_ids.push_back(sibling_id);

        const std::string sst_path = _tablet_manager->sst_location(result.target_tablet_id, orphan.name());
        ASSERT_OK(FileSystem::Default()->path_exists(sst_path));
        auto run_vacuum = [&](int64_t min_retain_version) {
            VacuumRequest request;
            VacuumResponse response;
            auto add_info = [&](int64_t tablet_id) {
                auto* info = request.add_tablet_infos();
                info->set_tablet_id(tablet_id);
                info->set_min_version(result.target_version);
            };
            add_info(result.target_tablet_id);
            for (int64_t tablet_id : source_tablet_ids) add_info(tablet_id);
            request.set_min_retain_version(min_retain_version);
            request.set_grace_timestamp(::time(nullptr) + 3600);
            request.set_min_active_txn_id(std::numeric_limits<int64_t>::max());
            request.set_enable_file_bundling(false);
            request.set_enable_shared_file_cleanup(true);
            request.set_delete_txn_log(false);
            lake::vacuum(_tablet_manager.get(), request, &response);
            EXPECT_TRUE(response.has_status());
            EXPECT_EQ(0, response.status().status_code())
                    << (response.status().error_msgs_size() > 0 ? response.status().error_msgs(0) : "");
            return response;
        };
        auto references_orphan = [&](const TabletMetadataPB& metadata) {
            return std::any_of(metadata.sstable_meta().sstables().begin(), metadata.sstable_meta().sstables().end(),
                               [&](const auto& sstable) { return sstable.filename() == orphan.name(); });
        };

        const int64_t live_reference_version = result.target_version + 1;
        TabletMetadataPB live_target(target);
        live_target.set_version(live_reference_version);
        live_target.clear_sstable_meta();
        live_target.clear_orphan_files();
        live_target.set_prev_garbage_version(result.target_version);
        ASSERT_OK(put_tablet_metadata(live_target));

        std::map<int64_t, TabletMetadataPB> live_references;
        for (int64_t tablet_id : source_tablet_ids) {
            TabletMetadataPB live = tablet_id == sibling_id ? sibling : result.published.at(tablet_id);
            live.set_id(tablet_id);
            live.set_version(live_reference_version);
            live.clear_orphan_files();
            live.set_prev_garbage_version(result.target_version);
            ASSERT_GT(live.sstable_meta().sstables_size(), 0);
            ASSERT_OK(put_tablet_metadata(live));
            live_references.emplace(tablet_id, std::move(live));
        }
        _tablet_manager->prune_metacache();

        // The old target orphan is now eligible, but the newer source and
        // sibling snapshots still retain the physical SST.
        auto retained_response = run_vacuum(live_reference_version);
        EXPECT_TRUE(retained_response.has_status());
        ASSERT_OK(FileSystem::Default()->path_exists(sst_path));

        const int64_t retirement_version = live_reference_version + 1;
        TabletMetadataPB retired_target(live_target);
        retired_target.set_version(retirement_version);
        retired_target.set_prev_garbage_version(live_reference_version);
        ASSERT_OK(put_tablet_metadata(retired_target));
        std::map<int64_t, TabletMetadataPB> retired_sources;
        for (const auto& [tablet_id, live] : live_references) {
            TabletMetadataPB retired(live);
            retired.set_version(retirement_version);
            retired.clear_sstable_meta();
            retired.clear_orphan_files();
            if (references_orphan(live)) retired.add_orphan_files()->CopyFrom(orphan);
            retired.set_prev_garbage_version(live_reference_version);
            ASSERT_OK(put_tablet_metadata(retired));
            retired_sources.emplace(tablet_id, std::move(retired));
        }
        _tablet_manager->prune_metacache();

        auto reclaimed_response = run_vacuum(retirement_version);
        EXPECT_GE(reclaimed_response.vacuumed_file_size(), orphan.size());
        EXPECT_TRUE(FileSystem::Default()->path_exists(sst_path).is_not_found());

        // Advance past the retirement metadata without linking it into the
        // next garbage chain, so a subsequent normal-vacuum pass has no
        // candidate with which to attempt a second physical deletion.
        const int64_t cleared_version = retirement_version + 1;
        TabletMetadataPB cleared_target(retired_target);
        cleared_target.set_version(cleared_version);
        cleared_target.clear_prev_garbage_version();
        ASSERT_OK(put_tablet_metadata(cleared_target));
        for (const auto& [tablet_id, retired] : retired_sources) {
            TabletMetadataPB cleared(retired);
            cleared.set_version(cleared_version);
            cleared.clear_orphan_files();
            cleared.clear_prev_garbage_version();
            ASSERT_OK(put_tablet_metadata(cleared));
        }
        _tablet_manager->prune_metacache();

        auto exact_once_response = run_vacuum(cleared_version);
        EXPECT_EQ(0, exact_once_response.vacuumed_file_size());
        EXPECT_TRUE(FileSystem::Default()->path_exists(sst_path).is_not_found());
    }
}

// The second source lists its SST twice. No writer does that, so even a copy that matches is wrong metadata and is
// refused; copies that disagree on the file's size or encryption are refused as conflicting declarations.
TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_sst_listed_twice_in_one_source) {
    enum class Copy { kMatching, kOtherSize, kOtherEncryption };
    for (auto copy : {Copy::kMatching, Copy::kOtherSize, Copy::kOtherEncryption}) {
        SCOPED_TRACE(static_cast<int>(copy));
        auto list_twice = [=](std::vector<std::shared_ptr<TabletMetadataPB>>& sources) {
            auto* duplicate = sources[1]->mutable_sstable_meta()->add_sstables();
            duplicate->CopyFrom(sources[1]->sstable_meta().sstables(0));
            if (copy == Copy::kOtherSize) duplicate->set_filesize(duplicate->filesize() + 1);
            if (copy == Copy::kOtherEncryption) duplicate->set_encryption_meta("conflicting encryption metadata");
        };
        auto status = expect_metadata_only_merge_rejected_without_writes(MetadataOnlyMergeShape::kPrivate, list_twice);
        EXPECT_TRUE(status.message().contains("metadata_only_private_b.sst")) << status;
        EXPECT_TRUE(status.message().contains(copy == Copy::kMatching ? "twice" : "conflicting physical metadata"))
                << status;
    }
}

// Malformed source ranges -- a gap, an overlap, a bound of the wrong arity or type, a wrong inclusivity flag -- are
// refused before anything is written. Well-formed ranges merge whatever order the request lists the sources in: the
// merge packs the sources in tablet id order, so a request in reversed order carries the SSTs over with the same
// translated values as one in range order, and the merged tablet serves DML and a reopen. Unbounded outer edges merge
// too.
TEST_F(LakeTabletMergerTest, test_tablet_merging_metadata_only_source_range_partition_validation) {
    enum class Outcome { kRejected, kProjected, kProjectedFromReversedRequest };
    struct Case {
        const char* name;
        Outcome outcome;
        std::function<void(std::vector<std::shared_ptr<TabletMetadataPB>>&)> mutate;
    };
    auto set_range = [&](TabletMetadataPB* metadata, std::optional<int> lower, std::optional<int> upper) {
        metadata->clear_range();
        if (lower.has_value()) {
            metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(*lower));
            metadata->mutable_range()->set_lower_bound_included(true);
        }
        if (upper.has_value()) {
            metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(*upper));
            metadata->mutable_range()->set_upper_bound_included(false);
        }
    };
    const std::vector<Case> cases = {
            {"source gap", Outcome::kRejected,
             [&](auto& sources) {
                 set_range(sources[0].get(), 0, 40);
                 set_range(sources[1].get(), 50, 100);
             }},
            {"source overlap", Outcome::kRejected,
             [&](auto& sources) {
                 set_range(sources[0].get(), 0, 60);
                 set_range(sources[1].get(), 50, 100);
             }},
            {"well-formed reversed source order", Outcome::kProjectedFromReversedRequest,
             [&](auto& sources) {
                 // Keep the outer edges in positions 0 and N-1 while listing two empty middle partitions in reverse.
                 // The first source keeps key 10 and the last key 60.
                 set_range(sources[0].get(), 0, 20);
                 set_range(sources[1].get(), 50, 100);
                 auto middle_high = std::make_shared<TabletMetadataPB>(*sources[0]);
                 middle_high->set_id(next_id());
                 middle_high->clear_rowsets();
                 middle_high->clear_sstable_meta();
                 middle_high->set_next_rowset_id(1);
                 set_range(middle_high.get(), 35, 50);
                 prepare_tablet_dirs(middle_high->id());
                 auto middle_low = std::make_shared<TabletMetadataPB>(*middle_high);
                 middle_low->set_id(next_id());
                 set_range(middle_low.get(), 20, 35);
                 prepare_tablet_dirs(middle_low->id());
                 sources = {sources[0], middle_high, middle_low, sources[1]};
             }},
            {"well-formed fully reversed source order", Outcome::kProjectedFromReversedRequest,
             [](auto& sources) { std::swap(sources[0], sources[1]); }},
            {"wrong bound arity", Outcome::kRejected,
             [](auto& sources) {
                 auto* lower = sources[0]->mutable_range()->mutable_lower_bound();
                 lower->add_values()->CopyFrom(lower->values(0));
             }},
            {"wrong bound logical type", Outcome::kRejected,
             [](auto& sources) {
                 // Keep the malformed declarations mutually comparable so the
                 // range union cannot DCHECK before validation returns
                 // Corruption against the INT schema.
                 for (auto& source : sources) {
                     auto* range = source->mutable_range();
                     range->mutable_lower_bound()->mutable_values(0)->mutable_type()->CopyFrom(
                             TypeDescriptor(TYPE_VARCHAR).to_protobuf());
                     range->mutable_upper_bound()->mutable_values(0)->mutable_type()->CopyFrom(
                             TypeDescriptor(TYPE_VARCHAR).to_protobuf());
                 }
             }},
            {"invalid inclusivity flag", Outcome::kRejected,
             [](auto& sources) { sources[0]->mutable_range()->set_lower_bound_included(false); }},
            {"valid lower-unbounded outer edge", Outcome::kProjected,
             [&](auto& sources) {
                 set_range(sources[0].get(), std::nullopt, 50);
                 set_range(sources[1].get(), 50, 100);
             }},
            {"valid upper-unbounded outer edge", Outcome::kProjected,
             [&](auto& sources) {
                 set_range(sources[0].get(), 0, 50);
                 set_range(sources[1].get(), 50, std::nullopt);
             }},
            {"valid both-unbounded outer edges", Outcome::kProjected,
             [&](auto& sources) {
                 set_range(sources[0].get(), std::nullopt, 50);
                 set_range(sources[1].get(), 50, std::nullopt);
             }},
    };

    // The SSTs as a merge listing the sources in range order projects them, without the per-merge fileset id and the
    // versions each fixture stamps with its own base version.
    auto stable_sstables = [](const TabletMetadataPB& metadata) {
        std::vector<std::string> sstables;
        for (auto sstable : metadata.sstable_meta().sstables()) {
            sstable.clear_fileset_id();
            sstable.clear_generation_version();
            sstable.clear_shared_version();
            sstables.push_back(sstable.SerializeAsString());
        }
        return sstables;
    };
    ASSIGN_OR_ABORT(auto forward, publish_metadata_only_merge_fixture(MetadataOnlyMergeShape::kPrivate,
                                                                      /*enable_tde=*/false, /*with_del_file=*/false,
                                                                      /*skip_source_flush=*/true));
    const auto forward_sstables = stable_sstables(forward.published.at(forward.target_tablet_id));
    ASSERT_EQ(2, forward_sstables.size());

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        int64_t target_id = 0;
        int64_t target_version = 0;
        auto result_or = publish_metadata_only_merge_fixture(MetadataOnlyMergeShape::kPrivate,
                                                             /*enable_tde=*/false, /*with_del_file=*/false,
                                                             /*skip_source_flush=*/true, test_case.mutate, &target_id,
                                                             &target_version);
        if (test_case.outcome == Outcome::kRejected) {
            EXPECT_TRUE(result_or.status().is_corruption()) << result_or.status();
            EXPECT_TRUE(result_or.status().message().starts_with("MERGE_REJECTED: ")) << result_or.status();
            expect_target_version_not_published(target_id, target_version);
            continue;
        }
        ASSERT_OK(result_or);
        auto result = std::move(result_or).value();
        const auto& target = result.published.at(result.target_tablet_id);
        std::set<std::string> filenames;
        for (const auto& sstable : target.sstable_meta().sstables()) filenames.insert(sstable.filename());
        EXPECT_EQ(result.source_sst_filenames, filenames);
        if (test_case.outcome == Outcome::kProjected) continue;

        EXPECT_EQ(forward_sstables, stable_sstables(target));
        _primary_key_manager->unload_and_remove_primary_index(result.target_tablet_id);
        ASSIGN_OR_ABORT(auto after_dml,
                        publish_followup_upsert_delete(result.target_tablet_id, result.target_version,
                                                       /*upsert_key=*/50, /*upsert_value=*/5050, /*delete_key=*/60));
        expect_lifecycle_oracle(after_dml, {{10, 100}, {50, 5050}}, {60});
        _primary_key_manager->unload_and_remove_primary_index(result.target_tablet_id);
        ASSIGN_OR_ABORT(auto reopened,
                        _tablet_manager->get_tablet_metadata(result.target_tablet_id, result.target_version + 1));
        expect_lifecycle_oracle(reopened, {{10, 100}, {50, 5050}}, {60});
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_empty_ordinary_rowsets_before_io) {
    for (bool primary_key : {false, true}) {
        SCOPED_TRACE(primary_key);
        auto source = std::make_shared<TabletMetadataPB>();
        source->set_id(next_id());
        source->set_version(1);
        source->set_next_rowset_id(2);
        if (primary_key) {
            set_primary_key_schema(source.get(), 1001);
        } else {
            source->mutable_schema()->set_id(1001);
            source->mutable_schema()->set_keys_type(DUP_KEYS);
        }
        auto* rowset = source->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(0);
        rowset->set_data_size(0);
        lake::tablet_reshard_helper::set_rowset_uid(rowset);

        expect_merge_rejected_without_writes({source}, next_id(), 2);
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_invalid_segment_num_rows_rejected_before_io) {
    struct Mutation {
        const char* name;
        std::function<void(SegmentMetadataPB*)> apply;
    };
    const std::vector<Mutation> mutations = {
            {"missing", [](SegmentMetadataPB* segment) { segment->clear_num_rows(); }},
            {"negative", [](SegmentMetadataPB* segment) { segment->set_num_rows(-1); }},
    };

    for (const auto& mutation : mutations) {
        for (const bool reverse_sources : {false, true}) {
            SCOPED_TRACE(fmt::format("mutation={} reverse_sources={}", mutation.name, reverse_sources));
            auto source_a = make_preflight_sidecar_source(next_id(), fmt::format("invalid_{}_a.dat", next_id()));
            auto source_b = make_preflight_sidecar_source(next_id(), fmt::format("invalid_{}_b.dat", next_id()));
            mutation.apply(source_a->mutable_rowsets(0)->mutable_segment_metas(0));
            std::vector<TabletMetadataPtr> sources = {source_a, source_b};
            if (reverse_sources) std::reverse(sources.begin(), sources.end());

            expect_merge_rejected_without_writes(sources, next_id(), /*target_version=*/2);
        }
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_indexless_allocation_covers_pruned_delete_history) {
    constexpr uint32_t kContainingRowset = 10;
    constexpr uint32_t kOriginRowset = 1;
    constexpr uint32_t kOpOffset = 7;
    auto add_pruned_delete_history = [=](std::vector<std::shared_ptr<TabletMetadataPB>>& sources) {
        auto* rowset = sources[0]->mutable_rowsets(0);
        // Compaction transferred this del from rowset 1; its offset remains in
        // the old origin's coordinate space after all current segments vanish.
        rowset->set_id(kContainingRowset);
        sources[0]->set_next_rowset_id(kContainingRowset + 1);
        rowset->clear_segment_metas();
        rowset->set_num_rows(0);
        rowset->set_data_size(0);
        auto* del = rowset->add_del_files();
        del->set_name("metadata_only_pruned_history.del");
        del->set_origin_rowset_id(kOriginRowset);
        del->set_op_offset(kOpOffset);
        write_binary_del_file(sources[0]->id(), del->name(), {encode_int_primary_key(10)});
    };
    // The source flush records the transferred delete in the first source's index. The merged tablet needs that:
    // the first source's rowsets end below the second source's translated watermark, so a cold load of the merged
    // tablet does not replay them.
    auto result_or = publish_metadata_only_merge_fixture(MetadataOnlyMergeShape::kPrivate, /*enable_tde=*/false,
                                                         /*with_del_file=*/false, /*skip_source_flush=*/false,
                                                         add_pruned_delete_history);
    ASSERT_OK(result_or);
    auto result = std::move(result_or).value();
    const auto& target = result.published.at(result.target_tablet_id);
    const uint64_t origin_ceiling = uint64_t{kOriginRowset} + kOpOffset;
    EXPECT_GT(target.next_rowset_id(), origin_ceiling);
    EXPECT_GT(target.sstable_meta().sstables_size(), 0);

    _primary_key_manager->unload_and_remove_primary_index(result.target_tablet_id);
    ASSIGN_OR_ABORT(auto after_dml,
                    publish_followup_upsert_delete(result.target_tablet_id, result.target_version,
                                                   /*upsert_key=*/10, /*upsert_value=*/1010, /*delete_key=*/60));
    const RowsetMetadataPB* write_rowset = nullptr;
    for (const auto& rowset : after_dml->rowsets()) {
        if (rowset.version() == result.target_version + 1 && rowset.segment_metas_size() > 0) {
            write_rowset = &rowset;
        }
    }
    ASSERT_NE(nullptr, write_rowset);
    const auto& write_segment = write_rowset->segment_metas(0);
    const uint64_t allocated_rssid = uint64_t{write_rowset->id()} + write_segment.segment_idx();
    EXPECT_GT(allocated_rssid, origin_ceiling);
    ASSIGN_OR_ABORT(auto rows, read_two_column_rows(after_dml));
    EXPECT_EQ((std::vector<std::pair<int32_t, int32_t>>{{10, 1010}}), rows);
    const std::vector<std::string> keys = {encode_int_primary_key(10), encode_int_primary_key(60)};
    ASSIGN_OR_ABORT(auto values, load_index_values(after_dml, result.target_tablet_id, keys));
    ASSERT_EQ(2, values.size());
    EXPECT_EQ(allocated_rssid, values[0].get_value() >> 32);
    EXPECT_EQ(IndexValue(NullIndexValue), values[1]);
    _primary_key_manager->unload_and_remove_primary_index(result.target_tablet_id);
    ASSIGN_OR_ABORT(auto reopened,
                    _tablet_manager->get_tablet_metadata(result.target_tablet_id, result.target_version + 1));
    ASSIGN_OR_ABORT(auto reopened_rows, read_two_column_rows(reopened));
    EXPECT_EQ(rows, reopened_rows);
    ASSIGN_OR_ABORT(auto reopened_values, load_index_values(reopened, result.target_tablet_id, keys));
    EXPECT_EQ(values, reopened_values);

    constexpr uint32_t kForeignOriginRowset = 20;
    auto foreign_origin_delete_history = [&](std::vector<std::shared_ptr<TabletMetadataPB>>& sources) {
        auto* rowset = sources[0]->mutable_rowsets(0);
        rowset->clear_segment_metas();
        rowset->set_num_rows(0);
        rowset->set_data_size(0);
        auto* del = rowset->add_del_files();
        del->set_name("metadata_only_foreign_origin_history.del");
        del->set_origin_rowset_id(kForeignOriginRowset);
        del->set_op_offset(kOpOffset);
        write_binary_del_file(sources[0]->id(), del->name(), {});
        sources[0]->clear_sstable_meta();
        sources[1]->clear_rowsets();
        sources[1]->clear_sstable_meta();
        sources[1]->set_next_rowset_id(1);
    };
    ASSIGN_OR_ABORT(auto foreign_origin_control,
                    publish_metadata_only_merge_fixture(MetadataOnlyMergeShape::kPrivate, /*enable_tde=*/false,
                                                        /*with_del_file=*/false, /*skip_source_flush=*/true,
                                                        foreign_origin_delete_history));
    const auto& foreign_origin_target = foreign_origin_control.published.at(foreign_origin_control.target_tablet_id);
    ASSERT_EQ(1, foreign_origin_target.rowsets_size());
    ASSERT_EQ(1, foreign_origin_target.rowsets(0).del_files_size());
    // A primary-key source keeps its gaps: rowset 1 and the delete's replay span [20, 28) are one run [1, 28), which
    // the merge leaves where it is.
    EXPECT_EQ(20, foreign_origin_target.rowsets(0).del_files(0).origin_rowset_id());
    EXPECT_EQ(28, foreign_origin_target.next_rowset_id());

    auto zero_segment_no_del = [](std::vector<std::shared_ptr<TabletMetadataPB>>& sources) {
        sources[0]->mutable_rowsets(0)->clear_segment_metas();
        sources[0]->mutable_rowsets(0)->clear_del_files();
        sources[0]->clear_sstable_meta();
        sources[1]->clear_rowsets();
        sources[1]->clear_sstable_meta();
        sources[1]->set_next_rowset_id(1);
    };
    auto zero_control = publish_metadata_only_merge_fixture(MetadataOnlyMergeShape::kPrivate, /*enable_tde=*/false,
                                                            /*with_del_file=*/false, /*skip_source_flush=*/true,
                                                            zero_segment_no_del);
    EXPECT_TRUE(zero_control.status().is_corruption()) << zero_control.status();

    auto one_sst_boundary = [](std::vector<std::shared_ptr<TabletMetadataPB>>& sources) {
        sources[0]->clear_rowsets();
        sources[0]->clear_sstable_meta();
        sources[0]->set_next_rowset_id(1);
    };
    auto boundary_or =
            publish_metadata_only_merge_fixture(MetadataOnlyMergeShape::kPrivate, /*enable_tde=*/false,
                                                /*with_del_file=*/false, /*skip_source_flush=*/true, one_sst_boundary);
    ASSERT_OK(boundary_or);
    const auto& boundary = boundary_or->published.at(boundary_or->target_tablet_id);
    ASSERT_EQ(1, boundary.sstable_meta().sstables_size());
    EXPECT_EQ(1, boundary.sstable_meta().sstables(0).max_rss_rowid() >> 32);
    EXPECT_EQ(2, boundary.next_rowset_id());

    auto exhausted = [&](std::vector<std::shared_ptr<TabletMetadataPB>>& sources) {
        auto* rowset = sources[0]->mutable_rowsets(0);
        rowset->clear_segment_metas();
        auto* del = rowset->add_del_files();
        del->set_name("metadata_only_exhausted_history.del");
        del->set_origin_rowset_id(2);
        del->set_op_offset(std::numeric_limits<int32_t>::max());
        write_binary_del_file(sources[0]->id(), del->name(), {});
    };
    int64_t exhausted_target_id = 0;
    int64_t exhausted_target_version = 0;
    auto exhausted_or = publish_metadata_only_merge_fixture(MetadataOnlyMergeShape::kPrivate, /*enable_tde=*/false,
                                                            /*with_del_file=*/false, /*skip_source_flush=*/true,
                                                            exhausted, &exhausted_target_id, &exhausted_target_version);
    EXPECT_TRUE(exhausted_or.status().is_invalid_argument()) << exhausted_or.status();
    expect_target_version_not_published(exhausted_target_id, exhausted_target_version);
}

// The second source's primary-key index SST holds keys 0 and 1, which belong to the first source's range, under a
// legacy watermark that is live in the second source. Carried over with the second source's RSSID translation, it
// would point those keys at the wrong rows, so the merge refuses it: its declared key range lies outside its source.
TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_sst_holding_sibling_keys) {
    auto [source_a, source_b] = make_sibling_key_sst_sources(next_id(), next_id());
    auto status = expect_merge_rejected_without_writes({source_a, source_b}, next_id(), /*target_version=*/601);
    EXPECT_TRUE(status.message().contains("sibling_keys_stale.sst has a key range outside the tablet range")) << status;
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_issue11939_falls_back_then_dml_is_exact) {
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_flush_failpoints([&] {
        set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    });
    constexpr uint32_t kTombstone = std::numeric_limits<uint32_t>::max();
    const std::string tombstone_key = encode_int_primary_key(10);
    struct Case {
        const char* name;
        const char* filename;
        uint32_t source_high;
        bool force_exact_zero;
    };
    const std::vector<Case> cases = {
            {"negative below-floor projection", "issue11939_negative_watermark.sst", 47, false},
            {"exact zero watermark", "issue11939_zero_watermark.sst", 0, true},
    };
    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        auto fixture = make_below_floor_legacy_fixture(
                test_case.filename,
                {{tombstone_key, serialize_index_values({{/*version=*/23, kTombstone, kTombstone}})}},
                test_case.source_high);
        if (test_case.force_exact_zero) {
            fixture.hot_metadata->mutable_sstable_meta()->mutable_sstables(0)->set_max_rss_rowid(0);
            ASSERT_EQ(0, fixture.hot_metadata->sstable_meta().sstables(0).max_rss_rowid());
        }
        auto merged_or = merge_modern_shared_occurrences(fixture.cold_metadata, fixture.hot_metadata,
                                                         fixture.merged_tablet, BelowFloorLegacyFixture::kBaseVersion,
                                                         BelowFloorLegacyFixture::kMergedVersion, next_id());
        ASSERT_OK(merged_or);
        auto merged = std::move(merged_or).value();
        EXPECT_EQ(0, merged->sstable_meta().sstables_size());
        int orphan_count = 0;
        for (const auto& orphan : merged->orphan_files()) {
            if (orphan.name() == fixture.source_filename) {
                ++orphan_count;
                EXPECT_TRUE(orphan.shared());
            }
        }
        EXPECT_EQ(1, orphan_count);

        _primary_key_manager->unload_and_remove_primary_index(fixture.merged_tablet);
        ASSIGN_OR_ABORT(auto after_dml,
                        publish_followup_upsert_delete(fixture.merged_tablet, BelowFloorLegacyFixture::kMergedVersion,
                                                       /*upsert_key=*/10,
                                                       /*upsert_value=*/1010, /*delete_key=*/20));
        expect_lifecycle_oracle(after_dml, {{10, 1010}}, {20});

        ASSIGN_OR_ABORT(auto compacted,
                        compact_tablet(fixture.merged_tablet, after_dml->version(), /*force_base=*/true));
        EXPECT_EQ(after_dml->version() + 1, compacted->version());
        ASSERT_GT(compacted->rowsets_size(), 0);
        expect_lifecycle_oracle(compacted, {{10, 1010}}, {20});

        _primary_key_manager->unload_and_remove_primary_index(fixture.merged_tablet);
        ASSIGN_OR_ABORT(auto reopened,
                        _tablet_manager->get_tablet_metadata(fixture.merged_tablet, compacted->version()));
        expect_lifecycle_oracle(reopened, {{10, 1010}}, {20});
    }
}

// Real lifecycle sources, their index flushed, merged in eight shapes. Where every source SST can be translated the
// merged tablet carries them over; where one cannot -- a modern owner past the source's RSSIDs, or a tombstone SST
// whose watermark is the source's next rowset id -- it drops them all and rebuilds its index, handing each source
// SST off once as a shared orphan, and its first writer persists no SST. Either way the rows and the index stay
// exact through DML, deletes, reopens and a full compaction.
TEST_F(LakeTabletMergerTest, test_tablet_merging_sst_carry_or_fallback_lifecycle_matrix) {
    using lake::ConfigResetGuard;
    ConfigResetGuard<int32_t> files_threshold(&config::cloud_native_pk_index_rebuild_files_threshold, 0);
    ConfigResetGuard<int64_t> rows_threshold(&config::cloud_native_pk_index_rebuild_rows_threshold, 0);
    ConfigResetGuard<int64_t> l0_limit(&config::l0_max_mem_usage, 1LL << 30);

    enum class Shape {
        kLegacy,
        kModernOwnerOutsideSource,
        kMixedTombstoneAndLive,
        kTombstoneOnly,
        kMergedDelvec,
        kTde,
        kSourceCompactionBeforeMerge,
        kSourceDeleteAfterCompaction,
    };
    struct Case {
        const char* name;
        Shape shape;
        bool tde;
        bool falls_back;
    };
    const Case cases[] = {
            {"legacy", Shape::kLegacy, false, false},
            {"modern owner outside source", Shape::kModernOwnerOutsideSource, false, true},
            {"mixed tombstone and live", Shape::kMixedTombstoneAndLive, false, true},
            {"tombstone only", Shape::kTombstoneOnly, false, true},
            {"merged delvec", Shape::kMergedDelvec, false, false},
            {"TDE", Shape::kTde, true, false},
            {"source compaction before merge", Shape::kSourceCompactionBeforeMerge, false, false},
            {"source delete after compaction", Shape::kSourceDeleteAfterCompaction, false, false},
    };

    const bool old_tde = config::enable_transparent_data_encryption;
    const int32_t old_min_segments = config::lake_pk_compaction_min_input_segments;
    DeferOp restore_config([&] {
        config::enable_transparent_data_encryption = old_tde;
        config::lake_pk_compaction_min_input_segments = old_min_segments;
    });
    DeferOp wait_flush_pool(
            [] { ExecEnv::GetInstance()->lake_services().pk_index_memtable_flush_thread_pool->wait(); });
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    DeferOp restore_flush([&] { set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE); });
    config::lake_pk_compaction_min_input_segments = 1;

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        config::enable_transparent_data_encryption = test_case.tde;
        if (test_case.tde) ensure_kek_in_key_cache();

        const int64_t left_id = next_id();
        const int64_t right_id = next_id();
        const int64_t target_id = next_id();
        prepare_tablet_dirs(target_id);
        ASSIGN_OR_ABORT(auto left, create_lifecycle_source(left_id, 0, 50, 10, 100));
        ASSIGN_OR_ABORT(auto right, create_lifecycle_source(right_id, 50, 100, 60, 600));
        std::map<int32_t, int32_t> expected = {{10, 100}, {60, 600}};

        if (test_case.shape == Shape::kMergedDelvec) {
            ASSIGN_OR_ABORT(left, publish_followup_upsert_delete(left_id, left->version(), 11, 110, 10));
            ASSIGN_OR_ABORT(right, publish_followup_upsert_delete(right_id, right->version(), 61, 610, 60));
            expected = {{11, 110}, {61, 610}};
        }
        if (test_case.shape == Shape::kSourceCompactionBeforeMerge ||
            test_case.shape == Shape::kSourceDeleteAfterCompaction) {
            ASSIGN_OR_ABORT(left, publish_followup_upsert_delete(left_id, left->version(), 12, 120, 49));
            ASSIGN_OR_ABORT(right, publish_followup_upsert_delete(right_id, right->version(), 62, 620, 99));
            ASSIGN_OR_ABORT(left, compact_tablet(left_id, left->version(), /*force_base=*/true));
            ASSIGN_OR_ABORT(right, compact_tablet(right_id, right->version(), /*force_base=*/true));
            expected = {{10, 100}, {12, 120}, {60, 600}, {62, 620}};
            if (test_case.shape == Shape::kSourceDeleteAfterCompaction) {
                ASSIGN_OR_ABORT(left, publish_followup_upsert_delete(left_id, left->version(), 13, 130, 10));
                ASSIGN_OR_ABORT(right, publish_followup_upsert_delete(right_id, right->version(), 63, 630, 60));
                expected = {{12, 120}, {13, 130}, {62, 620}, {63, 630}};
            }
        }

        // Drain the real source writers before shaping the SSTs. The merge flushes once more itself unless it
        // rebuilds the index.
        ASSIGN_OR_ABORT(left, _primary_key_manager->flush_pk_memtable(left, left->version()));
        ASSIGN_OR_ABORT(right, _primary_key_manager->flush_pk_memtable(right, right->version()));

        auto mutable_left = std::make_shared<TabletMetadataPB>(*left);
        auto mutable_right = std::make_shared<TabletMetadataPB>(*right);
        ASSERT_GT(mutable_left->sstable_meta().sstables_size(), 0);
        ASSERT_GT(mutable_right->sstable_meta().sstables_size(), 0);
        if (test_case.shape == Shape::kMixedTombstoneAndLive) {
            append_tombstone_sstable(mutable_left.get(), 30, fmt::format("lifecycle_mixed_{}.sst", next_id()),
                                     /*shared=*/false);
        } else if (test_case.shape == Shape::kTombstoneOnly) {
            mutable_left->mutable_sstable_meta()->clear_sstables();
            mutable_right->mutable_sstable_meta()->clear_sstables();
            append_tombstone_sstable(mutable_left.get(), 10, fmt::format("lifecycle_tombstone_{}.sst", next_id()),
                                     /*shared=*/false);
            append_tombstone_sstable(mutable_right.get(), 60, fmt::format("lifecycle_tombstone_{}.sst", next_id()),
                                     /*shared=*/false);
        } else if (test_case.shape == Shape::kModernOwnerOutsideSource) {
            for (auto& sstable : *mutable_right->mutable_sstable_meta()->mutable_sstables()) {
                sstable.set_shared_rssid(mutable_right->next_rowset_id());
                sstable.set_shared_version(mutable_right->version());
            }
        }

        std::map<std::string, int> source_sst_counts;
        for (const auto* source : {mutable_left.get(), mutable_right.get()}) {
            for (const auto& sstable : source->sstable_meta().sstables()) {
                ++source_sst_counts[sstable.filename()];
            }
        }
        ASSERT_FALSE(source_sst_counts.empty());
        for (const auto& [filename, count] : source_sst_counts) {
            ASSERT_EQ(1, count) << "fixture source SST filename is not unique: " << filename;
        }
        ASSIGN_OR_ABORT(auto left_sst_inventory_before_merge, sst_inventory(left_id));
        ASSIGN_OR_ABORT(auto right_sst_inventory_before_merge, sst_inventory(right_id));

        _primary_key_manager->unload_and_remove_primary_index(left_id);
        _primary_key_manager->unload_and_remove_primary_index(right_id);
        std::unordered_map<int64_t, TabletMetadataPtr> published;
        ASSERT_OK(publish_resharding_merge({mutable_left, mutable_right}, target_id, left->version(),
                                           left->version() + 1, next_id(), published));
        const auto& merged = published.at(target_id);
        ASSIGN_OR_ABORT(auto left_sst_inventory_after_merge, sst_inventory(left_id));
        ASSIGN_OR_ABORT(auto right_sst_inventory_after_merge, sst_inventory(right_id));
        std::set<std::string> written_by_merge;
        for (const auto& [before, after] :
             {std::pair{&left_sst_inventory_before_merge, &left_sst_inventory_after_merge},
              std::pair{&right_sst_inventory_before_merge, &right_sst_inventory_after_merge}}) {
            for (const auto& filename : *after) {
                if (!before->contains(filename)) written_by_merge.insert(filename);
            }
        }

        std::map<std::string, int> sst_orphan_counts;
        for (const auto& orphan : merged->orphan_files()) {
            if (!orphan.name().ends_with(".sst")) continue;
            EXPECT_TRUE(orphan.shared());
            ++sst_orphan_counts[orphan.name()];
        }
        if (test_case.falls_back) {
            ASSERT_EQ(0, merged->sstable_meta().sstables_size());
            EXPECT_TRUE(written_by_merge.empty()) << "a merge that rebuilds the index must not flush its sources";
            EXPECT_EQ(source_sst_counts, sst_orphan_counts) << "each source SST must be handed off exactly once";
        } else {
            EXPECT_TRUE(sst_orphan_counts.empty());
            std::map<std::string, int> carried;
            for (const auto& sstable : merged->sstable_meta().sstables()) ++carried[sstable.filename()];
            for (const auto& [filename, count] : source_sst_counts) {
                EXPECT_EQ(1, carried[filename]) << "source SST must be carried exactly once: " << filename;
            }
            for (const auto& [filename, count] : carried) {
                EXPECT_EQ(1, count) << filename;
                EXPECT_TRUE(source_sst_counts.contains(filename) || written_by_merge.contains(filename)) << filename;
            }
        }

        _primary_key_manager->unload_and_remove_primary_index(target_id);
        ASSIGN_OR_ABORT(auto reopened_merge, _tablet_manager->get_tablet_metadata(target_id, merged->version()));
        ASSIGN_OR_ABORT(auto merge_rows, read_two_column_rows(reopened_merge));
        EXPECT_EQ(expected.size(), merge_rows.size());

        ASSIGN_OR_ABORT(auto after_first_upsert,
                        publish_followup_upsert_delete(target_id, merged->version(), 20, 2000, 0,
                                                       /*include_delete=*/false));
        expected[20] = 2000;
        std::vector<std::pair<int32_t, int32_t>> expected_rows(expected.begin(), expected.end());
        expect_lifecycle_oracle(after_first_upsert, expected_rows, {});
        if (test_case.falls_back) EXPECT_EQ(0, after_first_upsert->sstable_meta().sstables_size());

        _primary_key_manager->unload_and_remove_primary_index(target_id);
        ASSIGN_OR_ABORT(auto after_delete, publish_followup_delete(target_id, after_first_upsert->version(), 60));
        expected.erase(60);
        expected_rows.assign(expected.begin(), expected.end());
        expect_lifecycle_oracle(after_delete, expected_rows, {60});

        _primary_key_manager->unload_and_remove_primary_index(target_id);
        ASSIGN_OR_ABORT(auto reopened_after_delete,
                        _tablet_manager->get_tablet_metadata(target_id, after_delete->version()));
        expect_lifecycle_oracle(reopened_after_delete, expected_rows, {60});

        ASSIGN_OR_ABORT(auto after_second_upsert,
                        publish_followup_upsert_delete(target_id, reopened_after_delete->version(), 70, 7000, 0,
                                                       /*include_delete=*/false));
        expected[70] = 7000;
        expected_rows.assign(expected.begin(), expected.end());
        expect_lifecycle_oracle(after_second_upsert, expected_rows, {60});

        _primary_key_manager->unload_and_remove_primary_index(target_id);
        ASSIGN_OR_ABORT(auto reopened, _tablet_manager->get_tablet_metadata(target_id, after_second_upsert->version()));
        expect_lifecycle_oracle(reopened, expected_rows, {60});

        ASSIGN_OR_ABORT(auto compacted, compact_tablet(target_id, reopened->version(), /*force_base=*/true));
        EXPECT_EQ(reopened->version() + 1, compacted->version());
        ASSERT_GT(compacted->rowsets_size(), 0);
        expect_lifecycle_oracle(compacted, expected_rows, {60});

        _primary_key_manager->unload_and_remove_primary_index(target_id);
        ASSIGN_OR_ABORT(auto reopened_compacted, _tablet_manager->get_tablet_metadata(target_id, compacted->version()));
        expect_lifecycle_oracle(reopened_compacted, expected_rows, {60});
    }
}

// A merged tablet that rebuilds its index must leave that to the normal cache-only native reload: its first writer
// persists no recovery SST. The left source's last SST has a watermark past every RSSID the source references, which
// cannot be translated, so the merge drops the source SSTs.
TEST_F(LakeTabletMergerTest, test_tablet_merging_indexless_fallback_uses_native_reload) {
    using lake::ConfigResetGuard;
    ConfigResetGuard<int32_t> files_threshold(&config::cloud_native_pk_index_rebuild_files_threshold, 0);
    ConfigResetGuard<int64_t> rows_threshold(&config::cloud_native_pk_index_rebuild_rows_threshold, 0);
    ConfigResetGuard<int64_t> l0_limit(&config::l0_max_mem_usage, 1LL << 30);
    ConfigResetGuard<int32_t> memtable_count(&config::pk_index_memtable_max_count, 1);
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    DeferOp restore_flush([&] { set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE); });

    const int64_t left_id = next_id();
    const int64_t right_id = next_id();
    const int64_t target_id = next_id();
    prepare_tablet_dirs(target_id);
    ASSIGN_OR_ABORT(auto left, create_lifecycle_source(left_id, 0, 50, 10, 100, /*include_delete=*/false));
    ASSIGN_OR_ABORT(auto right, create_lifecycle_source(right_id, 50, 100, 60, 600, /*include_delete=*/false));
    ASSIGN_OR_ABORT(left, _primary_key_manager->flush_pk_memtable(left, left->version()));
    ASSIGN_OR_ABORT(right, _primary_key_manager->flush_pk_memtable(right, right->version()));
    auto mutable_left = std::make_shared<TabletMetadataPB>(*left);
    auto mutable_right = std::make_shared<TabletMetadataPB>(*right);
    ASSERT_GT(mutable_left->sstable_meta().sstables_size(), 0);
    auto* last =
            mutable_left->mutable_sstable_meta()->mutable_sstables(mutable_left->sstable_meta().sstables_size() - 1);
    last->set_max_rss_rowid(static_cast<uint64_t>(mutable_left->next_rowset_id()) << 32);

    std::set<std::string> omitted_source_sstables;
    for (const auto* source : {mutable_left.get(), mutable_right.get()}) {
        for (const auto& sstable : source->sstable_meta().sstables()) {
            ASSERT_TRUE(omitted_source_sstables.insert(sstable.filename()).second);
        }
    }

    _primary_key_manager->unload_and_remove_primary_index(left_id);
    _primary_key_manager->unload_and_remove_primary_index(right_id);
    std::unordered_map<int64_t, TabletMetadataPtr> published;
    ASSERT_OK(publish_resharding_merge({mutable_left, mutable_right}, target_id, left->version(), left->version() + 1,
                                       next_id(), published));
    const auto& merged = published.at(target_id);
    ASSERT_EQ(0, merged->sstable_meta().sstables_size());

    std::map<std::string, int> orphan_counts;
    for (const auto& orphan : merged->orphan_files()) {
        if (omitted_source_sstables.contains(orphan.name())) {
            EXPECT_TRUE(orphan.shared());
            ++orphan_counts[orphan.name()];
        }
    }
    for (const auto& filename : omitted_source_sstables) {
        EXPECT_EQ(1, orphan_counts[filename]) << filename;
    }

    _primary_key_manager->unload_and_remove_primary_index(target_id);
    ASSIGN_OR_ABORT(auto after_first_upsert,
                    publish_followup_upsert_delete(target_id, merged->version(), /*upsert_key=*/20,
                                                   /*upsert_value=*/2000, /*delete_key=*/0,
                                                   /*include_delete=*/false));
    expect_lifecycle_oracle(after_first_upsert, {{10, 100}, {20, 2000}, {60, 600}}, {});
    ASSERT_EQ(0, after_first_upsert->sstable_meta().sstables_size());

    _primary_key_manager->unload_and_remove_primary_index(target_id);
    ASSIGN_OR_ABORT(auto after_delete, publish_followup_delete(target_id, after_first_upsert->version(), 60));
    expect_lifecycle_oracle(after_delete, {{10, 100}, {20, 2000}}, {60});

    _primary_key_manager->unload_and_remove_primary_index(target_id);
    ASSIGN_OR_ABORT(auto reopened_after_delete,
                    _tablet_manager->get_tablet_metadata(target_id, after_delete->version()));
    expect_lifecycle_oracle(reopened_after_delete, {{10, 100}, {20, 2000}}, {60});

    ASSIGN_OR_ABORT(auto after_second_upsert,
                    publish_followup_upsert_delete(target_id, reopened_after_delete->version(), /*upsert_key=*/70,
                                                   /*upsert_value=*/7000, /*delete_key=*/0,
                                                   /*include_delete=*/false));
    expect_lifecycle_oracle(after_second_upsert, {{10, 100}, {20, 2000}, {70, 7000}}, {60});

    _primary_key_manager->unload_and_remove_primary_index(target_id);
    ASSIGN_OR_ABORT(auto final_metadata,
                    _tablet_manager->get_tablet_metadata(target_id, after_second_upsert->version()));
    expect_lifecycle_oracle(final_metadata, {{10, 100}, {20, 2000}, {70, 7000}}, {60});
}

// Two private lifecycle sources merge with their SSTs carried, and the merged tablet takes a write. A split then gives
// both children the parent's SSTs, shared, over different physical segment layouts. Merging the children back is
// refused without writing anything, and counted.
TEST_F(LakeTabletMergerTest, test_tablet_merging_split_children_with_divergent_layouts_rejected) {
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    DeferOp restore_flush([&] { set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE); });

    const int64_t left_id = next_id();
    const int64_t right_id = next_id();
    const int64_t merged_id = next_id();
    prepare_tablet_dirs(merged_id);
    ASSIGN_OR_ABORT(auto left, create_lifecycle_source(left_id, 0, 50, 10, 100, /*include_delete=*/false));
    ASSIGN_OR_ABORT(auto right, create_lifecycle_source(right_id, 50, 100, 60, 600, /*include_delete=*/false));
    ASSIGN_OR_ABORT(left, _primary_key_manager->flush_pk_memtable(left, left->version()));
    ASSIGN_OR_ABORT(right, _primary_key_manager->flush_pk_memtable(right, right->version()));
    _primary_key_manager->unload_and_remove_primary_index(left_id);
    _primary_key_manager->unload_and_remove_primary_index(right_id);
    std::unordered_map<int64_t, TabletMetadataPtr> merge_published;
    ASSERT_OK(publish_resharding_merge({left, right}, merged_id, left->version(), left->version() + 1, next_id(),
                                       merge_published));
    auto merged = merge_published.at(merged_id);
    ASSERT_GT(merged->sstable_meta().sstables_size(), 0);

    _primary_key_manager->unload_and_remove_primary_index(merged_id);
    ASSIGN_OR_ABORT(auto written, publish_followup_upsert_delete(merged_id, merged->version(), 20, 2000,
                                                                 /*delete_key=*/0, /*include_delete=*/false));
    const std::vector<std::pair<int32_t, int32_t>> expected = {{10, 100}, {20, 2000}, {60, 600}};
    expect_lifecycle_oracle(written, expected, {});

    std::set<std::string> parent_segments;
    for (const auto& rowset : written->rowsets()) {
        for (const auto& segment : rowset.segment_metas()) parent_segments.insert(segment.filename());
    }
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    ReshardingTabletInfoPB split_info;
    auto& splitting = *split_info.mutable_splitting_tablet_info();
    splitting.set_old_tablet_id(merged_id);
    splitting.add_new_tablet_ids(child_a);
    splitting.add_new_tablet_ids(child_b);
    TxnInfoPB split_txn;
    split_txn.set_txn_id(next_id());
    split_txn.set_commit_time(1);
    split_txn.set_gtid(1);
    std::unordered_map<int64_t, TabletMetadataPtr> split_published;
    std::unordered_map<int64_t, TabletRangePB> split_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), split_info, written->version(),
                                              written->version() + 1, split_txn, false, split_published, split_ranges));
    ASSERT_TRUE(split_published.contains(child_a));
    ASSERT_TRUE(split_published.contains(child_b));
    for (int64_t child_id : {child_a, child_b}) {
        const auto& child = split_published.at(child_id);
        for (const auto& rowset : child->rowsets()) {
            for (const auto& segment : rowset.segment_metas()) {
                EXPECT_TRUE(parent_segments.contains(segment.filename()));
            }
        }
    }

    using PhysicalSegment = std::tuple<uint32_t, uint32_t, std::string, uint64_t>;
    auto physical_layout = [](const TabletMetadataPtr& metadata) {
        std::vector<PhysicalSegment> layout;
        for (const auto& rowset : metadata->rowsets()) {
            for (int i = 0; i < rowset.segment_metas_size(); ++i) {
                const auto& segment = rowset.segment_metas(i);
                layout.emplace_back(rowset.id(), segment.has_segment_idx() ? segment.segment_idx() : i,
                                    segment.filename(), segment.size());
            }
        }
        return layout;
    };
    const auto first_layout = physical_layout(split_published.at(child_a));
    const auto second_layout = physical_layout(split_published.at(child_b));
    ASSERT_FALSE(first_layout.empty());
    ASSERT_FALSE(second_layout.empty());
    EXPECT_TRUE(std::is_sorted(first_layout.begin(), first_layout.end()));
    EXPECT_TRUE(std::is_sorted(second_layout.begin(), second_layout.end()));
    EXPECT_NE(first_layout, second_layout) << "split children must have genuinely different physical segment layouts";

    const auto& first_sstables = split_published.at(child_a)->sstable_meta().sstables();
    const auto& second_sstables = split_published.at(child_b)->sstable_meta().sstables();
    ASSERT_GT(first_sstables.size(), 0);
    ASSERT_EQ(first_sstables.size(), second_sstables.size());
    for (int i = 0; i < first_sstables.size(); ++i) {
        EXPECT_TRUE(first_sstables.Get(i).shared());
        EXPECT_TRUE(second_sstables.Get(i).shared());
        PersistentIndexSstablePB normalized_first(first_sstables.Get(i));
        PersistentIndexSstablePB normalized_second(second_sstables.Get(i));
        normalized_first.clear_fileset_id();
        normalized_second.clear_fileset_id();
        EXPECT_EQ(normalized_first.SerializeAsString(), normalized_second.SerializeAsString())
                << "the children inherit one SST set, differing only in fileset grouping";
    }

    const int64_t rejected_before = merge_rejected_total();
    auto status = expect_merge_rejected_without_writes({split_published.at(child_a), split_published.at(child_b)},
                                                       next_id(), written->version() + 2);
    EXPECT_TRUE(status.message().contains("still holds shared data files")) << status;
    EXPECT_EQ(rejected_before + 1, merge_rejected_total());
}

// Two private lifecycle sources merge with their SSTs carried; after a write, a flush and a full compaction the
// tablet has one segment spanning both halves. A split gives both children that segment and the parent's SSTs,
// shared, with child-local ranges and statistics. Merging the children back is refused without writing anything.
TEST_F(LakeTabletMergerTest, test_tablet_merging_split_children_with_identical_layouts_rejected) {
    const int32_t old_min_segments = config::lake_pk_compaction_min_input_segments;
    config::lake_pk_compaction_min_input_segments = 1;
    DeferOp restore_config([&] { config::lake_pk_compaction_min_input_segments = old_min_segments; });
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    DeferOp restore_flush([&] { set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE); });

    const int64_t left_id = next_id();
    const int64_t right_id = next_id();
    const int64_t merged_id = next_id();
    prepare_tablet_dirs(merged_id);
    ASSIGN_OR_ABORT(auto left, create_lifecycle_source(left_id, 0, 50, 10, 100));
    ASSIGN_OR_ABORT(auto right, create_lifecycle_source(right_id, 50, 100, 60, 600));
    ASSIGN_OR_ABORT(left, _primary_key_manager->flush_pk_memtable(left, left->version()));
    ASSIGN_OR_ABORT(right, _primary_key_manager->flush_pk_memtable(right, right->version()));
    _primary_key_manager->unload_and_remove_primary_index(left_id);
    _primary_key_manager->unload_and_remove_primary_index(right_id);
    std::unordered_map<int64_t, TabletMetadataPtr> merge_published;
    ASSERT_OK(publish_resharding_merge({left, right}, merged_id, left->version(), left->version() + 1, next_id(),
                                       merge_published));
    auto merged = merge_published.at(merged_id);
    ASSERT_GT(merged->sstable_meta().sstables_size(), 0);

    _primary_key_manager->unload_and_remove_primary_index(merged_id);
    ASSIGN_OR_ABORT(auto written, publish_followup_upsert_delete(merged_id, merged->version(), 10, 2000,
                                                                 /*delete_key=*/0, /*include_delete=*/false));
    const std::vector<std::pair<int32_t, int32_t>> expected = {{10, 2000}, {60, 600}};
    expect_lifecycle_oracle(written, expected, {});

    // Persist the writer's index so the split children inherit an SST set.
    ASSIGN_OR_ABORT(auto persisted_index, _primary_key_manager->flush_pk_memtable(written, written->version()));
    ASSERT_OK(put_tablet_metadata(persisted_index));
    ASSERT_GT(persisted_index->sstable_meta().sstables_size(), 0);

    ASSIGN_OR_ABORT(auto compacted, compact_tablet(merged_id, persisted_index->version(), /*force_base=*/true));
    ASSERT_EQ(1, compacted->rowsets_size());
    ASSERT_EQ(1, compacted->rowsets(0).segment_metas_size()) << "one physical segment spanning both child ranges";
    expect_lifecycle_oracle(compacted, expected, {});

    const std::string parent_segment = compacted->rowsets(0).segment_metas(0).filename();
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    ReshardingTabletInfoPB split_info;
    auto& splitting = *split_info.mutable_splitting_tablet_info();
    splitting.set_old_tablet_id(merged_id);
    splitting.add_new_tablet_ids(child_a);
    splitting.add_new_tablet_ids(child_b);
    TxnInfoPB split_txn;
    split_txn.set_txn_id(next_id());
    split_txn.set_commit_time(1);
    split_txn.set_gtid(1);
    std::unordered_map<int64_t, TabletMetadataPtr> split_published;
    std::unordered_map<int64_t, TabletRangePB> split_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), split_info, compacted->version(),
                                              compacted->version() + 1, split_txn, false, split_published,
                                              split_ranges));
    ASSERT_TRUE(split_published.contains(child_a));
    ASSERT_TRUE(split_published.contains(child_b));
    const auto& first_child = split_published.at(child_a);
    const auto& second_child = split_published.at(child_b);
    ASSERT_EQ(1, first_child->rowsets_size());
    ASSERT_EQ(1, second_child->rowsets_size());
    ASSERT_EQ(1, first_child->rowsets(0).segment_metas_size());
    ASSERT_EQ(1, second_child->rowsets(0).segment_metas_size());
    EXPECT_EQ(parent_segment, first_child->rowsets(0).segment_metas(0).filename());
    EXPECT_EQ(parent_segment, second_child->rowsets(0).segment_metas(0).filename());
    EXPECT_TRUE(first_child->rowsets(0).segment_metas(0).shared());
    EXPECT_TRUE(second_child->rowsets(0).segment_metas(0).shared());
    const auto& first_rowset = first_child->rowsets(0);
    const auto& second_rowset = second_child->rowsets(0);
    EXPECT_TRUE(lake::tablet_reshard_helper::same_rowset_uid(first_rowset, second_rowset));
    EXPECT_EQ(first_rowset.id(), second_rowset.id());
    EXPECT_EQ(first_rowset.version(), second_rowset.version());
    // The per-child statistics projection is fed the boundary planner's samples, so a shared segment's bytes are
    // apportioned by sampled row positions rather than by an even split of its coarse [min, max].
    EXPECT_NE(first_rowset.data_size(), second_rowset.data_size())
            << "the split fixture must exercise proportional child-local statistics";
    ASSERT_TRUE(first_rowset.has_range());
    ASSERT_TRUE(second_rowset.has_range());
    EXPECT_NE(first_rowset.range().SerializeAsString(), second_rowset.range().SerializeAsString())
            << "a genuine split must stamp distinct child-local rowset ranges";
    ASSERT_EQ(first_rowset.del_files_size(), second_rowset.del_files_size());
    for (int i = 0; i < first_rowset.del_files_size(); ++i) {
        EXPECT_EQ(first_rowset.del_files(i).SerializeAsString(), second_rowset.del_files(i).SerializeAsString());
    }
    for (int i = 0; i < first_rowset.segment_metas_size(); ++i) {
        EXPECT_EQ(lake::get_segment_idx(first_rowset, i), lake::get_segment_idx(second_rowset, i));
        EXPECT_EQ(first_rowset.segment_metas(i).SerializeAsString(), second_rowset.segment_metas(i).SerializeAsString())
                << "split children must retain identical physical segment layout";
    }

    auto normalized_sstables = [](const TabletMetadataPtr& metadata) {
        auto normalized = metadata->sstable_meta();
        for (auto& sstable : *normalized.mutable_sstables()) sstable.clear_fileset_id();
        return normalized.SerializeAsString();
    };
    ASSERT_EQ(normalized_sstables(first_child), normalized_sstables(second_child))
            << "split children must inherit one semantically identical SST set";
    for (const auto& sstable : first_child->sstable_meta().sstables()) EXPECT_TRUE(sstable.shared());

    auto status =
            expect_merge_rejected_without_writes({first_child, second_child}, next_id(), compacted->version() + 2);
    EXPECT_TRUE(status.message().contains("still holds shared data files")) << status;
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_read_only_skip_stays_indexless_without_recovery) {
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    DeferOp restore_flush([&] { set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE); });
    const int64_t left_id = next_id();
    const int64_t right_id = next_id();
    const int64_t target_id = next_id();
    prepare_tablet_dirs(target_id);
    ASSIGN_OR_ABORT(auto left, create_lifecycle_source(left_id, 0, 50, 10, 100));
    ASSIGN_OR_ABORT(auto right, create_lifecycle_source(right_id, 50, 100, 60, 600));
    ASSIGN_OR_ABORT(left, _primary_key_manager->flush_pk_memtable(left, left->version()));
    ASSIGN_OR_ABORT(right, _primary_key_manager->flush_pk_memtable(right, right->version()));

    MergingTabletInfoPB merging;
    merging.add_old_tablet_ids(left_id);
    merging.add_old_tablet_ids(right_id);
    merging.set_new_tablet_id(target_id);
    TxnInfoPB txn_info;
    txn_info.set_txn_id(next_id());
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);
    ASSIGN_OR_ABORT(auto read_only, lake::virtual_merge_for_read(_tablet_manager.get(), {left, right}, merging,
                                                                 left->version() + 1, txn_info));
    EXPECT_EQ(0, read_only->sstable_meta().sstables_size());
    EXPECT_TRUE(read_only->orphan_files().empty());
}

// On a merged tablet that rebuilds its index, a native index load failure during the first write leaves the
// metadata, the SST directory and the index cache untouched and publishes nothing; the retry succeeds. Plaintext and
// TDE. The first source's modern SST has a watermark past its only rowset, which cannot be translated.
TEST_F(LakeTabletMergerTest, test_tablet_merging_indexless_tde_failure_retry_matrix) {
    using lake::ConfigResetGuard;
    ConfigResetGuard<int32_t> files_threshold(&config::cloud_native_pk_index_rebuild_files_threshold, 0);
    ConfigResetGuard<int64_t> rows_threshold(&config::cloud_native_pk_index_rebuild_rows_threshold, 0);
    ConfigResetGuard<int64_t> l0_limit(&config::l0_max_mem_usage, 1LL << 30);

    const bool old_tde = config::enable_transparent_data_encryption;
    DeferOp restore_tde([&] { config::enable_transparent_data_encryption = old_tde; });
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    DeferOp restore_flush([&] { set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE); });

    auto* sync = SyncPoint::GetInstance();
    DeferOp clear_load_failure([&] {
        sync->ClearCallBack("lake_index_load.1");
        sync->DisableProcessing();
    });
    auto force_fallback = [](std::vector<std::shared_ptr<TabletMetadataPB>>& sources) {
        sources[0]->mutable_sstable_meta()->mutable_sstables(0)->set_max_rss_rowid(static_cast<uint64_t>(2) << 32);
    };
    for (bool enable_tde : {false, true}) {
        SCOPED_TRACE(enable_tde ? "TDE" : "plaintext");
        config::enable_transparent_data_encryption = enable_tde;
        if (enable_tde) ensure_kek_in_key_cache();

        auto result = publish_metadata_only_merge_fixture(MetadataOnlyMergeShape::kPrivate, enable_tde,
                                                          /*with_del_file=*/false, /*skip_source_flush=*/false,
                                                          force_fallback);
        ASSERT_OK(result);
        auto target = std::make_shared<TabletMetadataPB>(result->published.at(result->target_tablet_id));
        ASSERT_EQ(0, target->sstable_meta().sstables_size());
        ExecEnv::GetInstance()->lake_services().pk_index_memtable_flush_thread_pool->wait();
        const std::string baseline_metadata = target->SerializeAsString();
        ASSIGN_OR_ABORT(auto baseline_inventory, sst_inventory(result->target_tablet_id));
        _primary_key_manager->unload_and_remove_primary_index(result->target_tablet_id);

        const Status injected = Status::InternalError(
                fmt::format("injected {} native index load failure", enable_tde ? "TDE" : "plaintext"));
        sync->SetCallBack("lake_index_load.1", [&](void* arg) { *static_cast<Status*>(arg) = injected; });
        sync->EnableProcessing();
        auto failed = publish_followup_upsert_delete(result->target_tablet_id, result->target_version, 20, 2000, 10);
        sync->ClearCallBack("lake_index_load.1");
        sync->DisableProcessing();
        ASSERT_FALSE(failed.ok());
        EXPECT_NE(std::string::npos, failed.status().to_string().find(std::string(injected.message())));
        auto* failed_entry = _primary_key_manager->index_cache().get(result->target_tablet_id);
        EXPECT_EQ(nullptr, failed_entry);
        if (failed_entry != nullptr) _primary_key_manager->index_cache().release(failed_entry);
        ASSIGN_OR_ABORT(auto metadata_after_failure,
                        _tablet_manager->get_tablet_metadata(result->target_tablet_id, result->target_version));
        EXPECT_EQ(baseline_metadata, metadata_after_failure->SerializeAsString());
        ExecEnv::GetInstance()->lake_services().pk_index_memtable_flush_thread_pool->wait();
        ASSIGN_OR_ABORT(auto failed_inventory, sst_inventory(result->target_tablet_id));
        EXPECT_EQ(baseline_inventory, failed_inventory);
        expect_target_version_not_published(result->target_tablet_id, result->target_version + 1);

        ASSIGN_OR_ABORT(auto recovered,
                        publish_followup_upsert_delete(result->target_tablet_id, result->target_version, 20, 2000, 10));
        EXPECT_EQ(0, recovered->sstable_meta().sstables_size());
        expect_lifecycle_oracle(recovered, {{20, 2000}, {60, 600}}, {10});
        _primary_key_manager->unload_and_remove_primary_index(result->target_tablet_id);
        ASSIGN_OR_ABORT(auto restarted,
                        _tablet_manager->get_tablet_metadata(result->target_tablet_id, recovered->version()));
        expect_lifecycle_oracle(restarted, {{20, 2000}, {60, 600}}, {10});
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_prechange_empty_protected_rowsets_before_io) {
    for (bool include_owner : {false, true}) {
        for (bool stale_first : {false, true}) {
            SCOPED_TRACE(fmt::format("include_owner={}, stale_first={}", include_owner, stale_first));
            ASSIGN_OR_ABORT(auto fixture, make_prechange_protected_rssid_fixture());
            std::vector<TabletMetadataPtr> sources = {include_owner ? fixture.owner : fixture.empty_child,
                                                      fixture.stale_child};
            if (stale_first) std::reverse(sources.begin(), sources.end());
            expect_merge_rejected_without_writes(sources, next_id(), 2);
        }
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_delvec_drops_live_source_page_without_target_segment) {
    constexpr int64_t kBaseVersion = 1;
    constexpr int64_t kMergeVersion = 2;
    const int64_t canonical_tablet = next_id();
    const int64_t noncanonical_tablet = next_id();
    const int64_t merged_tablet = next_id();
    for (int64_t tablet_id : {canonical_tablet, noncanonical_tablet, merged_tablet}) prepare_tablet_dirs(tablet_id);

    auto make_source = [&](int64_t tablet_id, int32_t lower, int32_t upper, bool with_segment) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(kBaseVersion);
        metadata->set_next_rowset_id(3);
        set_two_column_pk_schema(metadata.get(), /*schema_id=*/4001);
        metadata->set_enable_persistent_index(true);
        metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(lower));
        metadata->mutable_range()->set_lower_bound_included(true);
        metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(upper));
        metadata->mutable_range()->set_upper_bound_included(false);
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(kBaseVersion);
        rowset->set_num_rows(with_segment ? 1 : 0);
        rowset->set_data_size(with_segment ? 1 : 0);
        auto* predicate = rowset->mutable_delete_predicate();
        predicate->set_version(kBaseVersion);
        predicate->mutable_in_predicates();
        if (with_segment) {
            auto* segment = rowset->add_segment_metas();
            segment->set_segment_idx(1);
            segment->set_filename("noncanonical.dat");
            segment->set_size(1);
            segment->set_num_rows(1);
        }
        lake::tablet_reshard_helper::set_rowset_uid(rowset);
        return metadata;
    };

    // Duplicate delete-predicate rowsets intentionally do not union segment lists or
    // install a shared-rssid map. This defensive fixture gives the second source a real
    // get_rssid identity whose natural projection has no segment in the target, making
    // target-live filtering independently observable.
    auto canonical = make_source(canonical_tablet, /*lower=*/0, /*upper=*/50, /*with_segment=*/false);
    auto noncanonical = make_source(noncanonical_tablet, /*lower=*/50, /*upper=*/100, /*with_segment=*/true);
    const uint32_t source_live_rssid = lake::get_rssid(noncanonical->rowsets(0), 0);
    ASSERT_EQ(2, source_live_rssid);
    DelVector stale_delvec;
    const uint32_t deleted_rowid = 0;
    stale_delvec.init(kBaseVersion, &deleted_rowid, 1);
    add_delvec(noncanonical.get(), noncanonical_tablet, kBaseVersion, source_live_rssid,
               "live_source_target_dead.delvec", stale_delvec.save());

    ASSIGN_OR_ABORT(auto files_before_merge, delvec_inventory(merged_tablet));
    std::unordered_map<int64_t, TabletMetadataPtr> published;
    auto merge_status = publish_resharding_merge({canonical, noncanonical}, merged_tablet, kBaseVersion, kMergeVersion,
                                                 next_id(), published);
    EXPECT_TRUE(merge_status.is_corruption()) << merge_status;
    EXPECT_FALSE(published.contains(merged_tablet));
    ASSIGN_OR_ABORT(auto files_after_merge, delvec_inventory(merged_tablet));
    EXPECT_EQ(files_before_merge, files_after_merge);
}

TEST_F(LakeTabletMergerTest, test_merge_rowsets_reorder_by_predicate_version) {
    const int64_t base_version = 2;
    const int64_t new_version = 3;
    const int64_t tablet_a = next_id();
    const int64_t tablet_b = next_id();
    const int64_t new_tablet = next_id();

    prepare_tablet_dirs(tablet_a);
    prepare_tablet_dirs(tablet_b);
    prepare_tablet_dirs(new_tablet);

    TabletMetadataPB meta_a;
    meta_a.set_id(tablet_a);
    meta_a.set_version(base_version);
    meta_a.set_next_rowset_id(4);
    add_rowset_with_predicate(&meta_a, 1, 1, false);
    add_rowset_with_predicate(&meta_a, 2, 10, true);
    add_rowset_with_predicate(&meta_a, 3, 11, false);

    TabletMetadataPB meta_b;
    meta_b.set_id(tablet_b);
    meta_b.set_version(base_version);
    meta_b.set_next_rowset_id(4);
    add_rowset_with_predicate(&meta_b, 1, 1, false);
    add_rowset_with_predicate(&meta_b, 2, 10, true);
    add_rowset_with_predicate(&meta_b, 3, 11, false);
    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.set_new_tablet_id(new_tablet);
    merging_tablet.add_old_tablet_ids(tablet_a);
    merging_tablet.add_old_tablet_ids(tablet_b);

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto res = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                               txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);

    auto it = tablet_metadatas.find(new_tablet);
    ASSERT_TRUE(it != tablet_metadatas.end());
    const auto& merged_meta = it->second;
    ASSERT_EQ(5, merged_meta->rowsets_size());

    std::vector<uint32_t> rowset_ids;
    int predicate_count = 0;
    for (const auto& rowset : merged_meta->rowsets()) {
        rowset_ids.push_back(rowset.id());
        if (rowset.has_delete_predicate()) {
            predicate_count++;
            EXPECT_EQ(10, rowset.version());
        }
    }

    EXPECT_EQ(1, predicate_count);
    // Expected rowset order after reordering by predicate version:
    // - Tablet A rowset 1 (id=1, version 1, data) -> comes before predicate
    // - Tablet B rowset 1 (id=4, version 1, data, offset=3 from tablet A) -> comes before predicate
    // - Tablet A rowset 2 (id=2, version 10, predicate) -> kept, tablet B's duplicate predicate removed
    // - Tablet A rowset 3 (id=3, version 11, data) -> after predicate
    // - Tablet B rowset 3 (id=5, version 11, densely packed) -> after predicate
    EXPECT_EQ((std::vector<uint32_t>{1, 4, 2, 3, 5}), rowset_ids);
}

TEST_F(LakeTabletMergerTest, test_merge_rowsets_different_predicate_versions) {
    // Test case: tablets with different predicate versions
    // tablet_a: version 10 predicate
    // tablet_b: version 10 and 20 predicates
    // Expected: rowsets ordered by version 10, then 20
    // Version 10 predicate deduplicated, version 20 kept only from tablet_b
    const int64_t base_version = 2;
    const int64_t new_version = 3;
    const int64_t tablet_a = next_id();
    const int64_t tablet_b = next_id();
    const int64_t new_tablet = next_id();

    prepare_tablet_dirs(tablet_a);
    prepare_tablet_dirs(tablet_b);
    prepare_tablet_dirs(new_tablet);

    // Tablet A: data(v1) -> predicate(v10) -> data(v11)
    TabletMetadataPB meta_a;
    meta_a.set_id(tablet_a);
    meta_a.set_version(base_version);
    meta_a.set_next_rowset_id(4);
    add_rowset_with_predicate(&meta_a, 1, 1, false);  // data
    add_rowset_with_predicate(&meta_a, 2, 10, true);  // predicate v10
    add_rowset_with_predicate(&meta_a, 3, 11, false); // data

    // Tablet B: data(v1) -> predicate(v10) -> data(v11) -> predicate(v20) -> data(v21)
    TabletMetadataPB meta_b;
    meta_b.set_id(tablet_b);
    meta_b.set_version(base_version);
    meta_b.set_next_rowset_id(6);
    add_rowset_with_predicate(&meta_b, 1, 1, false);  // data
    add_rowset_with_predicate(&meta_b, 2, 10, true);  // predicate v10
    add_rowset_with_predicate(&meta_b, 3, 11, false); // data
    add_rowset_with_predicate(&meta_b, 4, 20, true);  // predicate v20 (only in tablet_b)
    add_rowset_with_predicate(&meta_b, 5, 21, false); // data
    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.set_new_tablet_id(new_tablet);
    merging_tablet.add_old_tablet_ids(tablet_a);
    merging_tablet.add_old_tablet_ids(tablet_b);

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto res = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                               txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);

    auto it = tablet_metadatas.find(new_tablet);
    ASSERT_TRUE(it != tablet_metadatas.end());
    const auto& merged_meta = it->second;

    // Expected: 3 data from A + 3 data from B + 1 predicate(v10) + 1 predicate(v20) = 8 rowsets
    // But v10 is deduplicated, so: 3 + 3 + 2 - 1 = 7 rowsets
    ASSERT_EQ(7, merged_meta->rowsets_size());

    std::vector<uint32_t> rowset_ids;
    int predicate_count = 0;
    std::vector<int64_t> predicate_versions;
    for (const auto& rowset : merged_meta->rowsets()) {
        rowset_ids.push_back(rowset.id());
        if (rowset.has_delete_predicate()) {
            predicate_count++;
            predicate_versions.push_back(rowset.version());
        }
    }

    EXPECT_EQ(2, predicate_count);
    // Predicate versions should be in order: v10, v20
    EXPECT_EQ((std::vector<int64_t>{10, 20}), predicate_versions);
    // Version-driven k-way merge order:
    // v1: A(id=1), B(id=4)
    // v10: A predicate(id=2) output, B predicate dedup skip
    // v11: A(id=3), B(id=5)
    // v20: B predicate(id=6)
    // v21: B(id=7)
    EXPECT_EQ((std::vector<uint32_t>{1, 4, 2, 3, 5, 6, 7}), rowset_ids);
}

TEST_F(LakeTabletMergerTest, test_merge_rowsets_no_predicates) {
    // Test case: tablets with no predicates
    // Both tablets have only data rowsets
    // Expected: no reordering needed, rowsets in original order
    const int64_t base_version = 2;
    const int64_t new_version = 3;
    const int64_t tablet_a = next_id();
    const int64_t tablet_b = next_id();
    const int64_t new_tablet = next_id();

    prepare_tablet_dirs(tablet_a);
    prepare_tablet_dirs(tablet_b);
    prepare_tablet_dirs(new_tablet);

    TabletMetadataPB meta_a;
    meta_a.set_id(tablet_a);
    meta_a.set_version(base_version);
    meta_a.set_next_rowset_id(4);
    add_rowset_with_predicate(&meta_a, 1, 1, false);
    add_rowset_with_predicate(&meta_a, 2, 2, false);
    add_rowset_with_predicate(&meta_a, 3, 3, false);

    TabletMetadataPB meta_b;
    meta_b.set_id(tablet_b);
    meta_b.set_version(base_version);
    meta_b.set_next_rowset_id(4);
    add_rowset_with_predicate(&meta_b, 1, 1, false);
    add_rowset_with_predicate(&meta_b, 2, 2, false);
    add_rowset_with_predicate(&meta_b, 3, 3, false);
    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.set_new_tablet_id(new_tablet);
    merging_tablet.add_old_tablet_ids(tablet_a);
    merging_tablet.add_old_tablet_ids(tablet_b);

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto res = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                               txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);

    auto it = tablet_metadatas.find(new_tablet);
    ASSERT_TRUE(it != tablet_metadatas.end());
    const auto& merged_meta = it->second;

    // All 6 rowsets should be present (no deduplication needed)
    ASSERT_EQ(6, merged_meta->rowsets_size());

    std::vector<uint32_t> rowset_ids;
    int predicate_count = 0;
    for (const auto& rowset : merged_meta->rowsets()) {
        rowset_ids.push_back(rowset.id());
        if (rowset.has_delete_predicate()) {
            predicate_count++;
        }
    }

    EXPECT_EQ(0, predicate_count);
    // Version-driven k-way merge interleaves by (version, old_tablet_index):
    // v1: A(id=1), B(id=4); v2: A(id=2), B(id=5); v3: A(id=3), B(id=6)
    EXPECT_EQ((std::vector<uint32_t>{1, 4, 2, 5, 3, 6}), rowset_ids);
}

TEST_F(LakeTabletMergerTest, test_merge_rowsets_single_tablet_predicate) {
    // Test case: only one tablet has predicates
    // tablet_a: has predicate version 10
    // tablet_b: no predicates
    // Expected: tablet_a data before predicate, then predicate,
    //           then all remaining data from both tablets
    const int64_t base_version = 2;
    const int64_t new_version = 3;
    const int64_t tablet_a = next_id();
    const int64_t tablet_b = next_id();
    const int64_t new_tablet = next_id();

    prepare_tablet_dirs(tablet_a);
    prepare_tablet_dirs(tablet_b);
    prepare_tablet_dirs(new_tablet);

    // Tablet A: data(v1) -> predicate(v10) -> data(v11)
    TabletMetadataPB meta_a;
    meta_a.set_id(tablet_a);
    meta_a.set_version(base_version);
    meta_a.set_next_rowset_id(4);
    add_rowset_with_predicate(&meta_a, 1, 1, false);  // data
    add_rowset_with_predicate(&meta_a, 2, 10, true);  // predicate v10
    add_rowset_with_predicate(&meta_a, 3, 11, false); // data

    // Tablet B: data(v1) -> data(v2) -> data(v3) (no predicates)
    TabletMetadataPB meta_b;
    meta_b.set_id(tablet_b);
    meta_b.set_version(base_version);
    meta_b.set_next_rowset_id(4);
    add_rowset_with_predicate(&meta_b, 1, 1, false);
    add_rowset_with_predicate(&meta_b, 2, 2, false);
    add_rowset_with_predicate(&meta_b, 3, 3, false);
    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.set_new_tablet_id(new_tablet);
    merging_tablet.add_old_tablet_ids(tablet_a);
    merging_tablet.add_old_tablet_ids(tablet_b);

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto res = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                               txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);

    auto it = tablet_metadatas.find(new_tablet);
    ASSERT_TRUE(it != tablet_metadatas.end());
    const auto& merged_meta = it->second;

    // 3 from A + 3 from B = 6 rowsets (no deduplication, only A has predicate)
    ASSERT_EQ(6, merged_meta->rowsets_size());

    std::vector<uint32_t> rowset_ids;
    int predicate_count = 0;
    for (const auto& rowset : merged_meta->rowsets()) {
        rowset_ids.push_back(rowset.id());
        if (rowset.has_delete_predicate()) {
            predicate_count++;
            EXPECT_EQ(10, rowset.version());
        }
    }

    EXPECT_EQ(1, predicate_count);
    // Version-driven k-way merge order:
    // v1: A(id=1), B(id=4); v2: B(id=5); v3: B(id=6);
    // v10: A predicate(id=2); v11: A(id=3)
    EXPECT_EQ((std::vector<uint32_t>{1, 4, 5, 6, 2, 3}), rowset_ids);
}

TEST_F(LakeTabletMergerTest, test_merge_rowsets_all_predicates) {
    // Test case: all rowsets are predicates (edge case)
    // Both tablets have only predicate rowsets (no data)
    // Expected: deduplicated predicates only
    const int64_t base_version = 2;
    const int64_t new_version = 3;
    const int64_t tablet_a = next_id();
    const int64_t tablet_b = next_id();
    const int64_t new_tablet = next_id();

    prepare_tablet_dirs(tablet_a);
    prepare_tablet_dirs(tablet_b);
    prepare_tablet_dirs(new_tablet);

    // Tablet A: predicate(v10) -> predicate(v20)
    TabletMetadataPB meta_a;
    meta_a.set_id(tablet_a);
    meta_a.set_version(base_version);
    meta_a.set_next_rowset_id(3);
    add_rowset_with_predicate(&meta_a, 1, 10, true); // predicate v10
    add_rowset_with_predicate(&meta_a, 2, 20, true); // predicate v20

    // Tablet B: predicate(v10) -> predicate(v20) (same versions)
    TabletMetadataPB meta_b;
    meta_b.set_id(tablet_b);
    meta_b.set_version(base_version);
    meta_b.set_next_rowset_id(3);
    add_rowset_with_predicate(&meta_b, 1, 10, true); // predicate v10
    add_rowset_with_predicate(&meta_b, 2, 20, true); // predicate v20
    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.set_new_tablet_id(new_tablet);
    merging_tablet.add_old_tablet_ids(tablet_a);
    merging_tablet.add_old_tablet_ids(tablet_b);

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto res = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                               txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);

    auto it = tablet_metadatas.find(new_tablet);
    ASSERT_TRUE(it != tablet_metadatas.end());
    const auto& merged_meta = it->second;

    // 4 predicates total, but v10 and v20 each deduplicated -> 2 rowsets
    ASSERT_EQ(2, merged_meta->rowsets_size());

    std::vector<uint32_t> rowset_ids;
    std::vector<int64_t> predicate_versions;
    for (const auto& rowset : merged_meta->rowsets()) {
        rowset_ids.push_back(rowset.id());
        EXPECT_TRUE(rowset.has_delete_predicate());
        predicate_versions.push_back(rowset.version());
    }

    // Both rowsets are predicates
    EXPECT_EQ(2u, rowset_ids.size());
    EXPECT_EQ((std::vector<int64_t>{10, 20}), predicate_versions);
    // First predicate for each version comes from tablet_a (ids 1 and 2)
    EXPECT_EQ((std::vector<uint32_t>{1, 2}), rowset_ids);
}

// Two primary-key sources with a delete vector, a DCG, a del file, a recovery key and an SST each. The first (lower
// tablet id) references RSSIDs 7 to 10 and is packed to [1, 5); the second references 1 to 3 and is packed to [5, 8).
// Each rowset keeps its range, schema mapping and sidecars under its new RSSID, and the delete-vector pages are copied
// in RSSID order. The first source's SST has a watermark (1) below its RSSIDs, so the merged tablet drops both SSTs
// and rebuilds its index.
TEST_F(LakeTabletMergerTest, test_tablet_merging_basic) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t old_tablet_id_1 = next_id();
    const int64_t old_tablet_id_2 = next_id();
    const int64_t new_tablet_id = next_id();

    prepare_tablet_dirs(old_tablet_id_1);
    prepare_tablet_dirs(old_tablet_id_2);
    prepare_tablet_dirs(new_tablet_id);

    auto meta1 = std::make_shared<TabletMetadataPB>();
    meta1->set_id(old_tablet_id_1);
    meta1->set_version(base_version);
    meta1->set_next_rowset_id(100);
    set_primary_key_schema(meta1.get(), 1001);
    add_historical_schema(meta1.get(), 5001);
    add_rowset(meta1.get(), 10, 7, 10);
    (*meta1->mutable_rowset_to_schema())[10] = 1001;
    add_delvec(meta1.get(), old_tablet_id_1, base_version, 10, "delvec-1", "aaaa");
    add_sstable(meta1.get(), "sst-1", (static_cast<uint64_t>(1) << 32) | 7, true);
    add_dcg_with_columns(meta1.get(), 10, "dcg-1", {101, 102}, 1, /*shared_file=*/false);

    auto meta2 = std::make_shared<TabletMetadataPB>();
    meta2->set_id(old_tablet_id_2);
    meta2->set_version(base_version);
    meta2->set_next_rowset_id(3);
    set_primary_key_schema(meta2.get(), 2002);
    add_historical_schema(meta2.get(), 5002);
    add_rowset(meta2.get(), 1, 3, 1);
    (*meta2->mutable_rowset_to_schema())[1] = 2002;
    add_delvec(meta2.get(), old_tablet_id_2, base_version, 1, "delvec-2", "bbbbbb");
    add_sstable(meta2.get(), "sst-2", (static_cast<uint64_t>(2) << 32) | 5, true);
    add_dcg_with_columns(meta2.get(), 1, "dcg-2", {201, 202}, 1, /*shared_file=*/false);

    // Each source's tablet range comes from its position: below key 1 for the first, from key 1 for the second.
    materialize_tombstone_sstables(meta1.get(), /*key=*/0);
    materialize_tombstone_sstables(meta2.get(), /*key=*/5);

    ASSERT_OK(put_merge_sources(meta1, meta2));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(old_tablet_id_1);
    merging_tablet.add_old_tablet_ids(old_tablet_id_2);
    merging_tablet.set_new_tablet_id(new_tablet_id);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(10);
    txn_info.set_commit_time(111);
    txn_info.set_gtid(222);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(new_tablet_id);
    ASSERT_TRUE(merged->has_range());
    EXPECT_EQ(8, merged->next_rowset_id());
    const uint32_t expected_rowset_id = 5;

    bool found_rowset = false;
    for (const auto& rowset : merged->rowsets()) {
        if (rowset.id() == expected_rowset_id) {
            found_rowset = true;
            ASSERT_TRUE(rowset.has_range());
            EXPECT_EQ(rowset.range().SerializeAsString(),
                      tablet_metadatas.at(old_tablet_id_2)->range().SerializeAsString());
            ASSERT_TRUE(rowset.has_max_compact_input_rowset_id());
            EXPECT_EQ(7, rowset.max_compact_input_rowset_id());
            ASSERT_EQ(1, rowset.del_files_size());
            EXPECT_EQ(5, rowset.del_files(0).origin_rowset_id());
            break;
        }
    }
    ASSERT_TRUE(found_rowset);

    bool found_rowset_from_meta1 = false;
    for (const auto& rowset : merged->rowsets()) {
        if (rowset.id() == 4) {
            found_rowset_from_meta1 = true;
            ASSERT_TRUE(rowset.has_range());
            EXPECT_EQ(rowset.range().SerializeAsString(),
                      tablet_metadatas.at(old_tablet_id_1)->range().SerializeAsString());
            ASSERT_TRUE(rowset.has_max_compact_input_rowset_id());
            EXPECT_EQ(1, rowset.max_compact_input_rowset_id());
            ASSERT_EQ(1, rowset.del_files_size());
            EXPECT_EQ(4, rowset.del_files(0).origin_rowset_id());
            break;
        }
    }
    ASSERT_TRUE(found_rowset_from_meta1);

    auto rowset_schema_it = merged->rowset_to_schema().find(expected_rowset_id);
    ASSERT_TRUE(rowset_schema_it != merged->rowset_to_schema().end());
    EXPECT_EQ(2002, rowset_schema_it->second);

    EXPECT_EQ(0, merged->sstable_meta().sstables_size());
    ASSERT_EQ(2, merged->orphan_files_size());
    std::map<std::string, const PersistentIndexSstablePB*> source_sstables;
    source_sstables.emplace("sst-1", &meta1->sstable_meta().sstables(0));
    source_sstables.emplace("sst-2", &meta2->sstable_meta().sstables(0));
    for (const auto& orphan : merged->orphan_files()) {
        auto source = source_sstables.find(orphan.name());
        ASSERT_NE(source_sstables.end(), source);
        EXPECT_TRUE(orphan.shared());
        EXPECT_EQ(0, orphan.version());
        EXPECT_EQ(source->second->filesize(), orphan.size());
        EXPECT_EQ(source->second->encryption_meta(), orphan.encryption_meta());
        source_sstables.erase(source);
    }
    EXPECT_TRUE(source_sstables.empty());

    const uint32_t expected_segment_id = 5;
    auto delvec_it = merged->delvec_meta().delvecs().find(expected_segment_id);
    ASSERT_TRUE(delvec_it != merged->delvec_meta().delvecs().end());
    EXPECT_EQ(new_version, delvec_it->second.version());
    EXPECT_EQ(static_cast<uint64_t>(4), delvec_it->second.offset());

    EXPECT_TRUE(merged->delvec_meta().version_to_file().find(new_version) !=
                merged->delvec_meta().version_to_file().end());

    auto dcg_it = merged->dcg_meta().dcgs().find(expected_segment_id);
    ASSERT_TRUE(dcg_it != merged->dcg_meta().dcgs().end());
    ASSERT_EQ(1, dcg_it->second.column_files_size());
    EXPECT_EQ("dcg-2", dcg_it->second.column_files(0));

    // Unreferenced historical schemas (5001, 5002) are pruned by merge_schemas().
    // The current schema (1001) is always preserved.
    EXPECT_TRUE(merged->historical_schemas().find(1001) != merged->historical_schemas().end());
}

// Strict-uid gate (MERGE side): a rowset reaching reshard merge without a valid
// uid must fail loudly, not silently mis-dedup. Every production producer mints a
// uid, and the test put_tablet_metadata wrapper auto-stamps one on every synthetic
// rowset specifically so fixtures behave like production — which means this gate is
// otherwise never exercised. Here we bypass the wrapper (persist via _tablet_manager
// directly) AND clear the uid that add_rowset stamps, driving a genuinely uid-less
// rowset into the global planner, which returns Corruption before allocation or I/O.
TEST_F(LakeTabletMergerTest, test_tablet_merging_rowset_without_uid_fails) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t old_tablet_id_1 = next_id();
    const int64_t old_tablet_id_2 = next_id();
    const int64_t new_tablet_id = next_id();

    prepare_tablet_dirs(old_tablet_id_1);
    prepare_tablet_dirs(old_tablet_id_2);
    prepare_tablet_dirs(new_tablet_id);

    auto meta1 = std::make_shared<TabletMetadataPB>();
    meta1->set_id(old_tablet_id_1);
    meta1->set_version(base_version);
    meta1->set_next_rowset_id(100);
    set_primary_key_schema(meta1.get(), 1001);
    add_rowset(meta1.get(), 10, 7, 10); // keeps its stamped uid (valid input)
    (*meta1->mutable_rowset_to_schema())[10] = 1001;

    auto meta2 = std::make_shared<TabletMetadataPB>();
    meta2->set_id(old_tablet_id_2);
    meta2->set_version(base_version);
    meta2->set_next_rowset_id(3);
    set_primary_key_schema(meta2.get(), 2002);
    add_rowset(meta2.get(), 1, 3, 1)->clear_uid(); // producer-side regression: no uid
    (*meta2->mutable_rowset_to_schema())[1] = 2002;

    // Persist directly, bypassing the uid-auto-stamping fixture wrapper.
    meta1->mutable_rowsets(0)->mutable_segment_metas(0)->set_segment_idx(0);
    meta2->mutable_rowsets(0)->mutable_segment_metas(0)->set_segment_idx(0);
    *meta1->mutable_range()->mutable_upper_bound() = generate_sort_key(1);
    meta1->mutable_range()->set_upper_bound_included(false);
    *meta2->mutable_range()->mutable_lower_bound() = generate_sort_key(1);
    meta2->mutable_range()->set_lower_bound_included(true);
    ASSERT_OK(_tablet_manager->put_tablet_metadata(meta1));
    ASSERT_OK(_tablet_manager->put_tablet_metadata(meta2));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(old_tablet_id_1);
    merging_tablet.add_old_tablet_ids(old_tablet_id_2);
    merging_tablet.set_new_tablet_id(new_tablet_id);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(10);

    auto do_merge = [&]() {
        std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
        std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
        return lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                               txn_info, false, tablet_metadatas, tablet_ranges);
    };
    auto st = do_merge();
    EXPECT_TRUE(st.is_corruption()) << st;
    EXPECT_TRUE(st.message().contains("uid")) << st;
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_without_delvec) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t old_tablet_id_1 = next_id();
    const int64_t old_tablet_id_2 = next_id();
    const int64_t new_tablet_id = next_id();

    prepare_tablet_dirs(old_tablet_id_1);
    prepare_tablet_dirs(old_tablet_id_2);
    prepare_tablet_dirs(new_tablet_id);

    auto meta1 = std::make_shared<TabletMetadataPB>();
    meta1->set_id(old_tablet_id_1);
    meta1->set_version(base_version);
    meta1->set_next_rowset_id(5);
    set_primary_key_schema(meta1.get(), 1001);
    add_rowset(meta1.get(), 1, 1, 1);

    auto meta2 = std::make_shared<TabletMetadataPB>();
    meta2->set_id(old_tablet_id_2);
    meta2->set_version(base_version);
    meta2->set_next_rowset_id(5);
    set_primary_key_schema(meta2.get(), 1002);
    add_rowset(meta2.get(), 2, 2, 2);

    ASSERT_OK(put_merge_sources(meta1, meta2));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(old_tablet_id_1);
    merging_tablet.add_old_tablet_ids(old_tablet_id_2);
    merging_tablet.set_new_tablet_id(new_tablet_id);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    EXPECT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_skip_missing_delvec_meta) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t old_tablet_id_1 = next_id();
    const int64_t old_tablet_id_2 = next_id();
    const int64_t new_tablet_id = next_id();

    prepare_tablet_dirs(old_tablet_id_1);
    prepare_tablet_dirs(old_tablet_id_2);
    prepare_tablet_dirs(new_tablet_id);

    auto meta1 = std::make_shared<TabletMetadataPB>();
    meta1->set_id(old_tablet_id_1);
    meta1->set_version(base_version);
    meta1->set_next_rowset_id(10);
    set_primary_key_schema(meta1.get(), 1001);
    add_rowset(meta1.get(), 1, 1, 1);
    add_delvec(meta1.get(), old_tablet_id_1, base_version, 1, "delvec-1", "aaa");

    auto meta2 = std::make_shared<TabletMetadataPB>();
    meta2->set_id(old_tablet_id_2);
    meta2->set_version(base_version);
    meta2->set_next_rowset_id(10);
    set_primary_key_schema(meta2.get(), 1002);
    add_rowset(meta2.get(), 2, 2, 2);

    ASSERT_OK(put_merge_sources(meta1, meta2));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(old_tablet_id_1);
    merging_tablet.add_old_tablet_ids(old_tablet_id_2);
    merging_tablet.set_new_tablet_id(new_tablet_id);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    EXPECT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_delvec_version_missing) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t old_tablet_id_1 = next_id();
    const int64_t old_tablet_id_2 = next_id();
    const int64_t new_tablet_id = next_id();

    prepare_tablet_dirs(old_tablet_id_1);
    prepare_tablet_dirs(old_tablet_id_2);
    prepare_tablet_dirs(new_tablet_id);

    auto meta1 = std::make_shared<TabletMetadataPB>();
    meta1->set_id(old_tablet_id_1);
    meta1->set_version(base_version);
    meta1->set_next_rowset_id(10);
    set_primary_key_schema(meta1.get(), 1001);
    add_rowset(meta1.get(), 1, 1, 1);
    add_delvec(meta1.get(), old_tablet_id_1, base_version, 1, "delvec-1", "aaa");

    auto meta2 = std::make_shared<TabletMetadataPB>();
    meta2->set_id(old_tablet_id_2);
    meta2->set_version(base_version);
    meta2->set_next_rowset_id(10);
    set_primary_key_schema(meta2.get(), 1002);
    add_rowset(meta2.get(), 2, 2, 2);
    auto* delvec_meta = meta2->mutable_delvec_meta();
    DelvecPagePB page;
    page.set_version(base_version);
    page.set_offset(0);
    page.set_size(1);
    (*delvec_meta->mutable_delvecs())[2] = page;

    ASSERT_OK(put_merge_sources(meta1, meta2));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(old_tablet_id_1);
    merging_tablet.add_old_tablet_ids(old_tablet_id_2);
    merging_tablet.set_new_tablet_id(new_tablet_id);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto st = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_TRUE(st.is_corruption()) << st;
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_delvec_missing_tablet_offset) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t old_tablet_id_1 = next_id();
    const int64_t old_tablet_id_2 = next_id();
    const int64_t new_tablet_id = next_id();

    prepare_tablet_dirs(old_tablet_id_1);
    prepare_tablet_dirs(old_tablet_id_2);
    prepare_tablet_dirs(new_tablet_id);

    auto meta1 = std::make_shared<TabletMetadataPB>();
    meta1->set_id(old_tablet_id_1);
    meta1->set_version(base_version);
    meta1->set_next_rowset_id(10);
    set_primary_key_schema(meta1.get(), 1001);
    add_rowset(meta1.get(), 1, 1, 1);
    add_delvec(meta1.get(), old_tablet_id_1, base_version, 1, "delvec-1", "aaa");

    auto meta2 = std::make_shared<TabletMetadataPB>();
    meta2->set_id(old_tablet_id_2);
    meta2->set_version(base_version);
    meta2->set_next_rowset_id(10);
    set_primary_key_schema(meta2.get(), 1002);
    add_rowset(meta2.get(), 2, 2, 2);
    add_delvec(meta2.get(), old_tablet_id_2, base_version, 2, "delvec-2", "bbb");

    ASSERT_OK(put_merge_sources(meta1, meta2));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(old_tablet_id_1);
    merging_tablet.add_old_tablet_ids(old_tablet_id_2);
    merging_tablet.set_new_tablet_id(new_tablet_id);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto st = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(st);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_cache_miss_fallback) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t old_tablet_id_1 = next_id();
    const int64_t old_tablet_id_2 = next_id();
    const int64_t new_tablet_id = next_id();

    prepare_tablet_dirs(old_tablet_id_1);
    prepare_tablet_dirs(old_tablet_id_2);
    prepare_tablet_dirs(new_tablet_id);

    auto meta1 = std::make_shared<TabletMetadataPB>();
    meta1->set_id(old_tablet_id_1);
    meta1->set_version(base_version);
    meta1->set_next_rowset_id(5);
    add_rowset(meta1.get(), 1, 1, 1);

    auto meta2 = std::make_shared<TabletMetadataPB>();
    meta2->set_id(old_tablet_id_2);
    meta2->set_version(base_version);
    meta2->set_next_rowset_id(5);
    add_rowset(meta2.get(), 2, 2, 2);

    ASSERT_OK(put_merge_sources(meta1, meta2));

    auto cached_meta1 = std::make_shared<TabletMetadataPB>(*meta1);
    cached_meta1->set_version(new_version);
    cached_meta1->set_commit_time(999);
    EXPECT_OK(_tablet_manager->cache_tablet_metadata(cached_meta1));

    auto cached_meta2 = std::make_shared<TabletMetadataPB>(*meta2);
    cached_meta2->set_version(new_version);
    cached_meta2->set_commit_time(999);
    EXPECT_OK(_tablet_manager->cache_tablet_metadata(cached_meta2));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(old_tablet_id_1);
    merging_tablet.add_old_tablet_ids(old_tablet_id_2);
    merging_tablet.set_new_tablet_id(new_tablet_id);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(10);
    txn_info.set_commit_time(123);
    txn_info.set_gtid(456);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    ASSERT_TRUE(tablet_metadatas.find(old_tablet_id_1) != tablet_metadatas.end());
    EXPECT_EQ(txn_info.commit_time(), tablet_metadatas.at(old_tablet_id_1)->commit_time());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_base_version_not_found) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t old_tablet_id_1 = next_id();
    const int64_t old_tablet_id_2 = next_id();
    const int64_t new_tablet_id = next_id();

    prepare_tablet_dirs(old_tablet_id_1);
    prepare_tablet_dirs(old_tablet_id_2);
    prepare_tablet_dirs(new_tablet_id);

    auto meta1 = std::make_shared<TabletMetadataPB>();
    meta1->set_id(old_tablet_id_1);
    meta1->set_version(new_version);
    meta1->set_next_rowset_id(5);
    meta1->set_gtid(100);

    auto meta2 = std::make_shared<TabletMetadataPB>();
    meta2->set_id(old_tablet_id_2);
    meta2->set_version(new_version);
    meta2->set_next_rowset_id(5);
    meta2->set_gtid(100);

    auto meta_new = std::make_shared<TabletMetadataPB>();
    meta_new->set_id(new_tablet_id);
    meta_new->set_version(new_version);
    meta_new->set_next_rowset_id(5);
    meta_new->set_gtid(100);

    ASSERT_OK(put_merge_sources(meta1, meta2));
    ASSERT_OK(put_tablet_metadata(meta_new));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(old_tablet_id_1);
    merging_tablet.add_old_tablet_ids(old_tablet_id_2);
    merging_tablet.set_new_tablet_id(new_tablet_id);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(100);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    EXPECT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));
    EXPECT_EQ(3, tablet_metadatas.size());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_get_metadata_error) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t old_tablet_id = next_id();
    const int64_t new_tablet_id = next_id();

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(old_tablet_id);
    merging_tablet.set_new_tablet_id(new_tablet_id);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    SyncPoint::GetInstance()->EnableProcessing();
    TEST_ENABLE_ERROR_POINT("TabletManager::get_tablet_metadata", Status::Corruption("injected"));

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto st = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_TRUE(st.is_corruption());

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_dcg_segment_overflow) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t old_tablet_id_1 = next_id();
    const int64_t old_tablet_id_2 = next_id();
    const int64_t new_tablet_id = next_id();

    prepare_tablet_dirs(old_tablet_id_1);
    prepare_tablet_dirs(old_tablet_id_2);
    prepare_tablet_dirs(new_tablet_id);

    auto meta1 = std::make_shared<TabletMetadataPB>();
    meta1->set_id(old_tablet_id_1);
    meta1->set_version(base_version);
    meta1->set_next_rowset_id(100);
    add_rowset(meta1.get(), 50, 50, 50);

    auto meta2 = std::make_shared<TabletMetadataPB>();
    meta2->set_id(old_tablet_id_2);
    meta2->set_version(base_version);
    meta2->set_next_rowset_id(10);
    add_rowset(meta2.get(), 90, 90, 90);
    add_dcg_with_columns(meta2.get(), std::numeric_limits<uint32_t>::max() - 5, "dcg-overflow", {301}, 1,
                         /*shared_file=*/false);

    ASSERT_OK(put_merge_sources(meta1, meta2));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(old_tablet_id_1);
    merging_tablet.add_old_tablet_ids(old_tablet_id_2);
    merging_tablet.set_new_tablet_id(new_tablet_id);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto st = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_TRUE(st.is_corruption()) << st;
}

// --- New tests for split-then-merge correctness ---

// Two children of a split, merged back before either has compacted: they still share the parent's segment and its
// primary-key index SST, so the merge refuses them without writing anything.
TEST_F(LakeTabletMergerTest, test_tablet_merging_split_then_merge) {
    auto make_child = [&](int64_t tablet_id) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(1);
        meta->set_next_rowset_id(3);
        set_primary_key_schema(meta.get(), 1001);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename("shared_seg.dat");
        segment->set_size(100);
        segment->set_num_rows(10);
        segment->set_segment_idx(0);
        segment->set_shared(true);
        stamp_physical_identity_uid(rowset, "shared_seg.dat");
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename("shared_sst.sst");
        sst->set_filesize(512);
        sst->set_shared(true);
        sst->set_shared_rssid(1);
        sst->set_shared_version(1);
        sst->set_generation_version(7);
        sst->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 99);
        return meta;
    };
    auto status = expect_merge_rejected_without_writes({make_child(next_id()), make_child(next_id())}, next_id(),
                                                       /*target_version=*/2);
    EXPECT_TRUE(status.message().contains("still holds shared data files")) << status;
}

// The merged tablet's async vector-index build watermark must be the MIN over all
// merge sources: the merged tablet contains rowsets from every source, so a rowset is
// only guaranteed built if it was built in its OWN source. source[0] here carries the
// HIGHER watermark (100); buggy code that just CopyFrom's source[0] would inherit 100
// and wrongly skip building child_b's unbuilt tail (whose true watermark is only 50).
TEST_F(LakeTabletMergerTest, test_tablet_merge_vector_index_built_version_min) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_child = [&](int64_t tablet_id, int64_t built_version) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(3);
        set_primary_key_schema(meta.get(), 1001);
        meta->set_vector_index_built_version(built_version);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        // Each source owns its segment and mints its own rowset uid.
        auto* sm = rowset->add_segment_metas();
        sm->set_filename(fmt::format("private_seg_{}.dat", tablet_id));
        sm->set_size(100);
        sm->set_num_rows(10);
        return meta;
    };

    // source[0] has the HIGHER watermark; buggy code would inherit 100 and skip child_b's unbuilt tail.
    auto meta_a = make_child(child_a, 100);
    auto meta_b = make_child(child_b, 50);
    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(child_a);
    merging_tablet.add_old_tablet_ids(child_b);
    merging_tablet.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto it = tablet_metadatas.find(merged_tablet);
    ASSERT_TRUE(it != tablet_metadatas.end());
    const auto& merged = it->second;
    ASSERT_TRUE(merged->has_vector_index_built_version());
    EXPECT_EQ(50, merged->vector_index_built_version());
}

// Mixed: one source sets the watermark, the other never calls set_vector_index_built_version
// (field absent). A source without the field guarantees nothing is built in it, so it
// contributes 0 to the min -- the merged field must be present and 0, not the other
// source's 100.
TEST_F(LakeTabletMergerTest, test_tablet_merge_vector_index_built_version_mixed) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_child = [&](int64_t tablet_id, bool set_built_version, int64_t built_version) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(3);
        set_primary_key_schema(meta.get(), 1001);
        if (set_built_version) {
            meta->set_vector_index_built_version(built_version);
        }
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        // Each source owns its segment and mints its own rowset uid.
        auto* sm = rowset->add_segment_metas();
        sm->set_filename(fmt::format("private_seg_{}.dat", tablet_id));
        sm->set_size(100);
        sm->set_num_rows(10);
        return meta;
    };

    auto meta_a = make_child(child_a, /*set_built_version=*/true, 100);
    auto meta_b = make_child(child_b, /*set_built_version=*/false, 0);
    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(child_a);
    merging_tablet.add_old_tablet_ids(child_b);
    merging_tablet.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto it = tablet_metadatas.find(merged_tablet);
    ASSERT_TRUE(it != tablet_metadatas.end());
    const auto& merged = it->second;
    ASSERT_TRUE(merged->has_vector_index_built_version());
    EXPECT_EQ(0, merged->vector_index_built_version());
}

// None: no source ever sets the watermark. The merged tablet must leave it unset too
// (has_vector_index_built_version() false), not default it to 0.
TEST_F(LakeTabletMergerTest, test_tablet_merge_vector_index_built_version_none) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_child = [&](int64_t tablet_id) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(3);
        set_primary_key_schema(meta.get(), 1001);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        // Each source owns its segment and mints its own rowset uid.
        auto* sm = rowset->add_segment_metas();
        sm->set_filename(fmt::format("private_seg_{}.dat", tablet_id));
        sm->set_size(100);
        sm->set_num_rows(10);
        return meta;
    };

    auto meta_a = make_child(child_a);
    auto meta_b = make_child(child_b);
    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(child_a);
    merging_tablet.add_old_tablet_ids(child_b);
    merging_tablet.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto it = tablet_metadatas.find(merged_tablet);
    ASSERT_TRUE(it != tablet_metadatas.end());
    const auto& merged = it->second;
    EXPECT_FALSE(merged->has_vector_index_built_version());
}

// A split's per-child ownership hands different segments of one parent rowset to different children, each
// private, so two sources can carry the same uid over disjoint segment indices. The merge must keep every
// segment exactly once and conserve the rows, whether it folds the two occurrences into one rowset or emits
// one rowset per source.
TEST_F(LakeTabletMergerTest, test_tablet_merge_same_uid_disjoint_segments_keep_every_segment) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_child = [&](int64_t tablet_id, const std::string& segment_name, uint32_t segment_idx) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(3);
        meta->mutable_schema()->set_keys_type(DUP_KEYS);
        meta->mutable_schema()->set_id(1001);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        auto* sm = rowset->add_segment_metas();
        sm->set_filename(segment_name);
        sm->set_size(100);
        sm->set_num_rows(10);
        sm->set_segment_idx(segment_idx);
        rowset->mutable_uid()->set_hi(0);
        rowset->mutable_uid()->set_lo(2024);
        return meta;
    };
    ASSERT_OK(
            put_merge_sources(make_child(child_a, "parent_seg_0.dat", 0), make_child(child_b, "parent_seg_1.dat", 1)));

    ReshardingTabletInfoPB resharding;
    auto& merging = *resharding.mutable_merging_tablet_info();
    merging.add_old_tablet_ids(child_a);
    merging.add_old_tablet_ids(child_b);
    merging.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, base_version, new_version, txn_info,
                                              false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    std::multiset<std::string> segments;
    int64_t num_rows = 0;
    for (const auto& rowset : merged->rowsets()) {
        num_rows += rowset.num_rows();
        for (const auto& segment : rowset.segment_metas()) segments.insert(segment.filename());
    }
    EXPECT_EQ((std::multiset<std::string>{"parent_seg_0.dat", "parent_seg_1.dat"}), segments);
    EXPECT_EQ(20, num_rows);
}

// Verify a merge carries num_dels alongside num_rows / data_size: the merged tablet's statistics must be the sum
// over its sources. A merge that kept only one source's num_dels would lose the other's deletes and make
// get_tablet_stats over-report live rows after a merge-back.
TEST_F(LakeTabletMergerTest, test_tablet_merging_accumulates_num_dels) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_child = [&](int64_t tablet_id, int64_t num_rows, int64_t data_size, int64_t num_dels) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(3);
        set_primary_key_schema(meta.get(), 1001);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(num_rows);
        rowset->set_data_size(data_size);
        rowset->set_num_dels(num_dels);
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename(fmt::format("private_seg_{}.dat", tablet_id));
            sm->set_size(100);
            sm->set_num_rows(10);
        }
        return meta;
    };

    // The parent's 10 rows / 6 dels / 100 bytes, held as A 4/3/40 and B 6/3/60.
    auto meta_a = make_child(child_a, /*num_rows=*/4, /*data_size=*/40, /*num_dels=*/3);
    auto meta_b = make_child(child_b, /*num_rows=*/6, /*data_size=*/60, /*num_dels=*/3);

    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(child_a);
    merging_tablet.add_old_tablet_ids(child_b);
    merging_tablet.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    int64_t num_rows = 0;
    int64_t data_size = 0;
    int64_t num_dels = 0;
    for (const auto& rowset : merged->rowsets()) {
        num_rows += rowset.num_rows();
        data_size += rowset.data_size();
        num_dels += rowset.num_dels();
    }
    EXPECT_EQ(10, num_rows);
    EXPECT_EQ(100, data_size);
    EXPECT_EQ(6, num_dels);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_predicate_dedup) {
    // Both children have the same predicate version from split.
    // Only one should be kept in the output.
    const int64_t base_version = 2;
    const int64_t new_version = 3;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    TabletMetadataPB meta_a;
    meta_a.set_id(child_a);
    meta_a.set_version(base_version);
    meta_a.set_next_rowset_id(3);
    add_rowset_with_predicate(&meta_a, 1, 5, true);  // predicate v5
    add_rowset_with_predicate(&meta_a, 2, 6, false); // data v6

    TabletMetadataPB meta_b;
    meta_b.set_id(child_b);
    meta_b.set_version(base_version);
    meta_b.set_next_rowset_id(3);
    add_rowset_with_predicate(&meta_b, 1, 5, true);  // same predicate v5
    add_rowset_with_predicate(&meta_b, 2, 6, false); // data v6
    EXPECT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    // 1 predicate (deduped) + 2 data = 3 rowsets
    ASSERT_EQ(3, merged->rowsets_size());

    int predicate_count = 0;
    for (const auto& rowset : merged->rowsets()) {
        if (rowset.has_delete_predicate()) {
            predicate_count++;
            EXPECT_EQ(5, rowset.version());
        }
    }
    EXPECT_EQ(1, predicate_count);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_delvec_selects_only_final_single_source_files) {
    constexpr int64_t kNewVersion = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    for (int64_t tablet_id : {child_a, child_b, merged_tablet}) {
        prepare_tablet_dirs(tablet_id);
    }

    const std::string segment_a = "consumer_segment_a.dat";
    auto meta_a = make_allocator_source(child_a, /*next_rowset_id=*/2);
    add_allocator_rowset(meta_a.get(), /*rowset_id=*/1, /*version=*/1, segment_a);
    auto meta_b = make_allocator_source(child_b, /*next_rowset_id=*/2);
    add_allocator_rowset(meta_b.get(), /*rowset_id=*/1, /*version=*/1, "consumer_segment_b.dat");

    DelVector live_delvec;
    const uint32_t live_deleted_rowids[] = {3, 9};
    live_delvec.init(/*version=*/10, live_deleted_rowids, std::size(live_deleted_rowids));
    const std::string live_content = live_delvec.save();
    const std::string live_filename = "consumer_live.delvec";
    add_delvec(meta_a.get(), child_a, /*version=*/10, /*segment_id=*/1, live_filename, live_content);
    const uint32_t live_crc = crc32c::Mask(crc32c::Value(live_content.data(), live_content.size()));
    auto& live_page = (*meta_a->mutable_delvec_meta()->mutable_delvecs())[1];
    live_page.set_crc32c(live_crc);
    live_page.set_crc32c_gen_version(10);

    const std::string stale_content = "stale-delvec-record-with-no-page";
    FileMetaPB stale_file;
    stale_file.set_name("consumer_stale.delvec");
    stale_file.set_size(stale_content.size());
    (*meta_a->mutable_delvec_meta()->mutable_version_to_file())[20] = stale_file;
    write_file(_tablet_manager->delvec_location(child_a, stale_file.name()), stale_content);

    ASSIGN_OR_ABORT(auto delvecs_before, delvec_inventory(merged_tablet));
    ASSIGN_OR_ABORT(auto merged, merge_delvec_sources({meta_a, meta_b}, merged_tablet, kNewVersion));
    ASSERT_EQ(2, merged->rowsets_size());
    auto rowset_a = std::find_if(merged->rowsets().begin(), merged->rowsets().end(),
                                 [&](const auto& rowset) { return rowset.segment_metas(0).filename() == segment_a; });
    ASSERT_NE(merged->rowsets().end(), rowset_a);
    const uint32_t target_rssid = rowset_a->id();
    expect_exact_delvec_output(*merged, merged_tablet, kNewVersion, {{target_rssid, live_content, live_crc}});

    // The one live page lands in one new file; the stale version_to_file record is neither copied nor rewritten.
    ASSIGN_OR_ABORT(auto delvecs_after, delvec_inventory(merged_tablet));
    std::set<std::string> written;
    std::set_difference(delvecs_after.begin(), delvecs_after.end(), delvecs_before.begin(), delvecs_before.end(),
                        std::inserter(written, written.end()));
    EXPECT_EQ(std::set<std::string>({merged->delvec_meta().version_to_file().at(kNewVersion).name()}), written);

    DelVector loaded;
    LakeIOOptions io_options;
    ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), *merged, target_rssid, false, io_options, &loaded));
    ASSERT_NE(nullptr, loaded.roaring());
    EXPECT_EQ(2, loaded.cardinality());
    EXPECT_TRUE(loaded.roaring()->contains(3));
    EXPECT_TRUE(loaded.roaring()->contains(9));
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_delvec_compacts_only_live_page_across_remerge) {
    constexpr int64_t kNewVersion = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    for (int64_t tablet_id : {child_a, child_b, merged_tablet}) prepare_tablet_dirs(tablet_id);

    const std::string segment_a = "compact-live-page-a.dat";
    auto meta_a = make_allocator_source(child_a, /*next_rowset_id=*/2);
    add_allocator_rowset(meta_a.get(), /*rowset_id=*/1, /*version=*/1, segment_a);
    auto meta_b = make_allocator_source(child_b, /*next_rowset_id=*/2);
    add_allocator_rowset(meta_b.get(), /*rowset_id=*/1, /*version=*/1, "compact-live-page-b.dat");
    DelVector live;
    const uint32_t deleted[] = {3, 9};
    live.init(/*version=*/10, deleted, std::size(deleted));
    const std::string live_bytes = live.save();
    const std::string dead_prefix = make_large_serialized_delvec(/*version=*/7, 2 * 1024 * 1024);
    const std::string dead_suffix = make_large_serialized_delvec(/*version=*/8, 2 * 1024 * 1024);
    DelVector unrelated;
    const uint32_t unrelated_deleted = 42;
    unrelated.init(/*version=*/9, &unrelated_deleted, 1);
    const std::string unrelated_bytes = unrelated.save();
    const std::string filename = "compact-live-page.delvec";
    const std::string object = dead_prefix + live_bytes + dead_suffix + unrelated_bytes;
    FileMetaPB file;
    file.set_name(filename);
    file.set_size(object.size());
    (*meta_a->mutable_delvec_meta()->mutable_version_to_file())[10] = file;
    DelvecPagePB page;
    page.set_version(10);
    page.set_offset(dead_prefix.size());
    page.set_size(live_bytes.size());
    page.set_crc32c(crc32c::Mask(crc32c::Value(live_bytes.data(), live_bytes.size())));
    page.set_crc32c_gen_version(10);
    (*meta_a->mutable_delvec_meta()->mutable_delvecs())[1] = page;
    write_file(_tablet_manager->delvec_location(child_a, filename), object);

    // The rssid a tablet assigns to the rowset holding |segment_name|.
    auto rssid_of = [](const TabletMetadataPB& metadata, const std::string& segment_name) {
        auto rowset = std::find_if(metadata.rowsets().begin(), metadata.rowsets().end(), [&](const auto& candidate) {
            return candidate.segment_metas(0).filename() == segment_name;
        });
        EXPECT_NE(metadata.rowsets().end(), rowset) << segment_name;
        return rowset == metadata.rowsets().end() ? 0 : rowset->id();
    };

    ASSIGN_OR_ABORT(auto merged, merge_delvec_sources({meta_a, meta_b}, merged_tablet, kNewVersion));
    ASSERT_EQ(2, merged->rowsets_size());
    const uint32_t target_rssid = rssid_of(*merged, segment_a);
    ASSERT_NE(0, target_rssid);
    ASSERT_EQ(1, merged->delvec_meta().delvecs_size());
    const auto& output_file = merged->delvec_meta().version_to_file().at(kNewVersion);
    const auto& output_page = merged->delvec_meta().delvecs().at(target_rssid);
    EXPECT_EQ(static_cast<int64_t>(live_bytes.size()), output_file.size());
    EXPECT_EQ(0, output_page.offset());
    EXPECT_EQ(live_bytes.size(), output_page.size());
    EXPECT_TRUE(output_page.has_crc32c());
    DelVector loaded;
    LakeIOOptions io_options;
    ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), *merged, target_rssid, false, io_options, &loaded));
    EXPECT_EQ(live_bytes, loaded.save());

    // Feed the compact output through the real SPLIT then MERGE publish path. The
    // second output must stay page-sized; a whole-object copy would reintroduce
    // the dead prefix/suffix on every remerge. Each rowset is laid out wholly inside
    // one child, so the split hands each child a privately owned segment.
    auto set_range = [&](TabletRangePB* range, int lower, int upper) {
        range->mutable_lower_bound()->CopyFrom(generate_sort_key(lower));
        range->set_lower_bound_included(true);
        range->mutable_upper_bound()->CopyFrom(generate_sort_key(upper));
        range->set_upper_bound_included(false);
    };
    auto split_parent = std::make_shared<TabletMetadataPB>(*merged);
    split_parent->clear_schema();
    set_two_column_pk_schema(split_parent.get(), /*schema_id=*/1001);
    set_range(split_parent->mutable_range(), 0, 100);
    for (auto& split_rowset : *split_parent->mutable_rowsets()) {
        ASSERT_EQ(1, split_rowset.segment_metas_size());
        const int lower = split_rowset.segment_metas(0).filename() == segment_a ? 0 : 50;
        split_rowset.set_num_rows(50);
        set_range(split_rowset.mutable_range(), lower, lower + 50);
        auto* split_segment = split_rowset.mutable_segment_metas(0);
        split_segment->set_num_rows(50);
        split_segment->mutable_sort_key_min()->CopyFrom(generate_sort_key(lower));
        split_segment->mutable_sort_key_max()->CopyFrom(generate_sort_key(lower + 49));
    }
    ASSERT_OK(put_tablet_metadata(split_parent));
    const int64_t split_left = next_id();
    const int64_t split_right = next_id();
    const int64_t remerged_tablet = next_id();
    for (int64_t tablet_id : {split_left, split_right, remerged_tablet}) prepare_tablet_dirs(tablet_id);
    ReshardingTabletInfoPB split_info;
    auto& splitting = *split_info.mutable_splitting_tablet_info();
    splitting.set_old_tablet_id(merged_tablet);
    splitting.add_new_tablet_ids(split_left);
    splitting.add_new_tablet_ids(split_right);
    set_range(splitting.add_new_tablet_ranges(), 0, 50);
    set_range(splitting.add_new_tablet_ranges(), 50, 100);
    TxnInfoPB split_txn;
    split_txn.set_txn_id(next_id());
    split_txn.set_commit_time(1);
    split_txn.set_gtid(1);
    std::unordered_map<int64_t, TabletMetadataPtr> split_published;
    std::unordered_map<int64_t, TabletRangePB> split_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), split_info, kNewVersion, kNewVersion + 1,
                                              split_txn, false, split_published, split_ranges));
    ASSERT_TRUE(split_published.contains(split_left));
    ASSERT_TRUE(split_published.contains(split_right));
    std::unordered_map<int64_t, TabletMetadataPtr> remerge_published;
    ASSERT_OK(publish_resharding_merge({split_published.at(split_left), split_published.at(split_right)},
                                       remerged_tablet, kNewVersion + 1, kNewVersion + 2, next_id(),
                                       remerge_published));
    const auto& remerged = remerge_published.at(remerged_tablet);
    ASSERT_EQ(1, remerged->delvec_meta().version_to_file_size());
    const auto& remerged_file = remerged->delvec_meta().version_to_file().at(kNewVersion + 2);
    EXPECT_EQ(static_cast<int64_t>(live_bytes.size()), remerged_file.size());
    const uint32_t remerged_rssid = rssid_of(*remerged, segment_a);
    ASSERT_NE(0, remerged_rssid);
    const auto& remerged_page = remerged->delvec_meta().delvecs().at(remerged_rssid);
    EXPECT_EQ(0, remerged_page.offset());
    EXPECT_EQ(live_bytes.size(), remerged_page.size());
    DelVector reloaded;
    ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), *remerged, remerged_rssid, false, io_options, &reloaded));
    EXPECT_EQ(live_bytes, reloaded.save());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_delvec_output_follows_target_rssid_order) {
    constexpr int64_t kNewVersion = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    for (int64_t tablet_id : {child_a, child_b, merged_tablet}) prepare_tablet_dirs(tablet_id);
    const std::string segment_a = "ordered-a-0.dat";
    const std::string segment_b = "ordered-b-0.dat";
    auto meta_a = make_allocator_source(child_a, /*next_rowset_id=*/3);
    auto* rowset_a = add_allocator_rowset(meta_a.get(), /*rowset_id=*/1, /*version=*/1, segment_a);
    add_allocator_segment(rowset_a, "ordered-a-1.dat", /*segment_idx=*/1);
    auto meta_b = make_allocator_source(child_b, /*next_rowset_id=*/2);
    add_allocator_rowset(meta_b.get(), /*rowset_id=*/1, /*version=*/1, segment_b);

    // Every source page carries a checksum that is live at its version, so each output page's checksum is
    // determined whether a merge inherits it or recomputes it.
    auto add_checked_delvec = [&](TabletMetadataPB* metadata, int64_t tablet_id, int64_t version, uint32_t segment_id,
                                  const std::string& file_name, uint32_t deleted_rowid) {
        DelVector delvec;
        delvec.init(version, &deleted_rowid, 1);
        const std::string bytes = delvec.save();
        add_delvec(metadata, tablet_id, version, segment_id, file_name, bytes);
        auto& page = (*metadata->mutable_delvec_meta()->mutable_delvecs())[segment_id];
        page.set_crc32c(crc32c::Mask(crc32c::Value(bytes.data(), bytes.size())));
        page.set_crc32c_gen_version(version);
        return ExpectedDelvecOutputPage{0, bytes, page.crc32c()};
    };
    auto lower_a = add_checked_delvec(meta_a.get(), child_a, /*version=*/10, /*segment_id=*/1, "ordered-lower-a.delvec",
                                      /*deleted_rowid=*/1);
    auto higher_a = add_checked_delvec(meta_a.get(), child_a, /*version=*/12, /*segment_id=*/2,
                                       "ordered-higher-a.delvec", /*deleted_rowid=*/7);
    auto only_b = add_checked_delvec(meta_b.get(), child_b, /*version=*/11, /*segment_id=*/1, "ordered-b.delvec",
                                     /*deleted_rowid=*/2);

    ASSIGN_OR_ABORT(auto delvecs_before, delvec_inventory(merged_tablet));
    ASSIGN_OR_ABORT(auto merged, merge_delvec_sources({meta_a, meta_b}, merged_tablet, kNewVersion));
    ASSERT_EQ(2, merged->rowsets_size());
    auto find_rowset = [&](const std::string& segment_name) {
        return std::find_if(merged->rowsets().begin(), merged->rowsets().end(),
                            [&](const auto& rowset) { return rowset.segment_metas(0).filename() == segment_name; });
    };
    auto merged_a = find_rowset(segment_a);
    auto merged_b = find_rowset(segment_b);
    ASSERT_NE(merged->rowsets().end(), merged_a);
    ASSERT_NE(merged->rowsets().end(), merged_b);
    ASSERT_EQ(2, merged_a->segment_metas_size());
    lower_a.rssid = merged_a->id() + lake::get_segment_idx(*merged_a, 0);
    higher_a.rssid = merged_a->id() + lake::get_segment_idx(*merged_a, 1);
    only_b.rssid = merged_b->id() + lake::get_segment_idx(*merged_b, 0);

    // One output file holding every live page, laid out in target rssid order however the rssids were assigned.
    std::vector<ExpectedDelvecOutputPage> expected_pages = {lower_a, higher_a, only_b};
    std::sort(expected_pages.begin(), expected_pages.end(),
              [](const auto& left, const auto& right) { return left.rssid < right.rssid; });
    expect_exact_delvec_output(*merged, merged_tablet, kNewVersion, expected_pages);
    ASSIGN_OR_ABORT(auto delvecs_after, delvec_inventory(merged_tablet));
    std::set<std::string> written;
    std::set_difference(delvecs_after.begin(), delvecs_after.end(), delvecs_before.begin(), delvecs_before.end(),
                        std::inserter(written, written.end()));
    EXPECT_EQ(std::set<std::string>({merged->delvec_meta().version_to_file().at(kNewVersion).name()}), written);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_delvec_io_is_bounded) {
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    for (int64_t tablet_id : {child_a, child_b, merged_tablet}) prepare_tablet_dirs(tablet_id);
    auto meta_a = make_allocator_source(child_a, /*next_rowset_id=*/2);
    add_allocator_rowset(meta_a.get(), /*rowset_id=*/1, /*version=*/1, "bounded-io-a.dat");
    auto meta_b = make_allocator_source(child_b, /*next_rowset_id=*/2);
    add_allocator_rowset(meta_b.get(), /*rowset_id=*/1, /*version=*/1, "bounded-io-b.dat");
    const std::string payload = make_large_serialized_delvec(/*version=*/10, 3 * (1UL << 20));
    DelVector dead_prefix_page;
    const uint32_t dead_prefix_deleted = 17;
    dead_prefix_page.init(/*version=*/8, &dead_prefix_deleted, 1);
    const std::string dead_prefix = dead_prefix_page.save();
    DelVector dead_suffix_page;
    const uint32_t dead_suffix_deleted = 19;
    dead_suffix_page.init(/*version=*/9, &dead_suffix_deleted, 1);
    const std::string dead_suffix = dead_suffix_page.save();
    FileMetaPB file;
    file.set_name("bounded-io.delvec");
    file.set_size(dead_prefix.size() + payload.size() + dead_suffix.size());
    (*meta_a->mutable_delvec_meta()->mutable_version_to_file())[10] = file;
    DelvecPagePB page;
    page.set_version(10);
    page.set_offset(dead_prefix.size());
    page.set_size(payload.size());
    (*meta_a->mutable_delvec_meta()->mutable_delvecs())[1] = page;
    write_file(_tablet_manager->delvec_location(child_a, file.name()), dead_prefix + payload + dead_suffix);

    size_t total_read = 0;
    size_t total_append = 0;
    int active_readers = 0;
    int peak_readers = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("write_compacted_delvec_pages:read_chunk_size", [&](void* arg) {
        const size_t size = *static_cast<size_t*>(arg);
        EXPECT_LE(size, 1UL << 20);
        total_read += size;
    });
    sync->SetCallBack("append_delvec_bytes_bounded:chunk_size", [&](void* arg) {
        const size_t size = *static_cast<size_t*>(arg);
        EXPECT_LE(size, 1UL << 20);
        total_append += size;
    });
    sync->SetCallBack("write_compacted_delvec_pages:source_options", [&](void* arg) {
        EXPECT_TRUE(static_cast<RandomAccessFileOptions*>(arg)->skip_fill_local_cache);
    });
    sync->SetCallBack("write_compacted_delvec_pages:copy_source_reader_delta", [&](void* arg) {
        active_readers += *static_cast<int*>(arg);
        peak_readers = std::max(peak_readers, active_readers);
    });
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
    });
    ASSERT_OK(merge_delvec_sources({meta_a, meta_b}, merged_tablet, /*new_version=*/2));
    EXPECT_EQ(payload.size(), total_read);
    EXPECT_EQ(payload.size(), total_append);
    EXPECT_EQ(1, peak_readers);
    EXPECT_EQ(0, active_readers);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_delvec_crc_contract) {
    auto merge_raw = [&](bool trusted) {
        const int64_t child_a = next_id();
        const int64_t child_b = next_id();
        const int64_t merged_tablet = next_id();
        for (int64_t tablet_id : {child_a, child_b, merged_tablet}) prepare_tablet_dirs(tablet_id);
        const std::string segment_a = "crc-raw-a.dat";
        auto meta_a = make_allocator_source(child_a, /*next_rowset_id=*/2);
        add_allocator_rowset(meta_a.get(), /*rowset_id=*/1, /*version=*/1, segment_a);
        auto meta_b = make_allocator_source(child_b, /*next_rowset_id=*/2);
        add_allocator_rowset(meta_b.get(), /*rowset_id=*/1, /*version=*/1, "crc-raw-b.dat");
        DelVector delvec;
        const uint32_t deleted = 3;
        delvec.init(/*version=*/10, &deleted, 1);
        const std::string bytes = delvec.save();
        add_delvec(meta_a.get(), child_a, 10, 1, trusted ? "crc-trusted.delvec" : "crc-untrusted.delvec", bytes);
        auto& page = (*meta_a->mutable_delvec_meta()->mutable_delvecs())[1];
        const uint32_t expected_crc = crc32c::Mask(crc32c::Value(bytes.data(), bytes.size()));
        // A checksum whose generation is not the page's version is not live, so it proves nothing about the bytes:
        // make the untrusted one wrong to catch a merge that carries it over anyway.
        page.set_crc32c(trusted ? expected_crc : expected_crc + 1);
        page.set_crc32c_gen_version(trusted ? 10 : 9);
        ASSIGN_OR_ABORT(auto merged, merge_delvec_sources({meta_a, meta_b}, merged_tablet, /*new_version=*/2));
        for (const auto& rowset : merged->rowsets()) {
            if (rowset.segment_metas(0).filename() == segment_a) {
                return std::make_pair(merged->delvec_meta().delvecs().at(rowset.id()), expected_crc);
            }
        }
        ADD_FAILURE() << "the source rowset is missing from the merged tablet";
        return std::make_pair(DelvecPagePB(), expected_crc);
    };
    const auto trusted = merge_raw(true);
    EXPECT_TRUE(trusted.first.has_crc32c());
    EXPECT_EQ(trusted.second, trusted.first.crc32c());
    EXPECT_EQ(2, trusted.first.version());
    EXPECT_EQ(2, trusted.first.crc32c_gen_version());
    const auto untrusted = merge_raw(false);
    EXPECT_EQ(2, untrusted.first.version());
    // An untrusted source checksum is dropped or recomputed, never inherited.
    if (untrusted.first.has_crc32c()) {
        EXPECT_EQ(untrusted.second, untrusted.first.crc32c());
        EXPECT_EQ(2, untrusted.first.crc32c_gen_version());
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_delvec_duplicate_source_metadata_mismatch_is_rejected) {
    struct MetadataMismatch {
        const char* name;
        std::function<void(FileMetaPB*)> apply;
    };
    const std::vector<MetadataMismatch> mismatches = {
            {"size", [](FileMetaPB* file) { file->set_size(file->size() + 1); }},
            {"shared", [](FileMetaPB* file) { file->set_shared(true); }},
    };

    for (const auto& mismatch : mismatches) {
        for (bool mismatch_first : {false, true}) {
            SCOPED_TRACE(fmt::format("field={}, order={}", mismatch.name,
                                     mismatch_first ? "mismatch-first" : "baseline-first"));
            const int64_t baseline_tablet = next_id();
            const int64_t mismatch_tablet = next_id();
            const int64_t merged_tablet = next_id();

            // Each source owns its own segment, but their delete vectors live in one physical file -- the way a
            // split hands its parent's delvec file to every child -- so the two declarations of it must agree.
            const std::string delvec_filename = fmt::format("duplicate_metadata_{}_{}.delvec", mismatch.name,
                                                            mismatch_first ? "mismatch_first" : "baseline_first");
            auto baseline = make_allocator_source(baseline_tablet, /*next_rowset_id=*/2);
            add_allocator_rowset(baseline.get(), /*rowset_id=*/1, /*version=*/1,
                                 fmt::format("duplicate_metadata_{}_baseline.dat", mismatch.name));
            auto conflicting = make_allocator_source(mismatch_tablet, /*next_rowset_id=*/2);
            add_allocator_rowset(conflicting.get(), /*rowset_id=*/1, /*version=*/1,
                                 fmt::format("duplicate_metadata_{}_conflicting.dat", mismatch.name));

            DelVector delvec;
            const uint32_t deleted_rowids[] = {1, 4};
            delvec.init(/*version=*/10, deleted_rowids, std::size(deleted_rowids));
            const std::string content = delvec.save();
            add_delvec(baseline.get(), baseline_tablet, /*version=*/10, /*segment_id=*/1, delvec_filename, content);
            add_delvec(conflicting.get(), mismatch_tablet, /*version=*/10, /*segment_id=*/1, delvec_filename, content);
            mismatch.apply(&(*conflicting->mutable_delvec_meta()->mutable_version_to_file())[/*version=*/10]);

            const std::vector<TabletMetadataPtr> sources =
                    mismatch_first ? std::vector<TabletMetadataPtr>{conflicting, baseline}
                                   : std::vector<TabletMetadataPtr>{baseline, conflicting};
            auto status = expect_merge_rejected_without_writes(sources, merged_tablet, /*target_version=*/2);
            EXPECT_TRUE(status.message().contains("metadata mismatch")) << status;
            EXPECT_TRUE(status.message().contains(delvec_filename)) << status;
        }
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_encrypted_delvec_input_rejected_before_read) {
    for (bool stale_first : {false, true}) {
        SCOPED_TRACE(stale_first ? "stale-first" : "plain-first");
        const int64_t plain_tablet = next_id();
        const int64_t stale_tablet = next_id();
        const int64_t merged_tablet = next_id();
        for (int64_t tablet_id : {plain_tablet, stale_tablet, merged_tablet}) {
            prepare_tablet_dirs(tablet_id);
        }

        const std::string plain_delvec_filename =
                fmt::format("stale_encryption_metadata_{}_plain.delvec", stale_first ? "stale_first" : "plain_first");
        const std::string stale_delvec_filename =
                fmt::format("stale_encryption_metadata_{}_stale.delvec", stale_first ? "stale_first" : "plain_first");
        auto plain = make_allocator_source(plain_tablet, /*next_rowset_id=*/2);
        add_allocator_rowset(plain.get(), /*rowset_id=*/1, /*version=*/1, "stale_encryption_metadata_plain.dat");
        auto stale = make_allocator_source(stale_tablet, /*next_rowset_id=*/2);
        add_allocator_rowset(stale.get(), /*rowset_id=*/1, /*version=*/1, "stale_encryption_metadata_stale.dat");
        DelVector plain_delvec;
        const uint32_t plain_deleted_rowid = 1;
        plain_delvec.init(/*version=*/10, &plain_deleted_rowid, 1);
        add_delvec(plain.get(), plain_tablet, /*version=*/10, /*segment_id=*/1, plain_delvec_filename,
                   plain_delvec.save());
        DelVector stale_delvec;
        const uint32_t stale_deleted_rowid = 4;
        stale_delvec.init(/*version=*/11, &stale_deleted_rowid, 1);
        add_delvec(stale.get(), stale_tablet, /*version=*/11, /*segment_id=*/1, stale_delvec_filename,
                   stale_delvec.save());
        (*stale->mutable_delvec_meta()->mutable_version_to_file())[11].set_encryption_meta(
                std::string(1, static_cast<char>(0xff)));

        ASSERT_OK(put_merge_sources(plain, stale));
        ReshardingTabletInfoPB resharding;
        auto& merging = *resharding.mutable_merging_tablet_info();
        const std::vector<int64_t> sources = stale_first ? std::vector<int64_t>{stale_tablet, plain_tablet}
                                                         : std::vector<int64_t>{plain_tablet, stale_tablet};
        for (int64_t source : sources) {
            merging.add_old_tablet_ids(source);
        }
        merging.set_new_tablet_id(merged_tablet);
        TxnInfoPB txn_info;
        txn_info.set_txn_id(next_id());
        txn_info.set_commit_time(1);
        txn_info.set_gtid(1);
        std::unordered_map<int64_t, TabletMetadataPtr> published;
        std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
        ASSIGN_OR_ABORT(auto delvecs_before, delvec_inventory(merged_tablet));
        int source_opens = 0;
        int range_reads = 0;
        int writer_opens = 0;
        auto* sync = SyncPoint::GetInstance();
        sync->SetCallBack("write_compacted_delvec_pages:preflight_source_open", [&](void*) { ++source_opens; });
        sync->SetCallBack("write_compacted_delvec_pages:read_chunk_size", [&](void*) { ++range_reads; });
        sync->SetCallBack("write_compacted_delvec_pages:writer_open", [&](void*) { ++writer_opens; });
        sync->EnableProcessing();
        DeferOp cleanup([&] {
            sync->ClearAllCallBacks();
            sync->DisableProcessing();
        });
        const Status status =
                lake::publish_resharding_tablet(_tablet_manager.get(), resharding, /*base_version=*/1,
                                                /*new_version=*/2, txn_info, false, published, tablet_ranges);
        EXPECT_TRUE(status.is_not_supported()) << status;
        EXPECT_EQ(fmt::format("MERGE_REJECTED: encrypted delvec input is unsupported; delvec must be plaintext: {}",
                              stale_delvec_filename),
                  status.message());
        EXPECT_EQ(0, source_opens);
        EXPECT_EQ(0, range_reads);
        EXPECT_EQ(0, writer_opens);
        EXPECT_FALSE(published.contains(merged_tablet));
        ASSIGN_OR_ABORT(auto delvecs_after, delvec_inventory(merged_tablet));
        EXPECT_EQ(delvecs_before, delvecs_after);
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_ignores_non_live_encrypted_delvec_declaration) {
    constexpr int64_t kNewVersion = 2;
    const std::set<std::string> expected_orders = {"plain-first", "stale-first"};
    std::set<std::string> attempted_orders;
    std::set<std::string> completed_orders;
    std::set<std::string> observed_failure_orders;

    auto run_case = [&](bool stale_first) {
        const std::string order = stale_first ? "stale-first" : "plain-first";
        attempted_orders.insert(order);

        const int64_t plain_tablet = next_id();
        const int64_t stale_tablet = next_id();
        const int64_t target_tablet = next_id();
        for (int64_t tablet_id : {plain_tablet, stale_tablet, target_tablet}) {
            prepare_tablet_dirs(tablet_id);
        }

        const std::string plain_segment = "non_live_encrypted_plain_segment.dat";
        const std::string stale_filename = fmt::format("non_live_encrypted_{}.delvec", order);
        auto plain = make_allocator_source(plain_tablet, /*next_rowset_id=*/2);
        add_allocator_rowset(plain.get(), /*rowset_id=*/1, /*version=*/1, plain_segment);
        auto stale = make_allocator_source(stale_tablet, /*next_rowset_id=*/2);
        add_allocator_rowset(stale.get(), /*rowset_id=*/1, /*version=*/1, "non_live_encrypted_stale_segment.dat");

        DelVector live_delvec;
        const uint32_t live_rowids[] = {2, 5};
        live_delvec.init(/*version=*/10, live_rowids, std::size(live_rowids));
        const std::string live_bytes = live_delvec.save();
        add_delvec(plain.get(), plain_tablet, /*version=*/10, /*segment_id=*/1, "non_live_plain.delvec", live_bytes);
        const uint32_t live_crc = crc32c::Mask(crc32c::Value(live_bytes.data(), live_bytes.size()));
        auto& live_page = (*plain->mutable_delvec_meta()->mutable_delvecs())[1];
        live_page.set_crc32c(live_crc);
        live_page.set_crc32c_gen_version(10);

        DelVector stale_delvec;
        const uint32_t stale_rowid = 7;
        stale_delvec.init(/*version=*/11, &stale_rowid, 1);
        add_delvec(stale.get(), stale_tablet, /*version=*/11, /*segment_id=*/99, stale_filename, stale_delvec.save());
        (*stale->mutable_delvec_meta()->mutable_version_to_file())[11].set_encryption_meta("unused-non-live");
        EXPECT_FALSE(stale->rowsets(0).segment_metas().empty());
        EXPECT_EQ(1, stale->rowsets(0).id());
        EXPECT_FALSE(stale->delvec_meta().delvecs().contains(1));

        const std::string plain_pb_before = plain->SerializeAsString();
        const std::string stale_pb_before = stale->SerializeAsString();
        auto shared_inventory_before_or = delvec_inventory(target_tablet);
        ASSERT_TRUE(shared_inventory_before_or.ok()) << shared_inventory_before_or.status();
        const auto shared_inventory_before = std::move(shared_inventory_before_or).value();

        const std::vector<TabletMetadataPtr> sources = stale_first ? std::vector<TabletMetadataPtr>{stale, plain}
                                                                   : std::vector<TabletMetadataPtr>{plain, stale};
        const std::string expected_stale_diagnostic =
                fmt::format("encrypted delvec input is unsupported; delvec must be plaintext: {}", stale_filename);
        auto merged_or = merge_delvec_sources(sources, target_tablet, kNewVersion, next_id());
        if (!merged_or.ok()) {
            ADD_FAILURE() << order << ": " << merged_or.status();
            observed_failure_orders.insert(order);
            EXPECT_TRUE(merged_or.status().is_not_supported());
            EXPECT_EQ(expected_stale_diagnostic, merged_or.status().message());
            expect_target_version_not_published(target_tablet, kNewVersion);
            auto inventory_after_or = delvec_inventory(target_tablet);
            ASSERT_TRUE(inventory_after_or.ok()) << inventory_after_or.status();
            EXPECT_EQ(shared_inventory_before, *inventory_after_or);
            EXPECT_EQ(plain_pb_before, plain->SerializeAsString());
            EXPECT_EQ(stale_pb_before, stale->SerializeAsString());
            return;
        }

        const auto merged = std::move(merged_or).value();
        ASSERT_EQ(2, merged->rowsets_size());
        auto plain_rowset = std::find_if(merged->rowsets().begin(), merged->rowsets().end(), [&](const auto& rowset) {
            return rowset.segment_metas(0).filename() == plain_segment;
        });
        ASSERT_NE(merged->rowsets().end(), plain_rowset);
        const uint32_t target_rssid = plain_rowset->id();
        ASSERT_NO_FATAL_FAILURE(expect_exact_delvec_output(*merged, target_tablet, kNewVersion,
                                                           {{target_rssid, live_bytes, live_crc}}));
        EXPECT_FALSE(merged->delvec_meta().delvecs().contains(99));
        const auto& output_file = merged->delvec_meta().version_to_file().at(kNewVersion);
        EXPECT_NE(stale_filename, output_file.name());
        EXPECT_FALSE(output_file.name().contains("non_live_encrypted_"));

        DelVector loaded;
        LakeIOOptions io_options;
        ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), *merged, target_rssid, false, io_options, &loaded));
        ASSERT_NE(nullptr, loaded.roaring());
        EXPECT_EQ(2, loaded.cardinality());
        EXPECT_TRUE(loaded.roaring()->contains(2));
        EXPECT_TRUE(loaded.roaring()->contains(5));

        EXPECT_EQ(plain_pb_before, plain->SerializeAsString());
        EXPECT_EQ(stale_pb_before, stale->SerializeAsString());
        auto shared_inventory_after_or = delvec_inventory(target_tablet);
        ASSERT_TRUE(shared_inventory_after_or.ok()) << shared_inventory_after_or.status();
        const auto shared_inventory_after = std::move(shared_inventory_after_or).value();
        std::set<std::string> shared_root_delta;
        std::set_difference(shared_inventory_after.begin(), shared_inventory_after.end(),
                            shared_inventory_before.begin(), shared_inventory_before.end(),
                            std::inserter(shared_root_delta, shared_root_delta.end()));
        EXPECT_EQ(std::set<std::string>({output_file.name()}), shared_root_delta);
        completed_orders.insert(order);
    };

    run_case(/*stale_first=*/false);
    run_case(/*stale_first=*/true);

    EXPECT_EQ(expected_orders, attempted_orders);
    EXPECT_EQ(expected_orders, completed_orders);
    EXPECT_TRUE(observed_failure_orders.empty());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_delvec_zero_target_writes_nothing) {
    constexpr int64_t kNewVersion = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    for (int64_t tablet_id : {child_a, child_b, merged_tablet}) {
        prepare_tablet_dirs(tablet_id);
    }

    auto meta_a = make_allocator_source(child_a, /*next_rowset_id=*/2);
    add_allocator_rowset(meta_a.get(), /*rowset_id=*/1, /*version=*/1, "zero_target_segment_a.dat");
    auto meta_b = make_allocator_source(child_b, /*next_rowset_id=*/2);
    add_allocator_rowset(meta_b.get(), /*rowset_id=*/1, /*version=*/1, "zero_target_segment_b.dat");
    const std::string stale_content = "stale-zero-target-data";
    FileMetaPB stale_file;
    stale_file.set_name("zero_target_stale.delvec");
    stale_file.set_size(stale_content.size());
    (*meta_a->mutable_delvec_meta()->mutable_version_to_file())[17] = stale_file;
    write_file(_tablet_manager->delvec_location(child_a, stale_file.name()), stale_content);
    ASSIGN_OR_ABORT(auto delvecs_before, delvec_inventory(merged_tablet));

    ASSIGN_OR_ABORT(auto merged, merge_delvec_sources({meta_a, meta_b}, merged_tablet, kNewVersion));
    EXPECT_EQ(0, merged->delvec_meta().delvecs_size());
    EXPECT_EQ(0, merged->delvec_meta().version_to_file_size());
    ASSIGN_OR_ABORT(auto delvecs_after, delvec_inventory(merged_tablet));
    EXPECT_EQ(delvecs_before, delvecs_after) << "zero final consumers must not create a delvec file";
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_preflight_rejects_idg_invalid_shape) {
    struct InvalidShape {
        const char* name;
        std::function<void(IndexDeltaGroupEntryPB*)> apply;
    };
    const std::vector<InvalidShape> invalid_shapes = {
            {"missing filename", [](IndexDeltaGroupEntryPB* entry) { entry->clear_index_file(); }},
            {"present empty filename", [](IndexDeltaGroupEntryPB* entry) { entry->set_index_file(""); }},
            {"declared key missing col_unique_id",
             [](IndexDeltaGroupEntryPB* entry) { entry->mutable_keys(0)->clear_col_unique_id(); }},
            {"declared key missing index_type",
             [](IndexDeltaGroupEntryPB* entry) { entry->mutable_keys(0)->clear_index_type(); }},
            {"dropped key missing col_unique_id",
             [](IndexDeltaGroupEntryPB* entry) {
                 auto* key = entry->add_dropped_keys();
                 key->set_index_type(BITMAP);
             }},
            {"dropped key missing index_type",
             [](IndexDeltaGroupEntryPB* entry) {
                 auto* key = entry->add_dropped_keys();
                 key->set_col_unique_id(5);
             }},
            {"duplicate declared key",
             [](IndexDeltaGroupEntryPB* entry) { entry->add_keys()->CopyFrom(entry->keys(0)); }},
            {"present negative file_size", [](IndexDeltaGroupEntryPB* entry) { entry->set_file_size(-1); }},
    };

    for (const auto& invalid : invalid_shapes) {
        SCOPED_TRACE(invalid.name);
        auto source = make_preflight_sidecar_source(next_id(), fmt::format("idg_invalid_{}.dat", next_id()));
        add_idg_with_key(source.get(), /*segment_id=*/1, "invalid.idx", /*col_uid=*/5, BITMAP, /*version=*/1,
                         /*shared_file=*/false);
        invalid.apply(source->mutable_idg_meta()->mutable_idgs()->at(1).mutable_entries(0));
        auto status = expect_merge_rejected_without_writes({source}, next_id(), /*target_version=*/2);
        EXPECT_TRUE(status.message().contains("IDG")) << status;
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_preflight_accepts_legacy_idg_optional_shape) {
    auto source = make_preflight_sidecar_source(next_id(), "legacy_idg_optional.dat");
    auto& idg = (*source->mutable_idg_meta()->mutable_idgs())[1];
    auto* active = idg.add_entries();
    active->set_index_file("legacy_active.idx");
    auto* active_key = active->add_keys();
    active_key->set_col_unique_id(5);
    active_key->set_index_type(BITMAP);
    auto* empty = idg.add_entries();
    empty->set_index_file("legacy_empty.idx");

    const int64_t target_id = next_id();
    prepare_tablet_dirs(source->id());
    prepare_tablet_dirs(target_id);
    ASSIGN_OR_ABORT(auto merged, publish_verbatim_merge({source}, target_id, /*target_version=*/2));

    ASSERT_EQ(1, merged->idg_meta().idgs_size());
    const auto& entries = merged->idg_meta().idgs().at(1).entries();
    ASSERT_EQ(1, entries.size());
    EXPECT_EQ("legacy_active.idx", entries.Get(0).index_file());
    EXPECT_FALSE(entries.Get(0).shared_file());
    EXPECT_FALSE(entries.Get(0).has_version());
    EXPECT_FALSE(entries.Get(0).has_file_size());
    EXPECT_FALSE(entries.Get(0).has_encryption_meta());
    ASSERT_EQ(1, merged->orphan_files_size());
    EXPECT_EQ("legacy_empty.idx", merged->orphan_files(0).name());
    EXPECT_FALSE(merged->orphan_files(0).has_size());
    EXPECT_FALSE(merged->orphan_files(0).shared());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_preflight_rejects_idg_same_file_declaration_conflict) {
    ensure_kek_in_key_cache();
    ASSIGN_OR_ABORT(auto encryption_a, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
    ASSIGN_OR_ABORT(auto encryption_b, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
    struct DeclarationConflict {
        const char* name;
        std::function<void(IndexDeltaGroupEntryPB*)> apply;
    };
    const std::vector<DeclarationConflict> conflicts = {
            {"keys", [](IndexDeltaGroupEntryPB* entry) { entry->mutable_keys(0)->set_col_unique_id(50); }},
            {"key order", [](IndexDeltaGroupEntryPB* entry) { entry->mutable_keys()->SwapElements(0, 1); }},
            {"version", [](IndexDeltaGroupEntryPB* entry) { entry->set_version(2); }},
            {"file size", [](IndexDeltaGroupEntryPB* entry) { entry->set_file_size(129); }},
            {"encryption",
             [&](IndexDeltaGroupEntryPB* entry) { entry->set_encryption_meta(encryption_b.encryption_meta); }},
            {"version presence", [](IndexDeltaGroupEntryPB* entry) { entry->clear_version(); }},
            {"file size presence", [](IndexDeltaGroupEntryPB* entry) { entry->clear_file_size(); }},
            {"encryption presence", [](IndexDeltaGroupEntryPB* entry) { entry->clear_encryption_meta(); }},
            {"unknown field",
             [](IndexDeltaGroupEntryPB* entry) {
                 entry->GetReflection()->MutableUnknownFields(entry)->AddVarint(1000, 1);
             }},
    };

    for (const auto& conflict : conflicts) {
        SCOPED_TRACE(conflict.name);
        // One private source declares conflicting.idx twice on its one segment, so both declarations sit on the
        // same physical base and differ only in the mutated field.
        auto source = make_preflight_sidecar_source(next_id(), fmt::format("idg_conflict_{}.dat", next_id()));
        add_idg_with_key(source.get(), /*segment_id=*/1, "conflicting.idx", /*col_uid=*/5, BITMAP, /*version=*/1,
                         /*shared_file=*/false);
        add_idg_key(source.get(), /*segment_id=*/1, /*col_uid=*/6, GIN);
        auto& idg = source->mutable_idg_meta()->mutable_idgs()->at(1);
        idg.mutable_entries(0)->set_file_size(128);
        idg.mutable_entries(0)->set_encryption_meta(encryption_a.encryption_meta);
        const IndexDeltaGroupEntryPB declared = idg.entries(0);
        auto* redeclared = idg.add_entries();
        redeclared->CopyFrom(declared);
        conflict.apply(redeclared);

        auto status = expect_merge_rejected_without_writes({source}, next_id(), /*target_version=*/2);
        EXPECT_TRUE(status.message().contains("IDG")) << status;
        EXPECT_TRUE(status.message().contains("conflicting.idx")) << status;
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_preflight_rejects_source_live_idg_target_omission) {
    auto source = make_preflight_sidecar_source(next_id(), "live_idg_omission.dat");
    add_idg_with_key(source.get(), /*segment_id=*/1, "live_omission.idx", /*col_uid=*/5, BITMAP, /*version=*/1);
    int omission_count = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("tablet_merge_test:force_idg_target_omission", [&](void* arg) {
        ++omission_count;
        *static_cast<bool*>(arg) = true;
    });
    sync->EnableProcessing();
    DeferOp clear_omission([&] {
        sync->ClearCallBack("tablet_merge_test:force_idg_target_omission");
        sync->DisableProcessing();
    });

    auto status = expect_merge_rejected_without_writes({source}, next_id(), /*target_version=*/2);
    EXPECT_EQ(1, omission_count);
    EXPECT_TRUE(status.message().contains("IDG")) << status;
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_preflight_rejects_source_live_delvec_target_omission) {
    auto source = make_preflight_sidecar_source(next_id(), "live_delvec_omission.dat");
    DelVector delvec;
    const uint32_t deleted = 0;
    delvec.init(/*version=*/1, &deleted, 1);
    prepare_tablet_dirs(source->id());
    add_delvec(source.get(), source->id(), /*version=*/1, /*segment_id=*/1, "live_omission.delvec", delvec.save());
    int omission_count = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("tablet_merge_test:force_delvec_target_omission", [&](void* arg) {
        ++omission_count;
        *static_cast<bool*>(arg) = true;
    });
    sync->EnableProcessing();
    DeferOp clear_omission([&] {
        sync->ClearCallBack("tablet_merge_test:force_delvec_target_omission");
        sync->DisableProcessing();
    });

    auto status = expect_merge_rejected_without_writes({source}, next_id(), /*target_version=*/2);
    EXPECT_EQ(1, omission_count);
    EXPECT_TRUE(status.message().contains("delvec")) << status;
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_preflight_rejects_source_live_dcg_target_omission) {
    auto source = make_preflight_sidecar_source(next_id(), "live_dcg_omission.dat");
    add_dcg_with_columns(source.get(), /*segment_id=*/1, "live_omission.cols", {5}, /*version=*/1);
    int omission_count = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("tablet_merge_test:force_dcg_target_omission", [&](void* arg) {
        ++omission_count;
        *static_cast<bool*>(arg) = true;
    });
    sync->EnableProcessing();
    DeferOp clear_omission([&] {
        sync->ClearCallBack("tablet_merge_test:force_dcg_target_omission");
        sync->DisableProcessing();
    });

    auto status = expect_merge_rejected_without_writes({source}, next_id(), /*target_version=*/2);
    EXPECT_EQ(1, omission_count);
    EXPECT_TRUE(status.message().contains("DCG")) << status;
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_preflight_allows_source_stale_idg_and_delvec) {
    auto source = make_preflight_sidecar_source(next_id(), "stale_sidecar_source.dat");
    auto* stale_entry = (*source->mutable_idg_meta()->mutable_idgs())[2].add_entries();
    stale_entry->clear_index_file();
    DelvecPagePB stale_page;
    stale_page.set_version(999);
    stale_page.set_offset(std::numeric_limits<uint64_t>::max());
    stale_page.set_size(1);
    (*source->mutable_delvec_meta()->mutable_delvecs())[2] = stale_page;

    const int64_t target_id = next_id();
    prepare_tablet_dirs(source->id());
    prepare_tablet_dirs(target_id);
    ASSIGN_OR_ABORT(auto merged, publish_verbatim_merge({source}, target_id, /*target_version=*/2));
    EXPECT_FALSE(merged->has_idg_meta());
    EXPECT_FALSE(merged->has_delvec_meta());
    EXPECT_EQ(0, merged->orphan_files_size());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_preflight_rejects_source_stale_dcg_before_materialize) {
    auto source = make_allocator_source(next_id(), /*next_rowset_id=*/4);
    auto* rowset = add_allocator_rowset(source.get(), /*rowset_id=*/1, /*version=*/1, "stale_dcg_0.dat",
                                        /*segment_idx=*/0);
    add_allocator_segment(rowset, "stale_dcg_2.dat", /*segment_idx=*/2);
    add_dcg_with_columns(source.get(), /*segment_id=*/2, "stale.cols", {5}, /*version=*/1, /*shared_file=*/false);

    auto status = expect_merge_rejected_without_writes({source}, next_id(), /*target_version=*/2);
    EXPECT_TRUE(status.message().contains("stale DCG")) << status;
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_cross_target_different_base_sidecars_rejected) {
    auto source_a = make_preflight_sidecar_source(next_id(), "cross_target_base_a.dat");
    auto source_b = make_preflight_sidecar_source(next_id(), "cross_target_base_b.dat");
    for (auto* source : {source_a.get(), source_b.get()}) {
        add_dcg_with_columns(source, /*segment_id=*/1, "cross_target_mismatch.cols", {5}, /*version=*/1,
                             /*shared_file=*/false);
        add_idg_with_key(source, /*segment_id=*/1, "cross_target_mismatch.idx", /*col_uid=*/5, BITMAP,
                         /*version=*/1, /*shared_file=*/false);
    }

    auto status = expect_merge_rejected_without_writes({source_a, source_b}, next_id(), /*target_version=*/2);
    EXPECT_TRUE(status.message().contains("physical base")) << status;
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_cross_target_physical_slice_declaration_conflict_before_io) {
    struct DeclarationConflict {
        const char* name;
        const char* expected_error;
        std::function<void(SegmentMetadataPB*)> apply;
    };
    const std::vector<DeclarationConflict> conflicts = {
            {"bundle offset presence", "bundled and standalone",
             [](SegmentMetadataPB* segment) { segment->set_bundle_file_offset(0); }},
            {"size presence", "physical segment slice", [](SegmentMetadataPB* segment) { segment->clear_size(); }},
            {"unknown field", "physical segment slice",
             [](SegmentMetadataPB* segment) {
                 segment->GetReflection()->MutableUnknownFields(segment)->AddVarint(1000, 1);
             }},
    };

    for (const auto& conflict : conflicts) {
        for (bool reverse_sources : {false, true}) {
            SCOPED_TRACE(fmt::format("{}; {} declaration first", conflict.name,
                                     reverse_sources ? "conflicting" : "canonical"));
            const std::string filename = fmt::format("cross_target_slice_conflict_{}.dat", next_id());
            auto canonical = make_preflight_sidecar_source(next_id(), filename);
            auto conflicting = make_preflight_sidecar_source(next_id(), filename);
            canonical->mutable_rowsets(0)->mutable_segment_metas(0)->set_size(128);
            conflicting->mutable_rowsets(0)->mutable_segment_metas(0)->set_size(128);
            conflict.apply(conflicting->mutable_rowsets(0)->mutable_segment_metas(0));
            std::vector<TabletMetadataPtr> sources = {canonical, conflicting};
            if (reverse_sources) std::swap(sources[0], sources[1]);

            auto status = expect_merge_rejected_without_writes(sources, next_id(), /*target_version=*/2);
            EXPECT_TRUE(status.message().contains(conflict.expected_error)) << status;
            EXPECT_TRUE(status.message().contains(filename)) << status;
        }
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_invalid_physical_segment_shape_before_io) {
    struct InvalidShape {
        const char* name;
        bool same_filename;
        const char* error_field;
        std::function<void(SegmentMetadataPB*, SegmentMetadataPB*)> apply;
    };
    const std::vector<InvalidShape> invalid_shapes = {
            {"negative offset versus absent", true, "bundle_file_offset",
             [](SegmentMetadataPB*, SegmentMetadataPB* right) { right->set_bundle_file_offset(-1); }},
            {"negative offset versus explicit zero", true, "bundle_file_offset",
             [](SegmentMetadataPB* left, SegmentMetadataPB* right) {
                 left->set_bundle_file_offset(0);
                 left->set_size(128);
                 right->set_bundle_file_offset(-1);
             }},
            {"negative size", true, "negative statistics",
             [](SegmentMetadataPB* left, SegmentMetadataPB* right) {
                 left->set_size(-1);
                 right->set_size(-1);
             }},
            {"single bundled missing size", false, "size",
             [](SegmentMetadataPB* left, SegmentMetadataPB*) {
                 left->set_bundle_file_offset(0);
                 left->clear_size();
             }},
            {"all bundled missing size", true, "size",
             [](SegmentMetadataPB* left, SegmentMetadataPB* right) {
                 for (auto* segment : {left, right}) {
                     segment->set_bundle_file_offset(0);
                     segment->clear_size();
                 }
             }},
    };

    for (const auto& invalid : invalid_shapes) {
        for (bool reverse_sources : {false, true}) {
            SCOPED_TRACE(fmt::format("{}; {} source first", invalid.name, reverse_sources ? "right" : "left"));
            const std::string stem = fmt::format("invalid_segment_shape_{}", next_id());
            auto left = make_preflight_sidecar_source(next_id(), stem + "_left.dat");
            auto right = make_preflight_sidecar_source(next_id(), invalid.same_filename
                                                                          ? left->rowsets(0).segment_metas(0).filename()
                                                                          : stem + "_right.dat");
            invalid.apply(left->mutable_rowsets(0)->mutable_segment_metas(0),
                          right->mutable_rowsets(0)->mutable_segment_metas(0));
            std::vector<TabletMetadataPtr> sources = {left, right};
            if (reverse_sources) std::swap(sources[0], sources[1]);

            auto status = expect_merge_rejected_without_writes(sources, next_id(), /*target_version=*/2);
            EXPECT_TRUE(status.message().contains("segment")) << status;
            EXPECT_TRUE(status.message().contains(invalid.error_field)) << status;
        }
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_invalid_physical_segment_filename_before_io) {
    struct InvalidFilename {
        const char* name;
        std::function<void(SegmentMetadataPB*)> apply;
    };
    const std::vector<InvalidFilename> invalid_filenames = {
            {"missing", [](SegmentMetadataPB* segment) { segment->clear_filename(); }},
            {"present empty", [](SegmentMetadataPB* segment) { segment->set_filename(""); }},
    };

    for (const auto& invalid : invalid_filenames) {
        for (bool reverse_sources : {false, true}) {
            SCOPED_TRACE(
                    fmt::format("{} filename; {} source first", invalid.name, reverse_sources ? "valid" : "invalid"));
            auto malformed =
                    make_preflight_sidecar_source(next_id(), fmt::format("invalid_filename_{}.dat", next_id()));
            invalid.apply(malformed->mutable_rowsets(0)->mutable_segment_metas(0));
            auto valid = make_preflight_sidecar_source(next_id(), fmt::format("valid_filename_{}.dat", next_id()));
            std::vector<TabletMetadataPtr> sources = {malformed, valid};
            if (reverse_sources) std::swap(sources[0], sources[1]);

            auto status = expect_merge_rejected_without_writes(sources, next_id(), /*target_version=*/2);
            EXPECT_TRUE(status.message().contains("filename")) << status;
        }
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_bundle_slice_end_overflow_before_io) {
    for (bool reverse_sources : {false, true}) {
        SCOPED_TRACE(fmt::format("{} source first", reverse_sources ? "valid" : "overflowing"));
        auto overflowing =
                make_preflight_sidecar_source(next_id(), fmt::format("overflowing_bundle_{}.dat", next_id()));
        auto* segment = overflowing->mutable_rowsets(0)->mutable_segment_metas(0);
        segment->set_bundle_file_offset(std::numeric_limits<int64_t>::max() - 11);
        segment->set_size(12);
        auto valid = make_preflight_sidecar_source(next_id(), fmt::format("valid_bundle_peer_{}.dat", next_id()));
        std::vector<TabletMetadataPtr> sources = {overflowing, valid};
        if (reverse_sources) std::swap(sources[0], sources[1]);

        auto status = expect_merge_rejected_without_writes(sources, next_id(), /*target_version=*/2);
        EXPECT_TRUE(status.message().contains("bundle")) << status;
        EXPECT_TRUE(status.message().contains("size")) << status;
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_cross_canonical_filename_bundle_form_before_io) {
    for (bool reverse_sources : {false, true}) {
        SCOPED_TRACE(fmt::format("{} source first", reverse_sources ? "bundled" : "standalone"));
        const std::string filename = fmt::format("cross_canonical_bundle_form_{}.dat", next_id());
        auto standalone = make_preflight_sidecar_source(next_id(), filename);
        auto bundled = make_preflight_sidecar_source(next_id(), filename);
        auto* bundled_segment = bundled->mutable_rowsets(0)->mutable_segment_metas(0);
        bundled_segment->set_size(128);
        bundled_segment->set_bundle_file_offset(128);
        std::vector<TabletMetadataPtr> sources = {standalone, bundled};
        if (reverse_sources) std::swap(sources[0], sources[1]);

        auto status = expect_merge_rejected_without_writes(sources, next_id(), /*target_version=*/2);
        EXPECT_TRUE(status.message().contains(filename)) << status;
        EXPECT_TRUE(status.message().contains("bundled")) << status;
        EXPECT_TRUE(status.message().contains("standalone")) << status;
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_same_uid_overlapping_bundle_slices_before_io) {
    for (bool reverse_sources : {false, true}) {
        SCOPED_TRACE(fmt::format("{} source first", reverse_sources ? "later slice" : "earlier slice"));
        const std::string filename = fmt::format("same_uid_overlapping_bundle_{}.dat", next_id());
        auto earlier = make_preflight_sidecar_source(next_id(), filename);
        auto later = make_preflight_sidecar_source(next_id(), filename);
        auto* earlier_rowset = earlier->mutable_rowsets(0);
        auto* later_rowset = later->mutable_rowsets(0);
        earlier_rowset->mutable_segment_metas(0)->set_size(128);
        earlier_rowset->mutable_segment_metas(0)->set_bundle_file_offset(0);
        later_rowset->mutable_segment_metas(0)->set_size(128);
        later_rowset->mutable_segment_metas(0)->set_bundle_file_offset(64);
        later_rowset->mutable_segment_metas(0)->set_segment_idx(1);
        later_rowset->mutable_uid()->CopyFrom(earlier_rowset->uid());
        std::vector<TabletMetadataPtr> sources = {earlier, later};
        if (reverse_sources) std::swap(sources[0], sources[1]);

        auto status = expect_merge_rejected_without_writes(sources, next_id(), /*target_version=*/2);
        EXPECT_TRUE(status.message().contains(filename)) << status;
        EXPECT_TRUE(status.message().contains("overlapping bundled slices")) << status;
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_cross_canonical_overlapping_bundle_slices_before_io) {
    for (bool reverse_sources : {false, true}) {
        SCOPED_TRACE(fmt::format("{} source first", reverse_sources ? "later slice" : "earlier slice"));
        const std::string filename = fmt::format("cross_canonical_overlapping_bundle_{}.dat", next_id());
        auto earlier = make_preflight_sidecar_source(next_id(), filename);
        auto later = make_preflight_sidecar_source(next_id(), filename);
        earlier->mutable_rowsets(0)->mutable_segment_metas(0)->set_size(128);
        earlier->mutable_rowsets(0)->mutable_segment_metas(0)->set_bundle_file_offset(0);
        later->mutable_rowsets(0)->mutable_segment_metas(0)->set_size(128);
        later->mutable_rowsets(0)->mutable_segment_metas(0)->set_bundle_file_offset(64);
        std::vector<TabletMetadataPtr> sources = {earlier, later};
        if (reverse_sources) std::swap(sources[0], sources[1]);

        auto status = expect_merge_rejected_without_writes(sources, next_id(), /*target_version=*/2);
        EXPECT_TRUE(status.message().contains(filename)) << status;
        EXPECT_TRUE(status.message().contains("overlapping bundled slices")) << status;
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_duplicate_segment_index_within_occurrence_before_io) {
    auto source = make_preflight_sidecar_source(next_id(), fmt::format("duplicate_index_{}.dat", next_id()));
    auto* rowset = source->mutable_rowsets(0);
    auto* duplicate = rowset->add_segment_metas();
    duplicate->CopyFrom(rowset->segment_metas(0));

    auto status = expect_merge_rejected_without_writes({source}, next_id(), /*target_version=*/2);
    EXPECT_TRUE(status.message().contains("not strictly increasing")) << status;
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_duplicate_physical_slice_in_one_rowset_before_io) {
    for (bool reverse_segments : {false, true}) {
        SCOPED_TRACE(fmt::format("segment index {} first", reverse_segments ? 1 : 0));
        const std::string filename = fmt::format("duplicate_rowset_slice_{}.dat", next_id());
        auto source = make_preflight_sidecar_source(next_id(), filename);
        auto* rowset = source->mutable_rowsets(0);
        auto* duplicate = rowset->add_segment_metas();
        duplicate->CopyFrom(rowset->segment_metas(0));
        duplicate->set_segment_idx(1);
        if (reverse_segments) rowset->mutable_segment_metas()->SwapElements(0, 1);

        auto status = expect_merge_rejected_without_writes({source}, next_id(), /*target_version=*/2);
        if (reverse_segments) {
            EXPECT_TRUE(status.message().contains("not strictly increasing")) << status;
        } else {
            EXPECT_TRUE(status.message().contains(filename)) << status;
            EXPECT_TRUE(status.message().contains("multiple segment indices")) << status;
        }
    }
}

// One segment file under one uid, at segment index 0 in one source and 1 in the other: the same physical slice
// declared by two sources, refused whichever source comes first.
TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_sibling_union_duplicate_physical_slice_before_io) {
    for (bool reverse_sources : {false, true}) {
        SCOPED_TRACE(fmt::format("segment index {} source first", reverse_sources ? 1 : 0));
        const std::string filename = fmt::format("duplicate_union_slice_{}.dat", next_id());
        auto first = make_preflight_sidecar_source(next_id(), filename);
        auto second = make_preflight_sidecar_source(next_id(), filename);
        second->mutable_rowsets(0)->mutable_segment_metas(0)->set_segment_idx(1);
        second->mutable_rowsets(0)->mutable_uid()->CopyFrom(first->rowsets(0).uid());
        std::vector<TabletMetadataPtr> sources = {first, second};
        if (reverse_sources) std::swap(sources[0], sources[1]);

        auto status = expect_merge_rejected_without_writes(sources, next_id(), /*target_version=*/2);
        EXPECT_TRUE(status.message().contains(fmt::format("both declare segment file {}", filename))) << status;
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_accepts_adjacent_and_disjoint_bundled_slices) {
    for (int64_t later_offset : {int64_t{128}, int64_t{256}}) {
        for (bool reverse_sources : {false, true}) {
            SCOPED_TRACE(fmt::format("offset {}; {} source first", later_offset,
                                     reverse_sources ? "later slice" : "earlier slice"));
            const std::string filename = fmt::format("compatible_bundle_slices_{}.dat", next_id());
            auto earlier = make_preflight_sidecar_source(next_id(), filename);
            auto later = make_preflight_sidecar_source(next_id(), filename);
            earlier->mutable_rowsets(0)->mutable_segment_metas(0)->set_size(128);
            earlier->mutable_rowsets(0)->mutable_segment_metas(0)->set_bundle_file_offset(0);
            later->mutable_rowsets(0)->mutable_segment_metas(0)->set_size(128);
            later->mutable_rowsets(0)->mutable_segment_metas(0)->set_bundle_file_offset(later_offset);
            std::vector<TabletMetadataPtr> sources = {earlier, later};
            if (reverse_sources) std::swap(sources[0], sources[1]);

            const int64_t target_id = next_id();
            ASSIGN_OR_ABORT(auto files_before,
                            directory_inventory(_location_provider->segment_root_location(target_id)));
            ASSIGN_OR_ABORT(auto merged, publish_verbatim_merge(sources, target_id, /*target_version=*/2));
            ASSERT_EQ(2, merged->rowsets_size());
            // Neither source carries a DCG or a delete vector, so the merge has no data file to write.
            ASSIGN_OR_ABORT(auto files_after,
                            directory_inventory(_location_provider->segment_root_location(target_id)));
            EXPECT_EQ(files_before, files_after);
        }
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_undersized_bundle_slice_before_io) {
    for (int64_t size : {int64_t{0}, int64_t{1}, int64_t{11}}) {
        for (bool reverse_sources : {false, true}) {
            SCOPED_TRACE(fmt::format("size {}; {} source first", size,
                                     reverse_sources ? "standalone" : "undersized bundled"));
            auto undersized =
                    make_preflight_sidecar_source(next_id(), fmt::format("undersized_bundle_{}.dat", next_id()));
            auto* segment = undersized->mutable_rowsets(0)->mutable_segment_metas(0);
            segment->set_bundle_file_offset(0);
            segment->set_size(size);
            auto standalone =
                    make_preflight_sidecar_source(next_id(), fmt::format("undersized_peer_{}.dat", next_id()));
            std::vector<TabletMetadataPtr> sources = {undersized, standalone};
            if (reverse_sources) std::swap(sources[0], sources[1]);

            auto status = expect_merge_rejected_without_writes(sources, next_id(), /*target_version=*/2);
            EXPECT_TRUE(status.message().contains("footer trailer")) << status;
        }
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_preflight_rejects_delvec_page_out_of_bounds_before_io) {
    auto source = make_preflight_sidecar_source(next_id(), "delvec_bounds.dat");
    DelVector delvec;
    const uint32_t deleted = 0;
    delvec.init(/*version=*/1, &deleted, 1);
    prepare_tablet_dirs(source->id());
    add_delvec(source.get(), source->id(), /*version=*/1, /*segment_id=*/1, "bounds.delvec", delvec.save());
    auto* page = &(*source->mutable_delvec_meta()->mutable_delvecs())[1];
    page->set_offset(source->delvec_meta().version_to_file().at(1).size());
    page->set_size(1);

    auto status = expect_merge_rejected_without_writes({source}, next_id(), /*target_version=*/2);
    EXPECT_TRUE(status.message().contains("bounds.delvec")) << status;
    EXPECT_TRUE(status.message().contains("bounds")) << status;
}

TEST_F(LakeTabletMergerTest,
       test_tablet_merging_preflight_rejects_delvec_repeated_file_declaration_conflict_before_io) {
    struct FileConflict {
        const char* name;
        bool empty_page;
        std::function<void(FileMetaPB*)> apply;
    };
    const std::vector<FileConflict> conflicts = {
            {"size", false, [](FileMetaPB* file) { file->set_size(file->size() + 1); }},
            {"size presence", true, [](FileMetaPB* file) { file->clear_size(); }},
            {"shared", false, [](FileMetaPB* file) { file->set_shared(true); }},
            {"shared presence", false, [](FileMetaPB* file) { file->clear_shared(); }},
    };

    for (const auto& conflict : conflicts) {
        SCOPED_TRACE(conflict.name);
        // Each source owns its segment; only the delvec file both of them name is declared twice.
        const std::string stem = fmt::format("delvec_conflict_{}", next_id());
        const std::string filename = stem + ".delvec";
        auto source_a = make_preflight_sidecar_source(next_id(), stem + "_a.dat");
        auto source_b = make_preflight_sidecar_source(next_id(), stem + "_b.dat");
        prepare_tablet_dirs(source_a->id());
        prepare_tablet_dirs(source_b->id());
        std::string content;
        if (!conflict.empty_page) {
            DelVector delvec;
            const uint32_t deleted = 0;
            delvec.init(/*version=*/1, &deleted, 1);
            content = delvec.save();
        }
        add_delvec(source_a.get(), source_a->id(), /*version=*/1, /*segment_id=*/1, filename, content);
        add_delvec(source_b.get(), source_b->id(), /*version=*/1, /*segment_id=*/1, filename, content);
        (*source_a->mutable_delvec_meta()->mutable_version_to_file())[1].set_shared(false);
        (*source_b->mutable_delvec_meta()->mutable_version_to_file())[1].set_shared(false);
        conflict.apply(&(*source_b->mutable_delvec_meta()->mutable_version_to_file())[1]);

        auto status = expect_merge_rejected_without_writes({source_a, source_b}, next_id(), /*target_version=*/2);
        EXPECT_TRUE(status.message().contains("metadata mismatch")) << status;
        EXPECT_TRUE(status.message().contains(filename)) << status;
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_preflight_rejects_late_dcg_declaration_before_materialize) {
    auto source = make_preflight_sidecar_source(next_id(), "late_dcg_declaration.dat");
    add_dcg(source.get(), /*segment_id=*/1, "late_malformed.cols");

    auto status = expect_merge_rejected_without_writes({source}, next_id(), /*target_version=*/2);
    EXPECT_TRUE(status.message().contains("DCG shape")) << status;
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_preflight_rejects_dcg_message_unknown_fields_before_materialize) {
    for (bool reverse_sources : {false, true}) {
        SCOPED_TRACE(reverse_sources ? "unknown-field source first" : "plain source first");
        const std::string stem = fmt::format("dcg_unknown_field_{}", next_id());
        auto plain = make_preflight_sidecar_source(next_id(), stem + "_plain.dat");
        auto with_unknown = make_preflight_sidecar_source(next_id(), stem + "_unknown.dat");
        add_dcg_with_columns(plain.get(), /*segment_id=*/1, stem + "_plain.cols", {5}, /*version=*/1,
                             /*shared_file=*/false);
        add_dcg_with_columns(with_unknown.get(), /*segment_id=*/1, stem + "_unknown.cols", {5}, /*version=*/1,
                             /*shared_file=*/false);
        auto& unknown_dcg = with_unknown->mutable_dcg_meta()->mutable_dcgs()->at(1);
        unknown_dcg.GetReflection()->MutableUnknownFields(&unknown_dcg)->AddVarint(1000, 1);

        ASSERT_OK(put_tablet_metadata(plain));
        ASSERT_OK(put_tablet_metadata(with_unknown));
        ASSIGN_OR_ABORT(auto persisted_plain, _tablet_manager->get_tablet_metadata(plain->id(), plain->version()));
        ASSIGN_OR_ABORT(auto persisted_with_unknown,
                        _tablet_manager->get_tablet_metadata(with_unknown->id(), with_unknown->version()));
        const auto& persisted_dcg = persisted_with_unknown->dcg_meta().dcgs().at(1);
        ASSERT_EQ(1, persisted_dcg.GetReflection()->GetUnknownFields(persisted_dcg).field_count());

        std::vector<TabletMetadataPtr> sources = {persisted_plain, persisted_with_unknown};
        if (reverse_sources) std::swap(sources[0], sources[1]);
        auto status = expect_merge_rejected_without_writes(sources, next_id(), /*target_version=*/2);
        EXPECT_TRUE(status.message().contains("DCG message has unknown fields")) << status;
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_preflight_rejects_sst_invalid_form_or_range_before_io) {
    struct InvalidDeclaration {
        const char* name;
        std::function<void(PersistentIndexSstablePB*)> apply;
    };
    const std::vector<InvalidDeclaration> invalid_declarations = {
            {"legacy shared version without shared rssid",
             [](PersistentIndexSstablePB* sst) {
                 sst->clear_shared_rssid();
                 sst->set_shared_version(7);
             }},
            {"legacy embedded delvec without shared rssid",
             [](PersistentIndexSstablePB* sst) {
                 sst->clear_shared_rssid();
                 sst->clear_shared_version();
                 sst->mutable_delvec()->set_version(1);
                 sst->mutable_delvec()->set_size(1);
             }},
            {"modern effective rssid overflow",
             [](PersistentIndexSstablePB* sst) {
                 sst->set_shared_rssid(std::numeric_limits<uint32_t>::max());
                 sst->set_rssid_offset(1);
             }},
            {"range missing start key", [](PersistentIndexSstablePB* sst) { sst->mutable_range()->clear_start_key(); }},
            {"range missing end key", [](PersistentIndexSstablePB* sst) { sst->mutable_range()->clear_end_key(); }},
            {"reversed range",
             [](PersistentIndexSstablePB* sst) {
                 sst->mutable_range()->set_start_key("z");
                 sst->mutable_range()->set_end_key("a");
             }},
    };

    for (const auto& invalid : invalid_declarations) {
        SCOPED_TRACE(invalid.name);
        auto sources = make_preflight_sst_sources(fmt::format("sst_invalid_{}", next_id()), /*shared_files=*/false);
        for (auto& source : sources) invalid.apply(source->mutable_sstable_meta()->mutable_sstables(0));
        std::vector<TabletMetadataPtr> immutable_sources(sources.begin(), sources.end());
        auto status = expect_merge_rejected_without_writes(immutable_sources, next_id(), /*target_version=*/2);
        EXPECT_TRUE(status.message().contains("SST")) << status;
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_preflight_rejects_modern_sst_invalid_shared_version_before_io) {
    struct InvalidSharedVersion {
        const char* name;
        std::function<void(PersistentIndexSstablePB*)> apply;
    };
    const std::vector<InvalidSharedVersion> invalid_versions = {
            {"missing", [](PersistentIndexSstablePB* sst) { sst->clear_shared_version(); }},
            {"zero", [](PersistentIndexSstablePB* sst) { sst->set_shared_version(0); }},
            {"negative", [](PersistentIndexSstablePB* sst) { sst->set_shared_version(-1); }},
    };

    for (const auto& invalid : invalid_versions) {
        SCOPED_TRACE(invalid.name);
        auto sources = make_preflight_sst_sources(fmt::format("sst_invalid_shared_version_{}", next_id()),
                                                  /*shared_files=*/false);
        for (auto& source : sources) invalid.apply(source->mutable_sstable_meta()->mutable_sstables(0));
        std::vector<TabletMetadataPtr> immutable_sources(sources.begin(), sources.end());
        auto status = expect_merge_rejected_without_writes(immutable_sources, next_id(), /*target_version=*/2);
        EXPECT_TRUE(status.message().contains("SST")) << status;
        EXPECT_TRUE(status.message().contains("shared_version")) << status;
    }
}

// Two sources that declare one SST file are two owners of it, so the merge refuses them however the declarations
// compare: identical ones at admission, and ones that disagree on the file's size, encryption, key range or any other
// field as soon as the declarations are read.
TEST_F(LakeTabletMergerTest, test_tablet_merging_preflight_rejects_late_sst_file_conflict_before_io) {
    ensure_kek_in_key_cache();
    ASSIGN_OR_ABORT(auto encryption_a, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
    ASSIGN_OR_ABORT(auto encryption_b, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
    struct SstConflict {
        const char* name;
        const char* diagnostic;
        std::function<void(std::vector<std::shared_ptr<TabletMetadataPB>>&)> apply;
    };
    const std::vector<SstConflict> conflicts = {
            {"identical", "both declare SST file", [](auto&) {}},
            {"size", "conflicting physical metadata",
             [](auto& sources) {
                 auto* sst = sources[1]->mutable_sstable_meta()->mutable_sstables(0);
                 sst->set_filesize(sst->filesize() + 1);
             }},
            {"size presence", "conflicting physical metadata",
             [](auto& sources) { sources[1]->mutable_sstable_meta()->mutable_sstables(0)->clear_filesize(); }},
            {"encryption", "conflicting physical metadata",
             [&](auto& sources) {
                 sources[1]->mutable_sstable_meta()->mutable_sstables(0)->set_encryption_meta(
                         encryption_b.encryption_meta);
             }},
            {"encryption presence", "conflicting physical metadata",
             [](auto& sources) { sources[1]->mutable_sstable_meta()->mutable_sstables(0)->clear_encryption_meta(); }},
            {"range presence", "conflicting form or range",
             [](auto& sources) { sources[1]->mutable_sstable_meta()->mutable_sstables(0)->clear_range(); }},
            {"range value", "conflicting form or range",
             [&](auto& sources) {
                 sources[1]->mutable_sstable_meta()->mutable_sstables(0)->mutable_range()->set_start_key(
                         encode_int_primary_key(11));
             }},
            {"unknown field", "conflicting form or range",
             [](auto& sources) {
                 auto* sst = sources[1]->mutable_sstable_meta()->mutable_sstables(0);
                 sst->GetReflection()->MutableUnknownFields(sst)->AddVarint(1000, 1);
             }},
    };

    for (const auto& conflict : conflicts) {
        for (bool reverse_sources : {false, true}) {
            SCOPED_TRACE(fmt::format("{}; reverse={}", conflict.name, reverse_sources));
            // Each source owns its segment; the one physical SST is what both of them declare, under the same
            // name and range, so only the mutation below tells the two declarations apart.
            auto sources = make_preflight_sst_sources(fmt::format("sst_preflight_{}", next_id()),
                                                      /*shared_files=*/false);
            for (auto& source : sources) {
                auto* sst = source->mutable_sstable_meta()->mutable_sstables(0);
                sst->set_filename("repeated_physical.sst");
                sst->set_encryption_meta(encryption_a.encryption_meta);
            }
            sources[1]->mutable_sstable_meta()->mutable_sstables(0)->mutable_range()->CopyFrom(
                    sources[0]->sstable_meta().sstables(0).range());
            conflict.apply(sources);
            std::vector<TabletMetadataPtr> immutable_sources(sources.begin(), sources.end());
            if (reverse_sources) std::swap(immutable_sources[0], immutable_sources[1]);

            auto status = expect_merge_rejected_without_writes(immutable_sources, next_id(), /*target_version=*/2);
            EXPECT_TRUE(status.message().contains("repeated_physical.sst")) << status;
            EXPECT_TRUE(status.message().contains(conflict.diagnostic)) << status;
        }
    }
}

// --- DCG merge tests ---

TEST_F(LakeTabletMergerTest, test_tablet_merging_dcg_disjoint_columns) {
    // child_a updated columns {1,2} and then {3,4} of its own segment, child_b updated column {1} of its own.
    // Each DCG is projected onto its segment's target rssid unchanged: both of child_a's entries survive with
    // every per-entry field still aligned to its .cols file, and child_b's entry is not mixed into them.
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_child = [&](int64_t tablet_id, const std::string& segment_filename) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(3);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        auto* sm = rowset->add_segment_metas();
        sm->set_filename(segment_filename);
        sm->set_size(100);
        sm->set_num_rows(10);
        return meta;
    };

    auto meta_a = make_child(child_a, "dcg_a.dat");
    add_dcg_with_columns(meta_a.get(), 1, "a1.cols", {1, 2}, 1, /*shared_file=*/false);
    add_dcg_with_columns(meta_a.get(), 1, "a2.cols", {3, 4}, 1, /*shared_file=*/false);

    auto meta_b = make_child(child_b, "dcg_b.dat");
    add_dcg_with_columns(meta_b.get(), 1, "b.cols", {1}, 1, /*shared_file=*/false);

    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(2, merged->rowsets_size());
    auto target_rssid_of = [&](const std::string& segment_filename) -> std::optional<uint32_t> {
        for (const auto& rowset : merged->rowsets()) {
            for (int i = 0; i < rowset.segment_metas_size(); ++i) {
                if (rowset.segment_metas(i).filename() == segment_filename) {
                    return rowset.id() + lake::get_segment_idx(rowset, i);
                }
            }
        }
        return std::nullopt;
    };
    auto rssid_a = target_rssid_of("dcg_a.dat");
    auto rssid_b = target_rssid_of("dcg_b.dat");
    ASSERT_TRUE(rssid_a.has_value());
    ASSERT_TRUE(rssid_b.has_value());
    ASSERT_NE(*rssid_a, *rssid_b);

    ASSERT_TRUE(merged->has_dcg_meta());
    ASSERT_EQ(2, merged->dcg_meta().dcgs().size());

    auto dcg_a = merged->dcg_meta().dcgs().find(*rssid_a);
    ASSERT_TRUE(dcg_a != merged->dcg_meta().dcgs().end());
    // 2 entries: a1.cols and a2.cols, each still paired with the columns it updated.
    ASSERT_EQ(2, dcg_a->second.column_files_size());
    EXPECT_EQ(2, dcg_a->second.unique_column_ids_size());
    EXPECT_EQ(2, dcg_a->second.versions_size());
    EXPECT_EQ(2, dcg_a->second.encryption_metas_size());
    EXPECT_EQ(2, dcg_a->second.shared_files_size());
    std::map<std::string, std::vector<uint32_t>> columns_by_file;
    for (int i = 0; i < dcg_a->second.column_files_size(); ++i) {
        const auto& ids = dcg_a->second.unique_column_ids(i).column_ids();
        columns_by_file[dcg_a->second.column_files(i)] = std::vector<uint32_t>(ids.begin(), ids.end());
        EXPECT_EQ(1, dcg_a->second.versions(i));
        EXPECT_FALSE(dcg_a->second.shared_files(i)) << "a private DCG must stay private";
    }
    EXPECT_EQ((std::map<std::string, std::vector<uint32_t>>{{"a1.cols", {1, 2}}, {"a2.cols", {3, 4}}}),
              columns_by_file);

    auto dcg_b = merged->dcg_meta().dcgs().find(*rssid_b);
    ASSERT_TRUE(dcg_b != merged->dcg_meta().dcgs().end());
    ASSERT_EQ(1, dcg_b->second.column_files_size());
    EXPECT_EQ("b.cols", dcg_b->second.column_files(0));
    ASSERT_EQ(1, dcg_b->second.unique_column_ids_size());
    ASSERT_EQ(1, dcg_b->second.unique_column_ids(0).column_ids_size());
    EXPECT_EQ(1, dcg_b->second.unique_column_ids(0).column_ids(0));
    ASSERT_EQ(1, dcg_b->second.shared_files_size());
    EXPECT_FALSE(dcg_b->second.shared_files(0));
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_dcg_missing_shape) {
    // DCG with column_files but missing unique_column_ids/versions (legacy add_dcg).
    // validate_dcg_shape should catch this -> Corruption.
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(merged_tablet);

    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(3);
    auto* rowset = meta_a->add_rowsets();
    rowset->set_id(1);
    rowset->set_version(1);
    rowset->set_num_rows(10);
    rowset->set_data_size(100);
    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("seg.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
    }
    // Use legacy add_dcg (no unique_column_ids/versions)
    add_dcg(meta_a.get(), 1, "malformed.cols");

    EXPECT_OK(put_tablet_metadata(meta_a));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto st = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_TRUE(st.is_corruption()) << st;
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_dcg_duplicate_column_uid) {
    // Single child DCG has two entries with overlapping column UIDs {1,2} and {2,3}.
    // validate_dcg_shape should catch column 2 duplication -> Corruption.
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(merged_tablet);

    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(3);
    auto* rowset = meta_a->add_rowsets();
    rowset->set_id(1);
    rowset->set_version(1);
    rowset->set_num_rows(10);
    rowset->set_data_size(100);
    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("seg.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
    }
    // Build a malformed DCG with overlapping column UIDs across entries
    add_dcg_with_columns(meta_a.get(), 1, "first.cols", {1, 2}, 1, /*shared_file=*/false);
    add_dcg_with_columns(meta_a.get(), 1, "second.cols", {2, 3}, 1, /*shared_file=*/false);

    EXPECT_OK(put_tablet_metadata(meta_a));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto st = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_TRUE(st.is_corruption()) << st;
}

// --- Tests for MERGING cross-publish drop-as-empty-compaction ---
//
// convert_txn_log() on MERGING_TABLET turns a compaction txn into a no-op at
// apply time by clearing the op_compaction / op_parallel_compaction fields,
// because their contents reference the source tablet's rowset-id space which
// is not valid against the merged tablet. Non-compaction ops are either passed
// through (op_write) or rejected (op_schema_change / op_replication /
// mixed op_write+compaction).

namespace {

// Build a MERGING PublishTabletInfo with |source_tablet_id| as the sole
// source and |merged_tablet_id| as the target.
lake::PublishTabletInfo make_merging_publish_info(int64_t source_tablet_id, int64_t merged_tablet_id) {
    int64_t ids[] = {source_tablet_id};
    return lake::PublishTabletInfo(lake::PublishTabletInfo::MERGING_TABLET, std::span<const int64_t>(ids, 1),
                                   merged_tablet_id);
}

TxnLogPtr make_op_write_only_log(int64_t source_tablet_id, const std::string& segment_name) {
    auto log = std::make_shared<TxnLogPB>();
    log->set_tablet_id(source_tablet_id);
    log->set_txn_id(1000);
    auto* rowset = log->mutable_op_write()->mutable_rowset();
    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename(segment_name);
        sm->set_size(128);
    }
    rowset->set_num_rows(1);
    return log;
}

} // namespace

TEST_F(LakeTabletMergerTest, test_convert_txn_log_merging_op_write_only_passthrough) {
    const int64_t source_tablet_id = next_id();
    const int64_t merged_tablet_id = next_id();
    auto log = make_op_write_only_log(source_tablet_id, "write_seg.dat");
    const auto original_rowset_serialized = log->op_write().rowset().SerializeAsString();

    auto info = make_merging_publish_info(source_tablet_id, merged_tablet_id);
    ASSIGN_OR_ABORT(auto converted, lake::convert_txn_log(log, nullptr /* base_metadata unused */, info));

    EXPECT_EQ(merged_tablet_id, converted->tablet_id());
    ASSERT_TRUE(converted->has_op_write());
    EXPECT_EQ(original_rowset_serialized, converted->op_write().rowset().SerializeAsString());
    EXPECT_FALSE(converted->has_op_compaction());
    EXPECT_FALSE(converted->has_op_parallel_compaction());
}

TEST_F(LakeTabletMergerTest, test_convert_txn_log_merging_drops_op_compaction) {
    const int64_t source_tablet_id = next_id();
    const int64_t merged_tablet_id = next_id();
    auto log = make_op_compaction_log(source_tablet_id);

    auto info = make_merging_publish_info(source_tablet_id, merged_tablet_id);
    ASSIGN_OR_ABORT(auto converted, lake::convert_txn_log(log, nullptr, info));

    // Compaction payload cleared → apply becomes a no-op.
    EXPECT_FALSE(converted->has_op_compaction());
    EXPECT_FALSE(converted->has_op_parallel_compaction());
    // Other fields preserved.
    EXPECT_EQ(merged_tablet_id, converted->tablet_id());
    EXPECT_EQ(log->txn_id(), converted->txn_id());
}

TEST_F(LakeTabletMergerTest, test_convert_txn_log_merging_drops_op_parallel_compaction) {
    const int64_t source_tablet_id = next_id();
    const int64_t merged_tablet_id = next_id();
    auto log = std::make_shared<TxnLogPB>();
    log->set_tablet_id(source_tablet_id);
    log->set_txn_id(2020);
    auto* op_parallel_compaction = log->mutable_op_parallel_compaction();
    for (int i = 0; i < 2; ++i) {
        auto* subtask = op_parallel_compaction->add_subtask_compactions();
        subtask->mutable_output_rowset()->add_segment_metas()->set_filename(fmt::format("subtask_seg_{}.dat", i));
        subtask->mutable_output_sstable()->set_filename(fmt::format("subtask_{}.sst", i));
    }

    auto info = make_merging_publish_info(source_tablet_id, merged_tablet_id);
    ASSIGN_OR_ABORT(auto converted, lake::convert_txn_log(log, nullptr, info));

    EXPECT_FALSE(converted->has_op_parallel_compaction());
    EXPECT_EQ(merged_tablet_id, converted->tablet_id());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_rowset_zero_before_any_side_effect) {
    auto run_case = [&](uint32_t rowset_id, bool primary_key, bool read_alias, bool expect_rejection) {
        SCOPED_TRACE(fmt::format("rowset_id={}, primary_key={}, read_alias={}", rowset_id, primary_key, read_alias));
        constexpr int64_t kBaseVersion = 1;
        constexpr int64_t kNewVersion = 2;
        constexpr int kNumRows = 2;
        const int64_t source_a_id = next_id();
        const int64_t source_b_id = next_id();
        const int64_t target_id = next_id();
        const int64_t txn_id = next_id();
        for (int64_t tablet_id : {source_a_id, source_b_id, target_id}) {
            prepare_tablet_dirs(tablet_id);
        }

        // The merge cases give each source a segment of its own. The read alias serves the children of a split,
        // which still share the parent's segment, so that case keeps one shared segment.
        auto build_source = [&](int64_t tablet_id, int lower_key, int upper_key, int source_index) {
            const std::string segment_name = read_alias ? fmt::format("rowset_zero_shared_{}.dat", txn_id)
                                                        : fmt::format("rowset_zero_{}_{}.dat", txn_id, source_index);
            const uint64_t segment_size =
                    write_two_column_segment(target_id, segment_name, kNumRows, [](int key) { return key * 10; });
            auto metadata = std::make_shared<TabletMetadataPB>();
            metadata->set_id(tablet_id);
            metadata->set_version(kBaseVersion);
            metadata->set_next_rowset_id(rowset_id + 1);
            set_two_column_pk_schema(metadata.get(), /*schema_id=*/4001);
            metadata->set_enable_persistent_index(primary_key);
            if (primary_key) {
                metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
            } else {
                metadata->mutable_schema()->set_keys_type(DUP_KEYS);
            }
            metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(lower_key));
            metadata->mutable_range()->set_lower_bound_included(true);
            metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(upper_key));
            metadata->mutable_range()->set_upper_bound_included(false);

            auto* rowset = metadata->add_rowsets();
            rowset->set_id(rowset_id);
            rowset->set_version(kBaseVersion);
            rowset->set_num_rows(kNumRows);
            rowset->set_data_size(segment_size);
            rowset->mutable_range()->CopyFrom(metadata->range());
            auto* segment = rowset->add_segment_metas();
            segment->set_filename(segment_name);
            segment->set_size(segment_size);
            segment->set_num_rows(kNumRows);
            segment->set_segment_idx(0);
            segment->set_shared(read_alias);
            if (read_alias) {
                stamp_physical_identity_uid(rowset, segment_name);
            } else {
                lake::tablet_reshard_helper::set_rowset_uid(rowset);
            }
            (*metadata->mutable_rowset_to_schema())[rowset_id] = metadata->schema().id();

            if (primary_key) {
                DelVector delvec;
                const uint32_t deleted_rowid = static_cast<uint32_t>(source_index);
                delvec.init(kBaseVersion + source_index, &deleted_rowid, 1);
                add_delvec(metadata.get(), tablet_id, kBaseVersion + source_index, rowset_id,
                           fmt::format("rowset_zero_{}_{}.delvec", txn_id, source_index), delvec.save());
            }
            return metadata;
        };

        auto source_a = build_source(source_a_id, /*lower_key=*/0, /*upper_key=*/1, /*source_index=*/0);
        auto source_b = build_source(source_b_id, /*lower_key=*/1, /*upper_key=*/2, /*source_index=*/1);
        const std::vector<std::string> source_pbs_before = {source_a->SerializeAsString(),
                                                            source_b->SerializeAsString()};
        // A real merge is published, so its sources must be stored before the inventories are taken.
        if (!read_alias) {
            ASSERT_OK(_tablet_manager->put_tablet_metadata(source_a));
            ASSERT_OK(_tablet_manager->put_tablet_metadata(source_b));
        }
        ASSIGN_OR_ABORT(auto files_before, directory_inventory(_location_provider->segment_root_location(target_id)));
        ASSIGN_OR_ABORT(auto metadata_before,
                        directory_inventory(_location_provider->metadata_root_location(target_id)));

        StatusOr<MutableTabletMetadataPtr> merged;
        if (read_alias) {
            MergingTabletInfoPB merging_info;
            merging_info.add_old_tablet_ids(source_a_id);
            merging_info.add_old_tablet_ids(source_b_id);
            merging_info.set_new_tablet_id(target_id);
            TxnInfoPB txn_info;
            txn_info.set_txn_id(txn_id);
            txn_info.set_commit_time(1);
            txn_info.set_gtid(1);
            merged = lake::virtual_merge_for_read(_tablet_manager.get(), {source_a, source_b}, merging_info,
                                                  kNewVersion, txn_info);
        } else {
            // The rejection has to come before any source primary-key index flush, so make a flush attempt fail
            // loudly instead of being skipped.
            if (expect_rejection) {
                set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
                set_failpoint_mode("fail_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
            }
            merged = publish_verbatim_merge({source_a, source_b}, target_id, kNewVersion);
            if (expect_rejection) {
                set_failpoint_mode("fail_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
                set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
            }
        }

        EXPECT_EQ(source_pbs_before[0], source_a->SerializeAsString());
        EXPECT_EQ(source_pbs_before[1], source_b->SerializeAsString());
        ASSIGN_OR_ABORT(auto files_after, directory_inventory(_location_provider->segment_root_location(target_id)));
        ASSIGN_OR_ABORT(auto metadata_after,
                        directory_inventory(_location_provider->metadata_root_location(target_id)));
        if (expect_rejection) {
            EXPECT_TRUE(merged.status().is_invalid_argument()) << merged.status();
            EXPECT_TRUE(merged.status().message().contains(fmt::format("tablet {}", source_a_id))) << merged.status();
            EXPECT_TRUE(merged.status().message().contains("rowset 0")) << merged.status();
            EXPECT_EQ(files_before, files_after);
            EXPECT_EQ(metadata_before, metadata_after);
            expect_target_version_not_published(target_id, kNewVersion);
        } else if (read_alias) {
            EXPECT_OK(merged.status());
            EXPECT_FALSE((*merged)->delvec_meta().delvecs().empty())
                    << "the alias must carry the children's delete vectors";
            EXPECT_TRUE((*merged)->sstable_meta().sstables().empty());
            EXPECT_TRUE((*merged)->dcg_meta().dcgs().empty());
            // Its one output is a delete-vector file, which location_provider puts under the segment root as
            // well, so this directory does change -- by exactly that file. It publishes nothing.
            EXPECT_NE(files_before, files_after);
            EXPECT_EQ(metadata_before, metadata_after);
        } else {
            EXPECT_OK(merged.status());
        }
    };

    run_case(/*rowset_id=*/0, /*primary_key=*/true, /*read_alias=*/false, /*expect_rejection=*/true);
    run_case(/*rowset_id=*/1, /*primary_key=*/true, /*read_alias=*/false, /*expect_rejection=*/false);
    // The read alias allocates no rssid and rebuilds no index, so rowset 0 costs it nothing and is
    // accepted -- the restriction above belongs to a real merge alone.
    run_case(/*rowset_id=*/0, /*primary_key=*/true, /*read_alias=*/true, /*expect_rejection=*/false);
    run_case(/*rowset_id=*/0, /*primary_key=*/false, /*read_alias=*/false, /*expect_rejection=*/false);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_packs_upper_half_live_rowset_without_sst) {
    const int64_t base_version = 1;
    const int64_t source_tablet = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(source_tablet);
    prepare_tablet_dirs(merged_tablet);

    auto source = std::make_shared<TabletMetadataPB>();
    source->set_id(source_tablet);
    source->set_version(base_version);
    source->set_next_rowset_id(std::numeric_limits<int32_t>::max());
    // A merge source always declares its range; this one covers everything.
    source->mutable_range();
    auto* rowset = source->add_rowsets();
    rowset->set_id(std::numeric_limits<int32_t>::max());
    rowset->set_version(base_version);
    rowset->set_num_rows(1);
    rowset->set_data_size(1);
    rowset->add_segment_metas()->set_filename("last_live_segment.dat");
    rowset->mutable_segment_metas(0)->set_num_rows(1);
    const std::string source_before = source->SerializeAsString();
    ASSERT_OK(put_tablet_metadata(source));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(source_tablet);
    merging_info.set_new_tablet_id(merged_tablet);
    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto status = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version,
                                                  base_version + 1, txn_info, false, tablet_metadatas, tablet_ranges);

    ASSERT_OK(status);
    EXPECT_EQ(source_before, source->SerializeAsString());
    auto target = tablet_metadatas.find(merged_tablet);
    ASSERT_NE(tablet_metadatas.end(), target);
    ASSERT_EQ(1, target->second->rowsets_size());
    EXPECT_EQ(1, target->second->rowsets(0).id());
    EXPECT_EQ(2, target->second->next_rowset_id());
}

// A source whose only rowset sits just below the top of the RSSID domain is packed down to RSSID 1, so the merged
// tablet keeps writing: next_rowset_id 2, and the next write gets 3.
TEST_F(LakeTabletMergerTest, test_tablet_merging_near_rssid_boundary_remains_writable) {
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    DeferOp restore_flush_failpoint(
            [&] { set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE); });

    const int64_t base_version = 1;
    const int64_t merged_version = 2;
    const int64_t source_tablet = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(source_tablet);
    prepare_tablet_dirs(merged_tablet);

    const uint64_t source_segment_size =
            write_two_column_segment(source_tablet, "near_boundary_source.dat", 1, [](int) { return 100; });
    auto source = make_single_segment_pk_tablet(source_tablet, base_version, "near_boundary_source.dat",
                                                source_segment_size, 1);
    // The whole key space.
    source->mutable_range();
    source->mutable_rowsets(0)->set_id(std::numeric_limits<int32_t>::max() - 1);
    source->set_next_rowset_id(std::numeric_limits<int32_t>::max());
    ASSERT_OK(put_tablet_metadata(source));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(source_tablet);
    merging_info.set_new_tablet_id(merged_tablet);
    TxnInfoPB merge_txn;
    merge_txn.set_txn_id(1);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, merged_version,
                                              merge_txn, false, tablet_metadatas, tablet_ranges));
    auto merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(2, merged->next_rowset_id());

    const uint64_t write_segment_size = write_two_column_segment(
            merged_tablet, "near_boundary_write.dat", 1, [](int) { return 200; }, 1);
    TxnLogPB write_log;
    write_log.set_tablet_id(merged_tablet);
    write_log.set_txn_id(2);
    auto* write_rowset = write_log.mutable_op_write()->mutable_rowset();
    write_rowset->set_num_rows(1);
    write_rowset->set_data_size(write_segment_size);
    auto* write_segment = write_rowset->add_segment_metas();
    write_segment->set_filename("near_boundary_write.dat");
    write_segment->set_size(write_segment_size);
    write_segment->set_num_rows(1);
    ASSERT_OK(_tablet_manager->put_txn_log(write_log));

    TxnInfoPB write_txn;
    write_txn.set_txn_id(2);
    write_txn.set_txn_type(TXN_NORMAL);
    write_txn.set_commit_time(1);
    auto published =
            lake::publish_version(_tablet_manager.get(), lake::PublishTabletInfo(merged_tablet), merged_version,
                                  merged_version + 1, std::span<const TxnInfoPB>(&write_txn, 1), false);
    ASSERT_OK(published.status());
    EXPECT_EQ(3, published.value()->next_rowset_id());
    if (!published.value()->sstable_meta().sstables().empty()) {
        EXPECT_LE(published.value()->sstable_meta().sstables().rbegin()->max_rss_rowid(),
                  static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_hot_sibling_id_space_does_not_overflow) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t cold_tablet = next_id();
    const int64_t hot_tablet = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(cold_tablet);
    prepare_tablet_dirs(hot_tablet);
    prepare_tablet_dirs(merged_tablet);

    // ctx[0]: the cold sibling. Everything it ever wrote has been compacted into rowset 11,
    // whose inputs (<= 10) are gone.
    auto cold_meta = std::make_shared<TabletMetadataPB>();
    cold_meta->set_id(cold_tablet);
    cold_meta->set_version(base_version);
    cold_meta->set_next_rowset_id(12);
    set_primary_key_schema(cold_meta.get(), 1001);
    add_rowset(cold_meta.get(), /*rowset_id=*/11, /*max_compact_input_rowset_id=*/10,
               /*del_origin_rowset_id=*/11);

    // ctx[1]: the hot sibling. min live id 798, and rowset 803 is a compaction output that
    // inherited a del file from input rowset 766 -- 32 ids below its own live minimum.
    auto hot_meta = std::make_shared<TabletMetadataPB>();
    hot_meta->set_id(hot_tablet);
    hot_meta->set_version(base_version);
    hot_meta->set_next_rowset_id(861);
    set_primary_key_schema(hot_meta.get(), 1001);
    add_rowset(hot_meta.get(), /*rowset_id=*/798, /*max_compact_input_rowset_id=*/797,
               /*del_origin_rowset_id=*/798);
    add_rowset(hot_meta.get(), /*rowset_id=*/803, /*max_compact_input_rowset_id=*/797,
               /*del_origin_rowset_id=*/766);
    add_rowset(hot_meta.get(), /*rowset_id=*/860, /*max_compact_input_rowset_id=*/859,
               /*del_origin_rowset_id=*/860);

    ASSERT_OK(put_merge_sources(cold_meta, hot_meta));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(cold_tablet);
    merging_tablet.add_old_tablet_ids(hot_tablet);
    merging_tablet.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(2);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(2);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    // Before the fix this returned InvalidArgument("Segment id overflow during tablet merge").
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto it = tablet_metadatas.find(merged_tablet);
    ASSERT_TRUE(it != tablet_metadatas.end());
    const auto& merged = it->second;

    // No dedup is possible (every rowset carries its own uid), so all four survive.
    ASSERT_EQ(4, merged->rowsets_size());

    std::map<uint32_t, const RowsetMetadataPB*> by_id;
    for (const auto& rowset : merged->rowsets()) {
        by_id[rowset.id()] = &rowset;
    }

    // A primary-key source moves as one run from its lowest to its highest reference. The cold
    // context is [10,12) and packs to [1,3); the hot context is [766,861) and packs from cursor 3,
    // so 766 -> 3, 797 -> 34, 798 -> 35, 803 -> 40 and 860 -> 97.
    ASSERT_TRUE(by_id.count(2));
    ASSERT_TRUE(by_id.count(35));
    ASSERT_TRUE(by_id.count(40));
    ASSERT_TRUE(by_id.count(97));

    // Every reference moves with the run it belongs to: raw max-compact 797 maps to 34 and
    // raw delete origin 766 maps to 3.
    const auto* compaction_output = by_id[40];
    EXPECT_EQ(34u, compaction_output->max_compact_input_rowset_id());
    ASSERT_EQ(1, compaction_output->del_files_size());
    EXPECT_EQ(3u, compaction_output->del_files(0).origin_rowset_id());

    // The hot run is packed after the cold run, so every carried value remains above the cold
    // rowset's target RSSID.
    for (const auto& rowset : merged->rowsets()) {
        if (rowset.id() == 2) continue;
        EXPECT_GT(rowset.id(), 2u);
        EXPECT_GT(rowset.max_compact_input_rowset_id(), 2u);
        for (const auto& del_file : rowset.del_files()) {
            EXPECT_GT(del_file.origin_rowset_id(), 2u);
        }
    }

    // The packed-run cursor is authoritative for future writes.
    EXPECT_EQ(98u, merged->next_rowset_id());
}

// A later context's compacted-away reference can numerically equal an earlier
// context's live rowset id. Both contribute primary atoms; packing the contexts'
// runs in order keeps the unrelated identifiers disjoint in the target namespace.
TEST_F(LakeTabletMergerTest, test_tablet_merging_dead_reference_does_not_collide_with_earlier_live_rowset) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t earlier_tablet = next_id();
    const int64_t later_tablet = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(earlier_tablet);
    prepare_tablet_dirs(later_tablet);
    prepare_tablet_dirs(merged_tablet);

    auto earlier_meta = std::make_shared<TabletMetadataPB>();
    earlier_meta->set_id(earlier_tablet);
    earlier_meta->set_version(base_version);
    earlier_meta->set_next_rowset_id(767);
    set_primary_key_schema(earlier_meta.get(), 1001);
    add_rowset(earlier_meta.get(), /*rowset_id=*/766, /*max_compact_input_rowset_id=*/765,
               /*del_origin_rowset_id=*/766);

    auto later_meta = std::make_shared<TabletMetadataPB>();
    later_meta->set_id(later_tablet);
    later_meta->set_version(base_version);
    later_meta->set_next_rowset_id(804);
    set_primary_key_schema(later_meta.get(), 1001);
    add_rowset(later_meta.get(), /*rowset_id=*/798, /*max_compact_input_rowset_id=*/797,
               /*del_origin_rowset_id=*/798);
    add_rowset(later_meta.get(), /*rowset_id=*/803, /*max_compact_input_rowset_id=*/765,
               /*del_origin_rowset_id=*/766);

    ASSERT_OK(put_merge_sources(earlier_meta, later_meta));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(earlier_tablet);
    merging_tablet.add_old_tablet_ids(later_tablet);
    merging_tablet.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(2);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(2);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto it = tablet_metadatas.find(merged_tablet);
    ASSERT_TRUE(it != tablet_metadatas.end());
    const auto& merged = it->second;

    std::map<uint32_t, const RowsetMetadataPB*> by_id;
    for (const auto& rowset : merged->rowsets()) {
        by_id[rowset.id()] = &rowset;
    }

    // The earlier source is the run [765,767) packed to [1,3); the later one is the single run [765,804) packed
    // from 3. Its dead references 765 and 766 land on 3 and 4, never on the earlier source's live rowset 2.
    ASSERT_TRUE(by_id.count(2));
    ASSERT_TRUE(by_id.count(36));
    ASSERT_TRUE(by_id.count(41));

    const auto* later_compaction_output = by_id[41];
    ASSERT_EQ(1, later_compaction_output->del_files_size());
    EXPECT_EQ(4u, later_compaction_output->del_files(0).origin_rowset_id());
    EXPECT_EQ(3u, later_compaction_output->max_compact_input_rowset_id());
    EXPECT_EQ(42u, merged->next_rowset_id());
}

// Three siblings whose rowset-id counters have diverged, where the middle one
// carries nothing but a delete predicate that dedups away against the first
// sibling's predicate at the same version.
//
// The discarded predicate contributes no canonical extent or primary atom. Its
// sparse raw id must therefore consume no target slot and must not raise the
// cursor seen by the third sibling.
TEST_F(LakeTabletMergerTest, test_tablet_merging_discarded_predicate_does_not_inflate_later_sibling_ceiling) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t low_tablet = next_id();
    const int64_t predicate_only_tablet = next_id();
    const int64_t wide_tablet = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(low_tablet);
    prepare_tablet_dirs(predicate_only_tablet);
    prepare_tablet_dirs(wide_tablet);
    prepare_tablet_dirs(merged_tablet);

    // ctx[0]: data(v1) -> predicate(v10). Their two canonical singleton atoms pack first.
    TabletMetadataPB low_meta;
    low_meta.set_id(low_tablet);
    low_meta.set_version(base_version);
    low_meta.set_next_rowset_id(3);
    add_rowset_with_predicate(&low_meta, /*rowset_id=*/1, /*version=*/1, /*has_predicate=*/false);
    add_rowset_with_predicate(&low_meta, /*rowset_id=*/2, /*version=*/10, /*has_predicate=*/true);

    // ctx[1]: the same v10 delete predicate, cross-published onto a sibling whose id
    // counter ran far ahead. Predicates dedup by version, so this context contributes
    // no canonical atom, run, occurrence alias, or target slot.
    constexpr uint32_t kFarAheadPredicateId = 2'100'000'000;
    TabletMetadataPB predicate_only_meta;
    predicate_only_meta.set_id(predicate_only_tablet);
    predicate_only_meta.set_version(base_version);
    predicate_only_meta.set_next_rowset_id(kFarAheadPredicateId + 1);
    add_rowset_with_predicate(&predicate_only_meta, kFarAheadPredicateId, /*version=*/10, /*has_predicate=*/true);

    // ctx[2]: two live rowsets ~1e8 ids apart. They form two singleton runs, so the
    // numeric gap and ctx[1]'s unused raw predicate id do not consume target space.
    constexpr uint32_t kWideSpanTopId = 100'000'000;
    TabletMetadataPB wide_meta;
    wide_meta.set_id(wide_tablet);
    wide_meta.set_version(base_version);
    wide_meta.set_next_rowset_id(kWideSpanTopId + 1);
    add_rowset_with_predicate(&wide_meta, /*rowset_id=*/5, /*version=*/20, /*has_predicate=*/false);
    add_rowset_with_predicate(&wide_meta, kWideSpanTopId, /*version=*/21, /*has_predicate=*/false);
    ASSERT_OK(put_merge_sources(low_meta, predicate_only_meta, wide_meta));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(low_tablet);
    merging_tablet.add_old_tablet_ids(predicate_only_tablet);
    merging_tablet.add_old_tablet_ids(wide_tablet);
    merging_tablet.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(2);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(2);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    // Before the fix this returned InvalidArgument("Segment id overflow during tablet merge").
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto it = tablet_metadatas.find(merged_tablet);
    ASSERT_TRUE(it != tablet_metadatas.end());
    const auto& merged = it->second;

    // ctx[0]'s two rowsets plus ctx[2]'s two; ctx[1]'s predicate is deduped away.
    ASSERT_EQ(4, merged->rowsets_size());

    std::vector<uint32_t> rowset_ids;
    int predicate_count = 0;
    for (const auto& rowset : merged->rowsets()) {
        rowset_ids.push_back(rowset.id());
        if (rowset.has_delete_predicate()) {
            ++predicate_count;
            EXPECT_EQ(10, rowset.version());
        }
    }
    EXPECT_EQ(1, predicate_count);

    // ctx[1] contributes nothing. ctx[2]'s two singleton runs pack at targets 3
    // and 4, and the authoritative cursor advances to exactly 5.
    EXPECT_EQ((std::vector<uint32_t>{1, 2, 3, 4}), rowset_ids);
    EXPECT_EQ(5, merged->next_rowset_id());
}

// Two pieces of one logical rowset (same uid, segment indices 0 and 1), one of them with every statistic at INT64_MAX.
// They stay two rowsets of the merged tablet, each with its own statistics; nothing is summed.
TEST_F(LakeTabletMergerTest, test_tablet_merging_keeps_same_uid_piece_statistics_verbatim) {
    for (bool reverse_sources : {false, true}) {
        SCOPED_TRACE(reverse_sources);
        auto first = make_allocator_source(next_id(), 20);
        auto* rowset = add_allocator_rowset(first.get(), 10, 1, "stats-overflow.dat", 0);
        rowset->set_num_rows(std::numeric_limits<int64_t>::max());
        rowset->set_data_size(std::numeric_limits<int64_t>::max());
        rowset->set_num_dels(std::numeric_limits<int64_t>::max());
        auto second = std::make_shared<TabletMetadataPB>(*first);
        second->set_id(next_id());
        second->mutable_rowsets(0)->set_num_rows(1);
        second->mutable_rowsets(0)->set_data_size(1);
        second->mutable_rowsets(0)->set_num_dels(1);
        second->mutable_rowsets(0)->mutable_segment_metas(0)->set_filename("stats-overflow-second.dat");
        second->mutable_rowsets(0)->mutable_segment_metas(0)->set_segment_idx(1);
        *first->mutable_range()->mutable_upper_bound() = generate_sort_key(10);
        first->mutable_range()->set_upper_bound_included(false);
        *second->mutable_range()->mutable_lower_bound() = generate_sort_key(10);
        second->mutable_range()->set_lower_bound_included(true);
        std::vector<TabletMetadataPtr> sources = {first, second};
        if (reverse_sources) std::swap(sources[0], sources[1]);

        ASSIGN_OR_ABORT(auto merged, publish_verbatim_merge(sources, next_id(), /*target_version=*/2));
        ASSERT_EQ(2, merged->rowsets_size());
        std::multiset<int64_t> num_rows;
        for (const auto& merged_rowset : merged->rowsets()) {
            EXPECT_TRUE(lake::tablet_reshard_helper::same_rowset_uid(merged_rowset, first->rowsets(0)));
            EXPECT_EQ(merged_rowset.num_rows(), merged_rowset.data_size());
            EXPECT_EQ(merged_rowset.num_rows(), merged_rowset.num_dels());
            num_rows.insert(merged_rowset.num_rows());
        }
        EXPECT_EQ((std::multiset<int64_t>{1, std::numeric_limits<int64_t>::max()}), num_rows);
    }
}

// One source lists a segment in two rowsets: rowset 10 holds segment 0, and rowset 20, a piece of the same logical
// rowset, holds segments 0 and 7. One segment cannot hold the rows of two rowsets, so the merge refuses the source.
TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_segment_in_two_rowsets_of_one_source) {
    auto first = make_allocator_source(next_id(), 20);
    auto* r = add_allocator_rowset(first.get(), 10, 1, "complete-0.dat");
    auto second = std::make_shared<TabletMetadataPB>(*first);
    second->set_id(next_id());
    add_allocator_segment(second->mutable_rowsets(0), "complete-7.dat", 7);
    second->mutable_rowsets(0)->set_num_rows(r->num_rows());
    second->mutable_rowsets(0)->set_data_size(r->data_size());
    first->add_rowsets()->CopyFrom(second->rowsets(0));
    first->mutable_rowsets(1)->set_id(20);
    first->set_next_rowset_id(30);
    auto status = expect_merge_rejected_without_writes({first}, next_id(), /*target_version=*/2);
    EXPECT_TRUE(status.message().contains("complete-0.dat at bundle offset 0 in more than one rowset")) << status;
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_strictly_overlapping_uid_ranges) {
    auto first = make_allocator_source(next_id(), 20);
    add_allocator_rowset(first.get(), 10, 1, "overlapping.dat");
    auto second = std::make_shared<TabletMetadataPB>(*first);
    second->set_id(next_id());
    *first->mutable_range()->mutable_upper_bound() = generate_sort_key(20);
    first->mutable_range()->set_upper_bound_included(false);
    *second->mutable_range()->mutable_lower_bound() = generate_sort_key(10);
    second->mutable_range()->set_lower_bound_included(true);
    first->mutable_rowsets(0)->mutable_range()->CopyFrom(first->range());
    auto* duplicate = first->add_rowsets();
    duplicate->CopyFrom(second->rowsets(0));
    duplicate->set_id(20);
    duplicate->mutable_range()->CopyFrom(second->range());
    first->clear_range();
    first->set_next_rowset_id(30);
    auto result = publish_allocator_merge({first});
    EXPECT_TRUE(result.status().is_corruption()) << result.status();
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_source_permutation_is_canonical) {
    auto first = make_allocator_source(next_id(), 20);
    auto second = make_allocator_source(next_id(), 20);
    for (auto* source : {first.get(), second.get()}) {
        auto* r = add_allocator_rowset(source, 10, 1, source == first.get() ? "order-a.dat" : "order-b.dat");
        r->mutable_uid()->set_hi(1);
        r->mutable_uid()->set_lo(source == first.get() ? 2 : 1);
    }
    ASSIGN_OR_ABORT(auto forward, publish_allocator_merge({first, second}));
    ASSIGN_OR_ABORT(auto reverse, publish_allocator_merge({second, first}));
    ASSERT_EQ(2, forward->rowsets_size());
    ASSERT_EQ(2, reverse->rowsets_size());
    for (int i = 0; i < 2; ++i) {
        EXPECT_EQ(forward->rowsets(i).SerializeAsString(), reverse->rowsets(i).SerializeAsString());
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_interval_projection_packs_sparse_contexts) {
    auto first = make_allocator_source(next_id(), 102);
    add_allocator_rowset(first.get(), 100, 1, "sparse_100.dat");
    add_allocator_rowset(first.get(), 101, 2, "sparse_101.dat");
    auto second = make_allocator_source(next_id(), 1'000'002);
    add_allocator_rowset(second.get(), 1'000'000, 3, "sparse_1000000.dat");
    add_allocator_rowset(second.get(), 1'000'001, 4, "sparse_1000001.dat");
    auto third = make_allocator_source(next_id(), 1'500'001);
    add_allocator_rowset(third.get(), 1'500'000, 5, "sparse_1500000.dat");

    ASSIGN_OR_ABORT(auto merged, publish_allocator_merge({first, second, third}));
    EXPECT_EQ((std::vector<uint32_t>{1, 2, 3, 4, 5}), allocator_rowset_ids(*merged));
    EXPECT_EQ(6, merged->next_rowset_id());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_interval_projection_compresses_context_zero) {
    auto source = make_allocator_source(next_id(), 106);
    // A primary-key source keeps its gaps (it moves as one run), so packing is shown on another key type.
    source->mutable_schema()->set_keys_type(DUP_KEYS);
    add_allocator_rowset(source.get(), 100, 1, "context_zero_100.dat");
    add_allocator_rowset(source.get(), 105, 2, "context_zero_105.dat");

    ASSIGN_OR_ABORT(auto merged, publish_allocator_merge({source}));
    EXPECT_EQ((std::vector<uint32_t>{1, 2}), allocator_rowset_ids(*merged));
    EXPECT_EQ(3, merged->next_rowset_id());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_interval_projection_accepts_upper_half_source_domain) {
    constexpr uint32_t kSourceRowset = std::numeric_limits<uint32_t>::max() - 10;
    auto source = make_allocator_source(next_id(), std::numeric_limits<uint32_t>::max());
    auto* rowset = add_allocator_rowset(source.get(), kSourceRowset, 1, "upper_half_0.dat", 0);
    add_allocator_segment(rowset, "upper_half_10.dat", 10);
    rowset->set_max_compact_input_rowset_id(std::numeric_limits<uint32_t>::max());

    ASSIGN_OR_ABORT(const uint32_t expected_cursor, repeated_merge_cursor_oracle({source}));
    EXPECT_EQ(12, expected_cursor) << "[UINT32_MAX, 2^32) recovery atom must pack with the upper-half extent";

    auto merged_or = publish_allocator_merge({source});
    ASSERT_OK(merged_or.status());
    auto merged = std::move(merged_or).value();
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_EQ(1, merged->rowsets(0).id());
    ASSERT_EQ(2, merged->rowsets(0).segment_metas_size());
    EXPECT_EQ(11, merged->rowsets(0).id() + lake::get_segment_idx(merged->rowsets(0), 1));
    ASSERT_TRUE(merged->rowsets(0).has_max_compact_input_rowset_id());
    EXPECT_EQ(11, merged->rowsets(0).max_compact_input_rowset_id());
    EXPECT_EQ(expected_cursor, merged->next_rowset_id());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_interval_projection_rejects_overlapping_final_ownership) {
    auto source = make_allocator_source(next_id(), 16);
    auto* wide = add_allocator_rowset(source.get(), 10, 1, "overlap_0.dat", 0);
    for (uint32_t idx = 1; idx <= 10; ++idx) {
        add_allocator_segment(wide, fmt::format("overlap_{}.dat", idx), idx);
    }
    add_allocator_rowset(source.get(), 15, 2, "overlap_other.dat", 0);

    expect_merge_rejected_without_writes({source}, next_id(), /*target_version=*/2);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_different_uid_recovery_histories_remain_distinct) {
    auto first = make_allocator_source(next_id(), 11);
    auto* a = add_allocator_rowset(first.get(), 10, 1, "independent_compaction_a.dat", 0);
    a->set_max_compact_input_rowset_id(9);
    auto second = make_allocator_source(next_id(), 101);
    auto* b = add_allocator_rowset(second.get(), 100, 1, "independent_compaction_b.dat", 0);
    b->set_max_compact_input_rowset_id(99);
    ASSERT_NE(a->uid().SerializeAsString(), b->uid().SerializeAsString());
    for (bool reverse_sources : {false, true}) {
        std::vector<std::shared_ptr<TabletMetadataPB>> sources{first, second};
        if (reverse_sources) std::reverse(sources.begin(), sources.end());
        ASSIGN_OR_ABORT(auto merged, publish_allocator_merge(sources));
        ASSERT_EQ(2, merged->rowsets_size());
        std::map<std::string, uint32_t> recovery_by_uid;
        for (const auto& rowset : merged->rowsets()) {
            recovery_by_uid.emplace(rowset.uid().SerializeAsString(), rowset.max_compact_input_rowset_id());
        }
        EXPECT_LT(recovery_by_uid.at(a->uid().SerializeAsString()), recovery_by_uid.at(b->uid().SerializeAsString()));
    }
}

// A third source holds a copy of the first source's rowset, and a copy of the second's or a rowset of its own, under
// other ids and recovery keys. The copied segment has two owners, so the merge refuses the three.
TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_conflicting_or_divergent_recovery_aliases_before_io) {
    for (bool divergent_primary : {false, true}) {
        SCOPED_TRACE(divergent_primary);
        auto first = make_allocator_source(next_id(), 101);
        auto* a = add_allocator_rowset(first.get(), 100, 1, "recovery_alias_a.dat", 0);
        a->set_max_compact_input_rowset_id(90);
        auto second = make_allocator_source(next_id(), 201);
        auto* b = add_allocator_rowset(second.get(), 200, 2, "recovery_alias_b.dat", 0);
        b->set_max_compact_input_rowset_id(190);
        auto third = make_allocator_source(next_id(), 601);
        auto* copy_a = third->add_rowsets();
        copy_a->CopyFrom(*a);
        copy_a->set_id(300);
        copy_a->set_max_compact_input_rowset_id(500);
        if (divergent_primary) {
            auto* independent = add_allocator_rowset(third.get(), 600, 3, "recovery_alias_independent.dat", 0);
            independent->set_max_compact_input_rowset_id(500);
        } else {
            auto* copy_b = third->add_rowsets();
            copy_b->CopyFrom(*b);
            copy_b->set_id(400);
            copy_b->set_max_compact_input_rowset_id(500);
        }
        auto status = expect_merge_rejected_without_writes({first, second, third}, next_id(), 2);
        EXPECT_TRUE(status.message().contains("both declare segment file recovery_alias_a.dat")) << status;
    }
}

// Each source holds a rowset whose del file was inherited from an earlier rowset (origin 40 or 400) and records no
// op_offset. The merge moves each rowset and its del origin by its source's delta -- [40, 42) to [1, 3) and
// [400, 402) to [3, 5) -- leaving op_offset absent and the segment indices alone, and the merged tablet's rows and
// index stay exact through DML, reopens and a full compaction.
TEST_F(LakeTabletMergerTest, test_tablet_merging_rejoins_independently_remapped_delete_origins) {
    const int64_t first_id = next_id();
    const int64_t second_id = next_id();
    prepare_tablet_dirs(first_id);
    prepare_tablet_dirs(second_id);
    auto first_source = make_allocator_source(first_id, 42);
    auto second_source = make_allocator_source(second_id, 402);
    for (auto* source : {first_source.get(), second_source.get()}) {
        set_two_column_pk_schema(source, 4001);
        source->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
        source->set_enable_persistent_index(true);
        source->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
    }
    first_source->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(0));
    first_source->mutable_range()->set_lower_bound_included(true);
    first_source->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(50));
    first_source->mutable_range()->set_upper_bound_included(false);
    second_source->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(50));
    second_source->mutable_range()->set_lower_bound_included(true);
    second_source->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(100));
    second_source->mutable_range()->set_upper_bound_included(false);
    auto add_inherited = [&](TabletMetadataPB* source, uint32_t rowset_id, const std::string& stem, int32_t key) {
        const uint64_t size = write_two_column_segment(
                source->id(), stem + ".dat", 1, [=](int) { return key * 10; }, key);
        auto* rowset = add_allocator_rowset(source, rowset_id, 1, stem + ".dat", 0);
        rowset->mutable_segment_metas(0)->set_size(size);
        auto* del = rowset->add_del_files();
        del->set_name(stem + ".del");
        del->set_origin_rowset_id(rowset_id - 1);
        del->set_encryption_meta(write_encrypted_binary_del_file(source->id(), del->name(), {}));
        return rowset->uid();
    };
    const auto first_uid = add_inherited(first_source.get(), 41, "rejoin_first", 10);
    const auto second_uid = add_inherited(second_source.get(), 401, "rejoin_second", 60);

    auto merged_or = publish_allocator_merge({first_source, second_source});
    ASSERT_OK(merged_or.status());
    auto merged = std::move(merged_or).value();
    ASSERT_EQ(2, merged->rowsets_size());
    for (const auto& [uid, rowset_id, origin] : {std::tuple{first_uid, 2u, 1u}, std::tuple{second_uid, 4u, 3u}}) {
        auto rowset = std::find_if(merged->rowsets().begin(), merged->rowsets().end(), [&](const auto& candidate) {
            return candidate.uid().SerializeAsString() == uid.SerializeAsString();
        });
        ASSERT_NE(merged->rowsets().end(), rowset);
        EXPECT_EQ(rowset_id, rowset->id());
        ASSERT_EQ(1, rowset->segment_metas_size());
        EXPECT_EQ(0, rowset->segment_metas(0).segment_idx());
        ASSERT_EQ(1, rowset->del_files_size());
        EXPECT_EQ(origin, rowset->del_files(0).origin_rowset_id());
        EXPECT_FALSE(rowset->del_files(0).has_op_offset());
    }
    EXPECT_EQ(5, merged->next_rowset_id());
    ASSERT_EQ(0, merged->sstable_meta().sstables_size());
    _primary_key_manager->unload_and_remove_primary_index(merged->id());
    expect_lifecycle_oracle(merged, {{10, 100}, {60, 600}}, {});

    _primary_key_manager->unload_and_remove_primary_index(merged->id());
    ASSIGN_OR_ABORT(auto after_dml, publish_followup_upsert_delete(merged->id(), merged->version(), 10, 1010, 60));
    expect_lifecycle_oracle(after_dml, {{10, 1010}}, {60});
    _primary_key_manager->unload_and_remove_primary_index(merged->id());
    ASSIGN_OR_ABORT(auto reopened_after_dml, _tablet_manager->get_tablet_metadata(merged->id(), after_dml->version()));
    expect_lifecycle_oracle(reopened_after_dml, {{10, 1010}}, {60});
    ASSIGN_OR_ABORT(auto compacted, compact_tablet(merged->id(), reopened_after_dml->version(), /*force_base=*/true));
    expect_lifecycle_oracle(compacted, {{10, 1010}}, {60});
    _primary_key_manager->unload_and_remove_primary_index(merged->id());
    ASSIGN_OR_ABORT(auto reopened_compacted, _tablet_manager->get_tablet_metadata(merged->id(), compacted->version()));
    expect_lifecycle_oracle(reopened_compacted, {{10, 1010}}, {60});
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_accepts_sparse_explicit_segment_indices) {
    auto source = make_allocator_source(next_id(), 20);
    auto* rowset = add_allocator_rowset(source.get(), 10, 1, "canonical_sparse_2.dat", 2);
    add_allocator_segment(rowset, "canonical_sparse_7.dat", 7);
    const auto uid = rowset->uid().SerializeAsString();
    ASSIGN_OR_ABORT(auto merged, publish_allocator_merge({source}));
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_EQ(uid, merged->rowsets(0).uid().SerializeAsString());
    ASSERT_EQ(2, merged->rowsets(0).segment_metas_size());
    EXPECT_EQ(2, merged->rowsets(0).segment_metas(0).segment_idx());
    EXPECT_EQ(7, merged->rowsets(0).segment_metas(1).segment_idx());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_accepts_nonzero_compaction_cursor) {
    auto a = make_allocator_source(next_id(), 20);
    auto* first = add_allocator_rowset(a.get(), 10, 1, "canonical_cursor.dat", 7);
    first->set_overlapped(true);
    first->set_next_compaction_offset(3);
    auto b = make_allocator_source(next_id(), 30);
    add_allocator_rowset(b.get(), 20, 2, "canonical_cursor_other.dat", 0);
    const auto cursor_uid = first->uid().SerializeAsString();
    ASSIGN_OR_ABORT(auto merged, publish_allocator_merge({a, b}));
    ASSERT_EQ(2, merged->rowsets_size());
    auto cursor_rowset = std::find_if(merged->rowsets().begin(), merged->rowsets().end(), [&](const auto& rowset) {
        return rowset.uid().SerializeAsString() == cursor_uid;
    });
    ASSERT_NE(merged->rowsets().end(), cursor_rowset);
    EXPECT_EQ(3, cursor_rowset->next_compaction_offset());
    EXPECT_TRUE(cursor_rowset->overlapped());
    EXPECT_EQ(7, cursor_rowset->segment_metas(0).segment_idx());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_invalid_predicate_shape_before_io) {
    auto source = make_allocator_source(next_id(), 20);
    auto* rowset = add_allocator_rowset(source.get(), 10, 1, "predicate_payload.dat");
    rowset->mutable_delete_predicate()->set_version(-1);
    expect_merge_rejected_without_writes({source}, next_id(), 2);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_duplicate_segment_index_before_io) {
    auto source = make_allocator_source(next_id(), 20);
    auto* rowset = add_allocator_rowset(source.get(), 10, 1, "canonical_duplicate_a.dat", 2);
    add_allocator_segment(rowset, "canonical_duplicate_b.dat", 2);
    expect_merge_rejected_without_writes({source}, next_id(), 2);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_self_del_offset_outside_replay_span) {
    auto source = make_allocator_source(next_id(), 20);
    auto* rowset = add_allocator_rowset(source.get(), 10, 1, "canonical_self.dat", 7);
    auto* del = rowset->add_del_files();
    del->set_name("canonical_self.del");
    del->set_origin_rowset_id(10);
    del->set_op_offset(8);
    expect_merge_rejected_without_writes({source}, next_id(), 2);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_accepts_segmentless_self_del_offset_zero) {
    auto source = make_allocator_source(next_id(), 20);
    auto* rowset = source->add_rowsets();
    rowset->set_id(10);
    rowset->set_version(1);
    lake::tablet_reshard_helper::set_rowset_uid(rowset);
    auto* del = rowset->add_del_files();
    del->set_name("canonical_segmentless.del");
    del->set_origin_rowset_id(10);
    del->set_op_offset(0);
    ASSIGN_OR_ABORT(auto merged, publish_allocator_merge({source}));
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_EQ(0, merged->rowsets(0).segment_metas_size());
    ASSERT_EQ(1, merged->rowsets(0).del_files_size());
    EXPECT_EQ(merged->rowsets(0).id(), merged->rowsets(0).del_files(0).origin_rowset_id());
    EXPECT_EQ(0, merged->rowsets(0).del_files(0).op_offset());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_accepts_inherited_del_offset_outside_current_segments) {
    auto source = make_allocator_source(next_id(), 20);
    auto* rowset = add_allocator_rowset(source.get(), 10, 1, "canonical_inherited.dat", 0);
    auto* del = rowset->add_del_files();
    del->set_name("canonical_inherited.del");
    del->set_origin_rowset_id(2);
    del->set_op_offset(5);
    ASSIGN_OR_ABORT(auto merged, publish_allocator_merge({source}));
    // The delete span [2,8) and the rowset [10,11) form one run [2,11) of this primary-key source.
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_EQ(9, merged->rowsets(0).id());
    EXPECT_EQ(1, merged->rowsets(0).del_files(0).origin_rowset_id());
    EXPECT_EQ(5, merged->rowsets(0).del_files(0).op_offset());
    EXPECT_EQ(10, merged->next_rowset_id());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_transferred_inherited_del_survives_cold_load) {
    const int32_t old_min_segments = config::lake_pk_compaction_min_input_segments;
    config::lake_pk_compaction_min_input_segments = 1;
    DeferOp restore([&] { config::lake_pk_compaction_min_input_segments = old_min_segments; });
    // A private neighbor that reaches the same version through the same write-then-compact history.
    ASSIGN_OR_ABORT(auto neighbor_initial, create_lifecycle_source(next_id(), 300, 400, 310, 3100, false));
    ASSIGN_OR_ABORT(auto neighbor_written,
                    publish_followup_upsert_delete(neighbor_initial->id(), neighbor_initial->version(), 320, 3200, 0,
                                                   /*include_delete=*/false));
    ASSIGN_OR_ABORT(auto neighbor, compact_tablet(neighbor_written->id(), neighbor_written->version(), true));

    ASSIGN_OR_ABORT(auto initial, create_lifecycle_source(next_id(), 0, 300, 10, 100, false));
    ASSIGN_OR_ABORT(auto deleted, publish_followup_upsert_delete(initial->id(), initial->version(), 110, 1100, 10));
    ASSERT_EQ(1, deleted->rowsets(deleted->rowsets_size() - 1).del_files_size());
    const auto original_del = deleted->rowsets(deleted->rowsets_size() - 1).del_files(0);
    ASSIGN_OR_ABORT(auto compacted, compact_tablet(deleted->id(), deleted->version(), true));
    ASSERT_EQ(1, compacted->rowsets_size());
    ASSERT_EQ(1, compacted->rowsets(0).del_files_size());
    EXPECT_EQ(original_del.origin_rowset_id(), compacted->rowsets(0).del_files(0).origin_rowset_id());
    ASSERT_NE(compacted->rowsets(0).id(), original_del.origin_rowset_id());
    ASSERT_EQ(compacted->version(), neighbor->version());

    const int64_t target = next_id();
    prepare_tablet_dirs(target);
    std::unordered_map<int64_t, TabletMetadataPtr> published;
    ASSERT_OK(publish_resharding_merge({compacted, neighbor}, target, compacted->version(), compacted->version() + 1,
                                       next_id(), published));
    auto merged = published.at(target);
    ASSERT_EQ(2, merged->rowsets_size());
    const RowsetMetadataPB* carrier = nullptr;
    for (const auto& rowset : merged->rowsets()) {
        if (rowset.uid().SerializeAsString() == compacted->rowsets(0).uid().SerializeAsString()) carrier = &rowset;
    }
    ASSERT_NE(nullptr, carrier);
    ASSERT_EQ(1, carrier->del_files_size());
    EXPECT_EQ(original_del.name(), carrier->del_files(0).name());
    EXPECT_NE(compacted->rowsets(0).id(), carrier->id()) << "the fixture must make the merge move the rowset";
    // Still inherited: the rowset and the delete's origin move together, never onto each other.
    EXPECT_NE(carrier->id(), carrier->del_files(0).origin_rowset_id());
    EXPECT_EQ(static_cast<int64_t>(compacted->rowsets(0).id()) - original_del.origin_rowset_id(),
              static_cast<int64_t>(carrier->id()) - carrier->del_files(0).origin_rowset_id());
    _primary_key_manager->unload_and_remove_primary_index(target);
    _tablet_manager->prune_metacache();
    ASSIGN_OR_ABORT(auto reopened, _tablet_manager->get_tablet_metadata(target, merged->version()));
    expect_lifecycle_oracle(reopened, {{110, 1100}, {310, 3100}, {320, 3200}}, {10});
}

// Two pieces of one logical rowset (same uid) in two sources, at segment indices 7 and 8, that differ in one field:
// version, schema mapping, a del file, next_compaction_offset, a recovery key, or a field this build does not know.
// Each stays a rowset of the merged tablet with its own value of that field. Pieces that both claim segment index 7
// are refused.
TEST_F(LakeTabletMergerTest, test_tablet_merging_keeps_same_uid_pieces_with_their_own_fields) {
    enum Mutation {
        VERSION,
        SCHEMA_MAPPING,
        DEL_BEARING_MODE,
        NEXT_COMPACTION_OFFSET,
        RECOVERY_PRESENCE,
        STABLE_RESIDUAL_FIELD
    };
    auto make_pieces = [&](uint32_t second_segment_idx) {
        auto a = make_allocator_source(next_id(), 20);
        auto* first = add_allocator_rowset(a.get(), 10, 1, fmt::format("canonical_{}.dat", a->id()), 7);
        first->set_overlapped(true);
        auto b = make_allocator_source(next_id(), 30);
        auto* second = b->add_rowsets();
        second->CopyFrom(*first);
        second->set_id(20);
        second->mutable_segment_metas(0)->set_filename(fmt::format("canonical_{}.dat", b->id()));
        second->mutable_segment_metas(0)->set_segment_idx(second_segment_idx);
        return std::pair{a, b};
    };
    for (auto mutation : {VERSION, SCHEMA_MAPPING, DEL_BEARING_MODE, NEXT_COMPACTION_OFFSET, RECOVERY_PRESENCE,
                          STABLE_RESIDUAL_FIELD}) {
        SCOPED_TRACE(mutation);
        auto [a, b] = make_pieces(/*second_segment_idx=*/8);
        auto* first = a->mutable_rowsets(0);
        auto* second = b->mutable_rowsets(0);
        switch (mutation) {
        case VERSION:
            second->set_version(2);
            break;
        case SCHEMA_MAPPING:
            add_historical_schema(b.get(), 4001);
            (*b->mutable_rowset_to_schema())[20] = 4001;
            break;
        case DEL_BEARING_MODE: {
            auto* del = second->add_del_files();
            del->set_name(fmt::format("canonical_{}.del", b->id()));
            del->set_origin_rowset_id(20);
            del->set_op_offset(7);
            break;
        }
        case NEXT_COMPACTION_OFFSET:
            second->set_next_compaction_offset(3);
            break;
        case RECOVERY_PRESENCE:
            first->set_max_compact_input_rowset_id(4);
            break;
        case STABLE_RESIDUAL_FIELD: {
            std::string serialized = second->SerializeAsString();
            serialized.append("\xF8\x07\x01", 3);
            ASSERT_TRUE(second->ParseFromString(serialized));
            break;
        }
        }
        ASSIGN_OR_ABORT(auto merged, publish_verbatim_merge({a, b}, next_id(), 2));
        ASSERT_EQ(2, merged->rowsets_size());
        const auto& x = merged->rowsets(0);
        const auto& y = merged->rowsets(1);
        EXPECT_TRUE(lake::tablet_reshard_helper::same_rowset_uid(x, y));
        auto count = [&](auto predicate) { return int(predicate(x)) + int(predicate(y)); };
        switch (mutation) {
        case VERSION:
            EXPECT_EQ(1, count([](const auto& rowset) { return rowset.version() == 2; }));
            break;
        case SCHEMA_MAPPING:
            EXPECT_EQ(1, merged->rowset_to_schema_size());
            EXPECT_EQ(1, count([&](const auto& rowset) {
                          auto found = merged->rowset_to_schema().find(rowset.id());
                          return found != merged->rowset_to_schema().end() && found->second == 4001;
                      }));
            break;
        case DEL_BEARING_MODE:
            EXPECT_EQ(1, count([](const auto& rowset) { return rowset.del_files_size() == 1; }));
            break;
        case NEXT_COMPACTION_OFFSET:
            EXPECT_EQ(1, count([](const auto& rowset) { return rowset.next_compaction_offset() == 3; }));
            break;
        case RECOVERY_PRESENCE:
            EXPECT_EQ(1, count([](const auto& rowset) { return rowset.has_max_compact_input_rowset_id(); }));
            break;
        case STABLE_RESIDUAL_FIELD:
            EXPECT_EQ(1, count([](const auto& rowset) {
                          return rowset.GetReflection()->GetUnknownFields(rowset).field_count() > 0;
                      }));
            break;
        }
    }

    auto [a, b] = make_pieces(/*second_segment_idx=*/7);
    auto status = expect_merge_rejected_without_writes({a, b}, next_id(), 2);
    EXPECT_TRUE(status.message().contains("both claim segment index 7")) << status;
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_empty_effective_rowset_range_before_io) {
    auto source = make_allocator_source(next_id(), 20);
    auto* rowset = add_allocator_rowset(source.get(), 10, 1, "canonical_empty_range.dat");
    source->mutable_range();
    *rowset->mutable_range()->mutable_lower_bound() = generate_sort_key(1);
    *rowset->mutable_range()->mutable_upper_bound() = generate_sort_key(1);
    rowset->mutable_range()->set_lower_bound_included(true);
    rowset->mutable_range()->set_upper_bound_included(false);
    expect_merge_rejected_without_writes({source}, next_id(), 2);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_rowset_range_outside_source_before_io) {
    auto source = make_allocator_source(next_id(), 20);
    auto* rowset = add_allocator_rowset(source.get(), 10, 1, "canonical_outside.dat");
    *source->mutable_range()->mutable_upper_bound() = generate_sort_key(10);
    source->mutable_range()->set_upper_bound_included(false);
    *rowset->mutable_range()->mutable_upper_bound() = generate_sort_key(20);
    rowset->mutable_range()->set_upper_bound_included(false);
    expect_merge_rejected_without_writes({source}, next_id(), 2);
}

// Two sources hold one rowset (same uid, same segment), and its del file is the rowset's own in one source and
// inherited in the other. The segment and the del file each have two owners, so the merge refuses the pair.
TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_mixed_self_inherited_del_class) {
    auto selected_source = make_allocator_source(next_id(), 11);
    auto* selected = add_allocator_rowset(selected_source.get(), 10, 1, "mixed_del.dat", 0);
    auto* selected_del = selected->add_del_files();
    selected_del->set_name("mixed_class.del");
    selected_del->set_origin_rowset_id(10);
    auto duplicate_source = make_allocator_source(next_id(), 21);
    auto* duplicate = add_allocator_rowset(duplicate_source.get(), 20, 1, "mixed_del.dat", 0);
    duplicate->mutable_uid()->CopyFrom(selected->uid());
    auto* duplicate_del = duplicate->add_del_files();
    duplicate_del->set_name(selected_del->name());
    duplicate_del->set_origin_rowset_id(9);

    auto status = expect_merge_rejected_without_writes({selected_source, duplicate_source}, next_id(), 2);
    EXPECT_TRUE(status.message().contains("both declare segment file mixed_del.dat")) << status;
}

// Two sources declare one segment file and disagree on its size, its encryption, whether it has a size, or a field
// this build does not know. The declarations of a file are compared before its owners are counted, so the
// disagreement is what the merge reports.
TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_segment_declaration_conflict) {
    enum class Mutation { kSize, kEncryption, kPresence, kUnknown };
    for (auto mutation : {Mutation::kSize, Mutation::kEncryption, Mutation::kPresence, Mutation::kUnknown}) {
        SCOPED_TRACE(static_cast<int>(mutation));
        auto selected_source = make_allocator_source(next_id(), 11);
        auto* selected = add_allocator_rowset(selected_source.get(), 10, 1, "segment_conflict.dat", 0);
        auto duplicate_source = make_allocator_source(next_id(), 21);
        auto* duplicate = add_allocator_rowset(duplicate_source.get(), 20, 1, "segment_conflict.dat", 0);
        duplicate->mutable_uid()->CopyFrom(selected->uid());
        auto* segment = duplicate->mutable_segment_metas(0);
        switch (mutation) {
        case Mutation::kSize:
            segment->set_size(2);
            break;
        case Mutation::kEncryption:
            segment->set_encryption_meta("different encryption metadata");
            break;
        case Mutation::kPresence:
            selected->mutable_segment_metas(0)->clear_size();
            break;
        case Mutation::kUnknown: {
            std::string serialized = segment->SerializeAsString();
            serialized.append("\xF8\x07\x01", 3);
            ASSERT_TRUE(segment->ParseFromString(serialized));
            break;
        }
        }
        auto status = expect_merge_rejected_without_writes({selected_source, duplicate_source}, next_id(), 2);
        EXPECT_TRUE(status.message().contains("segment_conflict.dat at bundle offset 0 has conflicting declarations"))
                << status;
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_absent_segment_idx_before_io) {
    auto selected_source = make_allocator_source(next_id(), 11);
    add_allocator_rowset(selected_source.get(), 10, 1, "segment_presence.dat", 0, /*explicit_segment_idx=*/false);
    auto peer_source = make_allocator_source(next_id(), 21);
    add_allocator_rowset(peer_source.get(), 20, 1, "segment_presence_peer.dat", 0);
    *selected_source->mutable_range()->mutable_upper_bound() = generate_sort_key(1);
    selected_source->mutable_range()->set_upper_bound_included(false);
    *peer_source->mutable_range()->mutable_lower_bound() = generate_sort_key(1);
    peer_source->mutable_range()->set_lower_bound_included(true);

    // The sources are stored verbatim, so the segment keeps its missing segment_idx all the way to the merge.
    auto status = expect_merge_rejected_without_writes({selected_source, peer_source}, next_id(), /*target_version=*/2);
    EXPECT_TRUE(status.message().contains("explicit segment_idx")) << status;
}

// Two sources each declare the del file declaration.del, agreeing on its fields or not. A del file belongs to one
// rowset of one source, so the merge refuses the pair.
TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_del_declaration_conflict) {
    for (bool same_fields : {true, false}) {
        SCOPED_TRACE(same_fields);
        auto selected_source = make_allocator_source(next_id(), 11);
        auto* selected = add_allocator_rowset(selected_source.get(), 10, 1, "del_conflict_a.dat", 0);
        auto* selected_del = selected->add_del_files();
        selected_del->set_name("declaration.del");
        selected_del->set_origin_rowset_id(10);
        selected_del->set_op_offset(0);
        selected_del->set_version(1);
        selected_del->set_num_rows(1);
        selected_del->set_crc32c(1);
        auto duplicate_source = make_allocator_source(next_id(), 21);
        auto* duplicate = add_allocator_rowset(duplicate_source.get(), 20, 1, "del_conflict_b.dat", 0);
        auto* duplicate_del = duplicate->add_del_files();
        duplicate_del->CopyFrom(*selected_del);
        duplicate_del->set_origin_rowset_id(20);
        if (!same_fields) duplicate_del->set_crc32c(2);
        auto status = expect_merge_rejected_without_writes({selected_source, duplicate_source}, next_id(), 2);
        EXPECT_TRUE(status.message().contains("both declare del file declaration.del")) << status;
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_predicate_declaration_conflict) {
    auto first = make_allocator_source(next_id(), 11);
    auto* left = add_rowset_with_predicate(first.get(), 10, 7, true);
    left->mutable_delete_predicate()->mutable_binary_predicates(0)->set_value("1");
    auto second = make_allocator_source(next_id(), 21);
    auto* right = add_rowset_with_predicate(second.get(), 20, 7, true);
    right->mutable_delete_predicate()->mutable_binary_predicates(0)->set_value("2");

    auto result = publish_allocator_merge({first, second});
    EXPECT_TRUE(result.status().is_corruption()) << result.status();
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_unions_matching_predicate_ranges) {
    auto first = make_allocator_source(next_id(), 11);
    auto* left = add_rowset_with_predicate(first.get(), 10, 7, true);
    left->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(0));
    left->mutable_range()->set_lower_bound_included(true);
    left->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(50));
    left->mutable_range()->set_upper_bound_included(false);
    auto second = make_allocator_source(next_id(), 21);
    auto* right = add_rowset_with_predicate(second.get(), 20, 7, true);
    right->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(50));
    right->mutable_range()->set_lower_bound_included(true);
    right->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(100));
    right->mutable_range()->set_upper_bound_included(false);
    first->mutable_range()->CopyFrom(left->range());
    second->mutable_range()->CopyFrom(right->range());

    ASSIGN_OR_ABORT(auto merged, publish_allocator_merge({first, second}));
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_EQ(1, merged->rowsets(0).id());
    EXPECT_EQ(generate_sort_key(0).SerializeAsString(), merged->rowsets(0).range().lower_bound().SerializeAsString());
    EXPECT_EQ(generate_sort_key(100).SerializeAsString(), merged->rowsets(0).range().upper_bound().SerializeAsString());
    EXPECT_EQ(2, merged->next_rowset_id());
}

// A delete is published to every tablet of a partition, so the predicate rowsets of one version in different sources
// are one delete and the merge keeps one copy. Copies mapped to different schemas cannot be one delete, and are
// refused.
TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_rowset_schema_mapping_conflict) {
    auto first = make_allocator_source(next_id(), 12);
    add_allocator_rowset(first.get(), 10, 1, fmt::format("schema_conflict_{}.dat", first->id()), 0);
    add_rowset_with_predicate(first.get(), 11, 1, /*has_predicate=*/true);
    (*first->mutable_rowset_to_schema())[11] = 4001;
    auto second = make_allocator_source(next_id(), 22);
    add_allocator_rowset(second.get(), 20, 1, fmt::format("schema_conflict_{}.dat", second->id()), 0);
    add_rowset_with_predicate(second.get(), 21, 1, /*has_predicate=*/true);
    (*second->mutable_rowset_to_schema())[21] = 4002;

    auto status = expect_merge_rejected_without_writes({first, second}, next_id(), 2);
    EXPECT_TRUE(status.message().contains("duplicate rowset schema mapping conflict")) << status;
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_preserves_equal_recovery_key_equivalence_on_cold_load) {
    const int64_t source_id = next_id();
    prepare_tablet_dirs(source_id);
    auto source = make_allocator_source(source_id, 805);
    set_two_column_pk_schema(source.get(), 4001);
    source->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
    source->set_enable_persistent_index(true);
    source->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
    const uint64_t first_size = write_two_column_segment(
            source_id, "recovery_equal_a.dat", 1, [](int) { return 100; }, 10);
    const uint64_t second_size = write_two_column_segment(
            source_id, "recovery_equal_b.dat", 1, [](int) { return 200; }, 20);
    auto* first = add_allocator_rowset(source.get(), 798, 1, "recovery_equal_a.dat", 0);
    first->mutable_segment_metas(0)->set_size(first_size);
    first->set_max_compact_input_rowset_id(797);
    auto* second = add_allocator_rowset(source.get(), 803, 2, "recovery_equal_b.dat", 0);
    second->mutable_segment_metas(0)->set_size(second_size);
    second->set_max_compact_input_rowset_id(797);

    ASSIGN_OR_ABORT(auto merged, publish_allocator_merge({source}));
    ASSERT_EQ(2, merged->rowsets_size());
    ASSERT_TRUE(merged->rowsets(0).has_max_compact_input_rowset_id());
    ASSERT_TRUE(merged->rowsets(1).has_max_compact_input_rowset_id());
    EXPECT_EQ(merged->rowsets(0).max_compact_input_rowset_id(), merged->rowsets(1).max_compact_input_rowset_id());
    ASSIGN_OR_ABORT(auto rows, read_two_column_rows(merged));
    EXPECT_EQ((std::vector<std::pair<int32_t, int32_t>>{{10, 100}, {20, 200}}), rows);
    _primary_key_manager->unload_and_remove_primary_index(merged->id());
    const std::vector<std::string> keys = {encode_int_primary_key(10), encode_int_primary_key(20)};
    ASSIGN_OR_ABORT(auto values, load_index_values(merged, merged->id(), keys));
    ASSERT_EQ(2, values.size());
    // The source is the single run [797,804): 798 -> 2 and 803 -> 7.
    EXPECT_EQ(2, values[0].get_value() >> 32);
    EXPECT_EQ(7, values[1].get_value() >> 32);
}

// The second source holds a copy of the first source's rowset after a rowset of its own with a higher recovery key.
// The copied segment has two owners, so the merge refuses the pair.
TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_alias_reversed_recovery_order_before_io) {
    auto canonical_source = make_allocator_source(next_id(), 11);
    auto* canonical = add_allocator_rowset(canonical_source.get(), 10, 2, "recovery_alias.dat", 0);
    auto second_source = make_allocator_source(next_id(), 101);
    auto* independent = add_allocator_rowset(second_source.get(), 99, 1, "recovery_independent.dat", 0);
    independent->set_max_compact_input_rowset_id(100);
    auto* duplicate = add_allocator_rowset(second_source.get(), 100, 2, "recovery_alias.dat", 0);
    duplicate->mutable_uid()->CopyFrom(canonical->uid());

    auto status = expect_merge_rejected_without_writes({canonical_source, second_source}, next_id(), 2);
    EXPECT_TRUE(status.message().contains("both declare segment file recovery_alias.dat")) << status;
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_preserves_strict_recovery_order_on_cold_load) {
    const int64_t first_id = next_id();
    const int64_t second_id = next_id();
    prepare_tablet_dirs(first_id);
    prepare_tablet_dirs(second_id);
    auto first = make_allocator_source(first_id, 3);
    auto second = make_allocator_source(second_id, 21);
    for (auto* source : {first.get(), second.get()}) {
        set_two_column_pk_schema(source, 4001);
        source->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
        source->set_enable_persistent_index(true);
        source->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
    }
    const uint64_t first_size = write_two_column_segment(
            first_id, "recovery_strict_2.dat", 1, [](int) { return 100; }, 10);
    const uint64_t second_size = write_two_column_segment(
            second_id, "recovery_strict_20.dat", 1, [](int) { return 600; }, 60);
    auto* first_rowset = add_allocator_rowset(first.get(), 2, 1, "recovery_strict_2.dat", 0);
    first_rowset->mutable_segment_metas(0)->set_size(first_size);
    auto* second_rowset = add_allocator_rowset(second.get(), 20, 2, "recovery_strict_20.dat", 0);
    second_rowset->mutable_segment_metas(0)->set_size(second_size);
    *first->mutable_range()->mutable_upper_bound() = generate_sort_key(50);
    first->mutable_range()->set_upper_bound_included(false);
    *second->mutable_range()->mutable_lower_bound() = generate_sort_key(50);
    second->mutable_range()->set_lower_bound_included(true);

    ASSIGN_OR_ABORT(auto merged, publish_allocator_merge({first, second}));
    ASSERT_EQ(2, merged->rowsets_size());
    EXPECT_EQ((std::vector<uint32_t>{1, 2}), allocator_rowset_ids(*merged));
    EXPECT_EQ(3, merged->next_rowset_id());
    ASSIGN_OR_ABORT(auto rows, read_two_column_rows(merged));
    EXPECT_EQ((std::vector<std::pair<int32_t, int32_t>>{{10, 100}, {60, 600}}), rows);
    _primary_key_manager->unload_and_remove_primary_index(merged->id());
    const std::vector<std::string> keys = {encode_int_primary_key(10), encode_int_primary_key(60)};
    ASSIGN_OR_ABORT(auto values, load_index_values(merged, merged->id(), keys));
    ASSERT_EQ(2, values.size());
    EXPECT_EQ(1, values[0].get_value() >> 32);
    EXPECT_EQ(2, values[1].get_value() >> 32);
}

// The first source's modern SST names RSSID 5 as its owner, outside the RSSIDs the source references, so the SST
// cannot be translated and the merged tablet rebuilds its index. The second source holds a piece of the first
// source's rowset (same uid) at segment index 4, apart from the first source's index 0, and keeps it as a rowset of
// its own.
TEST_F(LakeTabletMergerTest, test_tablet_merging_affine_modern_source_stale_falls_back) {
    auto result_or = publish_metadata_only_merge_fixture(
            MetadataOnlyMergeShape::kPrivate, false, false, true, [](auto& sources) {
                auto* sst = sources[0]->mutable_sstable_meta()->mutable_sstables(0);
                sst->set_shared_rssid(5);
                sst->set_max_rss_rowid(static_cast<uint64_t>(5) << 32);
                sources[1]->clear_sstable_meta();
                lake::tablet_reshard_helper::set_rowset_uid(sources[0]->mutable_rowsets(0));
                sources[1]->mutable_rowsets(0)->mutable_uid()->CopyFrom(sources[0]->rowsets(0).uid());
                sources[1]->mutable_rowsets(0)->mutable_segment_metas(0)->set_segment_idx(4);
            });
    ASSERT_OK(result_or.status());
    auto result = std::move(result_or).value();
    expect_affine_sst_fallback_orphans(result);
    const auto& target = result.published.at(result.target_tablet_id);
    ASSERT_EQ(2, target.rowsets_size());
    EXPECT_TRUE(lake::tablet_reshard_helper::same_rowset_uid(target.rowsets(0), target.rowsets(1)));
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_affine_modern_source_live_reuses) {
    ASSIGN_OR_ABORT(auto result,
                    publish_metadata_only_merge_fixture(MetadataOnlyMergeShape::kPrivate, false, false, true));
    const auto& target = result.published.at(result.target_tablet_id);
    auto modern = std::find_if(target.sstable_meta().sstables().begin(), target.sstable_meta().sstables().end(),
                               [](const auto& sst) { return sst.has_shared_rssid(); });
    ASSERT_NE(target.sstable_meta().sstables().end(), modern);
    EXPECT_EQ(1, modern->shared_rssid());
    EXPECT_EQ(static_cast<uint64_t>(1) << 32, modern->max_rss_rowid());
}

// The first source's modern SST has watermark 2 while it owns only RSSID 1, so the watermark cannot be translated and
// the merged tablet rebuilds its index.
TEST_F(LakeTabletMergerTest, test_tablet_merging_affine_modern_mismatched_high_falls_back) {
    ASSIGN_OR_ABORT(auto result, publish_metadata_only_merge_fixture(
                                         MetadataOnlyMergeShape::kPrivate, false, false, true, [](auto& sources) {
                                             for (auto& source : sources) {
                                                 auto* sst = source->mutable_sstable_meta()->mutable_sstables(0);
                                                 if (sst->has_shared_rssid()) {
                                                     sst->set_max_rss_rowid(static_cast<uint64_t>(2) << 32);
                                                 }
                                             }
                                         }));
    expect_affine_sst_fallback_orphans(result);
}

// A legacy SST may already carry a negative rssid_offset: the second source's SST stores raw RSSID 7 with offset -2,
// which names its rowset 5. Packed to RSSID 2, the source moves by -3, so the SST gets offset -5 and watermark 2, key
// 60 resolves to RSSID 2, and the merged tablet serves DML, reopens and a full compaction with the SSTs carried.
TEST_F(LakeTabletMergerTest, test_tablet_merging_affine_legacy_negative_offset_projects) {
    ASSIGN_OR_ABORT(auto result,
                    publish_metadata_only_merge_fixture(
                            MetadataOnlyMergeShape::kPrivate, false, false, true, [&](auto& sources) {
                                auto* legacy = sources[1]->mutable_sstable_meta()->mutable_sstables(0);
                                const auto file = write_raw_pk_sstable(
                                        _tablet_manager->sst_location(sources[1]->id(), legacy->filename()),
                                        {{encode_int_primary_key(60),
                                          serialize_index_values({{sources[1]->version(), 7, 0}})}});
                                legacy->set_filesize(file.filesize);
                                legacy->mutable_range()->CopyFrom(file.range);
                                legacy->set_rssid_offset(-2);
                            }));
    const auto& target = result.published.at(result.target_tablet_id);
    ASSERT_EQ(2, target.sstable_meta().sstables_size());
    auto legacy = std::find_if(target.sstable_meta().sstables().begin(), target.sstable_meta().sstables().end(),
                               [](const auto& sst) { return !sst.has_shared_rssid(); });
    ASSERT_NE(target.sstable_meta().sstables().end(), legacy);
    EXPECT_EQ(-5, legacy->rssid_offset());
    EXPECT_EQ(static_cast<uint64_t>(2) << 32, legacy->max_rss_rowid());
    auto target_ptr = std::make_shared<TabletMetadataPB>(target);
    _primary_key_manager->unload_and_remove_primary_index(target.id());
    ASSIGN_OR_ABORT(auto values, load_index_values(target_ptr, target.id(), {encode_int_primary_key(/*key=*/60)}));
    ASSERT_EQ(1, values.size());
    EXPECT_EQ(2, values[0].get_value() >> 32);
    expect_metadata_merge_lifecycle(result, {{10, 100}, {60, 600}}, {{10, 1010}}, {60});
}

// The second source holds rowsets 5 and 100 and one legacy SST, offset 5 and watermark 100, holding key 60 (raw 0)
// and key 70 (raw 95). A primary-key source is one run, so the gap between its rowsets stays and the whole source
// moves by -3: offset 2, watermark 97, next_rowset_id 98, key 60 at RSSID 2 and key 70 at 97. The merged tablet serves
// DML, reopens and a full compaction with the SSTs carried.
TEST_F(LakeTabletMergerTest, test_tablet_merging_affine_legacy_gap_projects_with_one_delta) {
    ASSIGN_OR_ABORT(auto result,
                    publish_metadata_only_merge_fixture(
                            MetadataOnlyMergeShape::kPrivate, false, false, true, [&](auto& sources) {
                                const uint64_t extra_size = write_two_column_segment(
                                        sources[1]->id(), "legacy_gap_100.dat", 1, [](int) { return 700; }, 70);
                                auto* extra =
                                        add_allocator_rowset(sources[1].get(), 100, sources[1]->rowsets(0).version(),
                                                             "legacy_gap_100.dat", 0);
                                extra->mutable_segment_metas(0)->set_size(extra_size);
                                sources[1]->set_next_rowset_id(101);
                                auto* legacy = sources[1]->mutable_sstable_meta()->mutable_sstables(0);
                                const int64_t version = sources[1]->version();
                                const auto file = write_raw_pk_sstable(
                                        _tablet_manager->sst_location(sources[1]->id(), legacy->filename()),
                                        {{encode_int_primary_key(60), serialize_index_values({{version, 0, 0}})},
                                         {encode_int_primary_key(70), serialize_index_values({{version, 95, 0}})}});
                                legacy->set_filesize(file.filesize);
                                legacy->mutable_range()->CopyFrom(file.range);
                                legacy->set_rssid_offset(5);
                                legacy->set_max_rss_rowid(static_cast<uint64_t>(100) << 32);
                            }));
    const auto& target = result.published.at(result.target_tablet_id);
    EXPECT_EQ(98, target.next_rowset_id());
    ASSERT_EQ(2, target.sstable_meta().sstables_size());
    auto legacy = std::find_if(target.sstable_meta().sstables().begin(), target.sstable_meta().sstables().end(),
                               [](const auto& sst) { return !sst.has_shared_rssid(); });
    ASSERT_NE(target.sstable_meta().sstables().end(), legacy);
    EXPECT_EQ(2, legacy->rssid_offset());
    EXPECT_EQ(static_cast<uint64_t>(97) << 32, legacy->max_rss_rowid());
    auto target_ptr = std::make_shared<TabletMetadataPB>(target);
    _primary_key_manager->unload_and_remove_primary_index(target.id());
    ASSIGN_OR_ABORT(auto values, load_index_values(target_ptr, target.id(),
                                                   {encode_int_primary_key(60), encode_int_primary_key(70)}));
    ASSERT_EQ(2, values.size());
    EXPECT_EQ(2, values[0].get_value() >> 32);
    EXPECT_EQ(97, values[1].get_value() >> 32);
    expect_metadata_merge_lifecycle(result, {{10, 100}, {60, 600}, {70, 700}}, {{10, 1010}, {70, 700}}, {60});
}

// The second source's legacy SST stores raw RSSID 0 with offset 5 for its rowset 5; an extra rowset 100 lies above the
// SST's watermark. The source moves by -3 as one run: offset 2, watermark 2, key 60 at RSSID 2, and next_rowset_id
// 98, because a primary-key source keeps the gap below rowset 100.
TEST_F(LakeTabletMergerTest, test_tablet_merging_affine_legacy_raw_zero_reuses) {
    ASSIGN_OR_ABORT(auto result,
                    publish_metadata_only_merge_fixture(
                            MetadataOnlyMergeShape::kPrivate, false, false, true, [&](auto& sources) {
                                const std::string extra_name = "affine_raw_zero_extra.dat";
                                const uint64_t extra_size = write_two_column_segment(
                                        sources[1]->id(), extra_name, 1, [](int key) { return key * 10; }, 70);
                                auto* extra = add_allocator_rowset(sources[1].get(), 100,
                                                                   sources[1]->rowsets(0).version(), extra_name, 0);
                                extra->mutable_segment_metas(0)->set_size(extra_size);
                                sources[1]->set_next_rowset_id(101);
                            }));
    const auto& target = result.published.at(result.target_tablet_id);
    ASSERT_EQ(2, target.sstable_meta().sstables_size());
    auto legacy = std::find_if(target.sstable_meta().sstables().begin(), target.sstable_meta().sstables().end(),
                               [](const auto& sst) { return !sst.has_shared_rssid(); });
    ASSERT_NE(target.sstable_meta().sstables().end(), legacy);
    EXPECT_EQ(2, legacy->rssid_offset());
    EXPECT_EQ(static_cast<uint64_t>(2) << 32, legacy->max_rss_rowid());
    EXPECT_EQ(98, target.next_rowset_id());

    auto target_ptr = std::make_shared<TabletMetadataPB>(target);
    _primary_key_manager->unload_and_remove_primary_index(target.id());
    ASSIGN_OR_ABORT(auto values, load_index_values(target_ptr, target.id(), {encode_int_primary_key(/*key=*/60)}));
    ASSERT_EQ(1, values.size());
    EXPECT_EQ(2, values[0].get_value() >> 32) << "stored raw RSSID 0 must use the accumulated output offset";
}

// The second source carries a copy of the first source's rowset (same uid, same segment) just past its own legacy
// SST span. The copied segment has two owners, so the merge refuses the pair.
TEST_F(LakeTabletMergerTest, test_tablet_merging_affine_legacy_alias_exclusive_end_rejected) {
    auto status =
            expect_metadata_only_merge_rejected_without_writes(MetadataOnlyMergeShape::kPrivate, [&](auto& sources) {
                lake::tablet_reshard_helper::set_rowset_uid(sources[0]->mutable_rowsets(0));
                auto* exclusive_alias = add_allocator_rowset(sources[1].get(), 6, sources[1]->rowsets(0).version(),
                                                             sources[0]->rowsets(0).segment_metas(0).filename(), 0);
                exclusive_alias->mutable_segment_metas(0)->CopyFrom(sources[0]->rowsets(0).segment_metas(0));
                exclusive_alias->mutable_uid()->CopyFrom(sources[0]->rowsets(0).uid());
                sources[1]->set_next_rowset_id(7);
            });
    EXPECT_TRUE(status.message().contains("both declare segment file metadata_only_a.dat")) << status;
}

// The second source spans RSSIDs 5 to 36 and lists four legacy SSTs with watermarks 5, 15, 25 and 36, all with
// rssid_offset 5. Packed after the first source, it moves by -3 as one run, so every one of its SSTs gets offset 2
// and watermarks 2, 12, 22 and 33, the merged SSTs stay sorted by watermark, and each key resolves to its translated
// RSSID.
TEST_F(LakeTabletMergerTest, test_tablet_merging_legacy_sstables_share_one_translation) {
    constexpr int kSourceBLiveRssids = 32;
    constexpr int kLegacySstables = 4;
    ASSIGN_OR_ABORT(auto result,
                    publish_metadata_only_merge_fixture(
                            MetadataOnlyMergeShape::kPrivate, false, false, true, [&](auto& sources) {
                                auto& source = sources[1];
                                for (int i = 1; i < kSourceBLiveRssids; ++i) {
                                    const std::string segment_name = fmt::format("legacy_liveness_{}.dat", i);
                                    const uint64_t segment_size = write_two_column_segment(
                                            source->id(), segment_name, 1, [](int key) { return key * 10; }, 60 + i);
                                    auto* rowset =
                                            add_allocator_rowset(source.get(), 5 + i, source->rowsets(0).version(),
                                                                 segment_name, /*segment_idx=*/0);
                                    rowset->mutable_segment_metas(0)->set_size(segment_size);
                                }
                                source->set_next_rowset_id(5 + kSourceBLiveRssids);

                                const PersistentIndexSstablePB seed = source->sstable_meta().sstables(0);
                                for (int i = 1; i < kLegacySstables; ++i) {
                                    const uint32_t source_high =
                                            i + 1 == kLegacySstables ? 5 + kSourceBLiveRssids - 1 : 5 + i * 10;
                                    const std::string filename = fmt::format("legacy_liveness_{}.sst", i);
                                    const auto file = write_raw_pk_sstable(
                                            _tablet_manager->sst_location(source->id(), filename),
                                            {{encode_int_primary_key(60 + i),
                                              serialize_index_values({{source->version(), source_high - 5, 0}})}});
                                    auto* sstable = source->mutable_sstable_meta()->add_sstables();
                                    sstable->CopyFrom(seed);
                                    sstable->set_filename(filename);
                                    sstable->set_filesize(file.filesize);
                                    sstable->set_encryption_meta(file.encryption_meta);
                                    sstable->mutable_range()->CopyFrom(file.range);
                                    sstable->set_rssid_offset(5);
                                    sstable->set_max_rss_rowid(static_cast<uint64_t>(source_high) << 32);
                                    sstable->mutable_fileset_id()->set_lo(0x6000 + i);
                                }
                            }));
    const auto& target = result.published.at(result.target_tablet_id);
    EXPECT_EQ(2 + kSourceBLiveRssids, target.next_rowset_id());
    ASSERT_EQ(1 + kLegacySstables, target.sstable_meta().sstables_size());
    const auto& modern = target.sstable_meta().sstables(0);
    ASSERT_TRUE(modern.has_shared_rssid());
    EXPECT_EQ(1, modern.shared_rssid());
    EXPECT_EQ(static_cast<uint64_t>(1) << 32, modern.max_rss_rowid());
    const std::vector<uint32_t> expected_highs = {2, 12, 22, 33};
    for (int i = 0; i < kLegacySstables; ++i) {
        const auto& legacy = target.sstable_meta().sstables(i + 1);
        EXPECT_FALSE(legacy.has_shared_rssid());
        EXPECT_EQ(2, legacy.rssid_offset());
        EXPECT_EQ(static_cast<uint64_t>(expected_highs[i]) << 32, legacy.max_rss_rowid());
    }

    auto target_ptr = std::make_shared<TabletMetadataPB>(target);
    _primary_key_manager->unload_and_remove_primary_index(target.id());
    std::vector<std::string> keys;
    for (int i = 0; i < kLegacySstables; ++i) keys.push_back(encode_int_primary_key(60 + i));
    ASSIGN_OR_ABORT(auto values, load_index_values(target_ptr, target.id(), keys));
    ASSERT_EQ(keys.size(), values.size());
    for (int i = 0; i < kLegacySstables; ++i) {
        EXPECT_EQ(expected_highs[i], values[i].get_value() >> 32) << "key " << 60 + i;
    }
}

// The second source carries a copy of the first source's rowset as rowset 7, inside the RSSID span [5, 11) of its
// own rowset 5. Two rowsets of one source cannot share RSSIDs, so the merge refuses the pair before building any
// rowset of the merged tablet.
TEST_F(LakeTabletMergerTest, test_tablet_merging_affine_legacy_alias_interior_rejected_before_materialize) {
    auto status =
            expect_metadata_only_merge_rejected_without_writes(MetadataOnlyMergeShape::kPrivate, [&](auto& sources) {
                lake::tablet_reshard_helper::set_rowset_uid(sources[0]->mutable_rowsets(0));
                auto* alias = add_allocator_rowset(sources[1].get(), 7, sources[1]->rowsets(0).version(),
                                                   sources[0]->rowsets(0).segment_metas(0).filename(), 0);
                alias->mutable_segment_metas(0)->CopyFrom(sources[0]->rowsets(0).segment_metas(0));
                alias->mutable_uid()->CopyFrom(sources[0]->rowsets(0).uid());
                add_allocator_segment(sources[1]->mutable_rowsets(0), "legacy_alias_high.dat", 5);
                sources[1]->set_next_rowset_id(11);
                auto* legacy = sources[1]->mutable_sstable_meta()->mutable_sstables(0);
                legacy->set_rssid_offset(5);
                legacy->set_max_rss_rowid(static_cast<uint64_t>(10) << 32);
            });
    EXPECT_TRUE(status.message().contains("rowset extents overlap")) << status;
}

// Merge of two PK parents that both have cloud-native persistent index enabled.
// This exercises the merge's source index flush end-to-end. Parents have no
// rowsets, so loading their index rebuilds nothing and the flush writes no SST;
// the point is to confirm the cloud-native branch doesn't crash and that the
// flush participates in producing a consistent merged metadata.
TEST_F(LakeTabletMergerTest, test_tablet_merging_cloud_native_pk_flush_path) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_child = [&](int64_t tablet_id) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(1);
        set_primary_key_schema(meta.get(), 1001);
        meta->set_enable_persistent_index(true);
        meta->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        return meta;
    };

    auto meta_a = make_child(child_a);
    auto meta_b = make_child(child_b);

    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(child_a);
    merging_tablet.add_old_tablet_ids(child_b);
    merging_tablet.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto it = tablet_metadatas.find(merged_tablet);
    ASSERT_TRUE(it != tablet_metadatas.end());
    const auto& merged = it->second;

    // No rowsets in parents means nothing to merge at rowset level.
    EXPECT_EQ(0, merged->rowsets_size());
    // No pre-existing sstables and the temp index had an empty memtable,
    // so the dumped sstable_meta is empty.
    EXPECT_EQ(0, merged->sstable_meta().sstables_size());
    // Basic merged-tablet invariants.
    EXPECT_EQ(merged_tablet, merged->id());
    EXPECT_EQ(new_version, merged->version());
    EXPECT_TRUE(merged->enable_persistent_index());
    EXPECT_EQ(PersistentIndexTypePB::CLOUD_NATIVE, merged->persistent_index_type());
}

// ---------------------------------------------------------------------------
// Sources shaped like split children: INT-keyed ranges, a child that still
// shares the parent's segment, and children that own their segments.
// ---------------------------------------------------------------------------

namespace pr1_helpers {

// Populate range = [lower, upper) on a TabletRangePB using INT sort key.
inline void set_int_range(TabletRangePB* range, int lower, int upper) {
    LakeTabletReshardTest::generate_sort_key(lower).Swap(range->mutable_lower_bound());
    range->set_lower_bound_included(true);
    LakeTabletReshardTest::generate_sort_key(upper).Swap(range->mutable_upper_bound());
    range->set_upper_bound_included(false);
}

// A child that owns its one rowset and segment. Tablet and rowset range are both [tablet_lower, tablet_upper).
inline std::shared_ptr<TabletMetadataPB> make_private_child(int64_t tablet_id, int64_t base_version, uint32_t rowset_id,
                                                            KeysType keys_type, int tablet_lower, int tablet_upper) {
    auto meta = std::make_shared<TabletMetadataPB>();
    meta->set_id(tablet_id);
    meta->set_version(base_version);
    meta->set_next_rowset_id(rowset_id + 1);
    auto* schema = meta->mutable_schema();
    schema->set_keys_type(keys_type);
    schema->set_id(7777);
    set_int_range(meta->mutable_range(), tablet_lower, tablet_upper);

    auto* rowset = meta->add_rowsets();
    rowset->set_id(rowset_id);
    rowset->set_version(base_version);
    rowset->set_num_rows(10);
    rowset->set_data_size(100);
    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename(fmt::format("child_{}.dat", tablet_id));
        sm->set_size(100);
        sm->set_num_rows(10);
    }
    lake::tablet_reshard_helper::set_rowset_uid(rowset);
    set_int_range(rowset->mutable_range(), tablet_lower, tablet_upper);
    return meta;
}

// PK schema with an INT key c0 and an INT value c1, the column layout write_two_column_segment writes.
inline void set_pk_int_key_schema(TabletMetadataPB* metadata, int64_t schema_id) {
    auto* schema = metadata->mutable_schema();
    schema->set_keys_type(PRIMARY_KEYS);
    schema->set_id(schema_id);
    schema->set_num_short_key_columns(1);
    schema->set_num_rows_per_row_block(65535);
    auto* c0 = schema->add_column();
    c0->set_unique_id(1001);
    c0->set_name("c0");
    c0->set_type("INT");
    c0->set_is_key(true);
    c0->set_is_nullable(false);
    c0->set_length(4);
    c0->set_index_length(4);
    auto* c1 = schema->add_column();
    c1->set_unique_id(1002);
    c1->set_name("c1");
    c1->set_type("INT");
    c1->set_is_key(false);
    c1->set_is_nullable(false);
    c1->set_aggregation("REPLACE");
}

inline std::shared_ptr<TabletMetadataPB> make_pk_shared_child_with_real_segment(int64_t tablet_id, int64_t base_version,
                                                                                uint32_t shared_id, int tablet_lower,
                                                                                int tablet_upper, uint64_t segment_size,
                                                                                int physical_num_rows) {
    auto meta = std::make_shared<TabletMetadataPB>();
    meta->set_id(tablet_id);
    meta->set_version(base_version);
    meta->set_next_rowset_id(shared_id + 1);
    set_pk_int_key_schema(meta.get(), 9001);
    set_int_range(meta->mutable_range(), tablet_lower, tablet_upper);

    auto* rowset = meta->add_rowsets();
    rowset->set_id(shared_id);
    rowset->set_version(base_version);
    rowset->set_num_rows(static_cast<int64_t>(tablet_upper - tablet_lower));
    rowset->set_data_size(static_cast<int64_t>(segment_size));
    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("shared_seg.dat");
        sm->set_size(segment_size);
        sm->set_num_rows(physical_num_rows);
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(rowset, "shared_seg.dat"); // same uid across shared siblings => dedup
    set_int_range(rowset->mutable_range(), tablet_lower, tablet_upper);
    return meta;
}

inline std::shared_ptr<TabletMetadataPB> make_pk_compacted_child(int64_t tablet_id, int64_t base_version,
                                                                 uint32_t compacted_id, int tablet_lower,
                                                                 int tablet_upper,
                                                                 const std::string& compacted_seg_name) {
    auto meta = std::make_shared<TabletMetadataPB>();
    meta->set_id(tablet_id);
    meta->set_version(base_version);
    meta->set_next_rowset_id(compacted_id + 1);
    set_pk_int_key_schema(meta.get(), 9001);
    set_int_range(meta->mutable_range(), tablet_lower, tablet_upper);

    auto* rowset = meta->add_rowsets();
    rowset->set_id(compacted_id);
    rowset->set_version(base_version + 1);
    rowset->set_num_rows(tablet_upper - tablet_lower);
    rowset->set_data_size(100);
    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename(compacted_seg_name);
        sm->set_size(100);
        sm->set_num_rows(tablet_upper - tablet_lower);
    }
    set_int_range(rowset->mutable_range(), tablet_lower, tablet_upper);
    return meta;
}

} // namespace pr1_helpers

// One child of a split still shares the parent's segment and primary-key index SST; its sibling has compacted its
// share away. The merge refuses the pair, without writing anything, until the first child has compacted too.
TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_shared_child_beside_compacted_child) {
    using namespace pr1_helpers;
    const int64_t base_version = 1;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    auto meta_a = make_pk_shared_child_with_real_segment(child_a, base_version, /*shared_id=*/10, /*lower=*/0,
                                                         /*upper=*/10, /*segment_size=*/1024,
                                                         /*physical_num_rows=*/20);
    meta_a->mutable_rowsets(0)->mutable_segment_metas(0)->set_segment_idx(0);
    auto* sst_a = meta_a->mutable_sstable_meta()->add_sstables();
    sst_a->set_filename("shared.sst");
    sst_a->set_filesize(512);
    sst_a->set_shared(true);
    sst_a->set_shared_rssid(10);
    sst_a->set_shared_version(base_version);
    sst_a->set_max_rss_rowid((static_cast<uint64_t>(10) << 32) | 99);
    auto meta_b = make_pk_compacted_child(child_b, base_version, /*compacted_id=*/11, /*lower=*/10, /*upper=*/20,
                                          "compacted_b.dat");
    meta_b->mutable_rowsets(0)->mutable_segment_metas(0)->set_segment_idx(0);
    lake::tablet_reshard_helper::set_rowset_uid(meta_b->mutable_rowsets(0));

    auto status = expect_merge_rejected_without_writes({meta_a, meta_b}, next_id(), /*target_version=*/2);
    EXPECT_TRUE(status.message().contains(fmt::format("source tablet {} still holds shared data files", child_a)))
            << status;
}

// A duplicate-key merge has no primary-key index: it never flushes one for its sources (a flush attempt fails here)
// and publishes neither an SST nor an SST orphan. Each source's rowset stays a rowset of the merged tablet.
TEST_F(LakeTabletMergerTest, test_tablet_merging_non_pk_skips_pk_sstable_pipeline) {
    using namespace pr1_helpers;
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);
    ASSERT_OK(put_merge_sources(make_private_child(child_a, base_version, 10, DUP_KEYS, 0, 10),
                                make_private_child(child_b, base_version, 10, DUP_KEYS, 10, 20)));

    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    set_failpoint_mode("fail_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_flush([&] {
        set_failpoint_mode("fail_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    });

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);
    TxnInfoPB txn_info;
    txn_info.set_txn_id(92);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    const auto& merged = tablet_metadatas.at(merged_tablet);
    EXPECT_EQ(0, merged->sstable_meta().sstables_size());
    EXPECT_EQ(0, merged->orphan_files_size());
    EXPECT_EQ(2, merged->rowsets_size());
}

// Two PK sources each carrying a delete-predicate rowset at the same version with an identical payload merge
// into a single predicate rowset. Each source minted its own uid for it, so the merge must key the predicate on
// version and payload rather than on uid.
TEST_F(LakeTabletMergerTest, test_tablet_merging_delete_predicate_dedup_unchanged_pk) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_pred_child = [&](int64_t tid, int tablet_lower, int tablet_upper) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tid);
        meta->set_version(base_version);
        meta->set_next_rowset_id(11);
        auto* schema = meta->mutable_schema();
        schema->set_keys_type(PRIMARY_KEYS);
        schema->set_id(7777);
        pr1_helpers::set_int_range(meta->mutable_range(), tablet_lower, tablet_upper);
        // Pure delete-predicate rowset: no segments, no del_files, just a predicate.
        auto* rowset = meta->add_rowsets();
        rowset->set_id(10);
        rowset->set_version(base_version);
        rowset->set_num_rows(0);
        rowset->set_data_size(0);
        auto* pred = rowset->mutable_delete_predicate();
        pred->set_version(base_version); // required field
        pred->mutable_in_predicates();   // make has_delete_predicate true
        // No uid here: put_merge_sources mints one per source, as Tablet::delete_data does per tablet.
        return meta;
    };

    auto meta_a = make_pred_child(child_a, 0, 10);
    auto meta_b = make_pred_child(child_b, 10, 20);

    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(9);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));
    auto merged = tablet_metadatas.at(merged_tablet);
    // Same version, same payload: one predicate rowset, although the two occurrences carry different uids.
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_TRUE(merged->rowsets(0).has_delete_predicate());
}

// MERGE: child_a's own .idx (col5) is fully tombstoned; child_b's own .idx (col5,col6) has only col5 tombstoned.
// The merged target must install no entry for the dead .idx (vacuum no-fully-tombstoned rule) and orphan it once,
// while the partially tombstoned entry survives with its tombstone.
TEST_F(LakeTabletMergerTest, test_tablet_merging_idg_drops_fully_tombstoned) {
    const int64_t base_version = 1, new_version = 2;
    const int64_t child_a = next_id(), child_b = next_id(), merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_child = [&](int64_t tablet_id, const std::string& segment_filename) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(3);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        auto* sm = rowset->add_segment_metas();
        sm->set_filename(segment_filename);
        sm->set_size(100);
        sm->set_num_rows(10);
        return meta;
    };
    auto meta_a = make_child(child_a, "seg_a.dat");
    add_idg_with_key(meta_a.get(), 1, "dead_a.idx", /*col_uid=*/5, BITMAP, 1, /*shared_file=*/false);
    add_idg_dropped_key(meta_a.get(), 1, /*col_uid=*/5, BITMAP);
    auto meta_b = make_child(child_b, "seg_b.dat");
    add_idg_with_key(meta_b.get(), 1, "live_b.idx", /*col_uid=*/5, BITMAP, 1, /*shared_file=*/false);
    add_idg_key(meta_b.get(), 1, /*col_uid=*/6, BITMAP);
    add_idg_dropped_key(meta_b.get(), 1, /*col_uid=*/5, BITMAP);
    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);
    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(2, merged->rowsets_size());
    int dead_entries = 0;
    int live_entries = 0;
    for (const auto& [rssid, ver] : merged->idg_meta().idgs()) {
        (void)rssid;
        for (const auto& e : ver.entries()) {
            if (e.index_file() == "dead_a.idx") ++dead_entries;
            if (e.index_file() != "live_b.idx") continue;
            ++live_entries;
            ASSERT_EQ(2, e.keys_size());
            ASSERT_EQ(1, e.dropped_keys_size()) << "a partial tombstone must survive the merge";
            EXPECT_EQ(5, e.dropped_keys(0).col_unique_id());
            EXPECT_FALSE(e.shared_file());
        }
    }
    EXPECT_EQ(0, dead_entries) << "fully-tombstoned IDG entry must not be installed";
    EXPECT_EQ(1, live_entries) << "an entry with an active key must be installed";
    // The dead .idx must be orphaned exactly once so vacuum reclaims it (mirrors apply_drop_index); the live one
    // must not be.
    EXPECT_EQ(1, std::count_if(merged->orphan_files().begin(), merged->orphan_files().end(),
                               [](const auto& file) { return file.name() == "dead_a.idx"; }));
    EXPECT_EQ(0, std::count_if(merged->orphan_files().begin(), merged->orphan_files().end(),
                               [](const auto& file) { return file.name() == "live_b.idx"; }));
}

// MERGE: child_a carries a STALE idg entry for a segment it no longer owns, keyed at an rssid that is live in the
// merged tablet only because child_b's segment lands there. The source-live check must omit it: stale.idx must
// appear nowhere in the target, neither as an entry nor in orphan_files (the stale .idx is reclaimed by the
// source's own file cleanup). next_rowset_id(2) makes child_b remap to rssid 2 so the stale target IS live,
// exercising the source-live branch.
TEST_F(LakeTabletMergerTest, test_tablet_merging_idg_skips_stale_source_entry) {
    const int64_t base_version = 1, new_version = 2;
    const int64_t child_a = next_id(), child_b = next_id(), merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(2); // so child_b's rowset remaps to rssid 2
    {
        auto* rowset = meta_a->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("seg_a.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
        stamp_physical_identity_uid(rowset, "seg_a.dat");
    }
    // Stale idg keyed at rssid 2 -- child_a has NO segment there.
    add_idg_with_key(meta_a.get(), /*segment_id=*/2, "stale.idx", /*col_uid=*/5, BITMAP, 1, /*shared_file=*/false);

    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(2);
    {
        auto* rowset = meta_b->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("seg_b.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
        stamp_physical_identity_uid(rowset, "seg_b.dat"); // distinct uid => not deduped
    }

    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);
    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    auto rowset_b = std::find_if(merged->rowsets().begin(), merged->rowsets().end(),
                                 [](const auto& rowset) { return rowset.segment_metas(0).filename() == "seg_b.dat"; });
    ASSERT_NE(merged->rowsets().end(), rowset_b);
    ASSERT_EQ(2, rowset_b->id()) << "the fixture relies on child_b's segment making the stale rssid live";
    for (const auto& [rssid, ver] : merged->idg_meta().idgs()) {
        for (const auto& e : ver.entries()) {
            EXPECT_NE("stale.idx", e.index_file()) << "stale source idg entry must be skipped";
        }
    }
    for (const auto& file : merged->orphan_files()) {
        EXPECT_NE("stale.idx", file.name()) << "a stale source idg entry must not be orphaned by the target";
    }
}

// Two contexts with distinct private segments each contribute a canonical atom
// [1,2). Their primary runs pack to different target RSSIDs, and both real .idx
// entries survive under those targets rather than being dropped.
TEST_F(LakeTabletMergerTest, test_tablet_merging_idg_remaps_private_segments) {
    const int64_t base_version = 1, new_version = 2;
    const int64_t child_a = next_id(), child_b = next_id(), merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_child = [&](int64_t tablet_id, const std::string& seg, const std::string& idx) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(2);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        auto* sm = rowset->add_segment_metas();
        sm->set_filename(seg);
        sm->set_size(100);
        sm->set_num_rows(10);
        stamp_physical_identity_uid(rowset, seg); // distinct seed per child => distinct uid
        // Exclusive (private) segment => its .idx is shared_file=false, as split's
        // propagate_pruned would have marked it. merge must PRESERVE this (not force true).
        add_idg_with_key(meta.get(), 1, idx, /*col_uid=*/5, BITMAP, 1, /*shared_file=*/false);
        return meta;
    };
    ASSERT_OK(put_merge_sources(make_child(child_a, "seg_a.dat", "a.idx"), make_child(child_b, "seg_b.dat", "b.idx")));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);
    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(2, merged->rowsets_size()) << "distinct-uid rowsets both kept";
    ASSERT_TRUE(merged->has_idg_meta());
    ASSERT_EQ(2, merged->idg_meta().idgs().size()) << "both private .idx remapped and retained";
    std::set<std::string> files;
    for (const auto& [rssid, ver] : merged->idg_meta().idgs()) {
        for (const auto& e : ver.entries()) {
            files.insert(e.index_file());
            EXPECT_FALSE(e.shared_file()) << "private-segment .idx flag must be preserved, not forced shared";
        }
    }
    EXPECT_EQ(1u, files.count("a.idx"));
    EXPECT_EQ(1u, files.count("b.idx"));
}

// =============================================================================
// Reachability nets for the merge-path failpoints. Same shape as the reshard-path ones: ARMED first
// (so the failure returns before any metadata is written and the disarmed run below still takes the
// full path rather than the metacache retry fast path), then disarmed on fresh tablet ids.
// =============================================================================

// =============================================================================
// The three merge file-write hooks. Each sits after its file is durable and before any metadata
// references it -- the orphan-file window. Each fixture must actually reach its own phase, so these
// reuse the shapes of the existing tests that exercise those phases.
// =============================================================================

// A merge writes one delvec file holding every live source page. Primary-key only, and skipped entirely when no
// source has a live delvec page, so both sources carry one here.
TEST_F(LakeTabletMergerTest, test_tablet_merging_delvec_failure_atomic_by_phase) {
    constexpr int64_t kVersion = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    const std::string segment_a = "atomic-a.dat";
    const std::string segment_b = "atomic-b.dat";
    auto meta_a = make_allocator_source(child_a, /*next_rowset_id=*/2);
    add_allocator_rowset(meta_a.get(), /*rowset_id=*/1, /*version=*/1, segment_a);
    auto meta_b = make_allocator_source(child_b, /*next_rowset_id=*/2);
    add_allocator_rowset(meta_b.get(), /*rowset_id=*/1, /*version=*/1, segment_b);
    DelVector delvec_a;
    const uint32_t deleted_a = 3;
    delvec_a.init(/*version=*/10, &deleted_a, 1);
    const std::string bytes_a = delvec_a.save();
    add_delvec(meta_a.get(), child_a, /*version=*/10, /*segment_id=*/1, "atomic-a.delvec", bytes_a);
    DelVector delvec_b;
    const uint32_t deleted_b = 7;
    delvec_b.init(/*version=*/12, &deleted_b, 1);
    const std::string bytes_b = delvec_b.save();
    add_delvec(meta_b.get(), child_b, /*version=*/12, /*segment_id=*/1, "atomic-b.delvec", bytes_b);
    const std::string meta_a_before = meta_a->SerializeAsString();
    const std::string meta_b_before = meta_b->SerializeAsString();
    std::set<std::string> allowed_target_outputs;

    auto publish = [&](int64_t target, int64_t txn, std::unordered_map<int64_t, TabletMetadataPtr>* published) {
        return publish_resharding_merge({meta_a, meta_b}, target, /*base_version=*/1, kVersion, txn, *published);
    };
    auto rssid_of = [](const TabletMetadataPB& metadata, const std::string& segment_name) {
        auto rowset = std::find_if(metadata.rowsets().begin(), metadata.rowsets().end(), [&](const auto& candidate) {
            return candidate.segment_metas(0).filename() == segment_name;
        });
        EXPECT_NE(metadata.rowsets().end(), rowset) << segment_name;
        return rowset == metadata.rowsets().end() ? 0 : rowset->id();
    };
    auto run_phase = [&](std::string_view seam, int fail_on_call) {
        SCOPED_TRACE(seam);
        const int64_t target = next_id();
        prepare_tablet_dirs(target);
        ASSIGN_OR_ABORT(const auto shared_inventory_before, delvec_inventory(child_a));
        const std::string message = "injected publish delvec " + std::string(seam);
        int calls = 0;
        auto* sync = SyncPoint::GetInstance();
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
        sync->SetCallBack(std::string(seam), [&](void* arg) {
            if (++calls == fail_on_call) *static_cast<Status*>(arg) = Status::InternalError(message);
        });
        sync->EnableProcessing();
        std::unordered_map<int64_t, TabletMetadataPtr> published;
        const int64_t failed_txn = next_id();
        const Status status = publish(target, failed_txn, &published);
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
        EXPECT_EQ(message, status.message()) << status;
        EXPECT_FALSE(published.contains(target));
        expect_target_version_not_published(target, kVersion);
        EXPECT_EQ(meta_a_before, meta_a->SerializeAsString());
        EXPECT_EQ(meta_b_before, meta_b->SerializeAsString());
        ASSIGN_OR_ABORT(const auto shared_inventory_after, delvec_inventory(child_a));
        std::set<std::string> failed_target_outputs;
        std::set_difference(shared_inventory_after.begin(), shared_inventory_after.end(),
                            shared_inventory_before.begin(), shared_inventory_before.end(),
                            std::inserter(failed_target_outputs, failed_target_outputs.end()));
        ASSERT_EQ(1, failed_target_outputs.size());
        const std::string failed_prefix = fmt::format("{:016x}_", failed_txn);
        EXPECT_TRUE(failed_target_outputs.begin()->starts_with(failed_prefix));
        allowed_target_outputs.insert(*failed_target_outputs.begin());
        auto without_allowed_target_outputs = [&](std::set<std::string> inventory) {
            for (const auto& target_output : allowed_target_outputs) inventory.erase(target_output);
            return inventory;
        };
        EXPECT_EQ(without_allowed_target_outputs(shared_inventory_before),
                  without_allowed_target_outputs(shared_inventory_after));

        std::unordered_map<int64_t, TabletMetadataPtr> retried;
        const int64_t retry_txn = next_id();
        ASSERT_OK(publish(target, retry_txn, &retried));
        ASSERT_TRUE(retried.contains(target));
        const auto& merged = *retried.at(target);
        ASSERT_EQ(2, merged.delvec_meta().delvecs().size());
        ASSERT_TRUE(merged.delvec_meta().version_to_file().contains(kVersion));
        const auto& retry_output = merged.delvec_meta().version_to_file().at(kVersion).name();
        EXPECT_TRUE(retry_output.starts_with(fmt::format("{:016x}_", retry_txn)));
        ASSIGN_OR_ABORT(const auto shared_inventory_after_retry, delvec_inventory(child_a));
        std::set<std::string> retry_target_outputs;
        std::set_difference(shared_inventory_after_retry.begin(), shared_inventory_after_retry.end(),
                            shared_inventory_after.begin(), shared_inventory_after.end(),
                            std::inserter(retry_target_outputs, retry_target_outputs.end()));
        ASSERT_EQ(std::set<std::string>({retry_output}), retry_target_outputs);
        allowed_target_outputs.insert(retry_output);
        EXPECT_EQ(without_allowed_target_outputs(shared_inventory_before),
                  without_allowed_target_outputs(shared_inventory_after_retry));
        DelVector loaded_a;
        DelVector loaded_b;
        LakeIOOptions options;
        ASSERT_OK(get_del_vec(_tablet_manager.get(), merged, rssid_of(merged, segment_a), false, options, &loaded_a));
        ASSERT_OK(get_del_vec(_tablet_manager.get(), merged, rssid_of(merged, segment_b), false, options, &loaded_b));
        EXPECT_EQ(bytes_a, loaded_a.save());
        EXPECT_EQ(bytes_b, loaded_b.save());
    };

    run_phase("write_compacted_delvec_pages:before_read_chunk", 1);
    run_phase("append_delvec_bytes_bounded:before_chunk", 2);
    run_phase("write_compacted_delvec_pages:before_close", 1);
    run_phase("write_compacted_delvec_pages:before_apply_offsets", 1);
}

TEST_F(LakeTabletMergerTest, test_merge_failpoint_after_write_delvec) {
    constexpr int64_t kBaseVersion = 1;
    constexpr int64_t kNewVersion = 2;
    const int64_t tablet_a = next_id();
    const int64_t tablet_b = next_id();
    const int64_t target_tablet = next_id();
    for (int64_t tablet_id : {tablet_a, tablet_b, target_tablet}) prepare_tablet_dirs(tablet_id);
    auto build = [&](int64_t tablet_id, uint32_t rowset_id, int64_t schema_id, const std::string& delvec_name,
                     const std::string& delvec_content) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(kBaseVersion);
        meta->set_next_rowset_id(rowset_id + 1);
        set_primary_key_schema(meta.get(), schema_id);
        add_rowset(meta.get(), rowset_id, 7, 1);
        add_delvec(meta.get(), tablet_id, kBaseVersion, rowset_id, delvec_name, delvec_content);
        return meta;
    };
    DelVector delvec_a;
    const uint32_t deleted_a = 4;
    delvec_a.init(/*version=*/kBaseVersion, &deleted_a, 1);
    DelVector delvec_b;
    const uint32_t deleted_b = 7;
    delvec_b.init(/*version=*/kBaseVersion, &deleted_b, 1);
    DelVector expected_merged_delvec;
    const uint32_t expected_deleted[] = {deleted_a};
    expected_merged_delvec.init(/*version=*/kNewVersion, expected_deleted, std::size(expected_deleted));
    auto meta_a = build(tablet_a, 10, 1001, "delvec-a", delvec_a.save());
    auto meta_b = build(tablet_b, 1, 2002, "delvec-b", delvec_b.save());
    ASSERT_OK(put_merge_sources(meta_a, meta_b));
    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(tablet_a);
    merging_tablet.add_old_tablet_ids(tablet_b);
    merging_tablet.set_new_tablet_id(target_tablet);
    auto run_merge = [&](int64_t txn_id) {
        TxnInfoPB txn_info;
        txn_info.set_txn_id(txn_id);
        txn_info.set_commit_time(1);
        txn_info.set_gtid(1);
        std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
        std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
        return lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, kBaseVersion, kNewVersion,
                                               txn_info, false, tablet_metadatas, tablet_ranges);
    };
    ASSIGN_OR_ABORT(const auto before_inventory, delvec_inventory(target_tablet));
    auto txn_files = [&](int64_t txn_id) {
        ASSIGN_OR_ABORT(const auto inventory, delvec_inventory(target_tablet));
        std::set<std::string> files;
        const std::string prefix = fmt::format("{:016x}_", txn_id);
        for (const auto& name : inventory) {
            if (name.starts_with(prefix) && name.ends_with(".delvec")) files.insert(name);
        }
        return files;
    };
    set_failpoint_mode("tablet_merge_after_write_delvec", FailPointTriggerModeType::ENABLE);
    auto armed = run_merge(/*txn_id=*/1);
    set_failpoint_mode("tablet_merge_after_write_delvec", FailPointTriggerModeType::DISABLE);
    EXPECT_FALSE(armed.ok()) << "hook not reached after the merged delvec file was written";
    const auto failed_files = txn_files(/*txn_id=*/1);
    ASSERT_EQ(1, failed_files.size());
    expect_target_version_not_published(target_tablet, kNewVersion);
    ASSIGN_OR_ABORT(const auto after_failure_inventory, delvec_inventory(target_tablet));
    EXPECT_TRUE(std::includes(after_failure_inventory.begin(), after_failure_inventory.end(), before_inventory.begin(),
                              before_inventory.end()));

    ASSERT_OK(run_merge(/*txn_id=*/3));
    ASSIGN_OR_ABORT(auto retry_metadata, _tablet_manager->get_tablet_metadata(target_tablet, kNewVersion));
    ASSERT_TRUE(retry_metadata->delvec_meta().version_to_file().contains(kNewVersion));
    const auto& retry_file = retry_metadata->delvec_meta().version_to_file().at(kNewVersion);
    EXPECT_TRUE(retry_file.name().starts_with("0000000000000003_"));
    const auto retry_files = txn_files(/*txn_id=*/3);
    ASSERT_EQ(1, retry_files.size());
    const auto source_a_output =
            std::find_if(retry_metadata->rowsets().begin(), retry_metadata->rowsets().end(), [&](const auto& rowset) {
                return lake::tablet_reshard_helper::same_rowset_uid(rowset, meta_a->rowsets(0));
            });
    ASSERT_NE(retry_metadata->rowsets().end(), source_a_output);
    const uint32_t source_a_rssid = source_a_output->id();
    DelVector loaded;
    LakeIOOptions options;
    ASSERT_OK(get_del_vec(_tablet_manager.get(), *retry_metadata, source_a_rssid, false, options, &loaded));
    EXPECT_EQ(expected_merged_delvec.save(), loaded.save());

    VacuumFullRequest request;
    request.set_partition_id(1);
    request.set_tablet_id(target_tablet);
    request.set_min_active_txn_id(2);
    request.set_grace_timestamp(time(nullptr) + 1);
    request.set_min_check_version(0);
    request.set_max_check_version(1);
    VacuumFullResponse response;
    vacuum_full(_tablet_manager.get(), request, &response);
    ASSERT_TRUE(response.has_status());
    EXPECT_EQ(0, response.status().status_code());
    for (const auto& failed : failed_files) EXPECT_FALSE(delvec_inventory(target_tablet).value().contains(failed));
    for (const auto& retried : retry_files) EXPECT_TRUE(delvec_inventory(target_tablet).value().contains(retried));
    ASSIGN_OR_ABORT(auto post_vacuum_metadata, _tablet_manager->get_tablet_metadata(target_tablet, kNewVersion));
    ASSERT_TRUE(post_vacuum_metadata->delvec_meta().version_to_file().contains(kNewVersion));
    const auto& post_vacuum_file = post_vacuum_metadata->delvec_meta().version_to_file().at(kNewVersion);
    EXPECT_EQ(retry_file.name(), post_vacuum_file.name());
    EXPECT_TRUE(post_vacuum_file.name().starts_with("0000000000000003_"));
    for (const auto& failed : failed_files) EXPECT_NE(failed, post_vacuum_file.name());
    DelVector reloaded;
    ASSERT_OK(get_del_vec(_tablet_manager.get(), *post_vacuum_metadata, source_a_rssid, false, options, &reloaded));
    EXPECT_EQ(expected_merged_delvec.save(), reloaded.save());
}

TEST_F(LakeTabletMergerTest, test_merge_retry_cache_complete_returns_without_recompute) {
    constexpr int64_t kVersion = 2;
    constexpr int64_t kGtid = 81;
    const int64_t source0 = next_id();
    const int64_t source1 = next_id();
    const int64_t target = next_id();
    for (auto id : {source0, source1, target}) prepare_tablet_dirs(id);
    auto base0 = std::make_shared<TabletMetadataPB>();
    base0->set_id(source0);
    base0->set_version(1);
    auto base1 = std::make_shared<TabletMetadataPB>();
    base1->set_id(source1);
    base1->set_version(1);
    ASSERT_OK(put_merge_sources(base0, base1));
    ReshardingTabletInfoPB request;
    auto* merge = request.mutable_merging_tablet_info();
    merge->add_old_tablet_ids(source0);
    merge->add_old_tablet_ids(source1);
    merge->set_new_tablet_id(target);
    std::unordered_map<int64_t, TabletMetadataPtr> expected;
    for (auto id : {source0, source1, target}) {
        expected.emplace(id, cache_reshard_metadata(id, id, kVersion, kGtid));
    }
    // A recomputed merge would have to read the sources back.
    for (auto id : {source0, source1}) {
        _tablet_manager->metacache()->erase(_tablet_manager->tablet_metadata_location(id, 1));
    }

    int metadata_reads = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("TabletManager::load_tablet_metadata:path", [&](void*) { ++metadata_reads; });
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->DisableProcessing();
        sync->ClearAllCallBacks();
    });
    TxnInfoPB txn;
    txn.set_gtid(kGtid);
    std::unordered_map<int64_t, TabletMetadataPtr> actual;
    std::unordered_map<int64_t, TabletRangePB> actual_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), request, 1, kVersion, txn, false, actual,
                                              actual_ranges));
    EXPECT_EQ(0, metadata_reads);
    ASSERT_EQ(expected.size(), actual.size());
    for (const auto& [id, metadata] : expected) {
        EXPECT_EQ(metadata->SerializeAsString(), actual.at(id)->SerializeAsString());
    }
}

// A merge inherits every file of its sources by reference. It therefore admits only sources that own their files
// alone, declare the key ranges the merge reasons about, agree on table-level configuration, and keep the
// coordinates of one logical rowset apart. Every refusal happens before anything is written.

TEST_F(LakeTabletMergerTest, test_merge_rejects_a_source_that_shares_a_data_file) {
    enum class Kind { kSegment, kDelFile, kDcg, kIdg, kSstable };
    for (auto kind : {Kind::kSegment, Kind::kDelFile, Kind::kDcg, Kind::kIdg, Kind::kSstable}) {
        SCOPED_TRACE(static_cast<int>(kind));
        const std::string stem = fmt::format("shared_{}_{}", static_cast<int>(kind), next_id());
        std::vector<std::shared_ptr<TabletMetadataPB>> sources;
        if (kind == Kind::kSstable) {
            sources = make_preflight_sst_sources(stem, /*shared_files=*/false);
        } else {
            sources = {make_preflight_sidecar_source(next_id(), stem + "_a.dat"),
                       make_preflight_sidecar_source(next_id(), stem + "_b.dat")};
        }
        auto& b = *sources[1];
        switch (kind) {
        case Kind::kSegment:
            b.mutable_rowsets(0)->mutable_segment_metas(0)->set_shared(true);
            break;
        case Kind::kDelFile: {
            auto* del = b.mutable_rowsets(0)->add_del_files();
            del->set_name(stem + "_b.del");
            del->set_origin_rowset_id(b.rowsets(0).id());
            del->set_op_offset(0);
            del->set_shared(true);
            break;
        }
        case Kind::kDcg:
            add_dcg_with_columns(&b, /*segment_id=*/1, stem + "_b.cols", {2}, /*version=*/1, /*shared_file=*/true);
            break;
        case Kind::kIdg:
            add_idg_with_key(&b, /*segment_id=*/1, stem + "_b.idx", /*col_uid=*/2, BITMAP, /*version=*/1,
                             /*shared_file=*/true);
            break;
        case Kind::kSstable:
            b.mutable_sstable_meta()->mutable_sstables(0)->set_shared(true);
            break;
        }
        const std::vector<TabletMetadataPtr> immutable(sources.begin(), sources.end());
        auto status = expect_merge_rejected_without_writes(immutable, next_id(), /*target_version=*/2);
        EXPECT_TRUE(status.message().find("shared") != std::string::npos) << status;
    }
}

// Two sources that each declare the same file, both as private, are two owners of one file: the shared flag only
// records what a writer believed, so the file name is what has to be unique.
TEST_F(LakeTabletMergerTest, test_merge_rejects_a_file_declared_by_two_private_sources) {
    enum class Kind { kSegment, kDelFile, kDcg, kIdg, kSstable };
    for (auto kind : {Kind::kSegment, Kind::kDelFile, Kind::kDcg, Kind::kIdg, Kind::kSstable}) {
        SCOPED_TRACE(static_cast<int>(kind));
        const std::string stem = fmt::format("coreferenced_{}_{}", static_cast<int>(kind), next_id());
        std::vector<std::shared_ptr<TabletMetadataPB>> sources;
        if (kind == Kind::kSstable) {
            sources = make_preflight_sst_sources(stem, /*shared_files=*/false);
        } else {
            sources = {make_preflight_sidecar_source(next_id(), stem + "_a.dat"),
                       make_preflight_sidecar_source(next_id(), stem + "_b.dat")};
        }
        auto& a = *sources[0];
        auto& b = *sources[1];
        std::string filename;
        switch (kind) {
        case Kind::kSegment:
            filename = a.rowsets(0).segment_metas(0).filename();
            b.mutable_rowsets(0)->mutable_segment_metas(0)->set_filename(filename);
            break;
        case Kind::kDelFile:
            filename = stem + ".del";
            for (auto* source : {&a, &b}) {
                auto* del = source->mutable_rowsets(0)->add_del_files();
                del->set_name(filename);
                del->set_origin_rowset_id(source->rowsets(0).id());
                del->set_op_offset(0);
            }
            break;
        case Kind::kDcg:
            filename = stem + ".cols";
            for (auto* source : {&a, &b}) {
                add_dcg_with_columns(source, /*segment_id=*/1, filename, {2}, /*version=*/1, /*shared_file=*/false);
            }
            break;
        case Kind::kIdg:
            filename = stem + ".idx";
            for (auto* source : {&a, &b}) {
                add_idg_with_key(source, /*segment_id=*/1, filename, /*col_uid=*/2, BITMAP, /*version=*/1,
                                 /*shared_file=*/false);
            }
            break;
        case Kind::kSstable:
            filename = a.sstable_meta().sstables(0).filename();
            b.mutable_sstable_meta()->mutable_sstables(0)->set_filename(filename);
            break;
        }
        const std::vector<TabletMetadataPtr> immutable(sources.begin(), sources.end());
        auto status = expect_merge_rejected_without_writes(immutable, next_id(), /*target_version=*/2);
        EXPECT_TRUE(status.message().find(filename) != std::string::npos) << status;
    }
}

TEST_F(LakeTabletMergerTest, test_merge_rejects_a_source_without_a_range) {
    auto publish = [&](bool declare_range) -> Status {
        auto source = make_preflight_sidecar_source(next_id(), fmt::format("rangeless_{}.dat", next_id()));
        if (declare_range) source->mutable_range();
        const int64_t target = next_id();
        prepare_tablet_dirs(source->id());
        prepare_tablet_dirs(target);
        RETURN_IF_ERROR(_tablet_manager->put_tablet_metadata(source));
        ReshardingTabletInfoPB resharding;
        auto* merging = resharding.mutable_merging_tablet_info();
        merging->add_old_tablet_ids(source->id());
        merging->set_new_tablet_id(target);
        TxnInfoPB txn_info;
        txn_info.set_txn_id(next_id());
        txn_info.set_commit_time(1);
        txn_info.set_gtid(1);
        std::unordered_map<int64_t, TabletMetadataPtr> published;
        std::unordered_map<int64_t, TabletRangePB> ranges;
        auto status = lake::publish_resharding_tablet(_tablet_manager.get(), resharding, /*base_version=*/1,
                                                      /*new_version=*/2, txn_info, false, published, ranges);
        if (!status.ok()) expect_target_version_not_published(target, 2);
        return status;
    };
    EXPECT_OK(publish(/*declare_range=*/true));
    auto status = publish(/*declare_range=*/false);
    EXPECT_TRUE(status.is_corruption()) << status;
    EXPECT_TRUE(status.message().find("range") != std::string::npos) << status;
}

// Every producer of a primary-key index sstable records its key range, so an sstable without one cannot be shown to
// hold only its own source's keys. Neither can one whose range reaches outside its source, or one listed twice.
TEST_F(LakeTabletMergerTest, test_merge_rejects_an_sstable_it_cannot_attribute_to_its_source) {
    enum class Case { kNoRange, kRangeOutsideSource, kListedTwice };
    for (auto c : {Case::kNoRange, Case::kRangeOutsideSource, Case::kListedTwice}) {
        SCOPED_TRACE(static_cast<int>(c));
        auto sources = make_preflight_sst_sources(fmt::format("unattributable_{}_{}", static_cast<int>(c), next_id()),
                                                  /*shared_files=*/false);
        auto& b = *sources[1];
        auto* sst = b.mutable_sstable_meta()->mutable_sstables(0);
        const std::string filename = sst->filename();
        switch (c) {
        case Case::kNoRange:
            sst->clear_range();
            break;
        case Case::kRangeOutsideSource:
            // The second source owns [50, 100).
            sst->mutable_range()->set_start_key(encode_int_primary_key(10));
            sst->mutable_range()->set_end_key(encode_int_primary_key(40));
            break;
        case Case::kListedTwice:
            *b.mutable_sstable_meta()->add_sstables() = *sst;
            break;
        }
        const std::vector<TabletMetadataPtr> immutable(sources.begin(), sources.end());
        auto status = expect_merge_rejected_without_writes(immutable, next_id(), /*target_version=*/2);
        EXPECT_TRUE(status.message().find(filename) != std::string::npos) << status;
    }
}

// The merged tablet starts as a copy of one source, so its table-level configuration is everyone's only if the
// sources agree on it.
TEST_F(LakeTabletMergerTest, test_merge_rejects_sources_that_disagree_on_table_configuration) {
    for (int field = 0; field < 5; ++field) {
        SCOPED_TRACE(field);
        const std::string stem = fmt::format("configuration_{}_{}", field, next_id());
        auto a = make_preflight_sidecar_source(next_id(), stem + "_a.dat");
        auto b = make_preflight_sidecar_source(next_id(), stem + "_b.dat");
        if (field <= 1) {
            // A primary-key tablet is normalized to the cloud-native index when its metadata is loaded, so only
            // other key types can carry differing index settings.
            for (auto* source : {a.get(), b.get()}) source->mutable_schema()->set_keys_type(DUP_KEYS);
        }
        switch (field) {
        case 0:
            a->set_enable_persistent_index(true);
            b->set_enable_persistent_index(false);
            break;
        case 1:
            a->set_enable_persistent_index(true);
            b->set_enable_persistent_index(true);
            a->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
            b->set_persistent_index_type(PersistentIndexTypePB::LOCAL);
            break;
        case 2:
            b->set_compaction_strategy(CompactionStrategyPB::REAL_TIME);
            break;
        case 3:
            b->mutable_flat_json_config()->set_flat_json_enable(true);
            break;
        case 4:
            b->mutable_source_schema()->CopyFrom(b->schema());
            break;
        }
        auto status = expect_merge_rejected_without_writes({a, b}, next_id(), /*target_version=*/2);
        const char* fields[] = {"enable_persistent_index", "persistent_index_type", "compaction_strategy",
                                "flat_json_config", "source_schema"};
        EXPECT_TRUE(status.message().contains(fmt::format("disagree on {}", fields[field]))) << status;
    }
}

// Rowsets that share a uid are pieces of one logical rowset (a split hands each child the segments it owns). Pieces
// whose segment indices are disjoint stay separate rowsets of the merged tablet, each keeping its own statistics and
// its own delete file -- even when the two delete files sit at the same raw coordinate, which is each source's own
// numbering and says nothing across sources.
TEST_F(LakeTabletMergerTest, test_merge_keeps_same_uid_rowsets_whose_coordinates_are_disjoint) {
    const std::string stem = fmt::format("same_uid_disjoint_{}", next_id());
    auto a = make_preflight_sidecar_source(next_id(), stem + "_a.dat");
    auto b = make_preflight_sidecar_source(next_id(), stem + "_b.dat");
    b->mutable_rowsets(0)->mutable_uid()->CopyFrom(a->rowsets(0).uid());
    b->mutable_rowsets(0)->mutable_segment_metas(0)->set_segment_idx(1);
    b->set_next_rowset_id(3);
    for (auto* source : {a.get(), b.get()}) {
        auto* del = source->mutable_rowsets(0)->add_del_files();
        del->set_name(fmt::format("{}_{}.del", stem, source->id()));
        del->set_origin_rowset_id(1);
        del->set_op_offset(0);
    }

    ASSIGN_OR_ABORT(auto merged, publish_verbatim_merge({a, b}, next_id(), /*target_version=*/2));
    ASSERT_EQ(2, merged->rowsets_size());
    std::multiset<uint32_t> segment_indices;
    for (const auto& rowset : merged->rowsets()) {
        EXPECT_TRUE(lake::tablet_reshard_helper::same_rowset_uid(rowset, a->rowsets(0)));
        ASSERT_EQ(1, rowset.segment_metas_size());
        segment_indices.insert(rowset.segment_metas(0).segment_idx());
        EXPECT_EQ(1, rowset.num_rows());
        EXPECT_EQ(1, rowset.data_size());
        ASSERT_EQ(1, rowset.del_files_size());
        EXPECT_EQ(rowset.id(), rowset.del_files(0).origin_rowset_id());
    }
    EXPECT_EQ((std::multiset<uint32_t>{0, 1}), segment_indices);
}

TEST_F(LakeTabletMergerTest, test_merge_rejects_same_uid_rowsets_whose_coordinates_overlap) {
    enum class Case { kSegmentAcrossSources, kSegmentInsideOneSource };
    for (auto c : {Case::kSegmentAcrossSources, Case::kSegmentInsideOneSource}) {
        SCOPED_TRACE(static_cast<int>(c));
        const std::string stem = fmt::format("same_uid_overlap_{}_{}", static_cast<int>(c), next_id());
        auto a = make_preflight_sidecar_source(next_id(), stem + "_a.dat");
        auto b = make_preflight_sidecar_source(next_id(), stem + "_b.dat");
        std::vector<TabletMetadataPtr> sources{a, b};
        std::string claim;
        switch (c) {
        case Case::kSegmentAcrossSources:
            b->mutable_rowsets(0)->mutable_uid()->CopyFrom(a->rowsets(0).uid());
            claim = "segment index 0";
            break;
        case Case::kSegmentInsideOneSource: {
            auto* second = add_allocator_rowset(a.get(), /*rowset_id=*/2, /*version=*/1, stem + "_a2.dat");
            second->mutable_uid()->CopyFrom(a->rowsets(0).uid());
            a->set_next_rowset_id(3);
            claim = "segment index 0";
            break;
        }
        }
        auto status = expect_merge_rejected_without_writes(sources, next_id(), /*target_version=*/2);
        EXPECT_TRUE(status.message().find("same logical rowset") != std::string::npos) << status;
        EXPECT_TRUE(status.message().find(claim) != std::string::npos) << status;
    }
}

TEST_F(LakeTabletMergerTest, test_merge_rejection_is_marked_and_counted) {
    const std::string stem = fmt::format("marked_{}", next_id());
    auto a = make_preflight_sidecar_source(next_id(), stem + "_a.dat");
    auto b = make_preflight_sidecar_source(next_id(), stem + "_b.dat", /*shared_segment=*/true);
    const int64_t before = merge_rejected_total();
    auto status = expect_merge_rejected_without_writes({a, b}, next_id(), /*target_version=*/2);
    EXPECT_TRUE(status.message().starts_with("MERGE_REJECTED: ")) << status;
    EXPECT_EQ(before + 1, merge_rejected_total());
}

TEST_F(LakeTabletMergerTest, test_merge_admits_sources_that_own_every_file_alone) {
    const std::string stem = fmt::format("admitted_{}", next_id());
    auto sources = make_preflight_sst_sources(stem, /*shared_files=*/false);
    for (auto& source : sources) {
        auto* del = source->mutable_rowsets(0)->add_del_files();
        del->set_name(fmt::format("{}_{}.del", stem, source->id()));
        del->set_origin_rowset_id(source->rowsets(0).id());
        del->set_op_offset(0);
        add_dcg_with_columns(source.get(), /*segment_id=*/1, fmt::format("{}_{}.cols", stem, source->id()), {2},
                             /*version=*/1, /*shared_file=*/false);
        add_idg_with_key(source.get(), /*segment_id=*/1, fmt::format("{}_{}.idx", stem, source->id()),
                         /*col_uid=*/2, BITMAP, /*version=*/1, /*shared_file=*/false);
    }
    const int64_t before = merge_rejected_total();
    const std::vector<TabletMetadataPtr> immutable(sources.begin(), sources.end());
    ASSIGN_OR_ABORT(auto merged, publish_verbatim_merge(immutable, next_id(), /*target_version=*/2));
    EXPECT_EQ(before, merge_rejected_total());
    ASSERT_EQ(2, merged->rowsets_size());
    EXPECT_EQ(2, merged->dcg_meta().dcgs_size());
    EXPECT_EQ(2, merged->idg_meta().idgs_size());
    ASSERT_EQ(2, merged->sstable_meta().sstables_size());
    for (const auto& sstable : merged->sstable_meta().sstables()) EXPECT_FALSE(sstable.shared());
    EXPECT_FALSE(lake::tablet_reshard_helper::has_shared_files(*merged));
}

// Rowset order. A lake tablet's rowset array is its recency order: merge-on-read, the coverage of a delete predicate
// and the primary-index rebuild all go by array position, not by RowsetMetadataPB.version.

// Each source looks like a tablet whose first two rowsets were compacted: the compaction output sits at array index 0
// (a single-output compaction replaces its first input in place) but carries the newest version, and the rowset after
// it holds newer data under a smaller version. Each source's own array order must survive the merge; a sort by
// version would put the compaction output last.
TEST_F(LakeTabletMergerTest, test_merge_preserves_source_order_across_compaction_output) {
    const int64_t base_version = 2;
    const int64_t new_version = 3;
    const int64_t tablet_a = next_id();
    const int64_t tablet_b = next_id();
    const int64_t new_tablet = next_id();

    prepare_tablet_dirs(tablet_a);
    prepare_tablet_dirs(tablet_b);
    prepare_tablet_dirs(new_tablet);

    TabletMetadataPB meta_a;
    meta_a.set_id(tablet_a);
    meta_a.set_version(base_version);
    meta_a.set_next_rowset_id(3);
    add_tagged_rowset(&meta_a, 1, 40, "a_compaction_output");
    add_tagged_rowset(&meta_a, 2, 30, "a_newer_data");

    TabletMetadataPB meta_b;
    meta_b.set_id(tablet_b);
    meta_b.set_version(base_version);
    meta_b.set_next_rowset_id(3);
    add_tagged_rowset(&meta_b, 1, 40, "b_compaction_output");
    add_tagged_rowset(&meta_b, 2, 30, "b_newer_data");
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    ASSERT_OK(publish_resharding_merge({merge_fixture_copy(meta_a), merge_fixture_copy(meta_b)}, new_tablet,
                                       base_version, new_version, next_id(), tablet_metadatas));

    auto it = tablet_metadatas.find(new_tablet);
    ASSERT_TRUE(it != tablet_metadatas.end());
    const auto tags = merged_tags(it->second);
    ASSERT_EQ(4u, tags.size());

    auto position_of = [&](const std::string& tag) { return tag_position(tags, tag); };
    EXPECT_LT(position_of("a_compaction_output"), position_of("a_newer_data"))
            << "tablet A's compaction output must stay ahead of its newer rowset";
    EXPECT_LT(position_of("b_compaction_output"), position_of("b_newer_data"))
            << "tablet B's compaction output must stay ahead of its newer rowset";
}

// A batch publish stamps every rowset of the batch with one version, so the rowsets are ordered only by array
// position, and a version sort falls through to the rowset uid, which production mints at random. Here uid.lo runs
// against the array order, standing in for the random draws that invert a pair.
TEST_F(LakeTabletMergerTest, test_merge_preserves_source_order_for_equal_versions) {
    const int64_t base_version = 2;
    const int64_t new_version = 3;
    const int64_t tablet_a = next_id();
    const int64_t tablet_b = next_id();
    const int64_t new_tablet = next_id();

    prepare_tablet_dirs(tablet_a);
    prepare_tablet_dirs(tablet_b);
    prepare_tablet_dirs(new_tablet);

    TabletMetadataPB meta_a;
    meta_a.set_id(tablet_a);
    meta_a.set_version(base_version);
    meta_a.set_next_rowset_id(4);
    add_tagged_rowset(&meta_a, 1, 10, "a_first", /*uid_lo=*/900);
    add_tagged_rowset(&meta_a, 2, 10, "a_second", /*uid_lo=*/500);
    add_tagged_rowset(&meta_a, 3, 10, "a_third", /*uid_lo=*/100);

    TabletMetadataPB meta_b;
    meta_b.set_id(tablet_b);
    meta_b.set_version(base_version);
    meta_b.set_next_rowset_id(4);
    add_tagged_rowset(&meta_b, 1, 10, "b_first", /*uid_lo=*/900);
    add_tagged_rowset(&meta_b, 2, 10, "b_second", /*uid_lo=*/500);
    add_tagged_rowset(&meta_b, 3, 10, "b_third", /*uid_lo=*/100);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    ASSERT_OK(publish_resharding_merge({merge_fixture_copy(meta_a), merge_fixture_copy(meta_b)}, new_tablet,
                                       base_version, new_version, next_id(), tablet_metadatas));

    auto it = tablet_metadatas.find(new_tablet);
    ASSERT_TRUE(it != tablet_metadatas.end());
    const auto tags = merged_tags(it->second);
    ASSERT_EQ(6u, tags.size());

    auto position_of = [&](const std::string& tag) { return tag_position(tags, tag); };
    EXPECT_LT(position_of("a_first"), position_of("a_second"));
    EXPECT_LT(position_of("a_second"), position_of("a_third"));
    EXPECT_LT(position_of("b_first"), position_of("b_second"));
    EXPECT_LT(position_of("b_second"), position_of("b_third"));
}

// A lake delete predicate covers the rowsets at or before its array index. The compaction output below holds data
// older than the DELETE but carries a newer version, so a merged tablet ordered by version would move it past the
// predicate and bring the deleted rows back.
TEST_F(LakeTabletMergerTest, test_merge_keeps_delete_predicate_ahead_of_older_compaction_output) {
    const int64_t base_version = 2;
    const int64_t new_version = 3;
    const int64_t tablet_a = next_id();
    const int64_t tablet_b = next_id();
    const int64_t new_tablet = next_id();

    prepare_tablet_dirs(tablet_a);
    prepare_tablet_dirs(tablet_b);
    prepare_tablet_dirs(new_tablet);

    // Array order per source: compaction output (v40, old data) -> DELETE (v25) -> newer data (v30).
    TabletMetadataPB meta_a;
    meta_a.set_id(tablet_a);
    meta_a.set_version(base_version);
    meta_a.set_next_rowset_id(4);
    add_tagged_rowset(&meta_a, 1, 40, "a_compaction_output");
    add_rowset_with_predicate(&meta_a, 2, 25, true);
    add_tagged_rowset(&meta_a, 3, 30, "a_after_delete");

    TabletMetadataPB meta_b;
    meta_b.set_id(tablet_b);
    meta_b.set_version(base_version);
    meta_b.set_next_rowset_id(4);
    add_tagged_rowset(&meta_b, 1, 40, "b_compaction_output");
    add_rowset_with_predicate(&meta_b, 2, 25, true);
    add_tagged_rowset(&meta_b, 3, 30, "b_after_delete");
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    ASSERT_OK(publish_resharding_merge({merge_fixture_copy(meta_a), merge_fixture_copy(meta_b)}, new_tablet,
                                       base_version, new_version, next_id(), tablet_metadatas));

    auto it = tablet_metadatas.find(new_tablet);
    ASSERT_TRUE(it != tablet_metadatas.end());
    const auto& merged = it->second;

    // The two sources' predicates are one delete and merge into one rowset: 2 + 2 data rowsets + 1 predicate.
    ASSERT_EQ(5, merged->rowsets_size());
    int predicate_index = -1;
    for (int i = 0; i < merged->rowsets_size(); ++i) {
        if (merged->rowsets(i).has_delete_predicate()) {
            EXPECT_EQ(-1, predicate_index) << "expected exactly one merged delete predicate";
            predicate_index = i;
        }
    }
    ASSERT_NE(-1, predicate_index);

    const auto tags = merged_tags(merged);
    auto position_of = [&](const std::string& tag) { return tag_position(tags, tag); };
    // Both compaction outputs must stay under the predicate, both later rowsets above it.
    EXPECT_LT(position_of("a_compaction_output"), predicate_index);
    EXPECT_LT(position_of("b_compaction_output"), predicate_index);
    EXPECT_GT(position_of("a_after_delete"), predicate_index);
    EXPECT_GT(position_of("b_after_delete"), predicate_index);
}

// The delete predicates of one version are one delete across sources, so the two sources below share both of their
// predicates but list them in opposite orders. No order satisfies both, and the merge refuses the pair rather than
// pick one. The message names the problem and a rowset that could not be placed.
TEST_F(LakeTabletMergerTest, test_merge_rejects_sources_that_disagree_on_rowset_order) {
    auto make_source = [&](int64_t first_version, int64_t second_version) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(next_id());
        metadata->set_version(2);
        metadata->set_next_rowset_id(3);
        add_rowset_with_predicate(metadata.get(), 1, first_version, true);
        add_rowset_with_predicate(metadata.get(), 2, second_version, true);
        return metadata;
    };
    auto meta_a = make_source(10, 20);
    auto meta_b = make_source(20, 10);
    auto status = expect_merge_rejected_without_writes({meta_a, meta_b}, next_id(), /*target_version=*/3);
    const std::string msg(status.message());
    EXPECT_TRUE(msg.contains("disagree on rowset order")) << msg;
    EXPECT_TRUE(msg.contains("2 of 2 rowsets unordered")) << msg;
    EXPECT_TRUE(msg.contains("source tablet")) << msg;
}

// One source can state the same delete twice with a write between them: a batch publish of DELETE, INSERT, DELETE
// gives the tablet [predicate@v, write@v, predicate@v]. That merges, and the merged predicate lands after the write --
// the second statement covers the first one's rows plus the write between them.
TEST_F(LakeTabletMergerTest, test_merge_orders_repeated_delete_predicate_after_the_write_between_them) {
    const int64_t base_version = 2;
    const int64_t new_version = 3;
    const int64_t tablet_a = next_id();
    const int64_t tablet_b = next_id();
    const int64_t new_tablet = next_id();

    prepare_tablet_dirs(tablet_a);
    prepare_tablet_dirs(tablet_b);
    prepare_tablet_dirs(new_tablet);

    // Tablet A: predicate(v10) -> write(v10) -> predicate(v10), all one batch-published version.
    TabletMetadataPB meta_a;
    meta_a.set_id(tablet_a);
    meta_a.set_version(base_version);
    meta_a.set_next_rowset_id(4);
    add_rowset_with_predicate(&meta_a, 1, 10, true);
    add_tagged_rowset(&meta_a, 2, 10, "a_write");
    add_rowset_with_predicate(&meta_a, 3, 10, true);

    // Tablet B: an unrelated data rowset, so the merge has a second source.
    TabletMetadataPB meta_b;
    meta_b.set_id(tablet_b);
    meta_b.set_version(base_version);
    meta_b.set_next_rowset_id(2);
    add_tagged_rowset(&meta_b, 1, 5, "b_data");
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    ASSERT_OK(publish_resharding_merge({merge_fixture_copy(meta_a), merge_fixture_copy(meta_b)}, new_tablet,
                                       base_version, new_version, next_id(), tablet_metadatas));

    auto it = tablet_metadatas.find(new_tablet);
    ASSERT_TRUE(it != tablet_metadatas.end());
    const auto& merged = it->second;

    int predicate_index = -1;
    int predicate_count = 0;
    for (int i = 0; i < merged->rowsets_size(); ++i) {
        if (merged->rowsets(i).has_delete_predicate()) {
            predicate_count++;
            predicate_index = i;
        }
    }
    EXPECT_EQ(1, predicate_count) << "the two statements of one delete must merge into one rowset";

    const auto tags = merged_tags(merged);
    const int write_index = tag_position(tags, "a_write");
    EXPECT_GT(predicate_index, write_index)
            << "the merged predicate must land after the write it was re-stated behind, or it would stop covering "
               "rows the source already committed as deleted";
}

// A delete predicate stated in every source is one delete; the merged tablet keeps one copy. Splitting the merged
// tablet again gives every child that predicate, and the row it deleted stays deleted in every child.
TEST_F(LakeTabletMergerTest, test_merge_then_split_keeps_the_delete_predicate_in_every_child) {
    auto source = fixed_point_source(/*primary_key=*/false);
    ASSIGN_OR_ABORT(auto expected_rows, read_two_column_rows(source));
    ASSERT_TRUE(
            std::none_of(expected_rows.begin(), expected_rows.end(), [](const auto& row) { return row.first == 99; }));
    auto count_predicates = [](const TabletMetadataPB& metadata) {
        return std::count_if(metadata.rowsets().begin(), metadata.rowsets().end(),
                             [](const auto& rowset) { return rowset.has_delete_predicate(); });
    };

    auto children = split_fixed_point_source(source, 2);
    for (const auto& child : children) EXPECT_EQ(1, count_predicates(*child));
    const int64_t merged_id = next_id();
    std::unordered_map<int64_t, TabletMetadataPtr> published;
    ASSERT_OK(publish_resharding_merge(children, merged_id, children[0]->version(), children[0]->version() + 1,
                                       next_id(), published));
    const auto& merged = published.at(merged_id);
    EXPECT_EQ(1, count_predicates(*merged));
    ASSIGN_OR_ABORT(auto merged_rows, read_two_column_rows(merged));
    EXPECT_EQ(expected_rows, merged_rows);

    auto regrouped = split_fixed_point_source(merged, 3);
    std::vector<std::pair<int32_t, int32_t>> regrouped_rows;
    for (const auto& child : regrouped) {
        EXPECT_EQ(1, count_predicates(*child));
        ASSIGN_OR_ABORT(auto rows, read_two_column_rows(child));
        regrouped_rows.insert(regrouped_rows.end(), rows.begin(), rows.end());
    }
    std::sort(regrouped_rows.begin(), regrouped_rows.end());
    EXPECT_EQ(expected_rows, regrouped_rows);
}

// An .idx file holds the index of one segment, so it belongs to one index entry. Named by the entries of two
// segments, it is attached to two physical bases; named by two identical entries of one segment, it would be
// installed twice, one entry's to drop and the other's to keep. The merge refuses both.
TEST_F(LakeTabletMergerTest, test_merge_rejects_an_idx_file_named_by_two_index_entries) {
    for (bool same_segment : {false, true}) {
        SCOPED_TRACE(same_segment ? "two entries of one segment" : "entries of two segments");
        auto source = make_preflight_sidecar_source(next_id(), fmt::format("idx_twice_{}.dat", next_id()));
        add_allocator_rowset(source.get(), 2, 1, fmt::format("idx_twice_second_{}.dat", source->id()));
        source->set_next_rowset_id(3);
        const std::string idx = fmt::format("idx_twice_{}.idx", source->id());
        add_idg_with_key(source.get(), /*segment_id=*/1, idx, /*col_uid=*/2, BITMAP, /*version=*/1,
                         /*shared_file=*/false);
        add_idg_with_key(source.get(), /*segment_id=*/same_segment ? 1 : 2, idx, /*col_uid=*/2, BITMAP,
                         /*version=*/1, /*shared_file=*/false);
        auto status = expect_merge_rejected_without_writes({source}, next_id(), /*target_version=*/2);
        EXPECT_TRUE(status.message().contains(same_segment ? idx + " is referenced by more than one index entry"
                                                           : idx + " is attached to a different physical base"))
                << status;
    }
}

// Primary-key index SSTs carried by translation.

// The first source's rowset 5 is the output of a compaction whose newest input was rowset 4, and its modern SST names
// rowset 4 as owner, with watermark 5. Rowset 4 has no segment any more but the source still references it, so the
// owner lies inside the source's run [4, 6) and the SST is carried: owner and watermark are translated on their own,
// 4 to 1 and 5 to 2, and the SST gets no delete-vector page because it had none.
TEST_F(LakeTabletMergerTest, test_tablet_merging_projects_a_modern_sst_whose_owner_was_compacted_away) {
    ASSIGN_OR_ABORT(auto result, publish_metadata_only_merge_fixture(
                                         MetadataOnlyMergeShape::kPrivate, /*enable_tde=*/false,
                                         /*with_del_file=*/false, /*skip_source_flush=*/true, [](auto& sources) {
                                             auto* first = sources[0].get();
                                             first->mutable_rowsets(0)->set_id(5);
                                             first->mutable_rowsets(0)->set_max_compact_input_rowset_id(4);
                                             first->set_next_rowset_id(6);
                                             auto* sst = first->mutable_sstable_meta()->mutable_sstables(0);
                                             sst->set_shared_rssid(4);
                                             sst->set_max_rss_rowid(static_cast<uint64_t>(5) << 32);
                                         }));
    const auto& target = result.published.at(result.target_tablet_id);
    ASSERT_EQ(2, target.sstable_meta().sstables_size());
    const auto& modern = target.sstable_meta().sstables(0);
    ASSERT_TRUE(modern.has_shared_rssid());
    EXPECT_EQ(1, modern.shared_rssid());
    EXPECT_EQ(0, modern.rssid_offset());
    EXPECT_EQ(static_cast<uint64_t>(2) << 32, modern.max_rss_rowid());
    EXPECT_FALSE(modern.has_delvec());
    const auto& legacy = target.sstable_meta().sstables(1);
    EXPECT_FALSE(legacy.has_shared_rssid());
    EXPECT_EQ(static_cast<uint64_t>(3) << 32, legacy.max_rss_rowid());
    EXPECT_EQ(4, target.next_rowset_id());
}

// A modern SST's embedded delete vector is its owner's page as of when the SST was written. The first source's page
// has grown since (one deleted row then, two now); the carried SST points at the merged page of its owner, which
// holds both.
TEST_F(LakeTabletMergerTest, test_tablet_merging_reattaches_the_current_delvec_page_to_a_carried_sst) {
    // Leave room below the fixture's base version for the older delete-vector version.
    (void)next_id();
    ASSIGN_OR_ABORT(auto result,
                    publish_metadata_only_merge_fixture(
                            MetadataOnlyMergeShape::kPrivate, /*enable_tde=*/false, /*with_del_file=*/false,
                            /*skip_source_flush=*/true, [&](auto& sources) {
                                auto* first = sources[0].get();
                                const int64_t base = first->version();
                                ASSERT_GE(base, 2);
                                DelVector older;
                                const uint32_t older_rows[] = {0};
                                older.init(base - 1, older_rows, std::size(older_rows));
                                add_delvec(first, first->id(), base - 1, /*segment_id=*/1, "reattach_older.delvec",
                                           older.save());
                                const DelvecPagePB pinned = first->delvec_meta().delvecs().at(1);
                                DelVector current;
                                const uint32_t current_rows[] = {0, 1};
                                current.init(base, current_rows, std::size(current_rows));
                                add_delvec(first, first->id(), base, /*segment_id=*/1, "reattach_current.delvec",
                                           current.save());
                                first->mutable_sstable_meta()->mutable_sstables(0)->mutable_delvec()->CopyFrom(pinned);
                            }));
    const auto& target = result.published.at(result.target_tablet_id);
    ASSERT_EQ(2, target.sstable_meta().sstables_size());
    const auto& modern = target.sstable_meta().sstables(0);
    ASSERT_TRUE(modern.has_shared_rssid());
    ASSERT_TRUE(modern.has_delvec());
    ASSERT_TRUE(target.delvec_meta().delvecs().contains(modern.shared_rssid()));
    EXPECT_EQ(target.delvec_meta().delvecs().at(modern.shared_rssid()).SerializeAsString(),
              modern.delvec().SerializeAsString());
    DelVector loaded;
    LakeIOOptions io_options;
    ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), target, modern.shared_rssid(), false, io_options, &loaded));
    EXPECT_EQ(2, loaded.cardinality());
}

// The first source has a delete-vector page for its rowset, but its modern SST embeds none. The carried SST still
// embeds none: a page is attached only to an SST that had one.
TEST_F(LakeTabletMergerTest, test_tablet_merging_attaches_no_delvec_page_to_a_carried_sst_that_had_none) {
    ASSIGN_OR_ABORT(auto result, publish_metadata_only_merge_fixture(
                                         MetadataOnlyMergeShape::kPrivate, /*enable_tde=*/false,
                                         /*with_del_file=*/true, /*skip_source_flush=*/true, [](auto& sources) {
                                             sources[0]->mutable_sstable_meta()->mutable_sstables(0)->clear_delvec();
                                         }));
    const auto& target = result.published.at(result.target_tablet_id);
    ASSERT_EQ(2, target.sstable_meta().sstables_size());
    const auto& modern = target.sstable_meta().sstables(0);
    ASSERT_TRUE(modern.has_shared_rssid());
    EXPECT_TRUE(target.delvec_meta().delvecs().contains(modern.shared_rssid()));
    EXPECT_FALSE(modern.has_delvec());
}

// A modern SST whose embedded delete-vector page cannot be read -- its version has no file in the source, or the page
// lies outside the file -- cannot be loaded. The merged tablet rebuilds its index instead of carrying the SSTs, and the
// merge does not flush the sources, since the flush would have to load that SST: a flush attempt fails the test here.
TEST_F(LakeTabletMergerTest, test_tablet_merging_unresolvable_sst_delvec_falls_back_without_flush) {
    set_failpoint_mode("fail_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore([&] { set_failpoint_mode("fail_lake_pk_index_flush", FailPointTriggerModeType::DISABLE); });
    for (bool version_has_file : {false, true}) {
        SCOPED_TRACE(version_has_file ? "page outside its file" : "version without a file");
        ASSIGN_OR_ABORT(
                auto result,
                publish_metadata_only_merge_fixture(
                        MetadataOnlyMergeShape::kPrivate, /*enable_tde=*/false,
                        /*with_del_file=*/version_has_file, /*skip_source_flush=*/false, [&](auto& sources) {
                            auto* delvec = sources[0]->mutable_sstable_meta()->mutable_sstables(0)->mutable_delvec();
                            if (version_has_file) {
                                const auto& file = sources[0]->delvec_meta().version_to_file().at(delvec->version());
                                delvec->set_offset(file.size() + 16);
                                delvec->set_size(4);
                            } else {
                                delvec->set_version(99);
                                delvec->set_size(1);
                            }
                        }));
        expect_affine_sst_fallback_orphans(result);
    }
}

// A primary-key source that references RSSIDs at or above 2^31 cannot flush its index: the index records its
// watermark as a signed 64-bit (rssid << 32 | rowid). The merge rebuilds the index instead of flushing, packs the
// source down to small RSSIDs, and the merged tablet's index works from then on.
TEST_F(LakeTabletMergerTest, test_tablet_merging_source_beyond_the_watermark_domain_rebuilds_without_flush) {
    constexpr uint32_t kHighRowset = (uint32_t{1} << 31) + 5;
    set_failpoint_mode("fail_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    ASSIGN_OR_ABORT(auto result, publish_metadata_only_merge_fixture(
                                         MetadataOnlyMergeShape::kPrivate, /*enable_tde=*/false,
                                         /*with_del_file=*/true, /*skip_source_flush=*/false, [&](auto& sources) {
                                             sources[1]->mutable_rowsets(0)->set_id(kHighRowset);
                                             sources[1]->set_next_rowset_id(kHighRowset + 1);
                                             sources[1]->clear_sstable_meta();
                                         }));
    set_failpoint_mode("fail_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    const auto& target = result.published.at(result.target_tablet_id);
    expect_affine_sst_fallback_orphans(result);
    EXPECT_EQ(3, target.next_rowset_id());
    expect_metadata_merge_lifecycle(result, {{10, 100}, {60, 600}}, {{10, 1010}}, {60});
}

// A source lists its SSTs in non-decreasing watermark order, the order its index commits them in. One listed below the
// watermark of an earlier one is wrong metadata, refused before anything is written.
TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_sstables_listed_below_an_earlier_watermark) {
    auto status =
            expect_metadata_only_merge_rejected_without_writes(MetadataOnlyMergeShape::kPrivate, [](auto& sources) {
                auto* second = sources[1].get();
                auto* later = second->mutable_sstable_meta()->add_sstables();
                later->CopyFrom(second->sstable_meta().sstables(0));
                later->set_filename("metadata_only_descending.sst");
                later->set_max_rss_rowid(second->sstable_meta().sstables(0).max_rss_rowid() - (uint64_t{1} << 32));
            });
    EXPECT_TRUE(status.message().contains("metadata_only_descending.sst below the watermark of an earlier one"))
            << status;
}

// The source flush. A cold load of the merged tablet replays only the rowsets above its highest SST watermark, which
// after packing can belong to a later source; so every source's index must be in its SSTs before they are carried.

// The left source has a row (key 11) its SSTs do not hold. Merged without the source flush, the merged tablet's index
// misses that key, because the right source's translated watermark lies above the left source's rowsets; merged with
// the flush, it has it.
TEST_F(LakeTabletMergerTest, test_tablet_merging_source_flush_persists_unflushed_index_entries) {
    for (bool skip_source_flush : {true, false}) {
        SCOPED_TRACE(skip_source_flush ? "without the source flush" : "with the source flush");
        const int64_t left_id = next_id();
        const int64_t right_id = next_id();
        const int64_t target_id = next_id();
        prepare_tablet_dirs(target_id);
        ASSIGN_OR_ABORT(auto left, create_lifecycle_source(left_id, 0, 50, 10, 100, /*include_delete=*/false));
        ASSIGN_OR_ABORT(auto right, create_lifecycle_source(right_id, 50, 100, 60, 600, /*include_delete=*/false));
        {
            set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
            DeferOp restore([&] { set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE); });
            ASSIGN_OR_ABORT(left, _primary_key_manager->flush_pk_memtable(left, left->version()));
            ASSERT_OK(put_tablet_metadata(left));
            ASSIGN_OR_ABORT(right, _primary_key_manager->flush_pk_memtable(right, right->version()));
            ASSERT_OK(put_tablet_metadata(right));
        }
        ASSERT_GT(left->sstable_meta().sstables_size(), 0);
        ASSERT_GT(right->sstable_meta().sstables_size(), 0);
        // One more write on each side that no SST holds.
        ASSIGN_OR_ABORT(left, publish_followup_upsert_delete(left_id, left->version(), 11, 110, 0, false));
        ASSIGN_OR_ABORT(right, publish_followup_upsert_delete(right_id, right->version(), 61, 610, 0, false));

        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
        if (skip_source_flush) {
            set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
        }
        DeferOp restore([&] {
            set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
            set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
        });
        std::unordered_map<int64_t, TabletMetadataPtr> published;
        ASSERT_OK(publish_resharding_merge({left, right}, target_id, left->version(), left->version() + 1, next_id(),
                                           published));
        const auto& merged = published.at(target_id);
        ASSERT_GT(merged->sstable_meta().sstables_size(), 0);
        _primary_key_manager->unload_and_remove_primary_index(target_id);
        ASSIGN_OR_ABORT(auto values,
                        load_index_values(merged, target_id, {encode_int_primary_key(11), encode_int_primary_key(61)}));
        ASSERT_EQ(2, values.size());
        if (skip_source_flush) {
            EXPECT_EQ(IndexValue(NullIndexValue), values[0]) << "the unflushed left entry is below the replay point";
        } else {
            EXPECT_NE(IndexValue(NullIndexValue), values[0]);
        }
        EXPECT_NE(IndexValue(NullIndexValue), values[1]);
    }
}

// One source flush can write several SSTs when the index rotates its memtables as it loads; the merge carries every
// one of them, beside the SSTs the other source had already flushed.
TEST_F(LakeTabletMergerTest, test_tablet_merging_carries_every_sst_one_source_flush_writes) {
    using lake::ConfigResetGuard;
    const int64_t left_id = next_id();
    const int64_t right_id = next_id();
    const int64_t target_id = next_id();
    prepare_tablet_dirs(target_id);
    ASSIGN_OR_ABORT(auto left, create_lifecycle_source(left_id, 0, 50, 10, 100, /*include_delete=*/false));
    ASSIGN_OR_ABORT(auto right, create_lifecycle_source(right_id, 50, 100, 60, 600, /*include_delete=*/false));
    for (int32_t i = 1; i <= 3; ++i) {
        ASSIGN_OR_ABORT(left, publish_followup_upsert_delete(left_id, left->version(), 10 + i, 100 + i, 0, false));
        ASSIGN_OR_ABORT(right, publish_followup_upsert_delete(right_id, right->version(), 60 + i, 600 + i, 0, false));
    }
    std::map<int32_t, int32_t> expected = {{10, 100}, {11, 101}, {12, 102}, {13, 103},
                                           {60, 600}, {61, 601}, {62, 602}, {63, 603}};
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    DeferOp restore([&] { set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE); });
    ASSIGN_OR_ABORT(right, _primary_key_manager->flush_pk_memtable(right, right->version()));
    ASSERT_OK(put_tablet_metadata(right));
    ASSERT_GT(right->sstable_meta().sstables_size(), 0);
    ASSERT_EQ(0, left->sstable_meta().sstables_size());
    ASSIGN_OR_ABORT(auto left_inventory_before, sst_inventory(left_id));
    ASSIGN_OR_ABORT(auto right_inventory_before, sst_inventory(right_id));

    _primary_key_manager->unload_and_remove_primary_index(left_id);
    _primary_key_manager->unload_and_remove_primary_index(right_id);
    std::unordered_map<int64_t, TabletMetadataPtr> published;
    {
        // A tiny memtable makes the left source's index rotate memtables while it rebuilds from its rowsets.
        ConfigResetGuard<int64_t> l0_limit(&config::l0_max_mem_usage, 1);
        ASSERT_OK(publish_resharding_merge({left, right}, target_id, left->version(), left->version() + 1, next_id(),
                                           published));
    }
    const auto& merged = published.at(target_id);
    ASSIGN_OR_ABORT(auto left_inventory_after, sst_inventory(left_id));
    ASSIGN_OR_ABORT(auto right_inventory_after, sst_inventory(right_id));
    std::set<std::string> written_for_left;
    std::set_difference(left_inventory_after.begin(), left_inventory_after.end(), left_inventory_before.begin(),
                        left_inventory_before.end(), std::inserter(written_for_left, written_for_left.end()));
    std::set<std::string> written_for_right;
    std::set_difference(right_inventory_after.begin(), right_inventory_after.end(), right_inventory_before.begin(),
                        right_inventory_before.end(), std::inserter(written_for_right, written_for_right.end()));
    EXPECT_GE(written_for_left.size(), 2) << "the left source's flush must write several SSTs";
    std::set<std::string> expected_carried(written_for_left.begin(), written_for_left.end());
    expected_carried.insert(written_for_right.begin(), written_for_right.end());
    for (const auto& sst : right->sstable_meta().sstables()) expected_carried.insert(sst.filename());
    std::set<std::string> carried;
    for (const auto& sst : merged->sstable_meta().sstables()) EXPECT_TRUE(carried.insert(sst.filename()).second);
    EXPECT_EQ(expected_carried, carried);
    EXPECT_TRUE(std::is_sorted(merged->sstable_meta().sstables().begin(), merged->sstable_meta().sstables().end(),
                               [](const auto& a, const auto& b) { return a.max_rss_rowid() < b.max_rss_rowid(); }));

    _primary_key_manager->unload_and_remove_primary_index(target_id);
    _tablet_manager->prune_metacache();
    ASSIGN_OR_ABORT(auto cold, _tablet_manager->get_tablet_metadata(target_id, merged->version()));
    expect_lifecycle_oracle(cold, repeated_expected_rows(expected), {});
}

// The merged tablet starts as a copy of its lowest-id source with every data, garbage and per-tablet field cleared:
// compaction inputs, orphan files, the previous garbage version and the cumulative point do not carry over. The
// sources' new metadata marks their data files shared but leaves their delete-vector files alone, since the merge
// copies the pages it keeps and never references a source's delete-vector file.
TEST_F(LakeTabletMergerTest, test_tablet_merging_clears_the_skeleton_and_leaves_source_delvec_files_private) {
    ASSIGN_OR_ABORT(auto result, publish_metadata_only_merge_fixture(
                                         MetadataOnlyMergeShape::kPrivate, /*enable_tde=*/false,
                                         /*with_del_file=*/true, /*skip_source_flush=*/true, [](auto& sources) {
                                             auto* first = sources[0].get();
                                             first->add_compaction_inputs()->CopyFrom(first->rowsets(0));
                                             auto* orphan = first->add_orphan_files();
                                             orphan->set_name("skeleton_orphan.dat");
                                             orphan->set_size(1);
                                             first->set_prev_garbage_version(first->version());
                                             first->set_cumulative_point(1);
                                         }));
    const auto& target = result.published.at(result.target_tablet_id);
    EXPECT_EQ(0, target.compaction_inputs_size());
    EXPECT_FALSE(std::any_of(target.orphan_files().begin(), target.orphan_files().end(),
                             [](const auto& orphan) { return orphan.name() == "skeleton_orphan.dat"; }));
    EXPECT_FALSE(target.has_prev_garbage_version());
    EXPECT_EQ(0, target.cumulative_point());

    bool saw_source_delvec = false;
    for (const auto& [tablet_id, metadata] : result.published) {
        if (tablet_id == result.target_tablet_id) continue;
        for (const auto& rowset : metadata.rowsets()) {
            for (const auto& segment : rowset.segment_metas()) EXPECT_TRUE(segment.shared());
        }
        for (const auto& [version, file] : metadata.delvec_meta().version_to_file()) {
            (void)version;
            saw_source_delvec = true;
            EXPECT_FALSE(file.shared()) << file.name();
        }
    }
    EXPECT_TRUE(saw_source_delvec);
}

// FE assigns schema ids, so two sources that map rowsets to the same historical schema id map them to one schema,
// even when their copies differ in bytes (written by different BE versions). The merge keeps the lowest-id
// source's copy instead of refusing the pair.
TEST_F(LakeTabletMergerTest, test_tablet_merging_keeps_one_copy_of_a_historical_schema) {
    const std::string stem = fmt::format("historical_schema_{}", next_id());
    auto a = make_preflight_sidecar_source(next_id(), stem + "_a.dat");
    auto b = make_preflight_sidecar_source(next_id(), stem + "_b.dat");
    for (auto* source : {a.get(), b.get()}) {
        add_historical_schema(source, 5001);
        (*source->mutable_rowset_to_schema())[source->rowsets(0).id()] = 5001;
    }
    (*b->mutable_historical_schemas())[5001].set_compression_level(3);
    ASSERT_NE(a->historical_schemas().at(5001).SerializeAsString(),
              b->historical_schemas().at(5001).SerializeAsString());

    ASSIGN_OR_ABORT(auto merged, publish_verbatim_merge({a, b}, next_id(), /*target_version=*/2));
    ASSERT_TRUE(merged->historical_schemas().contains(5001));
    EXPECT_EQ(a->historical_schemas().at(5001).SerializeAsString(),
              merged->historical_schemas().at(5001).SerializeAsString());
    EXPECT_EQ(2, merged->rowset_to_schema_size());
}

// A unique-key tablet through split, a forced base compaction of every child (which rewrites the segments the split
// left shared into private ones), a merge of the children, a compaction of the merged tablet, vacuum and a cold read:
// the same rows, distinct keys and checksum at every step.
TEST_F(LakeTabletMergerTest, test_tablet_merging_unique_key_split_compact_merge_lifecycle) {
    auto source = fixed_point_source(/*primary_key=*/false);
    ASSIGN_OR_ABORT(auto expected_rows, read_two_column_rows(source));
    auto summarize = [](const std::vector<std::pair<int32_t, int32_t>>& rows) {
        std::set<int32_t> keys;
        int64_t checksum = 0;
        for (const auto& [key, value] : rows) {
            keys.insert(key);
            checksum += int64_t{key} * 1000003 + value;
        }
        return std::tuple{rows.size(), keys.size(), checksum};
    };
    const auto expected = summarize(expected_rows);
    ASSERT_EQ(std::get<0>(expected), std::get<1>(expected));

    auto children = split_fixed_point_source(source, 3);
    std::vector<TabletMetadataPtr> compacted_children;
    std::vector<std::pair<int32_t, int32_t>> child_rows;
    for (const auto& child : children) {
        ASSERT_OK(put_tablet_metadata(child));
        ASSIGN_OR_ABORT(auto compacted, compact_tablet(child->id(), child->version(), /*force_base=*/true));
        EXPECT_FALSE(lake::tablet_reshard_helper::has_shared_files(*compacted));
        ASSIGN_OR_ABORT(auto rows, read_two_column_rows(compacted));
        child_rows.insert(child_rows.end(), rows.begin(), rows.end());
        compacted_children.push_back(std::move(compacted));
    }
    std::sort(child_rows.begin(), child_rows.end());
    EXPECT_EQ(expected, summarize(child_rows));

    const int64_t merged_id = next_id();
    std::unordered_map<int64_t, TabletMetadataPtr> published;
    ASSERT_OK(publish_resharding_merge(compacted_children, merged_id, compacted_children[0]->version(),
                                       compacted_children[0]->version() + 1, next_id(), published));
    const auto& merged = published.at(merged_id);
    ASSIGN_OR_ABORT(auto merged_rows, read_two_column_rows(merged));
    EXPECT_EQ(expected_rows, merged_rows);

    ASSIGN_OR_ABORT(auto compacted, compact_tablet(merged_id, merged->version(), /*force_base=*/true));
    EXPECT_LE(compacted->rowsets_size(), merged->rowsets_size());
    ASSIGN_OR_ABORT(auto compacted_rows, read_two_column_rows(compacted));
    EXPECT_EQ(expected, summarize(compacted_rows));

    VacuumRequest request;
    VacuumResponse response;
    auto* info = request.add_tablet_infos();
    info->set_tablet_id(merged_id);
    info->set_min_version(compacted->version());
    request.set_min_retain_version(compacted->version());
    request.set_grace_timestamp(::time(nullptr) + 3600);
    request.set_min_active_txn_id(std::numeric_limits<int64_t>::max());
    request.set_enable_file_bundling(false);
    request.set_delete_txn_log(false);
    lake::vacuum(_tablet_manager.get(), request, &response);
    ASSERT_TRUE(response.has_status());
    EXPECT_EQ(0, response.status().status_code());

    _tablet_manager->prune_metacache();
    ASSIGN_OR_ABORT(auto cold, _tablet_manager->get_tablet_metadata(merged_id, compacted->version()));
    ASSIGN_OR_ABORT(auto cold_rows, read_two_column_rows(cold));
    EXPECT_EQ(expected, summarize(cold_rows));
}

} // namespace starrocks
