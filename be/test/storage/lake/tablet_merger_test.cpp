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
    struct RepeatedLifecycleResult {
        TabletMetadataPtr metadata;
        std::vector<TabletMetadataPtr> final_sources;
        std::map<int32_t, int32_t> expected;
        std::vector<int32_t> deleted_keys;
        std::set<std::string> final_sst_orphans;
    };

    StatusOr<RepeatedLifecycleResult> run_repeated_four_cycle_lifecycle(bool enable_tde) {
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
        set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
        DeferOp restore_failpoints([&] {
            set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
            set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
        });

        const char* force_projection_env = std::getenv("STARROCKS_TEST_FORCE_CONTEXT_SPAN_PROJECTION");
        const bool force_context_span =
                force_projection_env != nullptr && std::string_view(force_projection_env) == "1";
        int context_span_hook_count = 0;
        auto* sync = SyncPoint::GetInstance();
        if (force_context_span) {
            sync->SetCallBack("tablet_merge_test:force_context_span_projection", [&](void* arg) {
                ++context_span_hook_count;
                *static_cast<bool*>(arg) = true;
            });
            sync->EnableProcessing();
        }
        DeferOp cleanup_sync_point([&] {
            if (force_context_span) {
                sync->ClearCallBack("tablet_merge_test:force_context_span_projection");
                sync->DisableProcessing();
            }
        });

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
        std::unordered_map<int64_t, TabletMetadataPtr> initial_published;
        int merge_count = 1;
        RETURN_IF_ERROR(publish_resharding_merge(initial_sources, initial_target, initial_sources.front()->version(),
                                                 initial_sources.front()->version() + 1, next_id(), initial_published));
        auto current = initial_published.at(initial_target);
        ASSIGN_OR_RETURN(const int initial_del_files, repeated_merge_delfile_count_oracle(initial_sources));
        expect_repeated_merge_pb(current, initial_cursor, /*expected_sidecar_count=*/0, initial_del_files,
                                 /*expected_sst_orphans=*/{}, enable_tde);
        expect_lifecycle_oracle(current, repeated_expected_rows(expected), /*deleted_keys=*/{});

        std::vector<TabletMetadataPtr> final_sources;
        std::set<std::string> final_sst_orphans;
        for (int cycle = 0; cycle < 4; ++cycle) {
            SCOPED_TRACE(fmt::format("repeated lifecycle cycle {}", cycle));
            const uint32_t prior_cursor = current->next_rowset_id();
            const int32_t upsert_key = 40 + cycle;
            const int32_t upsert_value = 4000 + cycle;
            const int32_t delete_key = cycle < 3 ? 10 + cycle * 100 : 40;
            _update_manager->unload_and_remove_primary_index(current->id());
            ASSIGN_OR_RETURN(current, publish_followup_upsert_delete(current->id(), current->version(), upsert_key,
                                                                     upsert_value, delete_key));
            expected[upsert_key] = upsert_value;
            expected.erase(delete_key);
            deleted.insert(delete_key);
            ASSIGN_OR_RETURN(current, compact_tablet(current->id(), current->version(), /*force_base=*/true));

            ASSIGN_OR_RETURN(auto children, repeated_split_three_way(current));
            std::vector<TabletMetadataPtr> merge_sources;
            std::set<std::string> expected_sst_orphans;
            for (int child_index = 0; child_index < 3; ++child_index) {
                auto child = children[child_index];
                const int32_t key = child_index * 100 + 20 + cycle;
                const int32_t value = (cycle + 1) * 10000 + key;
                _update_manager->unload_and_remove_primary_index(child->id());
                ASSIGN_OR_RETURN(child, publish_followup_upsert_delete(child->id(), child->version(), key, value,
                                                                       /*delete_key=*/0, /*include_delete=*/false));
                expected[key] = value;
                deleted.erase(key);
                if (enable_tde) {
                    ASSIGN_OR_RETURN(child, _update_manager->flush_pk_memtable(child, child->version()));
                    RETURN_IF_ERROR(put_tablet_metadata(child));
                    if (child->sstable_meta().sstables_size() == 0) {
                        return Status::InternalError("TDE repeated lifecycle source flush emitted no SST");
                    }
                    assert_published_sstables_reopen(child);
                }
                auto mutable_child = std::make_shared<TabletMetadataPB>(*child);
                for (auto& sstable : *mutable_child->mutable_sstable_meta()->mutable_sstables()) {
                    if (enable_tde) {
                        if (sstable.encryption_meta().empty()) {
                            return Status::InternalError("TDE repeated lifecycle SST is plaintext");
                        }
                    }
                    expected_sst_orphans.insert(sstable.filename());
                    sstable.set_shared(true);
                    sstable.clear_shared_rssid();
                    sstable.clear_shared_version();
                }
                add_repeated_sparse_sidecars(mutable_child.get(), cycle, child_index, value, enable_tde);
                _update_manager->unload_and_remove_primary_index(child->id());
                merge_sources.emplace_back(std::move(mutable_child));
            }

            ASSIGN_OR_RETURN(const uint32_t expected_cursor, repeated_merge_cursor_oracle(merge_sources));
            ASSIGN_OR_RETURN(const int expected_del_files, repeated_merge_delfile_count_oracle(merge_sources));
            const int64_t target_id = next_id();
            prepare_tablet_dirs(target_id);
            std::unordered_map<int64_t, TabletMetadataPtr> published;
            ++merge_count;
            RETURN_IF_ERROR(publish_resharding_merge(merge_sources, target_id, merge_sources.front()->version(),
                                                     merge_sources.front()->version() + 1, next_id(), published));
            current = published.at(target_id);
            EXPECT_LT(current->next_rowset_id(), uint64_t{prior_cursor} + 4096) << "bounded RSSID growth";
            expect_repeated_merge_pb(current, expected_cursor, /*expected_sidecar_count=*/3, expected_del_files,
                                     expected_sst_orphans, enable_tde);
            std::vector<int32_t> deleted_keys(deleted.begin(), deleted.end());
            expect_lifecycle_oracle(current, repeated_expected_rows(expected), deleted_keys);
            if (cycle == 3) {
                final_sst_orphans = std::move(expected_sst_orphans);
                for (const auto& source : merge_sources) final_sources.emplace_back(published.at(source->id()));
            }
        }
        EXPECT_EQ(force_context_span ? merge_count : 0, context_span_hook_count)
                << "context-span hook must fire exactly once per MERGE";

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
    ASSIGN_OR_ABORT(auto result, run_repeated_four_cycle_lifecycle(/*enable_tde=*/false));
    EXPECT_EQ(result.final_sst_orphans.size(), static_cast<size_t>(result.metadata->orphan_files_size()));
    for (const auto& orphan : result.metadata->orphan_files()) EXPECT_TRUE(orphan.encryption_meta().empty());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_interval_projection_repeated_tde_sidecar_vacuum_lifecycle) {
    ASSIGN_OR_ABORT(auto result, run_repeated_four_cycle_lifecycle(/*enable_tde=*/true));
    ASSERT_GE(result.final_sst_orphans.size(), 3);

    const bool old_tde = config::enable_transparent_data_encryption;
    config::enable_transparent_data_encryption = true;
    DeferOp restore_config([&] { config::enable_transparent_data_encryption = old_tde; });
    ensure_kek_in_key_cache();
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    DeferOp restore_flush([&] { set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE); });

    _update_manager->unload_and_remove_primary_index(result.metadata->id());
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

    _update_manager->unload_and_remove_primary_index(after_dml->id());
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

    _update_manager->unload_and_remove_primary_index(compacted->id());
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

TEST_F(LakeTabletMergerTest, test_tablet_merging_metadata_only_identical_complete_reuse) {
    int sstable_open_count = 0;
    SyncPoint::GetInstance()->SetCallBack("PersistentIndexSstable::init:table_open_error",
                                          [&](void*) { ++sstable_open_count; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp clear_open_counter([&] {
        SyncPoint::GetInstance()->ClearCallBack("PersistentIndexSstable::init:table_open_error");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    ASSIGN_OR_ABORT(auto result,
                    publish_metadata_only_merge_fixture(MetadataOnlyMergeShape::kIdentical, /*enable_tde=*/false,
                                                        /*with_del_file=*/false, /*skip_source_flush=*/false));
    const auto& target = result.published.at(result.target_tablet_id);
    ASSERT_EQ(1, target.sstable_meta().sstables_size());
    const auto& output = target.sstable_meta().sstables(0);
    EXPECT_EQ(*result.source_sst_filenames.begin(), output.filename());
    EXPECT_TRUE(output.shared());
    EXPECT_TRUE(output.has_fileset_id());
    EXPECT_EQ(result.source_sst_filenames.size() * 2, static_cast<size_t>(sstable_open_count))
            << "the mandatory source flush is the only phase allowed to open the inherited SST";
    for (const auto& [tablet_id, source] : result.published) {
        if (tablet_id == result.target_tablet_id) continue;
        ASSERT_EQ(1, source.sstable_meta().sstables_size());
        EXPECT_NE(source.sstable_meta().sstables(0).fileset_id().SerializeAsString(),
                  output.fileset_id().SerializeAsString());
    }
    ASSIGN_OR_ABORT(auto inventory, sst_inventory(result.target_tablet_id));
    EXPECT_EQ(result.source_sst_filenames, inventory) << "identical-cohort classification must not write an SST";

    auto target_ptr = std::make_shared<TabletMetadataPB>(target);
    auto index = std::make_unique<lake::LakePersistentIndex>(_tablet_manager.get(), result.target_tablet_id);
    ASSERT_OK(index->init(target_ptr));
    const std::string key = encode_int_primary_key(10);
    Slice key_slice(key);
    IndexValue value;
    ASSERT_OK(index->get(1, &key_slice, &value));
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(1) << 32), value);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_metadata_only_private_uncertainty_falls_back) {
    struct Case {
        const char* name;
        std::function<void(std::vector<std::shared_ptr<TabletMetadataPB>>&)> mutate;
    };
    const std::vector<Case> cases = {
            {"one shared PB",
             [](auto& sources) { sources[0]->mutable_sstable_meta()->mutable_sstables(0)->set_shared(true); }},
            {"duplicate filename",
             [](auto& sources) {
                 auto* duplicate = sources[1]->mutable_sstable_meta()->add_sstables();
                 duplicate->CopyFrom(sources[1]->sstable_meta().sstables(0));
             }},
            {"absent SST range",
             [](auto& sources) { sources[0]->mutable_sstable_meta()->mutable_sstables(0)->clear_range(); }},
            {"range outside source tablet",
             [](auto& sources) {
                 auto* range = sources[0]->mutable_sstable_meta()->mutable_sstables(0)->mutable_range();
                 range->set_start_key("\xff\xff");
                 range->set_end_key("\xff\xff\xff");
             }},
            {"nonuniform shared RSSID map",
             [](auto& sources) {
                 const auto shared_uid = UniqueId::gen_uid().to_proto();
                 sources[0]->mutable_rowsets(0)->mutable_uid()->CopyFrom(shared_uid);
                 sources[1]->mutable_rowsets(0)->mutable_uid()->CopyFrom(shared_uid);
                 sources[1]->mutable_rowsets(0)->mutable_segment_metas(0)->CopyFrom(
                         sources[0]->rowsets(0).segment_metas(0));
             }},
            {"unresolved embedded delvec",
             [](auto& sources) {
                 auto* delvec = sources[0]->mutable_sstable_meta()->mutable_sstables(0)->mutable_delvec();
                 delvec->set_version(99);
                 delvec->set_size(1);
             }},
            {"negative projection",
             [](auto& sources) {
                 auto* sstable = sources[1]->mutable_sstable_meta()->mutable_sstables(0);
                 sstable->set_rssid_offset(-100);
                 sstable->set_max_rss_rowid(0);
             }},
            {"overflow projection",
             [](auto& sources) {
                 auto* sstable = sources[1]->mutable_sstable_meta()->mutable_sstables(0);
                 sstable->set_rssid_offset(std::numeric_limits<int32_t>::max());
                 sstable->set_max_rss_rowid(static_cast<uint64_t>(std::numeric_limits<int32_t>::max()) << 32);
             }},
            {"signed ordering domain",
             [](auto& sources) {
                 sources[0]->mutable_sstable_meta()->mutable_sstables(0)->set_max_rss_rowid(static_cast<uint64_t>(1)
                                                                                            << 63);
             }},
    };

    for (const auto& test_case : cases) {
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
        expect_target_version_not_published(target_id, target_version);
        EXPECT_EQ(tde_before, config::enable_transparent_data_encryption);
        EXPECT_EQ(key_cache_size_before, KeyCache::instance().size());
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_projected_target_domain_before_target_io) {
    constexpr uint32_t kFirstRowset = std::numeric_limits<int32_t>::max() - 1;
    constexpr uint32_t kFirstNextRowset = std::numeric_limits<int32_t>::max();
    constexpr uint32_t kSecondRowset = 1;
    int dcg_rebuild_count = 0;
    int delvec_writer_count = 0;
    int source_flush_count = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("merge_dcg_meta:before_rebuild", [&](void*) { ++dcg_rebuild_count; });
    sync->SetCallBack("merge_delvecs:writer_invocations",
                      [&](void* arg) { delvec_writer_count += *static_cast<int*>(arg); });
    sync->SetCallBack("merge_sstables:source_pk_flush", [&](void*) { ++source_flush_count; });
    sync->EnableProcessing();
    DeferOp clear_callbacks([&] {
        sync->ClearCallBack("merge_dcg_meta:before_rebuild");
        sync->ClearCallBack("merge_delvecs:writer_invocations");
        sync->ClearCallBack("merge_sstables:source_pk_flush");
        sync->DisableProcessing();
    });

    int64_t target_id = 0;
    int64_t target_version = 0;
    std::set<std::string> target_segment_root_before;
    std::set<std::string> target_metadata_root_before;
    auto overflow_domain = [=](std::vector<std::shared_ptr<TabletMetadataPB>>& sources) {
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

        // The second source's synthetic source RSSID projects onto the first
        // target rowset. Both DCGs update c1, but use distinct real .cols
        // inputs, so a late allocation gate must reach the rebuild callback.
        const std::string conflicting_cols_name = "target_domain_overflow_conflict.cols";
        write_c1_only_cols_file(second->id(), conflicting_cols_name, /*num_rows=*/1, [](int) { return 600; });
        auto& conflicting_dcg = (*second->mutable_dcg_meta()->mutable_dcgs())[0];
        conflicting_dcg.add_column_files(conflicting_cols_name);
        conflicting_dcg.add_unique_column_ids()->add_column_ids(second->schema().column(1).unique_id());
        conflicting_dcg.add_versions(second->version());
        conflicting_dcg.add_shared_files(false);
    };

    // Dense allocation removes the old projected-overflow path. The synthetic
    // DCG key is not in the authoritative closure and must instead fail closed
    // before DCG/delvec/source-index output is attempted.
    auto result = publish_metadata_only_merge_fixture(
            MetadataOnlyMergeShape::kPrivate, /*enable_tde=*/false,
            /*with_del_file=*/true, /*skip_source_flush=*/false, overflow_domain, &target_id, &target_version,
            [&](int64_t, int64_t, int64_t target) {
                ASSIGN_OR_ABORT(target_segment_root_before,
                                directory_inventory(_location_provider->segment_root_location(target)));
                ASSIGN_OR_ABORT(target_metadata_root_before,
                                directory_inventory(_location_provider->metadata_root_location(target)));
            });
    EXPECT_TRUE(result.status().is_corruption()) << result.status();
    EXPECT_EQ(0, dcg_rebuild_count);
    EXPECT_EQ(0, delvec_writer_count);
    EXPECT_EQ(0, source_flush_count);
    ASSIGN_OR_ABORT(auto target_segment_root_after,
                    directory_inventory(_location_provider->segment_root_location(target_id)));
    ASSIGN_OR_ABORT(auto target_metadata_root_after,
                    directory_inventory(_location_provider->metadata_root_location(target_id)));
    EXPECT_EQ(target_segment_root_before, target_segment_root_after);
    EXPECT_EQ(target_metadata_root_before, target_metadata_root_after);
    expect_target_version_not_published(target_id, target_version);
    auto* target_cache_entry = _update_manager->index_cache().get(target_id);
    EXPECT_EQ(nullptr, target_cache_entry);
    if (target_cache_entry != nullptr) _update_manager->index_cache().release(target_cache_entry);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_metadata_only_identical_divergence_falls_back) {
    struct Case {
        const char* name;
        std::function<void(std::vector<std::shared_ptr<TabletMetadataPB>>&)> mutate;
    };
    auto append_identical_second_sst = [&](std::vector<std::shared_ptr<TabletMetadataPB>>& sources) {
        const std::string filename = "metadata_only_identical_second.sst";
        const std::string key = encode_int_primary_key(20);
        const auto file = write_raw_pk_sstable(
                _tablet_manager->sst_location(sources[0]->id(), filename),
                {{key, serialize_index_values({{sources[0]->version(), /*rssid=*/1, /*rowid=*/0}})}});
        for (auto& source : sources) {
            auto* second = source->mutable_sstable_meta()->add_sstables();
            second->CopyFrom(source->sstable_meta().sstables(0));
            second->set_filename(filename);
            second->set_filesize(file.filesize);
            second->mutable_range()->CopyFrom(file.range);
            second->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 1);
        }
    };
    const std::vector<Case> cases = {
            {"one private occurrence",
             [](auto& sources) { sources[1]->mutable_sstable_meta()->mutable_sstables(0)->set_shared(false); }},
            {"SST count mismatch",
             [&](auto& sources) {
                 append_identical_second_sst(sources);
                 sources[1]->mutable_sstable_meta()->mutable_sstables()->RemoveLast();
             }},
            {"same-count SST order mismatch",
             [&](auto& sources) {
                 append_identical_second_sst(sources);
                 sources[1]->mutable_sstable_meta()->mutable_sstables()->SwapElements(0, 1);
             }},
            {"generation mismatch",
             [](auto& sources) {
                 sources[1]->mutable_sstable_meta()->mutable_sstables(0)->set_generation_version(999);
             }},
            {"rowset UID mismatch",
             [](auto& sources) {
                 auto* uid = sources[1]->mutable_rowsets(0)->mutable_uid();
                 uid->set_lo(uid->lo() + 1);
             }},
            {"source-local delete-only rowset",
             [&](auto& sources) {
                 auto* rowset = sources[1]->add_rowsets();
                 rowset->set_id(7);
                 rowset->set_version(sources[1]->version());
                 lake::tablet_reshard_helper::set_rowset_uid(rowset);
                 auto* del = rowset->add_del_files();
                 del->set_name("metadata_only_identical_local.del");
                 del->set_origin_rowset_id(7);
                 del->set_op_offset(0);
                 write_binary_del_file(sources[1]->id(), del->name(), {});
                 sources[1]->set_next_rowset_id(8);
             }},
            {"source-local compaction rowset",
             [&](auto& sources) {
                 const std::string segment_name = "metadata_only_identical_compaction.dat";
                 const uint64_t segment_size = write_two_column_segment(
                         sources[1]->id(), segment_name, /*num_rows=*/1, [](int key) { return key * 10; }, 70);
                 auto* rowset = sources[1]->add_rowsets();
                 rowset->set_id(7);
                 rowset->set_version(sources[1]->version());
                 rowset->set_max_compact_input_rowset_id(6);
                 rowset->set_num_rows(1);
                 rowset->set_data_size(segment_size);
                 lake::tablet_reshard_helper::set_rowset_uid(rowset);
                 auto* segment = rowset->add_segment_metas();
                 segment->set_filename(segment_name);
                 segment->set_size(segment_size);
                 segment->set_num_rows(1);
                 sources[1]->set_next_rowset_id(8);
             }},
            {"segment index mismatch",
             [&](auto& sources) {
                 const std::string segment_name = "metadata_only_identical_index_mismatch.dat";
                 const uint64_t segment_size = write_two_column_segment(
                         sources[1]->id(), segment_name, /*num_rows=*/1, [](int key) { return key * 10; }, 10);
                 auto* segment = sources[1]->mutable_rowsets(0)->mutable_segment_metas(0);
                 segment->set_filename(segment_name);
                 segment->set_size(segment_size);
                 segment->set_segment_idx(7);
             }},
            {"segment layout count mismatch",
             [&](auto& sources) {
                 const std::string segment_name = "metadata_only_identical_second_segment.dat";
                 const uint64_t segment_size = write_two_column_segment(
                         sources[1]->id(), segment_name, /*num_rows=*/1, [](int key) { return key * 10; }, 11);
                 auto* segment = sources[1]->mutable_rowsets(0)->add_segment_metas();
                 segment->set_filename(segment_name);
                 segment->set_size(segment_size);
                 segment->set_num_rows(1);
                 segment->set_segment_idx(1);
                 sources[1]->mutable_rowsets(0)->set_num_rows(2);
                 sources[1]->mutable_rowsets(0)->set_data_size(sources[1]->rowsets(0).data_size() + segment_size);
             }},
    };
    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        auto result_or = publish_metadata_only_merge_fixture(MetadataOnlyMergeShape::kIdentical,
                                                             /*enable_tde=*/false, /*with_del_file=*/false,
                                                             /*skip_source_flush=*/true, test_case.mutate);
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
        for (const auto& filename : result.source_sst_filenames) EXPECT_EQ(1, orphan_counts[filename]);
    }

    for (bool encryption_conflict : {false, true}) {
        SCOPED_TRACE(encryption_conflict ? "identical encryption conflict" : "identical filesize conflict");
        if (encryption_conflict) ensure_kek_in_key_cache();
        const size_t key_cache_size_before = KeyCache::instance().size();
        const bool tde_before = config::enable_transparent_data_encryption;
        auto conflict = [=](std::vector<std::shared_ptr<TabletMetadataPB>>& sources) {
            auto* right = sources[1]->mutable_sstable_meta()->mutable_sstables(0);
            if (encryption_conflict) {
                right->set_encryption_meta("identical conflicting encryption metadata");
            } else {
                right->set_filesize(right->filesize() + 1);
            }
        };
        int64_t target_id = 0;
        int64_t target_version = 0;
        auto result_or = publish_metadata_only_merge_fixture(
                MetadataOnlyMergeShape::kIdentical, /*enable_tde=*/encryption_conflict,
                /*with_del_file=*/false, /*skip_source_flush=*/true, conflict, &target_id, &target_version);
        EXPECT_TRUE(result_or.status().is_corruption()) << result_or.status();
        expect_target_version_not_published(target_id, target_version);
        EXPECT_EQ(tde_before, config::enable_transparent_data_encryption);
        EXPECT_EQ(key_cache_size_before, KeyCache::instance().size());
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_metadata_only_fallback_folds_source_sst_orphans) {
    struct VersionCase {
        const char* name;
        int64_t left;
        int64_t right;
        int64_t expected;
    };
    const std::vector<VersionCase> cases = {
            {"unknown and positive", 0, 7, 0}, {"conflicting positive", 7, 8, 0}, {"negative", -1, -1, 0},
            {"future", 1000000, 1000000, 0},   {"common positive", 7, 7, 7},
    };
    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        auto mutate = [&](std::vector<std::shared_ptr<TabletMetadataPB>>& sources) {
            sources[0]->mutable_sstable_meta()->mutable_sstables(0)->set_generation_version(test_case.left);
            sources[1]->mutable_sstable_meta()->mutable_sstables(0)->set_generation_version(test_case.right);
            // Force the complete identical proof to reject while preserving one
            // matching physical declaration for the orphan fold.
            sources[1]->mutable_sstable_meta()->mutable_sstables(0)->set_shared(false);
        };
        auto result_or = publish_metadata_only_merge_fixture(MetadataOnlyMergeShape::kIdentical,
                                                             /*enable_tde=*/true, /*with_del_file=*/false,
                                                             /*skip_source_flush=*/true, mutate);
        if (!result_or.ok()) {
            ADD_FAILURE() << result_or.status();
            continue;
        }
        auto result = std::move(result_or).value();
        const auto& target = result.published.at(result.target_tablet_id);
        EXPECT_EQ(0, target.sstable_meta().sstables_size());
        std::vector<const FileMetaPB*> matching;
        for (const auto& orphan : target.orphan_files()) {
            if (result.source_sst_filenames.contains(orphan.name())) matching.push_back(&orphan);
        }
        EXPECT_EQ(1, matching.size());
        if (matching.size() != 1) continue;
        EXPECT_TRUE(matching[0]->shared());
        EXPECT_EQ(test_case.expected, matching[0]->version());
        const PersistentIndexSstablePB* source_declaration = nullptr;
        std::vector<int64_t> source_tablet_ids;
        for (const auto& [tablet_id, metadata] : result.published) {
            if (tablet_id == result.target_tablet_id) continue;
            source_tablet_ids.push_back(tablet_id);
            for (const auto& sstable : metadata.sstable_meta().sstables()) {
                if (sstable.filename() == matching[0]->name()) source_declaration = &sstable;
            }
        }
        ASSERT_NE(nullptr, source_declaration);
        EXPECT_EQ(source_declaration->filesize(), matching[0]->size());
        EXPECT_EQ(source_declaration->encryption_meta(), matching[0]->encryption_meta());

        if (test_case.expected != 7) continue;

        // Add one additional retained sibling reference beyond the two merge
        // sources, then run the normal shared-orphan vacuum path.
        const int64_t sibling_id = next_id();
        prepare_tablet_dirs(sibling_id);
        TabletMetadataPB sibling = result.published.at(source_tablet_ids.front());
        sibling.set_id(sibling_id);
        ASSERT_OK(put_tablet_metadata(sibling));
        source_tablet_ids.push_back(sibling_id);

        const std::string sst_path = _tablet_manager->sst_location(result.target_tablet_id, matching[0]->name());
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
            retired.add_orphan_files()->CopyFrom(*matching[0]);
            retired.set_prev_garbage_version(live_reference_version);
            ASSERT_OK(put_tablet_metadata(retired));
            retired_sources.emplace(tablet_id, std::move(retired));
        }
        _tablet_manager->prune_metacache();

        auto reclaimed_response = run_vacuum(retirement_version);
        EXPECT_GE(reclaimed_response.vacuumed_file_size(), matching[0]->size());
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

TEST_F(LakeTabletMergerTest, test_tablet_merging_metadata_only_identical_repeated_filename_rejected) {
    auto repeat_matching = [](std::vector<std::shared_ptr<TabletMetadataPB>>& sources) {
        for (auto& source : sources) {
            auto* duplicate = source->mutable_sstable_meta()->add_sstables();
            duplicate->CopyFrom(source->sstable_meta().sstables(0));
        }
    };
    auto matching_or = publish_metadata_only_merge_fixture(MetadataOnlyMergeShape::kIdentical,
                                                           /*enable_tde=*/false, /*with_del_file=*/false,
                                                           /*skip_source_flush=*/true, repeat_matching);
    ASSERT_OK(matching_or);
    auto matching = std::move(matching_or).value();
    const auto& matching_target = matching.published.at(matching.target_tablet_id);
    EXPECT_EQ(0, matching_target.sstable_meta().sstables_size());
    int folded_count = 0;
    for (const auto& orphan : matching_target.orphan_files()) {
        if (matching.source_sst_filenames.contains(orphan.name())) {
            ++folded_count;
            EXPECT_TRUE(orphan.shared());
        }
    }
    EXPECT_EQ(1, folded_count);

    for (bool encryption_conflict : {false, true}) {
        SCOPED_TRACE(encryption_conflict ? "encryption conflict" : "filesize conflict");
        if (encryption_conflict) ensure_kek_in_key_cache();
        const size_t key_cache_size_before = KeyCache::instance().size();
        const bool tde_before = config::enable_transparent_data_encryption;
        auto repeat_conflicting = [=](std::vector<std::shared_ptr<TabletMetadataPB>>& sources) {
            for (auto& source : sources) {
                auto* duplicate = source->mutable_sstable_meta()->add_sstables();
                duplicate->CopyFrom(source->sstable_meta().sstables(0));
                if (encryption_conflict) {
                    duplicate->set_encryption_meta("conflicting encryption metadata");
                } else {
                    duplicate->set_filesize(duplicate->filesize() + 1);
                }
            }
        };
        int64_t target_id = 0;
        int64_t target_version = 0;
        auto conflicting = publish_metadata_only_merge_fixture(
                MetadataOnlyMergeShape::kIdentical, /*enable_tde=*/encryption_conflict,
                /*with_del_file=*/false, /*skip_source_flush=*/true, repeat_conflicting, &target_id, &target_version);
        EXPECT_TRUE(conflicting.status().is_corruption()) << conflicting.status();
        expect_target_version_not_published(target_id, target_version);
        EXPECT_EQ(tde_before, config::enable_transparent_data_encryption);
        EXPECT_EQ(key_cache_size_before, KeyCache::instance().size());
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_metadata_only_source_range_partition_validation) {
    enum class Outcome { kFallback, kCorruption, kReusable };
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
            {"source gap", Outcome::kCorruption,
             [&](auto& sources) {
                 set_range(sources[0].get(), 0, 40);
                 set_range(sources[1].get(), 50, 100);
             }},
            {"source overlap", Outcome::kCorruption,
             [&](auto& sources) {
                 set_range(sources[0].get(), 0, 60);
                 set_range(sources[1].get(), 50, 100);
             }},
            {"well-formed reversed source order", Outcome::kFallback,
             [&](auto& sources) {
                 // Keep the authoritative outer edges in positions 0 and N-1,
                 // while reversing two well-formed internal partitions.
                 set_range(sources[0].get(), 0, 30);
                 set_range(sources[1].get(), 80, 100);
                 auto middle_high = std::make_shared<TabletMetadataPB>(*sources[0]);
                 middle_high->set_id(next_id());
                 middle_high->clear_rowsets();
                 middle_high->clear_sstable_meta();
                 middle_high->set_next_rowset_id(1);
                 set_range(middle_high.get(), 60, 80);
                 prepare_tablet_dirs(middle_high->id());
                 auto middle_low = std::make_shared<TabletMetadataPB>(*middle_high);
                 middle_low->set_id(next_id());
                 set_range(middle_low.get(), 30, 60);
                 prepare_tablet_dirs(middle_low->id());
                 sources = {sources[0], middle_high, middle_low, sources[1]};
             }},
            {"well-formed fully reversed source order", Outcome::kFallback,
             [](auto& sources) { std::swap(sources[0], sources[1]); }},
            {"wrong bound arity", Outcome::kCorruption,
             [](auto& sources) {
                 auto* lower = sources[0]->mutable_range()->mutable_lower_bound();
                 lower->add_values()->CopyFrom(lower->values(0));
             }},
            {"wrong bound logical type", Outcome::kCorruption,
             [](auto& sources) {
                 // Keep the malformed declarations mutually comparable so the
                 // legacy union helper cannot DCHECK before the classifier has
                 // an opportunity to return Corruption against the INT schema.
                 for (auto& source : sources) {
                     auto* range = source->mutable_range();
                     range->mutable_lower_bound()->mutable_values(0)->mutable_type()->CopyFrom(
                             TypeDescriptor(TYPE_VARCHAR).to_protobuf());
                     range->mutable_upper_bound()->mutable_values(0)->mutable_type()->CopyFrom(
                             TypeDescriptor(TYPE_VARCHAR).to_protobuf());
                 }
             }},
            {"invalid inclusivity flag", Outcome::kCorruption,
             [](auto& sources) { sources[0]->mutable_range()->set_lower_bound_included(false); }},
            {"valid lower-unbounded outer edge", Outcome::kReusable,
             [&](auto& sources) {
                 set_range(sources[0].get(), std::nullopt, 50);
                 set_range(sources[1].get(), 50, 100);
             }},
            {"valid upper-unbounded outer edge", Outcome::kReusable,
             [&](auto& sources) {
                 set_range(sources[0].get(), 0, 50);
                 set_range(sources[1].get(), 50, std::nullopt);
             }},
            {"valid both-unbounded outer edges", Outcome::kReusable,
             [&](auto& sources) {
                 set_range(sources[0].get(), std::nullopt, 50);
                 set_range(sources[1].get(), 50, std::nullopt);
             }},
    };

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        int64_t target_id = 0;
        int64_t target_version = 0;
        auto result_or = publish_metadata_only_merge_fixture(MetadataOnlyMergeShape::kPrivate,
                                                             /*enable_tde=*/false, /*with_del_file=*/false,
                                                             /*skip_source_flush=*/true, test_case.mutate, &target_id,
                                                             &target_version);
        if (test_case.outcome == Outcome::kCorruption) {
            EXPECT_TRUE(result_or.status().is_corruption()) << result_or.status();
            expect_target_version_not_published(target_id, target_version);
            continue;
        }
        ASSERT_OK(result_or);
        auto result = std::move(result_or).value();
        const auto& target = result.published.at(result.target_tablet_id);
        if (test_case.outcome == Outcome::kReusable) {
            std::set<std::string> filenames;
            for (const auto& sstable : target.sstable_meta().sstables()) filenames.insert(sstable.filename());
            EXPECT_EQ(result.source_sst_filenames, filenames);
            continue;
        }

        EXPECT_EQ(0, target.sstable_meta().sstables_size());
        std::set<std::string> folded;
        for (const auto& orphan : target.orphan_files()) {
            if (result.source_sst_filenames.contains(orphan.name())) {
                EXPECT_TRUE(orphan.shared());
                EXPECT_TRUE(folded.insert(orphan.name()).second);
            }
        }
        EXPECT_EQ(result.source_sst_filenames, folded);
        if (target.sstable_meta().sstables_size() != 0) {
            continue; // Keep unsafe range-proof mutations from entering the DML recovery oracle.
        }

        _update_manager->unload_and_remove_primary_index(result.target_tablet_id);
        ASSIGN_OR_ABORT(auto published_after_dml,
                        publish_followup_upsert_delete(result.target_tablet_id, result.target_version,
                                                       /*upsert_key=*/50, /*upsert_value=*/5050, /*delete_key=*/60));
        // Task 2 proves classifier fallback plus the existing rebuild/read path.
        // Materialize the returned snapshot explicitly for the reopen/get oracle;
        // this is not evidence that the DML publication itself persisted an SST.
        // Tasks 3/4 own that synchronous first-writer publication contract.
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
        DeferOp restore_index_flush(
                [&] { set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE); });
        ASSIGN_OR_ABORT(auto after_dml,
                        _update_manager->flush_pk_memtable(published_after_dml, published_after_dml->version()));
        ASSERT_OK(put_tablet_metadata(after_dml));
        EXPECT_GT(after_dml->sstable_meta().sstables_size(), 0) << "explicitly flushed classifier-stage snapshot";
        ASSIGN_OR_ABORT(auto rows, read_two_column_rows(after_dml));
        EXPECT_EQ((std::vector<std::pair<int32_t, int32_t>>{{10, 100}, {50, 5050}}), rows);
        const std::vector<std::string> keys = {encode_int_primary_key(10), encode_int_primary_key(50),
                                               encode_int_primary_key(60)};
        ASSIGN_OR_ABORT(auto values, load_index_values(after_dml, result.target_tablet_id, keys));
        ASSERT_EQ(3, values.size());
        EXPECT_NE(IndexValue(NullIndexValue), values[0]);
        EXPECT_NE(IndexValue(NullIndexValue), values[1]);
        EXPECT_EQ(IndexValue(NullIndexValue), values[2]);

        _update_manager->unload_and_remove_primary_index(result.target_tablet_id);
        ASSIGN_OR_ABORT(auto reopened,
                        _tablet_manager->get_tablet_metadata(result.target_tablet_id, result.target_version + 1));
        ASSIGN_OR_ABORT(auto reopened_rows, read_two_column_rows(reopened));
        EXPECT_EQ(rows, reopened_rows);
        ASSIGN_OR_ABORT(auto reopened_values, load_index_values(reopened, result.target_tablet_id, keys));
        EXPECT_EQ(values, reopened_values);
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
    auto result_or = publish_metadata_only_merge_fixture(MetadataOnlyMergeShape::kPrivate, /*enable_tde=*/false,
                                                         /*with_del_file=*/false, /*skip_source_flush=*/true,
                                                         add_pruned_delete_history);
    ASSERT_OK(result_or);
    auto result = std::move(result_or).value();
    const auto& target = result.published.at(result.target_tablet_id);
    const uint64_t origin_ceiling = uint64_t{kOriginRowset} + kOpOffset;
    EXPECT_GT(target.next_rowset_id(), origin_ceiling);
    EXPECT_EQ(0, target.sstable_meta().sstables_size());

    _update_manager->unload_and_remove_primary_index(result.target_tablet_id);
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
    _update_manager->unload_and_remove_primary_index(result.target_tablet_id);
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
    EXPECT_EQ(2, foreign_origin_target.rowsets(0).del_files(0).origin_rowset_id());
    EXPECT_EQ(10, foreign_origin_target.next_rowset_id());

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

TEST_F(LakeTabletMergerTest, test_tablet_merging_issue11935_falls_back_then_dml_is_exact) {
    const bool old_primary_key_recover = config::enable_primary_key_recover;
    config::enable_primary_key_recover = true;
    DeferOp restore_config([&] { config::enable_primary_key_recover = old_primary_key_recover; });
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_flush_failpoints([&] {
        set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    });
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t target_id = next_id();
    ASSIGN_OR_ABORT(auto fixture, make_issue11935_merge_fixture(child_a, child_b, target_id, "issue11935_old.dat",
                                                                "issue11935_tail.dat", "issue11935_sibling.dat",
                                                                "issue11935_tombstone.sst", "issue11935_stale.sst"));
    std::unordered_map<int64_t, TabletMetadataPtr> published;
    std::unordered_map<int64_t, TabletRangePB> ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), fixture.resharding, fixture.base_version,
                                              fixture.new_version, fixture.txn_info, false, published, ranges));
    const auto& target = published.at(target_id);
    EXPECT_EQ(0, target->sstable_meta().sstables_size());
    std::set<std::string> expected_orphans = {"issue11935_tombstone.sst", "issue11935_stale.sst"};
    std::set<std::string> actual_orphans;
    for (const auto& orphan : target->orphan_files()) {
        if (expected_orphans.contains(orphan.name())) {
            EXPECT_TRUE(orphan.shared());
            actual_orphans.insert(orphan.name());
        }
    }
    EXPECT_EQ(expected_orphans, actual_orphans);

    const std::vector<std::string> keys = {raw_int_primary_key(0), raw_int_primary_key(1), raw_int_primary_key(60)};
    auto expect_issue11935_oracle = [&](const TabletMetadataPtr& metadata) {
        ASSIGN_OR_ABORT(auto rows, read_two_column_rows(metadata));
        EXPECT_EQ((std::vector<std::pair<int32_t, int32_t>>{{0, 200}, {1, 111}}), rows);
        ASSIGN_OR_ABORT(auto values, load_index_values(metadata, target_id, keys));
        ASSERT_EQ(3, values.size());
        EXPECT_NE(IndexValue(NullIndexValue), values[0]);
        EXPECT_NE(IndexValue(NullIndexValue), values[1]);
        EXPECT_EQ(IndexValue(NullIndexValue), values[2]);
    };
    // This historical owner-collision fixture predates the big-endian range
    // encoding requirement. Remove only its tablet-level range before the
    // follow-up write so the real upsert/delete path exercises index recovery
    // rather than failing in range clipping for an unrelated legacy encoding.
    auto writable_target = std::make_shared<TabletMetadataPB>(*target);
    writable_target->clear_range();
    ASSERT_OK(put_tablet_metadata(writable_target));
    _update_manager->unload_and_remove_primary_index(target_id);
    ASSIGN_OR_ABORT(auto after_dml, publish_followup_upsert_delete(target_id, fixture.new_version, /*upsert_key=*/1,
                                                                   /*upsert_value=*/111, /*delete_key=*/60));
    expect_issue11935_oracle(after_dml);

    ASSIGN_OR_ABORT(auto compacted, compact_tablet(target_id, after_dml->version(), /*force_base=*/true));
    EXPECT_EQ(after_dml->version() + 1, compacted->version());
    ASSERT_GT(compacted->rowsets_size(), 0);
    expect_issue11935_oracle(compacted);

    _update_manager->unload_and_remove_primary_index(target_id);
    ASSIGN_OR_ABORT(auto reopened, _tablet_manager->get_tablet_metadata(target_id, compacted->version()));
    expect_issue11935_oracle(reopened);
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

        _update_manager->unload_and_remove_primary_index(fixture.merged_tablet);
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

        _update_manager->unload_and_remove_primary_index(fixture.merged_tablet);
        ASSIGN_OR_ABORT(auto reopened,
                        _tablet_manager->get_tablet_metadata(fixture.merged_tablet, compacted->version()));
        expect_lifecycle_oracle(reopened, {{10, 1010}}, {20});
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_indexless_fallback_lifecycle_matrix) {
    using lake::ConfigResetGuard;
    ConfigResetGuard<int32_t> files_threshold(&config::cloud_native_pk_index_rebuild_files_threshold, 0);
    ConfigResetGuard<int64_t> rows_threshold(&config::cloud_native_pk_index_rebuild_rows_threshold, 0);
    ConfigResetGuard<int64_t> l0_limit(&config::l0_max_mem_usage, 1LL << 30);

    enum class Shape {
        kLegacySharedCollision,
        kModernSharedOwnerDivergence,
        kNonSharedMappingDivergence,
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
    };
    const Case cases[] = {
            {"legacy shared collision", Shape::kLegacySharedCollision, false},
            {"modern shared owner divergence", Shape::kModernSharedOwnerDivergence, false},
            {"non-shared mapping divergence", Shape::kNonSharedMappingDivergence, false},
            {"mixed tombstone and live", Shape::kMixedTombstoneAndLive, false},
            {"tombstone only", Shape::kTombstoneOnly, false},
            {"merged delvec", Shape::kMergedDelvec, false},
            {"TDE", Shape::kTde, true},
            {"source compaction before merge", Shape::kSourceCompactionBeforeMerge, false},
            {"source delete after compaction", Shape::kSourceDeleteAfterCompaction, false},
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
            expected.clear();
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

        // Drain the real source writers before constructing the classifier
        // shape. MERGE will flush once more at its own mandatory boundary.
        ASSIGN_OR_ABORT(left, _update_manager->flush_pk_memtable(left, left->version()));
        ASSIGN_OR_ABORT(right, _update_manager->flush_pk_memtable(right, right->version()));

        auto mutable_left = std::make_shared<TabletMetadataPB>(*left);
        auto mutable_right = std::make_shared<TabletMetadataPB>(*right);
        ASSERT_GT(mutable_left->sstable_meta().sstables_size(), 0);
        ASSERT_GT(mutable_right->sstable_meta().sstables_size(), 0);
        if (test_case.shape == Shape::kMixedTombstoneAndLive) {
            append_tombstone_sstable(mutable_left.get(), 30, fmt::format("lifecycle_mixed_{}.sst", next_id()),
                                     /*shared=*/true);
        } else if (test_case.shape == Shape::kTombstoneOnly) {
            mutable_left->mutable_sstable_meta()->clear_sstables();
            mutable_right->mutable_sstable_meta()->clear_sstables();
            append_tombstone_sstable(mutable_left.get(), 10, fmt::format("lifecycle_tombstone_{}.sst", next_id()),
                                     /*shared=*/true);
            append_tombstone_sstable(mutable_right.get(), 60, fmt::format("lifecycle_tombstone_{}.sst", next_id()),
                                     /*shared=*/true);
        } else if (test_case.shape == Shape::kLegacySharedCollision) {
            for (auto* source : {mutable_left.get(), mutable_right.get()}) {
                for (auto& sstable : *source->mutable_sstable_meta()->mutable_sstables()) {
                    sstable.set_shared(true);
                    sstable.clear_shared_rssid();
                    sstable.clear_shared_version();
                }
            }
        } else if (test_case.shape == Shape::kModernSharedOwnerDivergence) {
            for (auto& sstable : *mutable_left->mutable_sstable_meta()->mutable_sstables()) {
                sstable.set_shared(true);
                sstable.set_shared_rssid(mutable_left->rowsets(0).id());
                sstable.set_shared_version(mutable_left->version());
            }
            for (auto& sstable : *mutable_right->mutable_sstable_meta()->mutable_sstables()) {
                sstable.set_shared(true);
                sstable.set_shared_rssid(mutable_right->rowsets(0).id());
                sstable.set_rssid_offset(1);
                sstable.set_shared_version(mutable_right->version());
            }
        } else if (test_case.shape == Shape::kNonSharedMappingDivergence) {
            mutable_left->mutable_sstable_meta()->mutable_sstables(0)->clear_range();
        } else {
            mutable_left->mutable_sstable_meta()->mutable_sstables(0)->set_shared(true);
        }

        std::map<std::string, int> omitted_sst_counts;
        for (const auto* source : {mutable_left.get(), mutable_right.get()}) {
            for (const auto& sstable : source->sstable_meta().sstables()) {
                ++omitted_sst_counts[sstable.filename()];
            }
        }
        ASSERT_FALSE(omitted_sst_counts.empty());
        for (const auto& [filename, count] : omitted_sst_counts) {
            ASSERT_EQ(1, count) << "fixture source SST filename is not unique: " << filename;
        }
        ASSIGN_OR_ABORT(auto left_sst_inventory_before_merge, sst_inventory(left_id));
        ASSIGN_OR_ABORT(auto right_sst_inventory_before_merge, sst_inventory(right_id));

        int classifier_opens = 0;
        bool classifier_active = false;
        auto* sync = SyncPoint::GetInstance();
        sync->SetCallBack("merge_sstables:metadata_classifier_entry", [&](void*) { classifier_active = true; });
        sync->SetCallBack("merge_sstables:metadata_classifier_exit", [&](void*) { classifier_active = false; });
        sync->SetCallBack("PersistentIndexSstable::init:table_open_error", [&](void*) {
            if (classifier_active) ++classifier_opens;
        });
        sync->EnableProcessing();
        _update_manager->unload_and_remove_primary_index(left_id);
        _update_manager->unload_and_remove_primary_index(right_id);
        std::unordered_map<int64_t, TabletMetadataPtr> published;
        const Status merge_status = publish_resharding_merge({mutable_left, mutable_right}, target_id, left->version(),
                                                             left->version() + 1, next_id(), published);
        for (const auto* point : {"merge_sstables:metadata_classifier_entry", "merge_sstables:metadata_classifier_exit",
                                  "PersistentIndexSstable::init:table_open_error"}) {
            sync->ClearCallBack(point);
        }
        sync->DisableProcessing();
        ASSERT_OK(merge_status);
        EXPECT_EQ(0, classifier_opens);

        const auto& merged = published.at(target_id);
        ASSERT_EQ(0, merged->sstable_meta().sstables_size());
        std::map<std::string, int> published_source_counts;
        for (const auto& [tablet_id, source] : published) {
            if (tablet_id == target_id) continue;
            for (const auto& sstable : source->sstable_meta().sstables()) {
                ++published_source_counts[sstable.filename()];
            }
        }
        EXPECT_EQ(omitted_sst_counts, published_source_counts);

        std::map<std::string, int> expected_target_sst_orphan_counts = omitted_sst_counts;
        ASSIGN_OR_ABORT(auto left_sst_inventory_after_merge, sst_inventory(left_id));
        ASSIGN_OR_ABORT(auto right_sst_inventory_after_merge, sst_inventory(right_id));
        for (const auto& [before, after] :
             {std::pair{&left_sst_inventory_before_merge, &left_sst_inventory_after_merge},
              std::pair{&right_sst_inventory_before_merge, &right_sst_inventory_after_merge}}) {
            for (const auto& filename : *after) {
                if (!before->contains(filename)) {
                    expected_target_sst_orphan_counts.emplace(filename, 1);
                }
            }
        }

        std::map<std::string, int> actual_handoff_counts;
        for (const auto& orphan : merged->orphan_files()) {
            if (!orphan.name().ends_with(".sst")) continue;
            EXPECT_TRUE(expected_target_sst_orphan_counts.contains(orphan.name()))
                    << "unexpected source SST orphan handoff: " << orphan.name();
            EXPECT_TRUE(orphan.shared());
            ++actual_handoff_counts[orphan.name()];
        }
        EXPECT_EQ(expected_target_sst_orphan_counts, actual_handoff_counts);
        for (const auto& [filename, count] : actual_handoff_counts) {
            EXPECT_EQ(1, count) << "source SST must be handed off exactly once: " << filename;
        }
        for (const auto& [filename, count] : omitted_sst_counts) {
            EXPECT_EQ(1, actual_handoff_counts[filename]) << "omitted source SST handoff missing: " << filename;
        }

        _update_manager->unload_and_remove_primary_index(target_id);
        ASSIGN_OR_ABORT(auto reopened_merge, _tablet_manager->get_tablet_metadata(target_id, merged->version()));
        ASSIGN_OR_ABORT(auto merge_rows, read_two_column_rows(reopened_merge));
        EXPECT_EQ(expected.size(), merge_rows.size());

        ASSIGN_OR_ABORT(auto after_first_upsert,
                        publish_followup_upsert_delete(target_id, merged->version(), 20, 2000, 0,
                                                       /*include_delete=*/false));
        expected[20] = 2000;
        std::vector<std::pair<int32_t, int32_t>> expected_rows(expected.begin(), expected.end());
        expect_lifecycle_oracle(after_first_upsert, expected_rows, {});
        EXPECT_EQ(0, after_first_upsert->sstable_meta().sstables_size());

        _update_manager->unload_and_remove_primary_index(target_id);
        ASSIGN_OR_ABORT(auto after_delete, publish_followup_delete(target_id, after_first_upsert->version(), 60));
        expected.erase(60);
        expected_rows.assign(expected.begin(), expected.end());
        expect_lifecycle_oracle(after_delete, expected_rows, {60});

        _update_manager->unload_and_remove_primary_index(target_id);
        ASSIGN_OR_ABORT(auto reopened_after_delete,
                        _tablet_manager->get_tablet_metadata(target_id, after_delete->version()));
        expect_lifecycle_oracle(reopened_after_delete, expected_rows, {60});

        ASSIGN_OR_ABORT(auto after_second_upsert,
                        publish_followup_upsert_delete(target_id, reopened_after_delete->version(), 70, 7000, 0,
                                                       /*include_delete=*/false));
        expected[70] = 7000;
        expected_rows.assign(expected.begin(), expected.end());
        expect_lifecycle_oracle(after_second_upsert, expected_rows, {60});

        _update_manager->unload_and_remove_primary_index(target_id);
        ASSIGN_OR_ABORT(auto reopened, _tablet_manager->get_tablet_metadata(target_id, after_second_upsert->version()));
        expect_lifecycle_oracle(reopened, expected_rows, {60});

        ASSIGN_OR_ABORT(auto compacted, compact_tablet(target_id, reopened->version(), /*force_base=*/true));
        EXPECT_EQ(reopened->version() + 1, compacted->version());
        ASSERT_GT(compacted->rowsets_size(), 0);
        expect_lifecycle_oracle(compacted, expected_rows, {60});

        _update_manager->unload_and_remove_primary_index(target_id);
        ASSIGN_OR_ABORT(auto reopened_compacted, _tablet_manager->get_tablet_metadata(target_id, compacted->version()));
        expect_lifecycle_oracle(reopened_compacted, expected_rows, {60});
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_indexless_fallback_uses_native_reload) {
    // This must fail if a fallback target's first writer persists a recovery SST
    // instead of relying on the normal cache-only native reload path.
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
    ASSIGN_OR_ABORT(left, _update_manager->flush_pk_memtable(left, left->version()));
    ASSIGN_OR_ABORT(right, _update_manager->flush_pk_memtable(right, right->version()));
    auto mutable_left = std::make_shared<TabletMetadataPB>(*left);
    auto mutable_right = std::make_shared<TabletMetadataPB>(*right);
    mutable_left->mutable_sstable_meta()->mutable_sstables(0)->set_shared(true);

    std::set<std::string> omitted_source_sstables;
    for (const auto* source : {mutable_left.get(), mutable_right.get()}) {
        for (const auto& sstable : source->sstable_meta().sstables()) {
            ASSERT_TRUE(omitted_source_sstables.insert(sstable.filename()).second);
        }
    }

    _update_manager->unload_and_remove_primary_index(left_id);
    _update_manager->unload_and_remove_primary_index(right_id);
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

    _update_manager->unload_and_remove_primary_index(target_id);
    ASSIGN_OR_ABORT(auto after_first_upsert,
                    publish_followup_upsert_delete(target_id, merged->version(), /*upsert_key=*/20,
                                                   /*upsert_value=*/2000, /*delete_key=*/0,
                                                   /*include_delete=*/false));
    expect_lifecycle_oracle(after_first_upsert, {{10, 100}, {20, 2000}, {60, 600}}, {});
    ASSERT_EQ(0, after_first_upsert->sstable_meta().sstables_size());

    _update_manager->unload_and_remove_primary_index(target_id);
    ASSIGN_OR_ABORT(auto after_delete, publish_followup_delete(target_id, after_first_upsert->version(), 60));
    expect_lifecycle_oracle(after_delete, {{10, 100}, {20, 2000}}, {60});

    _update_manager->unload_and_remove_primary_index(target_id);
    ASSIGN_OR_ABORT(auto reopened_after_delete,
                    _tablet_manager->get_tablet_metadata(target_id, after_delete->version()));
    expect_lifecycle_oracle(reopened_after_delete, {{10, 100}, {20, 2000}}, {60});

    ASSIGN_OR_ABORT(auto after_second_upsert,
                    publish_followup_upsert_delete(target_id, reopened_after_delete->version(), /*upsert_key=*/70,
                                                   /*upsert_value=*/7000, /*delete_key=*/0,
                                                   /*include_delete=*/false));
    expect_lifecycle_oracle(after_second_upsert, {{10, 100}, {20, 2000}, {70, 7000}}, {60});

    _update_manager->unload_and_remove_primary_index(target_id);
    ASSIGN_OR_ABORT(auto final_metadata,
                    _tablet_manager->get_tablet_metadata(target_id, after_second_upsert->version()));
    expect_lifecycle_oracle(final_metadata, {{10, 100}, {20, 2000}, {70, 7000}}, {60});
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_indexless_split_divergent_layout_falls_back_exact) {
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    DeferOp restore_flush([&] { set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE); });

    const int64_t left_id = next_id();
    const int64_t right_id = next_id();
    const int64_t merged_id = next_id();
    prepare_tablet_dirs(merged_id);
    ASSIGN_OR_ABORT(auto left, create_lifecycle_source(left_id, 0, 50, 10, 100, /*include_delete=*/false));
    ASSIGN_OR_ABORT(auto right, create_lifecycle_source(right_id, 50, 100, 60, 600, /*include_delete=*/false));
    ASSIGN_OR_ABORT(left, _update_manager->flush_pk_memtable(left, left->version()));
    ASSIGN_OR_ABORT(right, _update_manager->flush_pk_memtable(right, right->version()));
    auto mutable_left = std::make_shared<TabletMetadataPB>(*left);
    auto mutable_right = std::make_shared<TabletMetadataPB>(*right);
    mutable_left->mutable_sstable_meta()->mutable_sstables(0)->set_shared(true);
    _update_manager->unload_and_remove_primary_index(left_id);
    _update_manager->unload_and_remove_primary_index(right_id);
    std::unordered_map<int64_t, TabletMetadataPtr> merge_published;
    ASSERT_OK(publish_resharding_merge({mutable_left, mutable_right}, merged_id, left->version(), left->version() + 1,
                                       next_id(), merge_published));
    auto indexless = merge_published.at(merged_id);
    ASSERT_EQ(0, indexless->sstable_meta().sstables_size());

    _update_manager->unload_and_remove_primary_index(merged_id);
    ASSIGN_OR_ABORT(auto recovered, publish_followup_upsert_delete(merged_id, indexless->version(), 20, 2000,
                                                                   /*delete_key=*/0, /*include_delete=*/false));
    const std::vector<std::pair<int32_t, int32_t>> expected = {{10, 100}, {20, 2000}, {60, 600}};
    expect_lifecycle_oracle(recovered, expected, {});

    std::set<std::string> parent_segments;
    for (const auto& rowset : recovered->rowsets()) {
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
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), split_info, recovered->version(),
                                              recovered->version() + 1, split_txn, false, split_published,
                                              split_ranges));
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
                << "SST cohorts must differ only in allowed fileset grouping";
    }
    _update_manager->unload_and_remove_primary_index(child_a);
    _update_manager->unload_and_remove_primary_index(child_b);

    const int64_t final_id = next_id();
    prepare_tablet_dirs(final_id);
    auto rowset_layout_mismatch_count = [] {
        const std::string value =
                bvar::Variable::describe_exposed("tablet_merge_sstable_fallback_rowset_layout_mismatch_total");
        return value.empty() ? int64_t{-1} : std::stoll(value);
    };
    const int64_t mismatch_before = rowset_layout_mismatch_count();
    ASSERT_GE(mismatch_before, 0);
    std::unordered_map<int64_t, TabletMetadataPtr> final_published;
    ASSERT_OK(publish_resharding_merge({split_published.at(child_a), split_published.at(child_b)}, final_id,
                                       recovered->version() + 1, recovered->version() + 2, next_id(), final_published));
    const auto& final_metadata = final_published.at(final_id);
    ASSERT_EQ(0, final_metadata->sstable_meta().sstables_size());
    EXPECT_EQ(mismatch_before + 1, rowset_layout_mismatch_count());
    std::set<std::string> omitted_sstables;
    for (const auto& [tablet_id, source] : final_published) {
        if (tablet_id == final_id) continue;
        for (const auto& sstable : source->sstable_meta().sstables()) omitted_sstables.insert(sstable.filename());
    }
    std::set<std::string> handed_off_sstables;
    for (const auto& orphan : final_metadata->orphan_files()) {
        if (!omitted_sstables.contains(orphan.name())) continue;
        EXPECT_TRUE(orphan.shared());
        handed_off_sstables.insert(orphan.name());
    }
    EXPECT_EQ(omitted_sstables, handed_off_sstables);
    expect_lifecycle_oracle(final_metadata, expected, {});
    _update_manager->unload_and_remove_primary_index(final_id);
    ASSIGN_OR_ABORT(auto restarted, _tablet_manager->get_tablet_metadata(final_id, final_metadata->version()));
    expect_lifecycle_oracle(restarted, expected, {});
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_indexless_split_identical_layout_reuses_sstables) {
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
    ASSIGN_OR_ABORT(left, _update_manager->flush_pk_memtable(left, left->version()));
    ASSIGN_OR_ABORT(right, _update_manager->flush_pk_memtable(right, right->version()));
    auto mutable_left = std::make_shared<TabletMetadataPB>(*left);
    auto mutable_right = std::make_shared<TabletMetadataPB>(*right);
    mutable_left->mutable_sstable_meta()->mutable_sstables(0)->set_shared(true);
    _update_manager->unload_and_remove_primary_index(left_id);
    _update_manager->unload_and_remove_primary_index(right_id);
    std::unordered_map<int64_t, TabletMetadataPtr> merge_published;
    ASSERT_OK(publish_resharding_merge({mutable_left, mutable_right}, merged_id, left->version(), left->version() + 1,
                                       next_id(), merge_published));
    auto indexless = merge_published.at(merged_id);
    ASSERT_EQ(0, indexless->sstable_meta().sstables_size());

    _update_manager->unload_and_remove_primary_index(merged_id);
    ASSIGN_OR_ABORT(auto recovered, publish_followup_upsert_delete(merged_id, indexless->version(), 10, 2000,
                                                                   /*delete_key=*/0, /*include_delete=*/false));
    const std::vector<std::pair<int32_t, int32_t>> expected = {{10, 2000}, {60, 600}};
    expect_lifecycle_oracle(recovered, expected, {});

    // The writer is allowed to keep its rebuilt index cache-only. Persist it explicitly because the rest of this
    // fixture needs one physical SST cohort to prove identical metadata reuse after compaction and SPLIT.
    ASSIGN_OR_ABORT(auto persisted_index, _update_manager->flush_pk_memtable(recovered, recovered->version()));
    ASSERT_OK(put_tablet_metadata(persisted_index));
    ASSERT_GT(persisted_index->sstable_meta().sstables_size(), 0);

    ASSIGN_OR_ABORT(auto compacted, compact_tablet(merged_id, persisted_index->version(), /*force_base=*/true));
    ASSERT_EQ(1, compacted->rowsets_size());
    ASSERT_EQ(1, compacted->rowsets(0).segment_metas_size())
            << "the identical proof fixture needs one physical segment spanning both child ranges";
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
    // Mutation gate: child-local range/data_size intentionally differ; restoring the old full-PB
    // comparator must make this identical-layout merge fall back instead of reusing its source SSTs.
    const auto& first_rowset = first_child->rowsets(0);
    const auto& second_rowset = second_child->rowsets(0);
    EXPECT_TRUE(lake::tablet_reshard_helper::same_rowset_uid(first_rowset, second_rowset));
    EXPECT_EQ(first_rowset.id(), second_rowset.id());
    EXPECT_EQ(first_rowset.version(), second_rowset.version());
    // Restored: the per-child statistics projection is fed the boundary planner's samples, so a
    // shared segment's bytes are apportioned by sampled row positions rather than by an even split
    // of its coarse [min, max]. If this ever reads equal again, that routing has been lost.
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
            << "split children must inherit one semantically identical SST cohort";
    std::set<std::string> source_sstables;
    for (const auto& sstable : first_child->sstable_meta().sstables()) {
        EXPECT_TRUE(sstable.shared());
        source_sstables.insert(sstable.filename());
    }
    ASSERT_FALSE(source_sstables.empty());
    ASSIGN_OR_ABORT(auto inventory_before_merge, sst_inventory(merged_id));

    const int64_t final_id = next_id();
    prepare_tablet_dirs(final_id);
    std::unordered_map<int64_t, TabletMetadataPtr> final_published;
    ASSERT_OK(publish_resharding_merge({first_child, second_child}, final_id, compacted->version() + 1,
                                       compacted->version() + 2, next_id(), final_published));
    const auto& final_metadata = final_published.at(final_id);
    std::set<std::string> output_sstables;
    for (const auto& sstable : final_metadata->sstable_meta().sstables()) {
        EXPECT_TRUE(sstable.shared());
        output_sstables.insert(sstable.filename());
    }
    EXPECT_TRUE(output_sstables.empty()) << "Task 1 conservatively falls back for identical legacy SSTs";
    for (const auto& filename : source_sstables) {
        EXPECT_EQ(1, std::count_if(final_metadata->orphan_files().begin(), final_metadata->orphan_files().end(),
                                   [&](const auto& orphan) { return orphan.name() == filename; }));
    }
    ASSIGN_OR_ABORT(auto inventory_after_merge, sst_inventory(final_id));
    EXPECT_EQ(inventory_before_merge, inventory_after_merge) << "metadata-only reuse must write no SST";
    expect_lifecycle_oracle(final_metadata, expected, {});
    _update_manager->unload_and_remove_primary_index(final_id);
    ASSIGN_OR_ABORT(auto restarted, _tablet_manager->get_tablet_metadata(final_id, final_metadata->version()));
    expect_lifecycle_oracle(restarted, expected, {});
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
    ASSIGN_OR_ABORT(left, _update_manager->flush_pk_memtable(left, left->version()));
    ASSIGN_OR_ABORT(right, _update_manager->flush_pk_memtable(right, right->version()));

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
        sources[0]->mutable_sstable_meta()->mutable_sstables(0)->set_shared(true);
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
        _update_manager->unload_and_remove_primary_index(result->target_tablet_id);

        const Status injected = Status::InternalError(
                fmt::format("injected {} native index load failure", enable_tde ? "TDE" : "plaintext"));
        sync->SetCallBack("lake_index_load.1", [&](void* arg) { *static_cast<Status*>(arg) = injected; });
        sync->EnableProcessing();
        auto failed = publish_followup_upsert_delete(result->target_tablet_id, result->target_version, 20, 2000, 10);
        sync->ClearCallBack("lake_index_load.1");
        sync->DisableProcessing();
        ASSERT_FALSE(failed.ok());
        EXPECT_NE(std::string::npos, failed.status().to_string().find(std::string(injected.message())));
        auto* failed_entry = _update_manager->index_cache().get(result->target_tablet_id);
        EXPECT_EQ(nullptr, failed_entry);
        if (failed_entry != nullptr) _update_manager->index_cache().release(failed_entry);
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
        _update_manager->unload_and_remove_primary_index(result->target_tablet_id);
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
    add_dcg_with_columns(meta1.get(), 10, "dcg-1", {101, 102}, 1);

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
    add_dcg_with_columns(meta2.get(), 1, "dcg-2", {201, 202}, 1);

    materialize_tombstone_sstables(meta1.get());
    materialize_tombstone_sstables(meta2.get());

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
    const uint32_t expected_rowset_id = 3;

    bool found_rowset = false;
    for (const auto& rowset : merged->rowsets()) {
        if (rowset.id() == expected_rowset_id) {
            found_rowset = true;
            ASSERT_TRUE(rowset.has_range());
            EXPECT_EQ(rowset.range().SerializeAsString(),
                      tablet_metadatas.at(old_tablet_id_2)->range().SerializeAsString());
            ASSERT_TRUE(rowset.has_max_compact_input_rowset_id());
            EXPECT_EQ(4, rowset.max_compact_input_rowset_id());
            ASSERT_EQ(1, rowset.del_files_size());
            EXPECT_EQ(3, rowset.del_files(0).origin_rowset_id());
            break;
        }
    }
    ASSERT_TRUE(found_rowset);

    bool found_rowset_from_meta1 = false;
    for (const auto& rowset : merged->rowsets()) {
        if (rowset.id() == 2) {
            found_rowset_from_meta1 = true;
            ASSERT_TRUE(rowset.has_range());
            EXPECT_EQ(rowset.range().SerializeAsString(),
                      tablet_metadatas.at(old_tablet_id_1)->range().SerializeAsString());
            break;
        }
    }
    ASSERT_TRUE(found_rowset_from_meta1);

    auto rowset_schema_it = merged->rowset_to_schema().find(expected_rowset_id);
    ASSERT_TRUE(rowset_schema_it != merged->rowset_to_schema().end());
    EXPECT_EQ(2002, rowset_schema_it->second);

    // The two sources declare different shared SST cohorts and rowset layouts. Preserve the rowset/sidecar
    // merge above, but publish the binding lazy fallback with an exact shared orphan handoff.
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

    const uint32_t expected_segment_id = 3;
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

TEST_F(LakeTabletMergerTest, test_tablet_merging_split_then_merge) {
    // Split produces two children with identical shared rowsets.
    // Merging them should dedup shared rowsets and restore original rssid count.
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // Both children share the same rowset (version 1, segment "shared_seg.dat")
    // and the same shared sstable (with shared_rssid=1)
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
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename("shared_seg.dat");
            sm->set_size(100);
            sm->set_num_rows(10);
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(rowset, "shared_seg.dat"); // same uid across siblings => dedup
        // Add shared sstable with shared_rssid
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename("shared_sst.sst");
        sst->set_filesize(512);
        sst->set_shared(true);
        sst->set_shared_rssid(1);
        sst->set_shared_version(1);
        sst->set_generation_version(7); // generation version; merge projection must inherit this, not restamp it
        sst->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 99);
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

    // Should have only 1 rowset (deduped)
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_EQ("shared_seg.dat", merged->rowsets(0).segment_metas(0).filename());
    // num_rows/data_size should be accumulated from both children
    EXPECT_EQ(20, merged->rowsets(0).num_rows());
    EXPECT_EQ(200, merged->rowsets(0).data_size());
    // The contiguous producer ranges prove the identical shared cohort reusable.
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    EXPECT_EQ("shared_sst.sst", merged->sstable_meta().sstables(0).filename());
    EXPECT_EQ(7, merged->sstable_meta().sstables(0).generation_version());
    EXPECT_EQ(0, merged->orphan_files_size());
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

// Phase-1 merge (end-to-end): two siblings with matching uid and a shared segment
// + distinct private segments. uid dedup unions their segments into one merged
// rowset. No re-share: the merged tablet owns its segments via the ownership-transfer
// model (the source old tablets are marked all-shared so their drop/vacuum skips the
// files), so the union preserves per-segment flags -- a spanning segment stays
// shared=true, split-pruned segments stay shared=false. NOTE: built but NOT run
// locally (LLVM thirdparty mismatch); verify in CI.
TEST_F(LakeTabletMergerTest, test_tablet_merge_segment_union_preserves_ownership) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_child = [&](int64_t tablet_id, const std::string& private_seg, uint32_t private_idx) {
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
        // Shared segment (identical in both siblings) + private (per-sibling).
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename("shared.dat");
            sm->set_size(100);
            sm->set_num_rows(10);
            sm->set_shared(true);
            sm->set_segment_idx(0);
            sm->set_encryption_meta("enc_shared");
        }
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename(private_seg);
            sm->set_size(50);
            sm->set_num_rows(10);
            sm->set_shared(false);
            sm->set_segment_idx(private_idx);
            sm->set_encryption_meta("enc_" + private_seg);
        }
        // Same uid => same logical rowset => dedup at merge.
        rowset->mutable_uid()->set_hi(0);
        rowset->mutable_uid()->set_lo(777);
        return meta;
    };
    ASSERT_OK(put_merge_sources(make_child(child_a, "a_private.dat", 1), make_child(child_b, "b_private.dat", 2)));

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
    ASSERT_EQ(1, merged->rowsets_size()); // family dedup => single merged rowset
    const auto& mr = merged->rowsets(0);

    std::set<std::string> segs;
    for (const auto& s : mr.segment_metas()) segs.insert(s.filename());
    EXPECT_EQ((std::set<std::string>{"shared.dat", "a_private.dat", "b_private.dat"}), segs); // union, shared deduped

    // Each segment carries its own encryption_meta inside its SegmentMetadataPB, so the
    // union keeps every segment's encryption meta aligned with the segment.
    std::map<std::string, std::string> seg_to_enc;
    for (int i = 0; i < mr.segment_metas_size(); ++i)
        seg_to_enc[mr.segment_metas(i).filename()] = mr.segment_metas(i).encryption_meta();
    EXPECT_EQ("enc_shared", seg_to_enc["shared.dat"]);
    EXPECT_EQ("enc_a_private.dat", seg_to_enc["a_private.dat"]);
    EXPECT_EQ("enc_b_private.dat", seg_to_enc["b_private.dat"]);

    // No re-share: the merged tablet owns its segments. The union preserves per-segment
    // flags -- the spanning segment stays shared=true (still referenced by any non-merged
    // sibling), while split-pruned segments stay shared=false (owned by the merged tablet,
    // freed by its own GC instead of leaking onto the shared-file path).
    std::map<std::string, bool> seg_shared;
    for (int i = 0; i < mr.segment_metas_size(); ++i) {
        seg_shared[mr.segment_metas(i).filename()] = mr.segment_metas(i).shared();
    }
    EXPECT_TRUE(seg_shared.at("shared.dat"));
    EXPECT_FALSE(seg_shared.at("a_private.dat"));
    EXPECT_FALSE(seg_shared.at("b_private.dat"));
    EXPECT_EQ(20, mr.num_rows());
    EXPECT_EQ(200, mr.data_size());
    EXPECT_TRUE(mr.has_uid()); // preserved across merge
    EXPECT_EQ(777, mr.uid().lo());
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

// File bundling packs multiple segments into one physical file, so a bundled rowset's
// segments share a file NAME and differ only by bundle_file_offset. After a split prunes
// such a rowset, two same-uid siblings carry DISJOINT bundled segment subsets with
// IDENTICAL file-name lists. update_canonical must (1) detect divergence by comparing
// offsets (not just names) so the union fires, and (2) union bundle_file_offsets in
// lockstep with segments -- otherwise a sibling's bundled segments are silently dropped.
TEST_F(LakeTabletMergerTest, test_tablet_merge_bundled_segments_union_offsets) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto add_seg = [](RowsetMetadataPB* rowset, uint32_t idx, int64_t off) {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("bundle.dat"); // same physical file for every slice
        sm->set_size(100);
        sm->set_num_rows(10);
        sm->set_bundle_file_offset(off);
        sm->set_shared(true);
        sm->set_segment_idx(idx);
    };
    // Two bundled segments per child; the children hold DISJOINT idx subsets {0,1} and
    // {2,3} but identical file-name lists ["bundle.dat","bundle.dat"].
    auto make_child = [&](int64_t tablet_id, uint32_t idx0, int64_t off0, uint32_t idx1, int64_t off1) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(3);
        meta->mutable_schema()->set_keys_type(DUP_KEYS);
        meta->mutable_schema()->set_id(1001);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_overlapped(true);
        rowset->set_num_rows(20);
        rowset->set_data_size(200);
        add_seg(rowset, idx0, off0);
        add_seg(rowset, idx1, off1);
        rowset->mutable_uid()->set_hi(0); // same uid => same logical rowset => dedup at merge
        rowset->mutable_uid()->set_lo(2024);
        return meta;
    };
    ASSERT_OK(put_merge_sources(make_child(child_a, 0, 0, 1, 1024), make_child(child_b, 2, 2048, 3, 3072)));

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
    ASSERT_EQ(1, merged->rowsets_size()); // same uid => single merged rowset
    const auto& mr = merged->rowsets(0);
    // All four bundled slices survive the union (not just canonical child_a's two).
    ASSERT_EQ(4, mr.segment_metas_size()) << "bundled siblings' disjoint segments must union, not drop";
    int bundle_offset_count = 0;
    for (const auto& sm : mr.segment_metas()) {
        if (sm.has_bundle_file_offset()) ++bundle_offset_count;
    }
    ASSERT_EQ(4, bundle_offset_count) << "bundle_file_offset must union in lockstep with segments";
    // segment_idx-sorted order => offsets line up as [0,1024,2048,3072].
    std::vector<int64_t> offsets;
    for (const auto& sm : mr.segment_metas()) offsets.push_back(sm.bundle_file_offset());
    EXPECT_EQ((std::vector<int64_t>{0, 1024, 2048, 3072}), offsets);
}

// Companion regression to the test above, exercising same-uid all-shared siblings
// with UNEQUAL segment counts (one carried two ancestor segments after split, the
// other carried just one). The original DCHECK asserted segments_size parity for
// non-pruned siblings, which would falsely fire here; the fix relaxes the assertion
// to del_files parity only.
TEST_F(LakeTabletMergerTest, test_tablet_merge_multi_level_unequal_count_all_shared_segment_metas) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_child = [&](int64_t tablet_id, const std::vector<std::string>& segment_names,
                          const std::vector<uint32_t>& segment_indexes) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(3);
        meta->mutable_schema()->set_keys_type(DUP_KEYS);
        meta->mutable_schema()->set_id(1001);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(static_cast<int64_t>(segment_names.size() * 5));
        rowset->set_data_size(static_cast<int64_t>(segment_names.size() * 50));
        for (size_t i = 0; i < segment_names.size(); ++i) {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename(segment_names[i]);
            sm->set_size(50);
            sm->set_num_rows(10);
            sm->set_shared(true);
            sm->set_segment_idx(segment_indexes[i]);
        }
        rowset->mutable_uid()->set_hi(0);
        rowset->mutable_uid()->set_lo(2025);
        return meta;
    };
    ASSERT_OK(put_merge_sources(make_child(child_a, {"seg_0.dat", "seg_1.dat"}, {0, 1}),
                                make_child(child_b, {"seg_2.dat"}, {2})));

    ReshardingTabletInfoPB resharding;
    auto& merging = *resharding.mutable_merging_tablet_info();
    merging.add_old_tablet_ids(child_a);
    merging.add_old_tablet_ids(child_b);
    merging.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(2);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, base_version, new_version, txn_info,
                                              false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(1, merged->rowsets_size());
    const auto& mr = merged->rowsets(0);
    std::set<std::string> segs;
    for (const auto& s : mr.segment_metas()) segs.insert(s.filename());
    EXPECT_EQ((std::set<std::string>{"seg_0.dat", "seg_1.dat", "seg_2.dat"}), segs);
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

TEST_F(LakeTabletMergerTest, test_tablet_merging_split_with_upsert_delete) {
    // Split, then each child does independent upsert (new version).
    // Shared rowset is deduped, new rowsets are kept.
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(4);
    set_primary_key_schema(meta_a.get(), 1001);
    // Shared rowset (from split)
    auto* shared_a = meta_a->add_rowsets();
    shared_a->set_id(1);
    shared_a->set_version(1);
    shared_a->set_num_rows(10);
    shared_a->set_data_size(100);
    {
        auto* sm = shared_a->add_segment_metas();
        sm->set_filename("shared_seg.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(shared_a, "shared_seg.dat"); // same uid across siblings => dedup
    // Local upsert (new data after split)
    auto* local_a = meta_a->add_rowsets();
    local_a->set_id(2);
    local_a->set_version(2);
    local_a->set_num_rows(5);
    local_a->set_data_size(50);
    {
        auto* sm = local_a->add_segment_metas();
        sm->set_filename("local_a_seg.dat");
        sm->set_size(50);
        sm->set_num_rows(10);
    }

    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(4);
    set_primary_key_schema(meta_b.get(), 1001);
    // Shared rowset (from split)
    auto* shared_b = meta_b->add_rowsets();
    shared_b->set_id(1);
    shared_b->set_version(1);
    shared_b->set_num_rows(10);
    shared_b->set_data_size(100);
    {
        auto* sm = shared_b->add_segment_metas();
        sm->set_filename("shared_seg.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(shared_b, "shared_seg.dat"); // same uid across siblings => dedup
    // Local upsert (different new data)
    auto* local_b = meta_b->add_rowsets();
    local_b->set_id(2);
    local_b->set_version(3);
    local_b->set_num_rows(3);
    local_b->set_data_size(30);
    {
        auto* sm = local_b->add_segment_metas();
        sm->set_filename("local_b_seg.dat");
        sm->set_size(30);
        sm->set_num_rows(10);
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
    // 1 shared (deduped) + 2 local = 3 rowsets
    ASSERT_EQ(3, merged->rowsets_size());

    // First rowset should be the deduped shared one
    EXPECT_EQ("shared_seg.dat", merged->rowsets(0).segment_metas(0).filename());
    // Remaining two are the local ones
    std::unordered_set<std::string> local_segments;
    for (int i = 1; i < merged->rowsets_size(); ++i) {
        local_segments.insert(merged->rowsets(i).segment_metas(0).filename());
    }
    EXPECT_TRUE(local_segments.count("local_a_seg.dat") > 0);
    EXPECT_TRUE(local_segments.count("local_b_seg.dat") > 0);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_split_with_compaction) {
    // Child A compacted the shared rowset (new rowset replaces it).
    // Child B still has the shared rowset.
    // The compacted rowset in A is local (not shared), so no dedup.
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // Child A: compacted - shared rowset replaced by local
    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(5);
    set_primary_key_schema(meta_a.get(), 1001);
    auto* compacted_a = meta_a->add_rowsets();
    compacted_a->set_id(3);
    compacted_a->set_version(2);
    compacted_a->set_num_rows(10);
    compacted_a->set_data_size(100);
    {
        auto* sm = compacted_a->add_segment_metas();
        sm->set_filename("compacted_a.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
    }
    // not shared - this is the compaction output

    // Child B: still has shared rowset
    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(3);
    set_primary_key_schema(meta_b.get(), 1001);
    auto* shared_b = meta_b->add_rowsets();
    shared_b->set_id(1);
    shared_b->set_version(1);
    shared_b->set_num_rows(10);
    shared_b->set_data_size(100);
    {
        auto* sm = shared_b->add_segment_metas();
        sm->set_filename("shared_seg.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
        sm->set_shared(true);
    }

    set_two_column_pk_schema(meta_a.get(), 1001);
    set_two_column_pk_schema(meta_b.get(), 1001);
    const auto shared_size = write_two_column_segment(child_b, "shared_seg.dat", 100, [](int key) { return key; });
    shared_b->mutable_segment_metas(0)->set_size(shared_size);
    shared_b->mutable_segment_metas(0)->set_num_rows(100);
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
    // Both rowsets should be present (no dedup: different segments)
    ASSERT_EQ(2, merged->rowsets_size());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_shared_rowset_on_non_first_child) {
    // Shared rowset only appears in non-first child (child_b), not in child_a.
    // Child_a has a local rowset. No dedup should happen.
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(3);
    auto* rowset_a = meta_a->add_rowsets();
    rowset_a->set_id(1);
    rowset_a->set_version(1);
    rowset_a->set_num_rows(5);
    rowset_a->set_data_size(50);
    {
        auto* sm = rowset_a->add_segment_metas();
        sm->set_filename("local_a.dat");
        sm->set_size(50);
        sm->set_num_rows(5);
    }

    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(3);
    auto* rowset_b = meta_b->add_rowsets();
    rowset_b->set_id(1);
    rowset_b->set_version(1);
    rowset_b->set_num_rows(10);
    rowset_b->set_data_size(100);
    {
        auto* sm = rowset_b->add_segment_metas();
        sm->set_filename("shared_seg.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
        sm->set_shared(true);
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
    // No dedup: different segments
    ASSERT_EQ(2, merged->rowsets_size());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_delete_only_shared_rowset) {
    // Shared rowset that has no segments, only shared del_files
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_del_only_child = [&](int64_t tablet_id) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(3);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(0);
        rowset->set_data_size(0);
        // No segments, only del_file
        auto* del_file = rowset->add_del_files();
        del_file->set_name("shared_del.dat");
        del_file->set_shared(true);
        del_file->set_origin_rowset_id(1);
        stamp_physical_identity_uid(rowset, "shared_del.dat"); // same uid across siblings => dedup
        return meta;
    };

    auto meta_a = make_del_only_child(child_a);
    auto meta_b = make_del_only_child(child_b);

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
    // Delete-only shared rowset should be deduped
    ASSERT_EQ(1, merged->rowsets_size());
    ASSERT_EQ(1, merged->rowsets(0).del_files_size());
    EXPECT_EQ("shared_del.dat", merged->rowsets(0).del_files(0).name());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_different_split_families) {
    // C (from family A) and D (from family E) merge.
    // Different file names, no dedup expected.
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_c = next_id();
    const int64_t child_d = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_c);
    prepare_tablet_dirs(child_d);
    prepare_tablet_dirs(merged_tablet);

    auto meta_c = std::make_shared<TabletMetadataPB>();
    meta_c->set_id(child_c);
    meta_c->set_version(base_version);
    meta_c->set_next_rowset_id(3);
    auto* rowset_c = meta_c->add_rowsets();
    rowset_c->set_id(1);
    rowset_c->set_version(1);
    rowset_c->set_num_rows(10);
    rowset_c->set_data_size(100);
    {
        auto* sm = rowset_c->add_segment_metas();
        sm->set_filename("family_a_seg.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
        sm->set_shared(true);
    }

    auto meta_d = std::make_shared<TabletMetadataPB>();
    meta_d->set_id(child_d);
    meta_d->set_version(base_version);
    meta_d->set_next_rowset_id(3);
    auto* rowset_d = meta_d->add_rowsets();
    rowset_d->set_id(1);
    rowset_d->set_version(1);
    rowset_d->set_num_rows(10);
    rowset_d->set_data_size(100);
    {
        auto* sm = rowset_d->add_segment_metas();
        sm->set_filename("family_e_seg.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
        sm->set_shared(true);
    }

    ASSERT_OK(put_merge_sources(meta_c, meta_d));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_c);
    merging_info.add_old_tablet_ids(child_d);
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
    // Different families: no dedup
    ASSERT_EQ(2, merged->rowsets_size());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_cross_publish_different_id) {
    // Cross-publish: same txn log applied to both children, producing same segment
    // but different rowset.id(). Should be deduped by is_duplicate_rowset.
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // Both have same shared segment but different rowset IDs (cross-publish)
    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(5);
    auto* rowset_a = meta_a->add_rowsets();
    rowset_a->set_id(1);
    rowset_a->set_version(1);
    rowset_a->set_num_rows(10);
    rowset_a->set_data_size(100);
    {
        auto* sm = rowset_a->add_segment_metas();
        sm->set_filename("cross_pub.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
        sm->set_shared(true);
    }
    // Cross-publish: the same write txn log is applied to both children, so both
    // inherit the SAME write-time uid. Model that with a matching uid here.
    stamp_physical_identity_uid(rowset_a, "cross_pub.dat");

    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(8);
    auto* rowset_b = meta_b->add_rowsets();
    rowset_b->set_id(3); // Different ID from A's rowset
    rowset_b->set_version(1);
    rowset_b->set_num_rows(10);
    rowset_b->set_data_size(100);
    {
        auto* sm = rowset_b->add_segment_metas(); // Same segment
        sm->set_filename("cross_pub.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(rowset_b, "cross_pub.dat"); // same uid as A (cross-publish)

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
    // Should be deduped to 1 rowset
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_EQ("cross_pub.dat", merged->rowsets(0).segment_metas(0).filename());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_dcg_conflict_fail_fast) {
    // Two children independently apply column-mode partial update on the same shared segment.
    // DCG values differ -> should return error.
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(3);
    auto* rowset_a = meta_a->add_rowsets();
    rowset_a->set_id(1);
    rowset_a->set_version(1);
    rowset_a->set_num_rows(10);
    rowset_a->set_data_size(100);
    {
        auto* sm = rowset_a->add_segment_metas();
        sm->set_filename("shared_seg.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(rowset_a, "shared_seg.dat"); // same uid across siblings => dedup
    // DCG from child A's independent partial update
    add_dcg_with_columns(meta_a.get(), 1, "dcg_a.cols", {1}, 1);

    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(3);
    auto* rowset_b = meta_b->add_rowsets();
    rowset_b->set_id(1);
    rowset_b->set_version(1);
    rowset_b->set_num_rows(10);
    rowset_b->set_data_size(100);
    {
        auto* sm = rowset_b->add_segment_metas();
        sm->set_filename("shared_seg.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(rowset_b, "shared_seg.dat"); // same uid across siblings => dedup
    // DCG from child B's different independent partial update
    add_dcg_with_columns(meta_b.get(), 1, "dcg_b.cols", {1}, 1);

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
    auto st = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges);
    // Should fail with NotSupported for DCG conflict
    EXPECT_TRUE(st.is_not_supported()) << st;
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

TEST_F(LakeTabletMergerTest, test_tablet_merging_shared_dcg_dedup) {
    // Two children share the same DCG (from split). Should be deduped successfully.
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_child_with_dcg = [&](int64_t tablet_id) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(3);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename("shared_seg.dat");
            sm->set_size(100);
            sm->set_num_rows(10);
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(rowset, "shared_seg.dat"); // same uid across siblings => dedup
        // Same shared DCG on both children (inherited from split)
        add_dcg_with_columns(meta.get(), 1, "shared_dcg.cols", {1, 2}, 1);
        return meta;
    };

    auto meta_a = make_child_with_dcg(child_a);
    auto meta_b = make_child_with_dcg(child_b);

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
    // Rowset deduped to 1
    ASSERT_EQ(1, merged->rowsets_size());
    // DCG deduped: only one entry for the canonical rssid
    ASSERT_TRUE(merged->has_dcg_meta());
    ASSERT_EQ(1, merged->dcg_meta().dcgs().size());
    auto dcg_it = merged->dcg_meta().dcgs().find(merged->rowsets(0).id());
    ASSERT_TRUE(dcg_it != merged->dcg_meta().dcgs().end());
    ASSERT_EQ(1, dcg_it->second.column_files_size());
    EXPECT_EQ("shared_dcg.cols", dcg_it->second.column_files(0));
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

TEST_F(LakeTabletMergerTest, test_tablet_merging_plaintext_delvec_union_reads_both_sources) {
    constexpr int64_t kNewVersion = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    for (int64_t tablet_id : {child_a, child_b, merged_tablet}) {
        prepare_tablet_dirs(tablet_id);
    }

    const std::string segment_filename = "plaintext_union_segment.dat";
    auto meta_a = make_shared_delvec_source(child_a, {segment_filename});
    auto meta_b = make_shared_delvec_source(child_b, {segment_filename});
    DelVector dv_a;
    const uint32_t deleted_a = 1;
    dv_a.init(/*version=*/10, &deleted_a, 1);
    add_delvec(meta_a.get(), child_a, /*version=*/10, /*segment_id=*/1, "plaintext_union_a.delvec", dv_a.save());
    DelVector dv_b;
    const uint32_t deleted_b = 4;
    dv_b.init(/*version=*/11, &deleted_b, 1);
    add_delvec(meta_b.get(), child_b, /*version=*/11, /*segment_id=*/1, "plaintext_union_b.delvec", dv_b.save());

    int get_del_vec_calls = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("merge_delvecs:before_get_del_vec", [&](void*) { ++get_del_vec_calls; });
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
    });

    ASSIGN_OR_ABORT(auto merged, merge_delvec_sources({meta_a, meta_b}, merged_tablet, kNewVersion));
    EXPECT_EQ(2, get_del_vec_calls);
    ASSERT_EQ(1, merged->rowsets_size());
    const uint32_t target_rssid = merged->rowsets(0).id();
    DelVector expected;
    const uint32_t deleted_rowids[] = {deleted_a, deleted_b};
    expected.init(kNewVersion, deleted_rowids, std::size(deleted_rowids));
    const std::string expected_content = expected.save();
    const uint32_t expected_crc = crc32c::Mask(crc32c::Value(expected_content.data(), expected_content.size()));
    expect_exact_delvec_output(*merged, merged_tablet, kNewVersion, {{target_rssid, expected_content, expected_crc}});

    DelVector loaded;
    LakeIOOptions io_options;
    ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), *merged, target_rssid, false, io_options, &loaded));
    ASSERT_NE(nullptr, loaded.roaring());
    EXPECT_EQ(2, loaded.cardinality());
    EXPECT_TRUE(loaded.roaring()->contains(deleted_a));
    EXPECT_TRUE(loaded.roaring()->contains(deleted_b));
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
        EXPECT_EQ(fmt::format("encrypted delvec input is unsupported; delvec must be plaintext: {}",
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

TEST_F(LakeTabletMergerTest, test_tablet_merging_delvec_merged_duplicate_source_metadata_mismatch_is_rejected) {
    struct MetadataMismatch {
        const char* name;
        std::function<void(FileMetaPB*)> apply;
    };
    const std::vector<MetadataMismatch> mismatches = {
            {"size", [](FileMetaPB* file) { file->set_size(file->size() + 1); }},
            {"shared", [](FileMetaPB* file) { file->set_shared(true); }},
    };

    int writer_invocations = 0;
    SyncPoint::GetInstance()->SetCallBack("merge_delvecs:writer_invocations",
                                          [&](void* arg) { writer_invocations += *static_cast<int*>(arg); });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp cleanup_sync_points([&] {
        SyncPoint::GetInstance()->ClearAllCallBacks();
        SyncPoint::GetInstance()->DisableProcessing();
    });

    for (const auto& mismatch : mismatches) {
        for (bool mismatch_first : {false, true}) {
            SCOPED_TRACE(fmt::format("field={}, order={}", mismatch.name,
                                     mismatch_first ? "mismatch-first" : "baseline-first"));
            const int64_t baseline_x_tablet = next_id();
            const int64_t middle_y_tablet = next_id();
            const int64_t conflicting_x_tablet = next_id();
            const int64_t merged_tablet = next_id();
            for (int64_t tablet_id : {baseline_x_tablet, middle_y_tablet, conflicting_x_tablet, merged_tablet}) {
                prepare_tablet_dirs(tablet_id);
            }

            const std::string segment_filename = fmt::format("merged_duplicate_metadata_{}.dat", mismatch.name);
            const std::string x_filename = fmt::format("merged_duplicate_metadata_{}_x.delvec", mismatch.name);
            const std::string y_filename = fmt::format("merged_duplicate_metadata_{}_y.delvec", mismatch.name);
            auto baseline_x = make_shared_delvec_source(baseline_x_tablet, {segment_filename});
            auto middle_y = make_shared_delvec_source(middle_y_tablet, {segment_filename});
            auto conflicting_x = make_shared_delvec_source(conflicting_x_tablet, {segment_filename});

            DelVector x_delvec;
            const uint32_t x_deleted_rowids[] = {1, 4};
            x_delvec.init(/*version=*/10, x_deleted_rowids, std::size(x_deleted_rowids));
            add_delvec(baseline_x.get(), baseline_x_tablet, /*version=*/10, /*segment_id=*/1, x_filename,
                       x_delvec.save());
            (*conflicting_x->mutable_delvec_meta()->mutable_version_to_file())[/*version=*/10] =
                    baseline_x->delvec_meta().version_to_file().at(/*version=*/10);
            (*conflicting_x->mutable_delvec_meta()->mutable_delvecs())[/*segment_id=*/1] =
                    baseline_x->delvec_meta().delvecs().at(/*segment_id=*/1);
            auto* conflicting_file =
                    &(*conflicting_x->mutable_delvec_meta()->mutable_version_to_file())[/*version=*/10];
            mismatch.apply(conflicting_file);

            DelVector y_delvec;
            const uint32_t y_deleted_rowid = 7;
            y_delvec.init(/*version=*/11, &y_deleted_rowid, 1);
            add_delvec(middle_y.get(), middle_y_tablet, /*version=*/11, /*segment_id=*/1, y_filename, y_delvec.save());

            ASSERT_OK(put_merge_sources(baseline_x, middle_y, conflicting_x));
            ASSIGN_OR_ABORT(auto inventory_before, delvec_inventory(merged_tablet));

            ReshardingTabletInfoPB resharding;
            auto& merging = *resharding.mutable_merging_tablet_info();
            const std::vector<int64_t> source_tablets =
                    mismatch_first ? std::vector<int64_t>{conflicting_x_tablet, middle_y_tablet, baseline_x_tablet}
                                   : std::vector<int64_t>{baseline_x_tablet, middle_y_tablet, conflicting_x_tablet};
            for (int64_t source_tablet : source_tablets) {
                merging.add_old_tablet_ids(source_tablet);
            }
            merging.set_new_tablet_id(merged_tablet);
            TxnInfoPB txn_info;
            txn_info.set_txn_id(next_id());
            txn_info.set_commit_time(1);
            txn_info.set_gtid(1);
            std::unordered_map<int64_t, TabletMetadataPtr> published_metadatas;
            std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
            writer_invocations = 0;
            auto status = lake::publish_resharding_tablet(_tablet_manager.get(), resharding, /*base_version=*/1,
                                                          /*new_version=*/2, txn_info, false, published_metadatas,
                                                          tablet_ranges);

            EXPECT_TRUE(status.is_corruption()) << status;
            EXPECT_TRUE(status.message().contains("metadata mismatch")) << status;
            EXPECT_TRUE(status.message().contains(x_filename)) << status;
            EXPECT_EQ(0, writer_invocations);
            auto target_it = published_metadatas.find(merged_tablet);
            EXPECT_EQ(published_metadatas.end(), target_it);
            if (target_it != published_metadatas.end()) {
                EXPECT_EQ(0, target_it->second->delvec_meta().delvecs_size());
                EXPECT_EQ(0, target_it->second->delvec_meta().version_to_file_size());
            }
            EXPECT_TRUE(_tablet_manager->get_tablet_metadata(merged_tablet, /*version=*/2).status().is_not_found());
            ASSIGN_OR_ABORT(auto inventory_after, delvec_inventory(merged_tablet));
            EXPECT_EQ(inventory_before, inventory_after);
        }
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_delvec_merged_only_writes_plaintext_buffer_output) {
    constexpr int64_t kNewVersion = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    for (int64_t tablet_id : {child_a, child_b, merged_tablet}) {
        prepare_tablet_dirs(tablet_id);
    }

    auto meta_a = make_shared_delvec_source(child_a, {"plaintext_union_segment.dat"});
    auto meta_b = make_shared_delvec_source(child_b, {"plaintext_union_segment.dat"});

    DelVector delvec_a;
    const uint32_t deleted_a = 4;
    delvec_a.init(/*version=*/10, &deleted_a, 1);
    add_delvec(meta_a.get(), child_a, /*version=*/10, /*segment_id=*/1, "plaintext_union_a.delvec", delvec_a.save());
    DelVector delvec_b;
    const uint32_t deleted_b = 7;
    delvec_b.init(/*version=*/11, &deleted_b, 1);
    add_delvec(meta_b.get(), child_b, /*version=*/11, /*segment_id=*/1, "plaintext_union_b.delvec", delvec_b.save());

    DelVector expected_union;
    const uint32_t expected_deleted[] = {4, 7};
    expected_union.init(kNewVersion, expected_deleted, std::size(expected_deleted));
    const std::string expected_union_content = expected_union.save();

    int writer_invocations = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("merge_delvecs:writer_invocations",
                      [&](void* arg) { writer_invocations += *static_cast<int*>(arg); });
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
    });
    ASSIGN_OR_ABORT(auto merged, merge_delvec_sources({meta_a, meta_b}, merged_tablet, kNewVersion));
    EXPECT_EQ(1, writer_invocations);
    ASSERT_EQ(1, merged->rowsets_size());
    const uint32_t target_rssid = merged->rowsets(0).id();
    const uint32_t expected_crc =
            crc32c::Mask(crc32c::Value(expected_union_content.data(), expected_union_content.size()));
    expect_exact_delvec_output(*merged, merged_tablet, kNewVersion,
                               {{target_rssid, expected_union_content, expected_crc}});
    DelVector loaded;
    LakeIOOptions io_options;
    ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), *merged, target_rssid, false, io_options, &loaded));
    ASSERT_NE(nullptr, loaded.roaring());
    EXPECT_EQ(2, loaded.cardinality());
    EXPECT_TRUE(loaded.roaring()->contains(4));
    EXPECT_TRUE(loaded.roaring()->contains(7));
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_delvec_plain_single_plus_merged_writes_plaintext) {
    constexpr int64_t kNewVersion = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    for (int64_t tablet_id : {child_a, child_b, merged_tablet}) {
        prepare_tablet_dirs(tablet_id);
    }

    auto meta_a = make_shared_delvec_source(child_a, {"mixed_segment_0.dat", "mixed_segment_1.dat"});
    auto meta_b = make_shared_delvec_source(child_b, {"mixed_segment_0.dat", "mixed_segment_1.dat"});

    DelVector plain_single;
    const uint32_t plain_deleted = 2;
    plain_single.init(/*version=*/10, &plain_deleted, 1);
    const std::string plain_content = plain_single.save();
    const std::string plain_filename = "mixed_plain_single.delvec";
    add_delvec(meta_a.get(), child_a, /*version=*/10, /*segment_id=*/1, plain_filename, plain_content);
    const uint32_t plain_crc = crc32c::Mask(crc32c::Value(plain_content.data(), plain_content.size()));
    auto& plain_source_page = (*meta_a->mutable_delvec_meta()->mutable_delvecs())[1];
    plain_source_page.set_crc32c(plain_crc);
    plain_source_page.set_crc32c_gen_version(10);

    DelVector merged_a;
    const uint32_t merged_deleted_a = 5;
    merged_a.init(/*version=*/11, &merged_deleted_a, 1);
    add_delvec(meta_a.get(), child_a, /*version=*/11, /*segment_id=*/2, "mixed_merged_a.delvec", merged_a.save());
    DelVector merged_b;
    const uint32_t merged_deleted_b = 8;
    merged_b.init(/*version=*/12, &merged_deleted_b, 1);
    add_delvec(meta_b.get(), child_b, /*version=*/12, /*segment_id=*/2, "mixed_merged_b.delvec", merged_b.save());

    DelVector expected_union;
    const uint32_t expected_merged_deleted[] = {5, 8};
    expected_union.init(kNewVersion, expected_merged_deleted, std::size(expected_merged_deleted));
    const std::string expected_union_content = expected_union.save();

    int writer_invocations = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("merge_delvecs:writer_invocations",
                      [&](void* arg) { writer_invocations += *static_cast<int*>(arg); });
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
    });
    ASSIGN_OR_ABORT(auto merged, merge_delvec_sources({meta_a, meta_b}, merged_tablet, kNewVersion));
    EXPECT_EQ(1, writer_invocations);
    ASSERT_EQ(1, merged->rowsets_size());
    const uint32_t single_target_rssid = merged->rowsets(0).id();
    const uint32_t merged_target_rssid = single_target_rssid + 1;
    const uint32_t merged_crc =
            crc32c::Mask(crc32c::Value(expected_union_content.data(), expected_union_content.size()));
    expect_exact_delvec_output(*merged, merged_tablet, kNewVersion,
                               {{single_target_rssid, plain_content, plain_crc},
                                {merged_target_rssid, expected_union_content, merged_crc}});
    DelVector loaded_single;
    DelVector loaded_merged;
    LakeIOOptions io_options;
    ASSERT_OK(
            lake::get_del_vec(_tablet_manager.get(), *merged, single_target_rssid, false, io_options, &loaded_single));
    ASSERT_OK(
            lake::get_del_vec(_tablet_manager.get(), *merged, merged_target_rssid, false, io_options, &loaded_merged));
    ASSERT_NE(nullptr, loaded_single.roaring());
    ASSERT_NE(nullptr, loaded_merged.roaring());
    EXPECT_TRUE(loaded_single.roaring()->contains(2));
    EXPECT_TRUE(loaded_merged.roaring()->contains(5));
    EXPECT_TRUE(loaded_merged.roaring()->contains(8));
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

TEST_F(LakeTabletMergerTest, test_tablet_merging_delvec_independent_delete) {
    // Split, then each child independently deletes different rows on the shared segment.
    // Delvec pages come from different source files -> roaring union path.
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // Create delvec data: child_a deletes row 0, child_b deletes row 1
    DelVector dv_a;
    const uint32_t dels_a[] = {0};
    dv_a.init(1, dels_a, 1);
    std::string dv_a_data = dv_a.save();

    DelVector dv_b;
    const uint32_t dels_b[] = {1};
    dv_b.init(2, dels_b, 1);
    std::string dv_b_data = dv_b.save();

    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(3);
    set_primary_key_schema(meta_a.get(), 1001);
    auto* rowset_a = meta_a->add_rowsets();
    rowset_a->set_id(1);
    rowset_a->set_version(1);
    rowset_a->set_num_rows(10);
    rowset_a->set_data_size(100);
    {
        auto* sm = rowset_a->add_segment_metas();
        sm->set_filename("shared_seg.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(rowset_a, "shared_seg.dat"); // same uid across siblings => dedup
    // Delvec from child_a's independent delete
    add_delvec(meta_a.get(), child_a, 1, 1, "delvec_a.dv", dv_a_data);

    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(3);
    set_primary_key_schema(meta_b.get(), 1001);
    auto* rowset_b = meta_b->add_rowsets();
    rowset_b->set_id(1);
    rowset_b->set_version(1);
    rowset_b->set_num_rows(10);
    rowset_b->set_data_size(100);
    {
        auto* sm = rowset_b->add_segment_metas();
        sm->set_filename("shared_seg.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(rowset_b, "shared_seg.dat"); // same uid across siblings => dedup
    // Delvec from child_b's different independent delete
    add_delvec(meta_b.get(), child_b, 2, 1, "delvec_b.dv", dv_b_data);

    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(10);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    // Rowset deduped to 1
    ASSERT_EQ(1, merged->rowsets_size());
    // Delvec should exist for the deduped segment with union of both deletes
    ASSERT_TRUE(merged->has_delvec_meta());
    uint32_t target_rssid = merged->rowsets(0).id();
    auto dv_it = merged->delvec_meta().delvecs().find(target_rssid);
    ASSERT_TRUE(dv_it != merged->delvec_meta().delvecs().end());
    // The merged delvec page should have size > 0 (contains union of row 0 and row 1)
    EXPECT_GT(dv_it->second.size(), 0u);
    // Verify delvec content: should contain both row 0 and row 1
    {
        DelVector dv_result;
        LakeIOOptions io_opts;
        ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), *merged, target_rssid, false, io_opts, &dv_result));
        EXPECT_EQ(2, dv_result.cardinality());
        ASSERT_TRUE(dv_result.roaring() != nullptr);
        EXPECT_TRUE(dv_result.roaring()->contains(0));
        EXPECT_TRUE(dv_result.roaring()->contains(1));
    }
    // version_to_file should only have new_version (no new_version+1 or other entries)
    EXPECT_EQ(1, merged->delvec_meta().version_to_file_size());
    EXPECT_TRUE(merged->delvec_meta().version_to_file().find(new_version) !=
                merged->delvec_meta().version_to_file().end());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_delvec_multi_target_union) {
    // 2 children share 2 segments (rssid 1 and rssid 2), each independently deletes different rows.
    // Verifies: both target delvecs exist with size > 0; version_to_file has only new_version.
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // child_a deletes row 0 in segment 1, row 10 in segment 2
    DelVector dv_a1;
    const uint32_t dels_a1[] = {0};
    dv_a1.init(1, dels_a1, 1);
    std::string dv_a1_data = dv_a1.save();

    DelVector dv_a2;
    const uint32_t dels_a2[] = {10};
    dv_a2.init(1, dels_a2, 1);
    std::string dv_a2_data = dv_a2.save();

    // child_b deletes row 1 in segment 1, row 11 in segment 2
    DelVector dv_b1;
    const uint32_t dels_b1[] = {1};
    dv_b1.init(2, dels_b1, 1);
    std::string dv_b1_data = dv_b1.save();

    DelVector dv_b2;
    const uint32_t dels_b2[] = {11};
    dv_b2.init(2, dels_b2, 1);
    std::string dv_b2_data = dv_b2.save();

    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(4);
    set_primary_key_schema(meta_a.get(), 1001);
    auto* rowset_a = meta_a->add_rowsets();
    rowset_a->set_id(1);
    rowset_a->set_version(1);
    rowset_a->set_num_rows(10);
    rowset_a->set_data_size(100);
    {
        auto* sm = rowset_a->add_segment_metas();
        sm->set_filename("shared_seg1.dat");
        sm->set_size(100);
        sm->set_num_rows(5);
        sm->set_shared(true);
    }
    {
        auto* sm = rowset_a->add_segment_metas();
        sm->set_filename("shared_seg2.dat");
        sm->set_size(100);
        sm->set_num_rows(5);
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(rowset_a, "shared_seg1.dat"); // same uid across siblings => dedup
    // Delvec for segment 1 (rssid 1) and segment 2 (rssid 2) from child_a
    // Write a combined delvec file for child_a with both pages
    std::string combined_a = dv_a1_data + dv_a2_data;
    {
        FileMetaPB file_meta;
        file_meta.set_name("delvec_a.dv");
        file_meta.set_size(combined_a.size());
        (*meta_a->mutable_delvec_meta()->mutable_version_to_file())[1] = file_meta;

        DelvecPagePB page1;
        page1.set_version(1);
        page1.set_offset(0);
        page1.set_size(dv_a1_data.size());
        (*meta_a->mutable_delvec_meta()->mutable_delvecs())[1] = page1;

        DelvecPagePB page2;
        page2.set_version(1);
        page2.set_offset(dv_a1_data.size());
        page2.set_size(dv_a2_data.size());
        (*meta_a->mutable_delvec_meta()->mutable_delvecs())[2] = page2;

        write_file(_tablet_manager->delvec_location(child_a, "delvec_a.dv"), combined_a);
    }

    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(4);
    set_primary_key_schema(meta_b.get(), 1001);
    auto* rowset_b = meta_b->add_rowsets();
    rowset_b->set_id(1);
    rowset_b->set_version(1);
    rowset_b->set_num_rows(10);
    rowset_b->set_data_size(100);
    {
        auto* sm = rowset_b->add_segment_metas();
        sm->set_filename("shared_seg1.dat");
        sm->set_size(100);
        sm->set_num_rows(5);
        sm->set_shared(true);
    }
    {
        auto* sm = rowset_b->add_segment_metas();
        sm->set_filename("shared_seg2.dat");
        sm->set_size(100);
        sm->set_num_rows(5);
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(rowset_b, "shared_seg1.dat"); // same uid across siblings => dedup
    // Delvec for segment 1 and 2 from child_b
    std::string combined_b = dv_b1_data + dv_b2_data;
    {
        FileMetaPB file_meta;
        file_meta.set_name("delvec_b.dv");
        file_meta.set_size(combined_b.size());
        (*meta_b->mutable_delvec_meta()->mutable_version_to_file())[2] = file_meta;

        DelvecPagePB page1;
        page1.set_version(2);
        page1.set_offset(0);
        page1.set_size(dv_b1_data.size());
        (*meta_b->mutable_delvec_meta()->mutable_delvecs())[1] = page1;

        DelvecPagePB page2;
        page2.set_version(2);
        page2.set_offset(dv_b1_data.size());
        page2.set_size(dv_b2_data.size());
        (*meta_b->mutable_delvec_meta()->mutable_delvecs())[2] = page2;

        write_file(_tablet_manager->delvec_location(child_b, "delvec_b.dv"), combined_b);
    }

    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(10);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    // Rowset deduped to 1
    ASSERT_EQ(1, merged->rowsets_size());
    ASSERT_TRUE(merged->has_delvec_meta());
    uint32_t rssid = merged->rowsets(0).id();
    // Both target delvecs should exist
    auto dv_it1 = merged->delvec_meta().delvecs().find(rssid);
    ASSERT_TRUE(dv_it1 != merged->delvec_meta().delvecs().end());
    EXPECT_GT(dv_it1->second.size(), 0u);
    auto dv_it2 = merged->delvec_meta().delvecs().find(rssid + 1);
    ASSERT_TRUE(dv_it2 != merged->delvec_meta().delvecs().end());
    EXPECT_GT(dv_it2->second.size(), 0u);
    // Verify content: segment 1 should have rows {0, 1}, segment 2 should have rows {10, 11}
    {
        DelVector dv1;
        LakeIOOptions io_opts;
        ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), *merged, rssid, false, io_opts, &dv1));
        EXPECT_EQ(2, dv1.cardinality());
        ASSERT_TRUE(dv1.roaring() != nullptr);
        EXPECT_TRUE(dv1.roaring()->contains(0));
        EXPECT_TRUE(dv1.roaring()->contains(1));
    }
    {
        DelVector dv2;
        LakeIOOptions io_opts;
        ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), *merged, rssid + 1, false, io_opts, &dv2));
        EXPECT_EQ(2, dv2.cardinality());
        ASSERT_TRUE(dv2.roaring() != nullptr);
        EXPECT_TRUE(dv2.roaring()->contains(10));
        EXPECT_TRUE(dv2.roaring()->contains(11));
    }
    // version_to_file should only have new_version
    EXPECT_EQ(1, merged->delvec_meta().version_to_file_size());
    EXPECT_TRUE(merged->delvec_meta().version_to_file().find(new_version) !=
                merged->delvec_meta().version_to_file().end());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_delvec_three_way_union) {
    // 3 children share 1 segment, each independently deletes a different row (0, 1, 2).
    // Verifies: merge succeeds; delvec exists with size > 0.
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t child_c = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(child_c);
    prepare_tablet_dirs(merged_tablet);

    DelVector dv_a;
    const uint32_t dels_a[] = {0};
    dv_a.init(1, dels_a, 1);
    std::string dv_a_data = dv_a.save();

    DelVector dv_b;
    const uint32_t dels_b[] = {1};
    dv_b.init(2, dels_b, 1);
    std::string dv_b_data = dv_b.save();

    DelVector dv_c;
    const uint32_t dels_c[] = {2};
    dv_c.init(3, dels_c, 1);
    std::string dv_c_data = dv_c.save();

    auto make_child_meta = [&](int64_t tablet_id, int64_t delvec_version, const std::string& delvec_file_name,
                               const std::string& delvec_data) {
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
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename("shared_seg.dat");
            sm->set_size(100);
            sm->set_num_rows(10);
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(rowset, "shared_seg.dat"); // same uid across siblings => dedup
        add_delvec(meta.get(), tablet_id, delvec_version, 1, delvec_file_name, delvec_data);
        return meta;
    };

    auto meta_a = make_child_meta(child_a, 1, "delvec_a.dv", dv_a_data);
    auto meta_b = make_child_meta(child_b, 2, "delvec_b.dv", dv_b_data);
    auto meta_c = make_child_meta(child_c, 3, "delvec_c.dv", dv_c_data);

    ASSERT_OK(put_merge_sources(meta_a, meta_b, meta_c));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.add_old_tablet_ids(child_c);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(10);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(1, merged->rowsets_size());
    ASSERT_TRUE(merged->has_delvec_meta());
    uint32_t target_rssid = merged->rowsets(0).id();
    auto dv_it = merged->delvec_meta().delvecs().find(target_rssid);
    ASSERT_TRUE(dv_it != merged->delvec_meta().delvecs().end());
    EXPECT_GT(dv_it->second.size(), 0u);
    // Verify content: should contain rows {0, 1, 2} from three children
    {
        DelVector dv_result;
        LakeIOOptions io_opts;
        ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), *merged, target_rssid, false, io_opts, &dv_result));
        EXPECT_EQ(3, dv_result.cardinality());
        ASSERT_TRUE(dv_result.roaring() != nullptr);
        EXPECT_TRUE(dv_result.roaring()->contains(0));
        EXPECT_TRUE(dv_result.roaring()->contains(1));
        EXPECT_TRUE(dv_result.roaring()->contains(2));
    }
    // version_to_file should only have new_version
    EXPECT_EQ(1, merged->delvec_meta().version_to_file_size());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_delvec_no_independent_delete) {
    // 2 children share 1 segment and the same delvec (same file name, same offset/size).
    // Verifies: all goes through single_source path; version_to_file has only new_version.
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // Same delvec data for both children (split scenario, no independent delete)
    DelVector dv;
    const uint32_t dels[] = {0, 1};
    dv.init(1, dels, 2);
    std::string dv_data = dv.save();

    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(3);
    set_primary_key_schema(meta_a.get(), 1001);
    auto* rowset_a = meta_a->add_rowsets();
    rowset_a->set_id(1);
    rowset_a->set_version(1);
    rowset_a->set_num_rows(10);
    rowset_a->set_data_size(100);
    {
        auto* sm = rowset_a->add_segment_metas();
        sm->set_filename("shared_seg.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(rowset_a, "shared_seg.dat"); // same uid across siblings => dedup
    // Both children reference the same delvec file (shared after split)
    add_delvec(meta_a.get(), child_a, 1, 1, "shared_delvec.dv", dv_data);

    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(3);
    set_primary_key_schema(meta_b.get(), 1001);
    auto* rowset_b = meta_b->add_rowsets();
    rowset_b->set_id(1);
    rowset_b->set_version(1);
    rowset_b->set_num_rows(10);
    rowset_b->set_data_size(100);
    {
        auto* sm = rowset_b->add_segment_metas();
        sm->set_filename("shared_seg.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(rowset_b, "shared_seg.dat"); // same uid across siblings => dedup
    // Same file name, same offset/size -> page-ref dedup
    add_delvec(meta_b.get(), child_b, 1, 1, "shared_delvec.dv", dv_data);

    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(10);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(1, merged->rowsets_size());
    ASSERT_TRUE(merged->has_delvec_meta());
    uint32_t target_rssid = merged->rowsets(0).id();
    auto dv_it = merged->delvec_meta().delvecs().find(target_rssid);
    ASSERT_TRUE(dv_it != merged->delvec_meta().delvecs().end());
    EXPECT_GT(dv_it->second.size(), 0u);
    EXPECT_EQ(new_version, dv_it->second.version());
    // Verify content: should contain rows {0, 1} (original delvec preserved via dedup)
    {
        DelVector dv_result;
        LakeIOOptions io_opts;
        ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), *merged, target_rssid, false, io_opts, &dv_result));
        EXPECT_EQ(2, dv_result.cardinality());
        ASSERT_TRUE(dv_result.roaring() != nullptr);
        EXPECT_TRUE(dv_result.roaring()->contains(0));
        EXPECT_TRUE(dv_result.roaring()->contains(1));
    }
    // version_to_file should only have new_version (single_source path, no union file)
    EXPECT_EQ(1, merged->delvec_meta().version_to_file_size());
    EXPECT_TRUE(merged->delvec_meta().version_to_file().find(new_version) !=
                merged->delvec_meta().version_to_file().end());
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

TEST_F(LakeTabletMergerTest, test_tablet_merging_preflight_accepts_matching_idg_declaration) {
    const std::string segment = "matching_idg_declaration.dat";
    auto source_a = make_preflight_sidecar_source(next_id(), segment, /*shared_segment=*/false,
                                                  /*common_rowset_uid=*/true);
    auto source_b = make_preflight_sidecar_source(next_id(), segment, /*shared_segment=*/true,
                                                  /*common_rowset_uid=*/true);
    add_idg_with_key(source_a.get(), /*segment_id=*/1, "matching.idx", /*col_uid=*/5, BITMAP, /*version=*/1,
                     /*shared_file=*/false);
    add_idg_with_key(source_b.get(), /*segment_id=*/1, "matching.idx", /*col_uid=*/5, BITMAP, /*version=*/1,
                     /*shared_file=*/true);
    source_a->mutable_idg_meta()->mutable_idgs()->at(1).mutable_entries(0)->set_file_size(128);
    source_b->mutable_idg_meta()->mutable_idgs()->at(1).mutable_entries(0)->set_file_size(128);

    const int64_t target_id = next_id();
    prepare_tablet_dirs(source_a->id());
    prepare_tablet_dirs(source_b->id());
    prepare_tablet_dirs(target_id);
    auto merged_or = publish_verbatim_merge({source_a, source_b}, target_id, /*target_version=*/2);
    if (!merged_or.ok()) {
        ADD_FAILURE() << merged_or.status();
        return;
    }
    auto merged = std::move(merged_or).value();
    ASSERT_EQ(1, merged->idg_meta().idgs_size());
    ASSERT_EQ(1, merged->idg_meta().idgs().at(1).entries_size());
    EXPECT_TRUE(merged->idg_meta().idgs().at(1).entries(0).shared_file());
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

TEST_F(LakeTabletMergerTest, test_tablet_merging_cross_target_same_base_sidecars_survive) {
    const int64_t source_a_id = next_id();
    const int64_t source_b_id = next_id();
    const int64_t target_id = next_id();
    prepare_tablet_dirs(source_a_id);
    prepare_tablet_dirs(source_b_id);
    prepare_tablet_dirs(target_id);
    const std::string base_name = "cross_target_shared_base.dat";
    const std::string cols_name = "cross_target.cols";
    const std::string idx_name = "cross_target.idx";
    const std::string dead_idx_name = "cross_target_dead.idx";
    const auto bundled_segments =
            write_two_column_bundled_segments(source_a_id, base_name, 2, [](int key) { return key * 10; });
    ASSERT_EQ(2, bundled_segments.size());
    const auto [base_size, base_offset] = bundled_segments[0];
    const auto [empty_size, empty_offset] = bundled_segments[1];
    ASSERT_GT(base_offset, 0);
    ASSERT_GT(empty_offset, base_offset);
    const uint64_t cols_size =
            write_c1_only_cols_file(source_a_id, cols_name, 2, [](int row) { return (row + 1) * 1000; });
    const auto idx_file = write_sidecar_payload(_tablet_manager->segment_location(source_a_id, idx_name),
                                                "cross-target-index", /*encrypted=*/false);
    const auto dead_idx_file = write_sidecar_payload(_tablet_manager->segment_location(source_a_id, dead_idx_name),
                                                     "cross-target-dead-index", /*encrypted=*/false);

    auto source_a = make_preflight_sidecar_source(source_a_id, base_name, /*shared_segment=*/false);
    auto source_b = make_preflight_sidecar_source(source_b_id, base_name, /*shared_segment=*/true);
    for (int i = 0; i < 2; ++i) {
        auto* source = i == 0 ? source_a.get() : source_b.get();
        set_two_column_pk_schema(source, /*schema_id=*/4001);
        source->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
        source->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(i));
        source->mutable_range()->set_lower_bound_included(true);
        source->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(i + 1));
        source->mutable_range()->set_upper_bound_included(false);
        auto* rowset = source->mutable_rowsets(0);
        rowset->set_num_rows(2);
        rowset->set_data_size(base_size + empty_size);
        rowset->mutable_range()->CopyFrom(source->range());
        rowset->mutable_segment_metas(0)->set_size(base_size);
        rowset->mutable_segment_metas(0)->set_bundle_file_offset(base_offset);
        rowset->mutable_segment_metas(0)->set_num_rows(2);
        auto* empty_segment = rowset->add_segment_metas();
        empty_segment->set_filename(base_name);
        empty_segment->set_size(empty_size);
        empty_segment->set_bundle_file_offset(empty_offset);
        empty_segment->set_num_rows(0);
        empty_segment->set_segment_idx(1);
        empty_segment->set_shared(rowset->segment_metas(0).shared());
        source->set_next_rowset_id(3);
        add_dcg_with_columns(source, /*segment_id=*/1, cols_name, {1002}, /*version=*/1);
        auto* dcg = &(*source->mutable_dcg_meta()->mutable_dcgs())[1];
        dcg->add_column_file_sizes(cols_size);
        dcg->set_shared_files(0, false);
        add_idg_with_key(source, /*segment_id=*/1, idx_name, /*col_uid=*/1002, BITMAP, /*version=*/1,
                         /*shared_file=*/false);
        add_idg_key(source, /*segment_id=*/1, /*col_uid=*/1003, BITMAP);
        add_idg_with_key(source, /*segment_id=*/1, dead_idx_name, /*col_uid=*/1004, BITMAP, /*version=*/1,
                         /*shared_file=*/false);
        auto* idg = &(*source->mutable_idg_meta()->mutable_idgs())[1];
        idg->mutable_entries(0)->set_file_size(idx_file.filesize);
        idg->mutable_entries(1)->set_file_size(dead_idx_file.filesize);
        if (i == 1) {
            auto* partial_drop = idg->mutable_entries(0)->add_dropped_keys();
            partial_drop->set_col_unique_id(1002);
            partial_drop->set_index_type(BITMAP);
            auto* full_drop = idg->mutable_entries(1)->add_dropped_keys();
            full_drop->set_col_unique_id(1004);
            full_drop->set_index_type(BITMAP);
        }
    }

    auto merged_or = publish_verbatim_merge({source_a, source_b}, target_id, /*target_version=*/2);
    if (!merged_or.ok()) {
        ADD_FAILURE() << merged_or.status();
        return;
    }
    auto merged = std::move(merged_or).value();

    ASSERT_EQ(2, merged->rowsets_size());
    ASSERT_EQ(2, merged->dcg_meta().dcgs_size());
    ASSERT_EQ(2, merged->idg_meta().idgs_size());
    for (const auto& rowset : merged->rowsets()) {
        ASSERT_EQ(2, rowset.segment_metas_size());
        EXPECT_TRUE(std::all_of(rowset.segment_metas().begin(), rowset.segment_metas().end(),
                                [](const auto& segment) { return segment.shared(); }));
        EXPECT_TRUE(std::all_of(rowset.segment_metas().begin(), rowset.segment_metas().end(),
                                [](const auto& segment) { return segment.has_bundle_file_offset(); }));
    }
    for (const auto& [rssid, dcg] : merged->dcg_meta().dcgs()) {
        (void)rssid;
        ASSERT_EQ(1, dcg.shared_files_size());
        EXPECT_TRUE(dcg.shared_files(0));
    }
    for (const auto& [rssid, idg] : merged->idg_meta().idgs()) {
        ASSERT_EQ(1, idg.entries_size());
        EXPECT_EQ(idx_name, idg.entries(0).index_file());
        EXPECT_TRUE(idg.entries(0).shared_file());
        lake::LakeIndexDeltaGroupLoader loader(merged);
        lake::IndexDeltaGroupList loaded;
        ASSERT_OK(loader.load(TabletSegmentId(target_id, rssid), merged->version(), &loaded));
        ASSERT_EQ(1, loaded.size());
        ASSERT_EQ(1, loaded[0].keys.size());
        EXPECT_EQ(1003, loaded[0].keys[0].col_unique_id);
        EXPECT_EQ(BITMAP, loaded[0].keys[0].index_type);
    }
    EXPECT_TRUE(std::none_of(merged->orphan_files().begin(), merged->orphan_files().end(),
                             [&](const FileMetaPB& file) { return file.name() == idx_name; }));
    ASSERT_EQ(1, std::count_if(merged->orphan_files().begin(), merged->orphan_files().end(),
                               [&](const FileMetaPB& file) { return file.name() == dead_idx_name; }));

    ASSERT_OK(put_tablet_metadata(merged));
    _tablet_manager->prune_metacache();
    ASSIGN_OR_ABORT(auto reopened, _tablet_manager->get_tablet_metadata(target_id, merged->version()));
    ASSIGN_OR_ABORT(auto reopened_rows, read_two_column_rows(reopened));
    EXPECT_EQ((std::vector<std::pair<int32_t, int32_t>>{{0, 1000}, {1, 2000}}), reopened_rows);

    TabletMetadataPB compacted(*reopened);
    compacted.set_version(reopened->version() + 1);
    ASSERT_EQ(2, compacted.rowsets_size());
    const int retired_index =
            compacted.rowsets(0).range().lower_bound().SerializeAsString() == generate_sort_key(0).SerializeAsString()
                    ? 0
                    : 1;
    RowsetMetadataPB retired(compacted.rowsets(retired_index));
    RowsetMetadataPB retained(compacted.rowsets(1 - retired_index));
    compacted.clear_rowsets();
    compacted.add_rowsets()->CopyFrom(retained);
    compacted.clear_compaction_inputs();
    compacted.add_compaction_inputs()->CopyFrom(retired);
    const std::string compacted_name = "cross_target_compacted.dat";
    const uint64_t compacted_size = write_two_column_segment(target_id, compacted_name, 1, [](int) { return 1000; });
    auto* output = compacted.add_rowsets();
    output->set_id(compacted.next_rowset_id());
    output->set_version(compacted.version());
    output->set_num_rows(1);
    output->set_data_size(compacted_size);
    output->set_overlapped(false);
    output->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(0));
    output->mutable_range()->set_lower_bound_included(true);
    output->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(1));
    output->mutable_range()->set_upper_bound_included(false);
    auto* output_segment = output->add_segment_metas();
    output_segment->set_filename(compacted_name);
    output_segment->set_size(compacted_size);
    output_segment->set_num_rows(1);
    lake::tablet_reshard_helper::set_rowset_uid(output);
    compacted.set_next_rowset_id(output->id() + 1);
    compacted.mutable_dcg_meta()->mutable_dcgs()->erase(retired.id());
    compacted.mutable_idg_meta()->mutable_idgs()->erase(retired.id());
    compacted.mutable_delvec_meta()->mutable_delvecs()->erase(retired.id());
    compacted.clear_orphan_files();
    auto add_orphan = [&](const std::string& name, int64_t size, bool shared, int64_t version) {
        auto* orphan = compacted.add_orphan_files();
        orphan->set_name(name);
        orphan->set_size(size);
        orphan->set_shared(shared);
        orphan->set_version(version);
    };
    add_orphan(cols_name, cols_size, /*shared=*/true, /*version=*/1);
    add_orphan(idx_name, idx_file.filesize, /*shared=*/true, /*version=*/1);
    add_orphan(dead_idx_name, dead_idx_file.filesize, /*shared=*/true, /*version=*/1);
    const std::string private_control_name = "cross_target_private_control.idx";
    const auto private_control = write_sidecar_payload(
            _tablet_manager->segment_location(target_id, private_control_name), "private-vacuum-control",
            /*encrypted=*/false);
    add_orphan(private_control_name, private_control.filesize, /*shared=*/false, compacted.version());
    ASSERT_EQ(1, compacted.compaction_inputs_size());
    ASSERT_EQ(4, compacted.orphan_files_size());
    ASSERT_OK(put_tablet_metadata(compacted));

    _tablet_manager->prune_metacache();
    ASSIGN_OR_ABORT(auto cold_compacted, _tablet_manager->get_tablet_metadata(target_id, compacted.version()));
    ASSIGN_OR_ABORT(auto compacted_rows, read_two_column_rows(cold_compacted));
    EXPECT_EQ((std::vector<std::pair<int32_t, int32_t>>{{0, 1000}, {1, 2000}}), compacted_rows);
    EXPECT_OK(FileSystem::Default()->path_exists(_tablet_manager->segment_location(target_id, private_control_name)));

    VacuumRequest request;
    VacuumResponse response;
    auto* info = request.add_tablet_infos();
    info->set_tablet_id(target_id);
    info->set_min_version(compacted.version());
    request.set_min_retain_version(compacted.version());
    request.set_grace_timestamp(::time(nullptr) + 3600);
    request.set_min_active_txn_id(std::numeric_limits<int64_t>::max());
    request.set_enable_file_bundling(false);
    request.set_enable_shared_file_cleanup(true);
    request.set_delete_txn_log(false);
    lake::vacuum(_tablet_manager.get(), request, &response);
    ASSERT_TRUE(response.has_status());
    ASSERT_EQ(0, response.status().status_code())
            << (response.status().error_msgs_size() > 0 ? response.status().error_msgs(0) : "");
    EXPECT_OK(FileSystem::Default()->path_exists(_tablet_manager->segment_location(target_id, base_name)));
    EXPECT_OK(FileSystem::Default()->path_exists(_tablet_manager->segment_location(target_id, cols_name)));
    EXPECT_OK(FileSystem::Default()->path_exists(_tablet_manager->segment_location(target_id, idx_name)));
    EXPECT_TRUE(FileSystem::Default()
                        ->path_exists(_tablet_manager->segment_location(target_id, dead_idx_name))
                        .is_not_found());
    EXPECT_TRUE(FileSystem::Default()
                        ->path_exists(_tablet_manager->segment_location(target_id, private_control_name))
                        .is_not_found());

    _tablet_manager->prune_metacache();
    ASSIGN_OR_ABORT(auto reopened_after_vacuum, _tablet_manager->get_tablet_metadata(target_id, compacted.version()));
    ASSIGN_OR_ABORT(auto rows_after_vacuum, read_two_column_rows(reopened_after_vacuum));
    EXPECT_EQ((std::vector<std::pair<int32_t, int32_t>>{{0, 1000}, {1, 2000}}), rows_after_vacuum);
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

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_mixed_bundle_presence_after_segment_union_before_io) {
    for (bool reverse_sources : {false, true}) {
        SCOPED_TRACE(fmt::format("{} source first", reverse_sources ? "standalone" : "bundled"));
        auto bundled = make_preflight_sidecar_source(next_id(), fmt::format("mixed_bundle_{}.dat", next_id()));
        auto standalone = make_preflight_sidecar_source(next_id(), fmt::format("mixed_standalone_{}.dat", next_id()));
        auto* bundled_rowset = bundled->mutable_rowsets(0);
        auto* standalone_rowset = standalone->mutable_rowsets(0);
        bundled_rowset->mutable_segment_metas(0)->set_bundle_file_offset(0);
        bundled_rowset->mutable_segment_metas(0)->set_size(128);
        standalone_rowset->mutable_segment_metas(0)->set_segment_idx(1);
        standalone_rowset->mutable_uid()->CopyFrom(bundled_rowset->uid());
        std::vector<TabletMetadataPtr> sources = {bundled, standalone};
        if (reverse_sources) std::swap(sources[0], sources[1]);

        auto status = expect_merge_rejected_without_writes(sources, next_id(), /*target_version=*/2);
        EXPECT_TRUE(status.message().contains("bundled")) << status;
        EXPECT_TRUE(status.message().contains("standalone")) << status;
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
        EXPECT_TRUE(status.message().contains(filename)) << status;
        EXPECT_TRUE(status.message().contains("multiple segment indices")) << status;
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

TEST_F(LakeTabletMergerTest, test_tablet_merging_accepts_uniform_bundle_presence_after_segment_union) {
    for (bool bundled : {false, true}) {
        for (bool reverse_sources : {false, true}) {
            SCOPED_TRACE(fmt::format("{}; {} source first", bundled ? "bundled" : "standalone",
                                     reverse_sources ? "right" : "left"));
            auto left = make_preflight_sidecar_source(next_id(), fmt::format("uniform_left_{}.dat", next_id()));
            auto right = make_preflight_sidecar_source(next_id(), fmt::format("uniform_right_{}.dat", next_id()));
            auto* left_rowset = left->mutable_rowsets(0);
            auto* right_rowset = right->mutable_rowsets(0);
            right_rowset->mutable_segment_metas(0)->set_segment_idx(1);
            right_rowset->mutable_uid()->CopyFrom(left_rowset->uid());
            if (bundled) {
                left_rowset->mutable_segment_metas(0)->set_bundle_file_offset(0);
                left_rowset->mutable_segment_metas(0)->set_size(128);
                right_rowset->mutable_segment_metas(0)->set_bundle_file_offset(128);
                right_rowset->mutable_segment_metas(0)->set_size(128);
            }
            std::vector<TabletMetadataPtr> sources = {left, right};
            if (reverse_sources) std::swap(sources[0], sources[1]);

            const int64_t target_id = next_id();
            ASSIGN_OR_ABORT(auto files_before,
                            directory_inventory(_location_provider->segment_root_location(target_id)));
            ASSIGN_OR_ABORT(auto merged, publish_verbatim_merge(sources, target_id, /*target_version=*/2));
            ASSERT_EQ(1, merged->rowsets_size());
            ASSERT_EQ(2, merged->rowsets(0).segment_metas_size());
            EXPECT_EQ(bundled ? 2 : 0, std::count_if(merged->rowsets(0).segment_metas().begin(),
                                                     merged->rowsets(0).segment_metas().end(), [](const auto& segment) {
                                                         return segment.has_bundle_file_offset();
                                                     }));
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

TEST_F(LakeTabletMergerTest, test_tablet_merging_same_physical_sst_allows_logical_attachment_differences) {
    struct LogicalAttachment {
        const char* name;
        std::function<void(PersistentIndexSstablePB*)> apply;
        bool reuse_when_ranges_ordered = false;
    };
    const std::vector<LogicalAttachment> attachments = {
            {"deprecated version", [](PersistentIndexSstablePB* sst) { sst->set_version(2); }},
            {"max rss rowid", [](PersistentIndexSstablePB* sst) { sst->set_max_rss_rowid((uint64_t{2} << 32) | 99); }},
            {"shared", [](PersistentIndexSstablePB* sst) { sst->set_shared(false); }},
            {"shared rssid", [](PersistentIndexSstablePB* sst) { sst->set_shared_rssid(2); }},
            {"shared version", [](PersistentIndexSstablePB* sst) { sst->set_shared_version(2); }},
            {"embedded delvec",
             [](PersistentIndexSstablePB* sst) {
                 sst->mutable_delvec()->set_version(1);
                 sst->mutable_delvec()->set_size(1);
             }},
            {"fileset",
             [](PersistentIndexSstablePB* sst) {
                 sst->mutable_fileset_id()->set_hi(2);
                 sst->mutable_fileset_id()->set_lo(2);
             },
             true},
            {"rssid offset", [](PersistentIndexSstablePB* sst) { sst->set_rssid_offset(1); }},
            {"generation version", [](PersistentIndexSstablePB* sst) { sst->set_generation_version(2); }},
    };

    std::vector<std::pair<int32_t, int32_t>> expected_rows;
    for (int key = 0; key < 100; ++key) expected_rows.emplace_back(key, key * 10);
    for (const auto& attachment : attachments) {
        for (bool reverse_sources : {false, true}) {
            SCOPED_TRACE(fmt::format("{}; reverse={}", attachment.name, reverse_sources));
            auto sources = make_readable_preflight_sst_sources(fmt::format("sst_attachment_{}", next_id()));
            attachment.apply(sources[1]->mutable_sstable_meta()->mutable_sstables(0));
            std::vector<TabletMetadataPtr> immutable_sources(sources.begin(), sources.end());
            if (reverse_sources) std::swap(immutable_sources[0], immutable_sources[1]);
            const int64_t target = next_id();
            prepare_tablet_dirs(target);
            int classifier_entries = 0;
            auto* sync = SyncPoint::GetInstance();
            sync->SetCallBack("merge_sstables:metadata_classifier_entry", [&](void*) { ++classifier_entries; });
            sync->EnableProcessing();
            DeferOp clear_classifier_callback([&] {
                sync->ClearCallBack("merge_sstables:metadata_classifier_entry");
                sync->DisableProcessing();
            });
            std::unordered_map<int64_t, TabletMetadataPtr> published;
            ASSERT_OK(publish_resharding_merge(immutable_sources, target, /*base_version=*/1, /*new_version=*/2,
                                               next_id(), published));
            auto merged = published.at(target);
            EXPECT_EQ(1, classifier_entries);
            EXPECT_EQ(attachment.reuse_when_ranges_ordered && !reverse_sources ? 1 : 0,
                      merged->sstable_meta().sstables_size());
            _update_manager->unload_and_remove_primary_index(target);
            expect_lifecycle_oracle(merged, expected_rows, {});
        }
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_preflight_rejects_late_sst_file_conflict_before_io) {
    ensure_kek_in_key_cache();
    ASSIGN_OR_ABORT(auto encryption_a, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
    ASSIGN_OR_ABORT(auto encryption_b, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
    struct SstConflict {
        const char* name;
        std::function<void(std::vector<std::shared_ptr<TabletMetadataPB>>&)> apply;
    };
    const std::vector<SstConflict> conflicts = {
            {"size",
             [](auto& sources) {
                 auto* sst = sources[1]->mutable_sstable_meta()->mutable_sstables(0);
                 sst->set_filesize(sst->filesize() + 1);
             }},
            {"size presence",
             [](auto& sources) { sources[1]->mutable_sstable_meta()->mutable_sstables(0)->clear_filesize(); }},
            {"encryption",
             [&](auto& sources) {
                 sources[1]->mutable_sstable_meta()->mutable_sstables(0)->set_encryption_meta(
                         encryption_b.encryption_meta);
             }},
            {"encryption presence",
             [](auto& sources) { sources[1]->mutable_sstable_meta()->mutable_sstables(0)->clear_encryption_meta(); }},
            {"range presence",
             [](auto& sources) { sources[1]->mutable_sstable_meta()->mutable_sstables(0)->clear_range(); }},
            {"range value",
             [&](auto& sources) {
                 sources[1]->mutable_sstable_meta()->mutable_sstables(0)->mutable_range()->set_start_key(
                         encode_int_primary_key(11));
             }},
            {"unknown field",
             [](auto& sources) {
                 auto* sst = sources[1]->mutable_sstable_meta()->mutable_sstables(0);
                 sst->GetReflection()->MutableUnknownFields(sst)->AddVarint(1000, 1);
             }},
            {"empty filename",
             [](auto& sources) { sources[1]->mutable_sstable_meta()->mutable_sstables(0)->set_filename(""); }},
            {"negative size",
             [](auto& sources) { sources[1]->mutable_sstable_meta()->mutable_sstables(0)->set_filesize(-1); }},
            {"invalid range arity",
             [](auto& sources) {
                 auto* lower = sources[0]->mutable_range()->mutable_lower_bound();
                 lower->add_values()->CopyFrom(lower->values(0));
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
            if (std::string_view(conflict.name) != "invalid range arity") {
                EXPECT_TRUE(status.message().contains("SST")) << status;
            }
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

TEST_F(LakeTabletMergerTest, test_tablet_merging_dcg_exact_dedup) {
    // Both children have the same .cols file (inherited from split).
    // Exact dedup should keep only one entry.
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
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename("shared_seg.dat");
            sm->set_size(100);
            sm->set_num_rows(10);
            sm->set_shared(true);
        }
        // Matching uid across siblings so the rowsets dedup at merge — without this
        // the test put_tablet_metadata wrapper would auto-mint distinct random uids
        // and the DCG-dedup invariant below would not actually be exercised.
        stamp_physical_identity_uid(rowset, "shared_seg.dat");
        add_dcg_with_columns(meta.get(), 1, "shared.cols", {1, 2}, 1);
        return meta;
    };

    auto meta_a = make_child(child_a);
    auto meta_b = make_child(child_b);

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
    // Both children's rowsets share one uid → rowset dedup leaves a single merged
    // rowset, and DCG-exact-dedup folds the two identical .cols entries into one.
    ASSERT_EQ(1, merged->rowsets_size()) << "matching-uid rowsets must dedup at merge";
    auto dcg_it = merged->dcg_meta().dcgs().find(merged->rowsets(0).id());
    ASSERT_TRUE(dcg_it != merged->dcg_meta().dcgs().end());
    ASSERT_EQ(1, dcg_it->second.column_files_size());
    EXPECT_EQ("shared.cols", dcg_it->second.column_files(0));
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_dcg_same_column_conflict) {
    // child_a and child_b both update column {1} with different .cols files.
    // Same column conflict -> NotSupported.
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
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename("shared_seg.dat");
            sm->set_size(100);
            sm->set_num_rows(10);
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(rowset, "shared_seg.dat"); // same uid across siblings => dedup
        return meta;
    };

    auto meta_a = make_child(child_a);
    add_dcg_with_columns(meta_a.get(), 1, "a.cols", {1}, 1);

    auto meta_b = make_child(child_b);
    add_dcg_with_columns(meta_b.get(), 1, "b.cols", {1}, 1);

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
    auto st = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_TRUE(st.is_not_supported()) << st;
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_dcg_partial_overlap) {
    // child_a updates columns {1,2}, child_b updates columns {2,3}.
    // Column 2 overlaps -> NotSupported.
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
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename("shared_seg.dat");
            sm->set_size(100);
            sm->set_num_rows(10);
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(rowset, "shared_seg.dat"); // same uid across siblings => dedup
        return meta;
    };

    auto meta_a = make_child(child_a);
    add_dcg_with_columns(meta_a.get(), 1, "a.cols", {1, 2}, 1);

    auto meta_b = make_child(child_b);
    add_dcg_with_columns(meta_b.get(), 1, "b.cols", {2, 3}, 1);

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
    auto st = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_TRUE(st.is_not_supported()) << st;
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

// Helper: build a PK tablet metadata where `rowset_id`'s segment is marked
// shared across children (mirrors split-family structure). Returns the rowset.
RowsetMetadataPB* add_shared_rowset(TabletMetadataPB* metadata, uint32_t rowset_id, int64_t version,
                                    const std::string& segment_filename) {
    auto* rowset = metadata->add_rowsets();
    rowset->set_id(rowset_id);
    rowset->set_version(version);
    rowset->set_num_rows(10);
    rowset->set_data_size(100);
    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename(segment_filename);
        sm->set_size(100);
        sm->set_num_rows(10);
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(rowset, segment_filename);
    return rowset;
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

    // The cold context's atoms union into [10,12) and pack to [1,3). The hot
    // context then packs its sparse runs from cursor 3, preserving every live
    // rowset and direct-reference atom without allocating the intervening gaps.
    ASSERT_TRUE(by_id.count(2));
    ASSERT_TRUE(by_id.count(5));
    ASSERT_TRUE(by_id.count(6));
    ASSERT_TRUE(by_id.count(8));

    // Direct references use their authoritative primary runs: raw max-compact 797
    // maps to 4, while raw delete origin 766 maps to 3. Occurrence aliases do not
    // participate in either mapping.
    const auto* compaction_output = by_id[6];
    EXPECT_EQ(4u, compaction_output->max_compact_input_rowset_id());
    ASSERT_EQ(1, compaction_output->del_files_size());
    EXPECT_EQ(3u, compaction_output->del_files(0).origin_rowset_id());

    // Each hot run is packed after the cold run, so every carried value remains
    // above the cold rowset's target RSSID without retaining sparse source gaps.
    for (const auto& rowset : merged->rowsets()) {
        if (rowset.id() == 2) continue;
        EXPECT_GT(rowset.id(), 2u);
        EXPECT_GT(rowset.max_compact_input_rowset_id(), 2u);
        for (const auto& del_file : rowset.del_files()) {
            EXPECT_GT(del_file.origin_rowset_id(), 2u);
        }
    }

    // The packed-run cursor is authoritative for future writes.
    EXPECT_EQ(9u, merged->next_rowset_id());
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

    ASSERT_TRUE(by_id.count(2));
    ASSERT_TRUE(by_id.count(6));
    ASSERT_TRUE(by_id.count(7));

    const auto* later_compaction_output = by_id[7];
    ASSERT_EQ(1, later_compaction_output->del_files_size());
    EXPECT_EQ(4u, later_compaction_output->del_files(0).origin_rowset_id());
    EXPECT_EQ(3u, later_compaction_output->max_compact_input_rowset_id());
    EXPECT_EQ(8u, merged->next_rowset_id());
}

// A same-UID sibling occurrence is not emitted, so its historical references
// contribute no primary atoms or target slots. Only its physical rowset/segment
// occurrences alias to the selected canonical target; an unrelated high-ID
// canonical therefore still packs inside the supported target domain.
TEST_F(LakeTabletMergerTest, test_tablet_merging_discarded_duplicate_does_not_lower_rssid_floor) {
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
    constexpr uint32_t kBaseRowsetId = std::numeric_limits<int32_t>::max() - 101;
    constexpr uint32_t kBaseNextRowsetId = kBaseRowsetId + 1;
    earlier_meta->set_next_rowset_id(kBaseNextRowsetId);
    set_primary_key_schema(earlier_meta.get(), 1001);
    auto* canonical = add_rowset(earlier_meta.get(), /*rowset_id=*/kBaseRowsetId,
                                 /*max_compact_input_rowset_id=*/0, /*del_origin_rowset_id=*/0);

    auto later_meta = std::make_shared<TabletMetadataPB>();
    later_meta->set_id(later_tablet);
    later_meta->set_version(base_version);
    later_meta->set_next_rowset_id(std::numeric_limits<int32_t>::max());
    set_primary_key_schema(later_meta.get(), 1001);
    auto* duplicate = add_rowset(later_meta.get(), /*rowset_id=*/kBaseRowsetId,
                                 /*max_compact_input_rowset_id=*/0, /*del_origin_rowset_id=*/0);
    duplicate->mutable_uid()->CopyFrom(canonical->uid());
    duplicate->mutable_segment_metas(0)->CopyFrom(canonical->segment_metas(0));

    constexpr uint32_t kHighRowsetId = std::numeric_limits<int32_t>::max() - 1;
    add_rowset(later_meta.get(), /*rowset_id=*/kHighRowsetId,
               /*max_compact_input_rowset_id=*/kHighRowsetId,
               /*del_origin_rowset_id=*/kHighRowsetId);

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

    ASSERT_EQ(2, merged->rowsets_size()) << "the same-UID sibling copy must be deduplicated";
    std::map<uint32_t, const RowsetMetadataPB*> by_id;
    for (const auto& rowset : merged->rowsets()) {
        by_id[rowset.id()] = &rowset;
    }
    ASSERT_TRUE(by_id.count(2));
    ASSERT_TRUE(by_id.count(3));
    EXPECT_EQ(4, merged->next_rowset_id());
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

TEST_F(LakeTabletMergerTest, test_tablet_merging_groups_same_uid_across_nonmonotonic_versions) {
    auto first = make_allocator_source(next_id(), 40);
    auto second = make_allocator_source(next_id(), 40);
    for (auto* source : {first.get(), second.get()}) {
        source->mutable_schema()->set_keys_type(DUP_KEYS);
        const bool left = source == first.get();
        auto* range = source->mutable_range();
        *range->mutable_lower_bound() = generate_sort_key(left ? 0 : 10);
        *range->mutable_upper_bound() = generate_sort_key(left ? 10 : 20);
        range->set_lower_bound_included(true);
        range->set_upper_bound_included(false);
        const std::vector<int> versions = left ? std::vector<int>{1, 5, 3} : std::vector<int>{1, 4, 3};
        for (int i = 0; i < 3; ++i) {
            auto* r =
                    add_allocator_rowset(source, 10 * (i + 1), versions[i], fmt::format("global-{}.dat", versions[i]));
            r->mutable_uid()->set_hi(1);
            r->mutable_uid()->set_lo(versions[i]);
            r->set_num_rows(10 * versions[i]);
            r->set_data_size(100 * versions[i]);
            r->set_num_dels(versions[i]);
        }
    }
    ASSIGN_OR_ABORT(auto merged, publish_allocator_merge({first, second}));
    ASSERT_EQ(4, merged->rowsets_size());
    for (int i = 0; i < 4; ++i) {
        const int versions[] = {1, 3, 4, 5};
        const int totals[] = {2, 6, 4, 5};
        EXPECT_EQ(versions[i], merged->rowsets(i).version());
        EXPECT_EQ(versions[i], merged->rowsets(i).uid().lo());
        EXPECT_EQ(10 * totals[i], merged->rowsets(i).num_rows());
        EXPECT_EQ(100 * totals[i], merged->rowsets(i).data_size());
        EXPECT_EQ(totals[i], merged->rowsets(i).num_dels());
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_same_range_exact_duplicate_counts_once) {
    auto first = make_allocator_source(next_id(), 20);
    auto* rowset = add_allocator_rowset(first.get(), 10, 1, "exact.dat", 7);
    rowset->set_num_rows(10);
    rowset->set_data_size(100);
    rowset->set_num_dels(3);
    // Exact duplicates live inside one source; distinct source tablets must
    // still form a nonoverlapping partition.
    first->add_rowsets()->CopyFrom(*rowset);
    first->mutable_rowsets(1)->set_id(20);
    first->set_next_rowset_id(30);
    ASSIGN_OR_ABORT(auto merged, publish_allocator_merge({first}));
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_EQ(10, merged->rowsets(0).num_rows());
    EXPECT_EQ(100, merged->rowsets(0).data_size());
    EXPECT_EQ(3, merged->rowsets(0).num_dels());
    EXPECT_EQ(7, merged->rowsets(0).segment_metas(0).segment_idx());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_equivalent_range_encodings_count_once) {
    for (auto keys_type : {PRIMARY_KEYS, DUP_KEYS}) {
        for (int encoding = 0; encoding < 3; ++encoding) {
            SCOPED_TRACE(fmt::format("keys_type={}, encoding={}", static_cast<int>(keys_type), encoding));
            auto first = make_allocator_source(next_id(), 20);
            first->mutable_schema()->set_keys_type(keys_type);
            auto* rowset = add_allocator_rowset(first.get(), 10, 1, "equivalent-range.dat", 7);
            rowset->set_num_rows(10);
            rowset->set_data_size(100);
            rowset->set_num_dels(3);
            first->mutable_range();
            auto* range = rowset->mutable_range();
            if (encoding == 2) {
                *range->mutable_lower_bound() = generate_sort_key(0);
                range->set_lower_bound_included(true);
                *range->mutable_upper_bound() = generate_sort_key(20);
                range->set_upper_bound_included(false);
            }
            auto* second = first->add_rowsets();
            second->CopyFrom(*rowset);
            second->set_id(20);
            first->set_next_rowset_id(30);
            if (encoding < 2) {
                // Flags on absent endpoints do not change an unbounded range.
                second->mutable_range()->set_upper_bound_included(encoding == 1);
            } else {
                // NORMAL_VALUE is the default, whether explicit or absent.
                second->mutable_range()->mutable_lower_bound()->mutable_values(0)->clear_variant_type();
                second->mutable_range()->mutable_upper_bound()->mutable_values(0)->clear_variant_type();
            }
            ASSERT_NE(rowset->range().SerializeAsString(), second->range().SerializeAsString());
            auto forward = publish_allocator_merge({first});
            first->mutable_rowsets()->SwapElements(0, 1);
            auto reverse = publish_allocator_merge({first});
            EXPECT_OK(forward.status());
            EXPECT_OK(reverse.status());
            if (!forward.ok() || !reverse.ok()) continue;
            ASSERT_EQ(1, forward.value()->rowsets_size());
            ASSERT_EQ(1, reverse.value()->rowsets_size());
            const auto& output = forward.value()->rowsets(0);
            EXPECT_EQ(10, output.num_rows());
            EXPECT_EQ(100, output.data_size());
            EXPECT_EQ(3, output.num_dels());
            EXPECT_EQ(7, output.segment_metas(0).segment_idx());
            EXPECT_EQ(rowset->uid().SerializeAsString(), output.uid().SerializeAsString());
            EXPECT_EQ(output.SerializeAsString(), reverse.value()->rowsets(0).SerializeAsString());
        }
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_distinct_ranges_contribute_once) {
    auto first = make_allocator_source(next_id(), 20);
    auto* r = add_allocator_rowset(first.get(), 10, 1, "contributor.dat", 7);
    r->set_num_rows(10);
    r->set_data_size(100);
    r->set_num_dels(3);
    auto second = std::make_shared<TabletMetadataPB>(*first);
    second->set_id(next_id());
    *first->mutable_range()->mutable_upper_bound() = generate_sort_key(10);
    first->mutable_range()->set_upper_bound_included(false);
    *second->mutable_range()->mutable_lower_bound() = generate_sort_key(10);
    second->mutable_range()->set_lower_bound_included(true);
    ASSIGN_OR_ABORT(auto merged, publish_allocator_merge({second, first}));
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_EQ(20, merged->rowsets(0).num_rows());
    EXPECT_EQ(200, merged->rowsets(0).data_size());
    EXPECT_EQ(6, merged->rowsets(0).num_dels());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_accepts_dels_above_zero_estimated_rows) {
    auto first = make_allocator_source(next_id(), 20);
    auto* rowset = add_allocator_rowset(first.get(), 10, 1, "zero-estimate.dat", 0);
    rowset->set_num_rows(0);
    rowset->set_num_dels(3);
    rowset->mutable_segment_metas(0)->set_num_rows(10);
    auto second = std::make_shared<TabletMetadataPB>(*first);
    second->set_id(next_id());
    *first->mutable_range()->mutable_upper_bound() = generate_sort_key(10);
    first->mutable_range()->set_upper_bound_included(false);
    *second->mutable_range()->mutable_lower_bound() = generate_sort_key(10);
    second->mutable_range()->set_lower_bound_included(true);

    ASSIGN_OR_ABORT(auto merged, publish_allocator_merge({first, second}));
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_EQ(0, merged->rowsets(0).num_rows());
    EXPECT_EQ(2, merged->rowsets(0).data_size());
    EXPECT_EQ(6, merged->rowsets(0).num_dels());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_contributor_statistics_overflow_before_io) {
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
        *first->mutable_range()->mutable_upper_bound() = generate_sort_key(10);
        first->mutable_range()->set_upper_bound_included(false);
        *second->mutable_range()->mutable_lower_bound() = generate_sort_key(10);
        second->mutable_range()->set_lower_bound_included(true);
        std::vector<TabletMetadataPtr> sources = {first, second};
        if (reverse_sources) std::swap(sources[0], sources[1]);

        auto status = expect_merge_rejected_without_writes(sources, next_id(), /*target_version=*/2);
        EXPECT_TRUE(status.message().contains("statistics overflow")) << status;
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_same_range_requires_complete_segment_set) {
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
    auto result = publish_allocator_merge({first});
    EXPECT_TRUE(result.status().is_corruption()) << result.status();
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

TEST_F(LakeTabletMergerTest, test_tablet_merging_non_pk_true_gap_remains_distinct) {
    auto first = make_allocator_source(next_id(), 20);
    first->mutable_schema()->set_keys_type(DUP_KEYS);
    add_allocator_rowset(first.get(), 10, 1, "gap.dat", 7);
    auto second = std::make_shared<TabletMetadataPB>(*first);
    second->set_id(next_id());
    *first->mutable_range()->mutable_upper_bound() = generate_sort_key(10);
    first->mutable_range()->set_upper_bound_included(false);
    *second->mutable_range()->mutable_lower_bound() = generate_sort_key(20);
    second->mutable_range()->set_lower_bound_included(true);
    // The rowset has a true gap, but source ownership is still contiguous.
    second->mutable_rowsets(0)->mutable_range()->CopyFrom(second->range());
    *second->mutable_range()->mutable_lower_bound() = generate_sort_key(10);
    ASSIGN_OR_ABORT(auto merged, publish_allocator_merge({second, first}));
    ASSERT_EQ(2, merged->rowsets_size());
    for (const auto& r : merged->rowsets()) {
        EXPECT_EQ(1, r.num_rows());
        EXPECT_EQ(7, r.segment_metas(0).segment_idx());
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

TEST_F(LakeTabletMergerTest, test_tablet_merging_interval_projection_reserves_duplicate_high_segment_idx) {
    auto selected_source = make_allocator_source(next_id(), 11);
    auto* selected = add_allocator_rowset(selected_source.get(), 10, 1, "duplicate_low.dat", 0);
    auto duplicate_source = make_allocator_source(next_id(), 121);
    auto* duplicate = add_allocator_rowset(duplicate_source.get(), 20, 1, "duplicate_high.dat", 100);
    duplicate->mutable_uid()->CopyFrom(selected->uid());

    auto merged_or = publish_allocator_merge({selected_source, duplicate_source});
    ASSERT_OK(merged_or.status());
    auto merged = std::move(merged_or).value();
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_EQ(1, merged->rowsets(0).id());
    ASSERT_EQ(2, merged->rowsets(0).segment_metas_size());
    EXPECT_EQ(0, lake::get_segment_idx(merged->rowsets(0), 0));
    EXPECT_EQ(100, lake::get_segment_idx(merged->rowsets(0), 1));
    EXPECT_TRUE(merged->rowsets(0).overlapped());
    EXPECT_EQ(102, merged->next_rowset_id());
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

TEST_F(LakeTabletMergerTest, test_tablet_merging_interval_projection_missing_primary_map_fails_closed) {
    auto source = make_allocator_source(next_id(), 4);
    auto* rowset = add_allocator_rowset(source.get(), 3, 1, "missing_primary.dat", 0);
    rowset->set_max_compact_input_rowset_id(1);

    int drop_atom_count = 0;
    int materialize_count = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("tablet_merge_test:drop_primary_atom", [&](void* arg) {
        *static_cast<bool*>(arg) = true;
        ++drop_atom_count;
    });
    sync->SetCallBack("materialize_planned_rowsets:entry", [&](void*) { ++materialize_count; });
    sync->EnableProcessing();
    DeferOp clear_sync([&] {
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
    });
    auto result = publish_allocator_merge({source});
    EXPECT_TRUE(result.status().is_corruption()) << result.status();
    EXPECT_EQ(1, drop_atom_count);
    EXPECT_EQ(0, materialize_count);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_selected_delete_uses_final_canonical_max) {
    auto selected_source = make_allocator_source(next_id(), 201);
    auto* selected = add_allocator_rowset(selected_source.get(), 100, 1, "selected_del_low.dat", 0);
    add_allocator_segment(selected, "selected_del_high.dat", 100);
    auto* selected_del = selected->add_del_files();
    selected_del->set_name("selected_final.del");
    selected_del->set_origin_rowset_id(80);
    auto duplicate_source = make_allocator_source(next_id(), 301);
    auto* duplicate = add_allocator_rowset(duplicate_source.get(), 200, 1, "selected_del_low.dat", 0);
    add_allocator_segment(duplicate, "selected_del_high.dat", 100);
    duplicate->mutable_uid()->CopyFrom(selected->uid());
    auto* duplicate_del = duplicate->add_del_files();
    duplicate_del->set_name(selected_del->name());
    duplicate_del->set_origin_rowset_id(180);

    auto merged_or = publish_allocator_merge({selected_source, duplicate_source});
    ASSERT_OK(merged_or.status());
    auto merged = std::move(merged_or).value();
    ASSERT_EQ(1, merged->rowsets_size());
    const auto& output = merged->rowsets(0);
    EXPECT_EQ(21, output.id());
    ASSERT_EQ(1, output.del_files_size());
    EXPECT_EQ(1, output.del_files(0).origin_rowset_id());
    ASSERT_EQ(2, output.segment_metas_size());
    EXPECT_EQ(121, output.id() + lake::get_segment_idx(output, 1));
    EXPECT_GE(output.del_files(0).origin_rowset_id() + 100, output.id());
    EXPECT_EQ(122, merged->next_rowset_id());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_del_bearing_complementary_segment_sets_before_io) {
    auto first = make_allocator_source(next_id(), 101);
    auto* selected = add_allocator_rowset(first.get(), 100, 1, "opaque_low.dat", 0);
    auto* del = selected->add_del_files();
    del->set_name("opaque.del");
    del->set_origin_rowset_id(80);
    auto second = make_allocator_source(next_id(), 301);
    auto* other = add_allocator_rowset(second.get(), 200, 1, "opaque_high.dat", 100);
    other->mutable_uid()->CopyFrom(selected->uid());
    other->add_del_files()->CopyFrom(*del);
    other->mutable_del_files(0)->set_origin_rowset_id(180);
    // Public MERGE fixture assigns distinct adjacent ranges, so this is not the equal-range duplicate check.
    expect_merge_rejected_without_writes({first, second}, next_id(), 2);
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
        EXPECT_TRUE(status.message().contains(divergent_primary ? "recovery keys" : "conflicting aliases")) << status;
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_validation_only_delete_origin_allocates_no_slots) {
    auto selected_source = make_allocator_source(next_id(), 111);
    auto* selected = add_allocator_rowset(selected_source.get(), 10, 1, "validation_high.dat", 100);
    auto* selected_del = selected->add_del_files();
    selected_del->set_name("validation_only.del");
    selected_del->set_origin_rowset_id(5);
    auto duplicate_source = make_allocator_source(next_id(), 121);
    auto* duplicate = add_allocator_rowset(duplicate_source.get(), 20, 1, "validation_high.dat", 100);
    duplicate->mutable_uid()->CopyFrom(selected->uid());
    auto* duplicate_del = duplicate->add_del_files();
    duplicate_del->set_name(selected_del->name());
    duplicate_del->set_origin_rowset_id(std::numeric_limits<int32_t>::max() - 50);

    auto merged_or = publish_allocator_merge({selected_source, duplicate_source});
    ASSERT_OK(merged_or.status());
    auto merged = std::move(merged_or).value();
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_EQ(6, merged->rowsets(0).id());
    EXPECT_EQ(1, merged->rowsets(0).del_files(0).origin_rowset_id());
    EXPECT_EQ(107, merged->next_rowset_id());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_del_bearing_local_max_complementarity) {
    auto selected_source = make_allocator_source(next_id(), 111);
    auto* selected = add_allocator_rowset(selected_source.get(), 10, 1, "validation_local_high.dat", 100);
    auto* selected_del = selected->add_del_files();
    selected_del->set_name("validation_local.del");
    selected_del->set_origin_rowset_id(5);
    auto duplicate_source = make_allocator_source(next_id(), 21);
    auto* duplicate = add_allocator_rowset(duplicate_source.get(), 20, 1, "validation_local_low.dat", 0);
    duplicate->mutable_uid()->CopyFrom(selected->uid());
    auto* duplicate_del = duplicate->add_del_files();
    duplicate_del->set_name(selected_del->name());
    duplicate_del->set_origin_rowset_id(std::numeric_limits<int32_t>::max() - 50);

    expect_merge_rejected_without_writes({selected_source, duplicate_source}, next_id(), 2);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejoins_independently_remapped_delete_origins) {
    const int64_t selected_id = next_id();
    const int64_t duplicate_id = next_id();
    prepare_tablet_dirs(selected_id);
    prepare_tablet_dirs(duplicate_id);
    auto selected_source = make_allocator_source(selected_id, 42);
    auto duplicate_source = make_allocator_source(duplicate_id, 402);
    for (auto* source : {selected_source.get(), duplicate_source.get()}) {
        set_two_column_pk_schema(source, 4001);
        source->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
        source->set_enable_persistent_index(true);
        source->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
    }
    selected_source->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(0));
    selected_source->mutable_range()->set_lower_bound_included(true);
    selected_source->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(50));
    selected_source->mutable_range()->set_upper_bound_included(false);
    duplicate_source->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(50));
    duplicate_source->mutable_range()->set_lower_bound_included(true);
    duplicate_source->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(100));
    duplicate_source->mutable_range()->set_upper_bound_included(false);
    const uint64_t selected_size = write_two_column_segment(
            selected_id, "rejoin.dat", 1, [](int) { return 100; }, 10);
    write_two_column_segment(
            duplicate_id, "rejoin.dat", 1, [](int) { return 100; }, 10);
    auto* selected = add_allocator_rowset(selected_source.get(), 41, 1, "rejoin.dat", 0);
    selected->mutable_segment_metas(0)->set_size(selected_size);
    selected->mutable_segment_metas(0)->set_shared(true);
    auto* selected_del = selected->add_del_files();
    selected_del->set_name("rejoin.del");
    selected_del->set_origin_rowset_id(40);
    selected_del->set_encryption_meta(write_encrypted_binary_del_file(selected_id, selected_del->name(), {}));
    selected_del->set_shared(true);
    auto* duplicate = add_allocator_rowset(duplicate_source.get(), 401, 1, "rejoin.dat", 0);
    duplicate->mutable_segment_metas(0)->set_size(selected_size);
    duplicate->mutable_segment_metas(0)->set_shared(true);
    duplicate->mutable_uid()->CopyFrom(selected->uid());
    auto* duplicate_del = duplicate->add_del_files();
    duplicate_del->set_name(selected_del->name());
    duplicate_del->set_origin_rowset_id(400);
    duplicate_del->set_encryption_meta(selected_del->encryption_meta());
    duplicate_del->set_shared(true);

    auto merged_or = publish_allocator_merge({selected_source, duplicate_source});
    ASSERT_OK(merged_or.status());
    auto merged = std::move(merged_or).value();
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_EQ(2, merged->rowsets(0).id());
    ASSERT_EQ(1, merged->rowsets(0).del_files_size());
    EXPECT_EQ(1, merged->rowsets(0).del_files(0).origin_rowset_id());
    EXPECT_EQ(3, merged->next_rowset_id());
    ASSERT_EQ(0, merged->sstable_meta().sstables_size());
    _update_manager->unload_and_remove_primary_index(merged->id());
    expect_lifecycle_oracle(merged, {{10, 100}}, {});

    _update_manager->unload_and_remove_primary_index(merged->id());
    ASSIGN_OR_ABORT(auto after_dml, publish_followup_upsert_delete(merged->id(), merged->version(), 10, 1010, 60));
    expect_lifecycle_oracle(after_dml, {{10, 1010}}, {60});
    _update_manager->unload_and_remove_primary_index(merged->id());
    ASSIGN_OR_ABORT(auto reopened_after_dml, _tablet_manager->get_tablet_metadata(merged->id(), after_dml->version()));
    expect_lifecycle_oracle(reopened_after_dml, {{10, 1010}}, {60});
    ASSIGN_OR_ABORT(auto compacted, compact_tablet(merged->id(), reopened_after_dml->version(), /*force_base=*/true));
    expect_lifecycle_oracle(compacted, {{10, 1010}}, {60});
    _update_manager->unload_and_remove_primary_index(merged->id());
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
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_EQ(7, merged->rowsets(0).id());
    EXPECT_EQ(1, merged->rowsets(0).del_files(0).origin_rowset_id());
    EXPECT_EQ(5, merged->rowsets(0).del_files(0).op_offset());
    EXPECT_EQ(8, merged->next_rowset_id());
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
    _update_manager->unload_and_remove_primary_index(target);
    _tablet_manager->prune_metacache();
    ASSIGN_OR_ABORT(auto reopened, _tablet_manager->get_tablet_metadata(target, merged->version()));
    expect_lifecycle_oracle(reopened, {{110, 1100}, {310, 3100}, {320, 3200}}, {10});
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_same_uid_canonical_conflicts_before_io) {
    enum Mutation {
        VERSION,
        SCHEMA_MAPPING,
        DEL_BEARING_MODE,
        NEXT_COMPACTION_OFFSET,
        RECOVERY_PRESENCE,
        STABLE_RESIDUAL_FIELD
    };
    for (auto mutation : {VERSION, SCHEMA_MAPPING, DEL_BEARING_MODE, NEXT_COMPACTION_OFFSET, RECOVERY_PRESENCE,
                          STABLE_RESIDUAL_FIELD}) {
        SCOPED_TRACE(mutation);
        auto a = make_allocator_source(next_id(), 20);
        auto* first = add_allocator_rowset(a.get(), 10, 1, "canonical_conflict.dat", 7);
        first->set_overlapped(true);
        auto b = make_allocator_source(next_id(), 30);
        auto* second = b->add_rowsets();
        second->CopyFrom(*first);
        second->set_id(20);
        switch (mutation) {
        case VERSION:
            second->set_version(2);
            break;
        case SCHEMA_MAPPING:
            (*b->mutable_rowset_to_schema())[20] = 4001;
            break;
        case DEL_BEARING_MODE: {
            auto* del = second->add_del_files();
            del->set_name("canonical_conflict.del");
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
        expect_merge_rejected_without_writes({a, b}, next_id(), 2);
    }
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

    int materialize_count = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("materialize_planned_rowsets:entry", [&](void*) { ++materialize_count; });
    sync->EnableProcessing();
    DeferOp clear_sync([&] {
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
    });
    auto result = publish_allocator_merge({selected_source, duplicate_source});
    EXPECT_TRUE(result.status().is_corruption()) << result.status();
    EXPECT_EQ(0, materialize_count);
}

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
            selected->mutable_segment_metas(0)->clear_num_rows();
            segment->set_num_rows(0);
            break;
        case Mutation::kUnknown: {
            std::string serialized = segment->SerializeAsString();
            serialized.append("\xF8\x07\x01", 3);
            ASSERT_TRUE(segment->ParseFromString(serialized));
            break;
        }
        }
        auto result = publish_allocator_merge({selected_source, duplicate_source});
        EXPECT_TRUE(result.status().is_corruption()) << result.status();
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

TEST_F(LakeTabletMergerTest, test_tablet_merging_reconciles_segment_shared_flag) {
    auto selected_source = make_allocator_source(next_id(), 11);
    auto* selected = add_allocator_rowset(selected_source.get(), 10, 1, "segment_shared.dat", 0);
    selected->mutable_segment_metas(0)->set_shared(false);
    auto duplicate_source = make_allocator_source(next_id(), 21);
    auto* duplicate = add_allocator_rowset(duplicate_source.get(), 20, 1, "segment_shared.dat", 0);
    duplicate->mutable_uid()->CopyFrom(selected->uid());
    duplicate->mutable_segment_metas(0)->set_shared(true);

    ASSIGN_OR_ABORT(auto merged, publish_allocator_merge({selected_source, duplicate_source}));
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_TRUE(merged->rowsets(0).segment_metas(0).shared());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_preserves_overlapped_without_segment_union_expansion) {
    const int64_t selected_id = next_id();
    const int64_t duplicate_id = next_id();
    prepare_tablet_dirs(selected_id);
    prepare_tablet_dirs(duplicate_id);
    const uint64_t high_size = write_two_column_segment(
            selected_id, "overlapped_same_high.dat", 2, [](int key) { return key * 10; }, 1);
    const uint64_t low_size =
            write_two_column_segment(selected_id, "overlapped_same_low.dat", 2, [](int key) { return key * 10; });

    auto selected_source = make_allocator_source(selected_id, 12);
    set_two_column_pk_schema(selected_source.get(), /*schema_id=*/4001);
    auto* selected = add_allocator_rowset(selected_source.get(), 10, 1, "overlapped_same_high.dat", 0);
    selected->mutable_segment_metas(0)->set_size(high_size);
    selected->mutable_segment_metas(0)->set_num_rows(2);
    auto* low_segment = add_allocator_segment(selected, "overlapped_same_low.dat", 1);
    low_segment->set_size(low_size);
    low_segment->set_num_rows(2);
    selected->set_num_rows(4);
    selected->set_data_size(high_size + low_size);
    selected->set_overlapped(false);

    auto duplicate_source = make_allocator_source(duplicate_id, 22);
    set_two_column_pk_schema(duplicate_source.get(), /*schema_id=*/4001);
    auto* duplicate = duplicate_source->add_rowsets();
    duplicate->CopyFrom(*selected);
    duplicate->set_id(20);
    duplicate->set_overlapped(true);

    ASSIGN_OR_ABORT(auto merged, publish_allocator_merge({selected_source, duplicate_source}));
    ASSERT_EQ(1, merged->rowsets_size());
    ASSERT_EQ(2, merged->rowsets(0).segment_metas_size());
    EXPECT_TRUE(merged->rowsets(0).overlapped());
    const std::vector<std::pair<int32_t, int32_t>> expected = {{0, 0}, {1, 10}, {1, 10}, {2, 20}};
    ASSIGN_OR_ABORT(auto rows, read_two_column_rows_in_storage_order(merged, /*sorted_by_keys_per_tablet=*/true));
    EXPECT_EQ(expected, rows);

    ASSERT_OK(put_tablet_metadata(merged));
    _tablet_manager->prune_metacache();
    ASSIGN_OR_ABORT(auto reopened, _tablet_manager->get_tablet_metadata(merged->id(), merged->version()));
    ASSERT_TRUE(reopened->rowsets(0).overlapped());
    ASSIGN_OR_ABORT(auto reopened_rows,
                    read_two_column_rows_in_storage_order(reopened, /*sorted_by_keys_per_tablet=*/true));
    EXPECT_EQ(expected, reopened_rows);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_del_declaration_conflict) {
    enum class Mutation { kOffsetValue, kOffsetPresence, kEncryption, kVersion, kRows, kCrc, kUnknown };
    for (auto mutation : {Mutation::kOffsetValue, Mutation::kOffsetPresence, Mutation::kEncryption, Mutation::kVersion,
                          Mutation::kRows, Mutation::kCrc, Mutation::kUnknown}) {
        SCOPED_TRACE(static_cast<int>(mutation));
        auto selected_source = make_allocator_source(next_id(), 11);
        auto* selected = add_allocator_rowset(selected_source.get(), 10, 1, "del_conflict.dat", 0);
        auto* selected_del = selected->add_del_files();
        selected_del->set_name("declaration.del");
        selected_del->set_origin_rowset_id(10);
        selected_del->set_op_offset(0);
        selected_del->set_version(1);
        selected_del->set_num_rows(1);
        selected_del->set_crc32c(1);
        auto duplicate_source = make_allocator_source(next_id(), 21);
        auto* duplicate = add_allocator_rowset(duplicate_source.get(), 20, 1, "del_conflict.dat", 0);
        duplicate->mutable_uid()->CopyFrom(selected->uid());
        auto* duplicate_del = duplicate->add_del_files();
        duplicate_del->CopyFrom(*selected_del);
        duplicate_del->set_origin_rowset_id(20);
        switch (mutation) {
        case Mutation::kOffsetValue:
            duplicate_del->set_op_offset(1);
            break;
        case Mutation::kOffsetPresence:
            duplicate_del->clear_op_offset();
            break;
        case Mutation::kEncryption:
            duplicate_del->set_encryption_meta("different encryption metadata");
            break;
        case Mutation::kVersion:
            duplicate_del->set_version(2);
            break;
        case Mutation::kRows:
            duplicate_del->set_num_rows(2);
            break;
        case Mutation::kCrc:
            duplicate_del->set_crc32c(2);
            break;
        case Mutation::kUnknown: {
            std::string serialized = duplicate_del->SerializeAsString();
            serialized.append("\xF8\x07\x01", 3);
            ASSERT_TRUE(duplicate_del->ParseFromString(serialized));
            break;
        }
        }
        auto result = publish_allocator_merge({selected_source, duplicate_source});
        EXPECT_TRUE(result.status().is_corruption()) << result.status();
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_reconciles_del_shared_flag) {
    auto selected_source = make_allocator_source(next_id(), 11);
    auto* selected = add_allocator_rowset(selected_source.get(), 10, 1, "del_shared.dat", 0);
    auto* selected_del = selected->add_del_files();
    selected_del->set_name("shared.del");
    selected_del->set_origin_rowset_id(10);
    selected_del->set_shared(false);
    auto duplicate_source = make_allocator_source(next_id(), 21);
    auto* duplicate = add_allocator_rowset(duplicate_source.get(), 20, 1, "del_shared.dat", 0);
    duplicate->mutable_uid()->CopyFrom(selected->uid());
    auto* duplicate_del = duplicate->add_del_files();
    duplicate_del->CopyFrom(*selected_del);
    duplicate_del->set_origin_rowset_id(20);
    duplicate_del->set_shared(true);

    ASSIGN_OR_ABORT(auto merged, publish_allocator_merge({selected_source, duplicate_source}));
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_TRUE(merged->rowsets(0).del_files(0).shared());
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

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_rowset_schema_mapping_conflict) {
    auto selected_source = make_allocator_source(next_id(), 11);
    auto* selected = add_allocator_rowset(selected_source.get(), 10, 1, "schema_conflict.dat", 0);
    (*selected_source->mutable_rowset_to_schema())[10] = 4001;
    auto duplicate_source = make_allocator_source(next_id(), 21);
    auto* duplicate = add_allocator_rowset(duplicate_source.get(), 20, 1, "schema_conflict.dat", 0);
    duplicate->mutable_uid()->CopyFrom(selected->uid());
    (*duplicate_source->mutable_rowset_to_schema())[20] = 4002;

    auto result = publish_allocator_merge({selected_source, duplicate_source});
    EXPECT_TRUE(result.status().is_corruption()) << result.status();
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
    _update_manager->unload_and_remove_primary_index(merged->id());
    const std::vector<std::string> keys = {encode_int_primary_key(10), encode_int_primary_key(20)};
    ASSIGN_OR_ABORT(auto values, load_index_values(merged, merged->id(), keys));
    ASSERT_EQ(2, values.size());
    EXPECT_EQ(2, values[0].get_value() >> 32);
    EXPECT_EQ(3, values[1].get_value() >> 32);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_rejects_alias_reversed_recovery_order_before_io) {
    auto canonical_source = make_allocator_source(next_id(), 11);
    auto* canonical = add_allocator_rowset(canonical_source.get(), 10, 2, "recovery_alias.dat", 0);
    auto second_source = make_allocator_source(next_id(), 101);
    auto* independent = add_allocator_rowset(second_source.get(), 99, 1, "recovery_independent.dat", 0);
    independent->set_max_compact_input_rowset_id(100);
    auto* duplicate = add_allocator_rowset(second_source.get(), 100, 2, "recovery_alias.dat", 0);
    duplicate->mutable_uid()->CopyFrom(canonical->uid());

    int materialize_count = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("materialize_planned_rowsets:entry", [&](void*) { ++materialize_count; });
    sync->EnableProcessing();
    DeferOp clear_sync([&] {
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
    });
    auto result = publish_allocator_merge({canonical_source, second_source});
    EXPECT_TRUE(result.status().is_corruption()) << result.status();
    EXPECT_EQ(0, materialize_count);
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
    _update_manager->unload_and_remove_primary_index(merged->id());
    const std::vector<std::string> keys = {encode_int_primary_key(10), encode_int_primary_key(60)};
    ASSIGN_OR_ABORT(auto values, load_index_values(merged, merged->id(), keys));
    ASSERT_EQ(2, values.size());
    EXPECT_EQ(1, values[0].get_value() >> 32);
    EXPECT_EQ(2, values[1].get_value() >> 32);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_affine_modern_source_stale_falls_back) {
    for (auto shape : {MetadataOnlyMergeShape::kPrivate, MetadataOnlyMergeShape::kIdentical}) {
        SCOPED_TRACE(static_cast<int>(shape));
        auto result_or = publish_metadata_only_merge_fixture(shape, false, false, true, [shape](auto& sources) {
            for (auto& source : sources) {
                auto* sst = source->mutable_sstable_meta()->mutable_sstables(0);
                sst->set_shared_rssid(5);
                sst->set_max_rss_rowid(static_cast<uint64_t>(5) << 32);
            }
            if (shape == MetadataOnlyMergeShape::kPrivate) {
                sources[1]->clear_sstable_meta();
                lake::tablet_reshard_helper::set_rowset_uid(sources[0]->mutable_rowsets(0));
                sources[1]->mutable_rowsets(0)->mutable_uid()->CopyFrom(sources[0]->rowsets(0).uid());
                sources[1]->mutable_rowsets(0)->mutable_segment_metas(0)->set_segment_idx(4);
            }
        });
        ASSERT_OK(result_or.status());
        auto result = std::move(result_or).value();
        expect_affine_sst_fallback_orphans(result);
    }
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

TEST_F(LakeTabletMergerTest, test_tablet_merging_affine_modern_mismatched_high_falls_back) {
    for (auto shape : {MetadataOnlyMergeShape::kPrivate, MetadataOnlyMergeShape::kIdentical}) {
        SCOPED_TRACE(static_cast<int>(shape));
        ASSIGN_OR_ABORT(auto result, publish_metadata_only_merge_fixture(shape, false, false, true, [](auto& sources) {
                            for (auto& source : sources) {
                                auto* sst = source->mutable_sstable_meta()->mutable_sstables(0);
                                if (sst->has_shared_rssid()) {
                                    sst->set_max_rss_rowid(static_cast<uint64_t>(2) << 32);
                                }
                            }
                        }));
        expect_affine_sst_fallback_orphans(result);
    }
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_affine_legacy_negative_offset_falls_back) {
    ASSIGN_OR_ABORT(auto result, publish_metadata_only_merge_fixture(
                                         MetadataOnlyMergeShape::kPrivate, false, false, true, [](auto& sources) {
                                             auto* legacy = sources[1]->mutable_sstable_meta()->mutable_sstables(0);
                                             legacy->set_rssid_offset(-1);
                                         }));
    expect_affine_sst_fallback_orphans(result);
    expect_metadata_fallback_lifecycle(result, {{10, 100}, {60, 600}}, {{10, 1010}}, {60});
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_affine_legacy_compressed_gap_falls_back) {
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
                                legacy->set_rssid_offset(5);
                                legacy->set_max_rss_rowid(static_cast<uint64_t>(100) << 32);
                            }));
    expect_affine_sst_fallback_orphans(result);
    expect_metadata_fallback_lifecycle(result, {{10, 100}, {60, 600}, {70, 700}}, {{10, 1010}, {70, 700}}, {60});
}

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
    EXPECT_EQ(4, target.next_rowset_id());

    auto target_ptr = std::make_shared<TabletMetadataPB>(target);
    _update_manager->unload_and_remove_primary_index(target.id());
    ASSIGN_OR_ABORT(auto values, load_index_values(target_ptr, target.id(), {encode_int_primary_key(/*key=*/60)}));
    ASSERT_EQ(1, values.size());
    EXPECT_EQ(2, values[0].get_value() >> 32) << "stored raw RSSID 0 must use the accumulated output offset";
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_affine_legacy_alias_exclusive_end_reuses) {
    ASSIGN_OR_ABORT(
            auto result,
            publish_metadata_only_merge_fixture(
                    MetadataOnlyMergeShape::kPrivate, false, false, true, [&](auto& sources) {
                        lake::tablet_reshard_helper::set_rowset_uid(sources[0]->mutable_rowsets(0));
                        auto* exclusive_alias =
                                add_allocator_rowset(sources[1].get(), 6, sources[1]->rowsets(0).version(),
                                                     sources[0]->rowsets(0).segment_metas(0).filename(), 0);
                        exclusive_alias->mutable_segment_metas(0)->CopyFrom(sources[0]->rowsets(0).segment_metas(0));
                        exclusive_alias->mutable_uid()->CopyFrom(sources[0]->rowsets(0).uid());
                        sources[1]->set_next_rowset_id(7);
                    }));
    const auto& target = result.published.at(result.target_tablet_id);
    ASSERT_EQ(2, target.sstable_meta().sstables_size());
    auto legacy = std::find_if(target.sstable_meta().sstables().begin(), target.sstable_meta().sstables().end(),
                               [](const auto& sst) { return !sst.has_shared_rssid(); });
    ASSERT_NE(target.sstable_meta().sstables().end(), legacy);
    EXPECT_EQ(2, legacy->rssid_offset());
    EXPECT_EQ(static_cast<uint64_t>(2) << 32, legacy->max_rss_rowid());
    EXPECT_EQ(3, target.next_rowset_id());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_affine_legacy_uint32_exclusive_end_does_not_enumerate) {
    bool materialized = false;
    int classifier_visited_runs = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("materialize_planned_rowsets:entry", [&](void*) { materialized = true; });
    sync->SetCallBack("affine_delta:visited_run", [&](void*) {
        if (materialized) ++classifier_visited_runs;
    });
    sync->EnableProcessing();
    DeferOp clear_sync([&] {
        sync->ClearCallBack("materialize_planned_rowsets:entry");
        sync->ClearCallBack("affine_delta:visited_run");
        sync->DisableProcessing();
    });

    ASSIGN_OR_ABORT(auto result, publish_metadata_only_merge_fixture(
                                         MetadataOnlyMergeShape::kPrivate, false, false, true, [](auto& sources) {
                                             auto* legacy = sources[1]->mutable_sstable_meta()->mutable_sstables(0);
                                             legacy->set_rssid_offset(std::numeric_limits<int32_t>::max());
                                             legacy->set_max_rss_rowid(
                                                     static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) << 32);
                                         }));
    expect_affine_sst_fallback_orphans(result);
    EXPECT_LE(classifier_visited_runs, 1) << "the widened [INT32_MAX, 2^32) domain must not be enumerated";
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_affine_legacy_proof_visits_runs_not_rssids) {
    bool materialized = false;
    int classifier_visited_runs = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("materialize_planned_rowsets:entry", [&](void*) { materialized = true; });
    sync->SetCallBack("affine_delta:visited_run", [&](void*) {
        if (materialized) ++classifier_visited_runs;
    });
    sync->EnableProcessing();
    DeferOp clear_sync([&] {
        sync->ClearCallBack("materialize_planned_rowsets:entry");
        sync->ClearCallBack("affine_delta:visited_run");
        sync->DisableProcessing();
    });

    ASSIGN_OR_ABORT(auto result,
                    publish_metadata_only_merge_fixture(
                            MetadataOnlyMergeShape::kPrivate, false, false, true, [&](auto& sources) {
                                const std::string high_name = "affine_visited_high.dat";
                                const uint64_t high_size = write_two_column_segment(
                                        sources[1]->id(), high_name, 1, [](int key) { return key * 10; }, 70);
                                auto* high = add_allocator_segment(sources[1]->mutable_rowsets(0), high_name, 4095);
                                high->set_size(high_size);

                                const std::string extra_name = "affine_visited_extra.dat";
                                const uint64_t extra_size = write_two_column_segment(
                                        sources[1]->id(), extra_name, 1, [](int key) { return key * 10; }, 80);
                                auto* extra = add_allocator_rowset(sources[1].get(), 10000,
                                                                   sources[1]->rowsets(0).version(), extra_name, 0);
                                extra->mutable_segment_metas(0)->set_size(extra_size);
                                sources[1]->set_next_rowset_id(10001);
                                sources[1]->mutable_sstable_meta()->mutable_sstables(0)->set_max_rss_rowid(
                                        static_cast<uint64_t>(4100) << 32);
                            }));
    const auto& target = result.published.at(result.target_tablet_id);
    ASSERT_EQ(2, target.sstable_meta().sstables_size());
    auto legacy = std::find_if(target.sstable_meta().sstables().begin(), target.sstable_meta().sstables().end(),
                               [](const auto& sst) { return !sst.has_shared_rssid(); });
    ASSERT_NE(target.sstable_meta().sstables().end(), legacy);
    EXPECT_EQ(2, legacy->rssid_offset());
    EXPECT_EQ(static_cast<uint64_t>(4097) << 32, legacy->max_rss_rowid());
    EXPECT_EQ(1, classifier_visited_runs) << "a 4096-RSSID affine domain must visit one translation run";
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_legacy_liveness_precompute_visits_each_live_rssid_once) {
    constexpr int kSourceBLiveRssids = 32;
    constexpr int kLegacySstables = 4;
    constexpr int kExpectedContexts = 2;
    constexpr int kExpectedLiveRssids = 1 + kSourceBLiveRssids;
    int precomputed_contexts = 0;
    int visited_live_rssids = 0;
    int bounded_lookups = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("legacy_sstable_liveness_index:precompute_context", [&](void*) { ++precomputed_contexts; });
    sync->SetCallBack("legacy_sstable_liveness_index:visited_live_rssid", [&](void*) { ++visited_live_rssids; });
    sync->SetCallBack("legacy_sstable_liveness_index:bounded_lookup", [&](void*) { ++bounded_lookups; });
    sync->EnableProcessing();
    DeferOp clear_sync([&] {
        sync->ClearCallBack("legacy_sstable_liveness_index:precompute_context");
        sync->ClearCallBack("legacy_sstable_liveness_index:visited_live_rssid");
        sync->ClearCallBack("legacy_sstable_liveness_index:bounded_lookup");
        sync->DisableProcessing();
    });

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
    EXPECT_EQ(1 + kLegacySstables, target.sstable_meta().sstables_size());
    EXPECT_EQ(kExpectedContexts, precomputed_contexts);
    EXPECT_EQ(kExpectedLiveRssids, visited_live_rssids)
            << "source-live RSSIDs must be visited once per context, not once per legacy SST";
    EXPECT_EQ(kLegacySstables, bounded_lookups);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_affine_identical_common_nonzero_delta_reuses) {
    ASSIGN_OR_ABORT(auto result,
                    publish_metadata_only_merge_fixture(
                            MetadataOnlyMergeShape::kIdentical, false, false, true, [&](auto& sources) {
                                for (auto& source : sources) {
                                    source->mutable_rowsets(0)->set_id(5);
                                    source->set_next_rowset_id(6);
                                }

                                const auto file = write_raw_pk_sstable(
                                        _tablet_manager->sst_location(
                                                sources[0]->id(), sources[0]->sstable_meta().sstables(0).filename()),
                                        {{encode_int_primary_key(10),
                                          serialize_index_values({{sources[0]->version(), 0, 0}})}});
                                for (auto& source : sources) {
                                    auto* sst = source->mutable_sstable_meta()->mutable_sstables(0);
                                    sst->set_filesize(file.filesize);
                                    sst->set_encryption_meta(file.encryption_meta);
                                    sst->mutable_range()->CopyFrom(file.range);
                                    sst->clear_shared_rssid();
                                    sst->clear_shared_version();
                                    sst->set_rssid_offset(5);
                                    sst->set_max_rss_rowid(static_cast<uint64_t>(5) << 32);
                                }
                            }));
    const auto& target = result.published.at(result.target_tablet_id);
    ASSERT_EQ(1, target.sstable_meta().sstables_size());
    const auto& output = target.sstable_meta().sstables(0);
    EXPECT_TRUE(output.shared());
    EXPECT_EQ(1, output.rssid_offset());
    EXPECT_EQ(static_cast<uint64_t>(1) << 32, output.max_rss_rowid());
    EXPECT_EQ(2, target.next_rowset_id());

    auto target_ptr = std::make_shared<TabletMetadataPB>(target);
    _update_manager->unload_and_remove_primary_index(target.id());
    ASSIGN_OR_ABORT(auto values, load_index_values(target_ptr, target.id(), {encode_int_primary_key(/*key=*/10)}));
    ASSERT_EQ(1, values.size());
    EXPECT_EQ(1, values[0].get_value() >> 32);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_affine_identical_occurrence_disagreement_falls_back) {
    ASSIGN_OR_ABORT(auto result,
                    publish_metadata_only_merge_fixture(
                            MetadataOnlyMergeShape::kIdentical, false, false, true, [&](auto& sources) {
                                const std::string high_name = "affine_identical_disagreement_high.dat";
                                const uint64_t high_size = write_two_column_segment(
                                        sources[0]->id(), high_name, 1, [](int key) { return key * 10; }, 80);
                                auto* left_high = add_allocator_segment(sources[0]->mutable_rowsets(0), high_name, 1);
                                left_high->set_size(high_size);
                                left_high->set_shared(true);
                                auto* right_high = add_allocator_segment(sources[1]->mutable_rowsets(0), high_name, 1);
                                right_high->CopyFrom(*left_high);
                                sources[0]->mutable_rowsets(0)->set_id(5);
                                sources[0]->set_next_rowset_id(7);
                                sources[1]->mutable_rowsets(0)->set_id(6);
                                sources[1]->set_next_rowset_id(8);

                                const auto file = write_raw_pk_sstable(
                                        _tablet_manager->sst_location(
                                                sources[0]->id(), sources[0]->sstable_meta().sstables(0).filename()),
                                        {{encode_int_primary_key(10),
                                          serialize_index_values({{sources[0]->version(), 0, 0}})}});
                                for (auto& source : sources) {
                                    auto* sst = source->mutable_sstable_meta()->mutable_sstables(0);
                                    sst->set_filesize(file.filesize);
                                    sst->set_encryption_meta(file.encryption_meta);
                                    sst->mutable_range()->CopyFrom(file.range);
                                    sst->clear_shared_rssid();
                                    sst->clear_shared_version();
                                    sst->set_rssid_offset(5);
                                    sst->set_max_rss_rowid(static_cast<uint64_t>(6) << 32);
                                }
                            }));
    expect_affine_sst_fallback_orphans(result);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_affine_identical_prior_offset_reuses) {
    ASSIGN_OR_ABORT(auto result,
                    publish_metadata_only_merge_fixture(
                            MetadataOnlyMergeShape::kIdentical, false, false, true, [&](auto& sources) {
                                const std::string high_name = "affine_identical_prior_high.dat";
                                const uint64_t high_size = write_two_column_segment(
                                        sources[0]->id(), high_name, 1, [](int key) { return key * 10; }, 80);
                                auto* left_high = add_allocator_segment(sources[0]->mutable_rowsets(0), high_name, 3);
                                left_high->set_size(high_size);
                                left_high->set_shared(true);
                                auto* right_high = add_allocator_segment(sources[1]->mutable_rowsets(0), high_name, 3);
                                right_high->CopyFrom(*left_high);

                                const std::string live_name = "affine_identical_prior_live.dat";
                                const uint64_t live_size = write_two_column_segment(
                                        sources[0]->id(), live_name, 1, [](int key) { return key * 10; }, 90);
                                auto* left_live = add_allocator_rowset(sources[0].get(), 5,
                                                                       sources[0]->rowsets(0).version(), live_name, 0);
                                left_live->mutable_segment_metas(0)->set_size(live_size);
                                left_live->mutable_segment_metas(0)->set_shared(true);
                                auto* right_live = add_allocator_rowset(sources[1].get(), 5,
                                                                        sources[1]->rowsets(0).version(), live_name, 0);
                                right_live->mutable_segment_metas(0)->CopyFrom(left_live->segment_metas(0));
                                right_live->mutable_uid()->CopyFrom(left_live->uid());
                                for (auto& source : sources) {
                                    source->set_next_rowset_id(6);
                                }
                                const auto file = write_raw_pk_sstable(
                                        _tablet_manager->sst_location(
                                                sources[0]->id(), sources[0]->sstable_meta().sstables(0).filename()),
                                        {{encode_int_primary_key(10),
                                          serialize_index_values({{sources[0]->version(), 0, 0}})}});
                                for (auto& source : sources) {
                                    auto* sst = source->mutable_sstable_meta()->mutable_sstables(0);
                                    sst->set_filesize(file.filesize);
                                    sst->set_encryption_meta(file.encryption_meta);
                                    sst->mutable_range()->CopyFrom(file.range);
                                    sst->clear_shared_rssid();
                                    sst->clear_shared_version();
                                    sst->set_rssid_offset(5);
                                    sst->set_max_rss_rowid(static_cast<uint64_t>(5) << 32);
                                }
                            }));
    const auto& target = result.published.at(result.target_tablet_id);
    ASSERT_EQ(1, target.sstable_meta().sstables_size());
    const auto& output = target.sstable_meta().sstables(0);
    EXPECT_TRUE(output.shared());
    EXPECT_EQ(5, output.rssid_offset());
    EXPECT_EQ(static_cast<uint64_t>(5) << 32, output.max_rss_rowid());

    auto target_ptr = std::make_shared<TabletMetadataPB>(target);
    _update_manager->unload_and_remove_primary_index(target.id());
    ASSIGN_OR_ABORT(auto values, load_index_values(target_ptr, target.id(), {encode_int_primary_key(/*key=*/10)}));
    ASSERT_EQ(1, values.size());
    EXPECT_EQ(5, values[0].get_value() >> 32);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_affine_legacy_alias_interior_rejected_before_materialize) {
    int materialize_count = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("materialize_planned_rowsets:entry", [&](void*) { ++materialize_count; });
    sync->EnableProcessing();
    DeferOp clear_sync([&] {
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
    });
    auto result_or = publish_metadata_only_merge_fixture(
            MetadataOnlyMergeShape::kPrivate, false, false, true, [&](auto& sources) {
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
    EXPECT_TRUE(result_or.status().is_corruption()) << result_or.status();
    EXPECT_EQ(0, materialize_count);
}

// Merge of two PK parents that both have cloud-native persistent index enabled.
// This exercises the flush_parent_for_merge helper end-to-end. Parents have
// no rowsets so load_from_lake_tablet is a no-op; the dumped sstable_meta
// echoes the parents' original sstable_meta, and merge_sstables runs normally.
// The point is to confirm the cloud-native branch doesn't crash and that the
// helper participates in producing a consistent merged metadata.
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

// Both shared source segments resolve through occurrence aliases to the same
// canonical target RSSID. With the canonical atoms and authoritative cursor
// already fixed, identical DCG declarations deduplicate to one passthrough entry.
TEST_F(LakeTabletMergerTest, test_tablet_merging_dcg_exact_dedup_preserves_passthrough) {
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
    add_shared_rowset(meta1.get(), /*rowset_id=*/1, /*version=*/1, "shared_seg.dat");
    (*meta1->mutable_rowset_to_schema())[1] = 1001;
    add_dcg_with_columns(meta1.get(), /*segment_id=*/1, "shared.cols", {101, 102}, 1);

    auto meta2 = std::make_shared<TabletMetadataPB>();
    meta2->set_id(old_tablet_id_2);
    meta2->set_version(base_version);
    meta2->set_next_rowset_id(10);
    set_primary_key_schema(meta2.get(), 1001);
    add_shared_rowset(meta2.get(), /*rowset_id=*/1, /*version=*/1, "shared_seg.dat");
    (*meta2->mutable_rowset_to_schema())[1] = 1001;
    add_dcg_with_columns(meta2.get(), /*segment_id=*/1, "shared.cols", {101, 102}, 1);

    ASSERT_OK(put_merge_sources(meta1, meta2));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(old_tablet_id_1);
    merging_tablet.add_old_tablet_ids(old_tablet_id_2);
    merging_tablet.set_new_tablet_id(new_tablet_id);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(77);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));
    auto merged = tablet_metadatas.at(new_tablet_id);

    ASSERT_EQ(1, merged->dcg_meta().dcgs_size());
    const auto& entry = merged->dcg_meta().dcgs().begin()->second;
    ASSERT_EQ(1, entry.column_files_size());
    EXPECT_EQ("shared.cols", entry.column_files(0));
    ASSERT_EQ(1, entry.unique_column_ids_size());
    ASSERT_EQ(2, entry.unique_column_ids(0).column_ids_size());
    EXPECT_EQ(1, entry.versions(0));
}

// Disjoint columns on a shared rowset append as two entries; no rebuild.
TEST_F(LakeTabletMergerTest, test_tablet_merging_dcg_disjoint_columns_append) {
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
    set_primary_key_schema(meta1.get(), 2001);
    add_shared_rowset(meta1.get(), 1, 1, "shared_seg.dat");
    (*meta1->mutable_rowset_to_schema())[1] = 2001;
    add_dcg_with_columns(meta1.get(), 1, "a.cols", {201, 202}, 1);

    auto meta2 = std::make_shared<TabletMetadataPB>();
    meta2->set_id(old_tablet_id_2);
    meta2->set_version(base_version);
    meta2->set_next_rowset_id(10);
    set_primary_key_schema(meta2.get(), 2001);
    add_shared_rowset(meta2.get(), 1, 1, "shared_seg.dat");
    (*meta2->mutable_rowset_to_schema())[1] = 2001;
    add_dcg_with_columns(meta2.get(), 1, "b.cols", {301, 302}, 1);

    ASSERT_OK(put_merge_sources(meta1, meta2));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(old_tablet_id_1);
    merging_tablet.add_old_tablet_ids(old_tablet_id_2);
    merging_tablet.set_new_tablet_id(new_tablet_id);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(78);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));
    auto merged = tablet_metadatas.at(new_tablet_id);

    ASSERT_EQ(1, merged->dcg_meta().dcgs_size());
    const auto& entry = merged->dcg_meta().dcgs().begin()->second;
    ASSERT_EQ(2, entry.column_files_size());
    ASSERT_EQ(2, entry.unique_column_ids_size());
    std::set<uint32_t> seen;
    for (int i = 0; i < entry.unique_column_ids_size(); ++i) {
        for (auto uid : entry.unique_column_ids(i).column_ids()) {
            EXPECT_TRUE(seen.insert(uid).second);
        }
    }
}

// Conflicting columns on a shared rowset trigger the rebuild dispatch path.
// Source .cols files don't exist on disk in this fixture, so rebuild
// surfaces an I/O error — critically NOT the legacy "same column updated
// independently" NotSupported message the old code produced.
TEST_F(LakeTabletMergerTest, test_tablet_merging_dcg_conflict_triggers_rebuild_dispatch) {
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
    set_primary_key_schema(meta1.get(), 3001);
    add_shared_rowset(meta1.get(), 1, 1, "shared_seg.dat");
    (*meta1->mutable_rowset_to_schema())[1] = 3001;
    add_dcg_with_columns(meta1.get(), 1, "child1.cols", {401, 402}, 1);

    auto meta2 = std::make_shared<TabletMetadataPB>();
    meta2->set_id(old_tablet_id_2);
    meta2->set_version(base_version);
    meta2->set_next_rowset_id(10);
    set_primary_key_schema(meta2.get(), 3001);
    add_shared_rowset(meta2.get(), 1, 1, "shared_seg.dat");
    (*meta2->mutable_rowset_to_schema())[1] = 3001;
    // Column 401 overlaps with child1.cols => rebuild triggered.
    add_dcg_with_columns(meta2.get(), 1, "child2.cols", {401, 403}, 1);

    ASSERT_OK(put_merge_sources(meta1, meta2));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(old_tablet_id_1);
    merging_tablet.add_old_tablet_ids(old_tablet_id_2);
    merging_tablet.set_new_tablet_id(new_tablet_id);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(79);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto st = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges);

    ASSERT_FALSE(st.ok());
    EXPECT_EQ(std::string::npos, st.to_string().find("same column updated independently")) << st.to_string();
}

// Full end-to-end rebuild: two children each update column c1 on the same
// shared segment, with disjoint row windows. Merge must produce a single new
// .cols file whose row-by-row c1 values match each owner child's updates.
TEST_F(LakeTabletMergerTest, test_tablet_merging_dcg_rebuild_two_children_same_column_end_to_end) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    constexpr int kNumRows = 100;
    constexpr int kBoundary = 50; // child A owns [0, 50), child B owns [50, 100)
    constexpr uint32_t kSegmentRssid = 1;
    constexpr int64_t kTxnId = 777;

    // 1. Write the shared base segment under the merged tablet dir. Both
    //    children's metadata references "shared_seg.dat" and (in production
    //    object storage) resolves to the same physical file.
    auto source_value_of = [](int row) { return row * 10; };
    const std::string shared_segment_name = "shared_seg.dat";
    const uint64_t base_segment_size =
            write_two_column_segment(merged_tablet, shared_segment_name, kNumRows, source_value_of);

    // 2. Each child writes its own .cols file for column c1. A's file has
    //    updates for rows [0, kBoundary) and source copy-through for
    //    [kBoundary, kNumRows); B is the mirror. Filenames match the
    //    gen_cols_filename format so that subsequent ingests can't collide.
    auto child_a_update = [](int row) { return row + 100000; };
    auto child_b_update = [](int row) { return row + 200000; };
    const std::string cols_a_name = lake::gen_cols_filename(kTxnId);
    const std::string cols_b_name = lake::gen_cols_filename(kTxnId + 1);
    auto a_cell = [&](int row) { return row < kBoundary ? child_a_update(row) : source_value_of(row); };
    auto b_cell = [&](int row) { return row >= kBoundary ? child_b_update(row) : source_value_of(row); };
    write_c1_only_cols_file(child_a, cols_a_name, kNumRows, a_cell);
    write_c1_only_cols_file(child_b, cols_b_name, kNumRows, b_cell);

    // 3. Build the two children's metadata. Both share the base segment; each
    //    owns a different key range on column c0 (the sort key).
    auto build_child = [&](int64_t tablet_id, int lower_key, int upper_key, const std::string& cols_filename) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(base_version);
        metadata->set_next_rowset_id(10);
        const auto [c0_uid, c1_uid] = set_two_column_pk_schema(metadata.get(), 4001);
        (void)c0_uid;

        auto* tablet_range = metadata->mutable_range();
        tablet_range->set_lower_bound_included(true);
        tablet_range->set_upper_bound_included(false);
        *tablet_range->mutable_lower_bound() = generate_sort_key(lower_key);
        *tablet_range->mutable_upper_bound() = generate_sort_key(upper_key);

        auto* rowset = metadata->add_rowsets();
        rowset->set_id(/*rowset_id=*/kSegmentRssid);
        rowset->set_version(1);
        rowset->set_num_rows(kNumRows);
        rowset->set_data_size(base_segment_size);
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename(shared_segment_name);
            sm->set_size(base_segment_size);
            sm->set_num_rows(kNumRows);
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(rowset,
                                    shared_segment_name); // same UID across siblings => one canonical rowset
        *rowset->mutable_range()->mutable_lower_bound() = generate_sort_key(lower_key);
        *rowset->mutable_range()->mutable_upper_bound() = generate_sort_key(upper_key);
        rowset->mutable_range()->set_lower_bound_included(true);
        rowset->mutable_range()->set_upper_bound_included(false);
        (*metadata->mutable_rowset_to_schema())[kSegmentRssid] = 4001;

        // DCG entry claims column c1 on segment kSegmentRssid.
        auto& dcg = (*metadata->mutable_dcg_meta()->mutable_dcgs())[kSegmentRssid];
        dcg.add_column_files(cols_filename);
        dcg.add_unique_column_ids()->add_column_ids(c1_uid);
        dcg.add_versions(1);
        dcg.add_shared_files(false); // each child's local .cols
        return metadata;
    };

    auto meta_a = build_child(child_a, 0, kBoundary, cols_a_name);
    auto meta_b = build_child(child_b, kBoundary, kNumRows, cols_b_name);

    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    // 4. Run merge.
    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(child_a);
    merging_tablet.add_old_tablet_ids(child_b);
    merging_tablet.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(kTxnId + 2);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);

    // 5. Inspect merged metadata: exactly one DCG entry for the target
    //    segment, one new .cols file, claims c1, shared=false.
    ASSERT_EQ(1, merged->dcg_meta().dcgs_size());
    const auto& dcgs = merged->dcg_meta().dcgs();
    auto dcg_it = dcgs.find(kSegmentRssid);
    ASSERT_TRUE(dcg_it != dcgs.end());
    const auto& rebuilt_entry = dcg_it->second;
    ASSERT_EQ(1, rebuilt_entry.column_files_size());
    EXPECT_NE(cols_a_name, rebuilt_entry.column_files(0));
    EXPECT_NE(cols_b_name, rebuilt_entry.column_files(0));
    ASSERT_EQ(1, rebuilt_entry.unique_column_ids_size());
    ASSERT_EQ(1, rebuilt_entry.unique_column_ids(0).column_ids_size());
    EXPECT_EQ(1002, rebuilt_entry.unique_column_ids(0).column_ids(0));
    ASSERT_EQ(1, rebuilt_entry.versions_size());
    EXPECT_EQ(new_version, rebuilt_entry.versions(0));
    ASSERT_EQ(1, rebuilt_entry.shared_files_size());
    EXPECT_FALSE(rebuilt_entry.shared_files(0));

    // 6. Read the rebuilt .cols back and assert row values reflect each
    //    owner child's updates.
    auto values = read_c1_only_cols_file(merged_tablet, rebuilt_entry.column_files(0));
    ASSERT_EQ(kNumRows, static_cast<int>(values.size()));
    for (int row = 0; row < kBoundary; ++row) {
        EXPECT_EQ(child_a_update(row), values[row]) << "row " << row << " should carry child A's update";
    }
    for (int row = kBoundary; row < kNumRows; ++row) {
        EXPECT_EQ(child_b_update(row), values[row]) << "row " << row << " should carry child B's update";
    }
}

// DCG same-column conflict combined with a compacted-away child (gap) on
// canonical R0. The rebuild must accept the masked gap window and fill it from
// the base segment instead of returning NotSupported. Three cases exercise the
// leading, internal, and trailing gap positions.
TEST_F(LakeTabletMergerTest, test_tablet_merging_dcg_conflict_with_gap_first_child_compacts) {
    run_dcg_conflict_gap_rebuild_case(/*compacted_index=*/0, /*txn_id=*/3101);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_dcg_conflict_with_gap_middle_child_compacts) {
    run_dcg_conflict_gap_rebuild_case(/*compacted_index=*/1, /*txn_id=*/3102);
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_dcg_conflict_with_gap_last_child_compacts) {
    run_dcg_conflict_gap_rebuild_case(/*compacted_index=*/2, /*txn_id=*/3103);
}

// When two children's DCG entries share a .cols filename but the entry
// metadata (column set, version, encryption, etc.) disagrees, exact dedup
// must reject the merge with Corruption via verify_dcg_entry_consistency.
TEST_F(LakeTabletMergerTest, test_tablet_merging_dcg_exact_dedup_consistency_failure) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t tablet_a = next_id();
    const int64_t tablet_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(tablet_a);
    prepare_tablet_dirs(tablet_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_child = [&](int64_t tablet_id, const std::vector<uint32_t>& dcg_columns) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(base_version);
        metadata->set_next_rowset_id(10);
        set_primary_key_schema(metadata.get(), 5001);
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename("shared_seg.dat");
            sm->set_size(100);
            sm->set_num_rows(10);
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(rowset, "shared_seg.dat"); // same UID across siblings => one canonical rowset
        (*metadata->mutable_rowset_to_schema())[1] = 5001;
        add_dcg_with_columns(metadata.get(), 1, "inconsistent.cols", dcg_columns, 1);
        return metadata;
    };

    // Same .cols filename on the same shared target, but different columns.
    auto meta_a = make_child(tablet_a, {601, 602});
    auto meta_b = make_child(tablet_b, {603}); // differs from A

    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(tablet_a);
    merging_tablet.add_old_tablet_ids(tablet_b);
    merging_tablet.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(91);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto st = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.is_corruption()) << st;
    EXPECT_NE(std::string::npos, st.to_string().find("unique_column_ids")) << st.to_string();
}

// When the schema resolved from merged metadata is missing one of the
// rebuild column UIDs, TabletSchema::create_with_uid silently drops it.
// The rebuild must fail fast with NotSupported (via the num_columns
// mismatch guard) instead of producing a .cols file with a silently
// missing column.
TEST_F(LakeTabletMergerTest, test_tablet_merging_dcg_rebuild_missing_uid_falls_back) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // Schema registers only UIDs {1001, 1002}. DCG entries below claim UID
    // 9999 which does not exist in the merged tablet schema.
    auto make_child = [&](int64_t tablet_id, const std::string& cols_filename) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(base_version);
        metadata->set_next_rowset_id(10);
        (void)set_two_column_pk_schema(metadata.get(), 6001);
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename("shared_seg.dat");
            sm->set_size(100);
            sm->set_num_rows(10);
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(rowset, "shared_seg.dat"); // same UID across siblings => one canonical rowset
        (*metadata->mutable_rowset_to_schema())[1] = 6001;
        auto& dcg = (*metadata->mutable_dcg_meta()->mutable_dcgs())[1];
        dcg.add_column_files(cols_filename);
        dcg.add_unique_column_ids()->add_column_ids(9999);
        dcg.add_versions(1);
        dcg.add_shared_files(false);
        return metadata;
    };
    auto meta_a = make_child(child_a, "a_missing.cols");
    auto meta_b = make_child(child_b, "b_missing.cols");

    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(child_a);
    merging_tablet.add_old_tablet_ids(child_b);
    merging_tablet.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(92);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto st = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.is_not_supported()) << st;
    // The guard message mentions missing column UIDs.
    EXPECT_NE(std::string::npos, st.to_string().find("missing one or more rebuild column UIDs")) << st.to_string();
}

// Two conflicting shared rowsets on DIFFERENT target segments. The first
// target rebuild writes a real .cols file; the second target fails because
// its base segment does not exist on disk. The cleanup path must delete
// the first target's .cols so it does not leak on publish failure.
TEST_F(LakeTabletMergerTest, test_tablet_merging_dcg_rebuild_cleanup_on_failure) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    constexpr int kNumRows = 20;
    constexpr int kBoundary = 10;
    constexpr uint32_t kGoodSegmentRssid = 1;
    constexpr uint32_t kBadSegmentRssid = 2;
    constexpr int64_t kTxnId = 555;

    // Set up target rssid=1 with a real base segment + real .cols files.
    const std::string good_segment = "good_shared.dat";
    auto source_value_of = [](int row) { return row * 7; };
    const uint64_t good_seg_size = write_two_column_segment(merged_tablet, good_segment, kNumRows, source_value_of);
    const std::string cols_a = lake::gen_cols_filename(kTxnId);
    const std::string cols_b = lake::gen_cols_filename(kTxnId + 1);
    auto a_cell = [&](int row) { return row < kBoundary ? row + 50000 : source_value_of(row); };
    auto b_cell = [&](int row) { return row >= kBoundary ? row + 60000 : source_value_of(row); };
    write_c1_only_cols_file(child_a, cols_a, kNumRows, a_cell);
    write_c1_only_cols_file(child_b, cols_b, kNumRows, b_cell);

    // Set up target rssid=2 pointing to a base segment that does NOT exist
    // on disk. Rebuild on this target will fail when compute_row_windows
    // tries to open the segment.
    const std::string bad_segment = "does_not_exist.dat";

    auto build_child = [&](int64_t tablet_id, int lower_key, int upper_key, const std::string& good_cols_name,
                           const std::string& bad_cols_name) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(base_version);
        metadata->set_next_rowset_id(10);
        const auto [c0_uid, c1_uid] = set_two_column_pk_schema(metadata.get(), 7001);
        (void)c0_uid;

        auto* tablet_range = metadata->mutable_range();
        tablet_range->set_lower_bound_included(true);
        tablet_range->set_upper_bound_included(false);
        *tablet_range->mutable_lower_bound() = generate_sort_key(lower_key);
        *tablet_range->mutable_upper_bound() = generate_sort_key(upper_key);

        // Good target rowset.
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(kGoodSegmentRssid);
        rowset->set_version(1);
        rowset->set_num_rows(kNumRows);
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename(good_segment);
            sm->set_size(good_seg_size);
            sm->set_num_rows(kNumRows);
            sm->set_shared(true);
        }
        *rowset->mutable_range()->mutable_lower_bound() = generate_sort_key(lower_key);
        *rowset->mutable_range()->mutable_upper_bound() = generate_sort_key(upper_key);
        rowset->mutable_range()->set_lower_bound_included(true);
        rowset->mutable_range()->set_upper_bound_included(false);
        (*metadata->mutable_rowset_to_schema())[kGoodSegmentRssid] = 7001;

        // Bad target rowset (base segment file does not exist).
        auto* bad_rowset = metadata->add_rowsets();
        bad_rowset->set_id(kBadSegmentRssid);
        bad_rowset->set_version(1);
        bad_rowset->set_num_rows(10);
        {
            auto* sm = bad_rowset->add_segment_metas();
            sm->set_filename(bad_segment);
            sm->set_size(100);
            sm->set_num_rows(10);
            sm->set_shared(true);
        }
        *bad_rowset->mutable_range()->mutable_lower_bound() = generate_sort_key(lower_key);
        *bad_rowset->mutable_range()->mutable_upper_bound() = generate_sort_key(upper_key);
        bad_rowset->mutable_range()->set_lower_bound_included(true);
        bad_rowset->mutable_range()->set_upper_bound_included(false);
        (*metadata->mutable_rowset_to_schema())[kBadSegmentRssid] = 7001;

        auto& good_dcg = (*metadata->mutable_dcg_meta()->mutable_dcgs())[kGoodSegmentRssid];
        good_dcg.add_column_files(good_cols_name);
        good_dcg.add_unique_column_ids()->add_column_ids(c1_uid);
        good_dcg.add_versions(1);
        good_dcg.add_shared_files(false);
        auto& bad_dcg = (*metadata->mutable_dcg_meta()->mutable_dcgs())[kBadSegmentRssid];
        bad_dcg.add_column_files(bad_cols_name);
        bad_dcg.add_unique_column_ids()->add_column_ids(c1_uid);
        bad_dcg.add_versions(1);
        bad_dcg.add_shared_files(false);
        return metadata;
    };

    auto meta_a = build_child(child_a, 0, kBoundary, cols_a, "bad_a.cols");
    auto meta_b = build_child(child_b, kBoundary, kNumRows, cols_b, "bad_b.cols");

    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    // Snapshot merged tablet's segment dir so we can detect leftover files.
    const std::string merged_segment_dir = _location_provider->segment_root_location(merged_tablet);
    std::set<std::string> pre_files;
    {
        auto status = FileSystem::Default()->iterate_dir(merged_segment_dir, [&](std::string_view name) {
            pre_files.emplace(name);
            return true;
        });
        EXPECT_TRUE(status.ok()) << status;
    }

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(child_a);
    merging_tablet.add_old_tablet_ids(child_b);
    merging_tablet.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(kTxnId + 2);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto st = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges);
    ASSERT_FALSE(st.ok()) << "expected failure on bad target rebuild";

    // After cleanup, the merged tablet's segment dir should not contain any
    // newly written .cols files (gen_cols_filename pattern uses txn id).
    std::set<std::string> post_files;
    {
        auto status = FileSystem::Default()->iterate_dir(merged_segment_dir, [&](std::string_view name) {
            post_files.emplace(name);
            return true;
        });
        EXPECT_TRUE(status.ok()) << status;
    }
    for (const auto& file : post_files) {
        if (pre_files.count(file) > 0) continue; // pre-existing
        EXPECT_EQ(std::string::npos, file.find(".cols")) << "leftover .cols file after cleanup: " << file;
    }
}

// ---------------------------------------------------------------------------
// PR-1: PK fail-fast + non-PK skip-dedup tests for split → partial-children
// compaction → merge correctness fix.
// ---------------------------------------------------------------------------

namespace pr1_helpers {

// Populate range = [lower, upper) on a TabletRangePB using INT sort key.
inline void set_int_range(TabletRangePB* range, int lower, int upper) {
    LakeTabletReshardTest::generate_sort_key(lower).Swap(range->mutable_lower_bound());
    range->set_lower_bound_included(true);
    LakeTabletReshardTest::generate_sort_key(upper).Swap(range->mutable_upper_bound());
    range->set_upper_bound_included(false);
}

// Build a child metadata that retains a shared rowset (no compaction).
// Range conventions:
//   - tablet range = [tablet_lower, tablet_upper)
//   - rowset range = same as tablet (split clip semantics)
inline std::shared_ptr<TabletMetadataPB> make_shared_child(int64_t tablet_id, int64_t base_version, uint32_t shared_id,
                                                           KeysType keys_type, int tablet_lower, int tablet_upper) {
    auto meta = std::make_shared<TabletMetadataPB>();
    meta->set_id(tablet_id);
    meta->set_version(base_version);
    meta->set_next_rowset_id(shared_id + 1);
    auto* schema = meta->mutable_schema();
    schema->set_keys_type(keys_type);
    schema->set_id(7777);
    set_int_range(meta->mutable_range(), tablet_lower, tablet_upper);

    auto* rowset = meta->add_rowsets();
    rowset->set_id(shared_id);
    rowset->set_version(base_version);
    rowset->set_num_rows(10);
    rowset->set_data_size(100);
    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("shared_seg.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(rowset, "shared_seg.dat"); // same uid across shared siblings => dedup
    set_int_range(rowset->mutable_range(), tablet_lower, tablet_upper);
    return meta;
}

// Build a child metadata where the shared rowset has been compacted into a
// fresh non-shared output rowset.
inline std::shared_ptr<TabletMetadataPB> make_compacted_child(int64_t tablet_id, int64_t base_version,
                                                              uint32_t compacted_id, KeysType keys_type,
                                                              int tablet_lower, int tablet_upper,
                                                              const std::string& compacted_seg_name) {
    auto meta = std::make_shared<TabletMetadataPB>();
    meta->set_id(tablet_id);
    meta->set_version(base_version);
    meta->set_next_rowset_id(compacted_id + 1);
    auto* schema = meta->mutable_schema();
    schema->set_keys_type(keys_type);
    schema->set_id(7777);
    set_int_range(meta->mutable_range(), tablet_lower, tablet_upper);

    auto* rowset = meta->add_rowsets();
    rowset->set_id(compacted_id);
    rowset->set_version(base_version + 1); // compaction bumps version
    rowset->set_num_rows(10);
    rowset->set_data_size(100);
    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename(compacted_seg_name);
        sm->set_size(100);
        sm->set_num_rows(10);
    }
    // Not shared: this is the local compaction output.
    set_int_range(rowset->mutable_range(), tablet_lower, tablet_upper);
    return meta;
}

// PR-2: 1-column PK schema with c0:INT key, mirroring the column layout used
// by write_two_column_segment for the shared physical segment. Phase 0 only
// needs the key column; we omit c1 so the schema matches the segment's
// expected column ordering for the seek-range to rowid-range translation.
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

// PR-2 helper: build a 3-way split with one compacted child + 2 children
// retaining a shared rowset that points to a real on-disk segment, and
// publish the merge. The macro returns the merged metadata bound to |MERGED|
// and the canonical R0's segment rssid bound to |CANONICAL_RSSID|. Use a
// macro because the helper needs access to the fixture's protected
// |_tablet_manager| / |next_id| / |prepare_tablet_dirs| /
// |write_two_column_segment|.
#define BUILD_THREE_WAY_PK_GAP_MERGE(MERGED, CANONICAL_RSSID, MERGED_TABLET, COMPACTED_INDEX, TXN_ID)                  \
    TabletMetadataPtr MERGED;                                                                                          \
    int64_t MERGED_TABLET = 0;                                                                                         \
    uint32_t CANONICAL_RSSID = 0;                                                                                      \
    do {                                                                                                               \
        using namespace pr1_helpers;                                                                                   \
        const int64_t base_version = 1;                                                                                \
        const int64_t new_version = 2;                                                                                 \
        const int64_t child_ids[3] = {next_id(), next_id(), next_id()};                                                \
        MERGED_TABLET = next_id();                                                                                     \
        prepare_tablet_dirs(child_ids[0]);                                                                             \
        prepare_tablet_dirs(child_ids[1]);                                                                             \
        prepare_tablet_dirs(child_ids[2]);                                                                             \
        prepare_tablet_dirs(MERGED_TABLET);                                                                            \
        constexpr int kNumRows = 30;                                                                                   \
        constexpr int kRangeBoundaries[4] = {0, 10, 20, 30};                                                           \
        uint64_t segment_size =                                                                                        \
                write_two_column_segment(MERGED_TABLET, "shared_seg.dat", kNumRows, [](int i) { return i * 10; });     \
        std::shared_ptr<TabletMetadataPB> metas[3];                                                                    \
        for (int i = 0; i < 3; ++i) {                                                                                  \
            const int lower = kRangeBoundaries[i];                                                                     \
            const int upper = kRangeBoundaries[i + 1];                                                                 \
            if (i == (COMPACTED_INDEX)) {                                                                              \
                metas[i] = make_pk_compacted_child(child_ids[i], base_version, /*compacted_id=*/11, lower, upper,      \
                                                   fmt::format("compacted_{}.dat", i));                                \
            } else {                                                                                                   \
                metas[i] =                                                                                             \
                        make_pk_shared_child_with_real_segment(child_ids[i], base_version, /*shared_id=*/10, lower,    \
                                                               upper, segment_size, /*physical_num_rows=*/kNumRows);   \
            }                                                                                                          \
            EXPECT_OK(put_tablet_metadata(metas[i]));                                                                  \
        }                                                                                                              \
        ReshardingTabletInfoPB resharding_tablet;                                                                      \
        auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();                                         \
        merging_info.add_old_tablet_ids(child_ids[0]);                                                                 \
        merging_info.add_old_tablet_ids(child_ids[1]);                                                                 \
        merging_info.add_old_tablet_ids(child_ids[2]);                                                                 \
        merging_info.set_new_tablet_id(MERGED_TABLET);                                                                 \
        TxnInfoPB txn_info;                                                                                            \
        txn_info.set_txn_id(TXN_ID);                                                                                   \
        txn_info.set_commit_time(1);                                                                                   \
        txn_info.set_gtid(1);                                                                                          \
        std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;                                               \
        std::unordered_map<int64_t, TabletRangePB> tablet_ranges;                                                      \
        ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version, \
                                                  txn_info, false, tablet_metadatas, tablet_ranges));                  \
        MERGED = tablet_metadatas.at(MERGED_TABLET);                                                                   \
        ASSERT_NE(MERGED, nullptr);                                                                                    \
        for (const auto& r : MERGED->rowsets()) {                                                                      \
            bool has_shared = false;                                                                                   \
            for (int i = 0; i < r.segment_metas_size(); ++i) {                                                         \
                if (r.segment_metas(i).shared()) {                                                                     \
                    has_shared = true;                                                                                 \
                    break;                                                                                             \
                }                                                                                                      \
            }                                                                                                          \
            if (has_shared) {                                                                                          \
                CANONICAL_RSSID = r.id();                                                                              \
                break;                                                                                                 \
            }                                                                                                          \
        }                                                                                                              \
    } while (0)

#define ASSERT_SYNTHESIZED_GAP_DELVEC(MERGED, CANONICAL_RSSID)                                                     \
    do {                                                                                                           \
        ASSERT_TRUE((MERGED)->has_delvec_meta()) << "delvec_meta missing — Phase 0 did not synthesize gap delvec"; \
        ASSERT_EQ(1, (MERGED)->delvec_meta().version_to_file_size())                                               \
                << "expected exactly one delvec file written by merge_delvecs";                                    \
        auto delvec_it = (MERGED)->delvec_meta().delvecs().find(CANONICAL_RSSID);                                  \
        ASSERT_NE(delvec_it, (MERGED)->delvec_meta().delvecs().end())                                              \
                << "delvec_meta has no entry for canonical rssid " << (CANONICAL_RSSID);                           \
        EXPECT_GT(delvec_it->second.size(), 0u) << "synthesized delvec page is empty";                             \
        EXPECT_EQ(2, (MERGED)->version()) << "merged tablet version mismatch";                                     \
    } while (0)

TEST_F(LakeTabletMergerTest, test_tablet_merging_encrypted_delvec_gap_promotion_rejected_before_read) {
    using namespace pr1_helpers;
    constexpr int64_t kBaseVersion = 1;
    constexpr int64_t kNewVersion = 2;
    constexpr uint32_t kSharedRssid = 10;

    auto build_sources = [&](bool encrypted, int64_t merged_tablet) {
        const int64_t child_left = next_id();
        const int64_t child_gap = next_id();
        const int64_t child_right = next_id();
        for (int64_t tablet_id : {child_left, child_gap, child_right, merged_tablet}) {
            prepare_tablet_dirs(tablet_id);
        }
        const uint64_t segment_size =
                write_two_column_segment(merged_tablet, "shared_seg.dat", 30, [](int value) { return value * 10; });
        auto left = make_pk_shared_child_with_real_segment(child_left, kBaseVersion, kSharedRssid, 0, 10, segment_size,
                                                           /*physical_num_rows=*/30);
        auto compacted =
                make_pk_compacted_child(child_gap, kBaseVersion, /*compacted_id=*/11, 10, 20, "compacted_gap.dat");
        auto right =
                make_pk_shared_child_with_real_segment(child_right, kBaseVersion, kSharedRssid, 20, 30, segment_size,
                                                       /*physical_num_rows=*/30);
        DelVector source_delvec;
        const uint32_t deleted = 0;
        source_delvec.init(/*version=*/10, &deleted, 1);
        const std::string source_name = encrypted ? "encrypted-gap-promotion.delvec" : "plain-gap-promotion.delvec";
        add_delvec(left.get(), child_left, /*version=*/10, kSharedRssid, source_name, source_delvec.save());
        if (encrypted) {
            (*left->mutable_delvec_meta()->mutable_version_to_file())[10].set_encryption_meta("unsupported");
        }
        return std::vector<TabletMetadataPtr>{left, compacted, right};
    };

    // First prove this exact three-child layout reaches synthesized-gap promotion when plaintext:
    // two canonical shared contributors cover [0,10) and [20,30), while the compacted child leaves
    // [10,20) as a real Phase-0 gap. The single raw delvec on the left must be promoted and unioned.
    const int64_t plain_merged_tablet = next_id();
    int plaintext_promotions = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("merge_delvecs:before_gap_promotion", [&](void*) { ++plaintext_promotions; });
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
    });
    std::unordered_map<int64_t, TabletMetadataPtr> plaintext_published;
    ASSERT_OK(publish_resharding_merge(build_sources(false, plain_merged_tablet), plain_merged_tablet, kBaseVersion,
                                       kNewVersion, next_id(), plaintext_published));
    ASSERT_EQ(1, plaintext_promotions);
    const auto& plaintext_merged = plaintext_published.at(plain_merged_tablet);
    const uint32_t canonical_rssid = plaintext_merged->rowsets(0).id();
    ASSERT_TRUE(plaintext_merged->delvec_meta().delvecs().contains(canonical_rssid));
    DelVector promoted;
    LakeIOOptions io_options;
    ASSERT_OK(
            lake::get_del_vec(_tablet_manager.get(), *plaintext_merged, canonical_rssid, false, io_options, &promoted));
    ASSERT_NE(nullptr, promoted.roaring());
    EXPECT_TRUE(promoted.roaring()->contains(0));
    EXPECT_TRUE(promoted.roaring()->contains(10));

    const int64_t encrypted_merged_tablet = next_id();
    int encrypted_promotions = 0;
    int get_del_vec_calls = 0;
    int preflight_opens = 0;
    int range_reads = 0;
    int writer_opens = 0;
    sync->ClearAllCallBacks();
    sync->SetCallBack("merge_delvecs:before_gap_promotion", [&](void*) { ++encrypted_promotions; });
    sync->SetCallBack("merge_delvecs:before_get_del_vec", [&](void*) { ++get_del_vec_calls; });
    sync->SetCallBack("write_compacted_delvec_pages:preflight_source_open", [&](void*) { ++preflight_opens; });
    sync->SetCallBack("write_compacted_delvec_pages:read_chunk_size", [&](void*) { ++range_reads; });
    sync->SetCallBack("write_compacted_delvec_pages:writer_open", [&](void*) { ++writer_opens; });
    std::unordered_map<int64_t, TabletMetadataPtr> encrypted_published;
    const Status status =
            publish_resharding_merge(build_sources(true, encrypted_merged_tablet), encrypted_merged_tablet,
                                     kBaseVersion, kNewVersion, next_id(), encrypted_published);
    EXPECT_TRUE(status.is_not_supported()) << status;
    EXPECT_EQ("encrypted delvec input is unsupported; delvec must be plaintext: encrypted-gap-promotion.delvec",
              status.message());
    EXPECT_EQ(0, encrypted_promotions);
    EXPECT_EQ(0, get_del_vec_calls);
    EXPECT_EQ(0, preflight_opens);
    EXPECT_EQ(0, range_reads);
    EXPECT_EQ(0, writer_opens);
    EXPECT_FALSE(encrypted_published.contains(encrypted_merged_tablet));
}

// A synthesized gap delvec remains authoritative when divergent rowset layouts force lazy index rebuild, even when
// the inherited index metadata carries no delvec. Cold first-writer recovery must rebuild from rowsets, honor the
// synthesized target delvec, and preserve the exact data oracle across reopen.
TEST_F(LakeTabletMergerTest, test_tablet_merging_synthesized_delvec_survives_lazy_rebuild_fallback) {
    using namespace pr1_helpers;
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // Real shared segment under merged_tablet/segments/. c0 = [0..20).
    const uint64_t segment_size =
            write_two_column_segment(merged_tablet, "shared_seg.dat", 20, [](int i) { return i * 10; });

    // Child A retains the shared rowset for tablet range [0, 10). Its
    // sstable_meta carries a shared sstable with shared_rssid=10 (A's R0
    // namespace) and NO has_delvec on source — exercising the §5.2.4 path.
    auto meta_a = make_pk_shared_child_with_real_segment(child_a, base_version, /*shared_id=*/10, /*lower=*/0,
                                                         /*upper=*/10, segment_size, /*physical_num_rows=*/20);
    meta_a->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
    auto* sst_a = meta_a->mutable_sstable_meta()->add_sstables();
    sst_a->set_filename("shared.sst");
    sst_a->set_filesize(512);
    sst_a->set_shared(true);
    sst_a->set_shared_rssid(10);
    sst_a->set_shared_version(2);
    sst_a->set_max_rss_rowid((static_cast<uint64_t>(10) << 32) | 99);
    // intentionally NO sst_a->set_delvec(...): source has no delvec.

    // Child B has compacted away its share — non-shared compaction output for
    // tablet range [10, 20). Without B's contribution, canonical R0's
    // contributors only cover [0,10); compute_disjoint_gaps_within emits
    // [10, 20) within the merged tablet range.
    const uint64_t compacted_size = write_two_column_segment(
            merged_tablet, "compacted_b.dat", /*num_rows=*/10, [](int key) { return key * 10; }, /*key_start=*/10);
    auto meta_b = make_pk_compacted_child(child_b, base_version, /*compacted_id=*/11, /*lower=*/10, /*upper=*/20,
                                          "compacted_b.dat");
    meta_b->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
    meta_b->mutable_rowsets(0)->set_data_size(compacted_size);
    meta_b->mutable_rowsets(0)->mutable_segment_metas(0)->set_size(compacted_size);

    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(2010);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));
    auto merged = tablet_metadatas.at(merged_tablet);
    ASSERT_NE(merged, nullptr);

    // Locate canonical R0 (the rowset with at least one segment_metas(i).shared()==true).
    uint32_t canonical_rssid = 0;
    for (const auto& r : merged->rowsets()) {
        bool has_shared = false;
        for (const auto& segment_meta : r.segment_metas()) {
            if (segment_meta.shared()) {
                has_shared = true;
                break;
            }
        }
        if (has_shared) {
            canonical_rssid = r.id();
            break;
        }
    }
    ASSERT_NE(canonical_rssid, 0u);

    // delvec_meta should have a synthesized entry for canonical_rssid.
    auto delvec_it = merged->delvec_meta().delvecs().find(canonical_rssid);
    ASSERT_NE(delvec_it, merged->delvec_meta().delvecs().end());
    EXPECT_GT(delvec_it->second.size(), 0u);

    // The compacted sibling makes the rowset layouts divergent, so the complete shared cohort cannot be reused.
    // The synthesized delvec remains authoritative rowset metadata while the inherited SST is orphaned exactly once.
    EXPECT_EQ(0, merged->sstable_meta().sstables_size());
    ASSERT_EQ(1, merged->orphan_files_size());
    EXPECT_EQ("shared.sst", merged->orphan_files(0).name());
    EXPECT_EQ(512, merged->orphan_files(0).size());
    EXPECT_TRUE(merged->orphan_files(0).shared());
    EXPECT_EQ(0, merged->orphan_files(0).version());

    // A real first writer must rebuild from both physical rowsets while honoring the synthesized delvec, then
    // apply its upsert/delete. Reopen both the row reader and persistent index to prove no stale rows return.
    _update_manager->unload_and_remove_primary_index(merged_tablet);
    ASSIGN_OR_ABORT(auto after_dml, publish_followup_upsert_delete(merged_tablet, new_version, /*upsert_key=*/20,
                                                                   /*upsert_value=*/2020, /*delete_key=*/0));
    std::vector<std::pair<int32_t, int32_t>> expected_rows;
    for (int32_t key = 1; key < 20; ++key) expected_rows.emplace_back(key, key * 10);
    expected_rows.emplace_back(20, 2020);
    expect_lifecycle_oracle(after_dml, expected_rows, /*deleted_keys=*/{0});

    _update_manager->unload_and_remove_primary_index(merged_tablet);
    ASSIGN_OR_ABORT(auto reopened, _tablet_manager->get_tablet_metadata(merged_tablet, after_dml->version()));
    expect_lifecycle_oracle(reopened, expected_rows, /*deleted_keys=*/{0});
}

// PR-2: first child compacted. canonical_contribs covers [10,30) inside the
// merged tablet range [0,30); compute_disjoint_gaps_within emits [0,10),
// translated into rowid window [0,10) on the shared 30-row segment, which
// must end up in the synthesized delvec on canonical R0.
TEST_F(LakeTabletMergerTest, test_tablet_merging_pk_gap_delvec_first_child_compacts) {
    BUILD_THREE_WAY_PK_GAP_MERGE(merged, canonical_rssid, merged_tablet, /*compacted_index=*/0, /*txn_id=*/1001);
    ASSERT_SYNTHESIZED_GAP_DELVEC(merged, canonical_rssid);
    // Two rowsets in merged: canonical R0 (shared, deduped from B+C) and
    // R1 (A's compaction output, non-shared).
    ASSERT_EQ(2, merged->rowsets_size());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_pk_gap_delvec_middle_child_compacts) {
    BUILD_THREE_WAY_PK_GAP_MERGE(merged, canonical_rssid, merged_tablet, /*compacted_index=*/1, /*txn_id=*/1002);
    ASSERT_SYNTHESIZED_GAP_DELVEC(merged, canonical_rssid);
    ASSERT_EQ(2, merged->rowsets_size());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_pk_gap_delvec_last_child_compacts) {
    BUILD_THREE_WAY_PK_GAP_MERGE(merged, canonical_rssid, merged_tablet, /*compacted_index=*/2, /*txn_id=*/1003);
    ASSERT_SYNTHESIZED_GAP_DELVEC(merged, canonical_rssid);
    ASSERT_EQ(2, merged->rowsets_size());
}

// PR-2 deeper assertion: load the merged delvec file and decode its Roaring
// bitmap, verify the exact rowid set matches the compacted child's range. The
// shared segment in BUILD_THREE_WAY_PK_GAP_MERGE has c0 = [0..30) so rowid==key
// when the segment's short-key index resolves the seek range.
//
// compacted_index=0 → contributors cover [10,30) → gap [0,10) → masked rowids {0..9}
// compacted_index=1 → contributors cover [0,10)∪[20,30) → gap [10,20) → masked rowids {10..19}
// compacted_index=2 → contributors cover [0,20) → gap [20,30) → masked rowids {20..29}
TEST_F(LakeTabletMergerTest, test_tablet_merging_pk_gap_delvec_rowid_content_matches_compacted_range) {
    auto check = [&](int compacted_index, int64_t txn_id, uint32_t expected_lo, uint32_t expected_hi) {
        BUILD_THREE_WAY_PK_GAP_MERGE(merged, canonical_rssid, merged_tablet, compacted_index, txn_id);
        ASSERT_SYNTHESIZED_GAP_DELVEC(merged, canonical_rssid);

        DelVector loaded;
        LakeIOOptions io_opts;
        // get_del_vec takes a const TabletMetadata&; *merged is already that type.
        ASSERT_OK(get_del_vec(_tablet_manager.get(), *merged, canonical_rssid, /*fill_cache=*/false, io_opts, &loaded));
        ASSERT_TRUE(loaded.roaring() != nullptr) << "loaded delvec empty for compacted_index=" << compacted_index;
        const Roaring& bitmap = *loaded.roaring();

        // Expected: exactly {expected_lo .. expected_hi - 1}.
        Roaring expected;
        expected.addRange(expected_lo, expected_hi);
        EXPECT_EQ(expected.cardinality(), bitmap.cardinality())
                << "cardinality mismatch for compacted_index=" << compacted_index;
        EXPECT_TRUE(bitmap == expected) << "bitmap mismatch for compacted_index=" << compacted_index;
    };
    check(/*compacted_index=*/0, /*txn_id=*/1101, /*expected_lo=*/0, /*expected_hi=*/10);
    check(/*compacted_index=*/1, /*txn_id=*/1102, /*expected_lo=*/10, /*expected_hi=*/20);
    check(/*compacted_index=*/2, /*txn_id=*/1103, /*expected_lo=*/20, /*expected_hi=*/30);
}

// PR-2 contiguous: all children retain the shared rowset → contributors cover
// the merged tablet range → no synthesized gap delvec generated, no delvec
// file written. Phase 0 returns empty without opening any segment.
TEST_F(LakeTabletMergerTest, test_tablet_merging_pk_no_gap_passthrough) {
    using namespace pr1_helpers;
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t child_c = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(child_c);
    prepare_tablet_dirs(merged_tablet);

    auto meta_a = make_shared_child(child_a, base_version, 10, PRIMARY_KEYS, 0, 10);
    auto meta_b = make_shared_child(child_b, base_version, 10, PRIMARY_KEYS, 10, 20);
    auto meta_c = make_shared_child(child_c, base_version, 10, PRIMARY_KEYS, 20, 30);

    ASSERT_OK(put_merge_sources(meta_a, meta_b, meta_c));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.add_old_tablet_ids(child_c);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(4);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));
    auto merged = tablet_metadatas.at(merged_tablet);
    // Three shared rowsets dedup down to one canonical (PK always dedups, all contiguous).
    ASSERT_EQ(1, merged->rowsets_size());
    // No gap → Phase 0 emits no synthesized specs → no delvec_meta entries.
    EXPECT_EQ(0, merged->delvec_meta().delvecs_size())
            << "no children had delvecs and no gap was synthesized; expected empty delvec_meta";
}

// Non-PK (DUP) skip-dedup: three children with shared rowsets, middle child compacted →
// the two non-compacted children's ranges are non-adjacent, so dedup is skipped and
// they remain as separate rowsets in merged metadata.
TEST_F(LakeTabletMergerTest, test_tablet_merging_dup_keys_skip_dedup_on_gap) {
    using namespace pr1_helpers;
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t child_c = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(child_c);
    prepare_tablet_dirs(merged_tablet);

    auto meta_a = make_shared_child(child_a, base_version, 10, DUP_KEYS, 0, 10);
    auto meta_b = make_compacted_child(child_b, base_version, 11, DUP_KEYS, 10, 20, "cb.dat");
    auto meta_c = make_shared_child(child_c, base_version, 10, DUP_KEYS, 20, 30);

    ASSERT_OK(put_merge_sources(meta_a, meta_b, meta_c));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.add_old_tablet_ids(child_c);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(5);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));
    auto merged = tablet_metadatas.at(merged_tablet);
    // Expect 3 rowsets: A's shared (range [0,10)), C's shared (range [20,30)) NOT deduped
    // with A's because [10,20) gap, plus B's compacted output.
    ASSERT_EQ(3, merged->rowsets_size());
    int shared_count = 0;
    int local_count = 0;
    for (const auto& r : merged->rowsets()) {
        bool has_shared = false;
        for (const auto& segment_meta : r.segment_metas()) {
            if (segment_meta.shared()) {
                has_shared = true;
                break;
            }
        }
        if (has_shared) {
            ++shared_count;
        } else {
            ++local_count;
        }
    }
    EXPECT_EQ(2, shared_count) << "two non-deduped shared rowsets expected";
    EXPECT_EQ(1, local_count) << "one local compaction-output rowset expected";
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_agg_keys_skip_dedup_on_gap) {
    using namespace pr1_helpers;
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t child_c = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(child_c);
    prepare_tablet_dirs(merged_tablet);

    auto meta_a = make_shared_child(child_a, base_version, 10, AGG_KEYS, 0, 10);
    auto meta_b = make_compacted_child(child_b, base_version, 11, AGG_KEYS, 10, 20, "cb.dat");
    auto meta_c = make_shared_child(child_c, base_version, 10, AGG_KEYS, 20, 30);

    ASSERT_OK(put_merge_sources(meta_a, meta_b, meta_c));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.add_old_tablet_ids(child_c);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(6);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));
    auto merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(3, merged->rowsets_size());
}

TEST_F(LakeTabletMergerTest, test_tablet_merging_unique_keys_skip_dedup_on_gap) {
    using namespace pr1_helpers;
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t child_c = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(child_c);
    prepare_tablet_dirs(merged_tablet);

    auto meta_a = make_shared_child(child_a, base_version, 10, UNIQUE_KEYS, 0, 10);
    auto meta_b = make_compacted_child(child_b, base_version, 11, UNIQUE_KEYS, 10, 20, "cb.dat");
    auto meta_c = make_shared_child(child_c, base_version, 10, UNIQUE_KEYS, 20, 30);

    ASSERT_OK(put_merge_sources(meta_a, meta_b, meta_c));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.add_old_tablet_ids(child_c);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(7);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));
    auto merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(3, merged->rowsets_size());
}

// Non-PK + contiguous: the two adjacent shared rowsets dedup into one canonical,
// matching the pre-PR-1 behavior. No fail-fast (non-PK) and no skip (ranges contiguous).
TEST_F(LakeTabletMergerTest, test_tablet_merging_non_pk_contiguous_still_dedups) {
    using namespace pr1_helpers;
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto meta_a = make_shared_child(child_a, base_version, 10, DUP_KEYS, 0, 10);
    auto meta_b = make_shared_child(child_b, base_version, 10, DUP_KEYS, 10, 20);

    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(8);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));
    auto merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(1, merged->rowsets_size());
}

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
    ASSERT_OK(put_merge_sources(make_shared_child(child_a, base_version, 10, DUP_KEYS, 0, 10),
                                make_shared_child(child_b, base_version, 10, DUP_KEYS, 10, 20)));

    int source_flush_count = 0;
    int classifier_count = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("merge_sstables:source_pk_flush", [&](void*) { ++source_flush_count; });
    sync->SetCallBack("merge_sstables:metadata_classifier_entry", [&](void*) { ++classifier_count; });
    sync->EnableProcessing();
    DeferOp clear_callbacks([&] {
        sync->ClearCallBack("merge_sstables:source_pk_flush");
        sync->ClearCallBack("merge_sstables:metadata_classifier_entry");
        sync->DisableProcessing();
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
    auto status = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                                  txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(status);
    EXPECT_EQ(0, source_flush_count) << "non-PK writable merge must not enter source PK-index flush";
    EXPECT_EQ(0, classifier_count) << "non-PK writable merge must not enter the PK metadata classifier";
    if (!status.ok()) return;

    const auto& merged = tablet_metadatas.at(merged_tablet);
    EXPECT_EQ(0, merged->sstable_meta().sstables_size());
    EXPECT_EQ(0, merged->orphan_files_size());
    EXPECT_EQ(1, merged->rowsets_size());
}

// Regression for Codex round-1 finding: when a duplicate rowset lacks its own
// `range` but its tablet metadata has one, the canonical's stored range must
// still extend to cover the duplicate. Otherwise readers (which prefer
// rowset.range over tablet.range) miss rows from later contributors. PR-1
// pushes the *effective* duplicate range (rowset.range || ctx.metadata.range
// || unbounded) into update_canonical to fix this.
TEST_F(LakeTabletMergerTest, test_tablet_merging_canonical_range_extends_for_duplicate_without_rowset_range) {
    using namespace pr1_helpers;
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_no_rowset_range_child = [&](int64_t tid, int tablet_lower, int tablet_upper) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tid);
        meta->set_version(base_version);
        meta->set_next_rowset_id(11);
        auto* schema = meta->mutable_schema();
        schema->set_keys_type(DUP_KEYS);
        schema->set_id(7777);
        set_int_range(meta->mutable_range(), tablet_lower, tablet_upper);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(10);
        rowset->set_version(base_version);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename("shared_seg.dat");
            sm->set_size(100);
            sm->set_num_rows(10);
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(rowset, "shared_seg.dat"); // same uid across siblings => dedup
        // intentionally NO rowset->mutable_range(): rely on ctx tablet range
        return meta;
    };

    auto meta_a = make_no_rowset_range_child(child_a, 0, 10);
    auto meta_b = make_no_rowset_range_child(child_b, 10, 20);

    ASSERT_OK(put_merge_sources(meta_a, meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(91);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));
    auto merged = tablet_metadatas.at(merged_tablet);
    // Non-PK + adjacent ranges → dedup into one canonical.
    ASSERT_EQ(1, merged->rowsets_size());
    const auto& canonical = merged->rowsets(0);
    ASSERT_TRUE(canonical.has_range());
    // canonical.range should be the union [0, 20), not just A's [0, 10).
    TabletRangePB expected_full;
    set_int_range(&expected_full, 0, 20);
    EXPECT_EQ(expected_full.lower_bound().DebugString(), canonical.range().lower_bound().DebugString())
            << "canonical lower mismatch";
    EXPECT_EQ(expected_full.upper_bound().DebugString(), canonical.range().upper_bound().DebugString())
            << "canonical upper mismatch";
    EXPECT_EQ(expected_full.lower_bound_included(), canonical.range().lower_bound_included());
    EXPECT_EQ(expected_full.upper_bound_included(), canonical.range().upper_bound_included());
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

// MERGE: two split siblings share one physical .idx; it must dedup to a single entry under
// the canonical target rssid, marked shared. Mirrors test_tablet_merging_shared_dcg_dedup.
TEST_F(LakeTabletMergerTest, test_tablet_merging_shared_idg_dedup) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_child_with_idg = [&](int64_t tablet_id) {
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
        sm->set_filename("shared_seg.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
        sm->set_shared(true);
        stamp_physical_identity_uid(rowset, "shared_seg.dat"); // same uid across siblings => dedup
        add_idg_with_key(meta.get(), 1, "shared.idx", /*col_uid=*/5, BITMAP, 1);
        return meta;
    };
    ASSERT_OK(put_merge_sources(make_child_with_idg(child_a), make_child_with_idg(child_b)));

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
    ASSERT_EQ(1, merged->rowsets_size());
    ASSERT_TRUE(merged->has_idg_meta());
    ASSERT_EQ(1, merged->idg_meta().idgs().size());
    auto idg_it = merged->idg_meta().idgs().find(merged->rowsets(0).id());
    ASSERT_TRUE(idg_it != merged->idg_meta().idgs().end());
    ASSERT_EQ(1, idg_it->second.entries_size()) << "shared .idx deduped to one entry";
    EXPECT_EQ("shared.idx", idg_it->second.entries(0).index_file());
    EXPECT_TRUE(idg_it->second.entries(0).shared_file());
}

// MERGE: same shared .idx with TWO keys (col5,col6); sibling B tombstones only col5.
// The deduped entry must be retained (col6 active) with the col5 tombstone unioned in.
TEST_F(LakeTabletMergerTest, test_tablet_merging_idg_unions_divergent_tombstones) {
    const int64_t base_version = 1, new_version = 2;
    const int64_t child_a = next_id(), child_b = next_id(), merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_child = [&](int64_t tablet_id, bool drop_col5) {
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
        sm->set_filename("shared_seg.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
        sm->set_shared(true);
        stamp_physical_identity_uid(rowset, "shared_seg.dat");
        add_idg_with_key(meta.get(), 1, "shared.idx", /*col_uid=*/5, BITMAP, 1);
        add_idg_key(meta.get(), 1, /*col_uid=*/6, BITMAP); // two keys on the same .idx
        if (drop_col5) add_idg_dropped_key(meta.get(), 1, /*col_uid=*/5, BITMAP);
        return meta;
    };
    ASSERT_OK(put_merge_sources(make_child(child_a, /*drop_col5=*/false), make_child(child_b, /*drop_col5=*/true)));

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
    ASSERT_TRUE(merged->has_idg_meta());
    auto idg_it = merged->idg_meta().idgs().find(merged->rowsets(0).id());
    ASSERT_TRUE(idg_it != merged->idg_meta().idgs().end());
    ASSERT_EQ(1, idg_it->second.entries_size()) << "entry retained: col6 still active";
    ASSERT_EQ(1, idg_it->second.entries(0).dropped_keys_size()) << "divergent tombstone unioned in";
    EXPECT_EQ(5, idg_it->second.entries(0).dropped_keys(0).col_unique_id());
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

// MERGE: DROP INDEX tombstones are table-wide monotonic for one .idx + physical base.
// A tombstone under either co-referenced target therefore makes the single-key entry dead
// under both targets; with no active reference, the physical .idx is orphaned exactly once.
TEST_F(LakeTabletMergerTest, test_tablet_merging_idg_global_tombstone_orphans_without_active_reference) {
    const int64_t base_version = 1, new_version = 2;
    const int64_t child_a = next_id(), child_b = next_id(), merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_child = [&](int64_t tablet_id, bool tombstone) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(2); // child_b's rowset remaps to rssid 2
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("same_base.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
        lake::tablet_reshard_helper::set_rowset_uid(rowset); // distinct uid per child => both kept
        add_idg_with_key(meta.get(), 1, "same.idx", /*col_uid=*/5, BITMAP, 1);
        if (tombstone) add_idg_dropped_key(meta.get(), 1, /*col_uid=*/5, BITMAP);
        return meta;
    };
    ASSERT_OK(put_merge_sources(make_child(child_a, /*tombstone=*/false), make_child(child_b, /*tombstone=*/true)));

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
    int active = 0;
    for (const auto& [rssid, ver] : merged->idg_meta().idgs()) {
        (void)rssid;
        for (const auto& e : ver.entries()) {
            if (e.index_file() == "same.idx") ++active;
        }
    }
    EXPECT_EQ(0, active) << "the table-wide tombstone must remove every co-reference";
    EXPECT_EQ(1, std::count_if(merged->orphan_files().begin(), merged->orphan_files().end(),
                               [](const auto& file) { return file.name() == "same.idx"; }));
}

// MERGE: a source split before this fix can have segment.shared=true but a stale
// idg.shared_file=false (the old split marked segments shared but not idg). merge must
// DERIVE the .idx shared flag from the merged segment's ownership, upgrading the stale
// false to true, so vacuum does not later delete a shared .idx as if it were private.
TEST_F(LakeTabletMergerTest, test_tablet_merging_idg_derives_shared_from_segment) {
    const int64_t base_version = 1, new_version = 2;
    const int64_t child_a = next_id(), child_b = next_id(), merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // child_a: a SHARED segment (referenced by some non-merged sibling) whose idg entry
    // carries a stale shared_file=false.
    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(2);
    {
        auto* rowset = meta_a->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("seg_shared.dat");
        sm->set_size(100);
        sm->set_num_rows(10);
        sm->set_shared(true); // shared segment
        stamp_physical_identity_uid(rowset, "seg_shared.dat");
    }
    add_idg_with_key(meta_a.get(), 1, "shared.idx", /*col_uid=*/5, BITMAP, 1, /*shared_file=*/false); // stale

    // child_b: an unrelated private segment (distinct uid) so both rowsets survive the merge.
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
        stamp_physical_identity_uid(rowset, "seg_b.dat");
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
    ASSERT_TRUE(merged->has_idg_meta());
    bool found = false;
    for (const auto& [rssid, ver] : merged->idg_meta().idgs()) {
        for (const auto& e : ver.entries()) {
            if (e.index_file() == "shared.idx") {
                found = true;
                EXPECT_TRUE(e.shared_file()) << "shared segment's .idx flag must be derived (upgraded) to true";
            }
        }
    }
    EXPECT_TRUE(found) << "shared.idx entry must survive the merge";
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

// Merge phase 1 -- the rowset-id reassignment stage. Fires on every merge.
TEST_F(LakeTabletMergerTest, test_merge_failpoint_after_rssid_reassign) {
    auto run_merge = [&]() {
        const int64_t base_version = 1;
        const int64_t new_version = 2;
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
        add_rowset_with_predicate(&meta_a, 1, 1, false);
        add_rowset_with_predicate(&meta_a, 2, 2, false);

        TabletMetadataPB meta_b;
        meta_b.set_id(tablet_b);
        meta_b.set_version(base_version);
        meta_b.set_next_rowset_id(2);
        add_rowset_with_predicate(&meta_b, 1, 1, false);
        CHECK_OK(put_merge_sources(meta_a, meta_b));

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
        return lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                               txn_info, false, tablet_metadatas, tablet_ranges);
    };

    set_failpoint_mode("tablet_merge_after_rssid_reassign", FailPointTriggerModeType::ENABLE);
    auto armed = run_merge();
    set_failpoint_mode("tablet_merge_after_rssid_reassign", FailPointTriggerModeType::DISABLE);
    EXPECT_FALSE(armed.ok()) << "hook not reached at the merge rssid-reassignment stage";

    EXPECT_OK(run_merge());
}

// The window in which a delete predicate has been copied into the merged metadata but is not yet
// confined to its source tablet's range. The hook is guarded on has_delete_predicate(), so it fires
// only for a merge whose source actually carries one -- that guard is what makes it express this
// window rather than "the first rowset of any merge". Both sources here get a predicate; neither
// tablet is a primary-key tablet, which is the case that uses delete predicates in production.
TEST_F(LakeTabletMergerTest, test_merge_failpoint_before_delete_predicate_range) {
    auto run_merge = [&]() {
        const int64_t base_version = 1;
        const int64_t new_version = 2;
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
        add_rowset_with_predicate(&meta_a, 1, 1, false); // data
        add_rowset_with_predicate(&meta_a, 2, 2, true);  // delete predicate

        TabletMetadataPB meta_b;
        meta_b.set_id(tablet_b);
        meta_b.set_version(base_version);
        meta_b.set_next_rowset_id(3);
        add_rowset_with_predicate(&meta_b, 1, 1, false); // data
        add_rowset_with_predicate(&meta_b, 2, 2, true);  // delete predicate
        CHECK_OK(put_merge_sources(meta_a, meta_b));

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
        return lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                               txn_info, false, tablet_metadatas, tablet_ranges);
    };

    set_failpoint_mode("tablet_merge_before_delete_predicate_range", FailPointTriggerModeType::ENABLE);
    auto armed = run_merge();
    set_failpoint_mode("tablet_merge_before_delete_predicate_range", FailPointTriggerModeType::DISABLE);
    EXPECT_FALSE(armed.ok()) << "hook not reached before the delete-predicate range attachment";

    EXPECT_OK(run_merge());
}

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

// The .cols rebuild only runs when two DCG entries claim the SAME column id for the same target
// segment, so this mirrors the two-children-same-column fixture: both children update c1 on one
// shared base segment over disjoint row windows.
TEST_F(LakeTabletMergerTest, test_merge_failpoint_after_write_dcg_cols) {
    constexpr int kNumRows = 100;
    constexpr int kBoundary = 50;
    constexpr uint32_t kSegmentRssid = 1;
    constexpr int64_t kTxnId = 887;

    auto run_merge = [&]() {
        const int64_t base_version = 1;
        const int64_t new_version = 2;
        const int64_t child_a = next_id();
        const int64_t child_b = next_id();
        const int64_t merged_tablet = next_id();

        prepare_tablet_dirs(child_a);
        prepare_tablet_dirs(child_b);
        prepare_tablet_dirs(merged_tablet);

        auto source_value_of = [](int row) { return row * 10; };
        const std::string shared_segment_name = "shared_seg.dat";
        const uint64_t base_segment_size =
                write_two_column_segment(merged_tablet, shared_segment_name, kNumRows, source_value_of);

        auto child_a_update = [](int row) { return row + 100000; };
        auto child_b_update = [](int row) { return row + 200000; };
        const std::string cols_a_name = lake::gen_cols_filename(kTxnId);
        const std::string cols_b_name = lake::gen_cols_filename(kTxnId + 1);
        auto a_cell = [&](int row) { return row < kBoundary ? child_a_update(row) : source_value_of(row); };
        auto b_cell = [&](int row) { return row >= kBoundary ? child_b_update(row) : source_value_of(row); };
        write_c1_only_cols_file(child_a, cols_a_name, kNumRows, a_cell);
        write_c1_only_cols_file(child_b, cols_b_name, kNumRows, b_cell);

        auto build_child = [&](int64_t tablet_id, int lower_key, int upper_key, const std::string& cols_filename) {
            auto metadata = std::make_shared<TabletMetadataPB>();
            metadata->set_id(tablet_id);
            metadata->set_version(base_version);
            metadata->set_next_rowset_id(10);
            const auto [c0_uid, c1_uid] = set_two_column_pk_schema(metadata.get(), 4001);
            (void)c0_uid;

            auto* tablet_range = metadata->mutable_range();
            tablet_range->set_lower_bound_included(true);
            tablet_range->set_upper_bound_included(false);
            *tablet_range->mutable_lower_bound() = generate_sort_key(lower_key);
            *tablet_range->mutable_upper_bound() = generate_sort_key(upper_key);

            auto* rowset = metadata->add_rowsets();
            rowset->set_id(kSegmentRssid);
            rowset->set_version(1);
            rowset->set_num_rows(kNumRows);
            rowset->set_data_size(base_segment_size);
            {
                auto* sm = rowset->add_segment_metas();
                sm->set_filename(shared_segment_name);
                sm->set_size(base_segment_size);
                sm->set_num_rows(kNumRows);
                sm->set_shared(true);
            }
            stamp_physical_identity_uid(rowset, shared_segment_name);
            *rowset->mutable_range()->mutable_lower_bound() = generate_sort_key(lower_key);
            *rowset->mutable_range()->mutable_upper_bound() = generate_sort_key(upper_key);
            rowset->mutable_range()->set_lower_bound_included(true);
            rowset->mutable_range()->set_upper_bound_included(false);
            (*metadata->mutable_rowset_to_schema())[kSegmentRssid] = 4001;

            auto& dcg = (*metadata->mutable_dcg_meta()->mutable_dcgs())[kSegmentRssid];
            dcg.add_column_files(cols_filename);
            dcg.add_unique_column_ids()->add_column_ids(c1_uid);
            dcg.add_versions(1);
            dcg.add_shared_files(false);
            return metadata;
        };

        auto meta_a = build_child(child_a, 0, kBoundary, cols_a_name);
        auto meta_b = build_child(child_b, kBoundary, kNumRows, cols_b_name);
        CHECK_OK(put_merge_sources(meta_a, meta_b));

        ReshardingTabletInfoPB resharding_tablet;
        auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
        merging_tablet.add_old_tablet_ids(child_a);
        merging_tablet.add_old_tablet_ids(child_b);
        merging_tablet.set_new_tablet_id(merged_tablet);

        TxnInfoPB txn_info;
        txn_info.set_txn_id(kTxnId + 2);
        std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
        std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
        return lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                               txn_info, false, tablet_metadatas, tablet_ranges);
    };

    set_failpoint_mode("tablet_merge_after_write_dcg_cols", FailPointTriggerModeType::ENABLE);
    auto armed = run_merge();
    set_failpoint_mode("tablet_merge_after_write_dcg_cols", FailPointTriggerModeType::DISABLE);
    EXPECT_FALSE(armed.ok()) << "hook not reached after the rebuilt .cols segment was written";

    EXPECT_OK(run_merge());
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

} // namespace starrocks
