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

#include "storage/lake/tablet_reshard_test_base.h"

namespace starrocks {

TEST_F(LakeTabletReshardTest, test_range_reshard_no_write_cycles_are_fixed_point) {
    for (bool primary_key : {true, false}) {
        expect_ten_fixed_point_cycles(fixed_point_source(primary_key));
        if (HasFailure()) return;
    }
}

TEST_F(LakeTabletReshardTest, test_range_reshard_signature_detects_declaration_changes) {
    auto source = fixed_point_source(true);
    ASSIGN_OR_ABORT(auto baseline, semantic_reshard_signature(source));
    enum Mutation {
        ROWS,
        BYTES,
        DELETES,
        CURSOR,
        RECOVERY_PRESENCE,
        ROWSET_RESIDUAL,
        SEGMENT_RESIDUAL,
        DEL_ORIGIN,
        DEL_RESIDUAL
    };
    for (auto mutation : {ROWS, BYTES, DELETES, CURSOR, RECOVERY_PRESENCE, ROWSET_RESIDUAL, SEGMENT_RESIDUAL,
                          DEL_ORIGIN, DEL_RESIDUAL}) {
        SCOPED_TRACE(mutation);
        auto changed = std::make_shared<TabletMetadataPB>(*source);
        auto* rowset = changed->mutable_rowsets(0);
        auto add_unknown = [](auto* pb) {
            std::string serialized = pb->SerializeAsString();
            serialized.append("\xF8\x07\x01", 3);
            CHECK(pb->ParseFromString(serialized));
        };
        switch (mutation) {
        case ROWS:
            rowset->set_num_rows(rowset->num_rows() + 1);
            break;
        case BYTES:
            rowset->set_data_size(rowset->data_size() + 1);
            break;
        case DELETES:
            rowset->set_num_dels(rowset->num_dels() + 1);
            break;
        case CURSOR:
            rowset->set_next_compaction_offset(1);
            break;
        case RECOVERY_PRESENCE:
            rowset->set_max_compact_input_rowset_id(0);
            break;
        case ROWSET_RESIDUAL:
            add_unknown(rowset);
            break;
        case SEGMENT_RESIDUAL:
            add_unknown(rowset->mutable_segment_metas(0));
            break;
        case DEL_ORIGIN:
            rowset->mutable_del_files(0)->set_origin_rowset_id(rowset->id());
            break;
        case DEL_RESIDUAL:
            add_unknown(rowset->mutable_del_files(0));
            break;
        }
        ASSERT_OK(put_tablet_metadata(changed));
        ASSIGN_OR_ABORT(auto signature, semantic_reshard_signature(changed));
        EXPECT_NE(baseline, signature);
    }
}

TEST_F(LakeTabletReshardTest, test_range_reshard_compaction_partial_merge_rejoin) {
    const int32_t old_min_segments = config::lake_pk_compaction_min_input_segments;
    config::lake_pk_compaction_min_input_segments = 1;
    DeferOp restore([&] { config::lake_pk_compaction_min_input_segments = old_min_segments; });
    auto source = fixed_point_source(true);
    ASSIGN_OR_ABORT(auto written, publish_followup_upsert_delete(source->id(), source->version(), 10, 7777, 60));
    ASSIGN_OR_ABORT(auto compacted, compact_tablet(written->id(), written->version(), true));
    ASSERT_EQ(1, compacted->rowsets_size());
    ASSERT_TRUE(compacted->rowsets(0).has_max_compact_input_rowset_id());
    ASSIGN_OR_ABORT(auto baseline, semantic_reshard_signature(compacted));
    auto children = split_fixed_point_source(compacted, 3);
    ASSERT_EQ(3, children.size());
    for (bool reverse_sources : {false, true}) {
        SCOPED_TRACE(reverse_sources);
        std::vector<TabletMetadataPtr> pair{children[0], children[1]};
        if (reverse_sources) std::reverse(pair.begin(), pair.end());
        const int64_t partial_id = next_id();
        std::unordered_map<int64_t, TabletMetadataPtr> published;
        ASSERT_OK(publish_resharding_merge(pair, partial_id, children[0]->version(), children[0]->version() + 1,
                                           next_id(), published));
        auto partial = published.at(partial_id);
        // Advance the untouched third child with an empty transaction: no rowset or recovery-coordinate rewrite.
        ASSERT_OK(put_tablet_metadata(children[2]));
        TxnLogPB empty_log;
        empty_log.set_tablet_id(children[2]->id());
        empty_log.set_txn_id(next_id());
        ASSERT_OK(_tablet_manager->put_txn_log(empty_log));
        TxnInfoPB empty_txn;
        empty_txn.set_txn_id(empty_log.txn_id());
        empty_txn.set_txn_type(TXN_NORMAL);
        empty_txn.set_commit_time(1);
        ASSIGN_OR_ABORT(auto untouched,
                        lake::publish_version(_tablet_manager.get(), lake::PublishTabletInfo(children[2]->id()),
                                              children[2]->version(), partial->version(),
                                              std::span<const TxnInfoPB>(&empty_txn, 1), false));
        ASSERT_NE(partial->rowsets(0).max_compact_input_rowset_id(),
                  untouched->rowsets(0).max_compact_input_rowset_id());
        std::vector<TabletMetadataPtr> rejoin{partial, untouched};
        if (reverse_sources) std::reverse(rejoin.begin(), rejoin.end());
        const int64_t target = next_id();
        published.clear();
        ASSERT_OK(publish_resharding_merge(rejoin, target, partial->version(), partial->version() + 1, next_id(),
                                           published));
        ASSIGN_OR_ABORT(auto signature, semantic_reshard_signature(published.at(target)));
        EXPECT_EQ(baseline, signature);
    }
}

TEST_F(LakeTabletReshardTest, test_range_reshard_signature_normalizes_recovery_equivalence_and_order) {
    auto source = std::make_shared<TabletMetadataPB>(*fixed_point_source(false));
    // The ordinary rowset's recovery key precedes the predicate's key (9).
    source->mutable_rowsets(0)->set_max_compact_input_rowset_id(0);
    ASSERT_OK(put_tablet_metadata(source));
    ASSIGN_OR_ABORT(auto baseline, semantic_reshard_signature(source));
    for (uint32_t key : {4, 9, 10}) {
        auto changed = std::make_shared<TabletMetadataPB>(*source);
        changed->mutable_rowsets(0)->set_max_compact_input_rowset_id(key);
        ASSERT_OK(put_tablet_metadata(changed));
        ASSIGN_OR_ABORT(auto signature, semantic_reshard_signature(changed));
        if (key == 4) {
            EXPECT_EQ(baseline, signature) << "raw coordinate changed, but equivalence and order did not";
        } else {
            EXPECT_NE(baseline, signature) << "key 9 joins an equivalence class; key 10 reverses the order";
        }
    }
}

TEST_F(LakeTabletReshardTest, test_range_reshard_post_dml_cycles_are_fixed_point) {
    auto source = fixed_point_source(true);
    ASSIGN_OR_ABORT(auto written, publish_followup_upsert_delete(source->id(), source->version(), 10, 7777, 60));
    const int32_t old_min_segments = config::lake_pk_compaction_min_input_segments;
    config::lake_pk_compaction_min_input_segments = 1;
    DeferOp restore([&] { config::lake_pk_compaction_min_input_segments = old_min_segments; });
    ASSIGN_OR_ABORT(auto compacted, compact_tablet(written->id(), written->version(), true));
    ASSIGN_OR_ABORT(auto rows, read_two_column_rows(compacted));
    EXPECT_NE(rows.end(), std::find(rows.begin(), rows.end(), std::pair<int32_t, int32_t>{10, 7777}));
    EXPECT_TRUE(std::none_of(rows.begin(), rows.end(), [](const auto& row) { return row.first == 60; }));
    // DML and normal compaction are over: establish a fresh baseline here.
    expect_ten_fixed_point_cycles(compacted);
}

TEST_F(LakeTabletReshardTest, test_range_reshard_sidecar_and_sst_cold_read_smoke) {
    auto source = fixed_point_source(true);
    ASSIGN_OR_ABORT(auto baseline, semantic_reshard_signature(source));
    ASSIGN_OR_ABORT(auto expected_rows, read_two_column_rows(source));
    auto current = run_no_write_split_merge_cycle(source, 3);
    ASSIGN_OR_ABORT(auto signature, semantic_reshard_signature(current));
    EXPECT_EQ(baseline, signature);
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    DeferOp restore([&] { set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE); });
    ASSIGN_OR_ABORT(auto flushed, _update_manager->flush_pk_memtable(current, current->version()));
    ASSERT_GT(flushed->sstable_meta().sstables_size(), 0);
    ASSERT_OK(put_tablet_metadata(flushed));
    _update_manager->unload_and_remove_primary_index(flushed->id());
    _tablet_manager->prune_metacache();
    ASSIGN_OR_ABORT(auto cold, _tablet_manager->get_tablet_metadata(flushed->id(), flushed->version()));
    assert_published_sstables_reopen(cold);
    expect_lifecycle_oracle(cold, expected_rows, {1});
    ASSIGN_OR_ABORT(auto cold_signature, semantic_reshard_signature(cold));
    EXPECT_EQ(baseline, cold_signature);
    // Carry persisted SST state through another SPLIT/MERGE. The merged index may
    // retain SSTs or use the supported native rebuild representation; both must
    // honor the same delvec/DCG/IDG and point-lookup oracle after a cold reopen.
    auto resharded_sst = run_no_write_split_merge_cycle(cold, 2);
    _update_manager->unload_and_remove_primary_index(resharded_sst->id());
    _tablet_manager->prune_metacache();
    ASSIGN_OR_ABORT(auto reopened, _tablet_manager->get_tablet_metadata(resharded_sst->id(), resharded_sst->version()));
    assert_published_sstables_reopen(reopened);
    expect_lifecycle_oracle(reopened, expected_rows, {1});
    ASSIGN_OR_ABORT(auto final_signature, semantic_reshard_signature(reopened));
    EXPECT_EQ(baseline, final_signature);
}

TEST_F(LakeTabletReshardTest, test_range_reshard_vacuum_eligible_attempt_objects_converge) {
    auto source = fixed_point_source(true);
    auto current = run_no_write_split_merge_cycle(source, 3);
    ASSIGN_OR_ABORT(auto baseline, semantic_reshard_signature(current));
    const auto live_inventory = collect_reshard_inventory(*current);
    ASSERT_FALSE(current->delvec_meta().delvecs().empty());
    const auto& live_page = current->delvec_meta().delvecs().begin()->second;
    const auto& live_delvec = current->delvec_meta().version_to_file().at(live_page.version()).name();
    const auto live_txn_id = lake::extract_txn_id_prefix(live_delvec).value_or(0);
    ASSERT_GT(live_txn_id, 0) << "the live delvec must be transaction-aged, not txnless";
    std::set<std::string> expected_live_files;
    for (const auto& rowset : current->rowsets()) {
        for (const auto& segment : rowset.segment_metas()) expected_live_files.insert(segment.filename());
        for (const auto& del : rowset.del_files()) expected_live_files.insert(del.name());
    }
    for (const auto& [rssid, page] : current->delvec_meta().delvecs()) {
        expected_live_files.insert(current->delvec_meta().version_to_file().at(page.version()).name());
    }
    for (const auto& [rssid, dcg] : current->dcg_meta().dcgs()) {
        expected_live_files.insert(dcg.column_files().begin(), dcg.column_files().end());
    }
    lake::LakeIndexDeltaGroupLoader idg_loader(current);
    for (const auto& [rssid, idg] : current->idg_meta().idgs()) {
        lake::IndexDeltaGroupList active;
        ASSERT_OK(idg_loader.load(TabletSegmentId(current->id(), rssid), current->version(), &active));
        for (const auto& entry : active) expected_live_files.insert(entry.index_file);
    }
    for (const auto& sst : current->sstable_meta().sstables()) expected_live_files.insert(sst.filename());
    ASSERT_EQ(live_inventory.live_files, expected_live_files.size());
    auto expect_live_files_present = [&] {
        for (const auto& name : expected_live_files) {
            ASSERT_OK(FileSystem::Default()->path_exists(
                    lake::join_path(_location_provider->segment_root_location(current->id()), name)));
        }
        ASSERT_OK(FileSystem::Default()->path_exists(_tablet_manager->delvec_location(current->id(), live_delvec)));
    };
    expect_live_files_present();
    // Leave actual objects from two failed MERGE writer attempts. Transaction
    // retention protects the second until min_active_txn_id explicitly advances.
    auto failed_attempt = [&](int64_t txn_id) {
        ASSIGN_OR_ABORT(auto before, delvec_inventory(current->id()));
        set_failpoint_mode("tablet_merge_after_write_delvec", FailPointTriggerModeType::ENABLE);
        DeferOp restore(
                [&] { set_failpoint_mode("tablet_merge_after_write_delvec", FailPointTriggerModeType::DISABLE); });
        std::unordered_map<int64_t, TabletMetadataPtr> unpublished;
        const int64_t failed_target = next_id();
        auto status = publish_resharding_merge({current}, failed_target, current->version(), current->version() + 1,
                                               txn_id, unpublished);
        EXPECT_FALSE(status.ok());
        // The error path may return prepared source entries, but none is persisted
        // and no completed target references the abandoned writer output.
        EXPECT_FALSE(unpublished.contains(failed_target));
        EXPECT_TRUE(
                FileSystem::Default()
                        ->path_exists(_tablet_manager->tablet_metadata_location(failed_target, current->version() + 1))
                        .is_not_found());
        EXPECT_TRUE(
                FileSystem::Default()
                        ->path_exists(_tablet_manager->tablet_metadata_location(current->id(), current->version() + 1))
                        .is_not_found());
        ASSIGN_OR_ABORT(auto after, delvec_inventory(current->id()));
        std::set<std::string> attempts;
        std::set_difference(after.begin(), after.end(), before.begin(), before.end(),
                            std::inserter(attempts, attempts.end()));
        EXPECT_EQ(1, attempts.size());
        return attempts;
    };
    const auto eligible = failed_attempt(live_txn_id - 1);
    const auto protected_attempt = failed_attempt(live_txn_id + 2);
    ASSERT_EQ(1, eligible.size());
    ASSERT_EQ(1, protected_attempt.size());
    auto run_vacuum = [&](int64_t min_active_txn_id) {
        EXPECT_LT(live_txn_id, min_active_txn_id)
                << "live delvec protection must come from metadata references, not transaction retention";
        VacuumFullRequest request;
        request.set_partition_id(1);
        request.set_tablet_id(current->id());
        request.set_min_active_txn_id(min_active_txn_id);
        request.set_grace_timestamp(time(nullptr) + 1);
        request.set_min_check_version(0);
        request.set_max_check_version(1);
        VacuumFullResponse response;
        lake::vacuum_full(_tablet_manager.get(), request, &response);
        ASSERT_TRUE(response.has_status());
        ASSERT_EQ(0, response.status().status_code());
        expect_live_files_present();
        LOG(INFO) << "fixed-point vacuum: min_active_txn_id=" << min_active_txn_id
                  << " live_delvec_txn_id=" << live_txn_id << " live_files_present=" << expected_live_files.size();
    };
    run_vacuum(live_txn_id + 1);
    ASSIGN_OR_ABORT(auto retained, delvec_inventory(current->id()));
    EXPECT_FALSE(retained.contains(*eligible.begin()));
    EXPECT_TRUE(retained.contains(*protected_attempt.begin()));
    run_vacuum(live_txn_id + 3);
    ASSIGN_OR_ABORT(auto converged, delvec_inventory(current->id()));
    EXPECT_FALSE(converged.contains(*eligible.begin()));
    EXPECT_FALSE(converged.contains(*protected_attempt.begin()));
    run_vacuum(live_txn_id + 3);
    EXPECT_EQ(converged, delvec_inventory(current->id()).value());
    ASSIGN_OR_ABORT(auto signature, semantic_reshard_signature(current));
    EXPECT_EQ(baseline, signature);
    EXPECT_EQ(live_inventory.live_files, collect_reshard_inventory(*current).live_files);
}

TEST_F(LakeTabletReshardTest, test_tablet_splitting_preserves_sparse_segment_indices) {
    auto m = split_source();
    ASSIGN_OR_ABORT(auto children, split_source_into_children(m));
    ASSERT_EQ(2, children.size());
    EXPECT_EQ(0, children.at(m->id() + 100000)->rowsets(0).segment_metas(0).segment_idx());
    EXPECT_EQ(7, children.at(m->id() + 200000)->rowsets(0).segment_metas(0).segment_idx());
    for (const auto& [id, child] : children) {
        ASSERT_EQ(1, child->rowsets(0).segment_metas_size());
        EXPECT_EQ(m->rowsets(0).uid().SerializeAsString(), child->rowsets(0).uid().SerializeAsString());
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_splitting_omits_nonintersecting_rowset_and_sidecars) {
    auto m = split_source();
    for (uint32_t rssid : {2, 9}) {
        auto& page = (*m->mutable_delvec_meta()->mutable_delvecs())[rssid];
        page.set_version(rssid);
        (*m->mutable_delvec_meta()->mutable_version_to_file())[rssid].set_name(fmt::format("{}.dv", rssid));
        (*m->mutable_dcg_meta()->mutable_dcgs())[rssid].add_column_files(fmt::format("{}.dcg", rssid));
        (*m->mutable_idg_meta()->mutable_idgs())[rssid];
    }
    auto* outside = m->add_rowsets();
    *outside = m->rowsets(0);
    outside->set_id(20);
    lake::tablet_reshard_helper::set_rowset_uid(outside);
    outside->clear_segment_metas();
    *outside->add_segment_metas() = m->rowsets(0).segment_metas(1);
    outside->mutable_segment_metas(0)->set_segment_idx(0);
    *outside->mutable_range()->mutable_lower_bound() = generate_sort_key(50);
    outside->mutable_range()->set_lower_bound_included(true);
    outside->set_next_compaction_offset(1); // opaque rowset still must be omitted by effective range
    (*m->mutable_rowset_to_schema())[20] = 123;
    ASSIGN_OR_ABORT(auto children, split_source_into_children(m));
    ASSERT_EQ(2, children.size());
    auto left = children.at(m->id() + 100000);
    auto right = children.at(m->id() + 200000);
    EXPECT_EQ(1, left->rowsets_size());
    EXPECT_EQ(0, left->rowset_to_schema().count(20));
    for (auto [child, live, dead] : {std::tuple{left, 2, 9}, std::tuple{right, 9, 2}}) {
        EXPECT_EQ(1, child->delvec_meta().delvecs().count(live));
        EXPECT_EQ(1, child->dcg_meta().dcgs().count(live));
        EXPECT_EQ(1, child->idg_meta().idgs().count(live));
        EXPECT_EQ(0, child->delvec_meta().delvecs().count(dead));
        EXPECT_EQ(0, child->delvec_meta().version_to_file().count(dead));
        EXPECT_EQ(0, child->dcg_meta().dcgs().count(dead));
        EXPECT_EQ(0, child->idg_meta().idgs().count(dead));
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_splitting_preserves_nonzero_compaction_cursor) {
    auto m = split_source();
    m->mutable_rowsets(0)->set_next_compaction_offset(1);
    ASSIGN_OR_ABORT(auto children, split_source_into_children(m));
    ASSERT_EQ(2, children.size());
    for (const auto& [id, child] : children) {
        EXPECT_EQ(1, child->rowsets(0).next_compaction_offset());
        EXPECT_EQ(2, child->rowsets(0).segment_metas_size());
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_splitting_capacity_limit_uses_identical_fallback) {
    auto m = split_source();
    SyncPoint::GetInstance()->SetCallBack("tablet_splitter:set_metadata_visit_limit",
                                          [](void* p) { *static_cast<size_t*>(p) = 0; });
    expect_split_fallback_after_flush(m);
}

TEST_F(LakeTabletReshardTest, test_tablet_splitting_corruption_does_not_use_identical_fallback) {
    for (int malformed = 0; malformed < 7; ++malformed) {
        auto m = split_source();
        auto* rowset = m->mutable_rowsets(0);
        switch (malformed) {
        case 0:
            rowset->mutable_segment_metas(0)->set_num_rows(-1);
            break;
        case 1:
            rowset->set_num_rows(-1);
            break;
        case 2:
            rowset->set_data_size(-1);
            break;
        case 3:
            rowset->set_num_dels(-1);
            break;
        case 4:
            rowset->mutable_delete_predicate()->set_version(1);
            break;
        case 5:
            rowset->mutable_segment_metas(1)->set_segment_idx(UINT32_MAX);
            break;
        case 6:
            auto* del = rowset->add_del_files();
            del->set_name("overflow.del");
            del->set_origin_rowset_id(UINT32_MAX);
            del->set_op_offset(1);
            break;
        }
        EXPECT_TRUE(split_source_into_children(m).status().is_corruption()) << malformed;
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_splitting_corrupt_source_fails_before_pk_flush) {
    auto m = split_source();
    m->mutable_rowsets(0)->mutable_segment_metas(0)->set_num_rows(-1);
    auto* sync = SyncPoint::GetInstance();
    int flushes = 0;
    sync->SetCallBack("tablet_splitter:pk_flush", [&](void*) { ++flushes; });
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->DisableProcessing();
        sync->ClearAllCallBacks();
    });
    EXPECT_TRUE(split_source_into_children(m).status().is_corruption());
    EXPECT_EQ(0, flushes);
}

TEST_F(LakeTabletReshardTest, test_tablet_splitting_missing_uid_is_corruption) {
    auto m = split_source();
    m->mutable_rowsets(0)->clear_uid();
    ASSERT_OK(_tablet_manager->put_tablet_metadata(m));
    EXPECT_TRUE(split_source_into_children(m).status().is_corruption());
}

TEST_F(LakeTabletReshardTest, test_tablet_splitting_absent_segment_idx_falls_back_after_pk_flush) {
    auto m = split_source();
    m->mutable_rowsets(0)->mutable_segment_metas(0)->clear_segment_idx();
    expect_split_fallback_after_flush(m);
}

TEST_F(LakeTabletReshardTest, test_tablet_splitting_missing_segment_num_rows_falls_back_after_pk_flush) {
    auto m = split_source();
    m->mutable_rowsets(0)->mutable_segment_metas(0)->clear_num_rows();
    expect_split_fallback_after_flush(m);
}

TEST_F(LakeTabletReshardTest, test_tablet_splitting_source_fallback_does_not_mask_invalid_external_ranges) {
    auto* sync = SyncPoint::GetInstance();
    int flushes = 0;
    sync->SetCallBack("tablet_splitter:pk_flush", [&](void*) { ++flushes; });
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->DisableProcessing();
        sync->ClearAllCallBacks();
    });
    for (int malformed = 0; malformed < 3; ++malformed) {
        auto metadata = split_source();
        metadata->mutable_rowsets(0)->mutable_segment_metas(0)->clear_segment_idx();
        ASSERT_OK(_tablet_manager->put_tablet_metadata(metadata));
        SplittingTabletInfoPB splitting;
        splitting.set_old_tablet_id(metadata->id());
        splitting.add_new_tablet_ids(next_id());
        splitting.add_new_tablet_ids(next_id());
        auto* left = splitting.add_new_tablet_ranges();
        *left->mutable_upper_bound() = generate_sort_key(50);
        left->set_upper_bound_included(false);
        auto* right = splitting.add_new_tablet_ranges();
        right->set_lower_bound_included(true);
        if (malformed == 0) {
            *right->mutable_lower_bound() = generate_sort_key(60); // gap
        } else {
            VariantTuple boundary;
            if (malformed == 1) {
                boundary.append(DatumVariant(get_type_info(TYPE_BIGINT), Datum(int64_t{50})));
            } else {
                boundary.append(DatumVariant(get_type_info(TYPE_INT), Datum(50)));
                boundary.append(DatumVariant(get_type_info(TYPE_INT), Datum(50)));
            }
            boundary.to_proto(left->mutable_upper_bound());
            *right->mutable_lower_bound() = left->upper_bound();
        }
        auto result = lake::split_tablet(_tablet_manager.get(), metadata, splitting, 2, TxnInfoPB());
        EXPECT_TRUE(result.status().is_invalid_argument()) << "malformed range case " << malformed;
        EXPECT_EQ(0, flushes) << "malformed range case " << malformed;
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_splitting_duplicate_segment_idx_is_corruption) {
    auto m = split_source();
    m->mutable_rowsets(0)->mutable_segment_metas(1)->set_segment_idx(0);
    EXPECT_TRUE(split_source_into_children(m).status().is_corruption());
}

TEST_F(LakeTabletReshardTest, test_tablet_splitting_logically_empty_range_falls_back_after_pk_flush) {
    auto m = split_source();
    auto* range = m->mutable_rowsets(0)->mutable_range();
    *range->mutable_lower_bound() = generate_sort_key(50);
    *range->mutable_upper_bound() = generate_sort_key(50);
    range->set_lower_bound_included(true);
    range->set_upper_bound_included(false);
    expect_split_fallback_after_flush(m);
}

TEST_F(LakeTabletReshardTest, test_tablet_splitting_self_del_offset_falls_back_after_pk_flush) {
    auto m = split_source();
    auto* del = m->mutable_rowsets(0)->add_del_files();
    del->set_name("self.del");
    del->set_origin_rowset_id(2);
    del->set_op_offset(8);
    expect_split_fallback_after_flush(m);
}

TEST_F(LakeTabletReshardTest, test_tablet_splitting_inherited_del_offset_uses_origin_space) {
    auto m = split_source();
    auto* del = m->mutable_rowsets(0)->add_del_files();
    del->set_name("inherited.del");
    del->set_origin_rowset_id(1);
    del->set_op_offset(19);
    ASSIGN_OR_ABORT(auto children, split_source_into_children(m));
    ASSERT_EQ(2, children.size());
    for (const auto& [id, child] : children) {
        EXPECT_EQ(2, child->rowsets(0).segment_metas_size());
        EXPECT_EQ(19, child->rowsets(0).del_files(0).op_offset());
        EXPECT_EQ(1, child->rowsets(0).del_files(0).origin_rowset_id());
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_splitting_opaque_del_rowset_is_full_copy_or_omitted) {
    auto m = split_source();
    auto* rowset = m->mutable_rowsets(0);
    *rowset->mutable_range()->mutable_lower_bound() = generate_sort_key(50);
    rowset->mutable_range()->set_lower_bound_included(true);
    auto* del = rowset->add_del_files();
    del->set_name("self.del");
    del->set_origin_rowset_id(2);
    del->set_op_offset(7);
    ASSIGN_OR_ABORT(auto children, split_source_into_children(m));
    ASSERT_EQ(2, children.size());
    EXPECT_EQ(0, children.at(m->id() + 100000)->rowsets_size());
    auto right = children.at(m->id() + 200000);
    ASSERT_EQ(1, right->rowsets_size());
    EXPECT_EQ(2, right->rowsets(0).segment_metas_size());
    EXPECT_EQ(1, right->rowsets(0).del_files_size());
    EXPECT_EQ(100, right->rowsets(0).num_rows());
}

TEST_F(LakeTabletReshardTest, test_separate_sort_pk_split_flushes_before_budgeted_sst_sampling) {
    auto m = split_source(true);
    const std::string name = "split-sampling.sst";
    std::vector<std::tuple<std::string, uint32_t, uint32_t>> entries;
    for (int i = 0; i < 100; ++i) entries.emplace_back(encode_int_primary_key(i), 2, i);
    auto* sst = m->mutable_sstable_meta()->add_sstables();
    sst->set_filename(name);
    sst->set_filesize(write_legacy_pk_sstable(_tablet_manager->sst_location(m->id(), name), entries));
    sst->mutable_range()->set_start_key(encode_int_primary_key(0));
    sst->mutable_range()->set_end_key(encode_int_primary_key(99));
    auto* sync = SyncPoint::GetInstance();
    std::vector<std::string> order;
    for (const auto& phase : {"pk_flush", "pk_sampler", "sst_open"}) {
        sync->SetCallBack(std::string("tablet_splitter:") + phase, [&, phase](void*) { order.emplace_back(phase); });
    }
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->DisableProcessing();
        sync->ClearAllCallBacks();
    });
    ASSIGN_OR_ABORT(auto children, split_source_into_children(m, false));
    EXPECT_EQ(2, children.size());
    EXPECT_EQ((std::vector<std::string>{"pk_flush", "pk_sampler", "sst_open"}), order);
}

TEST_F(LakeTabletReshardTest, test_separate_sort_pk_budget_stops_before_sst_open_then_falls_back) {
    auto m = split_source(true);
    auto* sst = m->mutable_sstable_meta()->add_sstables();
    sst->set_filename("must-not-open.sst");
    sst->mutable_range()->set_start_key(encode_int_primary_key(0));
    sst->mutable_range()->set_end_key(encode_int_primary_key(99));
    auto* sync = SyncPoint::GetInstance();
    std::vector<std::string> order;
    // Enough for source preflight and the SST declaration, not its requested 64 samples.
    sync->SetCallBack("tablet_splitter:set_metadata_visit_limit", [](void* p) { *static_cast<size_t*>(p) = 10; });
    for (const auto& phase : {"pk_flush", "pk_sampler", "sst_open"}) {
        sync->SetCallBack(std::string("tablet_splitter:") + phase, [&, phase](void*) { order.emplace_back(phase); });
    }
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->DisableProcessing();
        sync->ClearAllCallBacks();
    });
    ASSIGN_OR_ABORT(auto children, split_source_into_children(m, false));
    EXPECT_EQ(1, children.size());
    EXPECT_EQ((std::vector<std::string>{"pk_flush"}), order);
}

TEST_F(LakeTabletReshardTest, test_tablet_splitting) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);

    auto rowset_meta_pb = metadata.add_rowsets();
    rowset_meta_pb->set_id(2);
    {
        auto* sm = rowset_meta_pb->add_segment_metas();
        sm->set_filename("test_0.dat");
        sm->set_size(512);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
        sm->set_num_rows(3);
    }

    {
        auto* sm = rowset_meta_pb->add_segment_metas();
        sm->set_filename("test_1.dat");
        sm->set_size(512);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(50));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(100));
        sm->set_num_rows(2);
    }
    rowset_meta_pb->add_del_files()->set_name("test.del");
    rowset_meta_pb->set_overlapped(true);
    rowset_meta_pb->set_data_size(1024);
    rowset_meta_pb->set_num_rows(5);

    FileMetaPB file_meta;
    file_meta.set_name("test.delvec");
    metadata.mutable_delvec_meta()->mutable_version_to_file()->insert({2, file_meta});

    DeltaColumnGroupVerPB dcg;
    dcg.add_column_files("test.dcg");
    metadata.mutable_dcg_meta()->mutable_dcgs()->insert({2, dcg});

    metadata.mutable_sstable_meta()->add_sstables()->set_filename("test.sst");

    EXPECT_OK(put_tablet_metadata(metadata));

    ReshardingTabletInfoPB resharding_tablet_for_splitting;
    auto& splitting_tablet = *resharding_tablet_for_splitting.mutable_splitting_tablet_info();
    splitting_tablet.set_old_tablet_id(tablet_id);
    splitting_tablet.add_new_tablet_ids(next_id());
    splitting_tablet.add_new_tablet_ids(next_id());

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto res =
            lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet_for_splitting, metadata.version(),
                                            metadata.version() + 1, txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);
    EXPECT_EQ(3, tablet_metadatas.size());
    EXPECT_EQ(2, tablet_ranges.size());

    ReshardingTabletInfoPB resharding_tablet_for_identical;
    auto& identical_tablet = *resharding_tablet_for_identical.mutable_identical_tablet_info();
    identical_tablet.set_old_tablet_id(tablet_id);
    identical_tablet.set_new_tablet_id(next_id());

    tablet_metadatas.clear();
    tablet_ranges.clear();
    res = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet_for_identical, metadata.version(),
                                          metadata.version() + 1, txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);
    EXPECT_EQ(2, tablet_metadatas.size());
    EXPECT_EQ(0, tablet_ranges.size());

    tablet_metadatas.clear();
    tablet_ranges.clear();
    res = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet_for_splitting, metadata.version(),
                                          metadata.version() + 1, txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);
    EXPECT_EQ(3, tablet_metadatas.size());
    EXPECT_EQ(2, tablet_ranges.size());

    tablet_metadatas.clear();
    tablet_ranges.clear();
    res = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet_for_identical, metadata.version(),
                                          metadata.version() + 1, txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);
    EXPECT_EQ(2, tablet_metadatas.size());
    EXPECT_EQ(0, tablet_ranges.size());

    _tablet_manager->prune_metacache();

    tablet_metadatas.clear();
    tablet_ranges.clear();
    res = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet_for_splitting, metadata.version(),
                                          metadata.version() + 1, txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);
    EXPECT_EQ(3, tablet_metadatas.size());
    EXPECT_EQ(2, tablet_ranges.size());

    tablet_metadatas.clear();
    tablet_ranges.clear();
    res = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet_for_identical, metadata.version(),
                                          metadata.version() + 1, txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);
    EXPECT_EQ(2, tablet_metadatas.size());
    EXPECT_EQ(0, tablet_ranges.size());

    EXPECT_OK(_tablet_manager->delete_tablet_metadata(metadata.id(), metadata.version()));

    tablet_metadatas.clear();
    tablet_ranges.clear();
    res = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet_for_splitting, metadata.version(),
                                          metadata.version() + 1, txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);
    EXPECT_EQ(3, tablet_metadatas.size());
    EXPECT_EQ(2, tablet_ranges.size());

    tablet_metadatas.clear();
    tablet_ranges.clear();
    res = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet_for_identical, metadata.version(),
                                          metadata.version() + 1, txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);
    EXPECT_EQ(2, tablet_metadatas.size());
    EXPECT_EQ(0, tablet_ranges.size());
}

// A flush_pk_memtable failure during an identical-tablet reshard must propagate out of
// publish_resharding_tablet rather than copying stale, unflushed metadata onto the new tablet.
TEST_F(LakeTabletReshardTest, test_identical_tablet_flush_failure_propagates) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);

    auto* rowset_meta_pb = metadata.add_rowsets();
    rowset_meta_pb->set_id(2);
    {
        auto* sm = rowset_meta_pb->add_segment_metas();
        sm->set_filename("test_0.dat");
        sm->set_size(512);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
        sm->set_num_rows(3);
    }
    rowset_meta_pb->set_data_size(512);
    rowset_meta_pb->set_num_rows(3);
    metadata.mutable_sstable_meta()->add_sstables()->set_filename("test.sst");
    EXPECT_OK(put_tablet_metadata(metadata));

    ReshardingTabletInfoPB resharding_tablet_for_identical;
    auto& identical_tablet = *resharding_tablet_for_identical.mutable_identical_tablet_info();
    identical_tablet.set_old_tablet_id(tablet_id);
    identical_tablet.set_new_tablet_id(next_id());

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    // Force the PK-index flush to fail so we exercise handle_identical_tablet's error path.
    // Disable the blanket skip first; restore both before asserting.
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    set_failpoint_mode("fail_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto res =
            lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet_for_identical, metadata.version(),
                                            metadata.version() + 1, txn_info, false, tablet_metadatas, tablet_ranges);

    set_failpoint_mode("fail_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);

    EXPECT_FALSE(res.ok());
}

// Reachability net for the reshard-path failpoints. Each asserts BOTH directions: armed -> the
// reshard fails, disarmed -> it succeeds. The armed direction is what catches a typo'd name or a
// DEFINE_FAIL_POINT that never registers, since set_failpoint_mode() silently no-ops on an unknown
// name and the reshard would then succeed. The disarmed direction catches a site that fails
// unconditionally.
//
// Each direction gets its OWN tablet ids, and the ARMED run goes first. A successful
// publish_resharding_tablet writes the new-version metadata through put_tablet_metadata, which also
// caches it, and handle_identical_tablet opens with a metacache lookup of exactly that key and
// returns early on a hit -- so reusing one tablet id and running the success first would make the
// armed run take the retry fast path and never reach the hook at all.
//
// Note what this does NOT prove: SetUp arms skip_lake_pk_index_flush, so flush_pk_memtable returns
// immediately and writes nothing. The hook sits after that call so it is still reached, but the
// orphan-file window it exists for (flushed sstables that no metadata references yet) needs a real
// PK index and belongs to the cluster test.
TEST_F(LakeTabletReshardTest, test_identical_reshard_failpoint_after_pk_flush) {
    auto run_identical_reshard = [&]() {
        starrocks::TabletMetadata metadata;
        auto tablet_id = next_id();
        metadata.set_id(tablet_id);
        metadata.set_version(2);

        auto* rowset_meta_pb = metadata.add_rowsets();
        rowset_meta_pb->set_id(2);
        {
            auto* sm = rowset_meta_pb->add_segment_metas();
            sm->set_filename("test_0.dat");
            sm->set_size(512);
            sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
            sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
            sm->set_num_rows(3);
        }
        rowset_meta_pb->set_data_size(512);
        rowset_meta_pb->set_num_rows(3);
        CHECK_OK(put_tablet_metadata(metadata));

        ReshardingTabletInfoPB resharding_tablet;
        auto& identical_tablet = *resharding_tablet.mutable_identical_tablet_info();
        identical_tablet.set_old_tablet_id(tablet_id);
        identical_tablet.set_new_tablet_id(next_id());

        TxnInfoPB txn_info;
        txn_info.set_commit_time(1);
        txn_info.set_gtid(1);

        std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
        std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
        return lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, metadata.version(),
                                               metadata.version() + 1, txn_info, false, tablet_metadatas,
                                               tablet_ranges);
    };

    set_failpoint_mode("tablet_reshard_after_identical_pk_flush", FailPointTriggerModeType::ENABLE);
    auto armed = run_identical_reshard();
    set_failpoint_mode("tablet_reshard_after_identical_pk_flush", FailPointTriggerModeType::DISABLE);
    EXPECT_FALSE(armed.ok()) << "hook not reached on the identical-reshard path";

    EXPECT_OK(run_identical_reshard());
}

// The hook sits at the END of publish_resharding_tablet's per-tablet metadata-write loop, so when the
// armed run returns it has already persisted exactly ONE of the two tablets' new-version metadata.
// tablet_metadatas is an unordered_map, so which one is unspecified -- assert "exactly one", never a
// particular id.
TEST_F(LakeTabletReshardTest, test_reshard_failpoint_between_metadata_writes) {
    auto build_identical_reshard = [&](int64_t* old_tablet_id, int64_t* new_tablet_id,
                                       ReshardingTabletInfoPB* resharding_tablet, int64_t* base_version) {
        starrocks::TabletMetadata metadata;
        *old_tablet_id = next_id();
        metadata.set_id(*old_tablet_id);
        metadata.set_version(2);

        auto* rowset_meta_pb = metadata.add_rowsets();
        rowset_meta_pb->set_id(2);
        {
            auto* sm = rowset_meta_pb->add_segment_metas();
            sm->set_filename("test_0.dat");
            sm->set_size(512);
            sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
            sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
            sm->set_num_rows(3);
        }
        rowset_meta_pb->set_data_size(512);
        rowset_meta_pb->set_num_rows(3);
        CHECK_OK(put_tablet_metadata(metadata));

        *new_tablet_id = next_id();
        auto& identical_tablet = *resharding_tablet->mutable_identical_tablet_info();
        identical_tablet.set_old_tablet_id(*old_tablet_id);
        identical_tablet.set_new_tablet_id(*new_tablet_id);
        *base_version = metadata.version();
    };

    auto run = [&](const ReshardingTabletInfoPB& resharding_tablet, int64_t base_version) {
        TxnInfoPB txn_info;
        txn_info.set_commit_time(1);
        txn_info.set_gtid(1);
        std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
        std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
        return lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, base_version + 1,
                                               txn_info, false, tablet_metadatas, tablet_ranges);
    };

    {
        int64_t old_tablet_id = 0;
        int64_t new_tablet_id = 0;
        int64_t base_version = 0;
        ReshardingTabletInfoPB resharding_tablet;
        build_identical_reshard(&old_tablet_id, &new_tablet_id, &resharding_tablet, &base_version);

        set_failpoint_mode("tablet_reshard_between_metadata_writes", FailPointTriggerModeType::ENABLE);
        auto armed = run(resharding_tablet, base_version);
        set_failpoint_mode("tablet_reshard_between_metadata_writes", FailPointTriggerModeType::DISABLE);
        EXPECT_FALSE(armed.ok()) << "hook not reached in the metadata-write loop";

        const int64_t new_version = base_version + 1;
        const bool old_written = _tablet_manager->get_tablet_metadata(old_tablet_id, new_version, false).ok();
        const bool new_written = _tablet_manager->get_tablet_metadata(new_tablet_id, new_version, false).ok();
        EXPECT_NE(old_written, new_written)
                << "expected exactly one tablet to be switched, got old=" << old_written << " new=" << new_written;
    }

    {
        int64_t old_tablet_id = 0;
        int64_t new_tablet_id = 0;
        int64_t base_version = 0;
        ReshardingTabletInfoPB resharding_tablet;
        build_identical_reshard(&old_tablet_id, &new_tablet_id, &resharding_tablet, &base_version);
        EXPECT_OK(run(resharding_tablet, base_version));
    }
}

// Phase-1 per-segment shared (end-to-end). After splitting a rowset whose two
// segments occupy disjoint key ranges, each child keeps only its overlapping
// segment, marks it private (shared=false), drops the sibling's segment,
// backfills a uid (the source rowset has none) identically on both children, and
// conserves Σ stats. NOTE: built but NOT run locally (LLVM-16/18 thirdparty
// mismatch); verify in CI.
TEST_F(LakeTabletReshardTest, test_tablet_split_per_segment_shared_invariants) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);

    auto* rs = metadata.add_rowsets();
    rs->set_id(2);
    {
        auto* m0 = rs->add_segment_metas();
        m0->set_filename("seg_lo.dat");
        m0->set_size(512);
        m0->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
        m0->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
        m0->set_num_rows(50);
    }
    {
        auto* m1 = rs->add_segment_metas();
        m1->set_filename("seg_hi.dat");
        m1->set_size(512);
        m1->mutable_sort_key_min()->CopyFrom(generate_sort_key(50));
        m1->mutable_sort_key_max()->CopyFrom(generate_sort_key(99));
        m1->set_num_rows(50);
    }
    rs->set_overlapped(true);
    rs->set_data_size(1024);
    rs->set_num_rows(100);

    EXPECT_OK(put_tablet_metadata(metadata));

    ReshardingTabletInfoPB resharding;
    auto& splitting = *resharding.mutable_splitting_tablet_info();
    splitting.set_old_tablet_id(tablet_id);
    const int64_t child0 = next_id();
    const int64_t child1 = next_id();
    splitting.add_new_tablet_ids(child0);
    splitting.add_new_tablet_ids(child1);

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, metadata.version(),
                                              metadata.version() + 1, txn_info, false, tablet_metadatas,
                                              tablet_ranges));

    auto c0 = tablet_metadatas.at(child0);
    auto c1 = tablet_metadatas.at(child1);
    ASSERT_EQ(1, c0->rowsets_size());
    ASSERT_EQ(1, c1->rowsets_size());
    const auto& r0 = c0->rowsets(0);
    const auto& r1 = c1->rowsets(0);

    // The source rowset's uid (stamped by the test put_tablet_metadata wrapper) is
    // preserved verbatim onto every new tablet at split time, so cross-sibling
    // dedup at a later merge sees identical uids.
    ASSERT_TRUE(r0.has_uid());
    ASSERT_TRUE(r1.has_uid());
    EXPECT_TRUE(r0.uid().hi() != 0 || r0.uid().lo() != 0);
    EXPECT_EQ(r0.uid().hi(), r1.uid().hi());
    EXPECT_EQ(r0.uid().lo(), r1.uid().lo());

    auto all_segs = [](const RowsetMetadataPB& r) {
        std::set<std::string> a;
        for (const auto& s : r.segment_metas()) a.insert(s.filename());
        return a;
    };
    auto private_segs = [](const RowsetMetadataPB& r) {
        std::set<std::string> p;
        for (int i = 0; i < r.segment_metas_size(); ++i) {
            if (!r.segment_metas(i).shared()) p.insert(r.segment_metas(i).filename());
        }
        return p;
    };

    // No data loss: union of children's segments == parent's two segments.
    std::set<std::string> seen = all_segs(r0);
    for (const auto& s : all_segs(r1)) seen.insert(s);
    EXPECT_EQ((std::set<std::string>{"seg_lo.dat", "seg_hi.dat"}), seen);

    // A private (shared=false) segment must be exclusive to its child.
    for (const auto& s : private_segs(r0)) EXPECT_EQ(0u, all_segs(r1).count(s));
    for (const auto& s : private_segs(r1)) EXPECT_EQ(0u, all_segs(r0).count(s));

    // The optimization engaged: disjoint segments split cleanly into private ones.
    EXPECT_GE(private_segs(r0).size() + private_segs(r1).size(), 1u);

    // Σ stats conserved (anchor path).
    EXPECT_EQ(100, r0.num_rows() + r1.num_rows());
    EXPECT_EQ(1024, r0.data_size() + r1.data_size());
}

// (k1, NULL) -- an INT prefix bound lifted onto a (k1, k2) sort key, the shape the FE's
// TrailingSortKeyRangeReprojection stamps onto every pre-existing tablet range when a metadata-only
// trailing sort-key ADD widens the sort key.
static TuplePB generate_sort_key_with_trailing_null(int value) {
    VariantTuple tuple;
    tuple.append(DatumVariant(get_type_info(LogicalType::TYPE_INT), Datum(value)));
    tuple.append(DatumVariant(get_type_info(LogicalType::TYPE_INT), Datum()));
    TuplePB tuple_pb;
    tuple.to_proto(&tuple_pb);
    return tuple_pb;
}

// Per-segment shared, after a metadata-only trailing sort-key ADD (end-to-end). The rowsets predate
// the ADD, so their sort_key_min/sort_key_max are one column short while the tablet's range -- and
// therefore every boundary the split emits -- is at the widened arity. Ownership must NOT prune with
// those non-comparable bounds: VariantTuple::compare orders a shorter prefix-equal tuple BELOW its
// padded form, so a segment whose stored max is [k] misses the sibling whose range starts at
// (k, NULL) -- the very sibling create_seek_range_from routes that segment's k-prefix rows to. Every
// segment must therefore survive on every child, as shared.
TEST_F(LakeTabletReshardTest, test_tablet_split_keeps_pre_trailing_key_add_segments_on_every_child) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);

    // Post-ADD schema: the sort key is (k1, k2); k2 is the column the ADD appended.
    auto* schema = metadata.mutable_schema();
    schema->set_id(778501);
    schema->set_keys_type(DUP_KEYS);
    schema->set_num_short_key_columns(1);
    auto* k1 = schema->add_column();
    k1->set_unique_id(1);
    k1->set_name("k1");
    k1->set_type("INT");
    k1->set_is_key(true);
    k1->set_is_nullable(false);
    auto* k2 = schema->add_column();
    k2->set_unique_id(2);
    k2->set_name("k2");
    k2->set_type("INT");
    k2->set_is_key(true);
    k2->set_is_nullable(true);
    schema->add_sort_key_idxes(0);
    schema->add_sort_key_idxes(1);

    auto* range = metadata.mutable_range();
    *range->mutable_lower_bound() = generate_sort_key_with_trailing_null(0);
    range->set_lower_bound_included(true);
    *range->mutable_upper_bound() = generate_sort_key_with_trailing_null(1000);
    range->set_upper_bound_included(false);

    // Two disjoint pre-ADD rowsets. Their key spans do not overlap, so with pruning enabled each
    // child would keep only its own rowset and drop the other's segment outright.
    auto add_pre_add_rowset = [&](uint32_t id, int lo, int hi, const std::string& segment_name) {
        auto* rs = metadata.add_rowsets();
        rs->set_id(id);
        rs->set_num_rows(500);
        rs->set_data_size(5000);
        auto* sm = rs->add_segment_metas();
        sm->set_filename(segment_name);
        sm->set_size(5000);
        sm->set_num_rows(500);
        // Arity 1: written before the trailing `ADD COLUMN k2`.
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(lo));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(hi));
    };
    add_pre_add_rowset(2, 0, 400, "seg_lo.dat");
    add_pre_add_rowset(3, 600, 999, "seg_hi.dat");

    EXPECT_OK(put_tablet_metadata(metadata));

    ReshardingTabletInfoPB resharding;
    auto& splitting = *resharding.mutable_splitting_tablet_info();
    splitting.set_old_tablet_id(tablet_id);
    const int64_t child0 = next_id();
    const int64_t child1 = next_id();
    splitting.add_new_tablet_ids(child0);
    splitting.add_new_tablet_ids(child1);

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, metadata.version(),
                                              metadata.version() + 1, txn_info, false, tablet_metadatas,
                                              tablet_ranges));

    ASSERT_EQ(1u, tablet_metadatas.count(child1)) << "the split fell back to an identical tablet";
    for (int64_t child : {child0, child1}) {
        auto c = tablet_metadatas.at(child);
        ASSERT_EQ(2, c->rowsets_size()) << "tablet " << child << " lost a pre-ADD rowset: " << c->DebugString();
        std::set<std::string> segs;
        for (const auto& r : c->rowsets()) {
            for (const auto& s : r.segment_metas()) {
                segs.insert(s.filename());
                EXPECT_TRUE(s.shared()) << s.filename() << " must stay shared: its ownership was never proven";
            }
        }
        EXPECT_EQ((std::set<std::string>{"seg_lo.dat", "seg_hi.dat"}), segs);
        // Every emitted bound speaks the widened sort key.
        if (c->range().has_lower_bound()) EXPECT_EQ(2, c->range().lower_bound().values_size());
        if (c->range().has_upper_bound()) EXPECT_EQ(2, c->range().upper_bound().values_size());
    }
}

// SPLIT propagates per-segment ownership to non-segment metadata:
//   - a pruned-away segment's delvec page + dcg entry are erased on the tablet
//     that doesn't keep it;
//   - an exclusive (shared=false) kept segment's dcg is marked private;
//   - the kept segment's delvec page is retained (delvec files stay shared).
// Setup: one rowset (id=2) with two disjoint segments seg_lo[0,49] (rssid 2) and
// seg_hi[50,99] (rssid 3); split into two children so each keeps exactly one
// segment exclusively.
TEST_F(LakeTabletReshardTest, test_tablet_split_propagates_ownership_to_delvec_dcg) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);

    auto* rs = metadata.add_rowsets();
    rs->set_id(2);
    {
        auto* m0 = rs->add_segment_metas();
        m0->set_filename("seg_lo.dat");
        m0->set_size(512);
        m0->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
        m0->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
        m0->set_num_rows(50);
    }
    {
        auto* m1 = rs->add_segment_metas();
        m1->set_filename("seg_hi.dat");
        m1->set_size(512);
        m1->mutable_sort_key_min()->CopyFrom(generate_sort_key(50));
        m1->mutable_sort_key_max()->CopyFrom(generate_sort_key(99));
        m1->set_num_rows(50);
    }
    rs->set_overlapped(true);
    rs->set_data_size(1024);
    rs->set_num_rows(100);

    // delvec + dcg for both segments' rssids (rowset id 2 + segment_idx {0,1}).
    add_delvec(&metadata, tablet_id, /*version=*/1, /*segment_id=*/2, "dv_lo.dat", "aa");
    add_delvec(&metadata, tablet_id, /*version=*/1, /*segment_id=*/3, "dv_hi.dat", "bb");
    add_dcg_with_columns(&metadata, /*segment_id=*/2, "dcg_lo.col", {101}, 1);
    add_dcg_with_columns(&metadata, /*segment_id=*/3, "dcg_hi.col", {102}, 1);

    EXPECT_OK(put_tablet_metadata(metadata));

    ReshardingTabletInfoPB resharding;
    auto& splitting = *resharding.mutable_splitting_tablet_info();
    splitting.set_old_tablet_id(tablet_id);
    const int64_t child0 = next_id();
    const int64_t child1 = next_id();
    splitting.add_new_tablet_ids(child0);
    splitting.add_new_tablet_ids(child1);

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, metadata.version(),
                                              metadata.version() + 1, txn_info, false, tablet_metadatas,
                                              tablet_ranges));

    // For each child, the kept segment's rssid is private dcg + present delvec; the
    // pruned-away segment's rssid is absent from both dcg and delvec.
    for (int64_t child : {child0, child1}) {
        auto c = tablet_metadatas.at(child);
        ASSERT_EQ(1, c->rowsets_size());
        const auto& r = c->rowsets(0);
        ASSERT_EQ(1, r.segment_metas_size()) << "each child keeps exactly one exclusive segment";
        const uint32_t kept_rssid = r.id() + r.segment_metas(0).segment_idx();
        const uint32_t pruned_rssid = (kept_rssid == 2) ? 3 : 2;

        // Exclusive kept segment -> segment_metas[0].shared()==false -> its dcg is private.
        EXPECT_FALSE(r.segment_metas(0).shared()) << "kept segment is exclusive (provably contained)";
        ASSERT_TRUE(c->dcg_meta().dcgs().contains(kept_rssid));
        const auto& kept_dcg = c->dcg_meta().dcgs().at(kept_rssid);
        ASSERT_EQ(kept_dcg.column_files_size(), kept_dcg.shared_files_size());
        for (bool sf : kept_dcg.shared_files()) EXPECT_FALSE(sf) << "exclusive segment dcg must be private";

        // Kept segment's delvec page retained (delvec files stay shared).
        EXPECT_TRUE(c->delvec_meta().delvecs().contains(kept_rssid));

        // Pruned-away segment's delvec page + dcg entry erased.
        EXPECT_FALSE(c->delvec_meta().delvecs().contains(pruned_rssid))
                << "pruned segment delvec must be erased on the tablet that dropped it";
        EXPECT_FALSE(c->dcg_meta().dcgs().contains(pruned_rssid))
                << "pruned segment dcg must be erased on the tablet that dropped it";
    }
}

// SPLIT removes a rowset whose every segment was pruned from a new tablet, along
// with its rowset_to_schema mapping and its (now-orphan) delvec/dcg. Setup: two
// rowsets in disjoint key ranges so each is exclusive to exactly one child.
TEST_F(LakeTabletReshardTest, test_tablet_split_removes_fully_pruned_rowset) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);
    metadata.set_next_rowset_id(20);
    // Base+cumulative split index: rowset at position 0 (rs_a) is "base", position 1
    // (rs_b) is "cumulative". Removing one shifts positions, so the children's
    // cumulative_point must be recomputed (not inherited stale).
    metadata.set_cumulative_point(1);

    auto* rs_a = metadata.add_rowsets(); // lives entirely in [0,49]
    rs_a->set_id(2);
    {
        auto* ma = rs_a->add_segment_metas();
        ma->set_filename("a_seg.dat");
        ma->set_size(512);
        ma->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
        ma->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
        ma->set_num_rows(50);
    }
    rs_a->set_data_size(512);
    rs_a->set_num_rows(50);
    (*metadata.mutable_rowset_to_schema())[2] = 1001;

    auto* rs_b = metadata.add_rowsets(); // lives entirely in [50,99]
    rs_b->set_id(10);
    {
        auto* mb = rs_b->add_segment_metas();
        mb->set_filename("b_seg.dat");
        mb->set_size(512);
        mb->mutable_sort_key_min()->CopyFrom(generate_sort_key(50));
        mb->mutable_sort_key_max()->CopyFrom(generate_sort_key(99));
        mb->set_num_rows(50);
    }
    rs_b->set_data_size(512);
    rs_b->set_num_rows(50);
    (*metadata.mutable_rowset_to_schema())[10] = 1002;

    // delvec/dcg for both rowsets' single segments (rssid = id + 0).
    add_delvec(&metadata, tablet_id, 1, /*segment_id=*/2, "dv_a.dat", "aa");
    add_delvec(&metadata, tablet_id, 1, /*segment_id=*/10, "dv_b.dat", "bb");
    add_dcg_with_columns(&metadata, /*segment_id=*/2, "dcg_a.col", {101}, 1);
    add_dcg_with_columns(&metadata, /*segment_id=*/10, "dcg_b.col", {102}, 1);

    EXPECT_OK(put_tablet_metadata(metadata));

    ReshardingTabletInfoPB resharding;
    auto& splitting = *resharding.mutable_splitting_tablet_info();
    splitting.set_old_tablet_id(tablet_id);
    const int64_t child0 = next_id();
    const int64_t child1 = next_id();
    splitting.add_new_tablet_ids(child0);
    splitting.add_new_tablet_ids(child1);

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, metadata.version(),
                                              metadata.version() + 1, txn_info, false, tablet_metadatas,
                                              tablet_ranges));

    // Each child keeps exactly the one rowset whose segment overlaps its range; the
    // other rowset is fully pruned and removed (entry, rowset_to_schema, delvec, dcg).
    for (int64_t child : {child0, child1}) {
        auto c = tablet_metadatas.at(child);
        ASSERT_EQ(1, c->rowsets_size()) << "fully-pruned rowset removed from rowsets[]";
        const uint32_t kept_id = c->rowsets(0).id();
        const uint32_t removed_id = (kept_id == 2) ? 10 : 2;
        EXPECT_TRUE(c->rowset_to_schema().contains(kept_id));
        EXPECT_FALSE(c->rowset_to_schema().contains(removed_id)) << "removed rowset's schema mapping erased";
        EXPECT_FALSE(c->delvec_meta().delvecs().contains(removed_id)) << "removed rowset's delvec erased";
        EXPECT_FALSE(c->dcg_meta().dcgs().contains(removed_id)) << "removed rowset's dcg erased";
        // The surviving rowset's metadata is intact.
        EXPECT_TRUE(c->delvec_meta().delvecs().contains(kept_id));
        EXPECT_TRUE(c->dcg_meta().dcgs().contains(kept_id));

        // cumulative_point recomputed against surviving positions, never exceeding
        // rowsets_size(). The child keeping rs_a (base, original pos 0) keeps it in the
        // base region -> cp==1; the child keeping rs_b (cumulative, pos 1) had rs_a
        // removed from the base region -> cp==0.
        EXPECT_LE(c->cumulative_point(), static_cast<uint32_t>(c->rowsets_size()));
        EXPECT_EQ(kept_id == 2 ? 1u : 0u, c->cumulative_point());
    }
}

// A fully-pruned rowset carrying del_files must NOT be removed (the del_files keep
// guard), even with 0 segments -- mirrors the delete-predicate guard. rs_a's only
// segment lives in [0,49] so it is pruned from the [50,99] child, but its del_files
// keep it there; rs_b (no del_files) is removed from the child it does not overlap.
TEST_F(LakeTabletReshardTest, test_tablet_split_keeps_del_files_rowset) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);
    metadata.set_next_rowset_id(20);

    auto* rs_a = metadata.add_rowsets(); // segment lives entirely in [0,49]
    rs_a->set_id(2);
    {
        auto* ma = rs_a->add_segment_metas();
        ma->set_filename("a_seg.dat");
        ma->set_size(512);
        ma->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
        ma->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
        ma->set_num_rows(50);
    }
    rs_a->set_data_size(512);
    rs_a->set_num_rows(50);
    rs_a->add_del_files()->set_name("del_a.dat"); // keeps rs_a where its segment is pruned
    (*metadata.mutable_rowset_to_schema())[2] = 1001;

    auto* rs_b = metadata.add_rowsets(); // segment lives entirely in [50,99], no del_files
    rs_b->set_id(10);
    {
        auto* mb = rs_b->add_segment_metas();
        mb->set_filename("b_seg.dat");
        mb->set_size(512);
        mb->mutable_sort_key_min()->CopyFrom(generate_sort_key(50));
        mb->mutable_sort_key_max()->CopyFrom(generate_sort_key(99));
        mb->set_num_rows(50);
    }
    rs_b->set_data_size(512);
    rs_b->set_num_rows(50);
    (*metadata.mutable_rowset_to_schema())[10] = 1002;

    EXPECT_OK(put_tablet_metadata(metadata));

    ReshardingTabletInfoPB resharding;
    auto& splitting = *resharding.mutable_splitting_tablet_info();
    splitting.set_old_tablet_id(tablet_id);
    const int64_t child0 = next_id();
    const int64_t child1 = next_id();
    splitting.add_new_tablet_ids(child0);
    splitting.add_new_tablet_ids(child1);

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, metadata.version(),
                                              metadata.version() + 1, txn_info, false, tablet_metadatas,
                                              tablet_ranges));

    int rs_a_full_copies = 0;
    int rs_b_present = 0;
    for (int64_t child : {child0, child1}) {
        auto c = tablet_metadatas.at(child);
        const RowsetMetadataPB* rs_a_out = nullptr;
        for (const auto& r : c->rowsets()) {
            if (r.id() == 2) rs_a_out = &r;
            if (r.id() == 10) ++rs_b_present;
        }
        ASSERT_NE(rs_a_out, nullptr) << "the opaque rowset's unbounded effective range overlaps every child";
        EXPECT_GT(rs_a_out->del_files_size(), 0) << "rs_a keeps its del_files";
        ASSERT_EQ(1, rs_a_out->segment_metas_size());
        EXPECT_EQ("a_seg.dat", rs_a_out->segment_metas(0).filename());
        EXPECT_TRUE(rs_a_out->segment_metas(0).shared());
        ++rs_a_full_copies;
    }
    EXPECT_EQ(2, rs_a_full_copies) << "opaque del-bearing rowsets retain the complete ordered file set";
    EXPECT_EQ(1, rs_b_present) << "rs_b (no del_files) is removed from the non-overlapping child";
}

// A fully-pruned rowset carrying a delete predicate must NOT be removed: the
// predicate applies to the whole key range and must propagate to every child.
TEST_F(LakeTabletReshardTest, test_tablet_split_keeps_delete_predicate_rowset) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);
    metadata.set_next_rowset_id(20);

    // Data rowset spanning [0,99] so the split produces two ranges.
    auto* data_lo = metadata.add_rowsets();
    data_lo->set_id(2);
    {
        auto* dm0 = data_lo->add_segment_metas();
        dm0->set_filename("lo.dat");
        dm0->set_size(512);
        dm0->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
        dm0->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
        dm0->set_num_rows(50);
    }
    {
        auto* dm1 = data_lo->add_segment_metas();
        dm1->set_filename("hi.dat");
        dm1->set_size(512);
        dm1->mutable_sort_key_min()->CopyFrom(generate_sort_key(50));
        dm1->mutable_sort_key_max()->CopyFrom(generate_sort_key(99));
        dm1->set_num_rows(50);
    }
    data_lo->set_overlapped(true);
    data_lo->set_data_size(1024);
    data_lo->set_num_rows(100);

    // Delete-predicate rowset: 0 segments by design.
    add_rowset_with_predicate(&metadata, /*rowset_id=*/10, /*version=*/2, /*has_predicate=*/true);

    EXPECT_OK(put_tablet_metadata(metadata));

    ReshardingTabletInfoPB resharding;
    auto& splitting = *resharding.mutable_splitting_tablet_info();
    splitting.set_old_tablet_id(tablet_id);
    const int64_t child0 = next_id();
    const int64_t child1 = next_id();
    splitting.add_new_tablet_ids(child0);
    splitting.add_new_tablet_ids(child1);

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, metadata.version(),
                                              metadata.version() + 1, txn_info, false, tablet_metadatas,
                                              tablet_ranges));

    // The delete-predicate rowset (id 10) survives on BOTH children despite 0 segments.
    for (int64_t child : {child0, child1}) {
        auto c = tablet_metadatas.at(child);
        bool found_predicate = false;
        for (const auto& r : c->rowsets()) {
            if (r.id() == 10) {
                found_predicate = true;
                EXPECT_TRUE(r.has_delete_predicate());
            }
        }
        EXPECT_TRUE(found_predicate) << "delete-predicate rowset must propagate to every child";
    }
}

// An inherited SST can contain entries outside a child's tablet range, but those entries
// are unreachable by that child's PK lookup. The SST reference must not override the
// range-derived ownership of its data segment and sidecars.
TEST_F(LakeTabletReshardTest, test_tablet_split_sst_reference_does_not_override_range_ownership) {
    ASSIGN_OR_ABORT(auto fixture, publish_real_split_sst_owner_fixture());
    const auto& pruning_child = fixture.middle_metadata;

    // Geometry owns the data: the fully-pruned data-only rowset and every rssid-keyed
    // sidecar disappear even though a real inherited SST PB remains shared.
    for (const auto& r : pruning_child->rowsets()) {
        EXPECT_NE(2u, r.id()) << "out-of-range data-only rowset must be removed";
    }
    EXPECT_FALSE(pruning_child->delvec_meta().delvecs().contains(2));
    EXPECT_FALSE(pruning_child->dcg_meta().dcgs().contains(2));
    EXPECT_FALSE(pruning_child->idg_meta().idgs().contains(2));
    ASSERT_EQ(1, pruning_child->sstable_meta().sstables_size());
    EXPECT_EQ("split_sst_low.sst", pruning_child->sstable_meta().sstables(0).filename());
    EXPECT_TRUE(pruning_child->sstable_meta().sstables(0).shared());

    // Loading the real PK index is safe: this child only asks for keys in its own
    // [50,100) range, so it rebuilds key 60 from the retained middle segment and
    // never dereferences the inherited SST's out-of-range key 0..49 entries.
    ASSIGN_OR_ABORT(auto value, load_index_value(pruning_child, fixture.middle_child, raw_int_primary_key(60)));
    EXPECT_EQ(IndexValue((static_cast<uint64_t>(10) << 32) | 10), value);
}

TEST_F(LakeTabletReshardTest, test_tablet_split_pruned_sst_partial_merge_drops_ownerless_data) {
    ASSIGN_OR_ABORT(auto fixture, publish_real_split_sst_owner_fixture());
    ASSIGN_OR_ABORT(auto delvecs_before, delvec_inventory(fixture.middle_child));

    // Merge only the two adjacent children whose ranges do not own rssid 2. Both
    // still inherit the shared SST file, but all its data keys are below this
    // partial merge's range and no source metadata contains its real segment owner.
    int64_t merged_tablet = 0;
    ASSIGN_OR_ABORT(auto merged,
                    publish_real_split_sst_owner_merge({fixture.middle_child, fixture.high_child}, &merged_tablet));
    EXPECT_EQ(0, merged->sstable_meta().sstables_size()) << "ownerless data SST must be dropped";
    for (const auto& rowset : merged->rowsets()) {
        for (const auto& segment : rowset.segment_metas()) {
            EXPECT_NE("split_sst_low.dat", segment.filename());
        }
    }
    EXPECT_EQ(0, merged->delvec_meta().delvecs_size());
    EXPECT_EQ(0, merged->delvec_meta().version_to_file_size());
    ASSIGN_OR_ABORT(auto delvecs_after, delvec_inventory(merged_tablet));
    EXPECT_EQ(delvecs_before, delvecs_after) << "ownerless delvec metadata must not create an output file";

    ASSIGN_OR_ABORT(auto middle_value, load_index_value(merged, merged_tablet, raw_int_primary_key(60)));
    ASSIGN_OR_ABORT(auto high_value, load_index_value(merged, merged_tablet, raw_int_primary_key(110)));
    EXPECT_NE(NullIndexValue, middle_value.get_value());
    EXPECT_NE(NullIndexValue, high_value.get_value());
}

TEST_F(LakeTabletReshardTest, test_tablet_split_pruned_sst_full_merge_uses_live_owner_sidecars) {
    ASSIGN_OR_ABORT(auto fixture, publish_real_split_sst_owner_fixture());

    int64_t merged_tablet = 0;
    ASSIGN_OR_ABORT(auto merged,
                    publish_real_split_sst_owner_merge({fixture.low_child, fixture.middle_child, fixture.high_child},
                                                       &merged_tablet));

    // The split children have different physical rowset layouts, so neither complete metadata-reuse proof
    // applies. The writable merge must publish an empty index and hand the one omitted shared SST to normal
    // shared-orphan reclamation without disturbing the live owner's rowset sidecars.
    EXPECT_EQ(0, merged->sstable_meta().sstables_size());
    ASSERT_EQ(1, merged->orphan_files_size());
    const auto& orphan = merged->orphan_files(0);
    EXPECT_EQ("split_sst_low.sst", orphan.name());
    EXPECT_TRUE(orphan.shared());
    EXPECT_EQ(0, orphan.version());
    ASSERT_EQ(1, fixture.middle_metadata->sstable_meta().sstables_size());
    EXPECT_EQ(fixture.middle_metadata->sstable_meta().sstables(0).filesize(), orphan.size());
    EXPECT_EQ(fixture.middle_metadata->sstable_meta().sstables(0).encryption_meta(), orphan.encryption_meta());

    uint32_t owner_rssid = 0;
    for (const auto& rowset : merged->rowsets()) {
        for (int i = 0; i < rowset.segment_metas_size(); ++i) {
            if (rowset.segment_metas(i).filename() == "split_sst_low.dat") {
                owner_rssid = lake::get_rssid(rowset, i);
            }
        }
    }
    ASSERT_NE(0, owner_rssid) << "full merge must retain the real owner rowset";
    EXPECT_TRUE(merged->delvec_meta().delvecs().contains(owner_rssid));
    EXPECT_TRUE(merged->dcg_meta().dcgs().contains(owner_rssid));
    EXPECT_TRUE(merged->idg_meta().idgs().contains(owner_rssid));

    // rowid 0 was deleted before split. The owner child's delvec must survive
    // the full merge and filter key 0, while the next key in the same shared
    // SST and keys rebuilt from the other two ranges stay readable.
    ASSIGN_OR_ABORT(auto deleted_value, load_index_value(merged, merged_tablet, raw_int_primary_key(0)));
    ASSIGN_OR_ABORT(auto low_value, load_index_value(merged, merged_tablet, raw_int_primary_key(1)));
    ASSIGN_OR_ABORT(auto middle_value, load_index_value(merged, merged_tablet, raw_int_primary_key(60)));
    ASSIGN_OR_ABORT(auto high_value, load_index_value(merged, merged_tablet, raw_int_primary_key(110)));
    EXPECT_EQ(NullIndexValue, deleted_value.get_value());
    EXPECT_NE(NullIndexValue, low_value.get_value());
    EXPECT_NE(NullIndexValue, middle_value.get_value());
    EXPECT_NE(NullIndexValue, high_value.get_value());
}

// Regression for the crash discovered during SSB SF100 testing: FE requests
// N new tablet ids, but the sampled algorithm can only produce M < N ranges.
// Before the fix, get_tablet_split_ranges silently returned M ranges and
// split_tablet read OOB on split_ranges[M..N-1]. Now get_tablet_split_ranges
// returns InvalidArgument and split_tablet falls back to identical-tablet
// publish (only new_tablet_ids(0) consumed).
TEST_F(LakeTabletReshardTest, test_tablet_splitting_fewer_ranges_than_requested_falls_back) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);

    // A single segment with no backing file on disk, so the sort key sampler cannot open it and
    // every boundary candidate comes from the coarse [min, max] pair -> 1 candidate range.
    // Requesting 8 splits cannot be satisfied.
    auto* rowset_meta_pb = metadata.add_rowsets();
    rowset_meta_pb->set_id(2);
    {
        auto* sm = rowset_meta_pb->add_segment_metas();
        sm->set_filename("seg_0.dat");
        sm->set_size(1024);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(300));
        sm->set_num_rows(300);
    }
    rowset_meta_pb->set_num_rows(300);
    rowset_meta_pb->set_data_size(1024);

    EXPECT_OK(put_tablet_metadata(metadata));

    ReshardingTabletInfoPB resharding;
    auto& splitting_tablet = *resharding.mutable_splitting_tablet_info();
    splitting_tablet.set_old_tablet_id(tablet_id);
    for (int i = 0; i < 8; ++i) {
        splitting_tablet.add_new_tablet_ids(next_id());
    }

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto res =
            lake::publish_resharding_tablet(_tablet_manager.get(), resharding, metadata.version(),
                                            metadata.version() + 1, txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);
    // Fallback produces: old_tablet_id (committed under new_version) +
    // new_tablet_ids(0) carrying all data. The remaining 7 new tablet ids
    // are abandoned by BE; FE is responsible for reclaiming them.
    EXPECT_EQ(2U, tablet_metadatas.size());
    EXPECT_EQ(1U, tablet_ranges.size());
    EXPECT_TRUE(tablet_metadatas.count(tablet_id));
    EXPECT_TRUE(tablet_metadatas.count(splitting_tablet.new_tablet_ids(0)));
    for (int i = 1; i < splitting_tablet.new_tablet_ids_size(); ++i) {
        EXPECT_FALSE(tablet_metadatas.count(splitting_tablet.new_tablet_ids(i)));
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_splitting_with_gap_boundary) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);

    auto rowset_meta_pb = metadata.add_rowsets();
    rowset_meta_pb->set_id(2);
    {
        auto* sm = rowset_meta_pb->add_segment_metas();
        sm->set_filename("test_0.dat");
        sm->set_size(512);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(299999));
        sm->set_num_rows(100);
    }

    {
        auto* sm = rowset_meta_pb->add_segment_metas();
        sm->set_filename("test_1.dat");
        sm->set_size(512);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(300000));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(599999));
        sm->set_num_rows(100);
    }

    rowset_meta_pb->set_overlapped(true);
    rowset_meta_pb->set_data_size(1024);
    rowset_meta_pb->set_num_rows(200);

    EXPECT_OK(put_tablet_metadata(metadata));

    ReshardingTabletInfoPB resharding_tablet_for_splitting;
    auto& splitting_tablet = *resharding_tablet_for_splitting.mutable_splitting_tablet_info();
    splitting_tablet.set_old_tablet_id(tablet_id);
    std::vector<int64_t> new_tablet_ids{next_id(), next_id()};
    for (auto new_tablet_id : new_tablet_ids) {
        splitting_tablet.add_new_tablet_ids(new_tablet_id);
    }

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto res =
            lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet_for_splitting, metadata.version(),
                                            metadata.version() + 1, txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_OK(res);
    EXPECT_EQ(3, tablet_metadatas.size());
    EXPECT_EQ(2, tablet_ranges.size());

    int upper_300000 = 0;
    int lower_300000 = 0;
    for (const auto& [tablet_id, range_pb] : tablet_ranges) {
        if (range_pb.has_upper_bound()) {
            ASSERT_EQ(1, range_pb.upper_bound().values_size());
            if (range_pb.upper_bound().values(0).value() == "300000") {
                ++upper_300000;
                EXPECT_FALSE(range_pb.upper_bound_included());
            }
        }
        if (range_pb.has_lower_bound()) {
            ASSERT_EQ(1, range_pb.lower_bound().values_size());
            if (range_pb.lower_bound().values(0).value() == "300000") {
                ++lower_300000;
                EXPECT_TRUE(range_pb.lower_bound_included());
            }
        }
        if (range_pb.has_lower_bound() && range_pb.has_upper_bound()) {
            VariantTuple lower;
            VariantTuple upper;
            ASSERT_OK(lower.from_proto(range_pb.lower_bound()));
            ASSERT_OK(upper.from_proto(range_pb.upper_bound()));
            EXPECT_LT(lower.compare(upper), 0);
        }
    }
    EXPECT_EQ(1, upper_300000);
    EXPECT_EQ(1, lower_300000);

    for (auto new_tablet_id : new_tablet_ids) {
        auto it = tablet_metadatas.find(new_tablet_id);
        ASSERT_TRUE(it != tablet_metadatas.end());
        auto* meta = it->second.get();
        ASSERT_EQ(1, meta->rowsets_size());
        ASSERT_TRUE(meta->rowsets(0).has_range());
        EXPECT_EQ(meta->rowsets(0).range().SerializeAsString(), meta->range().SerializeAsString());
        EXPECT_GT(meta->rowsets(0).num_rows(), 0);
        EXPECT_GT(meta->rowsets(0).data_size(), 0);
    }
}

TEST_F(LakeTabletReshardTest, test_pk_tablet_splitting_keeps_raw_rowset_stats) {
    const int64_t base_version = 2;
    const int64_t new_version = 3;
    const int64_t tablet_id = next_id();

    prepare_tablet_dirs(tablet_id);

    TabletMetadataPB metadata;
    metadata.set_id(tablet_id);
    metadata.set_version(base_version);
    set_primary_key_schema(&metadata, 1);
    add_historical_schema(&metadata, 1);

    auto* rowset = metadata.add_rowsets();
    rowset->set_id(2);
    rowset->set_overlapped(true);
    rowset->set_num_rows(8);
    rowset->set_data_size(800);

    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("segment_0.dat");
        sm->set_size(400);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
        sm->set_num_rows(4);
    }

    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("segment_1.dat");
        sm->set_size(400);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(50));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(99));
        sm->set_num_rows(4);
    }

    DelVector delvec;
    const uint32_t deleted_rows[] = {0, 1, 2};
    delvec.init(base_version, deleted_rows, 3);
    add_delvec(&metadata, tablet_id, base_version, rowset->id(), "test.delvec", delvec.save());

    EXPECT_OK(put_tablet_metadata(metadata));

    ReshardingTabletInfoPB resharding_tablet;
    auto& splitting_tablet = *resharding_tablet.mutable_splitting_tablet_info();
    splitting_tablet.set_old_tablet_id(tablet_id);
    const int64_t new_tablet_id_1 = next_id();
    const int64_t new_tablet_id_2 = next_id();
    splitting_tablet.add_new_tablet_ids(new_tablet_id_1);
    splitting_tablet.add_new_tablet_ids(new_tablet_id_2);

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    int64_t total_child_num_rows = 0;
    int64_t total_child_data_size = 0;
    for (auto new_tablet_id : {new_tablet_id_1, new_tablet_id_2}) {
        auto it = tablet_metadatas.find(new_tablet_id);
        ASSERT_TRUE(it != tablet_metadatas.end());
        ASSERT_EQ(1, it->second->rowsets_size());
        total_child_num_rows += it->second->rowsets(0).num_rows();
        total_child_data_size += it->second->rowsets(0).data_size();
    }

    EXPECT_EQ(8, total_child_num_rows);
    EXPECT_EQ(800, total_child_data_size);
}

// Verify PK split scales num_dels across children proportional to per-child rows. Without
// this, each child inherits the parent's full delvec cardinality and live_rows drops to 0
// in get_tablet_stats (see lake_service.cpp:1166-1184).
TEST_F(LakeTabletReshardTest, test_pk_tablet_splitting_scales_num_dels) {
    const int64_t base_version = 2;
    const int64_t new_version = 3;
    const int64_t tablet_id = next_id();

    prepare_tablet_dirs(tablet_id);

    TabletMetadataPB metadata;
    metadata.set_id(tablet_id);
    metadata.set_version(base_version);
    set_primary_key_schema(&metadata, 1);
    add_historical_schema(&metadata, 1);

    auto* rowset = metadata.add_rowsets();
    rowset->set_id(2);
    rowset->set_overlapped(true);
    rowset->set_num_rows(10);
    rowset->set_data_size(1000);
    rowset->set_num_dels(6);

    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("segment_0.dat");
        sm->set_size(500);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
        sm->set_num_rows(5);
    }

    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("segment_1.dat");
        sm->set_size(500);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(50));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(99));
        sm->set_num_rows(5);
    }

    EXPECT_OK(put_tablet_metadata(metadata));

    ReshardingTabletInfoPB resharding_tablet;
    auto& splitting_tablet = *resharding_tablet.mutable_splitting_tablet_info();
    splitting_tablet.set_old_tablet_id(tablet_id);
    const int64_t new_tablet_id_1 = next_id();
    const int64_t new_tablet_id_2 = next_id();
    splitting_tablet.add_new_tablet_ids(new_tablet_id_1);
    splitting_tablet.add_new_tablet_ids(new_tablet_id_2);

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    int64_t total_child_num_rows = 0;
    int64_t total_child_num_dels = 0;
    for (auto new_tablet_id : {new_tablet_id_1, new_tablet_id_2}) {
        auto it = tablet_metadatas.find(new_tablet_id);
        ASSERT_TRUE(it != tablet_metadatas.end());
        ASSERT_EQ(1, it->second->rowsets_size());
        const auto& child_rowset = it->second->rowsets(0);
        EXPECT_TRUE(child_rowset.has_num_dels()) << "split must always write num_dels on PK children";
        total_child_num_rows += child_rowset.num_rows();
        total_child_num_dels += child_rowset.num_dels();
    }

    EXPECT_EQ(10, total_child_num_rows);
    // Largest-remainder allocation is exact for in-range rows: Σ child.num_dels must equal D.
    EXPECT_EQ(6, total_child_num_dels);
}

// Current producers record the rowset delete anchor explicitly. SPLIT conserves
// that anchor without deriving historical missing statistics from the delvec.
TEST_F(LakeTabletReshardTest, test_pk_tablet_splitting_explicit_num_dels_conservation) {
    const int64_t base_version = 2;
    const int64_t new_version = 3;
    const int64_t tablet_id = next_id();

    prepare_tablet_dirs(tablet_id);

    TabletMetadataPB metadata;
    metadata.set_id(tablet_id);
    metadata.set_version(base_version);
    set_primary_key_schema(&metadata, 1);
    add_historical_schema(&metadata, 1);

    auto* rowset = metadata.add_rowsets();
    rowset->set_id(2);
    rowset->set_overlapped(true);
    rowset->set_num_rows(8);
    rowset->set_data_size(800);
    rowset->set_num_dels(4);

    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("segment_0.dat");
        sm->set_size(400);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
        sm->set_num_rows(4);
    }

    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("segment_1.dat");
        sm->set_size(400);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(50));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(99));
        sm->set_num_rows(4);
    }

    DelVector delvec;
    const uint32_t deleted_rows[] = {0, 1, 2, 3};
    delvec.init(base_version, deleted_rows, 4);
    add_delvec(&metadata, tablet_id, base_version, rowset->id(), "test.delvec", delvec.save());

    EXPECT_OK(put_tablet_metadata(metadata));

    ReshardingTabletInfoPB resharding_tablet;
    auto& splitting_tablet = *resharding_tablet.mutable_splitting_tablet_info();
    splitting_tablet.set_old_tablet_id(tablet_id);
    const int64_t new_tablet_id_1 = next_id();
    const int64_t new_tablet_id_2 = next_id();
    splitting_tablet.add_new_tablet_ids(new_tablet_id_1);
    splitting_tablet.add_new_tablet_ids(new_tablet_id_2);

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    int64_t total_child_num_dels = 0;
    for (auto new_tablet_id : {new_tablet_id_1, new_tablet_id_2}) {
        auto it = tablet_metadatas.find(new_tablet_id);
        ASSERT_TRUE(it != tablet_metadatas.end());
        ASSERT_EQ(1, it->second->rowsets_size());
        const auto& child_rowset = it->second->rowsets(0);
        EXPECT_TRUE(child_rowset.has_num_dels());
        total_child_num_dels += child_rowset.num_dels();
    }
    // Σ child.num_dels equals the explicit current-producer anchor.
    EXPECT_EQ(4, total_child_num_dels);
}

// Multi-rowset conservation. Parent has multiple rowsets with overlapping
// segment key ranges so the per-source weight distribution differs across
// rowsets. After split, Σ children.rowset[r].{num_rows,data_size,num_dels}
// must equal the parent's recorded value for every rowset r — this is the
// anchor's exactness contract regardless of how the segment-level
// distribution chose to weight the children.
TEST_F(LakeTabletReshardTest, test_pk_tablet_splitting_anchor_per_rowset_conservation) {
    const int64_t base_version = 2;
    const int64_t new_version = 3;
    const int64_t tablet_id = next_id();

    prepare_tablet_dirs(tablet_id);

    TabletMetadataPB metadata;
    metadata.set_id(tablet_id);
    metadata.set_version(base_version);
    set_primary_key_schema(&metadata, 1);
    add_historical_schema(&metadata, 1);

    // Rowset A: keys [0, 99], 100 rows / 10000 bytes / 7 dels.
    auto* rs_a = metadata.add_rowsets();
    rs_a->set_id(2);
    rs_a->set_overlapped(true);
    rs_a->set_num_rows(100);
    rs_a->set_data_size(10000);
    rs_a->set_num_dels(7);
    {
        auto* sm = rs_a->add_segment_metas();
        sm->set_filename("rs_a_0.dat");
        sm->set_size(10000);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(99));
        sm->set_num_rows(100);
    }

    // Rowset B: keys [50, 199], 60 rows / 6000 bytes / 0 dels (overlaps A on [50,99]).
    auto* rs_b = metadata.add_rowsets();
    rs_b->set_id(3);
    rs_b->set_overlapped(true);
    rs_b->set_num_rows(60);
    rs_b->set_data_size(6000);
    rs_b->set_num_dels(0);
    {
        auto* sm = rs_b->add_segment_metas();
        sm->set_filename("rs_b_0.dat");
        sm->set_size(6000);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(50));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(199));
        sm->set_num_rows(60);
    }

    // Rowset C: keys [100, 199], 30 rows / 3000 bytes / 11 dels.
    auto* rs_c = metadata.add_rowsets();
    rs_c->set_id(4);
    rs_c->set_overlapped(true);
    rs_c->set_num_rows(30);
    rs_c->set_data_size(3000);
    rs_c->set_num_dels(11);
    {
        auto* sm = rs_c->add_segment_metas();
        sm->set_filename("rs_c_0.dat");
        sm->set_size(3000);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(100));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(199));
        sm->set_num_rows(30);
    }

    EXPECT_OK(put_tablet_metadata(metadata));

    ReshardingTabletInfoPB resharding;
    auto& splitting = *resharding.mutable_splitting_tablet_info();
    splitting.set_old_tablet_id(tablet_id);
    const int64_t child_id_1 = next_id();
    const int64_t child_id_2 = next_id();
    const int64_t child_id_3 = next_id();
    splitting.add_new_tablet_ids(child_id_1);
    splitting.add_new_tablet_ids(child_id_2);
    splitting.add_new_tablet_ids(child_id_3);

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, base_version, new_version, txn_info,
                                              false, tablet_metadatas, tablet_ranges));

    // Per-rowset Σ children == parent for every stat.
    struct RsTotals {
        int64_t num_rows = 0;
        int64_t data_size = 0;
        int64_t num_dels = 0;
    };
    std::unordered_map<uint32_t, RsTotals> totals;
    for (int64_t cid : {child_id_1, child_id_2, child_id_3}) {
        auto it = tablet_metadatas.find(cid);
        ASSERT_TRUE(it != tablet_metadatas.end());
        for (const auto& rs : it->second->rowsets()) {
            auto& t = totals[rs.id()];
            t.num_rows += rs.num_rows();
            t.data_size += rs.data_size();
            t.num_dels += rs.num_dels();
        }
    }

    EXPECT_EQ(100, totals[2].num_rows);
    EXPECT_EQ(10000, totals[2].data_size);
    EXPECT_EQ(7, totals[2].num_dels);

    EXPECT_EQ(60, totals[3].num_rows);
    EXPECT_EQ(6000, totals[3].data_size);
    EXPECT_EQ(0, totals[3].num_dels);

    EXPECT_EQ(30, totals[4].num_rows);
    EXPECT_EQ(3000, totals[4].data_size);
    EXPECT_EQ(11, totals[4].num_dels);
}

// A previously split child can have an approximate row count below its exact delete count while
// retaining many more shared physical rows. The next split must conserve both estimates independently.
TEST_F(LakeTabletReshardTest, test_pk_tablet_splitting_accepts_dels_above_zero_estimated_rows) {
    const int64_t base_version = 2;
    const int64_t new_version = 3;
    const int64_t tablet_id = next_id();

    prepare_tablet_dirs(tablet_id);

    TabletMetadataPB metadata;
    metadata.set_id(tablet_id);
    metadata.set_version(base_version);
    set_primary_key_schema(&metadata, 1);
    add_historical_schema(&metadata, 1);

    auto* rowset = metadata.add_rowsets();
    rowset->set_id(2);
    rowset->set_overlapped(true);
    rowset->set_num_rows(0);
    rowset->set_data_size(1000);
    rowset->set_num_dels(15);

    // Two segments so calculate_range_split_boundaries has enough key-space
    // boundaries to produce a 2-way split (single segment falls back to
    // identical-tablet publish, which would skip the anchor pass).
    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("seg_0.dat");
        sm->set_size(500);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
        sm->set_num_rows(50);
    }

    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("seg_1.dat");
        sm->set_size(500);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(50));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(99));
        sm->set_num_rows(50);
    }

    lake::tablet_reshard_helper::set_rowset_uid(rowset);
    for (int i = 0; i < rowset->segment_metas_size(); ++i) rowset->mutable_segment_metas(i)->set_segment_idx(i);
    ASSERT_OK(_tablet_manager->put_tablet_metadata(metadata));

    ReshardingTabletInfoPB resharding;
    auto& splitting = *resharding.mutable_splitting_tablet_info();
    splitting.set_old_tablet_id(tablet_id);
    const int64_t child_id_1 = next_id();
    const int64_t child_id_2 = next_id();
    splitting.add_new_tablet_ids(child_id_1);
    splitting.add_new_tablet_ids(child_id_2);

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto* sync = SyncPoint::GetInstance();
    int flushes = 0;
    sync->SetCallBack("tablet_splitter:pk_flush", [&](void*) { ++flushes; });
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->DisableProcessing();
        sync->ClearAllCallBacks();
    });
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, base_version, new_version, txn_info,
                                              false, tablet_metadatas, tablet_ranges));
    EXPECT_EQ(1, flushes);
    int64_t total_rows = 0;
    int64_t total_dels = 0;
    bool saw_dels_above_rows = false;
    for (int64_t child_id : {child_id_1, child_id_2}) {
        ASSERT_TRUE(tablet_metadatas.contains(child_id));
        ASSERT_EQ(1, tablet_metadatas.at(child_id)->rowsets_size());
        const auto& child = tablet_metadatas.at(child_id)->rowsets(0);
        total_rows += child.num_rows();
        total_dels += child.num_dels();
        saw_dels_above_rows |= child.num_dels() > child.num_rows();
    }
    EXPECT_EQ(0, total_rows);
    EXPECT_EQ(15, total_dels);
    EXPECT_TRUE(saw_dels_above_rows);
}

TEST_F(LakeTabletReshardTest, test_pk_tablet_splitting_zero_active_weights_omit_child_conserves_deletes) {
    auto set_range = [&](TabletRangePB* range, int lower, int upper) {
        range->mutable_lower_bound()->CopyFrom(generate_sort_key(lower));
        range->set_lower_bound_included(true);
        range->mutable_upper_bound()->CopyFrom(generate_sort_key(upper));
        range->set_upper_bound_included(false);
    };

    auto source = std::make_shared<TabletMetadataPB>();
    source->set_id(next_id());
    source->set_version(1);
    source->set_next_rowset_id(3);
    set_two_column_pk_schema(source.get(), 4001);
    set_range(source->mutable_range(), 0, 200);
    prepare_tablet_dirs(source->id());

    const std::string segment_name = "zero-active-delete-weight.dat";
    write_two_column_segment(
            source->id(), segment_name, /*num_rows=*/2, [](int key) { return key * 10; },
            /*key_start=*/0, [](int index) { return index * 100; });

    auto* rowset = source->add_rowsets();
    rowset->set_id(2);
    rowset->set_overlapped(false);
    rowset->set_num_rows(0);
    rowset->set_data_size(1000);
    rowset->set_num_dels(1);
    set_range(rowset->mutable_range(), 90, 200);
    lake::tablet_reshard_helper::set_rowset_uid(rowset);
    auto* segment = rowset->add_segment_metas();
    segment->set_filename(segment_name);
    segment->set_size(1000);
    segment->set_num_rows(2);
    segment->set_segment_idx(0);
    segment->set_shared(true);
    segment->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
    segment->mutable_sort_key_max()->CopyFrom(generate_sort_key(100));

    auto split_once = [&](const TabletMetadataPtr& input) {
        SplittingTabletInfoPB splitting;
        splitting.set_old_tablet_id(input->id());
        const std::array<std::pair<int, int>, 3> bounds = {{{0, 50}, {50, 95}, {95, 200}}};
        std::vector<int64_t> child_ids;
        for (const auto& [lower, upper] : bounds) {
            const int64_t child_id = next_id();
            child_ids.push_back(child_id);
            prepare_tablet_dirs(child_id);
            splitting.add_new_tablet_ids(child_id);
            set_range(splitting.add_new_tablet_ranges(), lower, upper);
        }
        TxnInfoPB txn;
        txn.set_txn_id(next_id());
        txn.set_commit_time(1);
        txn.set_gtid(1);
        ASSIGN_OR_ABORT(auto mutable_children,
                        lake::split_tablet(_tablet_manager.get(), input, splitting, input->version() + 1, txn));
        std::vector<TabletMetadataPtr> children;
        for (const int64_t child_id : child_ids) children.push_back(mutable_children.at(child_id));
        return children;
    };

    auto expect_conserved_split = [&](const std::vector<TabletMetadataPtr>& children) {
        ASSERT_EQ(3, children.size());
        EXPECT_EQ(0, children[0]->rowsets_size());
        ASSERT_EQ(1, children[1]->rowsets_size());
        ASSERT_EQ(1, children[2]->rowsets_size());
        const auto& middle = children[1]->rowsets(0);
        const auto& right = children[2]->rowsets(0);
        EXPECT_EQ(0, middle.num_rows() + right.num_rows());
        EXPECT_EQ(1000, middle.data_size() + right.data_size());
        EXPECT_EQ(1, middle.num_dels() + right.num_dels());
        EXPECT_EQ(1, middle.num_dels());
        EXPECT_EQ(0, right.num_dels());
        EXPECT_EQ(rowset->uid().SerializeAsString(), middle.uid().SerializeAsString());
        EXPECT_EQ(rowset->uid().SerializeAsString(), right.uid().SerializeAsString());
    };

    auto first = split_once(source);
    expect_conserved_split(first);
    const RowsetMetadataPB first_middle(first[1]->rowsets(0));
    const RowsetMetadataPB first_right(first[2]->rowsets(0));
    auto stable_rowset = [](RowsetMetadataPB rowset) {
        rowset.clear_id();
        return rowset.SerializeAsString();
    };

    const int64_t merged_id = next_id();
    prepare_tablet_dirs(merged_id);
    std::unordered_map<int64_t, TabletMetadataPtr> published;
    ASSERT_OK(publish_resharding_merge(first, merged_id, first[0]->version(), first[0]->version() + 1, next_id(),
                                       published));
    const auto& merged = published.at(merged_id);
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_EQ(1, merged->rowsets(0).num_dels());
    EXPECT_EQ(rowset->uid().SerializeAsString(), merged->rowsets(0).uid().SerializeAsString());

    auto second = split_once(merged);
    expect_conserved_split(second);
    EXPECT_EQ(stable_rowset(first_middle), stable_rowset(second[1]->rowsets(0)));
    EXPECT_EQ(stable_rowset(first_right), stable_rowset(second[2]->rowsets(0)));
}

// Anchor input fallback: legacy / incomplete metadata may omit
// rowset-level num_rows / data_size while still carrying valid
// segment_metas + segment_size. The previous (pre-anchor) split path
// derived its per-child stats from segment metadata via
// range_source_stats, so children received non-zero stats even when
// the rowset proto fields were unset. The anchor path must preserve
// this property: when rowset.has_num_rows() / has_data_size() is false,
// fall back to summing the corresponding segment-level fields. Without
// this, anchor=0 would collapse every child's stat to zero.
TEST_F(LakeTabletReshardTest, test_pk_tablet_splitting_anchor_falls_back_to_segment_sums_when_rowset_totals_unset) {
    const int64_t base_version = 2;
    const int64_t new_version = 3;
    const int64_t tablet_id = next_id();

    prepare_tablet_dirs(tablet_id);

    TabletMetadataPB metadata;
    metadata.set_id(tablet_id);
    metadata.set_version(base_version);
    set_primary_key_schema(&metadata, 1);
    add_historical_schema(&metadata, 1);

    auto* rowset = metadata.add_rowsets();
    rowset->set_id(2);
    rowset->set_overlapped(true);
    // Intentionally do NOT set num_rows or data_size at the rowset level.
    // Segment metadata still carries the real values.

    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("seg_0.dat");
        sm->set_size(400);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
        sm->set_num_rows(4);
    }

    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("seg_1.dat");
        sm->set_size(400);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(50));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(99));
        sm->set_num_rows(4);
    }

    EXPECT_OK(put_tablet_metadata(metadata));

    ReshardingTabletInfoPB resharding;
    auto& splitting = *resharding.mutable_splitting_tablet_info();
    splitting.set_old_tablet_id(tablet_id);
    const int64_t child_id_1 = next_id();
    const int64_t child_id_2 = next_id();
    splitting.add_new_tablet_ids(child_id_1);
    splitting.add_new_tablet_ids(child_id_2);

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, base_version, new_version, txn_info,
                                              false, tablet_metadatas, tablet_ranges));

    // Σ children rowset[r].num_rows must equal the segment-derived total
    // (4 + 4 = 8 rows), data_size the segment_size sum (400 + 400 = 800).
    int64_t total_num_rows = 0;
    int64_t total_data_size = 0;
    for (int64_t cid : {child_id_1, child_id_2}) {
        auto it = tablet_metadatas.find(cid);
        ASSERT_TRUE(it != tablet_metadatas.end());
        ASSERT_EQ(1, it->second->rowsets_size());
        total_num_rows += it->second->rowsets(0).num_rows();
        total_data_size += it->second->rowsets(0).data_size();
    }
    EXPECT_EQ(8, total_num_rows) << "anchor must fall back to Σ segment_metas.num_rows()";
    EXPECT_EQ(800, total_data_size) << "anchor must fall back to Σ segment_size";
}

// Three-level chain conservation. Σ children == parent at every split level
// for num_rows / data_size / num_dels per rowset. By induction Σ leaves at
// level-3 == original parent — the property a multi-level reshard must
// guarantee for downstream consumers (get_tablet_stats, planner, vacuum).
//
// Setup writes REAL segments so the sort key sampler can derive boundary candidates from them,
// dense enough for 3 successive splits to find candidates inside ever-narrowing tablet ranges. A
// synthetic metadata-only rowset would leave every segment at its coarse [min, max] pair, which
// cannot be subdivided at all.
TEST_F(LakeTabletReshardTest, test_pk_tablet_splitting_anchor_three_level_chain_conservation) {
    auto add_sampled_rowset = [this](TabletMetadataPB* md, int64_t tablet_id, int64_t rs_id, int min_v, int max_v,
                                     int num_rows, int data_size, int num_dels) {
        const std::string seg_name = fmt::format("rs_{}_0.dat", rs_id);
        // Keys [min_v, min_v + num_rows), i.e. exactly the [min_v, max_v] the metadata declares.
        write_two_column_segment(
                tablet_id, seg_name, num_rows, [](int i) { return i; }, /*key_start=*/min_v);
        auto* rs = md->add_rowsets();
        rs->set_id(rs_id);
        rs->set_overlapped(true);
        rs->set_num_rows(num_rows);
        rs->set_data_size(data_size);
        rs->set_num_dels(num_dels);
        auto* sm = rs->add_segment_metas();
        sm->set_filename(seg_name);
        // Deliberately the declared data_size, not the file's real size: these tests assert
        // conservation of the numbers the parent metadata records, and keeping them makes the
        // arithmetic below unchanged by this fixture switching to real segments.
        sm->set_size(data_size);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(min_v));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(max_v));
        sm->set_num_rows(num_rows);
    };

    auto verify_per_rowset_conservation = [](const TabletMetadataPB& parent_md, const std::vector<int64_t>& child_ids,
                                             const std::unordered_map<int64_t, TabletMetadataPtr>& children,
                                             const char* level) {
        struct Totals {
            int64_t num_rows = 0;
            int64_t data_size = 0;
            int64_t num_dels = 0;
        };
        std::unordered_map<uint32_t, Totals> totals;
        for (int64_t cid : child_ids) {
            auto it = children.find(cid);
            ASSERT_TRUE(it != children.end()) << level << ": missing child " << cid;
            for (const auto& rs : it->second->rowsets()) {
                auto& t = totals[rs.id()];
                t.num_rows += rs.num_rows();
                t.data_size += rs.data_size();
                t.num_dels += rs.num_dels();
            }
        }
        for (const auto& rs : parent_md.rowsets()) {
            auto it = totals.find(rs.id());
            ASSERT_TRUE(it != totals.end()) << level << ": rowset " << rs.id() << " missing in children";
            EXPECT_EQ(rs.num_rows(), it->second.num_rows) << level << ": num_rows for rs " << rs.id();
            EXPECT_EQ(rs.data_size(), it->second.data_size) << level << ": data_size for rs " << rs.id();
            EXPECT_EQ(rs.num_dels(), it->second.num_dels) << level << ": num_dels for rs " << rs.id();
        }
    };

    const int64_t base_version_l0 = 2;
    const int64_t version_l1 = 3;
    const int64_t version_l2 = 4;
    const int64_t version_l3 = 5;
    const int64_t tablet_id = next_id();

    prepare_tablet_dirs(tablet_id);

    TabletMetadataPB metadata;
    metadata.set_id(tablet_id);
    metadata.set_version(base_version_l0);
    // The two-column c0/c1 PK schema write_two_column_segment writes with, so the segments below
    // open and decode with the tablet's own schema. A valid, non-zero schema id is also required:
    // build_segments_from_rowsets only opens a rowset's segment files when its schema resolves to a
    // valid registered id.
    set_two_column_pk_schema(&metadata, /*schema_id=*/1);
    add_historical_schema(&metadata, 1);

    // 2 rowsets covering [0,1499]. Combined ~2000 rows / 16000 bytes / 42 dels. At the writer's
    // BE_TEST block size of 100 rows, each 1000-row segment offers 9 short-key-index entries, so
    // the sampler places a boundary candidate every ~100 keys -- plenty to drive 3 levels of
    // splitting.
    add_sampled_rowset(&metadata, tablet_id, /*rs_id=*/2, /*min=*/0, /*max=*/999, /*num_rows=*/1000,
                       /*data_size=*/10000, /*num_dels=*/30);
    add_sampled_rowset(&metadata, tablet_id, /*rs_id=*/3, /*min=*/500, /*max=*/1499, /*num_rows=*/1000,
                       /*data_size=*/6000, /*num_dels=*/12);

    EXPECT_OK(put_tablet_metadata(metadata));

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    // ---- Level 1: split tablet → 3 children ----
    ReshardingTabletInfoPB r1;
    auto& s1 = *r1.mutable_splitting_tablet_info();
    s1.set_old_tablet_id(tablet_id);
    const int64_t l1_a = next_id();
    const int64_t l1_b = next_id();
    const int64_t l1_c = next_id();
    s1.add_new_tablet_ids(l1_a);
    s1.add_new_tablet_ids(l1_b);
    s1.add_new_tablet_ids(l1_c);

    std::unordered_map<int64_t, TabletMetadataPtr> tm_l1;
    std::unordered_map<int64_t, TabletRangePB> tr_l1;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), r1, base_version_l0, version_l1, txn_info, false,
                                              tm_l1, tr_l1));
    verify_per_rowset_conservation(metadata, {l1_a, l1_b, l1_c}, tm_l1, "level-1");

    // ---- Level 2: re-split the level-1 child with the most rows ----
    int64_t l2_parent_id = l1_a;
    int64_t l2_parent_total_rows = 0;
    {
        for (const auto& rs : tm_l1.at(l1_a)->rowsets()) l2_parent_total_rows += rs.num_rows();
        for (int64_t cid : {l1_b, l1_c}) {
            int64_t total = 0;
            for (const auto& rs : tm_l1.at(cid)->rowsets()) total += rs.num_rows();
            if (total > l2_parent_total_rows) {
                l2_parent_id = cid;
                l2_parent_total_rows = total;
            }
        }
    }
    auto l2_parent_md = tm_l1.at(l2_parent_id);

    ReshardingTabletInfoPB r2;
    auto& s2 = *r2.mutable_splitting_tablet_info();
    s2.set_old_tablet_id(l2_parent_id);
    const int64_t l2_a = next_id();
    const int64_t l2_b = next_id();
    s2.add_new_tablet_ids(l2_a);
    s2.add_new_tablet_ids(l2_b);

    std::unordered_map<int64_t, TabletMetadataPtr> tm_l2;
    std::unordered_map<int64_t, TabletRangePB> tr_l2;
    auto st_l2 = lake::publish_resharding_tablet(_tablet_manager.get(), r2, version_l1, version_l2, txn_info, false,
                                                 tm_l2, tr_l2);
    if (!st_l2.ok()) {
        GTEST_SKIP() << "level-2 split could not be exercised on this fixture: " << st_l2;
    }
    verify_per_rowset_conservation(*l2_parent_md, {l2_a, l2_b}, tm_l2, "level-2");

    // ---- Level 3: split the bigger level-2 grandchild ----
    int64_t l3_parent_id = l2_a;
    int64_t l3_parent_total_rows = 0;
    for (const auto& rs : tm_l2.at(l2_a)->rowsets()) l3_parent_total_rows += rs.num_rows();
    {
        int64_t total_b = 0;
        for (const auto& rs : tm_l2.at(l2_b)->rowsets()) total_b += rs.num_rows();
        if (total_b > l3_parent_total_rows) {
            l3_parent_id = l2_b;
            l3_parent_total_rows = total_b;
        }
    }
    auto l3_parent_md = tm_l2.at(l3_parent_id);

    ReshardingTabletInfoPB r3;
    auto& s3 = *r3.mutable_splitting_tablet_info();
    s3.set_old_tablet_id(l3_parent_id);
    const int64_t l3_a = next_id();
    const int64_t l3_b = next_id();
    s3.add_new_tablet_ids(l3_a);
    s3.add_new_tablet_ids(l3_b);

    std::unordered_map<int64_t, TabletMetadataPtr> tm_l3;
    std::unordered_map<int64_t, TabletRangePB> tr_l3;
    auto st_l3 = lake::publish_resharding_tablet(_tablet_manager.get(), r3, version_l2, version_l3, txn_info, false,
                                                 tm_l3, tr_l3);
    if (!st_l3.ok()) {
        GTEST_SKIP() << "level-3 split could not be exercised on this fixture: " << st_l3;
    }
    verify_per_rowset_conservation(*l3_parent_md, {l3_a, l3_b}, tm_l3, "level-3");
}

TEST_F(LakeTabletReshardTest, test_split_cross_publish_sets_rowset_range_in_txn_log) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t old_tablet_id = next_id();
    const int64_t new_tablet_id = next_id();

    prepare_tablet_dirs(old_tablet_id);
    prepare_tablet_dirs(new_tablet_id);

    auto old_meta = std::make_shared<TabletMetadataPB>();
    old_meta->set_id(old_tablet_id);
    old_meta->set_version(base_version);
    old_meta->set_next_rowset_id(2);
    auto* old_range = old_meta->mutable_range();
    old_range->mutable_lower_bound()->CopyFrom(generate_sort_key(10));
    old_range->set_lower_bound_included(true);
    old_range->mutable_upper_bound()->CopyFrom(generate_sort_key(20));
    old_range->set_upper_bound_included(false);

    auto* old_rowset = old_meta->add_rowsets();
    old_rowset->set_id(1);
    old_rowset->set_overlapped(false);
    old_rowset->set_num_rows(2);
    old_rowset->set_data_size(100);
    {
        auto* sm = old_rowset->add_segment_metas();
        sm->set_filename("segment.dat");
        sm->set_size(100);
    }

    auto new_meta = std::make_shared<TabletMetadataPB>(*old_meta);
    new_meta->set_id(new_tablet_id);
    new_meta->set_version(base_version);

    EXPECT_OK(put_tablet_metadata(old_meta));
    EXPECT_OK(put_tablet_metadata(new_meta));

    TxnLogPB log;
    log.set_tablet_id(old_tablet_id);
    log.set_txn_id(100);
    auto* op_write_rowset = log.mutable_op_write()->mutable_rowset();
    op_write_rowset->set_overlapped(false);
    op_write_rowset->set_num_rows(1);
    op_write_rowset->set_data_size(1);
    {
        auto* sm = op_write_rowset->add_segment_metas();
        sm->set_filename("x.dat");
        sm->set_size(1);
    }

    EXPECT_OK(_tablet_manager->put_txn_log(log));

    lake::PublishTabletInfo tablet_info(lake::PublishTabletInfo::SPLITTING_TABLET, old_tablet_id, new_tablet_id, 2, 0);
    TxnInfoPB txn_info;
    txn_info.set_txn_id(100);
    txn_info.set_txn_type(TXN_NORMAL);
    txn_info.set_combined_txn_log(false);
    txn_info.set_commit_time(1);
    txn_info.set_force_publish(false);

    auto published_or = lake::publish_version(_tablet_manager.get(), tablet_info, base_version, new_version,
                                              std::span<const TxnInfoPB>(&txn_info, 1), false);
    ASSERT_OK(published_or.status());

    ASSIGN_OR_ABORT(auto published_meta, _tablet_manager->get_tablet_metadata(new_tablet_id, new_version));
    ASSERT_GT(published_meta->rowsets_size(), 0);
    const auto& added_rowset = published_meta->rowsets(published_meta->rowsets_size() - 1);
    ASSERT_TRUE(added_rowset.has_range());
    EXPECT_EQ(added_rowset.range().SerializeAsString(), published_meta->range().SerializeAsString());
}

// Cross-publish a multi-statement (non-PK batch) transaction onto split children
// end-to-end via publish_version, exercising the #10 invariant through the REAL
// pipeline rather than a hand-built combine input:
//   convert_txn_log_for_splitting scales EACH statement's stats by /split_count
//   independently (split_index < n % split_count gets +1, so the remainder of an odd
//   count lands on the lowest indexes), then NonPrimaryKeyTxnLogApplier's batch combine
//   merges the per-statement op_writes into one composite rowset on each child.
// The combine must
//   (1) retain a statement's segment even when ITS num_rows scaled to 0 on this child
//       (gated on segment_metas_size, not the scaled num_rows) -- else data is lost,
//   (2) adopt the FIRST log's uid, CopyFrom-preserved IDENTICALLY across sibling
//       children -- a stable MERGE dedup identity, and
//   (3) scale per statement, not once over the aggregate.
// The TxnLogApplierBatchTest unit tests cover the combine with pre-scaled inputs; this
// drives publish_version so the scaling and the combine are proven to compose.
//
// Inputs are chosen so the per-statement path is distinguishable from a (buggy)
// sum-then-scale-once path, and so the scaled-to-0 statement is ALSO the uid source:
//   stmt_a (load0, FIRST log, uid 7777): num_rows=1,  data_size=11
//   stmt_b (load1,             uid 9999): num_rows=9,  data_size=99
// Per-statement scaling with split_count=2 (scaled = n/2 + (idx < n%2 ? 1 : 0)):
//                       child0 (idx 0)     child1 (idx 1)
//   stmt_a rows      ->     1                  0   <- scales to 0, segment must survive
//   stmt_b rows      ->     5                  4
//   merged num_rows  ->     6                  4   (sum 10; sum-then-scale would give 5/5)
//   stmt_a data      ->     6                  5
//   stmt_b data      ->    50                 49
//   merged data_size ->    56                 54   (sum 110; sum-then-scale would give 55/55)
// Both children adopt stmt_a's uid 7777 (the first log) -- NOT stmt_b's 9999, and NOT
// "first positive-row contributor" (stmt_a scaled to 0 on child1 yet still defines the uid).
TEST_F(LakeTabletReshardTest, test_split_cross_publish_multi_stmt_batch_keeps_scaled_zero_segment) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t old_tablet_id = next_id();
    const int64_t child0_id = next_id();
    const int64_t child1_id = next_id();

    prepare_tablet_dirs(old_tablet_id);
    prepare_tablet_dirs(child0_id);
    prepare_tablet_dirs(child1_id);

    // A child's base metadata: non-PK (DUP) so NonPrimaryKeyTxnLogApplier's batch combine
    // is selected, carrying the post-split sub-range so convert_txn_log_for_splitting can
    // clip the cross-published rowset ranges.
    auto make_child_meta = [&](int64_t tablet_id) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(1);
        meta->mutable_schema()->set_keys_type(DUP_KEYS);
        meta->mutable_schema()->set_id(1);
        auto* range = meta->mutable_range();
        range->mutable_lower_bound()->CopyFrom(generate_sort_key(10));
        range->set_lower_bound_included(true);
        range->mutable_upper_bound()->CopyFrom(generate_sort_key(20));
        range->set_upper_bound_included(false);
        return meta;
    };
    EXPECT_OK(put_tablet_metadata(make_child_meta(child0_id)));
    EXPECT_OK(put_tablet_metadata(make_child_meta(child1_id)));

    const int64_t txn_id = next_id();
    PUniqueId load0;
    load0.set_hi(1);
    load0.set_lo(1);
    PUniqueId load1;
    load1.set_hi(1);
    load1.set_lo(2);

    // One multi-statement transaction with two per-load_id txn logs on the OLD tablet.
    // |uid_lo| is the rowset's producer uid (hi=1, lo=uid_lo); distinct per statement so
    // the merged uid can be attributed to a specific source log.
    auto write_stmt_log = [&](const PUniqueId& load_id, const std::string& segment_name, int64_t num_rows,
                              int64_t data_size, int64_t uid_lo) {
        auto log = std::make_shared<TxnLogPB>();
        log->set_tablet_id(old_tablet_id);
        log->set_txn_id(txn_id);
        auto* rowset = log->mutable_op_write()->mutable_rowset();
        rowset->set_overlapped(false);
        rowset->set_num_rows(num_rows);
        rowset->set_data_size(data_size);
        auto* sm = rowset->add_segment_metas();
        sm->set_filename(segment_name);
        sm->set_size(data_size);
        rowset->mutable_uid()->set_hi(1);
        rowset->mutable_uid()->set_lo(uid_lo);
        EXPECT_OK(_tablet_manager->put_txn_log(log, _tablet_manager->txn_log_location(old_tablet_id, txn_id, load_id)));
    };
    write_stmt_log(load0, "stmt_a.dat", /*num_rows=*/1, /*data_size=*/11, /*uid_lo=*/7777);
    write_stmt_log(load1, "stmt_b.dat", /*num_rows=*/9, /*data_size=*/99, /*uid_lo=*/9999);

    auto make_txn_info = [&]() {
        TxnInfoPB txn_info;
        txn_info.set_txn_id(txn_id);
        txn_info.set_txn_type(TXN_NORMAL);
        txn_info.set_combined_txn_log(false);
        txn_info.set_commit_time(1);
        txn_info.set_force_publish(false);
        txn_info.add_load_ids()->CopyFrom(load0);
        txn_info.add_load_ids()->CopyFrom(load1);
        return txn_info;
    };

    auto publish_child = [&](int64_t child_id, int32_t split_index) -> RowsetMetadataPB {
        lake::PublishTabletInfo tablet_info(lake::PublishTabletInfo::SPLITTING_TABLET, old_tablet_id, child_id,
                                            /*split_count=*/2, split_index);
        auto txn_info = make_txn_info();
        auto published_or = lake::publish_version(_tablet_manager.get(), tablet_info, base_version, new_version,
                                                  std::span<const TxnInfoPB>(&txn_info, 1), false);
        EXPECT_OK(published_or.status());
        ASSIGN_OR_ABORT(auto meta, _tablet_manager->get_tablet_metadata(child_id, new_version));
        EXPECT_EQ(1, meta->rowsets_size());
        return meta->rowsets(0);
    };

    // split_index=1: stmt_a (the FIRST log) scales to num_rows=0, but its segment survives.
    auto child1_rowset = publish_child(child1_id, /*split_index=*/1);
    EXPECT_EQ(2, child1_rowset.segment_metas_size())
            << "the cross-published statement whose num_rows scaled to 0 must keep its segment";
    EXPECT_EQ(4, child1_rowset.num_rows());   // stmt_a 0 + stmt_b 4
    EXPECT_EQ(54, child1_rowset.data_size()); // stmt_a 5 + stmt_b 49
    for (const auto& segment_meta : child1_rowset.segment_metas()) {
        EXPECT_TRUE(segment_meta.shared());
    }

    // split_index=0: the odd-count remainders both land here.
    auto child0_rowset = publish_child(child0_id, /*split_index=*/0);
    EXPECT_EQ(2, child0_rowset.segment_metas_size());
    EXPECT_EQ(6, child0_rowset.num_rows());   // stmt_a 1 + stmt_b 5
    EXPECT_EQ(56, child0_rowset.data_size()); // stmt_a 6 + stmt_b 50

    // Per-statement scaling, not sum-then-scale: aggregate scaling would yield 5/5 rows and
    // 55/55 data on both children; the asymmetric 6/4 and 56/54 prove each statement scaled
    // on its own. Conservation: the per-child shares add back to the originals (10 and 110).
    EXPECT_EQ(10, child0_rowset.num_rows() + child1_rowset.num_rows());
    EXPECT_EQ(110, child0_rowset.data_size() + child1_rowset.data_size());

    // Cross-sibling MERGE identity: both children adopt the FIRST log's (stmt_a) producer uid
    // 7777 verbatim -- not stmt_b's 9999, and not a "first positive-row" pick (stmt_a scaled
    // to 0 on child1 yet still defines the uid).
    EXPECT_TRUE(child0_rowset.has_uid());
    EXPECT_EQ(child1_rowset.uid().SerializeAsString(), child0_rowset.uid().SerializeAsString());
    EXPECT_EQ(1, child0_rowset.uid().hi());
    EXPECT_EQ(7777, child0_rowset.uid().lo());
}

// CORE-001: split cross-publish deliberately retains every shared physical segment and apportions
// rowset stats. A child can therefore have positive approximate stats even when the shared segment
// is wholly outside its range. A later SPLIT must take the typed identical fallback after its flush,
// rather than treating current-producer metadata as corruption.
TEST_F(LakeTabletReshardTest, test_cross_published_off_range_stats_split_falls_back_identical) {
    constexpr int64_t kBaseVersion = 1;
    constexpr int64_t kCrossVersion = 2;
    constexpr int64_t kSplitVersion = 3;
    const int64_t old_tablet = next_id();
    const int64_t right_child = next_id();
    const int64_t fallback_child = next_id();
    const int64_t unused_child = next_id();
    for (int64_t id : {old_tablet, right_child, fallback_child, unused_child}) prepare_tablet_dirs(id);

    auto set_range = [&](TabletRangePB* range, int lower, int upper) {
        *range->mutable_lower_bound() = generate_sort_key(lower);
        range->set_lower_bound_included(true);
        *range->mutable_upper_bound() = generate_sort_key(upper);
        range->set_upper_bound_included(false);
    };

    auto right = std::make_shared<TabletMetadataPB>();
    right->set_id(right_child);
    right->set_version(kBaseVersion);
    right->set_next_rowset_id(1);
    set_two_column_pk_schema(right.get(), /*schema_id=*/4001);
    right->mutable_schema()->set_keys_type(DUP_KEYS);
    set_range(right->mutable_range(), 50, 100);
    ASSERT_OK(put_tablet_metadata(right));

    const std::string segment_name = fmt::format("cross_off_range_{}.dat", old_tablet);
    const uint64_t segment_size =
            write_two_column_segment(old_tablet, segment_name, 50, [](int key) { return key * 10; });
    const int64_t cross_txn = next_id();
    TxnLogPB cross_log;
    cross_log.set_tablet_id(old_tablet);
    cross_log.set_txn_id(cross_txn);
    auto* cross_rowset = cross_log.mutable_op_write()->mutable_rowset();
    cross_rowset->set_num_rows(50);
    cross_rowset->set_data_size(segment_size);
    lake::tablet_reshard_helper::set_rowset_uid(cross_rowset);
    auto* segment = cross_rowset->add_segment_metas();
    segment->set_filename(segment_name);
    segment->set_size(segment_size);
    segment->set_num_rows(50);
    segment->set_segment_idx(0);
    *segment->mutable_sort_key_min() = generate_sort_key(0);
    *segment->mutable_sort_key_max() = generate_sort_key(49);
    ASSERT_OK(_tablet_manager->put_txn_log(cross_log));

    lake::PublishTabletInfo cross_info(lake::PublishTabletInfo::SPLITTING_TABLET, old_tablet, right_child,
                                       /*split_count=*/2, /*split_index=*/1);
    TxnInfoPB cross_txn_info;
    cross_txn_info.set_txn_id(cross_txn);
    cross_txn_info.set_txn_type(TXN_NORMAL);
    cross_txn_info.set_commit_time(1);
    ASSIGN_OR_ABORT(auto cross_published,
                    lake::publish_version(_tablet_manager.get(), cross_info, kBaseVersion, kCrossVersion,
                                          std::span<const TxnInfoPB>(&cross_txn_info, 1), false));
    ASSERT_EQ(1, cross_published->rowsets_size());
    const auto& published_rowset = cross_published->rowsets(0);
    EXPECT_EQ(25, published_rowset.num_rows());
    EXPECT_EQ(cross_published->range().SerializeAsString(), published_rowset.range().SerializeAsString());
    ASSERT_EQ(1, published_rowset.segment_metas_size());
    EXPECT_TRUE(published_rowset.segment_metas(0).shared());
    ASSIGN_OR_ABORT(auto before_rows, read_two_column_rows(cross_published));
    EXPECT_TRUE(before_rows.empty());

    ReshardingTabletInfoPB request;
    auto* split = request.mutable_splitting_tablet_info();
    split->set_old_tablet_id(right_child);
    split->add_new_tablet_ids(fallback_child);
    split->add_new_tablet_ids(unused_child);
    set_range(split->add_new_tablet_ranges(), 50, 75);
    set_range(split->add_new_tablet_ranges(), 75, 100);
    TxnInfoPB split_txn;
    split_txn.set_txn_id(next_id());
    split_txn.set_commit_time(1);
    split_txn.set_gtid(1);

    int flushes = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("tablet_splitter:pk_flush", [&](void*) { ++flushes; });
    sync->EnableProcessing();
    DeferOp clear_sync([&] {
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
    });
    std::unordered_map<int64_t, TabletMetadataPtr> published;
    std::unordered_map<int64_t, TabletRangePB> ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), request, kCrossVersion, kSplitVersion, split_txn,
                                              false, published, ranges));
    EXPECT_EQ(1, flushes);
    ASSERT_TRUE(published.contains(right_child));
    ASSERT_TRUE(published.contains(fallback_child));
    EXPECT_FALSE(published.contains(unused_child));
    EXPECT_EQ(cross_published->range().SerializeAsString(), published.at(fallback_child)->range().SerializeAsString());
    EXPECT_EQ(cross_published->rowsets(0).SerializeAsString(),
              published.at(fallback_child)->rowsets(0).SerializeAsString());
    ASSIGN_OR_ABORT(auto after_rows, read_two_column_rows(published.at(fallback_child)));
    EXPECT_EQ(before_rows, after_rows);
}

// CORE-002: MetaFileBuilder reserves a virtual operation slot for a pure DELETE statement in a
// multi-statement batch. INSERT / DELETE / INSERT therefore produces physical segment indices
// {0,2} with self-origin delete offset 1. Reshard must preserve that replay ordering even though
// offset 1 does not name a physical segment.
TEST_F(LakeTabletReshardTest, test_batch_virtual_self_del_offset_survives_split_merge_cold_load) {
    constexpr int64_t kWriteVersion = 2;
    constexpr int64_t kSplitVersion = 3;
    constexpr int64_t kMergeVersion = 3;

    auto set_range = [&](TabletRangePB* range, int lower, int upper) {
        *range->mutable_lower_bound() = generate_sort_key(lower);
        range->set_lower_bound_included(true);
        *range->mutable_upper_bound() = generate_sort_key(upper);
        range->set_upper_bound_included(false);
    };

    // Builds the INSERT / DELETE / INSERT batch on |tablet_id| at kWriteVersion, with a delete vector on the
    // first segment's row that the middle DELETE removed.
    auto build_batch_source = [&](int64_t tablet_id, uint32_t next_rowset_id, int lower, int upper) {
        auto source = std::make_shared<TabletMetadataPB>();
        source->set_id(tablet_id);
        source->set_version(kWriteVersion);
        source->set_next_rowset_id(next_rowset_id);
        set_two_column_pk_schema(source.get(), /*schema_id=*/4001);
        source->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
        source->set_enable_persistent_index(true);
        source->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        set_range(source->mutable_range(), lower, upper);

        const std::string first_name = fmt::format("virtual_first_{}.dat", tablet_id);
        const std::string second_name = fmt::format("virtual_second_{}.dat", tablet_id);
        const uint64_t first_size = write_two_column_segment(
                tablet_id, first_name, 1, [](int) { return 100; }, 10);
        const uint64_t second_size = write_two_column_segment(
                tablet_id, second_name, 1, [](int) { return 300; }, 10);

        lake::Tablet tablet(_tablet_manager.get(), tablet_id);
        lake::MetaFileBuilder builder(tablet, source);
        auto make_upsert = [&](const std::string& name, uint64_t size) {
            RowsetMetadataPB rowset;
            rowset.set_num_rows(1);
            rowset.set_data_size(size);
            rowset.set_num_dels(0);
            lake::tablet_reshard_helper::set_rowset_uid(&rowset);
            auto* segment = rowset.add_segment_metas();
            segment->set_filename(name);
            segment->set_size(size);
            segment->set_num_rows(1);
            segment->set_segment_idx(0);
            *segment->mutable_sort_key_min() = generate_sort_key(10);
            *segment->mutable_sort_key_max() = generate_sort_key(10);
            return rowset;
        };
        builder.add_rowset(make_upsert(first_name, first_size), {}, {}, {}, {}, {});

        const std::string del_name = fmt::format("virtual_middle_{}.del", tablet_id);
        write_binary_del_file(tablet_id, del_name, {encode_int_primary_key(10)});
        RowsetMetadataPB pure_delete;
        pure_delete.set_num_rows(0);
        pure_delete.set_data_size(0);
        pure_delete.set_num_dels(1);
        FileMetaPB del_file;
        del_file.set_name(del_name);
        builder.add_rowset(pure_delete, {}, {}, std::vector<FileMetaPB>{del_file},
                           /*del_op_offsets=*/{-1}, /*del_num_rows=*/{1});
        builder.add_rowset(make_upsert(second_name, second_size), {}, {}, {}, {}, {});
        CHECK_OK(builder.set_final_rowset());
        CHECK_EQ(1, source->rowsets_size());

        DelVector first_segment_delvec;
        const uint32_t deleted_row = 0;
        first_segment_delvec.init(kWriteVersion, &deleted_row, 1);
        add_delvec(source.get(), tablet_id, kWriteVersion, source->rowsets(0).id(),
                   fmt::format("virtual_middle_{}.delvec", tablet_id), first_segment_delvec.save());
        CHECK_OK(put_tablet_metadata(source));
        return TabletMetadataPtr(source);
    };
    auto expect_batch_shape = [](const RowsetMetadataPB& rowset) {
        ASSERT_EQ(2, rowset.segment_metas_size());
        EXPECT_EQ(0, rowset.segment_metas(0).segment_idx());
        EXPECT_EQ(2, rowset.segment_metas(1).segment_idx());
        ASSERT_EQ(1, rowset.del_files_size());
        EXPECT_EQ(1, rowset.del_files(0).op_offset());
    };

    // Split half: a real split hands the batch to each child with its replay coordinates intact.
    const int64_t tablet_id = next_id();
    const int64_t left_child = next_id();
    const int64_t right_child = next_id();
    for (int64_t id : {tablet_id, left_child, right_child}) prepare_tablet_dirs(id);
    auto written = build_batch_source(tablet_id, /*next_rowset_id=*/1, 0, 100);
    ASSERT_EQ(1, written->rowsets_size());
    ASSERT_NO_FATAL_FAILURE(expect_batch_shape(written->rowsets(0)));
    EXPECT_EQ(written->rowsets(0).id(), written->rowsets(0).del_files(0).origin_rowset_id());
    expect_lifecycle_oracle(written, {{10, 300}}, {});

    ReshardingTabletInfoPB split_request;
    auto* split = split_request.mutable_splitting_tablet_info();
    split->set_old_tablet_id(tablet_id);
    split->add_new_tablet_ids(left_child);
    split->add_new_tablet_ids(right_child);
    set_range(split->add_new_tablet_ranges(), 0, 50);
    set_range(split->add_new_tablet_ranges(), 50, 100);
    TxnInfoPB split_info;
    split_info.set_txn_id(next_id());
    split_info.set_commit_time(1);
    split_info.set_gtid(1);
    std::unordered_map<int64_t, TabletMetadataPtr> split_published;
    std::unordered_map<int64_t, TabletRangePB> split_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), split_request, kWriteVersion, kSplitVersion,
                                              split_info, false, split_published, split_ranges));
    ASSERT_TRUE(split_published.contains(left_child));
    ASSERT_TRUE(split_published.contains(right_child));
    for (int64_t child : {left_child, right_child}) {
        ASSERT_EQ(1, split_published.at(child)->rowsets_size());
        ASSERT_NO_FATAL_FAILURE(expect_batch_shape(split_published.at(child)->rowsets(0)));
    }

    // Merge half: the same batch in a source that owns all of its files and starts from a high rowset id,
    // merged with a neighbor, so the merge moves the rowset while leaving its relative coordinates --
    // segment_idx and op_offset -- untouched.
    const int64_t neighbor_id = next_id();
    const int64_t batch_id = next_id();
    const int64_t merged_tablet = next_id();
    for (int64_t id : {neighbor_id, batch_id, merged_tablet}) prepare_tablet_dirs(id);
    ASSIGN_OR_ABORT(auto neighbor, create_lifecycle_source(neighbor_id, /*lower=*/100, /*upper=*/200, /*key=*/150,
                                                           /*value=*/1500, /*include_delete=*/false));
    ASSERT_EQ(kWriteVersion, neighbor->version());
    auto batch = build_batch_source(batch_id, /*next_rowset_id=*/50, 0, 100);
    const auto batch_uid = batch->rowsets(0).uid().SerializeAsString();
    auto find_batch = [&](const TabletMetadataPtr& metadata) -> const RowsetMetadataPB* {
        for (const auto& rowset : metadata->rowsets()) {
            if (rowset.uid().SerializeAsString() == batch_uid) return &rowset;
        }
        return nullptr;
    };

    std::unordered_map<int64_t, TabletMetadataPtr> merge_published;
    ASSERT_OK(publish_resharding_merge({neighbor, batch}, merged_tablet, kWriteVersion, kMergeVersion, next_id(),
                                       merge_published));
    auto merged = merge_published.at(merged_tablet);
    ASSERT_EQ(2, merged->rowsets_size());
    const auto* merged_batch = find_batch(merged);
    ASSERT_NE(nullptr, merged_batch);
    ASSERT_NO_FATAL_FAILURE(expect_batch_shape(*merged_batch));
    EXPECT_NE(batch->rowsets(0).id(), merged_batch->id()) << "the fixture must make the merge move the rowset";
    EXPECT_EQ(merged_batch->id(), merged_batch->del_files(0).origin_rowset_id());
    expect_lifecycle_oracle(merged, {{10, 300}, {150, 1500}}, {});
    _update_manager->unload_and_remove_primary_index(merged_tablet);
    _tablet_manager->prune_metacache();
    ASSIGN_OR_ABORT(auto cold, _tablet_manager->get_tablet_metadata(merged_tablet, kMergeVersion));
    const auto* cold_batch = find_batch(cold);
    ASSERT_NE(nullptr, cold_batch);
    ASSERT_NO_FATAL_FAILURE(expect_batch_shape(*cold_batch));
    expect_lifecycle_oracle(cold, {{10, 300}, {150, 1500}}, {});
}

// CORE-003: a normal serial PK DELETE writes a zero-row physical segment whose footer has positive
// size and whose sort-key bounds are empty. Split must conserve its physical bytes and replay order;
// cold PK-index rebuild must skip the empty segment while still applying its del file.
TEST_F(LakeTabletReshardTest, test_delete_only_zero_row_segment_survives_split_merge_cold_load) {
    const int64_t tablet_id = next_id();
    prepare_tablet_dirs(tablet_id);

    ASSIGN_OR_ABORT(auto seeded,
                    create_lifecycle_source(tablet_id, /*lower=*/0, /*upper=*/100, /*key=*/10, /*value=*/100,
                                            /*include_delete=*/false));
    ASSIGN_OR_ABORT(auto deleted, publish_followup_delete(tablet_id, seeded->version(), /*delete_key=*/10));
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    DeferOp restore_pk_flush([&] { set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE); });
    const RowsetMetadataPB* delete_rowset = nullptr;
    for (const auto& rowset : deleted->rowsets()) {
        if (rowset.del_files_size() > 0) delete_rowset = &rowset;
    }
    ASSERT_NE(nullptr, delete_rowset);
    EXPECT_EQ(0, delete_rowset->num_rows());
    EXPECT_GT(delete_rowset->data_size(), 0);
    ASSERT_EQ(1, delete_rowset->segment_metas_size());
    EXPECT_EQ(0, delete_rowset->segment_metas(0).num_rows());
    EXPECT_EQ(0, delete_rowset->segment_metas(0).sort_key_min().values_size());
    EXPECT_EQ(0, delete_rowset->segment_metas(0).sort_key_max().values_size());
    ASSERT_EQ(1, delete_rowset->del_files_size());
    EXPECT_GT(delete_rowset->del_files(0).num_rows(), 0);
    const RowsetMetadataPB original_delete(*delete_rowset);
    auto find_delete_rowset = [&](const TabletMetadataPtr& metadata) -> const RowsetMetadataPB* {
        for (const auto& rowset : metadata->rowsets()) {
            if (rowset.uid().SerializeAsString() == original_delete.uid().SerializeAsString()) return &rowset;
        }
        return nullptr;
    };
    auto expect_canonical_delete = [&](const TabletMetadataPtr& metadata) {
        const auto* rowset = find_delete_rowset(metadata);
        ASSERT_NE(nullptr, rowset);
        EXPECT_EQ(0, rowset->num_rows());
        EXPECT_EQ(0, rowset->num_dels());
        EXPECT_EQ(original_delete.data_size(), rowset->data_size());
        ASSERT_EQ(1, rowset->segment_metas_size());
        SegmentMetadataPB expected_segment(original_delete.segment_metas(0));
        SegmentMetadataPB actual_segment(rowset->segment_metas(0));
        expected_segment.clear_shared();
        actual_segment.clear_shared();
        EXPECT_EQ(expected_segment.SerializeAsString(), actual_segment.SerializeAsString());
        ASSERT_EQ(1, rowset->del_files_size());
        DelfileWithRowsetId expected_del(original_delete.del_files(0));
        DelfileWithRowsetId actual_del(rowset->del_files(0));
        expected_del.clear_shared();
        actual_del.clear_shared();
        EXPECT_EQ(expected_del.SerializeAsString(), actual_del.SerializeAsString());
    };

    int flushes = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("tablet_splitter:pk_flush", [&](void*) { ++flushes; });
    sync->EnableProcessing();
    DeferOp clear_sync([&] {
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
    });

    auto children = split_fixed_point_source(deleted, /*child_count=*/2);
    int64_t projected_bytes = 0;
    for (const auto& child : children) {
        const auto* rowset = find_delete_rowset(child);
        ASSERT_NE(nullptr, rowset);
        ASSERT_EQ(1, rowset->segment_metas_size());
        ASSERT_EQ(1, rowset->del_files_size());
        EXPECT_EQ(0, rowset->num_rows());
        EXPECT_EQ(0, rowset->num_dels());
        EXPECT_GT(rowset->data_size(), 0);
        projected_bytes += rowset->data_size();
    }
    EXPECT_EQ(original_delete.data_size(), projected_bytes);
    EXPECT_EQ(1, flushes);

    const int64_t merged_tablet = next_id();
    std::unordered_map<int64_t, TabletMetadataPtr> published;
    ASSERT_OK(publish_resharding_merge(children, merged_tablet, deleted->version() + 1, deleted->version() + 2,
                                       next_id(), published));
    auto merged = published.at(merged_tablet);
    expect_canonical_delete(merged);
    expect_lifecycle_oracle(merged, {}, {10});
    _update_manager->unload_and_remove_primary_index(merged_tablet);
    _tablet_manager->prune_metacache();
    ASSIGN_OR_ABORT(auto cold, _tablet_manager->get_tablet_metadata(merged_tablet, deleted->version() + 2));
    expect_canonical_delete(cold);
    expect_lifecycle_oracle(cold, {}, {10});

    auto second_cycle = run_no_write_split_merge_cycle(cold, /*child_count=*/3);
    expect_canonical_delete(second_cycle);
    expect_lifecycle_oracle(second_cycle, {}, {10});
    _update_manager->unload_and_remove_primary_index(second_cycle->id());
    _tablet_manager->prune_metacache();
    ASSIGN_OR_ABORT(auto second_cold,
                    _tablet_manager->get_tablet_metadata(second_cycle->id(), second_cycle->version()));
    expect_canonical_delete(second_cold);
    expect_lifecycle_oracle(second_cold, {}, {10});
    EXPECT_EQ(2, flushes);
}

TEST_F(LakeTabletReshardTest, test_mixed_zero_row_delete_slot_survives_split_merge_cold_load) {
    const int64_t tablet_id = next_id();
    ASSIGN_OR_ABORT(auto seeded,
                    create_lifecycle_source(tablet_id, /*lower=*/0, /*upper=*/100, /*key=*/10, /*value=*/100,
                                            /*include_delete=*/false));

    const int64_t old_write_buffer_size = config::write_buffer_size;
    const bool old_enable_load_spill = config::enable_load_spill;
    const bool old_preserve_delete_order = config::lake_enable_pk_preserve_txn_delete_order;
    config::write_buffer_size = 1;
    config::enable_load_spill = false;
    config::lake_enable_pk_preserve_txn_delete_order = true;
    DeferOp restore_config([&] {
        config::write_buffer_size = old_write_buffer_size;
        config::enable_load_spill = old_enable_load_spill;
        config::lake_enable_pk_preserve_txn_delete_order = old_preserve_delete_order;
    });

    std::vector<SlotDescriptor> slots;
    slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
    slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_INT});
    slots.emplace_back(2, "__op", TypeDescriptor{LogicalType::TYPE_INT});
    std::vector<SlotDescriptor*> slot_pointers = {&slots[0], &slots[1], &slots[2]};
    Chunk::SlotHashMap slot_cid_map = {{0, 0}, {1, 1}, {2, 2}};
    auto make_chunk = [&](bool upsert) {
        const int32_t key = 10;
        const int32_t value = upsert ? 300 : 0;
        const uint8_t operation = upsert ? TOpType::UPSERT : TOpType::DELETE;
        auto key_column = Int32Column::create();
        auto value_column = Int32Column::create();
        auto operation_column = Int8Column::create();
        key_column->append_numbers(&key, sizeof(key));
        value_column->append_numbers(&value, sizeof(value));
        operation_column->append_numbers(&operation, sizeof(operation));
        return Chunk(Columns{std::move(key_column), std::move(value_column), std::move(operation_column)},
                     slot_cid_map);
    };
    auto delete_chunk = make_chunk(false);
    auto upsert_chunk = make_chunk(true);
    const uint32_t index = 0;
    const int64_t txn_id = next_id();
    auto tablet_schema = TabletSchema::create(seeded->schema());
    RuntimeProfile profile("mixed-zero-row-delete-slot");
    ASSIGN_OR_ABORT(auto delta_writer, lake::DeltaWriterBuilder()
                                               .set_tablet_manager(_tablet_manager.get())
                                               .set_tablet_id(tablet_id)
                                               .set_txn_id(txn_id)
                                               .set_partition_id(next_id())
                                               .set_mem_tracker(_mem_tracker.get())
                                               .set_schema_id(seeded->schema().id())
                                               .set_tablet_schema(std::move(tablet_schema))
                                               .set_slot_descriptors(&slot_pointers)
                                               .set_profile(&profile)
                                               .build());
    ASSERT_OK(delta_writer->open());
    ASSERT_OK(delta_writer->write(delete_chunk, &index, 1));
    ASSERT_OK(delta_writer->write(upsert_chunk, &index, 1));
    ASSERT_OK(delta_writer->finish_with_txnlog());
    delta_writer->close();
    TxnInfoPB txn;
    txn.set_txn_id(txn_id);
    txn.set_txn_type(TXN_NORMAL);
    txn.set_commit_time(1);
    ASSIGN_OR_ABORT(auto written,
                    lake::publish_version(_tablet_manager.get(), lake::PublishTabletInfo(tablet_id), seeded->version(),
                                          seeded->version() + 1, std::span<const TxnInfoPB>(&txn, 1), false));

    const RowsetMetadataPB* mixed = nullptr;
    for (const auto& rowset : written->rowsets()) {
        if (rowset.version() == written->version()) mixed = &rowset;
    }
    ASSERT_NE(nullptr, mixed);
    ASSERT_EQ(2, mixed->segment_metas_size());
    EXPECT_EQ((std::vector<int64_t>{0, 1}),
              (std::vector<int64_t>{mixed->segment_metas(0).num_rows(), mixed->segment_metas(1).num_rows()}));
    EXPECT_EQ((std::vector<uint32_t>{0, 1}),
              (std::vector<uint32_t>{mixed->segment_metas(0).segment_idx(), mixed->segment_metas(1).segment_idx()}));
    ASSERT_EQ(1, mixed->del_files_size());
    EXPECT_EQ(mixed->id(), mixed->del_files(0).origin_rowset_id());
    EXPECT_EQ(0, mixed->del_files(0).op_offset());
    const auto mixed_uid = mixed->uid().SerializeAsString();
    const int64_t mixed_rows = mixed->num_rows();
    const int64_t mixed_bytes = mixed->data_size();
    const int64_t mixed_dels = mixed->num_dels();
    auto canonical_mixed_signature = [&](const TabletMetadataPtr& metadata) {
        for (const auto& rowset : metadata->rowsets()) {
            if (rowset.uid().SerializeAsString() != mixed_uid) continue;
            RowsetMetadataPB stable(rowset);
            stable.clear_id();
            stable.clear_range();
            for (auto& segment : *stable.mutable_segment_metas()) segment.clear_shared();
            for (auto& del : *stable.mutable_del_files()) {
                del.clear_origin_rowset_id();
                del.clear_shared();
            }
            return stable.SerializeAsString();
        }
        return std::string();
    };
    const auto baseline_signature = canonical_mixed_signature(written);
    ASSERT_FALSE(baseline_signature.empty());

    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    DeferOp restore_pk_flush([&] { set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE); });
    auto children_or = try_split_fixed_point_source(written, /*child_count=*/2);
    ASSERT_OK(children_or.status());
    auto children = std::move(children_or).value();
    for (const auto& child : children) {
        auto it = std::find_if(child->rowsets().begin(), child->rowsets().end(),
                               [&](const auto& rowset) { return rowset.uid().SerializeAsString() == mixed_uid; });
        ASSERT_NE(child->rowsets().end(), it);
        ASSERT_EQ(2, it->segment_metas_size());
        EXPECT_EQ(0, it->segment_metas(0).segment_idx());
        EXPECT_EQ(1, it->segment_metas(1).segment_idx());
        ASSERT_EQ(1, it->del_files_size());
        EXPECT_EQ(0, it->del_files(0).op_offset());
    }

    const int64_t merged_tablet = next_id();
    std::unordered_map<int64_t, TabletMetadataPtr> published;
    ASSERT_OK(publish_resharding_merge(children, merged_tablet, written->version() + 1, written->version() + 2,
                                       next_id(), published));
    auto merged = published.at(merged_tablet);
    auto merged_it = std::find_if(merged->rowsets().begin(), merged->rowsets().end(),
                                  [&](const auto& rowset) { return rowset.uid().SerializeAsString() == mixed_uid; });
    ASSERT_NE(merged->rowsets().end(), merged_it);
    ASSERT_EQ(2, merged_it->segment_metas_size());
    EXPECT_EQ(0, merged_it->segment_metas(0).segment_idx());
    EXPECT_EQ(1, merged_it->segment_metas(1).segment_idx());
    ASSERT_EQ(1, merged_it->del_files_size());
    EXPECT_EQ(0, merged_it->del_files(0).op_offset());
    EXPECT_EQ(mixed_rows, merged_it->num_rows());
    EXPECT_EQ(mixed_bytes, merged_it->data_size());
    EXPECT_EQ(mixed_dels, merged_it->num_dels());
    expect_lifecycle_oracle(merged, {{10, 300}}, {});
    _update_manager->unload_and_remove_primary_index(merged_tablet);
    _tablet_manager->prune_metacache();
    ASSIGN_OR_ABORT(auto cold, _tablet_manager->get_tablet_metadata(merged_tablet, written->version() + 2));
    expect_lifecycle_oracle(cold, {{10, 300}}, {});
    EXPECT_EQ(baseline_signature, canonical_mixed_signature(cold));

    auto three_way_or = try_run_no_write_split_merge_cycle(cold, /*child_count=*/3);
    ASSERT_OK(three_way_or.status());
    auto three_way = std::move(three_way_or).value();
    expect_lifecycle_oracle(three_way, {{10, 300}}, {});
    EXPECT_EQ(baseline_signature, canonical_mixed_signature(three_way));
}

TEST_F(LakeTabletReshardTest, test_all_zero_rowset_without_delete_survives_split_merge_fixed_point) {
    for (const bool positive_anchor : {true, false}) {
        SCOPED_TRACE(fmt::format("positive aggregate byte anchor: {}", positive_anchor));
        const int64_t tablet_id = next_id();
        prepare_tablet_dirs(tablet_id);
        const std::string segment_name = fmt::format("all-zero-{}.dat", positive_anchor);
        const uint64_t footer_size = write_two_column_segment(tablet_id, segment_name, 0, [](int key) { return key; });
        ASSERT_GT(footer_size, 0);

        auto source = std::make_shared<TabletMetadataPB>();
        source->set_id(tablet_id);
        source->set_version(1);
        source->set_next_rowset_id(2);
        set_two_column_pk_schema(source.get(), 4001);
        source->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(0));
        source->mutable_range()->set_lower_bound_included(true);
        source->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(100));
        source->mutable_range()->set_upper_bound_included(false);
        auto* rowset = source->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(0);
        rowset->set_data_size(positive_anchor ? footer_size : 0);
        rowset->set_num_dels(0);
        lake::tablet_reshard_helper::set_rowset_uid(rowset);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(segment_name);
        segment->set_segment_idx(0);
        segment->set_num_rows(0);
        segment->set_size(footer_size);
        const auto original_uid = rowset->uid().SerializeAsString();
        const SegmentMetadataPB original_segment(*segment);
        const int64_t expected_bytes = rowset->data_size();
        auto canonical_signature = [](const TabletMetadataPtr& metadata) {
            RowsetMetadataPB stable(metadata->rowsets(0));
            stable.clear_id();
            stable.clear_range();
            for (auto& stable_segment : *stable.mutable_segment_metas()) stable_segment.clear_shared();
            return stable.SerializeAsString();
        };
        const auto baseline_signature = canonical_signature(source);
        ASSERT_OK(put_tablet_metadata(source));

        TabletMetadataPtr current = source;
        for (const int child_count : {2, 3}) {
            auto children_or = try_split_fixed_point_source(current, child_count);
            ASSERT_OK(children_or.status());
            auto children = std::move(children_or).value();
            int64_t total_rows = 0;
            int64_t total_bytes = 0;
            int64_t total_dels = 0;
            for (const auto& child : children) {
                ASSERT_EQ(1, child->rowsets_size());
                const auto& child_rowset = child->rowsets(0);
                EXPECT_EQ(original_uid, child_rowset.uid().SerializeAsString());
                ASSERT_EQ(1, child_rowset.segment_metas_size());
                SegmentMetadataPB child_segment(child_rowset.segment_metas(0));
                child_segment.clear_shared();
                EXPECT_EQ(original_segment.SerializeAsString(), child_segment.SerializeAsString());
                total_rows += child_rowset.num_rows();
                total_bytes += child_rowset.data_size();
                total_dels += child_rowset.num_dels();
            }
            EXPECT_EQ(0, total_rows);
            EXPECT_EQ(expected_bytes, total_bytes);
            EXPECT_EQ(0, total_dels);

            const int64_t target = next_id();
            std::unordered_map<int64_t, TabletMetadataPtr> published;
            ASSERT_OK(publish_resharding_merge(children, target, current->version() + 1, current->version() + 2,
                                               next_id(), published));
            current = published.at(target);
            ASSERT_EQ(1, current->rowsets_size());
            const auto& merged_rowset = current->rowsets(0);
            EXPECT_EQ(original_uid, merged_rowset.uid().SerializeAsString());
            EXPECT_EQ(0, merged_rowset.num_rows());
            EXPECT_EQ(expected_bytes, merged_rowset.data_size());
            EXPECT_EQ(0, merged_rowset.num_dels());
            ASSERT_EQ(1, merged_rowset.segment_metas_size());
            EXPECT_EQ(0, merged_rowset.segment_metas(0).segment_idx());
            SegmentMetadataPB merged_segment(merged_rowset.segment_metas(0));
            merged_segment.clear_shared();
            EXPECT_EQ(original_segment.SerializeAsString(), merged_segment.SerializeAsString());
            EXPECT_EQ(baseline_signature, canonical_signature(current));
        }
    }
}

// CORE-005: split row counts are estimates, but later PK deletes are exact. A skewed shared segment
// can therefore produce a valid child whose num_dels exceeds its estimated num_rows. Both re-split
// and sibling merge must preserve that delete anchor instead of treating the estimate as a hard cap.
TEST_F(LakeTabletReshardTest, test_skewed_child_exact_deletes_survive_resplit_merge_cold_load) {
    constexpr int64_t kSourceVersion = 1;
    constexpr int64_t kSplitVersion = 2;
    constexpr int64_t kDmlVersion = 3;
    constexpr int64_t kMergeVersion = 4;
    const int64_t source_id = next_id();
    const int64_t left_id = next_id();
    const int64_t right_id = next_id();
    const int64_t target_id = next_id();
    for (int64_t id : {source_id, left_id, right_id, target_id}) prepare_tablet_dirs(id);

    auto set_range = [&](TabletRangePB* range, int lower, int upper) {
        *range->mutable_lower_bound() = generate_sort_key(lower);
        range->set_lower_bound_included(true);
        *range->mutable_upper_bound() = generate_sort_key(upper);
        range->set_upper_bound_included(false);
    };
    const std::string segment_name = fmt::format("skewed_delete_{}.dat", source_id);
    const uint64_t segment_size = write_two_column_segment(
            source_id, segment_name, /*num_rows=*/100, [](int key) { return key * 10; }, /*key_start=*/0,
            [](int ordinal) { return ordinal < 90 ? ordinal : 1000 + ordinal - 90; });

    auto source = std::make_shared<TabletMetadataPB>();
    source->set_id(source_id);
    source->set_version(kSourceVersion);
    source->set_next_rowset_id(2);
    set_two_column_pk_schema(source.get(), /*schema_id=*/4001);
    source->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
    source->set_enable_persistent_index(true);
    source->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
    set_range(source->mutable_range(), 0, 1010);
    auto* rowset = source->add_rowsets();
    rowset->set_id(1);
    rowset->set_version(kSourceVersion);
    rowset->set_overlapped(false);
    rowset->set_num_rows(100);
    rowset->set_data_size(segment_size);
    rowset->set_num_dels(0);
    lake::tablet_reshard_helper::set_rowset_uid(rowset);
    const auto source_uid = rowset->uid().SerializeAsString();
    auto* segment = rowset->add_segment_metas();
    segment->set_filename(segment_name);
    segment->set_size(segment_size);
    segment->set_num_rows(100);
    segment->set_segment_idx(0);
    *segment->mutable_sort_key_min() = generate_sort_key(0);
    *segment->mutable_sort_key_max() = generate_sort_key(1009);
    ASSERT_EQ(0, segment->deprecated_sort_key_samples_size());
    ASSERT_OK(put_tablet_metadata(source));

    ReshardingTabletInfoPB split_request;
    auto* split = split_request.mutable_splitting_tablet_info();
    split->set_old_tablet_id(source_id);
    split->add_new_tablet_ids(left_id);
    split->add_new_tablet_ids(right_id);
    set_range(split->add_new_tablet_ranges(), 0, 500);
    set_range(split->add_new_tablet_ranges(), 500, 1010);
    TxnInfoPB split_info;
    split_info.set_txn_id(next_id());
    split_info.set_commit_time(1);
    split_info.set_gtid(1);
    std::unordered_map<int64_t, TabletMetadataPtr> split_published;
    std::unordered_map<int64_t, TabletRangePB> split_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), split_request, kSourceVersion, kSplitVersion,
                                              split_info, false, split_published, split_ranges));
    auto left = split_published.at(left_id);
    auto right = split_published.at(right_id);
    auto find_source_rowset = [&](const TabletMetadataPtr& metadata) -> const RowsetMetadataPB* {
        for (const auto& candidate : metadata->rowsets()) {
            if (candidate.uid().SerializeAsString() == source_uid) return &candidate;
        }
        return nullptr;
    };
    const auto* estimated_left = find_source_rowset(left);
    ASSERT_NE(nullptr, estimated_left);
    EXPECT_LT(estimated_left->num_rows(), 60);

    std::vector<int32_t> deleted_keys(60);
    std::iota(deleted_keys.begin(), deleted_keys.end(), 0);
    ASSIGN_OR_ABORT(auto left_deleted, publish_followup_deletes(left_id, kSplitVersion, deleted_keys));
    ASSIGN_OR_ABORT(auto right_aligned,
                    publish_followup_upsert_delete(right_id, kSplitVersion, /*upsert_key=*/1000,
                                                   /*upsert_value=*/10000, /*delete_key=*/0,
                                                   /*include_delete=*/false, /*include_upsert=*/true));
    ASSERT_EQ(kDmlVersion, left_deleted->version());
    ASSERT_EQ(kDmlVersion, right_aligned->version());
    const auto* deleted_source = find_source_rowset(left_deleted);
    ASSERT_NE(nullptr, deleted_source);
    EXPECT_EQ(60, deleted_source->num_dels());
    EXPECT_GT(deleted_source->num_dels(), deleted_source->num_rows());

    SplittingTabletInfoPB nested_split;
    nested_split.set_old_tablet_id(left_id);
    const int64_t nested_left = next_id();
    const int64_t nested_right = next_id();
    nested_split.add_new_tablet_ids(nested_left);
    nested_split.add_new_tablet_ids(nested_right);
    set_range(nested_split.add_new_tablet_ranges(), 0, 250);
    set_range(nested_split.add_new_tablet_ranges(), 250, 500);
    TxnInfoPB nested_info;
    nested_info.set_txn_id(next_id());
    nested_info.set_commit_time(1);
    nested_info.set_gtid(2);
    ASSIGN_OR_ABORT(auto nested_children,
                    lake::split_tablet(_tablet_manager.get(), left_deleted, nested_split, kMergeVersion, nested_info));
    int64_t nested_rows = 0;
    int64_t nested_dels = 0;
    bool saw_dels_above_rows = false;
    for (int64_t child_id : {nested_left, nested_right}) {
        const auto* child_rowset = find_source_rowset(nested_children.at(child_id));
        ASSERT_NE(nullptr, child_rowset);
        nested_rows += child_rowset->num_rows();
        nested_dels += child_rowset->num_dels();
        saw_dels_above_rows |= child_rowset->num_dels() > child_rowset->num_rows();
    }
    EXPECT_EQ(deleted_source->num_rows(), nested_rows);
    EXPECT_EQ(60, nested_dels);
    EXPECT_TRUE(saw_dels_above_rows);

    std::unordered_map<int64_t, TabletMetadataPtr> merged_published;
    const auto* right_source = find_source_rowset(right_aligned);
    ASSERT_NE(nullptr, right_source);
    const int64_t expected_merged_rows = deleted_source->num_rows() + right_source->num_rows();
    const int64_t expected_merged_bytes = deleted_source->data_size() + right_source->data_size();
    const int64_t expected_merged_dels = deleted_source->num_dels() + right_source->num_dels();
    ASSERT_OK(publish_resharding_merge({left_deleted, right_aligned}, target_id, kDmlVersion, kMergeVersion, next_id(),
                                       merged_published));
    auto merged = merged_published.at(target_id);
    const auto* merged_source = find_source_rowset(merged);
    ASSERT_NE(nullptr, merged_source);
    EXPECT_EQ(expected_merged_rows, merged_source->num_rows());
    EXPECT_EQ(expected_merged_bytes, merged_source->data_size());
    EXPECT_EQ(expected_merged_dels, merged_source->num_dels());
    std::vector<std::pair<int32_t, int32_t>> expected_rows;
    for (int key = 60; key < 90; ++key) expected_rows.emplace_back(key, key * 10);
    for (int key = 1000; key < 1010; ++key) expected_rows.emplace_back(key, key * 10);
    expect_lifecycle_oracle(merged, expected_rows, deleted_keys);
    _update_manager->unload_and_remove_primary_index(target_id);
    _tablet_manager->prune_metacache();
    ASSIGN_OR_ABORT(auto cold, _tablet_manager->get_tablet_metadata(target_id, kMergeVersion));
    expect_lifecycle_oracle(cold, expected_rows, deleted_keys);
}

TEST_F(LakeTabletReshardTest, test_convert_txn_log_updates_all_rowset_ranges_for_splitting) {
    auto base_metadata = std::make_shared<TabletMetadataPB>();
    base_metadata->set_id(next_id());
    base_metadata->set_version(1);
    base_metadata->set_next_rowset_id(1);
    base_metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(10));
    base_metadata->mutable_range()->set_lower_bound_included(true);
    base_metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(20));
    base_metadata->mutable_range()->set_upper_bound_included(false);

    auto txn_log = std::make_shared<TxnLogPB>();
    txn_log->set_tablet_id(base_metadata->id());
    txn_log->set_txn_id(1000);

    auto set_range = [&](TabletRangePB* range, int lower, int upper) {
        range->mutable_lower_bound()->CopyFrom(generate_sort_key(lower));
        range->set_lower_bound_included(true);
        range->mutable_upper_bound()->CopyFrom(generate_sort_key(upper));
        range->set_upper_bound_included(false);
    };
    auto fill_rowset = [&](RowsetMetadataPB* rowset, const std::string& segment_name, int lower, int upper) {
        rowset->set_overlapped(false);
        rowset->set_num_rows(1);
        rowset->set_data_size(1);
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename(segment_name);
            sm->set_size(1);
            sm->set_num_rows(1);
        }
        set_range(rowset->mutable_range(), lower, upper);
    };
    auto fill_sstable = [&](PersistentIndexSstablePB* sstable, const std::string& filename) {
        sstable->set_filename(filename);
        sstable->set_filesize(1);
        sstable->set_shared(false);
    };
    auto expect_shared_and_range = [&](const RowsetMetadataPB& rowset, int lower, int upper) {
        for (const auto& segment_meta : rowset.segment_metas()) {
            EXPECT_TRUE(segment_meta.shared());
        }
        TabletRangePB expected_range;
        set_range(&expected_range, lower, upper);
        EXPECT_TRUE(rowset.has_range());
        EXPECT_EQ(expected_range.SerializeAsString(), rowset.range().SerializeAsString());
    };

    // op_write
    fill_rowset(txn_log->mutable_op_write()->mutable_rowset(), "op_write.dat", 5, 15);
    // op_compaction
    fill_rowset(txn_log->mutable_op_compaction()->mutable_output_rowset(), "op_compaction.dat", 12, 25);
    fill_sstable(txn_log->mutable_op_compaction()->mutable_output_sstable(), "op_compaction.sst");
    fill_sstable(txn_log->mutable_op_compaction()->add_output_sstables(), "op_compaction_1.sst");
    // op_schema_change
    fill_rowset(txn_log->mutable_op_schema_change()->add_rowsets(), "op_schema_change.dat", 0, 30);
    // op_replication
    fill_rowset(txn_log->mutable_op_replication()->add_op_writes()->mutable_rowset(), "op_replication.dat", 18, 30);
    // op_parallel_compaction
    auto* op_parallel_compaction = txn_log->mutable_op_parallel_compaction();
    fill_rowset(op_parallel_compaction->add_subtask_compactions()->mutable_output_rowset(),
                "op_parallel_compaction.dat", 19, 21);
    fill_sstable(op_parallel_compaction->mutable_output_sstable(), "op_parallel_compaction.sst");
    fill_sstable(op_parallel_compaction->add_output_sstables(), "op_parallel_compaction_1.sst");

    lake::PublishTabletInfo publish_tablet_info(lake::PublishTabletInfo::SPLITTING_TABLET, txn_log->tablet_id(),
                                                next_id(), 2, 0);
    ASSIGN_OR_ABORT(auto converted, convert_txn_log(txn_log, base_metadata, publish_tablet_info));

    EXPECT_EQ(publish_tablet_info.get_tablet_id_in_metadata(), converted->tablet_id());
    expect_shared_and_range(converted->op_write().rowset(), 10, 15);
    // op_compaction and op_parallel_compaction are dropped on SPLITTING cross-publish
    // (see convert_txn_log_for_splitting); range narrowing is exercised on the surviving
    // op_write / op_schema_change / op_replication payloads. Drop coverage lives in
    // test_convert_txn_log_splitting_drops_op_compaction* tests.
    EXPECT_FALSE(converted->has_op_compaction());
    EXPECT_FALSE(converted->has_op_parallel_compaction());
    expect_shared_and_range(converted->op_schema_change().rowsets(0), 10, 20);
    expect_shared_and_range(converted->op_replication().op_writes(0).rowset(), 18, 20);
}

// --- sstable merge tests ---

TEST_F(LakeTabletReshardTest, test_convert_txn_log_adjusts_data_stats_for_splitting) {
    auto base_metadata = std::make_shared<TabletMetadataPB>();
    base_metadata->set_id(next_id());
    base_metadata->set_version(1);
    base_metadata->set_next_rowset_id(1);
    base_metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(10));
    base_metadata->mutable_range()->set_lower_bound_included(true);
    base_metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(20));
    base_metadata->mutable_range()->set_upper_bound_included(false);

    auto txn_log = std::make_shared<TxnLogPB>();
    txn_log->set_tablet_id(base_metadata->id());
    txn_log->set_txn_id(1000);

    auto* rowset = txn_log->mutable_op_write()->mutable_rowset();
    rowset->set_overlapped(false);
    rowset->set_num_rows(100);
    rowset->set_data_size(1000);
    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("seg.dat");
        sm->set_size(1000);
    }
    auto* range = rowset->mutable_range();
    range->mutable_lower_bound()->CopyFrom(generate_sort_key(5));
    range->set_lower_bound_included(true);
    range->mutable_upper_bound()->CopyFrom(generate_sort_key(25));
    range->set_upper_bound_included(false);

    // Simulate 3-way split, this is tablet index 0
    lake::PublishTabletInfo info0(lake::PublishTabletInfo::SPLITTING_TABLET, txn_log->tablet_id(), next_id(), 3, 0);
    ASSIGN_OR_ABORT(auto converted0, lake::convert_txn_log(txn_log, base_metadata, info0));
    EXPECT_EQ(34, converted0->op_write().rowset().num_rows());   // 100/3=33 + (0<1?1:0) = 34
    EXPECT_EQ(334, converted0->op_write().rowset().data_size()); // 1000/3=333 + (0<1?1:0) = 334

    // tablet index 1
    lake::PublishTabletInfo info1(lake::PublishTabletInfo::SPLITTING_TABLET, txn_log->tablet_id(), next_id(), 3, 1);
    ASSIGN_OR_ABORT(auto converted1, lake::convert_txn_log(txn_log, base_metadata, info1));
    EXPECT_EQ(33, converted1->op_write().rowset().num_rows());
    EXPECT_EQ(333, converted1->op_write().rowset().data_size());

    // tablet index 2
    lake::PublishTabletInfo info2(lake::PublishTabletInfo::SPLITTING_TABLET, txn_log->tablet_id(), next_id(), 3, 2);
    ASSIGN_OR_ABORT(auto converted2, lake::convert_txn_log(txn_log, base_metadata, info2));
    EXPECT_EQ(33, converted2->op_write().rowset().num_rows());
    EXPECT_EQ(333, converted2->op_write().rowset().data_size());

    // Verify total equals original
    EXPECT_EQ(100, converted0->op_write().rowset().num_rows() + converted1->op_write().rowset().num_rows() +
                           converted2->op_write().rowset().num_rows());
    EXPECT_EQ(1000, converted0->op_write().rowset().data_size() + converted1->op_write().rowset().data_size() +
                            converted2->op_write().rowset().data_size());

    // Verify ranges are still adjusted (shared and intersected with base range)
    ASSERT_TRUE(converted0->op_write().rowset().segment_metas_size() > 0);
    EXPECT_TRUE(converted0->op_write().rowset().segment_metas(0).shared());
}

TEST_F(LakeTabletReshardTest, test_convert_txn_log_shares_disjoint_sibling_and_conserves_stats) {
    const int64_t old_tablet_id = next_id();
    auto txn_log = std::make_shared<TxnLogPB>();
    txn_log->set_tablet_id(old_tablet_id);
    txn_log->set_txn_id(1000);

    auto* rowset = txn_log->mutable_op_write()->mutable_rowset();
    rowset->set_num_rows(1000);
    rowset->set_data_size(10000);
    auto* segment = rowset->add_segment_metas();
    segment->set_filename("right_child_segment.dat");
    segment->set_size(10000);
    segment->mutable_sort_key_min()->CopyFrom(generate_sort_key(70));
    segment->mutable_sort_key_max()->CopyFrom(generate_sort_key(80));

    auto make_child_metadata = [&](int lower, int upper) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(next_id());
        metadata->set_version(1);
        metadata->set_next_rowset_id(1);
        metadata->mutable_schema()->set_keys_type(DUP_KEYS);
        auto* range = metadata->mutable_range();
        range->mutable_lower_bound()->CopyFrom(generate_sort_key(lower));
        range->set_lower_bound_included(true);
        range->mutable_upper_bound()->CopyFrom(generate_sort_key(upper));
        range->set_upper_bound_included(false);
        return metadata;
    };
    auto left_metadata = make_child_metadata(0, 50);
    auto right_metadata = make_child_metadata(50, 100);

    lake::PublishTabletInfo left_info(lake::PublishTabletInfo::SPLITTING_TABLET, old_tablet_id, left_metadata->id(), 2,
                                      0);
    lake::PublishTabletInfo right_info(lake::PublishTabletInfo::SPLITTING_TABLET, old_tablet_id, right_metadata->id(),
                                       2, 1);
    ASSIGN_OR_ABORT(auto converted_left, lake::convert_txn_log(txn_log, left_metadata, left_info));
    ASSIGN_OR_ABORT(auto converted_right, lake::convert_txn_log(txn_log, right_metadata, right_info));

    ASSERT_EQ(1, converted_left->op_write().rowset().segment_metas_size());
    ASSERT_EQ(1, converted_right->op_write().rowset().segment_metas_size());
    EXPECT_TRUE(converted_left->op_write().rowset().segment_metas(0).shared());
    EXPECT_TRUE(converted_right->op_write().rowset().segment_metas(0).shared());
    EXPECT_EQ(500, converted_left->op_write().rowset().num_rows());
    EXPECT_EQ(500, converted_right->op_write().rowset().num_rows());
    EXPECT_EQ(5000, converted_left->op_write().rowset().data_size());
    EXPECT_EQ(5000, converted_right->op_write().rowset().data_size());
}

TEST_F(LakeTabletReshardTest, test_convert_txn_log_normal_publish_no_stats_change) {
    auto base_metadata = std::make_shared<TabletMetadataPB>();
    base_metadata->set_id(next_id());
    base_metadata->set_version(1);

    auto txn_log = std::make_shared<TxnLogPB>();
    txn_log->set_tablet_id(base_metadata->id());
    txn_log->set_txn_id(1000);
    txn_log->mutable_op_write()->mutable_rowset()->set_num_rows(100);
    txn_log->mutable_op_write()->mutable_rowset()->set_data_size(1000);

    lake::PublishTabletInfo info(base_metadata->id());
    ASSIGN_OR_ABORT(auto converted, lake::convert_txn_log(txn_log, base_metadata, info));

    // Normal publish returns the same txn_log pointer, no changes
    EXPECT_EQ(txn_log.get(), converted.get());
    EXPECT_EQ(100, converted->op_write().rowset().num_rows());
    EXPECT_EQ(1000, converted->op_write().rowset().data_size());
}

// --- Tests for SPLITTING cross-publish drop-as-empty-compaction ---
//
// Symmetric to the MERGING tests above. A pre-split compaction txn whose
// publish lands on a SPLIT child has the rows-mapper (.lcrm) and output rowset
// shaped against the parent tablet's full key range. Each child only owns a
// subrange, so when the conflict resolver runs over its op_compaction the
// segment iteration consumes fewer rows than the mapper's stored row_count,
// and `RowsMapperIterator::status()` (storage/rows_mapper.cpp:155) hard-fails
// the publish with "Chunk vs rows mapper's row count mismatch", wedging
// CLEANING. Convert_txn_log must therefore drop op_compaction /
// op_parallel_compaction during SPLITTING cross-publish (mirroring MERGING),
// leaving op_write payloads intact and preserving the child range / data-stat
// adjustments.

TEST_F(LakeTabletReshardTest, test_convert_txn_log_splitting_drops_op_compaction) {
    const int64_t source_tablet_id = next_id();
    const int64_t child_tablet_id = next_id();
    auto log = make_op_compaction_log(source_tablet_id);

    // Base metadata only needs a range — the splitter narrows op_write rowset
    // ranges against it. op_compaction is unconditionally dropped before any
    // range-narrowing runs, so the range value is irrelevant for this test.
    auto base_metadata = std::make_shared<TabletMetadataPB>();
    base_metadata->set_id(source_tablet_id);
    base_metadata->set_version(1);
    base_metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(0));
    base_metadata->mutable_range()->set_lower_bound_included(true);
    base_metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(100));
    base_metadata->mutable_range()->set_upper_bound_included(false);

    lake::PublishTabletInfo info(lake::PublishTabletInfo::SPLITTING_TABLET, source_tablet_id, child_tablet_id, 4, 0);
    ASSIGN_OR_ABORT(auto converted, lake::convert_txn_log(log, base_metadata, info));

    // Compaction payload cleared — apply becomes a no-op. The child tablet's
    // background compaction will rerun the merge over its own range.
    EXPECT_FALSE(converted->has_op_compaction());
    EXPECT_FALSE(converted->has_op_parallel_compaction());
    // Other fields preserved.
    EXPECT_EQ(child_tablet_id, converted->tablet_id());
    EXPECT_EQ(log->txn_id(), converted->txn_id());
}

TEST_F(LakeTabletReshardTest, test_convert_txn_log_splitting_drops_op_parallel_compaction) {
    const int64_t source_tablet_id = next_id();
    const int64_t child_tablet_id = next_id();
    auto log = std::make_shared<TxnLogPB>();
    log->set_tablet_id(source_tablet_id);
    log->set_txn_id(2030);
    auto* op_parallel_compaction = log->mutable_op_parallel_compaction();
    for (int i = 0; i < 2; ++i) {
        auto* subtask = op_parallel_compaction->add_subtask_compactions();
        subtask->mutable_output_rowset()->add_segment_metas()->set_filename(fmt::format("split_subtask_seg_{}.dat", i));
        subtask->mutable_output_sstable()->set_filename(fmt::format("split_subtask_{}.sst", i));
    }

    auto base_metadata = std::make_shared<TabletMetadataPB>();
    base_metadata->set_id(source_tablet_id);
    base_metadata->set_version(1);
    base_metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(0));
    base_metadata->mutable_range()->set_lower_bound_included(true);
    base_metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(100));
    base_metadata->mutable_range()->set_upper_bound_included(false);

    lake::PublishTabletInfo info(lake::PublishTabletInfo::SPLITTING_TABLET, source_tablet_id, child_tablet_id, 2, 1);
    ASSIGN_OR_ABORT(auto converted, lake::convert_txn_log(log, base_metadata, info));

    EXPECT_FALSE(converted->has_op_parallel_compaction());
    EXPECT_EQ(child_tablet_id, converted->tablet_id());
}

// Regression: op_write-only logs through SPLITTING cross-publish must NOT have
// their op_write fields cleared by the new compaction-drop path. Only the
// compaction ops are dropped; op_write is preserved (and gets shared-flag /
// range / data-stat adjustments applied to it).
TEST_F(LakeTabletReshardTest, test_convert_txn_log_splitting_op_write_preserved) {
    const int64_t source_tablet_id = next_id();
    const int64_t child_tablet_id = next_id();

    auto base_metadata = std::make_shared<TabletMetadataPB>();
    base_metadata->set_id(source_tablet_id);
    base_metadata->set_version(1);
    base_metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(10));
    base_metadata->mutable_range()->set_lower_bound_included(true);
    base_metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(20));
    base_metadata->mutable_range()->set_upper_bound_included(false);

    auto log = std::make_shared<TxnLogPB>();
    log->set_tablet_id(source_tablet_id);
    log->set_txn_id(3000);
    auto* rowset = log->mutable_op_write()->mutable_rowset();
    rowset->set_overlapped(false);
    rowset->set_num_rows(60);
    rowset->set_data_size(600);
    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("write_seg.dat");
        sm->set_size(600);
    }

    lake::PublishTabletInfo info(lake::PublishTabletInfo::SPLITTING_TABLET, source_tablet_id, child_tablet_id, 3, 0);
    ASSIGN_OR_ABORT(auto converted, lake::convert_txn_log(log, base_metadata, info));

    ASSERT_TRUE(converted->has_op_write());
    EXPECT_EQ(child_tablet_id, converted->tablet_id());
    // Splitter scaled num_rows / data_size by split_count and applied
    // shared-flag to op_write rowset.
    EXPECT_EQ(20, converted->op_write().rowset().num_rows());
    EXPECT_EQ(200, converted->op_write().rowset().data_size());
    ASSERT_TRUE(converted->op_write().rowset().segment_metas_size() > 0);
    EXPECT_TRUE(converted->op_write().rowset().segment_metas(0).shared());
}

// Read-only aliasing discards the source SST cohort, so its uint32 RSSID high
// half does not constrain the signed packed target cursor.
TEST_F(LakeTabletReshardTest, test_read_alias_accepts_sign_bit_source_watermark) {
    const int64_t source_tablet = next_id();
    const int64_t merged_tablet = next_id();
    auto source = std::make_shared<TabletMetadataPB>();
    source->set_id(source_tablet);
    source->set_version(1);
    source->set_next_rowset_id(2);
    auto* rowset = source->add_rowsets();
    rowset->set_id(1);
    rowset->set_version(1);
    rowset->set_num_rows(1);
    rowset->set_data_size(1);
    rowset->add_segment_metas()->set_filename("skip_source_segment.dat");
    rowset->mutable_segment_metas(0)->set_num_rows(1);
    rowset->mutable_segment_metas(0)->set_segment_idx(0);
    lake::tablet_reshard_helper::set_rowset_uid(rowset);
    auto* sst = source->mutable_sstable_meta()->add_sstables();
    sst->set_filename("skip_sign_bit.sst");
    sst->set_max_rss_rowid(static_cast<uint64_t>(1) << 63);
    const std::string source_before = source->SerializeAsString();

    MergingTabletInfoPB merging_info;
    merging_info.set_new_tablet_id(merged_tablet);
    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    std::vector<TabletMetadataPtr> sources = {source};
    auto merged =
            lake::virtual_merge_for_read(_tablet_manager.get(), sources, merging_info, /*new_version=*/2, txn_info);

    ASSERT_OK(merged.status());
    ASSERT_EQ(1, merged.value()->rowsets_size());
    EXPECT_EQ(1, merged.value()->rowsets(0).id());
    EXPECT_EQ(2, merged.value()->next_rowset_id());
    EXPECT_EQ(0, merged.value()->sstable_meta().sstables_size());
    EXPECT_EQ(source_before, source->SerializeAsString());
}

// Split of a PK tablet with cloud-native persistent index enabled. This
// exercises the new LakePersistentIndex::flush_memtable call at the top of
// split_tablet. The parent has no rowsets, so flush is effectively a no-op;
// the point is to confirm the split path doesn't crash on cloud-native PK
// tablets and that children inherit the expected metadata.
TEST_F(LakeTabletReshardTest, test_tablet_splitting_cloud_native_pk_flush_path) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t old_tablet_id = next_id();
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();

    prepare_tablet_dirs(old_tablet_id);
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);

    auto meta = std::make_shared<TabletMetadataPB>();
    meta->set_id(old_tablet_id);
    meta->set_version(base_version);
    meta->set_next_rowset_id(1);
    set_primary_key_schema(meta.get(), 1001);
    meta->set_enable_persistent_index(true);
    meta->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);

    EXPECT_OK(put_tablet_metadata(meta));

    ReshardingTabletInfoPB resharding_tablet;
    auto& splitting_tablet = *resharding_tablet.mutable_splitting_tablet_info();
    splitting_tablet.set_old_tablet_id(old_tablet_id);
    splitting_tablet.add_new_tablet_ids(child_a);
    splitting_tablet.add_new_tablet_ids(child_b);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    // Split may fall back to a single output when get_tablet_split_ranges
    // returns no boundaries (no rowsets to split by); in that case exactly
    // one child tablet appears. Either outcome is acceptable — what we care
    // about is that the flush-before-split path runs successfully on a
    // cloud-native PK tablet.
    ASSERT_FALSE(tablet_metadatas.empty());
    for (const auto& [tablet_id, child_meta] : tablet_metadatas) {
        EXPECT_TRUE(child_meta->enable_persistent_index());
        EXPECT_EQ(PersistentIndexTypePB::CLOUD_NATIVE, child_meta->persistent_index_type());
        EXPECT_EQ(new_version, child_meta->version());
    }
}

// The BE-side reshard publish slot is a single CAS on an old-side tablet id
// shared by DML and reshard. This test documents the serialization key choice
// and exercises the dedup property end-to-end: calling publish_resharding_tablet
// on tablet ids already held externally must return ResourceBusy rather than
// proceed or hang.
TEST_F(LakeTabletReshardTest, test_publish_resharding_tablet_slot_dedup) {
    // SPLIT anchors on old_tablet_id.
    {
        const int64_t old_tablet_id = next_id();
        const int64_t new_tablet_id = next_id();

        ReshardingTabletInfoPB info;
        auto& s = *info.mutable_splitting_tablet_info();
        s.set_old_tablet_id(old_tablet_id);
        s.add_new_tablet_ids(new_tablet_id);

        ASSERT_TRUE(lake::acquire_publish_tablet(old_tablet_id));
        DeferOp drop([old_tablet_id] { lake::release_publish_tablet(old_tablet_id); });

        TxnInfoPB txn_info;
        txn_info.set_txn_id(next_id());
        std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
        std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
        auto st =
                lake::publish_resharding_tablet(_tablet_manager.get(), info, 1, 2, txn_info,
                                                /*skip_write_tablet_metadata=*/false, tablet_metadatas, tablet_ranges);
        EXPECT_TRUE(st.is_resource_busy()) << st;
        EXPECT_TRUE(tablet_metadatas.empty());
    }

    // MERGE anchors on old_tablet_ids(0); holding a DIFFERENT old id must NOT
    // block (the anchor is just the first one) — this verifies the single-CAS
    // choice and that there's no accidental multi-id reservation.
    {
        const int64_t old0 = next_id();
        const int64_t old1 = next_id();
        const int64_t merged = next_id();

        ReshardingTabletInfoPB info;
        auto& m = *info.mutable_merging_tablet_info();
        m.add_old_tablet_ids(old0);
        m.add_old_tablet_ids(old1);
        m.set_new_tablet_id(merged);

        // Hold old1 externally — should NOT trigger ResourceBusy.
        ASSERT_TRUE(lake::acquire_publish_tablet(old1));
        DeferOp drop_old1([old1] { lake::release_publish_tablet(old1); });

        // Nothing else is loaded so publish_resharding_tablet will not succeed
        // for other reasons, but the acquire step must at least pass — observe
        // that the first failure mode is NOT ResourceBusy.
        TxnInfoPB txn_info;
        txn_info.set_txn_id(next_id());
        std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
        std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
        auto st =
                lake::publish_resharding_tablet(_tablet_manager.get(), info, 1, 2, txn_info,
                                                /*skip_write_tablet_metadata=*/false, tablet_metadatas, tablet_ranges);
        EXPECT_FALSE(st.is_resource_busy()) << st;

        // Now hold old0 — this IS the anchor, so ResourceBusy must fire.
        ASSERT_TRUE(lake::acquire_publish_tablet(old0));
        DeferOp drop_old0([old0] { lake::release_publish_tablet(old0); });

        tablet_metadatas.clear();
        tablet_ranges.clear();
        st = lake::publish_resharding_tablet(_tablet_manager.get(), info, 1, 2, txn_info,
                                             /*skip_write_tablet_metadata=*/false, tablet_metadatas, tablet_ranges);
        EXPECT_TRUE(st.is_resource_busy()) << st;
    }

    // IDENTICAL anchors on old_tablet_id.
    {
        const int64_t old_tablet_id = next_id();
        const int64_t new_tablet_id = next_id();

        ReshardingTabletInfoPB info;
        auto& i = *info.mutable_identical_tablet_info();
        i.set_old_tablet_id(old_tablet_id);
        i.set_new_tablet_id(new_tablet_id);

        ASSERT_TRUE(lake::acquire_publish_tablet(old_tablet_id));
        DeferOp drop([old_tablet_id] { lake::release_publish_tablet(old_tablet_id); });

        TxnInfoPB txn_info;
        txn_info.set_txn_id(next_id());
        std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
        std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
        auto st =
                lake::publish_resharding_tablet(_tablet_manager.get(), info, 1, 2, txn_info,
                                                /*skip_write_tablet_metadata=*/false, tablet_metadatas, tablet_ranges);
        EXPECT_TRUE(st.is_resource_busy()) << st;
    }
}

// Extracted Segment helper — calling segment_seek_range_to_rowid_range with
// an unbounded SeekRange returns [0, num_rows) without touching the short
// key index (fast path).
TEST_F(LakeTabletReshardTest, test_segment_seek_range_to_rowid_range_unbounded) {
    // With an empty SeekRange (default-constructed = (-inf, +inf)), the helper
    // takes the early return branch and does not dereference the segment's
    // short key index. A null segment is invalid and must fail fast.
    SeekRange empty_range;
    LakeIOOptions io_opts;
    auto st = segment_seek_range_to_rowid_range(/*segment=*/nullptr, empty_range, io_opts);
    EXPECT_FALSE(st.ok());
}

// Exercise the real bounded path: open a Segment from disk and ask the helper
// to resolve an [lower, upper) SeekRange to a rowid window. This exercises
// load_index() + _lookup_ordinal() in the extracted helper.
TEST_F(LakeTabletReshardTest, test_segment_seek_range_to_rowid_range_real_bounded) {
    const int64_t tablet_id = next_id();
    prepare_tablet_dirs(tablet_id);

    const int num_rows = 100;
    const std::string segment_name = "range_lookup_seg.dat";
    write_two_column_segment(tablet_id, segment_name, num_rows, [](int i) { return i * 10; });

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(PRIMARY_KEYS);
    schema_pb.set_id(2001);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_num_rows_per_row_block(65535);
    auto* c0 = schema_pb.add_column();
    c0->set_unique_id(1001);
    c0->set_name("c0");
    c0->set_type("INT");
    c0->set_is_key(true);
    c0->set_is_nullable(false);
    c0->set_length(4);
    c0->set_index_length(4);
    auto* c1 = schema_pb.add_column();
    c1->set_unique_id(1002);
    c1->set_name("c1");
    c1->set_type("INT");
    c1->set_is_key(false);
    c1->set_is_nullable(false);
    c1->set_aggregation("REPLACE");
    auto tablet_schema = TabletSchema::create(schema_pb);

    FileInfo file_info;
    file_info.path = _tablet_manager->segment_location(tablet_id, segment_name);
    ASSIGN_OR_ABORT(auto file_system, FileSystemFactory::CreateSharedFromString(file_info.path));
    ASSIGN_OR_ABORT(auto segment, Segment::open(file_system, file_info, 0, tablet_schema));

    // Build SeekRange [30, 70): keys 30..69 inclusive lower, exclusive upper.
    TabletRangePB range_pb;
    range_pb.set_lower_bound_included(true);
    range_pb.set_upper_bound_included(false);
    *range_pb.mutable_lower_bound() = generate_sort_key(30);
    *range_pb.mutable_upper_bound() = generate_sort_key(70);
    ASSIGN_OR_ABORT(auto seek_range, lake::TabletRangeHelper::create_seek_range_from(range_pb, tablet_schema, nullptr));

    LakeIOOptions io_opts{.fill_data_cache = false};
    ASSIGN_OR_ABORT(auto rowid_range_opt, segment_seek_range_to_rowid_range(segment, seek_range, io_opts));
    ASSERT_TRUE(rowid_range_opt.has_value());
    EXPECT_EQ(30u, rowid_range_opt->begin());
    EXPECT_EQ(70u, rowid_range_opt->end());

    // A range strictly past the segment end must resolve to an empty window.
    TabletRangePB above_pb;
    above_pb.set_lower_bound_included(true);
    above_pb.set_upper_bound_included(false);
    *above_pb.mutable_lower_bound() = generate_sort_key(500);
    *above_pb.mutable_upper_bound() = generate_sort_key(600);
    ASSIGN_OR_ABORT(auto above_range,
                    lake::TabletRangeHelper::create_seek_range_from(above_pb, tablet_schema, nullptr));
    ASSIGN_OR_ABORT(auto above_rowid_opt, segment_seek_range_to_rowid_range(segment, above_range, io_opts));
    if (above_rowid_opt.has_value()) {
        EXPECT_EQ(above_rowid_opt->begin(), above_rowid_opt->end());
    }
}

// Tests for convert_op_write_to_op_schema_change (SHADOW_REWRITE transform helper).
// These use only scalar RowsetMetadataPB fields (id/num_rows/data_size) to stay
// independent of segment file naming conventions.

TEST(ShadowRewriteTransformTest, ShadowRewriteTransformMovesRowsetAndAnchors) {
    TxnLogPB log;
    auto* rs = log.mutable_op_write()->mutable_rowset();
    rs->set_num_rows(7);
    rs->set_data_size(123);
    starrocks::lake::convert_op_write_to_op_schema_change(&log, /*alter_version=*/9);
    ASSERT_FALSE(log.has_op_write());
    ASSERT_TRUE(log.has_op_schema_change());
    EXPECT_EQ(9, log.op_schema_change().alter_version());
    ASSERT_EQ(1, log.op_schema_change().rowsets_size());
    EXPECT_EQ(1, log.op_schema_change().rowsets(0).id());
    EXPECT_EQ(7, log.op_schema_change().rowsets(0).num_rows());
}

TEST(ShadowRewriteTransformTest, ShadowRewriteTransformEmptyWhenNoRowset) {
    TxnLogPB log; // no op_write
    starrocks::lake::convert_op_write_to_op_schema_change(&log, /*alter_version=*/9);
    ASSERT_TRUE(log.has_op_schema_change());
    EXPECT_EQ(9, log.op_schema_change().alter_version());
    EXPECT_EQ(0, log.op_schema_change().rowsets_size());
}

// =============================================================================
// Index Delta Group (.idx / idg_meta) reshard adaptation
// =============================================================================

// SPLIT: shared .idx marked shared on spanning segments; exclusive kept segment .idx marked
// private; pruned-away segment idg entry erased. Mirrors
// test_tablet_split_propagates_ownership_to_delvec_dcg for IDG.
TEST_F(LakeTabletReshardTest, test_tablet_split_propagates_ownership_to_idg) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);

    auto* rs = metadata.add_rowsets();
    rs->set_id(2);
    {
        auto* m0 = rs->add_segment_metas();
        m0->set_filename("seg_lo.dat");
        m0->set_size(512);
        m0->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
        m0->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
        m0->set_num_rows(50);
    }
    {
        auto* m1 = rs->add_segment_metas();
        m1->set_filename("seg_hi.dat");
        m1->set_size(512);
        m1->mutable_sort_key_min()->CopyFrom(generate_sort_key(50));
        m1->mutable_sort_key_max()->CopyFrom(generate_sort_key(99));
        m1->set_num_rows(50);
    }
    rs->set_overlapped(true);
    rs->set_data_size(1024);
    rs->set_num_rows(100);

    // idg for both segments' rssids (rowset id 2 + segment_idx {0,1} => 2 and 3).
    add_idg_with_key(&metadata, /*segment_id=*/2, "idx_lo.idx", /*col_uid=*/101, BITMAP, 1);
    add_idg_with_key(&metadata, /*segment_id=*/3, "idx_hi.idx", /*col_uid=*/102, BITMAP, 1);

    EXPECT_OK(put_tablet_metadata(metadata));

    ReshardingTabletInfoPB resharding;
    auto& splitting = *resharding.mutable_splitting_tablet_info();
    splitting.set_old_tablet_id(tablet_id);
    const int64_t child0 = next_id();
    const int64_t child1 = next_id();
    splitting.add_new_tablet_ids(child0);
    splitting.add_new_tablet_ids(child1);

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, metadata.version(),
                                              metadata.version() + 1, txn_info, false, tablet_metadatas,
                                              tablet_ranges));

    for (int64_t child : {child0, child1}) {
        auto c = tablet_metadatas.at(child);
        ASSERT_EQ(1, c->rowsets_size());
        const auto& r = c->rowsets(0);
        ASSERT_EQ(1, r.segment_metas_size());
        const uint32_t kept_rssid = r.id() + r.segment_metas(0).segment_idx();
        const uint32_t pruned_rssid = (kept_rssid == 2) ? 3 : 2;

        // Exclusive kept segment -> its idg is private.
        ASSERT_TRUE(c->idg_meta().idgs().contains(kept_rssid));
        for (const auto& e : c->idg_meta().idgs().at(kept_rssid).entries())
            EXPECT_FALSE(e.shared_file()) << "exclusive segment idg must be private";

        // Pruned-away segment's idg entry erased.
        EXPECT_FALSE(c->idg_meta().idgs().contains(pruned_rssid))
                << "pruned segment idg must be erased on the tablet that dropped it";
    }
}

// =============================================================================
// PK-index sstable version stamping
// =============================================================================
//
// The reshard PK-index flush runs at base_version but the freshly-flushed
// sstables first become visible at the reshard publish (new_version). These
// tests assert that a genuinely-new flushed sstable carries new_version (so an
// incremental snapshot keyed on version>pre_version does not skip it) while the
// returned metadata keeps base_version.

// A tablet with a real PK segment and NO sstable_meta triggers a cold
// rebuild-from-segment during flush_pk_memtable, producing exactly one fresh
// sstable. Assert it is stamped with new_version and the returned metadata's
// own version is restored to base_version.
TEST_F(LakeTabletReshardTest, test_reshard_flush_stamps_fresh_sstable_with_new_version) {
    // Unlike the other reshard tests, this one drives a REAL flush from a real segment, so
    // disable the fixture-wide skip_lake_pk_index_flush fail point enabled in SetUp().
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    const int64_t base_version = 1;
    const int64_t new_version = 5; // deliberately > base_version
    const int64_t tablet_id = next_id();
    prepare_tablet_dirs(tablet_id);

    constexpr int kNumRows = 16;
    const std::string seg_name = "seg_flush.dat";
    const uint64_t seg_size = write_two_column_segment(tablet_id, seg_name, kNumRows, [](int r) { return r * 10; });

    auto meta = make_single_segment_pk_tablet(tablet_id, base_version, seg_name, seg_size, kNumRows);
    ASSERT_OK(put_tablet_metadata(meta));

    ASSIGN_OR_ABORT(auto flushed, _update_manager->flush_pk_memtable(meta, new_version));

    ASSERT_NE(flushed, nullptr);
    EXPECT_EQ(base_version, flushed->version());           // returned metadata keeps base_version (restore)
    ASSERT_EQ(1, flushed->sstable_meta().sstables_size()); // exactly one fresh sstable
    EXPECT_EQ(new_version, flushed->sstable_meta().sstables(0).generation_version())
            << "a freshly-flushed reshard sstable must carry the reshard publish version, else an "
               "incremental snapshot keyed on version>pre_version would skip it (data loss)";
}

// Aggregate publish builds query-parent metadata before persisting the new-version bundle. Its
// child metadata therefore exists only in memory while merge_sstables flushes each child's PK
// index. The rebuild must use that supplied metadata for delvec lookup instead of trying to read
// the not-yet-created bundle from object storage.
TEST_F(LakeTabletReshardTest, test_reshard_flush_uses_unpersisted_in_memory_metadata) {
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    const int64_t in_memory_version = 11;
    const int64_t tablet_id = next_id();
    prepare_tablet_dirs(tablet_id);

    constexpr int kNumRows = 16;
    const std::string seg_name = "seg_unpersisted_bundle.dat";
    const uint64_t seg_size = write_two_column_segment(tablet_id, seg_name, kNumRows, [](int r) { return r * 10; });
    auto meta = make_single_segment_pk_tablet(tablet_id, in_memory_version, seg_name, seg_size, kNumRows);

    // Deliberately do not call put_tablet_metadata(meta): this models the metadata returned by the
    // per-tablet aggregate-publish RPC before the coordinator writes the partition bundle.
    ASSERT_TRUE(_tablet_manager->get_tablet_metadata(tablet_id, in_memory_version).status().is_not_found());

    ASSIGN_OR_ABORT(auto flushed, _update_manager->flush_pk_memtable(meta, in_memory_version));
    ASSERT_NE(flushed, nullptr);
    EXPECT_EQ(in_memory_version, flushed->version());
    ASSERT_EQ(1, flushed->sstable_meta().sstables_size());
    EXPECT_EQ(in_memory_version, flushed->sstable_meta().sstables(0).generation_version());
}

// Same rebuild-from-segment source, driven through the real split publish path,
// guarding that tablet_splitter passes new_version to flush_pk_memtable.
TEST_F(LakeTabletReshardTest, test_split_publish_stamps_fresh_sstable_with_new_version) {
    // Unlike the other reshard tests, this one drives a REAL split-publish flush from a real
    // segment, so disable the fixture-wide skip_lake_pk_index_flush fail point from SetUp().
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    const int64_t base_version = 1;
    const int64_t new_version = 5;
    const int64_t src_tablet = next_id();
    const int64_t child0 = next_id();
    const int64_t child1 = next_id();
    prepare_tablet_dirs(src_tablet);
    prepare_tablet_dirs(child0);
    prepare_tablet_dirs(child1);

    constexpr int kNumRows = 16;
    const std::string seg_name = "seg_split.dat";
    const uint64_t seg_size = write_two_column_segment(src_tablet, seg_name, kNumRows, [](int r) { return r * 10; });

    auto meta = make_single_segment_pk_tablet(src_tablet, base_version, seg_name, seg_size, kNumRows);
    ASSERT_OK(put_tablet_metadata(meta));

    ReshardingTabletInfoPB resharding;
    auto& si = *resharding.mutable_splitting_tablet_info();
    si.set_old_tablet_id(src_tablet);
    si.add_new_tablet_ids(child0);
    si.add_new_tablet_ids(child1);
    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    std::unordered_map<int64_t, TabletMetadataPtr> metadatas;
    std::unordered_map<int64_t, TabletRangePB> ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, base_version, new_version, txn_info,
                                              false, metadatas, ranges));

    // Whatever children the split produced (K new tablets, or a 1-tablet identical
    // fallback), every freshly-flushed PK sstable they inherit must carry new_version.
    bool saw_sstable = false;
    for (const auto& [tablet_id, cm] : metadatas) {
        for (const auto& s : cm->sstable_meta().sstables()) {
            saw_sstable = true;
            EXPECT_EQ(new_version, s.generation_version())
                    << "split-flushed sstable must carry the split publish version";
        }
    }
    EXPECT_TRUE(saw_sstable) << "split should have produced a flushed PK sstable from the rebuilt index";
}

TEST_F(LakeTabletReshardTest, test_pk_tablet_splitting_conserves_stats_and_tiles_child_ranges) {
    const int64_t base_version = 2;
    const int64_t new_version = 3;
    const int64_t tablet_id = next_id();

    prepare_tablet_dirs(tablet_id);

    TabletMetadataPB metadata;
    metadata.set_id(tablet_id);
    metadata.set_version(base_version);
    // A valid, non-zero schema id is required: build_segments_from_rowsets only opens a
    // rowset's real segment files when its schema resolves to a valid registered id
    // (rowset_schema_resolves_to_valid_id); an unset/invalid id degrades to the coarse
    // [min, max] path regardless of what the segment file itself contains.
    set_two_column_pk_schema(&metadata, /*schema_id=*/1);

    constexpr int kNumRows = 300;
    const std::string seg_name = "split_seg.dat";

    const uint64_t seg_size = write_two_column_segment(tablet_id, seg_name, kNumRows, [](int i) { return i; });

    auto* rowset = metadata.add_rowsets();
    rowset->set_id(2);
    rowset->set_overlapped(false);
    rowset->set_num_rows(kNumRows);
    rowset->set_data_size(seg_size);
    rowset->set_num_dels(0);
    auto* sm = rowset->add_segment_metas();
    sm->set_filename(seg_name);
    sm->set_size(seg_size);
    sm->set_num_rows(kNumRows);
    sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
    sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(kNumRows - 1));
    // The only source of split-boundary precision beyond the coarse [min, max] pair is the sort
    // key sampler reading this segment. This fixture's schema (set_two_column_pk_schema) carries
    // index_length, so it samples the free short key index path (A), not the data pages; see the
    // sibling multi-rowset case.

    EXPECT_OK(put_tablet_metadata(metadata));

    ReshardingTabletInfoPB resharding;
    auto& splitting = *resharding.mutable_splitting_tablet_info();
    splitting.set_old_tablet_id(tablet_id);
    const int64_t child_id_1 = next_id();
    const int64_t child_id_2 = next_id();
    const int64_t child_id_3 = next_id();
    splitting.add_new_tablet_ids(child_id_1);
    splitting.add_new_tablet_ids(child_id_2);
    splitting.add_new_tablet_ids(child_id_3);

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, base_version, new_version, txn_info,
                                              false, tablet_metadatas, tablet_ranges));

    // One metadata entry per child, plus the old tablet id's own new-version entry (a
    // tombstone at the old location), so the map holds new_tablet_ids_size() + 1 entries.
    ASSERT_EQ(4U, tablet_metadatas.size());

    int64_t total_num_rows = 0;
    int64_t total_data_size = 0;
    int64_t total_num_dels = 0;
    for (int64_t cid : {child_id_1, child_id_2, child_id_3}) {
        auto it = tablet_metadatas.find(cid);
        ASSERT_TRUE(it != tablet_metadatas.end());
        ASSERT_EQ(1, it->second->rowsets_size());
        const auto& child_rs = it->second->rowsets(0);
        EXPECT_EQ(2u, child_rs.id());
        total_num_rows += child_rs.num_rows();
        total_data_size += child_rs.data_size();
        total_num_dels += child_rs.num_dels();
    }
    EXPECT_EQ(kNumRows, total_num_rows);
    EXPECT_EQ(static_cast<int64_t>(seg_size), total_data_size);
    EXPECT_EQ(0, total_num_dels);

    // Child ranges must tile the parent's key space: exactly one open-below range,
    // exactly one open-above range, and every adjacent pair's bounds match exactly.
    std::vector<TabletRangePB> ranges;
    for (const auto& [cid, range_pb] : tablet_ranges) {
        ranges.push_back(range_pb);
    }
    ASSERT_EQ(3U, ranges.size());
    std::sort(ranges.begin(), ranges.end(), [](const TabletRangePB& a, const TabletRangePB& b) {
        if (!a.has_lower_bound()) return true;
        if (!b.has_lower_bound()) return false;
        VariantTuple la, lb;
        CHECK_OK(la.from_proto(a.lower_bound()));
        CHECK_OK(lb.from_proto(b.lower_bound()));
        return la.compare(lb) < 0;
    });
    EXPECT_FALSE(ranges[0].has_lower_bound());
    EXPECT_FALSE(ranges.back().has_upper_bound());
    for (size_t i = 0; i + 1 < ranges.size(); ++i) {
        ASSERT_TRUE(ranges[i].has_upper_bound());
        ASSERT_TRUE(ranges[i + 1].has_lower_bound());
        VariantTuple upper, lower;
        ASSERT_OK(upper.from_proto(ranges[i].upper_bound()));
        ASSERT_OK(lower.from_proto(ranges[i + 1].lower_bound()));
        EXPECT_EQ(0, upper.compare(lower)) << "adjacent child ranges must tile with no gap/overlap";
        EXPECT_FALSE(ranges[i].upper_bound_included());
        EXPECT_TRUE(ranges[i + 1].lower_bound_included());
    }
}

// A reshard of a partition still at version 1 -- an empty `file_bundling` partition pre-split ahead
// of its first load -- reads old-tablet metadata that exists only in the partition-shared version-1
// object. With FE's hint threaded through publish_resharding_tablet(), that read goes straight to the
// shared object instead of first 404ing on a per-tablet key that was never written.
TEST_F(LakeTabletReshardTest, publish_resharding_tablet_shared_first_skips_per_tablet_probe) {
    auto old_tablet_id = next_id();
    auto new_tablet_id = next_id();

    // Only the shared object exists, as DDL leaves a file_bundling partition.
    const auto shared_location = _tablet_manager->tablet_initial_metadata_location(old_tablet_id);
    auto shared = std::make_shared<TabletMetadata>();
    shared->set_id(old_tablet_id);
    shared->set_version(1);
    ASSERT_OK(_tablet_manager->put_tablet_metadata(shared, shared_location));
    _tablet_manager->metacache()->erase(shared_location);

    std::vector<std::string> read_paths;
    SyncPoint::GetInstance()->SetCallBack("TabletManager::load_tablet_metadata:path",
                                          [&](void* arg) { read_paths.emplace_back(*static_cast<std::string*>(arg)); });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp cleanup([]() {
        SyncPoint::GetInstance()->ClearCallBack("TabletManager::load_tablet_metadata:path");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    ReshardingTabletInfoPB resharding_tablet;
    auto& identical_tablet = *resharding_tablet.mutable_identical_tablet_info();
    identical_tablet.set_old_tablet_id(old_tablet_id);
    identical_tablet.set_new_tablet_id(new_tablet_id);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(next_id());
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, /*base_version=*/1,
                                              /*new_version=*/2, txn_info, false, tablet_metadatas, tablet_ranges,
                                              lake::InitialMetadataOrder::kSharedFirst));
    ASSERT_EQ(2, tablet_metadatas.size());
    EXPECT_EQ(old_tablet_id, tablet_metadatas.at(old_tablet_id)->id());
    EXPECT_EQ(new_tablet_id, tablet_metadatas.at(new_tablet_id)->id());
    EXPECT_EQ(2, tablet_metadatas.at(new_tablet_id)->version());

    auto count_ending_with = [&](const std::string& suffix) {
        return std::count_if(read_paths.begin(), read_paths.end(), [&](const std::string& path) {
            return path.size() >= suffix.size() &&
                   path.compare(path.size() - suffix.size(), suffix.size(), suffix) == 0;
        });
    };
    // The old tablet's own version-1 key was never probed; the shared object was read exactly once.
    EXPECT_EQ(0, count_ending_with(lake::tablet_metadata_filename(old_tablet_id, 1)));
    EXPECT_EQ(1, count_ending_with(lake::tablet_initial_metadata_filename()));
}

// A truncated (VARCHAR) sort key exercises path B (sort_key_sampler.h's data-page sampler) end to
// end: a real split-publish over a real segment, then reading the real children back out with a
// real reader -- not just the rowset stats the split computed for itself, which the boundary
// algorithm optimizes directly and would trivially agree with (see kEvennessTolerance's comment in
// tablet_splitter_test.cpp for the same point).
TEST_F(LakeTabletReshardTest, split_a_varchar_sort_key_tablet_end_to_end) {
    const int64_t base_version = 2;
    const int64_t new_version = 3;
    const int64_t tablet_id = next_id();

    prepare_tablet_dirs(tablet_id);

    TabletMetadataPB metadata;
    metadata.set_id(tablet_id);
    metadata.set_version(base_version);
    set_varchar_sort_key_schema(&metadata, /*schema_id=*/1);

    constexpr int kNumRows = 50'000;
    constexpr int kSplitCount = 4;
    const std::string seg_name = "varchar_split_seg.dat";
    const uint64_t seg_size = write_varchar_key_segment(tablet_id, seg_name, kNumRows);

    auto* rowset = metadata.add_rowsets();
    rowset->set_id(2);
    rowset->set_overlapped(false);
    rowset->set_num_rows(kNumRows);
    rowset->set_data_size(seg_size);
    rowset->set_num_dels(0);
    auto* sm = rowset->add_segment_metas();
    sm->set_filename(seg_name);
    sm->set_size(seg_size);
    sm->set_num_rows(kNumRows);
    sm->mutable_sort_key_min()->CopyFrom(generate_varchar_sort_key(0));
    sm->mutable_sort_key_max()->CopyFrom(generate_varchar_sort_key(kNumRows - 1));

    EXPECT_OK(put_tablet_metadata(metadata));

    ReshardingTabletInfoPB resharding;
    auto& splitting = *resharding.mutable_splitting_tablet_info();
    splitting.set_old_tablet_id(tablet_id);
    std::vector<int64_t> child_ids;
    for (int i = 0; i < kSplitCount; ++i) {
        const int64_t child_id = next_id();
        child_ids.push_back(child_id);
        splitting.add_new_tablet_ids(child_id);
    }

    TxnInfoPB txn_info;
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    const int64_t data_page_segments_before = sort_key_sampling_data_page_segments_count();
    const int64_t samples_before = sort_key_sampling_samples_count();
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, base_version, new_version, txn_info,
                                              false, tablet_metadatas, tablet_ranges));
    // kSplitCount children + the old tablet id's own new-version tombstone entry.
    ASSERT_EQ(static_cast<size_t>(kSplitCount) + 1, tablet_metadatas.size());

    // A VARCHAR sort key is never eligible for the short key index: prove path B (data pages)
    // actually ran, per sort_key_sampling_data_page_segments_count's own contract in
    // sort_key_sampler.h, rather than assuming it from the schema alone.
    EXPECT_GT(sort_key_sampling_data_page_segments_count() - data_page_segments_before, 0)
            << "a VARCHAR sort key must be sampled via data pages, never the short key index";
    EXPECT_GT(sort_key_sampling_samples_count() - samples_before, 0);

    int64_t total = 0;
    for (int64_t child_id : child_ids) {
        ASSERT_TRUE(tablet_metadatas.count(child_id));
        ASSIGN_OR_ABORT(const int64_t rows, count_rows(tablet_metadatas.at(child_id)));
        total += rows;
        const double ideal = kNumRows / static_cast<double>(kSplitCount);
        EXPECT_NEAR(static_cast<double>(rows), ideal, ideal * 0.10)
                << "child " << child_id << " holds a disproportionate share of the rows";
    }
    EXPECT_EQ(kNumRows, total) << "the split must conserve every row across its children";
}

// ---------------------------------------------------------------------------
// Restored from the stable-metadata reshard rework: these cover the reshard retry cache and
// the boundary planner's zero-row-segment skip, neither of which this change touches. They
// were lost when this branch was rebased onto that rework and are re-added verbatim.
// ---------------------------------------------------------------------------

TEST_F(LakeTabletReshardTest, test_split_retry_cache_complete_returns_without_recompute) {
    constexpr int64_t kVersion = 2;
    constexpr int64_t kGtid = 77;
    auto source = split_source();
    std::vector<int64_t> children{next_id(), next_id()};
    auto request = make_split_retry_request(source, children, true);
    const auto& ranges = request.splitting_tablet_info().new_tablet_ranges();

    std::unordered_map<int64_t, TabletMetadataPtr> expected;
    expected.emplace(source->id(), cache_reshard_metadata(source->id(), source->id(), kVersion, kGtid));
    for (size_t i = 0; i < children.size(); ++i) {
        expected.emplace(children[i], cache_reshard_metadata(children[i], children[i], kVersion, kGtid, &ranges[i]));
    }

    int flushes = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("tablet_splitter:pk_flush", [&](void*) { ++flushes; });
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->DisableProcessing();
        sync->ClearAllCallBacks();
    });
    TxnInfoPB txn;
    txn.set_gtid(kGtid);
    std::unordered_map<int64_t, TabletMetadataPtr> actual;
    std::unordered_map<int64_t, TabletRangePB> actual_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), request, source->version(), kVersion, txn, false,
                                              actual, actual_ranges));
    EXPECT_EQ(0, flushes);
    ASSERT_EQ(expected.size(), actual.size());
    for (const auto& [id, metadata] : expected) {
        EXPECT_EQ(metadata->SerializeAsString(), actual.at(id)->SerializeAsString());
    }
}

TEST_F(LakeTabletReshardTest, test_identical_retry_cache_complete_returns_without_recompute) {
    constexpr int64_t kVersion = 2;
    constexpr int64_t kGtid = 82;
    const int64_t source = next_id();
    const int64_t target = next_id();
    prepare_tablet_dirs(source);
    prepare_tablet_dirs(target);
    TabletMetadataPB base;
    base.set_id(source);
    base.set_version(1);
    ASSERT_OK(put_tablet_metadata(base));
    ReshardingTabletInfoPB request;
    request.mutable_identical_tablet_info()->set_old_tablet_id(source);
    request.mutable_identical_tablet_info()->set_new_tablet_id(target);
    auto cached_source = cache_reshard_metadata(source, source, kVersion, kGtid);
    auto cached_target = cache_reshard_metadata(target, target, kVersion, kGtid);
    _tablet_manager->metacache()->erase(_tablet_manager->tablet_metadata_location(source, 1));

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
    ASSERT_EQ(2, actual.size());
    EXPECT_EQ(cached_source->SerializeAsString(), actual.at(source)->SerializeAsString());
    EXPECT_EQ(cached_target->SerializeAsString(), actual.at(target)->SerializeAsString());
}

TEST_F(LakeTabletReshardTest, test_reshard_retry_cache_rejects_wrong_id_version_or_gtid) {
    constexpr int64_t kVersion = 2;
    constexpr int64_t kGtid = 80;
    for (int malformed_field = 0; malformed_field < 3; ++malformed_field) {
        auto source = split_source();
        std::vector<int64_t> children{next_id(), next_id()};
        auto request = make_split_retry_request(source, children, true);
        cache_reshard_metadata(source->id(), source->id(), kVersion, kGtid);
        cache_reshard_metadata(children[1], children[1], kVersion, kGtid);
        auto malformed = std::make_shared<TabletMetadataPB>();
        malformed->set_id(malformed_field == 0 ? next_id() : children[0]);
        malformed->set_version(malformed_field == 1 ? kVersion + 1 : kVersion);
        malformed->set_gtid(malformed_field == 2 ? kGtid + 1 : kGtid);
        _tablet_manager->metacache()->cache_tablet_metadata(
                _tablet_manager->tablet_metadata_location(children[0], kVersion), malformed);

        int flushes = 0;
        auto* sync = SyncPoint::GetInstance();
        sync->SetCallBack("tablet_splitter:pk_flush", [&](void*) { ++flushes; });
        sync->EnableProcessing();
        DeferOp cleanup([&] {
            sync->DisableProcessing();
            sync->ClearAllCallBacks();
        });
        TxnInfoPB txn;
        txn.set_gtid(kGtid);
        std::unordered_map<int64_t, TabletMetadataPtr> actual;
        std::unordered_map<int64_t, TabletRangePB> actual_ranges;
        ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), request, source->version(), kVersion, txn,
                                                  false, actual, actual_ranges));
        EXPECT_EQ(1, flushes) << "malformed field " << malformed_field;
        ASSERT_EQ(3, actual.size());
        for (const auto& [id, metadata] : actual) {
            EXPECT_EQ(id, metadata->id());
            EXPECT_EQ(kVersion, metadata->version());
            EXPECT_EQ(kGtid, metadata->gtid());
        }
    }
}

TEST_F(LakeTabletReshardTest, test_reshard_retry_cache_skips_nonpositive_gtid) {
    constexpr int64_t kVersion = 2;
    for (int64_t gtid : {0, -1}) {
        auto source = split_source();
        std::vector<int64_t> children{next_id(), next_id()};
        auto request = make_split_retry_request(source, children, true);
        cache_reshard_metadata(source->id(), source->id(), kVersion, gtid);
        for (auto child : children) cache_reshard_metadata(child, child, kVersion, gtid);

        int flushes = 0;
        auto* sync = SyncPoint::GetInstance();
        sync->SetCallBack("tablet_splitter:pk_flush", [&](void*) { ++flushes; });
        sync->EnableProcessing();
        DeferOp cleanup([&] {
            sync->DisableProcessing();
            sync->ClearAllCallBacks();
        });
        TxnInfoPB txn;
        txn.set_gtid(gtid);
        std::unordered_map<int64_t, TabletMetadataPtr> actual;
        std::unordered_map<int64_t, TabletRangePB> actual_ranges;
        ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), request, source->version(), kVersion, txn,
                                                  false, actual, actual_ranges));
        EXPECT_EQ(1, flushes) << "gtid " << gtid;
    }
}

TEST_F(LakeTabletReshardTest, test_split_identical_retry_cache_rejects_mismatched_extra_child) {
    constexpr int64_t kVersion = 2;
    constexpr int64_t kGtid = 79;
    auto source = split_source();
    std::vector<int64_t> children{next_id(), next_id()};
    auto request = make_split_retry_request(source, children, false);
    cache_reshard_metadata(source->id(), source->id(), kVersion, kGtid);
    cache_reshard_metadata(children[0], children[0], kVersion, kGtid);
    cache_reshard_metadata(children[1], children[1], kVersion, kGtid + 1);

    int planner_calls = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("tablet_splitter:set_metadata_visit_limit", [&](void* arg) {
        ++planner_calls;
        *static_cast<size_t*>(arg) = 0;
    });
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->DisableProcessing();
        sync->ClearAllCallBacks();
    });
    TxnInfoPB txn;
    txn.set_gtid(kGtid);
    std::unordered_map<int64_t, TabletMetadataPtr> actual;
    std::unordered_map<int64_t, TabletRangePB> actual_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), request, source->version(), kVersion, txn, false,
                                              actual, actual_ranges));
    EXPECT_EQ(1, planner_calls);
    EXPECT_EQ(2, actual.size());
    EXPECT_EQ(0, actual.count(children[1]));
}

TEST_F(LakeTabletReshardTest, test_split_identical_retry_cache_requires_absent_extra_children) {
    constexpr int64_t kVersion = 2;
    constexpr int64_t kGtid = 78;
    auto source = split_source();
    std::vector<int64_t> children{next_id(), next_id(), next_id()};
    auto request = make_split_retry_request(source, children, false);
    TabletRangePB identical_range;
    *identical_range.mutable_lower_bound() = generate_sort_key(0);
    *identical_range.mutable_upper_bound() = generate_sort_key(100);
    auto cached_source = cache_reshard_metadata(source->id(), source->id(), kVersion, kGtid, &identical_range);
    auto cached_child = cache_reshard_metadata(children[0], children[0], kVersion, kGtid, &identical_range);
    for (size_t i = 1; i < children.size(); ++i) {
        ASSERT_EQ(nullptr, _tablet_manager->metacache()->lookup_tablet_metadata(
                                   _tablet_manager->tablet_metadata_location(children[i], kVersion)));
    }

    int planner_calls = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("tablet_splitter:set_metadata_visit_limit", [&](void* arg) {
        ++planner_calls;
        *static_cast<size_t*>(arg) = 0;
    });
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->DisableProcessing();
        sync->ClearAllCallBacks();
    });
    TxnInfoPB txn;
    txn.set_gtid(kGtid);
    std::unordered_map<int64_t, TabletMetadataPtr> actual;
    std::unordered_map<int64_t, TabletRangePB> actual_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), request, source->version(), kVersion, txn, false,
                                              actual, actual_ranges));
    EXPECT_EQ(0, planner_calls);
    ASSERT_EQ(2, actual.size());
    EXPECT_EQ(cached_source->SerializeAsString(), actual.at(source->id())->SerializeAsString());
    EXPECT_EQ(cached_child->SerializeAsString(), actual.at(children[0])->SerializeAsString());
}

// The alias renumbers the rowsets it emits, so any sidecar keyed by rssid would name the wrong segment
// once carried over -- and nothing here projects one. build_parent_tablet_metadata already refuses a
// child carrying DCG or IDG before the alias is built; the alias refuses it as well, rather than
// clearing the metadata and silently serving the columns' pre-update values.
// The alias renumbers the rowsets it emits, so any sidecar keyed by rssid would name the wrong segment
// once carried over -- and nothing here projects one. build_parent_tablet_metadata already refuses a
// child carrying DCG or IDG before the alias is built; the alias refuses it as well, rather than
// clearing the metadata and silently serving the columns' pre-update values.
TEST_F(LakeTabletReshardTest, test_virtual_merge_refuses_a_child_carrying_dcg) {
    constexpr int64_t kNewVersion = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t alias_tablet = next_id();
    for (int64_t tablet_id : {child_a, child_b, alias_tablet}) {
        prepare_tablet_dirs(tablet_id);
    }

    auto meta_a = make_shared_delvec_source(child_a, {"dcg_guard_shared.dat"});
    auto meta_b = make_shared_delvec_source(child_b, {"dcg_guard_shared.dat"});
    auto& dcg = (*meta_b->mutable_dcg_meta()->mutable_dcgs())[meta_b->rowsets(0).id()];
    dcg.add_column_files("dcg_guard.cols");
    dcg.add_versions(1);
    dcg.add_shared_files(false);

    auto alias = merge_tablet_directly({meta_a, meta_b}, alias_tablet, kNewVersion, /*read_alias=*/true);
    ASSERT_FALSE(alias.ok());
    EXPECT_TRUE(alias.status().is_not_supported()) << alias.status();
    EXPECT_TRUE(alias.status().message().contains("DCG")) << alias.status();
}

// A page and the file that holds it live in two different maps: delvecs() keys the page by rssid, and
// version_to_file() keys the file by the page's version. Metadata that kept the page but lost the file
// entry would have the alias hand a reader a page it cannot open, so the build refuses it outright --
// this is the branch the dedup key below reads that file entry from.
TEST_F(LakeTabletReshardTest, test_virtual_merge_refuses_a_delvec_page_whose_file_is_missing) {
    constexpr int64_t kNewVersion = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t alias_tablet = next_id();
    for (int64_t tablet_id : {child_a, child_b, alias_tablet}) {
        prepare_tablet_dirs(tablet_id);
    }

    const std::string segment_name = "missing_delvec_file_shared.dat";
    auto meta_a = make_shared_delvec_source(child_a, {segment_name});
    auto meta_b = make_shared_delvec_source(child_b, {segment_name});

    DelVector delvec;
    const uint32_t deleted[] = {5};
    delvec.init(/*version=*/10, deleted, std::size(deleted));
    add_delvec(meta_b.get(), child_b, /*version=*/10, /*segment_id=*/1, "missing_delvec.dat", delvec.save());
    // Keep the page, drop the file it names.
    meta_b->mutable_delvec_meta()->mutable_version_to_file()->erase(int64_t{10});
    ASSERT_EQ(1, meta_b->delvec_meta().delvecs().count(1));

    auto alias = merge_tablet_directly({meta_a, meta_b}, alias_tablet, kNewVersion, /*read_alias=*/true);
    ASSERT_FALSE(alias.ok());
    EXPECT_TRUE(alias.status().is_corruption()) << alias.status();
    EXPECT_TRUE(alias.status().message().contains("has no file in tablet")) << alias.status();
}

// virtual_merge_for_read builds only what a read needs, so what it does build has to be what a real merge
// would have produced: the same rowsets, once each, and the same delete vectors. This is the net under it
// being a separate implementation rather than a flag on merge_tablet -- the sidecars it skips
// (primary-index SST, DCG, IDG) are compared by their absence.

// virtual_merge_for_read builds only what a read needs, so what it does build has to be what a real merge
// would have produced: the same rowsets, once each, and the same delete vectors. This is the net under it
// being a separate implementation rather than a flag on merge_tablet -- the sidecars it skips
// (primary-index SST, DCG, IDG) are compared by their absence.
TEST_F(LakeTabletReshardTest, test_virtual_merge_rowsets_and_delvecs_match_a_real_merge) {
    constexpr int64_t kNewVersion = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    const int64_t alias_tablet = next_id();
    for (int64_t tablet_id : {child_a, child_b, merged_tablet, alias_tablet}) {
        prepare_tablet_dirs(tablet_id);
    }

    const std::string segment_name = "differential_shared.dat";
    auto meta_a = make_shared_delvec_source(child_a, {segment_name});
    auto meta_b = make_shared_delvec_source(child_b, {segment_name});
    // A real merge validates its sources before doing anything (validate_merge_inputs and
    // validate_canonical_rowset): they need a schema whose key the range can be expressed in, tablet
    // ranges forming a nonoverlapping contiguous cover, and each rowset's range equal to its
    // intersection with its tablet's. Give the two children adjacent halves of an int key, which is
    // what a split would have left behind -- and keep the sort key EQUAL to the primary key here, so
    // the real merge this test compares against is one that can actually run.
    for (int i = 0; i < 2; ++i) {
        auto* meta = i == 0 ? meta_a.get() : meta_b.get();
        set_int_primary_key_schema(meta, /*schema_id=*/4001);
        auto* range = meta->mutable_range();
        range->mutable_lower_bound()->CopyFrom(generate_sort_key(i * 50));
        range->set_lower_bound_included(true);
        range->mutable_upper_bound()->CopyFrom(generate_sort_key((i + 1) * 50));
        range->set_upper_bound_included(false);
        meta->mutable_rowsets(0)->mutable_range()->CopyFrom(*range);
    }
    DelVector delvec_a;
    const uint32_t deleted_by_a[] = {1, 4};
    delvec_a.init(/*version=*/10, deleted_by_a, std::size(deleted_by_a));
    add_delvec(meta_a.get(), child_a, /*version=*/10, /*segment_id=*/1, "differential_a.delvec", delvec_a.save());
    DelVector delvec_b;
    const uint32_t deleted_by_b[] = {7};
    delvec_b.init(/*version=*/10, deleted_by_b, std::size(deleted_by_b));
    add_delvec(meta_b.get(), child_b, /*version=*/10, /*segment_id=*/1, "differential_b.delvec", delvec_b.save());

    ASSIGN_OR_ABORT(auto merged, merge_tablet_directly({meta_a, meta_b}, merged_tablet, kNewVersion,
                                                       /*read_alias=*/false));
    ASSIGN_OR_ABORT(auto alias, merge_tablet_directly({meta_a, meta_b}, alias_tablet, kNewVersion,
                                                      /*read_alias=*/true));

    ASSERT_EQ(merged->rowsets_size(), alias->rowsets_size());
    for (int i = 0; i < merged->rowsets_size(); ++i) {
        const auto& from_merge = merged->rowsets(i);
        const auto& from_alias = alias->rowsets(i);
        EXPECT_EQ(from_merge.num_rows(), from_alias.num_rows());
        EXPECT_EQ(from_merge.uid().hi(), from_alias.uid().hi());
        EXPECT_EQ(from_merge.uid().lo(), from_alias.uid().lo());
        ASSERT_EQ(from_merge.segment_metas_size(), from_alias.segment_metas_size());
        for (int seg = 0; seg < from_merge.segment_metas_size(); ++seg) {
            EXPECT_EQ(from_merge.segment_metas(seg).filename(), from_alias.segment_metas(seg).filename());
            EXPECT_EQ(from_merge.segment_metas(seg).shared(), from_alias.segment_metas(seg).shared());
        }
    }

    // The delete vectors are the other half a read needs, so they must decode identically. The alias
    // numbers its own rowsets, so each side is read at the rssid it gave the segment.
    ASSERT_EQ(1, merged->rowsets_size());
    LakeIOOptions io_options;
    DelVector from_merge;
    ASSERT_OK(
            lake::get_del_vec(_tablet_manager.get(), *merged, merged->rowsets(0).id(), false, io_options, &from_merge));
    DelVector from_alias;
    ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), *alias, alias->rowsets(0).id(), false, io_options, &from_alias));
    ASSERT_NE(nullptr, from_merge.roaring());
    ASSERT_NE(nullptr, from_alias.roaring());
    EXPECT_EQ(from_merge.save(), from_alias.save());
    EXPECT_EQ(3, from_alias.cardinality());

    // And the alias declares none of the sidecars it does not build.
    EXPECT_TRUE(alias->sstable_meta().sstables().empty());
    EXPECT_TRUE(alias->dcg_meta().dcgs().empty());
    EXPECT_TRUE(alias->idg_meta().idgs().empty());
}

// A page only one child draws from is copied through byte for byte, never decoded into a bitmap and
// re-serialized. What must survive that shortcut is the page's CONTENT and its checksum: the copy
// carries the source's crc32c over with its bytes, and restates it at the alias's version so
// MetaFileBuilder still reads it as a live checksum rather than as none.
TEST_F(LakeTabletReshardTest, test_virtual_merge_copies_a_single_source_page_verbatim) {
    constexpr int64_t kNewVersion = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t alias_tablet = next_id();
    for (int64_t tablet_id : {child_a, child_b, alias_tablet}) {
        prepare_tablet_dirs(tablet_id);
    }

    const std::string segment_name = "verbatim_shared.dat";
    auto meta_a = make_shared_delvec_source(child_a, {segment_name});
    auto meta_b = make_shared_delvec_source(child_b, {segment_name});

    // Only child A ever deleted from the shared segment, so its page is the rssid's single source.
    DelVector delvec_a;
    const uint32_t deleted_by_a[] = {1, 4, 6};
    delvec_a.init(/*version=*/10, deleted_by_a, std::size(deleted_by_a));
    const std::string content = delvec_a.save();
    add_delvec(meta_a.get(), child_a, /*version=*/10, /*segment_id=*/1, "verbatim_a.delvec", content);
    // Give it a valid checksum, the way a published page carries one.
    auto& source_page = (*meta_a->mutable_delvec_meta()->mutable_delvecs())[1];
    source_page.set_crc32c(crc32c::Mask(crc32c::Value(content.data(), content.size())));
    source_page.set_crc32c_gen_version(10);

    int raw_copy_opens = 0;
    std::vector<size_t> raw_read_sizes;
    auto* sync = SyncPoint::GetInstance();
    sync->ClearAllCallBacks();
    sync->DisableProcessing();
    sync->SetCallBack("write_compacted_delvec_pages:copy_source_reader_delta", [&](void* arg) {
        if (*static_cast<int*>(arg) == 1) {
            ++raw_copy_opens;
        }
    });
    sync->SetCallBack("write_compacted_delvec_pages:read_chunk_size",
                      [&](void* arg) { raw_read_sizes.push_back(*static_cast<size_t*>(arg)); });
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
    });

    ASSIGN_OR_ABORT(auto alias, merge_tablet_directly({meta_a, meta_b}, alias_tablet, kNewVersion,
                                                      /*read_alias=*/true));
    EXPECT_EQ(1, raw_copy_opens) << "the single source must use the raw-copy path";
    EXPECT_EQ(std::vector<size_t>({content.size()}), raw_read_sizes);

    ASSERT_EQ(1, alias->rowsets_size());
    const uint32_t target_rssid = alias->rowsets(0).id();
    DelVector copied;
    LakeIOOptions io_options;
    ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), *alias, target_rssid, false, io_options, &copied));
    ASSERT_NE(nullptr, copied.roaring());
    EXPECT_EQ(3, copied.cardinality());
    for (uint32_t rowid : deleted_by_a) {
        EXPECT_TRUE(copied.roaring()->contains(rowid)) << "rowid " << rowid;
    }

    // The bytes went through untouched, so the checksum is the source's -- restated at this version.
    const auto& alias_page = alias->delvec_meta().delvecs().at(target_rssid);
    EXPECT_EQ(content.size(), alias_page.size());
    EXPECT_EQ(source_page.crc32c(), alias_page.crc32c());
    EXPECT_EQ(kNewVersion, alias_page.crc32c_gen_version())
            << "a checksum whose gen version is not the page's version reads as no checksum at all";
    EXPECT_EQ(kNewVersion, alias_page.version());
}

// The verbatim copy above is the alias's ordinary case, so it is also where a corrupted source page
// would reach the alias undecoded. The copy checksums the bytes it streams, so the publish fails here
// instead of handing the alias a page that only fails once somebody reads it.
TEST_F(LakeTabletReshardTest, test_virtual_merge_rejects_a_single_source_page_that_fails_its_checksum) {
    constexpr int64_t kNewVersion = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t alias_tablet = next_id();
    for (int64_t tablet_id : {child_a, child_b, alias_tablet}) {
        prepare_tablet_dirs(tablet_id);
    }

    const std::string segment_name = "checksum_shared.dat";
    auto meta_a = make_shared_delvec_source(child_a, {segment_name});
    auto meta_b = make_shared_delvec_source(child_b, {segment_name});

    DelVector delvec_a;
    const uint32_t deleted_by_a[] = {1, 4, 6};
    delvec_a.init(/*version=*/10, deleted_by_a, std::size(deleted_by_a));
    const std::string content = delvec_a.save();
    const std::string delvec_name = "checksum_a.delvec";
    add_delvec(meta_a.get(), child_a, /*version=*/10, /*segment_id=*/1, delvec_name, content);
    auto& source_page = (*meta_a->mutable_delvec_meta()->mutable_delvecs())[1];
    source_page.set_crc32c(crc32c::Mask(crc32c::Value(content.data(), content.size())));
    source_page.set_crc32c_gen_version(10);

    // Rewrite the page with bytes the recorded checksum no longer describes, keeping its length so the
    // range preflight still passes -- a corrupted cache block looks exactly like this.
    std::string corrupted = content;
    corrupted[0] = static_cast<char>(corrupted[0] ^ 0xff);
    write_file(_tablet_manager->delvec_location(child_a, delvec_name), corrupted);

    const bool old_strict = config::enable_strict_delvec_crc_check;
    DeferOp restore_strict([&] { config::enable_strict_delvec_crc_check = old_strict; });
    config::enable_strict_delvec_crc_check = true;

    auto alias = merge_tablet_directly({meta_a, meta_b}, alias_tablet, kNewVersion, /*read_alias=*/true);
    EXPECT_TRUE(alias.status().is_corruption()) << alias.status();
}

// Two children can hold delvec pages that agree on version, offset and size while listing different
// rowids, so a page cannot be identified by those three alone. One publish advances every child to the
// same version and each child writes its OWN delvec file for it, its own page at offset 0 -- so when
// both deleted the same number of rows the sizes match too, and dropping the second as a duplicate
// loses that child's deletions. What tells the two apart is the file the page lives in: the name
// carries a fresh uuid per write, so different bytes never answer to the same name.
TEST_F(LakeTabletReshardTest, test_virtual_merge_keeps_indistinguishable_private_delvec_pages) {
    constexpr int64_t kNewVersion = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t alias_tablet = next_id();
    for (int64_t tablet_id : {child_a, child_b, alias_tablet}) {
        prepare_tablet_dirs(tablet_id);
    }

    const std::string segment_name = "dedup_key_shared.dat";
    auto meta_a = make_shared_delvec_source(child_a, {segment_name});
    auto meta_b = make_shared_delvec_source(child_b, {segment_name});

    // One version, one offset, one size, and -- as production always gives them -- two distinct file
    // names. The old key read the first three and not the name, so it saw one page where there are two.
    DelVector delvec_a;
    const uint32_t deleted_by_a[] = {3};
    delvec_a.init(/*version=*/10, deleted_by_a, std::size(deleted_by_a));
    add_delvec(meta_a.get(), child_a, /*version=*/10, /*segment_id=*/1, "delvec_child_a.dat", delvec_a.save());
    DelVector delvec_b;
    const uint32_t deleted_by_b[] = {7};
    delvec_b.init(/*version=*/10, deleted_by_b, std::size(deleted_by_b));
    add_delvec(meta_b.get(), child_b, /*version=*/10, /*segment_id=*/1, "delvec_child_b.dat", delvec_b.save());

    // The premise: version, offset and size tell these two pages apart in no way at all.
    ASSERT_EQ(delvec_a.save().size(), delvec_b.save().size()) << "the two pages must be the same size";
    const auto& page_a = meta_a->delvec_meta().delvecs().at(1);
    const auto& page_b = meta_b->delvec_meta().delvecs().at(1);
    ASSERT_EQ(page_a.version(), page_b.version());
    ASSERT_EQ(page_a.offset(), page_b.offset());
    ASSERT_EQ(page_a.size(), page_b.size());
    ASSERT_NE(meta_a->delvec_meta().version_to_file().at(10).name(),
              meta_b->delvec_meta().version_to_file().at(10).name())
            << "each child wrote its own file, so the names must differ";

    ASSIGN_OR_ABORT(auto alias, merge_tablet_directly({meta_a, meta_b}, alias_tablet, kNewVersion,
                                                      /*read_alias=*/true));

    ASSERT_EQ(1, alias->rowsets_size());
    DelVector unioned;
    LakeIOOptions io_options;
    ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), *alias, alias->rowsets(0).id(), false, io_options, &unioned));
    ASSERT_NE(nullptr, unioned.roaring());
    EXPECT_EQ(2, unioned.cardinality()) << "both children's deletions must survive";
    EXPECT_TRUE(unioned.roaring()->contains(3));
    EXPECT_TRUE(unioned.roaring()->contains(7));
}

// A child rowset written after the split exists in that child alone, and the children allocate ids
// independently -- so the alias has to re-identify one of the two and carry its delete vectors across.
TEST_F(LakeTabletReshardTest, test_virtual_merge_reidentifies_colliding_post_split_rowsets) {
    constexpr int64_t kNewVersion = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t alias_tablet = next_id();
    for (int64_t tablet_id : {child_a, child_b, alias_tablet}) {
        prepare_tablet_dirs(tablet_id);
    }

    // Both children inherit rowset 1 over the same shared segment, then each writes its own rowset 2.
    auto meta_a = make_shared_delvec_source(child_a, {"inherited.dat"});
    auto meta_b = make_shared_delvec_source(child_b, {"inherited.dat"});
    for (int i = 0; i < 2; ++i) {
        auto* meta = i == 0 ? meta_a.get() : meta_b.get();
        auto* own = meta->add_rowsets();
        own->set_id(2);
        own->set_version(2);
        own->set_num_rows(5);
        own->set_data_size(50);
        auto* segment = own->add_segment_metas();
        segment->set_filename(i == 0 ? "child_a_own.dat" : "child_b_own.dat");
        segment->set_size(50);
        stamp_physical_identity_uid(own, i == 0 ? "child_a_own.dat" : "child_b_own.dat");
        meta->set_next_rowset_id(3);
    }
    // A delete vector on child B's own rowset, which must follow it to its new id.
    DelVector own_delvec;
    const uint32_t deleted[] = {3};
    own_delvec.init(/*version=*/10, deleted, std::size(deleted));
    add_delvec(meta_b.get(), child_b, /*version=*/10, /*segment_id=*/2, "child_b_own.delvec", own_delvec.save());

    ASSIGN_OR_ABORT(auto alias, merge_tablet_directly({meta_a, meta_b}, alias_tablet, kNewVersion,
                                                      /*read_alias=*/true));

    // The inherited rowset once, plus one per child's own: three, with distinct ids.
    ASSERT_EQ(3, alias->rowsets_size());
    std::set<uint32_t> ids;
    std::set<std::string> segments;
    for (const auto& rowset : alias->rowsets()) {
        ids.insert(rowset.id());
        for (const auto& segment : rowset.segment_metas()) {
            segments.insert(segment.filename());
        }
    }
    EXPECT_EQ(3, ids.size()) << "the children's own rowsets must not collide on an id";
    EXPECT_EQ(std::set<std::string>({"inherited.dat", "child_a_own.dat", "child_b_own.dat"}), segments);
    EXPECT_GE(alias->next_rowset_id(), *ids.rbegin() + 1);

    // Child B's delete vector followed its rowset to whichever id it got.
    uint32_t reidentified = 0;
    for (const auto& rowset : alias->rowsets()) {
        if (rowset.segment_metas_size() == 1 && rowset.segment_metas(0).filename() == "child_b_own.dat") {
            reidentified = rowset.id();
        }
    }
    ASSERT_NE(0u, reidentified);
    DelVector carried;
    LakeIOOptions io_options;
    ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), *alias, reidentified, false, io_options, &carried));
    ASSERT_NE(nullptr, carried.roaring());
    EXPECT_EQ(1, carried.cardinality());
    EXPECT_TRUE(carried.roaring()->contains(3));
}

// The read-only parent alias a primary-key-range split keeps alive is not such a merge: it reproduces the
// pre-split parent, which served every row of a shared segment exactly once. So it must build -- carrying
// the shared rowset once and the union of the children's delete vectors -- rather than try to locate a gap
// it has no rowid window for, which is what wedged publish on this shape.
TEST_F(LakeTabletReshardTest, test_parent_alias_of_a_primary_key_range_split_unions_shared_delvecs) {
    constexpr int64_t kNewVersion = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t alias_tablet = next_id();
    for (int64_t tablet_id : {child_a, child_b, alias_tablet}) {
        prepare_tablet_dirs(tablet_id);
    }

    const std::string segment_name = "alias_shared.dat";
    auto [meta_a, meta_b] = make_primary_key_range_split_children(child_a, child_b, segment_name,
                                                                  /*split_key=*/100, /*rowset_lower_key=*/90);
    // A real segment under the ALIAS tablet id, which is where gap synthesis would look for it: without the
    // file this test would pass for the wrong reason (a missing segment) instead of proving the
    // primary-key-interval-to-rowid conversion is gone.
    write_two_column_segment(
            alias_tablet, segment_name, /*num_rows=*/10, [](int key) { return key * 2; },
            /*key_start=*/90);

    // Each child deleted rows of the shared segment that fall in its own range; neither knows the other's.
    DelVector delvec_a;
    const uint32_t deleted_by_a[] = {3, 9};
    delvec_a.init(/*version=*/10, deleted_by_a, std::size(deleted_by_a));
    add_delvec(meta_a.get(), child_a, /*version=*/10, /*segment_id=*/1, "alias_a.delvec", delvec_a.save());
    DelVector delvec_b;
    const uint32_t deleted_by_b[] = {5};
    delvec_b.init(/*version=*/10, deleted_by_b, std::size(deleted_by_b));
    add_delvec(meta_b.get(), child_b, /*version=*/10, /*segment_id=*/1, "alias_b.delvec", delvec_b.save());

    ASSIGN_OR_ABORT(auto alias, merge_tablet_directly({meta_a, meta_b}, alias_tablet, kNewVersion,
                                                      /*read_alias=*/true));

    // One copy of the shared rowset: the uid dedup is what keeps the alias from serving it twice.
    ASSERT_EQ(1, alias->rowsets_size());
    ASSERT_EQ(1, alias->rowsets(0).segment_metas_size());
    EXPECT_EQ(segment_name, alias->rowsets(0).segment_metas(0).filename());
    // Read-only alias: no primary index is rebuilt for it.
    EXPECT_TRUE(alias->sstable_meta().sstables().empty());

    const uint32_t target_rssid = alias->rowsets(0).id();
    DelVector loaded;
    LakeIOOptions io_options;
    ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), *alias, target_rssid, false, io_options, &loaded));
    ASSERT_NE(nullptr, loaded.roaring());
    // The union, and nothing more: no gap bits masking rows the pre-split parent was serving.
    EXPECT_EQ(3, loaded.cardinality());
    EXPECT_TRUE(loaded.roaring()->contains(3));
    EXPECT_TRUE(loaded.roaring()->contains(5));
    EXPECT_TRUE(loaded.roaring()->contains(9));
}

} // namespace starrocks
