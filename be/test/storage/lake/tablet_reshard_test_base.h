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

#pragma once

#include <bvar/bvar.h>
#include <fmt/format.h>
#include <gmock/gmock.h>
#include <google/protobuf/unknown_field_set.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <ctime>
#include <functional>
#include <iterator>
#include <limits>
#include <optional>
#include <set>

#include "base/failpoint/fail_point.h"
#include "base/hash/crc32c.h"
#include "base/path/filesystem_util.h"
#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/chunk_factory.h"
#include "column/column_helper.h"
#include "column/datum_tuple.h"
#include "column/serde/column_array_serde.h"
#include "common/config_compaction_fwd.h"
#include "common/config_ingest_fwd.h"
#include "common/config_lake_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_rowset_fwd.h"
#include "common/config_starlet_fwd.h"
#include "common/config_storage_fwd.h"
#include "common/runtime_profile.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "platform/key_cache.h"
#include "platform/store_path.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_env.h"
#include "storage/chunk_helper.h"
#include "storage/del_vector.h"
#include "storage/lake/compaction_task.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/filenames.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/index_delta_group_loader.h"
#include "storage/lake/join_path.h"
#include "storage/lake/lake_persistent_index.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/metacache.h"
#include "storage/lake/persistent_index_sstable.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_merger.h"
#include "storage/lake/tablet_range_helper.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_reshard.h"
#include "storage/lake/tablet_reshard_helper.h"
#include "storage/lake/tablet_splitter.h"
#include "storage/lake/tablet_virtual_merge.h"
#include "storage/lake/test_util.h"
#include "storage/lake/transactions.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/vacuum.h"
#include "storage/lake/vacuum_full.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/seek_range.h"
#include "storage/sort_key_sampler.h"
#include "storage/sstable/block.h"
#include "storage/sstable/comparator.h"
#include "storage/sstable/format.h"
#include "storage/sstable/iterator.h"
#include "storage/sstable/options.h"
#include "storage/sstable/table_builder.h"
#include "storage/storage_metrics.h"
#include "storage/tablet_schema.h"
#include "storage/variant_tuple.h"
#include "storage_primitive/primary_key_encoder.h"

namespace starrocks {

// Mirror reality for tests that build child metadata directly: cross-published / split
// siblings carry a uid that is IDENTICAL across siblings (set at write time in the
// shared txn log, or backfilled once at split). Derive a deterministic uid from a
// physical-identity seed (a shared segment filename, or a shared del-file name) so two
// siblings modeling "the same logical rowset" (same shared file) dedup at merge,
// exactly as the pre-uid physical-identity rule did. A private (per-child) file name is
// unique, so it yields a distinct uid and never falsely dedups. No-op if the rowset
// already carries an explicit uid (pruned-sibling tests set their own matching uid).
inline void stamp_physical_identity_uid(RowsetMetadataPB* rowset, const std::string& seed) {
    if (rowset->has_uid()) return;
    rowset->mutable_uid()->set_hi(1); // non-zero => valid even if the hash is 0
    rowset->mutable_uid()->set_lo(static_cast<int64_t>(std::hash<std::string>{}(seed)));
}

// Mirror reality for tests that build child metadata directly: cross-published / split
// siblings carry a uid that is IDENTICAL across siblings (set at write time in the
// shared txn log, or backfilled once at split). Derive a deterministic uid from a
// physical-identity seed (a shared segment filename, or a shared del-file name) so two
// siblings modeling "the same logical rowset" (same shared file) dedup at merge,
// exactly as the pre-uid physical-identity rule did. A private (per-child) file name is
// unique, so it yields a distinct uid and never falsely dedups. No-op if the rowset
// already carries an explicit uid (pruned-sibling tests set their own matching uid).
// Routes by purpose the way the two production callers do: a real MERGE goes through merge_tablet, the
// split's read alias through virtual_merge_for_read. Tests that must hold for both stay parameterized.
inline StatusOr<MutableTabletMetadataPtr> merge_tablet_or_read_alias(lake::TabletManager* tablet_manager,
                                                                     const std::vector<TabletMetadataPtr>& sources,
                                                                     const MergingTabletInfoPB& merging,
                                                                     int64_t new_version, const TxnInfoPB& txn_info,
                                                                     bool read_alias) {
    return read_alias ? lake::virtual_merge_for_read(tablet_manager, sources, merging, new_version, txn_info)
                      : lake::merge_tablet(tablet_manager, sources, merging, new_version, txn_info);
}

class LakeTabletReshardTest : public testing::Test {
public:
    static TuplePB generate_sort_key(int value) {
        DatumVariant variant(get_type_info(LogicalType::TYPE_INT), Datum(value));
        VariantTuple tuple;
        tuple.append(variant);
        TuplePB tuple_pb;
        tuple.to_proto(&tuple_pb);
        return tuple_pb;
    }

    // Zero-padded so byte order == numeric order, matching write_varchar_key_segment's encoding.
    static TuplePB generate_varchar_sort_key(int value) {
        // DatumVariant holds a CopiedDatum, which deep-copies the Slice, so the temporary
        // std::string fmt::format returns may die at the end of this statement.
        DatumVariant variant(get_type_info(LogicalType::TYPE_VARCHAR), Datum(Slice(fmt::format("{:06d}", value))));
        VariantTuple tuple;
        tuple.append(variant);
        TuplePB tuple_pb;
        tuple.to_proto(&tuple_pb);
        return tuple_pb;
    }

    void SetUp() override {
        std::vector<starrocks::StorePath> paths;
        CHECK_OK(starrocks::parse_conf_store_paths(starrocks::config::storage_root_path, &paths));
        _test_dir = paths[0].path + "/lake";
        _location_provider = std::make_shared<lake::FixedLocationProvider>(_test_dir);
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->metadata_root_location(1)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->txn_log_root_location(1)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->segment_root_location(1)));
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_manager = std::make_unique<lake::TabletManager>(_location_provider, _update_manager.get(), 16384);
        // PK index major compaction always runs on the parallel manager, so wire in the process-wide
        // one and point it at this fixture's tablet manager, the same way TestBase does.
        if (auto* parallel_compact_mgr = StorageEnv::GetInstance()->parallel_compact_mgr();
            parallel_compact_mgr != nullptr) {
            _update_manager->set_parallel_compact_mgr(parallel_compact_mgr);
            parallel_compact_mgr->TEST_set_tablet_mgr(_tablet_manager.get());
        }

        // These reshard tests use hand-crafted metadata with no real in-memory PK memtable, so
        // the cloud-native index flush that split/merge trigger has nothing to flush. Skip it so
        // the tests exercise the metadata merge/split logic without loading a real index from the
        // (intentionally fake) segment/del/sstable files.
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    }

    void TearDown() override {
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
        // Only remove this test's own subdirectory. Removing the entire
        // config::storage_root_path would wipe out DataDir's persistent /tmp/
        // subdirectory (created once at StorageEngine init) and break any later
        // test that writes local CRM files during compaction (e.g.
        // LakePrimaryKeyPublishTest.test_individual_index_compaction).
        auto status = fs::remove_all(_test_dir);
        EXPECT_TRUE(status.ok() || status.is_not_found()) << status;
    }

    static void set_failpoint_mode(const std::string& name, FailPointTriggerModeType mode) {
        PFailPointTriggerMode trigger_mode;
        trigger_mode.set_mode(mode);
        auto* fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get(name);
        if (fp != nullptr) {
            fp->setMode(trigger_mode);
        }
    }

protected:
    std::shared_ptr<TabletMetadataPB> split_source(bool separate_sort = false) {
        auto m = std::make_shared<TabletMetadataPB>();
        m->set_id(next_id());
        m->set_version(1);
        m->set_next_rowset_id(30);
        set_int_primary_key_schema(m.get(), next_id());
        if (separate_sort) {
            auto* c = m->mutable_schema()->add_column();
            c->set_unique_id(1);
            c->set_name("v");
            c->set_type("INT");
            c->set_is_key(false);
            c->set_is_nullable(false);
            m->mutable_schema()->add_sort_key_idxes(1);
        }
        auto* r = m->add_rowsets();
        r->set_id(2);
        lake::tablet_reshard_helper::set_rowset_uid(r);
        r->set_num_rows(100);
        r->set_data_size(1000);
        r->set_num_dels(10);
        for (int i = 0; i < 2; ++i) {
            auto* sm = r->add_segment_metas();
            sm->set_filename(fmt::format("split-{}.dat", i));
            sm->set_segment_idx(i == 0 ? 0 : 7);
            sm->set_num_rows(50);
            sm->set_size(500);
            *sm->mutable_sort_key_min() = generate_sort_key(i * 50);
            *sm->mutable_sort_key_max() = generate_sort_key(i * 50 + 49);
        }
        return m;
    }

    auto split_source_into_children(const TabletMetadataPtr& m, bool external = true) {
        // Deliberately bypass the current-producer stamping wrapper: malformed
        // rejection fixtures must reach SPLIT exactly as declared by each test.
        CHECK_OK(_tablet_manager->put_tablet_metadata(m));
        SplittingTabletInfoPB splitting;
        splitting.set_old_tablet_id(m->id());
        splitting.add_new_tablet_ids(m->id() + 100000);
        splitting.add_new_tablet_ids(m->id() + 200000);
        if (external) {
            *splitting.add_new_tablet_ranges()->mutable_upper_bound() = generate_sort_key(50);
            splitting.mutable_new_tablet_ranges(0)->set_upper_bound_included(false);
            *splitting.add_new_tablet_ranges()->mutable_lower_bound() = generate_sort_key(50);
            splitting.mutable_new_tablet_ranges(1)->set_lower_bound_included(true);
        }
        return lake::split_tablet(_tablet_manager.get(), m, splitting, 2, TxnInfoPB());
    }

    TabletMetadataPtr cache_reshard_metadata(int64_t key_id, int64_t metadata_id, int64_t version, int64_t gtid,
                                             const TabletRangePB* range = nullptr) {
        _tablet_manager->metacache()->update_capacity(1024 * 1024);
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(metadata_id);
        metadata->set_version(version);
        metadata->set_gtid(gtid);
        if (range != nullptr) *metadata->mutable_range() = *range;
        _tablet_manager->metacache()->cache_tablet_metadata(_tablet_manager->tablet_metadata_location(key_id, version),
                                                            metadata);
        return metadata;
    }

    ReshardingTabletInfoPB make_split_retry_request(const TabletMetadataPtr& source,
                                                    const std::vector<int64_t>& children, bool external) {
        CHECK_OK(put_tablet_metadata(source));
        ReshardingTabletInfoPB request;
        auto* split = request.mutable_splitting_tablet_info();
        split->set_old_tablet_id(source->id());
        for (auto child : children) {
            prepare_tablet_dirs(child);
            split->add_new_tablet_ids(child);
        }
        if (external) {
            *split->add_new_tablet_ranges()->mutable_upper_bound() = generate_sort_key(50);
            split->mutable_new_tablet_ranges(0)->set_upper_bound_included(false);
            *split->add_new_tablet_ranges()->mutable_lower_bound() = generate_sort_key(50);
            split->mutable_new_tablet_ranges(1)->set_lower_bound_included(true);
        }
        return request;
    }

    void expect_split_fallback_after_flush(const std::shared_ptr<TabletMetadataPB>& m) {
        auto* sync = SyncPoint::GetInstance();
        int flushes = 0;
        sync->SetCallBack("tablet_splitter:pk_flush", [&](void*) { ++flushes; });
        sync->EnableProcessing();
        DeferOp cleanup([&] {
            sync->DisableProcessing();
            sync->ClearAllCallBacks();
        });
        ASSIGN_OR_ABORT(auto children, split_source_into_children(m));
        ASSERT_EQ(1, children.size());
        EXPECT_EQ(1, flushes);
        EXPECT_EQ(m->rowsets(0).SerializeAsString(), children.begin()->second->rowsets(0).SerializeAsString());
    }

    void prepare_tablet_dirs(int64_t tablet_id) {
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->metadata_root_location(tablet_id)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->txn_log_root_location(tablet_id)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->segment_root_location(tablet_id)));
    }

    void write_file(const std::string& path, const std::string& content) {
        WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_ABORT(auto writer, fs::new_writable_file(opts, path));
        ASSERT_OK(writer->append(Slice(content)));
        ASSERT_OK(writer->close());
    }

    void write_binary_del_file(int64_t tablet_id, const std::string& name, const std::vector<std::string>& keys) {
        auto column = BinaryColumn::create();
        for (const auto& key : keys) column->append(Slice(key));
        const int64_t max_size = serde::ColumnArraySerde::max_serialized_size(*column);
        std::vector<uint8_t> buffer(max_size);
        ASSIGN_OR_ABORT(auto* end, serde::ColumnArraySerde::serialize(*column, buffer.data()));
        WritableFileOptions options;
        options.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
        ASSIGN_OR_ABORT(auto writer, FileSystem::Default()->new_writable_file(
                                             options, _tablet_manager->del_location(tablet_id, name)));
        ASSERT_OK(writer->append(Slice(reinterpret_cast<const char*>(buffer.data()), end - buffer.data())));
        ASSERT_OK(writer->close());
    }

    std::string write_encrypted_binary_del_file(int64_t tablet_id, const std::string& name,
                                                const std::vector<std::string>& keys) {
        auto column = BinaryColumn::create();
        for (const auto& key : keys) column->append(Slice(key));
        const int64_t max_size = serde::ColumnArraySerde::max_serialized_size(*column);
        std::vector<uint8_t> buffer(max_size);
        ASSIGN_OR_ABORT(auto* end, serde::ColumnArraySerde::serialize(*column, buffer.data()));
        ensure_kek_in_key_cache();
        ASSIGN_OR_ABORT(auto encryption_pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
        WritableFileOptions options;
        options.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
        options.encryption_info = encryption_pair.info;
        ASSIGN_OR_ABORT(auto writer, FileSystem::Default()->new_writable_file(
                                             options, _tablet_manager->del_location(tablet_id, name)));
        CHECK_OK(writer->append(Slice(reinterpret_cast<const char*>(buffer.data()), end - buffer.data())));
        CHECK_OK(writer->close());
        return encryption_pair.encryption_meta;
    }

    void set_primary_key_schema(TabletMetadataPB* metadata, int64_t schema_id) {
        auto* schema = metadata->mutable_schema();
        schema->set_keys_type(PRIMARY_KEYS);
        schema->set_id(schema_id);
    }

    void set_int_primary_key_schema(TabletMetadataPB* metadata, int64_t schema_id) {
        auto* schema = metadata->mutable_schema();
        schema->set_keys_type(PRIMARY_KEYS);
        schema->set_id(schema_id);
        schema->set_num_short_key_columns(1);
        schema->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
        auto* column = schema->add_column();
        column->set_unique_id(1);
        column->set_name("c0");
        column->set_type("INT");
        column->set_is_key(true);
        column->set_is_nullable(false);
    }

    std::string encode_int_primary_key(int32_t value) {
        TabletMetadataPB metadata;
        set_int_primary_key_schema(&metadata, 1);
        auto tablet_schema = TabletSchema::create(metadata.schema());
        std::vector<ColumnId> pk_columns = {0};
        auto pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);
        auto chunk = std::make_unique<Chunk>();
        auto column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false);
        column->append_datum(Datum(value));
        chunk->append_column(std::move(column), (SlotId)0);
        MutableColumnPtr encoded;
        CHECK_OK(PrimaryKeyEncoder::create_column(pkey_schema, &encoded, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2));
        PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, 1, encoded.get(),
                                  PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);
        return down_cast<BinaryColumn*>(encoded.get())->get_slice(0).to_string();
    }

    static std::string raw_int_primary_key(int32_t value) {
        return std::string(reinterpret_cast<const char*>(&value), sizeof(value));
    }

    void add_historical_schema(TabletMetadataPB* metadata, int64_t schema_id) {
        auto& schema = (*metadata->mutable_historical_schemas())[schema_id];
        schema.set_id(schema_id);
        schema.set_keys_type(PRIMARY_KEYS);
    }

    // Test-only wrapper for TabletManager::put_tablet_metadata. Stamps a fresh uid on
    // every rowset that doesn't already carry one before persisting. Production rowset
    // producers (delta_writer, compaction, schema_change, splitter backfill, column-mode
    // synthesis, ...) all mint a uid at creation, so the merge-side strict invariant
    // (DCHECK + Status::InternalError on missing uid in tablet_merger.cpp) never fires
    // in production. Synthetic test fixtures that omit uid would otherwise trip that
    // invariant; auto-stamp them with a fresh random uid here so they behave like
    // production local-data writes (distinct uid across tablets → no false dedup).
    // Dedup tests that need siblings to share a uid stamp it explicitly via
    // stamp_physical_identity_uid BEFORE calling this helper, and the ensure_rowset_uid
    // below is a no-op (set-if-absent semantics).
    Status put_tablet_metadata(TabletMetadataPB metadata) {
        for (auto& rowset : *metadata.mutable_rowsets()) {
            lake::tablet_reshard_helper::ensure_rowset_uid(&rowset);
            for (int i = 0; i < rowset.segment_metas_size(); ++i) {
                if (!rowset.segment_metas(i).has_segment_idx()) rowset.mutable_segment_metas(i)->set_segment_idx(i);
            }
        }
        return _tablet_manager->put_tablet_metadata(metadata);
    }

    Status put_tablet_metadata(const TabletMetadataPtr& metadata) {
        auto mutable_meta = std::make_shared<TabletMetadataPB>(*metadata);
        for (auto& rowset : *mutable_meta->mutable_rowsets()) {
            lake::tablet_reshard_helper::ensure_rowset_uid(&rowset);
            for (int i = 0; i < rowset.segment_metas_size(); ++i) {
                if (!rowset.segment_metas(i).has_segment_idx()) rowset.mutable_segment_metas(i)->set_segment_idx(i);
            }
        }
        return _tablet_manager->put_tablet_metadata(mutable_meta);
    }

    RowsetMetadataPB* add_rowset(TabletMetadataPB* metadata, uint32_t rowset_id, uint32_t max_compact_input_rowset_id,
                                 uint32_t del_origin_rowset_id) {
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(rowset_id);
        rowset->set_max_compact_input_rowset_id(max_compact_input_rowset_id);
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename(fmt::format("segment-{}-{}.dat", metadata->id(), rowset_id));
            sm->set_size(128);
            sm->set_num_rows(1);
        }
        auto* del_file = rowset->add_del_files();
        del_file->set_name("del.dat");
        del_file->set_origin_rowset_id(del_origin_rowset_id);
        // Match production: every lake writer mints a unique uid so distinct local
        // rowsets never alias across tablets at merge.
        lake::tablet_reshard_helper::set_rowset_uid(rowset);
        return rowset;
    }

    RowsetMetadataPB* add_rowset_with_predicate(TabletMetadataPB* metadata, uint32_t rowset_id, int64_t version,
                                                bool has_predicate) {
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(rowset_id);
        rowset->set_version(version);
        rowset->set_overlapped(false);
        if (!has_predicate) {
            {
                auto* sm = rowset->add_segment_metas();
                sm->set_filename(fmt::format("segment_{}.dat", rowset_id));
                sm->set_size(128);
                sm->set_num_rows(1);
            }
            rowset->set_num_rows(1);
            rowset->set_data_size(128);
            // Stable, distinct producer UIDs make order assertions follow the
            // canonical UID key instead of the order of the source list.
            rowset->mutable_uid()->set_hi(metadata->id());
            rowset->mutable_uid()->set_lo(rowset_id);
            return rowset;
        }

        rowset->set_num_rows(0);
        rowset->set_data_size(0);
        auto* delete_predicate = rowset->mutable_delete_predicate();
        delete_predicate->set_version(-1);
        auto* binary_predicate = delete_predicate->add_binary_predicates();
        binary_predicate->set_column_name("c0");
        binary_predicate->set_op(">");
        binary_predicate->set_value("0");
        // Production-faithful: Tablet::delete_data mints an independent (random) uid
        // per tablet, so sibling predicates at the same version do NOT share a uid.
        // MERGE must dedup them by version, not uid -- this exercises that path.
        lake::tablet_reshard_helper::set_rowset_uid(rowset);
        return rowset;
    }

    std::vector<TabletMetadataPtr> merge_fixture_sources(const std::vector<TabletMetadataPtr>& sources) {
        const bool assign_ranges =
                std::all_of(sources.begin(), sources.end(), [](const auto& source) { return !source->has_range(); });
        std::vector<int64_t> ids;
        for (const auto& source : sources) ids.push_back(source->id());
        std::sort(ids.begin(), ids.end());
        std::vector<TabletMetadataPtr> result;
        for (const auto& source : sources) {
            auto copy = std::make_shared<TabletMetadataPB>(*source);
            if (assign_ranges) {
                if (copy->has_schema() && copy->schema().column_size() == 0) {
                    auto* column = copy->mutable_schema()->add_column();
                    column->set_unique_id(1);
                    column->set_name("c0");
                    column->set_type("INT");
                    column->set_is_key(true);
                    column->set_is_nullable(false);
                }
                const int position = std::lower_bound(ids.begin(), ids.end(), source->id()) - ids.begin();
                auto* range = copy->mutable_range();
                if (position > 0) {
                    *range->mutable_lower_bound() = generate_sort_key(position);
                    range->set_lower_bound_included(true);
                }
                if (position + 1 < ids.size()) {
                    *range->mutable_upper_bound() = generate_sort_key(position + 1);
                    range->set_upper_bound_included(false);
                }
            } else if (!source->has_range()) {
                CHECK(source->has_range()) << "explicit merge fixtures must provide every source range";
            }
            result.emplace_back(std::move(copy));
        }
        return result;
    }

    static TabletMetadataPtr merge_fixture_copy(const TabletMetadataPB& source) {
        return std::make_shared<TabletMetadataPB>(source);
    }
    static TabletMetadataPtr merge_fixture_copy(const TabletMetadataPtr& source) { return source; }

    template <typename... Sources>
    Status put_merge_sources(const Sources&... sources) {
        for (const auto& source : merge_fixture_sources({merge_fixture_copy(sources)...})) {
            RETURN_IF_ERROR(put_tablet_metadata(source));
        }
        return Status::OK();
    }

    Status publish_resharding_merge(const std::vector<TabletMetadataPtr>& sources, int64_t merged_tablet,
                                    int64_t base_version, int64_t new_version, int64_t txn_id,
                                    std::unordered_map<int64_t, TabletMetadataPtr>& tablet_metadatas,
                                    const std::function<void()>& before_merge = {}) {
        for (const auto& source : merge_fixture_sources(sources)) RETURN_IF_ERROR(put_tablet_metadata(source));
        if (before_merge) before_merge();
        ReshardingTabletInfoPB resharding_tablet;
        auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
        for (const auto& source : sources) {
            merging_info.add_old_tablet_ids(source->id());
        }
        merging_info.set_new_tablet_id(merged_tablet);
        TxnInfoPB txn_info;
        txn_info.set_txn_id(txn_id);
        txn_info.set_commit_time(1);
        txn_info.set_gtid(1);
        std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
        return lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                               txn_info, false, tablet_metadatas, tablet_ranges);
    }

    StatusOr<TabletMetadataPtr> merge_modern_shared_occurrences(const TabletMetadataPtr& child_a,
                                                                const TabletMetadataPtr& child_b, int64_t merged_tablet,
                                                                int64_t base_version = 1, int64_t new_version = 2,
                                                                int64_t txn_id = 1) {
        std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
        RETURN_IF_ERROR(publish_resharding_merge({child_a, child_b}, merged_tablet, base_version, new_version, txn_id,
                                                 tablet_metadatas));
        return tablet_metadatas.at(merged_tablet);
    }

    std::shared_ptr<TabletMetadataPB> make_allocator_source(int64_t tablet_id, uint32_t next_rowset_id) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(1);
        metadata->set_next_rowset_id(next_rowset_id);
        set_primary_key_schema(metadata.get(), 1001);
        return metadata;
    }

    RowsetMetadataPB* add_allocator_rowset(TabletMetadataPB* metadata, uint32_t rowset_id, int64_t version,
                                           const std::string& segment_name, uint32_t segment_idx = 0,
                                           bool explicit_segment_idx = true) {
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(rowset_id);
        rowset->set_version(version);
        rowset->set_num_rows(1);
        rowset->set_data_size(1);
        rowset->set_overlapped(false);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(segment_name);
        segment->set_size(1);
        segment->set_num_rows(1);
        if (explicit_segment_idx) segment->set_segment_idx(segment_idx);
        lake::tablet_reshard_helper::set_rowset_uid(rowset);
        return rowset;
    }

    SegmentMetadataPB* add_allocator_segment(RowsetMetadataPB* rowset, const std::string& segment_name,
                                             uint32_t segment_idx) {
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(segment_name);
        segment->set_size(1);
        segment->set_num_rows(1);
        segment->set_segment_idx(segment_idx);
        rowset->set_num_rows(rowset->num_rows() + 1);
        rowset->set_data_size(rowset->data_size() + 1);
        return segment;
    }

    StatusOr<TabletMetadataPtr> publish_allocator_merge(
            const std::vector<std::shared_ptr<TabletMetadataPB>>& mutable_sources) {
        if (mutable_sources.empty()) return Status::InvalidArgument("allocator merge fixture has no source");
        const int64_t target_id = next_id();
        prepare_tablet_dirs(target_id);
        std::vector<TabletMetadataPtr> sources;
        sources.reserve(mutable_sources.size());
        for (const auto& source : mutable_sources) {
            prepare_tablet_dirs(source->id());
            sources.emplace_back(source);
        }
        std::unordered_map<int64_t, TabletMetadataPtr> published;
        RETURN_IF_ERROR(publish_resharding_merge(sources, target_id, /*base_version=*/1, /*new_version=*/2,
                                                 /*txn_id=*/next_id(), published));
        auto target = published.find(target_id);
        if (target == published.end()) return Status::InternalError("allocator merge target was not published");
        return target->second;
    }

    static std::vector<uint32_t> allocator_rowset_ids(const TabletMetadataPB& metadata) {
        std::vector<uint32_t> result;
        result.reserve(metadata.rowsets_size());
        for (const auto& rowset : metadata.rowsets()) result.emplace_back(rowset.id());
        return result;
    }

    struct MetadataOnlyMergeResult {
        int64_t target_tablet_id = 0;
        int64_t target_version = 0;
        std::map<int64_t, TabletMetadataPB> published;
        std::set<std::string> source_sst_filenames;
    };

    enum class MetadataOnlyMergeShape { kPrivate, kIdentical, kSharedDivergent };

    StatusOr<MetadataOnlyMergeResult> publish_metadata_only_merge_fixture(
            MetadataOnlyMergeShape shape, bool enable_tde = false, bool with_del_file = false,
            bool skip_source_flush = false,
            const std::function<void(std::vector<std::shared_ptr<TabletMetadataPB>>&)>& mutate_sources = {},
            int64_t* attempted_target_id = nullptr, int64_t* attempted_target_version = nullptr,
            const std::function<void(int64_t, int64_t, int64_t)>& before_publish = {}) {
        const int64_t base_version = next_id();
        const int64_t target_version = base_version + 1;
        const int64_t source_a_id = next_id();
        const int64_t source_b_id = next_id();
        const int64_t target_id = next_id();
        if (attempted_target_id != nullptr) *attempted_target_id = target_id;
        if (attempted_target_version != nullptr) *attempted_target_version = target_version;
        prepare_tablet_dirs(source_a_id);
        prepare_tablet_dirs(source_b_id);
        prepare_tablet_dirs(target_id);

        const bool old_tde = config::enable_transparent_data_encryption;
        config::enable_transparent_data_encryption = enable_tde;
        DeferOp restore_tde([&] { config::enable_transparent_data_encryption = old_tde; });
        if (enable_tde) ensure_kek_in_key_cache();

        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
        if (skip_source_flush) {
            set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
        }
        DeferOp restore_flush_failpoints([&] {
            set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
            set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
        });

        const bool identical = shape == MetadataOnlyMergeShape::kIdentical;
        const std::string segment_a = "metadata_only_a.dat";
        const std::string segment_b = identical ? segment_a : "metadata_only_b.dat";
        const uint64_t segment_a_size = write_two_column_segment(
                source_a_id, segment_a, /*num_rows=*/1, [](int key) { return key * 10; }, 10);
        uint64_t segment_b_size = segment_a_size;
        if (!identical) {
            segment_b_size = write_two_column_segment(
                    source_b_id, segment_b, /*num_rows=*/1, [](int key) { return key * 10; }, 60);
        }

        auto make_source = [&](int64_t tablet_id, int lower, int upper, uint32_t rowset_id,
                               const std::string& segment_filename, uint64_t segment_size) {
            auto metadata = std::make_shared<TabletMetadataPB>();
            metadata->set_id(tablet_id);
            metadata->set_version(base_version);
            metadata->set_next_rowset_id(rowset_id + 1);
            set_two_column_pk_schema(metadata.get(), /*schema_id=*/4001);
            metadata->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
            metadata->set_enable_persistent_index(true);
            metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
            metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(lower));
            metadata->mutable_range()->set_lower_bound_included(true);
            metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(upper));
            metadata->mutable_range()->set_upper_bound_included(false);
            auto* rowset = metadata->add_rowsets();
            rowset->set_id(rowset_id);
            rowset->set_version(base_version);
            rowset->set_num_rows(1);
            rowset->set_data_size(segment_size);
            rowset->set_overlapped(false);
            auto* segment = rowset->add_segment_metas();
            segment->set_filename(segment_filename);
            segment->set_size(segment_size);
            segment->set_num_rows(1);
            if (identical) {
                segment->set_shared(true);
                stamp_physical_identity_uid(rowset, segment_filename);
            }
            return metadata;
        };

        const uint32_t rowset_a = 1;
        const uint32_t rowset_b = identical ? 1 : 5;
        auto source_a = make_source(source_a_id, /*lower=*/0, /*upper=*/50, rowset_a, segment_a, segment_a_size);
        auto source_b = make_source(source_b_id, /*lower=*/50, /*upper=*/100, rowset_b, segment_b, segment_b_size);

        const std::string filename_a = identical ? "metadata_only_identical.sst" : "metadata_only_private_a.sst";
        const std::string filename_b = identical ? filename_a : "metadata_only_private_b.sst";
        const std::string key_a = encode_int_primary_key(10);
        const std::string key_b = encode_int_primary_key(identical ? 10 : 60);
        RawPkSstableFile file_a;
        RawPkSstableFile file_b;
        if (identical) {
            file_a = write_raw_pk_sstable(_tablet_manager->sst_location(source_a_id, filename_a),
                                          {{key_a, serialize_index_values({{base_version, /*rssid=*/1, /*rowid=*/0}})}},
                                          enable_tde);
            file_b = file_a;
        } else {
            file_a = write_raw_pk_sstable(_tablet_manager->sst_location(source_a_id, filename_a),
                                          {{key_a, serialize_index_values({{base_version, /*rssid=*/1, /*rowid=*/0}})}},
                                          enable_tde);
            file_b = write_raw_pk_sstable(_tablet_manager->sst_location(source_b_id, filename_b),
                                          {{key_b, serialize_index_values({{base_version, /*rssid=*/0, /*rowid=*/0}})}},
                                          enable_tde);
        }

        auto* sst_a = source_a->mutable_sstable_meta()->add_sstables();
        sst_a->set_filename(filename_a);
        sst_a->set_filesize(file_a.filesize);
        sst_a->set_encryption_meta(file_a.encryption_meta);
        sst_a->set_shared(shape != MetadataOnlyMergeShape::kPrivate);
        sst_a->set_shared_rssid(1);
        sst_a->set_shared_version(base_version);
        sst_a->set_max_rss_rowid(static_cast<uint64_t>(1) << 32);
        sst_a->set_generation_version(base_version);
        sst_a->mutable_range()->CopyFrom(file_a.range);
        sst_a->mutable_fileset_id()->set_hi(0x1111);
        sst_a->mutable_fileset_id()->set_lo(identical ? 0x3333 : 0x2222);

        auto* sst_b = source_b->mutable_sstable_meta()->add_sstables();
        sst_b->set_filename(filename_b);
        sst_b->set_filesize(file_b.filesize);
        sst_b->set_encryption_meta(file_b.encryption_meta);
        sst_b->set_shared(shape != MetadataOnlyMergeShape::kPrivate);
        if (shape != MetadataOnlyMergeShape::kPrivate) {
            sst_b->set_shared_rssid(shape == MetadataOnlyMergeShape::kSharedDivergent ? rowset_b : 1);
            sst_b->set_shared_version(base_version);
            sst_b->set_max_rss_rowid(static_cast<uint64_t>(sst_b->shared_rssid()) << 32);
        } else {
            // Legacy private form with a non-zero, source-local safe offset.
            sst_b->set_rssid_offset(rowset_b);
            sst_b->set_max_rss_rowid(static_cast<uint64_t>(rowset_b) << 32);
        }
        sst_b->set_generation_version(base_version);
        sst_b->mutable_range()->CopyFrom(file_b.range);
        sst_b->mutable_fileset_id()->set_hi(identical ? 0x1111 : 0x4444);
        sst_b->mutable_fileset_id()->set_lo(identical ? 0x3333 : 0x5555);

        if (with_del_file) {
            DelVector delvec;
            delvec.init(base_version, /*data=*/nullptr, /*length=*/0);
            add_delvec(source_a.get(), source_a_id, base_version, rowset_a, "metadata_only_a.delvec", delvec.save());
            sst_a->mutable_delvec()->CopyFrom(source_a->delvec_meta().delvecs().at(rowset_a));
        }

        std::vector<std::shared_ptr<TabletMetadataPB>> mutable_sources = {source_a, source_b};
        if (mutate_sources) mutate_sources(mutable_sources);
        std::vector<TabletMetadataPtr> sources(mutable_sources.begin(), mutable_sources.end());

        std::unordered_map<int64_t, TabletMetadataPtr> published;
        RETURN_IF_ERROR(
                publish_resharding_merge(sources, target_id, base_version, target_version, next_id(), published, [&] {
                    if (before_publish) before_publish(source_a_id, source_b_id, target_id);
                }));

        MetadataOnlyMergeResult result;
        result.target_tablet_id = target_id;
        result.target_version = target_version;
        for (const auto& [tablet_id, metadata] : published) {
            result.published.emplace(tablet_id, *metadata);
            if (tablet_id == target_id) continue;
            for (const auto& sstable : metadata->sstable_meta().sstables()) {
                result.source_sst_filenames.insert(sstable.filename());
            }
        }
        return result;
    }

    void expect_target_version_not_published(int64_t tablet_id, int64_t version) {
        auto metadata = _tablet_manager->get_tablet_metadata(tablet_id, version);
        EXPECT_TRUE(metadata.status().is_not_found()) << "target " << tablet_id << " version " << version
                                                      << " was unexpectedly published: " << metadata.status();
    }

    // Stores |sources| verbatim -- bypassing the uid / segment_idx stamping of put_tablet_metadata, so a
    // malformed fixture reaches the merge exactly as declared -- and merges them through the reshard publish.
    StatusOr<MutableTabletMetadataPtr> publish_verbatim_merge(const std::vector<TabletMetadataPtr>& sources,
                                                              int64_t target_tablet_id, int64_t target_version) {
        ReshardingTabletInfoPB resharding;
        auto* merging = resharding.mutable_merging_tablet_info();
        for (const auto& source : merge_fixture_sources(sources)) {
            RETURN_IF_ERROR(_tablet_manager->put_tablet_metadata(source));
            merging->add_old_tablet_ids(source->id());
        }
        merging->set_new_tablet_id(target_tablet_id);
        TxnInfoPB txn_info;
        txn_info.set_txn_id(next_id());
        txn_info.set_commit_time(1);
        txn_info.set_gtid(1);
        std::unordered_map<int64_t, TabletMetadataPtr> published;
        std::unordered_map<int64_t, TabletRangePB> ranges;
        RETURN_IF_ERROR(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, sources.front()->version(),
                                                        target_version, txn_info, false, published, ranges));
        return std::make_shared<TabletMetadataPB>(*published.at(target_tablet_id));
    }

    StatusOr<MutableTabletMetadataPtr> merge_tablet_directly(const std::vector<TabletMetadataPtr>& sources,
                                                             int64_t target_tablet_id, int64_t new_version,
                                                             bool read_alias) {
        MergingTabletInfoPB merging;
        for (const auto& source : sources) {
            merging.add_old_tablet_ids(source->id());
        }
        merging.set_new_tablet_id(target_tablet_id);
        TxnInfoPB txn_info;
        txn_info.set_txn_id(next_id());
        txn_info.set_commit_time(1);
        txn_info.set_gtid(1);
        return merge_tablet_or_read_alias(_tablet_manager.get(), sources, merging, new_version, txn_info, read_alias);
    }

    // Two children of a primary-key-range split, in the layout the production stall was found in: both
    // inherit the SAME rowset (same uid) over the same shared segment, their tablet ranges partition the
    // parent's, and each rowset carries its own effective range -- child A's narrower than its tablet range,
    // because update_rowset_range intersects at every reshard while a merge unions the tablet range. The
    // union of the two rowset ranges therefore covers only [rowset_lower_key, +inf) of the merged parent's
    // (-inf, +inf), which is what makes a gap the alias build used to try to convert into a rowid window.
    // A range-distributed primary-key tablet whose ORDER BY is (c1, c0): the shape whose tablet range lives
    // in PRIMARY-KEY space (one value) while its segments are laid out in sort-key order (two columns), so
    // no rowid interval of a segment corresponds to the range. Column unique ids and types match
    // write_two_column_segment's, so a segment written by that helper opens with this schema.
    void set_separate_sort_key_primary_key_schema(TabletMetadataPB* metadata, int64_t schema_id) {
        auto* schema = metadata->mutable_schema();
        schema->set_keys_type(PRIMARY_KEYS);
        schema->set_id(schema_id);
        schema->set_num_short_key_columns(1);
        auto* c0 = schema->add_column();
        c0->set_unique_id(1001);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
        auto* c1 = schema->add_column();
        c1->set_unique_id(1002);
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
        c1->set_aggregation("REPLACE");
        // ORDER BY (c1, c0): a sort key that is not the primary key.
        schema->add_sort_key_idxes(1);
        schema->add_sort_key_idxes(0);
    }

    std::pair<MutableTabletMetadataPtr, MutableTabletMetadataPtr> make_primary_key_range_split_children(
            int64_t child_a, int64_t child_b, const std::string& segment_name, int split_key,
            std::optional<int> rowset_lower_key) {
        auto make_child = [&](int64_t tablet_id, bool is_left) {
            auto metadata = std::make_shared<TabletMetadataPB>();
            metadata->set_id(tablet_id);
            metadata->set_version(1);
            metadata->set_next_rowset_id(2);
            set_separate_sort_key_primary_key_schema(metadata.get(), /*schema_id=*/9001);

            auto* range = metadata->mutable_range();
            if (is_left) {
                *range->mutable_upper_bound() = generate_sort_key(split_key);
                range->set_upper_bound_included(false);
            } else {
                *range->mutable_lower_bound() = generate_sort_key(split_key);
                range->set_lower_bound_included(true);
            }

            auto* rowset = metadata->add_rowsets();
            rowset->set_id(1);
            rowset->set_version(1);
            rowset->set_num_rows(10);
            rowset->set_data_size(100);
            auto* segment = rowset->add_segment_metas();
            segment->set_filename(segment_name);
            segment->set_size(100);
            segment->set_num_rows(10);
            // Stamped for the same reason a split stamps it: the rssid is rowset id + segment_idx, and
            // a source without it is refused outright by a real merge.
            segment->set_segment_idx(0);
            segment->set_shared(true);
            auto* rowset_range = rowset->mutable_range();
            if (is_left) {
                // No lower bound means this rowset claims everything below the split point, so the two
                // contributions cover the merged range and no gap arises.
                if (rowset_lower_key.has_value()) {
                    *rowset_range->mutable_lower_bound() = generate_sort_key(*rowset_lower_key);
                    rowset_range->set_lower_bound_included(true);
                }
                *rowset_range->mutable_upper_bound() = generate_sort_key(split_key);
                rowset_range->set_upper_bound_included(false);
            } else {
                *rowset_range->mutable_lower_bound() = generate_sort_key(split_key);
                rowset_range->set_lower_bound_included(true);
            }
            // Same uid on both children: one physical rowset handed to each by the split.
            stamp_physical_identity_uid(rowset, segment_name);
            return metadata;
        };
        return {make_child(child_a, /*is_left=*/true), make_child(child_b, /*is_left=*/false)};
    }

    // A rejected merge must leave nothing behind: the publish fails with Corruption, no file appears under the
    // data or metadata directory of any source or of the target, and the target version is never published.
    // The rejection must also come before any source primary-key index flush, so a flush attempt is made to
    // fail loudly here rather than being skipped as the fixture otherwise does.
    Status expect_merge_rejected_without_writes(const std::vector<TabletMetadataPtr>& sources, int64_t target_tablet_id,
                                                int64_t target_version) {
        std::vector<std::string> source_pbs;
        std::map<int64_t, std::set<std::string>> segment_inventories;
        std::map<int64_t, std::set<std::string>> metadata_inventories;
        for (const auto& source : merge_fixture_sources(sources)) {
            CHECK_OK(_tablet_manager->put_tablet_metadata(source));
        }
        for (const auto& source : sources) {
            prepare_tablet_dirs(source->id());
            source_pbs.emplace_back(source->SerializeAsString());
            ASSIGN_OR_ABORT(segment_inventories[source->id()],
                            directory_inventory(_location_provider->segment_root_location(source->id())));
            ASSIGN_OR_ABORT(metadata_inventories[source->id()],
                            directory_inventory(_location_provider->metadata_root_location(source->id())));
        }
        prepare_tablet_dirs(target_tablet_id);
        ASSIGN_OR_ABORT(segment_inventories[target_tablet_id],
                        directory_inventory(_location_provider->segment_root_location(target_tablet_id)));
        ASSIGN_OR_ABORT(metadata_inventories[target_tablet_id],
                        directory_inventory(_location_provider->metadata_root_location(target_tablet_id)));

        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("fail_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
        auto merged = publish_verbatim_merge(sources, target_tablet_id, target_version);
        set_failpoint_mode("fail_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);

        EXPECT_TRUE(merged.status().is_corruption()) << merged.status();
        for (size_t i = 0; i < sources.size(); ++i) {
            EXPECT_EQ(source_pbs[i], sources[i]->SerializeAsString());
        }
        for (const auto& [tablet_id, before] : segment_inventories) {
            ASSIGN_OR_ABORT(auto after, directory_inventory(_location_provider->segment_root_location(tablet_id)));
            EXPECT_EQ(before, after) << "segment inventory changed for tablet " << tablet_id;
        }
        for (const auto& [tablet_id, before] : metadata_inventories) {
            ASSIGN_OR_ABORT(auto after, directory_inventory(_location_provider->metadata_root_location(tablet_id)));
            EXPECT_EQ(before, after) << "metadata inventory changed for tablet " << tablet_id;
        }
        expect_target_version_not_published(target_tablet_id, target_version);
        return merged.status();
    }

    std::shared_ptr<TabletMetadataPB> make_preflight_sidecar_source(int64_t tablet_id,
                                                                    const std::string& segment_filename,
                                                                    bool shared_segment = false,
                                                                    bool common_rowset_uid = false) {
        auto metadata = make_allocator_source(tablet_id, /*next_rowset_id=*/2);
        auto* rowset = add_allocator_rowset(metadata.get(), /*rowset_id=*/1, /*version=*/1, segment_filename);
        rowset->mutable_segment_metas(0)->set_shared(shared_segment);
        if (common_rowset_uid) {
            rowset->clear_uid();
            stamp_physical_identity_uid(rowset, segment_filename);
        }
        return metadata;
    }

    // |shared_files| = true is two split siblings co-owning one segment and one SST. false gives each source its
    // own segment and its own SST inside its own range, so a rejection can only come from the SST declaration.
    std::vector<std::shared_ptr<TabletMetadataPB>> make_preflight_sst_sources(const std::string& stem,
                                                                              bool shared_files = true) {
        const std::string segment = stem + ".dat";
        auto source_a = make_preflight_sidecar_source(next_id(), shared_files ? segment : stem + "_a.dat",
                                                      /*shared_segment=*/shared_files,
                                                      /*common_rowset_uid=*/shared_files);
        auto source_b = make_preflight_sidecar_source(next_id(), shared_files ? segment : stem + "_b.dat",
                                                      /*shared_segment=*/shared_files,
                                                      /*common_rowset_uid=*/shared_files);
        for (int i = 0; i < 2; ++i) {
            auto* source = i == 0 ? source_a.get() : source_b.get();
            set_int_primary_key_schema(source, /*schema_id=*/4001);
            source->set_enable_persistent_index(true);
            source->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
            source->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(i * 50));
            source->mutable_range()->set_lower_bound_included(true);
            source->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key((i + 1) * 50));
            source->mutable_range()->set_upper_bound_included(false);
            source->mutable_rowsets(0)->mutable_range()->CopyFrom(source->range());
            auto* sst = source->mutable_sstable_meta()->add_sstables();
            sst->set_filename(shared_files ? stem + ".sst" : fmt::format("{}_{}.sst", stem, i));
            sst->set_filesize(128);
            sst->set_shared(shared_files);
            sst->set_shared_rssid(1);
            sst->set_shared_version(1);
            sst->set_max_rss_rowid(static_cast<uint64_t>(1) << 32);
            sst->set_generation_version(1);
            sst->mutable_range()->set_start_key(encode_int_primary_key(shared_files ? 10 : i * 50 + 10));
            sst->mutable_range()->set_end_key(encode_int_primary_key(shared_files ? 90 : i * 50 + 40));
        }
        return {std::move(source_a), std::move(source_b)};
    }

    std::vector<std::shared_ptr<TabletMetadataPB>> make_readable_preflight_sst_sources(const std::string& stem) {
        auto sources = make_preflight_sst_sources(stem);
        const std::string segment_name = stem + ".dat";
        const uint64_t segment_size =
                write_two_column_segment(sources[0]->id(), segment_name, 100, [](int key) { return key * 10; });
        std::vector<std::pair<std::string, std::string>> entries;
        entries.reserve(100);
        for (uint32_t key = 0; key < 100; ++key) {
            entries.emplace_back(encode_int_primary_key(key), serialize_index_values({{1, 1, key}}));
        }
        const auto sst_file = write_raw_pk_sstable(_tablet_manager->sst_location(sources[0]->id(), stem + ".sst"),
                                                   std::move(entries), /*encrypted=*/false);
        for (auto& source : sources) {
            source->clear_schema();
            set_two_column_pk_schema(source.get(), /*schema_id=*/4001);
            source->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
            auto* rowset = source->mutable_rowsets(0);
            rowset->set_num_rows(50);
            rowset->set_data_size(segment_size / 2);
            rowset->set_num_dels(0);
            auto* segment = rowset->mutable_segment_metas(0);
            segment->set_size(segment_size);
            segment->set_num_rows(100);
            segment->set_segment_idx(0);
            segment->set_shared(true);
            *segment->mutable_sort_key_min() = generate_sort_key(0);
            *segment->mutable_sort_key_max() = generate_sort_key(99);
            auto* sst = source->mutable_sstable_meta()->mutable_sstables(0);
            sst->set_version(1);
            sst->set_filesize(sst_file.filesize);
            sst->set_max_rss_rowid((uint64_t{1} << 32) | 99);
            *sst->mutable_range() = sst_file.range;
            sst->mutable_fileset_id()->set_hi(1);
            sst->mutable_fileset_id()->set_lo(1);
        }
        sources[1]->mutable_rowsets(0)->set_data_size(segment_size - segment_size / 2);
        return sources;
    }

    void add_delvec(TabletMetadataPB* metadata, int64_t tablet_id, int64_t version, uint32_t segment_id,
                    const std::string& file_name, const std::string& content) {
        FileMetaPB file_meta;
        file_meta.set_name(file_name);
        file_meta.set_size(content.size());
        (*metadata->mutable_delvec_meta()->mutable_version_to_file())[version] = file_meta;

        DelvecPagePB page;
        page.set_version(version);
        page.set_offset(0);
        page.set_size(content.size());
        (*metadata->mutable_delvec_meta()->mutable_delvecs())[segment_id] = page;

        write_file(_tablet_manager->delvec_location(tablet_id, file_name), content);
    }

    std::shared_ptr<TabletMetadataPB> make_shared_delvec_source(int64_t tablet_id,
                                                                const std::vector<std::string>& segment_filenames) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(1);
        metadata->set_next_rowset_id(segment_filenames.size() + 1);
        set_primary_key_schema(metadata.get(), 1001);

        auto* rowset = metadata->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10 * segment_filenames.size());
        rowset->set_data_size(100 * segment_filenames.size());
        uint32_t segment_idx = 0;
        for (const auto& segment_filename : segment_filenames) {
            auto* segment = rowset->add_segment_metas();
            segment->set_filename(segment_filename);
            segment->set_size(100);
            segment->set_num_rows(10);
            // A split always stamps this -- apply_segment_ownership_to_new_tablet_rowset synthesizes a
            // positional one when the source lacks it -- and a real merge now refuses a source segment
            // without it. Setting it to the position it already had leaves every reader unchanged,
            // since get_segment_idx falls back to exactly that when the field is absent.
            segment->set_segment_idx(segment_idx++);
            segment->set_shared(true);
        }
        stamp_physical_identity_uid(rowset, segment_filenames.front());
        return metadata;
    }

    StatusOr<TabletMetadataPtr> merge_delvec_sources(const std::vector<TabletMetadataPtr>& sources,
                                                     int64_t merged_tablet, int64_t new_version = 2,
                                                     int64_t txn_id = 10) {
        std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
        RETURN_IF_ERROR(publish_resharding_merge(sources, merged_tablet, /*base_version=*/1, new_version, txn_id,
                                                 tablet_metadatas));
        auto merged_it = tablet_metadatas.find(merged_tablet);
        if (merged_it == tablet_metadatas.end()) {
            return Status::InternalError("merged delvec test metadata is missing");
        }
        return merged_it->second;
    }

    struct ExpectedDelvecOutputPage {
        uint32_t rssid;
        std::string bytes;
        std::optional<uint32_t> crc32c;
    };

    void expect_exact_delvec_output(const TabletMetadataPB& metadata, int64_t tablet_id, int64_t version,
                                    const std::vector<ExpectedDelvecOutputPage>& expected_pages) {
        ASSERT_EQ(1, metadata.delvec_meta().version_to_file_size());
        ASSERT_TRUE(metadata.delvec_meta().version_to_file().contains(version));
        ASSERT_EQ(expected_pages.size(), metadata.delvec_meta().delvecs().size());
        const auto& output_file = metadata.delvec_meta().version_to_file().at(version);
        EXPECT_FALSE(output_file.name().empty());
        EXPECT_TRUE(output_file.has_size());
        EXPECT_TRUE(output_file.has_shared());
        EXPECT_FALSE(output_file.shared());
        EXPECT_FALSE(output_file.has_encryption_meta());
        EXPECT_TRUE(output_file.encryption_meta().empty());

        std::string expected_file_bytes;
        uint64_t expected_offset = 0;
        for (const auto& expected : expected_pages) {
            ASSERT_TRUE(metadata.delvec_meta().delvecs().contains(expected.rssid));
            const auto& page = metadata.delvec_meta().delvecs().at(expected.rssid);
            EXPECT_EQ(version, page.version());
            EXPECT_EQ(version, page.crc32c_gen_version());
            EXPECT_EQ(expected_offset, page.offset());
            EXPECT_EQ(expected.bytes.size(), page.size());
            if (expected.crc32c.has_value()) {
                EXPECT_TRUE(page.has_crc32c());
                EXPECT_EQ(*expected.crc32c, page.crc32c());
            } else {
                EXPECT_FALSE(page.has_crc32c());
            }
            expected_file_bytes.append(expected.bytes);
            expected_offset += expected.bytes.size();
        }
        EXPECT_EQ(static_cast<int64_t>(expected_file_bytes.size()), output_file.size());
        ASSIGN_OR_ABORT(auto reader,
                        fs::new_random_access_file(_tablet_manager->delvec_location(tablet_id, output_file.name())));
        ASSIGN_OR_ABORT(auto actual_file_bytes, reader->read_all());
        EXPECT_EQ(expected_file_bytes, actual_file_bytes);
    }

    std::string make_large_serialized_delvec(int64_t version, size_t minimum_size) {
        constexpr uint32_t kValuesPerContainer = 4097;
        constexpr uint32_t kLowValueStride = 2;
        constexpr size_t kBitsetContainerBytes = 1UL << 13;
        const uint32_t container_count = static_cast<uint32_t>(minimum_size / kBitsetContainerBytes + 2);
        std::vector<uint32_t> deleted_rowids;
        deleted_rowids.reserve(static_cast<size_t>(container_count) * kValuesPerContainer);
        for (uint32_t high = 0; high < container_count; ++high) {
            for (uint32_t index = 0; index < kValuesPerContainer; ++index) {
                deleted_rowids.push_back((high << 16) | (index * kLowValueStride));
            }
        }
        DelVector delvec;
        delvec.init(version, deleted_rowids.data(), deleted_rowids.size());
        std::string bytes = delvec.save();
        EXPECT_GT(bytes.size(), minimum_size);
        return bytes;
    }

    void add_sstable(TabletMetadataPB* metadata, const std::string& filename, uint64_t max_rss_rowid,
                     bool with_delvec) {
        auto* sstable = metadata->mutable_sstable_meta()->add_sstables();
        sstable->set_filename(filename);
        sstable->set_max_rss_rowid(max_rss_rowid);
        if (with_delvec) {
            sstable->mutable_delvec()->set_version(1);
        }
    }

    // Write a real PK-index sstable file for tests that need to exercise the
    // legacy-shared-sstable rebuild path (which opens the source file). Each
    // entry maps a key to (rssid, rowid, version=1). Returns the file size,
    // so callers can populate sst.set_filesize() consistently.
    uint64_t write_legacy_pk_sstable(const std::string& path,
                                     const std::vector<std::tuple<std::string, uint32_t, uint32_t>>& entries) {
        WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        auto wf_or = fs::new_writable_file(opts, path);
        CHECK_OK(wf_or.status());
        auto wf = std::move(wf_or.value());

        phmap::btree_map<std::string, lake::IndexValueWithVer, std::less<>> map;
        for (const auto& [key, rssid, rowid] : entries) {
            uint64_t packed = (static_cast<uint64_t>(rssid) << 32) | rowid;
            map.emplace(key, std::make_pair(int64_t{1}, IndexValue(packed)));
        }
        uint64_t filesz = 0;
        PersistentIndexSstableRangePB range_pb;
        CHECK_OK(lake::PersistentIndexSstable::build_sstable(map, wf.get(), &filesz, &range_pb));
        CHECK_OK(wf->close());
        return filesz;
    }

    uint64_t write_versioned_pk_sstable(
            const std::string& path, const std::vector<std::tuple<std::string, int64_t, uint32_t, uint32_t>>& entries) {
        WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        auto wf_or = fs::new_writable_file(opts, path);
        CHECK_OK(wf_or.status());
        auto wf = std::move(wf_or.value());

        phmap::btree_map<std::string, lake::IndexValueWithVer, std::less<>> map;
        for (const auto& [key, version, rssid, rowid] : entries) {
            uint64_t packed = (static_cast<uint64_t>(rssid) << 32) | rowid;
            map.emplace(key, std::make_pair(version, IndexValue(packed)));
        }
        uint64_t filesz = 0;
        PersistentIndexSstableRangePB range_pb;
        CHECK_OK(lake::PersistentIndexSstable::build_sstable(map, wf.get(), &filesz, &range_pb));
        CHECK_OK(wf->close());
        return filesz;
    }

    struct RawPkSstableFile {
        uint64_t filesize = 0;
        PersistentIndexSstableRangePB range;
        std::string encryption_meta;
    };

    void ensure_kek_in_key_cache() {
        if (KeyCache::instance().get_key("0000000000000000") != nullptr) {
            return;
        }
        EncryptionKeyPB key_pb;
        key_pb.set_id(EncryptionKey::DEFAULT_MASTER_KYE_ID);
        key_pb.set_type(EncryptionKeyTypePB::NORMAL_KEY);
        key_pb.set_algorithm(EncryptionAlgorithmPB::AES_128);
        key_pb.set_plain_key("0000000000000000");
        auto root_key = EncryptionKey::create_from_pb(key_pb).value();
        auto kek = root_key->generate_key().value();
        kek->set_id(2);
        KeyCache::instance().add_key(root_key);
        KeyCache::instance().add_key(kek);
    }

    std::string serialize_index_values(const std::vector<std::tuple<int64_t, uint32_t, uint32_t>>& values) const {
        IndexValuesWithVerPB values_pb;
        for (const auto& [version, rssid, rowid] : values) {
            auto* value = values_pb.add_values();
            value->set_version(version);
            value->set_rssid(rssid);
            value->set_rowid(rowid);
        }
        return values_pb.SerializeAsString();
    }

    RawPkSstableFile write_raw_pk_sstable(const std::string& path,
                                          std::vector<std::pair<std::string, std::string>> entries,
                                          bool encrypted = false) {
        std::sort(entries.begin(), entries.end(), [](const auto& lhs, const auto& rhs) {
            return sstable::BytewiseComparator()->Compare(Slice(lhs.first), Slice(rhs.first)) < 0;
        });

        RawPkSstableFile result;
        WritableFileOptions write_options{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        if (encrypted) {
            ensure_kek_in_key_cache();
            auto encryption_pair = KeyCache::instance().create_encryption_meta_pair_using_current_kek().value();
            write_options.encryption_info = encryption_pair.info;
            result.encryption_meta = std::move(encryption_pair.encryption_meta);
        }
        auto writable_file_or = fs::new_writable_file(write_options, path);
        CHECK_OK(writable_file_or.status());
        auto writable_file = std::move(writable_file_or.value());

        std::unique_ptr<sstable::FilterPolicy> filter_policy(
                const_cast<sstable::FilterPolicy*>(sstable::NewBloomFilterPolicy(10)));
        sstable::Options options;
        options.filter_policy = filter_policy.get();
        sstable::TableBuilder builder(options, writable_file.get());
        for (const auto& [key, value] : entries) {
            CHECK_OK(builder.Add(Slice(key), Slice(value)));
        }
        CHECK_OK(builder.Finish());
        result.filesize = builder.FileSize();
        if (!entries.empty()) {
            auto [start_key, end_key] = builder.KeyRange();
            result.range.set_start_key(start_key.to_string());
            result.range.set_end_key(end_key.to_string());
        }
        CHECK_OK(writable_file->close());
        return result;
    }

    RawPkSstableFile write_sidecar_payload(const std::string& path, const std::string& payload, bool encrypted) {
        RawPkSstableFile result;
        WritableFileOptions options{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        if (encrypted) {
            ensure_kek_in_key_cache();
            auto encryption_pair = KeyCache::instance().create_encryption_meta_pair_using_current_kek().value();
            options.encryption_info = encryption_pair.info;
            result.encryption_meta = std::move(encryption_pair.encryption_meta);
        }
        ASSIGN_OR_ABORT(auto writer, fs::new_writable_file(options, path));
        CHECK_OK(writer->append(payload));
        CHECK_OK(writer->close());
        result.filesize = payload.size();
        return result;
    }

    StatusOr<std::string> read_sidecar_payload(const std::string& path, const std::string& encryption_meta) {
        RandomAccessFileOptions options;
        if (!encryption_meta.empty()) {
            ASSIGN_OR_RETURN(options.encryption_info, KeyCache::instance().unwrap_encryption_meta(encryption_meta));
        }
        ASSIGN_OR_RETURN(auto reader, fs::new_random_access_file(options, path));
        return reader->read_all();
    }

    struct BelowFloorLegacyFixture {
        static constexpr int64_t kBaseVersion = 1;
        static constexpr int64_t kMergedVersion = 2;
        static constexpr uint32_t kSourceLiveRssid = 107;

        int64_t merged_tablet = 0;
        std::string source_filename;
        std::shared_ptr<TabletMetadataPB> cold_metadata;
        std::shared_ptr<TabletMetadataPB> hot_metadata;
    };

    BelowFloorLegacyFixture make_below_floor_legacy_fixture(
            const std::string& source_filename, const std::vector<std::pair<std::string, std::string>>& entries,
            uint32_t source_high) {
        BelowFloorLegacyFixture fixture;
        const int64_t cold_tablet = next_id();
        const int64_t hot_tablet = next_id();
        fixture.merged_tablet = next_id();
        fixture.source_filename = source_filename;
        const std::string source_path = _tablet_manager->sst_location(hot_tablet, source_filename);
        const std::string segment_filename = source_filename + ".dat";
        prepare_tablet_dirs(cold_tablet);
        prepare_tablet_dirs(hot_tablet);
        prepare_tablet_dirs(fixture.merged_tablet);

        const uint64_t segment_size = write_two_column_segment(
                hot_tablet, segment_filename, /*num_rows=*/1, [](int key) { return key * 10; }, 20);
        const auto source_file = write_raw_pk_sstable(source_path, entries);

        auto make_metadata = [&](int64_t tablet_id) {
            auto metadata = std::make_shared<TabletMetadataPB>();
            metadata->set_id(tablet_id);
            metadata->set_version(BelowFloorLegacyFixture::kBaseVersion);
            set_two_column_pk_schema(metadata.get(), /*schema_id=*/4001);
            metadata->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
            metadata->set_enable_persistent_index(true);
            metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
            return metadata;
        };

        fixture.cold_metadata = make_metadata(cold_tablet);
        fixture.cold_metadata->set_next_rowset_id(1);

        fixture.hot_metadata = make_metadata(hot_tablet);
        fixture.hot_metadata->set_next_rowset_id(BelowFloorLegacyFixture::kSourceLiveRssid + 1);
        auto* rowset = fixture.hot_metadata->add_rowsets();
        rowset->set_id(BelowFloorLegacyFixture::kSourceLiveRssid);
        rowset->set_version(BelowFloorLegacyFixture::kBaseVersion);
        rowset->set_num_rows(1);
        rowset->set_data_size(segment_size);
        rowset->set_overlapped(false);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(segment_filename);
        segment->set_size(segment_size);
        segment->set_num_rows(1);

        auto* source_pb = fixture.hot_metadata->mutable_sstable_meta()->add_sstables();
        source_pb->set_version(13);
        source_pb->set_filename(source_filename);
        source_pb->set_filesize(source_file.filesize);
        source_pb->set_max_rss_rowid((static_cast<uint64_t>(source_high) << 32) | 9);
        source_pb->set_encryption_meta(source_file.encryption_meta);
        source_pb->set_shared(false);
        source_pb->set_rssid_offset(0);
        if (!entries.empty()) {
            source_pb->mutable_range()->CopyFrom(source_file.range);
        }
        source_pb->mutable_fileset_id()->set_hi(0x13579);
        source_pb->mutable_fileset_id()->set_lo(0x24680);
        source_pb->set_generation_version(17);

        return fixture;
    }

    StatusOr<std::set<std::string>> directory_inventory(const std::string& directory) {
        std::set<std::string> files;
        RETURN_IF_ERROR(FileSystem::Default()->iterate_dir(directory, [&](std::string_view name) {
            files.emplace(name);
            return true;
        }));
        return files;
    }

    StatusOr<std::set<std::string>> delvec_inventory(int64_t tablet_id) {
        ASSIGN_OR_RETURN(auto files, directory_inventory(_location_provider->segment_root_location(tablet_id)));
        std::erase_if(files, [](const std::string& file) {
            constexpr std::string_view kSuffix = ".delvec";
            return file.size() < kSuffix.size() ||
                   file.compare(file.size() - kSuffix.size(), kSuffix.size(), kSuffix) != 0;
        });
        return files;
    }

    StatusOr<std::set<std::string>> sst_inventory(int64_t tablet_id) {
        ASSIGN_OR_RETURN(auto files, directory_inventory(_location_provider->segment_root_location(tablet_id)));
        std::erase_if(files, [](const std::string& file) { return !file.ends_with(".sst"); });
        return files;
    }

    // contents still need a real file when production performs a conservative
    // exact owner scan. Tombstones have no segment owner, so they preserve the
    // metadata-only result those tests are intended to inspect.
    void materialize_tombstone_sstables(TabletMetadataPB* metadata) {
        const uint32_t tombstone = std::numeric_limits<uint32_t>::max();
        for (auto& sst : *metadata->mutable_sstable_meta()->mutable_sstables()) {
            const uint64_t filesize =
                    write_legacy_pk_sstable(_tablet_manager->sst_location(metadata->id(), sst.filename()),
                                            {{fmt::format("tombstone-{}", sst.filename()), tombstone, tombstone}});
            sst.set_filesize(filesize);
        }
    }

    void add_dcg(TabletMetadataPB* metadata, uint32_t segment_id, const std::string& file_name) {
        DeltaColumnGroupVerPB dcg;
        dcg.add_column_files(file_name);
        metadata->mutable_dcg_meta()->mutable_dcgs()->insert({segment_id, dcg});
    }

    void add_dcg_with_columns(TabletMetadataPB* metadata, uint32_t segment_id, const std::string& file_name,
                              const std::vector<uint32_t>& column_ids, int64_t version, bool shared_file = true) {
        auto& dcg = (*metadata->mutable_dcg_meta()->mutable_dcgs())[segment_id];
        dcg.add_column_files(file_name);
        auto* cids = dcg.add_unique_column_ids();
        for (auto cid : column_ids) {
            cids->add_column_ids(cid);
        }
        dcg.add_versions(version);
        dcg.add_shared_files(shared_file);
    }

    // IDG (.idx) test helpers, peers of add_dcg_with_columns. add_idg_with_key creates a
    // single-entry IDG for |segment_id| with one (col_uid, type) key; add_idg_key appends a
    // second key to that entry; add_idg_dropped_key appends a DROP INDEX tombstone.
    void add_idg_with_key(TabletMetadataPB* metadata, uint32_t segment_id, const std::string& file_name,
                          int32_t col_uid, IndexType type, int64_t version, bool shared_file = true) {
        auto& idg = (*metadata->mutable_idg_meta()->mutable_idgs())[segment_id];
        auto* e = idg.add_entries();
        e->set_index_file(file_name);
        e->set_version(version);
        e->set_shared_file(shared_file);
        auto* k = e->add_keys();
        k->set_col_unique_id(col_uid);
        k->set_index_type(type);
    }
    void add_idg_key(TabletMetadataPB* metadata, uint32_t segment_id, int32_t col_uid, IndexType type) {
        auto& idg = (*metadata->mutable_idg_meta()->mutable_idgs())[segment_id];
        ASSERT_GT(idg.entries_size(), 0);
        auto* k = idg.mutable_entries(0)->add_keys();
        k->set_col_unique_id(col_uid);
        k->set_index_type(type);
    }
    void add_idg_dropped_key(TabletMetadataPB* metadata, uint32_t segment_id, int32_t col_uid, IndexType type) {
        auto& idg = (*metadata->mutable_idg_meta()->mutable_idgs())[segment_id];
        ASSERT_GT(idg.entries_size(), 0);
        auto* dk = idg.mutable_entries(0)->add_dropped_keys();
        dk->set_col_unique_id(col_uid);
        dk->set_index_type(type);
    }

    // Build a two-column INT primary-key tablet schema in |metadata|'s
    // `schema` field. `c0` is the key (also the sort key); `c1` is a plain
    // data column. The returned (c0_uid, c1_uid) can be used to cross-
    // reference the columns from DCG metadata.
    std::pair<int32_t, int32_t> set_two_column_pk_schema(TabletMetadataPB* metadata, int64_t schema_id) {
        auto* schema = metadata->mutable_schema();
        schema->set_keys_type(PRIMARY_KEYS);
        schema->set_id(schema_id);
        schema->set_num_short_key_columns(1);
        schema->set_num_rows_per_row_block(65535);
        auto* c0 = schema->add_column();
        const int32_t c0_uid = 1001;
        c0->set_unique_id(c0_uid);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
        c0->set_length(4);
        c0->set_index_length(4);
        auto* c1 = schema->add_column();
        const int32_t c1_uid = 1002;
        c1->set_unique_id(c1_uid);
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
        c1->set_aggregation("REPLACE");
        return {c0_uid, c1_uid};
    }

    // Build a DUP_KEYS tablet schema whose sort key is a single, genuinely truncated VARCHAR
    // column: `k0` is the key and the sort key, `v0` is a plain data column. index_length(4) is
    // shorter than the 6-digit key width write_varchar_key_segment writes, so the legacy short key
    // index this schema produces cannot decode back to the whole sort key
    // (short_key_index_encodes_full_sort_key rejects it) and sampling must take path B, reading
    // data pages instead (sort_key_sampler.h).
    void set_varchar_sort_key_schema(TabletMetadataPB* metadata, int64_t schema_id) {
        auto* schema = metadata->mutable_schema();
        schema->set_keys_type(DUP_KEYS);
        schema->set_id(schema_id);
        schema->set_num_short_key_columns(1);
        schema->set_num_rows_per_row_block(65535);
        auto* k0 = schema->add_column();
        k0->set_unique_id(1001);
        k0->set_name("k0");
        k0->set_type("VARCHAR");
        k0->set_is_key(true);
        k0->set_is_nullable(false);
        k0->set_length(32);
        k0->set_index_length(4);
        auto* v0 = schema->add_column();
        v0->set_unique_id(1002);
        v0->set_name("v0");
        v0->set_type("INT");
        v0->set_is_key(false);
        v0->set_is_nullable(false);
        v0->set_aggregation("REPLACE");
        schema->add_sort_key_idxes(0);
    }

    // Build a single-rowset PK tablet with one two-column segment and NO sstable_meta, so
    // flush_pk_memtable takes the cold rebuild-from-segment path (rebuild_rss_id==0 ->
    // needs_rowset_rebuild) and produces exactly one fresh sstable. |seg_name|/|seg_size|
    // come from a prior write_two_column_segment. No rowset_to_schema mapping is set: the
    // rowset uses the tablet's main c0/c1 PK schema (the same schema the segment was written
    // with), so no historical_schemas entry is needed.
    std::shared_ptr<TabletMetadataPB> make_single_segment_pk_tablet(int64_t tablet_id, int64_t version,
                                                                    const std::string& seg_name, uint64_t seg_size,
                                                                    int num_rows) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(version);
        meta->set_next_rowset_id(10);
        set_two_column_pk_schema(meta.get(), /*schema_id=*/4001);
        meta->set_enable_persistent_index(true);
        meta->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(version);
        rowset->set_num_rows(num_rows);
        rowset->set_data_size(seg_size);
        auto* sm = rowset->add_segment_metas();
        sm->set_filename(seg_name);
        sm->set_size(seg_size);
        sm->set_num_rows(num_rows);
        return meta;
    }

    // Write a real Segment file with num_rows rows: c0 = [0..num_rows), c1 = source_value_of(c0).
    // Returns the segment file size on disk. The file is placed under
    // tablet_id's segment directory as |segment_name|.
    uint64_t write_two_column_segment(int64_t tablet_id, const std::string& segment_name, int num_rows,
                                      const std::function<int(int)>& source_value_of, int key_start = 0,
                                      const std::function<int(int)>& key_of = {}) {
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
        auto segment_path = _tablet_manager->segment_location(tablet_id, segment_name);

        WritableFileOptions fopts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        auto wfile_or = fs::new_writable_file(fopts, segment_path);
        CHECK_OK(wfile_or.status());

        SegmentWriterOptions opts;
        SegmentWriter writer(std::move(wfile_or.value()), 0, tablet_schema, opts);
        CHECK_OK(writer.init());

        auto col0 = Int32Column::create();
        auto col1 = Int32Column::create();
        std::vector<int> v0(num_rows), v1(num_rows);
        for (int i = 0; i < num_rows; ++i) {
            v0[i] = key_of ? key_of(i) : key_start + i;
            v1[i] = source_value_of(v0[i]);
        }
        col0->append_numbers(v0.data(), v0.size() * sizeof(int));
        col1->append_numbers(v1.data(), v1.size() * sizeof(int));
        auto chunk_schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));
        auto chunk = std::make_shared<Chunk>(Columns{std::move(col0), std::move(col1)}, chunk_schema);
        CHECK_OK(writer.append_chunk(*chunk));

        uint64_t segment_file_size = 0, index_size = 0, footer_position = 0;
        CHECK_OK(writer.finalize(&segment_file_size, &index_size, &footer_position));
        return segment_file_size;
    }

    // Write a real Segment file matching set_varchar_sort_key_schema: k0 = zero-padded [key_start,
    // key_start+num_rows), v0 = k0's integer value. Returns the segment file size on disk. Zero
    // padding keeps byte order == numeric order, which is what lets the sampler's monotonicity
    // validation see a well-formed segment.
    uint64_t write_varchar_key_segment(int64_t tablet_id, const std::string& segment_name, int num_rows,
                                       int key_start = 0) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_id(2101);
        schema_pb.set_num_short_key_columns(1);
        schema_pb.set_num_rows_per_row_block(65535);
        auto* k0 = schema_pb.add_column();
        k0->set_unique_id(1001);
        k0->set_name("k0");
        k0->set_type("VARCHAR");
        k0->set_is_key(true);
        k0->set_is_nullable(false);
        k0->set_length(32);
        k0->set_index_length(4);
        auto* v0 = schema_pb.add_column();
        v0->set_unique_id(1002);
        v0->set_name("v0");
        v0->set_type("INT");
        v0->set_is_key(false);
        v0->set_is_nullable(false);
        v0->set_aggregation("REPLACE");
        schema_pb.add_sort_key_idxes(0);

        auto tablet_schema = TabletSchema::create(schema_pb);
        auto segment_path = _tablet_manager->segment_location(tablet_id, segment_name);

        WritableFileOptions fopts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        auto wfile_or = fs::new_writable_file(fopts, segment_path);
        CHECK_OK(wfile_or.status());

        SegmentWriterOptions opts;
        SegmentWriter writer(std::move(wfile_or.value()), 0, tablet_schema, opts);
        CHECK_OK(writer.init());

        auto col0 = BinaryColumn::create();
        auto col1 = Int32Column::create();
        std::vector<int> v1(num_rows);
        for (int i = 0; i < num_rows; ++i) {
            const int key = key_start + i;
            col0->append(Slice(fmt::format("{:06d}", key)));
            v1[i] = key;
        }
        col1->append_numbers(v1.data(), v1.size() * sizeof(int));
        auto chunk_schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));
        auto chunk = std::make_shared<Chunk>(Columns{std::move(col0), std::move(col1)}, chunk_schema);
        CHECK_OK(writer.append_chunk(*chunk));

        uint64_t segment_file_size = 0, index_size = 0, footer_position = 0;
        CHECK_OK(writer.finalize(&segment_file_size, &index_size, &footer_position));
        return segment_file_size;
    }

    std::vector<std::pair<uint64_t, int64_t>> write_two_column_bundled_segments(
            int64_t tablet_id, const std::string& bundle_name, int num_rows,
            const std::function<int(int)>& source_value_of, int key_start = 0) {
        const std::string slice_name = fmt::format("{}.slice_{}", bundle_name, next_id());
        const uint64_t slice_size =
                write_two_column_segment(tablet_id, slice_name, num_rows, source_value_of, key_start);
        const std::string slice_path = _tablet_manager->segment_location(tablet_id, slice_name);
        ASSIGN_OR_ABORT(auto reader, fs::new_random_access_file(slice_path));
        ASSIGN_OR_ABORT(auto slice_contents, reader->read_all());

        const std::string empty_slice_name = fmt::format("{}.empty_slice_{}", bundle_name, next_id());
        const uint64_t empty_slice_size =
                write_two_column_segment(tablet_id, empty_slice_name, 0, source_value_of, key_start + num_rows);
        const std::string empty_slice_path = _tablet_manager->segment_location(tablet_id, empty_slice_name);
        ASSIGN_OR_ABORT(auto empty_reader, fs::new_random_access_file(empty_slice_path));
        ASSIGN_OR_ABORT(auto empty_slice_contents, empty_reader->read_all());

        const std::string prefix = "tablet-merge-bundle-prefix";
        const int64_t slice_offset = prefix.size();
        const int64_t empty_slice_offset = slice_offset + static_cast<int64_t>(slice_size);
        write_file(_tablet_manager->segment_location(tablet_id, bundle_name),
                   prefix + slice_contents + empty_slice_contents);
        CHECK_OK(fs::delete_file(slice_path));
        CHECK_OK(fs::delete_file(empty_slice_path));
        return {{slice_size, slice_offset}, {empty_slice_size, empty_slice_offset}};
    }

    // Write a real .cols file for column c1 only, with `num_rows` entries.
    // cell_value(row) supplies the c1 value at segment row |row|.
    uint64_t write_c1_only_cols_file(int64_t tablet_id, const std::string& cols_filename, int num_rows,
                                     const std::function<int(int)>& cell_value, bool encrypted = false,
                                     std::string* encryption_meta = nullptr) {
        TabletSchemaPB full_pb;
        full_pb.set_keys_type(PRIMARY_KEYS);
        full_pb.set_id(3001);
        full_pb.set_num_short_key_columns(1);
        full_pb.set_num_rows_per_row_block(65535);
        auto* c0 = full_pb.add_column();
        c0->set_unique_id(1001);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
        auto* c1 = full_pb.add_column();
        c1->set_unique_id(1002);
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
        c1->set_aggregation("REPLACE");

        auto full_schema = TabletSchema::create(full_pb);
        auto cols_schema = TabletSchema::create_with_uid(full_schema, std::vector<ColumnUID>{1002});

        auto cols_path = _tablet_manager->segment_location(tablet_id, cols_filename);
        WritableFileOptions fopts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        if (encrypted) {
            ensure_kek_in_key_cache();
            auto encryption_pair = KeyCache::instance().create_encryption_meta_pair_using_current_kek().value();
            fopts.encryption_info = encryption_pair.info;
            if (encryption_meta != nullptr) *encryption_meta = std::move(encryption_pair.encryption_meta);
        } else if (encryption_meta != nullptr) {
            encryption_meta->clear();
        }
        auto wfile_or = fs::new_writable_file(fopts, cols_path);
        CHECK_OK(wfile_or.status());

        SegmentWriterOptions opts;
        SegmentWriter writer(std::move(wfile_or.value()), 0, cols_schema, opts);
        CHECK_OK(writer.init(false));

        auto col = Int32Column::create();
        std::vector<int> values(num_rows);
        for (int i = 0; i < num_rows; ++i) values[i] = cell_value(i);
        col->append_numbers(values.data(), values.size() * sizeof(int));
        auto chunk_schema = std::make_shared<Schema>(ChunkHelper::convert_schema(cols_schema));
        auto chunk = std::make_shared<Chunk>(Columns{std::move(col)}, chunk_schema);
        CHECK_OK(writer.append_chunk(*chunk));

        uint64_t segment_file_size = 0, index_size = 0, footer_position = 0;
        CHECK_OK(writer.finalize(&segment_file_size, &index_size, &footer_position));
        return segment_file_size;
    }

    // Open a .cols file that contains only column c1 (UID 1002) and return
    // its materialized integer values.
    std::vector<int32_t> read_c1_only_cols_file(int64_t tablet_id, const std::string& cols_filename) {
        TabletSchemaPB full_pb;
        full_pb.set_keys_type(PRIMARY_KEYS);
        full_pb.set_id(3002);
        full_pb.set_num_short_key_columns(1);
        full_pb.set_num_rows_per_row_block(65535);
        auto* c0 = full_pb.add_column();
        c0->set_unique_id(1001);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
        auto* c1 = full_pb.add_column();
        c1->set_unique_id(1002);
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
        c1->set_aggregation("REPLACE");

        auto full_schema = TabletSchema::create(full_pb);
        auto cols_schema = TabletSchema::create_with_uid(full_schema, std::vector<ColumnUID>{1002});

        FileInfo file_info;
        file_info.path = _tablet_manager->segment_location(tablet_id, cols_filename);
        auto fs_or = FileSystemFactory::CreateSharedFromString(file_info.path);
        CHECK_OK(fs_or.status());
        auto segment_or = Segment::open(fs_or.value(), file_info, 0, cols_schema);
        CHECK_OK(segment_or.status());
        auto segment = segment_or.value();

        SegmentReadOptions read_options;
        OlapReaderStatistics stats;
        read_options.stats = &stats;
        ASSIGN_OR_ABORT(read_options.fs, FileSystemFactory::CreateSharedFromString(file_info.path));
        read_options.tablet_id = tablet_id;
        read_options.rowset_id = 0;
        read_options.version = 1;
        Schema iter_schema = ChunkHelper::convert_schema(cols_schema);
        auto iter_or = segment->new_iterator(iter_schema, read_options);
        CHECK_OK(iter_or.status());

        std::vector<int32_t> result;
        auto chunk = ChunkFactory::new_chunk(iter_schema, 4096);
        while (true) {
            chunk->reset();
            auto status = iter_or.value()->get_next(chunk.get());
            if (status.is_end_of_file()) break;
            CHECK_OK(status);
            auto col = chunk->get_column_by_index(0);
            for (size_t i = 0; i < col->size(); ++i) {
                result.push_back(col->get(i).get_int32());
            }
        }
        return result;
    }

    // Drive a 3-way PK merge where two surviving children update column c1 on the
    // shared base segment (same-column DCG conflict -> rebuild) and the child at
    // |compacted_index| has compacted its share away, leaving a gap on canonical
    // R0. Before the gap fix the rebuild's coverage check rejected the gap with
    // NotSupported; now it accepts the masked gap and fills those rows from the
    // base segment. Verifies the rebuilt .cols row values (surviving children's
    // updates on their windows, base values on the gap) and that a gap delvec
    // masks the compacted child's rows.
    void run_dcg_conflict_gap_rebuild_case(int compacted_index, int64_t txn_id) {
        const int64_t base_version = 1;
        const int64_t new_version = 2;
        constexpr int kNumRows = 30;
        constexpr int kRangeRows = 10; // three equal key ranges: [0,10) [10,20) [20,30)
        constexpr int64_t kSchemaId = 4001;
        constexpr uint32_t kSharedRowsetId = 1;

        const int64_t child_ids[3] = {next_id(), next_id(), next_id()};
        const int64_t merged_tablet = next_id();
        for (int64_t child_id : child_ids) prepare_tablet_dirs(child_id);
        prepare_tablet_dirs(merged_tablet);

        // Base segment: c0 = row index (key == rowid), c1 = row * 10.
        auto base_value_of = [](int row) { return row * 10; };
        auto update_of = [](int child_index, int row) { return row + 100000 * (child_index + 1); };
        const std::string shared_segment_name = "shared_seg.dat";
        const uint64_t base_segment_size =
                write_two_column_segment(merged_tablet, shared_segment_name, kNumRows, base_value_of);

        auto set_key_range = [&](TabletRangePB* range, int lower_key, int upper_key) {
            range->set_lower_bound_included(true);
            range->set_upper_bound_included(false);
            *range->mutable_lower_bound() = generate_sort_key(lower_key);
            *range->mutable_upper_bound() = generate_sort_key(upper_key);
        };

        for (int i = 0; i < 3; ++i) {
            const int lower = i * kRangeRows;
            const int upper = (i + 1) * kRangeRows;
            auto meta = std::make_shared<TabletMetadataPB>();
            meta->set_id(child_ids[i]);
            meta->set_version(base_version);
            meta->set_next_rowset_id(10);
            const auto [c0_uid, c1_uid] = set_two_column_pk_schema(meta.get(), kSchemaId);
            (void)c0_uid;
            set_key_range(meta->mutable_range(), lower, upper);

            if (i == compacted_index) {
                // Compacted child: a non-shared compaction output rowset (newer
                // version) covering this range. No shared segment, no DCG -> its
                // range is a gap on canonical R0.
                auto* rowset = meta->add_rowsets();
                rowset->set_id(2);
                rowset->set_version(new_version);
                rowset->set_num_rows(upper - lower);
                rowset->set_data_size(100);
                auto* segment_meta = rowset->add_segment_metas();
                segment_meta->set_filename(fmt::format("compacted_{}.dat", i));
                segment_meta->set_size(100);
                segment_meta->set_num_rows(upper - lower);
                set_key_range(rowset->mutable_range(), lower, upper);
                (*meta->mutable_rowset_to_schema())[2] = kSchemaId;
            } else {
                // Surviving child: shares the base segment and updates c1 on its
                // owned row window via a real .cols file (base copy-through
                // elsewhere, mirroring production partial-column update output).
                const std::string cols_name = lake::gen_cols_filename(txn_id + 1 + i);
                auto cell_value = [&](int row) {
                    return (row >= lower && row < upper) ? update_of(i, row) : base_value_of(row);
                };
                write_c1_only_cols_file(child_ids[i], cols_name, kNumRows, cell_value);

                auto* rowset = meta->add_rowsets();
                rowset->set_id(kSharedRowsetId);
                rowset->set_version(base_version);
                rowset->set_num_rows(kNumRows);
                rowset->set_data_size(base_segment_size);
                auto* segment_meta = rowset->add_segment_metas();
                segment_meta->set_filename(shared_segment_name);
                segment_meta->set_size(base_segment_size);
                segment_meta->set_num_rows(kNumRows);
                segment_meta->set_shared(true);
                stamp_physical_identity_uid(rowset, shared_segment_name); // same uid across siblings => dedup
                set_key_range(rowset->mutable_range(), lower, upper);
                (*meta->mutable_rowset_to_schema())[kSharedRowsetId] = kSchemaId;

                auto& dcg = (*meta->mutable_dcg_meta()->mutable_dcgs())[kSharedRowsetId];
                dcg.add_column_files(cols_name);
                dcg.add_unique_column_ids()->add_column_ids(c1_uid);
                dcg.add_versions(1);
                dcg.add_shared_files(false);
            }
            ASSERT_OK(put_tablet_metadata(meta));
        }

        ReshardingTabletInfoPB resharding_tablet;
        auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
        for (int64_t child_id : child_ids) merging_info.add_old_tablet_ids(child_id);
        merging_info.set_new_tablet_id(merged_tablet);

        TxnInfoPB txn_info;
        txn_info.set_txn_id(txn_id);
        txn_info.set_commit_time(1);
        txn_info.set_gtid(1);

        std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
        std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
        // Before the gap fix this returned NotSupported; the rebuild now succeeds.
        ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                                  txn_info, false, tablet_metadatas, tablet_ranges));
        auto merged = tablet_metadatas.at(merged_tablet);
        ASSERT_NE(merged, nullptr);

        // Canonical R0 == the rowset that still owns a shared segment.
        uint32_t canonical_rssid = 0;
        for (const auto& rowset : merged->rowsets()) {
            for (const auto& segment_meta : rowset.segment_metas()) {
                if (segment_meta.shared()) {
                    canonical_rssid = rowset.id();
                    break;
                }
            }
            if (canonical_rssid != 0) break;
        }
        ASSERT_NE(canonical_rssid, 0u);

        // A synthesized gap delvec must mask the compacted child's rows on R0.
        auto delvec_it = merged->delvec_meta().delvecs().find(canonical_rssid);
        ASSERT_NE(delvec_it, merged->delvec_meta().delvecs().end());
        EXPECT_GT(delvec_it->second.size(), 0u);

        // Exactly one rebuilt DCG entry for c1 on canonical R0.
        const auto& dcgs = merged->dcg_meta().dcgs();
        auto dcg_it = dcgs.find(canonical_rssid);
        ASSERT_TRUE(dcg_it != dcgs.end());
        const auto& rebuilt_entry = dcg_it->second;
        ASSERT_EQ(1, rebuilt_entry.column_files_size());
        ASSERT_EQ(1, rebuilt_entry.unique_column_ids_size());
        ASSERT_EQ(1, rebuilt_entry.unique_column_ids(0).column_ids_size());
        EXPECT_EQ(1002, rebuilt_entry.unique_column_ids(0).column_ids(0));
        ASSERT_EQ(1, rebuilt_entry.versions_size());
        EXPECT_EQ(new_version, rebuilt_entry.versions(0));

        // Rebuilt .cols values: surviving children's updates on their windows;
        // base values on the compacted child's gap window.
        auto values = read_c1_only_cols_file(merged_tablet, rebuilt_entry.column_files(0));
        ASSERT_EQ(kNumRows, static_cast<int>(values.size()));
        for (int row = 0; row < kNumRows; ++row) {
            const int range_index = row / kRangeRows;
            const int expected = (range_index == compacted_index) ? base_value_of(row) : update_of(range_index, row);
            EXPECT_EQ(expected, values[row]) << "row " << row << " (range " << range_index << ")";
        }
    }

    StatusOr<IndexValue> load_index_value(const TabletMetadataPtr& metadata, int64_t tablet_id,
                                          const std::string& key) {
        ASSIGN_OR_RETURN(auto values, load_index_values(metadata, tablet_id, std::vector<std::string>{key}));
        DCHECK_EQ(1, values.size());
        return values.front();
    }

    StatusOr<std::vector<IndexValue>> load_index_values(const TabletMetadataPtr& metadata, int64_t tablet_id,
                                                        const std::vector<std::string>& keys) {
        auto index = std::make_unique<lake::LakePersistentIndex>(_tablet_manager.get(), tablet_id);
        RETURN_IF_ERROR(index->init(metadata));
        lake::Tablet tablet(_tablet_manager.get(), tablet_id);
        auto metadata_copy = std::make_shared<TabletMetadataPB>(*metadata);
        lake::MetaFileBuilder builder(tablet, metadata_copy);
        RETURN_IF_ERROR(index->load_from_lake_tablet(_tablet_manager.get(), metadata, metadata->version(), &builder));
        std::vector<Slice> key_slices;
        key_slices.reserve(keys.size());
        for (const auto& key : keys) {
            key_slices.emplace_back(key);
        }
        std::vector<IndexValue> values(keys.size());
        RETURN_IF_ERROR(index->get(keys.size(), key_slices.data(), values.data()));
        ++_cold_pk_lookup_batches;
        return values;
    }

    StatusOr<TabletMetadataPtr> publish_followup_upsert_delete(int64_t tablet_id, int64_t base_version,
                                                               int32_t upsert_key, int32_t upsert_value,
                                                               int32_t delete_key, bool include_delete = true,
                                                               bool include_upsert = true) {
        std::vector<SlotDescriptor> slots;
        slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_INT});
        slots.emplace_back(2, "__op", TypeDescriptor{LogicalType::TYPE_INT});
        std::vector<SlotDescriptor*> slot_pointers = {&slots[0], &slots[1], &slots[2]};
        Chunk::SlotHashMap slot_cid_map = {{0, 0}, {1, 1}, {2, 2}};

        const int32_t keys[] = {upsert_key, delete_key};
        const int32_t values[] = {upsert_value, 0};
        const uint8_t operations[] = {TOpType::UPSERT, TOpType::DELETE};
        auto key_column = Int32Column::create();
        auto value_column = Int32Column::create();
        auto operation_column = Int8Column::create();
        const size_t row_offset = include_upsert ? 0 : 1;
        const size_t row_count = static_cast<size_t>(include_upsert) + static_cast<size_t>(include_delete);
        DCHECK_GT(row_count, 0);
        key_column->append_numbers(keys + row_offset, sizeof(keys[0]) * row_count);
        value_column->append_numbers(values + row_offset, sizeof(values[0]) * row_count);
        operation_column->append_numbers(operations + row_offset, sizeof(operations[0]) * row_count);
        Chunk chunk(Columns{std::move(key_column), std::move(value_column), std::move(operation_column)}, slot_cid_map);
        const uint32_t indexes[] = {0, 1};

        ASSIGN_OR_RETURN(auto metadata, _tablet_manager->get_tablet_metadata(tablet_id, base_version));
        auto tablet_schema = TabletSchema::create(metadata->schema());
        RuntimeProfile profile("tablet-merge-lifecycle-dml");
        const int64_t txn_id = next_id();
        ASSIGN_OR_RETURN(auto delta_writer, lake::DeltaWriterBuilder()
                                                    .set_tablet_manager(_tablet_manager.get())
                                                    .set_tablet_id(tablet_id)
                                                    .set_txn_id(txn_id)
                                                    .set_partition_id(next_id())
                                                    .set_mem_tracker(_mem_tracker.get())
                                                    .set_schema_id(metadata->schema().id())
                                                    .set_tablet_schema(std::move(tablet_schema))
                                                    .set_slot_descriptors(&slot_pointers)
                                                    .set_profile(&profile)
                                                    .build());
        RETURN_IF_ERROR(delta_writer->open());
        RETURN_IF_ERROR(delta_writer->write(chunk, indexes, row_count));
        RETURN_IF_ERROR(delta_writer->finish_with_txnlog());
        delta_writer->close();

        TxnInfoPB txn_info;
        txn_info.set_txn_id(txn_id);
        txn_info.set_txn_type(TXN_NORMAL);
        txn_info.set_commit_time(1);
        return lake::publish_version(_tablet_manager.get(), lake::PublishTabletInfo(tablet_id), base_version,
                                     base_version + 1, std::span<const TxnInfoPB>(&txn_info, 1), false);
    }

    StatusOr<TabletMetadataPtr> publish_followup_delete(int64_t tablet_id, int64_t base_version, int32_t delete_key) {
        return publish_followup_upsert_delete(tablet_id, base_version, /*upsert_key=*/0, /*upsert_value=*/0, delete_key,
                                              /*include_delete=*/true, /*include_upsert=*/false);
    }

    StatusOr<TabletMetadataPtr> publish_followup_deletes(int64_t tablet_id, int64_t base_version,
                                                         const std::vector<int32_t>& delete_keys) {
        std::vector<SlotDescriptor> slots;
        slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_INT});
        slots.emplace_back(2, "__op", TypeDescriptor{LogicalType::TYPE_INT});
        std::vector<SlotDescriptor*> slot_pointers = {&slots[0], &slots[1], &slots[2]};
        Chunk::SlotHashMap slot_cid_map = {{0, 0}, {1, 1}, {2, 2}};

        std::vector<int32_t> values(delete_keys.size(), 0);
        std::vector<uint8_t> operations(delete_keys.size(), TOpType::DELETE);
        std::vector<uint32_t> indexes(delete_keys.size());
        std::iota(indexes.begin(), indexes.end(), 0);
        auto key_column = Int32Column::create();
        auto value_column = Int32Column::create();
        auto operation_column = Int8Column::create();
        key_column->append_numbers(delete_keys.data(), sizeof(delete_keys[0]) * delete_keys.size());
        value_column->append_numbers(values.data(), sizeof(values[0]) * values.size());
        operation_column->append_numbers(operations.data(), sizeof(operations[0]) * operations.size());
        Chunk chunk(Columns{std::move(key_column), std::move(value_column), std::move(operation_column)}, slot_cid_map);

        ASSIGN_OR_RETURN(auto metadata, _tablet_manager->get_tablet_metadata(tablet_id, base_version));
        auto tablet_schema = TabletSchema::create(metadata->schema());
        RuntimeProfile profile("tablet-merge-lifecycle-delete-batch");
        const int64_t txn_id = next_id();
        ASSIGN_OR_RETURN(auto delta_writer, lake::DeltaWriterBuilder()
                                                    .set_tablet_manager(_tablet_manager.get())
                                                    .set_tablet_id(tablet_id)
                                                    .set_txn_id(txn_id)
                                                    .set_partition_id(next_id())
                                                    .set_mem_tracker(_mem_tracker.get())
                                                    .set_schema_id(metadata->schema().id())
                                                    .set_tablet_schema(std::move(tablet_schema))
                                                    .set_slot_descriptors(&slot_pointers)
                                                    .set_profile(&profile)
                                                    .build());
        RETURN_IF_ERROR(delta_writer->open());
        RETURN_IF_ERROR(delta_writer->write(chunk, indexes.data(), indexes.size()));
        RETURN_IF_ERROR(delta_writer->finish_with_txnlog());
        delta_writer->close();

        TxnInfoPB txn_info;
        txn_info.set_txn_id(txn_id);
        txn_info.set_txn_type(TXN_NORMAL);
        txn_info.set_commit_time(1);
        return lake::publish_version(_tablet_manager.get(), lake::PublishTabletInfo(tablet_id), base_version,
                                     base_version + 1, std::span<const TxnInfoPB>(&txn_info, 1), false);
    }

    StatusOr<TabletMetadataPtr> compact_tablet(int64_t tablet_id, int64_t base_version, bool force_base) {
        const int64_t txn_id = next_id();
        auto context = std::make_unique<lake::CompactionTaskContext>(txn_id, tablet_id, base_version, force_base,
                                                                     /*skip_write_txnlog=*/false, nullptr);
        ASSIGN_OR_RETURN(auto task, _tablet_manager->compact(context.get()));
        RETURN_IF_ERROR(task->execute(lake::CompactionTask::kNoCancelFn));
        TxnInfoPB txn_info;
        txn_info.set_txn_id(txn_id);
        txn_info.set_txn_type(TXN_NORMAL);
        txn_info.set_commit_time(1);
        return lake::publish_version(_tablet_manager.get(), lake::PublishTabletInfo(tablet_id), base_version,
                                     base_version + 1, std::span<const TxnInfoPB>(&txn_info, 1), false);
    }

    StatusOr<TabletMetadataPtr> create_lifecycle_source(int64_t tablet_id, int lower, int upper, int32_t key,
                                                        int32_t value, bool include_delete = true) {
        prepare_tablet_dirs(tablet_id);
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(1);
        metadata->set_next_rowset_id(1);
        set_two_column_pk_schema(metadata.get(), /*schema_id=*/4001);
        metadata->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
        metadata->set_enable_persistent_index(true);
        metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(lower));
        metadata->mutable_range()->set_lower_bound_included(true);
        metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(upper));
        metadata->mutable_range()->set_upper_bound_included(false);
        RETURN_IF_ERROR(put_tablet_metadata(metadata));
        return publish_followup_upsert_delete(tablet_id, /*base_version=*/1, key, value, upper - 1, include_delete);
    }

    void append_tombstone_sstable(TabletMetadataPB* metadata, int32_t key, const std::string& filename, bool shared) {
        const uint32_t tombstone = std::numeric_limits<uint32_t>::max();
        const auto file = write_raw_pk_sstable(
                _tablet_manager->sst_location(metadata->id(), filename),
                {{encode_int_primary_key(key), serialize_index_values({{metadata->version(), tombstone, tombstone}})}},
                config::enable_transparent_data_encryption);
        auto* sstable = metadata->mutable_sstable_meta()->add_sstables();
        sstable->set_filename(filename);
        sstable->set_filesize(file.filesize);
        sstable->set_encryption_meta(file.encryption_meta);
        sstable->set_max_rss_rowid((static_cast<uint64_t>(metadata->next_rowset_id()) << 32) | tombstone);
        sstable->set_generation_version(metadata->version());
        sstable->set_shared(shared);
        sstable->mutable_range()->CopyFrom(file.range);
        sstable->mutable_fileset_id()->CopyFrom(UniqueId::gen_uid().to_proto());
    }

    void expect_lifecycle_oracle(const TabletMetadataPtr& metadata,
                                 const std::vector<std::pair<int32_t, int32_t>>& expected_rows,
                                 const std::vector<int32_t>& deleted_keys) {
        ASSIGN_OR_ABORT(auto rows, read_two_column_rows(metadata));
        EXPECT_EQ(expected_rows, rows);
        std::set<int32_t> distinct_keys;
        std::vector<std::string> keys;
        for (const auto& [key, value] : expected_rows) {
            (void)value;
            distinct_keys.insert(key);
            keys.emplace_back(encode_int_primary_key(key));
        }
        for (int32_t key : deleted_keys) keys.emplace_back(encode_int_primary_key(key));
        EXPECT_EQ(expected_rows.size(), distinct_keys.size());

        ASSIGN_OR_ABORT(auto values, load_index_values(metadata, metadata->id(), keys));
        ASSERT_EQ(keys.size(), values.size());
        for (size_t i = 0; i < expected_rows.size(); ++i) {
            EXPECT_NE(IndexValue(NullIndexValue), values[i]) << expected_rows[i].first;
        }
        for (size_t i = expected_rows.size(); i < values.size(); ++i) {
            EXPECT_EQ(IndexValue(NullIndexValue), values[i]) << deleted_keys[i - expected_rows.size()];
        }
    }

    void expect_affine_sst_fallback_orphans(const MetadataOnlyMergeResult& result) {
        const auto& target = result.published.at(result.target_tablet_id);
        EXPECT_EQ(0, target.sstable_meta().sstables_size());
        for (const auto& filename : result.source_sst_filenames) {
            EXPECT_EQ(1, std::count_if(target.orphan_files().begin(), target.orphan_files().end(),
                                       [&](const auto& orphan) { return orphan.name() == filename; }));
        }
    }

    void expect_metadata_fallback_lifecycle(const MetadataOnlyMergeResult& result,
                                            const std::vector<std::pair<int32_t, int32_t>>& initial_rows,
                                            const std::vector<std::pair<int32_t, int32_t>>& rows_after_dml,
                                            const std::vector<int32_t>& deleted_keys) {
        auto target = std::make_shared<TabletMetadataPB>(result.published.at(result.target_tablet_id));
        ASSERT_EQ(0, target->sstable_meta().sstables_size()) << "fallback must enter native index rebuild";
        _update_manager->unload_and_remove_primary_index(result.target_tablet_id);
        expect_lifecycle_oracle(target, initial_rows, {});

        _update_manager->unload_and_remove_primary_index(result.target_tablet_id);
        ASSIGN_OR_ABORT(auto after_dml, publish_followup_upsert_delete(result.target_tablet_id, result.target_version,
                                                                       /*upsert_key=*/10, /*upsert_value=*/1010,
                                                                       /*delete_key=*/60));
        expect_lifecycle_oracle(after_dml, rows_after_dml, deleted_keys);

        _update_manager->unload_and_remove_primary_index(result.target_tablet_id);
        ASSIGN_OR_ABORT(auto reopened_after_dml,
                        _tablet_manager->get_tablet_metadata(result.target_tablet_id, after_dml->version()));
        expect_lifecycle_oracle(reopened_after_dml, rows_after_dml, deleted_keys);

        ASSIGN_OR_ABORT(auto compacted,
                        compact_tablet(result.target_tablet_id, reopened_after_dml->version(), /*force_base=*/true));
        EXPECT_EQ(reopened_after_dml->version() + 1, compacted->version());
        expect_lifecycle_oracle(compacted, rows_after_dml, deleted_keys);

        _update_manager->unload_and_remove_primary_index(result.target_tablet_id);
        ASSIGN_OR_ABORT(auto reopened_compacted,
                        _tablet_manager->get_tablet_metadata(result.target_tablet_id, compacted->version()));
        expect_lifecycle_oracle(reopened_compacted, rows_after_dml, deleted_keys);
    }

    void assert_published_sstables_reopen(const TabletMetadataPtr& metadata) {
        auto* block_cache = _update_manager->block_cache();
        ASSERT_NE(nullptr, block_cache);
        for (const auto& sstable : metadata->sstable_meta().sstables()) {
            auto opened = lake::PersistentIndexSstable::new_sstable(
                    sstable, _tablet_manager->sst_location(metadata->id(), sstable.filename()), block_cache->cache(),
                    /*need_filter=*/true, nullptr, metadata, _tablet_manager.get());
            ASSERT_OK(opened.status());
        }
    }

    StatusOr<std::vector<std::pair<int32_t, int32_t>>> read_two_column_rows_in_storage_order(
            const TabletMetadataPtr& metadata, bool sorted_by_keys_per_tablet = false) {
        auto tablet_schema = TabletSchema::create(metadata->schema());
        auto schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));
        auto reader = std::make_shared<lake::TabletReader>(_tablet_manager.get(), metadata, *schema);
        RETURN_IF_ERROR(reader->prepare());
        TabletReaderParams params;
        params.sorted_by_keys_per_tablet = sorted_by_keys_per_tablet;
        RETURN_IF_ERROR(reader->open(params));

        std::vector<std::pair<int32_t, int32_t>> rows;
        while (true) {
            auto chunk = ChunkFactory::new_chunk(*schema, 128);
            auto status = reader->get_next(chunk.get());
            if (status.is_end_of_file()) break;
            RETURN_IF_ERROR(status);
            for (size_t i = 0; i < chunk->num_rows(); ++i) {
                rows.emplace_back(chunk->get(i)[0].get_int32(), chunk->get(i)[1].get_int32());
            }
        }
        return rows;
    }

    StatusOr<std::vector<std::pair<int32_t, int32_t>>> read_two_column_rows(const TabletMetadataPtr& metadata) {
        ASSIGN_OR_RETURN(auto rows, read_two_column_rows_in_storage_order(metadata));
        std::sort(rows.begin(), rows.end());
        return rows;
    }

    // Total row count a real reader returns for |metadata|, column values ignored. Unlike the
    // rowset stats a split computes for itself (which the boundary algorithm optimizes directly and
    // would trivially agree with, see kEvennessTolerance's comment in tablet_splitter_test.cpp for
    // the same point), this is independent ground truth: it opens the child's real segment files
    // and counts what a query would actually see.
    StatusOr<int64_t> count_rows(const TabletMetadataPtr& metadata) {
        auto tablet_schema = TabletSchema::create(metadata->schema());
        auto schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));
        auto reader = std::make_shared<lake::TabletReader>(_tablet_manager.get(), metadata, *schema);
        RETURN_IF_ERROR(reader->prepare());
        TabletReaderParams params;
        RETURN_IF_ERROR(reader->open(params));

        int64_t rows = 0;
        while (true) {
            auto chunk = ChunkFactory::new_chunk(*schema, 4096);
            auto status = reader->get_next(chunk.get());
            if (status.is_end_of_file()) break;
            RETURN_IF_ERROR(status);
            rows += chunk->num_rows();
        }
        return rows;
    }

    static std::vector<std::pair<int32_t, int32_t>> repeated_expected_rows(const std::map<int32_t, int32_t>& expected) {
        return {expected.begin(), expected.end()};
    }

    StatusOr<uint32_t> repeated_merge_cursor_oracle(const std::vector<TabletMetadataPtr>& sources) {
        struct Family {
            size_t selected_context;
            const RowsetMetadataPB* selected;
            uint32_t final_max_segment_idx = 0;
        };
        std::map<std::pair<int64_t, int64_t>, Family> families;
        for (size_t context_index = 0; context_index < sources.size(); ++context_index) {
            for (const auto& rowset : sources[context_index]->rowsets()) {
                if (!rowset.has_uid()) return Status::Corruption("repeated lifecycle rowset is missing uid");
                auto it = families.try_emplace(std::pair{rowset.uid().hi(), rowset.uid().lo()},
                                               Family{.selected_context = context_index, .selected = &rowset})
                                  .first;
                auto& family = it->second;
                for (int segment_index = 0; segment_index < rowset.segment_metas_size(); ++segment_index) {
                    const auto& segment = rowset.segment_metas(segment_index);
                    family.final_max_segment_idx = std::max(
                            family.final_max_segment_idx,
                            segment.has_segment_idx() ? segment.segment_idx() : static_cast<uint32_t>(segment_index));
                }
            }
        }

        using Atom = std::pair<uint64_t, uint64_t>;
        std::vector<std::vector<Atom>> atoms(sources.size());
        auto add_atom = [&](size_t context_index, uint64_t begin, uint64_t end) -> Status {
            if (begin >= end || end > static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1) {
                return Status::InvalidArgument("repeated lifecycle atom is outside the source RSSID domain");
            }
            atoms[context_index].emplace_back(begin, end);
            return Status::OK();
        };
        for (const auto& [uid, family] : families) {
            (void)uid;
            const auto& rowset = *family.selected;
            const uint64_t extent = rowset.segment_metas_size() == 0 ? 1 : family.final_max_segment_idx + uint64_t{1};
            RETURN_IF_ERROR(add_atom(family.selected_context, rowset.id(), uint64_t{rowset.id()} + extent));
            if (rowset.has_max_compact_input_rowset_id()) {
                const uint64_t recovery = rowset.max_compact_input_rowset_id();
                RETURN_IF_ERROR(add_atom(family.selected_context, recovery, recovery + 1));
            }
            for (const auto& del : rowset.del_files()) {
                const uint64_t offset = del.has_op_offset() ? del.op_offset() : family.final_max_segment_idx;
                RETURN_IF_ERROR(add_atom(family.selected_context, del.origin_rowset_id(),
                                         uint64_t{del.origin_rowset_id()} + offset + 1));
            }
        }

        uint64_t cursor = 1;
        for (auto& context_atoms : atoms) {
            std::sort(context_atoms.begin(), context_atoms.end());
            uint64_t union_begin = 0;
            uint64_t union_end = 0;
            for (const auto& [begin, end] : context_atoms) {
                if (union_begin == union_end) {
                    union_begin = begin;
                    union_end = end;
                } else if (begin <= union_end) {
                    union_end = std::max(union_end, end);
                } else {
                    cursor += union_end - union_begin;
                    union_begin = begin;
                    union_end = end;
                }
            }
            cursor += union_end - union_begin;
        }
        if (cursor > std::numeric_limits<int32_t>::max()) {
            return Status::InvalidArgument("repeated lifecycle oracle exhausts the target RSSID domain");
        }
        return static_cast<uint32_t>(cursor);
    }

    StatusOr<int> repeated_merge_delfile_count_oracle(const std::vector<TabletMetadataPtr>& sources) {
        std::set<std::pair<int64_t, int64_t>> selected_families;
        int count = 0;
        for (const auto& source : sources) {
            for (const auto& rowset : source->rowsets()) {
                if (!rowset.has_uid()) return Status::Corruption("repeated lifecycle rowset is missing uid");
                if (selected_families.emplace(rowset.uid().hi(), rowset.uid().lo()).second) {
                    count += rowset.del_files_size();
                }
            }
        }
        return count;
    }

    void add_repeated_sparse_sidecars(TabletMetadataPB* metadata, int cycle, int child_index, int32_t value,
                                      bool enable_tde) {
        ASSERT_GT(metadata->rowsets_size(), 0);
        auto* rowset = metadata->mutable_rowsets(metadata->rowsets_size() - 1);
        ASSERT_EQ(1, rowset->segment_metas_size());
        auto* segment = rowset->mutable_segment_metas(0);
        const uint32_t sparse_index = 600 + child_index * 300;
        segment->set_segment_idx(sparse_index);
        EXPECT_EQ(enable_tde, !segment->encryption_meta().empty());
        const uint32_t sparse_rssid = rowset->id() + sparse_index;
        metadata->set_next_rowset_id(std::max(metadata->next_rowset_id(), sparse_rssid + 1));

        const std::string stem = fmt::format("repeated_{}_{}_{}", cycle, child_index, metadata->id());
        auto* del = rowset->add_del_files();
        del->set_name(stem + ".del");
        del->set_origin_rowset_id(rowset->id());
        del->set_op_offset(sparse_index);
        del->set_version(metadata->version());
        del->set_num_rows(0);
        if (enable_tde) {
            del->set_encryption_meta(write_encrypted_binary_del_file(metadata->id(), del->name(), {}));
        } else {
            write_binary_del_file(metadata->id(), del->name(), {});
        }

        ASSERT_FALSE(metadata->delvec_meta().version_to_file().contains(metadata->version()));
        DelVector empty_delvec;
        empty_delvec.init(metadata->version(), nullptr, 0);
        add_delvec(metadata, metadata->id(), metadata->version(), sparse_rssid, stem + ".delvec", empty_delvec.save());

        std::string dcg_encryption_meta;
        const std::string dcg_file = stem + ".cols";
        const uint64_t dcg_size = write_c1_only_cols_file(
                metadata->id(), dcg_file, /*num_rows=*/1, [=](int) { return value; }, enable_tde, &dcg_encryption_meta);
        auto& dcg = (*metadata->mutable_dcg_meta()->mutable_dcgs())[sparse_rssid];
        dcg.add_column_files(dcg_file);
        dcg.add_unique_column_ids()->add_column_ids(1002);
        dcg.add_versions(metadata->version());
        dcg.add_encryption_metas(dcg_encryption_meta);
        dcg.add_shared_files(false);
        dcg.add_column_file_sizes(dcg_size);

        const std::string idg_file = stem + ".idx";
        const std::string idg_payload = "repeated-idg:" + idg_file;
        const auto idg_physical = write_sidecar_payload(_tablet_manager->segment_location(metadata->id(), idg_file),
                                                        idg_payload, enable_tde);
        add_idg_with_key(metadata, sparse_rssid, idg_file, /*col_uid=*/1002, BITMAP, metadata->version(),
                         /*shared_file=*/false);
        auto* idg = metadata->mutable_idg_meta()->mutable_idgs()->at(sparse_rssid).mutable_entries(0);
        idg->set_file_size(idg_physical.filesize);
        idg->set_encryption_meta(idg_physical.encryption_meta);
    }

    StatusOr<std::vector<TabletMetadataPtr>> repeated_split_three_way(const TabletMetadataPtr& parent) {
        std::vector<int64_t> child_ids = {next_id(), next_id(), next_id()};
        ReshardingTabletInfoPB resharding;
        auto* splitting = resharding.mutable_splitting_tablet_info();
        splitting->set_old_tablet_id(parent->id());
        for (int child_index = 0; child_index < 3; ++child_index) {
            const int64_t child_id = child_ids[child_index];
            prepare_tablet_dirs(child_id);
            splitting->add_new_tablet_ids(child_id);
            auto* range = splitting->add_new_tablet_ranges();
            range->mutable_lower_bound()->CopyFrom(generate_sort_key(child_index * 100));
            range->set_lower_bound_included(true);
            range->mutable_upper_bound()->CopyFrom(generate_sort_key((child_index + 1) * 100));
            range->set_upper_bound_included(false);
        }
        TxnInfoPB txn_info;
        txn_info.set_txn_id(next_id());
        txn_info.set_commit_time(1);
        txn_info.set_gtid(1);
        std::unordered_map<int64_t, TabletMetadataPtr> published;
        std::unordered_map<int64_t, TabletRangePB> ranges;
        RETURN_IF_ERROR(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, parent->version(),
                                                        parent->version() + 1, txn_info, false, published, ranges));
        std::vector<TabletMetadataPtr> children;
        for (int64_t child_id : child_ids) {
            auto it = published.find(child_id);
            if (it == published.end()) return Status::InternalError("repeated lifecycle split child is missing");
            children.emplace_back(it->second);
        }
        return children;
    }

    void expect_repeated_merge_pb(const TabletMetadataPtr& metadata, uint32_t expected_cursor,
                                  int expected_sidecar_count, int expected_del_file_count,
                                  const std::set<std::string>& expected_sst_orphans, bool enable_tde) {
        EXPECT_EQ(expected_cursor, metadata->next_rowset_id()) << "independent atom-union cursor";
        std::set<uint32_t> live_rssids;
        int del_file_count = 0;
        for (const auto& rowset : metadata->rowsets()) {
            EXPECT_LT(rowset.id(), metadata->next_rowset_id());
            for (int segment_index = 0; segment_index < rowset.segment_metas_size(); ++segment_index) {
                const auto& segment = rowset.segment_metas(segment_index);
                const uint32_t effective_index = segment.has_segment_idx() ? segment.segment_idx() : segment_index;
                const uint32_t rssid = rowset.id() + effective_index;
                EXPECT_LT(rssid, metadata->next_rowset_id());
                EXPECT_TRUE(live_rssids.insert(rssid).second) << "duplicate target RSSID " << rssid;
                EXPECT_EQ(enable_tde, !segment.encryption_meta().empty());
            }
            for (const auto& del : rowset.del_files()) {
                ++del_file_count;
                EXPECT_LT(uint64_t{del.origin_rowset_id()} + del.op_offset(), metadata->next_rowset_id());
                EXPECT_EQ(enable_tde, !del.encryption_meta().empty());
            }
        }
        EXPECT_EQ(expected_del_file_count, del_file_count);
        EXPECT_EQ(expected_sidecar_count, metadata->delvec_meta().delvecs_size());
        EXPECT_EQ(expected_sidecar_count, metadata->dcg_meta().dcgs_size());
        EXPECT_EQ(expected_sidecar_count, metadata->idg_meta().idgs_size());

        for (const auto& [version, file] : metadata->delvec_meta().version_to_file()) {
            (void)version;
            EXPECT_TRUE(file.encryption_meta().empty());
        }
        LakeIOOptions io_options;
        for (const auto& [rssid, page] : metadata->delvec_meta().delvecs()) {
            (void)page;
            EXPECT_TRUE(live_rssids.contains(rssid));
            DelVector loaded;
            ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), *metadata, rssid, false, io_options, &loaded));
            EXPECT_EQ(0, loaded.cardinality());
        }
        for (const auto& [rssid, dcg] : metadata->dcg_meta().dcgs()) {
            EXPECT_TRUE(live_rssids.contains(rssid));
            ASSERT_EQ(1, dcg.column_files_size());
            ASSERT_EQ(1, dcg.encryption_metas_size());
            EXPECT_EQ(enable_tde, !dcg.encryption_metas(0).empty());
        }
        for (const auto& [rssid, idg] : metadata->idg_meta().idgs()) {
            EXPECT_TRUE(live_rssids.contains(rssid));
            ASSERT_EQ(1, idg.entries_size());
            const auto& entry = idg.entries(0);
            EXPECT_EQ(enable_tde, !entry.encryption_meta().empty());
            ASSIGN_OR_ABORT(auto payload,
                            read_sidecar_payload(_tablet_manager->segment_location(metadata->id(), entry.index_file()),
                                                 entry.encryption_meta()));
            EXPECT_EQ("repeated-idg:" + entry.index_file(), payload);
        }

        EXPECT_EQ(0, metadata->sstable_meta().sstables_size());
        std::set<std::string> actual_orphans;
        for (const auto& orphan : metadata->orphan_files()) {
            actual_orphans.insert(orphan.name());
            EXPECT_TRUE(orphan.shared());
            EXPECT_EQ(enable_tde, !orphan.encryption_meta().empty());
        }
        EXPECT_EQ(expected_sst_orphans, actual_orphans);
    }

    struct Issue11935MergeFixture {
        ReshardingTabletInfoPB resharding;
        TxnInfoPB txn_info;
        int64_t base_version;
        int64_t new_version;
    };

    StatusOr<Issue11935MergeFixture> make_issue11935_merge_fixture(
            int64_t child_a, int64_t child_b, int64_t target_tablet_id, const std::string& old_segment,
            const std::string& tail_segment, const std::string& sibling_segment, const std::string& tombstone_filename,
            const std::string& stale_live_filename) {
        Issue11935MergeFixture fixture;
        fixture.base_version = 600;
        fixture.new_version = 601;
        prepare_tablet_dirs(child_a);
        prepare_tablet_dirs(child_b);
        prepare_tablet_dirs(target_tablet_id);
        const uint64_t old_size = write_two_column_segment(child_a, old_segment, 1, [](int) { return 100; });
        const uint64_t tail_size = write_two_column_segment(child_a, tail_segment, 1, [](int) { return 200; });
        const uint64_t sibling_size = write_two_column_segment(
                child_b, sibling_segment, 1, [](int) { return 600; }, /*key_start=*/60);
        const uint32_t tombstone = std::numeric_limits<uint32_t>::max();
        const uint64_t tombstone_size =
                write_versioned_pk_sstable(_tablet_manager->sst_location(child_a, tombstone_filename),
                                           {{raw_int_primary_key(0), 540, tombstone, tombstone},
                                            {raw_int_primary_key(1), 540, tombstone, tombstone}});
        const uint64_t stale_size =
                write_versioned_pk_sstable(_tablet_manager->sst_location(child_b, stale_live_filename),
                                           {{raw_int_primary_key(0), 513, 1, 0}, {raw_int_primary_key(1), 513, 1, 0}});
        auto make_metadata = [&](int64_t tablet_id, int lower, int upper) {
            auto metadata = std::make_shared<TabletMetadataPB>();
            metadata->set_id(tablet_id);
            metadata->set_version(fixture.base_version);
            set_two_column_pk_schema(metadata.get(), 4001);
            metadata->set_enable_persistent_index(true);
            metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
            metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(lower));
            metadata->mutable_range()->set_lower_bound_included(true);
            metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(upper));
            metadata->mutable_range()->set_upper_bound_included(false);
            return metadata;
        };
        auto meta_a = make_metadata(child_a, 0, 50);
        meta_a->set_next_rowset_id(3);
        auto add_a_rowset = [&](uint32_t id, int64_t version, const std::string& filename, uint64_t size) {
            auto* rowset = meta_a->add_rowsets();
            rowset->set_id(id);
            rowset->set_version(version);
            rowset->set_num_rows(1);
            rowset->set_data_size(size);
            auto* segment = rowset->add_segment_metas();
            segment->set_filename(filename);
            segment->set_size(size);
            segment->set_num_rows(1);
        };
        add_a_rowset(1, 500, old_segment, old_size);
        add_a_rowset(2, fixture.base_version, tail_segment, tail_size);
        auto* tombstone_sst = meta_a->mutable_sstable_meta()->add_sstables();
        tombstone_sst->set_filename(tombstone_filename);
        tombstone_sst->set_filesize(tombstone_size);
        tombstone_sst->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | UINT32_MAX);
        auto meta_b = make_metadata(child_b, 50, 100);
        meta_b->set_next_rowset_id(2);
        auto* rowset = meta_b->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(500);
        rowset->set_num_rows(1);
        rowset->set_data_size(sibling_size);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(sibling_segment);
        segment->set_size(sibling_size);
        segment->set_num_rows(1);
        auto* stale_sst = meta_b->mutable_sstable_meta()->add_sstables();
        stale_sst->set_filename(stale_live_filename);
        stale_sst->set_filesize(stale_size);
        stale_sst->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | UINT32_MAX);
        RETURN_IF_ERROR(put_tablet_metadata(meta_a));
        RETURN_IF_ERROR(put_tablet_metadata(meta_b));
        auto& merging = *fixture.resharding.mutable_merging_tablet_info();
        merging.add_old_tablet_ids(child_a);
        merging.add_old_tablet_ids(child_b);
        merging.set_new_tablet_id(target_tablet_id);
        fixture.txn_info.set_txn_id(1);
        fixture.txn_info.set_commit_time(1);
        fixture.txn_info.set_gtid(1);
        return fixture;
    }

    struct RealSplitSstOwnerFixture {
        int64_t low_child = 0;
        int64_t middle_child = 0;
        int64_t high_child = 0;
        TabletMetadataPtr middle_metadata;
    };

    StatusOr<RealSplitSstOwnerFixture> publish_real_split_sst_owner_fixture() {
        constexpr int64_t kBaseVersion = 2;
        constexpr int64_t kSplitVersion = 3;
        constexpr int kRowsPerSegment = 50;
        const int64_t parent_tablet = next_id();
        const int64_t child_ids[] = {next_id(), next_id(), next_id()};

        prepare_tablet_dirs(parent_tablet);
        for (int64_t child_id : child_ids) prepare_tablet_dirs(child_id);

        TabletMetadataPB metadata;
        metadata.set_id(parent_tablet);
        metadata.set_version(kBaseVersion);
        metadata.set_next_rowset_id(20);
        set_two_column_pk_schema(&metadata, /*schema_id=*/4001);
        metadata.set_enable_persistent_index(true);
        metadata.set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);

        struct SegmentSpec {
            uint32_t rowset_id;
            const char* filename;
            int key_start;
        };
        const SegmentSpec specs[] = {
                {2, "split_sst_low.dat", 0}, {10, "split_sst_middle.dat", 50}, {15, "split_sst_high.dat", 100}};
        for (const auto& spec : specs) {
            const uint64_t filesize = write_two_column_segment(
                    parent_tablet, spec.filename, kRowsPerSegment, [](int key) { return key * 10; }, spec.key_start);
            auto* rowset = metadata.add_rowsets();
            rowset->set_id(spec.rowset_id);
            rowset->set_version(1);
            rowset->set_num_rows(kRowsPerSegment);
            rowset->set_data_size(filesize);
            rowset->set_overlapped(false);
            auto* segment = rowset->add_segment_metas();
            segment->set_filename(spec.filename);
            segment->set_size(filesize);
            segment->set_num_rows(kRowsPerSegment);
            segment->mutable_sort_key_min()->CopyFrom(generate_sort_key(spec.key_start));
            segment->mutable_sort_key_max()->CopyFrom(generate_sort_key(spec.key_start + kRowsPerSegment - 1));
        }

        DelVector low_delvec;
        const uint32_t deleted_rowid = 0;
        low_delvec.init(kBaseVersion, &deleted_rowid, 1);
        add_delvec(&metadata, parent_tablet, kBaseVersion, /*segment_id=*/2, "split_sst_low.delvec", low_delvec.save());
        write_c1_only_cols_file(parent_tablet, "split_sst_low.cols", kRowsPerSegment, [](int row) { return row * 10; });
        add_dcg_with_columns(&metadata, /*segment_id=*/2, "split_sst_low.cols", {1002}, kBaseVersion);
        add_idg_with_key(&metadata, /*segment_id=*/2, "split_sst_low.idx", /*col_uid=*/1002, BITMAP, kBaseVersion);

        std::vector<std::tuple<std::string, uint32_t, uint32_t>> sst_entries;
        sst_entries.reserve(kRowsPerSegment);
        for (uint32_t rowid = 0; rowid < kRowsPerSegment; ++rowid) {
            sst_entries.emplace_back(raw_int_primary_key(static_cast<int32_t>(rowid)), /*rssid=*/2, rowid);
        }
        const std::string sst_filename = "split_sst_low.sst";
        const uint64_t sst_filesize =
                write_legacy_pk_sstable(_tablet_manager->sst_location(parent_tablet, sst_filename), sst_entries);
        auto* sst = metadata.mutable_sstable_meta()->add_sstables();
        sst->set_filename(sst_filename);
        sst->set_filesize(sst_filesize);
        sst->set_shared_rssid(2);
        sst->set_shared_version(1);
        sst->set_max_rss_rowid((static_cast<uint64_t>(2) << 32) | (kRowsPerSegment - 1));
        sst->mutable_delvec()->CopyFrom(metadata.delvec_meta().delvecs().at(2));

        RETURN_IF_ERROR(put_tablet_metadata(metadata));

        ReshardingTabletInfoPB resharding;
        auto& splitting = *resharding.mutable_splitting_tablet_info();
        splitting.set_old_tablet_id(parent_tablet);
        for (int64_t child_id : child_ids) splitting.add_new_tablet_ids(child_id);
        TxnInfoPB txn_info;
        txn_info.set_txn_id(1);
        txn_info.set_commit_time(1);
        txn_info.set_gtid(1);
        std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
        std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
        RETURN_IF_ERROR(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, kBaseVersion, kSplitVersion,
                                                        txn_info, false, tablet_metadatas, tablet_ranges));

        RealSplitSstOwnerFixture result;
        for (int64_t child_id : child_ids) {
            auto metadata_it = tablet_metadatas.find(child_id);
            if (metadata_it == tablet_metadatas.end()) {
                return Status::InternalError(fmt::format("split child {} metadata is missing", child_id));
            }
            const auto& child = metadata_it->second;
            for (const auto& rowset : child->rowsets()) {
                for (const auto& segment : rowset.segment_metas()) {
                    if (segment.filename() == specs[0].filename) {
                        result.low_child = child_id;
                    } else if (segment.filename() == specs[1].filename) {
                        result.middle_child = child_id;
                        result.middle_metadata = child;
                    } else if (segment.filename() == specs[2].filename) {
                        result.high_child = child_id;
                    }
                }
            }
        }
        if (result.low_child == 0 || result.middle_child == 0 || result.high_child == 0) {
            return Status::InternalError("split did not produce one owner child per real segment");
        }
        return result;
    }

    StatusOr<TabletMetadataPtr> publish_real_split_sst_owner_merge(const std::vector<int64_t>& source_tablets,
                                                                   int64_t* merged_tablet) {
        constexpr int64_t kSplitVersion = 3;
        constexpr int64_t kMergeVersion = 4;
        *merged_tablet = next_id();
        prepare_tablet_dirs(*merged_tablet);

        ReshardingTabletInfoPB resharding;
        auto& merging = *resharding.mutable_merging_tablet_info();
        for (int64_t source_tablet : source_tablets) merging.add_old_tablet_ids(source_tablet);
        merging.set_new_tablet_id(*merged_tablet);
        TxnInfoPB txn_info;
        txn_info.set_txn_id(2);
        txn_info.set_commit_time(2);
        txn_info.set_gtid(2);
        std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
        std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
        RETURN_IF_ERROR(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, kSplitVersion, kMergeVersion,
                                                        txn_info, false, tablet_metadatas, tablet_ranges));
        auto merged_it = tablet_metadatas.find(*merged_tablet);
        if (merged_it == tablet_metadatas.end()) {
            return Status::InternalError(fmt::format("merged tablet {} metadata is missing", *merged_tablet));
        }
        return merged_it->second;
    }

    struct PrechangeProtectedRssidFixture {
        TabletMetadataPtr owner;
        TabletMetadataPtr stale_child;
        TabletMetadataPtr empty_child;
        uint32_t protected_rssid = 1;
    };

    StatusOr<PrechangeProtectedRssidFixture> make_prechange_protected_rssid_fixture() {
        constexpr int64_t kBaseVersion = 1;
        constexpr uint32_t kProtectedRssid = 1;
        const int64_t owner_tablet = next_id();
        const int64_t stale_tablet = next_id();
        const int64_t empty_tablet = next_id();
        for (int64_t tablet_id : {owner_tablet, stale_tablet, empty_tablet}) prepare_tablet_dirs(tablet_id);

        auto make_metadata = [&](int64_t tablet_id, int32_t lower, int32_t upper) {
            auto metadata = std::make_shared<TabletMetadataPB>();
            metadata->set_id(tablet_id);
            metadata->set_version(kBaseVersion);
            metadata->set_next_rowset_id(kProtectedRssid + 1);
            set_two_column_pk_schema(metadata.get(), /*schema_id=*/4001);
            metadata->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
            metadata->set_enable_persistent_index(true);
            metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
            metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(lower));
            metadata->mutable_range()->set_lower_bound_included(true);
            metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(upper));
            metadata->mutable_range()->set_upper_bound_included(false);
            return metadata;
        };

        auto owner = make_metadata(owner_tablet, /*lower=*/0, /*upper=*/50);
        const std::string segment_name = "prechange_protected_owner.dat";
        const uint64_t segment_size = write_two_column_segment(owner_tablet, segment_name, /*num_rows=*/2,
                                                               [](int32_t key) { return key * 10; });
        auto* owner_rowset = owner->add_rowsets();
        owner_rowset->set_id(kProtectedRssid);
        owner_rowset->set_version(kBaseVersion);
        owner_rowset->set_num_rows(2);
        owner_rowset->set_data_size(segment_size);
        auto* owner_segment = owner_rowset->add_segment_metas();
        owner_segment->set_filename(segment_name);
        owner_segment->set_size(segment_size);
        owner_segment->set_num_rows(2);
        owner_segment->set_segment_idx(0);
        stamp_physical_identity_uid(owner_rowset, segment_name);

        DelVector owner_delvec;
        const uint32_t owner_deleted_rowid = 1;
        owner_delvec.init(kBaseVersion, &owner_deleted_rowid, 1);
        add_delvec(owner.get(), owner_tablet, kBaseVersion, kProtectedRssid, "prechange_owner.delvec",
                   owner_delvec.save());

        auto stale_child = make_metadata(stale_tablet, /*lower=*/50, /*upper=*/100);
        // Recreate the exact metadata shape emitted by the old protected-rssid SPLIT path:
        // the out-of-range data rowset survived with no segment because an inherited SST
        // protected its rssid, and the child retained that rssid's delvec page.
        auto* protected_rowset = stale_child->add_rowsets();
        protected_rowset->CopyFrom(*owner_rowset);
        protected_rowset->clear_segment_metas();
        protected_rowset->set_num_rows(0);
        protected_rowset->set_data_size(0);
        DelVector stale_delvec;
        const uint32_t stale_deleted_rowid = 0;
        stale_delvec.init(kBaseVersion, &stale_deleted_rowid, 1);
        add_delvec(stale_child.get(), stale_tablet, kBaseVersion, kProtectedRssid, "prechange_stale.delvec",
                   stale_delvec.save());

        PrechangeProtectedRssidFixture fixture;
        fixture.owner = std::move(owner);
        fixture.stale_child = std::move(stale_child);
        fixture.empty_child = make_metadata(empty_tablet, /*lower=*/100, /*upper=*/150);
        fixture.protected_rssid = kProtectedRssid;
        return fixture;
    }

    StatusOr<std::string> cold_fixed_point_pk_signature(const TabletMetadataPtr& metadata,
                                                        const std::vector<std::pair<int32_t, int32_t>>& expected_rows) {
        // Every fixed-point fixture covers keys [0, 100). Query the whole domain,
        // including key 1 (delvec-deleted) and key 60 after the DML delete/compaction.
        std::map<int32_t, int32_t> expected(expected_rows.begin(), expected_rows.end());
        if (expected.empty() || expected.size() != expected_rows.size()) {
            return Status::Corruption("fixed-point PK scan must contain distinct live keys");
        }
        std::vector<std::string> keys;
        for (int32_t key = 0; key < 100; ++key) keys.push_back(encode_int_primary_key(key));
        ASSIGN_OR_RETURN(auto values, load_index_values(metadata, metadata->id(), keys));
        std::map<uint32_t, std::vector<uint32_t>> rowids_by_rssid;
        std::map<uint32_t, std::vector<std::pair<int32_t, int32_t>>> expected_by_rssid;
        std::map<uint32_t, uint64_t> segment_rows;
        for (const auto& rowset : metadata->rowsets()) {
            for (int i = 0; i < rowset.segment_metas_size(); ++i) {
                const auto& segment = rowset.segment_metas(i);
                segment_rows.emplace(rowset.id() + (segment.has_segment_idx() ? segment.segment_idx() : i),
                                     segment.num_rows());
            }
        }
        std::string signature;
        size_t deleted_keys = 0;
        for (int32_t key = 0; key < 100; ++key) {
            auto found = expected.find(key);
            if (found == expected.end()) {
                if (values[key] != IndexValue(NullIndexValue)) {
                    return Status::Corruption(fmt::format("cold PK lookup resurrected deleted key {}", key));
                }
                ++deleted_keys;
                signature += fmt::format("pk-deleted={};", key);
                continue;
            }
            const auto& value = values[key];
            if (value == IndexValue(NullIndexValue) || !segment_rows.contains(value.get_rssid()) ||
                value.get_rowid() >= segment_rows.at(value.get_rssid())) {
                return Status::Corruption(fmt::format("cold PK lookup has no valid physical row for key {}", key));
            }
            rowids_by_rssid[value.get_rssid()].push_back(value.get_rowid());
            expected_by_rssid[value.get_rssid()].push_back(*found);
            signature += fmt::format("pk-row={}:{};", key, found->second);
        }
        if (deleted_keys == 0) return Status::Corruption("fixed-point PK lookup must exercise deleted keys");

        // Resolve each returned (RSSID,rowid) to physical column values through the
        // fixture manager, including active DCG overrides. Compare logical rows,
        // never the projected RSSID numbers across reshard cycles.
        lake::RssidFileInfoContainer files;
        files.add_rssid_to_file(*metadata);
        auto schema = TabletSchema::create(metadata->schema());
        lake::Tablet tablet(_tablet_manager.get(), metadata->id());
        TxnLogPB_OpWrite read_context;
        read_context.mutable_txn_meta(); // enable DCG-aware physical column reads
        lake::RowsetUpdateStateParams params{read_context, schema, metadata, &tablet, files};
        MutableColumns columns;
        columns.push_back(Int32Column::create());
        columns.push_back(Int32Column::create());
        RETURN_IF_ERROR(_update_manager->get_column_values(params, {0, 1}, false, rowids_by_rssid, &columns));
        if (columns[0]->size() != expected.size() || columns[1]->size() != expected.size()) {
            return Status::Corruption("cold PK physical row resolution has the wrong cardinality");
        }
        size_t offset = 0;
        for (const auto& [rssid, rows] : expected_by_rssid) {
            for (const auto& [key, value] : rows) {
                if (columns[0]->get(offset).get_int32() != key || columns[1]->get(offset).get_int32() != value) {
                    return Status::Corruption(
                            fmt::format("cold PK lookup resolves to the wrong logical row for key {}", key));
                }
                ++offset;
            }
        }
        _cold_pk_deleted_keys_checked += deleted_keys;
        LOG(INFO) << "fixed-point cold PK lookup: live=" << expected.size() << " deleted=" << deleted_keys;
        return signature;
    }

    // Canonicalize logical state, not the protobuf's allocation/serialization choices.
    // Segment filenames and bundle offsets remain physical identity; generated sidecar
    // names, projected RSSIDs and the SST representation deliberately do not.
    StatusOr<std::string> semantic_reshard_signature(const TabletMetadataPtr& metadata) {
        _update_manager->unload_and_remove_primary_index(metadata->id());
        _tablet_manager->prune_metacache();
        ASSIGN_OR_RETURN(auto cold, _tablet_manager->get_tablet_metadata(metadata->id(), metadata->version()));
        std::vector<std::string> records;
        auto append = [](std::string* out, const std::string& value) {
            *out += fmt::format("{}:", value.size()) + value;
        };
        // Rank effective recovery keys, preserving equality classes and strict order
        // without comparing tablet-local projected integers across cycles.
        std::map<uint32_t, size_t> recovery_order;
        for (const auto& rowset : cold->rowsets()) {
            recovery_order.emplace(
                    rowset.has_max_compact_input_rowset_id() ? rowset.max_compact_input_rowset_id() : rowset.id(), 0);
        }
        size_t rank = 0;
        for (auto& [key, ordinal] : recovery_order) ordinal = rank++;
        for (const auto& rowset : cold->rowsets()) {
            std::string record =
                    fmt::format("uid={}:{};version={};", rowset.uid().hi(), rowset.uid().lo(), rowset.version());
            RowsetMetadataPB stable(rowset);
            stable.clear_id();
            stable.clear_overlapped();
            stable.clear_deprecated_segments();
            stable.clear_deprecated_segment_size();
            stable.clear_deprecated_segment_encryption_metas();
            stable.clear_deprecated_bundle_file_offsets();
            stable.clear_deprecated_shared_segments();
            stable.clear_range();
            stable.clear_max_compact_input_rowset_id();
            // Stats are semantic values, including an absent zero on a predicate.
            stable.set_num_rows(rowset.num_rows());
            stable.set_data_size(rowset.data_size());
            stable.set_num_dels(rowset.num_dels());
            for (auto& segment : *stable.mutable_segment_metas()) segment.clear_shared();
            for (auto& del : *stable.mutable_del_files()) {
                del.clear_origin_rowset_id();
                del.clear_shared();
            }
            if (rowset.has_delete_predicate()) {
                record = fmt::format("predicate-version={};", rowset.version());
                stable.clear_uid(); // Predicate identity is its version and payload, not its independently minted UID.
            }
            const auto& range = rowset.has_range() ? rowset.range() : cold->range();
            append(&record, range.SerializeAsString());
            append(&record, stable.SerializeAsString());
            const uint32_t recovery_key =
                    rowset.has_max_compact_input_rowset_id() ? rowset.max_compact_input_rowset_id() : rowset.id();
            record += fmt::format("recovery-present={};recovery-order={};", rowset.has_max_compact_input_rowset_id(),
                                  recovery_order.at(recovery_key));
            for (int i = 0; i < rowset.segment_metas_size(); ++i) {
                const auto& segment = rowset.segment_metas(i);
                const uint32_t original_idx = segment.has_segment_idx() ? segment.segment_idx() : i;
                const uint32_t rssid = rowset.id() + original_idx;
                record += fmt::format("segment={};", original_idx);
                if (cold->delvec_meta().delvecs().contains(rssid)) {
                    DelVector delvec;
                    RETURN_IF_ERROR(_update_manager->get_del_vec_in_meta(TabletSegmentId(cold->id(), rssid),
                                                                         cold->version(), false, &delvec));
                    record += "delvec={";
                    if (delvec.roaring() != nullptr) {
                        for (uint32_t rowid : *delvec.roaring()) record += fmt::format("{},", rowid);
                    }
                    record += "};";
                }
                if (auto dcg = cold->dcg_meta().dcgs().find(rssid); dcg != cold->dcg_meta().dcgs().end()) {
                    std::set<uint32_t> active_columns;
                    for (int j = 0; j < dcg->second.column_files_size(); ++j) {
                        if (dcg->second.versions(j) > cold->version()) continue;
                        for (auto uid : dcg->second.unique_column_ids(j).column_ids()) {
                            if (!active_columns.insert(uid).second) continue;
                            // These fixtures have one integer value column. Read the actual .cols
                            // segment including rows masked by delvec, not merely the scan output.
                            if (uid != 1002) return Status::InvalidArgument("unexpected fixed-point DCG column");
                            record += fmt::format("dcg-column={};values=", uid);
                            for (int32_t value : read_c1_only_cols_file(cold->id(), dcg->second.column_files(j))) {
                                record += fmt::format("{},", value);
                            }
                        }
                    }
                }
                lake::LakeIndexDeltaGroupLoader loader(cold);
                lake::IndexDeltaGroupList active;
                RETURN_IF_ERROR(loader.load(TabletSegmentId(cold->id(), rssid), cold->version(), &active));
                std::set<std::pair<int32_t, int>> active_keys;
                for (const auto& entry : active) {
                    ASSIGN_OR_RETURN(auto payload, read_sidecar_payload(_tablet_manager->segment_location(
                                                                                cold->id(), entry.index_file),
                                                                        entry.encryption_meta));
                    for (const auto& key : entry.keys) {
                        active_keys.emplace(key.col_unique_id, key.index_type);
                        record += fmt::format("idg-active={}:{};", key.col_unique_id, int(key.index_type));
                        append(&record, payload);
                    }
                }
                if (auto idg = cold->idg_meta().idgs().find(rssid); idg != cold->idg_meta().idgs().end()) {
                    for (const auto& entry : idg->second.entries()) {
                        for (const auto& key : entry.dropped_keys()) {
                            if (active_keys.contains({key.col_unique_id(), key.index_type()})) {
                                return Status::Corruption("cold IDG lookup returned a dropped key");
                            }
                            record += fmt::format("idg-dropped={}:{};", key.col_unique_id(), int(key.index_type()));
                        }
                    }
                }
            }
            for (const auto& del : rowset.del_files()) {
                record += del.origin_rowset_id() == rowset.id() ? "del-origin=self;" : "del-origin=inherited;";
            }
            records.emplace_back(std::move(record));
        }
        std::sort(records.begin(), records.end());
        std::string result;
        for (const auto& record : records) append(&result, record);
        ASSIGN_OR_RETURN(auto rows, read_two_column_rows(cold));
        for (const auto& [key, value] : rows) result += fmt::format("row={}:{};", key, value);
        if (cold->schema().keys_type() == PRIMARY_KEYS) {
            ASSIGN_OR_RETURN(auto pk_signature, cold_fixed_point_pk_signature(cold, rows));
            append(&result, pk_signature);
        }
        return result;
    }

    struct ReshardInventory {
        size_t rowsets;
        size_t segments;
        size_t dels;
        size_t live_sidecars;
        size_t live_files;
        size_t raw_bytes;
    };

    ReshardInventory collect_reshard_inventory(const TabletMetadataPB& metadata) {
        ReshardInventory result{.rowsets = static_cast<size_t>(metadata.rowsets_size()),
                                .raw_bytes = metadata.ByteSizeLong()};
        std::set<std::string> files;
        std::set<std::string> sidecars;
        for (const auto& rowset : metadata.rowsets()) {
            result.segments += rowset.segment_metas_size();
            result.dels += rowset.del_files_size();
            for (const auto& segment : rowset.segment_metas()) files.insert(segment.filename());
            for (const auto& del : rowset.del_files()) files.insert(del.name());
        }
        for (const auto& [rssid, page] : metadata.delvec_meta().delvecs()) {
            sidecars.insert(metadata.delvec_meta().version_to_file().at(page.version()).name());
        }
        for (const auto& [rssid, dcg] : metadata.dcg_meta().dcgs()) {
            for (const auto& name : dcg.column_files()) sidecars.insert(name);
        }
        for (const auto& [rssid, idg] : metadata.idg_meta().idgs()) {
            for (const auto& entry : idg.entries()) {
                if (entry.keys_size() > entry.dropped_keys_size()) sidecars.insert(entry.index_file());
            }
        }
        result.live_sidecars = sidecars.size();
        files.insert(sidecars.begin(), sidecars.end());
        for (const auto& sst : metadata.sstable_meta().sstables()) files.insert(sst.filename());
        result.live_files = files.size();
        return result;
    }

    StatusOr<std::vector<TabletMetadataPtr>> try_split_fixed_point_source(const TabletMetadataPtr& source,
                                                                          int child_count) {
        RETURN_IF_ERROR(put_tablet_metadata(source));
        SplittingTabletInfoPB split;
        split.set_old_tablet_id(source->id());
        for (int i = 0; i < child_count; ++i) {
            split.add_new_tablet_ids(next_id());
            auto* range = split.add_new_tablet_ranges();
            range->mutable_lower_bound()->CopyFrom(generate_sort_key(i * 100 / child_count));
            range->set_lower_bound_included(true);
            range->mutable_upper_bound()->CopyFrom(generate_sort_key((i + 1) * 100 / child_count));
            range->set_upper_bound_included(false);
        }
        TxnInfoPB txn;
        txn.set_txn_id(next_id());
        txn.set_commit_time(1);
        ASSIGN_OR_RETURN(auto children,
                         lake::split_tablet(_tablet_manager.get(), source, split, source->version() + 1, txn));
        if (children.size() != child_count) {
            return Status::InternalError("fixed-point split returned the wrong number of children");
        }
        std::vector<TabletMetadataPtr> sources;
        for (auto id : split.new_tablet_ids()) sources.push_back(children.at(id));
        return sources;
    }

    std::vector<TabletMetadataPtr> split_fixed_point_source(const TabletMetadataPtr& source, int child_count) {
        ASSIGN_OR_ABORT(auto sources, try_split_fixed_point_source(source, child_count));
        return sources;
    }

    StatusOr<TabletMetadataPtr> try_run_no_write_split_merge_cycle(const TabletMetadataPtr& source, int child_count) {
        ASSIGN_OR_RETURN(auto sources, try_split_fixed_point_source(source, child_count));
        std::unordered_map<int64_t, TabletMetadataPtr> published;
        const int64_t target = next_id();
        RETURN_IF_ERROR(publish_resharding_merge(sources, target, source->version() + 1, source->version() + 2,
                                                 next_id(), published));
        return published.at(target);
    }

    TabletMetadataPtr run_no_write_split_merge_cycle(const TabletMetadataPtr& source, int child_count) {
        ASSIGN_OR_ABORT(auto result, try_run_no_write_split_merge_cycle(source, child_count));
        return result;
    }

    TabletMetadataPtr fixed_point_source(bool primary_key) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(next_id());
        metadata->set_version(1);
        metadata->set_next_rowset_id(10);
        set_two_column_pk_schema(metadata.get(), 4001);
        metadata->mutable_schema()->set_keys_type(primary_key ? PRIMARY_KEYS : UNIQUE_KEYS);
        metadata->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
        metadata->set_enable_persistent_index(primary_key);
        if (primary_key) metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(0));
        metadata->mutable_range()->set_lower_bound_included(true);
        metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(100));
        metadata->mutable_range()->set_upper_bound_included(false);
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(100);
        rowset->set_overlapped(false);
        lake::tablet_reshard_helper::set_rowset_uid(rowset);
        for (int i = 0; i < 2; ++i) {
            const auto name = fmt::format("fixed_{}_{}.dat", metadata->id(), i);
            const auto size = write_two_column_segment(
                    metadata->id(), name, 50, [](int key) { return key * 10; }, i * 50);
            auto* segment = rowset->add_segment_metas();
            segment->set_filename(name);
            segment->set_size(size);
            segment->set_num_rows(50);
            segment->set_segment_idx(i == 0 ? 0 : 7);
            segment->mutable_sort_key_min()->CopyFrom(generate_sort_key(i * 50));
            segment->mutable_sort_key_max()->CopyFrom(generate_sort_key(i * 50 + 49));
            rowset->set_data_size(rowset->data_size() + size);
        }
        if (primary_key) {
            const std::string stem = fmt::format("fixed_{}", metadata->id());
            DelVector delvec;
            const uint32_t deleted = 1;
            delvec.init(1, &deleted, 1);
            add_delvec(metadata.get(), metadata->id(), 1, 1, stem + ".delvec", delvec.save());
            rowset->set_num_dels(1);
            const auto size = write_c1_only_cols_file(metadata->id(), stem + ".cols", 50,
                                                      [](int row) { return row * 10 + 1000; });
            add_dcg_with_columns(metadata.get(), 1, stem + ".cols", {1002}, 1);
            metadata->mutable_dcg_meta()->mutable_dcgs()->at(1).add_column_file_sizes(size);
            auto file = write_sidecar_payload(_tablet_manager->segment_location(metadata->id(), stem + ".idx"),
                                              "fixed-point-index-payload", false);
            add_idg_with_key(metadata.get(), 1, stem + ".idx", 1002, BITMAP, 1);
            add_idg_key(metadata.get(), 1, 1003, BITMAP);
            add_idg_dropped_key(metadata.get(), 1, 1003, BITMAP);
            metadata->mutable_idg_meta()->mutable_idgs()->at(1).mutable_entries(0)->set_file_size(file.filesize);
            auto* del = rowset->add_del_files();
            del->set_name(stem + ".del");
            del->set_origin_rowset_id(0);
            del->set_op_offset(7);
            del->set_version(1);
            del->set_num_rows(0);
            write_binary_del_file(metadata->id(), del->name(), {});
        } else {
            metadata->set_version(2);
            auto* predicate = add_rowset_with_predicate(metadata.get(), 9, 2, true);
            predicate->mutable_delete_predicate()->mutable_binary_predicates(0)->set_value("98");
        }
        CHECK_OK(put_tablet_metadata(metadata));
        return metadata;
    }

    void expect_ten_fixed_point_cycles(TabletMetadataPtr current) {
        const bool primary_key = current->schema().keys_type() == PRIMARY_KEYS;
        auto inherited_dels = [](const TabletMetadataPB& metadata) {
            size_t count = 0;
            for (const auto& rowset : metadata.rowsets()) {
                for (const auto& del : rowset.del_files()) count += del.origin_rowset_id() != rowset.id();
            }
            return count;
        };
        const size_t baseline_inherited_dels = inherited_dels(*current);
        if (primary_key) {
            ASSERT_GT(baseline_inherited_dels, 0);
        }
        size_t prior_lookup_batches = _cold_pk_lookup_batches;
        size_t prior_deleted_keys = _cold_pk_deleted_keys_checked;
        ASSIGN_OR_ABORT(auto baseline_signature, semantic_reshard_signature(current));
        ASSERT_EQ(prior_lookup_batches + static_cast<size_t>(primary_key), _cold_pk_lookup_batches)
                << "every PK baseline must exercise a cold PK lookup batch";
        if (primary_key) {
            ASSERT_GT(_cold_pk_deleted_keys_checked, prior_deleted_keys);
        }
        const auto baseline = collect_reshard_inventory(*current);
        // Conservative field census: tablet scalars plus per-rowset/segment/del,
        // sidecar and SST integer slots. Each varint can grow by at most 10 bytes.
        const size_t live_volatile_integer_fields =
                16 + 8 * baseline.rowsets + 8 * baseline.segments + 8 * baseline.dels + 8 * baseline.live_sidecars;
        const size_t live_generated_filenames = baseline.live_files;
        const size_t raw_byte_limit =
                baseline.raw_bytes + 10 * live_volatile_integer_fields + 64 * live_generated_filenames + 4096;
        LOG(INFO) << "fixed-point baseline: rowsets=" << baseline.rowsets << " segments=" << baseline.segments
                  << " dels=" << baseline.dels << " sidecars=" << baseline.live_sidecars
                  << " files=" << baseline.live_files << " raw_bytes=" << baseline.raw_bytes
                  << " raw_byte_limit=" << raw_byte_limit;
        for (int cycle = 0; cycle < 10; ++cycle) {
            SCOPED_TRACE(fmt::format("fixed-point cycle {} keys_type {}", cycle, int(current->schema().keys_type())));
            current = run_no_write_split_merge_cycle(current, cycle % 2 == 0 ? 2 : 3);
            prior_lookup_batches = _cold_pk_lookup_batches;
            prior_deleted_keys = _cold_pk_deleted_keys_checked;
            ASSIGN_OR_ABORT(auto signature, semantic_reshard_signature(current));
            EXPECT_EQ(prior_lookup_batches + static_cast<size_t>(primary_key), _cold_pk_lookup_batches)
                    << "every PK cycle must exercise a cold PK lookup batch";
            if (primary_key) {
                EXPECT_GT(_cold_pk_deleted_keys_checked, prior_deleted_keys);
            }
            const auto inventory = collect_reshard_inventory(*current);
            EXPECT_EQ(baseline_inherited_dels, inherited_dels(*current));
            EXPECT_EQ(baseline_signature, signature);
            EXPECT_EQ(baseline.rowsets, inventory.rowsets);
            EXPECT_EQ(baseline.segments, inventory.segments);
            EXPECT_EQ(baseline.dels, inventory.dels);
            EXPECT_EQ(baseline.live_sidecars, inventory.live_sidecars);
            EXPECT_EQ(baseline.live_files, inventory.live_files);
            EXPECT_LE(inventory.raw_bytes,
                      baseline.raw_bytes + 10 * live_volatile_integer_fields + 64 * live_generated_filenames + 4096);
            LOG(INFO) << "fixed-point cycle=" << cycle << " raw_bytes=" << inventory.raw_bytes;
            if (HasFailure()) return;
        }
    }

    size_t _cold_pk_lookup_batches = 0;
    size_t _cold_pk_deleted_keys_checked = 0;
    std::unique_ptr<starrocks::lake::TabletManager> _tablet_manager;
    std::string _test_dir;
    std::shared_ptr<lake::LocationProvider> _location_provider;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<lake::UpdateManager> _update_manager;
};

inline TxnLogPtr make_op_compaction_log(int64_t source_tablet_id) {
    auto log = std::make_shared<TxnLogPB>();
    log->set_tablet_id(source_tablet_id);
    log->set_txn_id(2000);
    auto* op = log->mutable_op_compaction();
    op->add_input_rowsets(100);
    op->add_input_rowsets(101);
    op->mutable_output_rowset()->add_segment_metas()->set_filename("out_seg.dat");
    // Normal (non-partial) compaction: all output segments are newly written.
    op->set_new_segment_offset(0);
    op->set_new_segment_count(1);
    op->mutable_output_sstable()->set_filename("out_sstable.sst");
    return log;
}

} // namespace starrocks
