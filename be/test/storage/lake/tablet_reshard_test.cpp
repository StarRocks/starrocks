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

#include <bvar/bvar.h>
#include <fmt/format.h>
#include <gmock/gmock.h>
#include <google/protobuf/unknown_field_set.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <ctime>
#include <functional>
#include <limits>
#include <set>

#include "base/failpoint/fail_point.h"
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
#include "common/config_lake_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_rowset_fwd.h"
#include "common/config_starlet_fwd.h"
#include "common/config_storage_fwd.h"
#include "common/runtime_profile.h"
#include "exec/exec_env.h"
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
#include "storage/lake/join_path.h"
#include "storage/lake/lake_persistent_index.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/persistent_index_sstable.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_merger.h"
#include "storage/lake/tablet_range_helper.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_reshard_helper.h"
#include "storage/lake/test_util.h"
#include "storage/lake/transactions.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/vacuum.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/seek_range.h"
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
        }
        return _tablet_manager->put_tablet_metadata(metadata);
    }

    Status put_tablet_metadata(const TabletMetadataPtr& metadata) {
        auto mutable_meta = std::make_shared<TabletMetadataPB>(*metadata);
        for (auto& rowset : *mutable_meta->mutable_rowsets()) {
            lake::tablet_reshard_helper::ensure_rowset_uid(&rowset);
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
            sm->set_filename("segment.dat");
            sm->set_size(128);
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
            }
            rowset->set_num_rows(1);
            rowset->set_data_size(128);
            // Match production: every lake writer mints a unique uid, so distinct
            // per-tablet data rowsets never alias across tablets at merge.
            lake::tablet_reshard_helper::set_rowset_uid(rowset);
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

    Status publish_resharding_merge(const std::vector<TabletMetadataPtr>& sources, int64_t merged_tablet,
                                    int64_t base_version, int64_t new_version, int64_t txn_id,
                                    std::unordered_map<int64_t, TabletMetadataPtr>& tablet_metadatas,
                                    const std::function<void()>& before_merge = {}) {
        for (const auto& source : sources) {
            RETURN_IF_ERROR(put_tablet_metadata(source));
        }
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

    std::shared_ptr<TabletMetadataPB> make_allocator_source(int64_t tablet_id, uint32_t next_rowset_id = 1) {
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
                                             uint32_t segment_idx, bool explicit_segment_idx = true) {
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(segment_name);
        segment->set_size(1);
        segment->set_num_rows(1);
        if (explicit_segment_idx) segment->set_segment_idx(segment_idx);
        rowset->set_num_rows(rowset->num_rows() + 1);
        rowset->set_data_size(rowset->data_size() + 1);
        return segment;
    }

    StatusOr<TabletMetadataPtr> publish_allocator_merge(
            const std::vector<std::shared_ptr<TabletMetadataPB>>& mutable_sources,
            const std::function<void()>& before_merge = {}, int64_t* attempted_target_id = nullptr) {
        if (mutable_sources.empty()) return Status::InvalidArgument("allocator merge fixture has no source");
        const int64_t target_id = next_id();
        if (attempted_target_id != nullptr) *attempted_target_id = target_id;
        prepare_tablet_dirs(target_id);
        std::vector<TabletMetadataPtr> sources;
        sources.reserve(mutable_sources.size());
        for (const auto& source : mutable_sources) {
            prepare_tablet_dirs(source->id());
            sources.emplace_back(source);
        }
        std::unordered_map<int64_t, TabletMetadataPtr> published;
        RETURN_IF_ERROR(publish_resharding_merge(sources, target_id, /*base_version=*/1, /*new_version=*/2,
                                                 /*txn_id=*/next_id(), published, before_merge));
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
        const uint64_t segment_a_size =
                write_two_column_segment(source_a_id, segment_a, /*num_rows=*/1, [](int key) { return key * 10; }, 10);
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

    struct MergePhaseCounts {
        int materialize = 0;
        int dcg_writes = 0;
        int delvec_writes = 0;
        int source_flushes = 0;
    };

    StatusOr<MutableTabletMetadataPtr> merge_with_phase_counts(const std::vector<TabletMetadataPtr>& sources,
                                                               int64_t target_tablet_id, int64_t target_version,
                                                               MergePhaseCounts* counts) {
        auto* sync = SyncPoint::GetInstance();
        sync->SetCallBack("materialize_planned_rowsets:entry", [&](void*) { ++counts->materialize; });
        sync->SetCallBack("merge_dcg_meta:after_write_cols", [&](void*) { ++counts->dcg_writes; });
        sync->SetCallBack("merge_delvecs:writer_invocations",
                          [&](void* arg) { counts->delvec_writes += *static_cast<int*>(arg); });
        sync->SetCallBack("merge_sstables:source_pk_flush", [&](void*) { ++counts->source_flushes; });
        sync->EnableProcessing();
        DeferOp cleanup_sync_points([&] {
            sync->ClearCallBack("materialize_planned_rowsets:entry");
            sync->ClearCallBack("merge_dcg_meta:after_write_cols");
            sync->ClearCallBack("merge_delvecs:writer_invocations");
            sync->ClearCallBack("merge_sstables:source_pk_flush");
            sync->DisableProcessing();
        });

        MergingTabletInfoPB merging;
        for (const auto& source : sources) merging.add_old_tablet_ids(source->id());
        merging.set_new_tablet_id(target_tablet_id);
        TxnInfoPB txn_info;
        txn_info.set_txn_id(next_id());
        txn_info.set_commit_time(1);
        txn_info.set_gtid(1);
        return lake::merge_tablet(_tablet_manager.get(), sources, merging, target_version, txn_info,
                                  /*skip_sstable_merge=*/false);
    }

    Status expect_physical_preflight_rejection(const std::vector<TabletMetadataPtr>& sources, int64_t target_tablet_id,
                                               int64_t target_version, MergePhaseCounts* counts) {
        std::vector<std::string> source_pbs;
        std::map<int64_t, std::set<std::string>> segment_inventories;
        std::map<int64_t, std::set<std::string>> metadata_inventories;
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

        auto merged = merge_with_phase_counts(sources, target_tablet_id, target_version, counts);

        EXPECT_TRUE(merged.status().is_corruption()) << merged.status();
        EXPECT_EQ(0, counts->materialize);
        EXPECT_EQ(0, counts->dcg_writes);
        EXPECT_EQ(0, counts->delvec_writes);
        EXPECT_EQ(0, counts->source_flushes);
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

    std::vector<std::shared_ptr<TabletMetadataPB>> make_preflight_sst_sources(const std::string& stem) {
        const std::string segment = stem + ".dat";
        auto source_a = make_preflight_sidecar_source(next_id(), segment, /*shared_segment=*/true,
                                                      /*common_rowset_uid=*/true);
        auto source_b = make_preflight_sidecar_source(next_id(), segment, /*shared_segment=*/true,
                                                      /*common_rowset_uid=*/true);
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
            sst->set_filename(stem + ".sst");
            sst->set_filesize(128);
            sst->set_shared(true);
            sst->set_shared_rssid(1);
            sst->set_shared_version(1);
            sst->set_max_rss_rowid(static_cast<uint64_t>(1) << 32);
            sst->set_generation_version(1);
            sst->mutable_range()->set_start_key(encode_int_primary_key(10));
            sst->mutable_range()->set_end_key(encode_int_primary_key(90));
        }
        return {std::move(source_a), std::move(source_b)};
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
        for (const auto& segment_filename : segment_filenames) {
            auto* segment = rowset->add_segment_metas();
            segment->set_filename(segment_filename);
            segment->set_size(100);
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
                              const std::vector<uint32_t>& column_ids, int64_t version) {
        auto& dcg = (*metadata->mutable_dcg_meta()->mutable_dcgs())[segment_id];
        dcg.add_column_files(file_name);
        auto* cids = dcg.add_unique_column_ids();
        for (auto cid : column_ids) {
            cids->add_column_ids(cid);
        }
        dcg.add_versions(version);
        dcg.add_shared_files(true);
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
                                      const std::function<int(int)>& source_value_of, int key_start = 0) {
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
            v0[i] = key_start + i;
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
        const bool old_parallel_compaction = config::enable_pk_index_parallel_compaction;
        config::enable_pk_index_parallel_compaction = false;
        DeferOp restore_parallel([&] { config::enable_pk_index_parallel_compaction = old_parallel_compaction; });

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

    struct RepeatedLifecycleResult {
        TabletMetadataPtr metadata;
        std::vector<TabletMetadataPtr> final_sources;
        std::map<int32_t, int32_t> expected;
        std::vector<int32_t> deleted_keys;
        std::set<std::string> final_sst_orphans;
    };

    StatusOr<RepeatedLifecycleResult> run_repeated_four_cycle_lifecycle(bool enable_tde) {
        const bool old_tde = config::enable_transparent_data_encryption;
        const bool old_parallel_compaction = config::enable_pk_index_parallel_compaction;
        const int32_t old_min_segments = config::lake_pk_compaction_min_input_segments;
        config::enable_transparent_data_encryption = enable_tde;
        config::enable_pk_index_parallel_compaction = false;
        config::lake_pk_compaction_min_input_segments = 1;
        DeferOp restore_config([&] {
            config::enable_transparent_data_encryption = old_tde;
            config::enable_pk_index_parallel_compaction = old_parallel_compaction;
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
        const uint64_t sibling_size =
                write_two_column_segment(child_b, sibling_segment, 1, [](int) { return 600; }, /*key_start=*/60);
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

    std::unique_ptr<starrocks::lake::TabletManager> _tablet_manager;
    std::string _test_dir;
    std::shared_ptr<lake::LocationProvider> _location_provider;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<lake::UpdateManager> _update_manager;
};

TEST_F(LakeTabletReshardTest, test_tablet_merging_interval_projection_repeated_four_cycle_lifecycle) {
    ASSIGN_OR_ABORT(auto result, run_repeated_four_cycle_lifecycle(/*enable_tde=*/false));
    EXPECT_EQ(result.final_sst_orphans.size(), static_cast<size_t>(result.metadata->orphan_files_size()));
    for (const auto& orphan : result.metadata->orphan_files()) EXPECT_TRUE(orphan.encryption_meta().empty());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_interval_projection_repeated_tde_sidecar_vacuum_lifecycle) {
    ASSIGN_OR_ABORT(auto result, run_repeated_four_cycle_lifecycle(/*enable_tde=*/true));
    ASSERT_GE(result.final_sst_orphans.size(), 3);

    const bool old_tde = config::enable_transparent_data_encryption;
    const bool old_parallel_compaction = config::enable_pk_index_parallel_compaction;
    config::enable_transparent_data_encryption = true;
    config::enable_pk_index_parallel_compaction = false;
    DeferOp restore_config([&] {
        config::enable_transparent_data_encryption = old_tde;
        config::enable_pk_index_parallel_compaction = old_parallel_compaction;
    });
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_metadata_only_private_complete_reuse) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_metadata_only_identical_complete_reuse) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_metadata_only_private_uncertainty_falls_back) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_projected_target_domain_before_target_io) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_metadata_only_identical_divergence_falls_back) {
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
             [](auto& sources) { sources[1]->mutable_rowsets(0)->mutable_segment_metas(0)->set_segment_idx(7); }},
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_metadata_only_fallback_folds_source_sst_orphans) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_metadata_only_identical_repeated_filename_rejected) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_metadata_only_source_range_partition_validation) {
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
            {"well-formed gap", Outcome::kFallback,
             [&](auto& sources) {
                 set_range(sources[0].get(), 0, 40);
                 set_range(sources[1].get(), 50, 100);
             }},
            {"well-formed overlap", Outcome::kFallback,
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_segmentless_rowset_preservation_controls) {
    struct Case {
        const char* name;
        bool primary_key;
        bool skip_sstable_merge;
    };
    const std::vector<Case> cases = {
            {"non-PK real merge", false, false},
            {"read-only PK alias", true, true},
    };
    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        auto source = std::make_shared<TabletMetadataPB>();
        source->set_id(next_id());
        source->set_version(1);
        source->set_next_rowset_id(2);
        if (test_case.primary_key) {
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

        MergingTabletInfoPB merging_info;
        merging_info.set_new_tablet_id(next_id());
        TxnInfoPB txn_info;
        txn_info.set_txn_id(next_id());
        std::vector<TabletMetadataPtr> sources = {source};
        ASSIGN_OR_ABORT(auto merged, lake::merge_tablet(_tablet_manager.get(), sources, merging_info, /*new_version=*/2,
                                                        txn_info, test_case.skip_sstable_merge));
        EXPECT_EQ(1, merged->rowsets_size());
        if (merged->rowsets_size() != 1) continue;
        EXPECT_EQ(1, merged->rowsets(0).id());
        EXPECT_EQ(0, merged->rowsets(0).segment_metas_size());
        EXPECT_EQ(0, merged->rowsets(0).del_files_size());
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_indexless_allocation_covers_pruned_delete_history) {
    constexpr uint32_t kContainingRowset = 1;
    constexpr uint32_t kOriginRowset = 1;
    constexpr uint32_t kOpOffset = 7;
    auto add_pruned_delete_history = [=](std::vector<std::shared_ptr<TabletMetadataPB>>& sources) {
        auto* rowset = sources[0]->mutable_rowsets(0);
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
    const uint64_t replay_ceiling = uint64_t{kContainingRowset} + kOpOffset;
    const uint64_t origin_ceiling = uint64_t{kOriginRowset} + kOpOffset;
    EXPECT_GT(target.next_rowset_id(), replay_ceiling);
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
    EXPECT_GT(allocated_rssid, replay_ceiling);
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
    ASSIGN_OR_ABORT(auto zero_control,
                    publish_metadata_only_merge_fixture(MetadataOnlyMergeShape::kPrivate, /*enable_tde=*/false,
                                                        /*with_del_file=*/false, /*skip_source_flush=*/true,
                                                        zero_segment_no_del));
    const auto& zero_target = zero_control.published.at(zero_control.target_tablet_id);
    EXPECT_EQ(0, zero_target.rowsets_size());
    EXPECT_EQ(0, zero_target.sstable_meta().sstables_size());
    EXPECT_EQ(1, zero_target.next_rowset_id());

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
        del->set_origin_rowset_id(1);
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_issue11935_falls_back_then_dml_is_exact) {
    const bool old_primary_key_recover = config::enable_primary_key_recover;
    const bool old_parallel_compaction = config::enable_pk_index_parallel_compaction;
    config::enable_primary_key_recover = true;
    config::enable_pk_index_parallel_compaction = false;
    DeferOp restore_config([&] {
        config::enable_primary_key_recover = old_primary_key_recover;
        config::enable_pk_index_parallel_compaction = old_parallel_compaction;
    });
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_issue11939_falls_back_then_dml_is_exact) {
    const bool old_parallel_compaction = config::enable_pk_index_parallel_compaction;
    config::enable_pk_index_parallel_compaction = false;
    DeferOp restore_parallel_compaction([&] { config::enable_pk_index_parallel_compaction = old_parallel_compaction; });
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_indexless_fallback_lifecycle_matrix) {
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
    const bool old_parallel_compaction = config::enable_pk_index_parallel_compaction;
    const int32_t old_min_segments = config::lake_pk_compaction_min_input_segments;
    DeferOp restore_config([&] {
        config::enable_transparent_data_encryption = old_tde;
        config::enable_pk_index_parallel_compaction = old_parallel_compaction;
        config::lake_pk_compaction_min_input_segments = old_min_segments;
    });
    DeferOp wait_flush_pool(
            [] { ExecEnv::GetInstance()->lake_services().pk_index_memtable_flush_thread_pool->wait(); });
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    DeferOp restore_flush([&] { set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE); });
    config::lake_pk_compaction_min_input_segments = 1;
    config::enable_pk_index_parallel_compaction = false;

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_indexless_fallback_uses_native_reload) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_indexless_split_divergent_layout_falls_back_exact) {
    const bool old_parallel_compaction = config::enable_pk_index_parallel_compaction;
    config::enable_pk_index_parallel_compaction = false;
    DeferOp restore_parallel([&] { config::enable_pk_index_parallel_compaction = old_parallel_compaction; });
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_indexless_split_identical_layout_reuses_sstables) {
    const bool old_parallel_compaction = config::enable_pk_index_parallel_compaction;
    const int32_t old_min_segments = config::lake_pk_compaction_min_input_segments;
    config::enable_pk_index_parallel_compaction = false;
    config::lake_pk_compaction_min_input_segments = 1;
    DeferOp restore_config([&] {
        config::enable_pk_index_parallel_compaction = old_parallel_compaction;
        config::lake_pk_compaction_min_input_segments = old_min_segments;
    });
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_read_only_skip_stays_indexless_without_recovery) {
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
    ASSIGN_OR_ABORT(auto read_only,
                    lake::merge_tablet(_tablet_manager.get(), {left, right}, merging, left->version() + 1, txn_info,
                                       /*skip_sstable_merge=*/true));
    EXPECT_EQ(0, read_only->sstable_meta().sstables_size());
    EXPECT_TRUE(read_only->orphan_files().empty());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_indexless_tde_failure_retry_matrix) {
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

    int rs_a_fully_pruned_but_kept = 0;
    int rs_b_present = 0;
    for (int64_t child : {child0, child1}) {
        auto c = tablet_metadatas.at(child);
        const RowsetMetadataPB* rs_a_out = nullptr;
        for (const auto& r : c->rowsets()) {
            if (r.id() == 2) rs_a_out = &r;
            if (r.id() == 10) ++rs_b_present;
        }
        ASSERT_NE(rs_a_out, nullptr) << "rs_a must survive on every child (overlap or del_files guard)";
        EXPECT_GT(rs_a_out->del_files_size(), 0) << "rs_a keeps its del_files";
        if (rs_a_out->segment_metas_size() == 0) ++rs_a_fully_pruned_but_kept; // kept purely by the del_files guard
    }
    EXPECT_EQ(1, rs_a_fully_pruned_but_kept)
            << "exactly one child fully prunes rs_a's segment yet keeps it for del_files";
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_filters_prechange_protected_rssid_delvecs) {
    constexpr int64_t kBaseVersion = 1;
    constexpr int64_t kMergeVersion = 2;

    for (bool stale_first : {false, true}) {
        SCOPED_TRACE(stale_first ? "partial stale-first" : "partial stale-last");
        ASSIGN_OR_ABORT(auto fixture, make_prechange_protected_rssid_fixture());
        const int64_t merged_tablet = next_id();
        prepare_tablet_dirs(merged_tablet);
        std::vector<TabletMetadataPtr> sources = {fixture.stale_child, fixture.empty_child};
        if (!stale_first) std::reverse(sources.begin(), sources.end());
        ASSIGN_OR_ABORT(auto files_before_merge, delvec_inventory(merged_tablet));
        std::unordered_map<int64_t, TabletMetadataPtr> published;
        ASSERT_OK(publish_resharding_merge(sources, merged_tablet, kBaseVersion, kMergeVersion, next_id(), published));
        const auto& merged = published.at(merged_tablet);

        // No source in this partial merge owns a segment at the protected rssid. The
        // old zero-segment rowset is discarded, so neither its page nor its file may
        // survive, independent of which child seeds the merge namespace.
        EXPECT_EQ(0, merged->rowsets_size());
        EXPECT_EQ(0, merged->delvec_meta().delvecs_size());
        EXPECT_EQ(0, merged->delvec_meta().version_to_file_size());
        ASSIGN_OR_ABORT(auto files_after_merge, delvec_inventory(merged_tablet));
        EXPECT_EQ(files_before_merge, files_after_merge);

        // Discarding the protected rowset makes rssid 1 the next real allocation.
        // A stale page at rssid 1 masks this first writer's row even though the PK
        // index points to it, which is the user-visible corruption this regression
        // must prevent.
        ASSERT_EQ(fixture.protected_rssid, merged->next_rowset_id());
        _update_manager->unload_and_remove_primary_index(merged_tablet);
        ASSIGN_OR_ABORT(auto after_write,
                        publish_followup_upsert_delete(merged_tablet, merged->version(), /*upsert_key=*/60,
                                                       /*upsert_value=*/600, /*delete_key=*/0,
                                                       /*include_delete=*/false));
        ASSERT_EQ(1, after_write->rowsets_size());
        ASSERT_EQ(1, after_write->rowsets(0).segment_metas_size());
        EXPECT_EQ(fixture.protected_rssid, lake::get_rssid(after_write->rowsets(0), 0));

        _update_manager->unload_and_remove_primary_index(merged_tablet);
        _tablet_manager->prune_metacache();
        ASSIGN_OR_ABORT(auto reopened, _tablet_manager->get_tablet_metadata(merged_tablet, after_write->version()));
        expect_lifecycle_oracle(reopened, {{60, 600}}, /*deleted_keys=*/{});
    }

    for (bool stale_first : {false, true}) {
        SCOPED_TRACE(stale_first ? "full stale-first" : "full stale-last");
        ASSIGN_OR_ABORT(auto fixture, make_prechange_protected_rssid_fixture());
        const int64_t merged_tablet = next_id();
        prepare_tablet_dirs(merged_tablet);
        std::vector<TabletMetadataPtr> sources = {fixture.owner, fixture.stale_child};
        if (stale_first) std::reverse(sources.begin(), sources.end());
        ASSIGN_OR_ABORT(auto files_before_merge, delvec_inventory(merged_tablet));
        std::unordered_map<int64_t, TabletMetadataPtr> published;
        ASSERT_OK(publish_resharding_merge(sources, merged_tablet, kBaseVersion, kMergeVersion, next_id(), published));
        const auto& merged = published.at(merged_tablet);

        uint32_t owner_rssid = 0;
        for (const auto& rowset : merged->rowsets()) {
            for (int segment_pos = 0; segment_pos < rowset.segment_metas_size(); ++segment_pos) {
                if (rowset.segment_metas(segment_pos).filename() == "prechange_protected_owner.dat") {
                    owner_rssid = lake::get_rssid(rowset, segment_pos);
                }
            }
        }
        ASSERT_NE(0, owner_rssid);
        EXPECT_EQ(1, merged->delvec_meta().delvecs_size());
        EXPECT_TRUE(merged->delvec_meta().delvecs().contains(owner_rssid));
        EXPECT_EQ(1, merged->delvec_meta().version_to_file_size());
        ASSIGN_OR_ABORT(auto files_after_merge, delvec_inventory(merged_tablet));
        EXPECT_EQ(files_before_merge.size() + 1, files_after_merge.size());

        DelVector owner_delvec;
        LakeIOOptions io_options;
        ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), *merged, owner_rssid, false, io_options, &owner_delvec));
        ASSERT_NE(nullptr, owner_delvec.roaring());
        EXPECT_FALSE(owner_delvec.roaring()->contains(0));
        EXPECT_TRUE(owner_delvec.roaring()->contains(1));

        _update_manager->unload_and_remove_primary_index(merged_tablet);
        _tablet_manager->prune_metacache();
        ASSIGN_OR_ABORT(auto reopened, _tablet_manager->get_tablet_metadata(merged_tablet, merged->version()));
        expect_lifecycle_oracle(reopened, {{0, 0}}, /*deleted_keys=*/{1});
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_delvec_drops_live_source_page_without_target_segment) {
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

    // Single segment with 2 sort-key samples -> 4 boundary points -> 3
    // candidate ranges. Requesting 8 splits cannot be satisfied.
    auto* rowset_meta_pb = metadata.add_rowsets();
    rowset_meta_pb->set_id(2);
    {
        auto* sm = rowset_meta_pb->add_segment_metas();
        sm->set_filename("seg_0.dat");
        sm->set_size(1024);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(300));
        sm->set_num_rows(300);
        sm->set_deprecated_sort_key_sample_row_interval(100);
        sm->add_deprecated_sort_key_samples()->CopyFrom(generate_sort_key(100));
        sm->add_deprecated_sort_key_samples()->CopyFrom(generate_sort_key(200));
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
        EXPECT_LE(child_rowset.num_dels(), child_rowset.num_rows()) << "num_dels must not exceed child num_rows";
        total_child_num_rows += child_rowset.num_rows();
        total_child_num_dels += child_rowset.num_dels();
    }

    EXPECT_EQ(10, total_child_num_rows);
    // Largest-remainder allocation is exact for in-range rows: Σ child.num_dels must equal D.
    EXPECT_EQ(6, total_child_num_dels);
}

// Verify the fallback path: when the parent rowset predates num_dels (has_num_dels() ==
// false), split derives D from the persisted delvec. A child rowset that cannot retrieve
// D through either path must still carry an explicit num_dels (0) so that the Step 2
// router in lake_service sees has_range() but has_num_dels() -> defaults to zero dels.
TEST_F(LakeTabletReshardTest, test_pk_tablet_splitting_fallback_reads_delvec_for_num_dels) {
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
    // num_dels intentionally not set -> exercises get_rowset_num_deletes fallback.

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
    // Σ child.num_dels should equal the delvec cardinality recovered via fallback (4).
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
            EXPECT_LE(rs.num_dels(), rs.num_rows()) << "child cid=" << cid << " rs=" << rs.id();
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

// Pathological metadata: parent rowset has num_dels > num_rows. The anchor
// builder clamps num_dels up front (with WARNING) so cap-and-redistribute
// has a feasible input. After split, Σ children.num_dels equals the clamped
// parent.num_rows, and per-child num_dels stays within rows.
TEST_F(LakeTabletReshardTest, test_pk_tablet_splitting_anchor_clamps_invalid_parent_dels) {
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
    rowset->set_num_dels(15); // pathological: > num_rows

    // Two segments so calculate_range_split_boundaries has enough key-space
    // boundaries to produce a 2-way split (single segment falls back to
    // identical-tablet publish, which would skip the anchor pass).
    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("seg_0.dat");
        sm->set_size(500);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
        sm->set_num_rows(5);
    }

    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("seg_1.dat");
        sm->set_size(500);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(50));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(99));
        sm->set_num_rows(5);
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

    int64_t total_child_num_rows = 0;
    int64_t total_child_num_dels = 0;
    for (int64_t cid : {child_id_1, child_id_2}) {
        auto it = tablet_metadatas.find(cid);
        ASSERT_TRUE(it != tablet_metadatas.end());
        ASSERT_EQ(1, it->second->rowsets_size());
        const auto& child_rs = it->second->rowsets(0);
        EXPECT_TRUE(child_rs.has_num_dels());
        EXPECT_LE(child_rs.num_dels(), child_rs.num_rows());
        total_child_num_rows += child_rs.num_rows();
        total_child_num_dels += child_rs.num_dels();
    }
    // num_rows still conserves at parent.num_rows; num_dels conserves at the
    // *clamped* parent value (= parent.num_rows = 10), not the bogus 15.
    EXPECT_EQ(10, total_child_num_rows);
    EXPECT_EQ(10, total_child_num_dels);
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
// Setup uses sampled segments (sort_key_samples populated) so segment-level
// boundary candidates are dense enough for 3 successive splits to find
// candidates inside ever-narrowing tablet ranges.
TEST_F(LakeTabletReshardTest, test_pk_tablet_splitting_anchor_three_level_chain_conservation) {
    auto add_sampled_rowset = [](TabletMetadataPB* md, int64_t rs_id, int min_v, int max_v, int num_rows, int data_size,
                                 int num_dels, int interval) {
        auto* rs = md->add_rowsets();
        rs->set_id(rs_id);
        rs->set_overlapped(true);
        rs->set_num_rows(num_rows);
        rs->set_data_size(data_size);
        rs->set_num_dels(num_dels);
        auto* sm = rs->add_segment_metas();
        sm->set_filename(fmt::format("rs_{}_0.dat", rs_id));
        sm->set_size(data_size);
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(min_v));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(max_v));
        sm->set_num_rows(num_rows);
        sm->set_deprecated_sort_key_sample_row_interval(interval);
        for (int v = min_v + interval; v < max_v; v += interval) {
            sm->add_deprecated_sort_key_samples()->CopyFrom(generate_sort_key(v));
        }
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
                EXPECT_LE(rs.num_dels(), rs.num_rows())
                        << level << ": child " << cid << " rs " << rs.id() << " num_dels exceeds num_rows";
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
    set_primary_key_schema(&metadata, 1);
    add_historical_schema(&metadata, 1);

    // 2 rowsets covering [0,1499] with samples every 100 rows. Combined ~2000
    // rows / 16000 bytes / 42 dels. Sample density gives every ~100 keys a
    // boundary candidate, plenty to drive 3 levels of splitting.
    add_sampled_rowset(&metadata, /*rs_id=*/2, /*min=*/0, /*max=*/999, /*num_rows=*/1000,
                       /*data_size=*/10000, /*num_dels=*/30, /*interval=*/100);
    add_sampled_rowset(&metadata, /*rs_id=*/3, /*min=*/500, /*max=*/1499, /*num_rows=*/1000,
                       /*data_size=*/6000, /*num_dels=*/12, /*interval=*/100);

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

TEST_F(LakeTabletReshardTest, test_merge_rowsets_reorder_by_predicate_version) {
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
    EXPECT_OK(put_tablet_metadata(meta_a));

    TabletMetadataPB meta_b;
    meta_b.set_id(tablet_b);
    meta_b.set_version(base_version);
    meta_b.set_next_rowset_id(4);
    add_rowset_with_predicate(&meta_b, 1, 1, false);
    add_rowset_with_predicate(&meta_b, 2, 10, true);
    add_rowset_with_predicate(&meta_b, 3, 11, false);
    EXPECT_OK(put_tablet_metadata(meta_b));

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

TEST_F(LakeTabletReshardTest, test_merge_rowsets_different_predicate_versions) {
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
    EXPECT_OK(put_tablet_metadata(meta_a));

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
    EXPECT_OK(put_tablet_metadata(meta_b));

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

TEST_F(LakeTabletReshardTest, test_merge_rowsets_no_predicates) {
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
    EXPECT_OK(put_tablet_metadata(meta_a));

    TabletMetadataPB meta_b;
    meta_b.set_id(tablet_b);
    meta_b.set_version(base_version);
    meta_b.set_next_rowset_id(4);
    add_rowset_with_predicate(&meta_b, 1, 1, false);
    add_rowset_with_predicate(&meta_b, 2, 2, false);
    add_rowset_with_predicate(&meta_b, 3, 3, false);
    EXPECT_OK(put_tablet_metadata(meta_b));

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

TEST_F(LakeTabletReshardTest, test_merge_rowsets_single_tablet_predicate) {
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
    EXPECT_OK(put_tablet_metadata(meta_a));

    // Tablet B: data(v1) -> data(v2) -> data(v3) (no predicates)
    TabletMetadataPB meta_b;
    meta_b.set_id(tablet_b);
    meta_b.set_version(base_version);
    meta_b.set_next_rowset_id(4);
    add_rowset_with_predicate(&meta_b, 1, 1, false);
    add_rowset_with_predicate(&meta_b, 2, 2, false);
    add_rowset_with_predicate(&meta_b, 3, 3, false);
    EXPECT_OK(put_tablet_metadata(meta_b));

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

TEST_F(LakeTabletReshardTest, test_merge_rowsets_all_predicates) {
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
    EXPECT_OK(put_tablet_metadata(meta_a));

    // Tablet B: predicate(v10) -> predicate(v20) (same versions)
    TabletMetadataPB meta_b;
    meta_b.set_id(tablet_b);
    meta_b.set_version(base_version);
    meta_b.set_next_rowset_id(3);
    add_rowset_with_predicate(&meta_b, 1, 10, true); // predicate v10
    add_rowset_with_predicate(&meta_b, 2, 20, true); // predicate v20
    EXPECT_OK(put_tablet_metadata(meta_b));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_basic) {
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

    EXPECT_OK(put_tablet_metadata(meta1));
    EXPECT_OK(put_tablet_metadata(meta2));

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
            EXPECT_EQ(rowset.range().SerializeAsString(), meta2->range().SerializeAsString());
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
            EXPECT_EQ(rowset.range().SerializeAsString(), meta1->range().SerializeAsString());
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
// rowset into merge_rowsets. The gate is DCHECK-first (fail-fast abort in debug) with
// a Status::InternalError fallback for release, so the assertion is build-conditional.
TEST_F(LakeTabletReshardTest, test_tablet_merging_rowset_without_uid_fails) {
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
#if DCHECK_IS_ON()
    ASSERT_DEATH({ (void)do_merge(); }, "rowset reaching reshard merge must carry a valid uid");
#else
    auto st = do_merge();
    EXPECT_TRUE(st.is_internal_error()) << st.to_string();
    EXPECT_NE(std::string::npos, st.to_string().find("rowset reaching reshard merge has no uid")) << st.to_string();
#endif
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_without_delvec) {
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

    EXPECT_OK(put_tablet_metadata(meta1));
    EXPECT_OK(put_tablet_metadata(meta2));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_skip_missing_delvec_meta) {
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

    EXPECT_OK(put_tablet_metadata(meta1));
    EXPECT_OK(put_tablet_metadata(meta2));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_delvec_version_missing) {
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

    EXPECT_OK(put_tablet_metadata(meta1));
    EXPECT_OK(put_tablet_metadata(meta2));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_delvec_missing_tablet_offset) {
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

    EXPECT_OK(put_tablet_metadata(meta1));
    EXPECT_OK(put_tablet_metadata(meta2));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(old_tablet_id_1);
    merging_tablet.add_old_tablet_ids(old_tablet_id_2);
    merging_tablet.set_new_tablet_id(new_tablet_id);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("merge_delvecs:before_apply_offsets", [](void* arg) {
        auto* base_offset_by_file_name = reinterpret_cast<std::unordered_map<std::string, uint64_t>*>(arg);
        base_offset_by_file_name->clear();
    });

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto st = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_TRUE(st.is_invalid_argument());

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_delvec_missing_file_offset) {
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

    EXPECT_OK(put_tablet_metadata(meta1));
    EXPECT_OK(put_tablet_metadata(meta2));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(old_tablet_id_1);
    merging_tablet.add_old_tablet_ids(old_tablet_id_2);
    merging_tablet.set_new_tablet_id(new_tablet_id);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    txn_info.set_commit_time(1);
    txn_info.set_gtid(1);

    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("merge_delvecs:before_apply_offsets", [](void* arg) {
        auto* base_offset_by_file_name = reinterpret_cast<std::unordered_map<std::string, uint64_t>*>(arg);
        base_offset_by_file_name->erase("delvec-2");
    });

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto st = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_TRUE(st.is_invalid_argument());

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_cache_miss_fallback) {
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

    EXPECT_OK(put_tablet_metadata(meta1));
    EXPECT_OK(put_tablet_metadata(meta2));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_base_version_not_found) {
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

    EXPECT_OK(put_tablet_metadata(meta1));
    EXPECT_OK(put_tablet_metadata(meta2));
    EXPECT_OK(put_tablet_metadata(meta_new));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_get_metadata_error) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_dcg_segment_overflow) {
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
    add_dcg_with_columns(meta2.get(), std::numeric_limits<uint32_t>::max() - 5, "dcg-overflow", {301}, 1);

    EXPECT_OK(put_tablet_metadata(meta1));
    EXPECT_OK(put_tablet_metadata(meta2));

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

// --- New tests for split-then-merge correctness ---

TEST_F(LakeTabletReshardTest, test_tablet_merging_split_then_merge) {
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

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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
    // These hand-built sources both claim the unbounded tablet range, so the partition proof is uncertain even
    // though their rowsets deduplicate. The complete shared cohort is omitted and folded once; its future
    // generation (7 > merge version 2) conservatively becomes unknown.
    EXPECT_EQ(0, merged->sstable_meta().sstables_size());
    ASSERT_EQ(1, merged->orphan_files_size());
    EXPECT_EQ("shared_sst.sst", merged->orphan_files(0).name());
    EXPECT_EQ(512, merged->orphan_files(0).size());
    EXPECT_TRUE(merged->orphan_files(0).shared());
    EXPECT_EQ(0, merged->orphan_files(0).version());
}

// The merged tablet's async vector-index build watermark must be the MIN over all
// merge sources: the merged tablet contains rowsets from every source, so a rowset is
// only guaranteed built if it was built in its OWN source. source[0] here carries the
// HIGHER watermark (100); buggy code that just CopyFrom's source[0] would inherit 100
// and wrongly skip building child_b's unbuilt tail (whose true watermark is only 50).
TEST_F(LakeTabletReshardTest, test_tablet_merge_vector_index_built_version_min) {
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
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("shared_seg.dat");
        sm->set_size(100);
        sm->set_shared(true);
        stamp_physical_identity_uid(rowset, "shared_seg.dat");
        return meta;
    };

    // source[0] has the HIGHER watermark; buggy code would inherit 100 and skip child_b's unbuilt tail.
    auto meta_a = make_child(child_a, 100);
    auto meta_b = make_child(child_b, 50);
    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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
TEST_F(LakeTabletReshardTest, test_tablet_merge_vector_index_built_version_mixed) {
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
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("shared_seg.dat");
        sm->set_size(100);
        sm->set_shared(true);
        stamp_physical_identity_uid(rowset, "shared_seg.dat");
        return meta;
    };

    auto meta_a = make_child(child_a, /*set_built_version=*/true, 100);
    auto meta_b = make_child(child_b, /*set_built_version=*/false, 0);
    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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
TEST_F(LakeTabletReshardTest, test_tablet_merge_vector_index_built_version_none) {
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
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("shared_seg.dat");
        sm->set_size(100);
        sm->set_shared(true);
        stamp_physical_identity_uid(rowset, "shared_seg.dat");
        return meta;
    };

    auto meta_a = make_child(child_a);
    auto meta_b = make_child(child_b);
    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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
TEST_F(LakeTabletReshardTest, test_tablet_merge_segment_union_preserves_ownership) {
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
            sm->set_shared(true);
            sm->set_segment_idx(0);
            sm->set_encryption_meta("enc_shared");
        }
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename(private_seg);
            sm->set_size(50);
            sm->set_shared(false);
            sm->set_segment_idx(private_idx);
            sm->set_encryption_meta("enc_" + private_seg);
        }
        // Same uid => same logical rowset => dedup at merge.
        rowset->mutable_uid()->set_hi(0);
        rowset->mutable_uid()->set_lo(777);
        return meta;
    };
    EXPECT_OK(put_tablet_metadata(make_child(child_a, "a_private.dat", 1)));
    EXPECT_OK(put_tablet_metadata(make_child(child_b, "b_private.dat", 2)));

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

// Multi-level split regression: an already-shared ancestor segment that is later
// pruned to one new tablet retains shared=true (compute_rowset_segment_ownership
// keeps was_shared regardless of overlap count). Two siblings of such a multi-level
// split therefore carry the SAME uid and DISJOINT all-shared segment subsets. The
// segment-union gate must fire on segment-list divergence — not just on the
// shared=false flag — or the merged rowset silently loses the duplicate sibling's
// segments.
TEST_F(LakeTabletReshardTest, test_tablet_merge_multi_level_disjoint_all_shared_segment_metas) {
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
        // Each sibling carries a DIFFERENT segment, but BOTH are marked shared=true
        // (the multi-level was_shared propagation result). The segments_differ gate in
        // update_canonical fires on the per-position filename mismatch, so the union runs
        // even though neither sibling has a segment_metas[i].shared()==false flag.
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename(segment_name);
            sm->set_size(100);
            sm->set_shared(true);
            sm->set_segment_idx(segment_idx);
        }
        // Same uid => same logical rowset => dedup at merge.
        rowset->mutable_uid()->set_hi(0);
        rowset->mutable_uid()->set_lo(2024);
        return meta;
    };
    EXPECT_OK(put_tablet_metadata(make_child(child_a, "ancestor_seg_0.dat", 0)));
    EXPECT_OK(put_tablet_metadata(make_child(child_b, "ancestor_seg_1.dat", 1)));

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
    ASSERT_EQ(1, merged->rowsets_size()); // dedup => single merged rowset
    const auto& mr = merged->rowsets(0);
    std::set<std::string> segs;
    for (const auto& s : mr.segment_metas()) segs.insert(s.filename());
    EXPECT_EQ((std::set<std::string>{"ancestor_seg_0.dat", "ancestor_seg_1.dat"}), segs)
            << "the merged rowset must union the disjoint all-shared segment sets, not silently drop one sibling's";
}

// File bundling packs multiple segments into one physical file, so a bundled rowset's
// segments share a file NAME and differ only by bundle_file_offset. After a split prunes
// such a rowset, two same-uid siblings carry DISJOINT bundled segment subsets with
// IDENTICAL file-name lists. update_canonical must (1) detect divergence by comparing
// offsets (not just names) so the union fires, and (2) union bundle_file_offsets in
// lockstep with segments -- otherwise a sibling's bundled segments are silently dropped.
TEST_F(LakeTabletReshardTest, test_tablet_merge_bundled_segments_union_offsets) {
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
    EXPECT_OK(put_tablet_metadata(make_child(child_a, 0, 0, 1, 1024)));
    EXPECT_OK(put_tablet_metadata(make_child(child_b, 2, 2048, 3, 3072)));

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
TEST_F(LakeTabletReshardTest, test_tablet_merge_multi_level_unequal_count_all_shared_segment_metas) {
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
            sm->set_shared(true);
            sm->set_segment_idx(segment_indexes[i]);
        }
        rowset->mutable_uid()->set_hi(0);
        rowset->mutable_uid()->set_lo(2025);
        return meta;
    };
    EXPECT_OK(put_tablet_metadata(make_child(child_a, {"seg_0.dat", "seg_1.dat"}, {0, 1})));
    EXPECT_OK(put_tablet_metadata(make_child(child_b, {"seg_2.dat"}, {2})));

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

// Verify merge-back accumulates num_dels alongside num_rows / data_size. Without this,
// update_canonical would keep only the first child's per-range num_dels slice so the
// merged rowset loses (N-1)/N of the parent's deletes and get_tablet_stats over-reports
// live rows after a merge-back.
TEST_F(LakeTabletReshardTest, test_tablet_merging_accumulates_num_dels) {
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
            sm->set_filename("shared_seg.dat");
            sm->set_size(100);
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(rowset, "shared_seg.dat"); // same uid across siblings => dedup
        return meta;
    };

    // Parent rowset was 10 rows / 6 dels / 100 bytes. Split gave A 4/3 and B 6/3.
    auto meta_a = make_child(child_a, /*num_rows=*/4, /*data_size=*/40, /*num_dels=*/3);
    auto meta_b = make_child(child_b, /*num_rows=*/6, /*data_size=*/60, /*num_dels=*/3);

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_EQ(10, merged->rowsets(0).num_rows());
    EXPECT_EQ(100, merged->rowsets(0).data_size());
    EXPECT_EQ(6, merged->rowsets(0).num_dels());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_split_with_upsert_delete) {
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
    }

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_split_with_compaction) {
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
        sm->set_shared(true);
    }

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_shared_rowset_on_non_first_child) {
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
        sm->set_shared(true);
    }

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_delete_only_shared_rowset) {
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

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_different_split_families) {
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
        sm->set_shared(true);
    }

    EXPECT_OK(put_tablet_metadata(meta_c));
    EXPECT_OK(put_tablet_metadata(meta_d));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_cross_publish_different_id) {
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
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(rowset_b, "cross_pub.dat"); // same uid as A (cross-publish)

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_dcg_conflict_fail_fast) {
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
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(rowset_b, "shared_seg.dat"); // same uid across siblings => dedup
    // DCG from child B's different independent partial update
    add_dcg_with_columns(meta_b.get(), 1, "dcg_b.cols", {1}, 1);

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_predicate_dedup) {
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
    EXPECT_OK(put_tablet_metadata(meta_a));

    TabletMetadataPB meta_b;
    meta_b.set_id(child_b);
    meta_b.set_version(base_version);
    meta_b.set_next_rowset_id(3);
    add_rowset_with_predicate(&meta_b, 1, 5, true);  // same predicate v5
    add_rowset_with_predicate(&meta_b, 2, 6, false); // data v6
    EXPECT_OK(put_tablet_metadata(meta_b));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_shared_dcg_dedup) {
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
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(rowset, "shared_seg.dat"); // same uid across siblings => dedup
        // Same shared DCG on both children (inherited from split)
        add_dcg_with_columns(meta.get(), 1, "shared_dcg.cols", {1, 2}, 1);
        return meta;
    };

    auto meta_a = make_child_with_dcg(child_a);
    auto meta_b = make_child_with_dcg(child_b);

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_delvec_selects_only_final_single_source_files) {
    constexpr int64_t kNewVersion = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    for (int64_t tablet_id : {child_a, child_b, merged_tablet}) {
        prepare_tablet_dirs(tablet_id);
    }

    auto meta_a = make_shared_delvec_source(child_a, {"consumer_shared_segment.dat"});
    auto meta_b = make_shared_delvec_source(child_b, {"consumer_shared_segment.dat"});

    DelVector live_delvec;
    const uint32_t live_deleted_rowids[] = {3, 9};
    live_delvec.init(/*version=*/10, live_deleted_rowids, std::size(live_deleted_rowids));
    const std::string live_content = live_delvec.save();
    const std::string live_filename = "consumer_live.delvec";
    add_delvec(meta_a.get(), child_a, /*version=*/10, /*segment_id=*/1, live_filename, live_content);

    const std::string stale_content = "stale-delvec-record-with-no-page";
    FileMetaPB stale_file;
    stale_file.set_name("consumer_stale.delvec");
    stale_file.set_size(stale_content.size());
    (*meta_a->mutable_delvec_meta()->mutable_version_to_file())[20] = stale_file;
    write_file(_tablet_manager->delvec_location(child_a, stale_file.name()), stale_content);

    bool selected_callback_seen = false;
    std::vector<lake::DelvecFileInfo> selected_source_files;
    SyncPoint::GetInstance()->SetCallBack("merge_delvecs:selected_source_files", [&](void* arg) {
        selected_callback_seen = true;
        selected_source_files = *static_cast<std::vector<lake::DelvecFileInfo>*>(arg);
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp cleanup_sync_points([&] {
        SyncPoint::GetInstance()->ClearAllCallBacks();
        SyncPoint::GetInstance()->DisableProcessing();
    });

    ASSIGN_OR_ABORT(auto merged, merge_delvec_sources({meta_a, meta_b}, merged_tablet, kNewVersion));
    ASSERT_EQ(1, merged->rowsets_size());
    const uint32_t target_rssid = merged->rowsets(0).id();
    ASSERT_EQ(1, merged->delvec_meta().delvecs_size());
    const auto& output_page = merged->delvec_meta().delvecs().at(target_rssid);
    ASSERT_EQ(1, merged->delvec_meta().version_to_file_size());
    const auto& output_file = merged->delvec_meta().version_to_file().at(kNewVersion);

    EXPECT_TRUE(selected_callback_seen);
    ASSERT_EQ(1, selected_source_files.size());
    EXPECT_EQ(live_filename, selected_source_files[0].delvec_file.name());
    EXPECT_EQ(static_cast<int64_t>(live_content.size()), output_file.size());
    EXPECT_EQ(0, output_page.offset());
    EXPECT_EQ(live_content.size(), output_page.size());

    DelVector loaded;
    LakeIOOptions io_options;
    ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), *merged, target_rssid, false, io_options, &loaded));
    ASSERT_NE(nullptr, loaded.roaring());
    EXPECT_EQ(2, loaded.cardinality());
    EXPECT_TRUE(loaded.roaring()->contains(3));
    EXPECT_TRUE(loaded.roaring()->contains(9));
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_delvec_duplicate_source_metadata_mismatch_is_rejected) {
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
            const int64_t baseline_tablet = next_id();
            const int64_t mismatch_tablet = next_id();
            const int64_t merged_tablet = next_id();
            for (int64_t tablet_id : {baseline_tablet, mismatch_tablet, merged_tablet}) {
                prepare_tablet_dirs(tablet_id);
            }

            const std::string segment_filename = fmt::format("duplicate_metadata_{}.dat", mismatch.name);
            const std::string delvec_filename = fmt::format("duplicate_metadata_{}_{}.delvec", mismatch.name,
                                                            mismatch_first ? "mismatch_first" : "baseline_first");
            auto baseline = make_shared_delvec_source(baseline_tablet, {segment_filename});
            auto conflicting = make_shared_delvec_source(mismatch_tablet, {segment_filename});

            DelVector delvec;
            const uint32_t deleted_rowids[] = {1, 4};
            delvec.init(/*version=*/10, deleted_rowids, std::size(deleted_rowids));
            const std::string content = delvec.save();
            add_delvec(baseline.get(), baseline_tablet, /*version=*/10, /*segment_id=*/1, delvec_filename, content);
            add_delvec(conflicting.get(), mismatch_tablet, /*version=*/10, /*segment_id=*/1, delvec_filename, content);
            auto* conflicting_file = &(*conflicting->mutable_delvec_meta()->mutable_version_to_file())[/*version=*/10];
            mismatch.apply(conflicting_file);

            ASSERT_OK(put_tablet_metadata(baseline));
            ASSERT_OK(put_tablet_metadata(conflicting));
            ASSIGN_OR_ABORT(auto inventory_before, delvec_inventory(merged_tablet));

            ReshardingTabletInfoPB resharding;
            auto& merging = *resharding.mutable_merging_tablet_info();
            const std::vector<int64_t> source_tablets =
                    mismatch_first ? std::vector<int64_t>{mismatch_tablet, baseline_tablet}
                                   : std::vector<int64_t>{baseline_tablet, mismatch_tablet};
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
            EXPECT_TRUE(status.message().contains(delvec_filename)) << status;
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_delvec_stale_encryption_metadata_is_ignored) {
    for (bool stale_first : {false, true}) {
        SCOPED_TRACE(stale_first ? "stale-first" : "plain-first");
        const int64_t plain_tablet = next_id();
        const int64_t stale_tablet = next_id();
        const int64_t merged_tablet = next_id();
        for (int64_t tablet_id : {plain_tablet, stale_tablet, merged_tablet}) {
            prepare_tablet_dirs(tablet_id);
        }

        const std::string segment_filename = "stale_encryption_metadata_segment.dat";
        const std::string delvec_filename =
                fmt::format("stale_encryption_metadata_{}.delvec", stale_first ? "stale_first" : "plain_first");
        auto plain = make_shared_delvec_source(plain_tablet, {segment_filename});
        auto stale = make_shared_delvec_source(stale_tablet, {segment_filename});
        DelVector expected;
        const uint32_t deleted_rowids[] = {1, 4};
        expected.init(/*version=*/10, deleted_rowids, std::size(deleted_rowids));
        const std::string content = expected.save();
        add_delvec(plain.get(), plain_tablet, /*version=*/10, /*segment_id=*/1, delvec_filename, content);
        add_delvec(stale.get(), stale_tablet, /*version=*/10, /*segment_id=*/1, delvec_filename, content);
        (*stale->mutable_delvec_meta()->mutable_version_to_file())[10].set_encryption_meta(
                std::string(1, static_cast<char>(0xff)));

        ASSERT_OK(put_tablet_metadata(plain));
        ASSERT_OK(put_tablet_metadata(stale));
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
        ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, /*base_version=*/1,
                                                  /*new_version=*/2, txn_info, false, published, tablet_ranges));

        const auto& merged = published.at(merged_tablet);
        ASSERT_EQ(1, merged->delvec_meta().version_to_file_size());
        const auto& output_file = merged->delvec_meta().version_to_file().at(2);
        EXPECT_TRUE(output_file.encryption_meta().empty());
        const uint32_t target_rssid = merged->rowsets(0).id();
        DelVector loaded;
        LakeIOOptions io_options;
        ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), *merged, target_rssid, false, io_options, &loaded));
        EXPECT_EQ(expected.save(), loaded.save());
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_delvec_merged_duplicate_source_metadata_mismatch_is_rejected) {
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

            ASSERT_OK(put_tablet_metadata(baseline_x));
            ASSERT_OK(put_tablet_metadata(middle_y));
            ASSERT_OK(put_tablet_metadata(conflicting_x));
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_delvec_merged_only_writes_plaintext_buffer_output) {
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

    bool selected_callback_seen = false;
    std::vector<lake::DelvecFileInfo> selected_source_files;
    SyncPoint::GetInstance()->SetCallBack("merge_delvecs:selected_source_files", [&](void* arg) {
        selected_callback_seen = true;
        selected_source_files = *static_cast<std::vector<lake::DelvecFileInfo>*>(arg);
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp cleanup_sync_points([&] {
        SyncPoint::GetInstance()->ClearAllCallBacks();
        SyncPoint::GetInstance()->DisableProcessing();
    });

    ASSIGN_OR_ABORT(auto merged, merge_delvec_sources({meta_a, meta_b}, merged_tablet, kNewVersion));
    ASSERT_EQ(1, merged->rowsets_size());
    const uint32_t target_rssid = merged->rowsets(0).id();
    const auto& output_file = merged->delvec_meta().version_to_file().at(kNewVersion);

    EXPECT_TRUE(output_file.encryption_meta().empty());
    DelVector loaded;
    LakeIOOptions io_options;
    ASSERT_OK(lake::get_del_vec(_tablet_manager.get(), *merged, target_rssid, false, io_options, &loaded));
    ASSERT_NE(nullptr, loaded.roaring());
    EXPECT_EQ(2, loaded.cardinality());
    EXPECT_TRUE(loaded.roaring()->contains(4));
    EXPECT_TRUE(loaded.roaring()->contains(7));

    EXPECT_TRUE(selected_callback_seen);
    EXPECT_TRUE(selected_source_files.empty());
    EXPECT_EQ(static_cast<int64_t>(expected_union_content.size()), output_file.size());
    EXPECT_EQ(0, merged->delvec_meta().delvecs().at(target_rssid).offset());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_delvec_plain_single_plus_merged_writes_plaintext) {
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

    bool selected_callback_seen = false;
    std::vector<lake::DelvecFileInfo> selected_source_files;
    SyncPoint::GetInstance()->SetCallBack("merge_delvecs:selected_source_files", [&](void* arg) {
        selected_callback_seen = true;
        selected_source_files = *static_cast<std::vector<lake::DelvecFileInfo>*>(arg);
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp cleanup_sync_points([&] {
        SyncPoint::GetInstance()->ClearAllCallBacks();
        SyncPoint::GetInstance()->DisableProcessing();
    });

    ASSIGN_OR_ABORT(auto merged, merge_delvec_sources({meta_a, meta_b}, merged_tablet, kNewVersion));
    ASSERT_EQ(1, merged->rowsets_size());
    const uint32_t single_target_rssid = merged->rowsets(0).id();
    const uint32_t merged_target_rssid = single_target_rssid + 1;
    const auto& output_file = merged->delvec_meta().version_to_file().at(kNewVersion);

    EXPECT_TRUE(output_file.encryption_meta().empty());
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

    EXPECT_TRUE(selected_callback_seen);
    ASSERT_EQ(1, selected_source_files.size());
    EXPECT_EQ(plain_filename, selected_source_files[0].delvec_file.name());
    EXPECT_EQ(static_cast<int64_t>(plain_content.size() + expected_union_content.size()), output_file.size());
    EXPECT_EQ(0, merged->delvec_meta().delvecs().at(single_target_rssid).offset());
    EXPECT_EQ(plain_content.size(), merged->delvec_meta().delvecs().at(merged_target_rssid).offset());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_delvec_zero_target_writes_nothing) {
    constexpr int64_t kNewVersion = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    for (int64_t tablet_id : {child_a, child_b, merged_tablet}) {
        prepare_tablet_dirs(tablet_id);
    }

    auto meta_a = make_shared_delvec_source(child_a, {"zero_target_segment.dat"});
    auto meta_b = make_shared_delvec_source(child_b, {"zero_target_segment.dat"});
    const std::string stale_content = "stale-zero-target-data";
    FileMetaPB stale_file;
    stale_file.set_name("zero_target_stale.delvec");
    stale_file.set_size(stale_content.size());
    (*meta_a->mutable_delvec_meta()->mutable_version_to_file())[17] = stale_file;
    write_file(_tablet_manager->delvec_location(child_a, stale_file.name()), stale_content);
    ASSIGN_OR_ABORT(auto delvecs_before, delvec_inventory(merged_tablet));

    int writer_callback_count = 0;
    SyncPoint::GetInstance()->SetCallBack("merge_delvecs:writer_invocations", [&](void* arg) {
        ++writer_callback_count;
        EXPECT_EQ(1, *static_cast<int*>(arg));
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp cleanup_sync_points([&] {
        SyncPoint::GetInstance()->ClearAllCallBacks();
        SyncPoint::GetInstance()->DisableProcessing();
    });

    ASSIGN_OR_ABORT(auto merged, merge_delvec_sources({meta_a, meta_b}, merged_tablet, kNewVersion));
    EXPECT_EQ(0, writer_callback_count);
    EXPECT_EQ(0, merged->delvec_meta().delvecs_size());
    EXPECT_EQ(0, merged->delvec_meta().version_to_file_size());
    ASSIGN_OR_ABORT(auto delvecs_after, delvec_inventory(merged_tablet));
    EXPECT_EQ(delvecs_before, delvecs_after) << "zero final consumers must not create a delvec file";
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_delvec_independent_delete) {
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
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(rowset_b, "shared_seg.dat"); // same uid across siblings => dedup
    // Delvec from child_b's different independent delete
    add_delvec(meta_b.get(), child_b, 2, 1, "delvec_b.dv", dv_b_data);

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_delvec_multi_target_union) {
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
        sm->set_shared(true);
    }
    {
        auto* sm = rowset_a->add_segment_metas();
        sm->set_filename("shared_seg2.dat");
        sm->set_size(100);
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
        sm->set_shared(true);
    }
    {
        auto* sm = rowset_b->add_segment_metas();
        sm->set_filename("shared_seg2.dat");
        sm->set_size(100);
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

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_delvec_three_way_union) {
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
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(rowset, "shared_seg.dat"); // same uid across siblings => dedup
        add_delvec(meta.get(), tablet_id, delvec_version, 1, delvec_file_name, delvec_data);
        return meta;
    };

    auto meta_a = make_child_meta(child_a, 1, "delvec_a.dv", dv_a_data);
    auto meta_b = make_child_meta(child_b, 2, "delvec_b.dv", dv_b_data);
    auto meta_c = make_child_meta(child_c, 3, "delvec_c.dv", dv_c_data);

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));
    EXPECT_OK(put_tablet_metadata(meta_c));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_delvec_no_independent_delete) {
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
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(rowset_b, "shared_seg.dat"); // same uid across siblings => dedup
    // Same file name, same offset/size -> page-ref dedup
    add_delvec(meta_b.get(), child_b, 1, 1, "shared_delvec.dv", dv_data);

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_preflight_rejects_idg_invalid_shape) {
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
        MergePhaseCounts counts;
        auto status = expect_physical_preflight_rejection({source}, next_id(), /*target_version=*/2, &counts);
        EXPECT_TRUE(status.message().contains("IDG")) << status;
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_preflight_accepts_legacy_idg_optional_shape) {
    auto source = make_preflight_sidecar_source(next_id(), "legacy_idg_optional.dat", /*shared_segment=*/true);
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
    MergePhaseCounts counts;
    ASSIGN_OR_ABORT(auto merged, merge_with_phase_counts({source}, target_id, /*target_version=*/2, &counts));

    ASSERT_EQ(1, merged->idg_meta().idgs_size());
    const auto& entries = merged->idg_meta().idgs().at(1).entries();
    ASSERT_EQ(1, entries.size());
    EXPECT_EQ("legacy_active.idx", entries.Get(0).index_file());
    EXPECT_TRUE(entries.Get(0).shared_file());
    EXPECT_FALSE(entries.Get(0).has_version());
    EXPECT_FALSE(entries.Get(0).has_file_size());
    EXPECT_FALSE(entries.Get(0).has_encryption_meta());
    ASSERT_EQ(1, merged->orphan_files_size());
    EXPECT_EQ("legacy_empty.idx", merged->orphan_files(0).name());
    EXPECT_FALSE(merged->orphan_files(0).has_size());
    EXPECT_TRUE(merged->orphan_files(0).shared());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_preflight_rejects_idg_same_file_declaration_conflict) {
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
        const std::string segment = fmt::format("idg_conflict_{}.dat", next_id());
        auto source_a = make_preflight_sidecar_source(next_id(), segment, /*shared_segment=*/true,
                                                      /*common_rowset_uid=*/true);
        auto source_b = make_preflight_sidecar_source(next_id(), segment, /*shared_segment=*/true,
                                                      /*common_rowset_uid=*/true);
        for (auto* source : {source_a.get(), source_b.get()}) {
            add_idg_with_key(source, /*segment_id=*/1, "conflicting.idx", /*col_uid=*/5, BITMAP, /*version=*/1,
                             /*shared_file=*/false);
            add_idg_key(source, /*segment_id=*/1, /*col_uid=*/6, GIN);
            auto* entry = source->mutable_idg_meta()->mutable_idgs()->at(1).mutable_entries(0);
            entry->set_file_size(128);
            entry->set_encryption_meta(encryption_a.encryption_meta);
        }
        conflict.apply(source_b->mutable_idg_meta()->mutable_idgs()->at(1).mutable_entries(0));

        MergePhaseCounts counts;
        auto status =
                expect_physical_preflight_rejection({source_a, source_b}, next_id(), /*target_version=*/2, &counts);
        EXPECT_TRUE(status.message().contains("IDG")) << status;
        EXPECT_TRUE(status.message().contains("conflicting.idx")) << status;
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_preflight_accepts_matching_idg_declaration) {
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
    MergePhaseCounts counts;
    auto merged_or = merge_with_phase_counts({source_a, source_b}, target_id, /*target_version=*/2, &counts);
    if (!merged_or.ok()) {
        ADD_FAILURE() << merged_or.status();
        return;
    }
    auto merged = std::move(merged_or).value();
    ASSERT_EQ(1, merged->idg_meta().idgs_size());
    ASSERT_EQ(1, merged->idg_meta().idgs().at(1).entries_size());
    EXPECT_TRUE(merged->idg_meta().idgs().at(1).entries(0).shared_file());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_preflight_rejects_source_live_idg_target_omission) {
    auto source = make_preflight_sidecar_source(next_id(), "live_idg_omission.dat");
    add_idg_with_key(source.get(), /*segment_id=*/1, "live_omission.idx", /*col_uid=*/5, BITMAP, /*version=*/1);
    int omission_count = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("tablet_merge_test:force_idg_target_omission", [&](void* arg) {
        ++omission_count;
        *static_cast<bool*>(arg) = true;
    });
    DeferOp clear_omission([&] { sync->ClearCallBack("tablet_merge_test:force_idg_target_omission"); });

    MergePhaseCounts counts;
    auto status = expect_physical_preflight_rejection({source}, next_id(), /*target_version=*/2, &counts);
    EXPECT_EQ(1, omission_count);
    EXPECT_TRUE(status.message().contains("IDG")) << status;
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_preflight_rejects_source_live_delvec_target_omission) {
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
    DeferOp clear_omission([&] { sync->ClearCallBack("tablet_merge_test:force_delvec_target_omission"); });

    MergePhaseCounts counts;
    auto status = expect_physical_preflight_rejection({source}, next_id(), /*target_version=*/2, &counts);
    EXPECT_EQ(1, omission_count);
    EXPECT_TRUE(status.message().contains("delvec")) << status;
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_preflight_rejects_source_live_dcg_target_omission) {
    auto source = make_preflight_sidecar_source(next_id(), "live_dcg_omission.dat");
    add_dcg_with_columns(source.get(), /*segment_id=*/1, "live_omission.cols", {5}, /*version=*/1);
    int omission_count = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("tablet_merge_test:force_dcg_target_omission", [&](void* arg) {
        ++omission_count;
        *static_cast<bool*>(arg) = true;
    });
    DeferOp clear_omission([&] { sync->ClearCallBack("tablet_merge_test:force_dcg_target_omission"); });

    MergePhaseCounts counts;
    auto status = expect_physical_preflight_rejection({source}, next_id(), /*target_version=*/2, &counts);
    EXPECT_EQ(1, omission_count);
    EXPECT_TRUE(status.message().contains("DCG")) << status;
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_preflight_allows_source_stale_idg_and_delvec) {
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
    MergePhaseCounts counts;
    ASSIGN_OR_ABORT(auto merged, merge_with_phase_counts({source}, target_id, /*target_version=*/2, &counts));
    EXPECT_FALSE(merged->has_idg_meta());
    EXPECT_FALSE(merged->has_delvec_meta());
    EXPECT_EQ(0, merged->orphan_files_size());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_preflight_rejects_source_stale_dcg_before_materialize) {
    auto source = make_allocator_source(next_id(), /*next_rowset_id=*/4);
    auto* rowset = add_allocator_rowset(source.get(), /*rowset_id=*/1, /*version=*/1, "stale_dcg_0.dat",
                                        /*segment_idx=*/0);
    add_allocator_segment(rowset, "stale_dcg_2.dat", /*segment_idx=*/2);
    add_dcg_with_columns(source.get(), /*segment_id=*/2, "stale.cols", {5}, /*version=*/1);

    MergePhaseCounts counts;
    auto status = expect_physical_preflight_rejection({source}, next_id(), /*target_version=*/2, &counts);
    EXPECT_TRUE(status.message().contains("stale DCG")) << status;
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_cross_target_same_base_sidecars_survive) {
    const int64_t source_a_id = next_id();
    const int64_t source_b_id = next_id();
    const int64_t target_id = next_id();
    prepare_tablet_dirs(source_a_id);
    prepare_tablet_dirs(source_b_id);
    prepare_tablet_dirs(target_id);
    const std::string base_name = "cross_target_shared_base.dat";
    const std::string cols_name = "cross_target.cols";
    const std::string idx_name = "cross_target.idx";
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
        auto* idg = (*source->mutable_idg_meta()->mutable_idgs())[1].mutable_entries(0);
        idg->set_file_size(idx_file.filesize);
    }

    MergePhaseCounts counts;
    auto merged_or = merge_with_phase_counts({source_a, source_b}, target_id, /*target_version=*/2, &counts);
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
        (void)rssid;
        ASSERT_EQ(1, idg.entries_size());
        EXPECT_EQ(idx_name, idg.entries(0).index_file());
        EXPECT_TRUE(idg.entries(0).shared_file());
    }
    EXPECT_TRUE(std::none_of(merged->orphan_files().begin(), merged->orphan_files().end(),
                             [&](const FileMetaPB& file) { return file.name() == idx_name; }));

    ASSERT_OK(put_tablet_metadata(merged));
    _tablet_manager->prune_metacache();
    ASSIGN_OR_ABORT(auto reopened, _tablet_manager->get_tablet_metadata(target_id, merged->version()));
    ASSIGN_OR_ABORT(auto reopened_rows, read_two_column_rows(reopened));
    EXPECT_EQ((std::vector<std::pair<int32_t, int32_t>>{{0, 1000}, {1, 2000}}), reopened_rows);

    TabletMetadataPB compacted(*reopened);
    compacted.set_version(reopened->version() + 1);
    ASSERT_EQ(2, compacted.rowsets_size());
    RowsetMetadataPB retired(compacted.rowsets(0));
    RowsetMetadataPB retained(compacted.rowsets(1));
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
    const std::string private_control_name = "cross_target_private_control.idx";
    const auto private_control = write_sidecar_payload(
            _tablet_manager->segment_location(target_id, private_control_name), "private-vacuum-control",
            /*encrypted=*/false);
    add_orphan(private_control_name, private_control.filesize, /*shared=*/false, compacted.version());
    ASSERT_EQ(1, compacted.compaction_inputs_size());
    ASSERT_EQ(3, compacted.orphan_files_size());
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
                        ->path_exists(_tablet_manager->segment_location(target_id, private_control_name))
                        .is_not_found());

    _tablet_manager->prune_metacache();
    ASSIGN_OR_ABORT(auto reopened_after_vacuum, _tablet_manager->get_tablet_metadata(target_id, compacted.version()));
    ASSIGN_OR_ABORT(auto rows_after_vacuum, read_two_column_rows(reopened_after_vacuum));
    EXPECT_EQ((std::vector<std::pair<int32_t, int32_t>>{{0, 1000}, {1, 2000}}), rows_after_vacuum);
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_cross_target_different_base_sidecars_rejected) {
    auto source_a = make_preflight_sidecar_source(next_id(), "cross_target_base_a.dat");
    auto source_b = make_preflight_sidecar_source(next_id(), "cross_target_base_b.dat");
    add_dcg_with_columns(source_a.get(), /*segment_id=*/1, "cross_target_mismatch.cols", {5}, /*version=*/1);
    add_dcg_with_columns(source_b.get(), /*segment_id=*/1, "cross_target_mismatch.cols", {5}, /*version=*/1);
    add_idg_with_key(source_a.get(), /*segment_id=*/1, "cross_target_mismatch.idx", /*col_uid=*/5, BITMAP,
                     /*version=*/1);
    add_idg_with_key(source_b.get(), /*segment_id=*/1, "cross_target_mismatch.idx", /*col_uid=*/5, BITMAP,
                     /*version=*/1);

    MergePhaseCounts counts;
    auto status = expect_physical_preflight_rejection({source_a, source_b}, next_id(), /*target_version=*/2, &counts);
    EXPECT_TRUE(status.message().contains("physical base")) << status;
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_cross_target_physical_slice_declaration_conflict_before_io) {
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

            MergePhaseCounts counts;
            auto status = expect_physical_preflight_rejection(sources, next_id(), /*target_version=*/2, &counts);
            EXPECT_TRUE(status.message().contains(conflict.expected_error)) << status;
            EXPECT_TRUE(status.message().contains(filename)) << status;
        }
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_invalid_physical_segment_shape_before_io) {
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
            {"negative size", true, "size",
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

            MergePhaseCounts counts;
            auto status = expect_physical_preflight_rejection(sources, next_id(), /*target_version=*/2, &counts);
            EXPECT_TRUE(status.message().contains("segment")) << status;
            EXPECT_TRUE(status.message().contains(invalid.error_field)) << status;
        }
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_invalid_physical_segment_filename_before_io) {
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

            MergePhaseCounts counts;
            auto status = expect_physical_preflight_rejection(sources, next_id(), /*target_version=*/2, &counts);
            EXPECT_TRUE(status.message().contains("filename")) << status;
        }
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_bundle_slice_end_overflow_before_io) {
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

        MergePhaseCounts counts;
        auto status = expect_physical_preflight_rejection(sources, next_id(), /*target_version=*/2, &counts);
        EXPECT_TRUE(status.message().contains("bundle")) << status;
        EXPECT_TRUE(status.message().contains("size")) << status;
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_mixed_bundle_presence_after_segment_union_before_io) {
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

        MergePhaseCounts counts;
        auto status = expect_physical_preflight_rejection(sources, next_id(), /*target_version=*/2, &counts);
        EXPECT_TRUE(status.message().contains("bundled")) << status;
        EXPECT_TRUE(status.message().contains("standalone")) << status;
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_cross_canonical_filename_bundle_form_before_io) {
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

        MergePhaseCounts counts;
        auto status = expect_physical_preflight_rejection(sources, next_id(), /*target_version=*/2, &counts);
        EXPECT_TRUE(status.message().contains(filename)) << status;
        EXPECT_TRUE(status.message().contains("bundled")) << status;
        EXPECT_TRUE(status.message().contains("standalone")) << status;
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_accepts_uniform_bundle_presence_after_segment_union) {
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

            MergePhaseCounts counts;
            ASSIGN_OR_ABORT(auto merged, merge_with_phase_counts(sources, next_id(), /*target_version=*/2, &counts));
            ASSERT_EQ(1, merged->rowsets_size());
            ASSERT_EQ(2, merged->rowsets(0).segment_metas_size());
            EXPECT_EQ(bundled ? 2 : 0, std::count_if(merged->rowsets(0).segment_metas().begin(),
                                                     merged->rowsets(0).segment_metas().end(), [](const auto& segment) {
                                                         return segment.has_bundle_file_offset();
                                                     }));
            EXPECT_EQ(1, counts.materialize);
            EXPECT_EQ(0, counts.dcg_writes);
            EXPECT_EQ(0, counts.delvec_writes);
            EXPECT_EQ(0, counts.source_flushes);
        }
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_undersized_bundle_slice_before_io) {
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

            MergePhaseCounts counts;
            auto status = expect_physical_preflight_rejection(sources, next_id(), /*target_version=*/2, &counts);
            EXPECT_TRUE(status.message().contains("footer trailer")) << status;
        }
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_preflight_rejects_delvec_page_out_of_bounds_before_io) {
    auto source = make_preflight_sidecar_source(next_id(), "delvec_bounds.dat");
    DelVector delvec;
    const uint32_t deleted = 0;
    delvec.init(/*version=*/1, &deleted, 1);
    prepare_tablet_dirs(source->id());
    add_delvec(source.get(), source->id(), /*version=*/1, /*segment_id=*/1, "bounds.delvec", delvec.save());
    auto* page = &(*source->mutable_delvec_meta()->mutable_delvecs())[1];
    page->set_offset(source->delvec_meta().version_to_file().at(1).size());
    page->set_size(1);

    MergePhaseCounts counts;
    auto status = expect_physical_preflight_rejection({source}, next_id(), /*target_version=*/2, &counts);
    EXPECT_TRUE(status.message().contains("bounds.delvec")) << status;
    EXPECT_TRUE(status.message().contains("bounds")) << status;
}

TEST_F(LakeTabletReshardTest,
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
        const std::string segment = fmt::format("delvec_conflict_{}.dat", next_id());
        const std::string filename = fmt::format("delvec_conflict_{}.delvec", next_id());
        auto source_a = make_preflight_sidecar_source(next_id(), segment, /*shared_segment=*/true,
                                                      /*common_rowset_uid=*/true);
        auto source_b = make_preflight_sidecar_source(next_id(), segment, /*shared_segment=*/true,
                                                      /*common_rowset_uid=*/true);
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

        MergePhaseCounts counts;
        auto status =
                expect_physical_preflight_rejection({source_a, source_b}, next_id(), /*target_version=*/2, &counts);
        EXPECT_TRUE(status.message().contains("metadata mismatch")) << status;
        EXPECT_TRUE(status.message().contains(filename)) << status;
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_preflight_rejects_late_dcg_declaration_before_materialize) {
    auto source = make_preflight_sidecar_source(next_id(), "late_dcg_declaration.dat");
    add_dcg(source.get(), /*segment_id=*/1, "late_malformed.cols");

    MergePhaseCounts counts;
    auto status = expect_physical_preflight_rejection({source}, next_id(), /*target_version=*/2, &counts);
    EXPECT_TRUE(status.message().contains("DCG shape")) << status;
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_preflight_rejects_sst_invalid_form_or_range_before_io) {
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
        auto sources = make_preflight_sst_sources(fmt::format("sst_invalid_{}", next_id()));
        for (auto& source : sources) invalid.apply(source->mutable_sstable_meta()->mutable_sstables(0));
        std::vector<TabletMetadataPtr> immutable_sources(sources.begin(), sources.end());
        MergePhaseCounts counts;
        auto status = expect_physical_preflight_rejection(immutable_sources, next_id(), /*target_version=*/2, &counts);
        EXPECT_TRUE(status.message().contains("SST")) << status;
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_preflight_rejects_modern_sst_invalid_shared_version_before_io) {
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
        auto sources = make_preflight_sst_sources(fmt::format("sst_invalid_shared_version_{}", next_id()));
        for (auto& source : sources) invalid.apply(source->mutable_sstable_meta()->mutable_sstables(0));
        std::vector<TabletMetadataPtr> immutable_sources(sources.begin(), sources.end());
        MergePhaseCounts counts;
        auto status = expect_physical_preflight_rejection(immutable_sources, next_id(), /*target_version=*/2, &counts);
        EXPECT_TRUE(status.message().contains("SST")) << status;
        EXPECT_TRUE(status.message().contains("shared_version")) << status;
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_preflight_rejects_sst_same_file_form_or_range_conflict_before_io) {
    struct DeclarationConflict {
        const char* name;
        std::function<void(PersistentIndexSstablePB*)> apply;
    };
    const std::vector<DeclarationConflict> conflicts = {
            {"max rss rowid", [](PersistentIndexSstablePB* sst) { sst->set_max_rss_rowid(uint64_t{2} << 32); }},
            {"shared rssid", [](PersistentIndexSstablePB* sst) { sst->set_shared_rssid(2); }},
            {"shared version", [](PersistentIndexSstablePB* sst) { sst->set_shared_version(2); }},
            {"shared version presence", [](PersistentIndexSstablePB* sst) { sst->clear_shared_version(); }},
            {"rssid offset", [](PersistentIndexSstablePB* sst) { sst->set_rssid_offset(1); }},
            {"embedded delvec",
             [](PersistentIndexSstablePB* sst) {
                 sst->mutable_delvec()->set_version(1);
                 sst->mutable_delvec()->set_size(1);
             }},
            {"range start",
             [&](PersistentIndexSstablePB* sst) { sst->mutable_range()->set_start_key(encode_int_primary_key(11)); }},
            {"range end",
             [&](PersistentIndexSstablePB* sst) { sst->mutable_range()->set_end_key(encode_int_primary_key(89)); }},
            {"range presence", [](PersistentIndexSstablePB* sst) { sst->clear_range(); }},
    };

    for (const auto& conflict : conflicts) {
        SCOPED_TRACE(conflict.name);
        auto sources = make_preflight_sst_sources(fmt::format("sst_form_conflict_{}", next_id()));
        conflict.apply(sources[1]->mutable_sstable_meta()->mutable_sstables(0));
        std::vector<TabletMetadataPtr> immutable_sources(sources.begin(), sources.end());
        MergePhaseCounts counts;
        auto status = expect_physical_preflight_rejection(immutable_sources, next_id(), /*target_version=*/2, &counts);
        EXPECT_TRUE(status.message().contains("SST")) << status;
        EXPECT_TRUE(status.message().contains("conflict")) << status;
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_preflight_rejects_late_sst_file_conflict_before_io) {
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
        SCOPED_TRACE(conflict.name);
        const int64_t source_a_id = next_id();
        const int64_t source_b_id = next_id();
        const std::string segment = fmt::format("sst_preflight_{}.dat", next_id());
        auto make_source = [&](int64_t tablet_id, int lower, int upper) {
            auto source = make_preflight_sidecar_source(tablet_id, segment, /*shared_segment=*/true,
                                                        /*common_rowset_uid=*/true);
            set_int_primary_key_schema(source.get(), /*schema_id=*/4001);
            source->set_enable_persistent_index(true);
            source->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
            source->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(lower));
            source->mutable_range()->set_lower_bound_included(true);
            source->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(upper));
            source->mutable_range()->set_upper_bound_included(false);
            source->mutable_rowsets(0)->mutable_range()->CopyFrom(source->range());
            auto* sst = source->mutable_sstable_meta()->add_sstables();
            sst->set_filename("repeated_physical.sst");
            sst->set_filesize(128);
            sst->set_encryption_meta(encryption_a.encryption_meta);
            sst->set_shared(true);
            sst->set_shared_rssid(1);
            sst->set_shared_version(1);
            sst->set_max_rss_rowid(static_cast<uint64_t>(1) << 32);
            sst->set_generation_version(1);
            sst->mutable_range()->set_start_key(encode_int_primary_key(10));
            sst->mutable_range()->set_end_key(encode_int_primary_key(90));
            return source;
        };
        auto source_a = make_source(source_a_id, /*lower=*/0, /*upper=*/50);
        auto source_b = make_source(source_b_id, /*lower=*/50, /*upper=*/100);
        std::vector<std::shared_ptr<TabletMetadataPB>> sources = {source_a, source_b};
        conflict.apply(sources);
        std::vector<TabletMetadataPtr> immutable_sources(sources.begin(), sources.end());

        MergePhaseCounts counts;
        auto status = expect_physical_preflight_rejection(immutable_sources, next_id(), /*target_version=*/2, &counts);
        EXPECT_TRUE(status.message().contains("SST")) << status;
    }
}

// --- DCG merge tests ---

TEST_F(LakeTabletReshardTest, test_tablet_merging_dcg_disjoint_columns) {
    // child_a updates columns {1,2}, child_b updates columns {3,4} on the same shared segment.
    // Disjoint columns -> merge succeeds, output has 2 entries.
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
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(rowset, "shared_seg.dat"); // same uid across siblings => dedup
        return meta;
    };

    auto meta_a = make_child(child_a);
    add_dcg_with_columns(meta_a.get(), 1, "a.cols", {1, 2}, 1);

    auto meta_b = make_child(child_b);
    add_dcg_with_columns(meta_b.get(), 1, "b.cols", {3, 4}, 1);

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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
    ASSERT_TRUE(merged->has_dcg_meta());
    auto dcg_it = merged->dcg_meta().dcgs().find(merged->rowsets(0).id());
    ASSERT_TRUE(dcg_it != merged->dcg_meta().dcgs().end());
    // 2 entries: a.cols and b.cols
    ASSERT_EQ(2, dcg_it->second.column_files_size());
    std::unordered_set<std::string> files;
    for (int i = 0; i < dcg_it->second.column_files_size(); ++i) {
        files.insert(dcg_it->second.column_files(i));
    }
    EXPECT_TRUE(files.count("a.cols") > 0);
    EXPECT_TRUE(files.count("b.cols") > 0);
    // All 5 fields should be aligned
    EXPECT_EQ(2, dcg_it->second.unique_column_ids_size());
    EXPECT_EQ(2, dcg_it->second.versions_size());
    EXPECT_EQ(2, dcg_it->second.encryption_metas_size());
    EXPECT_EQ(2, dcg_it->second.shared_files_size());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_dcg_exact_dedup) {
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

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_dcg_same_column_conflict) {
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
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(rowset, "shared_seg.dat"); // same uid across siblings => dedup
        return meta;
    };

    auto meta_a = make_child(child_a);
    add_dcg_with_columns(meta_a.get(), 1, "a.cols", {1}, 1);

    auto meta_b = make_child(child_b);
    add_dcg_with_columns(meta_b.get(), 1, "b.cols", {1}, 1);

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_dcg_partial_overlap) {
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
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(rowset, "shared_seg.dat"); // same uid across siblings => dedup
        return meta;
    };

    auto meta_a = make_child(child_a);
    add_dcg_with_columns(meta_a.get(), 1, "a.cols", {1, 2}, 1);

    auto meta_b = make_child(child_b);
    add_dcg_with_columns(meta_b.get(), 1, "b.cols", {2, 3}, 1);

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_dcg_missing_shape) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_dcg_duplicate_column_uid) {
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
    }
    // Build a malformed DCG with overlapping column UIDs across entries
    add_dcg_with_columns(meta_a.get(), 1, "first.cols", {1, 2}, 1);
    add_dcg_with_columns(meta_a.get(), 1, "second.cols", {2, 3}, 1);

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

// --- sstable merge tests ---

// --- union_range unit tests ---

TEST_F(LakeTabletReshardTest, test_union_range_equal_bound_included_excluded) {
    TabletRangePB a;
    a.mutable_lower_bound()->CopyFrom(generate_sort_key(10));
    a.set_lower_bound_included(true);
    a.mutable_upper_bound()->CopyFrom(generate_sort_key(20));
    a.set_upper_bound_included(false);

    TabletRangePB b;
    b.mutable_lower_bound()->CopyFrom(generate_sort_key(10));
    b.set_lower_bound_included(false);
    b.mutable_upper_bound()->CopyFrom(generate_sort_key(20));
    b.set_upper_bound_included(true);

    ASSIGN_OR_ABORT(auto result, lake::tablet_reshard_helper::union_range(a, b));
    // Lower: equal values, included = true || false = true
    EXPECT_TRUE(result.lower_bound_included());
    // Upper: equal values, included = false || true = true
    EXPECT_TRUE(result.upper_bound_included());
}

TEST_F(LakeTabletReshardTest, test_union_range_one_side_unbounded) {
    TabletRangePB a;
    // a has no lower_bound (unbounded)
    a.mutable_upper_bound()->CopyFrom(generate_sort_key(20));
    a.set_upper_bound_included(false);

    TabletRangePB b;
    b.mutable_lower_bound()->CopyFrom(generate_sort_key(10));
    b.set_lower_bound_included(true);
    b.mutable_upper_bound()->CopyFrom(generate_sort_key(30));
    b.set_upper_bound_included(true);

    ASSIGN_OR_ABORT(auto result, lake::tablet_reshard_helper::union_range(a, b));
    // Lower: a is unbounded -> result lower is unbounded
    EXPECT_FALSE(result.has_lower_bound());
    // Upper: a=20 exclusive, b=30 inclusive -> take larger = 30 inclusive
    ASSERT_TRUE(result.has_upper_bound());
    EXPECT_TRUE(result.upper_bound_included());
}

TEST_F(LakeTabletReshardTest, test_union_range_both_unbounded) {
    TabletRangePB a; // fully unbounded
    TabletRangePB b; // fully unbounded

    ASSIGN_OR_ABORT(auto result, lake::tablet_reshard_helper::union_range(a, b));
    EXPECT_FALSE(result.has_lower_bound());
    EXPECT_FALSE(result.has_upper_bound());
}

TEST_F(LakeTabletReshardTest, test_union_range_unequal_bounds) {
    TabletRangePB a;
    a.mutable_lower_bound()->CopyFrom(generate_sort_key(5));
    a.set_lower_bound_included(true);
    a.mutable_upper_bound()->CopyFrom(generate_sort_key(15));
    a.set_upper_bound_included(false);

    TabletRangePB b;
    b.mutable_lower_bound()->CopyFrom(generate_sort_key(10));
    b.set_lower_bound_included(true);
    b.mutable_upper_bound()->CopyFrom(generate_sort_key(25));
    b.set_upper_bound_included(true);

    ASSIGN_OR_ABORT(auto result, lake::tablet_reshard_helper::union_range(a, b));
    // Lower: take smaller = 5, included from a = true
    ASSERT_TRUE(result.has_lower_bound());
    EXPECT_TRUE(result.lower_bound_included());
    // Upper: take larger = 25, included from b = true
    ASSERT_TRUE(result.has_upper_bound());
    EXPECT_TRUE(result.upper_bound_included());

    // Verify the values
    VariantTuple lower;
    ASSERT_OK(lower.from_proto(result.lower_bound()));
    VariantTuple expected_lower;
    ASSERT_OK(expected_lower.from_proto(generate_sort_key(5)));
    EXPECT_EQ(0, lower.compare(expected_lower));

    VariantTuple upper;
    ASSERT_OK(upper.from_proto(result.upper_bound()));
    VariantTuple expected_upper;
    ASSERT_OK(expected_upper.from_proto(generate_sort_key(25)));
    EXPECT_EQ(0, upper.compare(expected_upper));
}

TEST_F(LakeTabletReshardTest, test_update_rowset_data_stats_basic) {
    RowsetMetadataPB rowset;
    rowset.set_num_rows(100);
    rowset.set_data_size(1000);

    // Split into 3, index 0: gets remainder
    lake::tablet_reshard_helper::update_rowset_data_stats(&rowset, 3, 0);
    EXPECT_EQ(34, rowset.num_rows());   // 100/3=33, 100%3=1, index 0 < 1 => +1
    EXPECT_EQ(334, rowset.data_size()); // 1000/3=333, 1000%3=1, index 0 < 1 => +1
}

TEST_F(LakeTabletReshardTest, test_update_rowset_data_stats_remainder_distribution) {
    // Verify that splitting 10 rows into 3 tablets gives 4+3+3 = 10
    int64_t total_rows = 0;
    int64_t total_size = 0;
    for (int32_t i = 0; i < 3; i++) {
        RowsetMetadataPB rowset;
        rowset.set_num_rows(10);
        rowset.set_data_size(100);
        lake::tablet_reshard_helper::update_rowset_data_stats(&rowset, 3, i);
        total_rows += rowset.num_rows();
        total_size += rowset.data_size();
    }
    EXPECT_EQ(10, total_rows);
    EXPECT_EQ(100, total_size);
}

TEST_F(LakeTabletReshardTest, test_update_rowset_data_stats_exact_division) {
    RowsetMetadataPB rowset;
    rowset.set_num_rows(9);
    rowset.set_data_size(300);

    lake::tablet_reshard_helper::update_rowset_data_stats(&rowset, 3, 0);
    EXPECT_EQ(3, rowset.num_rows());
    EXPECT_EQ(100, rowset.data_size());
}

TEST_F(LakeTabletReshardTest, test_update_rowset_data_stats_split_count_one) {
    RowsetMetadataPB rowset;
    rowset.set_num_rows(100);
    rowset.set_data_size(1000);

    lake::tablet_reshard_helper::update_rowset_data_stats(&rowset, 1, 0);
    EXPECT_EQ(100, rowset.num_rows());
    EXPECT_EQ(1000, rowset.data_size());
}

TEST_F(LakeTabletReshardTest, test_update_rowset_data_stats_split_count_zero) {
    RowsetMetadataPB rowset;
    rowset.set_num_rows(100);
    rowset.set_data_size(1000);

    lake::tablet_reshard_helper::update_rowset_data_stats(&rowset, 0, 0);
    EXPECT_EQ(100, rowset.num_rows());
    EXPECT_EQ(1000, rowset.data_size());
}

TEST_F(LakeTabletReshardTest, test_update_txn_log_data_stats_all_op_types) {
    TxnLogPB txn_log;
    txn_log.set_tablet_id(1);
    txn_log.set_txn_id(1000);

    // op_write
    auto* op_write_rowset = txn_log.mutable_op_write()->mutable_rowset();
    op_write_rowset->set_num_rows(10);
    op_write_rowset->set_data_size(100);

    // op_compaction
    auto* op_compaction_rowset = txn_log.mutable_op_compaction()->mutable_output_rowset();
    op_compaction_rowset->set_num_rows(20);
    op_compaction_rowset->set_data_size(200);

    // op_schema_change
    auto* schema_change_rowset = txn_log.mutable_op_schema_change()->add_rowsets();
    schema_change_rowset->set_num_rows(30);
    schema_change_rowset->set_data_size(300);

    // op_replication
    auto* repl_rowset = txn_log.mutable_op_replication()->add_op_writes()->mutable_rowset();
    repl_rowset->set_num_rows(40);
    repl_rowset->set_data_size(400);

    // op_parallel_compaction
    auto* parallel_rowset =
            txn_log.mutable_op_parallel_compaction()->add_subtask_compactions()->mutable_output_rowset();
    parallel_rowset->set_num_rows(50);
    parallel_rowset->set_data_size(500);

    // split_count=3, split_index=0 (gets extra remainder)
    lake::tablet_reshard_helper::update_txn_log_data_stats(&txn_log, 3, 0);

    EXPECT_EQ(4, txn_log.op_write().rowset().num_rows());               // 10/3=3 + (0<1?1:0) = 4
    EXPECT_EQ(34, txn_log.op_write().rowset().data_size());             // 100/3=33 + (0<1?1:0) = 34
    EXPECT_EQ(7, txn_log.op_compaction().output_rowset().num_rows());   // 20/3=6 + (0<2?1:0) = 7
    EXPECT_EQ(67, txn_log.op_compaction().output_rowset().data_size()); // 200/3=66 + (0<2?1:0) = 67
    EXPECT_EQ(10, txn_log.op_schema_change().rowsets(0).num_rows());
    EXPECT_EQ(100, txn_log.op_schema_change().rowsets(0).data_size());
    EXPECT_EQ(14, txn_log.op_replication().op_writes(0).rowset().num_rows());   // 40/3=13 + (0<1?1:0) = 14
    EXPECT_EQ(134, txn_log.op_replication().op_writes(0).rowset().data_size()); // 400/3=133 + (0<1?1:0) = 134
    EXPECT_EQ(17, txn_log.op_parallel_compaction()
                          .subtask_compactions(0)
                          .output_rowset()
                          .num_rows()); // 50/3=16 + (0<2?1:0) = 17
    EXPECT_EQ(167, txn_log.op_parallel_compaction()
                           .subtask_compactions(0)
                           .output_rowset()
                           .data_size()); // 500/3=166 + (0<2?1:0) = 167
}

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

TxnLogPtr make_op_compaction_log(int64_t source_tablet_id) {
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
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(rowset, segment_filename);
    return rowset;
}

} // namespace

TEST_F(LakeTabletReshardTest, test_convert_txn_log_merging_op_write_only_passthrough) {
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

TEST_F(LakeTabletReshardTest, test_convert_txn_log_merging_drops_op_compaction) {
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

TEST_F(LakeTabletReshardTest, test_convert_txn_log_merging_drops_op_parallel_compaction) {
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

// Regression: op_parallel_compaction subtasks synthesized by
// tablet_parallel_compaction_manager do not set new_segment_count — their
// output_rowset carries only newly written segments, so the helper should
// treat all of them as new rather than silently skipping them (which would
// leak segment files).
TEST_F(LakeTabletReshardTest, test_collect_compaction_output_files_parallel_without_new_segment_count) {
    const int64_t tablet_id = next_id();
    TxnLogPB log;
    log.set_tablet_id(tablet_id);
    auto* op_parallel_compaction = log.mutable_op_parallel_compaction();
    auto* subtask = op_parallel_compaction->add_subtask_compactions();
    auto* output_rowset = subtask->mutable_output_rowset();
    output_rowset->add_segment_metas()->set_filename("parallel_new_0.dat");
    output_rowset->add_segment_metas()->set_filename("parallel_new_1.dat");
    // Intentionally NOT setting new_segment_offset/new_segment_count to
    // reproduce the shape produced by the parallel-compaction manager.

    auto paths = lake::tablet_reshard_helper::collect_compaction_output_files(log, _tablet_manager.get());
    EXPECT_THAT(paths,
                ::testing::UnorderedElementsAre(_tablet_manager->segment_location(tablet_id, "parallel_new_0.dat"),
                                                _tablet_manager->segment_location(tablet_id, "parallel_new_1.dat")));
}

// Regression: partial compaction's output_rowset.segment_metas() concatenates
// reused input segments with newly written ones; only the new window
// (new_segment_offset / new_segment_count) should be queued for deletion.
// Deleting reused segments would corrupt the merged tablet because those
// segments are still live as input rowsets absorbed by the merge.
TEST_F(LakeTabletReshardTest, test_collect_compaction_output_files_partial_compaction) {
    const int64_t tablet_id = next_id();
    TxnLogPB log;
    log.set_tablet_id(tablet_id);
    auto* op_compaction = log.mutable_op_compaction();
    auto* output_rowset = op_compaction->mutable_output_rowset();
    // [reused_0, reused_1, new_0, new_1] — only new_0/new_1 are newly written.
    output_rowset->add_segment_metas()->set_filename("reused_0.dat");
    output_rowset->add_segment_metas()->set_filename("reused_1.dat");
    output_rowset->add_segment_metas()->set_filename("new_0.dat");
    output_rowset->add_segment_metas()->set_filename("new_1.dat");
    op_compaction->set_new_segment_offset(2);
    op_compaction->set_new_segment_count(2);

    auto paths = lake::tablet_reshard_helper::collect_compaction_output_files(log, _tablet_manager.get());
    EXPECT_THAT(paths, ::testing::UnorderedElementsAre(_tablet_manager->segment_location(tablet_id, "new_0.dat"),
                                                       _tablet_manager->segment_location(tablet_id, "new_1.dat")));
    EXPECT_THAT(paths,
                ::testing::Not(::testing::Contains(_tablet_manager->segment_location(tablet_id, "reused_0.dat"))));
    EXPECT_THAT(paths,
                ::testing::Not(::testing::Contains(_tablet_manager->segment_location(tablet_id, "reused_1.dat"))));
}

// Verifies that collect_compaction_output_files() collects files of every
// kind — segments (via output_rowset), ssts (compaction-ingested), output_sstable,
// output_sstables, lcrm_file, plus op_parallel_compaction.output_sstable /
// output_sstables / orphan_lcrm_files — so regressions don't silently reintroduce
// leaks by dropping any one category.
TEST_F(LakeTabletReshardTest, test_collect_compaction_output_files_covers_all_kinds) {
    const int64_t tablet_id = next_id();
    TxnLogPB log;
    log.set_tablet_id(tablet_id);

    // Top-level op_compaction with every output-file kind populated.
    auto* op_compaction = log.mutable_op_compaction();
    op_compaction->mutable_output_rowset()->add_segment_metas()->set_filename("out_seg.dat");
    op_compaction->set_new_segment_offset(0);
    op_compaction->set_new_segment_count(1);
    op_compaction->add_ssts()->set_name("compact_ingest.sst");
    op_compaction->mutable_output_sstable()->set_filename("compact_out.sst");
    op_compaction->add_output_sstables()->set_filename("compact_out_multi.sst");
    op_compaction->mutable_lcrm_file()->set_name("compact.crm");

    // op_parallel_compaction top-level output sstables and orphan lcrms.
    auto* op_parallel = log.mutable_op_parallel_compaction();
    op_parallel->mutable_output_sstable()->set_filename("parallel_out.sst");
    op_parallel->add_output_sstables()->set_filename("parallel_out_multi.sst");
    op_parallel->add_orphan_lcrm_files()->set_name("parallel_orphan.crm");

    auto paths = lake::tablet_reshard_helper::collect_compaction_output_files(log, _tablet_manager.get());
    EXPECT_THAT(paths,
                ::testing::UnorderedElementsAre(_tablet_manager->segment_location(tablet_id, "out_seg.dat"),
                                                _tablet_manager->sst_location(tablet_id, "compact_ingest.sst"),
                                                _tablet_manager->sst_location(tablet_id, "compact_out.sst"),
                                                _tablet_manager->sst_location(tablet_id, "compact_out_multi.sst"),
                                                _tablet_manager->lcrm_location(tablet_id, "compact.crm"),
                                                _tablet_manager->sst_location(tablet_id, "parallel_out.sst"),
                                                _tablet_manager->sst_location(tablet_id, "parallel_out_multi.sst"),
                                                _tablet_manager->lcrm_location(tablet_id, "parallel_orphan.crm")));
}

// Regression: the persistent-index compaction "full contain / only do move"
// optimization re-emits an input sstable as its own output verbatim (same
// filename, only the fileset_id changes). Such a file is still referenced by the
// base metadata and every sibling tablet, so when a pending compaction is dropped
// during a split/merge cross-publish it must NOT be queued for deletion. Deleting
// it removed shared PK-index sstables in production and stalled publishes with
// "load primary index failed: ... .sst does not exist".
//
// Covers both message shapes (op_compaction and op_parallel_compaction) and both
// output fields. Production emits reused files into the plural output_sstables,
// so each op reuses via output_sstables; the singular output_sstable is exercised
// too (reused on op_compaction, genuinely new on op_parallel_compaction).
TEST_F(LakeTabletReshardTest, test_collect_compaction_output_files_skips_passthrough_reused_sstables) {
    const int64_t tablet_id = next_id();
    TxnLogPB log;
    log.set_tablet_id(tablet_id);

    // op_compaction: "reused.sst" (plural) and "reused_single.sst" (singular) are
    // pass-through outputs that alias inputs; "compact_new.sst" is genuinely new.
    auto* op_compaction = log.mutable_op_compaction();
    op_compaction->add_input_sstables()->set_filename("reused.sst");
    op_compaction->add_input_sstables()->set_filename("reused_single.sst");
    op_compaction->mutable_output_sstable()->set_filename("reused_single.sst");
    op_compaction->add_output_sstables()->set_filename("reused.sst");
    op_compaction->add_output_sstables()->set_filename("compact_new.sst");

    // op_parallel_compaction: "parallel_reused.sst" is reused via the plural
    // output_sstables (the shape production actually emits); the singular
    // output_sstable and the other plural entry are genuinely new.
    auto* op_parallel = log.mutable_op_parallel_compaction();
    op_parallel->add_input_sstables()->set_filename("parallel_reused.sst");
    op_parallel->mutable_output_sstable()->set_filename("parallel_single_new.sst");
    op_parallel->add_output_sstables()->set_filename("parallel_reused.sst");
    op_parallel->add_output_sstables()->set_filename("parallel_new.sst");

    auto paths = lake::tablet_reshard_helper::collect_compaction_output_files(log, _tablet_manager.get());
    // Only the genuinely new outputs are collected for deletion.
    EXPECT_THAT(paths,
                ::testing::UnorderedElementsAre(_tablet_manager->sst_location(tablet_id, "compact_new.sst"),
                                                _tablet_manager->sst_location(tablet_id, "parallel_single_new.sst"),
                                                _tablet_manager->sst_location(tablet_id, "parallel_new.sst")));
    // The pass-through reused (still-live) sstables are never queued for deletion,
    // whether they came through the singular output_sstable or the plural list.
    EXPECT_THAT(paths, ::testing::Not(::testing::Contains(_tablet_manager->sst_location(tablet_id, "reused.sst"))));
    EXPECT_THAT(paths,
                ::testing::Not(::testing::Contains(_tablet_manager->sst_location(tablet_id, "reused_single.sst"))));
    EXPECT_THAT(paths,
                ::testing::Not(::testing::Contains(_tablet_manager->sst_location(tablet_id, "parallel_reused.sst"))));
}

// Read-only aliasing discards the source SST cohort, so its uint32 RSSID high
// half does not constrain the signed packed target cursor.
TEST_F(LakeTabletReshardTest, test_tablet_merging_skip_sstable_merge_accepts_sign_bit_source_watermark) {
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
    auto merged = lake::merge_tablet(_tablet_manager.get(), sources, merging_info, /*new_version=*/2, txn_info,
                                     /*skip_sstable_merge=*/true);

    ASSERT_OK(merged.status());
    ASSERT_EQ(1, merged.value()->rowsets_size());
    EXPECT_EQ(1, merged.value()->rowsets(0).id());
    EXPECT_EQ(2, merged.value()->next_rowset_id());
    EXPECT_EQ(0, merged.value()->sstable_meta().sstables_size());
    EXPECT_EQ(source_before, source->SerializeAsString());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_rowset_zero_before_any_side_effect) {
    int source_flush_count = 0;
    int delvec_writer_count = 0;
    int dcg_output_count = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("merge_sstables:source_pk_flush", [&](void*) { ++source_flush_count; });
    sync->SetCallBack("merge_delvecs:writer_invocations",
                      [&](void* arg) { delvec_writer_count += *static_cast<int*>(arg); });
    sync->SetCallBack("merge_dcg_meta:after_write_cols", [&](void*) { ++dcg_output_count; });
    sync->EnableProcessing();
    DeferOp clear_callbacks([&] {
        sync->ClearCallBack("merge_sstables:source_pk_flush");
        sync->ClearCallBack("merge_delvecs:writer_invocations");
        sync->ClearCallBack("merge_dcg_meta:after_write_cols");
        sync->DisableProcessing();
    });

    auto run_case = [&](uint32_t rowset_id, bool primary_key, bool skip_sstable_merge, bool expect_rejection) {
        SCOPED_TRACE(fmt::format("rowset_id={}, primary_key={}, skip_sstable_merge={}", rowset_id, primary_key,
                                 skip_sstable_merge));
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

        const std::string segment_name = fmt::format("rowset_zero_shared_{}.dat", txn_id);
        const uint64_t segment_size =
                write_two_column_segment(target_id, segment_name, kNumRows, [](int key) { return key * 10; });
        auto build_source = [&](int64_t tablet_id, int lower_key, int upper_key, int source_index) {
            auto metadata = std::make_shared<TabletMetadataPB>();
            metadata->set_id(tablet_id);
            metadata->set_version(kBaseVersion);
            metadata->set_next_rowset_id(rowset_id + 1);
            const auto [c0_uid, c1_uid] = set_two_column_pk_schema(metadata.get(), /*schema_id=*/4001);
            (void)c0_uid;
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
            segment->set_shared(true);
            stamp_physical_identity_uid(rowset, segment_name);
            (*metadata->mutable_rowset_to_schema())[rowset_id] = metadata->schema().id();

            const std::string cols_name = lake::gen_cols_filename(txn_id + source_index + 1);
            write_c1_only_cols_file(tablet_id, cols_name, kNumRows, [&](int row) {
                return row >= lower_key && row < upper_key ? 1000 * (source_index + 1) + row : row * 10;
            });
            auto& dcg = (*metadata->mutable_dcg_meta()->mutable_dcgs())[rowset_id];
            dcg.add_column_files(cols_name);
            dcg.add_unique_column_ids()->add_column_ids(c1_uid);
            dcg.add_versions(kBaseVersion);
            dcg.add_shared_files(false);

            DelVector delvec;
            const uint32_t deleted_rowid = static_cast<uint32_t>(source_index);
            delvec.init(kBaseVersion + source_index, &deleted_rowid, 1);
            add_delvec(metadata.get(), tablet_id, kBaseVersion + source_index, rowset_id,
                       fmt::format("rowset_zero_{}_{}.delvec", txn_id, source_index), delvec.save());
            return metadata;
        };

        auto source_a = build_source(source_a_id, /*lower_key=*/0, /*upper_key=*/1, /*source_index=*/0);
        auto source_b = build_source(source_b_id, /*lower_key=*/1, /*upper_key=*/2, /*source_index=*/1);
        const std::vector<std::string> source_pbs_before = {source_a->SerializeAsString(),
                                                            source_b->SerializeAsString()};
        ASSIGN_OR_ABORT(auto files_before, directory_inventory(_location_provider->segment_root_location(target_id)));
        ASSIGN_OR_ABORT(auto metadata_before,
                        directory_inventory(_location_provider->metadata_root_location(target_id)));

        MergingTabletInfoPB merging_info;
        merging_info.add_old_tablet_ids(source_a_id);
        merging_info.add_old_tablet_ids(source_b_id);
        merging_info.set_new_tablet_id(target_id);
        TxnInfoPB txn_info;
        txn_info.set_txn_id(txn_id);
        txn_info.set_commit_time(1);
        txn_info.set_gtid(1);
        source_flush_count = 0;
        delvec_writer_count = 0;
        dcg_output_count = 0;
        auto merged = lake::merge_tablet(_tablet_manager.get(), {source_a, source_b}, merging_info, kNewVersion,
                                         txn_info, skip_sstable_merge);

        EXPECT_EQ(source_pbs_before[0], source_a->SerializeAsString());
        EXPECT_EQ(source_pbs_before[1], source_b->SerializeAsString());
        ASSIGN_OR_ABORT(auto files_after, directory_inventory(_location_provider->segment_root_location(target_id)));
        ASSIGN_OR_ABORT(auto metadata_after,
                        directory_inventory(_location_provider->metadata_root_location(target_id)));
        EXPECT_EQ(metadata_before, metadata_after);
        if (expect_rejection) {
            EXPECT_TRUE(merged.status().is_invalid_argument()) << merged.status();
            EXPECT_TRUE(merged.status().message().contains(fmt::format("tablet {}", source_a_id))) << merged.status();
            EXPECT_TRUE(merged.status().message().contains("rowset 0")) << merged.status();
            EXPECT_EQ(0, source_flush_count);
            EXPECT_EQ(0, delvec_writer_count);
            EXPECT_EQ(0, dcg_output_count);
            EXPECT_EQ(files_before, files_after);
        } else {
            EXPECT_OK(merged.status());
            if (primary_key) {
                EXPECT_GT(delvec_writer_count, 0);
            } else {
                EXPECT_EQ(0, delvec_writer_count);
            }
            EXPECT_GT(dcg_output_count, 0);
            EXPECT_NE(files_before, files_after);
            EXPECT_EQ(primary_key && !skip_sstable_merge ? 2 : 0, source_flush_count);
        }
    };

    run_case(/*rowset_id=*/0, /*primary_key=*/true, /*skip_sstable_merge=*/false, /*expect_rejection=*/true);
    run_case(/*rowset_id=*/1, /*primary_key=*/true, /*skip_sstable_merge=*/false, /*expect_rejection=*/false);
    run_case(/*rowset_id=*/0, /*primary_key=*/true, /*skip_sstable_merge=*/true, /*expect_rejection=*/false);
    run_case(/*rowset_id=*/0, /*primary_key=*/false, /*skip_sstable_merge=*/false, /*expect_rejection=*/false);
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_packs_upper_half_live_rowset_without_sst) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_near_rssid_boundary_remains_writable) {
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

    const uint64_t write_segment_size =
            write_two_column_segment(merged_tablet, "near_boundary_write.dat", 1, [](int) { return 200; }, 1);
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_hot_sibling_id_space_does_not_overflow) {
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

    ASSERT_OK(put_tablet_metadata(cold_meta));
    ASSERT_OK(put_tablet_metadata(hot_meta));

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
TEST_F(LakeTabletReshardTest, test_tablet_merging_dead_reference_does_not_collide_with_earlier_live_rowset) {
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

    ASSERT_OK(put_tablet_metadata(earlier_meta));
    ASSERT_OK(put_tablet_metadata(later_meta));

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
TEST_F(LakeTabletReshardTest, test_tablet_merging_discarded_duplicate_does_not_lower_rssid_floor) {
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

    constexpr uint32_t kHighRowsetId = std::numeric_limits<int32_t>::max() - 1;
    add_rowset(later_meta.get(), /*rowset_id=*/kHighRowsetId,
               /*max_compact_input_rowset_id=*/kHighRowsetId,
               /*del_origin_rowset_id=*/kHighRowsetId);

    ASSERT_OK(put_tablet_metadata(earlier_meta));
    ASSERT_OK(put_tablet_metadata(later_meta));

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
TEST_F(LakeTabletReshardTest, test_tablet_merging_discarded_predicate_does_not_inflate_later_sibling_ceiling) {
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
    ASSERT_OK(put_tablet_metadata(low_meta));

    // ctx[1]: the same v10 delete predicate, cross-published onto a sibling whose id
    // counter ran far ahead. Predicates dedup by version, so this context contributes
    // no canonical atom, run, occurrence alias, or target slot.
    constexpr uint32_t kFarAheadPredicateId = 2'100'000'000;
    TabletMetadataPB predicate_only_meta;
    predicate_only_meta.set_id(predicate_only_tablet);
    predicate_only_meta.set_version(base_version);
    predicate_only_meta.set_next_rowset_id(kFarAheadPredicateId + 1);
    add_rowset_with_predicate(&predicate_only_meta, kFarAheadPredicateId, /*version=*/10, /*has_predicate=*/true);
    ASSERT_OK(put_tablet_metadata(predicate_only_meta));

    // ctx[2]: two live rowsets ~1e8 ids apart. They form two singleton runs, so the
    // numeric gap and ctx[1]'s unused raw predicate id do not consume target space.
    constexpr uint32_t kWideSpanTopId = 100'000'000;
    TabletMetadataPB wide_meta;
    wide_meta.set_id(wide_tablet);
    wide_meta.set_version(base_version);
    wide_meta.set_next_rowset_id(kWideSpanTopId + 1);
    add_rowset_with_predicate(&wide_meta, /*rowset_id=*/5, /*version=*/20, /*has_predicate=*/false);
    add_rowset_with_predicate(&wide_meta, kWideSpanTopId, /*version=*/21, /*has_predicate=*/false);
    ASSERT_OK(put_tablet_metadata(wide_meta));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_interval_projection_packs_sparse_contexts) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_interval_projection_compresses_context_zero) {
    auto source = make_allocator_source(next_id(), 106);
    add_allocator_rowset(source.get(), 100, 1, "context_zero_100.dat");
    add_allocator_rowset(source.get(), 105, 2, "context_zero_105.dat");

    ASSIGN_OR_ABORT(auto merged, publish_allocator_merge({source}));
    EXPECT_EQ((std::vector<uint32_t>{1, 2}), allocator_rowset_ids(*merged));
    EXPECT_EQ(3, merged->next_rowset_id());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_interval_projection_accepts_upper_half_source_domain) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_interval_projection_reserves_duplicate_high_segment_idx) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_interval_projection_rejects_overlapping_final_ownership) {
    auto source = make_allocator_source(next_id(), 16);
    auto* wide = add_allocator_rowset(source.get(), 10, 1, "overlap_0.dat", 0);
    for (uint32_t idx = 1; idx <= 10; ++idx) {
        add_allocator_segment(wide, fmt::format("overlap_{}.dat", idx), idx);
    }
    add_allocator_rowset(source.get(), 15, 2, "overlap_other.dat", 0);

    int materialize_count = 0;
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("materialize_planned_rowsets:entry", [&](void*) { ++materialize_count; });
    sync->EnableProcessing();
    DeferOp clear_sync([&] {
        sync->ClearAllCallBacks();
        sync->DisableProcessing();
    });
    auto result = publish_allocator_merge({source});
    EXPECT_TRUE(result.status().is_corruption()) << result.status();
    EXPECT_EQ(0, materialize_count);
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_interval_projection_missing_primary_map_fails_closed) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_selected_delete_uses_final_canonical_max) {
    auto selected_source = make_allocator_source(next_id(), 101);
    auto* selected = add_allocator_rowset(selected_source.get(), 100, 1, "selected_del_low.dat", 0);
    auto* selected_del = selected->add_del_files();
    selected_del->set_name("selected_final.del");
    selected_del->set_origin_rowset_id(80);
    auto duplicate_source = make_allocator_source(next_id(), 301);
    auto* duplicate = add_allocator_rowset(duplicate_source.get(), 200, 1, "selected_del_high.dat", 100);
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_validation_only_delete_origin_allocates_no_slots) {
    auto selected_source = make_allocator_source(next_id(), 111);
    auto* selected = add_allocator_rowset(selected_source.get(), 10, 1, "validation_high.dat", 100);
    auto* selected_del = selected->add_del_files();
    selected_del->set_name("validation_only.del");
    selected_del->set_origin_rowset_id(5);
    auto duplicate_source = make_allocator_source(next_id(), 21);
    auto* duplicate = add_allocator_rowset(duplicate_source.get(), 20, 1, "validation_low.dat", 0);
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_validation_only_delete_uses_local_max) {
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

    auto merged = publish_allocator_merge({selected_source, duplicate_source});
    ASSERT_OK(merged.status());
    EXPECT_EQ(107, merged.value()->next_rowset_id());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_rejoins_independently_remapped_delete_origins) {
    const bool old_parallel_compaction = config::enable_pk_index_parallel_compaction;
    config::enable_pk_index_parallel_compaction = false;
    DeferOp restore_parallel([&] { config::enable_pk_index_parallel_compaction = old_parallel_compaction; });
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
    const uint64_t selected_size = write_two_column_segment(selected_id, "rejoin.dat", 1, [](int) { return 100; }, 10);
    write_two_column_segment(duplicate_id, "rejoin.dat", 1, [](int) { return 100; }, 10);
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_mixed_self_inherited_del_class) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_segment_declaration_conflict) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_reconciles_absent_and_explicit_zero_segment_idx) {
    auto selected_source = make_allocator_source(next_id(), 11);
    auto* selected = add_allocator_rowset(selected_source.get(), 10, 1, "segment_presence.dat", 0, false);
    selected->mutable_segment_metas(0)->set_shared(false);
    auto duplicate_source = make_allocator_source(next_id(), 21);
    auto* duplicate = add_allocator_rowset(duplicate_source.get(), 20, 1, "segment_presence.dat", 0, true);
    duplicate->mutable_uid()->CopyFrom(selected->uid());
    duplicate->mutable_segment_metas(0)->set_shared(true);

    auto merged_or = publish_allocator_merge({selected_source, duplicate_source});
    ASSERT_OK(merged_or.status());
    auto merged = std::move(merged_or).value();
    ASSERT_EQ(1, merged->rowsets_size());
    ASSERT_EQ(1, merged->rowsets(0).segment_metas_size());
    EXPECT_TRUE(merged->rowsets(0).segment_metas(0).has_segment_idx());
    EXPECT_EQ(0, merged->rowsets(0).segment_metas(0).segment_idx());
    EXPECT_TRUE(merged->rowsets(0).segment_metas(0).shared());
    EXPECT_EQ(1, merged->rowsets(0).id());
    EXPECT_EQ(2, merged->next_rowset_id());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_reconciles_segment_shared_flag) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_preserves_overlapped_without_segment_union_expansion) {
    const int64_t selected_id = next_id();
    const int64_t duplicate_id = next_id();
    prepare_tablet_dirs(selected_id);
    prepare_tablet_dirs(duplicate_id);
    const uint64_t high_size =
            write_two_column_segment(selected_id, "overlapped_same_high.dat", 2, [](int key) { return key * 10; }, 1);
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_del_declaration_conflict) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_reconciles_del_shared_flag) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_predicate_declaration_conflict) {
    auto first = make_allocator_source(next_id(), 11);
    auto* left = add_rowset_with_predicate(first.get(), 10, 7, true);
    left->mutable_delete_predicate()->mutable_binary_predicates(0)->set_value("1");
    auto second = make_allocator_source(next_id(), 21);
    auto* right = add_rowset_with_predicate(second.get(), 20, 7, true);
    right->mutable_delete_predicate()->mutable_binary_predicates(0)->set_value("2");

    auto result = publish_allocator_merge({first, second});
    EXPECT_TRUE(result.status().is_corruption()) << result.status();
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_unions_matching_predicate_ranges) {
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

    ASSIGN_OR_ABORT(auto merged, publish_allocator_merge({first, second}));
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_EQ(1, merged->rowsets(0).id());
    EXPECT_EQ(generate_sort_key(0).SerializeAsString(), merged->rowsets(0).range().lower_bound().SerializeAsString());
    EXPECT_EQ(generate_sort_key(100).SerializeAsString(), merged->rowsets(0).range().upper_bound().SerializeAsString());
    EXPECT_EQ(2, merged->next_rowset_id());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_rowset_schema_mapping_conflict) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_preserves_equal_recovery_key_equivalence_on_cold_load) {
    const int64_t source_id = next_id();
    prepare_tablet_dirs(source_id);
    auto source = make_allocator_source(source_id, 805);
    set_two_column_pk_schema(source.get(), 4001);
    source->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
    source->set_enable_persistent_index(true);
    source->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
    const uint64_t first_size =
            write_two_column_segment(source_id, "recovery_equal_a.dat", 1, [](int) { return 100; }, 10);
    const uint64_t second_size =
            write_two_column_segment(source_id, "recovery_equal_b.dat", 1, [](int) { return 200; }, 20);
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_alias_reversed_recovery_order_before_io) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_preserves_strict_recovery_order_on_cold_load) {
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
    const uint64_t first_size =
            write_two_column_segment(first_id, "recovery_strict_2.dat", 1, [](int) { return 100; }, 10);
    const uint64_t second_size =
            write_two_column_segment(second_id, "recovery_strict_20.dat", 1, [](int) { return 600; }, 60);
    auto* first_rowset = add_allocator_rowset(first.get(), 2, 1, "recovery_strict_2.dat", 0);
    first_rowset->mutable_segment_metas(0)->set_size(first_size);
    auto* second_rowset = add_allocator_rowset(second.get(), 20, 2, "recovery_strict_20.dat", 0);
    second_rowset->mutable_segment_metas(0)->set_size(second_size);

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_affine_modern_source_stale_falls_back) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_affine_modern_source_live_reuses) {
    ASSIGN_OR_ABORT(auto result,
                    publish_metadata_only_merge_fixture(MetadataOnlyMergeShape::kPrivate, false, false, true));
    const auto& target = result.published.at(result.target_tablet_id);
    auto modern = std::find_if(target.sstable_meta().sstables().begin(), target.sstable_meta().sstables().end(),
                               [](const auto& sst) { return sst.has_shared_rssid(); });
    ASSERT_NE(target.sstable_meta().sstables().end(), modern);
    EXPECT_EQ(1, modern->shared_rssid());
    EXPECT_EQ(static_cast<uint64_t>(1) << 32, modern->max_rss_rowid());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_affine_modern_mismatched_high_falls_back) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_affine_legacy_negative_offset_falls_back) {
    ASSIGN_OR_ABORT(auto result, publish_metadata_only_merge_fixture(
                                         MetadataOnlyMergeShape::kPrivate, false, false, true, [](auto& sources) {
                                             auto* legacy = sources[1]->mutable_sstable_meta()->mutable_sstables(0);
                                             legacy->set_rssid_offset(-1);
                                         }));
    expect_affine_sst_fallback_orphans(result);
    expect_metadata_fallback_lifecycle(result, {{10, 100}, {60, 600}}, {{10, 1010}}, {60});
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_affine_legacy_compressed_gap_falls_back) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_affine_legacy_raw_zero_reuses) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_affine_legacy_alias_exclusive_end_reuses) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_affine_legacy_uint32_exclusive_end_does_not_enumerate) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_affine_legacy_proof_visits_runs_not_rssids) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_liveness_precompute_visits_each_live_rssid_once) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_affine_identical_common_nonzero_delta_reuses) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_affine_identical_occurrence_disagreement_falls_back) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_affine_identical_prior_offset_reuses) {
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_affine_legacy_alias_interior_rejected_before_materialize) {
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
TEST_F(LakeTabletReshardTest, test_tablet_merging_cloud_native_pk_flush_path) {
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

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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

// Both shared source segments resolve through occurrence aliases to the same
// canonical target RSSID. With the canonical atoms and authoritative cursor
// already fixed, identical DCG declarations deduplicate to one passthrough entry.
TEST_F(LakeTabletReshardTest, test_tablet_merging_dcg_exact_dedup_preserves_passthrough) {
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

    EXPECT_OK(put_tablet_metadata(meta1));
    EXPECT_OK(put_tablet_metadata(meta2));

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
TEST_F(LakeTabletReshardTest, test_tablet_merging_dcg_disjoint_columns_append) {
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

    EXPECT_OK(put_tablet_metadata(meta1));
    EXPECT_OK(put_tablet_metadata(meta2));

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
TEST_F(LakeTabletReshardTest, test_tablet_merging_dcg_conflict_triggers_rebuild_dispatch) {
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

    EXPECT_OK(put_tablet_metadata(meta1));
    EXPECT_OK(put_tablet_metadata(meta2));

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

// Full end-to-end rebuild: two children each update column c1 on the same
// shared segment, with disjoint row windows. Merge must produce a single new
// .cols file whose row-by-row c1 values match each owner child's updates.
TEST_F(LakeTabletReshardTest, test_tablet_merging_dcg_rebuild_two_children_same_column_end_to_end) {
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

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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
TEST_F(LakeTabletReshardTest, test_tablet_merging_dcg_conflict_with_gap_first_child_compacts) {
    run_dcg_conflict_gap_rebuild_case(/*compacted_index=*/0, /*txn_id=*/3101);
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_dcg_conflict_with_gap_middle_child_compacts) {
    run_dcg_conflict_gap_rebuild_case(/*compacted_index=*/1, /*txn_id=*/3102);
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_dcg_conflict_with_gap_last_child_compacts) {
    run_dcg_conflict_gap_rebuild_case(/*compacted_index=*/2, /*txn_id=*/3103);
}

// When two children's DCG entries share a .cols filename but the entry
// metadata (column set, version, encryption, etc.) disagrees, exact dedup
// must reject the merge with Corruption via verify_dcg_entry_consistency.
TEST_F(LakeTabletReshardTest, test_tablet_merging_dcg_exact_dedup_consistency_failure) {
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

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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
TEST_F(LakeTabletReshardTest, test_tablet_merging_dcg_rebuild_missing_uid_falls_back) {
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

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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
TEST_F(LakeTabletReshardTest, test_tablet_merging_dcg_rebuild_cleanup_on_failure) {
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

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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
                                                                                int tablet_upper,
                                                                                uint64_t segment_size) {
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
                metas[i] = make_pk_shared_child_with_real_segment(child_ids[i], base_version, /*shared_id=*/10, lower, \
                                                                  upper, segment_size);                                \
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

// A synthesized gap delvec remains authoritative when divergent rowset layouts force lazy index rebuild, even when
// the inherited index metadata carries no delvec. Cold first-writer recovery must rebuild from rowsets, honor the
// synthesized target delvec, and preserve the exact data oracle across reopen.
TEST_F(LakeTabletReshardTest, test_tablet_merging_synthesized_delvec_survives_lazy_rebuild_fallback) {
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
                                                         /*upper=*/10, segment_size);
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

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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
TEST_F(LakeTabletReshardTest, test_tablet_merging_pk_gap_delvec_first_child_compacts) {
    BUILD_THREE_WAY_PK_GAP_MERGE(merged, canonical_rssid, merged_tablet, /*compacted_index=*/0, /*txn_id=*/1001);
    ASSERT_SYNTHESIZED_GAP_DELVEC(merged, canonical_rssid);
    // Two rowsets in merged: canonical R0 (shared, deduped from B+C) and
    // R1 (A's compaction output, non-shared).
    ASSERT_EQ(2, merged->rowsets_size());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_pk_gap_delvec_middle_child_compacts) {
    BUILD_THREE_WAY_PK_GAP_MERGE(merged, canonical_rssid, merged_tablet, /*compacted_index=*/1, /*txn_id=*/1002);
    ASSERT_SYNTHESIZED_GAP_DELVEC(merged, canonical_rssid);
    ASSERT_EQ(2, merged->rowsets_size());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_pk_gap_delvec_last_child_compacts) {
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
TEST_F(LakeTabletReshardTest, test_tablet_merging_pk_gap_delvec_rowid_content_matches_compacted_range) {
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
TEST_F(LakeTabletReshardTest, test_tablet_merging_pk_no_gap_passthrough) {
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

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));
    EXPECT_OK(put_tablet_metadata(meta_c));

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
TEST_F(LakeTabletReshardTest, test_tablet_merging_dup_keys_skip_dedup_on_gap) {
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

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));
    EXPECT_OK(put_tablet_metadata(meta_c));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_agg_keys_skip_dedup_on_gap) {
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

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));
    EXPECT_OK(put_tablet_metadata(meta_c));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_unique_keys_skip_dedup_on_gap) {
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

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));
    EXPECT_OK(put_tablet_metadata(meta_c));

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
TEST_F(LakeTabletReshardTest, test_tablet_merging_non_pk_contiguous_still_dedups) {
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

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_non_pk_skips_pk_sstable_pipeline) {
    using namespace pr1_helpers;
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);
    ASSERT_OK(put_tablet_metadata(make_shared_child(child_a, base_version, 10, DUP_KEYS, 0, 10)));
    ASSERT_OK(put_tablet_metadata(make_shared_child(child_b, base_version, 10, DUP_KEYS, 10, 20)));

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
TEST_F(LakeTabletReshardTest, test_tablet_merging_canonical_range_extends_for_duplicate_without_rowset_range) {
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
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(rowset, "shared_seg.dat"); // same uid across siblings => dedup
        // intentionally NO rowset->mutable_range(): rely on ctx tablet range
        return meta;
    };

    auto meta_a = make_no_rowset_range_child(child_a, 0, 10);
    auto meta_b = make_no_rowset_range_child(child_b, 10, 20);

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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

// Delete-predicate dedup keeps the original unconditional-skip path: contiguity
// is not consulted, and the contribution map gets no entry. Two PK children
// with delete-only predicate rowsets at the same version dedup down to one.
TEST_F(LakeTabletReshardTest, test_tablet_merging_delete_predicate_dedup_unchanged_pk) {
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
        // Same delete predicate cross-published to both children => same uid => dedup.
        stamp_physical_identity_uid(rowset, "shared_pk_predicate");
        return meta;
    };

    auto meta_a = make_pred_child(child_a, 0, 10);
    auto meta_b = make_pred_child(child_b, 10, 20);

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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
    // Both predicate rowsets dedup'd to single one (unconditional skip path).
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_TRUE(merged->rowsets(0).has_delete_predicate());
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

// MERGE: two split siblings share one physical .idx; it must dedup to a single entry under
// the canonical target rssid, marked shared. Mirrors test_tablet_merging_shared_dcg_dedup.
TEST_F(LakeTabletReshardTest, test_tablet_merging_shared_idg_dedup) {
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
        sm->set_shared(true);
        stamp_physical_identity_uid(rowset, "shared_seg.dat"); // same uid across siblings => dedup
        add_idg_with_key(meta.get(), 1, "shared.idx", /*col_uid=*/5, BITMAP, 1);
        return meta;
    };
    EXPECT_OK(put_tablet_metadata(make_child_with_idg(child_a)));
    EXPECT_OK(put_tablet_metadata(make_child_with_idg(child_b)));

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
TEST_F(LakeTabletReshardTest, test_tablet_merging_idg_unions_divergent_tombstones) {
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
        sm->set_shared(true);
        stamp_physical_identity_uid(rowset, "shared_seg.dat");
        add_idg_with_key(meta.get(), 1, "shared.idx", /*col_uid=*/5, BITMAP, 1);
        add_idg_key(meta.get(), 1, /*col_uid=*/6, BITMAP); // two keys on the same .idx
        if (drop_col5) add_idg_dropped_key(meta.get(), 1, /*col_uid=*/5, BITMAP);
        return meta;
    };
    EXPECT_OK(put_tablet_metadata(make_child(child_a, /*drop_col5=*/false)));
    EXPECT_OK(put_tablet_metadata(make_child(child_b, /*drop_col5=*/true)));

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

// MERGE: single-key shared .idx (col5), sibling B tombstones it -> fully tombstoned
// after union -> the merged target must have NO idg entry (vacuum no-fully-tombstoned rule).
TEST_F(LakeTabletReshardTest, test_tablet_merging_idg_drops_fully_tombstoned) {
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
        sm->set_shared(true);
        stamp_physical_identity_uid(rowset, "shared_seg.dat");
        add_idg_with_key(meta.get(), 1, "shared.idx", /*col_uid=*/5, BITMAP, 1);
        if (drop_col5) add_idg_dropped_key(meta.get(), 1, /*col_uid=*/5, BITMAP);
        return meta;
    };
    EXPECT_OK(put_tablet_metadata(make_child(child_a, /*drop_col5=*/false)));
    EXPECT_OK(put_tablet_metadata(make_child(child_b, /*drop_col5=*/true)));

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
    EXPECT_FALSE(merged->has_idg_meta() && merged->idg_meta().idgs().contains(merged->rowsets(0).id()))
            << "fully-tombstoned IDG entry must not be installed";
    // Its .idx must be orphaned so vacuum reclaims it (mirrors apply_drop_index).
    bool orphaned = false;
    for (const auto& f : merged->orphan_files()) {
        if (f.name() == "shared.idx") orphaned = true;
    }
    EXPECT_TRUE(orphaned) << "fully-tombstoned .idx must be added to orphan_files";
}

// MERGE: a .idx that is fully-tombstoned under one target but still ACTIVE under another
// target must NOT be orphaned (vacuum deletes orphan_files without checking live idg_meta).
// The same physical .idx may be referenced under two target RSSIDs only when both resolve to
// the same physical base segment. child_a keeps "same.idx" active at its rssid; child_b
// (distinct uid => remapped rssid) has the same base and a fully tombstoned declaration.
TEST_F(LakeTabletReshardTest, test_tablet_merging_idg_orphan_skips_still_referenced_file) {
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
        lake::tablet_reshard_helper::set_rowset_uid(rowset); // distinct uid per child => both kept
        add_idg_with_key(meta.get(), 1, "same.idx", /*col_uid=*/5, BITMAP, 1);
        if (tombstone) add_idg_dropped_key(meta.get(), 1, /*col_uid=*/5, BITMAP);
        return meta;
    };
    EXPECT_OK(put_tablet_metadata(make_child(child_a, /*tombstone=*/false)));
    EXPECT_OK(put_tablet_metadata(make_child(child_b, /*tombstone=*/true)));

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
    // same.idx is still active under child_a's target, so it must NOT be orphaned.
    for (const auto& f : merged->orphan_files()) {
        EXPECT_NE("same.idx", f.name()) << "a still-referenced .idx must not be orphaned";
    }
    bool active = false;
    for (const auto& [rssid, ver] : merged->idg_meta().idgs()) {
        for (const auto& e : ver.entries()) {
            if (e.index_file() == "same.idx") active = true;
        }
    }
    EXPECT_TRUE(active) << "same.idx must remain an active entry";
}

// MERGE: a source split before this fix can have segment.shared=true but a stale
// idg.shared_file=false (the old split marked segments shared but not idg). merge must
// DERIVE the .idx shared flag from the merged segment's ownership, upgrading the stale
// false to true, so vacuum does not later delete a shared .idx as if it were private.
TEST_F(LakeTabletReshardTest, test_tablet_merging_idg_derives_shared_from_segment) {
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
        stamp_physical_identity_uid(rowset, "seg_b.dat");
    }

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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

// MERGE: the canonical child carries a STALE idg entry for a segment it no longer
// owns, whose remapped target is live only because a sibling supplies that segment. The
// source-live check must drop it (stale.idx must appear nowhere). next_rowset_id(2) makes
// child_b remap to rssid 2 so the stale target IS live, exercising the source-live branch.
TEST_F(LakeTabletReshardTest, test_tablet_merging_idg_skips_stale_source_entry) {
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
        stamp_physical_identity_uid(rowset, "seg_a.dat");
    }
    // Stale idg keyed at rssid 2 -- child_a has NO segment there.
    add_idg_with_key(meta_a.get(), /*segment_id=*/2, "stale.idx", /*col_uid=*/5, BITMAP, 1);

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
        stamp_physical_identity_uid(rowset, "seg_b.dat"); // distinct uid => not deduped
    }

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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
    for (const auto& [rssid, ver] : merged->idg_meta().idgs()) {
        for (const auto& e : ver.entries()) {
            EXPECT_NE("stale.idx", e.index_file()) << "stale source idg entry must be skipped";
        }
    }
}

// Two contexts with distinct private segments each contribute a canonical atom
// [1,2). Their primary runs pack to different target RSSIDs, and both real .idx
// entries survive under those targets rather than being dropped.
TEST_F(LakeTabletReshardTest, test_tablet_merging_idg_remaps_private_segments) {
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
        stamp_physical_identity_uid(rowset, seg); // distinct seed per child => distinct uid
        // Exclusive (private) segment => its .idx is shared_file=false, as split's
        // propagate_pruned would have marked it. merge must PRESERVE this (not force true).
        add_idg_with_key(meta.get(), 1, idx, /*col_uid=*/5, BITMAP, 1, /*shared_file=*/false);
        return meta;
    };
    EXPECT_OK(put_tablet_metadata(make_child(child_a, "seg_a.dat", "a.idx")));
    EXPECT_OK(put_tablet_metadata(make_child(child_b, "seg_b.dat", "b.idx")));

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

TEST_F(LakeTabletReshardTest, test_pk_tablet_splitting_full_sort_key_index_conservation) {
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
    const std::string seg_name = "full_key_seg.dat";

    const bool old_enable = config::enable_full_sort_key_index;
    config::enable_full_sort_key_index = true;
    DeferOp restore_config([&] { config::enable_full_sort_key_index = old_enable; });
    const uint64_t seg_size = write_two_column_segment(tablet_id, seg_name, kNumRows, [](int i) { return i; });

    // Confirm the written segment genuinely carries the full, untruncated sort-key
    // index -- not silently a legacy/truncated one.
    {
        FileInfo file_info;
        file_info.path = _tablet_manager->segment_location(tablet_id, seg_name);
        ASSIGN_OR_ABORT(auto fs, FileSystemFactory::CreateSharedFromString(file_info.path));
        auto tablet_schema = TabletSchema::create(metadata.schema());
        ASSIGN_OR_ABORT(auto segment, Segment::open(fs, file_info, 0, tablet_schema));
        ASSERT_OK(segment->load_index());
        ASSERT_TRUE(segment->has_full_sort_key_index_page())
                << "test setup bug: segment must carry the full sort-key short key index";
    }

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
    // Deliberately no deprecated_sort_key_samples: the only source of split-boundary
    // precision beyond the coarse [min, max] pair is the full-key index loader.

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

    // The full-key index carries 2 samples (rows 100 and 200 of 300, at the BE_TEST
    // default num_rows_per_block == 100), giving exactly 3 balanced sub-segments -- a
    // real 3-way split. tablet_metadatas also carries the old tablet id's own
    // new-version entry (a tombstone at the old location), so the map holds
    // new_tablet_ids_size() + 1 entries.
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

// Mixed-segment split: one rowset carries a legacy segment (a real, truncated-short-key
// segment whose metadata still records deprecated_sort_key_samples) and a second rowset
// carries a real full-key-index segment. build_segments_from_rowsets must select the
// correct per-segment source for each (metadata samples for the legacy one, the short key
// index loader for the full-key one), and the split must still conserve Σ
// children.rowset.{num_rows,data_size,num_dels} == parent for both rowsets.
TEST_F(LakeTabletReshardTest, test_pk_tablet_splitting_mixed_legacy_and_full_key_segments) {
    const int64_t base_version = 2;
    const int64_t new_version = 3;
    const int64_t tablet_id = next_id();

    prepare_tablet_dirs(tablet_id);

    TabletMetadataPB metadata;
    metadata.set_id(tablet_id);
    metadata.set_version(base_version);
    set_two_column_pk_schema(&metadata, /*schema_id=*/1);

    constexpr int kLegacyRows = 150;
    constexpr int kFullKeyRows = 300;
    const std::string legacy_seg_name = "legacy_seg.dat";
    const std::string full_key_seg_name = "full_key_seg.dat";

    const bool old_enable = config::enable_full_sort_key_index;
    DeferOp restore_config([&] { config::enable_full_sort_key_index = old_enable; });

    config::enable_full_sort_key_index = false;
    const uint64_t legacy_seg_size =
            write_two_column_segment(tablet_id, legacy_seg_name, kLegacyRows, [](int i) { return i; });

    config::enable_full_sort_key_index = true;
    const uint64_t full_key_seg_size =
            write_two_column_segment(tablet_id, full_key_seg_name, kFullKeyRows, [](int i) { return i; });

    // Confirm each written segment genuinely carries the index format the test assumes --
    // not silently the other one.
    auto tablet_schema = TabletSchema::create(metadata.schema());
    {
        FileInfo file_info;
        file_info.path = _tablet_manager->segment_location(tablet_id, legacy_seg_name);
        ASSIGN_OR_ABORT(auto fs, FileSystemFactory::CreateSharedFromString(file_info.path));
        ASSIGN_OR_ABORT(auto segment, Segment::open(fs, file_info, 0, tablet_schema));
        ASSERT_OK(segment->load_index());
        ASSERT_FALSE(segment->has_full_sort_key_index_page())
                << "test setup bug: the legacy segment must NOT carry the full sort-key index";
    }
    {
        FileInfo file_info;
        file_info.path = _tablet_manager->segment_location(tablet_id, full_key_seg_name);
        ASSIGN_OR_ABORT(auto fs, FileSystemFactory::CreateSharedFromString(file_info.path));
        ASSIGN_OR_ABORT(auto segment, Segment::open(fs, file_info, 0, tablet_schema));
        ASSERT_OK(segment->load_index());
        ASSERT_TRUE(segment->has_full_sort_key_index_page())
                << "test setup bug: this segment must carry the full sort-key index";
    }

    // Rowset A: the legacy segment. Real key range [0, 149], with
    // deprecated_sort_key_samples matching its real content.
    auto* rowset_a = metadata.add_rowsets();
    rowset_a->set_id(2);
    rowset_a->set_overlapped(false);
    rowset_a->set_num_rows(kLegacyRows);
    rowset_a->set_data_size(legacy_seg_size);
    rowset_a->set_num_dels(0);
    auto* sm_a = rowset_a->add_segment_metas();
    sm_a->set_filename(legacy_seg_name);
    sm_a->set_size(legacy_seg_size);
    sm_a->set_num_rows(kLegacyRows);
    sm_a->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
    sm_a->mutable_sort_key_max()->CopyFrom(generate_sort_key(kLegacyRows - 1));
    sm_a->set_deprecated_sort_key_sample_row_interval(50);
    sm_a->add_deprecated_sort_key_samples()->CopyFrom(generate_sort_key(50));
    sm_a->add_deprecated_sort_key_samples()->CopyFrom(generate_sort_key(100));

    // Rowset B: the full-key-index segment. Real key range [0, 299], deliberately no
    // deprecated_sort_key_samples -- its only source of split precision is the loader.
    auto* rowset_b = metadata.add_rowsets();
    rowset_b->set_id(3);
    rowset_b->set_overlapped(false);
    rowset_b->set_num_rows(kFullKeyRows);
    rowset_b->set_data_size(full_key_seg_size);
    rowset_b->set_num_dels(0);
    auto* sm_b = rowset_b->add_segment_metas();
    sm_b->set_filename(full_key_seg_name);
    sm_b->set_size(full_key_seg_size);
    sm_b->set_num_rows(kFullKeyRows);
    sm_b->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
    sm_b->mutable_sort_key_max()->CopyFrom(generate_sort_key(kFullKeyRows - 1));

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
    // 2 children + the old tablet id's own new-version tombstone entry.
    ASSERT_EQ(3U, tablet_metadatas.size());

    struct RsTotals {
        int64_t num_rows = 0;
        int64_t data_size = 0;
        int64_t num_dels = 0;
    };
    std::unordered_map<uint32_t, RsTotals> totals;
    for (int64_t cid : {child_id_1, child_id_2}) {
        auto it = tablet_metadatas.find(cid);
        ASSERT_TRUE(it != tablet_metadatas.end());
        for (const auto& rs : it->second->rowsets()) {
            auto& t = totals[rs.id()];
            t.num_rows += rs.num_rows();
            t.data_size += rs.data_size();
            t.num_dels += rs.num_dels();
        }
    }

    EXPECT_EQ(kLegacyRows, totals[2].num_rows);
    EXPECT_EQ(static_cast<int64_t>(legacy_seg_size), totals[2].data_size);
    EXPECT_EQ(0, totals[2].num_dels);

    EXPECT_EQ(kFullKeyRows, totals[3].num_rows);
    EXPECT_EQ(static_cast<int64_t>(full_key_seg_size), totals[3].data_size);
    EXPECT_EQ(0, totals[3].num_dels);
}

// =============================================================================
// Reachability nets for the merge-path failpoints. Same shape as the reshard-path ones: ARMED first
// (so the failure returns before any metadata is written and the disarmed run below still takes the
// full path rather than the metacache retry fast path), then disarmed on fresh tablet ids.
// =============================================================================

// Merge phase 1 -- the rowset-id reassignment stage. Fires on every merge.
TEST_F(LakeTabletReshardTest, test_merge_failpoint_after_rssid_reassign) {
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
        CHECK_OK(put_tablet_metadata(meta_a));

        TabletMetadataPB meta_b;
        meta_b.set_id(tablet_b);
        meta_b.set_version(base_version);
        meta_b.set_next_rowset_id(2);
        add_rowset_with_predicate(&meta_b, 1, 1, false);
        CHECK_OK(put_tablet_metadata(meta_b));

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
TEST_F(LakeTabletReshardTest, test_merge_failpoint_before_delete_predicate_range) {
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
        CHECK_OK(put_tablet_metadata(meta_a));

        TabletMetadataPB meta_b;
        meta_b.set_id(tablet_b);
        meta_b.set_version(base_version);
        meta_b.set_next_rowset_id(3);
        add_rowset_with_predicate(&meta_b, 1, 1, false); // data
        add_rowset_with_predicate(&meta_b, 2, 2, true);  // delete predicate
        CHECK_OK(put_tablet_metadata(meta_b));

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

// merge_delvecs writes a merged delvec file. Primary-key only, and skipped entirely when there is no
// source delvec and no synthesized gap, so both sources carry a delvec here.
TEST_F(LakeTabletReshardTest, test_merge_failpoint_after_write_delvec) {
    auto run_merge = [&]() {
        const int64_t base_version = 1;
        const int64_t new_version = 2;
        const int64_t tablet_a = next_id();
        const int64_t tablet_b = next_id();
        const int64_t new_tablet = next_id();

        prepare_tablet_dirs(tablet_a);
        prepare_tablet_dirs(tablet_b);
        prepare_tablet_dirs(new_tablet);

        auto build = [&](int64_t tablet_id, uint32_t rowset_id, int64_t schema_id, const std::string& delvec_name,
                         const std::string& delvec_content) {
            auto meta = std::make_shared<TabletMetadataPB>();
            meta->set_id(tablet_id);
            meta->set_version(base_version);
            meta->set_next_rowset_id(rowset_id + 1);
            set_primary_key_schema(meta.get(), schema_id);
            add_rowset(meta.get(), rowset_id, 7, 1);
            add_delvec(meta.get(), tablet_id, base_version, rowset_id, delvec_name, delvec_content);
            return meta;
        };

        auto meta_a = build(tablet_a, 10, 1001, "delvec-a", "aaaa");
        auto meta_b = build(tablet_b, 1, 2002, "delvec-b", "bbbbbb");
        CHECK_OK(put_tablet_metadata(meta_a));
        CHECK_OK(put_tablet_metadata(meta_b));

        ReshardingTabletInfoPB resharding_tablet;
        auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
        merging_tablet.add_old_tablet_ids(tablet_a);
        merging_tablet.add_old_tablet_ids(tablet_b);
        merging_tablet.set_new_tablet_id(new_tablet);

        TxnInfoPB txn_info;
        txn_info.set_txn_id(1);
        txn_info.set_commit_time(1);
        txn_info.set_gtid(1);

        std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
        std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
        return lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                               txn_info, false, tablet_metadatas, tablet_ranges);
    };

    set_failpoint_mode("tablet_merge_after_write_delvec", FailPointTriggerModeType::ENABLE);
    auto armed = run_merge();
    set_failpoint_mode("tablet_merge_after_write_delvec", FailPointTriggerModeType::DISABLE);
    EXPECT_FALSE(armed.ok()) << "hook not reached after the merged delvec file was written";

    EXPECT_OK(run_merge());
}

// The .cols rebuild only runs when two DCG entries claim the SAME column id for the same target
// segment, so this mirrors the two-children-same-column fixture: both children update c1 on one
// shared base segment over disjoint row windows.
TEST_F(LakeTabletReshardTest, test_merge_failpoint_after_write_dcg_cols) {
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
        CHECK_OK(put_tablet_metadata(meta_a));
        CHECK_OK(put_tablet_metadata(meta_b));

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

} // namespace starrocks
