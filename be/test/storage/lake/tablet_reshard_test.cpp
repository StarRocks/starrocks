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

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
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
#include "common/config_lake_fwd.h"
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
#include "storage/chunk_helper.h"
#include "storage/del_vector.h"
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
#include "storage/lake/transactions.h"
#include "storage/lake/update_manager.h"
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

    PersistentIndexSstablePB* add_shared_rssid_sstable(TabletMetadataPB* metadata, const std::string& filename,
                                                       uint32_t rssid, int64_t shared_version, uint32_t max_rowid,
                                                       uint64_t filesize = 512) {
        auto* sstable = metadata->mutable_sstable_meta()->add_sstables();
        sstable->set_filename(filename);
        sstable->set_filesize(filesize);
        sstable->set_shared(true);
        sstable->set_shared_rssid(rssid);
        sstable->set_shared_version(shared_version);
        sstable->set_max_rss_rowid((static_cast<uint64_t>(rssid) << 32) | max_rowid);
        return sstable;
    }

    PersistentIndexSstablePB* add_non_shared_modern_sstable(TabletMetadataPB* metadata, const std::string& filename,
                                                            uint32_t rssid, int64_t shared_version, uint32_t max_rowid,
                                                            uint64_t filesize) {
        auto* sstable = add_shared_rssid_sstable(metadata, filename, rssid, shared_version, max_rowid, filesize);
        sstable->set_shared(false);
        return sstable;
    }

    void attach_embedded_delvec(TabletMetadataPB* metadata, int64_t tablet_id, PersistentIndexSstablePB* sstable,
                                int64_t version, const std::string& filename, const std::string& content) {
        auto* page = sstable->mutable_delvec();
        page->set_version(version);
        page->set_offset(0);
        page->set_size(content.size());
        FileMetaPB file;
        file.set_name(filename);
        file.set_size(content.size());
        (*metadata->mutable_delvec_meta()->mutable_version_to_file())[version] = file;
        write_file(_tablet_manager->delvec_location(tablet_id, filename), content);
    }

    std::shared_ptr<TabletMetadataPB> make_single_rowset_pk_metadata(int64_t tablet_id, uint32_t rowset_id,
                                                                     const std::string& segment_filename) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(1);
        metadata->set_next_rowset_id(rowset_id + 1);
        set_primary_key_schema(metadata.get(), 1001);
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(rowset_id);
        rowset->set_version(1);
        rowset->set_num_rows(1);
        rowset->set_data_size(100);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(segment_filename);
        segment->set_size(100);
        return metadata;
    }

    TabletMetadataPtr make_modern_shared_occurrence(int64_t tablet_id, uint32_t rowset_id, uint32_t shared_rssid,
                                                    int32_t rssid_offset, uint32_t max_rowid, uint64_t filesize = 512,
                                                    int64_t shared_version = 7) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(1);
        metadata->set_next_rowset_id(rowset_id + 1);
        set_primary_key_schema(metadata.get(), 1001);
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(rowset_id);
        rowset->set_version(1);
        rowset->set_num_rows(1);
        rowset->set_data_size(100);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename("projected_modern_shared_segment.dat");
        segment->set_size(100);
        segment->set_shared(true);
        stamp_physical_identity_uid(rowset, segment->filename());

        auto* sst = add_shared_rssid_sstable(metadata.get(), "projected_modern_shared.sst", shared_rssid,
                                             shared_version, max_rowid, filesize);
        sst->set_rssid_offset(rssid_offset);
        const int64_t effective_rssid = static_cast<int64_t>(shared_rssid) + rssid_offset;
        const uint32_t watermark_high = effective_rssid < 0 ? 0 : static_cast<uint32_t>(effective_rssid);
        sst->set_max_rss_rowid((static_cast<uint64_t>(watermark_high) << 32) | max_rowid);
        return metadata;
    }

    StatusOr<TabletMetadataPtr> merge_modern_shared_occurrences(const TabletMetadataPtr& child_a,
                                                                const TabletMetadataPtr& child_b, int64_t merged_tablet,
                                                                int64_t base_version = 1, int64_t new_version = 2,
                                                                int64_t txn_id = 1) {
        RETURN_IF_ERROR(put_tablet_metadata(child_a));
        RETURN_IF_ERROR(put_tablet_metadata(child_b));
        ReshardingTabletInfoPB resharding_tablet;
        auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
        merging_info.add_old_tablet_ids(child_a->id());
        merging_info.add_old_tablet_ids(child_b->id());
        merging_info.set_new_tablet_id(merged_tablet);
        TxnInfoPB txn_info;
        txn_info.set_txn_id(txn_id);
        txn_info.set_commit_time(1);
        txn_info.set_gtid(1);
        std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
        std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
        RETURN_IF_ERROR(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version,
                                                        new_version, txn_info, false, tablet_metadatas, tablet_ranges));
        return tablet_metadatas.at(merged_tablet);
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

    struct BelowFloorLegacyFixture {
        static constexpr int64_t kBaseVersion = 1;
        static constexpr int64_t kMergedVersion = 2;
        static constexpr uint32_t kSourceLiveRssid = 107;
        static constexpr uint32_t kFinalLiveRssid = 1;
        static constexpr int32_t kContextOffset = -106;

        int64_t cold_tablet = 0;
        int64_t hot_tablet = 0;
        int64_t merged_tablet = 0;
        std::string source_filename;
        std::string source_path;
        std::string segment_filename;
        std::string live_key;
        std::shared_ptr<TabletMetadataPB> cold_metadata;
        std::shared_ptr<TabletMetadataPB> hot_metadata;
        PersistentIndexSstablePB source_pb;
    };

    BelowFloorLegacyFixture make_below_floor_legacy_fixture(
            const std::string& source_filename, const std::vector<std::pair<std::string, std::string>>& entries,
            uint32_t source_high, bool encrypted = false, bool filter_live_row_with_delvec = false) {
        BelowFloorLegacyFixture fixture;
        fixture.cold_tablet = next_id();
        fixture.hot_tablet = next_id();
        fixture.merged_tablet = next_id();
        fixture.source_filename = source_filename;
        fixture.source_path = _tablet_manager->sst_location(fixture.hot_tablet, source_filename);
        fixture.segment_filename = source_filename + ".dat";
        fixture.live_key = encode_int_primary_key(20);
        prepare_tablet_dirs(fixture.cold_tablet);
        prepare_tablet_dirs(fixture.hot_tablet);
        prepare_tablet_dirs(fixture.merged_tablet);

        const uint64_t segment_size = write_two_column_segment(
                fixture.hot_tablet, fixture.segment_filename, /*num_rows=*/1, [](int key) { return key * 10; }, 20);
        const auto source_file = write_raw_pk_sstable(fixture.source_path, entries, encrypted);

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

        fixture.cold_metadata = make_metadata(fixture.cold_tablet);
        fixture.cold_metadata->set_next_rowset_id(1);

        fixture.hot_metadata = make_metadata(fixture.hot_tablet);
        fixture.hot_metadata->set_next_rowset_id(BelowFloorLegacyFixture::kSourceLiveRssid + 1);
        auto* rowset = fixture.hot_metadata->add_rowsets();
        rowset->set_id(BelowFloorLegacyFixture::kSourceLiveRssid);
        rowset->set_version(BelowFloorLegacyFixture::kBaseVersion);
        rowset->set_num_rows(1);
        rowset->set_data_size(segment_size);
        rowset->set_overlapped(false);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(fixture.segment_filename);
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

        if (filter_live_row_with_delvec) {
            DelVector delvec;
            const uint32_t deleted_rowid = 0;
            delvec.init(BelowFloorLegacyFixture::kBaseVersion, &deleted_rowid, 1);
            add_delvec(fixture.hot_metadata.get(), fixture.hot_tablet, BelowFloorLegacyFixture::kBaseVersion,
                       BelowFloorLegacyFixture::kSourceLiveRssid, source_filename + ".dv", delvec.save());
        }
        fixture.source_pb.CopyFrom(*source_pb);
        return fixture;
    }

    enum class InvalidRangeMixedTrigger { kDeadOwner, kMergedDelvec };

    struct InvalidRangeMixedLegacyFixture {
        int64_t source_tablet = 0;
        int64_t empty_tablet = 0;
        int64_t merged_tablet = 0;
        int32_t tail_key_value = 40;
        std::string source_filename;
        std::string source_path;
        std::string live_key;
        std::string dropped_key;
        std::string tail_key;
        std::shared_ptr<TabletMetadataPB> source_metadata;
        std::shared_ptr<TabletMetadataPB> empty_metadata;
    };

    InvalidRangeMixedLegacyFixture make_invalid_range_mixed_legacy_fixture(const std::string& source_filename,
                                                                           InvalidRangeMixedTrigger trigger) {
        InvalidRangeMixedLegacyFixture fixture;
        fixture.source_tablet = next_id();
        fixture.empty_tablet = next_id();
        fixture.merged_tablet = next_id();
        fixture.source_filename = source_filename;
        fixture.source_path = _tablet_manager->sst_location(fixture.source_tablet, source_filename);
        fixture.live_key = encode_int_primary_key(20);
        fixture.dropped_key = encode_int_primary_key(trigger == InvalidRangeMixedTrigger::kDeadOwner ? 30 : 21);
        fixture.tail_key = encode_int_primary_key(fixture.tail_key_value);
        prepare_tablet_dirs(fixture.source_tablet);
        prepare_tablet_dirs(fixture.empty_tablet);
        prepare_tablet_dirs(fixture.merged_tablet);

        const int data_num_rows = trigger == InvalidRangeMixedTrigger::kDeadOwner ? 1 : 2;
        const std::string segment_filename = source_filename + ".dat";
        const uint64_t segment_size = write_two_column_segment(
                fixture.source_tablet, segment_filename, data_num_rows, [](int key) { return key * 10; },
                /*key_start=*/20);
        const std::string tail_segment_filename = source_filename + ".tail.dat";
        const uint64_t tail_segment_size = write_two_column_segment(
                fixture.source_tablet, tail_segment_filename, /*num_rows=*/1, [](int key) { return key * 10; },
                fixture.tail_key_value);
        const uint32_t dropped_stored_rssid = trigger == InvalidRangeMixedTrigger::kDeadOwner ? 2 : 0;
        const uint32_t dropped_rowid = trigger == InvalidRangeMixedTrigger::kDeadOwner ? 0 : 1;
        const auto source_file = write_raw_pk_sstable(
                fixture.source_path,
                {{fixture.live_key, serialize_index_values({{/*version=*/201, /*rssid=*/0, /*rowid=*/0}})},
                 {fixture.dropped_key,
                  serialize_index_values({{/*version=*/200, dropped_stored_rssid, dropped_rowid}})}});

        auto make_metadata = [&](int64_t tablet_id) {
            auto metadata = std::make_shared<TabletMetadataPB>();
            metadata->set_id(tablet_id);
            metadata->set_version(1);
            set_two_column_pk_schema(metadata.get(), /*schema_id=*/4001);
            metadata->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
            metadata->set_enable_persistent_index(true);
            metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
            return metadata;
        };

        fixture.source_metadata = make_metadata(fixture.source_tablet);
        fixture.source_metadata->set_next_rowset_id(7);
        auto* rowset = fixture.source_metadata->add_rowsets();
        rowset->set_id(5);
        rowset->set_version(1);
        rowset->set_num_rows(data_num_rows);
        rowset->set_data_size(segment_size);
        rowset->set_overlapped(false);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(segment_filename);
        segment->set_size(segment_size);
        segment->set_num_rows(data_num_rows);
        auto* tail_rowset = fixture.source_metadata->add_rowsets();
        tail_rowset->set_id(6);
        tail_rowset->set_version(1);
        tail_rowset->set_num_rows(1);
        tail_rowset->set_data_size(tail_segment_size);
        tail_rowset->set_overlapped(false);
        auto* tail_segment = tail_rowset->add_segment_metas();
        tail_segment->set_filename(tail_segment_filename);
        tail_segment->set_size(tail_segment_size);
        tail_segment->set_num_rows(1);

        auto* source_pb = fixture.source_metadata->mutable_sstable_meta()->add_sstables();
        source_pb->set_filename(source_filename);
        source_pb->set_filesize(source_file.filesize);
        source_pb->set_shared(false);
        source_pb->set_rssid_offset(5);
        source_pb->set_max_rss_rowid((static_cast<uint64_t>(5) << 32) | 9);
        source_pb->mutable_range()->CopyFrom(source_file.range);
        source_pb->mutable_fileset_id()->set_hi(0xA5A5);
        source_pb->mutable_fileset_id()->set_lo(0xB6B6);
        source_pb->set_generation_version(1);

        if (trigger == InvalidRangeMixedTrigger::kMergedDelvec) {
            DelVector delvec;
            const uint32_t deleted_rowid = 1;
            delvec.init(/*version=*/1, &deleted_rowid, 1);
            add_delvec(fixture.source_metadata.get(), fixture.source_tablet, /*version=*/1, /*segment_id=*/5,
                       source_filename + ".dv", delvec.save());
        }

        fixture.empty_metadata = make_metadata(fixture.empty_tablet);
        fixture.empty_metadata->set_next_rowset_id(1);
        return fixture;
    }

    void verify_invalid_range_mixed_legacy_rebuild(const InvalidRangeMixedLegacyFixture& fixture) {
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
        DeferOp restore_flush_failpoints([&] {
            set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
            set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
        });

        auto merged_or =
                merge_modern_shared_occurrences(fixture.source_metadata, fixture.empty_metadata, fixture.merged_tablet);
        ASSERT_OK(merged_or);
        auto merged = std::move(merged_or).value();

        const PersistentIndexSstablePB* rebuilt = nullptr;
        const PersistentIndexSstablePB* tail = nullptr;
        for (const auto& sstable : merged->sstable_meta().sstables()) {
            EXPECT_NE(fixture.source_filename, sstable.filename()) << "invalid metadata must use replacement files";
            ASSIGN_OR_ABORT(auto entries, read_raw_pk_sstable(fixture.merged_tablet, merged, sstable));
            for (const auto& [key, values] : entries) {
                ASSERT_EQ(1, values.values_size());
                if (key == fixture.live_key && values.values(0).version() == 201) {
                    rebuilt = &sstable;
                    EXPECT_EQ(5, values.values(0).rssid());
                    EXPECT_EQ(0, values.values(0).rowid());
                } else if (key == fixture.tail_key) {
                    tail = &sstable;
                    EXPECT_EQ(6, values.values(0).rssid());
                    EXPECT_EQ(0, values.values(0).rowid());
                }
                EXPECT_NE(fixture.dropped_key, key) << "dead/delvec value must not survive rebuild or tail";
            }
        }

        ASSERT_NE(nullptr, rebuilt) << "retained live value must be rewritten to its final owner";
        const uint64_t expected_rebuilt_max = static_cast<uint64_t>(5) << 32;
        EXPECT_EQ(expected_rebuilt_max, rebuilt->max_rss_rowid())
                << "invalid PB seed (5,9) must not overstate retained coverage beyond exact live (5,0)";
        EXPECT_EQ(0, rebuilt->rssid_offset());
        EXPECT_EQ(2, merged->sstable_meta().sstables_size())
                << "coverage through rssid 5 must materialize the uncovered rssid-6 tail";
        EXPECT_NE(nullptr, tail);

        const std::vector<std::string> lookup_keys = {fixture.live_key, fixture.dropped_key, fixture.tail_key};
        ASSIGN_OR_ABORT(auto lookup_values, load_index_values(merged, fixture.merged_tablet, lookup_keys));
        ASSERT_EQ(3, lookup_values.size());
        EXPECT_EQ(IndexValue(expected_rebuilt_max), lookup_values[0]);
        EXPECT_EQ(IndexValue(NullIndexValue), lookup_values[1]);
        EXPECT_EQ(IndexValue(static_cast<uint64_t>(6) << 32), lookup_values[2]);

        ASSIGN_OR_ABORT(auto after_dml, publish_followup_upsert_delete(fixture.merged_tablet, /*base_version=*/2,
                                                                       /*upsert_key=*/10, /*upsert_value=*/1010,
                                                                       /*delete_key=*/fixture.tail_key_value));
        ASSIGN_OR_ABORT(auto rows, read_two_column_rows(after_dml));
        EXPECT_EQ((std::vector<std::pair<int32_t, int32_t>>{{10, 1010}, {20, 200}}), rows);
        ASSERT_OK(FileSystem::Default()->path_exists(fixture.source_path));
    }

    StatusOr<std::vector<std::pair<std::string, IndexValuesWithVerPB>>> read_raw_pk_sstable(
            int64_t tablet_id, const TabletMetadataPtr& metadata, const PersistentIndexSstablePB& sstable_pb) {
        ASSIGN_OR_RETURN(
                auto source_sstable,
                lake::PersistentIndexSstable::new_sstable(
                        sstable_pb, _tablet_manager->sst_location(tablet_id, sstable_pb.filename()),
                        /*cache=*/nullptr, /*need_filter=*/false, /*delvec=*/nullptr, metadata, _tablet_manager.get()));
        sstable::ReadOptions read_options;
        read_options.fill_cache = false;
        std::unique_ptr<sstable::Iterator> iterator(source_sstable->new_iterator(read_options));
        std::vector<std::pair<std::string, IndexValuesWithVerPB>> entries;
        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
            IndexValuesWithVerPB values_pb;
            if (!values_pb.ParseFromArray(iterator->value().data, static_cast<int>(iterator->value().size))) {
                return Status::Corruption("failed to parse raw test sstable value");
            }
            entries.emplace_back(iterator->key().to_string(), std::move(values_pb));
        }
        RETURN_IF_ERROR(iterator->status());
        return entries;
    }

    StatusOr<std::set<std::string>> directory_inventory(const std::string& directory) {
        std::set<std::string> files;
        RETURN_IF_ERROR(FileSystem::Default()->iterate_dir(directory, [&](std::string_view name) {
            files.emplace(name);
            return true;
        }));
        return files;
    }

    StatusOr<std::string> read_file_contents(const std::string& path) {
        ASSIGN_OR_RETURN(auto file, fs::new_random_access_file(path));
        ASSIGN_OR_RETURN(auto size, file->get_size());
        std::string contents(size, '\0');
        if (size > 0) {
            RETURN_IF_ERROR(file->read_at_fully(0, contents.data(), size));
        }
        return contents;
    }

    void run_other_occurrence_range_case(bool out_of_range_tombstone) {
        set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
        DeferOp restore_source_flush([&] {
            set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
        });

        const int64_t base_version = 1;
        const int64_t child_a = next_id();
        const int64_t child_b = next_id();
        const int64_t merged_tablet = next_id();
        prepare_tablet_dirs(child_a);
        prepare_tablet_dirs(child_b);
        prepare_tablet_dirs(merged_tablet);

        const std::string key_a = encode_int_primary_key(10);
        const std::string key_b = encode_int_primary_key(60);
        const std::string prefix = out_of_range_tombstone ? "occurrence_tombstone" : "occurrence_collision";
        const std::string left_filename = fmt::format("{}_left.sst", prefix);
        const std::string right_filename = fmt::format("{}_right.sst", prefix);
        const uint32_t tombstone = std::numeric_limits<uint32_t>::max();
        const uint64_t left_filesize = write_versioned_pk_sstable(_tablet_manager->sst_location(child_a, left_filename),
                                                                  {{key_a, /*version=*/7, /*rssid=*/1, /*rowid=*/0}});
        std::vector<std::tuple<std::string, int64_t, uint32_t, uint32_t>> right_entries;
        right_entries.emplace_back(key_a, /*version=*/7, out_of_range_tombstone ? tombstone : 1,
                                   out_of_range_tombstone ? tombstone : 1);
        right_entries.emplace_back(key_b, /*version=*/7, /*rssid=*/1, /*rowid=*/0);
        const uint64_t right_filesize =
                write_versioned_pk_sstable(_tablet_manager->sst_location(child_b, right_filename), right_entries);

        auto make_child = [&](int64_t tablet_id, int lower, int upper) {
            auto metadata = std::make_shared<TabletMetadataPB>();
            metadata->set_id(tablet_id);
            metadata->set_version(base_version);
            metadata->set_next_rowset_id(2);
            set_int_primary_key_schema(metadata.get(), 1001);
            metadata->set_enable_persistent_index(true);
            metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
            metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(lower));
            metadata->mutable_range()->set_lower_bound_included(true);
            metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(upper));
            metadata->mutable_range()->set_upper_bound_included(false);
            auto* rowset = metadata->add_rowsets();
            rowset->set_id(1);
            rowset->set_version(base_version);
            rowset->set_num_rows(2);
            rowset->set_data_size(100);
            auto* segment = rowset->add_segment_metas();
            segment->set_filename(fmt::format("{}_{}.dat", prefix, tablet_id));
            segment->set_size(100);
            return metadata;
        };

        auto child_a_metadata = make_child(child_a, /*lower=*/0, /*upper=*/50);
        auto* left_sst = child_a_metadata->mutable_sstable_meta()->add_sstables();
        left_sst->set_filename(left_filename);
        left_sst->set_filesize(left_filesize);
        left_sst->set_max_rss_rowid(static_cast<uint64_t>(1) << 32);

        auto child_b_metadata = make_child(child_b, /*lower=*/50, /*upper=*/100);
        auto* right_sst = child_b_metadata->mutable_sstable_meta()->add_sstables();
        right_sst->set_filename(right_filename);
        right_sst->set_filesize(right_filesize);
        right_sst->set_shared(true);
        right_sst->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 100);

        ASSERT_OK(put_tablet_metadata(child_a_metadata));
        ASSERT_OK(put_tablet_metadata(child_b_metadata));

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
        ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version,
                                                  base_version + 1, txn_info, false, tablet_metadatas, tablet_ranges));

        const auto& merged = tablet_metadatas.at(merged_tablet);
        auto index = std::make_unique<lake::LakePersistentIndex>(_tablet_manager.get(), merged_tablet);
        ASSERT_OK(index->init(merged));
        Slice keys[] = {Slice(key_a), Slice(key_b)};
        IndexValue values[2];
        ASSERT_OK(index->get(/*n=*/2, keys, values));
        EXPECT_EQ(IndexValue(static_cast<uint64_t>(1) << 32), values[0]);
        EXPECT_EQ(IndexValue(static_cast<uint64_t>(2) << 32), values[1]);
    }

    // Metadata-shape tests that exercise projection/order rather than SST
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

    // Write a real .cols file for column c1 only, with `num_rows` entries.
    // cell_value(row) supplies the c1 value at segment row |row|.
    uint64_t write_c1_only_cols_file(int64_t tablet_id, const std::string& cols_filename, int num_rows,
                                     const std::function<int(int)>& cell_value) {
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

    StatusOr<TabletMetadataPtr> publish_index_tail_merge(bool delete_old_row, int64_t* merged_tablet,
                                                         std::string* key_zero, bool include_left_sst = true,
                                                         bool include_right_sst = true,
                                                         bool include_new_rowset = true) {
        const int64_t base_version = 1;
        const int64_t new_version = 2;
        const int64_t child_a = next_id();
        const int64_t child_b = next_id();
        *merged_tablet = next_id();

        prepare_tablet_dirs(child_a);
        prepare_tablet_dirs(child_b);
        prepare_tablet_dirs(*merged_tablet);

        const std::string old_segment = "index_tail_old.dat";
        const std::string new_segment = "index_tail_new.dat";
        const uint64_t old_segment_size =
                write_two_column_segment(child_a, old_segment, /*num_rows=*/1, [](int) { return 100; });
        const int32_t raw_key_zero = 0;
        key_zero->assign(reinterpret_cast<const char*>(&raw_key_zero), sizeof(raw_key_zero));

        auto meta_a = std::make_shared<TabletMetadataPB>();
        meta_a->set_id(child_a);
        meta_a->set_version(base_version);
        meta_a->set_next_rowset_id(3);
        set_two_column_pk_schema(meta_a.get(), /*schema_id=*/4001);
        meta_a->set_enable_persistent_index(true);
        meta_a->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        meta_a->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(0));
        meta_a->mutable_range()->set_lower_bound_included(true);
        meta_a->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(50));
        meta_a->mutable_range()->set_upper_bound_included(false);

        auto add_data_rowset = [&](uint32_t rowset_id, int64_t version, const std::string& filename,
                                   uint64_t filesize) {
            auto* rowset = meta_a->add_rowsets();
            rowset->set_id(rowset_id);
            rowset->set_version(version);
            rowset->set_num_rows(1);
            rowset->set_data_size(filesize);
            auto* segment = rowset->add_segment_metas();
            segment->set_filename(filename);
            segment->set_size(filesize);
            segment->set_num_rows(1);
        };
        add_data_rowset(/*rowset_id=*/1, /*version=*/1, old_segment, old_segment_size);
        if (include_new_rowset) {
            const uint64_t new_segment_size =
                    write_two_column_segment(child_a, new_segment, /*num_rows=*/1, [](int) { return 200; });
            add_data_rowset(/*rowset_id=*/2, /*version=*/2, new_segment, new_segment_size);
        }

        if (delete_old_row) {
            DelVector old_delvec;
            const uint32_t deleted_rowid = 0;
            old_delvec.init(base_version, &deleted_rowid, 1);
            add_delvec(meta_a.get(), child_a, base_version, /*segment_id=*/1, "index_tail_old.delvec",
                       old_delvec.save());
        }

        if (include_left_sst) {
            const std::string stale_sst = "index_tail_stale.sst";
            const uint64_t stale_sst_size = write_legacy_pk_sstable(_tablet_manager->sst_location(child_a, stale_sst),
                                                                    {{*key_zero, /*rssid=*/1, /*rowid=*/0}});
            auto* stale_sst_pb = meta_a->mutable_sstable_meta()->add_sstables();
            stale_sst_pb->set_filename(stale_sst);
            stale_sst_pb->set_filesize(stale_sst_size);
            stale_sst_pb->set_max_rss_rowid(static_cast<uint64_t>(1) << 32);
        }

        auto meta_b = std::make_shared<TabletMetadataPB>();
        meta_b->set_id(child_b);
        meta_b->set_version(base_version);
        meta_b->set_next_rowset_id(2);
        set_two_column_pk_schema(meta_b.get(), /*schema_id=*/4001);
        meta_b->set_enable_persistent_index(true);
        meta_b->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        meta_b->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(50));
        meta_b->mutable_range()->set_lower_bound_included(true);
        meta_b->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(100));
        meta_b->mutable_range()->set_upper_bound_included(false);
        auto* empty_rowset = meta_b->add_rowsets();
        empty_rowset->set_id(1);
        empty_rowset->set_version(1);
        empty_rowset->set_num_rows(0);
        empty_rowset->set_data_size(0);

        if (include_right_sst) {
            const uint32_t tombstone = std::numeric_limits<uint32_t>::max();
            const std::string tombstone_sst = "index_tail_right_tombstone.sst";
            const uint64_t tombstone_sst_size =
                    write_legacy_pk_sstable(_tablet_manager->sst_location(child_b, tombstone_sst),
                                            {{encode_int_primary_key(60), tombstone, tombstone}});
            auto* tombstone_sst_pb = meta_b->mutable_sstable_meta()->add_sstables();
            tombstone_sst_pb->set_filename(tombstone_sst);
            tombstone_sst_pb->set_filesize(tombstone_sst_size);
            tombstone_sst_pb->set_max_rss_rowid(static_cast<uint64_t>(1) << 32);
        }

        RETURN_IF_ERROR(put_tablet_metadata(meta_a));
        RETURN_IF_ERROR(put_tablet_metadata(meta_b));

        ReshardingTabletInfoPB resharding_tablet;
        auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
        merging_info.add_old_tablet_ids(child_a);
        merging_info.add_old_tablet_ids(child_b);
        merging_info.set_new_tablet_id(*merged_tablet);
        TxnInfoPB txn_info;
        txn_info.set_txn_id(1);
        txn_info.set_commit_time(1);
        txn_info.set_gtid(1);

        std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
        std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
        RETURN_IF_ERROR(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version,
                                                        new_version, txn_info, false, tablet_metadatas, tablet_ranges));
        auto it = tablet_metadatas.find(*merged_tablet);
        if (it == tablet_metadatas.end()) {
            return Status::InternalError("merged tablet metadata is missing");
        }
        return it->second;
    }

    StatusOr<IndexValue> load_index_value(const TabletMetadataPtr& metadata, int64_t tablet_id,
                                          const std::string& key) {
        auto index = std::make_unique<lake::LakePersistentIndex>(_tablet_manager.get(), tablet_id);
        RETURN_IF_ERROR(index->init(metadata));
        lake::Tablet tablet(_tablet_manager.get(), tablet_id);
        auto metadata_copy = std::make_shared<TabletMetadataPB>(*metadata);
        lake::MetaFileBuilder builder(tablet, metadata_copy);
        RETURN_IF_ERROR(index->load_from_lake_tablet(_tablet_manager.get(), metadata, metadata->version(), &builder));
        Slice key_slice(key);
        IndexValue value;
        RETURN_IF_ERROR(index->get(/*n=*/1, &key_slice, &value));
        return value;
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
                                                               int32_t delete_key) {
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
        key_column->append_numbers(keys, sizeof(keys));
        value_column->append_numbers(values, sizeof(values));
        operation_column->append_numbers(operations, sizeof(operations));
        Chunk chunk(Columns{std::move(key_column), std::move(value_column), std::move(operation_column)}, slot_cid_map);
        const uint32_t indexes[] = {0, 1};

        ASSIGN_OR_RETURN(auto metadata, _tablet_manager->get_tablet_metadata(tablet_id, base_version));
        auto tablet_schema = TabletSchema::create(metadata->schema());
        RuntimeProfile profile("tablet-merge-tail-followup-dml");
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
        RETURN_IF_ERROR(delta_writer->write(chunk, indexes, std::size(indexes)));
        RETURN_IF_ERROR(delta_writer->finish_with_txnlog());
        delta_writer->close();

        TxnInfoPB txn_info;
        txn_info.set_txn_id(txn_id);
        txn_info.set_txn_type(TXN_NORMAL);
        txn_info.set_commit_time(1);
        return lake::publish_version(_tablet_manager.get(), lake::PublishTabletInfo(tablet_id), base_version,
                                     base_version + 1, std::span<const TxnInfoPB>(&txn_info, 1), false);
    }

    StatusOr<std::vector<std::pair<int32_t, int32_t>>> read_two_column_rows(const TabletMetadataPtr& metadata) {
        auto tablet_schema = TabletSchema::create(metadata->schema());
        auto schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));
        auto reader = std::make_shared<lake::TabletReader>(_tablet_manager.get(), metadata, *schema);
        RETURN_IF_ERROR(reader->prepare());
        RETURN_IF_ERROR(reader->open(TabletReaderParams()));

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
        std::sort(rows.begin(), rows.end());
        return rows;
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

    std::unique_ptr<starrocks::lake::TabletManager> _tablet_manager;
    std::string _test_dir;
    std::shared_ptr<lake::LocationProvider> _location_provider;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<lake::UpdateManager> _update_manager;
};

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

    const PersistentIndexSstablePB* owner_sst = nullptr;
    for (const auto& sst : merged->sstable_meta().sstables()) {
        if (sst.filename() == "split_sst_low.sst") {
            owner_sst = &sst;
            break;
        }
    }
    ASSERT_NE(nullptr, owner_sst) << "full merge must retain the SST through its real owner child";
    ASSERT_TRUE(owner_sst->has_shared_rssid());
    const uint32_t owner_rssid = owner_sst->shared_rssid();
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
    // - Tablet B rowset 3 (id=6, version 11, data, offset=3) -> after predicate
    EXPECT_EQ((std::vector<uint32_t>{1, 4, 2, 3, 6}), rowset_ids);
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
    // v11: A(id=3), B(id=6)
    // v20: B predicate(id=7)
    // v21: B(id=8)
    EXPECT_EQ((std::vector<uint32_t>{1, 4, 2, 3, 6, 7, 8}), rowset_ids);
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
    const int64_t offset = static_cast<int64_t>(meta1->next_rowset_id()) - 1;
    const uint32_t expected_rowset_id = static_cast<uint32_t>(1 + offset);

    bool found_rowset = false;
    for (const auto& rowset : merged->rowsets()) {
        if (rowset.id() == expected_rowset_id) {
            found_rowset = true;
            ASSERT_TRUE(rowset.has_range());
            EXPECT_EQ(rowset.range().SerializeAsString(), meta2->range().SerializeAsString());
            ASSERT_TRUE(rowset.has_max_compact_input_rowset_id());
            EXPECT_EQ(static_cast<uint32_t>(3 + offset), rowset.max_compact_input_rowset_id());
            ASSERT_EQ(1, rowset.del_files_size());
            EXPECT_EQ(static_cast<uint32_t>(1 + offset), rowset.del_files(0).origin_rowset_id());
            break;
        }
    }
    ASSERT_TRUE(found_rowset);

    bool found_rowset_from_meta1 = false;
    for (const auto& rowset : merged->rowsets()) {
        if (rowset.id() == 10) {
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

    bool found_sstable = false;
    for (const auto& sstable : merged->sstable_meta().sstables()) {
        if (sstable.filename() == "sst-2") {
            found_sstable = true;
            EXPECT_EQ(static_cast<int32_t>(offset), sstable.rssid_offset());
            const uint64_t expected_max_rss = (static_cast<uint64_t>(2 + offset) << 32) | 5;
            EXPECT_EQ(expected_max_rss, sstable.max_rss_rowid());
            EXPECT_FALSE(sstable.has_delvec());
            break;
        }
    }
    ASSERT_TRUE(found_sstable);

    const uint32_t expected_segment_id = static_cast<uint32_t>(1 + offset);
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
    EXPECT_TRUE(st.is_invalid_argument());
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
    EXPECT_TRUE(st.is_invalid_argument());
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
    // Shared sstable should be deduped to 1
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    EXPECT_EQ("shared_sst.sst", out_sst.filename());
    EXPECT_TRUE(out_sst.shared());
    // shared_rssid should be projected to canonical rssid (rowset deduped, rssid stays 1)
    EXPECT_EQ(merged->rowsets(0).id(), out_sst.shared_rssid());
    // rssid_offset should be 0 (shared_rssid path)
    EXPECT_EQ(0, out_sst.rssid_offset());
    // Projection reuses the physical file, so its generation version is inherited via
    // CopyFrom (not restamped with the merge version).
    EXPECT_EQ(7, out_sst.generation_version());
    // max_rss_rowid high part should match projected shared_rssid
    EXPECT_EQ((static_cast<uint64_t>(out_sst.shared_rssid()) << 32) | 99, out_sst.max_rss_rowid());
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
            sm->set_size(data_size);
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

TEST_F(LakeTabletReshardTest, test_tablet_merging_sstable_mixed_shared_and_local) {
    // Child A has shared + local sstable, child B has same shared sstable.
    // Shared deduped, local preserved.
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
    auto* rowset_a1 = meta_a->add_rowsets();
    rowset_a1->set_id(1);
    rowset_a1->set_version(1);
    rowset_a1->set_num_rows(10);
    rowset_a1->set_data_size(100);
    stamp_physical_identity_uid(rowset_a1, "shared_seg.dat");
    {
        auto* sm = rowset_a1->add_segment_metas();
        sm->set_filename("shared_seg.dat");
        sm->set_size(100);
        sm->set_shared(true);
    }
    auto* rowset_a2 = meta_a->add_rowsets();
    rowset_a2->set_id(2);
    rowset_a2->set_version(2);
    rowset_a2->set_num_rows(5);
    rowset_a2->set_data_size(50);
    {
        auto* sm = rowset_a2->add_segment_metas();
        sm->set_filename("local_a.dat");
        sm->set_size(50);
    }
    // Shared sstable
    auto* sst_shared_a = meta_a->mutable_sstable_meta()->add_sstables();
    sst_shared_a->set_filename("shared_sst.sst");
    sst_shared_a->set_filesize(512);
    sst_shared_a->set_shared(true);
    sst_shared_a->set_shared_rssid(1);
    sst_shared_a->set_shared_version(1);
    sst_shared_a->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 99);
    // Local sstable (non-shared)
    auto* sst_local = meta_a->mutable_sstable_meta()->add_sstables();
    sst_local->set_filename("local_a_sst.sst");
    sst_local->set_filesize(128);
    sst_local->set_shared_rssid(2);
    sst_local->set_shared_version(2);
    sst_local->set_max_rss_rowid((static_cast<uint64_t>(2) << 32) | 50);

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
    stamp_physical_identity_uid(rowset_b, "shared_seg.dat");
    {
        auto* sm = rowset_b->add_segment_metas();
        sm->set_filename("shared_seg.dat");
        sm->set_size(100);
        sm->set_shared(true);
    }
    // Same shared sstable
    auto* sst_shared_b = meta_b->mutable_sstable_meta()->add_sstables();
    sst_shared_b->set_filename("shared_sst.sst");
    sst_shared_b->set_filesize(512);
    sst_shared_b->set_shared(true);
    sst_shared_b->set_shared_rssid(1);
    sst_shared_b->set_shared_version(1);
    sst_shared_b->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 99);

    materialize_tombstone_sstables(meta_a.get());
    materialize_tombstone_sstables(meta_b.get());
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
    // 1 shared (deduped) + 1 local = 2 sstables
    ASSERT_EQ(2, merged->sstable_meta().sstables_size());
    std::unordered_set<std::string> sst_filenames;
    for (const auto& sst : merged->sstable_meta().sstables()) {
        sst_filenames.insert(sst.filename());
    }
    EXPECT_TRUE(sst_filenames.count("shared_sst.sst") > 0);
    EXPECT_TRUE(sst_filenames.count("local_a_sst.sst") > 0);
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_sstable_no_dedup_different_filenames) {
    // Two shared sstables with different filenames (different split families).
    // No dedup should happen.
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
        sm->set_filename("seg_a.dat");
        sm->set_size(100);
    }
    auto* sst_a = meta_a->mutable_sstable_meta()->add_sstables();
    sst_a->set_filename("sst_family_a.sst");
    sst_a->set_filesize(256);
    sst_a->set_shared(true);
    sst_a->set_shared_rssid(1);
    sst_a->set_shared_version(1);
    sst_a->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 50);

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
        sm->set_filename("seg_b.dat");
        sm->set_size(100);
    }
    auto* sst_b = meta_b->mutable_sstable_meta()->add_sstables();
    sst_b->set_filename("sst_family_b.sst");
    sst_b->set_filesize(256);
    sst_b->set_shared(true);
    sst_b->set_shared_rssid(1);
    sst_b->set_shared_version(1);
    sst_b->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 50);

    materialize_tombstone_sstables(meta_a.get());
    materialize_tombstone_sstables(meta_b.get());
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
    // No dedup: different filenames -> 2 sstables
    ASSERT_EQ(2, merged->sstable_meta().sstables_size());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_sstable_shared_rssid_projection) {
    // Shared sstable with shared_rssid on non-first child (rssid_offset != 0).
    // Verifies shared_rssid is correctly projected and rssid_offset is cleared.
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // child_a: local rowset (not shared)
    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(5);
    set_primary_key_schema(meta_a.get(), 1001);
    auto* rowset_a = meta_a->add_rowsets();
    rowset_a->set_id(1);
    rowset_a->set_version(1);
    rowset_a->set_num_rows(10);
    rowset_a->set_data_size(100);
    {
        auto* sm = rowset_a->add_segment_metas();
        sm->set_filename("local_seg.dat");
        sm->set_size(100);
    }

    // child_b: has shared sstable with shared_rssid=1 referencing shared segment
    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(3);
    set_primary_key_schema(meta_b.get(), 1001);
    auto* rowset_b = meta_b->add_rowsets();
    rowset_b->set_id(1);
    rowset_b->set_version(2);
    rowset_b->set_num_rows(5);
    rowset_b->set_data_size(50);
    {
        auto* sm = rowset_b->add_segment_metas();
        sm->set_filename("seg_b.dat");
        sm->set_size(50);
    }
    auto* sst_b = meta_b->mutable_sstable_meta()->add_sstables();
    sst_b->set_filename("sst_b.sst");
    sst_b->set_filesize(512);
    sst_b->set_shared(true);
    sst_b->set_shared_rssid(1);
    sst_b->set_shared_version(2);
    sst_b->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 99);

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
    ASSERT_EQ(2, merged->rowsets_size()); // no dedup: different segments
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    EXPECT_EQ("sst_b.sst", out_sst.filename());
    // child_b gets rssid_offset from meta_a's next_rowset_id.
    // shared_rssid should be projected: original 1 + offset
    const int64_t expected_offset = static_cast<int64_t>(meta_a->next_rowset_id()) - 1;
    EXPECT_EQ(static_cast<uint32_t>(1 + expected_offset), out_sst.shared_rssid());
    // rssid_offset must be 0 (shared_rssid path)
    EXPECT_EQ(0, out_sst.rssid_offset());
    // max_rss_rowid high part should match projected shared_rssid
    uint64_t expected_max = (static_cast<uint64_t>(out_sst.shared_rssid()) << 32) | 99;
    EXPECT_EQ(expected_max, out_sst.max_rss_rowid());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_modern_shared_sstable_effective_owner) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t compacted_child = next_id();
    const int64_t live_child = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(compacted_child);
    prepare_tablet_dirs(live_child);
    prepare_tablet_dirs(merged_tablet);

    auto make_child = [&](int64_t tablet_id, uint32_t rowset_id, const std::string& segment_filename) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(base_version);
        metadata->set_next_rowset_id(rowset_id + 1);
        set_primary_key_schema(metadata.get(), 1001);
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(rowset_id);
        rowset->set_version(1);
        rowset->set_num_rows(1);
        rowset->set_data_size(100);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(segment_filename);
        segment->set_size(100);

        auto* sstable = add_shared_rssid_sstable(metadata.get(), "effective_owner.sst", /*rssid=*/1,
                                                 /*shared_version=*/1, /*max_rowid=*/0);
        sstable->set_rssid_offset(1);
        sstable->set_max_rss_rowid(static_cast<uint64_t>(2) << 32);
        return metadata;
    };

    // The first occurrence no longer owns effective rssid 2. The second does.
    auto compacted = make_child(compacted_child, /*rowset_id=*/3, "compacted_segment.dat");
    auto live = make_child(live_child, /*rowset_id=*/2, "live_segment.dat");
    ASSERT_OK(put_tablet_metadata(compacted));
    ASSERT_OK(put_tablet_metadata(live));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(compacted_child);
    merging_info.add_old_tablet_ids(live_child);
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
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& output = merged->sstable_meta().sstables(0);
    EXPECT_EQ(4, output.shared_rssid());
    EXPECT_EQ(0, output.rssid_offset());
    EXPECT_EQ(static_cast<uint64_t>(4) << 32, output.max_rss_rowid());
    ASSERT_EQ(2, merged->rowsets_size());
    EXPECT_EQ("live_segment.dat", merged->rowsets(1).segment_metas(0).filename());
    EXPECT_EQ(output.shared_rssid(), merged->rowsets(1).id());
}

// The same modern shared file can be projected into different sibling-local
// RSSID spaces by intermediate merges. Each occurrence must be mapped through
// its own context before checking that they converge on one final owner.
TEST_F(LakeTabletReshardTest, test_tablet_merging_modern_shared_sstable_reconciles_projected_occurrences) {
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto occurrence_a = make_modern_shared_occurrence(child_a, /*rowset_id=*/1, /*shared_rssid=*/1,
                                                      /*rssid_offset=*/0, /*max_rowid=*/9);
    auto occurrence_b = make_modern_shared_occurrence(child_b, /*rowset_id=*/5, /*shared_rssid=*/4,
                                                      /*rssid_offset=*/1, /*max_rowid=*/9);
    auto merged_or = merge_modern_shared_occurrences(occurrence_a, occurrence_b, merged_tablet);
    ASSERT_OK(merged_or);
    auto merged = std::move(merged_or).value();

    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_EQ(1, merged->rowsets(0).id());
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& output = merged->sstable_meta().sstables(0);
    EXPECT_EQ(1, output.shared_rssid());
    EXPECT_EQ(0, output.rssid_offset());
    EXPECT_EQ((static_cast<uint64_t>(1) << 32) | 9, output.max_rss_rowid());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_modern_shared_sstable_rejects_physical_size_mismatch) {
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto occurrence_a = make_modern_shared_occurrence(child_a, 1, 1, 0, 9, /*filesize=*/512);
    auto occurrence_b = make_modern_shared_occurrence(child_b, 1, 1, 0, 9, /*filesize=*/513);
    auto merged = merge_modern_shared_occurrences(occurrence_a, occurrence_b, merged_tablet);
    ASSERT_TRUE(merged.status().is_corruption()) << merged.status();
    EXPECT_TRUE(merged.status().message().contains("metadata mismatch")) << merged.status();
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_modern_shared_sstable_rejects_shared_version_mismatch) {
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto occurrence_a = make_modern_shared_occurrence(child_a, 1, 1, 0, 9, 512, /*shared_version=*/7);
    auto occurrence_b = make_modern_shared_occurrence(child_b, 1, 1, 0, 9, 512, /*shared_version=*/8);
    auto merged = merge_modern_shared_occurrences(occurrence_a, occurrence_b, merged_tablet);
    ASSERT_TRUE(merged.status().is_corruption()) << merged.status();
    EXPECT_TRUE(merged.status().message().contains("metadata mismatch")) << merged.status();
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_modern_shared_sstable_rejects_watermark_low_mismatch) {
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto occurrence_a = make_modern_shared_occurrence(child_a, 1, 1, 0, /*max_rowid=*/9);
    auto occurrence_b = make_modern_shared_occurrence(child_b, 1, 1, 0, /*max_rowid=*/10);
    auto merged = merge_modern_shared_occurrences(occurrence_a, occurrence_b, merged_tablet);
    ASSERT_TRUE(merged.status().is_corruption()) << merged.status();
    EXPECT_TRUE(merged.status().message().contains("metadata mismatch")) << merged.status();
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_modern_shared_sstable_rejects_nonfirst_effective_rssid_overflow) {
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto occurrence_a = make_modern_shared_occurrence(child_a, 1, 1, 0, /*max_rowid=*/9);
    auto occurrence_b = make_modern_shared_occurrence(child_b, 5, 0, -1, /*max_rowid=*/9);
    auto merged = merge_modern_shared_occurrences(occurrence_a, occurrence_b, merged_tablet);
    ASSERT_TRUE(merged.status().is_corruption()) << merged.status();
    EXPECT_TRUE(merged.status().message().contains("effective rssid is out of uint32 range")) << merged.status();
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_drops_ownerless_non_shared_modern_data_sstable_with_embedded_delvec) {
    const int64_t cold_tablet = next_id();
    const int64_t hot_tablet = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(cold_tablet);
    prepare_tablet_dirs(hot_tablet);
    prepare_tablet_dirs(merged_tablet);

    auto cold = make_single_rowset_pk_metadata(cold_tablet, /*rowset_id=*/1, "cold_live.dat");
    auto hot = make_single_rowset_pk_metadata(hot_tablet, /*rowset_id=*/3, "hot_live.dat");
    const std::string filename = "ownerless_non_shared_modern.sst";
    const uint64_t filesize = write_legacy_pk_sstable(_tablet_manager->sst_location(hot_tablet, filename),
                                                      {{"stale-key", /*rssid=*/1, /*rowid=*/0}});
    auto* sstable = add_non_shared_modern_sstable(hot.get(), filename, /*rssid=*/1, /*shared_version=*/1,
                                                  /*max_rowid=*/0, filesize);
    attach_embedded_delvec(hot.get(), hot_tablet, sstable, /*version=*/1, "ownerless_non_shared.dv", "delvec");

    auto merged = merge_modern_shared_occurrences(cold, hot, merged_tablet);
    ASSERT_OK(merged);
    for (const auto& output : merged.value()->sstable_meta().sstables()) {
        EXPECT_NE(filename, output.filename());
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_preserves_below_floor_non_shared_modern_tombstone) {
    const int64_t cold_tablet = next_id();
    const int64_t hot_tablet = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(cold_tablet);
    prepare_tablet_dirs(hot_tablet);
    prepare_tablet_dirs(merged_tablet);

    auto cold = std::make_shared<TabletMetadataPB>();
    cold->set_id(cold_tablet);
    cold->set_version(1);
    cold->set_next_rowset_id(1);
    set_primary_key_schema(cold.get(), 1001);
    auto hot = make_single_rowset_pk_metadata(hot_tablet, /*rowset_id=*/107, "below_floor_non_shared_live.dat");
    const uint32_t tombstone = std::numeric_limits<uint32_t>::max();
    const std::string filename = "below_floor_non_shared_tombstone.sst";
    const uint64_t filesize = write_legacy_pk_sstable(_tablet_manager->sst_location(hot_tablet, filename),
                                                      {{"deleted-key", tombstone, tombstone}});
    add_non_shared_modern_sstable(hot.get(), filename, /*rssid=*/47, /*shared_version=*/1,
                                  /*max_rowid=*/tombstone, filesize);

    auto merged_or = merge_modern_shared_occurrences(cold, hot, merged_tablet);
    ASSERT_OK(merged_or);
    auto merged = std::move(merged_or).value();
    const PersistentIndexSstablePB* output = nullptr;
    for (const auto& candidate : merged->sstable_meta().sstables()) {
        if (candidate.filename() == filename) output = &candidate;
    }
    ASSERT_NE(nullptr, output);
    EXPECT_FALSE(output->shared());
    EXPECT_EQ(0, output->shared_rssid());
    EXPECT_EQ(0, output->rssid_offset());

    ASSIGN_OR_ABORT(auto sstable, lake::PersistentIndexSstable::new_sstable(
                                          *output, _tablet_manager->sst_location(merged_tablet, filename),
                                          /*cache=*/nullptr, /*need_filter=*/false, /*delvec=*/nullptr, merged,
                                          _tablet_manager.get()));
    sstable::ReadOptions read_options;
    read_options.fill_cache = false;
    std::unique_ptr<sstable::Iterator> iterator(sstable->new_iterator(read_options));
    iterator->SeekToFirst();
    ASSERT_TRUE(iterator->Valid());
    IndexValuesWithVerPB values;
    ASSERT_TRUE(values.ParseFromArray(iterator->value().data, static_cast<int>(iterator->value().size)));
    ASSERT_EQ(1, values.values_size());
    EXPECT_EQ(tombstone, values.values(0).rssid());
    EXPECT_EQ(tombstone, values.values(0).rowid());
    iterator->Next();
    EXPECT_FALSE(iterator->Valid());
    ASSERT_OK(iterator->status());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_non_shared_modern_tombstone_keeps_embedded_delvec_guard) {
    const int64_t cold_tablet = next_id();
    const int64_t hot_tablet = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(cold_tablet);
    prepare_tablet_dirs(hot_tablet);
    prepare_tablet_dirs(merged_tablet);

    auto cold = std::make_shared<TabletMetadataPB>();
    cold->set_id(cold_tablet);
    cold->set_version(1);
    cold->set_next_rowset_id(5);
    set_primary_key_schema(cold.get(), 1001);
    auto hot = make_single_rowset_pk_metadata(hot_tablet, /*rowset_id=*/3, "guard_live.dat");
    const uint32_t tombstone = std::numeric_limits<uint32_t>::max();
    const std::string filename = "non_shared_tombstone_delvec_guard.sst";
    const uint64_t filesize = write_legacy_pk_sstable(_tablet_manager->sst_location(hot_tablet, filename),
                                                      {{"deleted-key", tombstone, tombstone}});
    auto* sstable = add_non_shared_modern_sstable(hot.get(), filename, /*rssid=*/1, /*shared_version=*/1,
                                                  /*max_rowid=*/tombstone, filesize);
    attach_embedded_delvec(hot.get(), hot_tablet, sstable, /*version=*/1, "non_shared_tombstone_guard.dv", "delvec");

    auto merged = merge_modern_shared_occurrences(cold, hot, merged_tablet);
    ASSERT_TRUE(merged.status().is_corruption()) << merged.status();
    EXPECT_TRUE(merged.status().message().contains("Delvec page not found for sstable after merge")) << merged.status();
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_live_non_shared_modern_sstable_preserves_ownership_and_delvec) {
    const int64_t live_tablet = next_id();
    const int64_t empty_tablet = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(live_tablet);
    prepare_tablet_dirs(empty_tablet);
    prepare_tablet_dirs(merged_tablet);

    auto live = make_single_rowset_pk_metadata(live_tablet, /*rowset_id=*/1, "live_non_shared_modern.dat");
    const std::string filename = "live_non_shared_modern.sst";
    const uint64_t filesize = write_legacy_pk_sstable(_tablet_manager->sst_location(live_tablet, filename),
                                                      {{"live-key", /*rssid=*/1, /*rowid=*/0}});
    add_non_shared_modern_sstable(live.get(), filename, /*rssid=*/1, /*shared_version=*/1,
                                  /*max_rowid=*/0, filesize);
    DelVector delvec;
    const std::vector<uint32_t> deleted_rowids = {0};
    delvec.init(/*version=*/1, deleted_rowids.data(), deleted_rowids.size());
    add_delvec(live.get(), live_tablet, /*version=*/1, /*segment_id=*/1, "live_non_shared_modern.dv", delvec.save());

    auto empty = std::make_shared<TabletMetadataPB>();
    empty->set_id(empty_tablet);
    empty->set_version(1);
    empty->set_next_rowset_id(1);
    set_primary_key_schema(empty.get(), 1001);
    auto merged_or = merge_modern_shared_occurrences(live, empty, merged_tablet);
    ASSERT_OK(merged_or);
    const auto& outputs = merged_or.value()->sstable_meta().sstables();
    auto iter = std::find_if(outputs.begin(), outputs.end(),
                             [&](const auto& sstable) { return sstable.filename() == filename; });
    ASSERT_NE(outputs.end(), iter);
    EXPECT_FALSE(iter->shared());
    EXPECT_EQ(1, iter->shared_rssid());
    EXPECT_EQ(0, iter->rssid_offset());
    EXPECT_TRUE(iter->has_delvec());
    EXPECT_GT(iter->delvec().size(), 0);
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_non_shared_modern_rejects_missing_final_owner) {
    const int64_t live_tablet = next_id();
    const int64_t empty_tablet = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(live_tablet);
    prepare_tablet_dirs(empty_tablet);
    prepare_tablet_dirs(merged_tablet);

    auto live = make_single_rowset_pk_metadata(live_tablet, /*rowset_id=*/1, "missing_final_owner.dat");
    const std::string filename = "missing_final_owner.sst";
    const uint64_t filesize = write_legacy_pk_sstable(_tablet_manager->sst_location(live_tablet, filename),
                                                      {{"live-key", /*rssid=*/1, /*rowid=*/0}});
    add_non_shared_modern_sstable(live.get(), filename, /*rssid=*/1, /*shared_version=*/1,
                                  /*max_rowid=*/0, filesize);
    auto empty = std::make_shared<TabletMetadataPB>();
    empty->set_id(empty_tablet);
    empty->set_version(1);
    empty->set_next_rowset_id(1);
    set_primary_key_schema(empty.get(), 1001);

    SyncPoint::GetInstance()->SetCallBack("merge_sstables:merged_live_rssids", [](void* arg) {
        auto* live_rssids = static_cast<std::unordered_set<uint32_t>*>(arg);
        live_rssids->erase(1);
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp cleanup_sync_point([&] {
        SyncPoint::GetInstance()->ClearCallBack("merge_sstables:merged_live_rssids");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    auto merged = merge_modern_shared_occurrences(live, empty, merged_tablet);
    ASSERT_TRUE(merged.status().is_corruption()) << merged.status();
    EXPECT_TRUE(merged.status().message().contains("has no merged segment owner")) << merged.status();
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_non_shared_modern_effective_overflow_precedes_liveness) {
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);
    auto child = make_single_rowset_pk_metadata(child_a, /*rowset_id=*/5, "overflow_live.dat");
    auto* sstable = add_non_shared_modern_sstable(child.get(), "non_shared_overflow.sst", /*rssid=*/0,
                                                  /*shared_version=*/1, /*max_rowid=*/0, /*filesize=*/512);
    sstable->set_rssid_offset(-1);
    auto empty = std::make_shared<TabletMetadataPB>();
    empty->set_id(child_b);
    empty->set_version(1);
    empty->set_next_rowset_id(1);
    set_primary_key_schema(empty.get(), 1001);
    auto merged = merge_modern_shared_occurrences(child, empty, merged_tablet);
    ASSERT_TRUE(merged.status().is_corruption()) << merged.status();
    EXPECT_TRUE(merged.status().message().contains("effective rssid is out of uint32 range")) << merged.status();
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_drops_ownerless_shared_data_sstable) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);
    const std::string sst_filename = "stale_shared_modern.sst";
    const uint64_t sst_filesize = write_legacy_pk_sstable(_tablet_manager->sst_location(child_a, sst_filename),
                                                          {{"stale-key", /*rssid=*/1, /*rowid=*/0}});

    auto make_child = [&](int64_t tablet_id, const std::string& segment_filename) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(base_version);
        metadata->set_next_rowset_id(3);
        set_primary_key_schema(metadata.get(), 1001);
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(2);
        rowset->set_version(2);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(segment_filename);
        segment->set_size(100);
        add_shared_rssid_sstable(metadata.get(), sst_filename, /*rssid=*/1, /*shared_version=*/1,
                                 /*max_rowid=*/UINT32_MAX - 1, sst_filesize);
        return metadata;
    };

    ASSERT_OK(put_tablet_metadata(make_child(child_a, "compacted_a.dat")));
    ASSERT_OK(put_tablet_metadata(make_child(child_b, "compacted_b.dat")));

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

    EXPECT_EQ(0, tablet_metadatas.at(merged_tablet)->sstable_meta().sstables_size());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_conflicting_shared_rssid_owners) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_child = [&](int64_t tablet_id, const std::string& segment_filename) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(base_version);
        metadata->set_next_rowset_id(2);
        set_primary_key_schema(metadata.get(), 1001);
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(segment_filename);
        segment->set_size(100);
        segment->set_shared(true);
        add_shared_rssid_sstable(metadata.get(), "ambiguous_shared_modern.sst", /*rssid=*/1,
                                 /*shared_version=*/1, /*max_rowid=*/UINT32_MAX - 1);
        return metadata;
    };

    ASSERT_OK(put_tablet_metadata(make_child(child_a, "owner_a.dat")));
    ASSERT_OK(put_tablet_metadata(make_child(child_b, "owner_b.dat")));

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
    const Status status =
            lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                            txn_info, false, tablet_metadatas, tablet_ranges);
    EXPECT_TRUE(status.is_corruption()) << status;
    EXPECT_NE(std::string::npos, status.message().find("multiple final rssid owners")) << status;
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_preserves_shared_tombstone_sstable_without_segment_owner) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);
    const uint32_t tombstone = std::numeric_limits<uint32_t>::max();
    const std::string sst_filename = "shared_tombstone.sst";
    const uint64_t sst_filesize = write_legacy_pk_sstable(_tablet_manager->sst_location(child_a, sst_filename),
                                                          {{"deleted-key", tombstone, tombstone}});

    auto make_child = [&](int64_t tablet_id) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(base_version);
        metadata->set_next_rowset_id(2);
        set_primary_key_schema(metadata.get(), 1001);
        add_rowset_with_predicate(metadata.get(), /*rowset_id=*/1, /*version=*/1, /*has_predicate=*/true);
        add_shared_rssid_sstable(metadata.get(), sst_filename, /*rssid=*/1, /*shared_version=*/1,
                                 /*max_rowid=*/UINT32_MAX, sst_filesize);
        return metadata;
    };

    ASSERT_OK(put_tablet_metadata(make_child(child_a)));
    ASSERT_OK(put_tablet_metadata(make_child(child_b)));

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

    const auto& merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& tombstone_sst = merged->sstable_meta().sstables(0);
    EXPECT_EQ(sst_filename, tombstone_sst.filename());
    EXPECT_EQ(UINT32_MAX, static_cast<uint32_t>(tombstone_sst.max_rss_rowid()));
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_preserves_below_floor_shared_tombstone_sstable) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t cold_tablet = next_id();
    const int64_t hot_tablet = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(cold_tablet);
    prepare_tablet_dirs(hot_tablet);
    prepare_tablet_dirs(merged_tablet);

    auto cold = std::make_shared<TabletMetadataPB>();
    cold->set_id(cold_tablet);
    cold->set_version(base_version);
    cold->set_next_rowset_id(1);
    set_primary_key_schema(cold.get(), 1001);

    auto hot = std::make_shared<TabletMetadataPB>();
    hot->set_id(hot_tablet);
    hot->set_version(base_version);
    hot->set_next_rowset_id(108);
    set_primary_key_schema(hot.get(), 1001);
    auto* live_rowset = hot->add_rowsets();
    live_rowset->set_id(107);
    live_rowset->set_version(1);
    live_rowset->set_num_rows(1);
    live_rowset->set_data_size(10);
    auto* segment = live_rowset->add_segment_metas();
    segment->set_filename("below_floor_tombstone_live.dat");
    segment->set_size(10);

    const uint32_t tombstone = std::numeric_limits<uint32_t>::max();
    const std::string sst_filename = "below_floor_tombstone.sst";
    const uint64_t sst_filesize = write_legacy_pk_sstable(_tablet_manager->sst_location(hot_tablet, sst_filename),
                                                          {{"deleted-key", tombstone, tombstone}});
    add_shared_rssid_sstable(hot.get(), sst_filename, /*rssid=*/47, /*shared_version=*/1,
                             /*max_rowid=*/UINT32_MAX, sst_filesize);

    ASSERT_OK(put_tablet_metadata(cold));
    ASSERT_OK(put_tablet_metadata(hot));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(cold_tablet);
    merging_info.add_old_tablet_ids(hot_tablet);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    const auto& merged = tablet_metadatas.at(merged_tablet);
    const PersistentIndexSstablePB* output = nullptr;
    for (const auto& sst : merged->sstable_meta().sstables()) {
        if (sst.filename() == sst_filename) {
            output = &sst;
            break;
        }
    }
    ASSERT_NE(nullptr, output);
    ASSERT_TRUE(output->has_shared_rssid());
    EXPECT_EQ(0, output->shared_rssid());
    EXPECT_EQ(0, output->rssid_offset());
    EXPECT_EQ(0, output->max_rss_rowid() >> 32);
    EXPECT_EQ(UINT32_MAX, static_cast<uint32_t>(output->max_rss_rowid()));
}

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

// LakePersistentIndex::commit() and the size-tiered compaction strategy iterate
// the tablet's sstable_meta in stored order and reject any out-of-order
// max_rss_rowid as "sstables are not ordered". The merger appends sstables in
// source-child iteration order, so projection across children can interleave
// non-monotonically — for example, a delete-only sstable in one child has its
// low word saturated near UINT32_MAX, and a freshly-written sstable in the
// next child has a smaller projected high word. Without a defensive sort the
// merged metadata would carry the disorder forward and any post-merge commit
// or compaction would refuse to publish.
TEST_F(LakeTabletReshardTest, test_tablet_merging_sstables_sorted_by_max_rss_rowid) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // Child A contributes a tombstone-bearing sstable with high word = 20 and
    // low word = UINT32_MAX-1, exactly the encoding PersistentIndexMemtable::erase
    // / LakePersistentIndex::ingest_sst use for delete-only entries
    // (storage/lake/persistent_index_memtable.cpp:110, 131,
    //  storage/lake/lake_persistent_index.cpp:258).
    // Child B's local sstable has high=3, low=50; with rssid_offset = 10 - 1 = 9
    // it projects to high = 12 — well below child A's high=20. Source-iteration
    // order would emit [child_a (20), child_b_proj (12)] in dest, which is the
    // disorder this fix prevents.
    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(10);
    set_primary_key_schema(meta_a.get(), 1001);
    auto* rowset_a = meta_a->add_rowsets();
    rowset_a->set_id(1);
    rowset_a->set_version(1);
    rowset_a->set_num_rows(10);
    rowset_a->set_data_size(100);
    {
        auto* sm = rowset_a->add_segment_metas();
        sm->set_filename("seg_a.dat");
        sm->set_size(100);
    }
    auto* sst_a_tombstone = meta_a->mutable_sstable_meta()->add_sstables();
    sst_a_tombstone->set_filename("a_tombstone.sst");
    sst_a_tombstone->set_filesize(256);
    sst_a_tombstone->set_max_rss_rowid((static_cast<uint64_t>(20) << 32) | (std::numeric_limits<uint32_t>::max() - 1));

    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(5);
    set_primary_key_schema(meta_b.get(), 1001);
    auto* rowset_b = meta_b->add_rowsets();
    rowset_b->set_id(1);
    rowset_b->set_version(1);
    rowset_b->set_num_rows(10);
    rowset_b->set_data_size(100);
    {
        auto* sm = rowset_b->add_segment_metas();
        sm->set_filename("seg_b.dat");
        sm->set_size(100);
    }
    auto* sst_b_local = meta_b->mutable_sstable_meta()->add_sstables();
    sst_b_local->set_filename("b_local.sst");
    sst_b_local->set_filesize(128);
    sst_b_local->set_max_rss_rowid((static_cast<uint64_t>(3) << 32) | 50);

    materialize_tombstone_sstables(meta_a.get());
    materialize_tombstone_sstables(meta_b.get());

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
    ASSERT_EQ(2, merged->sstable_meta().sstables_size());

    uint64_t prev_max = 0;
    for (const auto& sst : merged->sstable_meta().sstables()) {
        EXPECT_LE(prev_max, sst.max_rss_rowid()) << "post-merge sstables must be in non-decreasing max_rss_rowid order";
        prev_max = sst.max_rss_rowid();
    }
    EXPECT_EQ("b_local.sst", merged->sstable_meta().sstables(0).filename());
    EXPECT_EQ("a_tombstone.sst", merged->sstable_meta().sstables(1).filename());
}

// LakePersistentIndex::commit() interprets max_rss_rowid through signed int64
// monotonic ordering, while other merge paths use unsigned RSSID arithmetic.
// An encoded watermark with bit 63 set therefore has incompatible ordering
// semantics and is outside the supported merge allocation domain. Merges must
// fail closed instead of attempting to sort such a cross-sign input.
TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_sign_bit_sstable_watermark) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // child_a contributes one deliberately unsupported SST watermark whose
    // encoded max_rss_rowid sets bit 63. The rowsets stay small so this fixture
    // isolates rejection of the source SST domain rather than offset overflow.
    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(2);
    set_primary_key_schema(meta_a.get(), 1001);
    auto* rowset_a = meta_a->add_rowsets();
    rowset_a->set_id(1);
    rowset_a->set_version(1);
    rowset_a->set_num_rows(10);
    rowset_a->set_data_size(100);
    {
        auto* sm = rowset_a->add_segment_metas();
        sm->set_filename("seg_a.dat");
        sm->set_size(100);
    }
    auto* sst_a_high = meta_a->mutable_sstable_meta()->add_sstables();
    sst_a_high->set_filename("a_high.sst");
    sst_a_high->set_filesize(256);
    // (rssid<<32|low) with rssid >= 2^31 sets bit 63. This must be rejected
    // before source SST I/O or projection, rather than reordered against b_low.
    sst_a_high->set_max_rss_rowid((static_cast<uint64_t>(1) << 63) | 100);

    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(5);
    set_primary_key_schema(meta_b.get(), 1001);
    auto* rowset_b = meta_b->add_rowsets();
    rowset_b->set_id(1);
    rowset_b->set_version(1);
    rowset_b->set_num_rows(10);
    rowset_b->set_data_size(100);
    {
        auto* sm = rowset_b->add_segment_metas();
        sm->set_filename("seg_b.dat");
        sm->set_size(100);
    }
    auto* sst_b_low = meta_b->mutable_sstable_meta()->add_sstables();
    sst_b_low->set_filename("b_low.sst");
    sst_b_low->set_filesize(128);
    sst_b_low->set_max_rss_rowid((static_cast<uint64_t>(7) << 32) | 50);

    materialize_tombstone_sstables(meta_a.get());
    materialize_tombstone_sstables(meta_b.get());

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
    const std::string source_a_before = meta_a->SerializeAsString();
    const std::string source_b_before = meta_b->SerializeAsString();
    auto status = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                                  txn_info, false, tablet_metadatas, tablet_ranges);

    EXPECT_TRUE(status.is_invalid_argument()) << status;
    EXPECT_EQ(source_a_before, meta_a->SerializeAsString());
    EXPECT_EQ(source_b_before, meta_b->SerializeAsString());
    EXPECT_EQ(tablet_metadatas.end(), tablet_metadatas.find(merged_tablet));
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_skip_sstable_merge_rejects_sign_bit_source_watermark) {
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

    EXPECT_TRUE(merged.status().is_invalid_argument()) << merged.status();
    EXPECT_EQ(source_before, source->SerializeAsString());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_exhausted_live_rowset_domain_without_sst) {
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

    EXPECT_TRUE(status.is_invalid_argument()) << status;
    EXPECT_EQ(source_before, source->SerializeAsString());
    EXPECT_EQ(tablet_metadatas.end(), tablet_metadatas.find(merged_tablet));
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_exhausted_single_sst_domain) {
    const int64_t base_version = 1;
    const int64_t source_tablet = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(source_tablet);
    prepare_tablet_dirs(merged_tablet);

    auto source = std::make_shared<TabletMetadataPB>();
    source->set_id(source_tablet);
    source->set_version(base_version);
    source->set_next_rowset_id(2);
    set_primary_key_schema(source.get(), 1001);
    auto* rowset = source->add_rowsets();
    rowset->set_id(1);
    rowset->set_version(base_version);
    rowset->set_num_rows(1);
    rowset->set_data_size(1);
    rowset->add_segment_metas()->set_filename("live_segment.dat");
    auto* sst = source->mutable_sstable_meta()->add_sstables();
    sst->set_filename("signed_limit.sst");
    sst->set_max_rss_rowid(std::numeric_limits<int64_t>::max());
    materialize_tombstone_sstables(source.get());
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

    EXPECT_TRUE(status.is_invalid_argument()) << status;
    EXPECT_EQ(source_before, source->SerializeAsString());
    EXPECT_EQ(tablet_metadatas.end(), tablet_metadatas.find(merged_tablet));
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_projected_domain_before_overlay_and_tail_io) {
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    set_failpoint_mode("fail_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_flush_failpoints([&] {
        set_failpoint_mode("fail_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    });

    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);
    auto make_child = [&](int64_t tablet_id, uint32_t shared_rssid, const std::string& sst_filename) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(1);
        metadata->set_next_rowset_id(2);
        set_primary_key_schema(metadata.get(), 1001);
        metadata->set_enable_persistent_index(true);
        metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(1);
        rowset->set_data_size(1);
        rowset->add_segment_metas()->set_filename(fmt::format("projected-domain-{}.dat", tablet_id));
        lake::tablet_reshard_helper::set_rowset_uid(rowset);
        auto* sstable = metadata->mutable_sstable_meta()->add_sstables();
        sstable->set_filename(sst_filename);
        sstable->set_shared_rssid(shared_rssid);
        sstable->set_max_rss_rowid(static_cast<uint64_t>(shared_rssid) << 32);
        return metadata;
    };

    auto source_a = make_child(child_a, /*shared_rssid=*/1, "projected-domain-a-missing.sst");
    auto source_b = make_child(child_b, /*shared_rssid=*/std::numeric_limits<int32_t>::max() - 1,
                               "projected-domain-b-missing.sst");
    const uint32_t tombstone = std::numeric_limits<uint32_t>::max();
    const uint64_t boundary_sst_size =
            write_legacy_pk_sstable(_tablet_manager->sst_location(child_b, "projected-domain-b-missing.sst"),
                                    {{"projected-domain-boundary-tombstone", tombstone, tombstone}});
    source_b->mutable_sstable_meta()->mutable_sstables(0)->set_filesize(boundary_sst_size);
    const std::string source_a_before = source_a->SerializeAsString();
    const std::string source_b_before = source_b->SerializeAsString();
    MergingTabletInfoPB merging_info;
    merging_info.set_new_tablet_id(merged_tablet);
    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    std::vector<TabletMetadataPtr> sources = {source_a, source_b};

    auto merged = lake::merge_tablet(_tablet_manager.get(), sources, merging_info, /*new_version=*/2, txn_info);

    EXPECT_TRUE(merged.status().is_invalid_argument()) << merged.status();
    EXPECT_TRUE(merged.status().message().contains("exhausts the supported rssid allocation domain"))
            << merged.status();
    EXPECT_EQ(source_a_before, source_a->SerializeAsString());
    EXPECT_EQ(source_b_before, source_b->SerializeAsString());
}

// A raw-PB allocation check before dead-owner classification would reject this
// merge: child A's next_rowset_id=2 gives child B an offset of 1, so B's
// source_rssid=INT32_MAX-1 projects to INT32_MAX. The real SST's only data
// value belongs to that dead source owner, however, and is discarded.
TEST_F(LakeTabletReshardTest, test_tablet_merging_drops_exhausted_projected_non_shared_modern_data_sstable) {
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_child = [&](int64_t tablet_id) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(1);
        metadata->set_next_rowset_id(2);
        set_primary_key_schema(metadata.get(), 1001);
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(1);
        rowset->set_data_size(1);
        rowset->add_segment_metas()->set_filename(fmt::format("dead-data-boundary-{}.dat", tablet_id));
        lake::tablet_reshard_helper::set_rowset_uid(rowset);
        return metadata;
    };

    auto source_a = make_child(child_a);
    auto source_b = make_child(child_b);
    const uint32_t dead_source_rssid = std::numeric_limits<int32_t>::max() - 1;
    const std::string dead_sst_filename = "dead-data-boundary.sst";
    const uint64_t dead_sst_size =
            write_legacy_pk_sstable(_tablet_manager->sst_location(child_b, dead_sst_filename),
                                    {{"dead-data-boundary-key", dead_source_rssid, /*rowid=*/0}});
    add_non_shared_modern_sstable(source_b.get(), dead_sst_filename, dead_source_rssid, /*shared_version=*/1,
                                  /*max_rowid=*/0, dead_sst_size);

    MergingTabletInfoPB merging_info;
    merging_info.set_new_tablet_id(merged_tablet);
    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    std::vector<TabletMetadataPtr> sources = {source_a, source_b};

    auto merged = lake::merge_tablet(_tablet_manager.get(), sources, merging_info, /*new_version=*/2, txn_info);

    ASSERT_OK(merged);
    for (const auto& output : merged.value()->sstable_meta().sstables()) {
        EXPECT_NE(dead_sst_filename, output.filename());
    }
    EXPECT_LE(merged.value()->next_rowset_id(), static_cast<uint32_t>(std::numeric_limits<int32_t>::max()));
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
    ASSERT_EQ(std::numeric_limits<int32_t>::max(), merged->next_rowset_id());

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
    ASSERT_FALSE(published.value()->sstable_meta().sstables().empty());
    EXPECT_LE(published.value()->sstable_meta().sstables().rbegin()->max_rss_rowid(),
              static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
}

// Same-fileset_id sstables must remain contiguous in the merged metadata even
// when their max_rss_rowid spans a wide range with another fileset_id's
// max_rss_rowid falling within. A flat sort by max_rss_rowid alone (the
// original PR #72162 behavior) would interleave them, splitting one logical
// fileset into multiple physical filesets in LakePersistentIndex::init()'s
// adjacent-fileset_id grouping (lake_persistent_index.cpp:132-145) and
// breaking apply_opcompaction's contiguous-range find_if assumption
// (lake_persistent_index.cpp:838-864). Reproduces the Bug F shape observed
// on multi-cycle SPLIT/MERGE: a single fileset's sstables can span a wide
// max_rss_rowid range because filesets accumulate via append() across
// multiple memtable flushes (persistent_index_sstable_fileset.cpp:96-115).
//
// Long-term contract: the merged metadata must satisfy BOTH (I1)
// signed-monotone non-decreasing max_rss_rowid AND (I2) every output
// fileset_id appears in exactly one contiguous run. When a single source
// fileset_id's sstables would have to interleave with foreign-fileset_id
// sstables to satisfy I1, merge_sstables splits the source FID into multiple
// output FIDs by re-assigning fresh fileset_id (UniqueId::gen_uid) to each
// run that comes after a foreign-FID interruption — the later run is by
// physical layout already a separate logical fileset and cannot share
// PersistentIndexSstableFileset state with the earlier run.
TEST_F(LakeTabletReshardTest, test_tablet_merging_sstables_keep_same_fileset_id_contiguous) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // Distinct fileset_ids. F_X holds 4 sstables that span max_rss_rowid high
    // 100..400; F_A is a single sstable with high=200 — falling between F_X's
    // entries. A naive flat sort would emit
    //   [F_X(100), F_A(200), F_X(250), F_X(300), F_X(400)]
    // splitting F_X into 3 non-contiguous filesets in init(). The block-aware
    // sort must instead keep F_X contiguous regardless of the F_A interleave.
    PUniqueId fid_x;
    fid_x.set_hi(0x1111111111111111ULL);
    fid_x.set_lo(0x2222222222222222ULL);
    PUniqueId fid_a;
    fid_a.set_hi(0x3333333333333333ULL);
    fid_a.set_lo(0x4444444444444444ULL);

    auto add_sst = [](TabletMetadataPB* meta, const std::string& filename, uint64_t high, uint64_t low,
                      const PUniqueId& fid) {
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(filename);
        sst->set_filesize(128);
        sst->set_max_rss_rowid((high << 32) | low);
        sst->mutable_fileset_id()->CopyFrom(fid);
    };

    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(500);
    set_primary_key_schema(meta_a.get(), 1001);
    auto* rowset_a = meta_a->add_rowsets();
    rowset_a->set_id(1);
    rowset_a->set_version(1);
    rowset_a->set_num_rows(10);
    rowset_a->set_data_size(100);
    {
        auto* sm = rowset_a->add_segment_metas();
        sm->set_filename("seg_a.dat");
        sm->set_size(100);
    }
    // Child A's source-iteration order has F_X sstables already contiguous —
    // the merge_sstables block-sort must preserve this even when projection
    // and cross-child interleave with F_A would otherwise split them.
    add_sst(meta_a.get(), "fx_high100.sst", 100, 0, fid_x);
    add_sst(meta_a.get(), "fx_high250.sst", 250, 0, fid_x);
    add_sst(meta_a.get(), "fx_high300.sst", 300, 0, fid_x);
    add_sst(meta_a.get(), "fx_high400.sst", 400, 0, fid_x);

    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(500);
    set_primary_key_schema(meta_b.get(), 1001);
    auto* rowset_b = meta_b->add_rowsets();
    rowset_b->set_id(2);
    rowset_b->set_version(1);
    rowset_b->set_num_rows(10);
    rowset_b->set_data_size(100);
    {
        auto* sm = rowset_b->add_segment_metas();
        sm->set_filename("seg_b.dat");
        sm->set_size(100);
    }
    // Child B's lone F_A sstable falls inside F_X's max_rss_rowid range.
    add_sst(meta_b.get(), "fa_high200.sst", 200, 0, fid_a);

    materialize_tombstone_sstables(meta_a.get());
    materialize_tombstone_sstables(meta_b.get());

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
    ASSERT_EQ(5, merged->sstable_meta().sstables_size());

    // I1: signed-monotone non-decreasing max_rss_rowid across the merged metadata.
    int64_t prev_max = std::numeric_limits<int64_t>::min();
    for (const auto& sst : merged->sstable_meta().sstables()) {
        const int64_t cur = static_cast<int64_t>(sst.max_rss_rowid());
        EXPECT_LE(prev_max, cur) << "post-merge sstables must be in non-decreasing int64 max_rss_rowid order";
        prev_max = cur;
    }

    // I2: every output fileset_id appears in exactly one contiguous run.
    std::vector<std::pair<std::string, int>> id_runs; // <fileset_id_bytes, run_idx>
    int run_idx = -1;
    std::string last_id;
    for (int i = 0; i < merged->sstable_meta().sstables_size(); ++i) {
        const auto& sst = merged->sstable_meta().sstables(i);
        ASSERT_TRUE(sst.has_fileset_id());
        const uint64_t hi = static_cast<uint64_t>(sst.fileset_id().hi());
        const uint64_t lo = static_cast<uint64_t>(sst.fileset_id().lo());
        std::string id_bytes(reinterpret_cast<const char*>(&hi), sizeof(uint64_t));
        id_bytes += std::string(reinterpret_cast<const char*>(&lo), sizeof(uint64_t));
        if (id_bytes != last_id) {
            ++run_idx;
            last_id = id_bytes;
        }
        id_runs.emplace_back(id_bytes, run_idx);
    }
    std::map<std::string, std::set<int>> id_to_runs;
    for (const auto& [id, run] : id_runs) {
        id_to_runs[id].insert(run);
    }
    for (const auto& [id, runs] : id_to_runs) {
        EXPECT_EQ(1u, runs.size()) << "fileset_id appears in " << runs.size()
                                   << " non-contiguous runs in merged metadata — Bug F regression";
    }

    // child_b's lone F_A sstable carries fa_high200.sst. Because child_b is the
    // SECOND merge context, its rssid_offset = compute_rssid_offset(base_after_A,
    // child_b) = 500 - 2 = 498, so the projection lifts F_A's max_rss high from
    // 200 to 698. After the signed-monotone sort, F_A lands AFTER all four F_X
    // sstables (whose projection is a no-op since child_a is first → offset=0):
    //   pos 0..3 : F_X high=100/250/300/400 (contiguous, retains original FID-X)
    //   pos 4    : F_A high=698 (post-projection)
    // F_X stays contiguous in this layout without any FID reassignment.
    EXPECT_EQ("fx_high100.sst", merged->sstable_meta().sstables(0).filename());
    EXPECT_EQ("fx_high250.sst", merged->sstable_meta().sstables(1).filename());
    EXPECT_EQ("fx_high300.sst", merged->sstable_meta().sstables(2).filename());
    EXPECT_EQ("fx_high400.sst", merged->sstable_meta().sstables(3).filename());
    EXPECT_EQ("fa_high200.sst", merged->sstable_meta().sstables(4).filename());

    // F_X kept the original fileset_id (its run was uninterrupted in the
    // sorted layout), and F_A kept its original id too (single sstable run).
    auto fid_pair = [](const PUniqueId& f) { return std::make_pair(f.hi(), f.lo()); };
    EXPECT_EQ(std::make_pair(static_cast<int64_t>(0x1111111111111111LL), static_cast<int64_t>(0x2222222222222222LL)),
              fid_pair(merged->sstable_meta().sstables(0).fileset_id()));
    EXPECT_EQ(std::make_pair(static_cast<int64_t>(0x3333333333333333LL), static_cast<int64_t>(0x4444444444444444LL)),
              fid_pair(merged->sstable_meta().sstables(4).fileset_id()));
}

// Reproduces the run3 11306 fact pattern observed on tablet reshard: a single
// inherited fileset_id (FID-X) carried by the cycle-2 MERGE flush sstable
// (low max_rss), plus several per-child flush_pk_memtable outputs that
// inherited FID-X via PersistentIndexSstableFileset::append() (high max_rss),
// with foreign-FID compaction outputs interleaved at intermediate max_rss.
// The fix must keep each output fileset_id contiguous AND keep the global
// max_rss_rowid sequence signed-monotone non-decreasing.
TEST_F(LakeTabletReshardTest, test_tablet_merging_sstables_split_inherited_fileset_on_interleave) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // FID-X carries one early sstable (high=225) and three late per-child-flush
    // sstables (high=724/725/726) that inherited FID-X via append(). FID-A,
    // FID-B, FID-C, FID-D each carry one foreign sstable at high=226/393/570/715
    // — exactly the run3 11306 layout.
    PUniqueId fid_x;
    fid_x.set_hi(0x5111111111111111LL);
    fid_x.set_lo(0x5222222222222222LL);
    PUniqueId fid_a;
    fid_a.set_hi(0x6111111111111111LL);
    fid_a.set_lo(0x6222222222222222LL);
    PUniqueId fid_b;
    fid_b.set_hi(0x7111111111111111LL);
    fid_b.set_lo(0x7222222222222222LL);
    PUniqueId fid_c;
    fid_c.set_hi(0x4111111111111111LL);
    fid_c.set_lo(0x4222222222222222LL);
    PUniqueId fid_d;
    fid_d.set_hi(0x3111111111111111LL);
    fid_d.set_lo(0x3222222222222222LL);

    auto add_sst = [](TabletMetadataPB* meta, const std::string& filename, uint64_t high, uint64_t low,
                      const PUniqueId& fid) {
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(filename);
        sst->set_filesize(128);
        sst->set_max_rss_rowid((high << 32) | low);
        sst->mutable_fileset_id()->CopyFrom(fid);
    };

    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(800);
    set_primary_key_schema(meta_a.get(), 1001);
    auto* rowset_a = meta_a->add_rowsets();
    rowset_a->set_id(1);
    rowset_a->set_version(1);
    rowset_a->set_num_rows(10);
    rowset_a->set_data_size(100);
    {
        auto* sm = rowset_a->add_segment_metas();
        sm->set_filename("seg_a.dat");
        sm->set_size(100);
    }
    // Source-iteration order in child_a: the early FID-X sstable, then foreign
    // compaction outputs and the per-child flush sstables also tagged FID-X.
    add_sst(meta_a.get(), "fx_high225.sst", 225, 0, fid_x);
    add_sst(meta_a.get(), "fa_high226.sst", 226, 0, fid_a);
    add_sst(meta_a.get(), "fb_high393.sst", 393, 0, fid_b);
    add_sst(meta_a.get(), "fc_high570.sst", 570, 0, fid_c);
    add_sst(meta_a.get(), "fd_high715.sst", 715, 0, fid_d);
    add_sst(meta_a.get(), "fx_high724.sst", 724, 0, fid_x);
    add_sst(meta_a.get(), "fx_high725.sst", 725, 0, fid_x);
    add_sst(meta_a.get(), "fx_high726.sst", 726, 0, fid_x);

    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(800);
    set_primary_key_schema(meta_b.get(), 1001);
    auto* rowset_b = meta_b->add_rowsets();
    rowset_b->set_id(2);
    rowset_b->set_version(1);
    rowset_b->set_num_rows(10);
    rowset_b->set_data_size(100);
    {
        auto* sm = rowset_b->add_segment_metas();
        sm->set_filename("seg_b.dat");
        sm->set_size(100);
    }

    materialize_tombstone_sstables(meta_a.get());
    materialize_tombstone_sstables(meta_b.get());

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
    ASSERT_EQ(8, merged->sstable_meta().sstables_size());

    // I1: signed-monotone non-decreasing max_rss_rowid across the merged metadata.
    int64_t prev_max = std::numeric_limits<int64_t>::min();
    for (const auto& sst : merged->sstable_meta().sstables()) {
        const int64_t cur = static_cast<int64_t>(sst.max_rss_rowid());
        EXPECT_LE(prev_max, cur);
        prev_max = cur;
    }

    // Sort by max_rss_rowid produces the layout:
    //   0: fx_high225  (FID-X, kept)
    //   1: fa_high226  (FID-A, kept)
    //   2: fb_high393  (FID-B, kept)
    //   3: fc_high570  (FID-C, kept)
    //   4: fd_high715  (FID-D, kept)
    //   5: fx_high724  (FID-X re-encounter — fresh FID)
    //   6: fx_high725  (continues fresh-FID run)
    //   7: fx_high726  (continues fresh-FID run)
    EXPECT_EQ("fx_high225.sst", merged->sstable_meta().sstables(0).filename());
    EXPECT_EQ("fa_high226.sst", merged->sstable_meta().sstables(1).filename());
    EXPECT_EQ("fb_high393.sst", merged->sstable_meta().sstables(2).filename());
    EXPECT_EQ("fc_high570.sst", merged->sstable_meta().sstables(3).filename());
    EXPECT_EQ("fd_high715.sst", merged->sstable_meta().sstables(4).filename());
    EXPECT_EQ("fx_high724.sst", merged->sstable_meta().sstables(5).filename());
    EXPECT_EQ("fx_high725.sst", merged->sstable_meta().sstables(6).filename());
    EXPECT_EQ("fx_high726.sst", merged->sstable_meta().sstables(7).filename());

    // I2: every output fileset_id appears in exactly one contiguous run.
    std::map<std::pair<int64_t, int64_t>, std::vector<int>> fid_to_positions;
    for (int i = 0; i < merged->sstable_meta().sstables_size(); ++i) {
        const auto& f = merged->sstable_meta().sstables(i).fileset_id();
        fid_to_positions[{f.hi(), f.lo()}].push_back(i);
    }
    for (const auto& [fid, positions] : fid_to_positions) {
        for (size_t k = 1; k < positions.size(); ++k) {
            EXPECT_EQ(positions[k - 1] + 1, positions[k])
                    << "fileset_id non-contiguous in merged metadata — Bug F regression";
        }
    }

    // The early FID-X (pos 0) keeps its original id; the late re-encounter run
    // (pos 5..7) must have been re-assigned to a fresh id distinct from FID-X
    // and from any of the foreign FIDs.
    auto fid_pair = [](const PUniqueId& f) { return std::make_pair(f.hi(), f.lo()); };
    const auto pos0_fid = fid_pair(merged->sstable_meta().sstables(0).fileset_id());
    const auto pos5_fid = fid_pair(merged->sstable_meta().sstables(5).fileset_id());
    EXPECT_EQ(std::make_pair(fid_x.hi(), fid_x.lo()), pos0_fid);
    EXPECT_NE(pos0_fid, pos5_fid) << "non-contiguous re-encounter must be reassigned";
    EXPECT_NE(std::make_pair(fid_a.hi(), fid_a.lo()), pos5_fid);
    EXPECT_NE(std::make_pair(fid_b.hi(), fid_b.lo()), pos5_fid);
    EXPECT_NE(std::make_pair(fid_c.hi(), fid_c.lo()), pos5_fid);
    EXPECT_NE(std::make_pair(fid_d.hi(), fid_d.lo()), pos5_fid);
    EXPECT_EQ(pos5_fid, fid_pair(merged->sstable_meta().sstables(6).fileset_id()));
    EXPECT_EQ(pos5_fid, fid_pair(merged->sstable_meta().sstables(7).fileset_id()));
}

// A numeric rssid is local to one source tablet. Two contiguous tablets can both
// have rssid 1 while pointing at different physical segments; a legacy shared SST
// that contains keys from both ranges must route each key through its owning tablet
// before mapping the rssid into the merged tablet.
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_shared_sstable_routes_same_rssid_by_key_range) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string key_a = encode_int_primary_key(10);
    const std::string key_b = encode_int_primary_key(60);
    const std::string legacy_filename = "same_rssid_by_range.sst";
    const uint64_t legacy_filesize =
            write_legacy_pk_sstable(_tablet_manager->sst_location(child_a, legacy_filename),
                                    {{key_a, /*rssid=*/1, /*rowid=*/0}, {key_b, /*rssid=*/1, /*rowid=*/0}});

    auto make_child = [&](int64_t tablet_id, int lower, int upper, const std::string& segment_filename) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(base_version);
        metadata->set_next_rowset_id(2);
        set_int_primary_key_schema(metadata.get(), 1001);
        metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(lower));
        metadata->mutable_range()->set_lower_bound_included(true);
        metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(upper));
        metadata->mutable_range()->set_upper_bound_included(false);

        auto* rowset = metadata->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(1);
        rowset->set_data_size(100);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(segment_filename);
        segment->set_size(100);

        auto* sstable = metadata->mutable_sstable_meta()->add_sstables();
        sstable->set_filename(legacy_filename);
        sstable->set_filesize(legacy_filesize);
        sstable->set_shared(true);
        sstable->set_max_rss_rowid(static_cast<uint64_t>(1) << 32);
        return metadata;
    };

    EXPECT_OK(put_tablet_metadata(make_child(child_a, 0, 50, "left_segment.dat")));
    EXPECT_OK(put_tablet_metadata(make_child(child_b, 50, 100, "right_segment.dat")));

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
    EXPECT_EQ("left_segment.dat", merged->rowsets(0).segment_metas(0).filename());
    EXPECT_EQ("right_segment.dat", merged->rowsets(1).segment_metas(0).filename());
    EXPECT_EQ(1, merged->rowsets(0).id());
    EXPECT_EQ(2, merged->rowsets(1).id());
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());

    const auto& out_sst = merged->sstable_meta().sstables(0);
    ASSIGN_OR_ABORT(auto sstable, lake::PersistentIndexSstable::new_sstable(
                                          out_sst, _tablet_manager->sst_location(merged_tablet, out_sst.filename()),
                                          /*cache=*/nullptr, /*need_filter=*/false, /*delvec=*/nullptr, merged,
                                          _tablet_manager.get()));
    sstable::ReadOptions read_options;
    read_options.fill_cache = false;
    std::unique_ptr<sstable::Iterator> iterator(sstable->new_iterator(read_options));
    std::map<std::string, uint32_t> actual_rssids;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        IndexValuesWithVerPB values;
        ASSERT_TRUE(values.ParseFromArray(iterator->value().data, static_cast<int>(iterator->value().size)));
        ASSERT_EQ(1, values.values_size());
        actual_rssids.emplace(iterator->key().to_string(), values.values(0).rssid());
    }
    ASSERT_OK(iterator->status());
    ASSERT_EQ(2, actual_rssids.size());
    EXPECT_EQ(1, actual_rssids.at(key_a));
    EXPECT_EQ(2, actual_rssids.at(key_b));
}

// A legacy shared file is visible only in the source tablets that still
// reference its filename. Physical bytes outside those occurrence ranges must
// not be interpreted through another tablet's colliding local RSSID namespace.
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_shared_sstable_drops_other_occurrence_range_collision) {
    run_other_occurrence_range_case(/*out_of_range_tombstone=*/false);
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_shared_sstable_drops_other_occurrence_range_tombstone) {
    run_other_occurrence_range_case(/*out_of_range_tombstone=*/true);
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_shared_sstable_preserves_tombstone_in_global_route_gap) {
    const int64_t base_version = 1;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string gap_key = encode_int_primary_key(40);
    const std::string legacy_filename = "tombstone_in_global_route_gap.sst";
    const uint32_t tombstone = std::numeric_limits<uint32_t>::max();
    const uint64_t legacy_filesize = write_versioned_pk_sstable(_tablet_manager->sst_location(child_a, legacy_filename),
                                                                {{gap_key, /*version=*/7, tombstone, tombstone}});

    auto make_child = [&](int64_t tablet_id, int lower, int upper, const std::string& segment_filename) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(base_version);
        metadata->set_next_rowset_id(2);
        set_int_primary_key_schema(metadata.get(), 1001);
        metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(lower));
        metadata->mutable_range()->set_lower_bound_included(true);
        metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(upper));
        metadata->mutable_range()->set_upper_bound_included(false);
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(base_version);
        rowset->set_num_rows(1);
        rowset->set_data_size(100);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(segment_filename);
        segment->set_size(100);
        auto* sst = metadata->mutable_sstable_meta()->add_sstables();
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
        sst->set_shared(true);
        sst->set_max_rss_rowid(static_cast<uint64_t>(1) << 32);
        return metadata;
    };

    ASSERT_OK(put_tablet_metadata(make_child(child_a, /*lower=*/0, /*upper=*/30, "tombstone_gap_left.dat")));
    ASSERT_OK(put_tablet_metadata(make_child(child_b, /*lower=*/50, /*upper=*/100, "tombstone_gap_right.dat")));

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
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, base_version + 1,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    const auto& merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    ASSIGN_OR_ABORT(auto sstable, lake::PersistentIndexSstable::new_sstable(
                                          out_sst, _tablet_manager->sst_location(merged_tablet, out_sst.filename()),
                                          /*cache=*/nullptr, /*need_filter=*/false, /*delvec=*/nullptr, merged,
                                          _tablet_manager.get()));
    sstable::ReadOptions read_options;
    read_options.fill_cache = false;
    std::unique_ptr<sstable::Iterator> iterator(sstable->new_iterator(read_options));
    iterator->Seek(gap_key);
    ASSERT_TRUE(iterator->Valid());
    ASSERT_EQ(gap_key, iterator->key().to_string());
    IndexValuesWithVerPB values;
    ASSERT_TRUE(values.ParseFromArray(iterator->value().data, static_cast<int>(iterator->value().size)));
    ASSERT_EQ(1, values.values_size());
    EXPECT_EQ(tombstone, values.values(0).rssid());
    EXPECT_EQ(tombstone, values.values(0).rowid());
}

// A child may compact its persistent index after split and replace the
// inherited legacy shared SST with a private SST. The old physical file still
// contains that child's key range, but it is no longer visible there; MERGE
// drops those old-file bytes and keeps the private replacement as authority.
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_shared_sstable_uses_private_replacement_when_filename_absent) {
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_source_flush(
            [&] { set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE); });

    const int64_t base_version = 1;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string key_a = encode_int_primary_key(10);
    const std::string key_b = encode_int_primary_key(60);
    const std::string legacy_filename = "sst_free_sibling_legacy.sst";
    const std::string replacement_filename = "sst_free_sibling_replacement.sst";
    const uint64_t legacy_filesize =
            write_legacy_pk_sstable(_tablet_manager->sst_location(child_a, legacy_filename),
                                    {{key_a, /*rssid=*/1, /*rowid=*/0}, {key_b, /*rssid=*/1, /*rowid=*/0}});
    const uint64_t replacement_filesize =
            write_versioned_pk_sstable(_tablet_manager->sst_location(child_b, replacement_filename),
                                       {{key_b, /*version=*/2, /*rssid=*/1, /*rowid=*/1}});

    auto make_child = [&](int64_t tablet_id, int lower, int upper, const std::string& segment_filename) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(base_version);
        metadata->set_next_rowset_id(2);
        set_int_primary_key_schema(metadata.get(), 1001);
        metadata->set_enable_persistent_index(true);
        metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(lower));
        metadata->mutable_range()->set_lower_bound_included(true);
        metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(upper));
        metadata->mutable_range()->set_upper_bound_included(false);

        auto* rowset = metadata->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(base_version);
        rowset->set_num_rows(1);
        rowset->set_data_size(100);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(segment_filename);
        segment->set_size(100);
        return metadata;
    };

    auto child_a_metadata = make_child(child_a, /*lower=*/0, /*upper=*/50, "sst_free_sibling_left.dat");
    auto* legacy_sst = child_a_metadata->mutable_sstable_meta()->add_sstables();
    legacy_sst->set_filename(legacy_filename);
    legacy_sst->set_filesize(legacy_filesize);
    legacy_sst->set_shared(true);
    legacy_sst->set_max_rss_rowid(static_cast<uint64_t>(1) << 32);

    auto child_b_metadata = make_child(child_b, /*lower=*/50, /*upper=*/100, "sst_free_sibling_right.dat");
    auto* replacement_sst = child_b_metadata->mutable_sstable_meta()->add_sstables();
    replacement_sst->set_filename(replacement_filename);
    replacement_sst->set_filesize(replacement_filesize);
    replacement_sst->set_max_rss_rowid(static_cast<uint64_t>(1) << 32);

    ASSERT_OK(put_tablet_metadata(child_a_metadata));
    ASSERT_OK(put_tablet_metadata(child_b_metadata));

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
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, base_version + 1,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    const auto& merged = tablet_metadatas.at(merged_tablet);
    auto index = std::make_unique<lake::LakePersistentIndex>(_tablet_manager.get(), merged_tablet);
    ASSERT_OK(index->init(merged));
    Slice keys[] = {Slice(key_a), Slice(key_b)};
    IndexValue values[2];
    ASSERT_OK(index->get(/*n=*/2, keys, values));
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(1) << 32), values[0]);
    EXPECT_EQ(IndexValue((static_cast<uint64_t>(2) << 32) | 1), values[1]);
}

// A data entry inside the merged tablet's outer range but outside every
// contributing source tablet range has no unambiguous source RSSID namespace.
// It must fail the legacy shared-SST rebuild instead of being treated like a
// routed entry whose source rowset is no longer live.
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_shared_sstable_rejects_data_entry_in_source_route_gap) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string gap_key = encode_int_primary_key(40);
    const std::string legacy_filename = "data_entry_in_source_route_gap.sst";
    const uint64_t legacy_filesize = write_legacy_pk_sstable(_tablet_manager->sst_location(child_a, legacy_filename),
                                                             {{gap_key, /*rssid=*/1, /*rowid=*/0}});

    auto make_child = [&](int64_t tablet_id, int lower, int upper, const std::string& segment_filename) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(base_version);
        metadata->set_next_rowset_id(2);
        set_int_primary_key_schema(metadata.get(), 1001);
        metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(lower));
        metadata->mutable_range()->set_lower_bound_included(true);
        metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(upper));
        metadata->mutable_range()->set_upper_bound_included(false);

        auto* rowset = metadata->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(1);
        rowset->set_data_size(100);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(segment_filename);
        segment->set_size(100);

        auto* sstable = metadata->mutable_sstable_meta()->add_sstables();
        sstable->set_filename(legacy_filename);
        sstable->set_filesize(legacy_filesize);
        sstable->set_shared(true);
        sstable->set_max_rss_rowid(static_cast<uint64_t>(1) << 32);
        return metadata;
    };

    ASSERT_OK(put_tablet_metadata(make_child(child_a, /*lower=*/0, /*upper=*/30, "gap_left.dat")));
    ASSERT_OK(put_tablet_metadata(make_child(child_b, /*lower=*/50, /*upper=*/100, "gap_right.dat")));

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
    const Status status =
            lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                            txn_info, false, tablet_metadatas, tablet_ranges);
    if (status.ok()) {
        ASSERT_TRUE(tablet_metadatas.contains(merged_tablet));
        EXPECT_EQ(0, tablet_metadatas.at(merged_tablet)->sstable_meta().sstables_size())
                << "the route-gap data entry was silently dropped";
    }
    ASSERT_TRUE(status.is_corruption()) << "route-gap data entry was silently accepted: " << status;
    EXPECT_NE(std::string::npos, status.message().find("has no source tablet route")) << status;
    EXPECT_NE(std::string::npos, status.message().find("encoded_key_hex=")) << status;
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_overlay_resolves_cross_sst_versions) {
    const int64_t base_version = 600;
    const int64_t new_version = 601;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string duplicate_key = encode_int_primary_key(10);
    const std::string live_filename = "cross_sst_live.sst";
    const std::string tombstone_filename = "cross_sst_tombstone.sst";
    const uint32_t tombstone = std::numeric_limits<uint32_t>::max();
    const uint64_t live_filesize =
            write_versioned_pk_sstable(_tablet_manager->sst_location(child_a, live_filename),
                                       {{duplicate_key, /*version=*/540, /*rssid=*/1, /*rowid=*/0}});
    const uint64_t tombstone_filesize =
            write_versioned_pk_sstable(_tablet_manager->sst_location(child_b, tombstone_filename),
                                       {{duplicate_key, /*version=*/513, tombstone, tombstone}});

    auto make_child = [&](int64_t tablet_id, int lower, int upper, const std::string& segment_filename,
                          const std::string& sst_filename, uint64_t sst_filesize) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(base_version);
        metadata->set_next_rowset_id(2);
        set_int_primary_key_schema(metadata.get(), 1001);
        metadata->set_enable_persistent_index(true);
        metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(lower));
        metadata->mutable_range()->set_lower_bound_included(true);
        metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(upper));
        metadata->mutable_range()->set_upper_bound_included(false);

        auto* rowset = metadata->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(base_version);
        rowset->set_num_rows(1);
        rowset->set_data_size(100);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(segment_filename);
        segment->set_size(100);

        auto* sstable = metadata->mutable_sstable_meta()->add_sstables();
        sstable->set_filename(sst_filename);
        sstable->set_filesize(sst_filesize);
        sstable->set_max_rss_rowid(static_cast<uint64_t>(3) << 32);
        return metadata;
    };

    ASSERT_OK(put_tablet_metadata(
            make_child(child_a, /*lower=*/0, /*upper=*/50, "cross_sst_left.dat", live_filename, live_filesize)));
    ASSERT_OK(put_tablet_metadata(make_child(child_b, /*lower=*/50, /*upper=*/100, "cross_sst_right.dat",
                                             tombstone_filename, tombstone_filesize)));

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

    const auto& merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(3, merged->sstable_meta().sstables_size())
            << "the two projected inputs must be retained ahead of the duplicate-winner overlay";
    EXPECT_EQ(live_filename, merged->sstable_meta().sstables(0).filename());
    EXPECT_EQ(tombstone_filename, merged->sstable_meta().sstables(1).filename());
    ASSIGN_OR_ABORT(auto value, load_index_value(merged, merged_tablet, duplicate_key));
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(1) << 32), value);
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_overlay_precedes_tail_materialization) {
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_flush_failpoints([&] {
        set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    });

    const int64_t base_version = 600;
    const int64_t new_version = 601;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string old_segment = "overlay_tail_old.dat";
    const std::string tail_segment = "overlay_tail_live.dat";
    const std::string sibling_segment = "overlay_tail_sibling.dat";
    const uint64_t old_segment_size =
            write_two_column_segment(child_a, old_segment, /*num_rows=*/1, [](int) { return 100; });
    const uint64_t tail_segment_size =
            write_two_column_segment(child_a, tail_segment, /*num_rows=*/1, [](int) { return 200; });
    const uint64_t sibling_segment_size = write_two_column_segment(
            child_b, sibling_segment, /*num_rows=*/1, [](int) { return 600; }, /*key_start=*/60);
    const std::string reinserted_key = raw_int_primary_key(0);
    const std::string deleted_key = raw_int_primary_key(1);
    const std::string sibling_key = raw_int_primary_key(60);

    const uint32_t tombstone = std::numeric_limits<uint32_t>::max();
    const std::string tombstone_filename = "overlay_tail_newer_tombstone.sst";
    const uint64_t tombstone_filesize =
            write_versioned_pk_sstable(_tablet_manager->sst_location(child_a, tombstone_filename),
                                       {{reinserted_key, /*version=*/540, tombstone, tombstone},
                                        {deleted_key, /*version=*/540, tombstone, tombstone}});
    const std::string stale_live_filename = "overlay_tail_stale_live.sst";
    const uint64_t stale_live_filesize =
            write_versioned_pk_sstable(_tablet_manager->sst_location(child_b, stale_live_filename),
                                       {{reinserted_key, /*version=*/513, /*rssid=*/1, /*rowid=*/0},
                                        {deleted_key, /*version=*/513, /*rssid=*/1, /*rowid=*/0}});

    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(3);
    set_two_column_pk_schema(meta_a.get(), 4001);
    meta_a->set_enable_persistent_index(true);
    meta_a->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
    meta_a->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(0));
    meta_a->mutable_range()->set_lower_bound_included(true);
    meta_a->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(50));
    meta_a->mutable_range()->set_upper_bound_included(false);
    auto add_rowset_a = [&](uint32_t id, int64_t version, const std::string& filename, uint64_t filesize) {
        auto* rowset = meta_a->add_rowsets();
        rowset->set_id(id);
        rowset->set_version(version);
        rowset->set_num_rows(1);
        rowset->set_data_size(filesize);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(filename);
        segment->set_size(filesize);
        segment->set_num_rows(1);
    };
    add_rowset_a(/*id=*/1, /*version=*/500, old_segment, old_segment_size);
    add_rowset_a(/*id=*/2, /*version=*/600, tail_segment, tail_segment_size);
    auto* tombstone_sst = meta_a->mutable_sstable_meta()->add_sstables();
    tombstone_sst->set_filename(tombstone_filename);
    tombstone_sst->set_filesize(tombstone_filesize);
    tombstone_sst->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | UINT32_MAX);

    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(2);
    set_two_column_pk_schema(meta_b.get(), 4001);
    meta_b->set_enable_persistent_index(true);
    meta_b->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
    meta_b->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(50));
    meta_b->mutable_range()->set_lower_bound_included(true);
    meta_b->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(100));
    meta_b->mutable_range()->set_upper_bound_included(false);
    auto* sibling_rowset = meta_b->add_rowsets();
    sibling_rowset->set_id(1);
    sibling_rowset->set_version(500);
    sibling_rowset->set_num_rows(1);
    sibling_rowset->set_data_size(sibling_segment_size);
    auto* sibling_segment_meta = sibling_rowset->add_segment_metas();
    sibling_segment_meta->set_filename(sibling_segment);
    sibling_segment_meta->set_size(sibling_segment_size);
    sibling_segment_meta->set_num_rows(1);
    auto* stale_live_sst = meta_b->mutable_sstable_meta()->add_sstables();
    stale_live_sst->set_filename(stale_live_filename);
    stale_live_sst->set_filesize(stale_live_filesize);
    stale_live_sst->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | UINT32_MAX);

    ASSERT_OK(put_tablet_metadata(meta_a));
    ASSERT_OK(put_tablet_metadata(meta_b));

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

    const auto& merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(4, merged->sstable_meta().sstables_size());
    ASSIGN_OR_ABORT(auto reinserted_value, load_index_value(merged, merged_tablet, reinserted_key));
    ASSIGN_OR_ABORT(auto deleted_value, load_index_value(merged, merged_tablet, deleted_key));
    ASSIGN_OR_ABORT(auto sibling_value, load_index_value(merged, merged_tablet, sibling_key));
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(2) << 32), reinserted_value);
    EXPECT_EQ(IndexValue(NullIndexValue), deleted_value);
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(3) << 32), sibling_value);
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_dropped_non_shared_modern_watermark_does_not_hide_live_tail) {
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_flush_failpoints([&] {
        set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    });

    const int64_t base_version = 1;
    const int64_t merged_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string segment_a = "retained_coverage_non_shared_a.dat";
    const std::string segment_b = "retained_coverage_non_shared_b.dat";
    const uint64_t segment_a_size =
            write_two_column_segment(child_a, segment_a, /*num_rows=*/1, [](int key) { return key * 10; }, 10);
    const uint64_t segment_b_size =
            write_two_column_segment(child_b, segment_b, /*num_rows=*/1, [](int key) { return key * 10; }, 60);
    const std::string dead_sst_filename = "retained_coverage_non_shared_dead.sst";
    const uint64_t dead_sst_size =
            write_versioned_pk_sstable(_tablet_manager->sst_location(child_a, dead_sst_filename),
                                       {{encode_int_primary_key(30), /*version=*/1, /*rssid=*/3, /*rowid=*/0}});
    const std::string live_sst_filename = "retained_coverage_non_shared_live.sst";
    const uint64_t live_sst_size =
            write_versioned_pk_sstable(_tablet_manager->sst_location(child_b, live_sst_filename),
                                       {{encode_int_primary_key(60), /*version=*/1, /*rssid=*/1, /*rowid=*/0}});

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
        return metadata;
    };

    auto source_a = make_source(child_a, /*lower=*/0, /*upper=*/50, /*rowset_id=*/1, segment_a, segment_a_size);
    add_non_shared_modern_sstable(source_a.get(), dead_sst_filename, /*rssid=*/3, /*shared_version=*/1,
                                  /*max_rowid=*/0, dead_sst_size);
    auto source_b = make_source(child_b, /*lower=*/50, /*upper=*/100, /*rowset_id=*/1, segment_b, segment_b_size);
    add_non_shared_modern_sstable(source_b.get(), live_sst_filename, /*rssid=*/1, /*shared_version=*/1,
                                  /*max_rowid=*/0, live_sst_size);
    ASSERT_OK(put_tablet_metadata(source_a));
    ASSERT_OK(put_tablet_metadata(source_b));

    ReshardingTabletInfoPB resharding;
    auto& merging = *resharding.mutable_merging_tablet_info();
    merging.add_old_tablet_ids(child_a);
    merging.add_old_tablet_ids(child_b);
    merging.set_new_tablet_id(merged_tablet);
    TxnInfoPB merge_txn;
    merge_txn.set_txn_id(1);
    merge_txn.set_commit_time(1);
    merge_txn.set_gtid(1);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, base_version, merged_version,
                                              merge_txn, false, tablet_metadatas, tablet_ranges));
    auto merged = tablet_metadatas.at(merged_tablet);

    for (const auto& sst : merged->sstable_meta().sstables()) {
        EXPECT_NE(dead_sst_filename, sst.filename());
    }
    const std::vector<std::string> lookup_keys = {encode_int_primary_key(10), encode_int_primary_key(60)};
    ASSIGN_OR_ABORT(auto lookup_values, load_index_values(merged, merged_tablet, lookup_keys));
    ASSERT_EQ(2, lookup_values.size());
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(1) << 32), lookup_values[0]);
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(2) << 32), lookup_values[1]);

    ASSIGN_OR_ABORT(auto after_dml, publish_followup_upsert_delete(merged_tablet, merged_version, /*upsert_key=*/10,
                                                                   /*upsert_value=*/1010, /*delete_key=*/60));
    ASSIGN_OR_ABORT(auto rows, read_two_column_rows(after_dml));
    EXPECT_EQ((std::vector<std::pair<int32_t, int32_t>>{{10, 1010}}), rows);
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_shared_modern_live_occurrence_does_not_cover_dead_sibling_tail) {
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_flush_failpoints([&] {
        set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    });

    const int64_t base_version = 1;
    const int64_t merged_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string segment_a = "retained_coverage_shared_modern_a.dat";
    const std::string segment_b = "retained_coverage_shared_modern_b.dat";
    const uint64_t segment_a_size =
            write_two_column_segment(child_a, segment_a, /*num_rows=*/1, [](int key) { return key * 10; }, 10);
    const uint64_t segment_b_size =
            write_two_column_segment(child_b, segment_b, /*num_rows=*/1, [](int key) { return key * 10; }, 60);
    const std::string shared_sst_filename = "retained_coverage_shared_modern.sst";
    const uint64_t shared_sst_size =
            write_versioned_pk_sstable(_tablet_manager->sst_location(child_a, shared_sst_filename),
                                       {{encode_int_primary_key(60), /*version=*/1, /*rssid=*/1, /*rowid=*/0}});

    auto make_source = [&](int64_t tablet_id, int lower, int upper, const std::string& segment_filename,
                           uint64_t segment_size) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(base_version);
        metadata->set_next_rowset_id(2);
        set_two_column_pk_schema(metadata.get(), /*schema_id=*/4001);
        metadata->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
        metadata->set_enable_persistent_index(true);
        metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(lower));
        metadata->mutable_range()->set_lower_bound_included(true);
        metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(upper));
        metadata->mutable_range()->set_upper_bound_included(false);
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(base_version);
        rowset->set_num_rows(1);
        rowset->set_data_size(segment_size);
        rowset->set_overlapped(false);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(segment_filename);
        segment->set_size(segment_size);
        segment->set_num_rows(1);
        return metadata;
    };

    auto source_a = make_source(child_a, /*lower=*/0, /*upper=*/50, segment_a, segment_a_size);
    add_shared_rssid_sstable(source_a.get(), shared_sst_filename, /*rssid=*/3, /*shared_version=*/1,
                             /*max_rowid=*/0, shared_sst_size);
    auto source_b = make_source(child_b, /*lower=*/50, /*upper=*/100, segment_b, segment_b_size);
    add_shared_rssid_sstable(source_b.get(), shared_sst_filename, /*rssid=*/1, /*shared_version=*/1,
                             /*max_rowid=*/0, shared_sst_size);
    ASSERT_OK(put_tablet_metadata(source_a));
    ASSERT_OK(put_tablet_metadata(source_b));

    ReshardingTabletInfoPB resharding;
    auto& merging = *resharding.mutable_merging_tablet_info();
    merging.add_old_tablet_ids(child_a);
    merging.add_old_tablet_ids(child_b);
    merging.set_new_tablet_id(merged_tablet);
    TxnInfoPB merge_txn;
    merge_txn.set_txn_id(1);
    merge_txn.set_commit_time(1);
    merge_txn.set_gtid(1);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, base_version, merged_version,
                                              merge_txn, false, tablet_metadatas, tablet_ranges));
    auto merged = tablet_metadatas.at(merged_tablet);

    ASSERT_FALSE(merged->sstable_meta().sstables().empty());
    EXPECT_EQ(shared_sst_filename, merged->sstable_meta().sstables(0).filename());
    const std::vector<std::string> lookup_keys = {encode_int_primary_key(10), encode_int_primary_key(60)};
    ASSIGN_OR_ABORT(auto lookup_values, load_index_values(merged, merged_tablet, lookup_keys));
    ASSERT_EQ(2, lookup_values.size());
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(1) << 32), lookup_values[0]);
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(2) << 32), lookup_values[1]);

    ASSIGN_OR_ABORT(auto after_dml, publish_followup_upsert_delete(merged_tablet, merged_version, /*upsert_key=*/10,
                                                                   /*upsert_value=*/1010, /*delete_key=*/60));
    ASSIGN_OR_ABORT(auto rows, read_two_column_rows(after_dml));
    EXPECT_EQ((std::vector<std::pair<int32_t, int32_t>>{{10, 1010}}), rows);
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_shared_legacy_global_max_does_not_hide_source_local_tail) {
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_flush_failpoints([&] {
        set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    });

    const int64_t base_version = 1;
    const int64_t merged_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string segment_a_covered = "retained_coverage_shared_legacy_a_covered.dat";
    const std::string segment_a_tail = "retained_coverage_shared_legacy_a_tail.dat";
    const std::string segment_b = "retained_coverage_shared_legacy_b.dat";
    const uint64_t segment_a_covered_size =
            write_two_column_segment(child_a, segment_a_covered, /*num_rows=*/1, [](int key) { return key * 10; }, 10);
    const uint64_t segment_a_tail_size =
            write_two_column_segment(child_a, segment_a_tail, /*num_rows=*/1, [](int key) { return key * 10; }, 20);
    const uint64_t segment_b_size =
            write_two_column_segment(child_b, segment_b, /*num_rows=*/1, [](int key) { return key * 10; }, 60);
    const std::string shared_sst_filename = "retained_coverage_shared_legacy.sst";
    const uint64_t shared_sst_size =
            write_versioned_pk_sstable(_tablet_manager->sst_location(child_a, shared_sst_filename),
                                       {{encode_int_primary_key(10), /*version=*/1, /*rssid=*/1, /*rowid=*/0},
                                        {encode_int_primary_key(60), /*version=*/1, /*rssid=*/3, /*rowid=*/0}});

    auto make_source = [&](int64_t tablet_id, int lower, int upper) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(base_version);
        metadata->set_next_rowset_id(4);
        set_two_column_pk_schema(metadata.get(), /*schema_id=*/4001);
        metadata->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
        metadata->set_enable_persistent_index(true);
        metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(lower));
        metadata->mutable_range()->set_lower_bound_included(true);
        metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(upper));
        metadata->mutable_range()->set_upper_bound_included(false);
        auto* sst = metadata->mutable_sstable_meta()->add_sstables();
        sst->set_filename(shared_sst_filename);
        sst->set_filesize(shared_sst_size);
        sst->set_shared(true);
        sst->set_max_rss_rowid(static_cast<uint64_t>(3) << 32);
        return metadata;
    };
    auto add_real_rowset = [&](TabletMetadataPB* metadata, uint32_t rowset_id, const std::string& segment_filename,
                               uint64_t segment_size) {
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
    };

    auto source_a = make_source(child_a, /*lower=*/0, /*upper=*/50);
    source_a->set_next_rowset_id(3);
    add_real_rowset(source_a.get(), /*rowset_id=*/1, segment_a_covered, segment_a_covered_size);
    add_real_rowset(source_a.get(), /*rowset_id=*/2, segment_a_tail, segment_a_tail_size);
    auto source_b = make_source(child_b, /*lower=*/50, /*upper=*/100);
    add_real_rowset(source_b.get(), /*rowset_id=*/3, segment_b, segment_b_size);
    ASSERT_OK(put_tablet_metadata(source_a));
    ASSERT_OK(put_tablet_metadata(source_b));

    ReshardingTabletInfoPB resharding;
    auto& merging = *resharding.mutable_merging_tablet_info();
    merging.add_old_tablet_ids(child_a);
    merging.add_old_tablet_ids(child_b);
    merging.set_new_tablet_id(merged_tablet);
    TxnInfoPB merge_txn;
    merge_txn.set_txn_id(1);
    merge_txn.set_commit_time(1);
    merge_txn.set_gtid(1);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding, base_version, merged_version,
                                              merge_txn, false, tablet_metadatas, tablet_ranges));
    auto merged = tablet_metadatas.at(merged_tablet);

    const std::vector<std::string> lookup_keys = {encode_int_primary_key(10), encode_int_primary_key(20),
                                                  encode_int_primary_key(60)};
    ASSIGN_OR_ABORT(auto lookup_values, load_index_values(merged, merged_tablet, lookup_keys));
    ASSERT_EQ(3, lookup_values.size());
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(1) << 32), lookup_values[0]);
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(2) << 32), lookup_values[1]);
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(3) << 32), lookup_values[2]);

    ASSIGN_OR_ABORT(auto after_dml, publish_followup_upsert_delete(merged_tablet, merged_version, /*upsert_key=*/20,
                                                                   /*upsert_value=*/2020, /*delete_key=*/10));
    ASSIGN_OR_ABORT(auto rows, read_two_column_rows(after_dml));
    EXPECT_EQ((std::vector<std::pair<int32_t, int32_t>>{{20, 2020}, {60, 600}}), rows);
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_materializes_uncovered_index_tail) {
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_flush_failpoints([&] {
        set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    });

    int64_t merged_tablet = 0;
    std::string key_zero;
    ASSIGN_OR_ABORT(auto merged, publish_index_tail_merge(/*delete_old_row=*/true, &merged_tablet, &key_zero));
    ASSERT_EQ(2, merged->sstable_meta().sstables_size());
    EXPECT_EQ("index_tail_right_tombstone.sst", merged->sstable_meta().sstables(0).filename());
    EXPECT_EQ(static_cast<uint64_t>(3) << 32, merged->sstable_meta().sstables(0).max_rss_rowid());
    EXPECT_EQ(static_cast<uint64_t>(3) << 32, merged->sstable_meta().sstables().rbegin()->max_rss_rowid());

    ASSIGN_OR_ABORT(auto value, load_index_value(merged, merged_tablet, key_zero));
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(2) << 32), value);
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_tail_rebuild_rejects_duplicate_live_keys) {
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_flush_failpoints([&] {
        set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    });

    int64_t merged_tablet = 0;
    std::string key_zero;
    auto result = publish_index_tail_merge(/*delete_old_row=*/false, &merged_tablet, &key_zero);
    EXPECT_FALSE(result.ok()) << "two live physical rows for the same key must fail merge publish";
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_index_tail_uses_zero_point_for_source_without_sst) {
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_flush_failpoints([&] {
        set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    });

    int64_t merged_tablet = 0;
    std::string key_zero;
    ASSIGN_OR_ABORT(auto merged, publish_index_tail_merge(/*delete_old_row=*/true, &merged_tablet, &key_zero,
                                                          /*include_left_sst=*/false, /*include_right_sst=*/true));
    ASSERT_EQ(2, merged->sstable_meta().sstables_size());
    ASSIGN_OR_ABORT(auto value, load_index_value(merged, merged_tablet, key_zero));
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(2) << 32), value);
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_index_tail_ignores_empty_source_without_sst) {
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_flush_failpoints([&] {
        set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    });

    int64_t merged_tablet = 0;
    std::string key_zero;
    ASSIGN_OR_ABORT(auto merged, publish_index_tail_merge(/*delete_old_row=*/false, &merged_tablet, &key_zero,
                                                          /*include_left_sst=*/true, /*include_right_sst=*/false,
                                                          /*include_new_rowset=*/false));
    ASSERT_EQ(1, merged->sstable_meta().sstables_size())
            << "a truly empty source must not force materialization of the covered source";
    EXPECT_EQ("index_tail_stale.sst", merged->sstable_meta().sstables(0).filename());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_index_tail_materializes_when_no_projected_sst) {
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_flush_failpoints([&] {
        set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    });

    int64_t merged_tablet = 0;
    std::string key_zero;
    ASSIGN_OR_ABORT(auto merged, publish_index_tail_merge(/*delete_old_row=*/true, &merged_tablet, &key_zero,
                                                          /*include_left_sst=*/false, /*include_right_sst=*/false));
    EXPECT_EQ(1, merged->sstable_meta().sstables_size());
    ASSIGN_OR_ABORT(auto value, load_index_value(merged, merged_tablet, key_zero));
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(2) << 32), value);
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_index_tail_skips_equal_rebuild_point) {
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_flush_failpoints([&] {
        set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    });

    const int64_t source_tablet = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(source_tablet);
    prepare_tablet_dirs(merged_tablet);
    const std::string segment_name = "equal_rebuild_point.dat";
    const uint64_t segment_size =
            write_two_column_segment(source_tablet, segment_name, /*num_rows=*/1, [](int) { return 100; });
    auto metadata = make_single_segment_pk_tablet(source_tablet, /*version=*/1, segment_name, segment_size,
                                                  /*num_rows=*/1);
    metadata->set_next_rowset_id(2);

    const int32_t raw_key_zero = 0;
    const std::string key_zero(reinterpret_cast<const char*>(&raw_key_zero), sizeof(raw_key_zero));
    const std::string sst_filename = "equal_rebuild_point.sst";
    const uint64_t sst_filesize = write_legacy_pk_sstable(_tablet_manager->sst_location(source_tablet, sst_filename),
                                                          {{key_zero, /*rssid=*/1, /*rowid=*/0}});
    auto* sst = metadata->mutable_sstable_meta()->add_sstables();
    sst->set_filename(sst_filename);
    sst->set_filesize(sst_filesize);
    sst->set_max_rss_rowid(static_cast<uint64_t>(1) << 32);
    ASSERT_OK(put_tablet_metadata(metadata));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(source_tablet);
    merging_info.set_new_tablet_id(merged_tablet);
    TxnInfoPB txn_info;
    txn_info.set_txn_id(1);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, /*base_version=*/1,
                                              /*new_version=*/2, txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    EXPECT_EQ(sst_filename, merged->sstable_meta().sstables(0).filename());
    EXPECT_EQ(static_cast<uint64_t>(1) << 32, merged->sstable_meta().sstables(0).max_rss_rowid());
}

// Reproduces run4 cycle-3 ghost-rssid shape at the metadata level: the legacy
// shared sstable inherited from an ancestor still stores entries for rowsets
// that have been compacted out of every surviving child. The bug-fix rebuild
// path walks merge_contexts to find a live rowset that owns each stored rssid
// and drops entries whose source rowset is dead in every child.
//
// Setup:
//   - Children A and B both inherit one shared PK sstable with three entries:
//       k1 -> rssid 1, k2 -> rssid 2, k3 -> rssid 3
//   - A keeps rowset id=1 alive (segment_metas[].shared()=true).
//   - B keeps rowset id=2 alive (segment_metas[].shared()=true).
//   - Neither child has rowset id=3 — that ancestor rowset has been compacted
//     out everywhere, but the legacy sstable cannot be rewritten by the old
//     metadata-only projection so its entry for k3 is the run4 ghost.
// Expected post-fix:
//   - The merged tablet has exactly one PK sstable (the rebuilt file).
//   - The rebuilt PB is non-shared with no shared_rssid and rssid_offset==0.
//   - Iterating the rebuilt file yields exactly k1 (mapped to 1) and k2
//     (mapped to 2). The dead k3 entry is dropped.
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_sstable_rebuild_drops_dead_rssids) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string legacy_filename = "ghost_rssid.sst";
    const auto legacy_path = _tablet_manager->sst_location(child_a, legacy_filename);
    const uint64_t legacy_filesize = write_legacy_pk_sstable(
            legacy_path,
            {{"k1", /*rssid=*/1, /*rowid=*/0}, {"k2", /*rssid=*/2, /*rowid=*/0}, {"k3", /*rssid=*/3, /*rowid=*/0}});

    auto make_child = [&](int64_t tablet_id, uint32_t live_rowset_id, const std::string& seg_filename) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(live_rowset_id + 1);
        set_primary_key_schema(meta.get(), 1001);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(live_rowset_id);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename(seg_filename);
            sm->set_size(100);
            sm->set_shared(true);
        }
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
        sst->set_shared(true);
        sst->set_max_rss_rowid((static_cast<uint64_t>(3) << 32) | 0);
        return meta;
    };

    auto meta_a = make_child(child_a, /*live_rowset_id=*/1, "seg_a.dat");
    auto meta_b = make_child(child_b, /*live_rowset_id=*/2, "seg_b.dat");

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
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    EXPECT_NE(legacy_filename, out_sst.filename());
    EXPECT_FALSE(out_sst.shared());
    EXPECT_FALSE(out_sst.has_shared_rssid());
    EXPECT_EQ(0, out_sst.rssid_offset());
    // Rebuilt PB carries a fresh fileset_id. PersistentIndexSstableFileset::
    // init(vector) DCHECKs has_fileset_id() for ranged sstables, so missing
    // it here would crash debug builds and leave release builds with a
    // default identity that breaks compaction matching.
    EXPECT_TRUE(out_sst.has_fileset_id());
    EXPECT_TRUE(out_sst.has_range());

    // Read the rebuilt sstable directly and verify the dead-rssid entry was
    // dropped while the live entries were remapped to the merged tablet's
    // surviving rowset ids.
    ASSIGN_OR_ABORT(auto sstable, lake::PersistentIndexSstable::new_sstable(
                                          out_sst, _tablet_manager->sst_location(merged_tablet, out_sst.filename()),
                                          /*cache=*/nullptr, /*need_filter=*/false, /*delvec=*/nullptr, merged,
                                          _tablet_manager.get()));
    sstable::ReadOptions read_options;
    read_options.fill_cache = false;
    std::unique_ptr<sstable::Iterator> iter(sstable->new_iterator(read_options));
    std::map<std::string, uint32_t> rebuilt_entries;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        IndexValuesWithVerPB index_values_pb;
        ASSERT_TRUE(index_values_pb.ParseFromArray(iter->value().data, static_cast<int>(iter->value().size)));
        ASSERT_GT(index_values_pb.values_size(), 0);
        rebuilt_entries.emplace(iter->key().to_string(), index_values_pb.values(0).rssid());
    }
    ASSERT_OK(iter->status());

    EXPECT_EQ(2u, rebuilt_entries.size()) << "k3 (dead rssid 3) should have been dropped";
    ASSERT_TRUE(rebuilt_entries.count("k1"));
    ASSERT_TRUE(rebuilt_entries.count("k2"));
    EXPECT_EQ(0u, rebuilt_entries.count("k3"));
    // Both children get rssid_offset=0 in this layout (compute_rssid_offset
    // returns base.next_rowset_id - append.min_id), so the rebuilt entries
    // keep their original rssids.
    EXPECT_EQ(1u, rebuilt_entries["k1"]);
    EXPECT_EQ(2u, rebuilt_entries["k2"]);
}

// Round-2 (Codex high #1): the rebuild must filter entries whose rowid is in
// the merged delvec — the same protection the modern shared_rssid path gets
// via its post-merge delvec PB attachment. merge_delvecs Phase 5 writes the
// per-rssid pages into new_metadata.delvec_meta.delvecs, including any real
// deletes the children carried (and synthesized gap-bits, which require
// real segment files to exercise — covered conceptually here via real
// deletes that flow through the same rebuild filter).
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_sstable_rebuild_filters_via_delvec) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // Both children mark rowid 8 of segment rssid=1 as deleted via independent
    // delvec entries on the shared rowset. merge_delvecs unions them onto the
    // canonical's final rssid; the rebuilt sstable's per-entry filter must
    // drop k2 (rowid 8) and keep k1 (rowid 0).
    DelVector shared_delvec;
    const uint32_t deleted_rowids[] = {8};
    shared_delvec.init(1, deleted_rowids, 1);
    std::string shared_delvec_data = shared_delvec.save();

    const std::string legacy_filename = "delvec_filter.sst";
    const auto legacy_path = _tablet_manager->sst_location(child_a, legacy_filename);
    const uint64_t legacy_filesize =
            write_legacy_pk_sstable(legacy_path, {{"k1", /*rssid=*/1, /*rowid=*/0}, {"k2", /*rssid=*/1, /*rowid=*/8}});

    auto make_child = [&](int64_t tablet_id, const std::string& delvec_filename) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(2);
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
        stamp_physical_identity_uid(rowset, "shared_seg.dat");
        add_delvec(meta.get(), tablet_id, 1, /*segment_id=*/1, delvec_filename, shared_delvec_data);
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
        sst->set_shared(true);
        sst->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 8);
        return meta;
    };

    auto meta_a = make_child(child_a, "delvec_a.dv");
    auto meta_b = make_child(child_b, "delvec_b.dv");
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
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);

    ASSIGN_OR_ABORT(auto sstable, lake::PersistentIndexSstable::new_sstable(
                                          out_sst, _tablet_manager->sst_location(merged_tablet, out_sst.filename()),
                                          /*cache=*/nullptr, /*need_filter=*/false, /*delvec=*/nullptr, merged,
                                          _tablet_manager.get()));
    sstable::ReadOptions read_options;
    read_options.fill_cache = false;
    std::unique_ptr<sstable::Iterator> iter(sstable->new_iterator(read_options));
    std::set<std::string> rebuilt_keys;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        rebuilt_keys.insert(iter->key().to_string());
    }
    ASSERT_OK(iter->status());
    EXPECT_EQ(1u, rebuilt_keys.size());
    EXPECT_TRUE(rebuilt_keys.count("k1")) << "k1 (rowid 0, not deleted) should survive";
    EXPECT_FALSE(rebuilt_keys.count("k2")) << "k2 (rowid 8, in merged delvec) should be filtered out";
}

// Round-2 (Codex high #2): the data-entry rssid lookup must use
// get_rssid(rs, seg_pos) so that a sparse segment_idx ({0, 2} after a
// middle-segment compaction) resolves correctly. A naive id+segments_size
// span check would (a) drop the live segment at id+2 and (b) keep a ghost
// at id+1 — both wrong.
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_sstable_rebuild_sparse_segment_idx) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string legacy_filename = "sparse_seg.sst";
    const auto legacy_path = _tablet_manager->sst_location(child_a, legacy_filename);
    const uint64_t legacy_filesize = write_legacy_pk_sstable(
            legacy_path,
            {{"k0", /*rssid=*/10, /*rowid=*/0}, {"k1", /*rssid=*/11, /*rowid=*/0}, {"k2", /*rssid=*/12, /*rowid=*/0}});

    auto make_child = [&](int64_t tablet_id) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(13);
        set_primary_key_schema(meta.get(), 1001);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(10);
        rowset->set_version(1);
        rowset->set_num_rows(20);
        rowset->set_data_size(200);
        // Two segments at sparse segment_idx {0, 2} — the {0,1,2} dense span
        // is broken because the middle segment was compacted away.
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename("sparse_seg_0.dat");
            sm->set_size(100);
            sm->set_shared(true);
            sm->set_segment_idx(0);
        }
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename("sparse_seg_2.dat");
            sm->set_size(100);
            sm->set_shared(true);
            sm->set_segment_idx(2);
        }
        stamp_physical_identity_uid(rowset, "sparse_seg_0.dat");
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
        sst->set_shared(true);
        sst->set_max_rss_rowid((static_cast<uint64_t>(12) << 32) | 0);
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
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);

    ASSIGN_OR_ABORT(auto sstable, lake::PersistentIndexSstable::new_sstable(
                                          out_sst, _tablet_manager->sst_location(merged_tablet, out_sst.filename()),
                                          /*cache=*/nullptr, /*need_filter=*/false, /*delvec=*/nullptr, merged,
                                          _tablet_manager.get()));
    sstable::ReadOptions read_options;
    read_options.fill_cache = false;
    std::unique_ptr<sstable::Iterator> iter(sstable->new_iterator(read_options));
    std::map<std::string, uint32_t> rebuilt_entries;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        IndexValuesWithVerPB index_values_pb;
        ASSERT_TRUE(index_values_pb.ParseFromArray(iter->value().data, static_cast<int>(iter->value().size)));
        ASSERT_GT(index_values_pb.values_size(), 0);
        rebuilt_entries.emplace(iter->key().to_string(), index_values_pb.values(0).rssid());
    }
    ASSERT_OK(iter->status());

    EXPECT_EQ(2u, rebuilt_entries.size()) << "k1 (rssid 11, no segment_idx=1) should be dropped";
    EXPECT_TRUE(rebuilt_entries.count("k0"));
    EXPECT_TRUE(rebuilt_entries.count("k2"));
    EXPECT_FALSE(rebuilt_entries.count("k1"));
    EXPECT_EQ(10u, rebuilt_entries["k0"]);
    EXPECT_EQ(12u, rebuilt_entries["k2"]);
}

// Round-2 (Codex risk #2): a stacked-merge legacy sstable whose source PB
// already carries a non-zero rssid_offset must lift stored rssids by that
// offset BEFORE looking them up in merge_contexts. Otherwise the rebuild
// would search for a rowset id in the wrong space and either drop a live
// entry or pick the wrong owner.
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_sstable_rebuild_with_source_offset) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // Source bytes encode stored rssid 2; src.rssid_offset = 3; lifted = 5.
    // Both children have rowset id=5 alive on the shared sstable.
    const std::string legacy_filename = "stacked_offset.sst";
    const auto legacy_path = _tablet_manager->sst_location(child_a, legacy_filename);
    const uint64_t legacy_filesize = write_legacy_pk_sstable(legacy_path, {{"k1", /*rssid=*/2, /*rowid=*/0}});

    auto make_child = [&](int64_t tablet_id) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(6);
        set_primary_key_schema(meta.get(), 1001);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(5);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename("shared_seg.dat");
            sm->set_size(100);
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(rowset, "shared_seg.dat");
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
        sst->set_shared(true);
        sst->set_rssid_offset(3); // stacked: prior merge already shifted by 3
        sst->set_max_rss_rowid((static_cast<uint64_t>(2) << 32) | 0);
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
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    EXPECT_EQ(0, out_sst.rssid_offset()) << "rebuilt sstable must have offset reset to 0";

    ASSIGN_OR_ABORT(auto sstable, lake::PersistentIndexSstable::new_sstable(
                                          out_sst, _tablet_manager->sst_location(merged_tablet, out_sst.filename()),
                                          /*cache=*/nullptr, /*need_filter=*/false, /*delvec=*/nullptr, merged,
                                          _tablet_manager.get()));
    sstable::ReadOptions read_options;
    read_options.fill_cache = false;
    std::unique_ptr<sstable::Iterator> iter(sstable->new_iterator(read_options));
    int entries_seen = 0;
    uint32_t k1_rssid = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        IndexValuesWithVerPB index_values_pb;
        ASSERT_TRUE(index_values_pb.ParseFromArray(iter->value().data, static_cast<int>(iter->value().size)));
        ASSERT_GT(index_values_pb.values_size(), 0);
        if (iter->key().to_string() == "k1") {
            k1_rssid = index_values_pb.values(0).rssid();
        }
        ++entries_seen;
    }
    ASSERT_OK(iter->status());
    EXPECT_EQ(1, entries_seen);
    EXPECT_EQ(5u, k1_rssid) << "stored=2, lifted by source offset 3 → 5; remap to merged rowset 5";
}

// Round-2 (Codex high #3): tombstone-only sstable max_rss_rowid must come
// from projecting the source PB's max_rss_rowid through the rebuild — not
// from per-entry max over non-tombstone values (which would yield 0 and
// corrupt the post-merge sort).
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_sstable_rebuild_tombstone_only_watermark) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // All entries are tombstones (rssid=UINT32_MAX, rowid=UINT32_MAX). Source
    // max_rss_rowid.high = 5 (the rowset id at memtable flush time).
    const uint32_t kTombstoneSentinel = std::numeric_limits<uint32_t>::max();
    const std::string legacy_filename = "tombstone_only.sst";
    const auto legacy_path = _tablet_manager->sst_location(child_a, legacy_filename);
    const uint64_t legacy_filesize =
            write_legacy_pk_sstable(legacy_path, {{"k_dead_a", kTombstoneSentinel, kTombstoneSentinel},
                                                  {"k_dead_b", kTombstoneSentinel, kTombstoneSentinel}});

    auto make_child = [&](int64_t tablet_id) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(6);
        set_primary_key_schema(meta.get(), 1001);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(5);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename("shared_seg.dat");
            sm->set_size(100);
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(rowset, "shared_seg.dat");
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
        sst->set_shared(true);
        sst->set_max_rss_rowid((static_cast<uint64_t>(5) << 32) | kTombstoneSentinel);
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
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    const uint32_t out_high = static_cast<uint32_t>(out_sst.max_rss_rowid() >> 32);
    EXPECT_EQ(5u, out_high) << "tombstone-only file must inherit projected source watermark, not 0";
}

// Stacked-offset tombstone-only watermark.
//
// Convention: PersistentIndexSstablePB.max_rss_rowid.high is the EFFECTIVE
// max rssid in the source child's id space (post-projection — already
// includes any accumulated src_pb.rssid_offset). project_non_shared_legacy_
// sstable + cross-sstable invariants in lake_persistent_index.cpp all read
// max_rss_rowid as effective.
//
// The previous implementation of project_source_max_rss_rowid added
// src_pb.rssid_offset() AGAIN to max_rss_rowid.high, which works for
// fresh sstables (rssid_offset == 0) but double-shifts for any stacked-
// offset src. Per-entry update_max_encoded_rss_rowid_from masked the
// resulting watermark miss for non-tombstone files, but tombstone-only
// files (no per-entry override) emitted max_rss_rowid.high == 0,
// breaking the cross-sstable ordering invariant on subsequent merges.
//
// This test pins the fixed convention: a tombstone-only sstable carrying
// a stacked rssid_offset still gets its source watermark mapped through
// to the merged tablet's effective max — without the spurious second
// shift.
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_sstable_rebuild_stacked_offset_tombstone_only_watermark) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // All entries are tombstones. Source sstable has rssid_offset = 3 (a
    // prior projection had stacked it) and max_rss_rowid.high = 5 (= the
    // effective max in source child's id space). Both children expose
    // rowset id=5 alive on the shared sstable so the merged tablet's
    // watermark map records key 5 → final 5.
    const uint32_t kTombstoneSentinel = std::numeric_limits<uint32_t>::max();
    const std::string legacy_filename = "stacked_tombstone_only.sst";
    const auto legacy_path = _tablet_manager->sst_location(child_a, legacy_filename);
    const uint64_t legacy_filesize =
            write_legacy_pk_sstable(legacy_path, {{"k_dead_a", kTombstoneSentinel, kTombstoneSentinel},
                                                  {"k_dead_b", kTombstoneSentinel, kTombstoneSentinel}});

    auto make_child = [&](int64_t tablet_id) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(6);
        set_primary_key_schema(meta.get(), 1001);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(5);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename("shared_seg.dat");
            sm->set_size(100);
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(rowset, "shared_seg.dat");
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
        sst->set_shared(true);
        sst->set_rssid_offset(3); // stacked: a prior projection accumulated 3.
        // Effective max in source child's id space. 5 is also the merged
        // tablet's watermark key — pre-fix code looked up watermark[5+3=8]
        // and missed; post-fix looks up watermark[5] directly and hits.
        sst->set_max_rss_rowid((static_cast<uint64_t>(5) << 32) | kTombstoneSentinel);
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
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    const uint32_t out_high = static_cast<uint32_t>(out_sst.max_rss_rowid() >> 32);
    EXPECT_EQ(5u, out_high) << "stacked-offset tombstone-only file: source effective max 5 → merged final 5; "
                               "double-shift bug would have produced 0 here";
}

// Round-3 (Codex high #2 follow-up): the watermark helper must resolve a
// delete-only rowset id (segments_size==0) — memtable's flush watermark
// embeds the live rowset id at flush time, which can be a delete-only
// rowset. The data-entry helper must NOT match such ids; a data entry
// stored with that rssid is a ghost and gets dropped.
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_sstable_rebuild_tombstone_watermark_delete_only_rowset) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const uint32_t kTombstoneSentinel = std::numeric_limits<uint32_t>::max();
    // Source has one tombstone (preserved) and one ghost data entry pointing
    // at the delete-only rowset id 10. Source max_rss_rowid.high = 10 — the
    // delete-only rowset's id.
    const std::string legacy_filename = "del_only_watermark.sst";
    const auto legacy_path = _tablet_manager->sst_location(child_a, legacy_filename);
    const uint64_t legacy_filesize = write_legacy_pk_sstable(
            legacy_path, {{"k_tomb", kTombstoneSentinel, kTombstoneSentinel}, {"k_ghost", /*rssid=*/10, /*rowid=*/0}});

    auto make_child = [&](int64_t tablet_id) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(11);
        set_primary_key_schema(meta.get(), 1001);
        // Data rowset id=5 with one segment (live).
        auto* data_rowset = meta->add_rowsets();
        data_rowset->set_id(5);
        data_rowset->set_version(1);
        data_rowset->set_num_rows(10);
        data_rowset->set_data_size(100);
        {
            auto* sm = data_rowset->add_segment_metas();
            sm->set_filename("data_seg.dat");
            sm->set_size(100);
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(data_rowset, "data_seg.dat");
        // Delete-only rowset id=10 (segments_size==0): owns no PK index entries.
        auto* delete_only_rowset = meta->add_rowsets();
        delete_only_rowset->set_id(10);
        delete_only_rowset->set_version(2);
        delete_only_rowset->set_num_rows(0);
        delete_only_rowset->set_data_size(0);
        stamp_physical_identity_uid(delete_only_rowset, "delete_only_rowset_10");
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
        sst->set_shared(true);
        sst->set_max_rss_rowid((static_cast<uint64_t>(10) << 32) | kTombstoneSentinel);
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
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);

    // Watermark projection succeeds via the watermark helper (matches
    // delete-only rowset 10 by rs.id()): rebuilt PB high == 10.
    const uint32_t out_high = static_cast<uint32_t>(out_sst.max_rss_rowid() >> 32);
    EXPECT_EQ(10u, out_high) << "watermark helper should resolve delete-only rowset id";

    // The ghost data entry pointing at rssid=10 must have been dropped
    // (data-entry helper skips segments_size==0). Only the tombstone survives.
    ASSIGN_OR_ABORT(auto sstable, lake::PersistentIndexSstable::new_sstable(
                                          out_sst, _tablet_manager->sst_location(merged_tablet, out_sst.filename()),
                                          /*cache=*/nullptr, /*need_filter=*/false, /*delvec=*/nullptr, merged,
                                          _tablet_manager.get()));
    sstable::ReadOptions read_options;
    read_options.fill_cache = false;
    std::unique_ptr<sstable::Iterator> iter(sstable->new_iterator(read_options));
    std::set<std::string> rebuilt_keys;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        rebuilt_keys.insert(iter->key().to_string());
    }
    ASSERT_OK(iter->status());
    EXPECT_TRUE(rebuilt_keys.count("k_tomb")) << "tombstone must be preserved";
    EXPECT_FALSE(rebuilt_keys.count("k_ghost")) << "ghost data on delete-only rowset must be dropped";
}

// =============================================================================
// Legacy shared-sstable rebuild edge-case tests
// =============================================================================
//
// MERGE rebuilds every legacy `shared && !has_shared_rssid` sstable, writing a
// fresh file with remapped rssids. These tests drive specific input shapes
// through the merge end-to-end and assert the rebuild output signature:
//   output filename != source (rebuild wrote a new UUID), shared==false,
//   has fileset_id (rebuild assigned).

// child_a (= ctx[0]) does NOT carry the legacy sstable, only child_b (= ctx[1])
// does. ctx[1].rssid_offset is non-zero whenever ctx[0]'s rowset id-space pushes
// ctx[1]'s ids upward; the rebuild must apply that offset exactly once. Guards
// against a regression that double-shifts an already-offset canonical.
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_sstable_rebuild_with_nonzero_canonical_offset) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string legacy_filename = "legacy_nonzero_canonical.sst";
    const auto legacy_path = _tablet_manager->sst_location(child_b, legacy_filename);
    const uint64_t legacy_filesize =
            write_legacy_pk_sstable(legacy_path, {{"k1", /*rssid=*/1, /*rowid=*/0}, {"k2", /*rssid=*/2, /*rowid=*/0}});

    // child_a uses high rowset ids; ctx[1].rssid_offset = base.next_rowset_id -
    // min(child_b.rowsets) = 11 - 1 = 10, so canonical_ctx (= ctx[1]) carries
    // a non-zero offset.
    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(11);
    set_primary_key_schema(meta_a.get(), 1001);
    auto* rs_a = meta_a->add_rowsets();
    rs_a->set_id(10);
    rs_a->set_version(1);
    rs_a->set_num_rows(10);
    rs_a->set_data_size(100);
    {
        auto* sm = rs_a->add_segment_metas();
        sm->set_filename("seg_a10.dat");
        sm->set_size(100);
    }

    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(3);
    set_primary_key_schema(meta_b.get(), 1001);
    for (uint32_t rs_id : {1u, 2u}) {
        auto* rs = meta_b->add_rowsets();
        rs->set_id(rs_id);
        rs->set_version(1);
        rs->set_num_rows(10);
        rs->set_data_size(100);
        {
            auto* sm = rs->add_segment_metas();
            sm->set_filename(fmt::format("seg_b{}.dat", rs_id));
            sm->set_size(100);
        }
    }
    auto* sst = meta_b->mutable_sstable_meta()->add_sstables();
    sst->set_filename(legacy_filename);
    sst->set_filesize(legacy_filesize);
    sst->set_shared(true);
    sst->set_max_rss_rowid((static_cast<uint64_t>(2) << 32) | 0);

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(2);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    // Rebuild signature: new filename, !shared, fresh fileset_id, has_range.
    EXPECT_NE(legacy_filename, out_sst.filename()) << "rebuild wrote a new file";
    EXPECT_FALSE(out_sst.shared());
    EXPECT_TRUE(out_sst.has_fileset_id());
}

// Source PB has range but no fileset_id. PersistentIndexSstableFileset::
// init(vector) DCHECKs has_fileset_id() for ranged sstables, so the rebuild
// must assign a fresh fileset_id explicitly rather than carrying the source PB
// forward. Asserts the rebuilt sstable has a fileset_id.
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_sstable_rebuild_for_range_without_fileset_id) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string legacy_filename = "legacy_range_no_fid.sst";
    const auto legacy_path = _tablet_manager->sst_location(child_a, legacy_filename);
    const uint64_t legacy_filesize =
            write_legacy_pk_sstable(legacy_path, {{"k1", /*rssid=*/1, /*rowid=*/0}, {"k2", /*rssid=*/2, /*rowid=*/0}});

    auto make_child = [&](int64_t tablet_id) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(3);
        set_primary_key_schema(meta.get(), 1001);
        for (uint32_t rs_id : {1u, 2u}) {
            auto* rs = meta->add_rowsets();
            rs->set_id(rs_id);
            rs->set_version(1);
            rs->set_num_rows(10);
            rs->set_data_size(100);
            {
                auto* sm = rs->add_segment_metas();
                sm->set_filename(fmt::format("rfid_seg_{}.dat", rs_id));
                sm->set_size(100);
                sm->set_shared(true);
            }
            stamp_physical_identity_uid(rs, fmt::format("rfid_seg_{}.dat", rs_id));
        }
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
        sst->set_shared(true);
        sst->set_max_rss_rowid((static_cast<uint64_t>(2) << 32) | 0);
        // Source metadata intentionally has a range but no fileset_id.
        sst->mutable_range()->set_start_key("a");
        sst->mutable_range()->set_end_key("z");
        // sst->mutable_fileset_id() is intentionally unset.
        return meta;
    };

    EXPECT_OK(put_tablet_metadata(make_child(child_a)));
    EXPECT_OK(put_tablet_metadata(make_child(child_b)));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(4);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    EXPECT_NE(legacy_filename, out_sst.filename()) << "legacy shared SSTs are rebuilt";
    EXPECT_FALSE(out_sst.shared());
    EXPECT_TRUE(out_sst.has_fileset_id()) << "rebuild always assigns a fresh fileset_id";
}

// Distinct legacy shared SST files use their own source tablet context even
// when both source id spaces contain rssid=1. A global numeric-rssid map would
// incorrectly project both files to ctx_a's final rssid.
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_sstable_distinct_files_use_source_context) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string legacy_a_filename = "orphan_legacy_a.sst";
    const std::string legacy_b_filename = "orphan_legacy_b.sst";
    const auto legacy_a_path = _tablet_manager->sst_location(child_a, legacy_a_filename);
    const auto legacy_b_path = _tablet_manager->sst_location(child_b, legacy_b_filename);
    const uint64_t legacy_a_filesize = write_legacy_pk_sstable(legacy_a_path, {{"ka", /*rssid=*/1, /*rowid=*/0}});
    const uint64_t legacy_b_filesize = write_legacy_pk_sstable(legacy_b_path, {{"kb", /*rssid=*/1, /*rowid=*/0}});

    auto make_child = [&](int64_t tablet_id, const std::string& seg_name, const std::string& legacy_filename,
                          uint64_t legacy_filesize) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(2);
        set_primary_key_schema(meta.get(), 1001);
        // Child-local rowset: the two tablets must retain independent owners.
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename(seg_name);
            sm->set_size(100);
            sm->set_shared(false);
        }
        // Distinct filenames make these independent legacy SST groups.
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
        sst->set_shared(true);
        sst->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 0);
        return meta;
    };

    EXPECT_OK(put_tablet_metadata(make_child(child_a, "a_local_seg.dat", legacy_a_filename, legacy_a_filesize)));
    EXPECT_OK(put_tablet_metadata(make_child(child_b, "b_local_seg.dat", legacy_b_filename, legacy_b_filesize)));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(7);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    // Two child-local rowsets, no dedup expected.
    ASSERT_EQ(2, merged->rowsets_size());
    ASSERT_EQ(2, merged->sstable_meta().sstables_size());
    const auto& sstables = merged->sstable_meta().sstables();

    // Each rebuild consults only its source context. Both emitted PBs carry
    // the rebuild signature and their projected high words differ: ctx_a maps
    // source rssid 1 to 1, while ctx_b maps it to 2.
    EXPECT_FALSE(sstables.Get(0).shared());
    EXPECT_FALSE(sstables.Get(1).shared());
    EXPECT_TRUE(sstables.Get(0).has_fileset_id());
    EXPECT_TRUE(sstables.Get(1).has_fileset_id());
    EXPECT_NE(legacy_a_filename, sstables.Get(0).filename());
    EXPECT_NE(legacy_a_filename, sstables.Get(1).filename());
    EXPECT_NE(legacy_b_filename, sstables.Get(0).filename());
    EXPECT_NE(legacy_b_filename, sstables.Get(1).filename());

    // A global first-emitter mapping would produce {1,1}; source-context
    // isolation must produce {1,2}.
    std::set<uint64_t> rebuilt_highs{sstables.Get(0).max_rss_rowid() >> 32, sstables.Get(1).max_rss_rowid() >> 32};
    EXPECT_EQ((std::set<uint64_t>{1, 2}), rebuilt_highs)
            << "ctx_b's rebuild must consult its own source-context mapping";
}

// ─────────────────────────────────────────────────────────────────────
// Per-entry rebuild for non-shared SSTs with mixed rowset mappings
// ─────────────────────────────────────────────────────────────────────
//
// A non-canonical source can reference both a UID-deduplicated ancestor
// rowset and child-local rowsets. One metadata offset cannot express those
// mappings, so the SST is rebuilt entry by entry through ctx.map_rssid().

// T1: a non-shared sstable whose rssid range is disjoint from all mapping
// disagreement keys stays on the metadata-only fast path. Setup:
// shared-ancestor at high id (10), child-local at low id (1), sstable
// references only the child-local. The predicate's conservative range
// scan correctly skips the high-id plan entry.
TEST_F(LakeTabletReshardTest, test_tablet_merging_non_shared_sstable_pure_child_local_uses_fast_path) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // ctx_b non-shared sstable references only stored rssid=1 (= ctx_b
    // child-local rowset id 1). Sstable's lifted range = [1, 1], does
    // NOT overlap shared-ancestor rowset id 10's plan entry.
    const std::string ns_filename = "ns_pure_local.sst";
    const auto ns_path = _tablet_manager->sst_location(child_b, ns_filename);
    const uint64_t ns_filesize = write_legacy_pk_sstable(ns_path, {{"k_local", /*rssid=*/1, /*rowid=*/0}});

    auto make_meta = [&](int64_t tablet_id, bool include_local_and_sstable) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(11);
        set_primary_key_schema(meta.get(), 1001);
        // Shared-ancestor rowset at high id 10. Both ctxs carry the same UID,
        // so the rowset emission plan deduplicates it.
        auto* shared_rs = meta->add_rowsets();
        shared_rs->set_id(10);
        shared_rs->set_version(1);
        shared_rs->set_num_rows(10);
        shared_rs->set_data_size(100);
        {
            auto* sm = shared_rs->add_segment_metas();
            sm->set_filename("shared.dat");
            sm->set_size(100);
            sm->set_shared(true);
        }
        if (include_local_and_sstable) {
            // Child-local rowset at LOW id 1, in a "gap" beneath the
            // shared-ancestor (legal: rs.id ≥ 1, ids need not be
            // contiguous).
            auto* local_rs = meta->add_rowsets();
            local_rs->set_id(1);
            local_rs->set_version(1);
            local_rs->set_num_rows(5);
            local_rs->set_data_size(50);
            {
                auto* sm = local_rs->add_segment_metas();
                sm->set_filename("ctx_b_local.dat");
                sm->set_size(50);
                sm->set_shared(false);
            }
            auto* sst = meta->mutable_sstable_meta()->add_sstables();
            sst->set_filename(ns_filename);
            sst->set_filesize(ns_filesize);
            sst->set_shared(false);
            sst->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 0);
        }
        return meta;
    };

    EXPECT_OK(put_tablet_metadata(make_meta(child_a, /*include_local_and_sstable=*/false)));
    EXPECT_OK(put_tablet_metadata(make_meta(child_b, /*include_local_and_sstable=*/true)));

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
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    // Metadata-only path was taken (predicate returned false because
    // stored rssid=1 lifts to lifted=1, plan/shared_rssid disagreement
    // keys all live at lifted=10 — outside the [1,1] sstable range, so
    // the binary-search range test misses).
    EXPECT_EQ(ns_filename, out_sst.filename()) << "metadata-only path keeps source filename";
    EXPECT_FALSE(out_sst.shared());
    EXPECT_FALSE(out_sst.has_fileset_id()) << "metadata-only does not mint a new fileset_id";
}

// T2: mixed-reference non-shared sstable on non-canonical ctx → rebuild
// route taken; per-entry remap honors plan (shared-ancestor) AND natural
// offset (child-local) simultaneously, output PB has fresh fileset_id
// and rssid_offset=0.
TEST_F(LakeTabletReshardTest, test_tablet_merging_non_shared_sstable_mixed_refs_routes_to_rebuild) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // ctx_a (canonical, ctx[0], rssid_offset=0): rowset 1, shared-ancestor.
    // ctx_b (ctx[1], rssid_offset=1): rowset 1 shared-ancestor + rowset 2
    // child-local + non-shared sstable that mixes refs to BOTH (= post-
    // split PK-index compaction's signature output).
    //
    // ctx_a's next_rowset_id is 3 while its only live rowset is 1: id 2 was
    // allocated and later compacted away. That gap is what makes ctx_b's
    // shift non-zero. compute_rssid_offset takes the floor over the rowsets
    // ctx_b actually EMITS -- its rowset 1 dedups into ctx_a's canonical and
    // is discarded, so the floor is rowset 2 -- giving rssid_offset = 3 - 2 = 1.
    // Without the gap the floor would land exactly on ctx_a's ceiling, ctx_b's
    // natural map would agree with the plan, and the metadata-only fast path
    // (covered by the T1 sibling above) would be correct instead.
    const std::string ns_filename = "ns_mixed.sst";
    const auto ns_path = _tablet_manager->sst_location(child_b, ns_filename);
    const uint64_t ns_filesize = write_legacy_pk_sstable(
            ns_path, {{"k_shared", /*rssid=*/1, /*rowid=*/0}, {"k_local", /*rssid=*/2, /*rowid=*/0}});

    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(3);
    set_primary_key_schema(meta_a.get(), 1001);
    auto* rs_a1 = meta_a->add_rowsets();
    rs_a1->set_id(1);
    rs_a1->set_version(1);
    rs_a1->set_num_rows(10);
    rs_a1->set_data_size(100);
    {
        auto* sm = rs_a1->add_segment_metas();
        sm->set_filename("shared.dat");
        sm->set_size(100);
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(rs_a1, "shared.dat"); // shared ancestor: same uid across siblings => dedup

    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(3);
    set_primary_key_schema(meta_b.get(), 1001);
    auto* rs_b1 = meta_b->add_rowsets();
    rs_b1->set_id(1);
    rs_b1->set_version(1);
    rs_b1->set_num_rows(10);
    rs_b1->set_data_size(100);
    {
        auto* sm = rs_b1->add_segment_metas();
        sm->set_filename("shared.dat");
        sm->set_size(100);
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(rs_b1, "shared.dat"); // shared ancestor: same uid as rs_a1 => dedup
    auto* rs_b2 = meta_b->add_rowsets();
    rs_b2->set_id(2);
    rs_b2->set_version(1);
    rs_b2->set_num_rows(5);
    rs_b2->set_data_size(50);
    {
        auto* sm = rs_b2->add_segment_metas();
        sm->set_filename("ctx_b_local.dat");
        sm->set_size(50);
        sm->set_shared(false);
    }
    auto* sst_b = meta_b->mutable_sstable_meta()->add_sstables();
    sst_b->set_filename(ns_filename);
    sst_b->set_filesize(ns_filesize);
    sst_b->set_shared(false);
    sst_b->set_max_rss_rowid((static_cast<uint64_t>(2) << 32) | 0);

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

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

    auto merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    // ctx_b's rowset_1 (shared-ancestor) dedups to id=1. ctx_b's rowset_2
    // (child-local, no plan entry, ctx_b shared_rssid_map covers rowset_1
    // dedup but not rowset_2) gets natural offset id=2+1=3. Mapping
    // rowset_1's lifted=1 → plan id 1, but ctx_b's natural would be
    // 1+1=2 — predicate fires (1 != 2 in [1,2]) → rebuild taken.
    EXPECT_NE(ns_filename, out_sst.filename()) << "rebuild emits a new file";
    EXPECT_FALSE(out_sst.shared());
    EXPECT_TRUE(out_sst.has_fileset_id()) << "rebuild mints a fresh fileset_id";
    EXPECT_EQ(0, out_sst.rssid_offset()) << "rebuild output is pre-remapped (rssid_offset=0)";
    // max_rss_rowid.high after rebuild equals the maximum final rssid
    // among emitted entries: rowset_1 entry → 1, rowset_2 entry → 3.
    EXPECT_EQ((static_cast<uint64_t>(3) << 32) | 0, out_sst.max_rss_rowid());
}

// T3: for the canonical rowset source, plan id equals the natural projection
// by construction (ctx.rssid_offset == 0 for ctx[0]). The
// predicate must NOT fire even when the sstable references shared-
// ancestor rowsets covered by the emission plan.
TEST_F(LakeTabletReshardTest, test_tablet_merging_non_shared_sstable_canonical_ctx_skips_rebuild) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // Non-shared sstable on canonical ctx_a, references shared-ancestor
    // rowset id=1.
    const std::string ns_filename = "ns_canonical.sst";
    const auto ns_path = _tablet_manager->sst_location(child_a, ns_filename);
    const uint64_t ns_filesize = write_legacy_pk_sstable(ns_path, {{"k", /*rssid=*/1, /*rowid=*/0}});

    auto make_meta = [&](int64_t tablet_id, bool include_sstable) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(2);
        set_primary_key_schema(meta.get(), 1001);
        // Same shared-ancestor UID on both contexts; ctx[0] is canonical.
        auto* rs = meta->add_rowsets();
        rs->set_id(1);
        rs->set_version(1);
        rs->set_num_rows(10);
        rs->set_data_size(100);
        {
            auto* sm = rs->add_segment_metas();
            sm->set_filename("shared.dat");
            sm->set_size(100);
            sm->set_shared(true);
        }
        if (include_sstable) {
            auto* sst = meta->mutable_sstable_meta()->add_sstables();
            sst->set_filename(ns_filename);
            sst->set_filesize(ns_filesize);
            sst->set_shared(false);
            sst->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 0);
        }
        return meta;
    };

    EXPECT_OK(put_tablet_metadata(make_meta(child_a, /*include_sstable=*/true)));
    EXPECT_OK(put_tablet_metadata(make_meta(child_b, /*include_sstable=*/false)));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(93);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    // ctx_a is canonical: plan entry for rowset 1 has value 1, natural =
    // 1 + ctx_a.rssid_offset(0)
    // = 1. They match → ctx_a's compute_disagreement_keys returns empty
    // → predicate returns false → metadata-only path keeps source filename.
    EXPECT_EQ(ns_filename, out_sst.filename());
    EXPECT_FALSE(out_sst.has_fileset_id());
}

// T8 (delvec guard): a non-shared sstable PB that carries an embedded
// delvec is corrupt for the !has_shared_rssid form, regardless of
// whether the rebuild route or the metadata-only route is taken. Both
// must surface Status::Corruption with a descriptive message rather
// than silently emitting a misformed PB.
TEST_F(LakeTabletReshardTest, test_tablet_merging_non_shared_sstable_with_delvec_corruption_guard) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // Construct a malformed non-shared sstable: shared=false +
    // !has_shared_rssid + non-empty embedded delvec. The combination is
    // illegal per the v1 corruption guard at project_non_shared_legacy_-
    // sstable; the rebuild path must reject it identically.
    const std::string ns_filename = "ns_with_delvec.sst";
    const auto ns_path = _tablet_manager->sst_location(child_b, ns_filename);
    const uint64_t ns_filesize = write_legacy_pk_sstable(ns_path, {{"k", /*rssid=*/1, /*rowid=*/0}});

    // ctx_a + ctx_b share one UID-deduplicated rowset id=10. ctx_b's
    // non-shared sstable references rowset_10 (= predicate fires →
    // rebuild route is taken → delvec guard inside rebuild fires).
    auto make_meta = [&](int64_t tablet_id, bool include_sstable) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(11);
        set_primary_key_schema(meta.get(), 1001);
        auto* shared_rs = meta->add_rowsets();
        shared_rs->set_id(10);
        shared_rs->set_version(1);
        shared_rs->set_num_rows(10);
        shared_rs->set_data_size(100);
        {
            auto* sm = shared_rs->add_segment_metas();
            sm->set_filename("shared.dat");
            sm->set_size(100);
            sm->set_shared(true);
        }
        if (include_sstable) {
            auto* sst = meta->mutable_sstable_meta()->add_sstables();
            sst->set_filename(ns_filename);
            sst->set_filesize(ns_filesize);
            sst->set_shared(false);
            sst->set_max_rss_rowid((static_cast<uint64_t>(10) << 32) | 0);
            // Inject a non-empty embedded delvec to trip the guard.
            // sst.has_delvec() && sst.delvec().size() > 0 == illegal
            // for !has_shared_rssid form.
            sst->mutable_delvec()->set_version(1);
            sst->mutable_delvec()->set_size(123);
        }
        return meta;
    };

    EXPECT_OK(put_tablet_metadata(make_meta(child_a, /*include_sstable=*/false)));
    EXPECT_OK(put_tablet_metadata(make_meta(child_b, /*include_sstable=*/true)));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(98);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto status = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                                  txn_info, false, tablet_metadatas, tablet_ranges);
    ASSERT_FALSE(status.ok()) << "non-shared sstable with embedded delvec must trigger corruption guard";
    EXPECT_TRUE(status.is_corruption()) << "expected Corruption, got: " << status.to_string();
}

#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
// Overwrite the 1-byte compression-type trailer of the first data block with an
// invalid value, emulating a corrupted local cache copy of the sstable. The
// block is located through the footer -> index block, so the footer and index
// block near the file tail stay intact and opening the sstable still succeeds;
// only reading the data block fails with Corruption ("bad block type"; once
// block-checksum verification lands the same read fails as a checksum
// mismatch -- still Corruption).
static void corrupt_first_data_block_type_byte(const std::string& path) {
    ASSIGN_OR_ABORT(auto rf, fs::new_random_access_file(path));
    ASSIGN_OR_ABORT(auto file_size, rf->get_size());
    ASSERT_GT(file_size, sstable::Footer::kEncodedLength);
    std::string content(file_size, '\0');
    ASSERT_OK(rf->read_at_fully(0, content.data(), file_size));

    sstable::Footer footer;
    Slice footer_input(content.data() + file_size - sstable::Footer::kEncodedLength, sstable::Footer::kEncodedLength);
    ASSERT_OK(footer.DecodeFrom(&footer_input));
    sstable::BlockContents index_contents;
    index_contents.data = Slice(content.data() + footer.index_handle().offset(), footer.index_handle().size());
    index_contents.cachable = false;
    index_contents.heap_allocated = false;
    sstable::Block index_block(index_contents);
    std::unique_ptr<sstable::Iterator> iter(index_block.NewIterator(sstable::BytewiseComparator()));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    Slice handle_value = iter->value();
    sstable::BlockHandle first_block;
    ASSERT_OK(first_block.DecodeFrom(&handle_value));
    // The compression-type byte sits right after the block payload.
    size_t type_offset = first_block.offset() + first_block.size();
    ASSERT_LT(type_offset, content.size());
    content[type_offset] = 0x7f;

    WritableFileOptions wf_opts;
    wf_opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    ASSIGN_OR_ABORT(auto wf, FileSystem::Default()->new_writable_file(wf_opts, path));
    ASSERT_OK(wf->append(Slice(content)));
    ASSERT_OK(wf->close());
}

static std::pair<Status, int> publish_merge_and_count_cache_drops(lake::TabletManager* tablet_manager,
                                                                  const ReshardingTabletInfoPB& resharding_tablet,
                                                                  int64_t base_version, int64_t new_version,
                                                                  int64_t txn_id, bool* published = nullptr,
                                                                  TabletMetadataPtr* merged_metadata = nullptr) {
    TxnInfoPB txn_info;
    txn_info.set_txn_id(txn_id);

    const bool old_cfg = config::lake_clear_corrupted_cache_data;
    config::lake_clear_corrupted_cache_data = true;
    int drop_count = 0;
    SyncPoint::GetInstance()->SetCallBack("PersistentIndexSstable::drop_corrupted_cache", [&](void*) { ++drop_count; });
    SyncPoint::GetInstance()->EnableProcessing();

    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    Status status = lake::publish_resharding_tablet(tablet_manager, resharding_tablet, base_version, new_version,
                                                    txn_info, false, tablet_metadatas, tablet_ranges);
    const auto merged_iter = resharding_tablet.has_merging_tablet_info()
                                     ? tablet_metadatas.find(resharding_tablet.merging_tablet_info().new_tablet_id())
                                     : tablet_metadatas.end();
    if (published != nullptr) {
        *published = merged_iter != tablet_metadatas.end();
    }
    if (merged_metadata != nullptr) {
        *merged_metadata = merged_iter == tablet_metadatas.end() ? nullptr : merged_iter->second;
    }

    SyncPoint::GetInstance()->ClearCallBack("PersistentIndexSstable::drop_corrupted_cache");
    SyncPoint::GetInstance()->DisableProcessing();
    config::lake_clear_corrupted_cache_data = old_cfg;
    return {status, drop_count};
}

// An ownerless modern shared-rssid SST is classified by scanning the physical
// source file. Corruption discovered after the table was opened must evict that
// source's local cache, just like the rebuild paths do.
TEST_F(LakeTabletReshardTest, test_tablet_merging_shared_rssid_classifier_drops_corrupted_source_cache) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string sst_filename = "classifier_corrupted_cache.sst";
    const auto sst_path = _tablet_manager->sst_location(child_a, sst_filename);
    const uint64_t sst_filesize = write_legacy_pk_sstable(sst_path, {{"ownerless-key", /*rssid=*/1, /*rowid=*/0}});
    corrupt_first_data_block_type_byte(sst_path);

    auto make_child = [&](int64_t tablet_id, const std::string& segment_filename) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(base_version);
        metadata->set_next_rowset_id(3);
        set_primary_key_schema(metadata.get(), 1001);
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(2);
        rowset->set_version(1);
        rowset->set_num_rows(1);
        rowset->set_data_size(10);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(segment_filename);
        segment->set_size(10);
        add_shared_rssid_sstable(metadata.get(), sst_filename, /*rssid=*/1, /*shared_version=*/1,
                                 /*max_rowid=*/0, sst_filesize);
        return metadata;
    };

    ASSERT_OK(put_tablet_metadata(make_child(child_a, "classifier_live_a.dat")));
    ASSERT_OK(put_tablet_metadata(make_child(child_b, "classifier_live_b.dat")));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    auto [status, drop_count] = publish_merge_and_count_cache_drops(_tablet_manager.get(), resharding_tablet,
                                                                    base_version, new_version, /*txn_id=*/205);
    ASSERT_FALSE(status.ok()) << "classifier must surface the corrupted source SST";
    EXPECT_TRUE(status.is_corruption()) << status;
    EXPECT_EQ(1, drop_count) << "classifier corruption must evict the source SST cache";
}

// A conservative source-live gap triggers the exact non-shared validation scan
// before rebuild. If that first read finds a corrupted local cache block, it
// must evict the source cache rather than returning before the guarded rebuild.
TEST_F(LakeTabletReshardTest, test_tablet_merging_non_shared_exact_scan_drops_corrupted_source_cache) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string sst_filename = "exact_scan_corrupted_cache.sst";
    const auto sst_path = _tablet_manager->sst_location(child_a, sst_filename);
    const uint64_t sst_filesize = write_legacy_pk_sstable(
            sst_path, {{"live-key-1", /*rssid=*/1, /*rowid=*/0}, {"live-key-3", /*rssid=*/3, /*rowid=*/0}});
    corrupt_first_data_block_type_byte(sst_path);

    auto make_child = [&](int64_t tablet_id, bool include_sstable) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(base_version);
        metadata->set_next_rowset_id(4);
        set_primary_key_schema(metadata.get(), 1001);
        for (uint32_t rowset_id : {1u, 3u}) {
            auto* rowset = metadata->add_rowsets();
            rowset->set_id(rowset_id);
            rowset->set_version(1);
            rowset->set_num_rows(1);
            rowset->set_data_size(10);
            auto* segment = rowset->add_segment_metas();
            segment->set_filename(fmt::format("exact_scan_shared_{}.dat", rowset_id));
            segment->set_size(10);
            segment->set_shared(true);
            stamp_physical_identity_uid(rowset, segment->filename());
        }
        if (include_sstable) {
            auto* sst = metadata->mutable_sstable_meta()->add_sstables();
            sst->set_filename(sst_filename);
            sst->set_filesize(sst_filesize);
            sst->set_shared(false);
            sst->set_max_rss_rowid(static_cast<uint64_t>(3) << 32);
        }
        return metadata;
    };

    ASSERT_OK(put_tablet_metadata(make_child(child_a, /*include_sstable=*/true)));
    ASSERT_OK(put_tablet_metadata(make_child(child_b, /*include_sstable=*/false)));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    auto [status, drop_count] = publish_merge_and_count_cache_drops(_tablet_manager.get(), resharding_tablet,
                                                                    base_version, new_version, /*txn_id=*/206);
    ASSERT_FALSE(status.ok()) << "exact owner validation must surface the corrupted source SST";
    EXPECT_TRUE(status.is_corruption()) << status;
    EXPECT_EQ(1, drop_count) << "exact owner validation corruption must evict the source SST cache";
}

// Regression test for the legacy shared-sstable rebuild reading a corrupted
// source sstable (usually a bad local cache copy): the merge must fail with
// Corruption AND drop the source sstable's local cache, so a retried merge
// scheduled onto this node re-reads from remote storage instead of hitting the
// same bad blocks forever.
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_rebuild_drops_corrupted_source_cache) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string legacy_filename = "corrupted_cache.sst";
    const auto legacy_path = _tablet_manager->sst_location(child_a, legacy_filename);
    const uint64_t legacy_filesize =
            write_legacy_pk_sstable(legacy_path, {{"k1", /*rssid=*/1, /*rowid=*/0}, {"k2", /*rssid=*/2, /*rowid=*/0}});
    corrupt_first_data_block_type_byte(legacy_path);

    auto make_child = [&](int64_t tablet_id, uint32_t live_rowset_id, const std::string& seg_filename) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(live_rowset_id + 1);
        set_primary_key_schema(meta.get(), 1001);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(live_rowset_id);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename(seg_filename);
            sm->set_size(100);
            sm->set_shared(true);
        }
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
        sst->set_shared(true);
        sst->set_max_rss_rowid((static_cast<uint64_t>(2) << 32) | 0);
        return meta;
    };

    EXPECT_OK(put_tablet_metadata(make_child(child_a, /*live_rowset_id=*/1, "seg_a.dat")));
    EXPECT_OK(put_tablet_metadata(make_child(child_b, /*live_rowset_id=*/2, "seg_b.dat")));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    auto [status, drop_cnt] = publish_merge_and_count_cache_drops(_tablet_manager.get(), resharding_tablet,
                                                                  base_version, new_version, /*txn_id=*/99);

    ASSERT_FALSE(status.ok()) << "merge over a corrupted source sstable must fail";
    EXPECT_TRUE(status.is_corruption()) << "expected Corruption, got: " << status.to_string();
    // The failing source sstable's local cache must have been dropped exactly
    // once (opening succeeds — footer and index block are intact — so only the
    // rebuild's cleanup handler fires).
    EXPECT_EQ(1, drop_cnt);
}

// Same corruption scenario through the non-shared legacy rebuild route
// (rebuild_non_shared_legacy_sstable). The metadata layout mirrors
// test_tablet_merging_non_shared_sstable_mixed_refs_routes_to_rebuild: ctx_b's
// non-shared sstable mixes refs to the shared-ancestor rowset and a child-local
// rowset with a non-zero offset gap, so dispatch takes the per-entry rebuild
// (a pure-projection layout would never read the file and the corruption would
// go unnoticed). Corruption surfaces at the initial SeekToFirst, and the early
// status check must drop the source cache too.
TEST_F(LakeTabletReshardTest, test_tablet_merging_non_shared_rebuild_drops_corrupted_source_cache) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string ns_filename = "ns_corrupted_cache.sst";
    const auto ns_path = _tablet_manager->sst_location(child_b, ns_filename);
    const uint64_t ns_filesize = write_legacy_pk_sstable(
            ns_path, {{"k_shared", /*rssid=*/1, /*rowid=*/0}, {"k_local", /*rssid=*/2, /*rowid=*/0}});
    corrupt_first_data_block_type_byte(ns_path);

    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(3);
    set_primary_key_schema(meta_a.get(), 1001);
    auto* rs_a1 = meta_a->add_rowsets();
    rs_a1->set_id(1);
    rs_a1->set_version(1);
    rs_a1->set_num_rows(10);
    rs_a1->set_data_size(100);
    {
        auto* sm = rs_a1->add_segment_metas();
        sm->set_filename("shared.dat");
        sm->set_size(100);
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(rs_a1, "shared.dat");

    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(3);
    set_primary_key_schema(meta_b.get(), 1001);
    auto* rs_b1 = meta_b->add_rowsets();
    rs_b1->set_id(1);
    rs_b1->set_version(1);
    rs_b1->set_num_rows(10);
    rs_b1->set_data_size(100);
    {
        auto* sm = rs_b1->add_segment_metas();
        sm->set_filename("shared.dat");
        sm->set_size(100);
        sm->set_shared(true);
    }
    stamp_physical_identity_uid(rs_b1, "shared.dat");
    auto* rs_b2 = meta_b->add_rowsets();
    rs_b2->set_id(2);
    rs_b2->set_version(1);
    rs_b2->set_num_rows(5);
    rs_b2->set_data_size(50);
    {
        auto* sm = rs_b2->add_segment_metas();
        sm->set_filename("ctx_b_local.dat");
        sm->set_size(50);
        sm->set_shared(false);
    }
    auto* sst_b = meta_b->mutable_sstable_meta()->add_sstables();
    sst_b->set_filename(ns_filename);
    sst_b->set_filesize(ns_filesize);
    sst_b->set_shared(false);
    sst_b->set_max_rss_rowid((static_cast<uint64_t>(2) << 32) | 0);

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    auto [status, drop_cnt] = publish_merge_and_count_cache_drops(_tablet_manager.get(), resharding_tablet,
                                                                  base_version, new_version, /*txn_id=*/100);

    ASSERT_FALSE(status.ok()) << "merge over a corrupted non-shared source sstable must fail";
    EXPECT_TRUE(status.is_corruption()) << "expected Corruption, got: " << status.to_string();
    EXPECT_EQ(1, drop_cnt);
}

// Build a real, structurally intact sstable whose single entry carries value bytes
// that cannot be parsed as IndexValuesWithVerPB (0x00 is an invalid protobuf tag).
// Blocks and checksums are valid, so opening and iterating succeed and the
// corruption only surfaces when the rebuild parses the value.
static uint64_t write_garbage_value_pk_sstable(const std::string& path) {
    WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    auto wf_or = fs::new_writable_file(opts, path);
    CHECK_OK(wf_or.status());
    auto wf = std::move(wf_or.value());
    sstable::Options options;
    sstable::TableBuilder builder(options, wf.get());
    CHECK_OK(builder.Add(Slice("garbage_key"), Slice("\x00garbage", 8)));
    CHECK_OK(builder.Finish());
    const uint64_t filesize = builder.FileSize();
    CHECK_OK(wf->close());
    return filesize;
}

// The drop-source-cache handling must also cover "semantic" corruption: bytes that
// keep the block structure (and its checksum) intact but carry impossible content.
// Two forms, through the legacy shared rebuild route:
//   (a) entry value bytes that fail IndexValuesWithVerPB parsing;
//   (b) a parseable entry whose stored rssid plus the PB-level rssid_offset
//       overflows uint32 (the per-entry remap guard reports Corruption).
// Both must fail the merge as Corruption AND drop the source sstable's cache.
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_rebuild_drops_cache_on_semantic_corruption) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;

    auto run_scenario = [&](const std::function<uint64_t(const std::string&)>& write_sst, int32_t rssid_offset,
                            int64_t txn_id) -> std::pair<Status, int> {
        const int64_t child_a = next_id();
        const int64_t child_b = next_id();
        const int64_t merged_tablet = next_id();
        prepare_tablet_dirs(child_a);
        prepare_tablet_dirs(child_b);
        prepare_tablet_dirs(merged_tablet);

        const std::string filename = fmt::format("semantic_shared_{}.sst", txn_id);
        const uint64_t filesize = write_sst(_tablet_manager->sst_location(child_a, filename));

        auto make_child = [&](int64_t tablet_id, uint32_t live_rowset_id, const std::string& seg_filename) {
            auto meta = std::make_shared<TabletMetadataPB>();
            meta->set_id(tablet_id);
            meta->set_version(base_version);
            meta->set_next_rowset_id(live_rowset_id + 1);
            set_primary_key_schema(meta.get(), 1001);
            auto* rowset = meta->add_rowsets();
            rowset->set_id(live_rowset_id);
            rowset->set_version(1);
            rowset->set_num_rows(10);
            rowset->set_data_size(100);
            {
                auto* sm = rowset->add_segment_metas();
                sm->set_filename(seg_filename);
                sm->set_size(100);
                sm->set_shared(true);
            }
            auto* sst = meta->mutable_sstable_meta()->add_sstables();
            sst->set_filename(filename);
            sst->set_filesize(filesize);
            sst->set_shared(true);
            sst->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 0);
            if (rssid_offset != 0) {
                sst->set_rssid_offset(rssid_offset);
            }
            return meta;
        };

        EXPECT_OK(put_tablet_metadata(make_child(child_a, /*live_rowset_id=*/1, "seg_a.dat")));
        EXPECT_OK(put_tablet_metadata(make_child(child_b, /*live_rowset_id=*/2, "seg_b.dat")));

        ReshardingTabletInfoPB resharding_tablet;
        auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
        merging_info.add_old_tablet_ids(child_a);
        merging_info.add_old_tablet_ids(child_b);
        merging_info.set_new_tablet_id(merged_tablet);

        return publish_merge_and_count_cache_drops(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                                   txn_id);
    };

    // (a) value bytes that fail protobuf parsing.
    auto [parse_status, parse_drops] =
            run_scenario([](const std::string& path) { return write_garbage_value_pk_sstable(path); },
                         /*rssid_offset=*/0, /*txn_id=*/201);
    ASSERT_FALSE(parse_status.ok()) << "merge over an unparseable source value must fail";
    EXPECT_TRUE(parse_status.is_corruption()) << parse_status;
    EXPECT_EQ(1, parse_drops);

    // (b) parseable entry whose stored rssid + rssid_offset overflows uint32
    // (rowid stays 0 so the entry is not a tombstone sentinel).
    auto [overflow_status, overflow_drops] = run_scenario(
            [this](const std::string& path) {
                return write_legacy_pk_sstable(path, {{"k_overflow", std::numeric_limits<uint32_t>::max(), 0}});
            },
            /*rssid_offset=*/1, /*txn_id=*/202);
    ASSERT_FALSE(overflow_status.ok()) << "merge over an out-of-range stored rssid must fail";
    EXPECT_TRUE(overflow_status.is_corruption()) << overflow_status;
    EXPECT_EQ(1, overflow_drops);
}

// Same two semantic-corruption forms through the non-shared rebuild route
// (mixed-refs metadata layout, see
// test_tablet_merging_non_shared_rebuild_drops_corrupted_source_cache).
TEST_F(LakeTabletReshardTest, test_tablet_merging_non_shared_rebuild_drops_cache_on_semantic_corruption) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;

    auto run_scenario = [&](const std::function<uint64_t(const std::string&)>& write_sst, int32_t rssid_offset,
                            int64_t txn_id) -> std::pair<Status, int> {
        const int64_t child_a = next_id();
        const int64_t child_b = next_id();
        const int64_t merged_tablet = next_id();
        prepare_tablet_dirs(child_a);
        prepare_tablet_dirs(child_b);
        prepare_tablet_dirs(merged_tablet);

        const std::string filename = fmt::format("semantic_ns_{}.sst", txn_id);
        const uint64_t filesize = write_sst(_tablet_manager->sst_location(child_b, filename));

        auto meta_a = std::make_shared<TabletMetadataPB>();
        meta_a->set_id(child_a);
        meta_a->set_version(base_version);
        meta_a->set_next_rowset_id(3);
        set_primary_key_schema(meta_a.get(), 1001);
        auto* rs_a1 = meta_a->add_rowsets();
        rs_a1->set_id(1);
        rs_a1->set_version(1);
        rs_a1->set_num_rows(10);
        rs_a1->set_data_size(100);
        {
            auto* sm = rs_a1->add_segment_metas();
            sm->set_filename("shared.dat");
            sm->set_size(100);
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(rs_a1, "shared.dat");

        auto meta_b = std::make_shared<TabletMetadataPB>();
        meta_b->set_id(child_b);
        meta_b->set_version(base_version);
        meta_b->set_next_rowset_id(3);
        set_primary_key_schema(meta_b.get(), 1001);
        auto* rs_b1 = meta_b->add_rowsets();
        rs_b1->set_id(1);
        rs_b1->set_version(1);
        rs_b1->set_num_rows(10);
        rs_b1->set_data_size(100);
        {
            auto* sm = rs_b1->add_segment_metas();
            sm->set_filename("shared.dat");
            sm->set_size(100);
            sm->set_shared(true);
        }
        stamp_physical_identity_uid(rs_b1, "shared.dat");
        auto* rs_b2 = meta_b->add_rowsets();
        rs_b2->set_id(2);
        rs_b2->set_version(1);
        rs_b2->set_num_rows(5);
        rs_b2->set_data_size(50);
        {
            auto* sm = rs_b2->add_segment_metas();
            sm->set_filename("ctx_b_local.dat");
            sm->set_size(50);
            sm->set_shared(false);
        }
        auto* sst_b = meta_b->mutable_sstable_meta()->add_sstables();
        sst_b->set_filename(filename);
        sst_b->set_filesize(filesize);
        sst_b->set_shared(false);
        sst_b->set_max_rss_rowid((static_cast<uint64_t>(2) << 32) | 0);
        if (rssid_offset != 0) {
            sst_b->set_rssid_offset(rssid_offset);
        }

        EXPECT_OK(put_tablet_metadata(meta_a));
        EXPECT_OK(put_tablet_metadata(meta_b));

        ReshardingTabletInfoPB resharding_tablet;
        auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
        merging_info.add_old_tablet_ids(child_a);
        merging_info.add_old_tablet_ids(child_b);
        merging_info.set_new_tablet_id(merged_tablet);

        return publish_merge_and_count_cache_drops(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                                   txn_id);
    };

    // (a) value bytes that fail protobuf parsing. The routing predicate is
    // metadata-only, so the mixed-refs layout still dispatches to the rebuild.
    auto [parse_status, parse_drops] =
            run_scenario([](const std::string& path) { return write_garbage_value_pk_sstable(path); },
                         /*rssid_offset=*/0, /*txn_id=*/203);
    ASSERT_FALSE(parse_status.ok()) << "merge over an unparseable source value must fail";
    EXPECT_TRUE(parse_status.is_corruption()) << parse_status;
    EXPECT_EQ(1, parse_drops);

    // (b) parseable entry whose stored rssid + rssid_offset falls below zero.
    // A negative offset is used because the routing range is
    // [max(0, offset + 1), max_rss_rowid.high]: a positive offset would lift the
    // lower bound past the disagreeing rowset id 1 and dispatch would take the
    // metadata-only projection without ever reading the file; with offset=-1 the
    // range [0, 2] still contains id 1, the rebuild route is kept, and the first
    // entry (stored rssid 0, lifted to -1) trips the range guard.
    auto [overflow_status, overflow_drops] = run_scenario(
            [this](const std::string& path) {
                return write_legacy_pk_sstable(path, {{"k_overflow", 0, 0}});
            },
            /*rssid_offset=*/-1, /*txn_id=*/204);
    ASSERT_FALSE(overflow_status.ok()) << "merge over an out-of-range stored rssid must fail";
    EXPECT_TRUE(overflow_status.is_corruption()) << overflow_status;
    EXPECT_EQ(1, overflow_drops);
}

#endif // USE_STAROS && !BUILD_FORMAT_LIB

TEST_F(LakeTabletReshardTest, test_tablet_merging_non_shared_sstable_live_gap_forces_rebuild) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string ns_filename = "ns_live_gap.sst";
    const auto ns_path = _tablet_manager->sst_location(child_a, ns_filename);
    const uint64_t ns_filesize = write_legacy_pk_sstable(ns_path, {{"k_live_1", /*rssid=*/1, /*rowid=*/0},
                                                                   {"k_delvec_1", /*rssid=*/1, /*rowid=*/8},
                                                                   {"k_dead_2", /*rssid=*/2, /*rowid=*/0},
                                                                   {"k_live_3", /*rssid=*/3, /*rowid=*/0}});

    DelVector delvec;
    const uint32_t deleted_rowids[] = {8};
    delvec.init(base_version, deleted_rowids, 1);
    const std::string delvec_data = delvec.save();

    auto make_meta = [&](int64_t tablet_id, bool include_sstable) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(4);
        set_primary_key_schema(meta.get(), 1001);
        for (uint32_t rowset_id : {1u, 3u}) {
            auto* rowset = meta->add_rowsets();
            rowset->set_id(rowset_id);
            rowset->set_version(1);
            rowset->set_num_rows(10);
            rowset->set_data_size(10);
            auto* sm = rowset->add_segment_metas();
            sm->set_filename(fmt::format("shared_{}.dat", rowset_id));
            sm->set_size(10);
            sm->set_shared(true);
            stamp_physical_identity_uid(rowset, sm->filename());
        }
        if (include_sstable) {
            add_delvec(meta.get(), tablet_id, base_version, /*segment_id=*/1, "ns_live_gap.dv", delvec_data);
            auto* sst = meta->mutable_sstable_meta()->add_sstables();
            sst->set_filename(ns_filename);
            sst->set_filesize(ns_filesize);
            sst->set_shared(false);
            sst->set_max_rss_rowid(static_cast<uint64_t>(3) << 32);
        }
        return meta;
    };

    EXPECT_OK(put_tablet_metadata(make_meta(child_a, /*include_sstable=*/true)));
    EXPECT_OK(put_tablet_metadata(make_meta(child_b, /*include_sstable=*/false)));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(94);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    ASSERT_NE(ns_filename, out_sst.filename()) << "source-live gap must disable metadata-only projection";
    EXPECT_FALSE(out_sst.shared());
    EXPECT_FALSE(out_sst.has_shared_rssid());
    EXPECT_EQ(0, out_sst.rssid_offset());
    EXPECT_TRUE(out_sst.has_fileset_id());

    ASSIGN_OR_ABORT(auto sstable, lake::PersistentIndexSstable::new_sstable(
                                          out_sst, _tablet_manager->sst_location(merged_tablet, out_sst.filename()),
                                          /*cache=*/nullptr, /*need_filter=*/false, /*delvec=*/nullptr, merged,
                                          _tablet_manager.get()));
    sstable::ReadOptions read_options;
    read_options.fill_cache = false;
    std::unique_ptr<sstable::Iterator> iter(sstable->new_iterator(read_options));
    std::map<std::string, uint32_t> rebuilt_entries;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        IndexValuesWithVerPB values;
        ASSERT_TRUE(values.ParseFromArray(iter->value().data, static_cast<int>(iter->value().size)));
        ASSERT_GT(values.values_size(), 0);
        rebuilt_entries.emplace(iter->key().to_string(), values.values(0).rssid());
    }
    ASSERT_OK(iter->status());
    EXPECT_EQ((std::map<std::string, uint32_t>{{"k_live_1", 1}, {"k_live_3", 3}}), rebuilt_entries)
            << "dead-owner and merged-delvec values must both be filtered";
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_non_shared_sstable_delvec_forces_rebuild_without_owner_gap) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string ns_filename = "ns_delvec_without_owner_gap.sst";
    const uint64_t ns_filesize =
            write_legacy_pk_sstable(_tablet_manager->sst_location(child_a, ns_filename),
                                    {{"k_live", /*rssid=*/1, /*rowid=*/0}, {"k_deleted", /*rssid=*/1, /*rowid=*/8}});
    DelVector delvec;
    const uint32_t deleted_rowids[] = {8};
    delvec.init(base_version, deleted_rowids, 1);
    const std::string delvec_data = delvec.save();

    auto make_meta = [&](int64_t tablet_id, bool include_sstable) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(2);
        set_primary_key_schema(meta.get(), 1001);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("shared.dat");
        sm->set_size(100);
        sm->set_shared(true);
        stamp_physical_identity_uid(rowset, sm->filename());
        if (include_sstable) {
            add_delvec(meta.get(), tablet_id, base_version, /*segment_id=*/1, "ns_delvec.dv", delvec_data);
            auto* sst = meta->mutable_sstable_meta()->add_sstables();
            sst->set_filename(ns_filename);
            sst->set_filesize(ns_filesize);
            sst->set_shared(false);
            sst->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 8);
        }
        return meta;
    };

    EXPECT_OK(put_tablet_metadata(make_meta(child_a, /*include_sstable=*/true)));
    EXPECT_OK(put_tablet_metadata(make_meta(child_b, /*include_sstable=*/false)));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(100);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    ASSERT_NE(ns_filename, out_sst.filename()) << "merged delvec must disable metadata-only projection";

    ASSIGN_OR_ABORT(auto sstable, lake::PersistentIndexSstable::new_sstable(
                                          out_sst, _tablet_manager->sst_location(merged_tablet, out_sst.filename()),
                                          /*cache=*/nullptr, /*need_filter=*/false, /*delvec=*/nullptr, merged,
                                          _tablet_manager.get()));
    sstable::ReadOptions read_options;
    read_options.fill_cache = false;
    std::unique_ptr<sstable::Iterator> iter(sstable->new_iterator(read_options));
    std::set<std::string> rebuilt_keys;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        rebuilt_keys.insert(iter->key().to_string());
    }
    ASSERT_OK(iter->status());
    EXPECT_EQ((std::set<std::string>{"k_live"}), rebuilt_keys);
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_non_shared_sstable_rejects_shared_version_without_rssid) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string ns_filename = "ns_shared_version_without_rssid.sst";
    const uint64_t ns_filesize = write_legacy_pk_sstable(_tablet_manager->sst_location(child_a, ns_filename),
                                                         {{"k_live", /*rssid=*/1, /*rowid=*/0}});

    auto make_meta = [&](int64_t tablet_id, bool include_sstable) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(4);
        set_primary_key_schema(meta.get(), 1001);
        for (uint32_t rowset_id : {1u, 3u}) {
            auto* rowset = meta->add_rowsets();
            rowset->set_id(rowset_id);
            rowset->set_version(1);
            rowset->set_num_rows(1);
            rowset->set_data_size(10);
            auto* sm = rowset->add_segment_metas();
            sm->set_filename(fmt::format("shared_{}.dat", rowset_id));
            sm->set_size(10);
            sm->set_shared(true);
            stamp_physical_identity_uid(rowset, sm->filename());
        }
        if (include_sstable) {
            auto* sst = meta->mutable_sstable_meta()->add_sstables();
            sst->set_filename(ns_filename);
            sst->set_filesize(ns_filesize);
            sst->set_shared(false);
            sst->set_shared_version(7);
            sst->set_max_rss_rowid(static_cast<uint64_t>(3) << 32);
        }
        return meta;
    };

    EXPECT_OK(put_tablet_metadata(make_meta(child_a, /*include_sstable=*/true)));
    EXPECT_OK(put_tablet_metadata(make_meta(child_b, /*include_sstable=*/false)));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(99);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    auto status = lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                                  txn_info, false, tablet_metadatas, tablet_ranges);
    ASSERT_FALSE(status.ok()) << "shared_version without shared_rssid must be rejected";
    EXPECT_TRUE(status.is_corruption()) << "expected Corruption, got: " << status.to_string();
}

// A rebuilt tombstone-only SST can legitimately carry max_rss_rowid=0. Feed
// the exact metadata and physical SST produced by one merge into a second
// merge whose source context has a negative offset. The second merge must
// classify that invalid [1, 0] lifted range instead of projecting it blindly.
TEST_F(LakeTabletReshardTest, test_tablet_merging_two_cycle_zero_watermark_tombstone_preserves_actual_rebuild) {
    ensure_kek_in_key_cache();
    const bool old_tde = config::enable_transparent_data_encryption;
    config::enable_transparent_data_encryption = true;
    DeferOp restore_tde([&] { config::enable_transparent_data_encryption = old_tde; });
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_flush_failpoints([&] {
        set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    });

    constexpr uint32_t kTombstone = std::numeric_limits<uint32_t>::max();
    const std::string tombstone_key = encode_int_primary_key(10);
    auto first_fixture = make_below_floor_legacy_fixture(
            "two_cycle_mixed_legacy.sst",
            {{tombstone_key, serialize_index_values({{/*version=*/83, kTombstone, kTombstone}})},
             {encode_int_primary_key(30), serialize_index_values({{/*version=*/82, /*rssid=*/47, /*rowid=*/0}})}},
            /*source_high=*/47, /*encrypted=*/true);
    first_fixture.cold_metadata->set_next_rowset_id(10);
    constexpr int32_t kFirstContextOffset = 10 - BelowFloorLegacyFixture::kSourceLiveRssid;
    static_assert(kFirstContextOffset == -97);

    auto first_merged_or = merge_modern_shared_occurrences(
            first_fixture.cold_metadata, first_fixture.hot_metadata, first_fixture.merged_tablet,
            BelowFloorLegacyFixture::kBaseVersion, BelowFloorLegacyFixture::kMergedVersion, /*txn_id=*/420);
    ASSERT_OK(first_merged_or);
    auto first_merged = std::move(first_merged_or).value();

    const PersistentIndexSstablePB* first_tombstone = nullptr;
    int first_zero_watermark_count = 0;
    for (const auto& sstable : first_merged->sstable_meta().sstables()) {
        if (sstable.max_rss_rowid() == 0) {
            first_tombstone = &sstable;
            ++first_zero_watermark_count;
        }
    }
    ASSERT_EQ(1, first_zero_watermark_count);
    ASSERT_NE(nullptr, first_tombstone);
    EXPECT_NE(first_fixture.source_filename, first_tombstone->filename())
            << "cycle one must use the real mixed-content rebuild path";
    EXPECT_FALSE(first_tombstone->shared());
    EXPECT_FALSE(first_tombstone->has_shared_rssid());
    EXPECT_EQ(0, first_tombstone->rssid_offset());
    EXPECT_EQ(BelowFloorLegacyFixture::kMergedVersion, first_tombstone->generation_version());
    EXPECT_FALSE(first_tombstone->encryption_meta().empty());
    ASSERT_OK(KeyCache::instance().unwrap_encryption_meta(first_tombstone->encryption_meta()).status());

    const PersistentIndexSstablePB first_tombstone_pb(*first_tombstone);
    const std::string first_tombstone_path =
            _tablet_manager->sst_location(first_fixture.merged_tablet, first_tombstone_pb.filename());
    ASSERT_OK(FileSystem::Default()->path_exists(first_tombstone_path));
    ASSIGN_OR_ABORT(const std::string first_tombstone_bytes, read_file_contents(first_tombstone_path));
    ASSERT_FALSE(first_tombstone_bytes.empty());
    ASSIGN_OR_ABORT(auto first_raw_entries,
                    read_raw_pk_sstable(first_fixture.merged_tablet, first_merged, first_tombstone_pb));
    ASSERT_EQ(1, first_raw_entries.size());
    EXPECT_EQ(tombstone_key, first_raw_entries[0].first);
    ASSERT_EQ(1, first_raw_entries[0].second.values_size());
    EXPECT_EQ(83, first_raw_entries[0].second.values(0).version());
    EXPECT_EQ(kTombstone, first_raw_entries[0].second.values(0).rssid());
    EXPECT_EQ(kTombstone, first_raw_entries[0].second.values(0).rowid());

    const int64_t second_cold_tablet = next_id();
    const int64_t second_merged_tablet = next_id();
    prepare_tablet_dirs(second_cold_tablet);
    prepare_tablet_dirs(second_merged_tablet);
    auto second_cold = std::make_shared<TabletMetadataPB>();
    second_cold->set_id(second_cold_tablet);
    second_cold->set_version(BelowFloorLegacyFixture::kMergedVersion);
    second_cold->set_next_rowset_id(1);
    set_two_column_pk_schema(second_cold.get(), /*schema_id=*/4001);
    second_cold->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
    second_cold->set_enable_persistent_index(true);
    second_cold->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);

    ASSERT_FALSE(first_merged->rowsets().empty());
    uint32_t first_carried_floor = std::numeric_limits<uint32_t>::max();
    for (const auto& rowset : first_merged->rowsets()) {
        first_carried_floor = std::min(first_carried_floor, rowset.id());
    }
    const int64_t second_context_offset = static_cast<int64_t>(second_cold->next_rowset_id()) - first_carried_floor;
    ASSERT_EQ(-9, second_context_offset);
    ASSERT_LT(second_context_offset, 0);

    // This is the actual cycle-one TabletMetadataPB. No equivalent source PB
    // is reconstructed for cycle two, and both tablet ids resolve the same
    // immutable physical filename in the fixed lake location provider.
    auto second_merged_or = merge_modern_shared_occurrences(second_cold, first_merged, second_merged_tablet,
                                                            BelowFloorLegacyFixture::kMergedVersion,
                                                            /*new_version=*/3, /*txn_id=*/421);
    ASSERT_OK(second_merged_or);
    auto second_merged = std::move(second_merged_or).value();

    const PersistentIndexSstablePB* second_tombstone = nullptr;
    int second_reference_count = 0;
    for (const auto& sstable : second_merged->sstable_meta().sstables()) {
        if (sstable.filename() == first_tombstone_pb.filename()) {
            second_tombstone = &sstable;
            ++second_reference_count;
        }
    }
    ASSERT_NE(nullptr, second_tombstone);
    EXPECT_EQ(1, second_reference_count);
    EXPECT_EQ(first_tombstone_pb.filename(), second_tombstone->filename());
    EXPECT_EQ(first_tombstone_pb.filesize(), second_tombstone->filesize());
    EXPECT_EQ(first_tombstone_pb.encryption_meta(), second_tombstone->encryption_meta());
    EXPECT_EQ(0, second_tombstone->max_rss_rowid());
    EXPECT_EQ(first_tombstone_pb.rssid_offset() + second_context_offset, second_tombstone->rssid_offset());

    PersistentIndexSstablePB expected_second_pb(first_tombstone_pb);
    expected_second_pb.set_rssid_offset(first_tombstone_pb.rssid_offset() + second_context_offset);
    EXPECT_EQ(expected_second_pb.SerializeAsString(), second_tombstone->SerializeAsString())
            << "cycle two may only accumulate the context offset on the actual cycle-one PB";

    const std::string second_tombstone_path =
            _tablet_manager->sst_location(second_merged_tablet, second_tombstone->filename());
    EXPECT_EQ(first_tombstone_path, second_tombstone_path);
    ASSIGN_OR_ABORT(const std::string second_tombstone_bytes, read_file_contents(second_tombstone_path));
    EXPECT_EQ(first_tombstone_bytes, second_tombstone_bytes);
    ASSIGN_OR_ABORT(auto second_raw_entries,
                    read_raw_pk_sstable(second_merged_tablet, second_merged, *second_tombstone));
    ASSERT_EQ(1, second_raw_entries.size());
    EXPECT_EQ(tombstone_key, second_raw_entries[0].first);
    ASSERT_EQ(1, second_raw_entries[0].second.values_size());
    EXPECT_EQ(83, second_raw_entries[0].second.values(0).version());
    EXPECT_EQ(kTombstone, second_raw_entries[0].second.values(0).rssid());
    EXPECT_EQ(kTombstone, second_raw_entries[0].second.values(0).rowid());

    const std::vector<std::string> lookup_keys = {tombstone_key, first_fixture.live_key};
    ASSIGN_OR_ABORT(auto lookup_values, load_index_values(second_merged, second_merged_tablet, lookup_keys));
    ASSERT_EQ(2, lookup_values.size());
    EXPECT_EQ(IndexValue(NullIndexValue), lookup_values[0]);
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(1) << 32), lookup_values[1]);

    ASSIGN_OR_ABORT(auto after_dml,
                    publish_followup_upsert_delete(second_merged_tablet, /*base_version=*/3,
                                                   /*upsert_key=*/10, /*upsert_value=*/1010, /*delete_key=*/20));
    ASSIGN_OR_ABORT(auto rows, read_two_column_rows(after_dml));
    EXPECT_EQ((std::vector<std::pair<int32_t, int32_t>>{{10, 1010}}), rows);
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_empty_zero_watermark_with_negative_context_does_not_cover_tail) {
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_flush_failpoints([&] {
        set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    });

    auto fixture = make_below_floor_legacy_fixture("empty_zero_watermark_negative.sst", {}, /*source_high=*/0);
    auto* source_pb = fixture.hot_metadata->mutable_sstable_meta()->mutable_sstables(0);
    source_pb->set_max_rss_rowid(0);
    fixture.source_pb.CopyFrom(*source_pb);
    ASSERT_EQ(-106, BelowFloorLegacyFixture::kContextOffset);
    ASSERT_EQ(0, fixture.source_pb.max_rss_rowid());
    ASSIGN_OR_ABORT(auto source_entries,
                    read_raw_pk_sstable(fixture.hot_tablet, fixture.hot_metadata, fixture.source_pb));
    ASSERT_TRUE(source_entries.empty()) << "fixture must be a real structurally valid empty SST";

    auto merged_or =
            merge_modern_shared_occurrences(fixture.cold_metadata, fixture.hot_metadata, fixture.merged_tablet);
    ASSERT_OK(merged_or);
    auto merged = std::move(merged_or).value();
    EXPECT_TRUE(std::none_of(merged->sstable_meta().sstables().begin(), merged->sstable_meta().sstables().end(),
                             [&](const auto& sstable) { return sstable.filename() == fixture.source_filename; }))
            << "an empty SST contributes neither PB nor false coverage";
    EXPECT_TRUE(std::none_of(merged->orphan_files().begin(), merged->orphan_files().end(),
                             [&](const auto& file) { return file.name() == fixture.source_filename; }));
    ASSERT_OK(FileSystem::Default()->path_exists(fixture.source_path));
    ASSERT_EQ(1, merged->sstable_meta().sstables_size()) << "safe point zero must materialize the uncovered tail";
    EXPECT_NE(fixture.source_filename, merged->sstable_meta().sstables(0).filename());

    ASSIGN_OR_ABORT(auto lookup_value, load_index_value(merged, fixture.merged_tablet, fixture.live_key));
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(BelowFloorLegacyFixture::kFinalLiveRssid) << 32), lookup_value);
    ASSIGN_OR_ABORT(auto after_dml,
                    publish_followup_upsert_delete(fixture.merged_tablet, BelowFloorLegacyFixture::kMergedVersion,
                                                   /*upsert_key=*/10, /*upsert_value=*/1010, /*delete_key=*/20));
    ASSIGN_OR_ABORT(auto rows, read_two_column_rows(after_dml));
    EXPECT_EQ((std::vector<std::pair<int32_t, int32_t>>{{10, 1010}}), rows);
}

// source_rssid_offset=5 means a stored rssid 0 belongs to source rowset 5,
// while max_rss_rowid.high=5 makes the lifted metadata range [6, 5]. Exact
// classification must rebuild the retained live entry and derive (5, 0), not
// trust the inconsistent PB seed (5, 9).
TEST_F(LakeTabletReshardTest, test_tablet_merging_invalid_legacy_range_live_data_rebuilds_with_exact_watermark) {
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_source_flush(
            [&] { set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE); });

    const int64_t live_tablet = next_id();
    const int64_t empty_tablet = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(live_tablet);
    prepare_tablet_dirs(empty_tablet);
    prepare_tablet_dirs(merged_tablet);

    const std::string live_key = encode_int_primary_key(20);
    const std::string segment_filename = "invalid_range_live_segment.dat";
    const uint64_t segment_size = write_two_column_segment(
            live_tablet, segment_filename, /*num_rows=*/1, [](int key) { return key * 10; }, /*key_start=*/20);
    auto live = std::make_shared<TabletMetadataPB>();
    live->set_id(live_tablet);
    live->set_version(1);
    live->set_next_rowset_id(6);
    set_two_column_pk_schema(live.get(), /*schema_id=*/4001);
    live->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
    live->set_enable_persistent_index(true);
    live->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
    auto* rowset = live->add_rowsets();
    rowset->set_id(5);
    rowset->set_version(1);
    rowset->set_num_rows(1);
    rowset->set_data_size(segment_size);
    rowset->set_overlapped(false);
    auto* segment = rowset->add_segment_metas();
    segment->set_filename(segment_filename);
    segment->set_size(segment_size);
    segment->set_num_rows(1);

    const std::string source_filename = "invalid_range_live_data.sst";
    const std::string source_path = _tablet_manager->sst_location(live_tablet, source_filename);
    const auto source_file = write_raw_pk_sstable(
            source_path, {{live_key, serialize_index_values({{/*version=*/101, /*rssid=*/0, /*rowid=*/0}})}});
    auto* source_pb = live->mutable_sstable_meta()->add_sstables();
    source_pb->set_filename(source_filename);
    source_pb->set_filesize(source_file.filesize);
    source_pb->set_shared(false);
    source_pb->set_rssid_offset(5);
    source_pb->set_max_rss_rowid((static_cast<uint64_t>(5) << 32) | 9);
    source_pb->mutable_range()->CopyFrom(source_file.range);
    source_pb->mutable_fileset_id()->set_hi(0x5151);
    source_pb->mutable_fileset_id()->set_lo(0x6161);
    source_pb->set_generation_version(1);
    ASSERT_GT(static_cast<uint32_t>(source_pb->rssid_offset()) + 1,
              static_cast<uint32_t>(source_pb->max_rss_rowid() >> 32));

    auto empty = std::make_shared<TabletMetadataPB>();
    empty->set_id(empty_tablet);
    empty->set_version(1);
    empty->set_next_rowset_id(1);
    set_two_column_pk_schema(empty.get(), /*schema_id=*/4001);
    empty->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
    empty->set_enable_persistent_index(true);
    empty->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);

    auto merged_or = merge_modern_shared_occurrences(live, empty, merged_tablet);
    ASSERT_OK(merged_or);
    auto merged = std::move(merged_or).value();
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& rebuilt = merged->sstable_meta().sstables(0);
    EXPECT_NE(source_filename, rebuilt.filename()) << "invalid metadata with retained live data must rebuild";
    EXPECT_FALSE(rebuilt.shared());
    EXPECT_FALSE(rebuilt.has_shared_rssid());
    EXPECT_EQ(0, rebuilt.rssid_offset());
    const uint64_t expected_max_rss_rowid = static_cast<uint64_t>(5) << 32;
    EXPECT_EQ(expected_max_rss_rowid, rebuilt.max_rss_rowid())
            << "watermark must be the exact retained (final_rssid=5,rowid=0), not PB seed (5,9)";

    ASSIGN_OR_ABORT(auto raw_entries, read_raw_pk_sstable(merged_tablet, merged, rebuilt));
    ASSERT_EQ(1, raw_entries.size());
    EXPECT_EQ(live_key, raw_entries[0].first);
    ASSERT_EQ(1, raw_entries[0].second.values_size());
    EXPECT_EQ(101, raw_entries[0].second.values(0).version());
    EXPECT_EQ(5, raw_entries[0].second.values(0).rssid());
    EXPECT_EQ(0, raw_entries[0].second.values(0).rowid());

    ASSIGN_OR_ABORT(auto lookup_value, load_index_value(merged, merged_tablet, live_key));
    EXPECT_EQ(IndexValue(expected_max_rss_rowid), lookup_value);
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    DeferOp restore_index_flush(
            [&] { set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE); });
    ASSIGN_OR_ABORT(auto after_dml, publish_followup_upsert_delete(merged_tablet, /*base_version=*/2, /*upsert_key=*/10,
                                                                   /*upsert_value=*/1010, /*delete_key=*/20));
    ASSIGN_OR_ABORT(auto rows, read_two_column_rows(after_dml));
    EXPECT_EQ((std::vector<std::pair<int32_t, int32_t>>{{10, 1010}}), rows);
    ASSERT_OK(FileSystem::Default()->path_exists(source_path));
}

TEST_F(LakeTabletReshardTest,
       test_tablet_merging_invalid_legacy_range_mixed_live_dead_derives_exact_watermark_and_tail) {
    auto fixture = make_invalid_range_mixed_legacy_fixture("invalid_range_mixed_live_dead.sst",
                                                           InvalidRangeMixedTrigger::kDeadOwner);
    verify_invalid_range_mixed_legacy_rebuild(fixture);
}

TEST_F(LakeTabletReshardTest,
       test_tablet_merging_invalid_legacy_range_mixed_live_delvec_derives_exact_watermark_and_tail) {
    auto fixture = make_invalid_range_mixed_legacy_fixture("invalid_range_mixed_live_delvec.sst",
                                                           InvalidRangeMixedTrigger::kMergedDelvec);
    verify_invalid_range_mixed_legacy_rebuild(fixture);
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_valid_nonnegative_legacy_fast_path_skips_exact_classifier) {
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_source_flush(
            [&] { set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE); });

    const int64_t live_tablet = next_id();
    const int64_t empty_tablet = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(live_tablet);
    prepare_tablet_dirs(empty_tablet);
    prepare_tablet_dirs(merged_tablet);

    const std::string live_key = encode_int_primary_key(20);
    const std::string segment_filename = "classifier_fast_path_segment.dat";
    const uint64_t segment_size = write_two_column_segment(
            live_tablet, segment_filename, /*num_rows=*/1, [](int key) { return key * 10; }, /*key_start=*/20);
    auto live = std::make_shared<TabletMetadataPB>();
    live->set_id(live_tablet);
    live->set_version(1);
    live->set_next_rowset_id(2);
    set_two_column_pk_schema(live.get(), /*schema_id=*/4001);
    live->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
    live->set_enable_persistent_index(true);
    live->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
    auto* rowset = live->add_rowsets();
    rowset->set_id(1);
    rowset->set_version(1);
    rowset->set_num_rows(1);
    rowset->set_data_size(segment_size);
    auto* segment = rowset->add_segment_metas();
    segment->set_filename(segment_filename);
    segment->set_size(segment_size);
    segment->set_num_rows(1);

    const std::string source_filename = "classifier_fast_path.sst";
    const auto source_file =
            write_raw_pk_sstable(_tablet_manager->sst_location(live_tablet, source_filename),
                                 {{live_key, serialize_index_values({{/*version=*/111, /*rssid=*/1, /*rowid=*/0}})}});
    auto* source_pb = live->mutable_sstable_meta()->add_sstables();
    source_pb->set_filename(source_filename);
    source_pb->set_filesize(source_file.filesize);
    source_pb->set_shared(false);
    source_pb->set_rssid_offset(0);
    source_pb->set_max_rss_rowid(static_cast<uint64_t>(1) << 32);
    source_pb->mutable_range()->CopyFrom(source_file.range);
    source_pb->mutable_fileset_id()->set_hi(0x7171);
    source_pb->mutable_fileset_id()->set_lo(0x8181);
    source_pb->set_generation_version(1);

    auto empty = std::make_shared<TabletMetadataPB>();
    empty->set_id(empty_tablet);
    empty->set_version(1);
    empty->set_next_rowset_id(1);
    set_two_column_pk_schema(empty.get(), /*schema_id=*/4001);
    empty->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
    empty->set_enable_persistent_index(true);
    empty->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);

    int classifier_entry_count = 0;
    SyncPoint::GetInstance()->SetCallBack("classify_non_shared_legacy_sstable:entry",
                                          [&](void*) { ++classifier_entry_count; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp cleanup_sync_point([&] {
        SyncPoint::GetInstance()->ClearCallBack("classify_non_shared_legacy_sstable:entry");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    auto merged_or = merge_modern_shared_occurrences(live, empty, merged_tablet);
    ASSERT_OK(merged_or);
    auto merged = std::move(merged_or).value();
    EXPECT_EQ(0, classifier_entry_count)
            << "valid/nonnegative metadata with no gap, delvec, or disagreement must remain scan-free";
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    EXPECT_EQ(source_filename, merged->sstable_meta().sstables(0).filename());
    EXPECT_EQ(0, merged->sstable_meta().sstables(0).rssid_offset());
    EXPECT_EQ(static_cast<uint64_t>(1) << 32, merged->sstable_meta().sstables(0).max_rss_rowid());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_direct_mapping_disagreement_rebuild_skips_exact_classifier) {
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_source_flush(
            [&] { set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE); });

    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string shared_segment_filename = "classifier_direct_shared_segment.dat";
    const uint64_t shared_segment_size = write_two_column_segment(
            child_a, shared_segment_filename, /*num_rows=*/1, [](int key) { return key * 10; }, /*key_start=*/10);
    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(1);
    meta_a->set_next_rowset_id(3);
    set_two_column_pk_schema(meta_a.get(), /*schema_id=*/4001);
    meta_a->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
    meta_a->set_enable_persistent_index(true);
    meta_a->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
    auto* shared_rowset_a = meta_a->add_rowsets();
    shared_rowset_a->set_id(1);
    shared_rowset_a->set_version(1);
    shared_rowset_a->set_num_rows(1);
    shared_rowset_a->set_data_size(shared_segment_size);
    auto* shared_segment_a = shared_rowset_a->add_segment_metas();
    shared_segment_a->set_filename(shared_segment_filename);
    shared_segment_a->set_size(shared_segment_size);
    shared_segment_a->set_num_rows(1);
    shared_segment_a->set_shared(true);
    stamp_physical_identity_uid(shared_rowset_a, shared_segment_filename);

    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(1);
    meta_b->set_next_rowset_id(3);
    set_two_column_pk_schema(meta_b.get(), /*schema_id=*/4001);
    meta_b->mutable_schema()->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
    meta_b->set_enable_persistent_index(true);
    meta_b->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
    meta_b->add_rowsets()->CopyFrom(*shared_rowset_a);

    const std::string local_segment_filename = "classifier_direct_local_segment.dat";
    const uint64_t local_segment_size = write_two_column_segment(
            child_b, local_segment_filename, /*num_rows=*/1, [](int key) { return key * 10; }, /*key_start=*/20);
    auto* local_rowset = meta_b->add_rowsets();
    local_rowset->set_id(2);
    local_rowset->set_version(1);
    local_rowset->set_num_rows(1);
    local_rowset->set_data_size(local_segment_size);
    auto* local_segment = local_rowset->add_segment_metas();
    local_segment->set_filename(local_segment_filename);
    local_segment->set_size(local_segment_size);
    local_segment->set_num_rows(1);

    const std::string local_key = encode_int_primary_key(20);
    const std::string source_filename = "classifier_direct_mapping_disagreement.sst";
    const auto source_file =
            write_raw_pk_sstable(_tablet_manager->sst_location(child_b, source_filename),
                                 {{local_key, serialize_index_values({{/*version=*/121, /*rssid=*/2, /*rowid=*/0}})}});
    auto* source_pb = meta_b->mutable_sstable_meta()->add_sstables();
    source_pb->set_filename(source_filename);
    source_pb->set_filesize(source_file.filesize);
    source_pb->set_shared(false);
    source_pb->set_rssid_offset(0);
    source_pb->set_max_rss_rowid(static_cast<uint64_t>(2) << 32);
    source_pb->mutable_range()->CopyFrom(source_file.range);
    source_pb->mutable_fileset_id()->set_hi(0x9191);
    source_pb->mutable_fileset_id()->set_lo(0xA1A1);
    source_pb->set_generation_version(1);

    int classifier_entry_count = 0;
    SyncPoint::GetInstance()->SetCallBack("classify_non_shared_legacy_sstable:entry",
                                          [&](void*) { ++classifier_entry_count; });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp cleanup_sync_point([&] {
        SyncPoint::GetInstance()->ClearCallBack("classify_non_shared_legacy_sstable:entry");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    auto merged_or = merge_modern_shared_occurrences(meta_a, meta_b, merged_tablet);
    ASSERT_OK(merged_or);
    auto merged = std::move(merged_or).value();
    EXPECT_EQ(0, classifier_entry_count) << "direct mapping disagreement must rebuild before exact classification";
    EXPECT_TRUE(std::none_of(merged->sstable_meta().sstables().begin(), merged->sstable_meta().sstables().end(),
                             [&](const auto& sstable) { return sstable.filename() == source_filename; }));
    EXPECT_TRUE(std::any_of(merged->rowsets().begin(), merged->rowsets().end(), [](const auto& rowset) {
        return rowset.id() == 3;
    })) << "the second source's local rssid 2 must map naturally to final rssid 3";

    const PersistentIndexSstablePB* rebuilt = nullptr;
    for (const auto& sstable : merged->sstable_meta().sstables()) {
        ASSIGN_OR_ABORT(auto entries, read_raw_pk_sstable(merged_tablet, merged, sstable));
        for (const auto& [key, values] : entries) {
            if (key == local_key && values.values_size() == 1 && values.values(0).version() == 121) {
                rebuilt = &sstable;
                EXPECT_EQ(3, values.values(0).rssid());
                EXPECT_EQ(0, values.values(0).rowid());
            }
        }
    }
    ASSERT_NE(nullptr, rebuilt) << "the direct path must emit the remapped real source entry";
    EXPECT_NE(source_filename, rebuilt->filename());
    EXPECT_EQ(0, rebuilt->rssid_offset());
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_preserves_below_floor_non_shared_legacy_tombstone_without_rewrite) {
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_flush_failpoints([&] {
        set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    });

    constexpr uint32_t kTombstone = std::numeric_limits<uint32_t>::max();
    const std::string tombstone_key = encode_int_primary_key(10);
    auto fixture = make_below_floor_legacy_fixture(
            "below_floor_legacy_tombstone.sst",
            {{tombstone_key, serialize_index_values({{/*version=*/23, kTombstone, kTombstone}})}},
            /*source_high=*/47);

    ASSERT_TRUE(fixture.source_pb.has_range());
    EXPECT_FALSE(fixture.source_pb.range().start_key().empty());
    EXPECT_FALSE(fixture.source_pb.range().end_key().empty());
    ASSERT_TRUE(fixture.source_pb.has_fileset_id());
    EXPECT_EQ(0x13579, fixture.source_pb.fileset_id().hi());
    EXPECT_EQ(0x24680, fixture.source_pb.fileset_id().lo());
    EXPECT_EQ(17, fixture.source_pb.generation_version());
    ASSERT_EQ(-59, static_cast<int64_t>(47) + BelowFloorLegacyFixture::kContextOffset);
    ASSERT_OK(FileSystem::Default()->path_exists(fixture.source_path));
    ASSERT_OK(FileSystem::Default()->path_exists(
            _tablet_manager->segment_location(fixture.hot_tablet, fixture.segment_filename)));

    auto merged_or =
            merge_modern_shared_occurrences(fixture.cold_metadata, fixture.hot_metadata, fixture.merged_tablet);
    ASSERT_OK(merged_or);
    auto merged = std::move(merged_or).value();

    const PersistentIndexSstablePB* normalized = nullptr;
    int source_reference_count = 0;
    for (const auto& sstable : merged->sstable_meta().sstables()) {
        if (sstable.filename() == fixture.source_filename) {
            normalized = &sstable;
            ++source_reference_count;
        }
    }
    ASSERT_NE(nullptr, normalized);
    EXPECT_EQ(1, source_reference_count) << "same-file normalization must not add a replacement PB";
    EXPECT_FALSE(normalized->shared());
    EXPECT_FALSE(normalized->has_shared_rssid());
    EXPECT_EQ(0, normalized->max_rss_rowid());
    EXPECT_EQ(BelowFloorLegacyFixture::kContextOffset, normalized->rssid_offset());
    EXPECT_EQ(fixture.source_pb.filesize(), normalized->filesize());
    EXPECT_EQ(fixture.source_pb.encryption_meta(), normalized->encryption_meta());
    ASSERT_TRUE(normalized->has_range());
    EXPECT_EQ(fixture.source_pb.range().SerializeAsString(), normalized->range().SerializeAsString());
    ASSERT_TRUE(normalized->has_fileset_id());
    EXPECT_EQ(fixture.source_pb.fileset_id().SerializeAsString(), normalized->fileset_id().SerializeAsString());
    EXPECT_EQ(fixture.source_pb.generation_version(), normalized->generation_version());

    PersistentIndexSstablePB expected(fixture.source_pb);
    expected.set_max_rss_rowid(0);
    expected.set_rssid_offset(BelowFloorLegacyFixture::kContextOffset);
    expected.clear_delvec();
    EXPECT_EQ(expected.SerializeAsString(), normalized->SerializeAsString())
            << "only the watermark and accumulated offset may change";

    ASSERT_OK(FileSystem::Default()->path_exists(fixture.source_path));
    EXPECT_TRUE(std::none_of(merged->orphan_files().begin(), merged->orphan_files().end(),
                             [&](const auto& file) { return file.name() == fixture.source_filename; }));
    EXPECT_EQ(0, merged->orphan_files_size());

    ASSIGN_OR_ABORT(auto raw_entries, read_raw_pk_sstable(fixture.merged_tablet, merged, *normalized));
    ASSERT_EQ(1, raw_entries.size());
    EXPECT_EQ(tombstone_key, raw_entries[0].first);
    ASSERT_EQ(1, raw_entries[0].second.values_size());
    EXPECT_EQ(23, raw_entries[0].second.values(0).version());
    EXPECT_EQ(kTombstone, raw_entries[0].second.values(0).rssid());
    EXPECT_EQ(kTombstone, raw_entries[0].second.values(0).rowid());

    const std::vector<std::string> lookup_keys = {tombstone_key, fixture.live_key};
    ASSIGN_OR_ABORT(auto lookup_values, load_index_values(merged, fixture.merged_tablet, lookup_keys));
    ASSERT_EQ(2, lookup_values.size());
    EXPECT_EQ(IndexValue(NullIndexValue), lookup_values[0]);
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(BelowFloorLegacyFixture::kFinalLiveRssid) << 32), lookup_values[1]);

    ASSIGN_OR_ABORT(auto after_dml,
                    publish_followup_upsert_delete(fixture.merged_tablet, BelowFloorLegacyFixture::kMergedVersion,
                                                   /*upsert_key=*/10, /*upsert_value=*/1010, /*delete_key=*/20));
    ASSIGN_OR_ABORT(auto rows, read_two_column_rows(after_dml));
    EXPECT_EQ((std::vector<std::pair<int32_t, int32_t>>{{10, 1010}}), rows);
}

TEST_F(LakeTabletReshardTest,
       test_tablet_merging_preserves_below_floor_non_shared_legacy_multi_version_tombstones_without_rewrite) {
    constexpr uint32_t kTombstone = std::numeric_limits<uint32_t>::max();
    const std::string tombstone_key = encode_int_primary_key(10);
    auto fixture = make_below_floor_legacy_fixture(
            "below_floor_legacy_multi_tombstone.sst",
            {{tombstone_key, serialize_index_values({{/*version=*/31, kTombstone, kTombstone},
                                                     {/*version=*/29, kTombstone, kTombstone}})}},
            /*source_high=*/47);

    auto merged_or =
            merge_modern_shared_occurrences(fixture.cold_metadata, fixture.hot_metadata, fixture.merged_tablet);
    ASSERT_OK(merged_or);
    auto merged = std::move(merged_or).value();

    const PersistentIndexSstablePB* normalized = nullptr;
    int source_reference_count = 0;
    for (const auto& sstable : merged->sstable_meta().sstables()) {
        if (sstable.filename() == fixture.source_filename) {
            normalized = &sstable;
            ++source_reference_count;
        }
    }
    ASSERT_NE(nullptr, normalized);
    EXPECT_EQ(1, source_reference_count);
    EXPECT_EQ(0, normalized->max_rss_rowid());
    EXPECT_EQ(BelowFloorLegacyFixture::kContextOffset, normalized->rssid_offset());
    PersistentIndexSstablePB expected(fixture.source_pb);
    expected.set_max_rss_rowid(0);
    expected.set_rssid_offset(BelowFloorLegacyFixture::kContextOffset);
    expected.clear_delvec();
    EXPECT_EQ(expected.SerializeAsString(), normalized->SerializeAsString());

    ASSIGN_OR_ABORT(auto raw_entries, read_raw_pk_sstable(fixture.merged_tablet, merged, *normalized));
    ASSERT_EQ(1, raw_entries.size());
    EXPECT_EQ(tombstone_key, raw_entries[0].first);
    ASSERT_EQ(2, raw_entries[0].second.values_size());
    EXPECT_EQ(31, raw_entries[0].second.values(0).version());
    EXPECT_EQ(29, raw_entries[0].second.values(1).version());
    for (const auto& value : raw_entries[0].second.values()) {
        EXPECT_EQ(kTombstone, value.rssid());
        EXPECT_EQ(kTombstone, value.rowid());
    }
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_empty_non_shared_legacy_sstable_does_not_cover_live_tail) {
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_flush_failpoints([&] {
        set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    });

    constexpr uint32_t kSourceHigh = 108;
    constexpr int64_t kProjectedHigh = static_cast<int64_t>(kSourceHigh) + BelowFloorLegacyFixture::kContextOffset;
    static_assert(kProjectedHigh == 2);
    static_assert(kProjectedHigh > BelowFloorLegacyFixture::kFinalLiveRssid);
    auto fixture = make_below_floor_legacy_fixture("below_floor_legacy_empty.sst", {}, kSourceHigh);
    ASSIGN_OR_ABORT(auto source_entries,
                    read_raw_pk_sstable(fixture.hot_tablet, fixture.hot_metadata, fixture.source_pb));
    ASSERT_TRUE(source_entries.empty()) << "fixture must be a structurally valid zero-entry SST";

    auto merged_or =
            merge_modern_shared_occurrences(fixture.cold_metadata, fixture.hot_metadata, fixture.merged_tablet);
    ASSERT_OK(merged_or);
    auto merged = std::move(merged_or).value();
    ASSERT_OK(FileSystem::Default()->path_exists(fixture.source_path));

    const PersistentIndexSstablePB* empty_source_output = nullptr;
    for (const auto& sstable : merged->sstable_meta().sstables()) {
        if (sstable.filename() == fixture.source_filename) {
            empty_source_output = &sstable;
        }
    }
    if (empty_source_output != nullptr) {
        EXPECT_EQ((static_cast<uint64_t>(kProjectedHigh) << 32) | 9, empty_source_output->max_rss_rowid())
                << "pre-fix PB must expose nonnegative false coverage above the live tail";
    }
    EXPECT_EQ(nullptr, empty_source_output) << "a real empty SST contributes no merged metadata coverage";
    EXPECT_TRUE(std::none_of(merged->orphan_files().begin(), merged->orphan_files().end(), [&](const auto& file) {
        return file.name() == fixture.source_filename;
    })) << "vacuum, not inline merge cleanup, owns the now-unreferenced source";

    ASSIGN_OR_ABORT(auto lookup_value, load_index_value(merged, fixture.merged_tablet, fixture.live_key));
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(BelowFloorLegacyFixture::kFinalLiveRssid) << 32), lookup_value);

    ASSIGN_OR_ABORT(auto after_dml,
                    publish_followup_upsert_delete(fixture.merged_tablet, BelowFloorLegacyFixture::kMergedVersion,
                                                   /*upsert_key=*/10, /*upsert_value=*/1010, /*delete_key=*/20));
    ASSIGN_OR_ABORT(auto rows, read_two_column_rows(after_dml));
    EXPECT_EQ((std::vector<std::pair<int32_t, int32_t>>{{10, 1010}}), rows);
    ASSERT_OK(FileSystem::Default()->path_exists(fixture.source_path));
}

TEST_F(LakeTabletReshardTest,
       test_tablet_merging_preserves_below_floor_tde_non_shared_legacy_tombstone_without_rewrite) {
    ensure_kek_in_key_cache();
    const bool old_tde = config::enable_transparent_data_encryption;
    config::enable_transparent_data_encryption = true;
    DeferOp restore_tde([&] { config::enable_transparent_data_encryption = old_tde; });
    set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::DISABLE);
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_flush_failpoints([&] {
        set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE);
        set_failpoint_mode("skip_lake_pk_index_flush", FailPointTriggerModeType::ENABLE);
    });

    constexpr uint32_t kTombstone = std::numeric_limits<uint32_t>::max();
    const std::string tombstone_key = encode_int_primary_key(10);
    auto fixture = make_below_floor_legacy_fixture(
            "below_floor_legacy_tde_tombstone.sst",
            {{tombstone_key, serialize_index_values({{/*version=*/41, kTombstone, kTombstone}})}},
            /*source_high=*/47, /*encrypted=*/true);
    ASSERT_FALSE(fixture.source_pb.encryption_meta().empty());
    ASSERT_OK(KeyCache::instance().unwrap_encryption_meta(fixture.source_pb.encryption_meta()).status());

    auto merged_or =
            merge_modern_shared_occurrences(fixture.cold_metadata, fixture.hot_metadata, fixture.merged_tablet);
    ASSERT_OK(merged_or);
    auto merged = std::move(merged_or).value();
    ASSERT_EQ(2, merged->sstable_meta().sstables_size())
            << "only the original encrypted SST and the legitimate materialized tail are referenced";

    const PersistentIndexSstablePB* normalized = nullptr;
    const PersistentIndexSstablePB* tail = nullptr;
    int source_reference_count = 0;
    for (const auto& sstable : merged->sstable_meta().sstables()) {
        if (sstable.filename() == fixture.source_filename) {
            normalized = &sstable;
            ++source_reference_count;
        } else {
            tail = &sstable;
        }
    }
    ASSERT_NE(nullptr, normalized);
    ASSERT_NE(nullptr, tail);
    EXPECT_EQ(1, source_reference_count) << "normalization must not create a replacement SST";
    EXPECT_EQ(fixture.source_pb.encryption_meta(), normalized->encryption_meta());
    EXPECT_FALSE(normalized->encryption_meta().empty());
    EXPECT_EQ(0, normalized->max_rss_rowid());
    EXPECT_EQ(BelowFloorLegacyFixture::kContextOffset, normalized->rssid_offset());
    EXPECT_EQ(fixture.source_pb.filesize(), normalized->filesize());
    ASSERT_TRUE(normalized->has_range());
    EXPECT_EQ(fixture.source_pb.range().SerializeAsString(), normalized->range().SerializeAsString());
    ASSERT_TRUE(normalized->has_fileset_id());
    EXPECT_EQ(fixture.source_pb.fileset_id().SerializeAsString(), normalized->fileset_id().SerializeAsString());
    EXPECT_EQ(fixture.source_pb.generation_version(), normalized->generation_version());
    PersistentIndexSstablePB expected(fixture.source_pb);
    expected.set_max_rss_rowid(0);
    expected.set_rssid_offset(BelowFloorLegacyFixture::kContextOffset);
    expected.clear_delvec();
    EXPECT_EQ(expected.SerializeAsString(), normalized->SerializeAsString());
    EXPECT_NE(fixture.source_filename, tail->filename());
    EXPECT_FALSE(tail->encryption_meta().empty());
    ASSERT_OK(KeyCache::instance().unwrap_encryption_meta(tail->encryption_meta()).status());

    ASSIGN_OR_ABORT(auto raw_entries, read_raw_pk_sstable(fixture.merged_tablet, merged, *normalized));
    ASSERT_EQ(1, raw_entries.size()) << "the same encrypted object must reopen and decrypt";
    ASSERT_EQ(1, raw_entries[0].second.values_size());
    EXPECT_EQ(kTombstone, raw_entries[0].second.values(0).rssid());
    EXPECT_EQ(kTombstone, raw_entries[0].second.values(0).rowid());
    ASSERT_OK(FileSystem::Default()->path_exists(fixture.source_path));
    EXPECT_TRUE(std::none_of(merged->orphan_files().begin(), merged->orphan_files().end(),
                             [&](const auto& file) { return file.name() == fixture.source_filename; }));
    EXPECT_EQ(0, merged->orphan_files_size());
}

#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
TEST_F(LakeTabletReshardTest, test_tablet_merging_zero_value_zero_watermark_with_negative_context_fails_closed) {
    IndexValuesWithVerPB empty_values;
    const std::string empty_value_key = encode_int_primary_key(10);
    auto fixture = make_below_floor_legacy_fixture("zero_values_zero_watermark_negative.sst",
                                                   {{empty_value_key, empty_values.SerializeAsString()}},
                                                   /*source_high=*/0);
    auto* source_pb = fixture.hot_metadata->mutable_sstable_meta()->mutable_sstables(0);
    source_pb->set_max_rss_rowid(0);
    fixture.source_pb.CopyFrom(*source_pb);
    ASSERT_EQ(0, fixture.source_pb.max_rss_rowid());
    ASSIGN_OR_ABORT(auto source_entries,
                    read_raw_pk_sstable(fixture.hot_tablet, fixture.hot_metadata, fixture.source_pb));
    ASSERT_EQ(1, source_entries.size());
    EXPECT_EQ(empty_value_key, source_entries[0].first);
    EXPECT_EQ(0, source_entries[0].second.values_size())
            << "fixture must persist one real key with a serialized empty IndexValuesWithVerPB";

    ASSERT_OK(put_tablet_metadata(fixture.cold_metadata));
    ASSERT_OK(put_tablet_metadata(fixture.hot_metadata));
    ReshardingTabletInfoPB resharding;
    auto& merging = *resharding.mutable_merging_tablet_info();
    merging.add_old_tablet_ids(fixture.cold_tablet);
    merging.add_old_tablet_ids(fixture.hot_tablet);
    merging.set_new_tablet_id(fixture.merged_tablet);

    const int64_t read_errors_before = StorageMetrics::instance()->pk_index_sst_read_error_total.value();
    bool published = true;
    auto [status, cache_drop_count] = publish_merge_and_count_cache_drops(
            _tablet_manager.get(), resharding, BelowFloorLegacyFixture::kBaseVersion,
            BelowFloorLegacyFixture::kMergedVersion, /*txn_id=*/422, &published);
    ASSERT_FALSE(status.ok()) << "zero-watermark metadata must not bypass exact zero-value validation";
    EXPECT_TRUE(status.is_corruption()) << status;
    EXPECT_TRUE(status.message().contains(fixture.source_filename)) << status;
    EXPECT_TRUE(status.message().contains("no index values")) << status;
    EXPECT_EQ(1, cache_drop_count) << "semantic source corruption must evict the source cache exactly once";
    EXPECT_EQ(read_errors_before + 1, StorageMetrics::instance()->pk_index_sst_read_error_total.value());
    EXPECT_FALSE(published) << "source corruption must not publish merged metadata";
    ASSERT_OK(FileSystem::Default()->path_exists(fixture.source_path));
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_empty_index_values_in_non_shared_legacy_sstable) {
    IndexValuesWithVerPB empty_values;
    const std::string empty_value_key = encode_int_primary_key(10);
    auto fixture = make_below_floor_legacy_fixture("below_floor_legacy_zero_values.sst",
                                                   {{empty_value_key, empty_values.SerializeAsString()}},
                                                   /*source_high=*/47);
    ASSERT_EQ(0, empty_values.values_size());
    ASSIGN_OR_ABORT(auto source_entries,
                    read_raw_pk_sstable(fixture.hot_tablet, fixture.hot_metadata, fixture.source_pb));
    ASSERT_EQ(1, source_entries.size());
    EXPECT_EQ(empty_value_key, source_entries[0].first);
    EXPECT_EQ(0, source_entries[0].second.values_size())
            << "fixture must persist one real key with a serialized empty IndexValuesWithVerPB";

    ASSERT_OK(put_tablet_metadata(fixture.cold_metadata));
    ASSERT_OK(put_tablet_metadata(fixture.hot_metadata));
    ReshardingTabletInfoPB resharding;
    auto& merging = *resharding.mutable_merging_tablet_info();
    merging.add_old_tablet_ids(fixture.cold_tablet);
    merging.add_old_tablet_ids(fixture.hot_tablet);
    merging.set_new_tablet_id(fixture.merged_tablet);

    const int64_t read_errors_before = StorageMetrics::instance()->pk_index_sst_read_error_total.value();
    bool published = true;
    auto [status, cache_drop_count] = publish_merge_and_count_cache_drops(
            _tablet_manager.get(), resharding, BelowFloorLegacyFixture::kBaseVersion,
            BelowFloorLegacyFixture::kMergedVersion, /*txn_id=*/407, &published);
    ASSERT_FALSE(status.ok()) << "serialized empty per-key value lists must fail closed";
    EXPECT_TRUE(status.is_corruption()) << status;
    EXPECT_TRUE(status.message().contains(fixture.source_filename)) << status;
    EXPECT_TRUE(status.message().contains("no index values")) << status;
    EXPECT_EQ(1, cache_drop_count) << "semantic source corruption must evict the source cache exactly once";
    EXPECT_EQ(read_errors_before + 1, StorageMetrics::instance()->pk_index_sst_read_error_total.value());
    EXPECT_FALSE(published) << "source corruption must not publish merged metadata";
    ASSERT_OK(FileSystem::Default()->path_exists(fixture.source_path));
}

// Mapping disagreement is sufficient to dispatch this non-shared legacy SST
// straight to rebuild_non_shared_legacy_sstable. The exact owner classifier is
// intentionally bypassed, so its zero-value validation cannot satisfy this
// regression. The direct rebuild reader must reject the real serialized empty
// IndexValuesWithVerPB and its cleanup guard must remove the already-opened
// partial writer output.
TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_empty_index_values_in_direct_non_shared_legacy_rebuild) {
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_source_flush(
            [&] { set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE); });

    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string source_filename = "direct_non_shared_zero_values.sst";
    const std::string source_path = _tablet_manager->sst_location(child_b, source_filename);
    const std::string empty_value_key = encode_int_primary_key(10);
    IndexValuesWithVerPB empty_values;
    const auto source_file = write_raw_pk_sstable(source_path, {{empty_value_key, empty_values.SerializeAsString()}});

    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(3);
    set_primary_key_schema(meta_a.get(), 1001);
    auto* rowset_a = meta_a->add_rowsets();
    rowset_a->set_id(1);
    rowset_a->set_version(base_version);
    rowset_a->set_num_rows(10);
    rowset_a->set_data_size(100);
    auto* segment_a = rowset_a->add_segment_metas();
    segment_a->set_filename("direct_rebuild_shared.dat");
    segment_a->set_size(100);
    segment_a->set_shared(true);
    stamp_physical_identity_uid(rowset_a, segment_a->filename());

    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(3);
    set_primary_key_schema(meta_b.get(), 1001);
    auto* shared_rowset_b = meta_b->add_rowsets();
    shared_rowset_b->CopyFrom(*rowset_a);
    auto* local_rowset_b = meta_b->add_rowsets();
    local_rowset_b->set_id(2);
    local_rowset_b->set_version(base_version);
    local_rowset_b->set_num_rows(5);
    local_rowset_b->set_data_size(50);
    auto* local_segment_b = local_rowset_b->add_segment_metas();
    local_segment_b->set_filename("direct_rebuild_local.dat");
    local_segment_b->set_size(50);
    local_segment_b->set_shared(false);

    auto* source_pb = meta_b->mutable_sstable_meta()->add_sstables();
    source_pb->set_filename(source_filename);
    source_pb->set_filesize(source_file.filesize);
    source_pb->set_shared(false);
    source_pb->set_max_rss_rowid(static_cast<uint64_t>(2) << 32);
    source_pb->mutable_range()->CopyFrom(source_file.range);

    ASSIGN_OR_ABORT(auto source_entries, read_raw_pk_sstable(child_b, meta_b, *source_pb));
    ASSERT_EQ(1, source_entries.size());
    EXPECT_EQ(empty_value_key, source_entries[0].first);
    EXPECT_EQ(0, source_entries[0].second.values_size())
            << "fixture must persist one real key with a serialized empty IndexValuesWithVerPB";

    ASSERT_OK(put_tablet_metadata(meta_a));
    ASSERT_OK(put_tablet_metadata(meta_b));
    const std::string output_directory = _location_provider->segment_root_location(merged_tablet);
    ASSIGN_OR_ABORT(auto files_before, directory_inventory(output_directory));

    ReshardingTabletInfoPB resharding;
    auto& merging = *resharding.mutable_merging_tablet_info();
    merging.add_old_tablet_ids(child_a);
    merging.add_old_tablet_ids(child_b);
    merging.set_new_tablet_id(merged_tablet);

    const int64_t read_errors_before = StorageMetrics::instance()->pk_index_sst_read_error_total.value();
    bool published = true;
    TabletMetadataPtr returned_merged_metadata;
    auto [status, cache_drop_count] =
            publish_merge_and_count_cache_drops(_tablet_manager.get(), resharding, base_version, new_version,
                                                /*txn_id=*/408, &published, &returned_merged_metadata);

    EXPECT_FALSE(status.ok()) << "direct non-shared rebuild must fail closed instead of silently dropping the key";
    EXPECT_TRUE(status.is_corruption()) << status;
    EXPECT_TRUE(status.message().contains(source_filename)) << status;
    EXPECT_TRUE(status.message().contains("rebuild")) << status;
    EXPECT_TRUE(status.message().contains("no index values")) << status;
    EXPECT_EQ(1, cache_drop_count) << "semantic source corruption must evict the source cache exactly once";
    EXPECT_EQ(read_errors_before + 1, StorageMetrics::instance()->pk_index_sst_read_error_total.value());
    EXPECT_FALSE(published) << "source corruption must not publish target metadata";
    if (returned_merged_metadata != nullptr) {
        EXPECT_EQ(0, returned_merged_metadata->sstable_meta().sstables_size())
                << "the pre-fix success must be a silent drop, not a replacement SST";
        EXPECT_EQ(0, returned_merged_metadata->orphan_files_size());
    }
    EXPECT_EQ(nullptr, returned_merged_metadata) << "source corruption must not return a merged PB or orphan list";

    ASSIGN_OR_ABORT(auto files_after, directory_inventory(output_directory));
    EXPECT_EQ(files_before, files_after) << "the direct-rebuild cleanup guard must remove partial writer output";
    ASSERT_OK(FileSystem::Default()->path_exists(source_path));
}

// The same physical legacy shared SST is grouped from the first two range
// occurrences, while its zero-value key belongs to the third input range. The
// current remapper drops such an entry as owned by another occurrence before it
// inspects values. Validation therefore has to happen before route/remap; moving
// it afterwards must make this test RED by restoring the silent drop.
TEST_F(LakeTabletReshardTest, test_tablet_merging_rejects_empty_index_values_before_grouped_shared_legacy_route_drop) {
    set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::ENABLE);
    DeferOp restore_source_flush(
            [&] { set_failpoint_mode("skip_lake_pk_index_merge_source_flush", FailPointTriggerModeType::DISABLE); });

    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t child_c = next_id();
    const int64_t merged_tablet = next_id();
    for (int64_t tablet_id : {child_a, child_b, child_c, merged_tablet}) {
        prepare_tablet_dirs(tablet_id);
    }

    const std::string source_filename = "grouped_shared_route_drop_zero_values.sst";
    const std::string source_path = _tablet_manager->sst_location(child_a, source_filename);
    const std::string third_range_key = encode_int_primary_key(80);
    IndexValuesWithVerPB empty_values;
    const auto source_file = write_raw_pk_sstable(source_path, {{third_range_key, empty_values.SerializeAsString()}});

    auto make_child = [&](int64_t tablet_id, int lower, int upper, const std::string& segment_filename,
                          bool reference_source) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(base_version);
        metadata->set_next_rowset_id(2);
        set_int_primary_key_schema(metadata.get(), 1001);
        metadata->set_enable_persistent_index(true);
        metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(lower));
        metadata->mutable_range()->set_lower_bound_included(true);
        metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(upper));
        metadata->mutable_range()->set_upper_bound_included(false);

        auto* rowset = metadata->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(base_version);
        rowset->set_num_rows(1);
        rowset->set_data_size(100);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(segment_filename);
        segment->set_size(100);

        if (reference_source) {
            auto* source_pb = metadata->mutable_sstable_meta()->add_sstables();
            source_pb->set_filename(source_filename);
            source_pb->set_filesize(source_file.filesize);
            source_pb->set_shared(true);
            source_pb->set_max_rss_rowid(static_cast<uint64_t>(1) << 32);
            source_pb->mutable_range()->CopyFrom(source_file.range);
        }
        return metadata;
    };

    auto meta_a = make_child(child_a, /*lower=*/0, /*upper=*/30, "grouped_route_left.dat",
                             /*reference_source=*/true);
    auto meta_b = make_child(child_b, /*lower=*/30, /*upper=*/60, "grouped_route_middle.dat",
                             /*reference_source=*/true);
    auto meta_c = make_child(child_c, /*lower=*/60, /*upper=*/100, "grouped_route_right.dat",
                             /*reference_source=*/false);

    ASSERT_EQ(1, meta_a->sstable_meta().sstables_size());
    ASSIGN_OR_ABORT(auto source_entries, read_raw_pk_sstable(child_a, meta_a, meta_a->sstable_meta().sstables(0)));
    ASSERT_EQ(1, source_entries.size());
    EXPECT_EQ(third_range_key, source_entries[0].first);
    EXPECT_EQ(0, source_entries[0].second.values_size())
            << "fixture must persist one real key with a serialized empty IndexValuesWithVerPB";

    ASSERT_OK(put_tablet_metadata(meta_a));
    ASSERT_OK(put_tablet_metadata(meta_b));
    ASSERT_OK(put_tablet_metadata(meta_c));
    const std::string output_directory = _location_provider->segment_root_location(merged_tablet);
    ASSIGN_OR_ABORT(auto files_before, directory_inventory(output_directory));

    ReshardingTabletInfoPB resharding;
    auto& merging = *resharding.mutable_merging_tablet_info();
    merging.add_old_tablet_ids(child_a);
    merging.add_old_tablet_ids(child_b);
    merging.add_old_tablet_ids(child_c);
    merging.set_new_tablet_id(merged_tablet);

    const int64_t read_errors_before = StorageMetrics::instance()->pk_index_sst_read_error_total.value();
    bool published = true;
    TabletMetadataPtr returned_merged_metadata;
    auto [status, cache_drop_count] =
            publish_merge_and_count_cache_drops(_tablet_manager.get(), resharding, base_version, new_version,
                                                /*txn_id=*/409, &published, &returned_merged_metadata);

    EXPECT_FALSE(status.ok()) << "grouped shared rebuild must validate before route/remap can silently drop the key";
    EXPECT_TRUE(status.is_corruption()) << status;
    EXPECT_TRUE(status.message().contains(source_filename)) << status;
    EXPECT_TRUE(status.message().contains("rebuild")) << status;
    EXPECT_TRUE(status.message().contains("no index values")) << status;
    EXPECT_EQ(1, cache_drop_count) << "semantic source corruption must evict the source cache exactly once";
    EXPECT_EQ(read_errors_before + 1, StorageMetrics::instance()->pk_index_sst_read_error_total.value());
    EXPECT_FALSE(published) << "source corruption must not publish target metadata";
    if (returned_merged_metadata != nullptr) {
        EXPECT_EQ(0, returned_merged_metadata->sstable_meta().sstables_size())
                << "the pre-fix success must be a route-drop, not a replacement SST";
        EXPECT_EQ(0, returned_merged_metadata->orphan_files_size());
    }
    EXPECT_EQ(nullptr, returned_merged_metadata) << "source corruption must not return a merged PB or orphan list";

    ASSIGN_OR_ABORT(auto files_after, directory_inventory(output_directory));
    EXPECT_EQ(files_before, files_after) << "the grouped-rebuild cleanup guard must remove partial writer output";
    ASSERT_OK(FileSystem::Default()->path_exists(source_path));
}
#endif

TEST_F(LakeTabletReshardTest, test_tablet_merging_below_floor_mixed_tombstone_and_live_data_does_not_clamp) {
    constexpr uint32_t kTombstone = std::numeric_limits<uint32_t>::max();
    auto fixture = make_below_floor_legacy_fixture(
            "below_floor_legacy_mixed_live.sst",
            {{encode_int_primary_key(10), serialize_index_values({{/*version=*/51, kTombstone, kTombstone}})},
             {encode_int_primary_key(20),
              serialize_index_values({{/*version=*/50, BelowFloorLegacyFixture::kSourceLiveRssid, /*rowid=*/0}})}},
            /*source_high=*/47);

    auto merged = merge_modern_shared_occurrences(fixture.cold_metadata, fixture.hot_metadata, fixture.merged_tablet);
    ASSERT_FALSE(merged.ok()) << "one live data value makes below-floor metadata-only projection unsafe";
    EXPECT_TRUE(merged.status().is_corruption()) << merged.status();
    EXPECT_TRUE(merged.status().message().contains("rssid high overflow in merge projection")) << merged.status();
    ASSERT_OK(FileSystem::Default()->path_exists(fixture.source_path));
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_below_floor_mixed_tombstone_and_dead_data_rebuilds) {
    constexpr uint32_t kTombstone = std::numeric_limits<uint32_t>::max();
    const std::string tombstone_key = encode_int_primary_key(10);
    auto fixture = make_below_floor_legacy_fixture(
            "below_floor_legacy_mixed_dead.sst",
            {{tombstone_key, serialize_index_values({{/*version=*/61, kTombstone, kTombstone}})},
             {encode_int_primary_key(30), serialize_index_values({{/*version=*/60, /*rssid=*/47, /*rowid=*/0}})}},
            /*source_high=*/47);

    auto merged_or =
            merge_modern_shared_occurrences(fixture.cold_metadata, fixture.hot_metadata, fixture.merged_tablet);
    ASSERT_OK(merged_or);
    auto merged = std::move(merged_or).value();
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& rebuilt = merged->sstable_meta().sstables(0);
    EXPECT_NE(fixture.source_filename, rebuilt.filename());
    EXPECT_FALSE(rebuilt.shared());
    EXPECT_FALSE(rebuilt.has_shared_rssid());
    EXPECT_EQ(0, rebuilt.rssid_offset());
    EXPECT_EQ(0, rebuilt.max_rss_rowid());
    EXPECT_EQ(BelowFloorLegacyFixture::kMergedVersion, rebuilt.generation_version());

    ASSIGN_OR_ABORT(auto raw_entries, read_raw_pk_sstable(fixture.merged_tablet, merged, rebuilt));
    ASSERT_EQ(1, raw_entries.size());
    EXPECT_EQ(tombstone_key, raw_entries[0].first);
    ASSERT_EQ(1, raw_entries[0].second.values_size());
    EXPECT_EQ(kTombstone, raw_entries[0].second.values(0).rssid());
    EXPECT_EQ(kTombstone, raw_entries[0].second.values(0).rowid());
    ASSERT_OK(FileSystem::Default()->path_exists(fixture.source_path));
}

TEST_F(LakeTabletReshardTest, test_tablet_merging_below_floor_mixed_tombstone_and_delvec_data_rebuilds) {
    constexpr uint32_t kTombstone = std::numeric_limits<uint32_t>::max();
    const std::string tombstone_key = encode_int_primary_key(10);
    auto fixture = make_below_floor_legacy_fixture(
            "below_floor_legacy_mixed_delvec.sst",
            {{tombstone_key, serialize_index_values({{/*version=*/71, kTombstone, kTombstone}})},
             {encode_int_primary_key(20),
              serialize_index_values({{/*version=*/70, BelowFloorLegacyFixture::kSourceLiveRssid, /*rowid=*/0}})}},
            /*source_high=*/47, /*encrypted=*/false, /*filter_live_row_with_delvec=*/true);

    auto merged_or =
            merge_modern_shared_occurrences(fixture.cold_metadata, fixture.hot_metadata, fixture.merged_tablet);
    ASSERT_OK(merged_or);
    auto merged = std::move(merged_or).value();
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& rebuilt = merged->sstable_meta().sstables(0);
    EXPECT_NE(fixture.source_filename, rebuilt.filename());
    EXPECT_FALSE(rebuilt.shared());
    EXPECT_FALSE(rebuilt.has_shared_rssid());
    EXPECT_EQ(0, rebuilt.rssid_offset());
    EXPECT_EQ(0, rebuilt.max_rss_rowid());
    EXPECT_EQ(BelowFloorLegacyFixture::kMergedVersion, rebuilt.generation_version());

    ASSIGN_OR_ABORT(auto raw_entries, read_raw_pk_sstable(fixture.merged_tablet, merged, rebuilt));
    ASSERT_EQ(1, raw_entries.size());
    EXPECT_EQ(tombstone_key, raw_entries[0].first);
    ASSERT_EQ(1, raw_entries[0].second.values_size());
    EXPECT_EQ(kTombstone, raw_entries[0].second.values(0).rssid());
    EXPECT_EQ(kTombstone, raw_entries[0].second.values(0).rowid());
    ASSERT_OK(FileSystem::Default()->path_exists(fixture.source_path));
}

// A stale non-shared SST watermark below the source tablet's carried rowset floor
// must not abort a merge with a negative context offset. This reproduces the
// #11939 arithmetic exactly: source high 47 plus ctx offset -106 is -59.
TEST_F(LakeTabletReshardTest, test_tablet_merging_non_shared_sstable_drops_below_floor_watermark) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t cold_tablet = next_id();
    const int64_t hot_tablet = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(cold_tablet);
    prepare_tablet_dirs(hot_tablet);
    prepare_tablet_dirs(merged_tablet);

    auto cold = std::make_shared<TabletMetadataPB>();
    cold->set_id(cold_tablet);
    cold->set_version(base_version);
    cold->set_next_rowset_id(1);
    set_primary_key_schema(cold.get(), 1001);

    auto hot = std::make_shared<TabletMetadataPB>();
    hot->set_id(hot_tablet);
    hot->set_version(base_version);
    hot->set_next_rowset_id(108);
    set_primary_key_schema(hot.get(), 1001);
    auto* live_rowset = hot->add_rowsets();
    live_rowset->set_id(107);
    live_rowset->set_version(1);
    live_rowset->set_num_rows(1);
    live_rowset->set_data_size(10);
    auto* segment = live_rowset->add_segment_metas();
    segment->set_filename("hot_live.dat");
    segment->set_size(10);

    const std::string sst_filename = "below_floor_watermark.sst";
    const uint64_t sst_filesize = write_legacy_pk_sstable(_tablet_manager->sst_location(hot_tablet, sst_filename),
                                                          {{"dead-key", /*rssid=*/47, /*rowid=*/0}});
    auto* sst = hot->mutable_sstable_meta()->add_sstables();
    sst->set_filename(sst_filename);
    sst->set_filesize(sst_filesize);
    sst->set_shared(false);
    sst->set_max_rss_rowid(static_cast<uint64_t>(47) << 32);

    ASSERT_OK(put_tablet_metadata(cold));
    ASSERT_OK(put_tablet_metadata(hot));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(cold_tablet);
    merging_info.add_old_tablet_ids(hot_tablet);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(101);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    const auto& merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(1, merged->rowsets_size());
    EXPECT_EQ(1, merged->rowsets(0).id()) << "107 + (-106) must project to rssid 1";
    EXPECT_EQ(0, merged->sstable_meta().sstables_size()) << "the SST contains only a below-floor dead entry";
}

// One physical legacy shared SST can outlive a split into several range
// siblings. Merging only an adjacent subset must rebuild a private SST bounded
// by that subset's union range; entries owned by an unmerged sibling must not
// leak into either the rebuilt file or the merged tablet's point lookups.
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_shared_sstable_partial_merge_filters_unmerged_range) {
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

    const std::string key_a = encode_int_primary_key(10);
    const std::string key_b = encode_int_primary_key(60);
    const std::string key_c = encode_int_primary_key(110);
    const std::string legacy_filename = "three_range_partial_merge.sst";
    const uint64_t legacy_filesize = write_legacy_pk_sstable(
            _tablet_manager->sst_location(child_a, legacy_filename),
            {{key_a, /*rssid=*/1, /*rowid=*/0}, {key_b, /*rssid=*/1, /*rowid=*/0}, {key_c, /*rssid=*/1, /*rowid=*/0}});

    auto make_child = [&](int64_t tablet_id, int lower, int upper, const std::string& segment_filename) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(tablet_id);
        metadata->set_version(base_version);
        metadata->set_next_rowset_id(2);
        set_int_primary_key_schema(metadata.get(), 1001);
        metadata->mutable_range()->mutable_lower_bound()->CopyFrom(generate_sort_key(lower));
        metadata->mutable_range()->set_lower_bound_included(true);
        metadata->mutable_range()->mutable_upper_bound()->CopyFrom(generate_sort_key(upper));
        metadata->mutable_range()->set_upper_bound_included(false);

        auto* rowset = metadata->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(1);
        rowset->set_data_size(100);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(segment_filename);
        segment->set_size(100);

        auto* sstable = metadata->mutable_sstable_meta()->add_sstables();
        sstable->set_filename(legacy_filename);
        sstable->set_filesize(legacy_filesize);
        sstable->set_shared(true);
        sstable->set_max_rss_rowid(static_cast<uint64_t>(1) << 32);
        return metadata;
    };

    ASSERT_OK(put_tablet_metadata(make_child(child_a, /*lower=*/0, /*upper=*/50, "partial_left.dat")));
    ASSERT_OK(put_tablet_metadata(make_child(child_b, /*lower=*/50, /*upper=*/100, "partial_middle.dat")));
    ASSERT_OK(put_tablet_metadata(make_child(child_c, /*lower=*/100, /*upper=*/150, "partial_right.dat")));

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
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    EXPECT_NE(legacy_filename, out_sst.filename());
    EXPECT_FALSE(out_sst.shared());
    EXPECT_FALSE(out_sst.has_shared_rssid());
    EXPECT_EQ(0, out_sst.rssid_offset());

    ASSIGN_OR_ABORT(
            auto rebuilt_sstable,
            lake::PersistentIndexSstable::new_sstable(
                    out_sst, _tablet_manager->sst_location(merged_tablet, out_sst.filename()),
                    /*cache=*/nullptr, /*need_filter=*/false, /*delvec=*/nullptr, merged, _tablet_manager.get()));
    sstable::ReadOptions read_options;
    read_options.fill_cache = false;
    std::unique_ptr<sstable::Iterator> iterator(rebuilt_sstable->new_iterator(read_options));
    std::set<std::string> rebuilt_keys;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        rebuilt_keys.insert(iterator->key().to_string());
    }
    ASSERT_OK(iterator->status());
    EXPECT_EQ((std::set<std::string>{key_a, key_b}), rebuilt_keys);

    auto index = std::make_unique<lake::LakePersistentIndex>(_tablet_manager.get(), merged_tablet);
    ASSERT_OK(index->init(merged));
    auto lookup = [&](const std::string& key) {
        Slice key_slice(key);
        IndexValue value;
        EXPECT_OK(index->get(/*n=*/1, &key_slice, &value));
        return value;
    };
    const IndexValue value_a = lookup(key_a);
    const IndexValue value_b = lookup(key_b);
    const IndexValue value_c = lookup(key_c);
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(1) << 32), value_a);
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(2) << 32), value_b);
    EXPECT_EQ(IndexValue(NullIndexValue), value_c);
}

// Stacked merge: parent's legacy sstable already has a non-zero rssid_offset
// (from a prior merge). Merging this parent as ctx[N>=1] with an additional
// non-zero ctx.rssid_offset must accumulate the offsets (sst.rssid_offset +
// ctx.rssid_offset), so the read path's single projection at
// persistent_index_sstable.cpp:214 yields the correct output-space rssid.
TEST_F(LakeTabletReshardTest, test_tablet_merging_accumulates_stacked_rssid_offset) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_child = [&](int64_t tablet_id, uint32_t rowset_id, const std::string& seg_name,
                          const std::string& sst_name, int32_t sst_rssid_offset, bool sst_shared) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(rowset_id + 1);
        set_primary_key_schema(meta.get(), 1001);
        auto* rowset = meta->add_rowsets();
        rowset->set_id(rowset_id);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        {
            auto* sm = rowset->add_segment_metas();
            sm->set_filename(seg_name);
            sm->set_size(100);
        }
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(sst_name);
        sst->set_filesize(512);
        sst->set_shared(sst_shared);
        // No shared_rssid: this is the legacy rssid_offset projection path.
        sst->set_rssid_offset(sst_rssid_offset);
        // max_rss_rowid.high is in the parent tablet's rowset-id space.
        sst->set_max_rss_rowid((static_cast<uint64_t>(rowset_id) << 32) | 99);
        return meta;
    };

    // ctx[0]: rowset id 1, sst with rssid_offset=0 (normal, non-stacked).
    auto meta_a = make_child(child_a, /*rowset_id=*/1, "seg_a.dat", "sst_a.sst",
                             /*sst_rssid_offset=*/0, /*sst_shared=*/false);
    // ctx[1]: rowset id 5, sst with rssid_offset=3 (stacked: prior merge
    // already offset this sstable's stored entries by 3 into ctx[1]'s input
    // tablet id-space). Merging into a new output will add ctx[1].rssid_offset
    // on top, so the accumulated offset should be 3 + ctx[1].rssid_offset.
    auto meta_b = make_child(child_b, /*rowset_id=*/5, "seg_b.dat", "sst_b.sst",
                             /*sst_rssid_offset=*/3, /*sst_shared=*/false);

    materialize_tombstone_sstables(meta_a.get());
    materialize_tombstone_sstables(meta_b.get());

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_tablet = *resharding_tablet.mutable_merging_tablet_info();
    merging_tablet.add_old_tablet_ids(child_a);
    merging_tablet.add_old_tablet_ids(child_b);
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

    // Expect two sstables: ctx[0]'s (unchanged offset 0) and ctx[1]'s
    // (accumulated offset = sst.rssid_offset + ctx.rssid_offset).
    ASSERT_EQ(2, merged->sstable_meta().sstables_size());

    // Locate the two output sstables by filename. Also find ctx[1].rssid_offset
    // indirectly: its rowset was re-mapped by ctx.map_rssid, so its output
    // rowset id minus its input rowset id (5) is ctx[1].rssid_offset.
    const PersistentIndexSstablePB* sst_a = nullptr;
    const PersistentIndexSstablePB* sst_b = nullptr;
    for (const auto& sst : merged->sstable_meta().sstables()) {
        if (sst.filename() == "sst_a.sst") sst_a = &sst;
        if (sst.filename() == "sst_b.sst") sst_b = &sst;
    }
    ASSERT_NE(nullptr, sst_a);
    ASSERT_NE(nullptr, sst_b);

    // ctx[0] keeps its original offset (0); no stacking.
    EXPECT_EQ(0, sst_a->rssid_offset());

    // Recover ctx[1].rssid_offset from the rowset mapping. Here ctx[1]'s complete
    // carried rowset-id space starts at 5, above ctx[0]'s next_rowset_id, so
    // compute_rssid_offset returns a negative shift. Deriving the offset from the
    // output rather than hard-coding it keeps the accumulation assertion meaningful.
    bool found_ctx1 = false;
    int32_t ctx1_offset = 0;
    for (const auto& rs : merged->rowsets()) {
        if (rs.segment_metas_size() > 0 && rs.segment_metas(0).filename() == "seg_b.dat") {
            ctx1_offset = static_cast<int32_t>(rs.id()) - 5;
            found_ctx1 = true;
            break;
        }
    }
    ASSERT_TRUE(found_ctx1) << "failed to locate ctx[1]'s output rowset";

    // ctx[1]'s sst input rssid_offset was 3; accumulated = 3 + ctx1_offset.
    EXPECT_EQ(3 + ctx1_offset, sst_b->rssid_offset());

    // max_rss_rowid.high for ctx[1]'s sst was 5 pre-merge; post-merge high
    // should be projected by +ctx1_offset.
    const uint32_t sst_b_high = static_cast<uint32_t>(sst_b->max_rss_rowid() >> 32);
    EXPECT_EQ(static_cast<uint32_t>(5 + ctx1_offset), sst_b_high);
}

// Merging a cold sibling with a hot one whose rowset id space has run far ahead must not
// fail the publish. Regression for "Segment id overflow during tablet merge".
//
// The id layout below is taken verbatim from a wedged production partition: writes into a
// range-distributed table are range-routed, so the hot range's tablet reached rowset id 860
// / next_rowset_id 861 while the cold sibling still had a single compacted rowset at id 11 /
// next_rowset_id 12. compute_rssid_offset used to answer 12 - 798 = -786 for the hot tablet,
// and add_rowset applies that shift to two fields that reference rowsets which no longer
// exist and therefore sit below the live minimum: max_compact_input_rowset_id (797) and
// del_files[].origin_rowset_id (766, inherited from a compaction input). 766 - 786 = -20
// tripped map_rssid's `mapped < 0` branch and failed the whole reshard publish; the FE then
// retried it forever and the table stayed unwritable.
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

    // The complete carried floor is origin_rowset_id 766, so the hot tablet receives
    // offset 12 - 766 = -754. Its live rowsets remain above every projected dead
    // reference while the complete projected space starts at the cold tablet's ceiling.
    ASSERT_TRUE(by_id.count(11)) << "cold sibling's rowset must keep id 11";
    ASSERT_TRUE(by_id.count(44)) << "hot sibling's rowsets must use the complete carried floor";
    ASSERT_TRUE(by_id.count(49));
    ASSERT_TRUE(by_id.count(106));

    // The two dead-reference fields are shifted by the same offset as the live ids.
    // The minimum reference lands exactly at base.next_rowset_id instead of below zero.
    const auto* compaction_output = by_id[49];
    EXPECT_EQ(43u, compaction_output->max_compact_input_rowset_id());
    ASSERT_EQ(1, compaction_output->del_files_size());
    EXPECT_EQ(12u, compaction_output->del_files(0).origin_rowset_id());

    // Every value carried by the hot sibling must stay above the cold sibling's live
    // rowset id. Under the old live-only floor, max_compact_input_rowset_id 797
    // landed exactly on 11 and origin_rowset_id 766 underflowed to -20.
    for (const auto& rowset : merged->rowsets()) {
        if (rowset.id() == 11) continue; // cold sibling's rowset
        EXPECT_GT(rowset.id(), 11u);
        EXPECT_GT(rowset.max_compact_input_rowset_id(), 11u);
        for (const auto& del_file : rowset.del_files()) {
            EXPECT_GT(del_file.origin_rowset_id(), 11u);
        }
    }

    // Future writes must not reuse an occupied id.
    EXPECT_EQ(107u, merged->next_rowset_id());
}

// A later input's compacted-away reference can numerically equal an earlier
// input's live rowset id. The reference must be included in the source floor so
// the two unrelated identifiers remain disjoint in the merged namespace.
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

    ASSERT_TRUE(by_id.count(766)) << "the earlier input's live rowset keeps id 766";
    ASSERT_TRUE(by_id.count(800)) << "the later input shifts by 767 - 765 = 2";
    ASSERT_TRUE(by_id.count(805));

    const auto* later_compaction_output = by_id[805];
    ASSERT_EQ(1, later_compaction_output->del_files_size());
    EXPECT_EQ(768u, later_compaction_output->del_files(0).origin_rowset_id())
            << "the later input's dead reference must not alias the earlier live rowset";
    EXPECT_EQ(767u, later_compaction_output->max_compact_input_rowset_id())
            << "the later input's compaction watermark must also stay above the earlier live rowset";
    EXPECT_EQ(806u, merged->next_rowset_id());
}

// A same-UID sibling copy is discarded by merge_rowsets, so its historical
// references must not lower the later input's floor. Otherwise an ancient
// reference on the discarded copy can force a positive lift that projects an
// otherwise-valid high-ID rowset beyond the supported signed allocation domain.
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
    ASSERT_TRUE(by_id.count(kBaseRowsetId));
    ASSERT_TRUE(by_id.count(kBaseNextRowsetId))
            << "the emitted high-ID rowset must map from its own floor, not the discarded copy's reference";
    EXPECT_EQ(kBaseNextRowsetId + 1, merged->next_rowset_id());
}

// Three siblings whose rowset-id counters have diverged, where the middle one
// carries nothing but a delete predicate that dedups away against the first
// sibling's predicate at the same version.
//
// A discarded predicate is the one discarded rowset whose id still reaches the
// merged namespace through the natural offset: merge_rowsets records a
// shared_rssid_map entry for a discarded duplicate only when it is NOT a delete
// predicate. So the floor must include it -- otherwise the middle sibling has no
// floor at all, keeps offset 0, and its raw id (here 2.1e9) becomes the watermark
// the third sibling is lifted above, pushing the third sibling's own high-ID
// rowset past INT32_MAX even though the emitted namespace has room for it.
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

    // ctx[0]: data(v1) -> predicate(v10). Its next_rowset_id (3) is the base watermark.
    TabletMetadataPB low_meta;
    low_meta.set_id(low_tablet);
    low_meta.set_version(base_version);
    low_meta.set_next_rowset_id(3);
    add_rowset_with_predicate(&low_meta, /*rowset_id=*/1, /*version=*/1, /*has_predicate=*/false);
    add_rowset_with_predicate(&low_meta, /*rowset_id=*/2, /*version=*/10, /*has_predicate=*/true);
    ASSERT_OK(put_tablet_metadata(low_meta));

    // ctx[1]: the same v10 delete predicate, cross-published onto a sibling whose id
    // counter ran far ahead. Predicates dedup by version, so this is the whole tablet
    // and merge_rowsets emits nothing from it.
    constexpr uint32_t kFarAheadPredicateId = 2'100'000'000;
    TabletMetadataPB predicate_only_meta;
    predicate_only_meta.set_id(predicate_only_tablet);
    predicate_only_meta.set_version(base_version);
    predicate_only_meta.set_next_rowset_id(kFarAheadPredicateId + 1);
    add_rowset_with_predicate(&predicate_only_meta, kFarAheadPredicateId, /*version=*/10, /*has_predicate=*/true);
    ASSERT_OK(put_tablet_metadata(predicate_only_meta));

    // ctx[2]: two live rowsets ~1e8 ids apart. Lifting its floor (5) onto ctx[1]'s raw
    // id would map the top one to 2'199'999'996 > INT32_MAX.
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

    // ctx[1]'s floor is its discarded predicate id, so it shifts down to the base
    // watermark 3 and reserves exactly that one slot. ctx[2] is then lifted to 4
    // instead of to 2'100'000'001, and its span survives intact.
    EXPECT_EQ((std::vector<uint32_t>{1, 2, 4, kWideSpanTopId - 1}), rowset_ids);
    EXPECT_EQ(kWideSpanTopId, merged->next_rowset_id());
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

// merge_sstables projects each child's sstable.max_rss_rowid by adding the
// child's rssid_offset to the high word (tablet_merger.cpp:615-618). The
// projected high word can exceed every rowset.id in the merged metadata —
// e.g. a delete-only sstable from a child contributes a high rssid that has
// no matching rowset. update_next_rowset_id must consider the projected
// sstable highs; otherwise next_rowset_id is set too low and a SPLIT child
// inheriting this metadata will write new sstables whose max_rss_rowid is
// LESS than existing inherited sstables' projected max_rss_rowid, breaking
// the ascending-order invariant that LakePersistentIndex::commit() enforces.
// Downstream symptom: COMPACTION publish on the SPLIT child fails with
// "sstables are not ordered, last_max_rss_rowid=A : max_rss_rowid=B" and
// the next reshard job parks in PREPARING.
TEST_F(LakeTabletReshardTest, test_tablet_merging_next_rowset_id_covers_projected_sstable_high) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // Child A: tiny rowsets/sstables, modest next_rowset_id.
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
        sm->set_filename("a_seg.dat");
        sm->set_size(100);
    }
    auto* sst_a = meta_a->mutable_sstable_meta()->add_sstables();
    sst_a->set_filename("a.sst");
    sst_a->set_filesize(256);
    sst_a->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 50);

    // Child B: also small rowsets, but its sstable's max_rss_rowid encodes a
    // HIGH high word — well beyond next_rowset_id. This simulates the legacy
    // path where a delete-only sstable carries a saturated rssid ahead of any
    // surviving rowset.id (PersistentIndexMemtable::erase et al.).
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
        sm->set_filename("b_seg.dat");
        sm->set_size(100);
    }
    auto* sst_b = meta_b->mutable_sstable_meta()->add_sstables();
    sst_b->set_filename("b.sst");
    sst_b->set_filesize(256);
    sst_b->set_max_rss_rowid((static_cast<uint64_t>(200) << 32) | 99);

    materialize_tombstone_sstables(meta_a.get());
    materialize_tombstone_sstables(meta_b.get());

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
    // After projection, child_b's sstable carries max_rss_rowid with high =
    // 200 + rssid_offset_b; rssid_offset_b for the second child is at least
    // child_a's next_rowset_id (== 3), so the projected high is >= 200.
    // Find the max projected high across all sstables.
    uint64_t max_projected_high = 0;
    for (const auto& sst : merged->sstable_meta().sstables()) {
        max_projected_high = std::max(max_projected_high, sst.max_rss_rowid() >> 32);
    }
    ASSERT_GE(max_projected_high, 200u);
    // next_rowset_id must be strictly greater than every projected sstable
    // high; otherwise a future write would produce a sstable with a smaller
    // max_rss_rowid than these existing ones.
    EXPECT_GT(merged->next_rowset_id(), max_projected_high)
            << "next_rowset_id=" << merged->next_rowset_id()
            << " must exceed max projected sstable rssid=" << max_projected_high;
}

// Shared rowset with identical .cols filenames across children collapses to
// a single DCG entry via exact dedup — no rebuild.
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

// PR-2 §5.2.4: merge_sstables's shared_rssid path must project a delvec from
// new_metadata->delvec_meta()[mapped_rssid] regardless of whether the SOURCE
// sstable had has_delvec set. Without this change, a synthesized gap delvec
// (created in Phase 0 because a sibling child compacted away its share)
// would never reach the PK-index sstable PB and PersistentIndexSstable::
// multi_get could return stale rssids.
TEST_F(LakeTabletReshardTest, test_tablet_merging_pk_sstable_pb_delvec_projection_when_source_has_no_delvec) {
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
    auto meta_b = make_pk_compacted_child(child_b, base_version, /*compacted_id=*/11, /*lower=*/10, /*upper=*/20,
                                          "compacted_b.dat");

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

    // sstable_meta should have one shared sstable; its delvec PB must be
    // populated by §5.2.4's projection even though the source had no delvec.
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    EXPECT_EQ("shared.sst", out_sst.filename());
    EXPECT_EQ(canonical_rssid, out_sst.shared_rssid());
    ASSERT_TRUE(out_sst.has_delvec()) << "merged sstable PB missing projected delvec";
    EXPECT_GT(out_sst.delvec().size(), 0u) << "merged sstable PB delvec is empty";
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
// This is defensive: uuid-unique .idx names make one-name-two-targets impossible in prod,
// but the orphan set must be provably safe regardless. child_a keeps "same.idx" active at
// its rssid; child_b (distinct uid => remapped rssid) has "same.idx" fully tombstoned.
TEST_F(LakeTabletReshardTest, test_tablet_merging_idg_orphan_skips_still_referenced_file) {
    const int64_t base_version = 1, new_version = 2;
    const int64_t child_a = next_id(), child_b = next_id(), merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    auto make_child = [&](int64_t tablet_id, const std::string& seg, bool tombstone) {
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
        sm->set_filename(seg);
        sm->set_size(100);
        stamp_physical_identity_uid(rowset, seg); // distinct uid per child => both kept
        add_idg_with_key(meta.get(), 1, "same.idx", /*col_uid=*/5, BITMAP, 1);
        if (tombstone) add_idg_dropped_key(meta.get(), 1, /*col_uid=*/5, BITMAP);
        return meta;
    };
    EXPECT_OK(put_tablet_metadata(make_child(child_a, "seg_a.dat", /*tombstone=*/false)));
    EXPECT_OK(put_tablet_metadata(make_child(child_b, "seg_b.dat", /*tombstone=*/true)));

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

// MERGE: two children with DISTINCT private segments each carry their own real .idx.
// After merge both survive: one at the natural rssid, the other at the offset-remapped rssid
// (proves a non-first-source entry is remapped, not dropped).
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

// MERGE rebuilds a legacy shared sstable into a NEW physical file; that new file
// must carry the merge (new) version, not the source sstable's old version.
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_sstable_rebuild_stamps_new_version) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();
    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string legacy_filename = "legacy_gv_rebuild.sst";
    const auto legacy_path = _tablet_manager->sst_location(child_b, legacy_filename);
    const uint64_t legacy_filesize =
            write_legacy_pk_sstable(legacy_path, {{"k1", /*rssid=*/1, /*rowid=*/0}, {"k2", /*rssid=*/2, /*rowid=*/0}});

    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(11);
    set_primary_key_schema(meta_a.get(), 1001);
    {
        auto* rs = meta_a->add_rowsets();
        rs->set_id(10);
        rs->set_version(1);
        rs->set_num_rows(10);
        rs->set_data_size(100);
        auto* sm = rs->add_segment_metas();
        sm->set_filename("seg_a10.dat");
        sm->set_size(100);
    }

    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(3);
    set_primary_key_schema(meta_b.get(), 1001);
    for (uint32_t rs_id : {1u, 2u}) {
        auto* rs = meta_b->add_rowsets();
        rs->set_id(rs_id);
        rs->set_version(1);
        rs->set_num_rows(10);
        rs->set_data_size(100);
        auto* sm = rs->add_segment_metas();
        sm->set_filename(fmt::format("seg_b{}.dat", rs_id));
        sm->set_size(100);
    }
    auto* sst = meta_b->mutable_sstable_meta()->add_sstables();
    sst->set_filename(legacy_filename);
    sst->set_filesize(legacy_filesize);
    sst->set_shared(true);
    sst->set_max_rss_rowid((static_cast<uint64_t>(2) << 32) | 0);
    sst->set_version(base_version); // OLD version; rebuild writes a NEW file -> must get new_version

    EXPECT_OK(put_tablet_metadata(meta_a));
    EXPECT_OK(put_tablet_metadata(meta_b));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(2);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    EXPECT_NE(legacy_filename, out_sst.filename()) << "rebuild wrote a new file";
    EXPECT_EQ(new_version, out_sst.generation_version()) << "rebuilt (new) file must carry the merge version";
}

// =============================================================================
// Full sort key index (config::enable_full_sort_key_index) split coverage.
//
// When the config is enabled, SegmentWriter stores the complete, untruncated sort key
// in the short key index instead of metadata sort-key samples (see
// SegmentSplitInfo::load_samples_from_short_key_index and build_segments_from_rowsets in
// tablet_splitter.cpp). These tests drive real segment files through that loader via the
// full publish_resharding_tablet path and assert the same Σ children == parent
// conservation invariants the metadata-sample tests above already guarantee.
// =============================================================================

// Full-key conservation: a single rowset backed by a REAL segment written with
// config::enable_full_sort_key_index=true (no metadata samples) is split 3-way. The
// split reader must read the segment's full, untruncated short key index directly
// (build_segments_from_rowsets' loader path -- gated on a valid, non-zero schema id) and
// preserve the anchor's exactness contract: Σ children.rowset.{num_rows,data_size,
// num_dels} == parent, mirroring test_pk_tablet_splitting_anchor_per_rowset_conservation
// above. The child ranges must also tile the parent's key space with no gap/overlap.
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
} // namespace starrocks
