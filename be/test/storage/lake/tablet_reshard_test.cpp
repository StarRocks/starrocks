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

#include <limits>

#include "base/path/filesystem_util.h"
#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "common/config_storage_fwd.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "storage/del_vector.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reshard_helper.h"
#include "storage/lake/transactions.h"
#include "storage/lake/update_manager.h"
#include "storage/variant_tuple.h"

namespace starrocks {

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
    }

    void TearDown() override {
        // Only remove this test's own subdirectory. Removing the entire
        // config::storage_root_path would wipe out DataDir's persistent /tmp/
        // subdirectory (created once at StorageEngine init) and break any later
        // test that writes local CRM files during compaction (e.g.
        // LakePrimaryKeyPublishTest.test_individual_index_compaction).
        auto status = fs::remove_all(_test_dir);
        EXPECT_TRUE(status.ok() || status.is_not_found()) << status;
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

    void add_historical_schema(TabletMetadataPB* metadata, int64_t schema_id) {
        auto& schema = (*metadata->mutable_historical_schemas())[schema_id];
        schema.set_id(schema_id);
        schema.set_keys_type(PRIMARY_KEYS);
    }

    RowsetMetadataPB* add_rowset(TabletMetadataPB* metadata, uint32_t rowset_id, uint32_t max_compact_input_rowset_id,
                                 uint32_t del_origin_rowset_id) {
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(rowset_id);
        rowset->set_max_compact_input_rowset_id(max_compact_input_rowset_id);
        rowset->add_segments("segment.dat");
        rowset->add_segment_size(128);
        auto* del_file = rowset->add_del_files();
        del_file->set_name("del.dat");
        del_file->set_origin_rowset_id(del_origin_rowset_id);
        return rowset;
    }

    RowsetMetadataPB* add_rowset_with_predicate(TabletMetadataPB* metadata, uint32_t rowset_id, int64_t version,
                                                bool has_predicate) {
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(rowset_id);
        rowset->set_version(version);
        rowset->set_overlapped(false);
        if (!has_predicate) {
            rowset->add_segments(fmt::format("segment_{}.dat", rowset_id));
            rowset->add_segment_size(128);
            rowset->set_num_rows(1);
            rowset->set_data_size(128);
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
        return rowset;
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
    rowset_meta_pb->add_segments("test_0.dat");
    rowset_meta_pb->add_segment_size(512);
    auto* segment_meta0 = rowset_meta_pb->add_segment_metas();
    segment_meta0->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
    segment_meta0->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
    segment_meta0->set_num_rows(3);

    rowset_meta_pb->add_segments("test_1.dat");
    rowset_meta_pb->add_segment_size(512);
    auto* segment_meta1 = rowset_meta_pb->add_segment_metas();
    segment_meta1->mutable_sort_key_min()->CopyFrom(generate_sort_key(50));
    segment_meta1->mutable_sort_key_max()->CopyFrom(generate_sort_key(100));
    segment_meta1->set_num_rows(2);
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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

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
    rowset_meta_pb->add_segments("seg_0.dat");
    rowset_meta_pb->add_segment_size(1024);
    auto* segment_meta = rowset_meta_pb->add_segment_metas();
    segment_meta->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
    segment_meta->mutable_sort_key_max()->CopyFrom(generate_sort_key(300));
    segment_meta->set_num_rows(300);
    segment_meta->set_sort_key_sample_row_interval(100);
    segment_meta->add_sort_key_samples()->CopyFrom(generate_sort_key(100));
    segment_meta->add_sort_key_samples()->CopyFrom(generate_sort_key(200));
    rowset_meta_pb->set_num_rows(300);
    rowset_meta_pb->set_data_size(1024);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

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
    rowset_meta_pb->add_segments("test_0.dat");
    rowset_meta_pb->add_segment_size(512);
    auto* segment_meta0 = rowset_meta_pb->add_segment_metas();
    segment_meta0->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
    segment_meta0->mutable_sort_key_max()->CopyFrom(generate_sort_key(299999));
    segment_meta0->set_num_rows(100);

    rowset_meta_pb->add_segments("test_1.dat");
    rowset_meta_pb->add_segment_size(512);
    auto* segment_meta1 = rowset_meta_pb->add_segment_metas();
    segment_meta1->mutable_sort_key_min()->CopyFrom(generate_sort_key(300000));
    segment_meta1->mutable_sort_key_max()->CopyFrom(generate_sort_key(599999));
    segment_meta1->set_num_rows(100);

    rowset_meta_pb->set_overlapped(true);
    rowset_meta_pb->set_data_size(1024);
    rowset_meta_pb->set_num_rows(200);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

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

    rowset->add_segments("segment_0.dat");
    rowset->add_segment_size(400);
    auto* segment_meta0 = rowset->add_segment_metas();
    segment_meta0->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
    segment_meta0->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
    segment_meta0->set_num_rows(4);

    rowset->add_segments("segment_1.dat");
    rowset->add_segment_size(400);
    auto* segment_meta1 = rowset->add_segment_metas();
    segment_meta1->mutable_sort_key_min()->CopyFrom(generate_sort_key(50));
    segment_meta1->mutable_sort_key_max()->CopyFrom(generate_sort_key(99));
    segment_meta1->set_num_rows(4);

    DelVector delvec;
    const uint32_t deleted_rows[] = {0, 1, 2};
    delvec.init(base_version, deleted_rows, 3);
    add_delvec(&metadata, tablet_id, base_version, rowset->id(), "test.delvec", delvec.save());

    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

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

    rowset->add_segments("segment_0.dat");
    rowset->add_segment_size(500);
    auto* segment_meta0 = rowset->add_segment_metas();
    segment_meta0->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
    segment_meta0->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
    segment_meta0->set_num_rows(5);

    rowset->add_segments("segment_1.dat");
    rowset->add_segment_size(500);
    auto* segment_meta1 = rowset->add_segment_metas();
    segment_meta1->mutable_sort_key_min()->CopyFrom(generate_sort_key(50));
    segment_meta1->mutable_sort_key_max()->CopyFrom(generate_sort_key(99));
    segment_meta1->set_num_rows(5);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

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

    rowset->add_segments("segment_0.dat");
    rowset->add_segment_size(400);
    auto* segment_meta0 = rowset->add_segment_metas();
    segment_meta0->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
    segment_meta0->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
    segment_meta0->set_num_rows(4);

    rowset->add_segments("segment_1.dat");
    rowset->add_segment_size(400);
    auto* segment_meta1 = rowset->add_segment_metas();
    segment_meta1->mutable_sort_key_min()->CopyFrom(generate_sort_key(50));
    segment_meta1->mutable_sort_key_max()->CopyFrom(generate_sort_key(99));
    segment_meta1->set_num_rows(4);

    DelVector delvec;
    const uint32_t deleted_rows[] = {0, 1, 2, 3};
    delvec.init(base_version, deleted_rows, 4);
    add_delvec(&metadata, tablet_id, base_version, rowset->id(), "test.delvec", delvec.save());

    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

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
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));

    TabletMetadataPB meta_b;
    meta_b.set_id(tablet_b);
    meta_b.set_version(base_version);
    meta_b.set_next_rowset_id(4);
    add_rowset_with_predicate(&meta_b, 1, 1, false);
    add_rowset_with_predicate(&meta_b, 2, 10, true);
    add_rowset_with_predicate(&meta_b, 3, 11, false);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));

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
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));

    TabletMetadataPB meta_b;
    meta_b.set_id(tablet_b);
    meta_b.set_version(base_version);
    meta_b.set_next_rowset_id(4);
    add_rowset_with_predicate(&meta_b, 1, 1, false);
    add_rowset_with_predicate(&meta_b, 2, 2, false);
    add_rowset_with_predicate(&meta_b, 3, 3, false);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
    // Version-driven k-way merge interleaves by (version, child_index):
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
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));

    // Tablet B: data(v1) -> data(v2) -> data(v3) (no predicates)
    TabletMetadataPB meta_b;
    meta_b.set_id(tablet_b);
    meta_b.set_version(base_version);
    meta_b.set_next_rowset_id(4);
    add_rowset_with_predicate(&meta_b, 1, 1, false);
    add_rowset_with_predicate(&meta_b, 2, 2, false);
    add_rowset_with_predicate(&meta_b, 3, 3, false);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));

    // Tablet B: predicate(v10) -> predicate(v20) (same versions)
    TabletMetadataPB meta_b;
    meta_b.set_id(tablet_b);
    meta_b.set_version(base_version);
    meta_b.set_next_rowset_id(3);
    add_rowset_with_predicate(&meta_b, 1, 10, true); // predicate v10
    add_rowset_with_predicate(&meta_b, 2, 20, true); // predicate v20
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta1));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta2));

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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta1));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta2));

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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta1));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta2));

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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta1));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta2));

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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta1));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta2));

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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta1));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta2));

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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta1));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta2));

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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta1));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta2));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_new));

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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta1));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta2));

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
    old_rowset->add_segments("segment.dat");
    old_rowset->add_segment_size(100);

    auto new_meta = std::make_shared<TabletMetadataPB>(*old_meta);
    new_meta->set_id(new_tablet_id);
    new_meta->set_version(base_version);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(old_meta));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(new_meta));

    TxnLogPB log;
    log.set_tablet_id(old_tablet_id);
    log.set_txn_id(100);
    auto* op_write_rowset = log.mutable_op_write()->mutable_rowset();
    op_write_rowset->set_overlapped(false);
    op_write_rowset->set_num_rows(1);
    op_write_rowset->set_data_size(1);
    op_write_rowset->add_segments("x.dat");
    op_write_rowset->add_segment_size(1);

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
        rowset->add_segments(segment_name);
        rowset->add_segment_size(1);
        set_range(rowset->mutable_range(), lower, upper);
    };
    auto fill_sstable = [&](PersistentIndexSstablePB* sstable, const std::string& filename) {
        sstable->set_filename(filename);
        sstable->set_filesize(1);
        sstable->set_shared(false);
    };
    auto expect_shared_and_range = [&](const RowsetMetadataPB& rowset, int lower, int upper) {
        ASSERT_EQ(rowset.segments_size(), rowset.shared_segments_size());
        for (int i = 0; i < rowset.shared_segments_size(); ++i) {
            EXPECT_TRUE(rowset.shared_segments(i));
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
        rowset->add_segments("shared_seg.dat");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
        // Add shared sstable with shared_rssid
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename("shared_sst.sst");
        sst->set_filesize(512);
        sst->set_shared(true);
        sst->set_shared_rssid(1);
        sst->set_shared_version(1);
        sst->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 99);
        return meta;
    };

    auto meta_a = make_child(child_a);
    auto meta_b = make_child(child_b);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
    EXPECT_EQ("shared_seg.dat", merged->rowsets(0).segments(0));
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
    // max_rss_rowid high part should match projected shared_rssid
    EXPECT_EQ((static_cast<uint64_t>(out_sst.shared_rssid()) << 32) | 99, out_sst.max_rss_rowid());
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
        rowset->add_segments("shared_seg.dat");
        rowset->add_segment_size(data_size);
        rowset->add_shared_segments(true);
        return meta;
    };

    // Parent rowset was 10 rows / 6 dels / 100 bytes. Split gave A 4/3 and B 6/3.
    auto meta_a = make_child(child_a, /*num_rows=*/4, /*data_size=*/40, /*num_dels=*/3);
    auto meta_b = make_child(child_b, /*num_rows=*/6, /*data_size=*/60, /*num_dels=*/3);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
    shared_a->add_segments("shared_seg.dat");
    shared_a->add_segment_size(100);
    shared_a->add_shared_segments(true);
    // Local upsert (new data after split)
    auto* local_a = meta_a->add_rowsets();
    local_a->set_id(2);
    local_a->set_version(2);
    local_a->set_num_rows(5);
    local_a->set_data_size(50);
    local_a->add_segments("local_a_seg.dat");
    local_a->add_segment_size(50);

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
    shared_b->add_segments("shared_seg.dat");
    shared_b->add_segment_size(100);
    shared_b->add_shared_segments(true);
    // Local upsert (different new data)
    auto* local_b = meta_b->add_rowsets();
    local_b->set_id(2);
    local_b->set_version(3);
    local_b->set_num_rows(3);
    local_b->set_data_size(30);
    local_b->add_segments("local_b_seg.dat");
    local_b->add_segment_size(30);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
    EXPECT_EQ("shared_seg.dat", merged->rowsets(0).segments(0));
    // Remaining two are the local ones
    std::unordered_set<std::string> local_segments;
    for (int i = 1; i < merged->rowsets_size(); ++i) {
        local_segments.insert(merged->rowsets(i).segments(0));
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
    compacted_a->add_segments("compacted_a.dat");
    compacted_a->add_segment_size(100);
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
    shared_b->add_segments("shared_seg.dat");
    shared_b->add_segment_size(100);
    shared_b->add_shared_segments(true);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
    rowset_a->add_segments("local_a.dat");
    rowset_a->add_segment_size(50);

    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(3);
    auto* rowset_b = meta_b->add_rowsets();
    rowset_b->set_id(1);
    rowset_b->set_version(1);
    rowset_b->set_num_rows(10);
    rowset_b->set_data_size(100);
    rowset_b->add_segments("shared_seg.dat");
    rowset_b->add_segment_size(100);
    rowset_b->add_shared_segments(true);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
        return meta;
    };

    auto meta_a = make_del_only_child(child_a);
    auto meta_b = make_del_only_child(child_b);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
    rowset_c->add_segments("family_a_seg.dat");
    rowset_c->add_segment_size(100);
    rowset_c->add_shared_segments(true);

    auto meta_d = std::make_shared<TabletMetadataPB>();
    meta_d->set_id(child_d);
    meta_d->set_version(base_version);
    meta_d->set_next_rowset_id(3);
    auto* rowset_d = meta_d->add_rowsets();
    rowset_d->set_id(1);
    rowset_d->set_version(1);
    rowset_d->set_num_rows(10);
    rowset_d->set_data_size(100);
    rowset_d->add_segments("family_e_seg.dat");
    rowset_d->add_segment_size(100);
    rowset_d->add_shared_segments(true);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_c));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_d));

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
    rowset_a->add_segments("cross_pub.dat");
    rowset_a->add_segment_size(100);
    rowset_a->add_shared_segments(true);

    auto meta_b = std::make_shared<TabletMetadataPB>();
    meta_b->set_id(child_b);
    meta_b->set_version(base_version);
    meta_b->set_next_rowset_id(8);
    auto* rowset_b = meta_b->add_rowsets();
    rowset_b->set_id(3); // Different ID from A's rowset
    rowset_b->set_version(1);
    rowset_b->set_num_rows(10);
    rowset_b->set_data_size(100);
    rowset_b->add_segments("cross_pub.dat"); // Same segment
    rowset_b->add_segment_size(100);
    rowset_b->add_shared_segments(true);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
    EXPECT_EQ("cross_pub.dat", merged->rowsets(0).segments(0));
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
    rowset_a->add_segments("shared_seg.dat");
    rowset_a->add_segment_size(100);
    rowset_a->add_shared_segments(true);
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
    rowset_b->add_segments("shared_seg.dat");
    rowset_b->add_segment_size(100);
    rowset_b->add_shared_segments(true);
    // DCG from child B's different independent partial update
    add_dcg_with_columns(meta_b.get(), 1, "dcg_b.cols", {1}, 1);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));

    TabletMetadataPB meta_b;
    meta_b.set_id(child_b);
    meta_b.set_version(base_version);
    meta_b.set_next_rowset_id(3);
    add_rowset_with_predicate(&meta_b, 1, 5, true);  // same predicate v5
    add_rowset_with_predicate(&meta_b, 2, 6, false); // data v6
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
        rowset->add_segments("shared_seg.dat");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
        // Same shared DCG on both children (inherited from split)
        add_dcg_with_columns(meta.get(), 1, "shared_dcg.cols", {1, 2}, 1);
        return meta;
    };

    auto meta_a = make_child_with_dcg(child_a);
    auto meta_b = make_child_with_dcg(child_b);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
    rowset_a->add_segments("shared_seg.dat");
    rowset_a->add_segment_size(100);
    rowset_a->add_shared_segments(true);
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
    rowset_b->add_segments("shared_seg.dat");
    rowset_b->add_segment_size(100);
    rowset_b->add_shared_segments(true);
    // Delvec from child_b's different independent delete
    add_delvec(meta_b.get(), child_b, 2, 1, "delvec_b.dv", dv_b_data);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
    rowset_a->add_segments("shared_seg1.dat");
    rowset_a->add_segment_size(100);
    rowset_a->add_shared_segments(true);
    rowset_a->add_segments("shared_seg2.dat");
    rowset_a->add_segment_size(100);
    rowset_a->add_shared_segments(true);
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
    rowset_b->add_segments("shared_seg1.dat");
    rowset_b->add_segment_size(100);
    rowset_b->add_shared_segments(true);
    rowset_b->add_segments("shared_seg2.dat");
    rowset_b->add_segment_size(100);
    rowset_b->add_shared_segments(true);
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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
        rowset->add_segments("shared_seg.dat");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
        add_delvec(meta.get(), tablet_id, delvec_version, 1, delvec_file_name, delvec_data);
        return meta;
    };

    auto meta_a = make_child_meta(child_a, 1, "delvec_a.dv", dv_a_data);
    auto meta_b = make_child_meta(child_b, 2, "delvec_b.dv", dv_b_data);
    auto meta_c = make_child_meta(child_c, 3, "delvec_c.dv", dv_c_data);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_c));

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
    rowset_a->add_segments("shared_seg.dat");
    rowset_a->add_segment_size(100);
    rowset_a->add_shared_segments(true);
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
    rowset_b->add_segments("shared_seg.dat");
    rowset_b->add_segment_size(100);
    rowset_b->add_shared_segments(true);
    // Same file name, same offset/size -> page-ref dedup
    add_delvec(meta_b.get(), child_b, 1, 1, "shared_delvec.dv", dv_data);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
        rowset->add_segments("shared_seg.dat");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
        return meta;
    };

    auto meta_a = make_child(child_a);
    add_dcg_with_columns(meta_a.get(), 1, "a.cols", {1, 2}, 1);

    auto meta_b = make_child(child_b);
    add_dcg_with_columns(meta_b.get(), 1, "b.cols", {3, 4}, 1);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
        rowset->add_segments("shared_seg.dat");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
        add_dcg_with_columns(meta.get(), 1, "shared.cols", {1, 2}, 1);
        return meta;
    };

    auto meta_a = make_child(child_a);
    auto meta_b = make_child(child_b);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
        rowset->add_segments("shared_seg.dat");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
        return meta;
    };

    auto meta_a = make_child(child_a);
    add_dcg_with_columns(meta_a.get(), 1, "a.cols", {1}, 1);

    auto meta_b = make_child(child_b);
    add_dcg_with_columns(meta_b.get(), 1, "b.cols", {1}, 1);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
        rowset->add_segments("shared_seg.dat");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
        return meta;
    };

    auto meta_a = make_child(child_a);
    add_dcg_with_columns(meta_a.get(), 1, "a.cols", {1, 2}, 1);

    auto meta_b = make_child(child_b);
    add_dcg_with_columns(meta_b.get(), 1, "b.cols", {2, 3}, 1);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
    rowset->add_segments("seg.dat");
    rowset->add_segment_size(100);
    // Use legacy add_dcg (no unique_column_ids/versions)
    add_dcg(meta_a.get(), 1, "malformed.cols");

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));

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
    rowset->add_segments("seg.dat");
    rowset->add_segment_size(100);
    // Build a malformed DCG with overlapping column UIDs across entries
    add_dcg_with_columns(meta_a.get(), 1, "first.cols", {1, 2}, 1);
    add_dcg_with_columns(meta_a.get(), 1, "second.cols", {2, 3}, 1);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));

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

TEST_F(LakeTabletReshardTest, test_tablet_merging_sstable_shared_without_shared_rssid) {
    // Legacy shared sstable without shared_rssid: uses rssid_offset path.
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
        rowset->add_segments("shared_seg.dat");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
        // Legacy shared sstable: no shared_rssid, no delvec
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename("legacy_sst.sst");
        sst->set_filesize(256);
        sst->set_shared(true);
        sst->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 50);
        return meta;
    };

    auto meta_a = make_child(child_a);
    auto meta_b = make_child(child_b);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
    // Deduped to 1 sstable
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    EXPECT_EQ("legacy_sst.sst", out_sst.filename());
    // No shared_rssid: should use rssid_offset (first child offset = 0)
    EXPECT_FALSE(out_sst.has_shared_rssid());
    EXPECT_EQ(0, out_sst.rssid_offset());
    EXPECT_FALSE(out_sst.has_delvec());
}

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
    rowset_a1->add_segments("shared_seg.dat");
    rowset_a1->add_segment_size(100);
    rowset_a1->add_shared_segments(true);
    auto* rowset_a2 = meta_a->add_rowsets();
    rowset_a2->set_id(2);
    rowset_a2->set_version(2);
    rowset_a2->set_num_rows(5);
    rowset_a2->set_data_size(50);
    rowset_a2->add_segments("local_a.dat");
    rowset_a2->add_segment_size(50);
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
    rowset_b->add_segments("shared_seg.dat");
    rowset_b->add_segment_size(100);
    rowset_b->add_shared_segments(true);
    // Same shared sstable
    auto* sst_shared_b = meta_b->mutable_sstable_meta()->add_sstables();
    sst_shared_b->set_filename("shared_sst.sst");
    sst_shared_b->set_filesize(512);
    sst_shared_b->set_shared(true);
    sst_shared_b->set_shared_rssid(1);
    sst_shared_b->set_shared_version(1);
    sst_shared_b->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 99);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
    rowset_a->add_segments("seg_a.dat");
    rowset_a->add_segment_size(100);
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
    rowset_b->add_segments("seg_b.dat");
    rowset_b->add_segment_size(100);
    auto* sst_b = meta_b->mutable_sstable_meta()->add_sstables();
    sst_b->set_filename("sst_family_b.sst");
    sst_b->set_filesize(256);
    sst_b->set_shared(true);
    sst_b->set_shared_rssid(1);
    sst_b->set_shared_version(1);
    sst_b->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 50);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
    rowset_a->add_segments("local_seg.dat");
    rowset_a->add_segment_size(100);

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
    rowset_b->add_segments("seg_b.dat");
    rowset_b->add_segment_size(50);
    auto* sst_b = meta_b->mutable_sstable_meta()->add_sstables();
    sst_b->set_filename("sst_b.sst");
    sst_b->set_filesize(512);
    sst_b->set_shared(true);
    sst_b->set_shared_rssid(1);
    sst_b->set_shared_version(2);
    sst_b->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 99);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
    rowset->add_segments("seg.dat");
    rowset->add_segment_size(1000);
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
    ASSERT_TRUE(converted0->op_write().rowset().shared_segments_size() > 0);
    EXPECT_TRUE(converted0->op_write().rowset().shared_segments(0));
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
    rowset->add_segments(segment_name);
    rowset->add_segment_size(128);
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
    op->mutable_output_rowset()->add_segments("out_seg.dat");
    // Normal (non-partial) compaction: all output segments are newly written.
    op->set_new_segment_offset(0);
    op->set_new_segment_count(1);
    op->mutable_output_sstable()->set_filename("out_sstable.sst");
    return log;
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
        subtask->mutable_output_rowset()->add_segments(fmt::format("subtask_seg_{}.dat", i));
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
        subtask->mutable_output_rowset()->add_segments(fmt::format("split_subtask_seg_{}.dat", i));
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
    rowset->add_segments("write_seg.dat");
    rowset->add_segment_size(600);

    lake::PublishTabletInfo info(lake::PublishTabletInfo::SPLITTING_TABLET, source_tablet_id, child_tablet_id, 3, 0);
    ASSIGN_OR_ABORT(auto converted, lake::convert_txn_log(log, base_metadata, info));

    ASSERT_TRUE(converted->has_op_write());
    EXPECT_EQ(child_tablet_id, converted->tablet_id());
    // Splitter scaled num_rows / data_size by split_count and applied
    // shared-flag to op_write rowset.
    EXPECT_EQ(20, converted->op_write().rowset().num_rows());
    EXPECT_EQ(200, converted->op_write().rowset().data_size());
    ASSERT_TRUE(converted->op_write().rowset().shared_segments_size() > 0);
    EXPECT_TRUE(converted->op_write().rowset().shared_segments(0));
}

// Regression: op_parallel_compaction subtasks synthesized by
// tablet_parallel_compaction_manager do not set new_segment_count — their
// output_rowset carries only newly written segments, so the helper should
// treat all of them as new rather than silently skipping them (which would
// leak segment files).
TEST_F(LakeTabletReshardTest, test_collect_compaction_output_file_paths_parallel_without_new_segment_count) {
    const int64_t tablet_id = next_id();
    TxnLogPB log;
    log.set_tablet_id(tablet_id);
    auto* op_parallel_compaction = log.mutable_op_parallel_compaction();
    auto* subtask = op_parallel_compaction->add_subtask_compactions();
    auto* output_rowset = subtask->mutable_output_rowset();
    output_rowset->add_segments("parallel_new_0.dat");
    output_rowset->add_segments("parallel_new_1.dat");
    // Intentionally NOT setting new_segment_offset/new_segment_count to
    // reproduce the shape produced by the parallel-compaction manager.

    auto paths = lake::tablet_reshard_helper::collect_compaction_output_file_paths(log, _tablet_manager.get());
    EXPECT_THAT(paths,
                ::testing::UnorderedElementsAre(_tablet_manager->segment_location(tablet_id, "parallel_new_0.dat"),
                                                _tablet_manager->segment_location(tablet_id, "parallel_new_1.dat")));
}

// Regression: partial compaction's output_rowset.segments() concatenates
// reused input segments with newly written ones; only the new window
// (new_segment_offset / new_segment_count) should be queued for deletion.
// Deleting reused segments would corrupt the merged tablet because those
// segments are still live as input rowsets absorbed by the merge.
TEST_F(LakeTabletReshardTest, test_collect_compaction_output_file_paths_partial_compaction) {
    const int64_t tablet_id = next_id();
    TxnLogPB log;
    log.set_tablet_id(tablet_id);
    auto* op_compaction = log.mutable_op_compaction();
    auto* output_rowset = op_compaction->mutable_output_rowset();
    // [reused_0, reused_1, new_0, new_1] — only new_0/new_1 are newly written.
    output_rowset->add_segments("reused_0.dat");
    output_rowset->add_segments("reused_1.dat");
    output_rowset->add_segments("new_0.dat");
    output_rowset->add_segments("new_1.dat");
    op_compaction->set_new_segment_offset(2);
    op_compaction->set_new_segment_count(2);

    auto paths = lake::tablet_reshard_helper::collect_compaction_output_file_paths(log, _tablet_manager.get());
    EXPECT_THAT(paths, ::testing::UnorderedElementsAre(_tablet_manager->segment_location(tablet_id, "new_0.dat"),
                                                       _tablet_manager->segment_location(tablet_id, "new_1.dat")));
    EXPECT_THAT(paths,
                ::testing::Not(::testing::Contains(_tablet_manager->segment_location(tablet_id, "reused_0.dat"))));
    EXPECT_THAT(paths,
                ::testing::Not(::testing::Contains(_tablet_manager->segment_location(tablet_id, "reused_1.dat"))));
}

// Verifies that collect_compaction_output_file_paths() collects files of every
// kind — segments (via output_rowset), ssts (compaction-ingested), output_sstable,
// output_sstables, lcrm_file, plus op_parallel_compaction.output_sstable /
// output_sstables / orphan_lcrm_files — so regressions don't silently reintroduce
// leaks by dropping any one category.
TEST_F(LakeTabletReshardTest, test_collect_compaction_output_file_paths_covers_all_kinds) {
    const int64_t tablet_id = next_id();
    TxnLogPB log;
    log.set_tablet_id(tablet_id);

    // Top-level op_compaction with every output-file kind populated.
    auto* op_compaction = log.mutable_op_compaction();
    op_compaction->mutable_output_rowset()->add_segments("out_seg.dat");
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

    auto paths = lake::tablet_reshard_helper::collect_compaction_output_file_paths(log, _tablet_manager.get());
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

// Two PK parents (not cloud-native, so flush_parent_for_merge is a pass-through)
// each carry a shared legacy standalone sstable with the same filename but
// different `fileset_id` values (as would happen when PersistentIndexSstableFileset
// init synthesizes a random id per load). Without excluding fileset_id from the
// shared-sstable consistency check in merge_sstables dedup, this would return
// Status::Corruption. With fileset_id excluded (identity is pinned by filename
// + filesize + encryption_meta + range), the merge succeeds and the output
// sstable_meta deduplicates to a single entry.
TEST_F(LakeTabletReshardTest, test_tablet_merging_dedup_legacy_standalone_sstable) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // Both children share the same legacy standalone sstable (no range, no
    // shared_rssid) but carry different synthesized fileset_id values.
    auto make_child = [&](int64_t tablet_id, uint64_t fileset_hi, uint64_t fileset_lo) {
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
        rowset->add_segments("shared_seg.dat");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename("legacy_shared.sst");
        sst->set_filesize(512);
        sst->set_shared(true);
        // Legacy standalone: no range, no shared_rssid. Simulated per-parent
        // synthesized fileset_id (differs across parents).
        sst->mutable_fileset_id()->set_hi(fileset_hi);
        sst->mutable_fileset_id()->set_lo(fileset_lo);
        sst->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 99);
        return meta;
    };

    auto meta_a = make_child(child_a, 0x1111, 0x2222);
    auto meta_b = make_child(child_b, 0x3333, 0x4444); // different fileset_id

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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

    // Legacy standalone sstable should dedup to a single entry despite
    // differing fileset_ids across parents.
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    EXPECT_EQ("legacy_shared.sst", merged->sstable_meta().sstables(0).filename());
    EXPECT_TRUE(merged->sstable_meta().sstables(0).shared());
    EXPECT_FALSE(merged->sstable_meta().sstables(0).has_range());
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
        rowset->add_segments(seg_name);
        rowset->add_segment_size(100);
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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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

    // Recover ctx[1].rssid_offset from the rowset mapping. compute_rssid_offset
    // can be negative (base.next_rowset_id - append.min_id) when ctx[1]'s input
    // rowset ids are already higher than ctx[0]'s, which is legal and exercises
    // the accumulation arithmetic under signed offsets.
    bool found_ctx1 = false;
    int32_t ctx1_offset = 0;
    for (const auto& rs : merged->rowsets()) {
        if (rs.segments_size() > 0 && rs.segments(0) == "seg_b.dat") {
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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta));

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

} // namespace starrocks
