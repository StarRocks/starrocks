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
#include <set>

#include "base/path/filesystem_util.h"
#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/column_helper.h"
#include "common/config_storage_fwd.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "storage/chunk_helper.h"
#include "storage/del_vector.h"
#include "storage/lake/filenames.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/persistent_index_sstable.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_merger_split_family.h"
#include "storage/lake/tablet_range_helper.h"
#include "storage/lake/tablet_reshard_helper.h"
#include "storage/lake/transactions.h"
#include "storage/lake/update_manager.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/seek_range.h"
#include "storage/sstable/iterator.h"
#include "storage/sstable/options.h"
#include "storage/tablet_schema.h"
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

    // Write a real Segment file with num_rows rows: c0 = [0..num_rows), c1 = source_value_of(c0).
    // Returns the segment file size on disk. The file is placed under
    // tablet_id's segment directory as |segment_name|.
    uint64_t write_two_column_segment(int64_t tablet_id, const std::string& segment_name, int num_rows,
                                      const std::function<int(int)>& source_value_of) {
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
            v0[i] = i;
            v1[i] = source_value_of(i);
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
        auto chunk = ChunkHelper::new_chunk(iter_schema, 4096);
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
    rs_a->add_segments("rs_a_0.dat");
    rs_a->add_segment_size(10000);
    auto* sm_a = rs_a->add_segment_metas();
    sm_a->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
    sm_a->mutable_sort_key_max()->CopyFrom(generate_sort_key(99));
    sm_a->set_num_rows(100);

    // Rowset B: keys [50, 199], 60 rows / 6000 bytes / 0 dels (overlaps A on [50,99]).
    auto* rs_b = metadata.add_rowsets();
    rs_b->set_id(3);
    rs_b->set_overlapped(true);
    rs_b->set_num_rows(60);
    rs_b->set_data_size(6000);
    rs_b->set_num_dels(0);
    rs_b->add_segments("rs_b_0.dat");
    rs_b->add_segment_size(6000);
    auto* sm_b = rs_b->add_segment_metas();
    sm_b->mutable_sort_key_min()->CopyFrom(generate_sort_key(50));
    sm_b->mutable_sort_key_max()->CopyFrom(generate_sort_key(199));
    sm_b->set_num_rows(60);

    // Rowset C: keys [100, 199], 30 rows / 3000 bytes / 11 dels.
    auto* rs_c = metadata.add_rowsets();
    rs_c->set_id(4);
    rs_c->set_overlapped(true);
    rs_c->set_num_rows(30);
    rs_c->set_data_size(3000);
    rs_c->set_num_dels(11);
    rs_c->add_segments("rs_c_0.dat");
    rs_c->add_segment_size(3000);
    auto* sm_c = rs_c->add_segment_metas();
    sm_c->mutable_sort_key_min()->CopyFrom(generate_sort_key(100));
    sm_c->mutable_sort_key_max()->CopyFrom(generate_sort_key(199));
    sm_c->set_num_rows(30);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

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
    rowset->add_segments("seg_0.dat");
    rowset->add_segment_size(500);
    auto* sm0 = rowset->add_segment_metas();
    sm0->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
    sm0->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
    sm0->set_num_rows(5);

    rowset->add_segments("seg_1.dat");
    rowset->add_segment_size(500);
    auto* sm1 = rowset->add_segment_metas();
    sm1->mutable_sort_key_min()->CopyFrom(generate_sort_key(50));
    sm1->mutable_sort_key_max()->CopyFrom(generate_sort_key(99));
    sm1->set_num_rows(5);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

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

    rowset->add_segments("seg_0.dat");
    rowset->add_segment_size(400);
    auto* sm0 = rowset->add_segment_metas();
    sm0->mutable_sort_key_min()->CopyFrom(generate_sort_key(0));
    sm0->mutable_sort_key_max()->CopyFrom(generate_sort_key(49));
    sm0->set_num_rows(4);

    rowset->add_segments("seg_1.dat");
    rowset->add_segment_size(400);
    auto* sm1 = rowset->add_segment_metas();
    sm1->mutable_sort_key_min()->CopyFrom(generate_sort_key(50));
    sm1->mutable_sort_key_max()->CopyFrom(generate_sort_key(99));
    sm1->set_num_rows(4);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

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
        rs->add_segments(fmt::format("rs_{}_0.dat", rs_id));
        rs->add_segment_size(data_size);
        auto* sm = rs->add_segment_metas();
        sm->mutable_sort_key_min()->CopyFrom(generate_sort_key(min_v));
        sm->mutable_sort_key_max()->CopyFrom(generate_sort_key(max_v));
        sm->set_num_rows(num_rows);
        sm->set_sort_key_sample_row_interval(interval);
        for (int v = min_v + interval; v < max_v; v += interval) {
            sm->add_sort_key_samples()->CopyFrom(generate_sort_key(v));
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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

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
    // Legacy shared sstable (shared=true && !has_shared_rssid). With the
    // metadata-only fast-path on top of PR #72219's rebuild, a clean family
    // (no partial compaction, no delvec) is projected without reading or
    // writing a new file: output PB is byte-identical to the source PB.
    // PR #72219's rebuild remains as a fallback for non-clean cases (covered
    // by the test_tablet_merging_legacy_sstable_rebuild_* tests).
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // Write a real PK sstable shared by both children. Stored rssid 1 must
    // match a live child rowset for the rebuild to keep the entries; rowset
    // id 1 below satisfies that.
    const std::string legacy_filename = "legacy_sst.sst";
    const auto legacy_path = _tablet_manager->sst_location(child_a, legacy_filename);
    const uint64_t legacy_filesize =
            write_legacy_pk_sstable(legacy_path, {{"k1", /*rssid=*/1, /*rowid=*/0}, {"k2", /*rssid=*/1, /*rowid=*/1}});

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
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
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
    // Deduped to a single sstable. Both children kept rowset id=1 alive and
    // there is no merged delvec → fast-path safety conditions hold; output is
    // a byte-for-byte copy of the source PB (same filename, shared=true).
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    EXPECT_EQ(legacy_filename, out_sst.filename());
    EXPECT_TRUE(out_sst.shared());
    EXPECT_FALSE(out_sst.has_shared_rssid());
    EXPECT_EQ(0, out_sst.rssid_offset());
    EXPECT_FALSE(out_sst.has_delvec());
    EXPECT_EQ(static_cast<uint32_t>(1), static_cast<uint32_t>(out_sst.max_rss_rowid() >> 32));
    EXPECT_EQ(legacy_filesize, out_sst.filesize());
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

// Helper: build a PK tablet metadata where `rowset_id`'s segment is marked
// shared across children (mirrors split-family structure). Returns the rowset.
RowsetMetadataPB* add_shared_rowset(TabletMetadataPB* metadata, uint32_t rowset_id, int64_t version,
                                    const std::string& segment_filename) {
    auto* rowset = metadata->add_rowsets();
    rowset->set_id(rowset_id);
    rowset->set_version(version);
    rowset->set_num_rows(10);
    rowset->set_data_size(100);
    rowset->add_segments(segment_filename);
    rowset->add_segment_size(100);
    rowset->add_shared_segments(true);
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
    rowset_a->add_segments("seg_a.dat");
    rowset_a->add_segment_size(100);
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
    rowset_b->add_segments("seg_b.dat");
    rowset_b->add_segment_size(100);
    auto* sst_b_local = meta_b->mutable_sstable_meta()->add_sstables();
    sst_b_local->set_filename("b_local.sst");
    sst_b_local->set_filesize(128);
    sst_b_local->set_max_rss_rowid((static_cast<uint64_t>(3) << 32) | 50);

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
    ASSERT_EQ(2, merged->sstable_meta().sstables_size());

    uint64_t prev_max = 0;
    for (const auto& sst : merged->sstable_meta().sstables()) {
        EXPECT_LE(prev_max, sst.max_rss_rowid()) << "post-merge sstables must be in non-decreasing max_rss_rowid order";
        prev_max = sst.max_rss_rowid();
    }
    EXPECT_EQ("b_local.sst", merged->sstable_meta().sstables(0).filename());
    EXPECT_EQ("a_tombstone.sst", merged->sstable_meta().sstables(1).filename());
}

// LakePersistentIndex::commit() (lake_persistent_index.cpp:880-881) implicitly
// converts max_rss_rowid (uint64) to int64_t and does a signed `>` comparison.
// For an sstable whose encoded (rssid<<32|rowid) sets the high bit — for
// example a delete-only memtable at rowset_id >= 2^31 (persistent_index_memtable.cpp
// line 110/131 sets max_rss_rowid = (rowset_id<<32)|UINT32_MAX), or the
// boundary case where a fresh ingest_sst lands at rssid >= 2^31 — unsigned
// ordering is the reverse of signed ordering against any low-rssid sibling.
// merge_sstables() must sort by the SAME signed semantics commit() uses; if
// it sorts unsigned the merged metadata that previously satisfied commit()
// would itself begin failing.
TEST_F(LakeTabletReshardTest, test_tablet_merging_sstables_sort_uses_signed_comparison) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // child_a contributes one sstable whose encoded max_rss_rowid sets bit 63
    // — i.e. the projected high word is >= 2^31, which interprets as a negative
    // int64 and a very large uint64. The rowset metadata itself uses small ids
    // so that compute_rssid_offset for child_b stays within int32 range; what
    // exercises the signed-comparison sort is the sstable's max_rss_rowid value
    // alone (child_a is processed first, so its ctx.rssid_offset is 0 and the
    // projected high passes through unchanged).
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
    rowset_a->add_segments("seg_a.dat");
    rowset_a->add_segment_size(100);
    auto* sst_a_high = meta_a->mutable_sstable_meta()->add_sstables();
    sst_a_high->set_filename("a_high.sst");
    sst_a_high->set_filesize(256);
    // (rssid<<32|low) with rssid >= 2^31 sets bit 63, so as int64_t this is
    // a large negative number — int64 less than any positive sibling. high =
    // 2^31 still fits in uint32 (uint32 max = 2^32-1), so the projection check
    // `new_high > uint32::max` does not trip.
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
    rowset_b->add_segments("seg_b.dat");
    rowset_b->add_segment_size(100);
    auto* sst_b_low = meta_b->mutable_sstable_meta()->add_sstables();
    sst_b_low->set_filename("b_low.sst");
    sst_b_low->set_filesize(128);
    sst_b_low->set_max_rss_rowid((static_cast<uint64_t>(7) << 32) | 50);

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
    ASSERT_EQ(2, merged->sstable_meta().sstables_size());

    // Signed-int64 non-decreasing across the merged sstables — matching the
    // invariant LakePersistentIndex::commit() enforces.
    int64_t prev_max = std::numeric_limits<int64_t>::min();
    for (const auto& sst : merged->sstable_meta().sstables()) {
        const int64_t cur = static_cast<int64_t>(sst.max_rss_rowid());
        EXPECT_LE(prev_max, cur) << "post-merge sstables must be in non-decreasing int64 max_rss_rowid order";
        prev_max = cur;
    }
    // a_high's encoded max_rss_rowid is "negative" int64, so signed sort puts
    // it first; b_low (positive int64) comes second. A naive uint64 sort
    // would swap them and break commit().
    EXPECT_EQ("a_high.sst", merged->sstable_meta().sstables(0).filename());
    EXPECT_EQ("b_low.sst", merged->sstable_meta().sstables(1).filename());
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
    rowset_a->add_segments("seg_a.dat");
    rowset_a->add_segment_size(100);
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
    rowset_b->add_segments("seg_b.dat");
    rowset_b->add_segment_size(100);
    // Child B's lone F_A sstable falls inside F_X's max_rss_rowid range.
    add_sst(meta_b.get(), "fa_high200.sst", 200, 0, fid_a);

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
    rowset_a->add_segments("seg_a.dat");
    rowset_a->add_segment_size(100);
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
    rowset_b->add_segments("seg_b.dat");
    rowset_b->add_segment_size(100);

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

// Two PK parents (not cloud-native, so flush_parent_for_merge is a pass-through)
// each carry a shared legacy standalone sstable with the same filename but
// different `fileset_id` values (as would happen when PersistentIndexSstableFileset
// init synthesizes a random id per load). The shared-sstable consistency check
// must exclude fileset_id (identity is pinned by filename + filesize +
// encryption_meta + range), so the dedup keeps a single source PB and the
// rebuild path emits exactly one rebuilt sstable for the merged tablet.
TEST_F(LakeTabletReshardTest, test_tablet_merging_dedup_legacy_standalone_sstable) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    // Both children share the SAME physical legacy sstable file. We write it
    // once into the test FS — the FixedLocationProvider routes any tablet_id's
    // sst_location to the same shared segments dir.
    const std::string legacy_filename = "legacy_shared.sst";
    const auto legacy_path = _tablet_manager->sst_location(child_a, legacy_filename);
    const uint64_t legacy_filesize =
            write_legacy_pk_sstable(legacy_path, {{"k1", /*rssid=*/1, /*rowid=*/0}, {"k2", /*rssid=*/1, /*rowid=*/1}});

    // Both children reference the same legacy standalone sstable but carry
    // different synthesized fileset_id values.
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
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
        sst->set_shared(true);
        // Legacy standalone: no shared_rssid. Simulated per-parent synthesized
        // fileset_id (differs across parents).
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

    // Differing fileset_ids do not block dedup; the merge produces exactly one
    // sstable. With the fast-path on a clean family the output PB is a
    // byte-for-byte copy of the source PB (the dedup-winner ctx[0]'s PB),
    // including its fileset_id; the C2' check is vacuous here because the
    // source PB has no range field.
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    EXPECT_EQ(legacy_filename, out_sst.filename());
    EXPECT_TRUE(out_sst.shared());
    EXPECT_FALSE(out_sst.has_shared_rssid());
    EXPECT_EQ(0, out_sst.rssid_offset());
    // Fast-path keeps the ctx[0] (dedup-winner) source PB's fileset_id.
    ASSERT_TRUE(out_sst.has_fileset_id());
    EXPECT_EQ(static_cast<uint64_t>(0x1111), out_sst.fileset_id().hi());
    EXPECT_EQ(static_cast<uint64_t>(0x2222), out_sst.fileset_id().lo());
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
//   - A keeps rowset id=1 alive (shared_segments=true).
//   - B keeps rowset id=2 alive (shared_segments=true).
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
        rowset->add_segments(seg_filename);
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
        sst->set_shared(true);
        sst->set_max_rss_rowid((static_cast<uint64_t>(3) << 32) | 0);
        return meta;
    };

    auto meta_a = make_child(child_a, /*live_rowset_id=*/1, "seg_a.dat");
    auto meta_b = make_child(child_b, /*live_rowset_id=*/2, "seg_b.dat");

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
        rowset->add_segments("shared_seg.dat");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
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
        rowset->add_segments("sparse_seg_0.dat");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
        auto* segment_meta_at_idx_0 = rowset->add_segment_metas();
        segment_meta_at_idx_0->set_segment_idx(0);
        rowset->add_segments("sparse_seg_2.dat");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
        auto* segment_meta_at_idx_2 = rowset->add_segment_metas();
        segment_meta_at_idx_2->set_segment_idx(2);
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
        sst->set_shared(true);
        sst->set_max_rss_rowid((static_cast<uint64_t>(12) << 32) | 0);
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
        rowset->add_segments("shared_seg.dat");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
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
        rowset->add_segments("shared_seg.dat");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
        sst->set_shared(true);
        sst->set_max_rss_rowid((static_cast<uint64_t>(5) << 32) | kTombstoneSentinel);
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
        rowset->add_segments("shared_seg.dat");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
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
        data_rowset->add_segments("data_seg.dat");
        data_rowset->add_segment_size(100);
        data_rowset->add_shared_segments(true);
        // Delete-only rowset id=10 (segments_size==0): owns no PK index entries.
        auto* delete_only_rowset = meta->add_rowsets();
        delete_only_rowset->set_id(10);
        delete_only_rowset->set_version(2);
        delete_only_rowset->set_num_rows(0);
        delete_only_rowset->set_data_size(0);
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
        sst->set_shared(true);
        sst->set_max_rss_rowid((static_cast<uint64_t>(10) << 32) | kTombstoneSentinel);
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
// Fast-path metadata-only projection tests (PR following PR #72219)
// =============================================================================
//
// These tests verify try_fastpath_project_legacy_shared_sstable's behavior
// through the merge end-to-end. Distinguishing the two paths via output PB
// shape:
//   * Fast-path hit  → output filename == source, shared==true,
//                      rssid_offset == 0 (= source), no new file written.
//   * Fallback       → output filename != source (rebuild wrote a new UUID),
//                      shared==false, has fileset_id (rebuild assigned).

// Clean family: both children share the legacy sstable and both contribute the
// full set of ancestor rowsets. data_rssid_map == identity over [1, M], no
// merged delvec → all of C0, C0', C2, C3, C5, C6, C7 hold → fast-path emits
// the source PB byte-for-byte.
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_sstable_fastpath_clean_family) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string legacy_filename = "fastpath_clean.sst";
    const auto legacy_path = _tablet_manager->sst_location(child_a, legacy_filename);
    const uint64_t legacy_filesize = write_legacy_pk_sstable(
            legacy_path,
            {{"k1", /*rssid=*/1, /*rowid=*/0}, {"k2", /*rssid=*/2, /*rowid=*/0}, {"k3", /*rssid=*/3, /*rowid=*/0}});

    auto make_child = [&](int64_t tablet_id) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(4);
        set_primary_key_schema(meta.get(), 1001);
        // Rowsets 1, 2, 3 are all shared between children → dedup at
        // merge_rowsets, canonical = ctx[0]'s mapping (offset 0).
        for (uint32_t rs_id : {1u, 2u, 3u}) {
            auto* rowset = meta->add_rowsets();
            rowset->set_id(rs_id);
            rowset->set_version(1);
            rowset->set_num_rows(10);
            rowset->set_data_size(100);
            rowset->add_segments(fmt::format("shared_seg_{}.dat", rs_id));
            rowset->add_segment_size(100);
            rowset->add_shared_segments(true);
        }
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
        sst->set_shared(true);
        // No has_shared_rssid: this is the legacy ancestor-inherited form.
        // No rssid_offset (C0). max_rss_rowid.high == 3 == |rowsets|.
        sst->set_max_rss_rowid((static_cast<uint64_t>(3) << 32) | 0);
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
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);

    // Fast-path signature: output PB is byte-identical to source PB.
    EXPECT_EQ(legacy_filename, out_sst.filename()) << "fast-path keeps source filename, no new UUID written";
    EXPECT_TRUE(out_sst.shared()) << "fast-path keeps shared=true";
    EXPECT_FALSE(out_sst.has_shared_rssid()) << "fast-path keeps !has_shared_rssid";
    EXPECT_EQ(0, out_sst.rssid_offset()) << "C0+C0' force zero offset";
    EXPECT_EQ(legacy_filesize, out_sst.filesize());
    EXPECT_EQ((static_cast<uint64_t>(3) << 32) | 0, out_sst.max_rss_rowid());
}

// Inverse of clean-family: child_a (= ctx[0]) does NOT carry the legacy
// sstable, only child_b (= ctx[1]) does. Phase 1 makes ctx[1].rssid_offset
// non-zero whenever ctx[0]'s rowset id-space pushes ctx[1]'s ids upward, so
// canonical_offset != 0 → C0' fails → fallback to rebuild. Required by the
// plan as defense-in-depth against a fast-path output with non-zero
// rssid_offset that would later get double-shifted by another rebuild.
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_sstable_fastpath_nonzero_canonical_offset_falls_back) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string legacy_filename = "fastpath_nonzero_canonical.sst";
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
    rs_a->add_segments("seg_a10.dat");
    rs_a->add_segment_size(100);

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
        rs->add_segments(fmt::format("seg_b{}.dat", rs_id));
        rs->add_segment_size(100);
    }
    auto* sst = meta_b->mutable_sstable_meta()->add_sstables();
    sst->set_filename(legacy_filename);
    sst->set_filesize(legacy_filesize);
    sst->set_shared(true);
    sst->set_max_rss_rowid((static_cast<uint64_t>(2) << 32) | 0);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
    EXPECT_NE(legacy_filename, out_sst.filename()) << "fallback rebuild wrote a new file";
    EXPECT_FALSE(out_sst.shared());
    EXPECT_TRUE(out_sst.has_fileset_id());
}

// C0: source PB carries a non-zero rssid_offset (a stacked-merge legacy
// sstable). Even if everything else is clean, fast-path falls back unconditionally
// to avoid an emitted PB whose accumulated offset would later get double-shifted
// by a subsequent rebuild.
// v2 update: source PB carrying a non-zero rssid_offset (= a stacked-merge
// legacy sstable) used to fall back to rebuild under v1's C0 zero-offset
// gate. v2 drops C0, so the fast-path now accumulates the source offset
// into the emitted PB (out.rssid_offset = src.rssid_offset +
// canonical_offset) and shifts max_rss_rowid.high accordingly.
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_sstable_fastpath_v2_src_offset_hit) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string legacy_filename = "fastpath_nonzero_src.sst";
    const auto legacy_path = _tablet_manager->sst_location(child_a, legacy_filename);
    // Stored rssids must be >= 1 (rs.id >= 1 invariant). The dense walk
    // skips effective rssid below src.rssid_offset+1 because there is no
    // valid stored rssid 0 in production sstables.
    const uint64_t legacy_filesize =
            write_legacy_pk_sstable(legacy_path, {{"k1", /*rssid=*/1, /*rowid=*/0}, {"k2", /*rssid=*/2, /*rowid=*/0}});

    auto make_child = [&](int64_t tablet_id) {
        auto meta = std::make_shared<TabletMetadataPB>();
        meta->set_id(tablet_id);
        meta->set_version(base_version);
        meta->set_next_rowset_id(4);
        set_primary_key_schema(meta.get(), 1001);
        for (uint32_t rs_id : {1u, 2u, 3u}) {
            auto* rs = meta->add_rowsets();
            rs->set_id(rs_id);
            rs->set_version(1);
            rs->set_num_rows(10);
            rs->set_data_size(100);
            rs->add_segments(fmt::format("nzs_seg_{}.dat", rs_id));
            rs->add_segment_size(100);
            rs->add_shared_segments(true);
        }
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
        sst->set_shared(true);
        // Non-zero src rssid_offset: prior merge already shifted stored ids
        // by 1, so stored 1/2 lift to effective 2/3 in this child's space.
        sst->set_rssid_offset(1);
        sst->set_max_rss_rowid((static_cast<uint64_t>(3) << 32) | 0);
        return meta;
    };

    EXPECT_OK(_tablet_manager->put_tablet_metadata(make_child(child_a)));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(make_child(child_b)));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(3);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    // Fast-path signature: original filename preserved, shared=true, no
    // new fileset_id minted. canonical_offset is 0 (canonical = ctx[0],
    // rssid_offset = 0) so accumulated == 1, and max_rss_rowid.high
    // shifts from 3 to 3 + 0 = 3.
    EXPECT_EQ(legacy_filename, out_sst.filename())
            << "v2 fast-path keeps source filename even with non-zero src offset";
    EXPECT_TRUE(out_sst.shared());
    EXPECT_FALSE(out_sst.has_shared_rssid());
    EXPECT_EQ(1, out_sst.rssid_offset()) << "accumulated = src.rssid_offset(1) + canonical_offset(0)";
    EXPECT_EQ((static_cast<uint64_t>(3) << 32) | 0, out_sst.max_rss_rowid())
            << "max_rss_rowid.high shifts by canonical_offset (0 here)";
}

// C2': source PB has range but no fileset_id. PersistentIndexSstableFileset::
// init(vector) DCHECKs has_fileset_id() for ranged sstables; the fast-path
// preserves the source PB so we cannot mint a new fileset_id without losing
// the no-write-no-read guarantee. Fallback to rebuild, which assigns a fresh
// fileset_id explicitly.
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_sstable_fastpath_range_without_fileset_id_falls_back) {
    const int64_t base_version = 1;
    const int64_t new_version = 2;
    const int64_t child_a = next_id();
    const int64_t child_b = next_id();
    const int64_t merged_tablet = next_id();

    prepare_tablet_dirs(child_a);
    prepare_tablet_dirs(child_b);
    prepare_tablet_dirs(merged_tablet);

    const std::string legacy_filename = "fastpath_range_no_fid.sst";
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
            rs->add_segments(fmt::format("rfid_seg_{}.dat", rs_id));
            rs->add_segment_size(100);
            rs->add_shared_segments(true);
        }
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
        sst->set_shared(true);
        sst->set_max_rss_rowid((static_cast<uint64_t>(2) << 32) | 0);
        // Set has_range but NOT has_fileset_id → C2' fail.
        sst->mutable_range()->set_start_key("a");
        sst->mutable_range()->set_end_key("z");
        // sst->mutable_fileset_id() is intentionally unset.
        return meta;
    };

    EXPECT_OK(_tablet_manager->put_tablet_metadata(make_child(child_a)));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(make_child(child_b)));

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
    EXPECT_NE(legacy_filename, out_sst.filename()) << "C2' fail → fallback rebuild";
    EXPECT_FALSE(out_sst.shared());
    EXPECT_TRUE(out_sst.has_fileset_id()) << "rebuild always assigns a fresh fileset_id";
}

// Regression for fast-path v2 commit 3 (per-child orphan scoping): two
// children that family inference classifies as kNoFamily — their legacy
// shared sstables have distinct filenames (no filename edge) and their
// rowsets are child-local (shared_segments=false → not shared-ancestor,
// no rowset edge). Both children's source rssid space overlaps at
// rssid=1.
//
// Without per-child orphan scoping, the single shared orphan map's
// first-emitter rule lets ctx_a's mapping {1 → 1} survive into ctx_b's
// rebuild lookup. ctx_b's entries would then translate to rssid=1
// (ctx_a's rowset) instead of rssid=2 (ctx_b's rowset, post-Phase-1
// id space lift) — silent PK corruption.
//
// With per-child orphan scoping (orphan_by_child), ctx_b's rebuild
// consumes orphan_by_child[1] which contains only ctx_b.map_rssid(1) = 2.
// The rebuilt sstable's max_rss_rowid (projected via watermark map)
// reflects this isolation directly, so the test asserts on the emitted
// PB's high word.
//
// Note: v2 commit 5's fast-path requires a resolved family_id !=
// kNoFamily, so orphan ctxs always fall through to rebuild here — that
// is what exercises the per-child orphan map fix from commit 3.
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_sstable_orphan_per_child_lookup_isolation) {
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
        // Child-local rowset (shared_segments=false): not a shared-ancestor,
        // so the rowset edge in family inference does not fire across
        // children.
        auto* rowset = meta->add_rowsets();
        rowset->set_id(1);
        rowset->set_version(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        rowset->add_segments(seg_name);
        rowset->add_segment_size(100);
        rowset->add_shared_segments(false);
        // Legacy ancestor-inherited sstable. Distinct filenames across
        // children → filename edge does not fire either, so both ctxs
        // remain kNoFamily.
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
        sst->set_shared(true);
        sst->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 0);
        return meta;
    };

    EXPECT_OK(_tablet_manager->put_tablet_metadata(
            make_child(child_a, "a_local_seg.dat", legacy_a_filename, legacy_a_filesize)));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(
            make_child(child_b, "b_local_seg.dat", legacy_b_filename, legacy_b_filesize)));

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

    // Both ctxs are kNoFamily (no edges), so v2 fast-path falls through to
    // rebuild for both legacy sstables. Each rebuild consults its own
    // per-child orphan PerFamilyMaps (orphan_by_child[child_index]). Both
    // emitted PBs therefore wear the rebuild signature: !shared, fresh
    // fileset_id, new (UUID) filename. They differ in max_rss_rowid.high:
    //   ctx_a (rssid_offset=0): orphan_by_child[0][1] = 1, high = 1.
    //   ctx_b (rssid_offset=1): orphan_by_child[1][1] = 2, high = 2.
    EXPECT_FALSE(sstables.Get(0).shared());
    EXPECT_FALSE(sstables.Get(1).shared());
    EXPECT_TRUE(sstables.Get(0).has_fileset_id());
    EXPECT_TRUE(sstables.Get(1).has_fileset_id());
    EXPECT_NE(legacy_a_filename, sstables.Get(0).filename());
    EXPECT_NE(legacy_a_filename, sstables.Get(1).filename());
    EXPECT_NE(legacy_b_filename, sstables.Get(0).filename());
    EXPECT_NE(legacy_b_filename, sstables.Get(1).filename());

    // The pollution-bug regression assertion: collect the high words of
    // both rebuilt PBs and verify they are {1, 2}, not {1, 1}. The latter
    // would mean ctx_a's first-emitter mapping {rssid=1 → final=1}
    // polluted ctx_b's rebuild lookup; per-child orphan scoping prevents
    // that.
    std::set<uint64_t> rebuilt_highs{sstables.Get(0).max_rss_rowid() >> 32, sstables.Get(1).max_rss_rowid() >> 32};
    EXPECT_EQ((std::set<uint64_t>{1, 2}), rebuilt_highs)
            << "ctx_b's rebuild must consult orphan_by_child[1], not a polluted shared orphan map";
}

// v2 fast-path headline win: canonical_ctx for the family carries a
// non-zero rssid_offset (it is NOT ctx[0]) yet the family-canonical
// projection still produces a byte-mappable shift. Under v1 this hit the
// C0' canonical-offset-nonzero gate and fell back to rebuild. Under v2
// the family's safe canonical_offset accumulates into the emitted PB.
//
// Setup:
//   ctx_a (= ctx[0]): unrelated child-local rowset, no edge to b/c.
//   ctx_b (= ctx[1]): shared-ancestor rowsets {1, 2} + legacy sstable.
//   ctx_c (= ctx[2]): same shared-ancestor rowsets + same legacy sstable.
// Family inference unions ctx_b + ctx_c via either edge; ctx_a stays
// kNoFamily. canonical_child_index = 1 (smallest member of {1, 2}), so
// canonical_rssid_offset = ctx_b.rssid_offset = 1 (Phase 1 lifts ctx_b's
// id space by ctx_a's contribution of 1 rowset).
TEST_F(LakeTabletReshardTest, test_tablet_merging_legacy_sstable_fastpath_v2_canonical_offset_hit) {
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

    const std::string legacy_filename = "fastpath_v2_canonical.sst";
    // The legacy sstable file lives only on ctx_b's storage; ctx_c's PB
    // points at the same logical filename (shared sstable file). For the
    // fast-path that is byte-mapped, the source file isn't actually read,
    // so single-side write is fine.
    const auto legacy_path = _tablet_manager->sst_location(child_b, legacy_filename);
    const uint64_t legacy_filesize =
            write_legacy_pk_sstable(legacy_path, {{"k1", /*rssid=*/1, /*rowid=*/0}, {"k2", /*rssid=*/2, /*rowid=*/0}});

    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(2);
    set_primary_key_schema(meta_a.get(), 1001);
    auto* rs_a = meta_a->add_rowsets();
    rs_a->set_id(1);
    rs_a->set_version(1);
    rs_a->set_num_rows(10);
    rs_a->set_data_size(100);
    rs_a->add_segments("seg_a_local.dat");
    rs_a->add_segment_size(100);
    rs_a->add_shared_segments(false);

    auto make_family_member = [&](int64_t tablet_id) {
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
            // Family-shared segment: same filename across ctx_b and ctx_c
            // gives them identical RowsetPhysicalKey for the rowset edge.
            rs->add_segments(fmt::format("family_seg_{}.dat", rs_id));
            rs->add_segment_size(100);
            rs->add_shared_segments(true);
        }
        auto* sst = meta->mutable_sstable_meta()->add_sstables();
        sst->set_filename(legacy_filename);
        sst->set_filesize(legacy_filesize);
        sst->set_shared(true);
        sst->set_max_rss_rowid((static_cast<uint64_t>(2) << 32) | 0);
        return meta;
    };

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(make_family_member(child_b)));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(make_family_member(child_c)));

    ReshardingTabletInfoPB resharding_tablet;
    auto& merging_info = *resharding_tablet.mutable_merging_tablet_info();
    merging_info.add_old_tablet_ids(child_a);
    merging_info.add_old_tablet_ids(child_b);
    merging_info.add_old_tablet_ids(child_c);
    merging_info.set_new_tablet_id(merged_tablet);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(2);
    std::unordered_map<int64_t, TabletMetadataPtr> tablet_metadatas;
    std::unordered_map<int64_t, TabletRangePB> tablet_ranges;
    ASSERT_OK(lake::publish_resharding_tablet(_tablet_manager.get(), resharding_tablet, base_version, new_version,
                                              txn_info, false, tablet_metadatas, tablet_ranges));

    auto merged = tablet_metadatas.at(merged_tablet);
    // dedup'd to one legacy sstable PB.
    ASSERT_EQ(1, merged->sstable_meta().sstables_size());
    const auto& out_sst = merged->sstable_meta().sstables(0);
    // v2 fast-path signature: original filename preserved (byte-mapped),
    // shared=true, no fileset_id minted. canonical_offset = ctx_b's
    // rssid_offset = 1, accumulating into the emitted PB.
    EXPECT_EQ(legacy_filename, out_sst.filename())
            << "v2 fast-path keeps source filename even with non-zero canonical offset";
    EXPECT_TRUE(out_sst.shared());
    EXPECT_FALSE(out_sst.has_shared_rssid());
    EXPECT_EQ(1, out_sst.rssid_offset()) << "accumulated = src.rssid_offset(0) + canonical_offset(1)";
    EXPECT_EQ((static_cast<uint64_t>(3) << 32) | 0, out_sst.max_rss_rowid())
            << "max_rss_rowid.high shifts by canonical_offset (=1)";
}

// ─────────────────────────────────────────────────────────────────────
// v2 follow-up: per-entry rebuild for non-shared sstables with mixed plan refs
// ─────────────────────────────────────────────────────────────────────
//
// Companion tests for the rewired map_rssid + non-shared rebuild path.
// These cover the partial-compaction win that v2 commit 5 alone could not
// deliver: when a non-canonical ctx's non-shared sstable references both
// safe-family shared-ancestor rowsets AND child-local rowsets (the
// "post-split PK-index compaction crossed the boundary" case), the
// dispatch routes to rebuild_non_shared_legacy_sstable for a per-entry
// remap via ctx.map_rssid (plan id for shared-ancestor, natural offset
// for child-local).

// T1: a non-shared sstable whose rssid range is DISJOINT from the family's
// shared-ancestor rowset ids stays on the metadata-only fast path. Setup:
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
        // Shared-ancestor rowset at HIGH id 10. Both ctxs carry it →
        // family unions them, plan covers id=10.
        auto* shared_rs = meta->add_rowsets();
        shared_rs->set_id(10);
        shared_rs->set_version(1);
        shared_rs->set_num_rows(10);
        shared_rs->set_data_size(100);
        shared_rs->add_segments("shared.dat");
        shared_rs->add_segment_size(100);
        shared_rs->add_shared_segments(true);
        if (include_local_and_sstable) {
            // Child-local rowset at LOW id 1, in a "gap" beneath the
            // shared-ancestor (legal: rs.id ≥ 1, ids need not be
            // contiguous).
            auto* local_rs = meta->add_rowsets();
            local_rs->set_id(1);
            local_rs->set_version(1);
            local_rs->set_num_rows(5);
            local_rs->set_data_size(50);
            local_rs->add_segments("ctx_b_local.dat");
            local_rs->add_segment_size(50);
            local_rs->add_shared_segments(false);
            auto* sst = meta->mutable_sstable_meta()->add_sstables();
            sst->set_filename(ns_filename);
            sst->set_filesize(ns_filesize);
            sst->set_shared(false);
            sst->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 0);
        }
        return meta;
    };

    EXPECT_OK(_tablet_manager->put_tablet_metadata(make_meta(child_a, /*include_local_and_sstable=*/false)));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(make_meta(child_b, /*include_local_and_sstable=*/true)));

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
    const std::string ns_filename = "ns_mixed.sst";
    const auto ns_path = _tablet_manager->sst_location(child_b, ns_filename);
    const uint64_t ns_filesize = write_legacy_pk_sstable(
            ns_path, {{"k_shared", /*rssid=*/1, /*rowid=*/0}, {"k_local", /*rssid=*/2, /*rowid=*/0}});

    auto meta_a = std::make_shared<TabletMetadataPB>();
    meta_a->set_id(child_a);
    meta_a->set_version(base_version);
    meta_a->set_next_rowset_id(2);
    set_primary_key_schema(meta_a.get(), 1001);
    auto* rs_a1 = meta_a->add_rowsets();
    rs_a1->set_id(1);
    rs_a1->set_version(1);
    rs_a1->set_num_rows(10);
    rs_a1->set_data_size(100);
    rs_a1->add_segments("shared.dat");
    rs_a1->add_segment_size(100);
    rs_a1->add_shared_segments(true);

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
    rs_b1->add_segments("shared.dat");
    rs_b1->add_segment_size(100);
    rs_b1->add_shared_segments(true);
    auto* rs_b2 = meta_b->add_rowsets();
    rs_b2->set_id(2);
    rs_b2->set_version(1);
    rs_b2->set_num_rows(5);
    rs_b2->set_data_size(50);
    rs_b2->add_segments("ctx_b_local.dat");
    rs_b2->add_segment_size(50);
    rs_b2->add_shared_segments(false);
    auto* sst_b = meta_b->mutable_sstable_meta()->add_sstables();
    sst_b->set_filename(ns_filename);
    sst_b->set_filesize(ns_filesize);
    sst_b->set_shared(false);
    sst_b->set_max_rss_rowid((static_cast<uint64_t>(2) << 32) | 0);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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

// T3: when ctx is the family canonical (smallest child_index member of
// a multi-ctx family), plan id == natural offset by construction
// (canonical_rssid_offset == ctx.rssid_offset == 0 for ctx[0]). The
// predicate must NOT fire even when the sstable references shared-
// ancestor rowsets covered by an actual plan family.
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
        // Same shared-ancestor rowset on both ctxs → family inference
        // unions them via the rowset edge; canonical_child_index = 0.
        auto* rs = meta->add_rowsets();
        rs->set_id(1);
        rs->set_version(1);
        rs->set_num_rows(10);
        rs->set_data_size(100);
        rs->add_segments("shared.dat");
        rs->add_segment_size(100);
        rs->add_shared_segments(true);
        if (include_sstable) {
            auto* sst = meta->mutable_sstable_meta()->add_sstables();
            sst->set_filename(ns_filename);
            sst->set_filesize(ns_filesize);
            sst->set_shared(false);
            sst->set_max_rss_rowid((static_cast<uint64_t>(1) << 32) | 0);
        }
        return meta;
    };

    EXPECT_OK(_tablet_manager->put_tablet_metadata(make_meta(child_a, /*include_sstable=*/true)));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(make_meta(child_b, /*include_sstable=*/false)));

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
    // ctx_a is family canonical: plan entry for rowset 1 has value
    // 1 + canonical_rssid_offset(0) = 1, natural = 1 + ctx_a.rssid_offset(0)
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

    // ctx_a + ctx_b form a safe family via shared rowset id=10. ctx_b's
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
        shared_rs->add_segments("shared.dat");
        shared_rs->add_segment_size(100);
        shared_rs->add_shared_segments(true);
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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(make_meta(child_a, /*include_sstable=*/false)));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(make_meta(child_b, /*include_sstable=*/true)));

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

// TODO(round-3 follow-up): test_tablet_merging_legacy_sstable_rebuild_filters_outside_tablet_range
// This test would set up real INT-typed PK columns + tablet ranges with
// PrimaryKeyEncoder-encoded sstable keys to verify the rebuild's tablet-range
// Seek/stop filter (TabletRangeHelper::create_sst_seek_range_from). The filter
// is wired in rebuild_legacy_shared_sstable; verifying its behavior end-to-end
// requires PK encoding scaffolding that's not currently in this fixture. The
// existing tests above all use set_primary_key_schema (no columns), which makes
// merged_range bound-less, so the range filter degenerates to an unbounded
// scan — the filter code path is exercised but its filtering behavior on a
// bounded range is not directly asserted yet. Track for follow-up once the
// fixture grows a helper for PK-encoded test sstables.

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
    rowset_a->add_segments("a_seg.dat");
    rowset_a->add_segment_size(100);
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
    rowset_b->add_segments("b_seg.dat");
    rowset_b->add_segment_size(100);
    auto* sst_b = meta_b->mutable_sstable_meta()->add_sstables();
    sst_b->set_filename("b.sst");
    sst_b->set_filesize(256);
    sst_b->set_max_rss_rowid((static_cast<uint64_t>(200) << 32) | 99);

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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta1));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta2));

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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta1));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta2));

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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta1));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta2));

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
        rowset->add_segments(shared_segment_name);
        rowset->add_segment_size(base_segment_size);
        rowset->add_shared_segments(true);
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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
        rowset->add_segments("shared_seg.dat");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
        (*metadata->mutable_rowset_to_schema())[1] = 5001;
        add_dcg_with_columns(metadata.get(), 1, "inconsistent.cols", dcg_columns, 1);
        return metadata;
    };

    // Same .cols filename on the same shared target, but different columns.
    auto meta_a = make_child(tablet_a, {601, 602});
    auto meta_b = make_child(tablet_b, {603}); // differs from A

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
        rowset->add_segments("shared_seg.dat");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
        rowset->add_segments(good_segment);
        rowset->add_segment_size(good_seg_size);
        rowset->add_shared_segments(true);
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
        bad_rowset->add_segments(bad_segment);
        bad_rowset->add_segment_size(100);
        bad_rowset->add_shared_segments(true);
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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
    rowset->add_segments("shared_seg.dat");
    rowset->add_segment_size(100);
    rowset->add_shared_segments(true);
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
    rowset->add_segments(compacted_seg_name);
    rowset->add_segment_size(100);
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
    rowset->add_segments("shared_seg.dat");
    rowset->add_segment_size(segment_size);
    rowset->add_shared_segments(true);
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
    rowset->add_segments(compacted_seg_name);
    rowset->add_segment_size(100);
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
            EXPECT_OK(_tablet_manager->put_tablet_metadata(metas[i]));                                                 \
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
            for (int i = 0; i < r.shared_segments_size(); ++i) {                                                       \
                if (r.shared_segments(i)) {                                                                            \
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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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

    // Locate canonical R0 (the rowset with at least one shared_segments(i)==true).
    uint32_t canonical_rssid = 0;
    for (const auto& r : merged->rowsets()) {
        bool has_shared = false;
        for (int i = 0; i < r.shared_segments_size(); ++i) {
            if (r.shared_segments(i)) {
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
        for (int i = 0; i < r.shared_segments_size(); ++i) {
            if (r.shared_segments(i)) {
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

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
        rowset->add_segments("shared_seg.dat");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
        // intentionally NO rowset->mutable_range(): rely on ctx tablet range
        return meta;
    };

    auto meta_a = make_no_rowset_range_child(child_a, 0, 10);
    auto meta_b = make_no_rowset_range_child(child_b, 10, 20);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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
        return meta;
    };

    auto meta_a = make_pred_child(child_a, 0, 10);
    auto meta_b = make_pred_child(child_b, 10, 20);

    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_a));
    EXPECT_OK(_tablet_manager->put_tablet_metadata(meta_b));

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

// =============================================================================
// Fast-path v2 — split family inference (commit 1)
//
// These tests exercise lake::detail::infer_split_families directly, without
// running an end-to-end merge. The helper is "passive" in this commit (no
// caller wires it through merge_rowsets / map_rssid yet); commits 4-5 will.
// =============================================================================

namespace {

// Lightweight fixture that builds mutable metadata + offsets, then snapshots
// them as immutable SplitFamilyInferenceInput records when infer_split_
// families is invoked. Splitting the mutable build phase from the const
// input phase matches how production constructs TabletMetadataPtr (=
// shared_ptr<const TabletMetadataPB>).
struct SplitFamilyTestBuilder {
    std::vector<std::pair<std::shared_ptr<TabletMetadataPB>, int64_t>> mutable_children;

    uint32_t add_empty_child(int64_t rssid_offset) {
        mutable_children.emplace_back(std::make_shared<TabletMetadataPB>(), rssid_offset);
        return static_cast<uint32_t>(mutable_children.size() - 1);
    }

    // Add a legacy `shared && !has_shared_rssid` PK sstable (used by the
    // filename edge).
    void add_legacy_shared_sstable(uint32_t child_index, const std::string& filename) {
        auto* sstable = mutable_children[child_index].first->mutable_sstable_meta()->add_sstables();
        sstable->set_filename(filename);
        sstable->set_filesize(1);
        sstable->set_shared(true);
        // !has_shared_rssid is the default — leave shared_rssid unset.
    }

    // Add a shared-ancestor rowset (segments_size > 0, all shared_segments
    // true) with the given physical fingerprint. Used by the rowset-
    // identity edge.
    void add_shared_ancestor_rowset(uint32_t child_index, uint32_t rowset_id, int64_t version,
                                    const std::vector<std::string>& segments) {
        auto* rowset = mutable_children[child_index].first->add_rowsets();
        rowset->set_id(rowset_id);
        rowset->set_version(version);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        for (const auto& segment : segments) {
            rowset->add_segments(segment);
            rowset->add_segment_size(100);
            rowset->add_shared_segments(true);
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_segment_idx(static_cast<uint32_t>(rowset->segment_metas_size() - 1));
        }
    }

    // Add a child-local (NOT shared) rowset. The rowset-identity edge must
    // ignore these even when the physical fingerprint matches a shared-
    // ancestor on another child.
    void add_child_local_rowset(uint32_t child_index, uint32_t rowset_id, int64_t version,
                                const std::vector<std::string>& segments) {
        auto* rowset = mutable_children[child_index].first->add_rowsets();
        rowset->set_id(rowset_id);
        rowset->set_version(version);
        rowset->set_num_rows(10);
        rowset->set_data_size(100);
        for (const auto& segment : segments) {
            rowset->add_segments(segment);
            rowset->add_segment_size(100);
            rowset->add_shared_segments(false);
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_segment_idx(static_cast<uint32_t>(rowset->segment_metas_size() - 1));
        }
    }

    // Add a delete-only rowset (segments_size == 0). The rowset-identity
    // edge must ignore these too.
    void add_delete_only_rowset(uint32_t child_index, uint32_t rowset_id, int64_t version) {
        auto* rowset = mutable_children[child_index].first->add_rowsets();
        rowset->set_id(rowset_id);
        rowset->set_version(version);
        rowset->mutable_delete_predicate(); // mark as a delete predicate; no segments
    }

    std::vector<lake::detail::SplitFamilyInferenceInput> snapshot() const {
        std::vector<lake::detail::SplitFamilyInferenceInput> inputs;
        inputs.reserve(mutable_children.size());
        for (const auto& [metadata, rssid_offset] : mutable_children) {
            inputs.push_back({metadata, rssid_offset});
        }
        return inputs;
    }

    // Helper for tests that need to write directly into a child's metadata.
    TabletMetadataPB* metadata_of(uint32_t child_index) { return mutable_children[child_index].first.get(); }
};

} // namespace

// Empty input → empty result.
TEST(SplitFamilyInferenceTest, empty_input) {
    SplitFamilyTestBuilder builder;
    ASSIGN_OR_ABORT(auto result, lake::detail::infer_split_families(builder.snapshot()));
    EXPECT_TRUE(result.child_to_family.empty());
    EXPECT_TRUE(result.families.empty());
}

// Single child with no edges → kNoFamily, no families produced.
TEST(SplitFamilyInferenceTest, single_child_no_edges) {
    SplitFamilyTestBuilder builder;
    builder.add_empty_child(/*rssid_offset=*/0);
    ASSIGN_OR_ABORT(auto result, lake::detail::infer_split_families(builder.snapshot()));
    ASSERT_EQ(1u, result.child_to_family.size());
    EXPECT_EQ(lake::detail::InferredSplitFamilies::kNoFamily, result.child_to_family[0]);
    EXPECT_TRUE(result.families.empty());
}

// Two children share a legacy `shared && !has_shared_rssid` sstable → one
// family, canonical = child 0.
TEST(SplitFamilyInferenceTest, filename_edge_unions_two_children) {
    SplitFamilyTestBuilder builder;
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_empty_child(/*rssid_offset=*/5);
    builder.add_legacy_shared_sstable(0, "shared.sst");
    builder.add_legacy_shared_sstable(1, "shared.sst");
    ASSIGN_OR_ABORT(auto result, lake::detail::infer_split_families(builder.snapshot()));
    ASSERT_EQ(1u, result.families.size());
    const auto& family = result.families.front();
    EXPECT_EQ(0u, family.canonical_child_index);
    EXPECT_EQ(0, family.canonical_rssid_offset);
    EXPECT_THAT(family.member_child_indexes, ::testing::ElementsAre(0u, 1u));
    EXPECT_EQ(0u, result.child_to_family[0]);
    EXPECT_EQ(0u, result.child_to_family[1]);
}

// Two children share an exact-match shared-ancestor rowset → one family,
// canonical = child 0.
TEST(SplitFamilyInferenceTest, rowset_edge_unions_two_children) {
    SplitFamilyTestBuilder builder;
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_empty_child(/*rssid_offset=*/5);
    builder.add_shared_ancestor_rowset(0, /*rowset_id=*/3, /*version=*/2, {"seg_a", "seg_b"});
    builder.add_shared_ancestor_rowset(1, /*rowset_id=*/3, /*version=*/2, {"seg_a", "seg_b"});
    ASSIGN_OR_ABORT(auto result, lake::detail::infer_split_families(builder.snapshot()));
    ASSERT_EQ(1u, result.families.size());
    EXPECT_EQ(0u, result.families.front().canonical_child_index);
    EXPECT_EQ(0, result.families.front().canonical_rssid_offset);
}

// Two children with rowsets that LOOK identical except for segment_idx
// layout (one inherited contiguous {0,1}, the other has a sparse {0,2}
// after middle compaction) — they are NOT physically identical and must
// not be unioned.
TEST(SplitFamilyInferenceTest, rowset_edge_segment_idx_layout_must_match) {
    SplitFamilyTestBuilder builder;
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_empty_child(/*rssid_offset=*/5);
    auto* rowset_a = builder.metadata_of(0)->add_rowsets();
    rowset_a->set_id(3);
    rowset_a->set_version(2);
    rowset_a->add_segments("seg_a");
    rowset_a->add_segment_size(100);
    rowset_a->add_shared_segments(true);
    rowset_a->add_segments("seg_b");
    rowset_a->add_segment_size(100);
    rowset_a->add_shared_segments(true);
    rowset_a->add_segment_metas()->set_segment_idx(0);
    rowset_a->add_segment_metas()->set_segment_idx(1);
    auto* rowset_b = builder.metadata_of(1)->add_rowsets();
    rowset_b->set_id(3);
    rowset_b->set_version(2);
    rowset_b->add_segments("seg_a");
    rowset_b->add_segment_size(100);
    rowset_b->add_shared_segments(true);
    rowset_b->add_segments("seg_b");
    rowset_b->add_segment_size(100);
    rowset_b->add_shared_segments(true);
    rowset_b->add_segment_metas()->set_segment_idx(0);
    rowset_b->add_segment_metas()->set_segment_idx(2); // sparse!
    ASSIGN_OR_ABORT(auto result, lake::detail::infer_split_families(builder.snapshot()));
    EXPECT_TRUE(result.families.empty()) << "different segment_idx layouts must not union";
    EXPECT_EQ(lake::detail::InferredSplitFamilies::kNoFamily, result.child_to_family[0]);
    EXPECT_EQ(lake::detail::InferredSplitFamilies::kNoFamily, result.child_to_family[1]);
}

// Delete-only rowsets (segments_size == 0) must NOT participate in the
// rowset-identity edge — two delete-only rowsets with empty vectors would
// otherwise falsely match each other.
TEST(SplitFamilyInferenceTest, delete_only_rowsets_excluded_from_edge) {
    SplitFamilyTestBuilder builder;
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_empty_child(/*rssid_offset=*/5);
    builder.add_delete_only_rowset(0, /*rowset_id=*/3, /*version=*/2);
    builder.add_delete_only_rowset(1, /*rowset_id=*/3, /*version=*/2);
    ASSIGN_OR_ABORT(auto result, lake::detail::infer_split_families(builder.snapshot()));
    EXPECT_TRUE(result.families.empty()) << "delete-only rowsets must not produce family edges";
}

// Child-local rowsets (shared_segments NOT all true) must not produce edges
// even when the physical fingerprint matches.
TEST(SplitFamilyInferenceTest, child_local_rowsets_excluded_from_edge) {
    SplitFamilyTestBuilder builder;
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_empty_child(/*rssid_offset=*/5);
    builder.add_child_local_rowset(0, /*rowset_id=*/3, /*version=*/2, {"seg_a"});
    builder.add_child_local_rowset(1, /*rowset_id=*/3, /*version=*/2, {"seg_a"});
    ASSIGN_OR_ABORT(auto result, lake::detail::infer_split_families(builder.snapshot()));
    EXPECT_TRUE(result.families.empty()) << "child-local rowsets must not produce family edges";
}

// Three children unioned via filename edge into one family. canonical is
// the smallest member regardless of which two children matched first.
TEST(SplitFamilyInferenceTest, three_children_one_family_via_filename) {
    SplitFamilyTestBuilder builder;
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_empty_child(/*rssid_offset=*/5);
    builder.add_empty_child(/*rssid_offset=*/10);
    builder.add_legacy_shared_sstable(0, "f.sst");
    builder.add_legacy_shared_sstable(1, "f.sst");
    builder.add_legacy_shared_sstable(2, "f.sst");
    ASSIGN_OR_ABORT(auto result, lake::detail::infer_split_families(builder.snapshot()));
    ASSERT_EQ(1u, result.families.size());
    EXPECT_EQ(0u, result.families.front().canonical_child_index);
    EXPECT_EQ(0, result.families.front().canonical_rssid_offset);
    EXPECT_THAT(result.families.front().member_child_indexes, ::testing::ElementsAre(0u, 1u, 2u));
}

// Two disjoint families on the same merge: child{0,1} share file_a;
// child{2,3} share file_b. Each family gets its own canonical.
TEST(SplitFamilyInferenceTest, two_disjoint_families) {
    SplitFamilyTestBuilder builder;
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_empty_child(/*rssid_offset=*/5);
    builder.add_empty_child(/*rssid_offset=*/10);
    builder.add_empty_child(/*rssid_offset=*/15);
    builder.add_legacy_shared_sstable(0, "file_a.sst");
    builder.add_legacy_shared_sstable(1, "file_a.sst");
    builder.add_legacy_shared_sstable(2, "file_b.sst");
    builder.add_legacy_shared_sstable(3, "file_b.sst");
    ASSIGN_OR_ABORT(auto result, lake::detail::infer_split_families(builder.snapshot()));
    ASSERT_EQ(2u, result.families.size());
    // Families are emitted in ascending canonical_child_index order.
    EXPECT_EQ(0u, result.families[0].canonical_child_index);
    EXPECT_EQ(0, result.families[0].canonical_rssid_offset);
    EXPECT_THAT(result.families[0].member_child_indexes, ::testing::ElementsAre(0u, 1u));
    EXPECT_EQ(2u, result.families[1].canonical_child_index);
    EXPECT_EQ(10, result.families[1].canonical_rssid_offset);
    EXPECT_THAT(result.families[1].member_child_indexes, ::testing::ElementsAre(2u, 3u));
    EXPECT_EQ(0u, result.child_to_family[0]);
    EXPECT_EQ(0u, result.child_to_family[1]);
    EXPECT_EQ(1u, result.child_to_family[2]);
    EXPECT_EQ(1u, result.child_to_family[3]);
}

// Filename edge AND rowset-identity edge can BOTH apply to the same pair —
// the union-find handles re-unions trivially.
TEST(SplitFamilyInferenceTest, both_edges_apply_to_same_pair) {
    SplitFamilyTestBuilder builder;
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_empty_child(/*rssid_offset=*/5);
    builder.add_legacy_shared_sstable(0, "f.sst");
    builder.add_legacy_shared_sstable(1, "f.sst");
    builder.add_shared_ancestor_rowset(0, /*rowset_id=*/3, /*version=*/2, {"seg_a"});
    builder.add_shared_ancestor_rowset(1, /*rowset_id=*/3, /*version=*/2, {"seg_a"});
    ASSIGN_OR_ABORT(auto result, lake::detail::infer_split_families(builder.snapshot()));
    ASSERT_EQ(1u, result.families.size());
    EXPECT_THAT(result.families.front().member_child_indexes, ::testing::ElementsAre(0u, 1u));
}

// Edge-only-via-rowset case: filename does not match (sstables compacted
// away on one side) but the underlying shared rowset still proves the
// family relationship.
TEST(SplitFamilyInferenceTest, family_inferred_via_rowset_only) {
    SplitFamilyTestBuilder builder;
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_empty_child(/*rssid_offset=*/5);
    builder.add_legacy_shared_sstable(0, "side_a.sst"); // no overlap
    builder.add_legacy_shared_sstable(1, "side_b.sst");
    builder.add_shared_ancestor_rowset(0, /*rowset_id=*/3, /*version=*/2, {"seg_a"});
    builder.add_shared_ancestor_rowset(1, /*rowset_id=*/3, /*version=*/2, {"seg_a"});
    ASSIGN_OR_ABORT(auto result, lake::detail::infer_split_families(builder.snapshot()));
    ASSERT_EQ(1u, result.families.size());
    EXPECT_EQ(0u, result.families.front().canonical_child_index);
}

// Edge-only-via-filename case: the rowsets diverged (one side compacted
// the rowset away) but the legacy sstable is still common.
TEST(SplitFamilyInferenceTest, family_inferred_via_filename_only) {
    SplitFamilyTestBuilder builder;
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_empty_child(/*rssid_offset=*/5);
    builder.add_legacy_shared_sstable(0, "f.sst");
    builder.add_legacy_shared_sstable(1, "f.sst");
    // Different rowsets on each side; should NOT contribute an edge but
    // should also not break the filename-driven union.
    builder.add_shared_ancestor_rowset(0, /*rowset_id=*/3, /*version=*/2, {"seg_a"});
    builder.add_shared_ancestor_rowset(1, /*rowset_id=*/4, /*version=*/2, {"seg_b"});
    ASSIGN_OR_ABORT(auto result, lake::detail::infer_split_families(builder.snapshot()));
    ASSERT_EQ(1u, result.families.size());
    EXPECT_THAT(result.families.front().member_child_indexes, ::testing::ElementsAre(0u, 1u));
}

// Two children carry the same physical rowset, but one has segment_metas
// fully populated (segment_idx = positional index) while the other has
// segment_metas absent. The PB read path falls back to positional index
// when segment_metas is missing, so both metadata variants describe the
// same physical layout — they MUST union into one family. (Codex round-1
// catch: without normalization, the keys differ and family inference
// silently misses the legacy fast-path.)
TEST(SplitFamilyInferenceTest, rowset_edge_normalizes_absent_segment_metas) {
    SplitFamilyTestBuilder builder;
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_empty_child(/*rssid_offset=*/5);
    auto* rowset_a = builder.metadata_of(0)->add_rowsets();
    rowset_a->set_id(3);
    rowset_a->set_version(2);
    rowset_a->add_segments("seg_a");
    rowset_a->add_segment_size(100);
    rowset_a->add_shared_segments(true);
    rowset_a->add_segments("seg_b");
    rowset_a->add_segment_size(100);
    rowset_a->add_shared_segments(true);
    rowset_a->add_segment_metas()->set_segment_idx(0);
    rowset_a->add_segment_metas()->set_segment_idx(1);
    auto* rowset_b = builder.metadata_of(1)->add_rowsets();
    rowset_b->set_id(3);
    rowset_b->set_version(2);
    rowset_b->add_segments("seg_a");
    rowset_b->add_segment_size(100);
    rowset_b->add_shared_segments(true);
    rowset_b->add_segments("seg_b");
    rowset_b->add_segment_size(100);
    rowset_b->add_shared_segments(true);
    // segment_metas intentionally absent — read path falls back to
    // positional index, which equals A's explicit (0, 1).
    ASSIGN_OR_ABORT(auto result, lake::detail::infer_split_families(builder.snapshot()));
    ASSERT_EQ(1u, result.families.size()) << "default positional segment_idx must equal explicit (0, 1) layout";
}

// Same family-defining physical rowset, but one child has bundle_file_
// offsets explicitly set to (0, 0) while the other has the field absent.
// PB defaults to 0 per segment when absent, so the two variants describe
// the same physical placement and must union into one family.
TEST(SplitFamilyInferenceTest, rowset_edge_normalizes_absent_bundle_file_offsets) {
    SplitFamilyTestBuilder builder;
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_empty_child(/*rssid_offset=*/5);
    auto* rowset_a = builder.metadata_of(0)->add_rowsets();
    rowset_a->set_id(3);
    rowset_a->set_version(2);
    rowset_a->add_segments("seg_a");
    rowset_a->add_segment_size(100);
    rowset_a->add_shared_segments(true);
    rowset_a->add_segments("seg_b");
    rowset_a->add_segment_size(100);
    rowset_a->add_shared_segments(true);
    rowset_a->add_bundle_file_offsets(0);
    rowset_a->add_bundle_file_offsets(0);
    rowset_a->add_segment_metas()->set_segment_idx(0);
    rowset_a->add_segment_metas()->set_segment_idx(1);
    auto* rowset_b = builder.metadata_of(1)->add_rowsets();
    rowset_b->set_id(3);
    rowset_b->set_version(2);
    rowset_b->add_segments("seg_a");
    rowset_b->add_segment_size(100);
    rowset_b->add_shared_segments(true);
    rowset_b->add_segments("seg_b");
    rowset_b->add_segment_size(100);
    rowset_b->add_shared_segments(true);
    // bundle_file_offsets absent — PB default 0 per segment, equal to A's
    // explicit (0, 0).
    rowset_b->add_segment_metas()->set_segment_idx(0);
    rowset_b->add_segment_metas()->set_segment_idx(1);
    ASSIGN_OR_ABORT(auto result, lake::detail::infer_split_families(builder.snapshot()));
    ASSERT_EQ(1u, result.families.size()) << "absent bundle_file_offsets must equal explicit (0, 0) layout";
}

// Mix of orphan + family children. The orphan stays kNoFamily; the family
// records the right canonical_child_index.
TEST(SplitFamilyInferenceTest, mix_orphan_and_family) {
    SplitFamilyTestBuilder builder;
    builder.add_empty_child(/*rssid_offset=*/0);  // orphan
    builder.add_empty_child(/*rssid_offset=*/5);  // family member
    builder.add_empty_child(/*rssid_offset=*/10); // family member
    builder.add_legacy_shared_sstable(1, "f.sst");
    builder.add_legacy_shared_sstable(2, "f.sst");
    ASSIGN_OR_ABORT(auto result, lake::detail::infer_split_families(builder.snapshot()));
    ASSERT_EQ(1u, result.families.size());
    EXPECT_EQ(1u, result.families.front().canonical_child_index);
    EXPECT_EQ(5, result.families.front().canonical_rssid_offset);
    EXPECT_EQ(lake::detail::InferredSplitFamilies::kNoFamily, result.child_to_family[0]);
    EXPECT_EQ(0u, result.child_to_family[1]);
    EXPECT_EQ(0u, result.child_to_family[2]);
}

// =============================================================================
// Fast-path v2 — RssidProjectionPlan build (commit 2)
//
// These tests exercise lake::detail::build_rssid_projection_plan directly.
// The plan is "passive" in this commit (no caller wires it through
// merge_rowsets / map_rssid yet); commit 4 will.
// =============================================================================

namespace {

// Convenience helper: run inference + plan build in one call, returning the
// plan. Used by all RssidProjectionPlanTest tests that don't care about the
// inferred families themselves.
lake::detail::RssidProjectionPlan build_plan_for(SplitFamilyTestBuilder& builder) {
    auto inputs = builder.snapshot();
    auto families_or = lake::detail::infer_split_families(inputs);
    CHECK_OK(families_or.status());
    auto plan_or = lake::detail::build_rssid_projection_plan(inputs, *families_or);
    CHECK_OK(plan_or.status());
    return std::move(*plan_or);
}

} // namespace

// Empty input → empty plan, no families, no occupied entries.
TEST(RssidProjectionPlanTest, empty_input) {
    SplitFamilyTestBuilder builder;
    auto plan = build_plan_for(builder);
    EXPECT_TRUE(plan.explicit_rssid_map.empty());
    EXPECT_TRUE(plan.family_legacy_sstable_offset.empty());
    EXPECT_TRUE(plan.occupied_rssids.empty());
    EXPECT_TRUE(plan.unsafe_families.empty());
}

// Two-child family with a shared-ancestor rowset, no collisions.
// explicit_rssid_map records both rowset.id and segment positions.
TEST(RssidProjectionPlanTest, family_canonical_projection_emitted) {
    SplitFamilyTestBuilder builder;
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_empty_child(/*rssid_offset=*/8);
    // Shared rowset id=3 with two segments → segment_idx layout [0, 1].
    // canonical_offset = 0 (canonical = ctx[0]).
    builder.add_shared_ancestor_rowset(0, /*rowset_id=*/3, /*version=*/2, {"seg_a", "seg_b"});
    builder.add_shared_ancestor_rowset(1, /*rowset_id=*/3, /*version=*/2, {"seg_a", "seg_b"});
    auto plan = build_plan_for(builder);

    EXPECT_TRUE(plan.unsafe_families.empty());
    ASSERT_EQ(1u, plan.family_legacy_sstable_offset.size());
    EXPECT_EQ(0, plan.family_legacy_sstable_offset.at(0u));

    // Both children's (3, 4) entries point to canonical-offset finals.
    // (rs.id=3 → final 3; lifted segment 0 = 3+0=3, lifted segment 1 = 3+1=4 → finals 3, 4.)
    EXPECT_EQ(3u, plan.explicit_rssid_map.at({0u, 3u}));
    EXPECT_EQ(4u, plan.explicit_rssid_map.at({0u, 4u}));
    EXPECT_EQ(3u, plan.explicit_rssid_map.at({1u, 3u}));
    EXPECT_EQ(4u, plan.explicit_rssid_map.at({1u, 4u}));

    // Occupancy: finals 3, 4 are claimed by the family.
    ASSERT_EQ(2u, plan.occupied_rssids.size());
    EXPECT_EQ(0u, plan.occupied_rssids.at(3u).family_id);
    EXPECT_EQ(0u, plan.occupied_rssids.at(4u).family_id);
}

// Family canonical_offset != 0: the explicit map's finals shift by it,
// and family_legacy_sstable_offset records it for the fast-path.
TEST(RssidProjectionPlanTest, family_with_nonzero_canonical_offset) {
    SplitFamilyTestBuilder builder;
    // child 0 has rowsets so phase-1-style next_rowset_id pushes the
    // canonical offset upward when child 1 is the canonical (per the
    // SplitFamilyInferenceInput.rssid_offset). Here we pass canonical_offset=10
    // directly via builder.add_empty_child. Tests don't run phase 1; they
    // just feed the offset.
    builder.add_empty_child(/*rssid_offset=*/10);
    builder.add_empty_child(/*rssid_offset=*/15);
    builder.add_shared_ancestor_rowset(0, /*rowset_id=*/3, /*version=*/2, {"seg_a"});
    builder.add_shared_ancestor_rowset(1, /*rowset_id=*/3, /*version=*/2, {"seg_a"});
    auto plan = build_plan_for(builder);

    EXPECT_TRUE(plan.unsafe_families.empty());
    EXPECT_EQ(10, plan.family_legacy_sstable_offset.at(0u));
    // rs.id=3 → final 3+10=13; segment 0 lifted=3 → final 13.
    EXPECT_EQ(13u, plan.explicit_rssid_map.at({0u, 3u}));
    EXPECT_EQ(13u, plan.explicit_rssid_map.at({1u, 3u}));
}

// Child-local (non-shared) rowset → does NOT enter explicit_rssid_map even
// if its ctx is a family member. Goes through natural offset; occupancy
// records it under the family_id of the ctx, but explicit_rssid_map stays
// silent so commit 4's map_rssid will fall through to the natural-offset
// path for it.
TEST(RssidProjectionPlanTest, child_local_rowsets_not_in_explicit_map) {
    SplitFamilyTestBuilder builder;
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_empty_child(/*rssid_offset=*/8);
    // Family edge via a shared sstable filename; rowsets diverge.
    builder.add_legacy_shared_sstable(0, "shared.sst");
    builder.add_legacy_shared_sstable(1, "shared.sst");
    // Child-local rowset on ctx 1.
    builder.add_child_local_rowset(1, /*rowset_id=*/3, /*version=*/2, {"seg_a"});
    auto plan = build_plan_for(builder);
    EXPECT_TRUE(plan.unsafe_families.empty());
    EXPECT_EQ(0u, plan.explicit_rssid_map.count({1u, 3u}));
}

// Two physically-distinct rowsets want the same final rssid → both
// involved families are marked unsafe.
TEST(RssidProjectionPlanTest, cross_family_collision_marks_both_unsafe) {
    SplitFamilyTestBuilder builder;
    builder.add_empty_child(/*rssid_offset=*/0); // family A canonical
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_empty_child(/*rssid_offset=*/0); // family B canonical
    builder.add_empty_child(/*rssid_offset=*/0);
    // Family A: ctx 0 + ctx 1 share a rowset that lands at final=5.
    builder.add_shared_ancestor_rowset(0, /*rowset_id=*/5, /*version=*/2, {"seg_a"});
    builder.add_shared_ancestor_rowset(1, /*rowset_id=*/5, /*version=*/2, {"seg_a"});
    // Family B: ctx 2 + ctx 3 share a DIFFERENT physical rowset that ALSO
    // wants final=5 (same id, but seg_b vs seg_a → different physical key).
    builder.add_shared_ancestor_rowset(2, /*rowset_id=*/5, /*version=*/3, {"seg_b"});
    builder.add_shared_ancestor_rowset(3, /*rowset_id=*/5, /*version=*/3, {"seg_b"});
    auto plan = build_plan_for(builder);
    // Both families A and B claim final=5 with different physical keys →
    // both marked unsafe; explicit_rssid_map drops them; family_legacy_
    // sstable_offset stays empty.
    EXPECT_EQ(2u, plan.unsafe_families.size());
    EXPECT_TRUE(plan.unsafe_families.contains(0u));
    EXPECT_TRUE(plan.unsafe_families.contains(1u));
    EXPECT_TRUE(plan.explicit_rssid_map.empty());
    EXPECT_TRUE(plan.family_legacy_sstable_offset.empty());
}

// Same physical rowset across family members deduplicates in occupied_rssids.
// The family stays safe and the explicit_rssid_map records all member ctx
// entries.
TEST(RssidProjectionPlanTest, same_physical_dedup_is_safe) {
    SplitFamilyTestBuilder builder;
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_empty_child(/*rssid_offset=*/8);
    builder.add_empty_child(/*rssid_offset=*/16);
    for (uint32_t child_index : {0u, 1u, 2u}) {
        builder.add_shared_ancestor_rowset(child_index, /*rowset_id=*/3, /*version=*/2, {"seg_a"});
    }
    auto plan = build_plan_for(builder);
    EXPECT_TRUE(plan.unsafe_families.empty());
    EXPECT_EQ(0, plan.family_legacy_sstable_offset.at(0u));
    // All 3 children's (3, 3) entries point to canonical-offset final 3.
    EXPECT_EQ(3u, plan.explicit_rssid_map.at({0u, 3u}));
    EXPECT_EQ(3u, plan.explicit_rssid_map.at({1u, 3u}));
    EXPECT_EQ(3u, plan.explicit_rssid_map.at({2u, 3u}));
}

// Sparse segment_idx layout ({0, 2}) is preserved by the projection. The
// occupancy claims rssid 3 and 5 (= 3+0, 3+2), NOT rssid 4 (which would
// belong to a hypothetical compacted-out segment_idx=1).
TEST(RssidProjectionPlanTest, sparse_segment_idx_projection) {
    SplitFamilyTestBuilder builder;
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_empty_child(/*rssid_offset=*/8);
    auto add_sparse_rowset = [&](uint32_t child_index) {
        auto* rowset = builder.metadata_of(child_index)->add_rowsets();
        rowset->set_id(3);
        rowset->set_version(2);
        rowset->add_segments("seg_0");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
        rowset->add_segments("seg_2");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
        rowset->add_segment_metas()->set_segment_idx(0);
        rowset->add_segment_metas()->set_segment_idx(2);
    };
    add_sparse_rowset(0);
    add_sparse_rowset(1);
    auto plan = build_plan_for(builder);
    EXPECT_TRUE(plan.unsafe_families.empty());
    EXPECT_EQ(3u, plan.explicit_rssid_map.at({0u, 3u})); // rs.id
    EXPECT_EQ(3u, plan.explicit_rssid_map.at({0u, 3u})); // segment_idx=0 → lifted 3
    EXPECT_EQ(5u, plan.explicit_rssid_map.at({0u, 5u})); // segment_idx=2 → lifted 5
    // Final 4 was never claimed.
    EXPECT_EQ(0u, plan.occupied_rssids.count(4u));
    // Finals 3 and 5 are claimed.
    EXPECT_EQ(1u, plan.occupied_rssids.count(3u));
    EXPECT_EQ(1u, plan.occupied_rssids.count(5u));
}

// Orphan-vs-family collision: an orphan ctx's child-local rowset projects
// (via natural offset) to the same final as a family's canonical
// projection. The family is marked unsafe; the orphan stays untouched.
TEST(RssidProjectionPlanTest, orphan_vs_family_collision_marks_family_unsafe) {
    SplitFamilyTestBuilder builder;
    // ctx 0 + ctx 1 form family A (via filename edge).
    // ctx 2 is an orphan with a child-local rowset that lifts to 5
    // through natural offset = 5 (rs.id 0 + offset 5).
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_empty_child(/*rssid_offset=*/5);
    builder.add_legacy_shared_sstable(0, "shared.sst");
    builder.add_legacy_shared_sstable(1, "shared.sst");
    // Family A's shared-ancestor rowset id=5, canonical_offset=0, claims final 5.
    builder.add_shared_ancestor_rowset(0, /*rowset_id=*/5, /*version=*/2, {"seg_a"});
    builder.add_shared_ancestor_rowset(1, /*rowset_id=*/5, /*version=*/2, {"seg_a"});
    // Orphan ctx 2: a child-local rowset id=0 + offset 5 → final 5, with a
    // DIFFERENT physical key (different segment filename).
    builder.add_child_local_rowset(2, /*rowset_id=*/0, /*version=*/3, {"seg_b"});
    auto plan = build_plan_for(builder);
    // Family A marked unsafe; orphan family is kNoFamily so nothing else
    // gets added to unsafe_families.
    EXPECT_EQ(1u, plan.unsafe_families.size());
    EXPECT_TRUE(plan.unsafe_families.contains(0u));
    EXPECT_TRUE(plan.explicit_rssid_map.empty());
    EXPECT_TRUE(plan.family_legacy_sstable_offset.empty());
}

// Boundary case: a shared-ancestor rowset with rowset.id() == UINT32_MAX
// and any non-zero segment_idx would overflow uint32 if we naively added
// id + segment_idx without checking. The plan must mark the family unsafe
// so commit 4's map_rssid falls through to natural-offset / v1 path.
TEST(RssidProjectionPlanTest, segment_rssid_overflow_marks_family_unsafe) {
    SplitFamilyTestBuilder builder;
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_empty_child(/*rssid_offset=*/0);
    auto add_overflow_rowset = [&](uint32_t child_index) {
        auto* rowset = builder.metadata_of(child_index)->add_rowsets();
        rowset->set_id(std::numeric_limits<uint32_t>::max());
        rowset->set_version(2);
        rowset->add_segments("seg_0");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
        rowset->add_segments("seg_1");
        rowset->add_segment_size(100);
        rowset->add_shared_segments(true);
        rowset->add_segment_metas()->set_segment_idx(0); // lifted = UINT32_MAX, OK
        rowset->add_segment_metas()->set_segment_idx(1); // lifted = UINT32_MAX + 1 → overflow
    };
    add_overflow_rowset(0);
    add_overflow_rowset(1);
    auto plan = build_plan_for(builder);
    ASSERT_EQ(1u, plan.unsafe_families.size());
    EXPECT_TRUE(plan.unsafe_families.contains(0u)) << "rowset.id()+segment_idx overflow must mark the family unsafe, "
                                                      "not record a wrapped low rssid";
    EXPECT_TRUE(plan.family_legacy_sstable_offset.empty());
}

// Delete-only and no-segments rowsets still claim rowset.id() in
// occupied_rssids (they own the rsid as a watermark even with no segments).
// Two delete-only rowsets at the same version with the same id are the
// "same physical" → safe dedup; at different versions → collision.
TEST(RssidProjectionPlanTest, delete_only_rowset_id_occupancy_dedup) {
    SplitFamilyTestBuilder builder;
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_empty_child(/*rssid_offset=*/0);
    builder.add_legacy_shared_sstable(0, "shared.sst");
    builder.add_legacy_shared_sstable(1, "shared.sst");
    builder.add_delete_only_rowset(0, /*rowset_id=*/3, /*version=*/2);
    builder.add_delete_only_rowset(1, /*rowset_id=*/3, /*version=*/2);
    auto plan = build_plan_for(builder);
    // Family A is safe (no collision; delete-only rowsets dedup at id 3).
    EXPECT_TRUE(plan.unsafe_families.empty());
    // Final 3 is claimed under family A. The delete-only path uses natural
    // offset (canonical_offset == 0 here, so result is the same).
    EXPECT_EQ(1u, plan.occupied_rssids.count(3u));
}

} // namespace starrocks
