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

#include "storage/lake/meta_file.h"

#include <gtest/gtest.h>

#include <ctime>
#include <set>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/uid_util.h"
#include "common/config.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "storage/del_vector.h"
#include "storage/lake/column_mode_partial_update_handler.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/update_manager.h"
#include "storage/storage_metrics.h"

namespace starrocks::lake {

class TestLocationProvider : public LocationProvider {
public:
    explicit TestLocationProvider(LocationProvider* lp) : _lp(lp) {}

    std::string root_location(int64_t tablet_id) const override {
        if (_owned_shards.count(tablet_id) > 0) {
            return _lp->root_location(tablet_id);
        } else {
            return "/path/to/nonexist/directory/";
        }
    }

    std::set<int64_t> _owned_shards;
    LocationProvider* _lp;
};

class MetaFileTest : public ::testing::Test {
public:
    void SetUp() {
        CHECK_OK(fs::create_directories(join_path(kTestDir, kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(join_path(kTestDir, kTxnLogDirectoryName)));
        CHECK_OK(fs::create_directories(join_path(kTestDir, kSegmentDirectoryName)));

        _location_provider = std::make_unique<FixedLocationProvider>(kTestDir);
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_manager = std::make_unique<lake::TabletManager>(_location_provider, _update_manager.get(), 1638400000);
    }

    void TearDown() { (void)FileSystem::Default()->delete_dir_recursive(kTestDir); }

protected:
    void ensure_kek_in_key_cache() {
        if (KeyCache::instance().get_key("0000000000000000") != nullptr) {
            return;
        }
        EncryptionKeyPB pb;
        pb.set_id(EncryptionKey::DEFAULT_MASTER_KYE_ID);
        pb.set_type(EncryptionKeyTypePB::NORMAL_KEY);
        pb.set_algorithm(EncryptionAlgorithmPB::AES_128);
        pb.set_plain_key("0000000000000000");
        std::unique_ptr<EncryptionKey> root_encryption_key = EncryptionKey::create_from_pb(pb).value();
        auto kek = root_encryption_key->generate_key().value();
        kek->set_id(2);
        KeyCache::instance().add_key(root_encryption_key);
        KeyCache::instance().add_key(kek);
    }

    constexpr static const char* const kTestDir = "./lake_meta_test";
    std::shared_ptr<lake::LocationProvider> _location_provider;
    std::unique_ptr<TabletManager> _tablet_manager;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<UpdateManager> _update_manager;
};

TEST_F(MetaFileTest, test_meta_rw) {
    // 1. generate metadata
    const int64_t tablet_id = 10001;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);

    // 2. write to pk meta file
    MetaFileBuilder builder(*tablet, metadata);
    Status st = builder.finalize(next_id());
    EXPECT_TRUE(st.ok());

    // 3. read meta from meta file
    ASSIGN_OR_ABORT(auto metadata2, _tablet_manager->get_tablet_metadata(tablet_id, 10));
}

TEST_F(MetaFileTest, test_merge_delvec_files_empty) {
    std::vector<DelvecFileInfo> old_delvec_files;
    FileMetaPB new_delvec_file;
    std::vector<uint64_t> offsets;

    EXPECT_OK(merge_delvec_files(_tablet_manager.get(), old_delvec_files, 1, 1, &new_delvec_file, &offsets));
    EXPECT_TRUE(offsets.empty());
}

TEST_F(MetaFileTest, test_merge_delvec_files_encrypted) {
    ensure_kek_in_key_cache();

    const int64_t tablet_id = 2001;
    const int64_t new_tablet_id = 2002;
    const int64_t txn_id = 5;

    ASSIGN_OR_ABORT(auto pair, KeyCache::instance().create_plain_random_encryption_meta_pair());
    const std::string content = "encrypted-delvec";
    const std::string file_name = "delvec-encrypted";

    const std::string file_path = _tablet_manager->delvec_location(tablet_id, file_name);
    WritableFileOptions wopts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    wopts.encryption_info = pair.info;
    ASSIGN_OR_ABORT(auto writer, fs::new_writable_file(wopts, file_path));
    ASSERT_OK(writer->append(Slice(content)));
    ASSERT_OK(writer->close());

    DelvecFileInfo file_info;
    file_info.tablet_id = tablet_id;
    file_info.delvec_file.set_name(file_name);
    file_info.delvec_file.set_size(content.size());
    file_info.delvec_file.set_encryption_meta(pair.encryption_meta);

    std::vector<DelvecFileInfo> old_delvec_files{file_info};
    FileMetaPB new_delvec_file;
    std::vector<uint64_t> offsets;

    EXPECT_OK(merge_delvec_files(_tablet_manager.get(), old_delvec_files, new_tablet_id, txn_id, &new_delvec_file,
                                 &offsets));
    ASSERT_EQ(1, offsets.size());
    EXPECT_EQ(0, offsets[0]);
    EXPECT_FALSE(new_delvec_file.name().empty());
    EXPECT_EQ(static_cast<int64_t>(content.size()), new_delvec_file.size());
    EXPECT_FALSE(new_delvec_file.encryption_meta().empty());
    EXPECT_FALSE(new_delvec_file.shared());
}

TEST_F(MetaFileTest, test_delvec_rw) {
    // 1. generate metadata
    const int64_t tablet_id = 10002;
    const uint32_t segment_id = 1234;
    const int64_t version = 11;
    const int64_t version2 = 12;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    metadata->set_next_rowset_id(110);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    // 2. write pk meta & delvec
    MetaFileBuilder builder(*tablet, metadata);
    DelVector dv;
    dv.set_empty();
    EXPECT_TRUE(dv.empty());

    std::shared_ptr<DelVector> ndv;
    std::vector<uint32_t> dels = {1, 3, 5, 7, 90000};
    dv.add_dels_as_new_version(dels, version, &ndv);
    EXPECT_FALSE(ndv->empty());
    std::string before_delvec = ndv->save();
    builder.append_delvec(ndv, segment_id);
    Status st = builder.finalize(next_id());
    EXPECT_TRUE(st.ok());

    // 3. read delvec
    DelVector after_delvec;
    LakeIOOptions lake_io_opts;
    ASSIGN_OR_ABORT(auto metadata2, _tablet_manager->get_tablet_metadata(tablet_id, version));
    EXPECT_TRUE(get_del_vec(_tablet_manager.get(), *metadata2, segment_id, true, lake_io_opts, &after_delvec).ok());
    EXPECT_EQ(before_delvec, after_delvec.save());

    // 4. read meta
    auto iter = metadata2->delvec_meta().delvecs().find(segment_id);
    EXPECT_TRUE(iter != metadata2->delvec_meta().delvecs().end());
    auto delvec_pagepb = iter->second;
    EXPECT_EQ(delvec_pagepb.version(), version);

    // 5. update delvec
    metadata->set_version(version2);
    MetaFileBuilder builder2(*tablet, metadata);
    DelVector dv2;
    dv2.set_empty();
    EXPECT_TRUE(dv2.empty());
    std::shared_ptr<DelVector> ndv2;
    std::vector<uint32_t> dels2 = {1, 3, 5, 9, 90000};
    dv2.add_dels_as_new_version(dels2, version2, &ndv2);
    builder2.append_delvec(ndv2, segment_id);
    st = builder2.finalize(next_id());
    EXPECT_TRUE(st.ok());

    // 6. read again
    ASSIGN_OR_ABORT(auto metadata3, _tablet_manager->get_tablet_metadata(tablet_id, version2));

    iter = metadata3->delvec_meta().delvecs().find(segment_id);
    EXPECT_TRUE(iter != metadata3->delvec_meta().delvecs().end());
    auto delvecpb = iter->second;
    EXPECT_EQ(delvecpb.version(), version2);

    // 7. test reclaim delvec version to file name record
    ASSIGN_OR_ABORT(auto metadata4, _tablet_manager->get_tablet_metadata(tablet_id, version2));

    // clear all delvec meta element so that all element in
    // version_to_file map will also be removed
    // in this case, delvecs meta map has only one element [key=(segment=1234, value=(version=12, offset=0, size=35)]
    // delvec_to_file has also one element [key=(version=12), value=(delvec_file=xxx)]
    // after clearing,  delvecs meta map will have nothing, and element in delvec_to_file will also be useless
    auto new_meta = std::make_shared<TabletMetadataPB>(*metadata4);
    new_meta->mutable_delvec_meta()->mutable_delvecs()->clear();

    // insert a new delvec record into delvecs meta map with new version 13
    // we expect the old element in delvec_to_file map (version 12) will be removed
    auto new_version = version2 + 1;
    MetaFileBuilder builder3(*tablet, new_meta);
    new_meta->set_version(new_version);
    DelVector dv3;
    dv3.set_empty();
    EXPECT_TRUE(dv3.empty());
    std::shared_ptr<DelVector> ndv3;
    std::vector<uint32_t> dels3 = {1, 3, 5, 9, 90000};
    dv3.add_dels_as_new_version(dels3, new_version, &ndv3);
    builder3.append_delvec(ndv3, segment_id + 1);
    st = builder3.finalize(next_id());
    EXPECT_TRUE(st.ok());

    // validate delvec file record with version 12 been removed
    ASSIGN_OR_ABORT(auto metadata5, _tablet_manager->get_tablet_metadata(tablet_id, new_version));
    auto version_to_file_map = metadata5->delvec_meta().version_to_file();
    EXPECT_EQ(version_to_file_map.size(), 1);

    auto iter2 = version_to_file_map.find(version2);
    EXPECT_TRUE(iter2 == version_to_file_map.end());

    iter2 = version_to_file_map.find(new_version);
    EXPECT_TRUE(iter2 != version_to_file_map.end());
}

TEST_F(MetaFileTest, test_delvec_read_loop) {
    // 1. generate metadata
    const int64_t tablet_id = 10002;
    const int64_t version = 11;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    metadata->set_next_rowset_id(110);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    // 2. test delvec
    auto test_delvec = [&](uint32_t segment_id) {
        MetaFileBuilder builder(*tablet, metadata);
        DelVector dv;
        dv.set_empty();
        EXPECT_TRUE(dv.empty());

        std::shared_ptr<DelVector> ndv;
        std::vector<uint32_t> dels;
        for (int i = 0; i < 10; i++) {
            dels.push_back(rand() % 1000);
        }
        dv.add_dels_as_new_version(dels, version, &ndv);
        EXPECT_FALSE(ndv->empty());
        std::string before_delvec = ndv->save();
        builder.append_delvec(ndv, segment_id);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());

        // 3. read delvec
        DelVector after_delvec;
        LakeIOOptions lake_io_opts;
        ASSIGN_OR_ABORT(auto meta, _tablet_manager->get_tablet_metadata(tablet_id, version));
        EXPECT_TRUE(get_del_vec(_tablet_manager.get(), *meta, segment_id, false, lake_io_opts, &after_delvec).ok());
        EXPECT_EQ(before_delvec, after_delvec.save());
    };
    for (uint32_t segment_id = 1000; segment_id < 1200; segment_id++) {
        test_delvec(segment_id);
    }
    // test twice
    for (uint32_t segment_id = 1000; segment_id < 1200; segment_id++) {
        test_delvec(segment_id);
    }
}

TEST_F(MetaFileTest, test_dcg) {
    // 1. generate metadata
    const int64_t tablet_id = 10001;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);
    {
        MetaFileBuilder builder(*tablet, metadata);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
    }

    // 2. write first rowset
    {
        metadata->set_version(11);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segments("aaa.dat");
        TxnLogPB_OpWrite op_write;
        std::map<int, FileInfo> replace_segments;
        std::vector<FileMetaPB> orphan_files;
        op_write.mutable_rowset()->CopyFrom(rowset_metadata);
        builder.apply_opwrite(op_write, replace_segments, orphan_files);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
    }
    // 3. write dcg
    {
        metadata->set_version(12);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segments("bbb.dat");
        TxnLogPB_OpWrite op_write;
        op_write.mutable_rowset()->CopyFrom(rowset_metadata);
        std::vector<std::pair<std::string, std::string>> filenames;
        filenames.emplace_back("aaa.cols", "");
        filenames.emplace_back("bbb.cols", "");
        std::vector<std::vector<ColumnUID>> unique_column_id_list;
        unique_column_id_list.push_back({3, 4, 5});
        unique_column_id_list.push_back({6, 7, 8});
        builder.append_dcg(110, filenames, unique_column_id_list);
        builder.apply_column_mode_partial_update(op_write);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
        // <3, 4, 5> -> aaa.cols
        // <6, 7, 8> -> bbb.cols
    }
    {
        metadata->set_version(13);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segments("ccc.dat");
        TxnLogPB_OpWrite op_write;
        op_write.mutable_rowset()->CopyFrom(rowset_metadata);
        std::vector<std::pair<std::string, std::string>> filenames;
        filenames.emplace_back("ccc.cols", "");
        std::vector<std::vector<ColumnUID>> unique_column_id_list;
        unique_column_id_list.push_back({4, 7});
        builder.append_dcg(110, filenames, unique_column_id_list);
        builder.apply_column_mode_partial_update(op_write);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
        // <3, 5> -> aaa.cols
        // <6, 8> -> bbb.cols
        // <4, 7> -> ccc.cols
    }
    {
        metadata->set_version(14);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segments("ddd.dat");
        TxnLogPB_OpWrite op_write;
        op_write.mutable_rowset()->CopyFrom(rowset_metadata);
        std::vector<std::pair<std::string, std::string>> filenames;
        filenames.emplace_back("ddd.cols", "");
        std::vector<std::vector<ColumnUID>> unique_column_id_list;
        unique_column_id_list.push_back({3, 5});
        builder.append_dcg(110, filenames, unique_column_id_list);
        builder.apply_column_mode_partial_update(op_write);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
        auto dcg_ver_iter = metadata->dcg_meta().dcgs().find(110);
        EXPECT_TRUE(dcg_ver_iter != metadata->dcg_meta().dcgs().end());
        EXPECT_TRUE(dcg_ver_iter->second.versions_size() == 3);
        EXPECT_TRUE(dcg_ver_iter->second.column_files_size() == 3);
        EXPECT_TRUE(dcg_ver_iter->second.unique_column_ids_size() == 3);
        // <3, 5> -> ddd.cols
        // <6, 8> -> bbb.cols
        // <4, 7> -> ccc.cols
    }
    {
        auto loader = std::make_unique<LakeDeltaColumnGroupLoader>(metadata);
        TabletSegmentId tsid;
        tsid.tablet_id = tablet_id;
        tsid.segment_id = 110;
        DeltaColumnGroupList pdcgs;
        EXPECT_TRUE(loader->load(tsid, 1, &pdcgs).ok());
        EXPECT_TRUE(pdcgs.size() == 1);
        auto idx = pdcgs[0]->get_column_idx(3);
        EXPECT_TRUE("tmp/ddd.cols" == pdcgs[0]->column_files("tmp")[idx.first]);
        EXPECT_TRUE("tmp/ddd.cols" == pdcgs[0]->column_file_by_idx("tmp", idx.first).value());
        idx = pdcgs[0]->get_column_idx(4);
        EXPECT_TRUE("tmp/ccc.cols" == pdcgs[0]->column_files("tmp")[idx.first]);
        EXPECT_TRUE("tmp/ccc.cols" == pdcgs[0]->column_file_by_idx("tmp", idx.first).value());
        idx = pdcgs[0]->get_column_idx(5);
        EXPECT_TRUE("tmp/ddd.cols" == pdcgs[0]->column_files("tmp")[idx.first]);
        EXPECT_TRUE("tmp/ddd.cols" == pdcgs[0]->column_file_by_idx("tmp", idx.first).value());
        idx = pdcgs[0]->get_column_idx(6);
        EXPECT_TRUE("tmp/bbb.cols" == pdcgs[0]->column_files("tmp")[idx.first]);
        EXPECT_TRUE("tmp/bbb.cols" == pdcgs[0]->column_file_by_idx("tmp", idx.first).value());
        idx = pdcgs[0]->get_column_idx(7);
        EXPECT_TRUE("tmp/ccc.cols" == pdcgs[0]->column_files("tmp")[idx.first]);
        EXPECT_TRUE("tmp/ccc.cols" == pdcgs[0]->column_file_by_idx("tmp", idx.first).value());
        idx = pdcgs[0]->get_column_idx(8);
        EXPECT_TRUE("tmp/bbb.cols" == pdcgs[0]->column_files("tmp")[idx.first]);
        EXPECT_TRUE("tmp/bbb.cols" == pdcgs[0]->column_file_by_idx("tmp", idx.first).value());
    }
    // 4. compact (conflict)
    {
        metadata->set_version(15);
        MetaFileBuilder builder(*tablet, metadata);
        TxnLogPB_OpCompaction op_compaction;
        op_compaction.add_input_rowsets(110);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segments("eee.dat");
        op_compaction.mutable_output_rowset()->CopyFrom(rowset_metadata);
        op_compaction.set_compact_version(13);
        EXPECT_TRUE(CompactionUpdateConflictChecker::conflict_check(op_compaction, 111, *metadata, &builder));
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
    }
    // 5. compact
    {
        metadata->set_version(16);
        MetaFileBuilder builder(*tablet, metadata);
        TxnLogPB_OpCompaction op_compaction;
        op_compaction.add_input_rowsets(110);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segments("fff.dat");
        op_compaction.mutable_output_rowset()->CopyFrom(rowset_metadata);
        op_compaction.set_compact_version(14);
        EXPECT_FALSE(CompactionUpdateConflictChecker::conflict_check(op_compaction, 111, *metadata, &builder));
        builder.apply_opcompaction(op_compaction, 1, 0);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
    }
    {
        auto loader = std::make_unique<LakeDeltaColumnGroupLoader>(metadata);
        TabletSegmentId tsid;
        tsid.tablet_id = tablet_id;
        tsid.segment_id = 110;
        DeltaColumnGroupList pdcgs;
        EXPECT_TRUE(loader->load(tsid, 1, &pdcgs).ok());
        EXPECT_TRUE(pdcgs.empty());
    }
    // 6. check orphan files
    {
        std::set<std::string> to_check_filenames;
        to_check_filenames.insert("aaa.cols");
        to_check_filenames.insert("bbb.cols");
        to_check_filenames.insert("ccc.cols");
        to_check_filenames.insert("ddd.cols");
        to_check_filenames.insert("bbb.dat");
        to_check_filenames.insert("ccc.dat");
        to_check_filenames.insert("ddd.dat");
        to_check_filenames.insert("eee.dat");
        EXPECT_TRUE(metadata->orphan_files_size() == to_check_filenames.size());
        for (const auto& orphan_file : metadata->orphan_files()) {
            EXPECT_TRUE(to_check_filenames.count(orphan_file.name()) > 0);
        }
    }
}

TEST_F(MetaFileTest, test_unpersistent_del_files_when_compact) {
    // 1. generate metadata
    const int64_t tablet_id = 10001;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_enable_persistent_index(true);
    metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);
    {
        MetaFileBuilder builder(*tablet, metadata);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
    }

    // 2. write first rowset (110)
    {
        metadata->set_version(11);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segments("aaa.dat");
        TxnLogPB_OpWrite op_write;
        std::map<int, FileInfo> replace_segments;
        std::vector<FileMetaPB> orphan_files;
        op_write.mutable_rowset()->CopyFrom(rowset_metadata);
        builder.apply_opwrite(op_write, replace_segments, orphan_files);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
    }
    // 3. write second rowset with del files (111)
    {
        metadata->set_version(12);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segments("bbb.dat");
        DelfileWithRowsetId delfile;
        delfile.set_name("bbb1.del");
        delfile.set_origin_rowset_id(metadata->next_rowset_id());
        rowset_metadata.add_del_files()->CopyFrom(delfile);
        delfile.set_name("bbb2.del");
        rowset_metadata.add_del_files()->CopyFrom(delfile);
        TxnLogPB_OpWrite op_write;
        std::map<int, FileInfo> replace_segments;
        std::vector<FileMetaPB> orphan_files;
        op_write.mutable_rowset()->CopyFrom(rowset_metadata);
        builder.apply_opwrite(op_write, replace_segments, orphan_files);
        PersistentIndexSstablePB sstable;
        sstable.set_max_rss_rowid((uint64_t)111 << 32);
        metadata->mutable_sstable_meta()->add_sstables()->CopyFrom(sstable);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
    }
    // 4. compact (112)
    {
        metadata->set_version(13);
        MetaFileBuilder builder(*tablet, metadata);
        TxnLogPB_OpCompaction op_compaction;
        op_compaction.add_input_rowsets(110);
        op_compaction.add_input_rowsets(111);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segments("ccc.dat");
        op_compaction.mutable_output_rowset()->CopyFrom(rowset_metadata);
        op_compaction.set_compact_version(13);
        builder.apply_opcompaction(op_compaction, 111, 0);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
        // check unpersistent del files
        EXPECT_TRUE(metadata->rowsets_size() == 1);
        EXPECT_TRUE(metadata->rowsets(0).del_files_size() == 2);
        EXPECT_TRUE(metadata->rowsets(0).del_files(0).name() == "bbb1.del");
        EXPECT_TRUE(metadata->rowsets(0).del_files(0).origin_rowset_id() == 111);
        EXPECT_TRUE(metadata->rowsets(0).del_files(1).name() == "bbb2.del");
        EXPECT_TRUE(metadata->rowsets(0).del_files(1).origin_rowset_id() == 111);
        EXPECT_TRUE(metadata->compaction_inputs_size() == 2);
        EXPECT_TRUE(metadata->compaction_inputs(0).del_files_size() == 0);
        EXPECT_TRUE(metadata->compaction_inputs(1).del_files_size() == 0);
    }
    // 5. keep write (113)
    {
        metadata->set_version(14);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segments("ddd.dat");
        TxnLogPB_OpWrite op_write;
        std::map<int, FileInfo> replace_segments;
        std::vector<FileMetaPB> orphan_files;
        op_write.mutable_rowset()->CopyFrom(rowset_metadata);
        builder.apply_opwrite(op_write, replace_segments, orphan_files);
        PersistentIndexSstablePB sstable;
        sstable.set_max_rss_rowid((uint64_t)113 << 32);
        metadata->mutable_sstable_meta()->add_sstables()->CopyFrom(sstable);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
    }
    // 6. compact (114)
    {
        metadata->set_version(15);
        MetaFileBuilder builder(*tablet, metadata);
        TxnLogPB_OpCompaction op_compaction;
        op_compaction.add_input_rowsets(112);
        op_compaction.add_input_rowsets(113);
        RowsetMetadataPB rowset_metadata;
        rowset_metadata.add_segments("eee.dat");
        op_compaction.mutable_output_rowset()->CopyFrom(rowset_metadata);
        op_compaction.set_compact_version(15);
        builder.apply_opcompaction(op_compaction, 113, 0);
        Status st = builder.finalize(next_id());
        EXPECT_TRUE(st.ok());
        // check unpersistent del files
        EXPECT_TRUE(metadata->rowsets_size() == 1);
        EXPECT_TRUE(metadata->rowsets(0).del_files_size() == 0);
        EXPECT_TRUE(metadata->compaction_inputs(0).del_files_size() == 0);
        EXPECT_TRUE(metadata->compaction_inputs(1).del_files_size() == 0);
    }
}

TEST_F(MetaFileTest, test_compaction_conflict_checker_with_sparse_segment_id) {
    const int64_t tablet_id = 32001;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(200);

    auto* input_rowset = metadata->add_rowsets();
    input_rowset->set_id(110);
    input_rowset->add_segments("a.dat");
    input_rowset->add_segments("b.dat");
    input_rowset->add_segment_metas()->set_segment_idx(0);
    input_rowset->add_segment_metas()->set_segment_idx(5);

    DeltaColumnGroupVerPB dcg;
    dcg.add_versions(13);
    (*metadata->mutable_dcg_meta()->mutable_dcgs())[115] = dcg;

    MetaFileBuilder builder(*tablet, metadata);
    TxnLogPB_OpCompaction op_compaction;
    op_compaction.add_input_rowsets(110);
    op_compaction.set_compact_version(12);
    op_compaction.mutable_output_rowset()->add_segments("out.dat");

    EXPECT_TRUE(CompactionUpdateConflictChecker::conflict_check(op_compaction, 111, *metadata, &builder));
}

TEST_F(MetaFileTest, test_trim_partial_compaction_last_input_rowset) {
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(9);
    metadata->set_version(10);

    TxnLogPB_OpCompaction op_compaction;
    op_compaction.add_input_rowsets(1);
    op_compaction.add_input_rowsets(11);
    op_compaction.add_input_rowsets(22);
    op_compaction.mutable_output_rowset()->add_segments("aaa.dat");
    op_compaction.mutable_output_rowset()->add_segments("bbb.dat");
    op_compaction.mutable_output_rowset()->add_segments("ccc.dat");
    op_compaction.mutable_output_rowset()->add_segments("ddd.dat");
    RowsetMetadataPB last_input_rowset_metadata;

    last_input_rowset_metadata.set_id(33);
    last_input_rowset_metadata.mutable_segments()->Clear();
    last_input_rowset_metadata.add_segments("aaa.dat");
    last_input_rowset_metadata.add_segments("eee.dat");
    last_input_rowset_metadata.add_segments("fff.dat");
    last_input_rowset_metadata.add_segments("ddd.dat");
    EXPECT_EQ(last_input_rowset_metadata.segments_size(), 4);
    // rowset id mismatch
    trim_partial_compaction_last_input_rowset(metadata, op_compaction, last_input_rowset_metadata);
    EXPECT_EQ(last_input_rowset_metadata.segments_size(), 4);

    last_input_rowset_metadata.set_id(22);
    // normal case, duplicate segments will be trimed
    trim_partial_compaction_last_input_rowset(metadata, op_compaction, last_input_rowset_metadata);
    EXPECT_EQ(last_input_rowset_metadata.segments_size(), 2);
    EXPECT_EQ(last_input_rowset_metadata.segments(0), "eee.dat");
    EXPECT_EQ(last_input_rowset_metadata.segments(1), "fff.dat");

    // no duplicate segments
    last_input_rowset_metadata.mutable_segments()->Clear();
    last_input_rowset_metadata.add_segments("xxx.dat");
    last_input_rowset_metadata.add_segments("yyy.dat");
    EXPECT_EQ(last_input_rowset_metadata.segments_size(), 2);
    trim_partial_compaction_last_input_rowset(metadata, op_compaction, last_input_rowset_metadata);
    EXPECT_EQ(last_input_rowset_metadata.segments_size(), 2);
}

// Verify that trim_partial_compaction_last_input_rowset also trims segment_metas
// so that vacuum won't delete .vi files still referenced by the output rowset.
TEST_F(MetaFileTest, test_trim_partial_compaction_last_input_rowset_with_vi) {
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(9);
    metadata->set_version(10);

    TxnLogPB_OpCompaction op_compaction;
    op_compaction.add_input_rowsets(22);
    // Output rowset contains: aaa.dat (reused), new_seg.dat (new compacted), ddd.dat (reused)
    op_compaction.mutable_output_rowset()->add_segments("aaa.dat");
    op_compaction.mutable_output_rowset()->add_segments("new_seg.dat");
    op_compaction.mutable_output_rowset()->add_segments("ddd.dat");

    // Last input rowset has segments: [aaa.dat, bbb.dat, ccc.dat, ddd.dat]
    // where aaa.dat and ddd.dat are reused in output (uncompacted), bbb.dat and ccc.dat are consumed
    RowsetMetadataPB last_input_rowset;
    last_input_rowset.set_id(22);
    last_input_rowset.add_segments("aaa.dat");
    last_input_rowset.add_segments("bbb.dat");
    last_input_rowset.add_segments("ccc.dat");
    last_input_rowset.add_segments("ddd.dat");

    // All segments have vector index tracking via segment_metas
    auto* meta_aaa = last_input_rowset.add_segment_metas();
    meta_aaa->add_vector_index_ids(100);
    auto* meta_bbb = last_input_rowset.add_segment_metas();
    meta_bbb->add_vector_index_ids(100);
    meta_bbb->add_vector_index_ids(200);
    auto* meta_ccc = last_input_rowset.add_segment_metas();
    meta_ccc->add_vector_index_ids(100);
    auto* meta_ddd = last_input_rowset.add_segment_metas();
    meta_ddd->add_vector_index_ids(100);

    EXPECT_EQ(last_input_rowset.segments_size(), 4);
    EXPECT_EQ(last_input_rowset.segment_metas_size(), 4);

    trim_partial_compaction_last_input_rowset(metadata, op_compaction, last_input_rowset);

    // After trim: only consumed segments (bbb.dat, ccc.dat) should remain
    EXPECT_EQ(last_input_rowset.segments_size(), 2);
    EXPECT_EQ(last_input_rowset.segments(0), "bbb.dat");
    EXPECT_EQ(last_input_rowset.segments(1), "ccc.dat");

    // segment_metas should also be trimmed: aaa.dat and ddd.dat entries removed
    EXPECT_EQ(last_input_rowset.segment_metas_size(), 2);

    // Verify the index IDs are preserved correctly
    EXPECT_EQ(last_input_rowset.segment_metas(0).vector_index_ids_size(), 2); // bbb.dat
    EXPECT_EQ(last_input_rowset.segment_metas(1).vector_index_ids_size(), 1); // ccc.dat
}

TEST_F(MetaFileTest, test_error_state) {
    // generate metadata
    const int64_t tablet_id = 10001;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);

    // add rowset with segment
    RowsetMetadataPB rowset_metadata;
    rowset_metadata.set_id(110);
    rowset_metadata.add_segments("aaa.dat");
    rowset_metadata.add_segments("bbb.dat");
    metadata->add_rowsets()->CopyFrom(rowset_metadata);
    std::map<uint32_t, size_t> segment_id_to_add_dels;
    for (int i = 0; i < 10; i++) {
        segment_id_to_add_dels[i] = 100;
    }
    // generate error state
    MetaFileBuilder builder(*tablet, metadata);
    Status st = builder.update_num_del_stat(segment_id_to_add_dels);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(StorageMetrics::instance()->primary_key_table_error_state_total.value() > 0);
}

TEST_F(MetaFileTest, test_segment_id_helper_fallback_and_override) {
    RowsetMetadataPB rowset;
    rowset.set_id(1000);
    rowset.add_segments("a.dat");
    rowset.add_segments("b.dat");
    rowset.add_segment_metas()->set_num_rows(10);
    rowset.add_segment_metas()->set_num_rows(20);

    // Backward compatibility: fallback to segment index when segment_id is absent.
    EXPECT_EQ(0, get_segment_idx(rowset, 0));
    EXPECT_EQ(1, get_segment_idx(rowset, 1));
    EXPECT_EQ(1000, get_rssid(rowset, 0));
    EXPECT_EQ(1001, get_rssid(rowset, 1));

    rowset.mutable_segment_metas(0)->set_segment_idx(3);
    rowset.mutable_segment_metas(1)->set_segment_idx(8);

    EXPECT_EQ(3, get_segment_idx(rowset, 0));
    EXPECT_EQ(8, get_segment_idx(rowset, 1));
    EXPECT_EQ(1003, get_rssid(rowset, 0));
    EXPECT_EQ(1008, get_rssid(rowset, 1));
}

TEST_F(MetaFileTest, test_apply_opwrite_del_op_offset_uses_max_segment_id) {
    const int64_t tablet_id = 31001;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);

    MetaFileBuilder builder(*tablet, metadata);
    TxnLogPB_OpWrite op_write;
    auto* rowset = op_write.mutable_rowset();
    rowset->add_segments("a.dat");
    rowset->add_segments("b.dat");
    rowset->add_segment_metas()->set_segment_idx(2);
    rowset->add_segment_metas()->set_segment_idx(7);
    op_write.add_dels("d1.del");
    op_write.add_dels("d2.del");

    builder.apply_opwrite(op_write, {}, {});

    ASSERT_EQ(1, metadata->rowsets_size());
    const auto& written = metadata->rowsets(0);
    ASSERT_EQ(2, written.del_files_size());
    EXPECT_EQ(7, written.del_files(0).op_offset());
    EXPECT_EQ(7, written.del_files(1).op_offset());
    EXPECT_EQ(118, metadata->next_rowset_id());
}

TEST_F(MetaFileTest, test_apply_opcompaction_delete_delvec_with_segment_id) {
    const int64_t tablet_id = 31002;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(102);

    // input rowset with sparse segment ids: rssids are 100 and 105.
    auto* input_rowset = metadata->add_rowsets();
    input_rowset->set_id(100);
    input_rowset->add_segments("a.dat");
    input_rowset->add_segments("b.dat");
    input_rowset->add_segment_metas()->set_segment_idx(0);
    input_rowset->add_segment_metas()->set_segment_idx(5);

    // neighbor rowset with rssid 101 should not be deleted.
    auto* neighbor_rowset = metadata->add_rowsets();
    neighbor_rowset->set_id(101);
    neighbor_rowset->add_segments("c.dat");
    neighbor_rowset->add_segment_metas()->set_segment_idx(0);

    DelvecPagePB delvec_page;
    delvec_page.set_version(10);
    delvec_page.set_offset(0);
    delvec_page.set_size(1);
    (*metadata->mutable_delvec_meta()->mutable_delvecs())[100] = delvec_page;
    (*metadata->mutable_delvec_meta()->mutable_delvecs())[101] = delvec_page;
    (*metadata->mutable_delvec_meta()->mutable_delvecs())[105] = delvec_page;

    DeltaColumnGroupVerPB dcg;
    dcg.add_column_files("a.cols");
    (*metadata->mutable_dcg_meta()->mutable_dcgs())[100] = dcg;
    (*metadata->mutable_dcg_meta()->mutable_dcgs())[101] = dcg;
    (*metadata->mutable_dcg_meta()->mutable_dcgs())[105] = dcg;

    MetaFileBuilder builder(*tablet, metadata);
    TxnLogPB_OpCompaction op_compaction;
    op_compaction.add_input_rowsets(100);
    op_compaction.mutable_output_rowset()->add_segments("out.dat");
    op_compaction.mutable_output_rowset()->add_segment_metas()->set_segment_idx(0);

    ASSERT_OK(builder.apply_opcompaction(op_compaction, 101, 0));

    const auto& delvecs = metadata->delvec_meta().delvecs();
    EXPECT_TRUE(delvecs.find(100) == delvecs.end());
    EXPECT_TRUE(delvecs.find(105) == delvecs.end());
    EXPECT_TRUE(delvecs.find(101) != delvecs.end());

    const auto& dcgs = metadata->dcg_meta().dcgs();
    EXPECT_TRUE(dcgs.find(100) == dcgs.end());
    EXPECT_TRUE(dcgs.find(105) == dcgs.end());
    EXPECT_TRUE(dcgs.find(101) != dcgs.end());
}

TEST_F(MetaFileTest, test_apply_opcompaction_next_rowset_id_uses_max_segment_id) {
    const int64_t tablet_id = 31003;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(200);

    auto* input_rowset = metadata->add_rowsets();
    input_rowset->set_id(100);
    input_rowset->add_segments("in.dat");
    input_rowset->add_segment_metas()->set_segment_idx(0);

    MetaFileBuilder builder(*tablet, metadata);
    TxnLogPB_OpCompaction op_compaction;
    op_compaction.add_input_rowsets(100);
    auto* output_rowset = op_compaction.mutable_output_rowset();
    output_rowset->add_segments("out1.dat");
    output_rowset->add_segments("out2.dat");
    output_rowset->add_segment_metas()->set_segment_idx(1);
    output_rowset->add_segment_metas()->set_segment_idx(5);

    ASSERT_OK(builder.apply_opcompaction(op_compaction, 100, 0));

    ASSERT_EQ(1, metadata->rowsets_size());
    EXPECT_EQ(200, metadata->rowsets(0).id());
    EXPECT_EQ(206, metadata->next_rowset_id());
}

TEST_F(MetaFileTest, test_batch_apply_opwrite_set_final_rowset_basic) {
    const int64_t tablet_id = 30001;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);

    MetaFileBuilder builder(*tablet, metadata);

    // Batch 1: add two segments a.dat / b.dat
    TxnLogPB_OpWrite op_write1;
    RowsetMetadataPB rowset_meta1;
    rowset_meta1.add_segments("a.dat");
    rowset_meta1.add_segments("b.dat");
    op_write1.mutable_rowset()->CopyFrom(rowset_meta1);
    builder.batch_apply_opwrite(op_write1, /*replace_segments*/ {}, /*orphan_files*/ {});

    // Batch 2: append one segment c.dat (no cross-batch replacement to avoid OOB)
    TxnLogPB_OpWrite op_write2;
    RowsetMetadataPB rowset_meta2;
    rowset_meta2.add_segments("c.dat");
    op_write2.mutable_rowset()->CopyFrom(rowset_meta2);
    builder.batch_apply_opwrite(op_write2, /*replace_segments*/ {}, /*orphan_files*/ {});

    // Update delete stats before finalizing; predicted segment ids start from next_rowset_id
    std::map<uint32_t, size_t> segid_to_add_dels;
    segid_to_add_dels[110] = 5; // a.dat
    segid_to_add_dels[111] = 3; // b.dat
    segid_to_add_dels[112] = 2; // c.dat
    ASSERT_TRUE(builder.update_num_del_stat(segid_to_add_dels).ok());

    // Seal pending rowset
    ASSERT_TRUE(builder.set_final_rowset().ok());
    ASSERT_EQ(1, metadata->rowsets_size());
    const auto& final_rowset = metadata->rowsets(0);
    EXPECT_EQ(110, final_rowset.id());
    ASSERT_EQ(3, final_rowset.segments_size());
    EXPECT_EQ("a.dat", final_rowset.segments(0));
    EXPECT_EQ("b.dat", final_rowset.segments(1));
    EXPECT_EQ("c.dat", final_rowset.segments(2));
    EXPECT_EQ(10, final_rowset.num_dels()); // 5 + 3 + 2
    EXPECT_EQ(113, metadata->next_rowset_id());

    // Persist metadata
    metadata->set_version(11);
    ASSERT_TRUE(builder.finalize(next_id()).ok());
    ASSIGN_OR_ABORT(auto persisted, _tablet_manager->get_tablet_metadata(tablet_id, 11));
    ASSERT_EQ(1, persisted->rowsets_size());
    EXPECT_EQ(10, persisted->rowsets(0).num_dels());
    EXPECT_EQ("b.dat", persisted->rowsets(0).segments(1));
}

TEST_F(MetaFileTest, test_batch_apply_opwrite_merge_dels) {
    const int64_t tablet_id = 30002;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(20);
    metadata->set_next_rowset_id(500);

    MetaFileBuilder builder(*tablet, metadata);

    // batch 1: two segments + two del files
    TxnLogPB_OpWrite op_write1;
    RowsetMetadataPB rowset_meta1;
    rowset_meta1.add_segments("s1.dat");
    rowset_meta1.add_segments("s2.dat");
    rowset_meta1.add_segment_metas()->set_segment_idx(3);
    rowset_meta1.add_segment_metas()->set_segment_idx(9);
    op_write1.mutable_rowset()->CopyFrom(rowset_meta1);
    op_write1.add_dels("d1.del");
    op_write1.add_dels("d2.del");
    builder.batch_apply_opwrite(op_write1, {}, {});

    // batch 2: one segment + one del file
    TxnLogPB_OpWrite op_write2;
    RowsetMetadataPB rowset_meta2;
    rowset_meta2.add_segments("s3.dat");
    rowset_meta2.add_segment_metas()->set_segment_idx(4);
    op_write2.mutable_rowset()->CopyFrom(rowset_meta2);
    op_write2.add_dels("d3.del");
    builder.batch_apply_opwrite(op_write2, {}, {});

    ASSERT_TRUE(builder.set_final_rowset().ok());
    ASSERT_EQ(1, metadata->rowsets_size());
    const auto& final_rowset = metadata->rowsets(0);
    EXPECT_EQ(500, final_rowset.id());
    ASSERT_EQ(3, final_rowset.segments_size());
    EXPECT_EQ("s1.dat", final_rowset.segments(0));
    EXPECT_EQ("s2.dat", final_rowset.segments(1));
    EXPECT_EQ("s3.dat", final_rowset.segments(2));
    ASSERT_EQ(3, final_rowset.segment_metas_size());
    EXPECT_EQ(3, final_rowset.segment_metas(0).segment_idx());
    EXPECT_EQ(9, final_rowset.segment_metas(1).segment_idx());
    EXPECT_EQ(14, final_rowset.segment_metas(2).segment_idx());
    ASSERT_EQ(3, final_rowset.del_files_size());
    std::set<std::string> del_names;
    for (int i = 0; i < final_rowset.del_files_size(); ++i) {
        del_names.insert(final_rowset.del_files(i).name());
        EXPECT_EQ(final_rowset.id(), final_rowset.del_files(i).origin_rowset_id());
        EXPECT_EQ(14, final_rowset.del_files(i).op_offset());
    }
    EXPECT_TRUE(del_names.count("d1.del") > 0);
    EXPECT_TRUE(del_names.count("d2.del") > 0);
    EXPECT_TRUE(del_names.count("d3.del") > 0);
    EXPECT_EQ(515, metadata->next_rowset_id());

    metadata->set_version(21);
    ASSERT_TRUE(builder.finalize(next_id()).ok());
    ASSIGN_OR_ABORT(auto persisted, _tablet_manager->get_tablet_metadata(tablet_id, 21));
    ASSERT_EQ(1, persisted->rowsets_size());
    ASSERT_EQ(3, persisted->rowsets(0).del_files_size());
}

TEST_F(MetaFileTest, test_batch_apply_opwrite_mixed_segment_meta_presence) {
    const int64_t tablet_id = 30003;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(30);
    metadata->set_next_rowset_id(600);

    MetaFileBuilder builder(*tablet, metadata);

    // First rowset does not contain segment_metas (backward compatible input).
    TxnLogPB_OpWrite op_write1;
    op_write1.mutable_rowset()->add_segments("m1.dat");
    op_write1.mutable_rowset()->add_segments("m2.dat");
    op_write1.add_dels("d1.del");
    builder.batch_apply_opwrite(op_write1, {}, {});

    // Second rowset contains segment_metas.
    TxnLogPB_OpWrite op_write2;
    op_write2.mutable_rowset()->add_segments("m3.dat");
    op_write2.mutable_rowset()->add_segment_metas()->set_segment_idx(0);
    op_write2.add_dels("d2.del");
    builder.batch_apply_opwrite(op_write2, {}, {});

    ASSERT_TRUE(builder.set_final_rowset().ok());
    ASSERT_EQ(1, metadata->rowsets_size());
    const auto& final_rowset = metadata->rowsets(0);
    ASSERT_EQ(3, final_rowset.segments_size());
    ASSERT_EQ(3, final_rowset.segment_metas_size());
    EXPECT_EQ(0, final_rowset.segment_metas(0).segment_idx());
    EXPECT_EQ(1, final_rowset.segment_metas(1).segment_idx());
    EXPECT_EQ(2, final_rowset.segment_metas(2).segment_idx());
    ASSERT_EQ(2, final_rowset.del_files_size());
    EXPECT_EQ(2, final_rowset.del_files(0).op_offset());
    EXPECT_EQ(2, final_rowset.del_files(1).op_offset());
    EXPECT_EQ(603, metadata->next_rowset_id());
}

TEST_F(MetaFileTest, test_sstable_delvec_integration) {
    // Test SSTable delvec integration: test new get_del_vec(DelvecPagePB) function and
    // version reference collection from SSTable delvecs during finalization
    const int64_t tablet_id = 40001;
    const uint32_t segment_id = 1001;
    const int64_t version1 = 11;
    const int64_t version2 = 12;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(version1);
    metadata->set_next_rowset_id(110);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);
    metadata->set_enable_persistent_index(true);
    metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);

    // 1. Create and write delvec first
    MetaFileBuilder builder1(*tablet, metadata);
    DelVector dv1;
    dv1.set_empty();
    std::shared_ptr<DelVector> ndv1;
    std::vector<uint32_t> dels1 = {1, 3, 5, 7, 100};
    dv1.add_dels_as_new_version(dels1, version1, &ndv1);
    std::string original_delvec = ndv1->save();
    builder1.append_delvec(ndv1, segment_id);
    Status st = builder1.finalize(next_id());
    EXPECT_TRUE(st.ok());

    // 2. Get the delvec page info for creating SSTable delvec
    ASSIGN_OR_ABORT(auto metadata1, _tablet_manager->get_tablet_metadata(tablet_id, version1));
    auto iter = metadata1->delvec_meta().delvecs().find(segment_id);
    EXPECT_TRUE(iter != metadata1->delvec_meta().delvecs().end());
    DelvecPagePB delvec_page = iter->second;

    // 3. Test new get_del_vec function with DelvecPagePB
    DelVector read_delvec1;
    LakeIOOptions lake_io_opts;
    EXPECT_TRUE(get_del_vec(_tablet_manager.get(), *metadata1, delvec_page, true, lake_io_opts, &read_delvec1).ok());
    EXPECT_EQ(original_delvec, read_delvec1.save());

    // 4. Create SSTable with delvec and write to version2
    metadata->set_version(version2);
    MetaFileBuilder builder2(*tablet, metadata);

    PersistentIndexSstableMetaPB sstable_meta;
    PersistentIndexSstablePB* sstable = sstable_meta.add_sstables();
    sstable->set_filename("test_sstable.sst");
    sstable->set_filesize(1024);
    sstable->set_max_rss_rowid(100);
    sstable->mutable_delvec()->CopyFrom(delvec_page); // Use DelvecPagePB instead of has_delvec

    builder2.finalize_sstable_meta(sstable_meta);
    st = builder2.finalize(next_id());
    EXPECT_TRUE(st.ok());

    // 5. Verify SSTable contains delvec information
    ASSIGN_OR_ABORT(auto metadata2, _tablet_manager->get_tablet_metadata(tablet_id, version2));
    EXPECT_EQ(1, metadata2->sstable_meta().sstables_size());
    const auto& saved_sstable = metadata2->sstable_meta().sstables(0);
    EXPECT_TRUE(saved_sstable.has_delvec());
    EXPECT_EQ(delvec_page.version(), saved_sstable.delvec().version());
    EXPECT_EQ(delvec_page.offset(), saved_sstable.delvec().offset());
    EXPECT_EQ(delvec_page.size(), saved_sstable.delvec().size());

    // 6. Test reading delvec via SSTable's delvec page
    DelVector read_delvec2;
    EXPECT_TRUE(
            get_del_vec(_tablet_manager.get(), *metadata2, saved_sstable.delvec(), true, lake_io_opts, &read_delvec2)
                    .ok());
    EXPECT_EQ(original_delvec, read_delvec2.save());

    // 7. Test version reference collection: create new metadata without regular delvec but with SSTable delvec
    auto metadata3 = std::make_shared<TabletMetadataPB>(*metadata2);
    metadata3->set_version(version2 + 1);
    metadata3->mutable_delvec_meta()->mutable_delvecs()->clear(); // Clear regular delvecs

    MetaFileBuilder builder3(*tablet, metadata3);
    st = builder3.finalize(next_id());
    EXPECT_TRUE(st.ok());

    // 8. Verify that delvec file version is preserved due to SSTable reference
    ASSIGN_OR_ABORT(auto metadata4, _tablet_manager->get_tablet_metadata(tablet_id, version2 + 1));
    auto version_to_file_map = metadata4->delvec_meta().version_to_file();

    // The delvec file with version1 should still exist because SSTable references it
    auto version_iter = version_to_file_map.find(version1);
    EXPECT_TRUE(version_iter != version_to_file_map.end());

    // 9. Verify we can still read delvec from SSTable after cleanup
    DelVector read_delvec3;
    EXPECT_TRUE(
            get_del_vec(_tablet_manager.get(), *metadata4, saved_sstable.delvec(), true, lake_io_opts, &read_delvec3)
                    .ok());
    EXPECT_EQ(original_delvec, read_delvec3.save());

    // 10. Remove SSTable delvec reference and verify delvec file cleanup
    auto metadata5 = std::make_shared<TabletMetadataPB>(*metadata4);
    metadata5->set_version(version2 + 2);
    metadata5->mutable_sstable_meta()->clear_sstables(); // Remove SSTable that references delvec

    MetaFileBuilder builder4(*tablet, metadata5);
    st = builder4.finalize(next_id());
    EXPECT_TRUE(st.ok());

    // 11. Verify that delvec file version is now removed since no SSTable references it
    ASSIGN_OR_ABORT(auto metadata6, _tablet_manager->get_tablet_metadata(tablet_id, version2 + 2));
    auto final_version_to_file_map = metadata6->delvec_meta().version_to_file();

    // The delvec file with version1 should be removed because no SSTable references it anymore
    auto final_version_iter = final_version_to_file_map.find(version1);
    EXPECT_TRUE(final_version_iter == final_version_to_file_map.end());
}
// Test that remove_compacted_sst skips SST files that appear in both input and output,
// which happens when parallel compaction's "full contain" optimization reuses input SSTs.
TEST_F(MetaFileTest, test_remove_compacted_sst_skip_reused_sst) {
    const int64_t tablet_id = 10010;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);
    metadata->set_enable_persistent_index(true);
    metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);

    // Setup: 3 input SSTs, where "reused.sst" appears in both input and output
    // (simulating the "full contain" optimization in parallel compaction)
    TxnLogPB_OpCompaction op_compaction;

    auto* input1 = op_compaction.add_input_sstables();
    input1->set_filename("old1.sst");
    input1->set_filesize(100);

    auto* input2 = op_compaction.add_input_sstables();
    input2->set_filename("reused.sst");
    input2->set_filesize(200);

    auto* input3 = op_compaction.add_input_sstables();
    input3->set_filename("old2.sst");
    input3->set_filesize(150);

    // Output contains "reused.sst" (full contain) and a new merged file
    auto* output1 = op_compaction.add_output_sstables();
    output1->set_filename("reused.sst");
    output1->set_filesize(200);

    auto* output2 = op_compaction.add_output_sstables();
    output2->set_filename("merged_new.sst");
    output2->set_filesize(250);

    MetaFileBuilder builder(*tablet, metadata);
    builder.remove_compacted_sst(op_compaction);

    // Verify: only "old1.sst" and "old2.sst" should be in orphan_files.
    // "reused.sst" must NOT be in orphan_files since it's also an output.
    ASSERT_EQ(metadata->orphan_files_size(), 2);
    std::set<std::string> orphan_names;
    for (const auto& f : metadata->orphan_files()) {
        orphan_names.insert(f.name());
    }
    EXPECT_TRUE(orphan_names.count("old1.sst") > 0);
    EXPECT_TRUE(orphan_names.count("old2.sst") > 0);
    EXPECT_TRUE(orphan_names.count("reused.sst") == 0);
}

// Test that remove_compacted_sst also handles output_sstable (singular, from major_compact)
TEST_F(MetaFileTest, test_remove_compacted_sst_skip_reused_sst_singular_output) {
    const int64_t tablet_id = 10011;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);
    metadata->set_enable_persistent_index(true);
    metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);

    TxnLogPB_OpCompaction op_compaction;

    auto* input1 = op_compaction.add_input_sstables();
    input1->set_filename("reused_single.sst");
    input1->set_filesize(100);

    auto* input2 = op_compaction.add_input_sstables();
    input2->set_filename("old.sst");
    input2->set_filesize(200);

    // Singular output_sstable reuses one of the input files
    op_compaction.mutable_output_sstable()->set_filename("reused_single.sst");
    op_compaction.mutable_output_sstable()->set_filesize(100);

    MetaFileBuilder builder(*tablet, metadata);
    builder.remove_compacted_sst(op_compaction);

    ASSERT_EQ(metadata->orphan_files_size(), 1);
    EXPECT_EQ(metadata->orphan_files(0).name(), "old.sst");
}

// Test that when no SSTs are reused, all inputs go to orphan_files (original behavior)
TEST_F(MetaFileTest, test_remove_compacted_sst_no_reuse) {
    const int64_t tablet_id = 10012;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);
    metadata->set_enable_persistent_index(true);
    metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);

    TxnLogPB_OpCompaction op_compaction;

    auto* input1 = op_compaction.add_input_sstables();
    input1->set_filename("a.sst");
    input1->set_filesize(100);

    auto* input2 = op_compaction.add_input_sstables();
    input2->set_filename("b.sst");
    input2->set_filesize(200);

    auto* output = op_compaction.add_output_sstables();
    output->set_filename("c.sst");
    output->set_filesize(300);

    MetaFileBuilder builder(*tablet, metadata);
    builder.remove_compacted_sst(op_compaction);

    // All inputs should be orphaned since output is a new file
    ASSERT_EQ(metadata->orphan_files_size(), 2);
    std::set<std::string> orphan_names;
    for (const auto& f : metadata->orphan_files()) {
        orphan_names.insert(f.name());
    }
    EXPECT_TRUE(orphan_names.count("a.sst") > 0);
    EXPECT_TRUE(orphan_names.count("b.sst") > 0);
}

// Test that append_delvec followed by apply_opcompaction in the same builder
// does NOT create orphan delvec entries. This reproduces the bug where a write
// txn generates a delvec for a segment that is then compacted away in the same
// publish batch. Without the fix, _finalize_delvec would re-insert the deleted
// delvec entry, creating an orphan that prevents delvec file GC.
TEST_F(MetaFileTest, test_no_orphan_delvec_after_write_then_compaction) {
    const int64_t tablet_id = 40001;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(100);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    // Create initial metadata on disk
    {
        MetaFileBuilder builder(*tablet, metadata);
        ASSERT_OK(builder.finalize(next_id()));
    }

    // Add two rowsets: rowset 100 (segment "a.dat") and rowset 101 (segment "b.dat")
    {
        metadata->set_version(11);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rs;
        rs.add_segments("a.dat");
        TxnLogPB_OpWrite op_write;
        op_write.mutable_rowset()->CopyFrom(rs);
        builder.apply_opwrite(op_write, {}, {});
        ASSERT_OK(builder.finalize(next_id()));
    }
    {
        metadata->set_version(12);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rs;
        rs.add_segments("b.dat");
        TxnLogPB_OpWrite op_write;
        op_write.mutable_rowset()->CopyFrom(rs);
        builder.apply_opwrite(op_write, {}, {});
        ASSERT_OK(builder.finalize(next_id()));
    }

    // Now metadata has rowsets: id=100 (seg "a.dat"), id=101 (seg "b.dat")
    ASSERT_EQ(2, metadata->rowsets_size());
    ASSERT_EQ(100, metadata->rowsets(0).id());
    ASSERT_EQ(101, metadata->rowsets(1).id());

    // Simulate a publish batch where:
    // 1) A write txn creates a delvec for segment 100 (belonging to rowset 100)
    // 2) A compaction txn compacts rowset 100 away
    // Both operations use the same MetaFileBuilder (batch publish).
    {
        metadata->set_version(13);
        MetaFileBuilder builder(*tablet, metadata);

        // Step 1: Write txn generates a delvec for segment 100
        DelVector dv;
        dv.set_empty();
        std::shared_ptr<DelVector> ndv;
        std::vector<uint32_t> dels = {1, 3, 5};
        dv.add_dels_as_new_version(dels, 13, &ndv);
        builder.append_delvec(ndv, 100); // segment_id = 100, belongs to rowset 100

        // Step 2: Compaction removes rowset 100
        TxnLogPB_OpCompaction op_compaction;
        op_compaction.add_input_rowsets(100);
        RowsetMetadataPB output_rs;
        output_rs.add_segments("compacted.dat");
        op_compaction.mutable_output_rowset()->CopyFrom(output_rs);
        ASSERT_OK(builder.apply_opcompaction(op_compaction, 100, 0));

        // Step 3: Finalize - this is where the bug would manifest
        ASSERT_OK(builder.finalize(next_id()));

        // Verify: segment 100's delvec should NOT exist in metadata (it was compacted away)
        const auto& delvecs_map = metadata->delvec_meta().delvecs();
        EXPECT_TRUE(delvecs_map.find(100) == delvecs_map.end())
                << "Orphan delvec entry found for compacted segment 100";

        // Verify: rowset 100 should be gone, only rowset 101 and the new compaction output remain
        EXPECT_EQ(2, metadata->rowsets_size());

        // Verify: version_to_file should not hold unreferenced entries
        for (const auto& vtf_entry : metadata->delvec_meta().version_to_file()) {
            bool referenced = false;
            for (const auto& dv_entry : delvecs_map) {
                if (dv_entry.second.version() == vtf_entry.first) {
                    referenced = true;
                    break;
                }
            }
            EXPECT_TRUE(referenced) << "version_to_file entry for version " << vtf_entry.first
                                    << " is not referenced by any delvec";
        }
    }
}

// Test the orphan delvec scenario with multiple segments in the compacted rowset.
// The write txn creates delvecs for two segments of the input rowset, both should
// be cleaned up when the rowset is compacted.
TEST_F(MetaFileTest, test_no_orphan_delvec_multi_segment_compaction) {
    const int64_t tablet_id = 40002;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(200);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    // Create initial metadata
    {
        MetaFileBuilder builder(*tablet, metadata);
        ASSERT_OK(builder.finalize(next_id()));
    }

    // Add a rowset with 2 segments (rowset id=200, segments at rssid 200 and 201)
    {
        metadata->set_version(11);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rs;
        rs.add_segments("seg0.dat");
        rs.add_segments("seg1.dat");
        TxnLogPB_OpWrite op_write;
        op_write.mutable_rowset()->CopyFrom(rs);
        builder.apply_opwrite(op_write, {}, {});
        ASSERT_OK(builder.finalize(next_id()));
    }

    // Add a second rowset (rowset id=202)
    {
        metadata->set_version(12);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rs;
        rs.add_segments("other.dat");
        TxnLogPB_OpWrite op_write;
        op_write.mutable_rowset()->CopyFrom(rs);
        builder.apply_opwrite(op_write, {}, {});
        ASSERT_OK(builder.finalize(next_id()));
    }

    ASSERT_EQ(2, metadata->rowsets_size());
    ASSERT_EQ(200, metadata->rowsets(0).id());
    ASSERT_EQ(202, metadata->rowsets(1).id());

    // Simulate batch publish: write creates delvecs for both segments of rowset 200,
    // then compaction removes rowset 200
    {
        metadata->set_version(13);
        MetaFileBuilder builder(*tablet, metadata);

        // Write creates delvecs for both segments
        DelVector dv1;
        dv1.set_empty();
        std::shared_ptr<DelVector> ndv1;
        std::vector<uint32_t> dels1 = {10, 20};
        dv1.add_dels_as_new_version(dels1, 13, &ndv1);
        builder.append_delvec(ndv1, 200);

        DelVector dv2;
        dv2.set_empty();
        std::shared_ptr<DelVector> ndv2;
        std::vector<uint32_t> dels2 = {30, 40};
        dv2.add_dels_as_new_version(dels2, 13, &ndv2);
        builder.append_delvec(ndv2, 201);

        // Compaction removes rowset 200
        TxnLogPB_OpCompaction op_compaction;
        op_compaction.add_input_rowsets(200);
        RowsetMetadataPB output_rs;
        output_rs.add_segments("compacted.dat");
        op_compaction.mutable_output_rowset()->CopyFrom(output_rs);
        ASSERT_OK(builder.apply_opcompaction(op_compaction, 200, 0));

        ASSERT_OK(builder.finalize(next_id()));

        // Both delvec entries for compacted segments should be gone
        const auto& delvecs_map = metadata->delvec_meta().delvecs();
        EXPECT_TRUE(delvecs_map.find(200) == delvecs_map.end()) << "Orphan delvec for segment 200";
        EXPECT_TRUE(delvecs_map.find(201) == delvecs_map.end()) << "Orphan delvec for segment 201";
    }
}

// Test that pre-existing orphan delvec entries in metadata are cleaned up
// during compaction. This simulates the upgrade scenario where orphan entries
// accumulated from the historical bug are present in the metadata.
TEST_F(MetaFileTest, test_cleanup_preexisting_orphan_delvecs_on_compaction) {
    const int64_t tablet_id = 40003;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(300);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    // Create initial metadata
    {
        MetaFileBuilder builder(*tablet, metadata);
        ASSERT_OK(builder.finalize(next_id()));
    }

    // Add two rowsets: id=300 ("a.dat") and id=301 ("b.dat")
    {
        metadata->set_version(11);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rs;
        rs.add_segments("a.dat");
        TxnLogPB_OpWrite op_write;
        op_write.mutable_rowset()->CopyFrom(rs);
        builder.apply_opwrite(op_write, {}, {});
        ASSERT_OK(builder.finalize(next_id()));
    }
    {
        metadata->set_version(12);
        MetaFileBuilder builder(*tablet, metadata);
        RowsetMetadataPB rs;
        rs.add_segments("b.dat");
        TxnLogPB_OpWrite op_write;
        op_write.mutable_rowset()->CopyFrom(rs);
        builder.apply_opwrite(op_write, {}, {});
        ASSERT_OK(builder.finalize(next_id()));
    }

    ASSERT_EQ(2, metadata->rowsets_size());
    ASSERT_EQ(300, metadata->rowsets(0).id());
    ASSERT_EQ(301, metadata->rowsets(1).id());

    // Manually inject orphan delvec entries into metadata to simulate
    // pre-existing orphans from the historical bug.
    // Segment IDs 100, 200, 999 do not belong to any current rowset.
    DelvecPagePB orphan_page;
    orphan_page.set_version(5);
    orphan_page.set_offset(0);
    orphan_page.set_size(10);
    (*metadata->mutable_delvec_meta()->mutable_delvecs())[100] = orphan_page;
    (*metadata->mutable_delvec_meta()->mutable_delvecs())[200] = orphan_page;
    (*metadata->mutable_delvec_meta()->mutable_delvecs())[999] = orphan_page;

    // Also add a valid delvec for existing segment 301
    DelvecPagePB valid_page;
    valid_page.set_version(12);
    valid_page.set_offset(0);
    valid_page.set_size(10);
    (*metadata->mutable_delvec_meta()->mutable_delvecs())[301] = valid_page;

    // Add version_to_file entries referenced by orphans
    FileMetaPB file_meta;
    file_meta.set_name("old_orphan.delvec");
    file_meta.set_size(100);
    (*metadata->mutable_delvec_meta()->mutable_version_to_file())[5] = file_meta;

    EXPECT_EQ(4, metadata->delvec_meta().delvecs().size()); // 3 orphans + 1 valid

    // Enable orphan cleanup config for this test
    config::lake_enable_orphan_delvec_cleanup_on_compaction = true;

    // Compact rowset 300 — this triggers orphan cleanup in apply_opcompaction
    {
        metadata->set_version(13);
        MetaFileBuilder builder(*tablet, metadata);
        TxnLogPB_OpCompaction op_compaction;
        op_compaction.add_input_rowsets(300);
        RowsetMetadataPB output_rs;
        output_rs.add_segments("compacted.dat");
        op_compaction.mutable_output_rowset()->CopyFrom(output_rs);
        ASSERT_OK(builder.apply_opcompaction(op_compaction, 300, 0));
        ASSERT_OK(builder.finalize(next_id()));

        // All orphan entries should be removed by the compaction cleanup
        const auto& delvecs_map = metadata->delvec_meta().delvecs();
        EXPECT_TRUE(delvecs_map.find(100) == delvecs_map.end()) << "Orphan segment 100 should be cleaned";
        EXPECT_TRUE(delvecs_map.find(200) == delvecs_map.end()) << "Orphan segment 200 should be cleaned";
        EXPECT_TRUE(delvecs_map.find(999) == delvecs_map.end()) << "Orphan segment 999 should be cleaned";
        // Segment 300's delvec was also removed (rowset 300 was compacted, had no delvec anyway)
        EXPECT_TRUE(delvecs_map.find(300) == delvecs_map.end());

        // Valid entry for segment 301 (non-compacted rowset) should remain
        EXPECT_TRUE(delvecs_map.find(301) != delvecs_map.end()) << "Valid segment 301 should be kept";
        EXPECT_EQ(1, delvecs_map.size());

        // The orphan version_to_file entry (version=5) should now be unreferenced and removed
        const auto& vtf = metadata->delvec_meta().version_to_file();
        EXPECT_TRUE(vtf.find(5) == vtf.end()) << "version_to_file entry for orphan version 5 should be cleaned up";
    }

    config::lake_enable_orphan_delvec_cleanup_on_compaction = false;
}

// Verify that remove_compacted_sst takes the shared flag from tablet metadata
// rather than the txn log, because during tablet split the txn log value may
// have lost the shared=true marking.
TEST_F(MetaFileTest, test_remove_compacted_sst_shared_from_metadata) {
    const int64_t tablet_id = 10020;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);
    metadata->set_enable_persistent_index(true);
    metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);

    // Pre-populate sstable_meta in tablet metadata with shared=true
    auto* sst_in_meta = metadata->mutable_sstable_meta()->add_sstables();
    sst_in_meta->set_filename("shared.sst");
    sst_in_meta->set_filesize(100);
    sst_in_meta->set_shared(true);

    // Build an OpCompaction where the input_sstable has shared=false (lost during cross-publish)
    TxnLogPB_OpCompaction op_compaction;
    auto* input = op_compaction.add_input_sstables();
    input->set_filename("shared.sst");
    input->set_filesize(100);
    input->set_shared(false); // incorrect value from txn log

    auto* output = op_compaction.add_output_sstables();
    output->set_filename("new_output.sst");
    output->set_filesize(200);

    MetaFileBuilder builder(*tablet, metadata);
    builder.remove_compacted_sst(op_compaction);

    // The orphan file should have shared=true from metadata, not false from txn log
    ASSERT_EQ(metadata->orphan_files_size(), 1);
    EXPECT_EQ(metadata->orphan_files(0).name(), "shared.sst");
    EXPECT_TRUE(metadata->orphan_files(0).shared());
}

// Verify that remove_compacted_sst falls back to the txn log shared flag
// when the SST is not found in tablet metadata.
TEST_F(MetaFileTest, test_remove_compacted_sst_shared_fallback_to_txn_log) {
    const int64_t tablet_id = 10021;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(110);
    metadata->set_enable_persistent_index(true);
    metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);

    // No matching SST in metadata sstable_meta (empty)

    TxnLogPB_OpCompaction op_compaction;
    auto* input = op_compaction.add_input_sstables();
    input->set_filename("only_in_txnlog.sst");
    input->set_filesize(150);
    input->set_shared(true); // value only in txn log

    auto* output = op_compaction.add_output_sstables();
    output->set_filename("new_output.sst");
    output->set_filesize(200);

    MetaFileBuilder builder(*tablet, metadata);
    builder.remove_compacted_sst(op_compaction);

    // Should use the txn log value since SST not found in metadata
    ASSERT_EQ(metadata->orphan_files_size(), 1);
    EXPECT_EQ(metadata->orphan_files(0).name(), "only_in_txnlog.sst");
    EXPECT_TRUE(metadata->orphan_files(0).shared());
}

// Verify apply_opwrite copies op_write.shared_dels into the per-del shared flag on
// rowset.del_files. Regression guard for the cross-publish path where new dels in an
// in-flight write are shared across sibling split tablets.
TEST_F(MetaFileTest, test_apply_opwrite_preserves_shared_dels) {
    const int64_t tablet_id = 10030;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(100);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    TxnLogPB_OpWrite op_write;
    op_write.mutable_rowset()->add_segments("seg.dat");
    op_write.add_dels("shared_d.del");
    op_write.add_dels("private_d.del");
    op_write.add_shared_dels(true);
    op_write.add_shared_dels(false);

    MetaFileBuilder builder(*tablet, metadata);
    builder.apply_opwrite(op_write, {}, {});

    ASSERT_EQ(metadata->rowsets_size(), 1);
    const auto& rowset = metadata->rowsets(0);
    ASSERT_EQ(rowset.del_files_size(), 2);
    EXPECT_EQ(rowset.del_files(0).name(), "shared_d.del");
    EXPECT_TRUE(rowset.del_files(0).shared());
    EXPECT_EQ(rowset.del_files(1).name(), "private_d.del");
    EXPECT_FALSE(rowset.del_files(1).shared());
}

// Verify backward compatibility: when op_write.shared_dels is empty (old txn log or
// non-cross-publish path), apply_opwrite leaves del_files[i].shared unset (default false).
TEST_F(MetaFileTest, test_apply_opwrite_empty_shared_dels_defaults_false) {
    const int64_t tablet_id = 10031;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(100);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    TxnLogPB_OpWrite op_write;
    op_write.mutable_rowset()->add_segments("seg.dat");
    op_write.add_dels("d.del");
    // shared_dels left empty — legacy/normal write

    MetaFileBuilder builder(*tablet, metadata);
    builder.apply_opwrite(op_write, {}, {});

    ASSERT_EQ(metadata->rowsets_size(), 1);
    const auto& rowset = metadata->rowsets(0);
    ASSERT_EQ(rowset.del_files_size(), 1);
    EXPECT_FALSE(rowset.del_files(0).shared());
}

// Verify batch_apply_opwrite + set_final_rowset preserve shared_dels across multiple
// op_writes merged into a single rowset.
TEST_F(MetaFileTest, test_batch_apply_opwrite_preserves_shared_dels) {
    const int64_t tablet_id = 10032;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(100);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    MetaFileBuilder builder(*tablet, metadata);

    // First opwrite: 1 shared del
    TxnLogPB_OpWrite op_write1;
    op_write1.mutable_rowset()->add_segments("seg1.dat");
    op_write1.add_dels("d1.del");
    op_write1.add_shared_dels(true);
    builder.batch_apply_opwrite(op_write1, {}, {});

    // Second opwrite: no shared_dels set (legacy / private)
    TxnLogPB_OpWrite op_write2;
    op_write2.mutable_rowset()->add_segments("seg2.dat");
    op_write2.add_dels("d2.del");
    builder.batch_apply_opwrite(op_write2, {}, {});

    // Third opwrite: 1 shared del
    TxnLogPB_OpWrite op_write3;
    op_write3.mutable_rowset()->add_segments("seg3.dat");
    op_write3.add_dels("d3.del");
    op_write3.add_shared_dels(true);
    builder.batch_apply_opwrite(op_write3, {}, {});

    ASSERT_OK(builder.set_final_rowset());

    ASSERT_EQ(metadata->rowsets_size(), 1);
    const auto& rowset = metadata->rowsets(0);
    ASSERT_EQ(rowset.del_files_size(), 3);
    EXPECT_EQ(rowset.del_files(0).name(), "d1.del");
    EXPECT_TRUE(rowset.del_files(0).shared());
    EXPECT_EQ(rowset.del_files(1).name(), "d2.del");
    EXPECT_FALSE(rowset.del_files(1).shared());
    EXPECT_EQ(rowset.del_files(2).name(), "d3.del");
    EXPECT_TRUE(rowset.del_files(2).shared());
}

// Verify that apply_opwrite clears shared_segments[i] to false for segments replaced
// by partial-update rewrite files. The rewrite file is private to this tablet and
// must not be routed through the shared-file GC path.
TEST_F(MetaFileTest, test_apply_opwrite_clears_shared_segments_for_rewrite) {
    const int64_t tablet_id = 10040;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(100);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    // Build op_write with 2 segments, both marked shared (simulating post-cross-publish).
    TxnLogPB_OpWrite op_write;
    auto* src_rowset = op_write.mutable_rowset();
    src_rowset->add_segments("orig0.dat");
    src_rowset->add_segments("orig1.dat");
    src_rowset->add_segment_size(1000);
    src_rowset->add_segment_size(1000);
    src_rowset->add_shared_segments(true);
    src_rowset->add_shared_segments(true);

    // Partial update: segment 0 is rewritten into a new private file.
    std::map<int, FileInfo> replace_segments;
    FileInfo rewrite_info;
    rewrite_info.path = "rewrite0.dat";
    rewrite_info.size = 1500;
    replace_segments[0] = rewrite_info;

    MetaFileBuilder builder(*tablet, metadata);
    builder.apply_opwrite(op_write, replace_segments, {});

    ASSERT_EQ(metadata->rowsets_size(), 1);
    const auto& rowset = metadata->rowsets(0);
    ASSERT_EQ(rowset.segments_size(), 2);
    ASSERT_EQ(rowset.shared_segments_size(), 2);
    // Segment 0 was rewritten: filename updated AND shared flag cleared.
    EXPECT_EQ(rowset.segments(0), "rewrite0.dat");
    EXPECT_FALSE(rowset.shared_segments(0));
    // Segment 1 was not touched: shared flag preserved.
    EXPECT_EQ(rowset.segments(1), "orig1.dat");
    EXPECT_TRUE(rowset.shared_segments(1));
}

// Verify the same behavior for the batched path (batch_apply_opwrite + set_final_rowset).
TEST_F(MetaFileTest, test_batch_apply_opwrite_clears_shared_segments_for_rewrite) {
    const int64_t tablet_id = 10041;
    auto tablet = std::make_shared<Tablet>(_tablet_manager.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(10);
    metadata->set_next_rowset_id(100);
    metadata->mutable_schema()->set_keys_type(PRIMARY_KEYS);

    TxnLogPB_OpWrite op_write;
    auto* src_rowset = op_write.mutable_rowset();
    src_rowset->add_segments("orig0.dat");
    src_rowset->add_segments("orig1.dat");
    src_rowset->add_segment_size(1000);
    src_rowset->add_segment_size(1000);
    src_rowset->add_shared_segments(true);
    src_rowset->add_shared_segments(true);

    std::map<int, FileInfo> replace_segments;
    FileInfo rewrite_info;
    rewrite_info.path = "rewrite0.dat";
    rewrite_info.size = 1500;
    replace_segments[0] = rewrite_info;

    MetaFileBuilder builder(*tablet, metadata);
    builder.batch_apply_opwrite(op_write, replace_segments, {});
    ASSERT_OK(builder.set_final_rowset());

    ASSERT_EQ(metadata->rowsets_size(), 1);
    const auto& rowset = metadata->rowsets(0);
    ASSERT_EQ(rowset.segments_size(), 2);
    ASSERT_EQ(rowset.shared_segments_size(), 2);
    EXPECT_EQ(rowset.segments(0), "rewrite0.dat");
    EXPECT_FALSE(rowset.shared_segments(0));
    EXPECT_EQ(rowset.segments(1), "orig1.dat");
    EXPECT_TRUE(rowset.shared_segments(1));
}

} // namespace starrocks::lake
