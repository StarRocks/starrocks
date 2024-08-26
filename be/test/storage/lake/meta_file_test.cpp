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

#include "common/config.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/del_vector.h"
#include "storage/lake/column_mode_partial_update_handler.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/update_manager.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "util/uid_util.h"

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
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider.get(), _mem_tracker.get());
        _tablet_manager =
                std::make_unique<lake::TabletManager>(_location_provider.get(), _update_manager.get(), 1638400000);
    }

    void TearDown() { (void)FileSystem::Default()->delete_dir_recursive(kTestDir); }

protected:
    constexpr static const char* const kTestDir = "./lake_meta_test";
    std::unique_ptr<lake::LocationProvider> _location_provider;
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
    ASSIGN_OR_ABORT(auto metadata2, _tablet_manager->get_tablet_metadata(tablet_id, version));
    EXPECT_TRUE(get_del_vec(_tablet_manager.get(), *metadata2, segment_id, true, &after_delvec).ok());
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
        ASSIGN_OR_ABORT(auto meta, _tablet_manager->get_tablet_metadata(tablet_id, version));
        EXPECT_TRUE(get_del_vec(_tablet_manager.get(), *meta, segment_id, false, &after_delvec).ok());
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
        std::vector<std::string> orphan_files;
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
        std::vector<std::string> filenames;
        filenames.push_back("aaa.cols");
        filenames.push_back("bbb.cols");
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
        std::vector<std::string> filenames;
        filenames.push_back("ccc.cols");
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
        std::vector<std::string> filenames;
        filenames.push_back("ddd.cols");
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
        idx = pdcgs[0]->get_column_idx(4);
        EXPECT_TRUE("tmp/ccc.cols" == pdcgs[0]->column_files("tmp")[idx.first]);
        idx = pdcgs[0]->get_column_idx(5);
        EXPECT_TRUE("tmp/ddd.cols" == pdcgs[0]->column_files("tmp")[idx.first]);
        idx = pdcgs[0]->get_column_idx(6);
        EXPECT_TRUE("tmp/bbb.cols" == pdcgs[0]->column_files("tmp")[idx.first]);
        idx = pdcgs[0]->get_column_idx(7);
        EXPECT_TRUE("tmp/ccc.cols" == pdcgs[0]->column_files("tmp")[idx.first]);
        idx = pdcgs[0]->get_column_idx(8);
        EXPECT_TRUE("tmp/bbb.cols" == pdcgs[0]->column_files("tmp")[idx.first]);
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
        std::vector<std::string> orphan_files;
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
        std::vector<std::string> orphan_files;
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
        std::vector<std::string> orphan_files;
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

} // namespace starrocks::lake
