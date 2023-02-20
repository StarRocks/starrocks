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

#include "storage/tablet_meta_manager.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <thread>

#include "storage/del_vector.h"

namespace starrocks {

namespace fs = std::filesystem;

class TabletMetaManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        fs::path tmp = fs::temp_directory_path();
        fs::path dir = tmp / "tablet_meta_manager_test";
        fs::remove_all(dir);
        CHECK(fs::create_directory(dir));
        _data_dir = std::make_unique<DataDir>(dir.string());
        Status st = _data_dir->init();
        CHECK(st.ok()) << st.to_string();
    }

    void TearDown() override {
        _data_dir.reset();
        fs::path tmp = fs::temp_directory_path();
        fs::path dir = tmp / "tablet_meta_manager_test";
        fs::remove_all(dir);
    }

    std::unique_ptr<DataDir> _data_dir;
};

// NOLINTNEXTLINE
TEST_F(TabletMetaManagerTest, test_save_load_tablet_meta) {
    TabletMetaPB meta_pb;
    meta_pb.set_table_id(1);
    meta_pb.set_tablet_id(2);
    meta_pb.set_schema_hash(3);
    meta_pb.set_shard_id(4);
    meta_pb.set_creation_time(65432);
    meta_pb.mutable_tablet_uid()->set_lo(10);
    meta_pb.mutable_tablet_uid()->set_hi(20);
    meta_pb.set_tablet_type(TabletTypePB::TABLET_TYPE_DISK);
    meta_pb.set_tablet_state(PB_RUNNING);
    meta_pb.mutable_schema()->set_keys_type(DUP_KEYS);
    auto c0 = meta_pb.mutable_schema()->add_column();
    c0->set_name("c0");
    c0->set_is_key(true);
    c0->set_type("INT");
    c0->set_index_length(4);

    auto meta = std::make_shared<TabletMeta>();
    meta->init_from_pb(&meta_pb);
    // generate necessary field
    TabletMetaPB new_meta_pb;
    meta->to_meta_pb(&new_meta_pb);

    ASSERT_TRUE(TabletMetaManager::save(_data_dir.get(), new_meta_pb).ok());
    auto load_meta = std::make_shared<TabletMeta>();
    ASSERT_TRUE(
            TabletMetaManager::get_tablet_meta(_data_dir.get(), meta->tablet_id(), meta->schema_hash(), load_meta.get())
                    .ok());
    ASSERT_EQ(1, load_meta->table_id());
    ASSERT_EQ(2, load_meta->tablet_id());
    ASSERT_EQ(3, load_meta->schema_hash());
    ASSERT_EQ(4, load_meta->shard_id());
    ASSERT_EQ(65432, load_meta->creation_time());
    ASSERT_EQ(10, load_meta->tablet_uid().lo);
    ASSERT_EQ(20, load_meta->tablet_uid().hi);
    ASSERT_EQ(TabletTypePB::TABLET_TYPE_DISK, load_meta->tablet_type());
    ASSERT_EQ(DUP_KEYS, load_meta->tablet_schema().keys_type());
    ASSERT_EQ("c0", load_meta->tablet_schema().column(0).name());
    ASSERT_EQ(true, load_meta->tablet_schema().column(0).is_key());
    ASSERT_EQ(TYPE_INT, load_meta->tablet_schema().column(0).type());

    load_meta.reset(new TabletMeta());
    auto visit_func = [&](long tablet_id, long schema_hash, std::string_view meta) -> bool {
        CHECK(load_meta->deserialize(meta).ok());
        return true;
    };
    ASSERT_TRUE(TabletMetaManager::walk(_data_dir->get_meta(), visit_func).ok());
    ASSERT_EQ(1, load_meta->table_id());
    ASSERT_EQ(2, load_meta->tablet_id());
    ASSERT_EQ(3, load_meta->schema_hash());
    ASSERT_EQ(4, load_meta->shard_id());
    ASSERT_EQ(65432, load_meta->creation_time());
    ASSERT_EQ(10, load_meta->tablet_uid().lo);
    ASSERT_EQ(20, load_meta->tablet_uid().hi);
    ASSERT_EQ(TabletTypePB::TABLET_TYPE_DISK, load_meta->tablet_type());
    ASSERT_EQ(DUP_KEYS, load_meta->tablet_schema().keys_type());
    ASSERT_EQ("c0", load_meta->tablet_schema().column(0).name());
    ASSERT_EQ(true, load_meta->tablet_schema().column(0).is_key());
    ASSERT_EQ(TYPE_INT, load_meta->tablet_schema().column(0).type());
}

// NOLINTNEXTLINE
TEST_F(TabletMetaManagerTest, delete_vector_operations) {
    const TTabletId kTabletId = 10086;
    const uint32_t kSegmentId = 10;
    auto meta = _data_dir->get_meta();

    struct TestGetDelVecCase {
        uint32_t segment_id;
        int64_t version;
        bool success;
        int64_t latest_version;
        int64_t return_version;
        int64_t cardinality;
    };

    struct TestListDelVecCase {
        int64_t version;
        bool success;
        std::vector<std::pair<uint32_t, int64_t>> result;
    };

    auto test_get_del_vec = [&](const TestGetDelVecCase& test) {
        DelVector tmp;
        int64_t latest_version;
        auto st = TabletMetaManager::get_del_vector(meta, kTabletId, test.segment_id, test.version, &tmp,
                                                    &latest_version);
        if (test.success) {
            ASSERT_TRUE(st.ok()) << st.to_string();
            EXPECT_EQ(test.latest_version, latest_version);
            EXPECT_EQ(test.return_version, tmp.version());
            EXPECT_EQ(test.cardinality, tmp.cardinality());
        } else {
            ASSERT_FALSE(st.ok());
        }
    };

    auto test_list_del_vec = [&](const TestListDelVecCase& test) {
        auto res = TabletMetaManager::list_del_vector(meta, kTabletId, test.version);
        if (test.success) {
            ASSERT_TRUE(res.ok()) << res.status().to_string();
            ASSERT_EQ(test.result, res.value());
        } else {
            ASSERT_FALSE(res.ok());
        }
    };

    auto set_del_vector = [&](TTabletId tablet_id, uint32_t segment_id, const DelVector& delvec) {
        std::cout << "set delete vector. tablet_id=" << tablet_id << " segment_id=" << segment_id
                  << " delete vector version=" << delvec.version() << std::endl;
        auto st = TabletMetaManager::set_del_vector(meta, tablet_id, segment_id, delvec);
        ASSERT_TRUE(st.ok()) << st.to_string();
    };

    auto delete_del_vector_range = [&](TTabletId tablet_id, uint32_t segment_id, int64_t begin, int64_t end) {
        std::cout << "delete delete-vector range. tablet_id=" << tablet_id << " segment_id=" << segment_id
                  << " begin=" << begin << " end=" << end << std::endl;
        return TabletMetaManager::delete_del_vector_range(meta, tablet_id, segment_id, begin, end);
    };

    // no delete vector.
    {
        test_get_del_vec({kSegmentId, 0, false, 0, 0, 0});
        test_get_del_vec({kSegmentId, 1, false, 0, 0, 0});
        test_list_del_vec({0, true, {}});
        test_list_del_vec({1, true, {}});
    }
    // Delete Vectors:
    // +--------+------------+---------+---------+
    // | tablet | segment_id | version | content |
    // +--------+------------+---------+---------+
    // |  10086 |    10      | 1       |  { }    |
    // +--------+------------+---------+---------+
    DelVectorPtr delvec_v0 = std::make_shared<DelVector>();
    delvec_v0->init(1, nullptr, 0);
    set_del_vector(kTabletId, kSegmentId, *delvec_v0);
    {
        test_get_del_vec({kSegmentId, 1, true, 1, 1, 0});
        test_get_del_vec({kSegmentId, 2, true, 1, 1, 0});
        test_list_del_vec({1, true, {}});
        test_list_del_vec({2, true, {{kSegmentId, 1}}});
    }
    // Delete Vectors:
    // +--------+------------+---------+-----------------+
    // | tablet | segment_id | version | content         |
    // +--------+------------+---------+-----------------+
    // |  10086 |    10      | 1       |  { }            |
    // +--------+------------+---------+-----------------+
    // |  10086 |    10      | 2       |  {1,2,3}        |
    // +--------+------------+---------+-----------------+
    DelVectorPtr delvec_v1;
    delvec_v0->add_dels_as_new_version({1, 2, 3}, 2, &delvec_v1);
    set_del_vector(kTabletId, kSegmentId, *delvec_v1);
    {
        test_get_del_vec({kSegmentId, 1, true, 2, 1, 0});
        test_get_del_vec({kSegmentId, 2, true, 2, 2, 3});
        test_get_del_vec({kSegmentId, 3, true, 2, 2, 3});
        test_list_del_vec({1, true, {}});
        test_list_del_vec({2, true, {{kSegmentId, 1}}});
        test_list_del_vec({3, true, {{kSegmentId, 2}}});
    }
    // Delete Vectors:
    // +--------+------------+---------+-----------------+
    // | tablet | segment_id | version | content         |
    // +--------+------------+---------+-----------------+
    // |  10086 |    10      | 1       |  { }            |
    // +--------+------------+---------+-----------------+
    // |  10086 |    10      | 2       |  {1,2,3}        |
    // +--------+------------+---------+-----------------+
    // |  10086 |    10      | 3       |  {1,2,3,7,8,9}  |
    // +--------+------------+---------+-----------------+
    DelVectorPtr delvec_v2;
    delvec_v1->add_dels_as_new_version({7, 8, 9}, 3, &delvec_v2);
    set_del_vector(kTabletId, kSegmentId, *delvec_v2);
    {
        test_get_del_vec({kSegmentId, 1, true, 3, 1, 0});
        test_get_del_vec({kSegmentId, 2, true, 3, 2, 3});
        test_get_del_vec({kSegmentId, 3, true, 3, 3, 6});
        test_list_del_vec({1, true, {}});
        test_list_del_vec({2, true, {{kSegmentId, 1}}});
        test_list_del_vec({3, true, {{kSegmentId, 2}}});
        test_list_del_vec({4, true, {{kSegmentId, 3}}});
    }
    // Delete Vectors:
    // +--------+------------+---------+-----------------+
    // | tablet | segment_id | version | content         |
    // +--------+------------+---------+-----------------+
    // |  10086 |    10      | 1       |  { }            |
    // +--------+------------+---------+-----------------+
    // |  10086 |    10      | 2       |  {1,2,3}        |
    // +--------+------------+---------+-----------------+
    // |  10086 |    10      | 3       |  {1,2,3,7,8,9}  |
    // +--------+------------+---------+-----------------+
    // |  10086 |    11      | 2       |  {1,2,3}        |
    // +--------+------------+---------+-----------------+
    // |  10086 |    12      | 3       |  {1,2,3,7,8,9}  |
    // +--------+------------+---------+-----------------+
    set_del_vector(kTabletId, kSegmentId + 1, *delvec_v1);
    set_del_vector(kTabletId, kSegmentId + 2, *delvec_v2);
    {
        test_get_del_vec({kSegmentId, 1, true, 3, 1, 0});
        test_get_del_vec({kSegmentId, 2, true, 3, 2, 3});
        test_get_del_vec({kSegmentId, 3, true, 3, 3, 6});
        test_get_del_vec({kSegmentId, 4, true, 3, 3, 6});
        test_get_del_vec({kSegmentId + 1, 4, true, 2, 2, 3});
        test_get_del_vec({kSegmentId + 2, 4, true, 3, 3, 6});

        test_list_del_vec({1, true, {}});
        test_list_del_vec({2, true, {{kSegmentId, 1}}});
        test_list_del_vec({3, true, {{kSegmentId, 2}, {kSegmentId + 1, 2}}});
        test_list_del_vec({4, true, {{kSegmentId, 3}, {kSegmentId + 1, 2}, {kSegmentId + 2, 3}}});
    }

    auto st = delete_del_vector_range(kTabletId, kSegmentId, 2, 1);
    ASSERT_FALSE(st.ok());
    st = delete_del_vector_range(kTabletId, kSegmentId, 1, 1);
    ASSERT_TRUE(st.ok()) << st.to_string();
    {
        test_get_del_vec({kSegmentId, 1, true, 3, 1, 0});
        test_get_del_vec({kSegmentId, 2, true, 3, 2, 3});
        test_get_del_vec({kSegmentId, 3, true, 3, 3, 6});
        test_get_del_vec({kSegmentId, 4, true, 3, 3, 6});
        test_get_del_vec({kSegmentId + 1, 4, true, 2, 2, 3});
        test_get_del_vec({kSegmentId + 2, 4, true, 3, 3, 6});

        test_list_del_vec({1, true, {}});
        test_list_del_vec({2, true, {{kSegmentId, 1}}});
        test_list_del_vec({3, true, {{kSegmentId, 2}, {kSegmentId + 1, 2}}});
        test_list_del_vec({4, true, {{kSegmentId, 3}, {kSegmentId + 1, 2}, {kSegmentId + 2, 3}}});
    }
    st = delete_del_vector_range(kTabletId, kSegmentId, 1, 2);
    ASSERT_TRUE(st.ok()) << st.to_string();
    {
        test_get_del_vec({kSegmentId, 1, false, 0, 0, 0});
        test_get_del_vec({kSegmentId, 2, true, 3, 2, 3});
        test_get_del_vec({kSegmentId, 3, true, 3, 3, 6});
        test_get_del_vec({kSegmentId, 4, true, 3, 3, 6});
        test_get_del_vec({kSegmentId + 1, 4, true, 2, 2, 3});
        test_get_del_vec({kSegmentId + 2, 4, true, 3, 3, 6});

        test_list_del_vec({1, true, {}});
        test_list_del_vec({2, true, {}});
        test_list_del_vec({3, true, {{kSegmentId, 2}, {kSegmentId + 1, 2}}});
        test_list_del_vec({4, true, {{kSegmentId, 3}, {kSegmentId + 1, 2}, {kSegmentId + 2, 3}}});
    }
    st = delete_del_vector_range(kTabletId, kSegmentId, 1, 11);
    ASSERT_TRUE(st.ok()) << st.to_string();
    {
        test_get_del_vec({kSegmentId, 1, false, 0, 0, 0});
        test_get_del_vec({kSegmentId, 2, false, 0, 0, 0});
        test_get_del_vec({kSegmentId, 3, false, 0, 0, 0});
        test_get_del_vec({kSegmentId, 4, false, 0, 0, 0});
        test_get_del_vec({kSegmentId + 1, 4, true, 2, 2, 3});
        test_get_del_vec({kSegmentId + 2, 4, true, 3, 3, 6});

        test_list_del_vec({1, true, {}});
        test_list_del_vec({2, true, {}});
        test_list_del_vec({3, true, {{kSegmentId + 1, 2}}});
        test_list_del_vec({4, true, {{kSegmentId + 1, 2}, {kSegmentId + 2, 3}}});
    }
}

class TabletMetaManagerPerformanceTest : public ::testing::Test {
protected:
    static constexpr int64_t kMinTabletId = 0;
    static constexpr int64_t kMaxTabletId = 100;
    static constexpr int64_t kMaxRowset = 1000;

    static void SetUpTestCase() {
        fs::path cwd = fs::current_path();
        fs::path dir = cwd / "tablet_meta_manager_performance_test";
        CHECK(fs::create_directory(dir));
        _s_data_dir = std::make_unique<DataDir>(dir.string());
        Status st = _s_data_dir->init();
        CHECK(st.ok()) << st.to_string();

        std::atomic<TTabletId> next_tablet_id{kMinTabletId};

        auto rowset_commit_thread = [&]() {
            TTabletId tablet_id;
            while ((tablet_id = next_tablet_id.fetch_add(1)) < kMaxTabletId) {
                LOG(INFO) << "Committing rowset for tablet " << tablet_id;
                auto t0 = std::chrono::steady_clock::now();
                for (int i = 0; i < kMaxRowset; i++) {
                    RowsetMetaPB rowset_meta_pb;
                    rowset_meta_pb.set_creation_time(time(nullptr));
                    rowset_meta_pb.set_start_version(i);
                    rowset_meta_pb.set_end_version(i);
                    rowset_meta_pb.set_num_rows(10000);
                    rowset_meta_pb.set_num_segments(1);
                    rowset_meta_pb.set_data_disk_size(1024 * 1024);
                    rowset_meta_pb.set_empty(false);
                    rowset_meta_pb.set_deprecated_rowset_id(0);
                    rowset_meta_pb.set_rowset_seg_id(i);
                    RowsetId id;
                    id.init(2, i, 0, 0);
                    rowset_meta_pb.set_rowset_id(id.to_string());

                    EditVersionMetaPB edit;
                    auto v = edit.mutable_version();
                    v->set_major(i + 1);
                    v->set_minor(0);
                    edit.set_creation_time(time(nullptr));
                    edit.add_rowsets_add(rowset_meta_pb.rowset_seg_id());
                    edit.add_deltas(rowset_meta_pb.rowset_seg_id());
                    edit.set_rowsetid_add(1);
                    auto logid = i;
                    auto st = TabletMetaManager::rowset_commit(_s_data_dir.get(), tablet_id, logid, &edit,
                                                               rowset_meta_pb, string());
                    CHECK(st.ok());
                }
                auto t1 = std::chrono::steady_clock::now();
                auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
                LOG(INFO) << "Committed " << kMaxRowset << " rowsets of tablet " << tablet_id << " in " << cost << "ms";
            }
        };

        auto t0 = std::chrono::steady_clock::now();
        std::vector<std::thread> threads(CpuInfo::num_cores());
        for (int i = 0; i < CpuInfo::num_cores(); i++) {
            threads[i] = std::thread(rowset_commit_thread);
        }
        for (auto& t : threads) {
            t.join();
        }
        auto t1 = std::chrono::steady_clock::now();
        auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);
        LOG(INFO) << "Finish set up test case. cost=" << cost.count() << "ms";
    }

    static void TearDownTestCase() {
        LOG(INFO) << "Tear down test case";
        _s_data_dir.reset();
        fs::path cwd = fs::current_path();
        fs::path dir = cwd / "tablet_meta_manager_performance_test";
        fs::remove_all(dir);
    }

    inline static std::unique_ptr<DataDir> _s_data_dir;
};

/*
// NOLINTNEXTLINE
TEST_F(TabletMetaManagerPerformanceTest, rowset_iterate) {
    auto t0 = std::chrono::steady_clock::now();
    auto rowset_cnt = 0;
    for (TTabletId tablet_id = kMinTabletId; tablet_id < kMaxTabletId; tablet_id++) {
        auto st = TabletMetaManager::rowset_iterate(_s_data_dir.get(), tablet_id,
                                                    [&](auto rowset_meta) {
                                                        rowset_cnt++;
                                                        return true;
                                                    });
        CHECK(st.ok()) << st.to_string();
    }
    auto t1 = std::chrono::steady_clock::now();
    auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    LOG(INFO) << "Loaded " << rowset_cnt << " rowsets in " << cost << "ms";
}
*/

class DeleteVectorPerformanceTest : public ::testing::Test {
protected:
    static constexpr const char* const kCaseName = "delete_vector_performance_test";
    static constexpr TTabletId kTabletId = 12345;
    static constexpr int64_t kMaxSegmentId = 1000;
    static constexpr int64_t kDeleteVectorsPerSegment = 10;
    static constexpr int64_t kDeleteVectorSize = 10000;

    static void SetUpTestCase() {
        fs::path tmp = fs::temp_directory_path();
        fs::path dir = tmp / kCaseName;
        CHECK(fs::create_directory(dir));
        _s_data_dir = std::make_unique<DataDir>(dir.string());
        Status st = _s_data_dir->init();
        CHECK(st.ok()) << st.to_string();

        std::vector<uint32_t> dels;
        dels.resize(kDeleteVectorSize);
        for (int i = 0; i < kDeleteVectorSize; i++) {
            dels[i] = i;
        }
        DelVector empty_delvec;

        auto t0 = std::chrono::steady_clock::now();
        for (int i = 0; i < kMaxSegmentId; i++) {
            for (int j = 0; j < kDeleteVectorsPerSegment; j++) {
                DelVectorPtr delvec;
                empty_delvec.add_dels_as_new_version(dels, j + 1, &delvec);
                st = TabletMetaManager::set_del_vector(_s_data_dir->get_meta(), kTabletId, i, *delvec);
                CHECK(st.ok()) << st.to_string();
            }
        }
        auto t1 = std::chrono::steady_clock::now();
        auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);
        LOG(INFO) << "Finish set up test case. cost=" << cost.count() << "ms";
    }

    static void TearDownTestCase() {
        LOG(INFO) << "Tear down test case";
        _s_data_dir.reset();
        fs::path tmp = fs::temp_directory_path();
        fs::path dir = tmp / kCaseName;
        fs::remove_all(dir);
    }

    inline static std::unique_ptr<DataDir> _s_data_dir;
};

/*
// NOLINTNEXTLINE
TEST_F(DeleteVectorPerformanceTest, list_del_vector) {
    auto num_delvec = 0;
    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < kDeleteVectorsPerSegment; i++) {
        auto res = TabletMetaManager::list_del_vector(_s_data_dir->get_meta(), kTabletId, i + 1);
        CHECK(res.ok()) << res.status().to_string();
        num_delvec += res->size();
    }
    auto t1 = std::chrono::steady_clock::now();
    auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);
    LOG(INFO) << "Listed " << num_delvec << " delete vectors in " << cost.count() << "ms";
}

// NOLINTNEXTLINE
TEST_F(DeleteVectorPerformanceTest, get_del_vector) {
    auto num_delvec = 0;
    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < kMaxSegmentId; i++) {
        for (int j = 0; j < kDeleteVectorsPerSegment; j++) {
            DelVector delvec;
            int64_t latest_version;
            auto st = TabletMetaManager::get_del_vector(_s_data_dir->get_tablet_meta(), kTabletId, i,
                                                        j + 1, &delvec, &latest_version);
            CHECK(st.ok()) << st.to_string();
            num_delvec++;
        }
    }
    auto t1 = std::chrono::steady_clock::now();
    auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);
    LOG(INFO) << "Fetched " << num_delvec << " delete vectors in " << cost.count() << "ms "
              << "avg=" << (double)cost.count() / num_delvec << "ms";
}
*/

/*
TEST(DeleteVectorTest, delete_del_vector) {
    fs::path dir = fs::temp_directory_path() / "delete_del_vector";
    fs::remove_all(dir);
    CHECK(fs::create_directory(dir));
    auto data_dir = std::make_unique<DataDir>(dir.string());
    Status st = data_dir->init();
    CHECK(st.ok()) << st.to_string();

    bool use_del_range = false;
    size_t tablet_size = 200;
    size_t rssid_size = 10;
    size_t delvec_size = 10000;
    std::vector<uint32_t> dels;
    dels.resize(delvec_size);
    for (int i = 0; i < delvec_size; i++) {
        dels[i] = (i == 0 ? 0 : dels[i - 1]) + rand() % 10 + 1;
    }
    {
        DelVector empty_delvec;
        auto t0 = std::chrono::steady_clock::now();
        for (int tablet_id = 1; tablet_id <= tablet_size; tablet_id++) {
            for (int rssid = 0; rssid < rssid_size; rssid++) {
                DelVectorPtr delvec;
                empty_delvec.add_dels_as_new_version(dels, 50, &delvec);
                st = TabletMetaManager::set_del_vector(data_dir->get_meta(), tablet_id, rssid, *delvec);
                CHECK(st.ok()) << st.to_string();
                DelVectorPtr delvec2;
                delvec->add_dels_as_new_version({dels.back() + 3}, 60, &delvec2);
                st = TabletMetaManager::set_del_vector(data_dir->get_meta(), tablet_id, rssid, *delvec2);
                CHECK(st.ok()) << st.to_string();
            }
        }
        auto t1 = std::chrono::steady_clock::now();
        auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);
        data_dir->get_meta()->flush();
        LOG(INFO) << "creating " << rssid_size * tablet_size << " delvecs " << cost.count() << "ms";
    }
    {
        size_t n_del_range = 0;
        auto t0 = std::chrono::steady_clock::now();
        for (int j = 0; j < 10; j++) {
            for (int tablet_id = 1; tablet_id <= tablet_size; tablet_id++) {
                if (use_del_range) {
                    for (int rssid = 0; rssid < rssid_size; rssid++) {
                        TabletMetaManager::delete_del_vector_range(data_dir->get_meta(), tablet_id, rssid, 0, 59);
                        n_del_range++;
                    }
                } else {
                    auto st = TabletMetaManager::delete_del_vector_before_version(data_dir->get_meta(), tablet_id, 60);
                    CHECK(st.ok());
                    n_del_range += st.value();
                }
            }
        }
        auto t1 = std::chrono::steady_clock::now();
        auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);
        data_dir->get_meta()->flush();
        LOG(INFO) << "perform " << n_del_range << " del_ranges " << cost.count() << "ms";
    }
    {
        auto t0 = std::chrono::steady_clock::now();
        for (int tablet_id = 1; tablet_id <= tablet_size; tablet_id++) {
            for (int rssid = 0; rssid < rssid_size; rssid++) {
                DelVector delvec;
                int64_t latest_version;
                TabletMetaManager::get_del_vector(data_dir->get_meta(), tablet_id, rssid, 99, &delvec, &latest_version);
            }
        }
        auto t1 = std::chrono::steady_clock::now();
        auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);
        LOG(INFO) << "perform " << rssid_size * tablet_size << " get_del_vector " << cost.count() << "ms";
    }
    fs::remove_all(dir);
}
*/

} // namespace starrocks
