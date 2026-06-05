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

#ifdef USE_STAROS
#include "storage/lake/tablet_warmup_manager.h"

#include <gtest/gtest.h>

#include <random>

#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/versioned_tablet.h"
#include "test_util.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "testutil/scoped_updater.h"
#include "util/await.h"
#include "util/filesystem_util.h"
#include "util/threadpool.h"
#include "util/thrift_rpc_helper.h"

// NOLINTNEXTLINE
#include "service/staros_worker.h"

namespace starrocks {
extern std::unique_ptr<staros::starlet::Starlet> g_starlet;
extern std::shared_ptr<StarOSWorker> g_worker;
} // namespace starrocks

namespace starrocks::lake {

class TabletWarmupManagerTest : public testing::Test {
public:
    TabletWarmupManagerTest() = default;
    ~TabletWarmupManagerTest() override = default;
    void SetUp() override {
        // mock thrift rpc failure
        ThriftRpcHelper::setup(nullptr);

        std::vector<starrocks::StorePath> paths;
        CHECK_OK(starrocks::parse_conf_store_paths(starrocks::config::storage_root_path, &paths));
        const testing::TestInfo* const test_info = testing::UnitTest::GetInstance()->current_test_info();
        _test_dir = paths[0].path + "/lake_" + test_info->name();
        _location_provider = std::make_shared<lake::FixedLocationProvider>(_test_dir);
        _update_starlet_cache_config =
                std::make_unique<ScopedUpdater<std::string>>(config::starlet_cache_dir, _test_dir);

        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->metadata_root_location(1)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->txn_log_root_location(1)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->segment_root_location(1)));

        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_mgr = std::make_unique<TabletManager>(_location_provider, _update_manager.get(), 0);
        _warmup_mgr = _tablet_mgr->tablet_warmup_mgr();

        srand(time(NULL));
        _worker_id = random();
        // g_worker doesn't have add_shard_listner registered
        g_worker = std::make_shared<StarOSWorker>();
        g_worker->set_worker_id(_worker_id);
        staros::WorkerGroupProperty property;
        property.set_warmup_level(staros::WarmupLevel::WARMUP_ALL);
        g_worker->set_worker_group_property(property);
        g_starlet = std::make_unique<staros::starlet::Starlet>(g_worker);
    }

    void TearDown() override {
        _tablet_mgr->stop();
        _tablet_mgr.reset();
        g_starlet.reset();
        g_worker.reset();

        FileSystem::Default()->delete_dir_recursive(_test_dir);
        _update_starlet_cache_config.reset();
        // reset the env setup
        ThriftRpcHelper::setup(ExecEnv::GetInstance());
    }

    StarOSWorker::ShardInfo generateShardInfo(int64_t tablet_id, bool enable_warmup, int64_t partition_id = -1) {
        StarOSWorker::ShardInfo info;
        info.id = static_cast<StarOSWorker::ShardId>(tablet_id);
        info.cache_info.set_enable_cache(enable_warmup);
        ::staros::ReplicaInfoLite replicaInfo;
        replicaInfo.set_worker_id(_worker_id);
        replicaInfo.set_replica_state(::staros::ReplicaState::REPLICA_SCALE_OUT);
        info.replicas.push_back(replicaInfo);
        if (partition_id != -1) {
            info.properties.emplace("partitionId", std::to_string(partition_id));
        }
        return info;
    }

public:
    std::string _test_dir;
    std::shared_ptr<lake::LocationProvider> _location_provider = nullptr;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<lake::UpdateManager> _update_manager;
    uint64_t _worker_id;

    std::unique_ptr<TabletManager> _tablet_mgr;
    TabletWarmupManager* _warmup_mgr;
    std::unique_ptr<ScopedUpdater<std::string>> _update_starlet_cache_config;
};

namespace {
struct TabletContext {
    int64_t tablet_id = 0;
    int64_t partition_id = 0;
    int visible_version = 1;
};

static int random_pick_index(std::mt19937& gen, int num_per_shard, int shard_index) {
    return gen() % num_per_shard + shard_index * num_per_shard;
}

static void write_data_and_increase_visible_version(TabletContext& ctx, TabletManager* mgr) {
    std::random_device rd;
    std::mt19937 gen(rd());

    auto tablet_meta = generate_simple_tablet_metadata(DUP_KEYS);
    auto tablet_schema = TabletSchema::create(tablet_meta->schema());
    auto schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));
    tablet_meta->set_id(ctx.tablet_id);

    // generate random data
    int rows = gen() % 200 + 10;
    std::vector<int> k0;
    std::vector<int> v0;
    k0.reserve(rows);
    v0.reserve(rows);
    for (int i = 0; i < rows; ++i) {
        k0.push_back(gen());
        v0.push_back(gen());
    }

    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));
    Chunk chunk0({c0, c1}, schema);

    VersionedTablet tablet(mgr, tablet_meta);
    {
        int64_t txn_id = next_id();
        // write rowset 1 with 2 segments
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
        ASSERT_OK(writer->open());

        // write rowset data
        // segment #1
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->finish());

        // segment #2
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->finish());

        auto files = writer->segments();

        // add rowset metadata
        auto* rowset = tablet_meta->add_rowsets();
        rowset->set_overlapped(true);
        rowset->set_id(1);
        uint32_t segment_index = 0;
        for (const auto& file : files) {
            file.to_proto(segment_index++, rowset->add_segment_metas());
        }
        writer->close();
    }

    // write tablet metadata
    auto visible_version = next_id();
    tablet_meta->set_version(visible_version);
    CHECK_OK(mgr->put_tablet_metadata(*tablet_meta));
    ctx.visible_version = visible_version;
}
} // namespace

TEST_F(TabletWarmupManagerTest, test_init_and_stop) {
    auto mgr = std::make_unique<TabletWarmupManager>(nullptr);
    mgr->init();
    mgr->stop();
    mgr.reset();
}

TEST_F(TabletWarmupManagerTest, test_need_warmup) {
    int64_t tablet_id = 9527;
    // tablet info not exist
    EXPECT_FALSE(staros_need_warmup_tablet(tablet_id).ok());
    auto shardInfo = generateShardInfo(tablet_id, false);
    auto st = g_worker->add_shard(shardInfo);
    EXPECT_TRUE(st.ok()) << st;
    // still not need warmup because of shardInfo
    EXPECT_FALSE(staros_need_warmup_tablet(tablet_id).ok());

    shardInfo = generateShardInfo(tablet_id, true);
    st = g_worker->add_shard(shardInfo);
    EXPECT_TRUE(st.ok()) << st;
    // need warmup
    EXPECT_TRUE(staros_need_warmup_tablet(tablet_id).ok());
}

TEST_F(TabletWarmupManagerTest, test_manager_stopped) {
    // stop the manager
    _warmup_mgr->stop();
    auto future = _warmup_mgr->warmup_tablet2(1324);
    auto st = future.get();
    EXPECT_TRUE(st.is_aborted());
    EXPECT_EQ("warmup manager stopped!", st.message());
}

TEST_F(TabletWarmupManagerTest, test_tablet_abnormal_path) {
    { // invalid tablet id
        auto future = _warmup_mgr->warmup_tablet2(-1L);
        auto st = future.get();
        EXPECT_TRUE(st.is_invalid_argument()) << st;
    }
    { // the tablet is not found the from worker
        auto future = _warmup_mgr->warmup_tablet2(1L);
        EXPECT_FALSE(staros_need_warmup_tablet(1L).ok());
        auto st = future.get();
        EXPECT_TRUE(st.ok()) << st;
    }
    {
        int64_t tablet_id = 1024;
        auto shard_info = generateShardInfo(tablet_id, true);
        auto st = g_worker->add_shard(shard_info);
        EXPECT_TRUE(st.ok()) << st;

        std::vector<std::shared_future<Status>> results;
        int repeats = 20;
        for (int i = 0; i < repeats; ++i) {
            results.emplace_back(_warmup_mgr->warmup_tablet2(tablet_id));
        }
        int count_abort = 0;
        for (auto& future : results) {
            auto st = future.get();
            if (st.is_aborted()) {
                // abort due to duplicate tablet id
                EXPECT_EQ("duplicate tablet id found in warmup", st.message());
                ++count_abort;
            }
        }
        // 0 < count_abort
        EXPECT_LT(0, count_abort);
    }
}

TEST_F(TabletWarmupManagerTest, test_get_tablet_visible_version) {
    { // has no partition_id
        auto tablet_id = next_id();
        auto partition_id = -1;
        auto shard_info = generateShardInfo(tablet_id, true, partition_id);
        auto st = g_worker->add_shard(shard_info);
        EXPECT_TRUE(st.ok()) << st;
        auto future = _warmup_mgr->warmup_tablet2(tablet_id);
        auto ret = future.get();
        EXPECT_TRUE(ret.is_thrift_rpc_error()) << ret;
    }
    { // has valid partition_id, but cache miss in the visible version
        auto tablet_id = next_id();
        auto partition_id = next_id();
        auto shard_info = generateShardInfo(tablet_id, true, partition_id);
        auto st = g_worker->add_shard(shard_info);
        EXPECT_TRUE(st.ok()) << st;
        auto future = _warmup_mgr->warmup_tablet2(tablet_id);
        auto ret = future.get();
        EXPECT_TRUE(ret.is_thrift_rpc_error()) << ret;
    }
    { // get the visible version from cache
        auto tablet_id = next_id();
        auto partition_id = next_id();

        auto* cache = _warmup_mgr->TEST_partition_version_cache();
        // visible version to 0x1, no need to warmup
        cache->put(partition_id, 0x01);

        auto shard_info = generateShardInfo(tablet_id, true, partition_id);
        auto st = g_worker->add_shard(shard_info);
        EXPECT_TRUE(st.ok()) << st;

        auto future = _warmup_mgr->warmup_tablet2(tablet_id);
        auto ret = future.get();
        EXPECT_TRUE(ret.ok()) << ret;
    }
}

TEST_F(TabletWarmupManagerTest, test_batch_get_partitions_meta_from_frontend) {
    SyncPoint::GetInstance()->EnableProcessing();
    // pretend the frontend thrift rpc success
    SyncPoint::GetInstance()->SetCallBack("TabletWarmupManager::batch_get_partitions_meta.frontendrpc",
                                          [](void* arg) { *(Status*)arg = Status::OK(); });

    auto tablet_id = next_id();
    auto partition_id = next_id();
    auto shard_info = generateShardInfo(tablet_id, true, partition_id);
    auto st = g_worker->add_shard(shard_info);
    EXPECT_TRUE(st.ok()) << st;
    auto future = _warmup_mgr->warmup_tablet2(tablet_id);
    auto ret = future.get();
    EXPECT_TRUE(ret.ok()) << ret;

    SyncPoint::GetInstance()->ClearCallBack("TabletWarmupManager::batch_get_partitions_meta.frontendrpc");
    SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(TabletWarmupManagerTest, test_do_warmup_tablet_metadata_not_found) {
    auto tablet_id = next_id();
    auto partition_id = next_id();

    auto* cache = _warmup_mgr->TEST_partition_version_cache();
    // visible version to 0x0a
    cache->put(partition_id, 0x0a);

    auto shard_info = generateShardInfo(tablet_id, true, partition_id);
    auto st = g_worker->add_shard(shard_info);
    EXPECT_TRUE(st.ok()) << st;

    auto future = _warmup_mgr->warmup_tablet2(tablet_id);
    auto ret = future.get();
    EXPECT_TRUE(ret.is_not_found()) << ret;
}

TEST_F(TabletWarmupManagerTest, test_do_warmup_tablet_done) {
    TabletContext ctx{next_id(), next_id(), 1};

    write_data_and_increase_visible_version(ctx, _tablet_mgr.get());
    auto* cache = _warmup_mgr->TEST_partition_version_cache();
    cache->put(ctx.partition_id, ctx.visible_version);

    auto shard_info = generateShardInfo(ctx.tablet_id, true, ctx.partition_id);
    auto st = g_worker->add_shard(shard_info);
    EXPECT_TRUE(st.ok()) << st;

    auto future = _warmup_mgr->warmup_tablet2(ctx.tablet_id);
    auto ret = future.get();
    EXPECT_TRUE(ret.ok()) << ret;
}

TEST_F(TabletWarmupManagerTest, test_batch_report_tablet_replica_status) {
    std::vector<uint64_t> reported_tablet_ids;
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack(
            "TabletWarmupManager::batch_report_status.starlet_report", [&reported_tablet_ids](void* arg) {
                auto v = (std::vector<uint64_t>*)arg;
                reported_tablet_ids.insert(reported_tablet_ids.begin(), v->begin(), v->end());
            });

    std::vector<uint64_t> warmup_tablet_ids;
    int repeats = 10;
    { // get the visible version from cache
        std::vector<std::shared_future<Status>> results;
        for (int i = 0; i < repeats; ++i) {
            auto tablet_id = next_id();
            auto partition_id = next_id();
            TabletContext ctx{tablet_id, partition_id, 1};
            write_data_and_increase_visible_version(ctx, _tablet_mgr.get());

            auto* cache = _warmup_mgr->TEST_partition_version_cache();
            cache->put(ctx.partition_id, ctx.visible_version);

            auto shard_info = generateShardInfo(tablet_id, true, partition_id);
            auto st = g_worker->add_shard(shard_info);
            EXPECT_TRUE(st.ok()) << st;

            auto future = _warmup_mgr->warmup_tablet2(tablet_id);
            results.emplace_back(std::move(future));
            warmup_tablet_ids.push_back(tablet_id);
        }

        for (auto& future : results) {
            auto ret = future.get();
            EXPECT_TRUE(ret.ok()) << ret;
        }
    }
    Awaitility().timeout(2 * 1000000).until([&] { return reported_tablet_ids.size() == repeats; });

    // all should reports their status
    EXPECT_EQ(repeats, warmup_tablet_ids.size());
    std::set<uint64_t> set1(reported_tablet_ids.begin(), reported_tablet_ids.end());
    std::set<uint64_t> set2(warmup_tablet_ids.begin(), warmup_tablet_ids.end());
    EXPECT_EQ(set1, set2);

    SyncPoint::GetInstance()->ClearCallBack("TabletWarmupManager::batch_report_status.starlet_report");
    SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(TabletWarmupManagerTest, test_chaos_concurrent) {
    // Do the following things concurrently
    // * g_worker->add_shard()
    // * g_worker->remove_shard()
    // * write tablet data
    // * increase tablet version
    // * warmup tablet
    // * stop the warmupMgr in the middle
    // Check the system robust. Expect no crash, can exit cleanly
    _warmup_mgr->TEST_set_schedule_sleep_ms(1);
    std::atomic<bool> stopped = false;
    std::mutex list_mutex;
    std::list<std::shared_future<Status>> results;

    std::random_device rd;
    std::mt19937 gen(rd());

    int tablet_num = 256;
    std::vector<TabletContext> tablet_ctxs;

    tablet_ctxs.reserve(tablet_num);
    for (int i = 0; i < tablet_num; ++i) {
        TabletContext ctx = {next_id(), next_id(), 1};
        tablet_ctxs.push_back(std::move(ctx));
    }

    std::vector<std::unique_ptr<std::thread>> threads;

    // ADD SHARD
    int thread_add_shard = 4;
    for (int i = 0; i < thread_add_shard; ++i) {
        auto t = std::make_unique<std::thread>([&, index = i, num_per_shard = tablet_num / thread_add_shard] {
            while (!stopped) {
                int op_num = gen() % num_per_shard / 2 + 1;
                while (op_num-- > 0) {
                    auto pick_index = random_pick_index(gen, num_per_shard, index);
                    auto& ctx = tablet_ctxs[pick_index];
                    auto info = generateShardInfo(ctx.tablet_id, true, ctx.partition_id);
                    auto st = g_worker->add_shard(info);
                    EXPECT_TRUE(st.ok()) << st;
                }
                // sleep [10us, 50us) randomly
                std::this_thread::sleep_for(std::chrono::microseconds(gen() % 40 + 10));
            }
        });
        threads.push_back(std::move(t));
    }

    // REMOVE SHARD
    int thread_remove_shard = 2;
    for (int i = 0; i < thread_remove_shard; ++i) {
        auto t = std::make_unique<std::thread>([&, index = i, num_per_shard = tablet_num / thread_remove_shard] {
            while (!stopped) {
                int op_num = gen() % num_per_shard / 2 + 1;
                while (op_num-- > 0) {
                    auto pick_index = random_pick_index(gen, num_per_shard, index);
                    auto st = g_worker->remove_shard(tablet_ctxs[pick_index].tablet_id);
                    EXPECT_TRUE(st.ok()) << st;
                }
                // sleep [10us, 50us) randomly
                std::this_thread::sleep_for(std::chrono::microseconds(gen() % 40 + 10));
            }
        });
        threads.push_back(std::move(t));
    }

    // WARMUP TABLET
    int thread_warmup_tablet = 4;
    for (int i = 0; i < thread_warmup_tablet; ++i) {
        auto t = std::make_unique<std::thread>([&, index = i, num_per_shard = tablet_num / thread_warmup_tablet] {
            while (!stopped) {
                int op_num = gen() % num_per_shard / 2 + 1;
                std::vector<std::shared_future<Status>> futures;
                while (op_num-- > 0) {
                    auto pick_index = random_pick_index(gen, num_per_shard, index);
                    if (gen() % 10 == 0) {
                        auto future = _warmup_mgr->warmup_tablet2(tablet_ctxs[pick_index].tablet_id);
                        futures.emplace_back(future);
                    } else {
                        _warmup_mgr->warmup_tablet(tablet_ctxs[pick_index].tablet_id);
                    }
                }
                if (!futures.empty()) { // collect the pending results
                    std::unique_lock lock(list_mutex);
                    results.insert(results.end(), futures.begin(), futures.end());
                }
                // sleep [10us, 50us) randomly
                std::this_thread::sleep_for(std::chrono::microseconds(gen() % 40 + 10));
            }
        });
        threads.push_back(std::move(t));
    }

    // WRITE TABLET DATA and bump visible version
    int thread_write_data = 16;
    for (int i = 0; i < thread_write_data; ++i) {
        auto t = std::make_unique<std::thread>([&, index = i, num_per_shard = tablet_num / thread_write_data] {
            auto* cache = _warmup_mgr->TEST_partition_version_cache();
            while (!stopped) {
                int op_num = gen() % num_per_shard / 2 + 1;
                while (op_num-- > 0) {
                    auto pick_index = random_pick_index(gen, num_per_shard, index);
                    auto& ctx = tablet_ctxs[pick_index];
                    write_data_and_increase_visible_version(ctx, _tablet_mgr.get());
                    cache->put(ctx.partition_id, ctx.visible_version);
                }
                // sleep [10us, 50us) randomly
                std::this_thread::sleep_for(std::chrono::microseconds(gen() % 40 + 10));
            }
        });
        threads.push_back(std::move(t));
    }

    // let the monkeys play for a while
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    // async stop the _warmup_mgr
    threads.push_back(std::make_unique<std::thread>([&] { _warmup_mgr->stop(); }));
    // notify all threads to stop
    stopped = true;
    for (auto& t : threads) {
        t->join();
    }
    // verify all the futures can get results
    for (auto& future : results) {
        (void)future.get();
    }
}

} // namespace starrocks::lake
#endif // USE_STAROS
