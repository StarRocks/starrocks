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

#include "connector/hive/footer_prefetch_task.h"

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "base/testutil/assert.h"
#include "cache/datacache.h"
#include "cache/disk_cache/test_cache_utils.h"
#include "cache/mem_cache/lrucache_engine.h"
#include "cache/mem_cache/page_cache.h"
#include "common/config_cache_fwd.h"
#include "common/object_pool.h"
#include "connector/hive/hive_connector.h"
#include "exec_primitive/pipeline/scan/footer_prefetch_state.h"
#include "fs/fs.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/descriptors_ext.h"
#include "runtime/exec_env_fwd.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks::connector {

namespace {

constexpr const char* kParquetFile = "./be/test/exec/test_data/parquet_scanner/col_not_null.parquet";

std::shared_ptr<pipeline::FooterOpenContext> make_default_fs_open_ctx() {
    auto open_ctx = std::make_shared<pipeline::FooterOpenContext>();
    // Non-owning handle to the process-wide default FS; datacache_options default to disabled.
    open_ctx->fs = std::shared_ptr<FileSystem>(FileSystem::Default(), [](FileSystem*) {});
    open_ctx->case_sensitive = false;
    return open_ctx;
}

pipeline::FooterPrefetchItem make_local_item(const std::shared_ptr<pipeline::FooterOpenContext>& open_ctx,
                                             const std::string& path, int64_t file_size) {
    pipeline::FooterPrefetchItem item;
    item.key = path;
    item.path = path;
    item.file_size = file_size;
    item.modification_time = 0;
    item.open_ctx = open_ctx;
    return item;
}

TScanRangeParams make_parquet_range(const std::string& full_path, int64_t len = 1024) {
    THdfsScanRange hdfs;
    hdfs.__set_file_format(THdfsFileFormat::PARQUET);
    if (!full_path.empty()) {
        hdfs.__set_full_path(full_path);
    }
    hdfs.__set_file_length(len);
    TScanRangeParams params;
    params.scan_range.__set_hdfs_scan_range(hdfs);
    return params;
}

std::shared_ptr<RuntimeState> make_state_with_empty_desc_tbl(ObjectPool* pool) {
    TUniqueId fragment_id;
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    // No ExecEnv: the builder reads only query options and the descriptor table.
    auto state =
            std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, static_cast<ExecEnv*>(nullptr));
    DescriptorTbl* tbl = nullptr;
    CHECK(DescriptorTbl::create(state.get(), pool, TDescriptorTable(), &tbl, 4096).ok());
    state->set_desc_tbl(tbl);
    return state;
}

} // namespace

class FooterPrefetchTaskTest : public ::testing::Test {};

// footer_prefetch_key prefers full_path (the native object path) when present.
TEST_F(FooterPrefetchTaskTest, FooterPrefetchKeyUsesFullPath) {
    THdfsScanRange hdfs;
    hdfs.__set_full_path("/data/a/b.parquet");
    TScanRange scan_range;
    scan_range.__set_hdfs_scan_range(hdfs);
    EXPECT_EQ("/data/a/b.parquet", footer_prefetch_key(scan_range));
}

// Without full_path, the key falls back to partition_id + ':' + relative_path -- the same
// identity create_chunk_source derives, so the real scan can mark the file Started.
TEST_F(FooterPrefetchTaskTest, FooterPrefetchKeyFallsBackToPartitionRelative) {
    THdfsScanRange hdfs;
    hdfs.__set_partition_id(7);
    hdfs.__set_relative_path("part=1/c.parquet");
    TScanRange scan_range;
    scan_range.__set_hdfs_scan_range(hdfs);
    EXPECT_EQ("7:part=1/c.parquet", footer_prefetch_key(scan_range));
}

// A bad/empty item (no open context, zero size) is a no-op -- prefetch must never poison the
// real scan, so warm_footer swallows it and warms nothing.
TEST_F(FooterPrefetchTaskTest, WarmFooterNoOpOnEmptyItem) {
    pipeline::FooterPrefetchItem item; // open_ctx == nullptr, file_size == 0
    FooterWarmResult result = warm_footer(item, /*metacache_on=*/true);
    EXPECT_FALSE(result.wrote_pagecache);
    EXPECT_FALSE(result.wrote_blockcache);
}

// Smoke test of the warm path end to end on a real Parquet file: open the file the same way
// the scan does and run get_file_metadata. With no page cache (metacache off) and datacache
// disabled in the open context, the footer is parsed but warmed nowhere, so both counters
// stay false.
TEST_F(FooterPrefetchTaskTest, WarmFooterSmokeParsesRealParquetWithoutCache) {
    auto size_or = FileSystem::Default()->get_file_size(kParquetFile);
    ASSERT_TRUE(size_or.ok()) << size_or.status();

    auto open_ctx = make_default_fs_open_ctx();
    auto item = make_local_item(open_ctx, kParquetFile, static_cast<int64_t>(size_or.value()));

    FooterWarmResult result = warm_footer(item, /*metacache_on=*/false);
    EXPECT_FALSE(result.wrote_pagecache);
    EXPECT_FALSE(result.wrote_blockcache);
}

// The gate: when no cache can hold a warmed footer the builder returns an empty plan, even
// for a valid Parquet range. enable_file_metacache=false forces the PageCache branch off
// regardless of whether the test binary happens to have a global page cache, and scan
// datacache is left disabled -- so neither cache is warmable and we short-circuit.
TEST_F(FooterPrefetchTaskTest, BuildItemsEmptyWhenNoWarmableCache) {
    THdfsScanNode hdfs_scan_node;
    hdfs_scan_node.__set_tuple_id(0);
    HiveDataSourceProvider provider(0, hdfs_scan_node);

    TUniqueId fragment_id;
    TQueryOptions query_options;
    query_options.__set_enable_file_metacache(false);
    TQueryGlobals query_globals;
    // No ExecEnv: with metacache forced off and scan datacache unset, the builder short-circuits
    // on the warmable-cache check before touching anything env-backed.
    auto runtime_state =
            std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, static_cast<ExecEnv*>(nullptr));

    FooterPrefetchPlan plan =
            provider.build_footer_prefetch_items(runtime_state.get(), {make_parquet_range("/tmp/x.parquet")});
    EXPECT_TRUE(plan.items.empty());
    EXPECT_FALSE(plan.metacache_on);
    EXPECT_FALSE(plan.datacache_populate_on);
}

// Warm tests against real cache instances installed into the process-wide DataCache singleton.
// TearDown detaches them so the rest of the binary keeps running cache-less.
class FooterPrefetchWarmCacheTest : public ::testing::Test {
protected:
    void install_page_cache() {
        MemCacheOptions opts{.mem_space_size = 64 * MB};
        _lru_engine = std::make_shared<LRUCacheEngine>();
        ASSERT_OK(_lru_engine->init(opts));
        _page_cache = std::make_shared<StoragePageCache>(_lru_engine.get());
        DataCache::GetInstance()->set_page_cache(_page_cache);
    }

    void TearDown() override {
        DataCache::GetInstance()->set_page_cache(nullptr);
        DataCache::GetInstance()->set_block_cache(nullptr);
    }

    std::shared_ptr<LRUCacheEngine> _lru_engine;
    std::shared_ptr<StoragePageCache> _page_cache;
};

// The feature's core claim, metacache side: a warm parses the footer and inserts the
// FileMetaData into PageCache; a second warm of the same file is a cache hit and writes
// nothing (no wasted work).
TEST_F(FooterPrefetchWarmCacheTest, WarmFooterPopulatesPageCacheOnceThenHits) {
    install_page_cache();
    auto size_or = FileSystem::Default()->get_file_size(kParquetFile);
    ASSERT_TRUE(size_or.ok()) << size_or.status();

    auto open_ctx = make_default_fs_open_ctx();
    auto item = make_local_item(open_ctx, kParquetFile, static_cast<int64_t>(size_or.value()));

    FooterWarmResult first = warm_footer(item, /*metacache_on=*/true);
    EXPECT_TRUE(first.wrote_pagecache);
    EXPECT_FALSE(first.wrote_blockcache);

    FooterWarmResult second = warm_footer(item, /*metacache_on=*/true);
    EXPECT_FALSE(second.wrote_pagecache);
    EXPECT_FALSE(second.wrote_blockcache);
}

#ifdef WITH_STARCACHE
// The feature's core claim, datacache side: with a live BlockCache and populate enabled in the
// open context, the footer-tail read populates the block cache; a re-warm reads from cache and
// writes nothing.
TEST_F(FooterPrefetchWarmCacheTest, WarmFooterPopulatesBlockCacheOnceThenHits) {
    auto cache_options = TestCacheUtils::create_simple_options(config::datacache_block_size, 50 * MB);
    auto block_cache = TestCacheUtils::create_cache(cache_options);
    ASSERT_NE(block_cache, nullptr);
    DataCache::GetInstance()->set_block_cache(block_cache);

    auto size_or = FileSystem::Default()->get_file_size(kParquetFile);
    ASSERT_TRUE(size_or.ok()) << size_or.status();

    auto open_ctx = make_default_fs_open_ctx();
    open_ctx->datacache_options.enable_datacache = true;
    open_ctx->datacache_options.enable_populate_datacache = true;
    // Sync populate so the write is observable right after warm_footer returns.
    open_ctx->datacache_options.enable_datacache_async_populate_mode = false;
    auto item = make_local_item(open_ctx, kParquetFile, static_cast<int64_t>(size_or.value()));

    FooterWarmResult first = warm_footer(item, /*metacache_on=*/false);
    EXPECT_TRUE(first.wrote_blockcache);
    EXPECT_FALSE(first.wrote_pagecache);

    FooterWarmResult second = warm_footer(item, /*metacache_on=*/false);
    EXPECT_FALSE(second.wrote_blockcache);
    EXPECT_FALSE(second.wrote_pagecache);
}

// Builder tests need a warmable cache to get past the early gate; plan.metacache_on is only
// computed in WITH_STARCACHE builds, so they live under the same guard. Each skip condition
// mirrors an _init_scanner branch: drift between the two means warming files the real scan
// will not read that way.
class FooterPrefetchBuildItemsTest : public FooterPrefetchWarmCacheTest {
protected:
    void SetUp() override { install_page_cache(); }
};

TEST_F(FooterPrefetchBuildItemsTest, SkipsRangesTheNativeParquetReaderWillNotServe) {
    ObjectPool pool;
    auto state = make_state_with_empty_desc_tbl(&pool);
    THdfsScanNode hdfs_scan_node;
    hdfs_scan_node.__set_tuple_id(0);
    HiveDataSourceProvider provider(0, hdfs_scan_node);

    std::vector<TScanRangeParams> ranges;
    ranges.push_back(make_parquet_range("/tmp/warm_a.parquet"));
    // Non-parquet: no footer to warm.
    {
        auto r = make_parquet_range("/tmp/warm_b.orc");
        r.scan_range.hdfs_scan_range.__set_file_format(THdfsFileFormat::ORC);
        ranges.push_back(r);
    }
    // JNI-backed reader takes precedence over the native scanner: a warmed footer would never
    // be consumed.
    {
        auto r = make_parquet_range("/tmp/warm_c.parquet");
        r.scan_range.hdfs_scan_range.__set_use_hudi_jni_reader(true);
        ranges.push_back(r);
    }
    // priority == -1 keeps the range out of the data cache; don't speculatively warm it.
    {
        auto r = make_parquet_range("/tmp/warm_d.parquet");
        TDataCacheOptions dc;
        dc.__set_priority(-1);
        r.scan_range.hdfs_scan_range.__set_datacache_options(dc);
        ranges.push_back(r);
    }
    // Unresolvable path (no full_path, no partition info): skipped, not an error.
    {
        THdfsScanRange hdfs;
        hdfs.__set_file_format(THdfsFileFormat::PARQUET);
        hdfs.__set_file_length(1024);
        TScanRangeParams r;
        r.scan_range.__set_hdfs_scan_range(hdfs);
        ranges.push_back(r);
    }

    FooterPrefetchPlan plan = provider.build_footer_prefetch_items(state.get(), ranges);
    EXPECT_TRUE(plan.metacache_on);
    EXPECT_FALSE(plan.datacache_populate_on);
    ASSERT_EQ(1u, plan.items.size());
    EXPECT_EQ("/tmp/warm_a.parquet", plan.items[0].key);
    EXPECT_EQ("/tmp/warm_a.parquet", plan.items[0].path);
    EXPECT_EQ(1024, plan.items[0].file_size);
    ASSERT_NE(plan.items[0].open_ctx, nullptr);
    EXPECT_NE(plan.items[0].open_ctx->fs, nullptr);
}

// Partitioned tables resolve the native path from the partition location + relative_path,
// exactly as HiveDataSource::_init_scanner does -- the warm must open the same file the scan
// will open.
TEST_F(FooterPrefetchBuildItemsTest, ResolvesPartitionedPathLikeTheRealScan) {
    ObjectPool pool;
    auto state = make_state_with_empty_desc_tbl(&pool);

    TDescriptorTableBuilder tbl_builder;
    TSlotDescriptorBuilder slot_builder;
    auto slot = slot_builder.type(LogicalType::TYPE_INT).column_name("c1").column_pos(0).nullable(true).build();
    TTupleDescriptorBuilder tuple_builder;
    tuple_builder.add_slot(slot);
    tuple_builder.build(&tbl_builder);
    DescriptorTbl* tbl = nullptr;
    ASSERT_OK(DescriptorTbl::create(state.get(), &pool, tbl_builder.desc_tbl(), &tbl, 4096));

    THdfsPartition partition;
    THdfsPartitionLocation location;
    location.__set_suffix("/tmp/warm_part7");
    partition.__set_location(location);
    std::map<int64_t, THdfsPartition> p_map;
    p_map[7] = partition;
    THdfsTable t_hdfs_table;
    t_hdfs_table.__set_partitions(p_map);
    TTableDescriptor tdesc;
    tdesc.__set_hdfsTable(t_hdfs_table);
    auto* table_desc = pool.add(new HdfsTableDescriptor(tdesc, &pool));
    tbl->get_tuple_descriptor(0)->set_table_desc(table_desc);
    state->set_desc_tbl(tbl);

    THdfsScanNode hdfs_scan_node;
    hdfs_scan_node.__set_tuple_id(0);
    HiveDataSourceProvider provider(0, hdfs_scan_node);

    THdfsScanRange hdfs;
    hdfs.__set_file_format(THdfsFileFormat::PARQUET);
    hdfs.__set_partition_id(7);
    hdfs.__set_relative_path("f.parquet");
    hdfs.__set_file_length(2048);
    TScanRangeParams params;
    params.scan_range.__set_hdfs_scan_range(hdfs);

    FooterPrefetchPlan plan = provider.build_footer_prefetch_items(state.get(), {params});
    ASSERT_EQ(1u, plan.items.size());
    EXPECT_EQ("/tmp/warm_part7/f.parquet", plan.items[0].path);
    EXPECT_EQ("7:f.parquet", plan.items[0].key);

    // Unknown partition id: skipped, not an error.
    hdfs.__set_partition_id(8);
    params.scan_range.__set_hdfs_scan_range(hdfs);
    FooterPrefetchPlan missing = provider.build_footer_prefetch_items(state.get(), {params});
    EXPECT_TRUE(missing.items.empty());
}

// FileSystem creation failure must fail soft: prefetch gives up with an empty plan and the
// real scan reports the error. This runs on the coordinator RPC thread, so no crash and no
// status propagation.
TEST_F(FooterPrefetchBuildItemsTest, FailsSoftWhenFileSystemCannotBeCreated) {
    ObjectPool pool;
    auto state = make_state_with_empty_desc_tbl(&pool);
    THdfsScanNode hdfs_scan_node;
    hdfs_scan_node.__set_tuple_id(0);
    HiveDataSourceProvider provider(0, hdfs_scan_node);

    auto params = make_parquet_range("bogus-scheme://bucket/x.parquet");
    FooterPrefetchPlan plan = provider.build_footer_prefetch_items(state.get(), {params});
    EXPECT_TRUE(plan.items.empty());
}

// One FileSystem per scan: the shared open context is built on the first call and reused by
// incremental batches, so the FS is created once, not per coordinator RPC.
TEST_F(FooterPrefetchBuildItemsTest, ReusesSharedOpenContextAcrossBatches) {
    ObjectPool pool;
    auto state = make_state_with_empty_desc_tbl(&pool);
    THdfsScanNode hdfs_scan_node;
    hdfs_scan_node.__set_tuple_id(0);
    HiveDataSourceProvider provider(0, hdfs_scan_node);

    FooterPrefetchPlan first =
            provider.build_footer_prefetch_items(state.get(), {make_parquet_range("/tmp/w1.parquet")});
    FooterPrefetchPlan second =
            provider.build_footer_prefetch_items(state.get(), {make_parquet_range("/tmp/w2.parquet")});
    ASSERT_EQ(1u, first.items.size());
    ASSERT_EQ(1u, second.items.size());
    EXPECT_EQ(first.items[0].open_ctx, second.items[0].open_ctx);
    EXPECT_EQ(first.items[0].open_ctx->fs.get(), second.items[0].open_ctx->fs.get());
}

// The lazy open-context init races the prepare thread (initial feed) against brpc handler
// threads (incremental batches); the builder serializes it with a mutex. Two concurrent calls
// on a fresh provider must agree on a single FileSystem. Most valuable under TSAN.
TEST_F(FooterPrefetchBuildItemsTest, ConcurrentBuildInitializesOneFileSystem) {
    ObjectPool pool;
    auto state = make_state_with_empty_desc_tbl(&pool);

    for (int iter = 0; iter < 100; ++iter) {
        THdfsScanNode hdfs_scan_node;
        hdfs_scan_node.__set_tuple_id(0);
        HiveDataSourceProvider provider(0, hdfs_scan_node);

        FooterPrefetchPlan plans[2];
        std::atomic<int> start{0};
        std::vector<std::thread> threads;
        threads.reserve(2);
        for (int t = 0; t < 2; ++t) {
            threads.emplace_back([&, t]() {
                start.fetch_add(1);
                while (start.load() < 2) {
                }
                plans[t] = provider.build_footer_prefetch_items(state.get(),
                                                                {make_parquet_range("/tmp/warm_conc.parquet")});
            });
        }
        for (auto& th : threads) {
            th.join();
        }
        ASSERT_EQ(1u, plans[0].items.size());
        ASSERT_EQ(1u, plans[1].items.size());
        EXPECT_EQ(plans[0].items[0].open_ctx, plans[1].items[0].open_ctx);
        EXPECT_EQ(plans[0].items[0].open_ctx->fs.get(), plans[1].items[0].open_ctx->fs.get());
    }
}
#endif // WITH_STARCACHE

} // namespace starrocks::connector
