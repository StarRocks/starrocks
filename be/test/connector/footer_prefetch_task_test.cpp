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

#include "connector/footer_prefetch_task.h"

#include <gtest/gtest.h>

#include <memory>

#include "connector/hive_connector.h"
#include "exec/pipeline/scan/footer_prefetch_state.h"
#include "fs/fs.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::connector {

class FooterPrefetchTaskTest : public ::testing::Test {
public:
    void SetUp() override { _exec_env = ExecEnv::GetInstance(); }

protected:
    ExecEnv* _exec_env = nullptr;
};

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
// stay false. Asserting an actual PageCache/BlockCache write requires the global DataCache
// singleton to be wired, which a shared unit-test binary cannot do without leaking state.
TEST_F(FooterPrefetchTaskTest, WarmFooterSmokeParsesRealParquetWithoutCache) {
    const std::string path = "./be/test/exec/test_data/parquet_scanner/col_not_null.parquet";
    auto size_or = FileSystem::Default()->get_file_size(path);
    ASSERT_TRUE(size_or.ok()) << size_or.status();

    auto open_ctx = std::make_shared<pipeline::FooterOpenContext>();
    // Non-owning handle to the process-wide default FS; datacache_options left default
    // (enable_datacache == false) so create_random_access_file returns a plain file.
    open_ctx->fs = std::shared_ptr<FileSystem>(FileSystem::Default(), [](FileSystem*) {});
    open_ctx->case_sensitive = false;

    pipeline::FooterPrefetchItem item;
    item.key = path;
    item.path = path;
    item.file_size = static_cast<int64_t>(size_or.value());
    item.modification_time = 0;
    item.open_ctx = open_ctx;

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
    HiveDataSourceProvider provider(nullptr, 0, hdfs_scan_node);

    TUniqueId fragment_id;
    TQueryOptions query_options;
    query_options.__set_enable_file_metacache(false);
    TQueryGlobals query_globals;
    auto runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals,
                                                        &_exec_env->query_execution_services(), _exec_env);

    THdfsScanRange hdfs;
    hdfs.__set_file_format(THdfsFileFormat::PARQUET);
    hdfs.__set_full_path("/tmp/x.parquet");
    hdfs.__set_file_length(1024);
    TScanRangeParams params;
    params.scan_range.__set_hdfs_scan_range(hdfs);

    FooterPrefetchPlan plan = provider.build_footer_prefetch_items(runtime_state.get(), {params});
    EXPECT_TRUE(plan.items.empty());
    EXPECT_FALSE(plan.metacache_on);
    EXPECT_FALSE(plan.datacache_populate_on);
}

} // namespace starrocks::connector
