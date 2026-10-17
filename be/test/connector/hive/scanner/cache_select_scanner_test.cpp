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

#include "connector/hive/scanner/cache_select_scanner.h"

#include <gtest/gtest.h>

#include <limits>
#include <memory>

#include "base/testutil/assert.h"
#include "cache/datacache.h"
#include "cache/disk_cache/block_cache.h"
#include "cache/disk_cache/test_cache_utils.h"
#include "column/column_helper.h"
#include "common/config_exec_fwd.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "compute_env/query/fragment_runtime_state.h"
#include "connector/hive/scanner/hdfs_scanner_orc.h"
#include "connector/hive/scanner/hdfs_scanner_parquet.h"
#include "fs/fs_memory.h"
#include "runtime/chunk_helper.h"
#include "runtime/descriptor_helper.h"
#include "runtime/runtime_state.h"

namespace starrocks {

namespace {
struct SlotDesc {
    string name;
    TypeDescriptor type;
};
} // namespace

class CacheSelectScannerTest : public ::testing::Test {
public:
    void SetUp() override { _create_runtime_state(""); }
    void TearDown() override {}

protected:
    void _create_runtime_state(const std::string& timezone);
    HdfsScannerContext* _create_ctx(const std::string& file, THdfsScanRange* range, TupleDescriptor* tuple_desc);
    THdfsScanRange* _create_scan_range(const std::string& file, uint64_t offset, uint64_t length,
                                       const THdfsFileFormat::type& type);
    TupleDescriptor* _create_tuple_desc(SlotDesc* descs);

    ObjectPool _pool;
    RuntimeState* _runtime_state = nullptr;
};

void CacheSelectScannerTest::_create_runtime_state(const std::string& timezone) {
    TUniqueId fragment_id;
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    if (timezone != "") {
        query_globals.__set_time_zone(timezone);
    }
    _runtime_state =
            _pool.add(new RuntimeState(fragment_id, query_options, query_globals, static_cast<ExecEnv*>(nullptr)));
    _runtime_state->init_instance_mem_tracker();
    auto* fragment_runtime_state = _pool.add(new pipeline::FragmentRuntimeState());
    fragment_runtime_state->set_pred_tree_params({true, true});
    _runtime_state->set_fragment_runtime_state(fragment_runtime_state);
    _runtime_state->set_fragment_dict_state(_pool.add(new FragmentDictState()));
}

THdfsScanRange* CacheSelectScannerTest::_create_scan_range(const std::string& file, uint64_t offset, uint64_t length,
                                                           const THdfsFileFormat::type& type) {
    auto* scan_range = _pool.add(new THdfsScanRange());
    uint64_t file_size = 10;
    scan_range->relative_path = file;
    scan_range->offset = offset;
    scan_range->length = length == 0 ? file_size : length;
    scan_range->file_length = file_size;
    scan_range->file_format = type;
    return scan_range;
}

HdfsScannerContext* CacheSelectScannerTest::_create_ctx(const std::string& file, THdfsScanRange* range,
                                                        TupleDescriptor* tuple_desc) {
    auto* ctx = _pool.add(new HdfsScannerContext());
    auto* lazy_column_coalesce_counter = _pool.add(new std::atomic<int32_t>(0));
    ctx->fs = FileSystem::Default();
    ctx->file_path = file;
    ctx->file_size = range->file_length;
    ctx->scan_range = range;
    ctx->tuple_desc = tuple_desc;
    ctx->runtime_filter_collector = _pool.add(new RuntimeFilterProbeCollector());
    std::vector<int> materialize_index_in_chunk;
    std::vector<int> partition_index_in_chunk;
    std::vector<SlotDescriptor*> mat_slots;
    std::vector<SlotDescriptor*> part_slots;

    for (int i = 0; i < tuple_desc->slots().size(); i++) {
        SlotDescriptor* slot = tuple_desc->slots()[i];
        if (slot->col_name().find("PART_") != std::string::npos) {
            partition_index_in_chunk.push_back(i);
            part_slots.push_back(slot);
        } else {
            materialize_index_in_chunk.push_back(i);
            mat_slots.push_back(slot);
        }
    }

    ctx->partition_index_in_chunk = partition_index_in_chunk;
    ctx->materialize_index_in_chunk = materialize_index_in_chunk;
    ctx->materialize_slots = mat_slots;
    ctx->partition_slots = part_slots;
    ctx->format_scan_context.lazy_column_coalesce_counter = lazy_column_coalesce_counter;
    return ctx;
}

TupleDescriptor* CacheSelectScannerTest::_create_tuple_desc(SlotDesc* descs) {
    TDescriptorTableBuilder table_desc_builder;
    TSlotDescriptorBuilder slot_desc_builder;
    TTupleDescriptorBuilder tuple_desc_builder;
    int slot_id = 0;
    while (descs->name != "") {
        slot_desc_builder.column_name(descs->name).type(descs->type).id(slot_id).nullable(true);
        tuple_desc_builder.add_slot(slot_desc_builder.build());
        descs += 1;
        slot_id += 1;
    }
    tuple_desc_builder.build(&table_desc_builder);
    std::vector<TTupleId> row_tuples = std::vector<TTupleId>{0};
    DescriptorTbl* tbl = nullptr;
    CHECK(DescriptorTbl::create(_runtime_state, &_pool, table_desc_builder.desc_tbl(), &tbl, config::vector_chunk_size)
                  .ok());
    auto* tuple_desc = tbl->get_tuple_descriptor(row_tuples[0]);
    return tuple_desc;
}

TEST_F(CacheSelectScannerTest, TestUnknowFormat) {
    SlotDesc slot_desc[] = {{"Id", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)}, {""}};
    auto scanner = std::make_shared<CacheSelectScanner>();
    auto* range = _create_scan_range("jni_scan_range", 0, 0, THdfsFileFormat::UNKNOWN);
    auto* tuple_desc = _create_tuple_desc(slot_desc);
    auto* ctx = _create_ctx("fake_file", range, tuple_desc);

    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());

    ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
    status = scanner->get_next(_runtime_state, &chunk);
    ASSERT_TRUE(status.is_end_of_file());
}

class CacheSelectScannerCacheTest : public CacheSelectScannerTest {
public:
    void SetUp() override {
        CacheSelectScannerTest::SetUp();
#ifdef WITH_STARCACHE
        auto options = TestCacheUtils::create_simple_options(kBlockSize, 10 * MB);
        DataCache::GetInstance()->set_block_cache(TestCacheUtils::create_cache(options));
#else
        GTEST_SKIP() << "Data cache requires StarCache";
#endif
    }

    void TearDown() override {
        DataCache::GetInstance()->set_block_cache(nullptr);
        CacheSelectScannerTest::TearDown();
    }

protected:
    static constexpr int64_t kBlockSize = 256 * 1024;
};

TEST_F(CacheSelectScannerCacheTest, DeletionVectorsWarmOnlyReferencedBlocks) {
    MemoryFileSystem fs;
    const std::string path = "/cache_select_shared.puffin";
    const int64_t file_size = 4 * kBlockSize + 31;
    ASSERT_OK(fs.create_file(path));
    ASSERT_OK(fs.append_file(path, std::string(file_size, 'x')));

    TIcebergDeleteFile delete_file;
    delete_file.__set_full_path(path);
    delete_file.__set_file_format(THdfsFileFormat::PUFFIN);
    delete_file.__set_length(file_size);
    delete_file.__set_content_offset(kBlockSize + 7);
    delete_file.__set_content_size_in_bytes(32);

    HdfsScannerContext ctx;
    ctx.fs = &fs;
    ctx.datacache_options.enable_datacache = true;
    ctx.datacache_options.enable_cache_select = true;
    ctx.table_specific.iceberg_delete_files = {&delete_file};
    CacheSelectScanner scanner;
    scanner._scanner_ctx = &ctx;

    ASSERT_OK(scanner._fetch_iceberg_delete_files());
    EXPECT_EQ(kBlockSize, scanner._fs_stats.bytes_read);

    // A second DV in the same block reuses the cache populated with the full file's key.
    delete_file.__set_content_offset(kBlockSize + 39);
    delete_file.__set_content_size_in_bytes(40);
    ASSERT_OK(scanner._fetch_iceberg_delete_files());
    EXPECT_EQ(kBlockSize, scanner._fs_stats.bytes_read);

    delete_file.__set_content_offset(2 * kBlockSize + 3);
    delete_file.__set_content_size_in_bytes(24);
    ASSERT_OK(scanner._fetch_iceberg_delete_files());
    EXPECT_EQ(2 * kBlockSize, scanner._fs_stats.bytes_read);

    delete_file.__set_content_offset(4 * kBlockSize + 7);
    ASSERT_OK(scanner._fetch_iceberg_delete_files());
    EXPECT_EQ(2 * kBlockSize + 31, scanner._fs_stats.bytes_read);
    EXPECT_EQ(4, scanner._app_stats.iceberg_delete_files_per_scan);

    ASSIGN_OR_ABORT(auto file, fs.new_random_access_file(path));
    auto input = std::make_shared<SharedBufferedInputStream>(file->stream(), path, file_size);
    CacheInputStream reader(input, path, file_size, -1);
    auto* cache = BlockCache::instance();
    EXPECT_FALSE(cache->exist(reader._cache_key, 0, kBlockSize));
    EXPECT_TRUE(cache->exist(reader._cache_key, kBlockSize, kBlockSize));
    EXPECT_TRUE(cache->exist(reader._cache_key, 2 * kBlockSize, kBlockSize));
    EXPECT_FALSE(cache->exist(reader._cache_key, 3 * kBlockSize, kBlockSize));
    EXPECT_TRUE(cache->exist(reader._cache_key, 4 * kBlockSize, 31));
}

TEST_F(CacheSelectScannerCacheTest, PositionDeleteFilesWarmEntireFile) {
    MemoryFileSystem fs;
    const int64_t file_size = 2 * kBlockSize + 17;
    for (const auto format : {THdfsFileFormat::PARQUET, THdfsFileFormat::ORC}) {
        const std::string path = "/cache_select_delete_" + std::to_string(format);
        ASSERT_OK(fs.create_file(path));
        ASSERT_OK(fs.append_file(path, std::string(file_size, 'x')));

        TIcebergDeleteFile delete_file;
        delete_file.__set_full_path(path);
        delete_file.__set_file_format(format);
        delete_file.__set_length(file_size);

        HdfsScannerContext ctx;
        ctx.fs = &fs;
        ctx.datacache_options.enable_datacache = true;
        ctx.datacache_options.enable_cache_select = true;
        ctx.table_specific.iceberg_delete_files = {&delete_file};
        CacheSelectScanner scanner;
        scanner._scanner_ctx = &ctx;

        ASSERT_OK(scanner._fetch_iceberg_delete_files());
        EXPECT_EQ(file_size, scanner._fs_stats.bytes_read);
        EXPECT_EQ(1, scanner._app_stats.iceberg_delete_files_per_scan);
        ASSERT_OK(scanner._fetch_iceberg_delete_files());
        EXPECT_EQ(file_size, scanner._fs_stats.bytes_read);
    }
}

TEST_F(CacheSelectScannerTest, RejectInvalidDeletionVectorRangesBeforeOpeningFile) {
    TIcebergDeleteFile valid;
    valid.__set_full_path("/missing.puffin");
    valid.__set_file_format(THdfsFileFormat::PUFFIN);
    valid.__set_length(100);
    valid.__set_content_offset(10);
    valid.__set_content_size_in_bytes(20);

    std::vector<TIcebergDeleteFile> invalid(9, valid);
    invalid[0].__isset.content_offset = false;
    invalid[1].__isset.content_size_in_bytes = false;
    invalid[2].__set_content_offset(-1);
    invalid[3].__set_content_size_in_bytes(0);
    invalid[4].__set_content_size_in_bytes(-1);
    invalid[5].__set_length(-1);
    invalid[6].__set_content_offset(101);
    invalid[7].__set_content_offset(81);
    invalid[8].__set_content_size_in_bytes(std::numeric_limits<int64_t>::max());

    MemoryFileSystem fs;
    HdfsScannerContext ctx;
    ctx.fs = &fs;
    CacheSelectScanner scanner;
    scanner._scanner_ctx = &ctx;
    for (const auto& delete_file : invalid) {
        ctx.table_specific.iceberg_delete_files = {&delete_file};
        EXPECT_TRUE(scanner._fetch_iceberg_delete_files().is_invalid_argument());
    }
    EXPECT_EQ(0, scanner._fs_stats.io_count);
    EXPECT_EQ(0, scanner._app_stats.iceberg_delete_files_per_scan);
}
} // namespace starrocks
