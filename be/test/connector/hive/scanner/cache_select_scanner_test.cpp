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

#include <filesystem>
#include <memory>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "cache/datacache.h"
#include "cache/disk_cache/block_cache.h"
#include "cache/disk_cache/test_cache_utils.h"
#include "column/column_helper.h"
#include "common/config_cache_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/config_scan_io_fwd.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "compute_env/query/fragment_runtime_state.h"
#include "connector/hive/scanner/hdfs_scanner_orc.h"
#include "connector/hive/scanner/hdfs_scanner_parquet.h"
#include "formats/file_input_stream.h"
#include "formats/iceberg/iceberg_deletion_vector_reader.h"
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

#ifdef WITH_STARCACHE

// Cache select must also warm the puffin file backing a V3 deletion vector, otherwise the data
// file is cached but the DV blob is still fetched remotely on the first query.
class CacheSelectScannerDvTest : public CacheSelectScannerTest {
public:
    void SetUp() override {
        CacheSelectScannerTest::SetUp();
        _test_dir = "./cache_select_dv_test_" + std::to_string(::getpid());
        std::filesystem::create_directories(_test_dir);
        auto cache_options = TestCacheUtils::create_simple_options(config::datacache_block_size, 50 * MB);
        _block_cache = TestCacheUtils::create_cache(cache_options);
        ASSERT_NE(nullptr, _block_cache);
        DataCache::GetInstance()->set_block_cache(_block_cache);
    }

    void TearDown() override {
        DataCache::GetInstance()->set_block_cache(nullptr);
        _block_cache.reset();
        if (std::filesystem::exists(_test_dir)) {
            std::filesystem::remove_all(_test_dir);
        }
        CacheSelectScannerTest::TearDown();
    }

protected:
    std::string _write_file(const std::string& name, const std::string& contents) {
        std::string path = _test_dir + "/" + name;
        ASSIGN_OR_ABORT(auto wf, FileSystem::Default()->new_writable_file(path));
        CHECK_OK(wf->append(Slice(contents)));
        CHECK_OK(wf->close());
        return path;
    }

    // Runs cache select over a text data file that carries the given DV descriptor.
    void _run_cache_select(const std::string& data_file, TIcebergDeletionVectorDescriptor* dv_descriptor) {
        SlotDesc slot_desc[] = {{"Id", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)}, {""}};
        auto scanner = std::make_shared<CacheSelectScanner>();
        auto* range = _create_scan_range(data_file, 0, 0, THdfsFileFormat::TEXT);
        auto* tuple_desc = _create_tuple_desc(slot_desc);
        auto* ctx = _create_ctx(data_file, range, tuple_desc);
        ctx->datacache_options = DataCacheOptions{
                .enable_datacache = true, .enable_cache_select = true, .enable_populate_datacache = true};
        if (dv_descriptor != nullptr) {
            ctx->table_specific.iceberg_deletion_vector_descriptor =
                    std::make_shared<TIcebergDeletionVectorDescriptor>(*dv_descriptor);
        }

        ASSERT_OK(scanner->init(_runtime_state, ctx));
        ASSERT_OK(scanner->open(_runtime_state));
        ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
        auto status = scanner->get_next(_runtime_state, &chunk);
        ASSERT_TRUE(status.is_end_of_file()) << status.message();
        scanner->close();
    }

    std::string _test_dir;
    std::shared_ptr<BlockCache> _block_cache;
};

TEST_F(CacheSelectScannerDvTest, WarmsPuffinFile) {
    // The fixture hardcodes a 10-byte scan range, so the data file must be exactly that long.
    const std::string data_file = _write_file("data.txt", "0123456789");
    const std::string puffin_contents(4096, 'p');
    const std::string puffin_file = _write_file("dv.puffin", puffin_contents);

    TIcebergDeletionVectorDescriptor dv;
    dv.puffin_file_path = puffin_file;
    dv.content_offset = 0;
    dv.content_size_in_bytes = static_cast<int64_t>(puffin_contents.size());
    dv.record_count = 1;
    dv.referenced_data_file = data_file;
    dv.__set_puffin_file_size_in_bytes(static_cast<int64_t>(puffin_contents.size()));

    _run_cache_select(data_file, &dv);

    // Read the puffin range back through the same stack the query path uses: it must hit the
    // block that cache select populated.
    FormatScannerStats fs_stats;
    FormatScannerStats app_stats;
    std::shared_ptr<SharedBufferedInputStream> shared_stream;
    std::shared_ptr<CacheInputStream> cache_stream;
    const formats::FileInputStreamOptions options{.fs = FileSystem::Default(),
                                                  .file_path = puffin_file,
                                                  .file_size = static_cast<int64_t>(puffin_contents.size()),
                                                  .fs_stats = &fs_stats,
                                                  .app_stats = &app_stats,
                                                  .datacache_options = DataCacheOptions{.enable_datacache = true}};
    ASSIGN_OR_ABORT(auto file, formats::create_random_access_file(shared_stream, cache_stream, options));
    std::string read_back(puffin_contents.size(), '\0');
    ASSERT_OK(file->read_at_fully(0, read_back.data(), static_cast<int64_t>(read_back.size())));

    EXPECT_EQ(puffin_contents, read_back);
    EXPECT_GT(cache_stream->stats().read_block_cache_count, 0);
    EXPECT_EQ(0, cache_stream->stats().write_block_cache_count);
}

// A puffin larger than io_coalesce_read_max_buffer_size must be warmed through several bounded
// ranges rather than one oversized SharedBuffer. The threshold is lowered so a small fixture
// exercises the split path.
TEST_F(CacheSelectScannerDvTest, SplitsOversizedPuffinIntoBoundedRanges) {
    const int32_t saved = config::io_coalesce_read_max_buffer_size;
    config::io_coalesce_read_max_buffer_size = 1024;
    DeferOp restore([&]() { config::io_coalesce_read_max_buffer_size = saved; });

    const std::string data_file = _write_file("data_split.txt", "0123456789");
    // 4 KB against a 1 KB cap => 4 ranges.
    const std::string puffin_contents(4096, 'p');
    const std::string puffin_file = _write_file("dv_split.puffin", puffin_contents);

    TIcebergDeletionVectorDescriptor dv;
    dv.puffin_file_path = puffin_file;
    dv.content_offset = 0;
    dv.content_size_in_bytes = static_cast<int64_t>(puffin_contents.size());
    dv.record_count = 1;
    dv.referenced_data_file = data_file;
    dv.__set_puffin_file_size_in_bytes(static_cast<int64_t>(puffin_contents.size()));

    _run_cache_select(data_file, &dv);

    // Every byte must still be cached and readable back through the query-path stack.
    FormatScannerStats fs_stats;
    FormatScannerStats app_stats;
    std::shared_ptr<SharedBufferedInputStream> shared_stream;
    std::shared_ptr<CacheInputStream> cache_stream;
    const formats::FileInputStreamOptions options{.fs = FileSystem::Default(),
                                                  .file_path = puffin_file,
                                                  .file_size = static_cast<int64_t>(puffin_contents.size()),
                                                  .fs_stats = &fs_stats,
                                                  .app_stats = &app_stats,
                                                  .datacache_options = DataCacheOptions{.enable_datacache = true}};
    ASSIGN_OR_ABORT(auto file, formats::create_random_access_file(shared_stream, cache_stream, options));
    std::string read_back(puffin_contents.size(), '\0');
    ASSERT_OK(file->read_at_fully(0, read_back.data(), static_cast<int64_t>(read_back.size())));

    EXPECT_EQ(puffin_contents, read_back);
    EXPECT_GT(cache_stream->stats().read_block_cache_count, 0);
    EXPECT_EQ(0, cache_stream->stats().write_block_cache_count);
}

// Older FEs leave the puffin length unset; cache select must skip the DV instead of failing.
TEST_F(CacheSelectScannerDvTest, SkipsPuffinWithoutFileSize) {
    const std::string data_file = _write_file("data_no_size.txt", "0123456789");
    const std::string puffin_file = _write_file("dv_no_size.puffin", std::string(128, 'q'));

    TIcebergDeletionVectorDescriptor dv;
    dv.puffin_file_path = puffin_file;
    dv.content_offset = 0;
    dv.content_size_in_bytes = 128;
    dv.record_count = 1;
    dv.referenced_data_file = data_file;

    _run_cache_select(data_file, &dv);
}

#endif // WITH_STARCACHE

} // namespace starrocks
