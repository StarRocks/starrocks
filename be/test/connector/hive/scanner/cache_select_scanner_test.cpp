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
#include "formats/iceberg/iceberg_delete_builder.h"
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

    // Builds the delete-file entry a V3 deletion vector travels in: the Puffin file itself,
    // plus the blob locator. Cache select only needs full_path/length to warm the whole file.
    static TIcebergDeleteFile _make_dv_delete_file(const std::string& puffin_file, int64_t puffin_size,
                                                   const std::string& referenced_data_file) {
        TIcebergDeleteFile df;
        df.__set_full_path(puffin_file);
        df.__set_file_content(TIcebergFileContent::POSITION_DELETES);
        df.__set_length(puffin_size);
        TIcebergDeletionVectorBlob blob;
        blob.__set_content_offset(0);
        blob.__set_content_size_in_bytes(puffin_size);
        blob.__set_record_count(1);
        blob.__set_referenced_data_file(referenced_data_file);
        df.__set_deletion_vector(blob);
        return df;
    }

    // Runs cache select over a text data file that carries the given DV delete file.
    // `dv_delete_file` must outlive this call: table_specific keeps a bare pointer.
    // Runs a warm-up and publishes the scanner's counters into `profile`. do_update_counter is
    // invoked directly: going through close() would run the full HdfsScanner::update_counter
    // chain, which dereferences counter pointers this fixture never initialises.
    void _run_cache_select_and_publish(const std::string& data_file, const TIcebergDeleteFile* dv_delete_file,
                                       RuntimeProfile* profile) {
        SlotDesc slot_desc[] = {{"Id", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)}, {""}};
        auto scanner = std::make_shared<CacheSelectScanner>();
        auto* range = _create_scan_range(data_file, 0, 0, THdfsFileFormat::TEXT);
        auto* tuple_desc = _create_tuple_desc(slot_desc);
        auto* ctx = _create_ctx(data_file, range, tuple_desc);
        ctx->datacache_options = DataCacheOptions{
                .enable_datacache = true, .enable_cache_select = true, .enable_populate_datacache = true};
        if (dv_delete_file != nullptr) {
            ctx->table_specific.iceberg_delete_files.emplace_back(dv_delete_file);
        }

        CHECK_OK(scanner->init(_runtime_state, ctx));
        CHECK_OK(scanner->open(_runtime_state));
        ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
        auto status = scanner->get_next(_runtime_state, &chunk);
        CHECK(status.is_end_of_file()) << status.message();

        HdfsScannerProfile hdfs_profile;
        hdfs_profile.runtime_profile = profile;
        scanner->do_update_counter(&hdfs_profile);

        scanner->close();
    }

    void _run_cache_select(const std::string& data_file, const TIcebergDeleteFile* dv_delete_file) {
        SlotDesc slot_desc[] = {{"Id", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)}, {""}};
        auto scanner = std::make_shared<CacheSelectScanner>();
        auto* range = _create_scan_range(data_file, 0, 0, THdfsFileFormat::TEXT);
        auto* tuple_desc = _create_tuple_desc(slot_desc);
        auto* ctx = _create_ctx(data_file, range, tuple_desc);
        ctx->datacache_options = DataCacheOptions{
                .enable_datacache = true, .enable_cache_select = true, .enable_populate_datacache = true};
        if (dv_delete_file != nullptr) {
            ctx->table_specific.iceberg_delete_files.emplace_back(dv_delete_file);
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

TEST_F(CacheSelectScannerDvTest, WarmsOnlyTheBlobRange) {
    // The fixture hardcodes a 10-byte scan range, so the data file must be exactly that long.
    const std::string data_file = _write_file("data.txt", "0123456789");
    // A 1 MB puffin whose blob is a 64-byte slice: the query path reads only that slice, so
    // warming must not pull the rest.
    const std::string puffin_contents(1024 * 1024, 'p');
    const std::string puffin_file = _write_file("dv.puffin", puffin_contents);
    const auto puffin_size = static_cast<int64_t>(puffin_contents.size());

    TIcebergDeleteFile dv = _make_dv_delete_file(puffin_file, puffin_size, data_file);
    dv.deletion_vector.__set_content_offset(0);
    dv.deletion_vector.__set_content_size_in_bytes(64);
    _run_cache_select(data_file, &dv);

    // The blob itself must be cached: read it back through the query-path stack.
    FormatScannerStats fs_stats;
    FormatScannerStats app_stats;
    std::shared_ptr<SharedBufferedInputStream> shared_stream;
    std::shared_ptr<CacheInputStream> cache_stream;
    const formats::FileInputStreamOptions options{.fs = FileSystem::Default(),
                                                  .file_path = puffin_file,
                                                  .file_size = puffin_size,
                                                  .fs_stats = &fs_stats,
                                                  .app_stats = &app_stats,
                                                  .datacache_options = DataCacheOptions{.enable_datacache = true}};
    ASSIGN_OR_ABORT(auto file, formats::create_random_access_file(shared_stream, cache_stream, options));
    std::string blob(64, '\0');
    ASSERT_OK(file->read_at_fully(0, blob.data(), static_cast<int64_t>(blob.size())));
    EXPECT_EQ(std::string(64, 'p'), blob);
    EXPECT_GT(cache_stream->stats().read_block_cache_count, 0);
    EXPECT_EQ(0, cache_stream->stats().write_block_cache_count);
}

// Warming must be bounded by the blob, not by the Puffin: one Puffin backs many data files and
// each runs its own scanner, so a whole-file strategy re-reads it once per data file.
TEST_F(CacheSelectScannerDvTest, DoesNotWarmTheWholePuffin) {
    const std::string data_file = _write_file("data_bounded.txt", "0123456789");
    const std::string puffin_contents(1024 * 1024, 'p');
    const std::string puffin_file = _write_file("dv_bounded.puffin", puffin_contents);
    const auto puffin_size = static_cast<int64_t>(puffin_contents.size());

    TIcebergDeleteFile dv = _make_dv_delete_file(puffin_file, puffin_size, data_file);
    dv.deletion_vector.__set_content_offset(0);
    dv.deletion_vector.__set_content_size_in_bytes(64);
    _run_cache_select(data_file, &dv);

    // Far past the blob the data must NOT be cached, so reading it has to populate the cache now.
    FormatScannerStats fs_stats;
    FormatScannerStats app_stats;
    std::shared_ptr<SharedBufferedInputStream> shared_stream;
    std::shared_ptr<CacheInputStream> cache_stream;
    const formats::FileInputStreamOptions options{
            .fs = FileSystem::Default(),
            .file_path = puffin_file,
            .file_size = puffin_size,
            .fs_stats = &fs_stats,
            .app_stats = &app_stats,
            .datacache_options = DataCacheOptions{.enable_datacache = true, .enable_populate_datacache = true}};
    ASSIGN_OR_ABORT(auto file, formats::create_random_access_file(shared_stream, cache_stream, options));
    std::string tail(512, '\0');
    ASSERT_OK(file->read_at_fully(puffin_size - 512, tail.data(), static_cast<int64_t>(tail.size())));
    EXPECT_EQ(std::string(512, 'p'), tail);
    EXPECT_GT(cache_stream->stats().write_block_cache_count, 0)
            << "the puffin tail was already cached: warming was not bounded to the blob";
}

// A blob larger than io_coalesce_read_max_buffer_size is still split into bounded ranges.
TEST_F(CacheSelectScannerDvTest, SplitsOversizedBlobIntoBoundedRanges) {
    const int32_t saved = config::io_coalesce_read_max_buffer_size;
    config::io_coalesce_read_max_buffer_size = 1024;
    DeferOp restore([&]() { config::io_coalesce_read_max_buffer_size = saved; });

    const std::string data_file = _write_file("data_split.txt", "0123456789");
    // 4 KB blob against a 1 KB cap => 4 ranges.
    const std::string puffin_contents(4096, 'p');
    const std::string puffin_file = _write_file("dv_split.puffin", puffin_contents);
    const auto puffin_size = static_cast<int64_t>(puffin_contents.size());

    TIcebergDeleteFile dv = _make_dv_delete_file(puffin_file, puffin_size, data_file);
    _run_cache_select(data_file, &dv);

    FormatScannerStats fs_stats;
    FormatScannerStats app_stats;
    std::shared_ptr<SharedBufferedInputStream> shared_stream;
    std::shared_ptr<CacheInputStream> cache_stream;
    const formats::FileInputStreamOptions options{.fs = FileSystem::Default(),
                                                  .file_path = puffin_file,
                                                  .file_size = puffin_size,
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

// Warming a DV must be visible in the profile. DeleteFilesPerScan deliberately excludes V3
// vectors, so without a dedicated counter a CACHE SELECT gives no evidence it touched them.
TEST_F(CacheSelectScannerDvTest, ReportsDeletionVectorsPerScan) {
    const std::string data_file = _write_file("data_metric.txt", "0123456789");
    const std::string puffin_contents(4096, 'p');
    const std::string puffin_file = _write_file("dv_metric.puffin", puffin_contents);
    const auto puffin_size = static_cast<int64_t>(puffin_contents.size());

    TIcebergDeleteFile dv = _make_dv_delete_file(puffin_file, puffin_size, data_file);
    dv.deletion_vector.__set_content_offset(0);
    dv.deletion_vector.__set_content_size_in_bytes(64);

    RuntimeProfile profile("CacheSelectDvMetric");
    _run_cache_select_and_publish(data_file, &dv, &profile);

    auto* dv_per_scan = profile.get_counter("DeletionVectorsPerScan");
    ASSERT_NE(nullptr, dv_per_scan) << "CACHE SELECT reported no DV counter";
    EXPECT_EQ(1, dv_per_scan->value());

    // The V2 counter must stay at zero: this table has no position deletes.
    auto* delete_files_per_scan = profile.get_counter("DeleteFilesPerScan");
    ASSERT_NE(nullptr, delete_files_per_scan);
    EXPECT_EQ(0, delete_files_per_scan->value());
}

#endif // WITH_STARCACHE

} // namespace starrocks
