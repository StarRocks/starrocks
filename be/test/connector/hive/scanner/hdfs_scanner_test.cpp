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

#include "connector/hive/scanner/hdfs_scanner.h"

#include <bvar/bvar.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <functional>
#include <limits>
#include <memory>
#include <vector>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "cache/datacache.h"
#include "cache/disk_cache/block_cache.h"
#include "cache/disk_cache/starcache_engine.h"
#include "cache/disk_cache/test_cache_utils.h"
#include "cache/mem_cache/lrucache_engine.h"
#include "cache/mem_cache/page_cache.h"
#include "cache/scan/cache_input_stream.h"
#include "column/column_helper.h"
#include "common/config_cache_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/config_scan_io_fwd.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "compute_env/query/fragment_runtime_state.h"
#include "connector/hive/scanner/hdfs_scanner_avro.h"
#include "connector/hive/scanner/hdfs_scanner_orc.h"
#include "connector/hive/scanner/hdfs_scanner_parquet.h"
#include "connector/hive/scanner/hdfs_scanner_text.h"
#include "connector/hive/scanner/jni_scanner.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "formats/column_evaluator.h"
#include "formats/csv/csv_defaults.h"
#include "formats/iceberg/iceberg_delete_builder.h"
#include "formats/parquet/file_writer.h"
#include "formats/parquet/parquet_file_writer.h"
#include "runtime/chunk_helper.h"
#include "runtime/descriptor_helper.h"
#include "runtime/runtime_filter_builder.h"
#include "runtime/runtime_filter_factory.h"
#include "runtime/runtime_state.h"
#include "testutil/column_test_helper.h"

namespace starrocks {

namespace {
struct SlotDesc {
    string name;
    TypeDescriptor type;
};
} // namespace

// TODO: partition scan
class HdfsScannerTest : public ::testing::Test {
public:
    void SetUp() override {
        _create_runtime_profile();
        _create_runtime_state("");
    }
    void TearDown() override {}

    //private:
protected:
    void _create_runtime_state(const std::string& timezone);
    void _create_runtime_profile();
    HdfsScannerContext* _create_ctx(const std::string& file, THdfsScanRange* range, TupleDescriptor* tuple_desc);
    void build_hive_column_names(HdfsScannerContext* ctx, const TupleDescriptor* tuple_desc,
                                 bool diff_case_sensitive = false);

    THdfsScanRange* _create_scan_range(const std::string& file, uint64_t offset, uint64_t length);
    TupleDescriptor* _create_tuple_desc(SlotDesc* descs);
    TupleDescriptor* _create_tuple_desc_with_nullable(SlotDesc* descs, bool nullable);

    ObjectPool _pool;
    RuntimeProfile* _runtime_profile = nullptr;
    RuntimeState* _runtime_state = nullptr;
    std::vector<std::unique_ptr<FragmentDictState>> _fragment_dict_states;
    std::string _debug_row_output;
    int _debug_rows_per_call = 1;
};

void HdfsScannerTest::_create_runtime_profile() {
    _runtime_profile = _pool.add(new RuntimeProfile("test"));
    _runtime_profile->set_metadata(1);
}

void HdfsScannerTest::_create_runtime_state(const std::string& timezone) {
    TUniqueId fragment_id;
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    if (timezone != "") {
        query_globals.__set_time_zone(timezone);
    }
    _runtime_state =
            _pool.add(new RuntimeState(fragment_id, query_options, query_globals, static_cast<ExecEnv*>(nullptr)));
    _fragment_dict_states.emplace_back(std::make_unique<FragmentDictState>());
    _runtime_state->set_fragment_dict_state(_fragment_dict_states.back().get());
    _runtime_state->init_instance_mem_tracker();
    auto* fragment_runtime_state = _pool.add(new pipeline::FragmentRuntimeState());
    fragment_runtime_state->set_pred_tree_params({true, true});
    _runtime_state->set_fragment_runtime_state(fragment_runtime_state);
    _runtime_state->set_fragment_dict_state(_fragment_dict_states.back().get());
}

THdfsScanRange* HdfsScannerTest::_create_scan_range(const std::string& file, uint64_t offset, uint64_t length) {
    auto* scan_range = _pool.add(new THdfsScanRange());
    ASSIGN_OR_ABORT(uint64_t file_size, FileSystem::Default()->get_file_size(file));
    scan_range->relative_path = file;
    scan_range->offset = offset;
    scan_range->length = length == 0 ? file_size : length;
    scan_range->file_length = file_size;
    scan_range->text_file_desc.__set_field_delim(",");
    scan_range->text_file_desc.__set_line_delim("\n");
    scan_range->text_file_desc.__set_collection_delim("\003");
    scan_range->text_file_desc.__set_mapkey_delim("\004");
    return scan_range;
}

HdfsScannerContext* HdfsScannerTest::_create_ctx(const std::string& file, THdfsScanRange* range,
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

void HdfsScannerTest::build_hive_column_names(HdfsScannerContext* ctx, const TupleDescriptor* tuple_desc,
                                              bool diff_case_sensitive) {
    for (auto slot : tuple_desc->slots()) {
        std::string col_name(slot->col_name());
        if (diff_case_sensitive && std::isupper(col_name[0])) {
            std::transform(col_name.begin(), col_name.end(), col_name.begin(), ::tolower);
        } else if (diff_case_sensitive && std::islower(col_name[0])) {
            std::transform(col_name.begin(), col_name.end(), col_name.begin(), ::toupper);
        }
        ctx->hive_column_names.emplace_back(col_name);
    }
}

TupleDescriptor* HdfsScannerTest::_create_tuple_desc(SlotDesc* descs) {
    return _create_tuple_desc_with_nullable(descs, true);
}

TupleDescriptor* HdfsScannerTest::_create_tuple_desc_with_nullable(SlotDesc* descs, bool nullable) {
    TDescriptorTableBuilder table_desc_builder;
    TSlotDescriptorBuilder slot_desc_builder;
    TTupleDescriptorBuilder tuple_desc_builder;
    int slot_id = 0;
    while (descs->name != "") {
        slot_desc_builder.column_name(descs->name).type(descs->type).id(slot_id).nullable(nullable);
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

// ========================= PARQUET SCANNER ============================

static SlotDesc default_parquet_descs[] = {
        {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
        {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
        {"c3", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
        {"col_varchar", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME)},
        {""}};

std::string default_parquet_file = "./be/test/exec/test_data/parquet_scanner/file_reader_test.parquet1";

TEST_F(HdfsScannerTest, TestParquetInit) {
    auto scanner = std::make_shared<HdfsParquetScanner>();

    auto* range = _create_scan_range(default_parquet_file, 4, 1024);
    auto* tuple_desc = _create_tuple_desc(default_parquet_descs);
    auto* ctx = _create_ctx(default_parquet_file, range, tuple_desc);

    Status status = scanner->init(_runtime_state, ctx);
    ASSERT_TRUE(status.ok());
}

TEST_F(HdfsScannerTest, TestParquetOpen) {
    auto scanner = std::make_shared<HdfsParquetScanner>();

    auto* range = _create_scan_range(default_parquet_file, 4, 1024);
    auto* tuple_desc = _create_tuple_desc(default_parquet_descs);
    auto* ctx = _create_ctx(default_parquet_file, range, tuple_desc);

    Status status = scanner->init(_runtime_state, ctx);
    ASSERT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    ASSERT_TRUE(status.ok());
}

// A position delete file that covers every row of its data file: the data file is never opened.
// The delete file repeats position 3, so it has 5 rows but deletes only 4 distinct positions.
TEST_F(HdfsScannerTest, TestParquetSkipFileWithAllRowsDeleted) {
    const std::string delete_file_path =
            "./be/test/exec/test_data/parquet_scanner/iceberg_position_delete_repeated_pos.parquet";
    // The data file does not exist: any attempt to open it fails the scan.
    const std::string data_file_path = "missing_data_file.parquet";
    ASSIGN_OR_ABORT(const int64_t delete_file_size, FileSystem::Default()->get_file_size(delete_file_path));
    TIcebergDeleteFile delete_file;
    delete_file.__set_full_path(delete_file_path);
    delete_file.__set_file_content(TIcebergFileContent::POSITION_DELETES);
    delete_file.__set_length(delete_file_size);

    auto run = [&](int64_t record_count, bool expect_open_ok) {
        auto scanner = std::make_shared<HdfsParquetScanner>();
        auto* range = _create_scan_range(default_parquet_file, 0, 0);
        range->__set_record_count(record_count);
        auto* tuple_desc = _create_tuple_desc(default_parquet_descs);
        auto* ctx = _create_ctx(data_file_path, range, tuple_desc);
        ctx->format_scan_context.timezone = "UTC";
        ctx->profile.runtime_profile = _runtime_profile;
        ctx->table_specific.iceberg_delete_files.emplace_back(&delete_file);

        ASSERT_OK(scanner->init(_runtime_state, ctx));
        Status status = scanner->open(_runtime_state);
        if (!expect_open_ok) {
            ASSERT_FALSE(status.ok());
            ctx->profile.runtime_profile = nullptr;
            scanner->close();
            return;
        }
        ASSERT_OK(status);
        ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
        status = scanner->get_next(_runtime_state, &chunk);
        ASSERT_TRUE(status.is_end_of_file()) << status;
        ASSERT_EQ(0, chunk->num_rows());

        HdfsScannerProfile profile;
        profile.runtime_profile = _runtime_profile;
        scanner->do_update_counter(&profile);
        ASSERT_NE(nullptr, _runtime_profile->get_counter("DeletedFilesSkipped"));
        ASSERT_EQ(1, _runtime_profile->get_counter("DeletedFilesSkipped")->value());
        // The fixture never sets up the HdfsScannerProfile counters that close() publishes into.
        ctx->profile.runtime_profile = nullptr;
        scanner->close();
    };
    // 4 rows, all deleted.
    run(4, true);
    // 5 rows: the 5 delete rows do not cover row 4, so the file must be opened.
    run(5, false);
}

// Scans iceberg_three_row_groups.parquet (c1 = 0..11, 3 row groups of 4 rows) with Iceberg deletes.
class HdfsParquetDeletedSkipTest : public HdfsScannerTest {
protected:
    static constexpr const char* kDataFile =
            "./be/test/exec/test_data/parquet_scanner/iceberg_three_row_groups.parquet";

    const TIcebergDeleteFile* _position_delete(const std::string& name) {
        const std::string path = "./be/test/exec/test_data/parquet_scanner/" + name;
        auto* df = _pool.add(new TIcebergDeleteFile());
        df->__set_full_path(path);
        df->__set_file_content(TIcebergFileContent::POSITION_DELETES);
        ASSIGN_OR_ABORT(const int64_t size, FileSystem::Default()->get_file_size(path));
        df->__set_length(size);
        return df;
    }

    HdfsScannerContext* _create_deletes_ctx(SlotDesc* slot_descs, const std::vector<const TIcebergDeleteFile*>& deletes,
                                            TupleDescriptor** tuple_desc) {
        auto* range = _create_scan_range(kDataFile, 0, 0);
        range->__set_record_count(12);
        *tuple_desc = _create_tuple_desc(slot_descs);
        auto* ctx = _create_ctx(kDataFile, range, *tuple_desc);
        ctx->format_scan_context.timezone = "UTC";
        ctx->profile.runtime_profile = _runtime_profile;
        // As the FE sends them: on the scan range, and referenced from table_specific.
        std::vector<TIcebergDeleteFile> range_deletes;
        for (const auto* df : deletes) {
            range_deletes.emplace_back(*df);
        }
        range->__set_delete_files(range_deletes);
        for (const auto& df : range->delete_files) {
            ctx->table_specific.iceberg_delete_files.emplace_back(&df);
        }
        return ctx;
    }

    // Reads the scanner to EOF and returns the rows; publishes its counters into _runtime_profile.
    std::vector<std::string> _read_all(HdfsScanner* scanner, HdfsScannerContext* ctx, TupleDescriptor* tuple_desc) {
        std::vector<std::string> rows;
        Status status;
        while (status.ok()) {
            ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
            status = scanner->get_next(_runtime_state, &chunk);
            CHECK(status.ok() || status.is_end_of_file()) << status;
            for (size_t i = 0; i < chunk->num_rows(); i++) {
                rows.emplace_back(chunk->debug_row(i));
            }
        }
        HdfsScannerProfile profile;
        profile.runtime_profile = _runtime_profile;
        scanner->do_update_counter(&profile);
        // The fixture never sets up the HdfsScannerProfile counters that close() publishes into.
        ctx->profile.runtime_profile = nullptr;
        scanner->close();
        return rows;
    }

    std::vector<std::string> _scan(const std::vector<const TIcebergDeleteFile*>& deletes) {
        SlotDesc slot_descs[] = {{"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)}, {""}};
        TupleDescriptor* tuple_desc = nullptr;
        auto* ctx = _create_deletes_ctx(slot_descs, deletes, &tuple_desc);
        auto scanner = std::make_shared<HdfsParquetScanner>();
        CHECK_OK(scanner->init(_runtime_state, ctx));
        CHECK_OK(scanner->open(_runtime_state));
        return _read_all(scanner.get(), ctx, tuple_desc);
    }

    int64_t _counter(const std::string& name) {
        auto* counter = _runtime_profile->get_counter(name);
        return counter == nullptr ? -1 : counter->value();
    }

    // Rows left when row 0 and all of row group 1 (rows 4..7) are deleted.
    const std::vector<std::string> _rows_without_group1{"[1]", "[2]", "[3]", "[8]", "[9]", "[10]", "[11]"};
};

// Row group 1 is fully deleted (position 6 repeated) and is skipped before any IO; row 0 of row
// group 0 is deleted by the regular row filter.
TEST_F(HdfsParquetDeletedSkipTest, SkipRowGroup) {
    ASSERT_EQ(_rows_without_group1, _scan({_position_delete("iceberg_three_row_groups_position_delete.parquet")}));
    ASSERT_EQ(3, _counter("TotalRowGroups"));
    ASSERT_EQ(1, _counter("DeletedRowGroupsSkipped"));
    ASSERT_EQ(0, _counter("DeletedFilesSkipped"));
}

// Neither delete file alone covers row group 1 ({0,4,5} and {6,7}); their union does. With a third
// file the union covers the whole file.
TEST_F(HdfsParquetDeletedSkipTest, SkipOnUnionOfDeleteFiles) {
    const auto* a = _position_delete("iceberg_three_row_groups_position_delete_a.parquet");
    const auto* b = _position_delete("iceberg_three_row_groups_position_delete_b.parquet");
    const auto* c = _position_delete("iceberg_three_row_groups_position_delete_c.parquet");
    ASSERT_EQ(_rows_without_group1, _scan({a, b}));
    ASSERT_EQ(1, _counter("DeletedRowGroupsSkipped"));
    ASSERT_EQ(0, _counter("DeletedFilesSkipped"));

    ASSERT_TRUE(_scan({a, b, c}).empty());
    ASSERT_EQ(1, _counter("DeletedFilesSkipped"));
}

// COUNT(*) with position deletes has no metadata count, so it reads the file without any column:
// the skipped row group must not be counted, and a skipped file yields no count row.
TEST_F(HdfsParquetDeletedSkipTest, CountSkipsDeletedRows) {
    auto count = [&](const std::vector<const TIcebergDeleteFile*>& deletes) {
        SlotDesc slot_descs[] = {{"___count___", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)}, {""}};
        TupleDescriptor* tuple_desc = nullptr;
        auto* ctx = _create_deletes_ctx(slot_descs, deletes, &tuple_desc);
        ctx->format_scan_context.options.use_count_opt = true;
        auto scanner = std::make_shared<HdfsParquetScanner>();
        CHECK_OK(scanner->init(_runtime_state, ctx));
        CHECK_OK(scanner->open(_runtime_state));
        return _read_all(scanner.get(), ctx, tuple_desc);
    };
    ASSERT_EQ((std::vector<std::string>{"[7]"}),
              count({_position_delete("iceberg_three_row_groups_position_delete.parquet")}));
    ASSERT_EQ(1, _counter("DeletedRowGroupsSkipped"));

    ASSERT_TRUE(count({_position_delete("iceberg_three_row_groups_position_delete_a.parquet"),
                       _position_delete("iceberg_three_row_groups_position_delete_b.parquet"),
                       _position_delete("iceberg_three_row_groups_position_delete_c.parquet")})
                        .empty());
    ASSERT_EQ(1, _counter("DeletedFilesSkipped"));
}

// The task that splits the file creates no split task for the deleted row group; the split tasks
// it creates share its deletes.
TEST_F(HdfsParquetDeletedSkipTest, NoSplitTaskForDeletedRowGroup) {
    SlotDesc slot_descs[] = {{"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)}, {""}};
    TupleDescriptor* tuple_desc = nullptr;
    auto* ctx = _create_deletes_ctx(slot_descs, {_position_delete("iceberg_three_row_groups_position_delete.parquet")},
                                    &tuple_desc);
    ctx->format_scan_context.options.enable_split_tasks = true;
    // one split task per row group: never merge
    ctx->format_scan_context.options.connector_max_split_size = 1;
    auto scanner = std::make_shared<HdfsParquetScanner>();
    ASSERT_OK(scanner->init(_runtime_state, ctx));
    ASSERT_OK(scanner->open(_runtime_state));
    const auto& split_tasks = ctx->format_scan_context.split.split_tasks;
    ASSERT_EQ(2, split_tasks.size());
    ASSERT_TRUE(_read_all(scanner.get(), ctx, tuple_desc).empty());

    std::vector<std::string> rows;
    for (const auto& task : split_tasks) {
        auto* split_range = _create_scan_range(kDataFile, task->start_offset, task->end_offset - task->start_offset);
        split_range->__set_record_count(12);
        auto* split_ctx = _create_ctx(kDataFile, split_range, tuple_desc);
        split_ctx->format_scan_context.timezone = "UTC";
        split_ctx->format_scan_context.split_context = task.get();
        auto split_scanner = std::make_shared<HdfsParquetScanner>();
        ASSERT_OK(split_scanner->init(_runtime_state, split_ctx));
        ASSERT_OK(split_scanner->open(_runtime_state));
        auto split_rows = _read_all(split_scanner.get(), split_ctx, tuple_desc);
        rows.insert(rows.end(), split_rows.begin(), split_rows.end());
    }
    ASSERT_EQ(_rows_without_group1, rows);
    // counted once, by the task that splits the file
    ASSERT_EQ(1, _counter("DeletedRowGroupsSkipped"));
}

TEST_F(HdfsScannerTest, TestHdfsRunningTime) {
    auto scanner = std::make_shared<HdfsParquetScanner>();

    auto* range = _create_scan_range(default_parquet_file, 4, 1024);
    auto* tuple_desc = _create_tuple_desc(default_parquet_descs);
    auto* ctx = _create_ctx(default_parquet_file, range, tuple_desc);

    Status status = scanner->init(_runtime_state, ctx);
    ASSERT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    ASSERT_TRUE(status.ok());

    ASSERT_TRUE(scanner->cpu_time_spent() > 0);
}

TEST_F(HdfsScannerTest, TestParquetGetNext) {
    auto scanner = std::make_shared<HdfsParquetScanner>();

    auto* range = _create_scan_range(default_parquet_file, 4, 1024);
    auto* tuple_desc = _create_tuple_desc(default_parquet_descs);
    auto* ctx = _create_ctx(default_parquet_file, range, tuple_desc);

    Status status = scanner->init(_runtime_state, ctx);
    ASSERT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    ASSERT_TRUE(status.ok());

    ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
    status = scanner->get_next(_runtime_state, &chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(chunk->num_rows(), 4);

    status = scanner->get_next(_runtime_state, &chunk);
    ASSERT_TRUE(status.is_end_of_file());

    scanner->close();
}

static SlotDesc avro_multiblock_descs[] = {{"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)}, {""}};

std::string avro_multiblock_file = "./be/test/formats/test_data/avro/cpp/multiblock.avro";

TEST_F(HdfsScannerTest, TestAvroGetNextWithDirectReader) {
    auto scanner = std::make_shared<HdfsAvroScanner>();

    auto* range = _create_scan_range(avro_multiblock_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(avro_multiblock_descs);
    auto* ctx = _create_ctx(avro_multiblock_file, range, tuple_desc);

    Status status = scanner->init(_runtime_state, ctx);
    ASSERT_TRUE(status.ok()) << status.to_string();

    status = scanner->open(_runtime_state);
    ASSERT_TRUE(status.ok()) << status.to_string();

    ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
    status = scanner->get_next(_runtime_state, &chunk);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(100, chunk->num_rows());
    ASSERT_EQ("[0]", chunk->debug_row(0));
    ASSERT_EQ("[99]", chunk->debug_row(99));
    ASSERT_EQ(100, scanner->raw_rows_read());
    ASSERT_EQ(100, scanner->num_rows_read());

    HdfsScannerProfile profile;
    profile.runtime_profile = _runtime_profile;
    scanner->do_update_counter(&profile);
    ASSERT_NE(nullptr, _runtime_profile->get_counter("DirectPathUsed"));
    ASSERT_EQ(1, _runtime_profile->get_counter("DirectPathUsed")->value());
    ASSERT_EQ(100, _runtime_profile->get_counter("DirectRowsDecoded")->value());
    ASSERT_EQ(1, _runtime_profile->get_counter("DirectReadEntries")->value());
    ASSERT_EQ(1, _runtime_profile->get_counter("DirectSkipEntries")->value());
    ASSERT_EQ(1, _runtime_profile->get_counter("DirectFastSkipEntries")->value());

    ChunkPtr eof_chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
    status = scanner->get_next(_runtime_state, &eof_chunk);
    ASSERT_TRUE(status.is_end_of_file()) << status.to_string();

    scanner->close();
}

TEST_F(HdfsScannerTest, TestAvroUpdateCounterWithoutOpen) {
    HdfsAvroScanner scanner;
    HdfsScannerProfile profile;
    profile.runtime_profile = _runtime_profile;

    scanner.do_update_counter(&profile);

    ASSERT_NE(nullptr, _runtime_profile->get_counter("InputStreamReadCount"));
    ASSERT_EQ(0, _runtime_profile->get_counter("InputStreamReadCount")->value());
}

TEST_F(HdfsScannerTest, TestFillNotExistedColumnWithDefaultValue) {
    SlotDesc descs[] = {{"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                        {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                        {""}};
    auto* tuple_desc = _create_tuple_desc(descs);
    HdfsScannerContext ctx;
    ctx.format_scan_context.not_existed_slots.push_back(tuple_desc->slots()[0]);
    ctx.format_scan_context.not_existed_slots.push_back(tuple_desc->slots()[1]);
    ctx.format_scan_context.materialize_slot_default_values.emplace(tuple_desc->slots()[0]->id(), "42");

    ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
    ASSERT_OK(ctx.format_scan_context.append_or_update_not_existed_columns_to_chunk(&chunk, 1));
    ASSERT_EQ(1, chunk->num_rows());
    EXPECT_EQ("[42, NULL]", chunk->debug_row(0));
}

// Empty string defaults should be preserved for string columns.
TEST_F(HdfsScannerTest, TestFillNotExistedColumnWithEmptyDefaultNullable) {
    SlotDesc descs[] = {{"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)}, {""}};
    auto* tuple_desc = _create_tuple_desc(descs);
    HdfsScannerContext ctx;
    ctx.format_scan_context.not_existed_slots.push_back(tuple_desc->slots()[0]);
    ctx.format_scan_context.materialize_slot_default_values.emplace(tuple_desc->slots()[0]->id(), "");

    ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
    ASSERT_OK(ctx.format_scan_context.append_or_update_not_existed_columns_to_chunk(&chunk, 1));
    ASSERT_EQ(1, chunk->num_rows());
    EXPECT_EQ("['']", chunk->debug_row(0));
}

// Empty string defaults should also work for non-nullable string columns.
TEST_F(HdfsScannerTest, TestFillNotExistedColumnWithEmptyDefaultNonNullable) {
    SlotDesc descs[] = {{"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)}, {""}};
    auto* tuple_desc = _create_tuple_desc_with_nullable(descs, false);
    HdfsScannerContext ctx;
    ctx.format_scan_context.not_existed_slots.push_back(tuple_desc->slots()[0]);
    ctx.format_scan_context.materialize_slot_default_values.emplace(tuple_desc->slots()[0]->id(), "");

    ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
    ASSERT_OK(ctx.format_scan_context.append_or_update_not_existed_columns_to_chunk(&chunk, 1));
    ASSERT_EQ(1, chunk->num_rows());
    EXPECT_EQ("['']", chunk->debug_row(0));
}

TEST_F(HdfsScannerTest, TestFillNotExistedColumnWithEmptyDefaultNonString) {
    SlotDesc descs[] = {{"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)}, {""}};
    auto* tuple_desc = _create_tuple_desc(descs);
    HdfsScannerContext ctx;
    ctx.format_scan_context.not_existed_slots.push_back(tuple_desc->slots()[0]);
    ctx.format_scan_context.materialize_slot_default_values.emplace(tuple_desc->slots()[0]->id(), "");

    ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
    auto status = ctx.format_scan_context.append_or_update_not_existed_columns_to_chunk(&chunk, 1);
    EXPECT_FALSE(status.ok());
}

// An Iceberg TIME initial-default arrives as "HH:MM[:SS[.ffffff]]" and is stored as whole seconds.
TEST_F(HdfsScannerTest, TestFillNotExistedColumnWithTimeDefault) {
    SlotDesc descs[] = {{"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_TIME)},
                        {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_TIME)},
                        {""}};
    auto* tuple_desc = _create_tuple_desc(descs);
    HdfsScannerContext ctx;
    ctx.format_scan_context.not_existed_slots.push_back(tuple_desc->slots()[0]);
    ctx.format_scan_context.not_existed_slots.push_back(tuple_desc->slots()[1]);
    ctx.format_scan_context.materialize_slot_default_values.emplace(tuple_desc->slots()[0]->id(), "01:02:03.999999");
    ctx.format_scan_context.materialize_slot_default_values.emplace(tuple_desc->slots()[1]->id(), "10:15");

    ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
    ASSERT_OK(ctx.format_scan_context.append_or_update_not_existed_columns_to_chunk(&chunk, 2));
    ASSERT_EQ(2, chunk->num_rows());
    EXPECT_EQ("[3723, 36900]", chunk->debug_row(1));
}

// FE sends a TIMESTAMP default as LocalDateTime#toString (seconds omitted when zero) and a TIMESTAMPTZ
// default with microseconds.
TEST_F(HdfsScannerTest, TestFillNotExistedColumnWithDatetimeDefault) {
    SlotDesc descs[] = {{"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME)},
                        {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME)},
                        {""}};
    auto* tuple_desc = _create_tuple_desc(descs);
    HdfsScannerContext ctx;
    ctx.format_scan_context.not_existed_slots.push_back(tuple_desc->slots()[0]);
    ctx.format_scan_context.not_existed_slots.push_back(tuple_desc->slots()[1]);
    ctx.format_scan_context.materialize_slot_default_values.emplace(tuple_desc->slots()[0]->id(), "2024-06-15 10:30");
    ctx.format_scan_context.materialize_slot_default_values.emplace(tuple_desc->slots()[1]->id(),
                                                                    "2023-11-15 06:13:20.000000");

    ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
    ASSERT_OK(ctx.format_scan_context.append_or_update_not_existed_columns_to_chunk(&chunk, 1));
    EXPECT_EQ("[2024-06-15 10:30:00, 2023-11-15 06:13:20]", chunk->debug_row(0));
}

TEST_F(HdfsScannerTest, TestFillNotExistedColumnWithInvalidTimeDefault) {
    for (const char* value : {"", "01", "01:02:03:04", "24:00:00", "01:60", "01:02:x"}) {
        SlotDesc descs[] = {{"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_TIME)}, {""}};
        auto* tuple_desc = _create_tuple_desc(descs);
        HdfsScannerContext ctx;
        ctx.format_scan_context.not_existed_slots.push_back(tuple_desc->slots()[0]);
        ctx.format_scan_context.materialize_slot_default_values.emplace(tuple_desc->slots()[0]->id(), value);

        ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
        EXPECT_FALSE(ctx.format_scan_context.append_or_update_not_existed_columns_to_chunk(&chunk, 1).ok()) << value;
    }
}

TEST_F(HdfsScannerTest, TestCreateMinMaxValueColumnForDatetimeSupportsNegativeMicros) {
    SlotDesc descs[] = {{"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME)}, {""}};
    auto* tuple_desc = _create_tuple_desc(descs);
    HdfsScannerContext ctx;
    ctx.format_scan_context.is_first_split = true;

    TExprMinMaxValue min_max_value;
    min_max_value.__set_type(TExprNodeType::INT_LITERAL);
    min_max_value.__set_has_null(false);
    min_max_value.__set_all_null(false);
    min_max_value.__set_min_int_value(-1);
    min_max_value.__set_max_int_value(0);

    auto col = ctx.format_scan_context.create_min_max_value_column(tuple_desc->slots()[0], min_max_value, 2);
    ASSERT_EQ(2, col->size());
    EXPECT_EQ("1969-12-31 23:59:59.999999", col->debug_item(0));
    EXPECT_EQ("1970-01-01 00:00:00", col->debug_item(1));
}

// ========================= ORC SCANNER ============================

static TTypeDesc create_primitive_type_desc(TPrimitiveType::type type) {
    TTypeDesc result;
    TTypeNode node;
    node.__set_type(TTypeNodeType::SCALAR);
    TScalarType scalar_type;
    scalar_type.__set_type(type);
    node.__set_scalar_type(scalar_type);
    result.types.push_back(node);
    return result;
}

static TExprNode create_int_literal_node(TPrimitiveType::type value_type, int64_t value_literal) {
    TExprNode lit_node;
    lit_node.__set_node_type(TExprNodeType::INT_LITERAL);
    lit_node.__set_num_children(0);
    lit_node.__set_type(create_primitive_type_desc(value_type));
    TIntLiteral lit_value;
    lit_value.__set_value(value_literal);
    lit_node.__set_int_literal(lit_value);
    return lit_node;
}

static TExprNode create_string_literal_node(TPrimitiveType::type value_type, const std::string& value_literal) {
    TExprNode lit_node;
    lit_node.__set_node_type(TExprNodeType::STRING_LITERAL);
    lit_node.__set_num_children(0);
    lit_node.__set_type(create_primitive_type_desc(value_type));
    TStringLiteral lit_value;
    lit_value.__set_value(value_literal);
    lit_node.__set_string_literal(lit_value);
    return lit_node;
}

static TExprNode create_datetime_literal_node(TPrimitiveType::type value_type, const std::string& value_literal) {
    TExprNode lit_node;
    lit_node.__set_node_type(TExprNodeType::DATE_LITERAL);
    lit_node.__set_num_children(0);
    lit_node.__set_type(create_primitive_type_desc(value_type));
    TDateLiteral lit_value;
    lit_value.__set_value(value_literal);
    lit_node.__set_date_literal(lit_value);
    return lit_node;
}

template <typename ValueType>
static void push_binary_pred_texpr_node(std::vector<TExprNode>& nodes, TExprOpcode::type opcode,
                                        SlotDescriptor* slot_desc, ValueType value_type, const TExprNode& lit_node) {
    TExprNode eq_node;
    eq_node.__set_node_type(TExprNodeType::type::BINARY_PRED);
    eq_node.__set_child_type(value_type);
    eq_node.__set_type(create_primitive_type_desc(TPrimitiveType::BOOLEAN));
    eq_node.__set_opcode(opcode);
    eq_node.__set_num_children(2);

    TExprNode slot_node;
    slot_node.__set_node_type(TExprNodeType::SLOT_REF);
    slot_node.__set_num_children(0);
    slot_node.__set_type(create_primitive_type_desc(value_type));
    TSlotRef slot_ref;
    slot_ref.__set_slot_id(slot_desc->id());
    slot_ref.__set_tuple_id(slot_desc->parent());
    slot_node.__set_slot_ref(slot_ref);

    nodes.emplace_back(eq_node);
    nodes.emplace_back(slot_node);
    nodes.emplace_back(lit_node);
}

static ExprContext* create_expr_context(ObjectPool* pool, const std::vector<TExprNode>& nodes) {
    TExpr texpr;
    texpr.__set_nodes(nodes);
    ExprContext* ctx;
    Status st = ExprFactory::create_expr_tree(pool, texpr, &ctx, nullptr);
    DCHECK(st.ok()) << st.message();
    return ctx;
}

static void extend_partition_values(ObjectPool* pool, HdfsScannerContext* ctx, std::vector<ExprContext*>* part_ctxs,
                                    const std::vector<int64_t>& values) {
    std::vector<int> positions;
    for (int i = 0; i < values.size(); i++) {
        int64_t v = values[i];
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_int_literal_node(TPrimitiveType::INT, v);
        nodes.emplace_back(lit_node);
        ExprContext* expr_ctx = create_expr_context(pool, nodes);
        part_ctxs->push_back(expr_ctx);
        positions.push_back(i);
    }
    ctx->_partition_index_in_hdfs_partition_columns = positions;
}

#define READ_SCANNER_RETURN_ROWS(scanner, records)                                        \
    do {                                                                                  \
        _debug_row_output = "";                                                           \
        ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);                   \
        for (;;) {                                                                        \
            chunk->reset();                                                               \
            status = scanner->get_next(_runtime_state, &chunk);                           \
            if (!status.ok() && !status.is_end_of_file()) {                               \
                std::cout << "status not ok: " << status.message() << std::endl;          \
                break;                                                                    \
            }                                                                             \
            chunk->check_or_die();                                                        \
            if (chunk->num_rows() > 0) {                                                  \
                int rep = std::min(_debug_rows_per_call, (int)chunk->num_rows());         \
                for (int i = 0; i < rep; i++) {                                           \
                    std::cout << "row#" << i << ": " << chunk->debug_row(i) << std::endl; \
                    _debug_row_output += chunk->debug_row(i);                             \
                    _debug_row_output += '\n';                                            \
                }                                                                         \
                EXPECT_EQ(chunk->num_columns(), tuple_desc->slots().size());              \
            }                                                                             \
            records += chunk->num_rows();                                                 \
            if (status.is_end_of_file()) {                                                \
                break;                                                                    \
            }                                                                             \
        }                                                                                 \
    } while (0)

#define READ_SCANNER_ROWS(scanner, exp)             \
    {                                               \
        uint64_t records = 0;                       \
        READ_SCANNER_RETURN_ROWS(scanner, records); \
        EXPECT_EQ(records, exp);                    \
    }

// ====================================================================================================

static SlotDesc mtypes_orc_descs[] = {{"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
                                      {"col_float", TypeDescriptor::from_logical_type(LogicalType::TYPE_FLOAT)},
                                      {"col_double", TypeDescriptor::from_logical_type(LogicalType::TYPE_DOUBLE)},
                                      {"col_varchar", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col_char", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col_tinyint", TypeDescriptor::from_logical_type(LogicalType::TYPE_TINYINT)},
                                      {"col_smallint", TypeDescriptor::from_logical_type(LogicalType::TYPE_SMALLINT)},
                                      {"col_int", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                                      {"col_bigint", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
                                      {"col_largeint", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
                                      {"col0_i32p7s2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col1_i32p7s2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col0_i32p6s3", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col1_i32p6s3", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col0_i64p7s2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col1_i64p7s2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col0_i64p9s5", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col1_i64p9s5", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col0_i128p7s2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col1_i128p7s2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col0_i128p18s9", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col1_i128p18s9", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col0_i128p30s9", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"col1_i128p30s9", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                      {"PART_x", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                                      {"PART_y", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                                      {""}};
std::string mtypes_orc_file = "./be/test/exec/test_data/orc_scanner/mtypes_100.orc.zlib";

static SlotDesc mtypes_orc_min_max_descs[] = {{"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
                                              {"PART_y", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                                              {""}};

TEST_F(HdfsScannerTest, TestOrcGetNext) {
    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(mtypes_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(mtypes_orc_descs);
    auto* ctx = _create_ctx(mtypes_orc_file, range, tuple_desc);
    // partition values for [PART_x, PART_y]
    std::vector<int64_t> values = {10, 20};
    std::vector<ExprContext*> part_ctxs;
    extend_partition_values(&_pool, ctx, &part_ctxs, values);

    ASSERT_OK(ExprExecutor::prepare(part_ctxs, _runtime_state));
    ASSERT_OK(ExprExecutor::open(part_ctxs, _runtime_state));

    ctx->partition_expr_ctxs = std::move(part_ctxs);
    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 100);
    EXPECT_EQ(scanner->raw_rows_read(), 100);
    scanner->close();
}

TEST_F(HdfsScannerTest, TestOrcSkipFile) {
    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(mtypes_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(mtypes_orc_descs);
    auto* ctx = _create_ctx(mtypes_orc_file, range, tuple_desc);
    // partition values for [PART_x, PART_y]
    std::vector<int64_t> values = {10, 20};
    std::vector<ExprContext*> part_ctxs;
    extend_partition_values(&_pool, ctx, &part_ctxs, values);

    ASSERT_OK(ExprExecutor::prepare(part_ctxs, _runtime_state));
    ASSERT_OK(ExprExecutor::open(part_ctxs, _runtime_state));

    ctx->partition_expr_ctxs = std::move(part_ctxs);
    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());

    scanner->_should_skip_file = true;
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());

    READ_SCANNER_ROWS(scanner, 0);
    EXPECT_EQ(scanner->raw_rows_read(), 0);
    scanner->close();
}

class BadOrcFileStream : public ORCHdfsFileStream {
public:
    BadOrcFileStream() : ORCHdfsFileStream(nullptr, 1024 * 1024, nullptr) {}
    void read(void* buf, uint64_t length, uint64_t offset) override {
        errno = ret_errno;
        throw std::runtime_error(ret_message);
    }
    int ret_errno = 0;
    std::string ret_message;
};

TEST_F(HdfsScannerTest, TestOrcReaderException) {
    struct ErrorContent {
        int ret_errno;
        std::string ret_message;
        TStatusCode::type ret_code;
    };

    std::vector<ErrorContent> error_contents = {
            ErrorContent{.ret_errno = 0, .ret_message = "read error", .ret_code = TStatusCode::INTERNAL_ERROR},
            ErrorContent{.ret_errno = ENOENT,
                         .ret_message = "S3 SDK Error. Code = 404",
                         .ret_code = TStatusCode::REMOTE_FILE_NOT_FOUND},
    };

    for (const auto& ec : error_contents) {
        auto scanner = std::make_shared<HdfsOrcScanner>();

        auto* range = _create_scan_range(mtypes_orc_file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(mtypes_orc_descs);
        auto* ctx = _create_ctx(mtypes_orc_file, range, tuple_desc);

        // partition values for [PART_x, PART_y]
        std::vector<int64_t> values = {10, 20};
        std::vector<ExprContext*> part_ctxs;
        extend_partition_values(&_pool, ctx, &part_ctxs, values);

        ASSERT_OK(ExprExecutor::prepare(part_ctxs, _runtime_state));
        ASSERT_OK(ExprExecutor::open(part_ctxs, _runtime_state));

        std::unique_ptr<BadOrcFileStream> file_stream(new BadOrcFileStream());
        file_stream->ret_errno = ec.ret_errno;
        file_stream->ret_message = ec.ret_message;
        scanner->_input_stream = std::move(file_stream);

        ctx->partition_expr_ctxs = std::move(part_ctxs);
        Status status = scanner->init(_runtime_state, ctx);
        EXPECT_TRUE(status.ok());

        status = scanner->open(_runtime_state);
        EXPECT_FALSE(status.ok()) << status.message();
        EXPECT_EQ(status.code(), ec.ret_code);
        scanner->close();
    }
}

static void extend_mtypes_orc_min_max_conjuncts(ObjectPool* pool, HdfsScannerContext* ctx,
                                                const std::vector<int>& values) {
    const TupleDescriptor* min_max_tuple_desc = ctx->min_max_tuple_desc;

    // id >= values[0] && id <= values[1] && part_y >= values[2] && part_y <= values[3]
    // id min/max = 2629/5212
    // so there should be no row matched.
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_int_literal_node(TPrimitiveType::BIGINT, values[0]);
        push_binary_pred_texpr_node(nodes, TExprOpcode::GE, min_max_tuple_desc->slots()[0], TPrimitiveType::BIGINT,
                                    lit_node);
        ExprContext* expr_ctx = create_expr_context(pool, nodes);
        ctx->format_scan_context.conjuncts.min_max_ctxs.push_back(expr_ctx);
    }

    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_int_literal_node(TPrimitiveType::BIGINT, values[1]);
        push_binary_pred_texpr_node(nodes, TExprOpcode::LE, min_max_tuple_desc->slots()[0], TPrimitiveType::BIGINT,
                                    lit_node);
        ExprContext* expr_ctx = create_expr_context(pool, nodes);
        ctx->format_scan_context.conjuncts.min_max_ctxs.push_back(expr_ctx);
    }

    // PART_Y is always 20
    // to test capability to read non-file column from partition column
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_int_literal_node(TPrimitiveType::INT, values[2]);
        push_binary_pred_texpr_node(nodes, TExprOpcode::GE, min_max_tuple_desc->slots()[1], TPrimitiveType::INT,
                                    lit_node);
        ExprContext* expr_ctx = create_expr_context(pool, nodes);
        ctx->format_scan_context.conjuncts.min_max_ctxs.push_back(expr_ctx);
    }

    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_int_literal_node(TPrimitiveType::INT, values[3]);
        push_binary_pred_texpr_node(nodes, TExprOpcode::LE, min_max_tuple_desc->slots()[1], TPrimitiveType::INT,
                                    lit_node);
        ExprContext* expr_ctx = create_expr_context(pool, nodes);
        ctx->format_scan_context.conjuncts.min_max_ctxs.push_back(expr_ctx);
    }
}

TEST_F(HdfsScannerTest, TestOrcGetNextWithMinMaxFilterNoRows) {
    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(mtypes_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(mtypes_orc_descs);
    auto* ctx = _create_ctx(mtypes_orc_file, range, tuple_desc);
    // partition values for [PART_x, PART_y]
    std::vector<int64_t> values = {10, 20};
    std::vector<ExprContext*> part_ctxs;
    extend_partition_values(&_pool, ctx, &part_ctxs, values);

    ASSERT_OK(ExprExecutor::prepare(part_ctxs, _runtime_state));
    ASSERT_OK(ExprExecutor::open(part_ctxs, _runtime_state));

    // TupleDescriptor* min_max_tuple_desc = _create_tuple_desc(mtypes_orc_min_max_descs);
    TupleDescriptor* min_max_tuple_desc = nullptr;
    {
        TDescriptorTableBuilder table_desc_builder;
        TSlotDescriptorBuilder slot_desc_builder;
        TTupleDescriptorBuilder tuple_desc_builder;

        slot_desc_builder.column_name("id")
                .type(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT))
                .id(0)
                .nullable(true);
        tuple_desc_builder.add_slot(slot_desc_builder.build());
        slot_desc_builder.column_name("PART_y")
                .type(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT))
                .id(25)
                .nullable(true);
        tuple_desc_builder.add_slot(slot_desc_builder.build());

        tuple_desc_builder.build(&table_desc_builder);
        std::vector<TTupleId> row_tuples = std::vector<TTupleId>{0};
        DescriptorTbl* tbl = nullptr;
        CHECK(DescriptorTbl::create(_runtime_state, &_pool, table_desc_builder.desc_tbl(), &tbl,
                                    config::vector_chunk_size)
                      .ok());
        min_max_tuple_desc = tbl->get_tuple_descriptor(row_tuples[0]);
    }

    ctx->min_max_tuple_desc = min_max_tuple_desc;
    // id min/max = 2629/5212, PART_Y min/max=20/20
    std::vector<int> thres = {20, 30, 20, 20};
    extend_mtypes_orc_min_max_conjuncts(&_pool, ctx, thres);
    ASSERT_OK(ExprExecutor::prepare(ctx->format_scan_context.conjuncts.min_max_ctxs, _runtime_state));
    ASSERT_OK(ExprExecutor::open(ctx->format_scan_context.conjuncts.min_max_ctxs, _runtime_state));

    ctx->partition_expr_ctxs = std::move(part_ctxs);
    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 0);
    EXPECT_EQ(scanner->raw_rows_read(), 0);
    scanner->close();
}

TEST_F(HdfsScannerTest, TestOrcGetNextWithMinMaxFilterRows1) {
    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(mtypes_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(mtypes_orc_descs);
    auto* ctx = _create_ctx(mtypes_orc_file, range, tuple_desc);
    // partition values for [PART_x, PART_y]
    std::vector<int64_t> values = {10, 20};
    std::vector<ExprContext*> part_ctxs;
    extend_partition_values(&_pool, ctx, &part_ctxs, values);

    ASSERT_OK(ExprExecutor::prepare(part_ctxs, _runtime_state));
    ASSERT_OK(ExprExecutor::open(part_ctxs, _runtime_state));

    auto* min_max_tuple_desc = _create_tuple_desc(mtypes_orc_min_max_descs);
    ctx->min_max_tuple_desc = min_max_tuple_desc;
    // id min/max = 2629/5212, PART_Y min/max=20/20
    std::vector<int> thres = {2000, 5000, 20, 20};
    extend_mtypes_orc_min_max_conjuncts(&_pool, ctx, thres);
    ASSERT_OK(ExprExecutor::prepare(ctx->format_scan_context.conjuncts.min_max_ctxs, _runtime_state));
    ASSERT_OK(ExprExecutor::open(ctx->format_scan_context.conjuncts.min_max_ctxs, _runtime_state));

    ctx->partition_expr_ctxs = std::move(part_ctxs);
    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 100);
    EXPECT_EQ(scanner->raw_rows_read(), 100);
    scanner->close();
}

TEST_F(HdfsScannerTest, TestOrcGetNextWithMinMaxFilterRows2) {
    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(mtypes_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(mtypes_orc_descs);
    auto* ctx = _create_ctx(mtypes_orc_file, range, tuple_desc);
    // partition values for [PART_x, PART_y]
    std::vector<int64_t> values = {10, 20};
    std::vector<ExprContext*> part_ctxs;
    extend_partition_values(&_pool, ctx, &part_ctxs, values);

    ASSERT_OK(ExprExecutor::prepare(part_ctxs, _runtime_state));
    ASSERT_OK(ExprExecutor::open(part_ctxs, _runtime_state));

    auto* min_max_tuple_desc = _create_tuple_desc(mtypes_orc_min_max_descs);
    ctx->min_max_tuple_desc = min_max_tuple_desc;
    // id min/max = 2629/5212, PART_Y min/max=20/20
    std::vector<int> thres = {3000, 10000, 20, 20};
    extend_mtypes_orc_min_max_conjuncts(&_pool, ctx, thres);
    ASSERT_OK(ExprExecutor::prepare(ctx->format_scan_context.conjuncts.min_max_ctxs, _runtime_state));
    ASSERT_OK(ExprExecutor::open(ctx->format_scan_context.conjuncts.min_max_ctxs, _runtime_state));

    ctx->partition_expr_ctxs = std::move(part_ctxs);
    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 100);
    EXPECT_EQ(scanner->raw_rows_read(), 100);
    scanner->close();
}

// ====================================================================================================

/**
File be/test/exec/test_data/orc_scanner/string_key_value_10k.orc.zstd has 2 stripes
*** Stripe 0 ***

--- Column 0 ---
Column has 5120 values and has null value: no

--- Column 1 ---
Data type: String
Values: 5120
Has null: no
Minimum: aaaaaaaaaa
Maximum: ffffffffff
Total length: 51200

*** Stripe 1 ***

--- Column 0 ---
Column has 4880 values and has null value: no

--- Column 1 ---
Data type: String
Values: 4880
Has null: no
Minimum: ffffffffff
Maximum: jjjjjjjjjj
Total length: 48800
 */

TEST_F(HdfsScannerTest, TestOrcGetNextWithDictFilter) {
    SlotDesc string_key_value_orc_desc[] = {{"key", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                            {"value", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                            {""}};
    const std::string string_key_value_orc_file = "./be/test/exec/test_data/orc_scanner/string_key_value_10k.orc.zstd";

    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(string_key_value_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(string_key_value_orc_desc);
    auto* ctx = _create_ctx(string_key_value_orc_file, range, tuple_desc);

    // all in stripe1
    // and there are 1000 occurrences.
    // but we will read 4880 rows.

    // key = "gggggggg"
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_string_literal_node(TPrimitiveType::VARCHAR, "gggggggggg");
        push_binary_pred_texpr_node(nodes, TExprOpcode::EQ, tuple_desc->slots()[0], TPrimitiveType::VARCHAR, lit_node);
        ExprContext* expr_ctx = create_expr_context(&_pool, nodes);
        std::cout << "equal expr = " << expr_ctx->root()->debug_string() << std::endl;
        ctx->format_scan_context.conjuncts.by_slot[0].push_back(expr_ctx);
    }

    for (auto& it : ctx->format_scan_context.conjuncts.by_slot) {
        ASSERT_OK(ExprExecutor::prepare(it.second, _runtime_state));
        ASSERT_OK(ExprExecutor::open(it.second, _runtime_state));
    }

    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());

    // so stripe will not be filtered out by search argument
    // and we can test dict-filtering strategy.
    scanner->disable_use_orc_sargs();
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 1000);
    // since we use dict filter eval cache, we can do filter on orc cvb
    // so actually read rows is 1000.
    EXPECT_EQ(scanner->raw_rows_read(), 1000);
    scanner->close();
}

// ====================================================================================================
// Iceberg `_pos` on ORC: the scanner fills it from the ORC row number.

static SlotDesc string_key_value_with_pos_orc_desc[] = {
        {"key", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
        {"value", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
        {"_pos", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
        {""}};
static const std::string string_key_value_10k_orc_file =
        "./be/test/exec/test_data/orc_scanner/string_key_value_10k.orc.zstd";

// Reads every row of `scanner` as (key, _pos).
static Status read_keys_and_row_positions(HdfsScanner* scanner, RuntimeState* state, const TupleDescriptor& tuple_desc,
                                          std::vector<std::pair<std::string, int64_t>>* rows) {
    const SlotId key_slot = tuple_desc.slots()[0]->id();
    const SlotId pos_slot = tuple_desc.slots()[2]->id();
    ChunkPtr chunk = RuntimeChunkHelper::new_chunk(tuple_desc, 0);
    for (;;) {
        chunk->reset();
        Status st = scanner->get_next(state, &chunk);
        if (!st.ok() && !st.is_end_of_file()) return st;
        if (chunk->num_rows() > 0) {
            const auto& keys = chunk->get_column_by_slot_id(key_slot);
            const auto& positions = chunk->get_column_by_slot_id(pos_slot);
            for (size_t i = 0; i < chunk->num_rows(); i++) {
                rows->emplace_back(keys->get(i).get_slice().to_string(), positions->get(i).get_int64());
            }
        }
        if (st.is_end_of_file()) return Status::OK();
    }
}

TEST_F(HdfsScannerTest, TestOrcIcebergRowPositionWithParquetPositionDelete) {
    const std::string tmp_dir = "./ut_dir/hdfs_scanner_orc_row_position_test";
    const std::string delete_path = tmp_dir + "/position_delete.parquet";
    (void)FileSystem::Default()->delete_dir_recursive(tmp_dir);
    ASSERT_OK(FileSystem::Default()->create_dir_recursive(tmp_dir));

    // a Parquet position delete file for an ORC data file: rows 0, 4097 (2nd batch) and 9999 (last row) are deleted.
    {
        std::vector type_descs{TypeDescriptor::from_logical_type(TYPE_VARCHAR),
                               TypeDescriptor::from_logical_type(TYPE_BIGINT)};
        auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
        auto writer_options = std::make_shared<formats::ParquetWriterOptions>();
        writer_options->column_ids = {
                formats::FileColumnId{formats::IcebergDeleteFileMeta::get_delete_file_path_slot().id(), {}},
                formats::FileColumnId{formats::IcebergDeleteFileMeta::get_delete_file_pos_slot().id(), {}}};
        ASSIGN_OR_ABORT(auto writable_file, FileSystem::Default()->new_writable_file(delete_path));
        auto output_stream = std::make_shared<parquet::ParquetOutputStream>(std::move(writable_file));
        formats::ParquetFileWriter writer(delete_path, std::move(output_stream), {"file_path", "pos"}, type_descs,
                                          std::move(column_evaluators), TCompressionType::NO_COMPRESSION,
                                          std::move(writer_options), [] {}, {false, false});
        ASSERT_OK(writer.init());
        std::vector<Slice> file_paths{Slice(string_key_value_10k_orc_file), Slice("another_data_file.orc"),
                                      Slice(string_key_value_10k_orc_file), Slice(string_key_value_10k_orc_file)};
        std::vector<int64_t> delete_positions{0, 1, 4097, 9999};
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnTestHelper::build_column<Slice>(file_paths), 0);
        chunk->append_column(ColumnTestHelper::build_column<int64_t>(delete_positions), 1);
        ASSERT_OK(writer.write(chunk.get()));
        ASSERT_OK(writer.close().io_status);
    }
    ASSIGN_OR_ABORT(const int64_t delete_file_size, FileSystem::Default()->get_file_size(delete_path));
    TIcebergDeleteFile delete_file;
    delete_file.__set_full_path(delete_path);
    delete_file.__set_file_format(THdfsFileFormat::PARQUET);
    delete_file.__set_file_content(TIcebergFileContent::POSITION_DELETES);
    delete_file.__set_length(delete_file_size);

    auto scanner = std::make_shared<HdfsOrcScanner>();
    auto* range = _create_scan_range(string_key_value_10k_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(string_key_value_with_pos_orc_desc);
    auto* ctx = _create_ctx(string_key_value_10k_orc_file, range, tuple_desc);
    ctx->profile.runtime_profile = _runtime_profile;
    ctx->table_specific.iceberg_delete_files.emplace_back(&delete_file);

    ASSERT_OK(scanner->init(_runtime_state, ctx));
    ASSERT_OK(scanner->open(_runtime_state));
    std::vector<std::pair<std::string, int64_t>> rows;
    ASSERT_OK(read_keys_and_row_positions(scanner.get(), _runtime_state, *tuple_desc, &rows));
    // The delete builder needs a runtime profile, but the scanner's own counters are created by HiveDataSource,
    // which this test does not build; clear the profile so close() skips update_counter().
    ctx->profile.runtime_profile = nullptr;
    scanner->close();

    // `_pos` is the row's position in the whole file, so it keeps counting across batches and stripes,
    // and the deleted positions are gone.
    std::vector<int64_t> expected_positions;
    for (int64_t pos = 0; pos < 10000; pos++) {
        if (pos != 0 && pos != 4097 && pos != 9999) expected_positions.push_back(pos);
    }
    std::vector<int64_t> positions;
    for (const auto& row : rows) positions.push_back(row.second);
    EXPECT_EQ(expected_positions, positions);

    (void)FileSystem::Default()->delete_dir_recursive(tmp_dir);
}

// A dict filter drops rows from the ORC batch; `_pos` must be filtered the same way and stay aligned with its row.
TEST_F(HdfsScannerTest, TestOrcIcebergRowPositionWithDictFilter) {
    auto* tuple_desc = _create_tuple_desc(string_key_value_with_pos_orc_desc);

    std::vector<std::pair<std::string, int64_t>> all_rows;
    {
        auto scanner = std::make_shared<HdfsOrcScanner>();
        auto* range = _create_scan_range(string_key_value_10k_orc_file, 0, 0);
        auto* ctx = _create_ctx(string_key_value_10k_orc_file, range, tuple_desc);
        ASSERT_OK(scanner->init(_runtime_state, ctx));
        ASSERT_OK(scanner->open(_runtime_state));
        ASSERT_OK(read_keys_and_row_positions(scanner.get(), _runtime_state, *tuple_desc, &all_rows));
        scanner->close();
    }
    ASSERT_EQ(10000u, all_rows.size());
    std::vector<std::string> key_by_pos(all_rows.size());
    for (size_t i = 0; i < all_rows.size(); i++) {
        ASSERT_EQ(static_cast<int64_t>(i), all_rows[i].second);
        key_by_pos[i] = all_rows[i].first;
    }

    auto scanner = std::make_shared<HdfsOrcScanner>();
    auto* range = _create_scan_range(string_key_value_10k_orc_file, 0, 0);
    auto* ctx = _create_ctx(string_key_value_10k_orc_file, range, tuple_desc);
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_string_literal_node(TPrimitiveType::VARCHAR, "gggggggggg");
        push_binary_pred_texpr_node(nodes, TExprOpcode::EQ, tuple_desc->slots()[0], TPrimitiveType::VARCHAR, lit_node);
        ctx->format_scan_context.conjuncts.by_slot[tuple_desc->slots()[0]->id()].push_back(
                create_expr_context(&_pool, nodes));
    }
    for (auto& it : ctx->format_scan_context.conjuncts.by_slot) {
        ASSERT_OK(ExprExecutor::prepare(it.second, _runtime_state));
        ASSERT_OK(ExprExecutor::open(it.second, _runtime_state));
    }
    ASSERT_OK(scanner->init(_runtime_state, ctx));
    scanner->disable_use_orc_sargs();
    ASSERT_OK(scanner->open(_runtime_state));
    std::vector<std::pair<std::string, int64_t>> rows;
    ASSERT_OK(read_keys_and_row_positions(scanner.get(), _runtime_state, *tuple_desc, &rows));
    // the dict filter ran on the ORC batch, so only the matching rows were materialized
    EXPECT_EQ(1000, scanner->raw_rows_read());
    scanner->close();

    ASSERT_EQ(1000u, rows.size());
    for (const auto& [key, pos] : rows) {
        ASSERT_EQ("gggggggggg", key);
        ASSERT_GE(pos, 0);
        ASSERT_LT(pos, static_cast<int64_t>(key_by_pos.size()));
        EXPECT_EQ(key, key_by_pos[pos]) << "pos=" << pos;
    }
}

// The scanner fills `_pos` itself, so it also evaluates the conjuncts on `_pos`.
TEST_F(HdfsScannerTest, TestOrcIcebergRowPositionConjunct) {
    auto scanner = std::make_shared<HdfsOrcScanner>();
    auto* range = _create_scan_range(string_key_value_10k_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(string_key_value_with_pos_orc_desc);
    auto* ctx = _create_ctx(string_key_value_10k_orc_file, range, tuple_desc);
    SlotDescriptor* pos_slot = tuple_desc->slots()[2];
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_int_literal_node(TPrimitiveType::BIGINT, 10);
        push_binary_pred_texpr_node(nodes, TExprOpcode::LT, pos_slot, TPrimitiveType::BIGINT, lit_node);
        ctx->format_scan_context.conjuncts.by_slot[pos_slot->id()].push_back(create_expr_context(&_pool, nodes));
    }
    for (auto& it : ctx->format_scan_context.conjuncts.by_slot) {
        ASSERT_OK(ExprExecutor::prepare(it.second, _runtime_state));
        ASSERT_OK(ExprExecutor::open(it.second, _runtime_state));
    }
    ASSERT_OK(scanner->init(_runtime_state, ctx));
    scanner->disable_use_orc_sargs();
    ASSERT_OK(scanner->open(_runtime_state));
    std::vector<std::pair<std::string, int64_t>> rows;
    ASSERT_OK(read_keys_and_row_positions(scanner.get(), _runtime_state, *tuple_desc, &rows));
    scanner->close();

    std::vector<int64_t> positions;
    for (const auto& row : rows) positions.push_back(row.second);
    EXPECT_EQ((std::vector<int64_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}), positions);
}

// ====================================================================================================

/**
 *
File be/test/exec/test_data/orc_scanner/two-strips-dict-and-nodict.orc has 2 stripes
Stripe Statistics:
  Stripe 1:
    Column 0: count: 100 hasNull: false
    Column 1: count: 100 hasNull: false bytesOnDisk: 19 min: 0 max: 9 sum: 450
    Column 2: count: 100 hasNull: false bytesOnDisk: 17 min: a max: a sum: 100
  Stripe 2:
    Column 0: count: 100 hasNull: false
    Column 1: count: 100 hasNull: false bytesOnDisk: 20 min: 0 max: 9 sum: 451
    Column 2: count: 100 hasNull: false bytesOnDisk: 1234 min: a max: ztrddxrhvzodrpeomoie sum: 1981

File Statistics:
  Column 0: count: 200 hasNull: false
  Column 1: count: 200 hasNull: false bytesOnDisk: 39 min: 0 max: 9 sum: 901
  Column 2: count: 200 hasNull: false bytesOnDisk: 1251 min: a max: ztrddxrhvzodrpeomoie sum: 2081

Stripes:
 Stripe1: offset: 3 data: 36 rows: 100 tail: 52 index: 63
   Stream: column 0 section ROW_INDEX start: 3 length 11
   Stream: column 1 section ROW_INDEX start: 14 length 25
   Stream: column 2 section ROW_INDEX start: 39 length 27
   Stream: column 1 section DATA start: 66 length 19
   Stream: column 2 section DATA start: 85 length 7
   Stream: column 2 section LENGTH start: 92 length 6
   Stream: column 2 section DICTIONARY_DATA start: 98 length 4
   Encoding column 0: DIRECT
   Encoding column 1: DIRECT_V2
   Encoding column 2: DICTIONARY_V2[1]
 Stripe2: offset: 154 data: 1254 rows: 100 tail: 50 index: 83
   Stream: column 0 section ROW_INDEX start: 154 length 11
   Stream: column 1 section ROW_INDEX start: 165 length 25
   Stream: column 2 section ROW_INDEX start: 190 length 47
   Stream: column 1 section DATA start: 237 length 20
   Stream: column 2 section DATA start: 257 length 1224
   Stream: column 2 section LENGTH start: 1481 length 10
   Encoding column 0: DIRECT
   Encoding column 1: DIRECT_V2
   Encoding column 2: DIRECT_V2
*/

TEST_F(HdfsScannerTest, TestOrcGetNextWithDiffEncodeDictFilter) {
    SlotDesc two_diff_encode_orc_desc[] = {{"x", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                                           {"y", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                           {""}};
    const std::string two_diff_encode_orc_file = "./be/test/exec/test_data/orc_scanner/two-strips-dict-and-nodict.orc";

    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(two_diff_encode_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(two_diff_encode_orc_desc);
    auto* ctx = _create_ctx(two_diff_encode_orc_file, range, tuple_desc);

    // key = "a", y = 'a' rows is 101
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_string_literal_node(TPrimitiveType::VARCHAR, "a");
        push_binary_pred_texpr_node(nodes, TExprOpcode::EQ, tuple_desc->slots()[1], TPrimitiveType::VARCHAR, lit_node);
        ExprContext* expr_ctx = create_expr_context(&_pool, nodes);
        std::cout << "equal expr = " << expr_ctx->root()->debug_string() << std::endl;
        ctx->format_scan_context.conjuncts.by_slot[1].push_back(expr_ctx);
    }

    for (auto& it : ctx->format_scan_context.conjuncts.by_slot) {
        ASSERT_OK(ExprExecutor::prepare(it.second, _runtime_state));
        ASSERT_OK(ExprExecutor::open(it.second, _runtime_state));
    }

    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());

    // so stripe will not be filtered out by search argument
    // and we can test dict-filtering strategy.
    scanner->disable_use_orc_sargs();
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 101);
    // since we use dict filter eval cache, we can do filter on orc cvb
    // so actually read rows is 200.
    EXPECT_EQ(scanner->raw_rows_read(), 200);
    scanner->close();
}

// ====================================================================================================

/**
 *
datetime are all in UTC timezone.

File Version: 0.12 with ORC_CPP_ORIGINAL
Rows: 20000
Compression: ZLIB
Compression size: 65536
Calendar: Julian/Gregorian
Type: struct<c0:timestamp,c1:varchar(60)>

Stripe Statistics:
  Stripe 1:
    Column 0: count: 5120 hasNull: false
    Column 1: count: 5120 hasNull: false min: 2021-05-25 04:43:22.941 max: 2021-05-25 06:08:41.941999999
    Column 2: count: 5120 hasNull: false min: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa max: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa sum: 307200
  Stripe 2:
    Column 0: count: 5120 hasNull: false
    Column 1: count: 5120 hasNull: false min: 2021-05-25 06:08:42.941 max: 2021-05-25 07:34:01.941999999
    Column 2: count: 5120 hasNull: false min: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa max: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa sum: 307200
  Stripe 3:
    Column 0: count: 5120 hasNull: false
    Column 1: count: 5120 hasNull: false min: 2021-05-25 07:34:02.941 max: 2021-05-25 08:59:21.941999999
    Column 2: count: 5120 hasNull: false min: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa max: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa sum: 307200
  Stripe 4:
    Column 0: count: 4640 hasNull: false
    Column 1: count: 4640 hasNull: false min: 2021-05-25 08:59:22.941 max: 2021-05-25 10:16:41.941999999
    Column 2: count: 4640 hasNull: false min: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa max: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa sum: 278400

File Statistics:
  Column 0: count: 20000 hasNull: false
  Column 1: count: 20000 hasNull: false min: 2021-05-25 04:43:22.941 max: 2021-05-25 10:16:41.941999999
  Column 2: count: 20000 hasNull: false min: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa max: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa sum: 1200000

Stripes:
  ....
  // last stripe. Stripe4
  Stripe: offset: 5105 data: 1423 rows: 4640 tail: 71 index: 89 timezone: GMT
    Stream: column 0 section ROW_INDEX start: 5105 length 18
    Stream: column 1 section ROW_INDEX start: 5123 length 35
    Stream: column 2 section ROW_INDEX start: 5158 length 36
    Stream: column 0 section PRESENT start: 5194 length 11
    Stream: column 1 section PRESENT start: 5205 length 11
    Stream: column 1 section DATA start: 5216 length 46
    Stream: column 1 section SECONDARY start: 5262 length 19
    Stream: column 2 section PRESENT start: 5281 length 11
    Stream: column 2 section LENGTH start: 5292 length 16
    Stream: column 2 section DATA start: 5308 length 1309
    Encoding column 0: DIRECT
    Encoding column 1: DIRECT_V2
    Encoding column 2: DIRECT_V2
*/

TEST_F(HdfsScannerTest, TestOrcGetNextWithDatetimeMinMaxFilter) {
    SlotDesc datetime_orc_descs[] = {{"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME)}, {""}};
    const std::string datetime_orc_file = "./be/test/exec/test_data/orc_scanner/datetime_20k.orc.zlib";

    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(datetime_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(datetime_orc_descs);
    auto* ctx = _create_ctx(datetime_orc_file, range, tuple_desc);

    ctx->min_max_tuple_desc = tuple_desc;
    const TupleDescriptor* min_max_tuple_desc = ctx->min_max_tuple_desc;

    // expect c0 >= '2021-05-25 08:59:22'
    // which means only stripe3 matches, and all rows in stripe3 matches.
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_datetime_literal_node(TPrimitiveType::DATETIME, "2021-05-25 08:59:22");
        push_binary_pred_texpr_node(nodes, TExprOpcode::GE, min_max_tuple_desc->slots()[0], TPrimitiveType::DATETIME,
                                    lit_node);
        ExprContext* expr_ctx = create_expr_context(&_pool, nodes);
        ctx->format_scan_context.conjuncts.min_max_ctxs.push_back(expr_ctx);
    }

    ASSERT_OK(ExprExecutor::prepare(ctx->format_scan_context.conjuncts.min_max_ctxs, _runtime_state));
    ASSERT_OK(ExprExecutor::open(ctx->format_scan_context.conjuncts.min_max_ctxs, _runtime_state));

    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());

    scanner->disable_use_orc_sargs();
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 4640);
    EXPECT_EQ(scanner->raw_rows_read(), 4640);
    scanner->close();
}

// ====================================================================================================

/**
Type: struct<c0:char(100),c1:varchar(100)>

Stripe Statistics:
  Stripe 1:
    Column 0: count: 1024 hasNull: false
    Column 1: count: 1024 hasNull: false min: hello                                                                                                max: world                                                                                                sum: 102400
    Column 2: count: 1024 hasNull: false min: hello max: world sum: 5120
  Stripe 2:
    Column 0: count: 976 hasNull: false
    Column 1: count: 976 hasNull: false min: hello                                                                                                max: world                                                                                                sum: 97600
    Column 2: count: 976 hasNull: false min: hello max: world sum: 4880

File Statistics:
  Column 0: count: 2000 hasNull: false
  Column 1: count: 2000 hasNull: false min: hello                                                                                                max: world                                                                                                sum: 200000
  Column 2: count: 2000 hasNull: false min: hello max: world sum: 10000

Stripes:
  Stripe: offset: 3 data: 88 rows: 1024 tail: 75 index: 103
    Stream: column 0 section ROW_INDEX start: 3 length 18
    Stream: column 1 section ROW_INDEX start: 21 length 48
    Stream: column 2 section ROW_INDEX start: 69 length 37
    Stream: column 0 section PRESENT start: 106 length 5
    Stream: column 1 section PRESENT start: 111 length 5
    Stream: column 1 section DATA start: 116 length 13
    Stream: column 1 section DICTIONARY_DATA start: 129 length 21
    Stream: column 1 section LENGTH start: 150 length 7
    Stream: column 2 section PRESENT start: 157 length 5
    Stream: column 2 section DATA start: 162 length 13
    Stream: column 2 section DICTIONARY_DATA start: 175 length 13
    Stream: column 2 section LENGTH start: 188 length 6
    Encoding column 0: DIRECT
    Encoding column 1: DICTIONARY_V2[2]
    Encoding column 2: DICTIONARY_V2[2]
  Stripe: offset: 269 data: 90 rows: 976 tail: 77 index: 103
    Stream: column 0 section ROW_INDEX start: 269 length 18
    Stream: column 1 section ROW_INDEX start: 287 length 48
    Stream: column 2 section ROW_INDEX start: 335 length 37
    Stream: column 0 section PRESENT start: 372 length 5
    Stream: column 1 section PRESENT start: 377 length 5
    Stream: column 1 section DATA start: 382 length 14
    Stream: column 1 section DICTIONARY_DATA start: 396 length 21
    Stream: column 1 section LENGTH start: 417 length 7
    Stream: column 2 section PRESENT start: 424 length 5
    Stream: column 2 section DATA start: 429 length 14
    Stream: column 2 section DICTIONARY_DATA start: 443 length 13
    Stream: column 2 section LENGTH start: 456 length 6
    Encoding column 0: DIRECT
    Encoding column 1: DICTIONARY_V2[2]
    Encoding column 2: DICTIONARY_V2[2]

File length: 799 bytes
Padding length: 0 bytes
Padding ratio: 0%
 */

TEST_F(HdfsScannerTest, TestOrcGetNextWithPaddingCharDictFilter) {
    SlotDesc padding_char_varchar_desc[] = {{"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_CHAR)},
                                            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                            {""}};
    const std::string padding_char_varchar_orc_file =
            "./be/test/exec/test_data/orc_scanner/padding_char_varchar_10k.orc";

    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(padding_char_varchar_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(padding_char_varchar_desc);
    auto* ctx = _create_ctx(padding_char_varchar_orc_file, range, tuple_desc);

    // c0 <= "hello"
    // and we expect we can strip of ' ' in dictionary data.
    // so return 2000 rows.
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_string_literal_node(TPrimitiveType::VARCHAR, "hello");
        push_binary_pred_texpr_node(nodes, TExprOpcode::LE, tuple_desc->slots()[0], TPrimitiveType::VARCHAR, lit_node);
        ExprContext* expr_ctx = create_expr_context(&_pool, nodes);
        std::cout << "less&eq expr = " << expr_ctx->root()->debug_string() << std::endl;
        ctx->format_scan_context.conjuncts.by_slot[0].push_back(expr_ctx);
    }

    for (auto& it : ctx->format_scan_context.conjuncts.by_slot) {
        ASSERT_OK(ExprExecutor::prepare(it.second, _runtime_state));
        ASSERT_OK(ExprExecutor::open(it.second, _runtime_state));
    }

    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());

    // so stripe will not be filtered out by search argument
    // and we can test dict-filtering strategy.
    scanner->disable_use_orc_sargs();
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 1000);
    // since we use dict filter eval cache, we can do filter on orc cvb
    // so actually read rows is 1000.
    EXPECT_EQ(scanner->raw_rows_read(), 1000);
    scanner->close();
}

// ====================================================================================================

/*

Structure for writer_tz_shanghai.orc
File Version: 0.12 with ORC_14 by ORC Java 1.8.0-SNAPSHOT
Rows: 1
Compression: ZLIB
Compression size: 262144
Calendar: Julian/Gregorian
Type: struct<c0:timestamp,c1:date>

Stripe Statistics:
  Stripe 1:
    Column 0: count: 1 hasNull: false
    Column 1: count: 1 hasNull: false bytesOnDisk: 15 min: 2022-04-09 07:13:00.0 max: 2022-04-09 07:13:00.0 raw: min=0, min-utc=1649488380000, max=0, max-utc=1649488380000
    Column 2: count: 1 hasNull: false bytesOnDisk: 7 min: Hybrid AD 2022-04-09 max: Hybrid AD 2022-04-09

File Statistics:
  Column 0: count: 1 hasNull: false
  Column 1: count: 1 hasNull: false bytesOnDisk: 15 min: 2022-04-09 07:13:00.0 max: 2022-04-09 07:13:00.0 raw: min=0, min-utc=1649488380000, max=0, max-utc=1649488380000
  Column 2: count: 1 hasNull: false bytesOnDisk: 7 min: Hybrid AD 2022-04-09 max: Hybrid AD 2022-04-09

Stripes:
  Stripe: offset: 3 data: 22 rows: 1 tail: 61 index: 68 timezone: Asia/Shanghai


{"c0":"2022-04-09 07:13:00.0","c1":"2022-04-09"}
 
*/

/*

Structure for writer_tz_utc.orc
File Version: 0.12 with ORC_14 by ORC Java 1.8.0-SNAPSHOT
Rows: 1
Compression: ZLIB
Compression size: 262144
Calendar: Julian/Gregorian
Type: struct<c0:timestamp,c1:date>

Stripe Statistics:
  Stripe 1:
    Column 0: count: 1 hasNull: false
    Column 1: count: 1 hasNull: false bytesOnDisk: 15 min: 2022-04-09 07:13:00.0 max: 2022-04-09 07:13:00.0 raw: min=0, min-utc=1649488380000, max=0, max-utc=1649488380000
    Column 2: count: 1 hasNull: false bytesOnDisk: 7 min: Hybrid AD 2022-04-09 max: Hybrid AD 2022-04-09

File Statistics:
  Column 0: count: 1 hasNull: false
  Column 1: count: 1 hasNull: false bytesOnDisk: 15 min: 2022-04-09 07:13:00.0 max: 2022-04-09 07:13:00.0 raw: min=0, min-utc=1649488380000, max=0, max-utc=1649488380000
  Column 2: count: 1 hasNull: false bytesOnDisk: 7 min: Hybrid AD 2022-04-09 max: Hybrid AD 2022-04-09

Stripes:
  Stripe: offset: 3 data: 22 rows: 1 tail: 51 index: 68 timezone: UTC

{"c0":"2022-04-09 07:13:00.0","c1":"2022-04-09"}

*/

TEST_F(HdfsScannerTest, TestOrcDecodeMinMaxDateTime) {
    SlotDesc timezone_datetime_slot_descs[] = {{"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME)},
                                               {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATE)},
                                               {""}};

    // They are a timestamp type, we will ignore all timezone information
    const std::string timezone_datetime_shanghai_orc_file =
            "./be/test/exec/test_data/orc_scanner/writer_tz_shanghai.orc";
    const std::string timezone_datetime_utc_orc_file = "./be/test/exec/test_data/orc_scanner/writer_tz_utc.orc";

    struct Case {
        std::string file;
        std::string literal;
        std::string query_timezone;
        int exp;
    };
    std::vector<Case> cases = {
            {timezone_datetime_shanghai_orc_file, "2022-04-09 07:13:00", "Asia/Shanghai", 1},
            {timezone_datetime_shanghai_orc_file, "2022-04-09 07:13:00", "UTC", 1},
            {timezone_datetime_shanghai_orc_file, "2022-04-08 23:13:00", "UTC", 0},
            {timezone_datetime_utc_orc_file, "2022-04-09 07:13:00", "Asia/Shanghai", 1},
            {timezone_datetime_utc_orc_file, "2022-04-09 15:13:00", "Asia/Shanghai", 0},
            {timezone_datetime_utc_orc_file, "2022-04-09 07:13:00", "UTC", 1},
    };

    for (const Case& c : cases) {
        _create_runtime_state(c.query_timezone);
        std::cout << "Query in timezone = " << _runtime_state->timezone() << std::endl;
        auto* range = _create_scan_range(c.file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(timezone_datetime_slot_descs);
        auto* ctx = _create_ctx(c.file, range, tuple_desc);

        ctx->min_max_tuple_desc = tuple_desc;
        const TupleDescriptor* min_max_tuple_desc = ctx->min_max_tuple_desc;

        {
            std::vector<TExprNode> nodes;
            TExprNode lit_node = create_datetime_literal_node(TPrimitiveType::DATETIME, c.literal);
            push_binary_pred_texpr_node(nodes, TExprOpcode::EQ, min_max_tuple_desc->slots()[0],
                                        TPrimitiveType::DATETIME, lit_node);
            ExprContext* expr_ctx = create_expr_context(&_pool, nodes);
            ctx->format_scan_context.conjuncts.min_max_ctxs.push_back(expr_ctx);
        }

        ASSERT_OK(ExprExecutor::prepare(ctx->format_scan_context.conjuncts.min_max_ctxs, _runtime_state));
        ASSERT_OK(ExprExecutor::open(ctx->format_scan_context.conjuncts.min_max_ctxs, _runtime_state));

        auto scanner = std::make_shared<HdfsOrcScanner>();
        Status status = scanner->init(_runtime_state, ctx);
        EXPECT_TRUE(status.ok());

        scanner->disable_use_orc_sargs();
        status = scanner->open(_runtime_state);
        EXPECT_TRUE(status.ok()) << status.to_string();
        READ_SCANNER_ROWS(scanner, c.exp);
        scanner->close();
    }
}

/**
{"col1":"10"}
{"col1":"10000"}
{"col1":"100001"}
{"col1":"11"}

Type: struct<col1:string>

Stripe Statistics:
       Stripe 1:
       Column 0: count: 4 hasNull: false
       Column 1: count: 4 hasNull: false bytesOnDisk: 21 min: 10 max: 11 sum: 15
 */
TEST_F(HdfsScannerTest, TestOrcDecodeMinMaxWithTypeMismatch) {
    SlotDesc slot_descs[] = {{"col1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)}, {""}};

    // They are a timestamp type, we will ignore all timezone information
    const std::string orc_file = "./be/test/exec/test_data/orc_scanner/type_mismatch.orc";

    // _create_runtime_state(c.query_timezone);
    auto* range = _create_scan_range(orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(slot_descs);
    auto* ctx = _create_ctx(orc_file, range, tuple_desc);

    ctx->min_max_tuple_desc = tuple_desc;
    const TupleDescriptor* min_max_tuple_desc = ctx->min_max_tuple_desc;

    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_int_literal_node(TPrimitiveType::INT, 100);
        push_binary_pred_texpr_node(nodes, TExprOpcode::GT, min_max_tuple_desc->slots()[0], TPrimitiveType::INT,
                                    lit_node);
        ExprContext* expr_ctx = create_expr_context(&_pool, nodes);
        ctx->format_scan_context.conjuncts.min_max_ctxs.push_back(expr_ctx);
    }

    ASSERT_OK(ExprExecutor::prepare(ctx->format_scan_context.conjuncts.min_max_ctxs, _runtime_state));
    ASSERT_OK(ExprExecutor::open(ctx->format_scan_context.conjuncts.min_max_ctxs, _runtime_state));

    auto scanner = std::make_shared<HdfsOrcScanner>();
    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());

    scanner->disable_use_orc_sargs();
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok()) << status.to_string();
    READ_SCANNER_ROWS(scanner, 4);
    scanner->close();
}

// ====================================================================================================
/**
 * Processing data file 00000-26-85109feb-fc85-4ad2-9a97-4102e245220d-00001.orc [length: 1109]
Structure for 00000-26-85109feb-fc85-4ad2-9a97-4102e245220d-00001.orc
File Version: 0.12 with ORC_14 by ORC Java
Rows: 1
Compression: ZLIB
Compression size: 262144
Calendar: Julian/Gregorian
Type: struct<col_boolean:boolean,col_int:int,col_long:bigint,col_float:float,col_double:double,col_decimal:decimal(38,18),col_date:date,col_timestamp:timestamp with local time zone,col_string:string,col_binary:binary,col_struct:struct<a:string,b:int>,col_map:map<string,int * 

Stripes:
  Stripe: offset: 3 data: 60 rows: 1 tail: 160 index: 338
    Stream: column 0 section ROW_INDEX start: 3 length 11
    Stream: column 1 section ROW_INDEX start: 14 length 22
    Stream: column 2 section ROW_INDEX start: 36 length 21
    Stream: column 3 section ROW_INDEX start: 57 length 21
    Stream: column 4 section ROW_INDEX start: 78 length 24
    Stream: column 5 section ROW_INDEX start: 102 length 24
    Stream: column 6 section ROW_INDEX start: 126 length 22
    Stream: column 7 section ROW_INDEX start: 148 length 19
    Stream: column 8 section ROW_INDEX start: 167 length 19
    Stream: column 9 section ROW_INDEX start: 186 length 19
    Stream: column 10 section ROW_INDEX start: 205 length 21
    Stream: column 11 section ROW_INDEX start: 226 length 17
    Stream: column 12 section ROW_INDEX start: 243 length 18
    Stream: column 13 section ROW_INDEX start: 261 length 20
    Stream: column 14 section ROW_INDEX start: 281 length 22
    Stream: column 15 section ROW_INDEX start: 303 length 18
    Stream: column 16 section ROW_INDEX start: 321 length 20
    Stream: column 1 section PRESENT start: 341 length 5
    Stream: column 1 section DATA start: 346 length 0
    Stream: column 2 section PRESENT start: 346 length 5
    Stream: column 2 section DATA start: 351 length 0
    Stream: column 3 section PRESENT start: 351 length 5
    Stream: column 3 section DATA start: 356 length 0
    Stream: column 4 section PRESENT start: 356 length 5
    Stream: column 4 section DATA start: 361 length 0
    Stream: column 5 section PRESENT start: 361 length 5
    Stream: column 5 section DATA start: 366 length 0
    Stream: column 6 section PRESENT start: 366 length 5
    Stream: column 6 section DATA start: 371 length 0
    Stream: column 6 section SECONDARY start: 371 length 0
    Stream: column 7 section PRESENT start: 371 length 5
    Stream: column 7 section DATA start: 376 length 0
    Stream: column 8 section PRESENT start: 376 length 5
    Stream: column 8 section DATA start: 381 length 0
    Stream: column 8 section SECONDARY start: 381 length 0
    Stream: column 9 section PRESENT start: 381 length 5
    Stream: column 9 section DATA start: 386 length 0
    Stream: column 9 section LENGTH start: 386 length 0
    Stream: column 9 section DICTIONARY_DATA start: 386 length 0
    Stream: column 10 section PRESENT start: 386 length 5
    Stream: column 10 section DATA start: 391 length 0
    Stream: column 10 section LENGTH start: 391 length 0
    Stream: column 11 section PRESENT start: 391 length 5
    Stream: column 12 section DATA start: 396 length 0
    Stream: column 12 section LENGTH start: 396 length 0
    Stream: column 12 section DICTIONARY_DATA start: 396 length 0
    Stream: column 13 section DATA start: 396 length 0
    Stream: column 14 section PRESENT start: 396 length 5
    Stream: column 14 section LENGTH start: 401 length 0
    Stream: column 15 section DATA start: 401 length 0
    Stream: column 15 section LENGTH start: 401 length 0
    Stream: column 15 section DICTIONARY_DATA start: 401 length 0
    Stream: column 16 section DATA start: 401 length 0
 */
TEST_F(HdfsScannerTest, TestOrcZeroSizeStream) {
    SlotDesc slot_descs[] = {{"col_boolean", TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN)},
                             {"col_int", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                             {"col_long", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
                             {"col_float", TypeDescriptor::from_logical_type(LogicalType::TYPE_FLOAT)},
                             {"col_double", TypeDescriptor::from_logical_type(LogicalType::TYPE_DOUBLE)},
                             {"col_date", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATE)},
                             {"col_timestamp", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME)},
                             {"col_string", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                             {"col_binary", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARBINARY)},
                             {""}};

    const std::string input_orc_file = "./be/test/exec/test_data/orc_scanner/orc_zero_size_stream.orc";

    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(input_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(slot_descs);
    auto* ctx = _create_ctx(input_orc_file, range, tuple_desc);

    Status status = scanner->init(_runtime_state, ctx);
    ASSERT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 1);
    EXPECT_EQ(scanner->raw_rows_read(), 1);
    scanner->close();
}

/**
 * ORC format: struct<c0:int,c1:struct<cc0:int,Cc11:string>>
 * Data:
 * {c0: 1, c1: {cc0: 11, Cc1: "Smith"}}
 * {c0: 2, c1: {cc0: 22, Cc1: "Cruise"}}
 * {c0: 3, c1: {cc0: 33, Cc1: "hello"}}
 * {c0: 4, c1: {cc0: 44, Cc1: "world"}}
 */
TEST_F(HdfsScannerTest, TestOrcLazyLoad) {
    static const std::string input_orc_file = "./be/test/exec/test_data/orc_scanner/orc_test_struct_basic.orc";

    SlotDesc c0{"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)};
    SlotDesc c1{"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT)};
    c1.type.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    c1.type.field_names.emplace_back("Cc1");

    SlotDesc slot_descs[] = {c0, c1, {""}};

    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(input_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(slot_descs);
    auto* ctx = _create_ctx(input_orc_file, range, tuple_desc);

    // c0 >= 3
    // so return 2 rows.
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_int_literal_node(TPrimitiveType::INT, 3);
        push_binary_pred_texpr_node(nodes, TExprOpcode::GE, tuple_desc->slots()[0], TPrimitiveType::INT, lit_node);
        ExprContext* expr_ctx = create_expr_context(&_pool, nodes);
        std::cout << "greater&eq expr = " << expr_ctx->root()->debug_string() << std::endl;
        ctx->format_scan_context.conjuncts.by_slot[0].push_back(expr_ctx);
    }

    for (auto& it : ctx->format_scan_context.conjuncts.by_slot) {
        ASSERT_OK(ExprExecutor::prepare(it.second, _runtime_state));
        ASSERT_OK(ExprExecutor::open(it.second, _runtime_state));
    }

    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());

    ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
    status = scanner->get_next(_runtime_state, &chunk);
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(2, chunk->num_rows());

    EXPECT_EQ("[3, {Cc1:'hello'}]", chunk->debug_row(0));
    EXPECT_EQ("[4, {Cc1:'World'}]", chunk->debug_row(1));

    status = scanner->get_next(_runtime_state, &chunk);
    // Should be end of file in next read.
    EXPECT_TRUE(status.is_end_of_file());

    scanner->close();
}

/**
 * ORC format: struct<col1:int,col2:map<string,string>>
 * Data:
 * {"col1":1,"col2":[{"_key":"hello","_value":"world"}]}
 * {"col1":1,"col2":[{"_key":"hello","_value":"world"}]}
 * {"col1":2,"col2":[{"_key":"hello1","_value":"world1"},{"_key":"aef","_value":"efafe"}]}
 * {"col1":3,"col2":[{"_key":"hello","_value":"world"}]}
 * {"col1":1,"col2":[{"_key":"hello","_value":"world"}]}
 * {"col1":1,"col2":[{"_key":"hello","_value":"world"}]}
 * {"col1":2,"col2":[{"_key":"hello1","_value":"world1"},{"_key":"aef","_value":"efafe"}]}
 * {"col1":3,"col2":[{"_key":"hello","_value":"world"}]}
 * We will only select map values here, and then apply filter in this case
 */
TEST_F(HdfsScannerTest, TestOrcMapLazyLoadWithSubfieldSeleted) {
    static const std::string input_orc_file = "./be/test/exec/test_data/orc_scanner/map_filter_bug.orc";

    SlotDesc c0{"col1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)};
    SlotDesc c1{"col2", TypeDescriptor::from_logical_type(LogicalType::TYPE_MAP)};
    c1.type.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_UNKNOWN));
    c1.type.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));

    SlotDesc slot_descs[] = {c0, c1, {""}};

    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(input_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(slot_descs);
    auto* ctx = _create_ctx(input_orc_file, range, tuple_desc);

    // c0 == 3
    // so return 2 rows.
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_int_literal_node(TPrimitiveType::INT, 3);
        push_binary_pred_texpr_node(nodes, TExprOpcode::EQ, tuple_desc->slots()[0], TPrimitiveType::INT, lit_node);
        ExprContext* expr_ctx = create_expr_context(&_pool, nodes);
        ctx->format_scan_context.conjuncts.by_slot[0].push_back(expr_ctx);
    }

    for (auto& it : ctx->format_scan_context.conjuncts.by_slot) {
        ASSERT_OK(ExprExecutor::prepare(it.second, _runtime_state));
        ASSERT_OK(ExprExecutor::open(it.second, _runtime_state));
    }

    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());

    ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
    status = scanner->get_next(_runtime_state, &chunk);
    EXPECT_TRUE(status.ok());

    // two records across two stripes
    ASSERT_EQ(1, chunk->num_rows());
    ASSERT_EQ("[3, {NULL:'world'}]", chunk->debug_row(0));

    chunk->reset();
    status = scanner->get_next(_runtime_state, &chunk);
    ASSERT_EQ(1, chunk->num_rows());
    ASSERT_EQ("[3, {NULL:'world'}]", chunk->debug_row(0));
    // Should be end of file in next read.
    chunk->reset();
    status = scanner->get_next(_runtime_state, &chunk);
    ASSERT_EQ(0, chunk->num_rows());
    ASSERT_TRUE(status.is_end_of_file());

    scanner->close();
}

TEST_F(HdfsScannerTest, TestOrcBooleanConjunct) {
    static const std::string input_orc_file = "./be/test/exec/test_data/orc_scanner/boolean_slot_ref.orc";

    SlotDesc c0{"sex", TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN)};

    SlotDesc slot_descs[] = {c0, {""}};

    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(input_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(slot_descs);
    auto* ctx = _create_ctx(input_orc_file, range, tuple_desc);

    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node;
        lit_node.__set_node_type(TExprNodeType::SLOT_REF);
        lit_node.__set_num_children(0);
        lit_node.__set_type(create_primitive_type_desc(TPrimitiveType::BOOLEAN));
        TSlotRef t_slot_ref = TSlotRef();
        t_slot_ref.slot_id = 0;
        t_slot_ref.tuple_id = 0;
        lit_node.__set_slot_ref(t_slot_ref);
        nodes.emplace_back(lit_node);
        ExprContext* expr_ctx = create_expr_context(&_pool, nodes);
        ctx->format_scan_context.conjuncts.by_slot[0].push_back(expr_ctx);
    }

    for (auto& it : ctx->format_scan_context.conjuncts.by_slot) {
        ASSERT_OK(ExprExecutor::prepare(it.second, _runtime_state));
        ASSERT_OK(ExprExecutor::open(it.second, _runtime_state));
    }

    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());

    ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
    status = scanner->get_next(_runtime_state, &chunk);
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[1]", chunk->debug_row(0));

    scanner->close();
}

TEST_F(HdfsScannerTest, TestOrcCompoundConjunct) {
    static const std::string input_orc_file = "./be/test/exec/test_data/orc_scanner/scalar_types.orc";

    SlotDesc c0{"col_tinyint", TypeDescriptor::from_logical_type(LogicalType::TYPE_TINYINT)};
    SlotDesc c1{"col_smallint", TypeDescriptor::from_logical_type(LogicalType::TYPE_SMALLINT)};

    SlotDesc slot_descs[] = {c0, c1, {""}};

    auto scanner = std::make_shared<HdfsOrcScanner>();

    auto* range = _create_scan_range(input_orc_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(slot_descs);
    auto* ctx = _create_ctx(input_orc_file, range, tuple_desc);

    {
        std::vector<TExprNode> nodes;
        TExprNode compound_node;
        compound_node.__set_opcode(TExprOpcode::COMPOUND_OR);
        compound_node.__set_node_type(TExprNodeType::COMPOUND_PRED);
        compound_node.__set_type(create_primitive_type_desc(TPrimitiveType::BOOLEAN));
        compound_node.__set_num_children(2);
        nodes.emplace_back(compound_node);

        {
            TExprNode left_eq;
            left_eq.__set_opcode(TExprOpcode::EQ);
            left_eq.__set_node_type(TExprNodeType::BINARY_PRED);
            left_eq.__set_type(create_primitive_type_desc(TPrimitiveType::BOOLEAN));
            left_eq.__set_child_type(TPrimitiveType::TINYINT);
            left_eq.__set_num_children(2);

            TExprNode left_leaf_node;
            left_leaf_node.__set_node_type(TExprNodeType::SLOT_REF);
            left_leaf_node.__set_num_children(0);
            left_leaf_node.__set_type(create_primitive_type_desc(TPrimitiveType::TINYINT));
            TSlotRef t_slot_ref = TSlotRef();
            t_slot_ref.slot_id = 0;
            t_slot_ref.tuple_id = 0;
            left_leaf_node.__set_slot_ref(t_slot_ref);

            TExprNode left_literal = create_int_literal_node(TPrimitiveType::TINYINT, 1);

            nodes.emplace_back(left_eq);
            nodes.emplace_back(left_leaf_node);
            nodes.emplace_back(left_literal);
        }

        {
            TExprNode right_eq;
            right_eq.__set_opcode(TExprOpcode::EQ);
            right_eq.__set_node_type(TExprNodeType::BINARY_PRED);
            right_eq.__set_type(create_primitive_type_desc(TPrimitiveType::BOOLEAN));
            right_eq.__set_child_type(TPrimitiveType::SMALLINT);
            right_eq.__set_num_children(2);

            TExprNode right_leaf_node;
            right_leaf_node.__set_node_type(TExprNodeType::SLOT_REF);
            right_leaf_node.__set_num_children(0);
            right_leaf_node.__set_type(create_primitive_type_desc(TPrimitiveType::TINYINT));
            TSlotRef t_slot_ref = TSlotRef();
            t_slot_ref.slot_id = 1;
            t_slot_ref.tuple_id = 0;
            right_leaf_node.__set_slot_ref(t_slot_ref);

            TExprNode right_literal = create_int_literal_node(TPrimitiveType::SMALLINT, 1);

            nodes.emplace_back(right_eq);
            nodes.emplace_back(right_leaf_node);
            nodes.emplace_back(right_literal);
        }

        ExprContext* expr_ctx = create_expr_context(&_pool, nodes);
        std::cout << expr_ctx->root()->debug_string() << std::endl;
        ctx->format_scan_context.conjuncts.scanner_ctxs.push_back(expr_ctx);
    }

    ASSERT_OK(ExprExecutor::prepare(ctx->format_scan_context.conjuncts.scanner_ctxs, _runtime_state));
    ASSERT_OK(ExprExecutor::open(ctx->format_scan_context.conjuncts.scanner_ctxs, _runtime_state));

    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ("leaf-0 = (column(id=2) = 1), leaf-1 = (column(id=3) = 1), expr = (or leaf-0 leaf-1)",
              scanner->_orc_reader->get_search_argument_string());

    ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
    status = scanner->get_next(_runtime_state, &chunk);
    EXPECT_TRUE(status.is_end_of_file());
    scanner->close();
}

// The task that splits an ORC file builds its deletes once; its split tasks share them, and since the deletes are
// keyed by file-level row numbers, a split task starting at a later stripe skips the right rows.
TEST_F(HdfsScannerTest, TestOrcSplitTasksShareDeletes) {
    const std::string file = "orc_split_tasks_share_deletes.orc";
    std::filesystem::remove(file);
    const size_t num_rows = 20000;
    {
        ORC_UNIQUE_PTR<orc::Type> schema(orc::Type::buildTypeFromString("struct<id:bigint>"));
        orc::WriterOptions options;
        // force a stripe per batch
        options.setStripeSize(0);
        ORC_UNIQUE_PTR<orc::OutputStream> out = orc::writeLocalFile(file);
        ORC_UNIQUE_PTR<orc::Writer> writer = orc::createWriter(*schema, out.get(), options);
        const size_t batch_size = 1000;
        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = writer->createRowBatch(batch_size);
        auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
        auto* id = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);
        for (size_t start = 0; start < num_rows; start += batch_size) {
            for (size_t i = 0; i < batch_size; i++) {
                id->data[i] = start + i;
            }
            id->numElements = batch_size;
            root->numElements = batch_size;
            writer->add(*batch);
        }
        writer->close();
    }
    DeferOp remove_file([&]() { std::filesystem::remove(file); });
    ASSERT_GT(orc::createReader(orc::readLocalFile(file), orc::ReaderOptions{})->getNumberOfStripes(), 2);

    // every 7th row of the file is deleted
    auto bitmap = std::make_shared<DeletionBitmap>(roaring64_bitmap_create());
    for (size_t i = 0; i < num_rows; i += 7) {
        bitmap->add_value(i);
    }

    SlotDesc descs[] = {{"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)}, {""}};
    auto* tuple_desc = _create_tuple_desc(descs);
    auto* range = _create_scan_range(file, 0, 0);
    auto* ctx = _create_ctx(file, range, tuple_desc);
    ctx->format_scan_context.options.enable_split_tasks = true;
    auto scanner = std::make_shared<HdfsOrcScanner>();
    scanner->_skip_rows_ctx->deletion_bitmap = bitmap;
    ASSERT_OK(scanner->init(_runtime_state, ctx));
    ASSERT_OK(scanner->open(_runtime_state));
    const auto& split_tasks = ctx->format_scan_context.split.split_tasks;
    ASSERT_GT(split_tasks.size(), 2);
    scanner->close();

    // read the last split task alone, over its range as HiveDataSource sets it
    const FileScanSplitContext* last = split_tasks.back().get();
    auto* split_range = _create_scan_range(file, last->start_offset, last->end_offset - last->start_offset);
    auto* split_ctx = _create_ctx(file, split_range, tuple_desc);
    split_ctx->format_scan_context.split_context = last;
    auto split_scanner = std::make_shared<HdfsOrcScanner>();
    ASSERT_OK(split_scanner->init(_runtime_state, split_ctx));
    ASSERT_OK(split_scanner->open(_runtime_state));
    EXPECT_EQ(bitmap, split_scanner->_skip_rows_ctx->deletion_bitmap);

    ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
    int64_t rows = 0;
    int64_t min_id = std::numeric_limits<int64_t>::max();
    int64_t max_id = -1;
    Status status;
    for (;;) {
        chunk->reset();
        status = split_scanner->get_next(_runtime_state, &chunk);
        if (!status.ok() && !status.is_end_of_file()) break;
        const Column* col = chunk->get_column_raw_ptr_by_slot_id(0);
        for (size_t i = 0; i < chunk->num_rows(); i++) {
            int64_t v = col->get(i).get_int64();
            EXPECT_NE(0, v % 7) << v;
            min_id = std::min(min_id, v);
            max_id = std::max(max_id, v);
        }
        rows += chunk->num_rows();
        if (status.is_end_of_file()) break;
    }
    ASSERT_TRUE(status.is_end_of_file()) << status;
    ASSERT_GT(rows, 0);
    // the last stripe holds ids [19000, 20000); 19999 is a multiple of 7, so deleted
    EXPECT_EQ(19000, min_id);
    EXPECT_EQ(19998, max_id);
    int64_t live = 0;
    for (int64_t v = min_id; v < static_cast<int64_t>(num_rows); v++) {
        live += (v % 7 != 0);
    }
    EXPECT_EQ(live, rows);
    split_scanner->close();
}

// Stripes whose rows are all deleted are skipped: no IO, no split task; a scan range left with no
// stripe returns nothing.
TEST_F(HdfsScannerTest, TestOrcSkipStripesWithAllRowsDeleted) {
    const std::string file = "orc_skip_deleted_stripes.orc";
    std::filesystem::remove(file);
    const size_t num_rows = 5000;
    const size_t stripe_rows = 1000;
    {
        ORC_UNIQUE_PTR<orc::Type> schema(orc::Type::buildTypeFromString("struct<id:bigint>"));
        orc::WriterOptions options;
        // force a stripe per batch
        options.setStripeSize(0);
        ORC_UNIQUE_PTR<orc::OutputStream> out = orc::writeLocalFile(file);
        ORC_UNIQUE_PTR<orc::Writer> writer = orc::createWriter(*schema, out.get(), options);
        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = writer->createRowBatch(stripe_rows);
        auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
        auto* id = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);
        for (size_t start = 0; start < num_rows; start += stripe_rows) {
            for (size_t i = 0; i < stripe_rows; i++) {
                id->data[i] = start + i;
            }
            id->numElements = stripe_rows;
            root->numElements = stripe_rows;
            writer->add(*batch);
        }
        writer->close();
    }
    DeferOp remove_file([&]() { std::filesystem::remove(file); });
    const size_t num_stripes = orc::createReader(orc::readLocalFile(file), orc::ReaderOptions{})->getNumberOfStripes();
    ASSERT_EQ(num_rows / stripe_rows, num_stripes);

    SlotDesc descs[] = {{"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)}, {""}};
    auto* tuple_desc = _create_tuple_desc(descs);
    auto open_scanner = [&](const DeletionBitmapPtr& bitmap, bool enable_split_tasks) {
        auto* range = _create_scan_range(file, 0, 0);
        auto* ctx = _create_ctx(file, range, tuple_desc);
        ctx->format_scan_context.options.enable_split_tasks = enable_split_tasks;
        auto scanner = std::make_shared<HdfsOrcScanner>();
        scanner->_skip_rows_ctx->deletion_bitmap = bitmap;
        CHECK_OK(scanner->init(_runtime_state, ctx));
        CHECK_OK(scanner->open(_runtime_state));
        return std::make_pair(scanner, ctx);
    };
    auto read_ids = [&](HdfsOrcScanner* scanner) {
        std::vector<int64_t> ids;
        Status status;
        while (status.ok()) {
            ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
            status = scanner->get_next(_runtime_state, &chunk);
            CHECK(status.ok() || status.is_end_of_file()) << status;
            const Column* col = chunk->get_column_raw_ptr_by_slot_id(0);
            for (size_t i = 0; i < chunk->num_rows(); i++) {
                ids.push_back(col->get(i).get_int64());
            }
        }
        return ids;
    };
    auto skipped_counter = [&](HdfsOrcScanner* scanner) {
        RuntimeProfile profile("orc");
        HdfsScannerProfile hdfs_profile;
        hdfs_profile.runtime_profile = &profile;
        scanner->do_update_counter(&hdfs_profile);
        auto* counter = profile.get_counter("DeletedStripesSkipped");
        return counter == nullptr ? -1 : counter->value();
    };

    // row 0 and all of stripe 1
    auto bitmap = std::make_shared<DeletionBitmap>(roaring64_bitmap_create());
    bitmap->add_value(0);
    for (size_t i = stripe_rows; i < 2 * stripe_rows; i++) {
        bitmap->add_value(i);
    }
    {
        auto [scanner, ctx] = open_scanner(bitmap, false);
        auto ids = read_ids(scanner.get());
        std::vector<int64_t> expected;
        for (int64_t v = 1; v < static_cast<int64_t>(num_rows); v++) {
            if (v < static_cast<int64_t>(stripe_rows) || v >= static_cast<int64_t>(2 * stripe_rows)) {
                expected.push_back(v);
            }
        }
        EXPECT_EQ(expected, ids);
        EXPECT_EQ(1, skipped_counter(scanner.get()));
        scanner->close();
    }
    {
        // no split task for the deleted stripe
        auto [scanner, ctx] = open_scanner(bitmap, true);
        const auto& split_tasks = ctx->format_scan_context.split.split_tasks;
        ASSERT_EQ(num_stripes - 1, split_tasks.size());
        const int64_t deleted_offset =
                orc::createReader(orc::readLocalFile(file), orc::ReaderOptions{})->getStripe(1)->getOffset();
        for (const auto& task : split_tasks) {
            EXPECT_NE(deleted_offset, task->start_offset);
        }
        // the task that splits the file reports the stripe it dropped
        EXPECT_EQ(1, skipped_counter(scanner.get()));
        scanner->close();
    }
    {
        // every stripe deleted
        auto all = std::make_shared<DeletionBitmap>(roaring64_bitmap_create());
        for (size_t i = 0; i < num_rows; i++) {
            all->add_value(i);
        }
        auto [scanner, ctx] = open_scanner(all, false);
        EXPECT_TRUE(read_ids(scanner.get()).empty());
        EXPECT_EQ(num_stripes, skipped_counter(scanner.get()));
        scanner->close();
    }
}

// The whole-stripe (tiny stripe) read is decided on every stripe of the scan range: dropping a large,
// fully deleted stripe must not switch the small surviving ones to whole-stripe reads.
TEST_F(HdfsScannerTest, TestOrcSkipDeletedStripeKeepsTinyStripeDecision) {
    const std::string file = "orc_skip_deleted_large_stripe.orc";
    std::filesystem::remove(file);
    // stripe 1 is much larger than stripes 0 and 2
    const std::vector<size_t> stripe_rows{100, 100000, 100};
    {
        ORC_UNIQUE_PTR<orc::Type> schema(orc::Type::buildTypeFromString("struct<id:bigint>"));
        orc::WriterOptions options;
        options.setStripeSize(0);
        ORC_UNIQUE_PTR<orc::OutputStream> out = orc::writeLocalFile(file);
        ORC_UNIQUE_PTR<orc::Writer> writer = orc::createWriter(*schema, out.get(), options);
        int64_t next = 0;
        for (size_t rows : stripe_rows) {
            ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = writer->createRowBatch(rows);
            auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
            auto* id = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);
            for (size_t i = 0; i < rows; i++) {
                // not compressible to nothing: keep the large stripe large
                id->data[i] = (next++) * 2654435761LL;
            }
            id->numElements = rows;
            root->numElements = rows;
            writer->add(*batch);
        }
        writer->close();
    }
    DeferOp remove_file([&]() { std::filesystem::remove(file); });
    auto reader = orc::createReader(orc::readLocalFile(file), orc::ReaderOptions{});
    ASSERT_EQ(stripe_rows.size(), reader->getNumberOfStripes());
    auto stripe_size = [&](size_t idx) {
        auto stripe = reader->getStripe(idx);
        return static_cast<int64_t>(stripe->getIndexLength() + stripe->getDataLength() + stripe->getFooterLength());
    };
    ASSERT_GT(stripe_size(1), 4 * stripe_size(0));
    // stripes 0 and 2 are tiny, stripe 1 is not
    const int32_t saved_threshold = config::orc_tiny_stripe_threshold_size;
    DeferOp restore([&]() { config::orc_tiny_stripe_threshold_size = saved_threshold; });
    config::orc_tiny_stripe_threshold_size = static_cast<int32_t>((stripe_size(0) + stripe_size(1)) / 2);

    auto bitmap = std::make_shared<DeletionBitmap>(roaring64_bitmap_create());
    for (size_t i = stripe_rows[0]; i < stripe_rows[0] + stripe_rows[1]; i++) {
        bitmap->add_value(i);
    }
    SlotDesc descs[] = {{"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)}, {""}};
    auto* tuple_desc = _create_tuple_desc(descs);
    auto* range = _create_scan_range(file, 0, 0);
    auto* ctx = _create_ctx(file, range, tuple_desc);
    auto scanner = std::make_shared<HdfsOrcScanner>();
    scanner->_skip_rows_ctx->deletion_bitmap = bitmap;
    ASSERT_OK(scanner->init(_runtime_state, ctx));
    ASSERT_OK(scanner->open(_runtime_state));
    size_t rows = 0;
    Status status;
    while (status.ok()) {
        ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
        status = scanner->get_next(_runtime_state, &chunk);
        ASSERT_TRUE(status.ok() || status.is_end_of_file()) << status;
        rows += chunk->num_rows();
    }
    EXPECT_EQ(stripe_rows[0] + stripe_rows[2], rows);

    RuntimeProfile profile("orc");
    HdfsScannerProfile hdfs_profile;
    hdfs_profile.runtime_profile = &profile;
    scanner->do_update_counter(&hdfs_profile);
    EXPECT_EQ(1, profile.get_counter("DeletedStripesSkipped")->value());
    EXPECT_EQ(0, profile.get_counter("TotalTinyStripeSize")->value());
    scanner->close();
}

// =============================================================================

/*
file:         file:/Users/dirlt/repo/private/project/pyscript/starrocks/small_row_group_data.parquet 
creator:      parquet-cpp-arrow version 7.0.0 
extra:        ARROW:schema = /////9gAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAAMAAABwAAAAMAAAAAQAAACs////AAABBRAAAAAYAAAABAAAAAAAAAACAAAAYzMAAAQABAAEAAAA1P///wAAAQIQAAAAFAAAAAQAAAAAAAAAAgAAAGMyAADE////AAAAAUAAAAAQABQACAAGAAcADAAAABAAEAAAAAAAAQIQAAAAHAAAAAQAAAAAAAAAAgAAAGMxAAAIAAwACAAHAAgAAAAAAAABQAAAAAAAAAA= 

file schema:  schema 
--------------------------------------------------------------------------------
c1:           OPTIONAL INT64 R:0 D:1
c2:           OPTIONAL INT64 R:0 D:1
c3:           OPTIONAL BINARY O:UTF8 R:0 D:1

row group 1:  RC:5120 TS:1197023 OFFSET:4 
--------------------------------------------------------------------------------
c1:            INT64 SNAPPY DO:4 FPO:20522 SZ:28928/49384/1.71 VC:5120 ENC:PLAIN,PLAIN_DICTIONARY,RLE
c2:            INT64 SNAPPY DO:29030 FPO:53541 SZ:32921/49384/1.50 VC:5120 ENC:PLAIN,PLAIN_DICTIONARY,RLE
c3:            BINARY SNAPPY DO:62051 FPO:134420 SZ:81159/1098255/13.53 VC:5120 ENC:PLAIN,PLAIN_DICTIONARY,RLE

...

row group 20: RC:2720 TS:638517 OFFSET:2732382 
--------------------------------------------------------------------------------
c1:            INT64 SNAPPY DO:2732382 FPO:2743296 SZ:15077/25937/1.72 VC:2720 ENC:PLAIN,PLAIN_DICTIONARY,RLE
c2:            INT64 SNAPPY DO:2747562 FPO:2760616 SZ:17217/25937/1.51 VC:2720 ENC:PLAIN,PLAIN_DICTIONARY,RLE
c3:            BINARY SNAPPY DO:2764882 FPO:2803392 SZ:43059/586643/13.62 VC:2720 ENC:PLAIN,PLAIN_DICTIONARY,RLE
 */

TEST_F(HdfsScannerTest, TestParquetCoalesceReadAcrossRowGroup) {
    SlotDesc parquet_descs[] = {{"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
                                {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
                                {"c3", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                                {""}};

    const std::string parquet_file = "./be/test/exec/test_data/parquet_scanner/small_row_group_data.parquet";

    auto scanner = std::make_shared<HdfsParquetScanner>();

    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* ctx = _create_ctx(parquet_file, range, tuple_desc);

    Status status = scanner->init(_runtime_state, ctx);
    ASSERT_TRUE(status.ok()) << status.message();

    status = scanner->open(_runtime_state);
    ASSERT_TRUE(status.ok()) << status.message();

    READ_SCANNER_ROWS(scanner, 100000);

    scanner->close();
}

TEST_F(HdfsScannerTest, TestParquetRuntimeFilter) {
    SlotDesc parquet_descs[] = {{"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
                                {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
                                {"c3", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                                {""}};

    const std::string parquet_file = "./be/test/exec/test_data/parquet_scanner/small_row_group_data.parquet";

    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* ctx = _create_ctx(parquet_file, range, tuple_desc);

    Status status;

    struct Case {
        int min_value;
        int max_value;
        int exp_rows;
    };
    // c1 max is 99999
    Case cases[] = {{.min_value = 10000000, .max_value = 10000000, .exp_rows = 0},
                    {.min_value = -10, .max_value = -10, .exp_rows = 0},
                    {.min_value = -10, .max_value = 10000000, .exp_rows = 100000}};

    for (const Case& tc : cases) {
        auto scanner = std::make_shared<HdfsParquetScanner>();

        RuntimeFilterProbeCollector rf_collector;
        RuntimeFilterProbeDescriptor rf_probe_desc;
        ColumnRef c1ref(tuple_desc->slots()[0]);
        ExprContext probe_expr_ctx(&c1ref);

        status = probe_expr_ctx.prepare(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();
        status = probe_expr_ctx.open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();

        // build runtime filter.
        RuntimeFilter* f = RuntimeFilterFactory::create_bloom_filter(&_pool, LogicalType::TYPE_BIGINT, 0);
        f->get_membership_filter()->init(10);
        auto column = ColumnHelper::create_column(tuple_desc->slots()[0]->type(), false);
        auto c = ColumnHelper::cast_to_raw<LogicalType::TYPE_BIGINT>(column);
        c->append(tc.max_value);
        c->append(tc.min_value);
        ASSERT_OK(RuntimeFilterBuilder::fill(f, LogicalType::TYPE_BIGINT, column, 0, false));

        ASSERT_OK(rf_probe_desc.init(0, &probe_expr_ctx));
        rf_probe_desc.set_runtime_filter(f);
        rf_collector.add_descriptor(&rf_probe_desc);
        ctx->runtime_filter_collector = &rf_collector;

        status = scanner->init(_runtime_state, ctx);
        ASSERT_TRUE(status.ok()) << status.message();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();

        READ_SCANNER_ROWS(scanner, tc.exp_rows);

        scanner->close();
        probe_expr_ctx.close(_runtime_state);
    }
}

// =============================================================================

/*
file:                  file:/Users/dirlt/Downloads/part-00000-4a878ed5-fa12-4e43-a164-1650976be336-c000.snappy.parquet
creator:               parquet-mr version 1.10.1 (build a89df8f9932b6ef6633d06069e50c9b7970bebd1)
extra:                 org.apache.spark.version = 2.4.7
extra:                 org.apache.spark.sql.parquet.row.metadata = {"type":"struct","fields":[{"name":"vin","type":"string","nullable":true,"metadata":{}},{"name":"log_domain","type":"string","nullable":true,"metadata":{}},{"name":"file_name","type":"string","nullable":true,"metadata":{}},{"name":"is_collection","type":"integer","nullable":false,"metadata":{}},{"name":"is_center","type":"integer","nullable":false,"metadata":{}},{"name":"is_cloud","type":"integer","nullable":false,"metadata":{}},{"name":"collection_time","type":"string","nullable":false,"metadata":{}},{"name":"center_time","type":"string","nullable":false,"metadata":{}},{"name":"cloud_time","type":"string","nullable":false,"metadata":{}},{"name":"error_collection_tips","type":"string","nullable":false,"metadata":{}},{"name":"error_center_tips","type":"string","nullable":false,"metadata":{}},{"name":"error_cloud_tips","type":"string","nullable":false,"metadata":{}},{"name":"error_collection_time","type":"string","nullable":false,"metadata":{}},{"name":"error_center_time","type":"string","nullable":false,"metadata":{}},{"name":"error_cloud_time","type":"string","nullable":false,"metadata":{}},{"name":"original_time","type":"string","nullable":false,"metadata":{}},{"name":"is_original","type":"integer","nullable":false,"metadata":{}}]}

file schema:           spark_schema
--------------------------------------------------------------------------------
vin:                   OPTIONAL BINARY O:UTF8 R:0 D:1
log_domain:            OPTIONAL BINARY O:UTF8 R:0 D:1
file_name:             OPTIONAL BINARY O:UTF8 R:0 D:1
is_collection:         REQUIRED INT32 R:0 D:0
is_center:             REQUIRED INT32 R:0 D:0
is_cloud:              REQUIRED INT32 R:0 D:0
collection_time:       REQUIRED BINARY O:UTF8 R:0 D:0
center_time:           REQUIRED BINARY O:UTF8 R:0 D:0
cloud_time:            REQUIRED BINARY O:UTF8 R:0 D:0
error_collection_tips: REQUIRED BINARY O:UTF8 R:0 D:0
error_center_tips:     REQUIRED BINARY O:UTF8 R:0 D:0
error_cloud_tips:      REQUIRED BINARY O:UTF8 R:0 D:0
error_collection_time: REQUIRED BINARY O:UTF8 R:0 D:0
error_center_time:     REQUIRED BINARY O:UTF8 R:0 D:0
error_cloud_time:      REQUIRED BINARY O:UTF8 R:0 D:0
original_time:         REQUIRED BINARY O:UTF8 R:0 D:0
is_original:           REQUIRED INT32 R:0 D:0
*/

TEST_F(HdfsScannerTest, TestParqueTypeMismatchDecodeMinMax) {
    SlotDesc parquet_descs[] = {{"vin", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                                {"is_cloud", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                {""}};

    SlotDesc min_max_descs[] = {{"vin", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)}, {""}};

    const std::string parquet_file = "./be/test/exec/test_data/parquet_scanner/type_mismatch_decode_min_max.parquet";

    auto scanner = std::make_shared<HdfsParquetScanner>();
    ObjectPool* pool = &_pool;
    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* ctx = _create_ctx(parquet_file, range, tuple_desc);

    // select vin,is_cloud from table where is_cloud >= '0';
    auto* min_max_tuple_desc = _create_tuple_desc(min_max_descs);
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_string_literal_node(TPrimitiveType::VARCHAR, "0");
        push_binary_pred_texpr_node(nodes, TExprOpcode::GE, min_max_tuple_desc->slots()[0], TPrimitiveType::VARCHAR,
                                    lit_node);
        ExprContext* expr_ctx = create_expr_context(pool, nodes);
        ctx->format_scan_context.conjuncts.min_max_ctxs.push_back(expr_ctx);
    }

    ctx->min_max_tuple_desc = min_max_tuple_desc;
    ASSERT_OK(ExprExecutor::prepare(ctx->format_scan_context.conjuncts.min_max_ctxs, _runtime_state));
    ASSERT_OK(ExprExecutor::open(ctx->format_scan_context.conjuncts.min_max_ctxs, _runtime_state));

    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    EXPECT_TRUE(!status.ok());
    scanner->close();
}

// =============================================================================
/*
UID0,ACTION0
UID1,ACTION1
UID2,ACTION2
UID3,ACTION3
UID4,ACTION4
UID5,ACTION5
UID6,ACTION6
UID7,ACTION7
UID8,ACTION8
UID9,ACTION9
...
UID99,ACTION99
*/

TEST_F(HdfsScannerTest, TestCSVCompressed) {
    SlotDesc csv_descs[] = {{"user_id", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"action", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {""}};

    const std::string uncompressed_file = "./be/test/exec/test_data/csv_scanner/compressed.csv";
    const std::string compressed_file = "./be/test/exec/test_data/csv_scanner/compressed.csv.gz";
    Status status;

    {
        auto* range = _create_scan_range(uncompressed_file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(uncompressed_file, range, tuple_desc);
        build_hive_column_names(ctx, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        ASSERT_TRUE(status.ok()) << status.message();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();

        READ_SCANNER_ROWS(scanner, 100);
        scanner->close();
    }
    {
        auto* range = _create_scan_range(compressed_file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(compressed_file, range, tuple_desc);
        build_hive_column_names(ctx, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        ASSERT_TRUE(status.ok()) << status.message();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();

        READ_SCANNER_ROWS(scanner, 100);
        scanner->close();
    }
    {
        // compressed file with offset != 0
        auto* range = _create_scan_range(compressed_file, 1, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(compressed_file, range, tuple_desc);
        build_hive_column_names(ctx, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        ASSERT_TRUE(status.ok()) << status.message();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();
        ASSERT_EQ(0, scanner->estimated_mem_usage());
        scanner->close();
    }
    {
        // compressed file with skip_header_line_count
        auto* range = _create_scan_range(compressed_file, 0, 0);
        range->text_file_desc.__set_skip_header_line_count(50);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(compressed_file, range, tuple_desc);
        build_hive_column_names(ctx, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        ASSERT_TRUE(status.ok()) << status.message();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();
        READ_SCANNER_ROWS(scanner, 50);
        scanner->close();
    }
    {
        // compressed file with skip_header_line_count > total line count
        auto* range = _create_scan_range(compressed_file, 0, 0);
        range->text_file_desc.__set_skip_header_line_count(200);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(compressed_file, range, tuple_desc);
        build_hive_column_names(ctx, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        ASSERT_TRUE(status.ok()) << status.message();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();
        READ_SCANNER_ROWS(scanner, 0);
        scanner->close();
    }
    {
        auto* range = _create_scan_range(compressed_file, 0, 0);
        // Forcr to parse csv as uncompressed data.
        range->text_file_desc.__set_compression_type(TCompressionType::NO_COMPRESSION);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(compressed_file, range, tuple_desc);
        build_hive_column_names(ctx, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        ASSERT_TRUE(status.ok()) << status.message();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();

        uint64_t records = 0;
        READ_SCANNER_RETURN_ROWS(scanner, records);
        EXPECT_NE(records, 100);
        scanner->close();
    }
}

TEST_F(HdfsScannerTest, TestCSVWithDifferentLineDelimiter) {
    SlotDesc csv_descs[] = {{"user_id", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"action", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {""}};

    const std::string end_with_r = "./be/test/exec/test_data/csv_scanner/line_delimiter_end_with_r.csv";
    const std::string end_with_n = "./be/test/exec/test_data/csv_scanner/line_delimiter_end_with_n.csv";
    Status status;

    {
        // test line delimiter = \r
        auto* range = _create_scan_range(end_with_r, 0, 0);
        range->text_file_desc.__isset.line_delim = false;
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(end_with_r, range, tuple_desc);
        build_hive_column_names(ctx, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        ASSERT_TRUE(status.ok()) << status.message();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();

        READ_SCANNER_ROWS(scanner, 5);
        scanner->close();
    }
    {
        // test line delimiter = \r
        auto* range = _create_scan_range(end_with_n, 0, 0);
        range->text_file_desc.__isset.line_delim = false;
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(end_with_n, range, tuple_desc);
        build_hive_column_names(ctx, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        ASSERT_TRUE(status.ok()) << status.message();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();

        READ_SCANNER_ROWS(scanner, 5);
        scanner->close();
    }
}

// =============================================================================
/*
1,"INFANTS, PETS",ok
2,"a,b,c",done
3,plain,end
4,"she said ""hi""",quoted
*/
// OpenCSVSerde (enclose/escape set): separators inside quoted fields must NOT be
// treated as column delimiters, and doubled quotes ("") must be de-escaped.
TEST_F(HdfsScannerTest, TestCSVOpenCSVSerdeQuote) {
    SlotDesc csv_descs[] = {{"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"name", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 50)},
                            {"status", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {""}};

    const std::string quote_file = "./be/test/exec/test_data/csv_scanner/quote_escape.csv";
    Status status;

    // 1) explicit line.delim: quote-aware parsing only.
    {
        auto* range = _create_scan_range(quote_file, 0, 0);
        range->text_file_desc.__set_enclose('"');
        range->text_file_desc.__set_escape('\\');
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(quote_file, range, tuple_desc);
        build_hive_column_names(ctx, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        ASSERT_TRUE(status.ok()) << status.message();
        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();

        ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 4096);
        status = scanner->get_next(_runtime_state, &chunk);
        ASSERT_TRUE(status.ok()) << status.message();
        EXPECT_EQ(5, chunk->num_rows());
        EXPECT_EQ("['1', 'INFANTS, PETS', 'ok']", chunk->debug_row(0));
        EXPECT_EQ("['2', 'a,b,c', 'done']", chunk->debug_row(1));
        EXPECT_EQ("['3', 'plain', 'end']", chunk->debug_row(2));
        EXPECT_EQ("['4', 'she said \"hi\"', 'quoted']", chunk->debug_row(3));
        // OpenCSVSerde has NO null literal: quoted "\\N" (escaped backslash + N -- the
        // quotes matter: only inside quotes/a token is the escape effective, so this is
        // the one spelling whose field bytes come out as exactly \N) must be the 2-char
        // STRING "\N", never SQL NULL (Options::hive_null_literal=false). And in "\N"
        // the escape doesn't protect 'N' (not quote/escape), so opencsv silently drops
        // the backslash -> "N".
        EXPECT_EQ("['5', '\\N', 'N']", chunk->debug_row(4));
        scanner->close();
    }

    // 2) no line.delim from HMS: exercises probe_row_delimiter together with v2.
    {
        auto* range = _create_scan_range(quote_file, 0, 0);
        range->text_file_desc.__set_enclose('"');
        range->text_file_desc.__isset.line_delim = false;
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(quote_file, range, tuple_desc);
        build_hive_column_names(ctx, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        ASSERT_TRUE(status.ok()) << status.message();
        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();

        ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 4096);
        status = scanner->get_next(_runtime_state, &chunk);
        ASSERT_TRUE(status.ok()) << status.message();
        EXPECT_EQ(5, chunk->num_rows());
        EXPECT_EQ("['1', 'INFANTS, PETS', 'ok']", chunk->debug_row(0));
        scanner->close();
    }

    // 3) splittable: read in two ranges; total rows must stay 5 regardless of
    //    the split point (validates the set_limit/_parsed_bytes boundary in v2).
    for (int offset = 8; offset < 30; offset++) {
        int records = 0;
        auto read_range = [&](int start, int end) {
            auto* range0 = _create_scan_range(quote_file, start, end);
            range0->text_file_desc.__set_enclose('"');
            range0->text_file_desc.__set_escape('\\');
            auto* tuple_desc = _create_tuple_desc(csv_descs);
            auto* ctx = _create_ctx(quote_file, range0, tuple_desc);
            build_hive_column_names(ctx, tuple_desc);
            auto scanner = std::make_shared<HdfsTextScanner>();
            status = scanner->init(_runtime_state, ctx);
            ASSERT_TRUE(status.ok()) << status.message();
            status = scanner->open(_runtime_state);
            ASSERT_TRUE(status.ok()) << status.message();
            READ_SCANNER_RETURN_ROWS(scanner, records);
            scanner->close();
        };
        read_range(0, offset);
        read_range(offset, 0);
        ASSERT_EQ(records, 5) << "offset=" << offset;
    }

    // 4) One combined fixture (BOM + CRLF + a blank line + backslash escape + the
    //    audit-O2 malformed-quote family) to keep the number of test-data files down:
    //    - UTF-8 BOM at file start: the BOM-skip probe must not leak into the first
    //      column;
    //    - CRLF with no explicit line.delim: the probing _find_line_delimiter plus
    //      _trim_row_delimeter must keep the last (unquoted) column free of '\r';
    //    - a blank line must be KEPT as an all-null row (Hive: opencsv's readNext()
    //      returns null for it), not dropped the way the load path drops blank lines;
    //    - backslash-escaped enclose char (\") de-escapes to a literal '"';
    //    - rows are split at PHYSICAL line boundaries BEFORE quote handling (audit
    //      case O2), exactly like Hive's LineRecordReader-then-serde layering: each
    //      half of a "quoted newline" is malformed on its own and yields an all-null
    //      row; fields completed before an unterminated quote survive; and the
    //      escape does NOT protect a separator (opencsv drops it and still splits).
    {
        const std::string combo_file = "./be/test/exec/test_data/csv_scanner/quote_escape_crlf_bom.csv";
        auto* range = _create_scan_range(combo_file, 0, 0);
        range->text_file_desc.__set_enclose('"');
        range->text_file_desc.__set_escape('\\');
        range->text_file_desc.__isset.line_delim = false; // force the probe path
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(combo_file, range, tuple_desc);
        build_hive_column_names(ctx, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        ASSERT_TRUE(status.ok()) << status.message();
        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();

        ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 4096);
        status = scanner->get_next(_runtime_state, &chunk);
        ASSERT_TRUE(status.ok()) << status.message();
        EXPECT_EQ(7, chunk->num_rows());
        // BOM stripped (first col '1'), embedded comma kept, last col has no '\r'.
        EXPECT_EQ("['1', 'INFANTS, PETS', 'ok']", chunk->debug_row(0));
        // The blank line is kept as an all-null row (matches Hive), not dropped.
        EXPECT_EQ("[NULL, NULL, NULL]", chunk->debug_row(1));
        // Backslash-escaped quotes de-escape to a literal '"'.
        EXPECT_EQ("['2', 'she said \"hi\"', 'quoted']", chunk->debug_row(2));
        // '"a' -- unterminated quote, no completed field: all-null row (O2 half 1).
        EXPECT_EQ("[NULL, NULL, NULL]", chunk->debug_row(3));
        // 'b",c' -- the mid-field quote swallows the rest of the line (O2 half 2).
        EXPECT_EQ("[NULL, NULL, NULL]", chunk->debug_row(4));
        // 'x,"a' -- 'x' completed before the quote opened.
        EXPECT_EQ("['x', NULL, NULL]", chunk->debug_row(5));
        // 'p\,q,r' -- the escape is dropped and the separators still split.
        EXPECT_EQ("['p', 'q', 'r']", chunk->debug_row(6));
        scanner->close();
    }
}

// =============================================================================
/*
a\,b,c,a|b|c,k1:v1|k2:v2
\N,x,,
\\N,y,x,k:
p\\q,z,,\N
m,\N,,
*/
// One combined LazySimpleSerDe fixture (ESCAPED BY '\', collection '|', mapkey ':')
// covering audit cases L9 and L6 together:
// - an escaped separator stays inside the field, and the null sequence "\N" is
//   decided on the RAW (pre-unescape) bytes like Hive does -- "\N" is null even
//   though its unescaped form is "N", while "\\N" unescapes to a literal "\N"
//   string that is NOT null;
// - Hive text maps/arrays have no braces or quotes: map entries split on the
//   collection delimiter and each entry splits at the first mapkey delimiter; an
//   empty field is an EMPTY collection ({} / []) while a raw "\N" field is NULL.
TEST_F(HdfsScannerTest, TestCSVLazySimpleCompat) {
    TypeDescriptor map_col = TypeDescriptor::from_logical_type(LogicalType::TYPE_MAP);
    map_col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22));
    map_col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22));
    TypeDescriptor arr_col = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);
    arr_col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22));

    SlotDesc csv_descs[] = {{"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"arr", arr_col},
                            {"m", map_col},
                            {""}};

    const std::string compat_file = "./be/test/exec/test_data/csv_scanner/lazysimple_compat.csv";
    Status status;

    {
        auto* range = _create_scan_range(compat_file, 0, 0);
        range->text_file_desc.__set_escape('\\'); // no enclose: pure LazySimpleSerDe shape
        range->text_file_desc.__set_collection_delim("|");
        range->text_file_desc.__set_mapkey_delim(":");
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(compat_file, range, tuple_desc);
        build_hive_column_names(ctx, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        ASSERT_TRUE(status.ok()) << status.message();
        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();

        ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 4096);
        status = scanner->get_next(_runtime_state, &chunk);
        ASSERT_TRUE(status.ok()) << status.message();
        EXPECT_EQ(9, chunk->num_rows());
        // Escaped separator is field data; Hive text array/map parse without braces.
        EXPECT_EQ("['a,b', 'c', ['a','b','c'], {'k1':'v1','k2':'v2'}]", chunk->debug_row(0));
        // Raw "\N" is null (mid-row emission site); empty fields are EMPTY collections.
        EXPECT_EQ("[NULL, 'x', [], {}]", chunk->debug_row(1));
        // Raw "\\N" is a literal "\N" string, NOT null; 'k:' keeps an empty value.
        EXPECT_EQ("['\\N', 'y', ['x'], {'k':''}]", chunk->debug_row(2));
        // Escaped escape character; a raw "\N" map FIELD is a null map.
        EXPECT_EQ("['p\\q', 'z', [], NULL]", chunk->debug_row(3));
        // Raw "\N" at row end (row-delimiter emission site).
        EXPECT_EQ("['m', NULL, [], {}]", chunk->debug_row(4));
        // An escaped ARRAY-ELEMENT separator ("a\|b|c") is not a real boundary: the
        // array's own escape-aware split must see it, giving 2 elements ("a|b","c"),
        // not 3 -- this is the bug the "MapConverter unescape" review comment flagged.
        EXPECT_EQ("['x', 'y', ['a|b','c'], {}]", chunk->debug_row(5));
        // A raw "\N" ARRAY ELEMENT ("a|\N") is a null element, decided on its own raw
        // bytes one level down in NullableConverter, not corrupted by an eager
        // top-level unescape.
        EXPECT_EQ("['x', 'y', ['a',NULL], {}]", chunk->debug_row(6));
        // Duplicate map keys must be detected on the CONVERTED key, not the raw bytes:
        // "\a" and "a" are different raw slices but unescape to the same logical key,
        // and Hive keeps only the last entry for a duplicate deserialized key.
        EXPECT_EQ("['x', 'y', [], {'a':'v2'}]", chunk->debug_row(7));
        // An escaped kv delimiter ("a\:b") is key DATA, not a boundary: both entries'
        // keys unescape to "a:b", so they too deduplicate (last wins) after conversion.
        EXPECT_EQ("['x', 'y', [], {'a:b':'v2'}]", chunk->debug_row(8));
        scanner->close();
    }

    // Splittable: total rows must stay 9 regardless of the split point (the escape
    // handling must not disturb the scan-range boundary bookkeeping).
    for (int offset = 4; offset < 80; offset++) {
        int records = 0;
        auto read_range = [&](int start, int end) {
            auto* range0 = _create_scan_range(compat_file, start, end);
            range0->text_file_desc.__set_escape('\\');
            range0->text_file_desc.__set_collection_delim("|");
            range0->text_file_desc.__set_mapkey_delim(":");
            auto* tuple_desc = _create_tuple_desc(csv_descs);
            auto* ctx = _create_ctx(compat_file, range0, tuple_desc);
            build_hive_column_names(ctx, tuple_desc);
            auto scanner = std::make_shared<HdfsTextScanner>();
            status = scanner->init(_runtime_state, ctx);
            ASSERT_TRUE(status.ok()) << status.message();
            status = scanner->open(_runtime_state);
            ASSERT_TRUE(status.ok()) << status.message();
            READ_SCANNER_RETURN_ROWS(scanner, records);
            scanner->close();
        };
        read_range(0, offset);
        read_range(offset, 0);
        ASSERT_EQ(records, 9) << "offset=" << offset;
    }
}

// =============================================================================
/*
UID0,ACTION0
UID1,ACTION1
*/
// there is no newline at EOF.

TEST_F(HdfsScannerTest, TestCSVSmall) {
    SlotDesc csv_descs[] = {{"user_id", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"action", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {""}};

    const std::string small_file = "./be/test/exec/test_data/csv_scanner/small.csv";
    Status status;

    {
        auto* range = _create_scan_range(small_file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(small_file, range, tuple_desc);
        build_hive_column_names(ctx, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        ASSERT_TRUE(status.ok()) << status.message();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();

        READ_SCANNER_ROWS(scanner, 2);
        scanner->close();
    }

    for (int offset = 10; offset < 20; offset++) {
        // read two scan range ranges and # of records = 2
        int records = 0;

        auto read_range = [&](int start, int end) {
            auto* range0 = _create_scan_range(small_file, start, end);
            auto* tuple_desc = _create_tuple_desc(csv_descs);
            auto* ctx = _create_ctx(small_file, range0, tuple_desc);
            build_hive_column_names(ctx, tuple_desc);
            auto scanner = std::make_shared<HdfsTextScanner>();

            status = scanner->init(_runtime_state, ctx);
            ASSERT_TRUE(status.ok()) << status.message();

            status = scanner->open(_runtime_state);
            ASSERT_TRUE(status.ok()) << status.message();

            READ_SCANNER_RETURN_ROWS(scanner, records);

            scanner->close();
        };

        read_range(0, offset);
        read_range(offset, 0);
        ASSERT_EQ(records, 2);
    }
}

TEST_F(HdfsScannerTest, TestCSVCaseIgnore) {
    SlotDesc csv_descs[] = {{"USER_id", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"ACTION", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {""}};

    const std::string small_file = "./be/test/exec/test_data/csv_scanner/small.csv";
    Status status;

    {
        auto* range = _create_scan_range(small_file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(small_file, range, tuple_desc);
        build_hive_column_names(ctx, tuple_desc, true);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        ASSERT_TRUE(status.ok()) << status.message();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();

        READ_SCANNER_ROWS(scanner, 2);
        scanner->close();
    }
}

TEST_F(HdfsScannerTest, TestCSVWithoutEndDelemeter) {
    SlotDesc csv_descs[] = {{"col1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                            {"col2", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                            {"col3", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                            {""}};

    const std::string small_file = "./be/test/exec/test_data/csv_scanner/delimiter.csv";
    Status status;

    {
        auto* range = _create_scan_range(small_file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(small_file, range, tuple_desc);
#if defined(WITH_STARCACHE)
        auto cache_options = TestCacheUtils::create_simple_options(config::datacache_block_size, 50 * MB);
        auto block_cache = TestCacheUtils::create_cache(cache_options);
        DataCache::GetInstance()->set_block_cache(block_cache);
#endif
        build_hive_column_names(ctx, tuple_desc, true);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        ASSERT_TRUE(status.ok()) << status.message();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();

        READ_SCANNER_ROWS(scanner, 3);
        scanner->close();
    }
}

TEST_F(HdfsScannerTest, TestCSVWithWindowsEndDelemeter) {
    SlotDesc csv_descs[] = {{"uuid", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)}, {""}};

    const std::string windows_file = "./be/test/exec/test_data/csv_scanner/windows.csv";
    Status status;

    {
        auto* range = _create_scan_range(windows_file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(windows_file, range, tuple_desc);
        build_hive_column_names(ctx, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        ASSERT_TRUE(status.ok()) << status.message();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();

        ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 4096);

        status = scanner->get_next(_runtime_state, &chunk);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(3, chunk->num_rows());

        EXPECT_EQ("['hello']", chunk->debug_row(0));
        EXPECT_EQ("['world']", chunk->debug_row(1));
        EXPECT_EQ("['starrocks']", chunk->debug_row(2));
        scanner->close();
    }
}

TEST_F(HdfsScannerTest, TestCSVWithUTFBOM) {
    SlotDesc csv_descs[] = {{"uuid", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)}, {""}};

    const std::string bom_file = "./be/test/exec/test_data/csv_scanner/bom.csv";
    Status status;

    {
        auto* range = _create_scan_range(bom_file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(bom_file, range, tuple_desc);
        build_hive_column_names(ctx, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        ASSERT_TRUE(status.ok()) << status.message();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();

        ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 4096);

        status = scanner->get_next(_runtime_state, &chunk);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(3, chunk->num_rows());

        EXPECT_EQ("['5c3ffda0d1d7']", chunk->debug_row(0));
        EXPECT_EQ("['62ef51eae5d8']", chunk->debug_row(1));
        scanner->close();
    }

    {
        // bom + skip 2 line
        auto* range = _create_scan_range(bom_file, 0, 0);
        range->text_file_desc.__set_skip_header_line_count(2);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(bom_file, range, tuple_desc);
        build_hive_column_names(ctx, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        ASSERT_TRUE(status.ok()) << status.message();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();

        ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 4096);

        status = scanner->get_next(_runtime_state, &chunk);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(1, chunk->num_rows());

        EXPECT_EQ("['6358db89e73b']", chunk->debug_row(0));
        scanner->close();
    }

    {
        // offset + bom + skip 1 line, but bom and skip_header_line_count will not take effect because of start offset != 0
        auto* range = _create_scan_range(bom_file, 20, 0);
        range->text_file_desc.__set_skip_header_line_count(1);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(bom_file, range, tuple_desc);
        build_hive_column_names(ctx, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        ASSERT_TRUE(status.ok()) << status.message();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();

        ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 4096);

        status = scanner->get_next(_runtime_state, &chunk);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(2, chunk->num_rows());
        EXPECT_EQ("['62ef51eae5d8']", chunk->debug_row(0));
        EXPECT_EQ("['6358db89e73b']", chunk->debug_row(1));
        scanner->close();
    }
}

TEST_F(HdfsScannerTest, TestCSVNewlyAddColumn) {
    SlotDesc csv_descs[] = {{"name1", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"age", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"name2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"newly", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {""}};

    const std::string small_file = "./be/test/exec/test_data/csv_scanner/newly_add_column.csv";
    Status status;

    {
        auto* range = _create_scan_range(small_file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(small_file, range, tuple_desc);
        build_hive_column_names(ctx, tuple_desc, true);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        EXPECT_TRUE(status.ok());

        status = scanner->open(_runtime_state);
        EXPECT_TRUE(status.ok());

        ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 4096);

        status = scanner->get_next(_runtime_state, &chunk);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(2, chunk->num_rows());

        EXPECT_EQ("['hello', '5', 'smith', NULL]", chunk->debug_row(0));
        EXPECT_EQ("['world', '6', 'cruise', NULL]", chunk->debug_row(1));

        scanner->close();
    }
}

TEST_F(HdfsScannerTest, TestCSVDifferentOrderColumn) {
    SlotDesc csv_descs[] = {{"name1", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"age", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"name2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {"newly", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                            {""}};

    const std::string small_file = "./be/test/exec/test_data/csv_scanner/newly_add_column.csv";
    Status status;

    {
        auto* range = _create_scan_range(small_file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(small_file, range, tuple_desc);
        std::vector<std::string> hive_column_names{"name2", "age", "name1", "newly"};
        ctx->hive_column_names = std::move(hive_column_names);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        EXPECT_TRUE(status.ok());

        status = scanner->open(_runtime_state);
        EXPECT_TRUE(status.ok());

        ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 4096);

        status = scanner->get_next(_runtime_state, &chunk);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(2, chunk->num_rows());

        EXPECT_EQ("['smith', '5', 'hello', NULL]", chunk->debug_row(0));
        EXPECT_EQ("['cruise', '6', 'world', NULL]", chunk->debug_row(1));

        scanner->close();
    }
}

TEST_F(HdfsScannerTest, TestCSVWithStructMap) {
    TypeDescriptor struct_col = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    struct_col.field_names.emplace_back("subfield");
    struct_col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    TypeDescriptor array_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);
    array_struct.children.emplace_back(struct_col);

    TypeDescriptor map_col = TypeDescriptor::from_logical_type(LogicalType::TYPE_MAP);
    map_col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    map_col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    TypeDescriptor array_map = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);
    array_map.children.emplace_back(map_col);

    SlotDesc csv_descs[] = {{"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                            {"struct", array_struct},
                            {"map", array_map},
                            {""}};

    const std::string small_file = "./be/test/exec/test_data/csv_scanner/array_struct_map.csv";
    Status status;

    {
        auto* range = _create_scan_range(small_file, 0, 0);
        range->text_file_desc.collection_delim = "|";
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(small_file, range, tuple_desc);
        std::vector<std::string> hive_column_names{"id", "struct", "map"};
        ctx->hive_column_names = std::move(hive_column_names);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        EXPECT_TRUE(status.ok());

        status = scanner->open(_runtime_state);
        EXPECT_TRUE(status.ok());

        ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 4096);

        status = scanner->get_next(_runtime_state, &chunk);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(2, chunk->num_rows());

        // Struct is still unsupported (DefaultValueConverter -> NULL). Maps now parse
        // as Hive text maps: each "map" element has no kv separator, so it becomes a
        // single entry whose key ("map") fails the int conversion (-> NULL) and whose
        // value is missing (-> NULL).
        EXPECT_EQ("[1, [NULL,NULL], [{NULL:NULL},{NULL:NULL},{NULL:NULL}]]", chunk->debug_row(0));
        EXPECT_EQ("[2, [NULL], [{NULL:NULL},{NULL:NULL}]]", chunk->debug_row(1));

        scanner->close();
    }
}

// =============================================================================
// Differential check of the OpenCSVSerde field splitter against Hive's bundled
// opencsv 2.3 parser: opencsv23_golden.tsv exhaustively enumerates every line of
// length 0..5 over the alphabet {a , " \ space} and records what opencsv's
// readNext() returned (N = null, i.e. an all-null row; otherwise fields joined by
// \x1F). Regenerate with the OpenCsvFixtureGen harness against
// io.trino.hive:hive-apache's relocated au.com.bytecode.opencsv if it ever needs
// to change.
TEST_F(HdfsScannerTest, TestOpenCsvSplitterGolden) {
    std::ifstream fin("./be/test/exec/test_data/csv_scanner/opencsv23_golden.tsv");
    ASSERT_TRUE(fin.is_open());
    std::string probe;
    int checked = 0;
    int failed = 0;
    HiveTextFields out;
    while (std::getline(fin, probe)) {
        auto tab = probe.find('\t');
        ASSERT_NE(std::string::npos, tab);
        std::string input = probe.substr(0, tab);
        std::string expected = probe.substr(tab + 1);
        split_hive_open_csv_line(Slice(input), ',', '"', '\\', &out);
        std::string actual;
        if (out.all_null_row) {
            actual = "N";
        } else {
            for (size_t i = 0; i < out.fields.size(); i++) {
                if (i > 0) {
                    actual += '\x1F';
                }
                actual.append(out.fields[i].data, out.fields[i].size);
            }
        }
        if (expected != actual) {
            EXPECT_EQ(expected, actual) << "input=<" << input << ">";
            if (++failed >= 20) {
                FAIL() << "too many splitter mismatches, stopping early";
            }
        }
        checked++;
    }
    EXPECT_EQ(3906, checked);
}

// LazySimpleSerDe splitter unit cases (semantics from Hive's LazyStruct/LazyUtils,
// cross-checked with the audit goldens L2/L3/L9).
// split_hive_lazy_simple_line only finds field BOUNDARIES now (an escaped separator
// does not split); it never decides null and never unescapes. Both of those now
// happen one level down, in NullableConverter, once it is known whether a field is a
// scalar leaf or a complex type that still needs its own escaped bytes intact (see
// TestCSVLazySimpleCompat for that end-to-end behavior, including \N and escaped
// array/map separators).
TEST_F(HdfsScannerTest, TestLazySimpleSplitter) {
    // Fields are zero-copy slices into the input line, so the line must outlive the
    // assertions: |holder| keeps the current case's bytes alive.
    std::string holder;
    auto split = [&holder](const std::string& line, HiveTextFields* out) {
        holder = line;
        split_hive_lazy_simple_line(Slice(holder), ',', '\\', out);
    };
    auto field = [](const HiveTextFields& out, size_t i) {
        return std::string(out.fields[i].data, out.fields[i].size);
    };

    HiveTextFields out;
    // Escaped separator stays in the field, RAW: the backslash is not stripped here.
    split("a\\,b,c", &out);
    ASSERT_EQ(2, out.fields.size());
    EXPECT_EQ("a\\,b", field(out, 0));
    EXPECT_EQ("c", field(out, 1));

    // Raw "\N" and raw "\\N" both pass through untouched -- deciding either is null
    // (only the first is) happens in NullableConverter, not here.
    split("\\N,\\\\N", &out);
    ASSERT_EQ(2, out.fields.size());
    EXPECT_EQ("\\N", field(out, 0));
    EXPECT_EQ("\\\\N", field(out, 1));

    // Empty line is ONE empty field (L3: first column '', the rest null).
    split("", &out);
    ASSERT_EQ(1, out.fields.size());
    EXPECT_EQ("", field(out, 0));
    EXPECT_FALSE(out.all_null_row);

    // A trailing escape at end of line escapes nothing (no next byte to protect) and
    // is not a boundary by itself.
    split("a,b\\", &out);
    ASSERT_EQ(2, out.fields.size());
    EXPECT_EQ("b\\", field(out, 1));

    // Escaped escape: RAW, both backslashes survive (unescaping is not this
    // function's job anymore).
    split("p\\\\q,z", &out);
    ASSERT_EQ(2, out.fields.size());
    EXPECT_EQ("p\\\\q", field(out, 0));
}

TEST_F(HdfsScannerTest, TestCSVArrayLastElementEmpty) {
    TypeDescriptor array_col = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);
    array_col.children.emplace_back(TypeDescriptor::from_logical_type(TYPE_VARCHAR));

    SlotDesc csv_descs[] = {{"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                            {"array", array_col},
                            {"sex", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                            {""}};

    const std::string small_file = "./be/test/exec/test_data/csv_scanner/array_last_element_is_empty.csv";
    Status status;

    {
        auto* range = _create_scan_range(small_file, 0, 0);
        range->text_file_desc.__set_field_delim(csv::DEFAULT_FIELD_DELIM);
        range->text_file_desc.__set_collection_delim(csv::DEFAULT_COLLECTION_DELIM);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(small_file, range, tuple_desc);
        std::vector<std::string> hive_column_names{"id", "array", "sex"};
        ctx->hive_column_names = std::move(hive_column_names);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        EXPECT_TRUE(status.ok());

        status = scanner->open(_runtime_state);
        EXPECT_TRUE(status.ok());

        ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 4096);

        status = scanner->get_next(_runtime_state, &chunk);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(3, chunk->num_rows());

        EXPECT_EQ("['1', ['1','2',''], 'man']", chunk->debug_row(0));
        EXPECT_EQ("['2', ['2','3',''], 'female']", chunk->debug_row(1));
        EXPECT_EQ("['3', ['3','4','5'], 'man']", chunk->debug_row(2));
        scanner->close();
    }
}

TEST_F(HdfsScannerTest, TestCSVWithBlankDelimiter) {
    const std::string small_file = "./be/test/exec/test_data/csv_scanner/array_struct_map.csv";

    SlotDesc csv_descs[] = {{"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)}, {""}};

    auto* tuple_desc = _create_tuple_desc(csv_descs);
    auto* common_range = _create_scan_range(small_file, 0, 0);
    auto* ctx = _create_ctx(small_file, common_range, tuple_desc);
    std::vector<std::string> hive_column_names{"id"};
    ctx->hive_column_names = std::move(hive_column_names);

    {
        auto* range = _create_scan_range(small_file, 0, 0);
        range->text_file_desc.field_delim = "";
        ctx->scan_range = range;
        auto scanner = std::make_shared<HdfsTextScanner>();
        auto status = scanner->init(_runtime_state, ctx);
        EXPECT_FALSE(status.ok());
    }
    {
        auto* range = _create_scan_range(small_file, 0, 0);
        range->text_file_desc.collection_delim = "";
        ctx->scan_range = range;
        auto scanner = std::make_shared<HdfsTextScanner>();
        auto status = scanner->init(_runtime_state, ctx);
        EXPECT_FALSE(status.ok());
    }
    {
        auto* range = _create_scan_range(small_file, 0, 0);
        range->text_file_desc.mapkey_delim = "";
        ctx->scan_range = range;
        auto scanner = std::make_shared<HdfsTextScanner>();
        auto status = scanner->init(_runtime_state, ctx);
        EXPECT_FALSE(status.ok());
    }
    {
        auto* range = _create_scan_range(small_file, 0, 0);
        range->text_file_desc.line_delim = "";
        ctx->scan_range = range;
        auto scanner = std::make_shared<HdfsTextScanner>();
        auto status = scanner->init(_runtime_state, ctx);
        EXPECT_FALSE(status.ok());
    }
}

// =============================================================================

/*
row group 1:           RC:100 TS:28244 OFFSET:4
--------------------------------------------------------------------------------
:                       BINARY SNAPPY DO:4 FPO:447 SZ:577/738/1.28 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
vin:                    BINARY SNAPPY DO:640 FPO:1070 SZ:570/1044/1.83 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
log_domain:             BINARY SNAPPY DO:1279 FPO:1810 SZ:685/1758/2.57 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
file_name:              BINARY SNAPPY DO:2054 FPO:2584 SZ:682/1656/2.43 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
is_collection:          BINARY SNAPPY DO:2823 FPO:3360 SZ:697/2064/2.96 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
is_center:              BINARY SNAPPY DO:3619 FPO:4149 SZ:682/1656/2.43 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
is_cloud:               BINARY SNAPPY DO:4388 FPO:4927 SZ:689/1554/2.26 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
collection_time:        INT96 SNAPPY DO:5161 FPO:5189 SZ:61/57/0.93 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
center_time:            INT96 SNAPPY DO:5284 FPO:5312 SZ:61/57/0.93 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
cloud_time:             INT96 SNAPPY DO:5403 FPO:5431 SZ:61/57/0.93 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
error_collection_tips:  BINARY SNAPPY DO:5521 FPO:6063 SZ:718/2880/4.01 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
error_center_tips:      BINARY SNAPPY DO:6362 FPO:6900 SZ:706/2472/3.50 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
error_cloud_tips:       BINARY SNAPPY DO:7179 FPO:7716 SZ:703/2370/3.37 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
error_collection_time:  BINARY SNAPPY DO:7990 FPO:8532 SZ:718/2880/4.01 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
error_center_time:      BINARY SNAPPY DO:8833 FPO:9371 SZ:706/2472/3.50 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
error_cloud_time:       BINARY SNAPPY DO:9653 FPO:10190 SZ:703/2370/3.37 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
original_time:          BINARY SNAPPY DO:10467 FPO:11001 SZ:694/2064/2.97 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
is_original:            INT64 SNAPPY DO:11263 FPO:11287 SZ:99/95/0.96 VC:100 ENC:RLE,PLAIN_DICTIONARY,PLAIN
*/

TEST_F(HdfsScannerTest, TestParqueTypeMismatchInt96String) {
    SlotDesc parquet_descs[] = {{"vin", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                                {"collection_time", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                                {""}};

    const std::string parquet_file = "./be/test/exec/test_data/parquet_scanner/type_mismatch_int96_string.parquet";

    auto scanner = std::make_shared<HdfsParquetScanner>();
    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* ctx = _create_ctx(parquet_file, range, tuple_desc);

    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    // parquet column reader: not supported convert from parquet `INT96` to `VARCHAR`
    EXPECT_TRUE(!status.ok()) << status.message();
    scanner->close();
}

// =============================================================================
/* data: we expect to read 3 rows [NULL,"",abc]
\N

abc
*/
// there is no newline at EOF.

TEST_F(HdfsScannerTest, TestCSVSingleColumnNullAndEmpty) {
    SlotDesc csv_descs[] = {{"user_id", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)}, {""}};

    const std::string small_file = "./be/test/exec/test_data/csv_scanner/single_column_null_and_empty.csv";
    Status status;

    {
        auto* range = _create_scan_range(small_file, 0, 0);
        auto* tuple_desc = _create_tuple_desc(csv_descs);
        auto* ctx = _create_ctx(small_file, range, tuple_desc);
        build_hive_column_names(ctx, tuple_desc);
        auto scanner = std::make_shared<HdfsTextScanner>();

        status = scanner->init(_runtime_state, ctx);
        ASSERT_TRUE(status.ok()) << status.message();

        status = scanner->open(_runtime_state);
        ASSERT_TRUE(status.ok()) << status.message();

        READ_SCANNER_ROWS(scanner, 3);
        scanner->close();
    }
}

// =============================================================================

/*
id 0 ~ 99
A "00" ~ "99"

file schema: schema
--------------------------------------------------------------------------------
id:          OPTIONAL INT64 R:0 D:1
A:           OPTIONAL BINARY O:UTF8 R:0 D:1

row group 1: RC:100 TS:1730 OFFSET:4
--------------------------------------------------------------------------------
id:           INT64 SNAPPY DO:4 FPO:432 SZ:595/981/1.65 VC:100 ENC:PLAIN,RLE,PLAIN_DICTIONARY
A:            BINARY SNAPPY DO:693 FPO:1139 SZ:581/749/1.29 VC:100 ENC:PLAIN,RLE,PLAIN_DICTIONARY
*/

TEST_F(HdfsScannerTest, TestParquetUppercaseFiledPredicate) {
    SlotDesc parquet_descs[] = {{"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                                {"a", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                                {""}};

    const std::string parquet_file = "./be/test/exec/test_data/parquet_scanner/upcase_field.parquet";

    auto scanner = std::make_shared<HdfsParquetScanner>();
    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* ctx = _create_ctx(parquet_file, range, tuple_desc);

    ctx->min_max_tuple_desc = tuple_desc;
    const TupleDescriptor* min_max_tuple_desc = ctx->min_max_tuple_desc;

    // expect a = '05'
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_string_literal_node(TPrimitiveType::VARCHAR, "05");
        push_binary_pred_texpr_node(nodes, TExprOpcode::GE, min_max_tuple_desc->slots()[1], TPrimitiveType::VARCHAR,
                                    lit_node);
        ExprContext* expr_ctx = create_expr_context(&_pool, nodes);
        ctx->format_scan_context.conjuncts.min_max_ctxs.push_back(expr_ctx);
        ctx->format_scan_context.conjuncts.scanner_ctxs.push_back(expr_ctx);
    }
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_string_literal_node(TPrimitiveType::VARCHAR, "05");
        push_binary_pred_texpr_node(nodes, TExprOpcode::LE, min_max_tuple_desc->slots()[1], TPrimitiveType::VARCHAR,
                                    lit_node);
        ExprContext* expr_ctx = create_expr_context(&_pool, nodes);
        ctx->format_scan_context.conjuncts.min_max_ctxs.push_back(expr_ctx);
        ctx->format_scan_context.conjuncts.scanner_ctxs.push_back(expr_ctx);
    }

    ASSERT_OK(ExprExecutor::prepare(ctx->format_scan_context.conjuncts.min_max_ctxs, _runtime_state));
    ASSERT_OK(ExprExecutor::open(ctx->format_scan_context.conjuncts.min_max_ctxs, _runtime_state));

    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 1);
    EXPECT_EQ(scanner->raw_rows_read(), 100);
    scanner->close();
}

// =============================================================================

/*
file schema: schema
--------------------------------------------------------------------------------
id:          OPTIONAL INT64 R:0 D:1
f00:         OPTIONAL F:1
.list:       REPEATED F:1
..item:      OPTIONAL INT64 R:1 D:3
f01:         OPTIONAL F:1
.list:       REPEATED F:1
..item:      OPTIONAL INT64 R:1 D:3

row group 1: RC:1500 TS:52144 OFFSET:4
--------------------------------------------------------------------------------
id:           INT64 UNCOMPRESSED DO:4 FPO:12023 SZ:14162/14162/1.00 VC:1500 ENC:RLE,PLAIN_DICTIONARY,PLAIN
f00:
.list:
..item:       INT64 UNCOMPRESSED DO:14264 FPO:26307 SZ:18991/18991/1.00 VC:4500 ENC:RLE,PLAIN_DICTIONARY,PLAIN
f01:
.list:
..item:       INT64 UNCOMPRESSED DO:33367 FPO:45410 SZ:18991/18991/1.00 VC:4500 ENC:RLE,PLAIN_DICTIONARY,PLAIN

python code to generate this file.

def array2_parquet():
    def fn():
        records = []
        N = 1500
        for i in range(N):
            x = {'id': i}
            for f in range(2):
                key = 'f%02d' % f
                x[key] = [(j + i) for j in range(5)]
                if i % 2 == 0:
                    x[key] = None

            records.append(x)
        return records

    name = 'array2.parquet'
    write_parquet_file(name, fn)
*/

TEST_F(HdfsScannerTest, TestParquetArrayDecode) {
    TypeDescriptor array_type(TYPE_ARRAY);
    array_type.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    SlotDesc parquet_descs[] = {
            {"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)}, {"f00", array_type}, {""}};

    const std::string parquet_file = "./be/test/exec/test_data/parquet_scanner/array_decode.parquet";

    auto scanner = std::make_shared<HdfsParquetScanner>();
    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* ctx = _create_ctx(parquet_file, range, tuple_desc);

    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 1500);
    EXPECT_EQ(scanner->raw_rows_read(), 1500);
    scanner->close();
}

// =============================================================================

/*

a column, in a row group it's encoded as dict page
but in another row group it's encoded as plain page.

sandbox-cloud :: ~ » hadoop jar ~/installed/parquet-tools-1.9.0.jar dump -c id --disable-data --disable-crop file:///home/disk4/zhangyan/dict2.parquet                                                                                                                                                                                                         1 ↵
row group 0
--------------------------------------------------------------------------------
id:  BINARY UNCOMPRESSED DO:5709 FPO:5709 SZ:163/163/1.00 VC:100 ENC:PLAIN,RLE_DICTIONARY

    id TV=100 RL=0 DL=1 DS: 3 DE:PLAIN
    ----------------------------------------------------------------------------
    page 0:                  DLE:RLE RLE:BIT_PACKED VLE:RLE_DICTIONARY ST:[no stats for this column] SZ:103 VC:100

row group 1
--------------------------------------------------------------------------------
id:  BINARY UNCOMPRESSED DO:0 FPO:16447 SZ:721/721/1.00 VC:100 ENC:PLAIN

    id TV=100 RL=0 DL=1
    ----------------------------------------------------------------------------
    page 0:  DLE:RLE RLE:BIT_PACKED VLE:PLAIN ST:[no stats for this column] SZ:701 VC:100


Python code

def dict2_parquet():
    def fn():
        records = []
        d = ['ysq01', 'ysq02', 'ysq03', None]
        N = 100
        for i in range(N):
            x = {'seq': i}
            for f in range(2):
                x['f%02d' % f] = ''.join([chr(ord('a') + (i + f + x) % 26) for x in range(20)])
            x['id'] = d[i % 4]
            for f in range(2):
                x['f%02d' % (f + 3)] = ''.join([chr(ord('a') + (i + f + x) % 26) for x in range(20)])
            records.append(x)
        return records

    name = 'dict2.parquet'
    json_data = fn()
    df = pd.DataFrame.from_records(json_data)
    # to enforce as dictionary page.
    df['id'] = df['id'].astype('category')
    fastparquet.write(name, df)

    df = pd.DataFrame.from_records(json_data)
    fastparquet.write(name, df, append=True)
    peek_parquet_data(name)
*/

TEST_F(HdfsScannerTest, TestParquetDictTwoPage) {
    SlotDesc parquet_descs[] = {{"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)}, {""}};

    const std::string parquet_file = "./be/test/exec/test_data/parquet_scanner/dict_two_page.parquet";

    auto scanner = std::make_shared<HdfsParquetScanner>();
    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* ctx = _create_ctx(parquet_file, range, tuple_desc);

    ctx->min_max_tuple_desc = tuple_desc;
    const TupleDescriptor* min_max_tuple_desc = ctx->min_max_tuple_desc;

    // expect id = 'ysq01'
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_string_literal_node(TPrimitiveType::VARCHAR, "ysq01");
        push_binary_pred_texpr_node(nodes, TExprOpcode::GE, min_max_tuple_desc->slots()[0], TPrimitiveType::VARCHAR,
                                    lit_node);
        ExprContext* expr_ctx = create_expr_context(&_pool, nodes);
        ctx->format_scan_context.conjuncts.min_max_ctxs.push_back(expr_ctx);
        ctx->format_scan_context.conjuncts.scanner_ctxs.push_back(expr_ctx);
    }
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_string_literal_node(TPrimitiveType::VARCHAR, "ysq01");
        push_binary_pred_texpr_node(nodes, TExprOpcode::LE, min_max_tuple_desc->slots()[0], TPrimitiveType::VARCHAR,
                                    lit_node);
        ExprContext* expr_ctx = create_expr_context(&_pool, nodes);
        ctx->format_scan_context.conjuncts.min_max_ctxs.push_back(expr_ctx);
        ctx->format_scan_context.conjuncts.scanner_ctxs.push_back(expr_ctx);
    }

    ASSERT_OK(ExprExecutor::prepare(ctx->format_scan_context.conjuncts.min_max_ctxs, _runtime_state));
    ASSERT_OK(ExprExecutor::open(ctx->format_scan_context.conjuncts.min_max_ctxs, _runtime_state));

    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 50);
    EXPECT_EQ(scanner->raw_rows_read(), 200);
    scanner->close();
}

// Test min-max logic when parquet file contains complex types
TEST_F(HdfsScannerTest, TestMinMaxFilterWhenContainsComplexTypes) {
    SlotDesc parquet_descs[] = {{"col82", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)}, {""}};

    const std::string parquet_file = "./be/test/exec/test_data/parquet_data/min-max-complex-types.parquet";

    auto scanner = std::make_shared<HdfsParquetScanner>();
    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* ctx = _create_ctx(parquet_file, range, tuple_desc);

    ctx->min_max_tuple_desc = tuple_desc;
    const TupleDescriptor* min_max_tuple_desc = ctx->min_max_tuple_desc;

    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_int_literal_node(TPrimitiveType::INT, 82);
        push_binary_pred_texpr_node(nodes, TExprOpcode::GE, min_max_tuple_desc->slots()[0], TPrimitiveType::INT,
                                    lit_node);
        ExprContext* expr_ctx = create_expr_context(&_pool, nodes);
        ctx->format_scan_context.conjuncts.min_max_ctxs.push_back(expr_ctx);
        ctx->format_scan_context.conjuncts.scanner_ctxs.push_back(expr_ctx);
        ctx->format_scan_context.conjuncts.all_ctxs.push_back(expr_ctx);
    }
    {
        std::vector<TExprNode> nodes;
        TExprNode lit_node = create_int_literal_node(TPrimitiveType::INT, 82);
        push_binary_pred_texpr_node(nodes, TExprOpcode::LE, min_max_tuple_desc->slots()[0], TPrimitiveType::INT,
                                    lit_node);
        ExprContext* expr_ctx = create_expr_context(&_pool, nodes);
        ctx->format_scan_context.conjuncts.min_max_ctxs.push_back(expr_ctx);
        ctx->format_scan_context.conjuncts.scanner_ctxs.push_back(expr_ctx);
        ctx->format_scan_context.conjuncts.all_ctxs.push_back(expr_ctx);
    }

    ASSERT_OK(ExprExecutor::prepare(ctx->format_scan_context.conjuncts.min_max_ctxs, _runtime_state));
    ASSERT_OK(ExprExecutor::open(ctx->format_scan_context.conjuncts.min_max_ctxs, _runtime_state));

    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 0);
    EXPECT_EQ(scanner->raw_rows_read(), 0);
    scanner->close();
}

// =======================================================

static bool can_run_jni_test() {
    if (getenv("STARROCKS_SRC_HOME") == nullptr) {
        // Right now it's very hard to construct jni ut, because we only build ut-binary but not java extensions.
        // To make this test run, we have to define `STARROCKS_SRC_HOME` and `STARROCKS_HOME`(which has java-extensions)
        // and we have to set following env vars
        /*
        STARROCKS_SRC_HOME=${STARROCKS_HOME} \
        STARROCKS_HOME=${STARROCKS_HOME}/output/be \
        HADOOP_CLASSPATH=${STARROCKS_HOME}/lib/hadoop/common/*:${STARROCKS_HOME}/lib/hadoop/common/lib/*:${STARROCKS_HOME}/lib/hadoop/hdfs/*:${STARROCKS_HOME}/lib/hadoop/hdfs/lib/* \
        CLASSPATH=$STARROCKS_HOME/conf:$STARROCKS_HOME/lib/jni-packages/*:$HADOOP_CLASSPATH:$CLASSPATH \
        LD_LIBRARY_PATH=$STARROCKS_HOME/lib/hadoop/native:$LD_LIBRARY_PATH \
        */
        return false;
    }
    return true;
}

/*

CREATE TABLE `test_hudi_mor2` (
  `uuid` STRING,
  `ts` int,
  `a` int,
  `b` string,
  `c` array<array<int>>,
  `d` map<string, array<int>>,
  `e` struct<a:array<int>, b:map<string,int>, c:struct<a:array<int>, b:struct<a:int,b:string>>>)
  USING hudi
TBLPROPERTIES (
  'primaryKey' = 'uuid',
  'preCombineField' = 'ts',
  'type' = 'mor');

insert into test_hudi_mor2 values('AA0', 10, 0, "hello", array(array(10,20,30), array(40,50,60,70) ), map('key1', array(1,10), 'key2', array(2, 20), 'key3', null), struct(array(10, 20), map('key1', 10), struct(array(10, 20), struct(10, "world")))),
 ('AA1', 10, 0, "hello", null, null , struct(null, map('key1', 10), struct(array(10, 20), struct(10, "world")))),
 ('AA2', 10, 0, null, array(array(30, 40), array(10,20,30)), null , struct(null, map('key1', 10), struct(array(10, 20), null)));

spark-sql> select a,b,c,d,e from test_hudi_mor2;
a       b       c       d       e
0       hello   NULL    NULL    {"a":null,"b":{"key1":10},"c":{"a":[10,20],"b":{"a":10,"b":"world"}}}
0       NULL    [[30,40],[10,20,30]]    NULL    {"a":null,"b":{"key1":10},"c":{"a":[10,20],"b":null}}
0       hello   [[10,20,30],[40,50,60,70]]      {"key1":[1,10],"key2":[2,20],"key3":null}       {"a":[10,20],"b":{"key1":10},"c":{"a":[10,20],"b":{"a":10,"b":"world"}}}

*/
TEST_F(HdfsScannerTest, TestHudiMORArrayMapStruct) {
    if (!can_run_jni_test()) {
        GTEST_SKIP();
    }

    TypeDescriptor C(LogicalType::TYPE_ARRAY);
    TypeDescriptor C1(LogicalType::TYPE_ARRAY);
    C1.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    C.children.push_back(C1); // array<array<int>>

    TypeDescriptor D(LogicalType::TYPE_MAP);
    D.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22));
    D.children.push_back(C1); // map<string, array<int>>

    TypeDescriptor E(LogicalType::TYPE_STRUCT);
    {
        E.children.push_back(C1); // a: array<int>
        E.field_names.emplace_back("a");

        TypeDescriptor B0(LogicalType::TYPE_MAP);
        B0.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22));
        B0.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        E.children.push_back(B0);
        E.field_names.emplace_back("b");

        TypeDescriptor C0(LogicalType::TYPE_STRUCT);
        {
            C0.children.push_back(C1);
            C0.field_names.emplace_back("a");

            TypeDescriptor B1(LogicalType::TYPE_STRUCT);
            B1.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
            B1.field_names.emplace_back("a");
            B1.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22));
            B1.field_names.emplace_back("b");
            C0.children.push_back(B1);
            C0.field_names.emplace_back("b");
        }

        E.children.push_back(C0);
        E.field_names.emplace_back("c");
    }

    SlotDesc parquet_descs[] = {{"a", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                                {"b", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                                {"c", C},
                                {"d", D},
                                {"e", E},
                                {""}};
    std::string starrocks_home = getenv("STARROCKS_SRC_HOME");
    std::string basePath = starrocks_home + "/be/test/exec/test_data/jni_scanner/test_hudi_mor_ams";
    std::string parquet_file = basePath + "/0df0196b-f46f-43f5-8cf0-06fad7143af3-0_0-27-35_20230110191854854.parquet";

    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* ctx = _create_ctx(parquet_file, range, tuple_desc);

    // be/test/exec/test_data/jni_scanner/test_hudi_mor_ams
    std::map<std::string, std::string> params;

    params.emplace("base_path", basePath);
    params.emplace("data_file_path", parquet_file);
    params.emplace("delta_file_paths", "");
    params.emplace("hive_column_names",
                   "_hoodie_commit_time,_hoodie_commit_seqno,_hoodie_record_key,_hoodie_partition_path,_hoodie_file_"
                   "name,uuid,ts,a,b,c,d,e");
    params.emplace("hive_column_types",
                   "string#string#string#string#string#string#int#int#string#array<array<int>>#map<string,array<int>>#"
                   "struct<a:array<int>,b:map<string,int>,c:struct<a:array<int>,b:struct<a:int,b:string>>>");
    params.emplace("instant_time", "20230110185815638");
    params.emplace("data_file_length", "438311");
    params.emplace("input_format", "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
    params.emplace("serde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
    params.emplace("required_fields", "a,b,c,d,e");

    std::string scanner_factory_class = "com/starrocks/hudi/reader/HudiSliceScannerFactory";
    JniScanner* scanner = _pool.add(new JniScanner(scanner_factory_class, params));

    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 3);
    EXPECT_EQ(scanner->raw_rows_read(), 3);
    scanner->close();

    EXPECT_EQ(_debug_row_output, "[0, 'hello', NULL, NULL, {a:NULL,b:{'key1':10},c:{a:[10,20],b:{a:10,b:'world'}}}]\n");
}

/*
CREATE TABLE `test_hudi_mor` (
  `uuid` STRING,
  `ts` int,
  `a` int,
  `b` string,
  `c` array<int>,
  `d` map<string, int>,
  `e` struct<a:int, b:string>)
  USING hudi
TBLPROPERTIES (
  'primaryKey' = 'uuid',
  'preCombineField' = 'ts',
  'type' = 'mor');

spark-sql> select a,b,c,d,e from test_hudi_mor;
a       b       c       d       e
1       hello   [10,20,30]      {"key1":1,"key2":2}     {"a":10,"b":"world"}

*/
TEST_F(HdfsScannerTest, TestHudiMORArrayMapStruct2) {
    if (!can_run_jni_test()) {
        GTEST_SKIP();
    }

    std::string starrocks_home = getenv("STARROCKS_SRC_HOME");
    std::string basePath = starrocks_home + "/be/test/exec/test_data/jni_scanner/test_hudi_mor_ams2";
    std::string parquet_file = basePath + "/64798197-be6a-4eca-9898-0c2ed75b9d65-0_0-54-41_20230105142938081.parquet";

    auto* range = _create_scan_range(parquet_file, 0, 0);

    // be/test/exec/test_data/jni_scanner/test_hudi_mor_ams
    std::map<std::string, std::string> params;

    params.emplace("base_path", basePath);
    params.emplace("data_file_path", parquet_file);
    params.emplace("delta_file_paths",
                   basePath + "/.64798197-be6a-4eca-9898-0c2ed75b9d65-0_20230105142938081.log.1_0-95-78");
    params.emplace("hive_column_names",
                   "_hoodie_commit_time,_hoodie_commit_seqno,_hoodie_record_key,_hoodie_partition_path,_hoodie_file_"
                   "name,uuid,ts,a,b,c,d,e");
    params.emplace("hive_column_types",
                   "string#string#string#string#string#string#int#int#string#array<int>#map<string,int>#struct<a:int,b:"
                   "string>");
    params.emplace("instant_time", "20230105143305070");
    params.emplace("data_file_length", "436081");
    params.emplace("input_format", "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
    params.emplace("serde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
    params.emplace("required_fields", "a,b,c,d,e");

    std::string scanner_factory_class = "com/starrocks/hudi/reader/HudiSliceScannerFactory";

    // select a,b,c,d,e
    {
        // c: array<int>
        TypeDescriptor C(LogicalType::TYPE_ARRAY);
        C.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

        // d: map<string, int>
        TypeDescriptor D(LogicalType::TYPE_MAP);
        D.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22));
        D.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

        TypeDescriptor E(LogicalType::TYPE_STRUCT);
        E.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        E.field_names.emplace_back("a");
        E.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22));
        E.field_names.emplace_back("b");

        SlotDesc parquet_descs[] = {{"a", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                                    {"b", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
                                    {"c", C},
                                    {"d", D},
                                    {"e", E},
                                    {""}};

        auto* tuple_desc = _create_tuple_desc(parquet_descs);
        auto* ctx = _create_ctx(parquet_file, range, tuple_desc);

        JniScanner* scanner = _pool.add(new JniScanner(scanner_factory_class, params));

        Status status = scanner->init(_runtime_state, ctx);
        EXPECT_TRUE(status.ok());
        status = scanner->open(_runtime_state);
        EXPECT_TRUE(status.ok());
        READ_SCANNER_ROWS(scanner, 1);
        EXPECT_EQ(scanner->raw_rows_read(), 1);
        scanner->close();

        EXPECT_EQ(_debug_row_output, "[1, 'hello', [10,20,30], {'key1':1,'key2':2}, {a:10,b:'world'}]\n");
    }

    // select e but as <b:string, a:int>
    {
        TypeDescriptor E(LogicalType::TYPE_STRUCT);
        E.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22));
        E.field_names.emplace_back("b");
        E.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        E.field_names.emplace_back("a");

        SlotDesc parquet_descs[] = {{"e", E}, {""}};

        auto* tuple_desc = _create_tuple_desc(parquet_descs);
        auto* ctx = _create_ctx(parquet_file, range, tuple_desc);

        params["required_fields"] = "e";
        params["nested_fields"] = "e.b,e.a";
        JniScanner* scanner = _pool.add(new JniScanner(scanner_factory_class, params));

        Status status = scanner->init(_runtime_state, ctx);
        EXPECT_TRUE(status.ok());
        status = scanner->open(_runtime_state);
        EXPECT_TRUE(status.ok());
        READ_SCANNER_ROWS(scanner, 1);
        EXPECT_EQ(scanner->raw_rows_read(), 1);
        scanner->close();

        EXPECT_EQ(_debug_row_output, "[{b:'world',a:10}]\n");
    }

    // select e but as <B:string, a:int>
    {
        TypeDescriptor E(LogicalType::TYPE_STRUCT);
        E.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22));
        E.field_names.emplace_back("B");
        E.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        E.field_names.emplace_back("a");

        SlotDesc parquet_descs[] = {{"e", E}, {""}};

        auto* tuple_desc = _create_tuple_desc(parquet_descs);
        auto* ctx = _create_ctx(parquet_file, range, tuple_desc);

        params["required_fields"] = "e";
        params["nested_fields"] = "e.B,e.a";
        JniScanner* scanner = _pool.add(new JniScanner(scanner_factory_class, params));

        Status status = scanner->init(_runtime_state, ctx);
        EXPECT_TRUE(status.ok());
        status = scanner->open(_runtime_state);
        EXPECT_TRUE(status.ok());
        READ_SCANNER_ROWS(scanner, 1);
        EXPECT_EQ(scanner->raw_rows_read(), 1);
        scanner->close();

        EXPECT_EQ(_debug_row_output, "[{B:NULL,a:10}]\n");
    }

    // select e but as <a:int>
    {
        TypeDescriptor E(LogicalType::TYPE_STRUCT);
        E.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        E.field_names.emplace_back("a");

        SlotDesc parquet_descs[] = {{"e", E}, {""}};

        auto* tuple_desc = _create_tuple_desc(parquet_descs);
        auto* ctx = _create_ctx(parquet_file, range, tuple_desc);

        params["required_fields"] = "e";
        params["nested_fields"] = "e.a";
        JniScanner* scanner = _pool.add(new JniScanner(scanner_factory_class, params));

        Status status = scanner->init(_runtime_state, ctx);
        EXPECT_TRUE(status.ok());
        status = scanner->open(_runtime_state);
        EXPECT_TRUE(status.ok());
        READ_SCANNER_ROWS(scanner, 1);
        EXPECT_EQ(scanner->raw_rows_read(), 1);
        scanner->close();

        EXPECT_EQ(_debug_row_output, "[{a:10}]\n");
    }

    // select d but only key
    {
        // d: map<string, int>
        TypeDescriptor D(LogicalType::TYPE_MAP);
        D.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22));
        D.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_UNKNOWN));

        SlotDesc parquet_descs[] = {{"d", D}, {""}};

        auto* tuple_desc = _create_tuple_desc(parquet_descs);
        auto* ctx = _create_ctx(parquet_file, range, tuple_desc);

        params["required_fields"] = "d";
        params["nested_fields"] = "d.$0";
        JniScanner* scanner = _pool.add(new JniScanner(scanner_factory_class, params));

        Status status = scanner->init(_runtime_state, ctx);
        EXPECT_TRUE(status.ok());
        status = scanner->open(_runtime_state);
        EXPECT_TRUE(status.ok());
        READ_SCANNER_ROWS(scanner, 1);
        EXPECT_EQ(scanner->raw_rows_read(), 1);
        scanner->close();

        EXPECT_EQ(_debug_row_output, "[{'key1':NULL,'key2':NULL}]\n");
    }
}

// =============================================================================
/*
spark-sql> select * from test_hudi_mor13;
_hoodie_commit_time     _hoodie_commit_seqno    _hoodie_record_key      _hoodie_partition_path  _hoodie_file_name       a       b       ts      uuid
20230216103800029       20230216103800029_0_0   aa1             14260209-008c-4170-bed3-5533d8783f0f-0_0-167-153_20230216103800029.parquet      300     3023-01-01 00:00:00     40      aa1
20230216103800029       20230216103800029_0_1   aa0             14260209-008c-4170-bed3-5533d8783f0f-0_0-167-153_20230216103800029.parquet      200     2023-01-01 00:00:00     30      aa0
20230216103800029       20230216103800029_0_2   aa2             14260209-008c-4170-bed3-5533d8783f0f-0_0-167-153_20230216103800029.parquet      400     1000-01-01 00:00:00     50      aa2
20230216103800029       20230216103800029_0_3   aa              14260209-008c-4170-bed3-5533d8783f0f-0_0-167-153_20230216103800029.parquet      100     1900-01-01 00:00:00     20      aa
*/

/*
_hoodie_commit_time = 20230216103800029
_hoodie_commit_seqno = 20230216103800029_0_0
_hoodie_record_key = aa1
_hoodie_partition_path =
_hoodie_file_name = 14260209-008c-4170-bed3-5533d8783f0f-0_0-167-153_20230216103800029.parquet
a = 300
b = 33229411200000000
ts = 40
uuid = aa1

_hoodie_commit_time = 20230216103800029
_hoodie_commit_seqno = 20230216103800029_0_1
_hoodie_record_key = aa0
_hoodie_partition_path =
_hoodie_file_name = 14260209-008c-4170-bed3-5533d8783f0f-0_0-167-153_20230216103800029.parquet
a = 200
b = 1672502400000000
ts = 30
uuid = aa0

_hoodie_commit_time = 20230216103800029
_hoodie_commit_seqno = 20230216103800029_0_2
_hoodie_record_key = aa2
_hoodie_partition_path =
_hoodie_file_name = 14260209-008c-4170-bed3-5533d8783f0f-0_0-167-153_20230216103800029.parquet
a = 400
b = -30610253143000000
ts = 50
uuid = aa2

_hoodie_commit_time = 20230216103800029
_hoodie_commit_seqno = 20230216103800029_0_3
_hoodie_record_key = aa
_hoodie_partition_path =
_hoodie_file_name = 14260209-008c-4170-bed3-5533d8783f0f-0_0-167-153_20230216103800029.parquet
a = 100
b = -2209017943000000
ts = 20
uuid = aa
*/

TEST_F(HdfsScannerTest, TestParquetTimestampToDatetime) {
    SlotDesc parquet_descs[] = {{"b", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME)}, {""}};

    const std::string parquet_file = "./be/test/exec/test_data/parquet_scanner/timestamp_to_datetime.parquet";

    _create_runtime_state("Asia/Shanghai");
    auto scanner = std::make_shared<HdfsParquetScanner>();
    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* ctx = _create_ctx(parquet_file, range, tuple_desc);

    _debug_rows_per_call = 10;
    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 4);
    EXPECT_EQ(scanner->raw_rows_read(), 4);
    EXPECT_EQ(_debug_row_output,
              "[3023-01-01 00:00:00]\n[2023-01-01 00:00:00]\n[1000-01-01 00:00:00]\n[1900-01-01 00:00:00]\n");
    scanner->close();
}

TEST_F(HdfsScannerTest, TestParquetIcebergCaseSensitive) {
    SlotDesc parquet_descs[] = {{"Id", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)}, {""}};

    const std::string parquet_file =
            "./be/test/formats/parquet/test_data/iceberg_schema_evolution/add_struct_subfield.parquet";

    _create_runtime_state("Asia/Shanghai");
    auto scanner = std::make_shared<HdfsParquetScanner>();
    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* ctx = _create_ctx(parquet_file, range, tuple_desc);

    TIcebergSchema schema = TIcebergSchema{};

    TIcebergSchemaField field_id{};
    field_id.__set_field_id(1);
    field_id.__set_name("Id");

    std::vector<TIcebergSchemaField> fields{field_id};
    schema.__set_fields(fields);
    ctx->format_scan_context.lake_schema = &schema;

    _debug_rows_per_call = 10;
    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 1);
    EXPECT_EQ(scanner->raw_rows_read(), 1);
    EXPECT_EQ(_debug_row_output, "[1]\n");
    scanner->close();
}

TEST_F(HdfsScannerTest, TestParquetLZOFormat) {
    SlotDesc parquet_descs[] = {{"a", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                {"b", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                {""}};

    const std::string parquet_file = "./be/test/exec/test_data/parquet_scanner/lzo_compression.parquet";

    _create_runtime_state("Asia/Shanghai");
    auto scanner = std::make_shared<HdfsParquetScanner>();
    auto* range = _create_scan_range(parquet_file, 0, 0);
    auto* tuple_desc = _create_tuple_desc(parquet_descs);
    auto* ctx = _create_ctx(parquet_file, range, tuple_desc);

    Status status = scanner->init(_runtime_state, ctx);
    EXPECT_TRUE(status.ok());
    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());
    READ_SCANNER_ROWS(scanner, 100000);
    scanner->close();
}

#if defined(WITH_STARCACHE)
// Real columns of `default_parquet_file`, so that its data pages are read.
static SlotDesc corrupted_datacache_parquet_descs[] = {
        {"col1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
        {"col2", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
        {"col3", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR, 22)},
        {"col4", TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME)},
        {""}};

// Two row groups, with c0 in [0, 1023] and [1024, 1999].
static const std::string multi_row_groups_parquet_file =
        "./be/test/exec/test_data/parquet_scanner/multi_stripes.parquet";
static SlotDesc multi_row_groups_parquet_descs[] = {
        {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
        {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
        {""}};
// The first data page of c0 in the second row group: a header, then the Snappy compressed data of one literal.
static constexpr size_t second_row_group_page_offset = 9859;
static constexpr size_t second_row_group_page_header_size = 64;

static const std::string corrupted_datacache_orc_file = "./be/test/exec/test_data/orc_scanner/mtypes_100.orc.zlib";
static SlotDesc corrupted_datacache_orc_descs[] = {
        {"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
        {"col_varchar", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
        {""}};

#define ASSERT_SCAN_MATCHES(Scanner, file)            \
    do {                                              \
        auto res = _scan<Scanner>(file);              \
        ASSERT_TRUE(res.ok()) << res.status();        \
        ASSERT_EQ((file).expected_rows, res.value()); \
    } while (false)

class HdfsScannerCorruptedDatacacheTest : public HdfsScannerTest {
public:
    void SetUp() override {
        HdfsScannerTest::SetUp();
        auto cache_options = TestCacheUtils::create_simple_options(config::datacache_block_size, 50 * MB);
        _block_cache = TestCacheUtils::create_cache(cache_options);
        DataCache::GetInstance()->set_block_cache(_block_cache);
        _clear_corrupted_cache_data = config::external_table_clear_corrupted_cache_data;
    }

    void TearDown() override {
        config::external_table_clear_corrupted_cache_data = _clear_corrupted_cache_data;
        DataCache::GetInstance()->set_page_cache(nullptr);
        DataCache::GetInstance()->set_block_cache(nullptr);
        HdfsScannerTest::TearDown();
    }

protected:
    struct TestFile {
        std::string path;
        SlotDesc* descs = nullptr;
        int64_t size = 0;
        std::string cache_key;
        std::vector<std::string> expected_rows;
        // Applied to the context of every scan.
        std::function<void(HdfsScannerContext*)> setup;
    };

    // Scan the whole file and return its rows.
    template <typename Scanner>
    StatusOr<std::vector<std::string>> _scan(const TestFile& file, bool enable_datacache = true,
                                             const std::function<void()>& after_open = nullptr) {
        auto* range = _create_scan_range(file.path, 0, 0);
        auto* tuple_desc = _create_tuple_desc(file.descs);
        auto* ctx = _create_ctx(file.path, range, tuple_desc);
        ctx->datacache_options.enable_datacache = enable_datacache;
        ctx->datacache_options.enable_populate_datacache = enable_datacache;
        if (file.setup) {
            file.setup(ctx);
        }

        auto scanner = std::make_shared<Scanner>();
        RETURN_IF_ERROR(scanner->init(_runtime_state, ctx));
        DeferOp defer([&]() { scanner->close(); });
        RETURN_IF_ERROR(scanner->open(_runtime_state));
        if (after_open) {
            after_open();
        }
        std::vector<std::string> rows;
        while (true) {
            ChunkPtr chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 0);
            Status status = scanner->get_next(_runtime_state, &chunk);
            if (status.is_end_of_file()) {
                break;
            }
            RETURN_IF_ERROR(status);
            for (size_t i = 0; i < chunk->num_rows(); i++) {
                rows.emplace_back(chunk->debug_row(i));
            }
        }
        return rows;
    }

    // Expected rows are read bypassing the datacache.
    template <typename Scanner>
    void _prepare(const std::string& path, SlotDesc* descs, size_t expected_num_rows, TestFile* file) {
        file->path = path;
        file->descs = descs;
        ASSIGN_OR_ABORT(file->size, FileSystem::Default()->get_file_size(path));
        // The whole file fits in block 0.
        ASSERT_LT(file->size, config::datacache_block_size);
        file->cache_key = CacheInputStream::calc_cache_key(path, file->size, 0);
        ASSIGN_OR_ABORT(file->expected_rows, _scan<Scanner>(*file, false));
        ASSERT_EQ(expected_num_rows, file->expected_rows.size());
        ASSERT_FALSE(_file_cached(*file));
    }

    void _write_cached_file(const TestFile& file, const std::string& data) {
        DiskCacheWriteOptions options;
        options.overwrite = true;
        ASSERT_OK(_block_cache->write(file.cache_key, 0, data.size(), data.data(), &options));
    }

    // Overwrite the cached copy of the file with `mutate(content)`.
    void _corrupt_cached_file(const TestFile& file, const std::function<void(std::string*)>& mutate) {
        std::string data(file.size, '\0');
        ASSERT_OK(_block_cache->read(file.cache_key, 0, file.size, data.data()).status());
        mutate(&data);
        _write_cached_file(file, data);
    }

    // Zero the header of the first data page of c0 in the second row group of `multi_row_groups_parquet_file`.
    static void _corrupt_second_row_group(std::string* data) {
        auto begin = data->begin() + second_row_group_page_offset;
        std::fill(begin, begin + second_row_group_page_header_size, '\0');
    }

    // Filter out the rows of the first row group of `multi_row_groups_parquet_file`. The conjunct is not in the
    // predicate tree, so the row group is not filtered by its statistics but returns an empty chunk.
    void _filter_first_row_group(HdfsScannerContext* ctx) {
        SlotDescriptor* c0 = ctx->tuple_desc->slots()[0];
        std::vector<TExprNode> nodes;
        push_binary_pred_texpr_node(nodes, TExprOpcode::GE, c0, TPrimitiveType::BIGINT,
                                    create_int_literal_node(TPrimitiveType::BIGINT, 1024));
        std::vector<ExprContext*> conjuncts{create_expr_context(&_pool, nodes)};
        ASSERT_OK(ExprExecutor::prepare(conjuncts, _runtime_state));
        ASSERT_OK(ExprExecutor::open(conjuncts, _runtime_state));
        ctx->format_scan_context.conjuncts.by_slot[c0->id()] = conjuncts;
    }

    // Replace the page cache, which also caches footers, with an empty one.
    void _reset_page_cache() {
        auto engine = std::make_shared<LRUCacheEngine>();
        ASSERT_OK(engine->init(MemCacheOptions{.mem_space_size = 100 * MB}));
        DataCache::GetInstance()->set_page_cache(std::make_shared<StoragePageCache>(engine.get()));
        _page_cache_engine = std::move(engine);
    }

    bool _file_cached(const TestFile& file) const { return _block_cache->exist(file.cache_key, 0, file.size); }

    static int64_t _removed_files() {
        return std::stoll(bvar::Variable::describe_exposed("datacache_corruption_removed_files"));
    }
    static int64_t _retried_files() {
        return std::stoll(bvar::Variable::describe_exposed("datacache_corruption_retried_files"));
    }

    std::shared_ptr<BlockCache> _block_cache;
    std::shared_ptr<LRUCacheEngine> _page_cache_engine;
    bool _clear_corrupted_cache_data = true;
};

// Corrupted footer in the cache: the cache is dropped and the file is read again bypassing it.
TEST_F(HdfsScannerCorruptedDatacacheTest, TestParquetDropCorruptedFooterAndRetry) {
    TestFile file;
    ASSERT_NO_FATAL_FAILURE(
            _prepare<HdfsParquetScanner>(default_parquet_file, corrupted_datacache_parquet_descs, 4, &file));
    // Populate the datacache.
    ASSERT_SCAN_MATCHES(HdfsParquetScanner, file);
    ASSERT_TRUE(_file_cached(file));

    ASSERT_NO_FATAL_FAILURE(
            _corrupt_cached_file(file, [](std::string* data) { data->replace(data->size() - 4, 4, "XXXX"); }));

    // Switch off: the error is returned and the corrupted copy is kept.
    config::external_table_clear_corrupted_cache_data = false;
    auto res = _scan<HdfsParquetScanner>(file);
    ASSERT_TRUE(res.status().is_corruption()) << res.status();
    ASSERT_TRUE(res.status().message().find("magic not matched") != std::string::npos) << res.status();
    ASSERT_TRUE(_file_cached(file));

    // Switch on: the corrupted copy is dropped and the file is read again.
    config::external_table_clear_corrupted_cache_data = true;
    const int64_t removed_files = _removed_files();
    const int64_t retried_files = _retried_files();
    ASSERT_SCAN_MATCHES(HdfsParquetScanner, file);
    ASSERT_FALSE(_file_cached(file));
    ASSERT_EQ(removed_files + 1, _removed_files());
    ASSERT_EQ(retried_files + 1, _retried_files());

    // The next reads re-populate and hit the datacache.
    ASSERT_SCAN_MATCHES(HdfsParquetScanner, file);
    ASSERT_TRUE(_file_cached(file));
    ASSERT_SCAN_MATCHES(HdfsParquetScanner, file);
    ASSERT_EQ(retried_files + 1, _retried_files());
}

// Corrupted data pages in the cache, with an intact footer.
TEST_F(HdfsScannerCorruptedDatacacheTest, TestParquetDropCorruptedPagesAndRetry) {
    TestFile file;
    ASSERT_NO_FATAL_FAILURE(
            _prepare<HdfsParquetScanner>(default_parquet_file, corrupted_datacache_parquet_descs, 4, &file));
    ASSERT_SCAN_MATCHES(HdfsParquetScanner, file);
    ASSERT_TRUE(_file_cached(file));

    // Zero the first page header, right after the leading magic.
    ASSERT_NO_FATAL_FAILURE(_corrupt_cached_file(
            file, [](std::string* data) { std::fill(data->begin() + 4, data->begin() + 64, '\0'); }));

    config::external_table_clear_corrupted_cache_data = false;
    auto res = _scan<HdfsParquetScanner>(file);
    ASSERT_FALSE(res.ok()) << "corrupted pages are read without error: " << ::testing::PrintToString(res.value());
    ASSERT_TRUE(res.status().is_corruption() || res.status().is_internal_error()) << res.status();

    config::external_table_clear_corrupted_cache_data = true;
    const int64_t retried_files = _retried_files();
    ASSERT_SCAN_MATCHES(HdfsParquetScanner, file);
    ASSERT_FALSE(_file_cached(file));
    ASSERT_EQ(retried_files + 1, _retried_files());
}

// A file corrupted in the remote storage: the cached copy matches it, so the cache is kept.
TEST_F(HdfsScannerCorruptedDatacacheTest, TestKeepCacheOfCorruptedRemoteFile) {
    std::ifstream in(default_parquet_file, std::ios::binary);
    std::string data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    data.replace(data.size() - 4, 4, "XXXX");
    const std::string corrupted_file = "./be/test/exec/test_data/parquet_scanner/corrupted_remote_file.parquet";
    std::ofstream(corrupted_file, std::ios::binary) << data;
    DeferOp defer([&]() { std::filesystem::remove(corrupted_file); });
    TestFile file{.path = corrupted_file,
                  .descs = corrupted_datacache_parquet_descs,
                  .size = static_cast<int64_t>(data.size()),
                  .cache_key = CacheInputStream::calc_cache_key(corrupted_file, data.size(), 0)};

    const int64_t removed_files = _removed_files();
    const int64_t retried_files = _retried_files();
    // Nothing comes from the datacache, so nothing is handled, while the content is populated.
    auto res = _scan<HdfsParquetScanner>(file);
    ASSERT_TRUE(res.status().is_corruption()) << res.status();
    ASSERT_EQ(retried_files, _retried_files());
    ASSERT_TRUE(_file_cached(file));

    // Reading again bypassing the datacache fails as well, so the cache is kept.
    res = _scan<HdfsParquetScanner>(file);
    ASSERT_TRUE(res.status().is_corruption()) << res.status();
    ASSERT_EQ(retried_files + 1, _retried_files());
    ASSERT_EQ(removed_files, _removed_files());
    ASSERT_TRUE(_file_cached(file));
}

// Empty chunks are returned before the corrupted page in the cache is read, with the page cache and the footer cache
// on as by default: the file is read again up to some rows.
TEST_F(HdfsScannerCorruptedDatacacheTest, TestParquetRetryPastEmptyChunks) {
    TestFile file;
    file.setup = [this](HdfsScannerContext* ctx) {
        _filter_first_row_group(ctx);
        ctx->format_scan_context.options.use_file_metacache = true;
        ctx->format_scan_context.options.use_file_pagecache = true;
    };
    ASSERT_NO_FATAL_FAILURE(_reset_page_cache());
    ASSERT_NO_FATAL_FAILURE(
            _prepare<HdfsParquetScanner>(multi_row_groups_parquet_file, multi_row_groups_parquet_descs, 976, &file));
    ASSERT_NO_FATAL_FAILURE(_reset_page_cache());
    ASSERT_SCAN_MATCHES(HdfsParquetScanner, file);
    ASSERT_TRUE(_file_cached(file));

    ASSERT_NO_FATAL_FAILURE(_corrupt_cached_file(file, _corrupt_second_row_group));
    ASSERT_NO_FATAL_FAILURE(_reset_page_cache());
    const int64_t retried_files = _retried_files();
    ASSERT_SCAN_MATCHES(HdfsParquetScanner, file);
    ASSERT_FALSE(_file_cached(file));
    ASSERT_EQ(retried_files + 1, _retried_files());

    // The corrupted page header fails before the page is put into the page cache, so the next read recovers.
    ASSERT_SCAN_MATCHES(HdfsParquetScanner, file);
    ASSERT_EQ(retried_files + 1, _retried_files());
}

// A data page failing decompression in the cache, which returns InvalidArgument.
TEST_F(HdfsScannerCorruptedDatacacheTest, TestParquetRetryOnFailedDecompression) {
    TestFile file;
    file.setup = [this](HdfsScannerContext* ctx) { _filter_first_row_group(ctx); };
    ASSERT_NO_FATAL_FAILURE(
            _prepare<HdfsParquetScanner>(multi_row_groups_parquet_file, multi_row_groups_parquet_descs, 976, &file));
    ASSERT_SCAN_MATCHES(HdfsParquetScanner, file);
    ASSERT_TRUE(_file_cached(file));

    // Skip the 2-byte length of the Snappy data, and turn its literal tag into a copy before the start of the output.
    ASSERT_NO_FATAL_FAILURE(_corrupt_cached_file(file, [](std::string* data) {
        (*data)[second_row_group_page_offset + second_row_group_page_header_size + 2] = '\xff';
    }));
    config::external_table_clear_corrupted_cache_data = false;
    auto res = _scan<HdfsParquetScanner>(file);
    ASSERT_TRUE(res.status().is_invalid_argument()) << res.status();

    config::external_table_clear_corrupted_cache_data = true;
    const int64_t retried_files = _retried_files();
    ASSERT_SCAN_MATCHES(HdfsParquetScanner, file);
    ASSERT_FALSE(_file_cached(file));
    ASSERT_EQ(retried_files + 1, _retried_files());
}

// A file corrupted in the remote storage after empty chunks: reading it again must not stop at an empty chunk and
// take the cached copy as corrupted.
TEST_F(HdfsScannerCorruptedDatacacheTest, TestKeepCacheOfFileCorruptedAfterEmptyChunks) {
    std::ifstream in(multi_row_groups_parquet_file, std::ios::binary);
    std::string data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    _corrupt_second_row_group(&data);
    const std::string corrupted_file =
            "./be/test/exec/test_data/parquet_scanner/corrupted_remote_multi_row_groups.parquet";
    std::ofstream(corrupted_file, std::ios::binary) << data;
    DeferOp defer([&]() { std::filesystem::remove(corrupted_file); });
    TestFile file{.path = corrupted_file,
                  .descs = multi_row_groups_parquet_descs,
                  .size = static_cast<int64_t>(data.size()),
                  .cache_key = CacheInputStream::calc_cache_key(corrupted_file, data.size(), 0),
                  .setup = [this](HdfsScannerContext* ctx) { _filter_first_row_group(ctx); }};
    // The cached copy is the same as the remote file.
    ASSERT_NO_FATAL_FAILURE(_write_cached_file(file, data));

    const int64_t removed_files = _removed_files();
    const int64_t retried_files = _retried_files();
    auto res = _scan<HdfsParquetScanner>(file);
    ASSERT_TRUE(res.status().is_corruption() || res.status().is_internal_error()) << res.status();
    ASSERT_EQ(retried_files + 1, _retried_files());
    ASSERT_EQ(removed_files, _removed_files());
    ASSERT_TRUE(_file_cached(file));
}

// The file is deleted before it is read again bypassing the datacache: the error of reading it is returned instead of
// the corruption, e.g. a 404 lets the FE refresh the table and retry.
TEST_F(HdfsScannerCorruptedDatacacheTest, TestReturnErrorOfDeletedFile) {
    const std::string deleted_file = "./be/test/exec/test_data/parquet_scanner/deleted_remote_file.parquet";
    std::filesystem::copy_file(multi_row_groups_parquet_file, deleted_file,
                               std::filesystem::copy_options::overwrite_existing);
    DeferOp defer([&]() { std::filesystem::remove(deleted_file); });
    TestFile file;
    file.setup = [this](HdfsScannerContext* ctx) { _filter_first_row_group(ctx); };
    ASSERT_NO_FATAL_FAILURE(_prepare<HdfsParquetScanner>(deleted_file, multi_row_groups_parquet_descs, 976, &file));
    ASSERT_SCAN_MATCHES(HdfsParquetScanner, file);
    ASSERT_NO_FATAL_FAILURE(_corrupt_cached_file(file, _corrupt_second_row_group));

    const int64_t removed_files = _removed_files();
    const int64_t retried_files = _retried_files();
    auto res = _scan<HdfsParquetScanner>(file, true, [&]() { std::filesystem::remove(deleted_file); });
    ASSERT_TRUE(res.status().is_not_found()) << res.status();
    ASSERT_EQ(retried_files + 1, _retried_files());
    ASSERT_EQ(removed_files, _removed_files());
    ASSERT_TRUE(_file_cached(file));
}

// ORC cannot be reopened: the corrupted copy is dropped but the error is returned.
TEST_F(HdfsScannerCorruptedDatacacheTest, TestOrcDropCorruptedCacheWithoutRetry) {
    TestFile file;
    ASSERT_NO_FATAL_FAILURE(
            _prepare<HdfsOrcScanner>(corrupted_datacache_orc_file, corrupted_datacache_orc_descs, 100, &file));
    ASSERT_SCAN_MATCHES(HdfsOrcScanner, file);
    ASSERT_TRUE(_file_cached(file));

    // Corrupt the postscript at the file tail.
    ASSERT_NO_FATAL_FAILURE(
            _corrupt_cached_file(file, [](std::string* data) { data->replace(data->size() - 4, 4, "XXXX"); }));

    config::external_table_clear_corrupted_cache_data = false;
    ASSERT_FALSE(_scan<HdfsOrcScanner>(file).ok());
    ASSERT_TRUE(_file_cached(file));

    config::external_table_clear_corrupted_cache_data = true;
    const int64_t removed_files = _removed_files();
    const int64_t retried_files = _retried_files();
    auto res = _scan<HdfsOrcScanner>(file);
    ASSERT_TRUE(res.status().is_corruption() || res.status().is_internal_error()) << res.status();
    ASSERT_FALSE(_file_cached(file));
    ASSERT_EQ(removed_files + 1, _removed_files());
    ASSERT_EQ(retried_files, _retried_files());

    // The next read recovers from the remote storage.
    ASSERT_SCAN_MATCHES(HdfsOrcScanner, file);
    ASSERT_TRUE(_file_cached(file));
}
#undef ASSERT_SCAN_MATCHES
#endif

} // namespace starrocks
