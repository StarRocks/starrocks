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

#include "formats/avro/cpp/avro_reader.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "column/adaptive_nullable_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/config_scan_io_fwd.h"
#include "exec/file_scanner/file_scanner.h"
#include "fs/fs.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class AvroReaderTest : public ::testing::Test {
public:
    void SetUp() override {
        std::string starrocks_home = getenv("STARROCKS_HOME");
        _test_exec_dir = starrocks_home + "/be/test/formats/test_data/avro/cpp/";
        _counter = _obj_pool.add(new ScannerCounter());
        _state = create_runtime_state();
        _timezone = cctz::utc_time_zone();
    }

    ChunkPtr create_src_chunk(const std::vector<SlotDescriptor*>& slot_descs) {
        auto chunk = std::make_shared<Chunk>();
        for (auto* slot_desc : slot_descs) {
            auto column = ColumnHelper::create_column(slot_desc->type(), true, false, 0, true);
            chunk->append_column(std::move(column), slot_desc->id());
        }
        return chunk;
    }

    void materialize_src_chunk_adaptive_nullable_column(ChunkPtr& chunk) {
        chunk->materialized_nullable();
        for (int i = 0; i < chunk->num_columns(); i++) {
            auto* adaptive_column = down_cast<AdaptiveNullableColumn*>(chunk->get_column_raw_ptr_by_index(i));
            chunk->update_column_by_index(NullableColumn::create(adaptive_column->materialized_raw_data_column(),
                                                                 adaptive_column->materialized_raw_null_column()),
                                          i);
        }
    }

    void create_column_readers(const std::vector<SlotDescriptor*>& slot_descs, const cctz::time_zone& timezone,
                               bool invalid_as_null) {
        _column_readers.clear();
        for (auto* slot_desc : slot_descs) {
            _column_readers.emplace_back(avrocpp::ColumnReader::get_nullable_column_reader(
                    slot_desc->col_name(), slot_desc->type(), timezone, invalid_as_null));
        }
    }

    AvroReaderUniquePtr create_avro_reader(const std::string& filename) {
        auto file_or = FileSystem::Default()->new_random_access_file(_test_exec_dir + filename);
        CHECK_OK(file_or.status());

        auto avro_reader = std::make_unique<AvroReader>();
        CHECK_OK(avro_reader->init(std::make_unique<AvroBufferInputStream>(
                                           std::move(file_or.value()), config::avro_reader_buffer_size_bytes, _counter),
                                   filename, _state.get(), _counter, nullptr, &_column_readers, true));
        return avro_reader;
    }

    // Helper: open an AvroReader with explicit split_offset / split_length on a file that
    // has columns {id INT, name VARCHAR}.  Returns both the reader and the slot descriptors
    // (so callers can create chunks).
    // out_descs    : populated with SlotDescriptor values (owns the storage).
    // out_slot_ptrs: populated with pointers into out_descs (kept alive by caller).
    AvroReaderUniquePtr create_split_reader(const std::string& filename, int64_t split_offset, int64_t split_length,
                                            std::vector<SlotDescriptor>* out_descs,
                                            std::vector<SlotDescriptor*>* out_slot_ptrs) {
        // Use a schema-reader first to get slot descriptors.
        auto schema_reader = create_avro_reader(filename);
        EXPECT_OK(schema_reader->get_schema(out_descs));

        out_slot_ptrs->clear();
        for (auto& sd : *out_descs) out_slot_ptrs->push_back(&sd);

        create_column_readers(*out_slot_ptrs, _timezone, /*invalid_as_null=*/false);

        // Open a fresh RandomAccessFile for the stream and one for raw_file (count path).
        auto stream_file_or = FileSystem::Default()->new_random_access_file(_test_exec_dir + filename);
        CHECK_OK(stream_file_or.status());
        auto raw_file_or = FileSystem::Default()->new_random_access_file(_test_exec_dir + filename);
        CHECK_OK(raw_file_or.status());

        _raw_file = std::move(raw_file_or.value());

        auto reader = std::make_unique<AvroReader>();
        CHECK_OK(reader->init(std::make_unique<AvroBufferInputStream>(std::move(stream_file_or.value()),
                                                                      config::avro_reader_buffer_size_bytes, _counter),
                              filename, _state.get(), _counter, out_slot_ptrs, &_column_readers,
                              /*col_not_found_as_null=*/false, _raw_file.get(), config::avro_reader_buffer_size_bytes,
                              split_offset, split_length));
        return reader;
    }

    // Drain all rows from a reader into a vector of (id, name) pairs.
    // Uses create_src_chunk() internally.
    std::vector<std::pair<int, std::string>> drain_rows(AvroReader& reader,
                                                        const std::vector<SlotDescriptor*>& slot_descs) {
        std::vector<std::pair<int, std::string>> result;
        while (true) {
            auto chunk = create_src_chunk(slot_descs);
            auto st = reader.read_chunk(chunk, 32);
            materialize_src_chunk_adaptive_nullable_column(chunk);
            for (int r = 0; r < static_cast<int>(chunk->num_rows()); ++r) {
                // col 0 = id (INT), col 1 = name (VARCHAR)
                auto id = chunk->get_column_by_index(0)->get(r).get_int32();
                auto name = chunk->get_column_by_index(1)->get(r).get_slice().to_string();
                result.emplace_back(id, name);
            }
            if (st.is_end_of_file()) break;
            EXPECT_OK(st);
        }
        return result;
    }

private:
    std::shared_ptr<RuntimeState> create_runtime_state() {
        TQueryOptions query_options;
        TUniqueId fragment_id;
        TQueryGlobals query_globals;
        std::shared_ptr<RuntimeState> state =
                std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, ExecEnv::GetInstance());
        TUniqueId id;
        state->init_mem_trackers(id);
        return state;
    }

    std::string _test_exec_dir;
    ObjectPool _obj_pool;
    ScannerCounter* _counter;
    std::shared_ptr<RuntimeState> _state;
    cctz::time_zone _timezone;
    std::vector<avrocpp::ColumnReaderUniquePtr> _column_readers;
    // Kept alive for the duration of a test that exercises the split / count path.
    std::shared_ptr<RandomAccessFile> _raw_file;
};

TEST_F(AvroReaderTest, test_get_schema_primitive_types) {
    std::string filename = "primitive.avro";
    auto reader = create_avro_reader(filename);

    std::vector<SlotDescriptor> schema;
    ASSERT_OK(reader->get_schema(&schema));

    ASSERT_EQ(8, schema.size());
    ASSERT_EQ("null_field", schema[0].col_name());
    ASSERT_EQ("VARCHAR(1048576)", schema[0].type().debug_string());
    ASSERT_EQ("bool_field", schema[1].col_name());
    ASSERT_EQ("BOOLEAN", schema[1].type().debug_string());
    ASSERT_EQ("int_field", schema[2].col_name());
    ASSERT_EQ("INT", schema[2].type().debug_string());
    ASSERT_EQ("long_field", schema[3].col_name());
    ASSERT_EQ("BIGINT", schema[3].type().debug_string());
    ASSERT_EQ("float_field", schema[4].col_name());
    ASSERT_EQ("FLOAT", schema[4].type().debug_string());
    ASSERT_EQ("double_field", schema[5].col_name());
    ASSERT_EQ("DOUBLE", schema[5].type().debug_string());
    ASSERT_EQ("bytes_field", schema[6].col_name());
    ASSERT_EQ("VARBINARY(1048576)", schema[6].type().debug_string());
    ASSERT_EQ("string_field", schema[7].col_name());
    ASSERT_EQ("VARCHAR(1048576)", schema[7].type().debug_string());
}

TEST_F(AvroReaderTest, test_get_schema_complex_types) {
    std::string filename = "complex.avro";
    auto reader = create_avro_reader(filename);

    std::vector<SlotDescriptor> schema;
    ASSERT_OK(reader->get_schema(&schema));

    ASSERT_EQ(6, schema.size());
    ASSERT_EQ("record_field", schema[0].col_name());
    ASSERT_EQ("STRUCT{id INT, name VARCHAR(1048576)}", schema[0].type().debug_string());
    ASSERT_EQ("enum_field", schema[1].col_name());
    ASSERT_EQ("VARCHAR(1048576)", schema[1].type().debug_string());
    ASSERT_EQ("array_field", schema[2].col_name());
    ASSERT_EQ("ARRAY<VARCHAR(1048576)>", schema[2].type().debug_string());
    ASSERT_EQ("map_field", schema[3].col_name());
    ASSERT_EQ("MAP<VARCHAR(1048576), INT>", schema[3].type().debug_string());
    ASSERT_EQ("union_field", schema[4].col_name());
    ASSERT_EQ("BIGINT", schema[4].type().debug_string());
    ASSERT_EQ("fixed_field", schema[5].col_name());
    ASSERT_EQ("VARBINARY(16)", schema[5].type().debug_string());
}

TEST_F(AvroReaderTest, test_get_schema_complex_nest_types) {
    std::string filename = "complex_nest.avro";
    auto reader = create_avro_reader(filename);

    std::vector<SlotDescriptor> schema;
    ASSERT_OK(reader->get_schema(&schema));

    ASSERT_EQ(13, schema.size());
    ASSERT_EQ("record_of_record", schema[0].col_name());
    ASSERT_EQ("STRUCT{inner STRUCT{val INT}}", schema[0].type().debug_string());
    ASSERT_EQ("record_of_array", schema[1].col_name());
    ASSERT_EQ("STRUCT{list ARRAY<INT>}", schema[1].type().debug_string());
    ASSERT_EQ("record_of_map", schema[2].col_name());
    ASSERT_EQ("STRUCT{dict MAP<VARCHAR(1048576), VARCHAR(1048576)>}", schema[2].type().debug_string());
    ASSERT_EQ("array_of_record", schema[3].col_name());
    ASSERT_EQ("ARRAY<STRUCT{value VARCHAR(1048576)}>", schema[3].type().debug_string());
    ASSERT_EQ("array_of_array", schema[4].col_name());
    ASSERT_EQ("ARRAY<ARRAY<INT>>", schema[4].type().debug_string());
    ASSERT_EQ("array_of_map", schema[5].col_name());
    ASSERT_EQ("ARRAY<MAP<VARCHAR(1048576), INT>>", schema[5].type().debug_string());
    ASSERT_EQ("map_of_record", schema[6].col_name());
    ASSERT_EQ("MAP<VARCHAR(1048576), STRUCT{id INT}>", schema[6].type().debug_string());
    ASSERT_EQ("map_of_array", schema[7].col_name());
    ASSERT_EQ("MAP<VARCHAR(1048576), ARRAY<INT>>", schema[7].type().debug_string());
    ASSERT_EQ("map_of_map", schema[8].col_name());
    ASSERT_EQ("MAP<VARCHAR(1048576), MAP<VARCHAR(1048576), VARCHAR(1048576)>>", schema[8].type().debug_string());
    ASSERT_EQ("record_array_of_record", schema[9].col_name());
    ASSERT_EQ("STRUCT{entries ARRAY<STRUCT{flag BOOLEAN}>}", schema[9].type().debug_string());
    ASSERT_EQ("record_array_of_map", schema[10].col_name());
    ASSERT_EQ("STRUCT{entries ARRAY<MAP<VARCHAR(1048576), VARCHAR(1048576)>>}", schema[10].type().debug_string());
    ASSERT_EQ("array_map_record", schema[11].col_name());
    ASSERT_EQ("ARRAY<MAP<VARCHAR(1048576), STRUCT{ok BOOLEAN}>>", schema[11].type().debug_string());
    ASSERT_EQ("map_array_record", schema[12].col_name());
    ASSERT_EQ("MAP<VARCHAR(1048576), ARRAY<STRUCT{score FLOAT}>>", schema[12].type().debug_string());
}

TEST_F(AvroReaderTest, test_get_schema_logical_types) {
    std::string filename = "logical.avro";
    auto reader = create_avro_reader(filename);

    std::vector<SlotDescriptor> schema;
    ASSERT_OK(reader->get_schema(&schema));

    ASSERT_EQ(11, schema.size());
    ASSERT_EQ("decimal_bytes", schema[0].col_name());
    ASSERT_EQ("DECIMAL64(10, 2)", schema[0].type().debug_string());
    ASSERT_EQ("decimal_fixed", schema[1].col_name());
    ASSERT_EQ("DECIMAL64(10, 2)", schema[1].type().debug_string());
    ASSERT_EQ("uuid_string", schema[2].col_name());
    ASSERT_EQ("VARCHAR(1048576)", schema[2].type().debug_string());
    ASSERT_EQ("date", schema[3].col_name());
    ASSERT_EQ("DATE", schema[3].type().debug_string());
    ASSERT_EQ("time_millis", schema[4].col_name());
    ASSERT_EQ("INT", schema[4].type().debug_string());
    ASSERT_EQ("time_micros", schema[5].col_name());
    ASSERT_EQ("BIGINT", schema[5].type().debug_string());
    ASSERT_EQ("timestamp_millis", schema[6].col_name());
    ASSERT_EQ("DATETIME", schema[6].type().debug_string());
    ASSERT_EQ("timestamp_micros", schema[7].col_name());
    ASSERT_EQ("DATETIME", schema[7].type().debug_string());
    ASSERT_EQ("local_timestamp_millis", schema[8].col_name());
    ASSERT_EQ("BIGINT", schema[8].type().debug_string());
    ASSERT_EQ("local_timestamp_micros", schema[9].col_name());
    ASSERT_EQ("BIGINT", schema[9].type().debug_string());
    ASSERT_EQ("duration", schema[10].col_name());
    ASSERT_EQ("VARBINARY(12)", schema[10].type().debug_string());
}

TEST_F(AvroReaderTest, test_read_primitive_types) {
    std::string filename = "primitive.avro";
    std::vector<SlotDescriptor*> slot_descs;
    bool column_not_found_as_null = false;
    int rows_to_read = 2;

    // init reader for read schema
    auto reader = create_avro_reader(filename);

    std::vector<SlotDescriptor> tmp_slot_descs;
    ASSERT_OK(reader->get_schema(&tmp_slot_descs));

    for (auto& slot_desc : tmp_slot_descs) {
        slot_descs.emplace_back(&slot_desc);
    }

    // create column readers
    create_column_readers(slot_descs, _timezone, false);

    // init reader for read data
    // some fields such as _field_indexes does not inited normally, so init reader again.
    reader->TEST_init(&slot_descs, &_column_readers, column_not_found_as_null);

    // create chunk
    auto chunk = create_src_chunk(slot_descs);

    // read data
    ASSERT_OK(reader->read_chunk(chunk, rows_to_read));
    materialize_src_chunk_adaptive_nullable_column(chunk);
    ASSERT_EQ(1, chunk->num_rows());
    ASSERT_EQ("[NULL, 1, 123, 1234567890123, 3.14, 2.71828, 'abc', 'hello avro']", chunk->debug_row(0));

    chunk = create_src_chunk(slot_descs);
    auto st = reader->read_chunk(chunk, rows_to_read);
    ASSERT_TRUE(st.is_end_of_file());
}

TEST_F(AvroReaderTest, test_read_complex_types) {
    std::string filename = "complex.avro";
    std::vector<SlotDescriptor*> slot_descs;
    bool column_not_found_as_null = false;
    int rows_to_read = 2;

    // init reader for read schema
    auto reader = create_avro_reader(filename);

    std::vector<SlotDescriptor> tmp_slot_descs;
    ASSERT_OK(reader->get_schema(&tmp_slot_descs));

    for (auto& slot_desc : tmp_slot_descs) {
        slot_descs.emplace_back(&slot_desc);
    }

    // create column readers
    create_column_readers(slot_descs, _timezone, false);

    // init reader for read data
    // some fields such as _field_indexes does not inited normally, so init reader again.
    reader->TEST_init(&slot_descs, &_column_readers, column_not_found_as_null);

    // create chunk
    auto chunk = create_src_chunk(slot_descs);

    // read data
    ASSERT_OK(reader->read_chunk(chunk, rows_to_read));
    materialize_src_chunk_adaptive_nullable_column(chunk);
    ASSERT_EQ(1, chunk->num_rows());
    ASSERT_EQ("[{id:1,name:'avro'}, 'HEARTS', ['one','two','three'], {'a':1,'b':2}, 100, 'abababababababab']",
              chunk->debug_row(0));

    chunk = create_src_chunk(slot_descs);
    auto st = reader->read_chunk(chunk, rows_to_read);
    ASSERT_TRUE(st.is_end_of_file());
}

TEST_F(AvroReaderTest, test_read_complex_types_as_varchar) {
    std::string filename = "complex.avro";
    std::vector<SlotDescriptor*> slot_descs;
    bool column_not_found_as_null = false;
    int rows_to_read = 2;

    // init reader for read schema
    auto reader = create_avro_reader(filename);

    std::vector<SlotDescriptor> tmp_slot_descs;
    ASSERT_OK(reader->get_schema(&tmp_slot_descs));

    // read as varchar type
    for (auto& slot_desc : tmp_slot_descs) {
        slot_descs.emplace_back(_obj_pool.add(
                new SlotDescriptor(slot_desc.id(), std::string(slot_desc.col_name()),
                                   TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH))));
    }

    // create column readers
    create_column_readers(slot_descs, _timezone, false);

    // init reader for read data
    // some fields such as _field_indexes does not inited normally, so init reader again.
    reader->TEST_init(&slot_descs, &_column_readers, column_not_found_as_null);

    // create chunk
    auto chunk = create_src_chunk(slot_descs);

    // read data
    ASSERT_OK(reader->read_chunk(chunk, rows_to_read));
    materialize_src_chunk_adaptive_nullable_column(chunk);
    ASSERT_EQ(1, chunk->num_rows());
    ASSERT_EQ(
            "['{\"id\":1,\"name\":\"avro\"}', 'HEARTS', '[\"one\",\"two\",\"three\"]', '{\"a\":1,\"b\":2}', '100', "
            "'abababababababab']",
            chunk->debug_row(0));

    chunk = create_src_chunk(slot_descs);
    auto st = reader->read_chunk(chunk, rows_to_read);
    ASSERT_TRUE(st.is_end_of_file());
}

TEST_F(AvroReaderTest, test_read_complex_nest_types) {
    std::string filename = "complex_nest.avro";
    std::vector<SlotDescriptor*> slot_descs;
    bool column_not_found_as_null = false;
    int rows_to_read = 2;

    // init reader for read schema
    auto reader = create_avro_reader(filename);

    std::vector<SlotDescriptor> tmp_slot_descs;
    ASSERT_OK(reader->get_schema(&tmp_slot_descs));

    for (auto& slot_desc : tmp_slot_descs) {
        slot_descs.emplace_back(&slot_desc);
    }

    // create column readers
    create_column_readers(slot_descs, _timezone, false);

    // init reader for read data
    // some fields such as _field_indexes does not inited normally, so init reader again.
    reader->TEST_init(&slot_descs, &_column_readers, column_not_found_as_null);

    // create chunk
    auto chunk = create_src_chunk(slot_descs);

    // read data
    ASSERT_OK(reader->read_chunk(chunk, rows_to_read));
    materialize_src_chunk_adaptive_nullable_column(chunk);
    ASSERT_EQ(1, chunk->num_rows());
    ASSERT_EQ(
            "[{inner:{val:123}}, {list:[1,2,3]}, {dict:{'a':'x','b':'y'}}, [{value:'one'},{value:'two'}], "
            "[[1,2],[3,4]], [{'a':1,'b':2},{'c':3}], {'r1':{id:10},'r2':{id:20}}, {'nums':[5,6,7]}, "
            "{'outer':{'inner':'val'}}, {entries:[{flag:1},{flag:0}]}, {entries:[{'x':'1'},{'y':'2'}]}, "
            "[{'one':{ok:1}},{'two':{ok:0}}], {'group':[{score:99.9},{score:88.8}]}]",
            chunk->debug_row(0));

    chunk = create_src_chunk(slot_descs);
    auto st = reader->read_chunk(chunk, rows_to_read);
    ASSERT_TRUE(st.is_end_of_file());
}

// Verifies that init() projects the Avro reader schema down to only the requested
// columns, and that the correct values are decoded for those columns.
TEST_F(AvroReaderTest, test_column_projection) {
    std::string filename = "primitive.avro";

    // Get full schema so we can pick individual SlotDescriptors by name.
    auto schema_reader = create_avro_reader(filename);
    std::vector<SlotDescriptor> all_descs;
    ASSERT_OK(schema_reader->get_schema(&all_descs));
    ASSERT_EQ(8, all_descs.size());

    // Request only "int_field" (index 2) and "string_field" (index 7).
    std::vector<SlotDescriptor*> slot_descs = {&all_descs[2], &all_descs[7]};
    ASSERT_EQ("int_field", slot_descs[0]->col_name());
    ASSERT_EQ("string_field", slot_descs[1]->col_name());
    create_column_readers(slot_descs, _timezone, false);

    // Open a fresh reader via the full init() path with projected slot_descs.
    auto file_or = FileSystem::Default()->new_random_access_file(_test_exec_dir + filename);
    ASSERT_OK(file_or.status());
    auto avro_reader = std::make_unique<AvroReader>();
    ASSERT_OK(avro_reader->init(std::make_unique<AvroBufferInputStream>(
                                        std::move(file_or.value()), config::avro_reader_buffer_size_bytes, _counter),
                                filename, _state.get(), _counter, &slot_descs, &_column_readers,
                                /*col_not_found_as_null=*/false));

    auto chunk = create_src_chunk(slot_descs);
    ASSERT_OK(avro_reader->read_chunk(chunk, 2));
    materialize_src_chunk_adaptive_nullable_column(chunk);

    ASSERT_EQ(1, chunk->num_rows());
    ASSERT_EQ(2, chunk->num_columns());
    ASSERT_EQ("[123, 'hello avro']", chunk->debug_row(0));

    chunk = create_src_chunk(slot_descs);
    ASSERT_TRUE(avro_reader->read_chunk(chunk, 2).is_end_of_file());
}

TEST_F(AvroReaderTest, test_read_logical_types) {
    std::string filename = "logical.avro";
    std::vector<SlotDescriptor*> slot_descs;
    bool column_not_found_as_null = false;
    int rows_to_read = 2;

    // init reader for read schema
    auto reader = create_avro_reader(filename);

    std::vector<SlotDescriptor> tmp_slot_descs;
    ASSERT_OK(reader->get_schema(&tmp_slot_descs));

    // add the last duration type column ut later
    for (int i = 0; i < tmp_slot_descs.size() - 1; ++i) {
        slot_descs.emplace_back(&tmp_slot_descs[i]);
    }

    // create column readers
    create_column_readers(slot_descs, _timezone, false);

    // init reader for read data
    // some fields such as _field_indexes does not inited normally, so init reader again.
    reader->TEST_init(&slot_descs, &_column_readers, column_not_found_as_null);

    // create chunk
    auto chunk = create_src_chunk(slot_descs);

    // read data
    ASSERT_OK(reader->read_chunk(chunk, rows_to_read));
    materialize_src_chunk_adaptive_nullable_column(chunk);
    ASSERT_EQ(1, chunk->num_rows());
    ASSERT_EQ(
            "[1234.56, 1234.56, '61ed1775-2ce2-4f88-8352-1da6847512d6', 2025-04-11, 55543806, 55543806481, "
            "2025-04-11 07:25:43.806000, 2025-04-11 07:25:43.806481, 1744356343806, 1744356343806481]",
            chunk->debug_row(0));

    chunk = create_src_chunk(slot_descs);
    auto st = reader->read_chunk(chunk, rows_to_read);
    ASSERT_TRUE(st.is_end_of_file());
}

// ---------------------------------------------------------------------------
// Split / multi-block tests using multiblock.avro
//
// multiblock.avro properties (generated by gen_multiblock_avro.py):
//   - Schema: {id: int, name: string}
//   - 100 records, 10 records per Avro block (10 blocks)
//   - File size: 1292 bytes
//   - Block layout (sync marker = 16 bytes):
//       Header:   sync [160..175]
//       Block  1: data [176..258],  sync [259..274]   → records  0.. 9
//       Block  2: data [275..367],  sync [368..383]   → records 10..19
//       Block  3: data [384..476],  sync [477..492]   → records 20..29
//       Block  4: data [493..585],  sync [586..601]   → records 30..39
//       Block  5: data [602..694],  sync [695..710]   → records 40..49
//       Block  6: data [711..803],  sync [804..819]   → records 50..59
//       Block  7: data [820..918],  sync [919..934]   → records 60..69
//       Block  8: data [935..1037], sync [1038..1053] → records 70..79
//       Block  9: data [1054..1156],sync [1157..1172] → records 80..89
//       Block 10: data [1173..1275],sync [1276..1291] → records 90..99
//
// avrocpp DataFileReader::sync(pos) seeks to the first sync marker whose START
// is >= pos, then positions the reader at the block immediately following that
// sync marker.
// pastSync(split_end) returns true when previousSync() >= split_end + SyncSize(16).
// ---------------------------------------------------------------------------

// Read the whole file as one split (split_offset=0, split_length=file_size).
// All 100 records should come back in order.
TEST_F(AvroReaderTest, test_split_whole_file) {
    const std::string filename = "multiblock.avro";
    // split_offset=0, split_length=1292 (full file)
    std::vector<SlotDescriptor> descs;
    std::vector<SlotDescriptor*> slot_descs;
    auto reader = create_split_reader(filename, 0, 1292, &descs, &slot_descs);

    auto rows = drain_rows(*reader, slot_descs);
    ASSERT_EQ(100, rows.size());
    for (int i = 0; i < 100; ++i) {
        ASSERT_EQ(i, rows[i].first);
        ASSERT_EQ("name_" + std::to_string(i), rows[i].second);
    }
}

// Read only the first split: split_offset=0, split_length covers just the
// first data block (ends at sync pos 259, so length ≤ 259 bytes from start).
// Expect exactly 10 records (records 0..9).
TEST_F(AvroReaderTest, test_split_first_block_only) {
    const std::string filename = "multiblock.avro";
    // First data block ends with sync at offset 259.
    // split_length=259 — pastSync(259) fires after the first block is consumed.
    std::vector<SlotDescriptor> descs;
    std::vector<SlotDescriptor*> slot_descs;
    auto reader = create_split_reader(filename, 0, 259, &descs, &slot_descs);

    auto rows = drain_rows(*reader, slot_descs);
    ASSERT_EQ(10, rows.size());
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(i, rows[i].first);
    }
}

// Read only the second split: split_offset inside block 1's data range.
// sync() finds the sync marker at 259 (end of block 1), then positions at
// block 2 start (275).  With split_end covering block 2 only, expect records 10..19.
TEST_F(AvroReaderTest, test_split_seek_to_second_block) {
    const std::string filename = "multiblock.avro";
    // multiblock.avro block layout:
    //   Block 1: data [176..258], sync [259..274]  → records 0..9
    //   Block 2: data [275..367], sync [368..383]  → records 10..19
    //   Block 3: data [384..476], sync [477..492]  → records 20..29
    //
    // split_offset=200 (inside block 1 data) → sync() scans forward, finds the
    //   16-byte sync marker at [259..274], then calls readDataBlock() which sets
    //   blockStart_ = 275 (the byte position of block 2's count varint).
    //
    // pastSync(split_end) := blockStart_ >= split_end + 16  (avrocpp DataFile.cc)
    // where blockStart_ is updated by readDataBlock() to the start of the NEXT block.
    //
    // After sync(200):        blockStart_ = 275 (block 2 loaded)
    // After block 2 is read:  blockStart_ = 384 (block 3 loaded)
    //
    // To include block 2: pastSync(split_end) must be false when blockStart_=275
    //   → 275 < split_end + 16  (always true for split_end > 0)
    // To exclude block 3: pastSync(split_end) must be true when blockStart_=384
    //   → 384 >= split_end + 16  →  split_end <= 368
    //
    // Use split_end = 368 → split_length = 368 - 200 = 168.
    //   Before block 2: 275 >= 384?  No  → read block 2 ✓
    //   Before block 3: 384 >= 384?  Yes → stop         ✓
    std::vector<SlotDescriptor> descs;
    std::vector<SlotDescriptor*> slot_descs;
    auto reader = create_split_reader(filename, 200, 168, &descs, &slot_descs);
    // split_end = 200 + 168 = 368.
    // block2: pastSync(368)? 275 >= 384? No → read block 2.
    // block3: pastSync(368)? 384 >= 384? Yes → stop. Exactly block 2 → records 10..19.

    auto rows = drain_rows(*reader, slot_descs);
    ASSERT_EQ(10, rows.size());
    // Block 2 contains records 10..19
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(10 + i, rows[i].first);
    }
}

// Read the last split: split_offset inside block 9's data range.
// sync() finds sync at 1157 (end of block 9), positions at block 10 start (1173).
// split_end covers to end of file → reads all of block 10 → records 90..99.
TEST_F(AvroReaderTest, test_split_last_block) {
    const std::string filename = "multiblock.avro";
    // Block 9: data [1054..1156], sync [1157..1172] → records 80..89
    // Block 10: data [1173..1275], sync [1276..1291] → records 90..99
    //
    // split_offset=1100 (inside block 9 data) → sync() finds sync at 1157,
    //   positions at block 10 start (1173).
    // split_length=192 → split_end=1292 (file size); pastSync never fires early.
    std::vector<SlotDescriptor> descs;
    std::vector<SlotDescriptor*> slot_descs;
    auto reader = create_split_reader(filename, 1100, 192, &descs, &slot_descs);

    auto rows = drain_rows(*reader, slot_descs);
    ASSERT_EQ(10, rows.size());
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(90 + i, rows[i].first);
    }
}

// count(*) fast path on the whole file (no columns, raw_file provided).
// Expect 100 records without any record-level decoding.
TEST_F(AvroReaderTest, test_count_avro_blocks_full_file) {
    const std::string filename = "multiblock.avro";
    // No columns → count(*) path.  Pass empty column_readers.
    std::vector<SlotDescriptor*> empty_descs;
    std::vector<avrocpp::ColumnReaderUniquePtr> empty_readers;

    auto stream_file_or = FileSystem::Default()->new_random_access_file(_test_exec_dir + filename);
    ASSERT_OK(stream_file_or.status());
    auto raw_file_or = FileSystem::Default()->new_random_access_file(_test_exec_dir + filename);
    ASSERT_OK(raw_file_or.status());
    _raw_file = std::move(raw_file_or.value());

    auto reader = std::make_unique<AvroReader>();
    ASSERT_OK(reader->init(std::make_unique<AvroBufferInputStream>(std::move(stream_file_or.value()),
                                                                   config::avro_reader_buffer_size_bytes, _counter),
                           filename, _state.get(), _counter, &empty_descs, &empty_readers,
                           /*col_not_found_as_null=*/false, _raw_file.get(), config::avro_reader_buffer_size_bytes,
                           /*split_offset=*/0, /*split_length=*/1292));

    auto chunk = std::make_shared<Chunk>();
    int64_t counted = 0;
    Status st;
    while (true) {
        st = reader->read_chunk(chunk, 1024, &counted);
        if (st.is_end_of_file()) break;
        ASSERT_OK(st);
    }
    // counted is updated each call; total across all calls equals 100
    // (the fast-path drains in one shot since rows_to_read >= total).
    // Re-run from scratch to capture the single-call total:
    {
        auto stream2_or = FileSystem::Default()->new_random_access_file(_test_exec_dir + filename);
        ASSERT_OK(stream2_or.status());
        auto raw2_or = FileSystem::Default()->new_random_access_file(_test_exec_dir + filename);
        ASSERT_OK(raw2_or.status());
        auto raw2 = std::move(raw2_or.value());

        auto reader2 = std::make_unique<AvroReader>();
        ASSERT_OK(reader2->init(std::make_unique<AvroBufferInputStream>(
                                        std::move(stream2_or.value()), config::avro_reader_buffer_size_bytes, _counter),
                                filename, _state.get(), _counter, &empty_descs, &empty_readers,
                                /*col_not_found_as_null=*/false, raw2.get(), config::avro_reader_buffer_size_bytes,
                                /*split_offset=*/0, /*split_length=*/1292));

        auto chunk2 = std::make_shared<Chunk>();
        int64_t batch = 0;
        ASSERT_OK(reader2->read_chunk(chunk2, 1024, &batch));
        ASSERT_EQ(100, batch);

        int64_t batch2 = 0;
        ASSERT_TRUE(reader2->read_chunk(chunk2, 1024, &batch2).is_end_of_file());
        ASSERT_EQ(0, batch2);
    }
}

// count(*) fast path on a split: split covers blocks 1-5 (records 0..49).
TEST_F(AvroReaderTest, test_count_avro_blocks_split) {
    const std::string filename = "multiblock.avro";
    // split_offset=0, split_end = sync pos of block 5 = 695
    // → count_avro_blocks should return 50 (5 blocks × 10 records).
    std::vector<SlotDescriptor*> empty_descs;
    std::vector<avrocpp::ColumnReaderUniquePtr> empty_readers;

    auto stream_file_or = FileSystem::Default()->new_random_access_file(_test_exec_dir + filename);
    ASSERT_OK(stream_file_or.status());
    auto raw_file_or = FileSystem::Default()->new_random_access_file(_test_exec_dir + filename);
    ASSERT_OK(raw_file_or.status());
    _raw_file = std::move(raw_file_or.value());

    auto reader = std::make_unique<AvroReader>();
    ASSERT_OK(reader->init(std::make_unique<AvroBufferInputStream>(std::move(stream_file_or.value()),
                                                                   config::avro_reader_buffer_size_bytes, _counter),
                           filename, _state.get(), _counter, &empty_descs, &empty_readers,
                           /*col_not_found_as_null=*/false, _raw_file.get(), config::avro_reader_buffer_size_bytes,
                           /*split_offset=*/0, /*split_length=*/695));

    auto chunk = std::make_shared<Chunk>();
    int64_t batch = 0;
    ASSERT_OK(reader->read_chunk(chunk, 1024, &batch));
    ASSERT_EQ(50, batch);
}

} // namespace starrocks
