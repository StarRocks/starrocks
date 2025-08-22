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

#include "column/adaptive_nullable_column.h"
#include "column/column_helper.h"
#include "exec/file_scanner/file_scanner.h"
#include "fs/fs.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

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
            AdaptiveNullableColumn* adaptive_column =
                    down_cast<AdaptiveNullableColumn*>(chunk->get_column_by_index(i).get());
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
                new SlotDescriptor(slot_desc.id(), slot_desc.col_name(),
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

} // namespace starrocks
