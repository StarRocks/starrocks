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
#include "exec/file_scanner.h"
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
    }

    AvroReaderUniquePtr create_avro_reader(const std::string& filename) {
        auto file_or = FileSystem::Default()->new_random_access_file(_test_exec_dir + filename);
        CHECK_OK(file_or.status());

        auto avro_reader = std::make_unique<AvroReader>();
        auto st = avro_reader->init(
                std::make_unique<AvroBufferInputStream>(std::move(file_or.value()), 1048576, _counter));
        CHECK_OK(st);
        return avro_reader;
    }

private:
    std::string _test_exec_dir;
    ObjectPool _obj_pool;
    ScannerCounter* _counter;
    std::shared_ptr<RuntimeState> _state;
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

} // namespace starrocks
