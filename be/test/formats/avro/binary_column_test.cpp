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

#include "formats/avro/binary_column.h"

#include <gtest/gtest.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <utility>

#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "runtime/types.h"
#include "util/defer_op.h"

#ifdef __cplusplus
extern "C" {
#endif
#include "avro.h"
#ifdef __cplusplus
}
#endif

namespace starrocks {

class AvroAddBinaryColumnTest : public ::testing::Test {};

struct AvroHelper {
    avro_schema_t schema = NULL;
    avro_value_iface_t* iface = NULL;
    avro_value_t avro_val;
    std::string schema_text;
};

static void init_avro_value(std::string schema_path, AvroHelper& avro_helper) {
    std::ifstream infile_schema;
    infile_schema.open(schema_path);
    std::stringstream ss;
    ss << infile_schema.rdbuf();
    std::string schema_str(ss.str());
    avro_schema_error_t error;
    int result = avro_schema_from_json(schema_str.c_str(), schema_str.size(), &avro_helper.schema, &error);
    if (result != 0) {
        std::cout << "parse schema from json error: " << avro_strerror() << std::endl;
    }
    EXPECT_EQ(0, result);
    avro_helper.iface = avro_generic_class_from_schema(avro_helper.schema);
    avro_generic_value_new(avro_helper.iface, &avro_helper.avro_val);
}

TEST_F(AvroAddBinaryColumnTest, test_add_string) {
    auto column = BinaryColumn::create();
    TypeDescriptor t = TypeDescriptor::create_varchar_type(20);

    std::string schema_path = "./be/test/formats/test_data/avro/single_string_schema.json";
    AvroHelper avro_helper;
    init_avro_value(schema_path, avro_helper);
    DeferOp avro_helper_deleter([&] {
        avro_schema_decref(avro_helper.schema);
        avro_value_iface_decref(avro_helper.iface);
        avro_value_decref(&avro_helper.avro_val);
    });

    avro_value_set_string(&avro_helper.avro_val, "3.14");

    auto st = add_binary_column(column.get(), t, "f_string", avro_helper.avro_val);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("['3.14']", column->debug_string());
}

TEST_F(AvroAddBinaryColumnTest, test_add_enum) {
    auto column = BinaryColumn::create();
    TypeDescriptor t = TypeDescriptor::create_varchar_type(20);

    std::string schema_path = "./be/test/formats/test_data/avro/single_enum_schema.json";
    AvroHelper avro_helper;
    init_avro_value(schema_path, avro_helper);
    DeferOp avro_helper_deleter([&] {
        avro_schema_decref(avro_helper.schema);
        avro_value_iface_decref(avro_helper.iface);
        avro_value_decref(&avro_helper.avro_val);
    });

    avro_value_set_enum(&avro_helper.avro_val, 2);

    auto st = add_binary_column(column.get(), t, "f_enum", avro_helper.avro_val);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("['DIAMONDS']", column->debug_string());
}

TEST_F(AvroAddBinaryColumnTest, test_add_number) {
    auto column = BinaryColumn::create();
    TypeDescriptor t = TypeDescriptor::create_varchar_type(20);
    std::string schema_path = "./be/test/formats/test_data/avro/single_float_schema.json";
    AvroHelper avro_helper;
    init_avro_value(schema_path, avro_helper);
    DeferOp avro_helper_deleter([&] {
        avro_schema_decref(avro_helper.schema);
        avro_value_iface_decref(avro_helper.iface);
        avro_value_decref(&avro_helper.avro_val);
    });

    avro_value_set_float(&avro_helper.avro_val, 3.14);

    auto st = add_binary_column(column.get(), t, "f_float", avro_helper.avro_val);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("['3.140000']", column->debug_string());
}

TEST_F(AvroAddBinaryColumnTest, test_add_boolean) {
    auto column = BinaryColumn::create();
    TypeDescriptor t = TypeDescriptor::create_varchar_type(20);

    std::string schema_path = "./be/test/formats/test_data/avro/single_boolean_schema.json";
    AvroHelper avro_helper;
    init_avro_value(schema_path, avro_helper);
    DeferOp avro_helper_deleter([&] {
        avro_schema_decref(avro_helper.schema);
        avro_value_iface_decref(avro_helper.iface);
        avro_value_decref(&avro_helper.avro_val);
    });

    avro_value_set_boolean(&avro_helper.avro_val, 1);

    auto st = add_binary_column(column.get(), t, "f_boolean", avro_helper.avro_val);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("['1']", column->debug_string());
}

TEST_F(AvroAddBinaryColumnTest, test_add_object) {
    auto column = BinaryColumn::create();
    TypeDescriptor t = TypeDescriptor::create_varchar_type(200);

    std::string schema_path = "./be/test/formats/test_data/avro/single_record_schema.json";
    AvroHelper avro_helper;
    init_avro_value(schema_path, avro_helper);
    DeferOp avro_helper_deleter([&] {
        avro_schema_decref(avro_helper.schema);
        avro_value_iface_decref(avro_helper.iface);
        avro_value_decref(&avro_helper.avro_val);
    });

    avro_value_t boolean_value;
    if (avro_value_get_by_name(&avro_helper.avro_val, "boolean_type", &boolean_value, NULL) == 0) {
        avro_value_set_boolean(&boolean_value, true);
    }

    avro_value_t long_value;
    if (avro_value_get_by_name(&avro_helper.avro_val, "long_type", &long_value, NULL) == 0) {
        avro_value_set_long(&long_value, 4294967296);
    }

    avro_value_t double_value;
    if (avro_value_get_by_name(&avro_helper.avro_val, "double_type", &double_value, NULL) == 0) {
        avro_value_set_double(&double_value, 1.234567);
    }

    auto st = add_binary_column(column.get(), t, "f_object", avro_helper.avro_val);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(R"(['{"boolean_type": true, "long_type": 4294967296, "double_type": 1.234567}'])",
              column->debug_string());
}

TEST_F(AvroAddBinaryColumnTest, test_add_array) {
    auto column = BinaryColumn::create();
    TypeDescriptor t = TypeDescriptor::create_varchar_type(200);

    std::string schema_path = "./be/test/formats/test_data/avro/single_array_schema.json";
    AvroHelper avro_helper;
    init_avro_value(schema_path, avro_helper);
    DeferOp avro_helper_deleter([&] {
        avro_schema_decref(avro_helper.schema);
        avro_value_iface_decref(avro_helper.iface);
        avro_value_decref(&avro_helper.avro_val);
    });

    avro_value_t ele1;
    avro_value_append(&avro_helper.avro_val, &ele1, NULL);
    avro_value_set_long(&ele1, 4294967297);

    avro_value_t ele2;
    avro_value_append(&avro_helper.avro_val, &ele2, NULL);
    avro_value_set_long(&ele2, 4294967298);

    auto st = add_binary_column(column.get(), t, "f_array", avro_helper.avro_val);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(R"(['[4294967297, 4294967298]'])", column->debug_string());
}

TEST_F(AvroAddBinaryColumnTest, test_add_map) {
    auto column = BinaryColumn::create();
    TypeDescriptor t = TypeDescriptor::create_varchar_type(200);

    std::string schema_path = "./be/test/formats/test_data/avro/single_map_schema.json";
    AvroHelper avro_helper;
    init_avro_value(schema_path, avro_helper);
    DeferOp avro_helper_deleter([&] {
        avro_schema_decref(avro_helper.schema);
        avro_value_iface_decref(avro_helper.iface);
        avro_value_decref(&avro_helper.avro_val);
    });

    avro_value_t ele1;
    avro_value_add(&avro_helper.avro_val, "ele1", &ele1, NULL, NULL);
    avro_value_set_long(&ele1, 4294967297);

    avro_value_t ele2;
    avro_value_add(&avro_helper.avro_val, "ele2", &ele2, NULL, NULL);
    avro_value_set_long(&ele2, 4294967298);

    auto st = add_binary_column(column.get(), t, "f_map", avro_helper.avro_val);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(R"(['{"ele1": 4294967297, "ele2": 4294967298}'])", column->debug_string());
}

} // namespace starrocks
