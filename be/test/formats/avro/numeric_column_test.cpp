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

#include "formats/avro/numeric_column.h"

#include <gtest/gtest.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <utility>

#include "column/fixed_length_column.h"
#include "exec/avro_test.h"
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

class AvroAddNumericColumnTest : public ::testing::Test {};

<<<<<<< HEAD
struct AvroHelper {
    avro_schema_t schema = NULL;
    avro_value_iface_t* iface = NULL;
    avro_value_t avro_val;
};

=======
>>>>>>> c898f4735c ([UT] Refractor Avro scanner UT (#38972))
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

TEST_F(AvroAddNumericColumnTest, test_add_number) {
    auto column = FixedLengthColumn<float>::create();
    TypeDescriptor t(TYPE_FLOAT);
    std::string schema_path = "./be/test/formats/test_data/avro/single_float_schema.json";
    AvroHelper avro_helper;
    init_avro_value(schema_path, avro_helper);
    DeferOp avro_helper_deleter([&] {
        avro_schema_decref(avro_helper.schema);
        avro_value_iface_decref(avro_helper.iface);
        avro_value_decref(&avro_helper.avro_val);
    });

    avro_value_set_float(&avro_helper.avro_val, 3.14);

    auto st = add_numeric_column<float>(column.get(), t, "f_float", avro_helper.avro_val);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("[3.14]", column->debug_string());
}

TEST_F(AvroAddNumericColumnTest, test_add_string) {
    auto column = FixedLengthColumn<float>::create();
    TypeDescriptor t(TYPE_FLOAT);
    std::string schema_path = "./be/test/formats/test_data/avro/single_string_schema.json";
    AvroHelper avro_helper;
    init_avro_value(schema_path, avro_helper);
    DeferOp avro_helper_deleter([&] {
        avro_schema_decref(avro_helper.schema);
        avro_value_iface_decref(avro_helper.iface);
        avro_value_decref(&avro_helper.avro_val);
    });

    avro_value_set_string(&avro_helper.avro_val, "3.14");

    auto st = add_numeric_column<float>(column.get(), t, "f_string", avro_helper.avro_val);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("[3.14]", column->debug_string());
}

TEST_F(AvroAddNumericColumnTest, test_add_boolean) {
    auto column = FixedLengthColumn<int8_t>::create();
    TypeDescriptor t(TYPE_BOOLEAN);
    std::string schema_path = "./be/test/formats/test_data/avro/single_boolean_schema.json";
    AvroHelper avro_helper;
    init_avro_value(schema_path, avro_helper);
    DeferOp avro_helper_deleter([&] {
        avro_schema_decref(avro_helper.schema);
        avro_value_iface_decref(avro_helper.iface);
        avro_value_decref(&avro_helper.avro_val);
    });

    avro_value_set_boolean(&avro_helper.avro_val, 1);

    auto st = add_numeric_column<int8_t>(column.get(), t, "f_boolean", avro_helper.avro_val);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("[1]", column->debug_string());
}

TEST_F(AvroAddNumericColumnTest, test_add_invalid) {
    auto column = FixedLengthColumn<float>::create();
    TypeDescriptor t(TYPE_FLOAT);
    std::string schema_path = "./be/test/exec/test_data/avro_scanner/avro_nest_schema.json";
    AvroHelper avro_helper;
    init_avro_value(schema_path, avro_helper);
    DeferOp avro_helper_deleter([&] {
        avro_schema_decref(avro_helper.schema);
        avro_value_iface_decref(avro_helper.iface);
        avro_value_decref(&avro_helper.avro_val);
    });

    avro_value_t boolean_value;
    if (avro_value_get_by_name(&avro_helper.avro_val, "booleantype", &boolean_value, NULL) == 0) {
        avro_value_set_boolean(&boolean_value, true);
    }

    avro_value_t long_value;
    if (avro_value_get_by_name(&avro_helper.avro_val, "longtype", &long_value, NULL) == 0) {
        avro_value_set_long(&long_value, 4294967296);
    }

    avro_value_t double_value;
    if (avro_value_get_by_name(&avro_helper.avro_val, "doubletype", &double_value, NULL) == 0) {
        avro_value_set_double(&double_value, 1.234567);
    }

    auto st = add_numeric_column<float>(column.get(), t, "fobject", avro_helper.avro_val);
    ASSERT_TRUE(st.is_invalid_argument());
}

TEST_F(AvroAddNumericColumnTest, test_add_int_overflow) {
    auto column = FixedLengthColumn<int32_t>::create();
    TypeDescriptor t(TYPE_INT);
    std::string schema_path = "./be/test/formats/test_data/avro/single_long_schema.json";
    AvroHelper avro_helper;
    init_avro_value(schema_path, avro_helper);
    DeferOp avro_helper_deleter([&] {
        avro_schema_decref(avro_helper.schema);
        avro_value_iface_decref(avro_helper.iface);
        avro_value_decref(&avro_helper.avro_val);
    });

    avro_value_set_long(&avro_helper.avro_val, 2147483648);

    auto st = add_numeric_column<int32_t>(column.get(), t, "f_bigint", avro_helper.avro_val);
    ASSERT_TRUE(st.is_invalid_argument());
}

TEST_F(AvroAddNumericColumnTest, test_add_int64_lowerbound) {
    auto column = FixedLengthColumn<int64_t>::create();
    TypeDescriptor t(TYPE_BIGINT);
    std::string schema_path = "./be/test/formats/test_data/avro/single_long_schema.json";
    AvroHelper avro_helper;
    init_avro_value(schema_path, avro_helper);
    DeferOp avro_helper_deleter([&] {
        avro_schema_decref(avro_helper.schema);
        avro_value_iface_decref(avro_helper.iface);
        avro_value_decref(&avro_helper.avro_val);
    });
    avro_value_set_long(&avro_helper.avro_val, -9223372036854775808ULL);
    auto st = add_numeric_column<int64_t>(column.get(), t, "f_int64", avro_helper.avro_val);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("[-9223372036854775808]", column->debug_string());
}

} // namespace starrocks
