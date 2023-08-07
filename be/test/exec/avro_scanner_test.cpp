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

#include "exec/avro_scanner.h"

#include <gtest/gtest.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <utility>

#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "fs/fs_util.h"
#include "gen_cpp/Descriptors_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"
#include "util/defer_op.h"

#ifdef __cplusplus
extern "C" {
#endif
#include "avro.h"
#ifdef __cplusplus
}
#endif

namespace starrocks {

struct AvroHelper {
    avro_schema_t schema = NULL;
    avro_value_iface_t* iface = NULL;
    avro_value_t avro_val;
    std::string schema_text;
};

class AvroScannerTest : public ::testing::Test {
protected:
    std::unique_ptr<AvroScanner> create_avro_scanner(const std::vector<TypeDescriptor>& types,
                                                     const std::vector<TBrokerRangeDesc>& ranges,
                                                     const std::vector<std::string>& col_names,
                                                     const std::string& schema_text) {
        /// Init DescriptorTable
        TDescriptorTableBuilder desc_tbl_builder;
        TTupleDescriptorBuilder tuple_desc_builder;
        for (int i = 0; i < types.size(); ++i) {
            TSlotDescriptorBuilder slot_desc_builder;
            slot_desc_builder.type(types[i]).column_name(col_names[i]).length(types[i].len).nullable(true);
            tuple_desc_builder.add_slot(slot_desc_builder.build());
        }
        tuple_desc_builder.build(&desc_tbl_builder);

        DescriptorTbl* desc_tbl = nullptr;
        Status st = DescriptorTbl::create(_state, &_pool, desc_tbl_builder.desc_tbl(), &desc_tbl,
                                          config::vector_chunk_size);
        CHECK(st.ok()) << st.to_string();

        /// Init RuntimeState
        _state->set_desc_tbl(desc_tbl);
        _state->init_instance_mem_tracker();

        /// TBrokerScanRangeParams
        TBrokerScanRangeParams* params = _pool.add(new TBrokerScanRangeParams());
        params->strict_mode = true;
        params->dest_tuple_id = 0;
        params->src_tuple_id = 0;
        for (int i = 0; i < types.size(); i++) {
            params->expr_of_dest_slot[i] = TExpr();
            params->expr_of_dest_slot[i].nodes.emplace_back(TExprNode());
            params->expr_of_dest_slot[i].nodes[0].__set_type(types[i].to_thrift());
            params->expr_of_dest_slot[i].nodes[0].__set_node_type(TExprNodeType::SLOT_REF);
            params->expr_of_dest_slot[i].nodes[0].__set_is_nullable(true);
            params->expr_of_dest_slot[i].nodes[0].__set_slot_ref(TSlotRef());
            params->expr_of_dest_slot[i].nodes[0].slot_ref.__set_slot_id(i);
            params->expr_of_dest_slot[i].nodes[0].__set_type(types[i].to_thrift());
        }

        for (int i = 0; i < types.size(); i++) {
            params->src_slot_ids.emplace_back(i);
        }

        TBrokerScanRange* broker_scan_range = _pool.add(new TBrokerScanRange());
        broker_scan_range->params = *params;
        broker_scan_range->ranges = ranges;
        return std::make_unique<AvroScanner>(_state, _profile, *broker_scan_range, _counter, schema_text);
    }

    static void SetUpTestCase() {
        ASSERT_TRUE(fs::create_directories("./be/test/exec/test_data/avro_scanner/tmp").ok());
    }

    static void TearDownTestCase() { ASSERT_TRUE(fs::remove_all("./be/test/exec/test_data/avro_scanner/tmp").ok()); }

    void SetUp() override {
        config::vector_chunk_size = 4096;
        _profile = _pool.add(new RuntimeProfile("test"));
        _counter = _pool.add(new ScannerCounter());
        _state = _pool.add(new RuntimeState(TQueryGlobals()));
        std::string starrocks_home = getenv("STARROCKS_HOME");
    }

    void TearDown() override {}

    void init_avro_value(std::string schema_path, AvroHelper& avro_helper) {
        std::ifstream infile_schema;
        infile_schema.open(schema_path);
        std::stringstream ss;
        ss << infile_schema.rdbuf();
        std::string schema_str(ss.str());
        avro_schema_error_t error;
        avro_helper.schema_text = schema_str;
        int result = avro_schema_from_json(schema_str.c_str(), schema_str.size(), &avro_helper.schema, &error);
        if (result != 0) {
            std::cout << "parse schema from json error: " << avro_strerror() << std::endl;
        }
        EXPECT_EQ(0, result);
        avro_helper.iface = avro_generic_class_from_schema(avro_helper.schema);
        avro_generic_value_new(avro_helper.iface, &avro_helper.avro_val);
    }

    Status write_avro_data(AvroHelper& avro_helper, std::string data_path) {
        avro_file_writer_t db;
        int rval = avro_file_writer_create(data_path.c_str(), avro_helper.schema, &db);
        if (rval) {
            auto err_msg =
                    strings::Substitute("There was an error creating $0 error message: $1", data_path, avro_strerror());
            return Status::InternalError(err_msg);
        }

        if (avro_file_writer_append_value(db, &avro_helper.avro_val)) {
            auto err_msg =
                    strings::Substitute("Unable to write Person value to memory buffer\nMessage: $0", avro_strerror());
            return Status::InternalError(err_msg);
        }
        avro_file_writer_flush(db);
        avro_file_writer_close(db);
        return Status::OK();
    }

private:
    RuntimeProfile* _profile = nullptr;
    ScannerCounter* _counter = nullptr;
    RuntimeState* _state = nullptr;
    ObjectPool _pool;
};

TEST_F(AvroScannerTest, test_basic_type) {
    std::string schema_path = "./be/test/exec/test_data/avro_scanner/avro_basic_schema.json";
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

    avro_value_t string_value;
    if (avro_value_get_by_name(&avro_helper.avro_val, "stringtype", &string_value, NULL) == 0) {
        avro_value_set_string(&string_value, "abcdefg");
    }

    avro_value_t enum_value;
    if (avro_value_get_by_name(&avro_helper.avro_val, "enumtype", &enum_value, NULL) == 0) {
        avro_value_set_enum(&enum_value, 2);
    }
    std::string data_path = "./be/test/exec/test_data/avro_scanner/tmp/avro_basic_data.json";
    write_avro_data(avro_helper, data_path);

    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_BOOLEAN);
    types.emplace_back(TYPE_BIGINT);
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_AVRO;
    range.__set_path(data_path);
    ranges.emplace_back(range);

    auto scanner =
            create_avro_scanner(types, ranges, {"booleantype", "longtype", "doubletype", "stringtype", "enumtype"},
                                avro_helper.schema_text);

    Status st = scanner->open();
    ASSERT_TRUE(st.ok());

    auto st2 = scanner->get_next();
    ASSERT_TRUE(st2.ok());

    ChunkPtr chunk = st2.value();
    EXPECT_EQ(5, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());
    EXPECT_EQ(1, chunk->get(0)[0].get_int8());
    EXPECT_EQ(4294967296, chunk->get(0)[1].get_int64());
    EXPECT_FLOAT_EQ(1.234567, chunk->get(0)[2].get_double());
    EXPECT_EQ("abcdefg", chunk->get(0)[3].get_slice());
    EXPECT_EQ("DIAMONDS", chunk->get(0)[4].get_slice());
}

TEST_F(AvroScannerTest, test_preprocess_jsonpaths) {
    std::string jsonpaths =
            R"(["$.decoded_logs.id", "$.decoded_logs.event_signature.*", "$.decoded_logs.event_params.*", "$.decoded_logs.raw_log.*.data"])";
    std::string new_jsonpaths = AvroScanner::preprocess_jsonpaths(jsonpaths);
    EXPECT_EQ(
            R"(["$.decoded_logs.id", "$.decoded_logs.event_signature", "$.decoded_logs.event_params", "$.decoded_logs.raw_log.data"])",
            new_jsonpaths);
}

TEST_F(AvroScannerTest, test_jsonpaths) {
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

    avro_value_t string_value;
    if (avro_value_get_by_name(&avro_helper.avro_val, "stringtype", &string_value, NULL) == 0) {
        avro_value_set_string(&string_value, "abcdefg");
    }

    avro_value_t nest_value;
    if (avro_value_get_by_name(&avro_helper.avro_val, "nesttype", &nest_value, NULL) == 0) {
        {
            avro_value_t boolean_value;
            if (avro_value_get_by_name(&nest_value, "booleantype", &boolean_value, NULL) == 0) {
                avro_value_set_boolean(&boolean_value, false);
            }

            avro_value_t long_value;
            if (avro_value_get_by_name(&nest_value, "longtype", &long_value, NULL) == 0) {
                avro_value_set_long(&long_value, 4294967297);
            }
        }
    }
    std::string data_path = "./be/test/exec/test_data/avro_scanner/tmp/avro_nest_data.json";
    write_avro_data(avro_helper, data_path);

    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_BOOLEAN);
    types.emplace_back(TYPE_BIGINT);
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TYPE_BIGINT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_AVRO;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.booleantype", "$.longtype", "$.doubletype", "$.stringtype", "$.nesttype.longtype"])";
    range.__set_path(data_path);
    ranges.emplace_back(range);

    auto scanner =
            create_avro_scanner(types, ranges, {"booleantype", "longtype", "doubletype", "stringtype", "longtype2"},
                                avro_helper.schema_text);
    Status st = scanner->open();
    ASSERT_TRUE(st.ok());

    auto st2 = scanner->get_next();
    ASSERT_TRUE(st2.ok());

    ChunkPtr chunk = st2.value();
    EXPECT_EQ(5, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());
    EXPECT_EQ(1, chunk->get(0)[0].get_int8());
    EXPECT_EQ(4294967296, chunk->get(0)[1].get_int64());
    EXPECT_FLOAT_EQ(1.234567, chunk->get(0)[2].get_double());
    EXPECT_EQ("abcdefg", chunk->get(0)[3].get_slice());
    EXPECT_EQ(4294967297, chunk->get(0)[4].get_int64());
}

TEST_F(AvroScannerTest, test_json_type) {
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

    avro_value_t string_value;
    if (avro_value_get_by_name(&avro_helper.avro_val, "stringtype", &string_value, NULL) == 0) {
        avro_value_set_string(&string_value, "abcdefg");
    }

    avro_value_t nest_value;
    if (avro_value_get_by_name(&avro_helper.avro_val, "nesttype", &nest_value, NULL) == 0) {
        {
            avro_value_t boolean_value;
            if (avro_value_get_by_name(&nest_value, "booleantype", &boolean_value, NULL) == 0) {
                avro_value_set_boolean(&boolean_value, false);
            }

            avro_value_t long_value;
            if (avro_value_get_by_name(&nest_value, "longtype", &long_value, NULL) == 0) {
                avro_value_set_long(&long_value, 4294967297);
            }
        }
    }

    std::string data_path = "./be/test/exec/test_data/avro_scanner/tmp/avro_nest_data.json";
    write_avro_data(avro_helper, data_path);

    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_BOOLEAN);
    types.emplace_back(TYPE_BIGINT);
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TYPE_JSON);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_AVRO;
    range.__isset.jsonpaths = false;
    range.__set_path(data_path);
    ranges.emplace_back(range);

    auto scanner =
            create_avro_scanner(types, ranges, {"booleantype", "longtype", "doubletype", "stringtype", "nesttype"},
                                avro_helper.schema_text);
    Status st = scanner->open();
    ASSERT_TRUE(st.ok());

    auto st2 = scanner->get_next();
    ASSERT_TRUE(st2.ok());

    ChunkPtr chunk = st2.value();
    EXPECT_EQ(5, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());
    EXPECT_EQ(1, chunk->get(0)[0].get_int8());
    EXPECT_EQ(4294967296, chunk->get(0)[1].get_int64());
    EXPECT_FLOAT_EQ(1.234567, chunk->get(0)[2].get_double());
    EXPECT_EQ("abcdefg", chunk->get(0)[3].get_slice());
    const JsonValue* json = chunk->get(0)[4].get_json();
    EXPECT_EQ("{\"booleantype\": false, \"longtype\": 4294967297}", json->to_string_uncheck());
}

TEST_F(AvroScannerTest, test_union_type_null) {
    std::string schema_path = "./be/test/exec/test_data/avro_scanner/avro_union_schema.json";
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

    avro_value_t union_value;
    if (avro_value_get_by_name(&avro_helper.avro_val, "uniontype", &union_value, NULL) == 0) {
        avro_value_t null_value;
        avro_value_set_branch(&union_value, 0, &null_value);
        avro_value_set_null(&null_value);
    }

    std::string data_path = "./be/test/exec/test_data/avro_scanner/tmp/avro_union_data.json";
    write_avro_data(avro_helper, data_path);

    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_BOOLEAN);
    types.emplace_back(TYPE_BIGINT);
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TypeDescriptor::create_varchar_type(20));

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_AVRO;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.booleantype", "$.longtype", "$.doubletype", "$.uniontype.*"])";
    range.__isset.json_root = false;
    range.__set_path(data_path);
    ranges.emplace_back(range);

    auto scanner = create_avro_scanner(types, ranges, {"booleantype", "longtype", "doubletype", "uniontype"},
                                       avro_helper.schema_text);
    Status st = scanner->open();
    ASSERT_TRUE(st.ok());

    auto st2 = scanner->get_next();
    ASSERT_TRUE(st2.ok());

    ChunkPtr chunk = st2.value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());
    EXPECT_EQ(1, chunk->get(0)[0].get_int8());
    EXPECT_EQ(4294967296, chunk->get(0)[1].get_int64());
    EXPECT_FLOAT_EQ(1.234567, chunk->get(0)[2].get_double());
    EXPECT_TRUE(chunk->get(0)[3].is_null());
}

TEST_F(AvroScannerTest, test_union_type_null_without_jsonpath) {
    std::string schema_path = "./be/test/exec/test_data/avro_scanner/avro_union_schema.json";
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

    avro_value_t union_value;
    if (avro_value_get_by_name(&avro_helper.avro_val, "uniontype", &union_value, NULL) == 0) {
        avro_value_t null_value;
        avro_value_set_branch(&union_value, 0, &null_value);
        avro_value_set_null(&null_value);
    }

    std::string data_path = "./be/test/exec/test_data/avro_scanner/tmp/avro_union_data.json";
    write_avro_data(avro_helper, data_path);

    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_BOOLEAN);
    types.emplace_back(TYPE_BIGINT);
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TypeDescriptor::create_varchar_type(20));

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_AVRO;
    range.__isset.strip_outer_array = false;
    range.__isset.json_root = false;
    range.__set_path(data_path);
    ranges.emplace_back(range);

    auto scanner = create_avro_scanner(types, ranges, {"booleantype", "longtype", "doubletype", "uniontype"},
                                       avro_helper.schema_text);
    Status st = scanner->open();
    ASSERT_TRUE(st.ok());

    auto st2 = scanner->get_next();
    ASSERT_TRUE(st2.ok());

    ChunkPtr chunk = st2.value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());
    EXPECT_EQ(1, chunk->get(0)[0].get_int8());
    EXPECT_EQ(4294967296, chunk->get(0)[1].get_int64());
    EXPECT_FLOAT_EQ(1.234567, chunk->get(0)[2].get_double());
    EXPECT_TRUE(chunk->get(0)[3].is_null());
}

TEST_F(AvroScannerTest, test_union_type_basic) {
    std::string schema_path = "./be/test/exec/test_data/avro_scanner/avro_union_schema.json";
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

    avro_value_t union_value;
    if (avro_value_get_by_name(&avro_helper.avro_val, "uniontype", &union_value, NULL) == 0) {
        avro_value_t string_value;
        avro_value_set_branch(&union_value, 1, &string_value);
        avro_value_set_string(&string_value, "abcdefg");
    }
    std::string data_path = "./be/test/exec/test_data/avro_scanner/tmp/avro_union_data.json";
    write_avro_data(avro_helper, data_path);

    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_BOOLEAN);
    types.emplace_back(TYPE_BIGINT);
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TypeDescriptor::create_varchar_type(20));

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_AVRO;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.booleantype", "$.longtype", "$.doubletype", "$.uniontype.*"])";
    range.__isset.json_root = false;
    range.__set_path(data_path);
    ranges.emplace_back(range);

    auto scanner = create_avro_scanner(types, ranges, {"booleantype", "longtype", "doubletype", "uniontype"},
                                       avro_helper.schema_text);
    Status st = scanner->open();
    ASSERT_TRUE(st.ok());

    auto st2 = scanner->get_next();
    ASSERT_TRUE(st2.ok());

    ChunkPtr chunk = st2.value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());
    EXPECT_EQ(1, chunk->get(0)[0].get_int8());
    EXPECT_EQ(4294967296, chunk->get(0)[1].get_int64());
    EXPECT_FLOAT_EQ(1.234567, chunk->get(0)[2].get_double());
    EXPECT_EQ("abcdefg", chunk->get(0)[3].get_slice());
}

TEST_F(AvroScannerTest, test_array) {
    std::string schema_path = "./be/test/exec/test_data/avro_scanner/avro_array_schema.json";
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

    avro_value_t array_value;
    if (avro_value_get_by_name(&avro_helper.avro_val, "arraytype", &array_value, NULL) == 0) {
        avro_value_t ele1;
        avro_value_append(&array_value, &ele1, NULL);
        avro_value_set_long(&ele1, 4294967297);

        avro_value_t ele2;
        avro_value_append(&array_value, &ele2, NULL);
        avro_value_set_long(&ele2, 4294967298);
    }

    std::string data_path = "./be/test/exec/test_data/avro_scanner/tmp/avro_array_data.json";
    write_avro_data(avro_helper, data_path);

    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_BOOLEAN);
    types.emplace_back(TYPE_BIGINT);
    types.emplace_back(TYPE_DOUBLE);
    TypeDescriptor t(TYPE_ARRAY);
    t.children.emplace_back(TYPE_BIGINT);
    types.emplace_back(t);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_AVRO;
    range.__isset.jsonpaths = false;
    range.__set_path(data_path);
    ranges.emplace_back(range);

    auto scanner = create_avro_scanner(types, ranges, {"booleantype", "longtype", "doubletype", "arraytype"},
                                       avro_helper.schema_text);
    Status st = scanner->open();
    ASSERT_TRUE(st.ok());

    auto st2 = scanner->get_next();
    ASSERT_TRUE(st2.ok());

    ChunkPtr chunk = st2.value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());
    EXPECT_EQ(1, chunk->get(0)[0].get_int8());
    EXPECT_EQ(4294967296, chunk->get(0)[1].get_int64());
    EXPECT_FLOAT_EQ(1.234567, chunk->get(0)[2].get_double());
    auto array = chunk->get(0)[3].get_array();
    ASSERT_EQ(4294967297, array[0].get_int64());
    ASSERT_EQ(4294967298, array[1].get_int64());
}

TEST_F(AvroScannerTest, test_complex_schema) {
    std::string schema_path = "./be/test/exec/test_data/avro_scanner/avro_complex_schema.json";
    AvroHelper avro_helper;
    init_avro_value(schema_path, avro_helper);
    DeferOp avro_helper_deleter([&] {
        avro_schema_decref(avro_helper.schema);
        avro_value_iface_decref(avro_helper.iface);
        avro_value_decref(&avro_helper.avro_val);
    });

    avro_value_t decoded_logs_value;
    avro_value_set_branch(&avro_helper.avro_val, 1, &decoded_logs_value);
    avro_value_t id_value;
    if (avro_value_get_by_name(&decoded_logs_value, "id", &id_value, NULL) == 0) {
        avro_value_set_string(&id_value, "12345");
    }

    avro_value_t event_signature_val;
    if (avro_value_get_by_name(&decoded_logs_value, "eventsignature", &event_signature_val, NULL) == 0) {
        avro_value_t null_vale;
        avro_value_set_branch(&event_signature_val, 0, &null_vale);
        avro_value_set_null(&null_vale);
    }

    avro_value_t event_params_val;
    if (avro_value_get_by_name(&decoded_logs_value, "eventparams", &event_params_val, NULL) == 0) {
        avro_value_t array_value;
        avro_value_set_branch(&event_params_val, 1, &array_value);

        avro_value_t ele1;
        avro_value_append(&array_value, &ele1, NULL);
        avro_value_set_string(&ele1, "abc");

        avro_value_t ele2;
        avro_value_append(&array_value, &ele2, NULL);
        avro_value_set_string(&ele2, "def");
    }

    avro_value_t raw_log_val;
    if (avro_value_get_by_name(&decoded_logs_value, "rawlog", &raw_log_val, NULL) == 0) {
        avro_value_t record_value;
        avro_value_set_branch(&raw_log_val, 1, &record_value);

        avro_value_t id_value;
        if (avro_value_get_by_name(&record_value, "id", &id_value, NULL) == 0) {
            avro_value_set_string(&id_value, "iop");
        }
        avro_value_t data_value;
        if (avro_value_get_by_name(&record_value, "data", &data_value, NULL) == 0) {
            avro_value_set_string(&data_value, "klj");
        }
    }

    std::string data_path = "./be/test/exec/test_data/avro_scanner/tmp/avro_complex_data.json";
    write_avro_data(avro_helper, data_path);

    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    TypeDescriptor t(TYPE_ARRAY);
    t.children.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(t);
    types.emplace_back(TypeDescriptor::create_varchar_type(20));

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_AVRO;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.id", "$.eventsignature.*", "$.eventparams.*", "$.rawlog.*.data"])";
    range.__set_path(data_path);
    ranges.emplace_back(range);

    auto scanner = create_avro_scanner(types, ranges, {"id", "eventsignature", "eventparams", "data"},
                                       avro_helper.schema_text);
    Status st = scanner->open();
    ASSERT_TRUE(st.ok());

    auto st2 = scanner->get_next();
    ASSERT_TRUE(st2.ok());

    ChunkPtr chunk = st2.value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());
    EXPECT_EQ("12345", chunk->get(0)[0].get_slice());
    EXPECT_TRUE(chunk->get(0)[1].is_null());
    auto array = chunk->get(0)[2].get_array();
    ASSERT_EQ("abc", array[0].get_slice());
    ASSERT_EQ("def", array[1].get_slice());
    EXPECT_EQ("klj", chunk->get(0)[3].get_slice());
}

TEST_F(AvroScannerTest, test_complex_schema_null_data) {
    std::string schema_path = "./be/test/exec/test_data/avro_scanner/avro_complex_schema.json";
    AvroHelper avro_helper;
    init_avro_value(schema_path, avro_helper);
    DeferOp avro_helper_deleter([&] {
        avro_schema_decref(avro_helper.schema);
        avro_value_iface_decref(avro_helper.iface);
        avro_value_decref(&avro_helper.avro_val);
    });

    avro_value_t decoded_logs_value;
    avro_value_set_branch(&avro_helper.avro_val, 1, &decoded_logs_value);
    avro_value_t id_value;
    if (avro_value_get_by_name(&decoded_logs_value, "id", &id_value, NULL) == 0) {
        avro_value_set_string(&id_value, "12345");
    }

    avro_value_t event_signature_val;
    if (avro_value_get_by_name(&decoded_logs_value, "eventsignature", &event_signature_val, NULL) == 0) {
        avro_value_t null_vale;
        avro_value_set_branch(&event_signature_val, 0, &null_vale);
        avro_value_set_null(&null_vale);
    }

    avro_value_t event_params_val;
    if (avro_value_get_by_name(&decoded_logs_value, "eventparams", &event_params_val, NULL) == 0) {
        avro_value_t array_value;
        avro_value_set_branch(&event_params_val, 1, &array_value);

        avro_value_t ele1;
        avro_value_append(&array_value, &ele1, NULL);
        avro_value_set_string(&ele1, "abc");

        avro_value_t ele2;
        avro_value_append(&array_value, &ele2, NULL);
        avro_value_set_string(&ele2, "def");
    }

    avro_value_t raw_log_val;
    if (avro_value_get_by_name(&decoded_logs_value, "rawlog", &raw_log_val, NULL) == 0) {
        avro_value_t null_vale;
        avro_value_set_branch(&raw_log_val, 0, &null_vale);
        avro_value_set_null(&null_vale);
    }

    std::string data_path = "./be/test/exec/test_data/avro_scanner/tmp/avro_complex_data.json";
    write_avro_data(avro_helper, data_path);

    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    TypeDescriptor t(TYPE_ARRAY);
    t.children.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(t);
    types.emplace_back(TypeDescriptor::create_varchar_type(20));

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_AVRO;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.id", "$.eventsignature.*", "$.eventparams.*", "$.rawlog.*.data"])";
    range.__isset.json_root = false;
    range.__set_path(data_path);
    ranges.emplace_back(range);

    auto scanner = create_avro_scanner(types, ranges, {{"id", "eventsignature", "eventparams", "data"}},
                                       avro_helper.schema_text);
    Status st = scanner->open();
    ASSERT_TRUE(st.ok());

    auto st2 = scanner->get_next();
    ASSERT_TRUE(st2.ok());

    ChunkPtr chunk = st2.value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());
    EXPECT_EQ("12345", chunk->get(0)[0].get_slice());
    EXPECT_TRUE(chunk->get(0)[1].is_null());
    auto array = chunk->get(0)[2].get_array();
    ASSERT_EQ("abc", array[0].get_slice());
    ASSERT_EQ("def", array[1].get_slice());
    EXPECT_TRUE(chunk->get(0)[3].is_null());
}

TEST_F(AvroScannerTest, test_map_to_json) {
    std::string schema_path = "./be/test/exec/test_data/avro_scanner/avro_map_schema.json";
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

    avro_value_t map_value;
    if (avro_value_get_by_name(&avro_helper.avro_val, "maptype", &map_value, NULL) == 0) {
        avro_value_t ele1;
        avro_value_add(&map_value, "ele1", &ele1, NULL, NULL);
        avro_value_set_long(&ele1, 4294967297);

        avro_value_t ele2;
        avro_value_add(&map_value, "ele2", &ele2, NULL, NULL);
        avro_value_set_long(&ele2, 4294967298);
    }

    std::string data_path = "./be/test/exec/test_data/avro_scanner/tmp/avro_map_data.json";
    write_avro_data(avro_helper, data_path);

    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_BOOLEAN);
    types.emplace_back(TYPE_BIGINT);
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TYPE_JSON);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_AVRO;
    range.__set_path(data_path);
    ranges.emplace_back(range);

    auto scanner = create_avro_scanner(types, ranges, {"booleantype", "longtype", "doubletype", "maptype"},
                                       avro_helper.schema_text);
    Status st = scanner->open();
    ASSERT_TRUE(st.ok());

    auto st2 = scanner->get_next();
    ASSERT_TRUE(st2.ok());

    ChunkPtr chunk = st2.value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());
    EXPECT_EQ(1, chunk->get(0)[0].get_int8());
    EXPECT_EQ(4294967296, chunk->get(0)[1].get_int64());
    EXPECT_FLOAT_EQ(1.234567, chunk->get(0)[2].get_double());
    const JsonValue* json = chunk->get(0)[3].get_json();
    EXPECT_EQ("{\"ele1\": 4294967297, \"ele2\": 4294967298}", json->to_string_uncheck());
}

TEST_F(AvroScannerTest, test_root_array) {
    std::string schema_path = "./be/test/exec/test_data/avro_scanner/avro_root_array_schema.json";
    AvroHelper avro_helper;
    init_avro_value(schema_path, avro_helper);
    DeferOp avro_helper_deleter([&] {
        avro_schema_decref(avro_helper.schema);
        avro_value_iface_decref(avro_helper.iface);
        avro_value_decref(&avro_helper.avro_val);
    });

    {
        avro_value_t ele;
        avro_value_append(&avro_helper.avro_val, &ele, NULL);

        avro_value_t boolean_value;
        if (avro_value_get_by_name(&ele, "booleantype", &boolean_value, NULL) == 0) {
            avro_value_set_boolean(&boolean_value, true);
        }

        avro_value_t long_value;
        if (avro_value_get_by_name(&ele, "longtype", &long_value, NULL) == 0) {
            avro_value_set_long(&long_value, 4294967296);
        }

        avro_value_t double_value;
        if (avro_value_get_by_name(&ele, "doubletype", &double_value, NULL) == 0) {
            avro_value_set_double(&double_value, 1.234567);
        }

        avro_value_t string_value;
        if (avro_value_get_by_name(&ele, "stringtype", &string_value, NULL) == 0) {
            avro_value_set_string(&string_value, "abcdefg");
        }

        avro_value_t enum_value;
        if (avro_value_get_by_name(&ele, "enumtype", &enum_value, NULL) == 0) {
            avro_value_set_enum(&enum_value, 2);
        }
    }

    {
        avro_value_t ele;
        avro_value_append(&avro_helper.avro_val, &ele, NULL);

        avro_value_t boolean_value;
        if (avro_value_get_by_name(&ele, "booleantype", &boolean_value, NULL) == 0) {
            avro_value_set_boolean(&boolean_value, true);
        }

        avro_value_t long_value;
        if (avro_value_get_by_name(&ele, "longtype", &long_value, NULL) == 0) {
            avro_value_set_long(&long_value, 429496);
        }

        avro_value_t double_value;
        if (avro_value_get_by_name(&ele, "doubletype", &double_value, NULL) == 0) {
            avro_value_set_double(&double_value, 1.23457);
        }

        avro_value_t string_value;
        if (avro_value_get_by_name(&ele, "stringtype", &string_value, NULL) == 0) {
            avro_value_set_string(&string_value, "aaafg");
        }

        avro_value_t enum_value;
        if (avro_value_get_by_name(&ele, "enumtype", &enum_value, NULL) == 0) {
            avro_value_set_enum(&enum_value, 1);
        }
    }

    std::string data_path = "./be/test/exec/test_data/avro_scanner/tmp/avro_root_array_schema.json";
    write_avro_data(avro_helper, data_path);

    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_BOOLEAN);
    types.emplace_back(TYPE_BIGINT);
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_AVRO;
    range.__set_path(data_path);
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$[0].booleantype", "$[0].longtype", "$[0].doubletype", "$[0].stringtype", "$[0].enumtype"])";
    ranges.emplace_back(range);

    auto scanner =
            create_avro_scanner(types, ranges, {"booleantype", "longtype", "doubletype", "stringtype", "enumtype"},
                                avro_helper.schema_text);

    Status st = scanner->open();
    ASSERT_TRUE(st.ok());

    auto st2 = scanner->get_next();
    ASSERT_TRUE(st2.ok());

    ChunkPtr chunk = st2.value();
    EXPECT_EQ(5, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());
    EXPECT_EQ(1, chunk->get(0)[0].get_int8());
    EXPECT_EQ(4294967296, chunk->get(0)[1].get_int64());
    EXPECT_FLOAT_EQ(1.234567, chunk->get(0)[2].get_double());
    EXPECT_EQ("abcdefg", chunk->get(0)[3].get_slice());
    EXPECT_EQ("DIAMONDS", chunk->get(0)[4].get_slice());
}

} // namespace starrocks
