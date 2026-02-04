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

#include "formats/avro/cpp/complex_column_reader.h"

#include <gtest/gtest.h>

#include <avrocpp/NodeImpl.hh>

#include "base/testutil/assert.h"
#include "column/column_helper.h"
#include "formats/avro/cpp/avro_schema_builder.h"
#include "formats/avro/cpp/test_avro_utils.h"
#include "gen_cpp/Descriptors_types.h"

namespace starrocks::avrocpp {

class ComplexColumnReaderTest : public ColumnReaderTest, public ::testing::Test {};

TEST_F(ComplexColumnReaderTest, test_struct) {
    // init record schema and datum
    avro::MultiLeaves field_nodes;
    field_nodes.add(avro::NodePtr(new avro::NodePrimitive(avro::AVRO_INT)));
    field_nodes.add(avro::NodePtr(new avro::NodePrimitive(avro::AVRO_STRING)));

    avro::LeafNames field_names;
    field_names.add("int_col");
    field_names.add("string_col");

    std::vector<avro::GenericDatum> datums;
    int32_t int_v = 10;
    datums.emplace_back(avro::GenericDatum(int_v));
    std::string string_v = "abc";
    datums.emplace_back(avro::GenericDatum(string_v));

    auto record_schema =
            avro::NodePtr(new avro::NodeRecord(avro::HasName(avro::Name(_col_name)), field_nodes, field_names, datums));

    auto record_datum = avro::GenericRecord(record_schema);
    for (size_t i = 0; i < datums.size(); ++i) {
        record_datum.setFieldAt(i, datums[i]);
    }

    auto datum = avro::GenericDatum(record_schema, record_datum);

    // init column and column reader
    TypeDescriptor type_desc;
    CHECK_OK(get_avro_type(record_schema, &type_desc));
    ASSERT_EQ(TYPE_STRUCT, type_desc.type);
    ASSERT_EQ(2, type_desc.children.size());
    ASSERT_EQ(TYPE_INT, type_desc.children[0].type);
    ASSERT_EQ(TYPE_VARCHAR, type_desc.children[1].type);

    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, false);

    // read datum
    CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));

    ASSERT_EQ(1, column->size());
    ASSERT_EQ("[{int_col:10,string_col:'abc'}]", column->debug_string());
}

TEST_F(ComplexColumnReaderTest, test_array) {
    // init array schema and datum
    avro::SingleLeaf item_node(avro::NodePtr(new avro::NodePrimitive(avro::AVRO_INT)));
    auto array_schema = avro::NodePtr(new avro::NodeArray(item_node));

    auto array_datum = avro::GenericArray(array_schema);
    auto& array_value = array_datum.value();
    {
        int32_t int_v = 10;
        array_value.emplace_back(avro::GenericDatum(int_v));
    }
    {
        int32_t int_v = 11;
        array_value.emplace_back(avro::GenericDatum(int_v));
    }

    auto datum = avro::GenericDatum(array_schema, array_datum);

    // init column and column reader
    TypeDescriptor type_desc;
    CHECK_OK(get_avro_type(array_schema, &type_desc));
    ASSERT_EQ(TYPE_ARRAY, type_desc.type);
    ASSERT_EQ(1, type_desc.children.size());
    ASSERT_EQ(TYPE_INT, type_desc.children[0].type);

    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, false);

    // read datum
    CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));

    ASSERT_EQ(1, column->size());
    ASSERT_EQ("[[10,11]]", column->debug_string());
}

TEST_F(ComplexColumnReaderTest, test_map) {
    // init map schema and datum
    avro::SingleLeaf value_node(avro::NodePtr(new avro::NodePrimitive(avro::AVRO_INT)));
    auto map_schema = avro::NodePtr(new avro::NodeMap(value_node));

    auto map_datum = avro::GenericMap(map_schema);
    auto& map_value = map_datum.value();
    {
        int32_t int_v = 10;
        map_value.emplace_back("abc", avro::GenericDatum(int_v));
    }
    {
        int32_t int_v = 11;
        map_value.emplace_back("def", avro::GenericDatum(int_v));
    }

    auto datum = avro::GenericDatum(map_schema, map_datum);

    // init column and column reader
    TypeDescriptor type_desc;
    CHECK_OK(get_avro_type(map_schema, &type_desc));
    ASSERT_EQ(TYPE_MAP, type_desc.type);
    ASSERT_EQ(2, type_desc.children.size());
    ASSERT_EQ(TYPE_VARCHAR, type_desc.children[0].type);
    ASSERT_EQ(TYPE_INT, type_desc.children[1].type);

    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, false);

    // read datum
    CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));

    ASSERT_EQ(1, column->size());
    ASSERT_EQ("[{'abc':10,'def':11}]", column->debug_string());
}

} // namespace starrocks::avrocpp
