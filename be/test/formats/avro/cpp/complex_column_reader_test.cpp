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

TEST_F(ComplexColumnReaderTest, test_map_nullable_date_values) {
    avro::LogicalType date_type(avro::LogicalType::DATE);
    auto date_node = avro::NodePtr(new avro::NodePrimitive(avro::AVRO_INT));
    date_node->setLogicalType(date_type);

    avro::MultiLeaves union_nodes;
    union_nodes.add(avro::NodePtr(new avro::NodePrimitive(avro::AVRO_NULL)));
    union_nodes.add(date_node);
    auto union_schema = avro::NodePtr(new avro::NodeUnion(union_nodes));

    auto map_schema = avro::NodePtr(new avro::NodeMap(avro::SingleLeaf(union_schema)));
    auto map_datum = avro::GenericMap(map_schema);
    auto& map_value = map_datum.value();
    {
        avro::GenericUnion null_union(union_schema);
        null_union.selectBranch(0);
        map_value.emplace_back("abc", avro::GenericDatum(union_schema, null_union));
    }
    {
        avro::GenericUnion date_union(union_schema);
        date_union.selectBranch(1);
        date_union.datum() = avro::GenericDatum(avro::AVRO_INT, date_type, days_since_epoch("2026-05-19"));
        map_value.emplace_back("def", avro::GenericDatum(union_schema, date_union));
    }

    auto datum = avro::GenericDatum(map_schema, map_datum);

    TypeDescriptor type_desc;
    CHECK_OK(get_avro_type(map_schema, &type_desc));
    ASSERT_EQ(TYPE_MAP, type_desc.type);
    ASSERT_EQ(TYPE_DATE, type_desc.children[1].type);

    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, false);

    CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));

    ASSERT_EQ(1, column->size());
    ASSERT_EQ("[{'abc':NULL,'def':2026-05-19}]", column->debug_string());
}

TEST_F(ComplexColumnReaderTest, test_map_nullable_timestamp_values) {
    avro::LogicalType ts_type(avro::LogicalType::TIMESTAMP_MILLIS);
    auto ts_node = avro::NodePtr(new avro::NodePrimitive(avro::AVRO_LONG));
    ts_node->setLogicalType(ts_type);

    avro::MultiLeaves union_nodes;
    union_nodes.add(avro::NodePtr(new avro::NodePrimitive(avro::AVRO_NULL)));
    union_nodes.add(ts_node);
    auto union_schema = avro::NodePtr(new avro::NodeUnion(union_nodes));

    auto map_schema = avro::NodePtr(new avro::NodeMap(avro::SingleLeaf(union_schema)));
    auto map_datum = avro::GenericMap(map_schema);
    auto& map_value = map_datum.value();
    {
        avro::GenericUnion null_union(union_schema);
        null_union.selectBranch(0);
        map_value.emplace_back("true", avro::GenericDatum(union_schema, null_union));
    }
    {
        avro::GenericUnion ts_union(union_schema);
        ts_union.selectBranch(1);
        ts_union.datum() =
                avro::GenericDatum(avro::AVRO_LONG, ts_type, milliseconds_since_epoch("2026-05-19 12:03:01.000"));
        map_value.emplace_back("false", avro::GenericDatum(union_schema, ts_union));
    }

    auto datum = avro::GenericDatum(map_schema, map_datum);

    TypeDescriptor type_desc;
    CHECK_OK(get_avro_type(map_schema, &type_desc));
    ASSERT_EQ(TYPE_MAP, type_desc.type);
    ASSERT_EQ(TYPE_DATETIME, type_desc.children[1].type);

    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, false);

    CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));

    ASSERT_EQ(1, column->size());
    ASSERT_EQ("[{'true':NULL,'false':2026-05-19 12:03:01}]", column->debug_string());
}

TEST_F(ComplexColumnReaderTest, test_adaptive_reader_rejects_non_adaptive_column) {
    auto type_desc = TypeDescriptor::from_logical_type(TYPE_INT);
    auto reader = get_column_reader(type_desc, false);
    auto column = ColumnHelper::create_column(type_desc, true, false, 0, false);
    avro::GenericDatum datum(int32_t{10});

    auto st = reader->read_datum_for_adaptive_column(datum, column.get());
    ASSERT_TRUE(st.is_internal_error());
    ASSERT_TRUE(st.message().find("AdaptiveNullableColumn") != std::string::npos);
}

TEST_F(ComplexColumnReaderTest, test_map_unknown_value_type_skips_value_decode) {
    avro::LogicalType ts_type(avro::LogicalType::TIMESTAMP_MILLIS);
    auto ts_node = avro::NodePtr(new avro::NodePrimitive(avro::AVRO_LONG));
    ts_node->setLogicalType(ts_type);

    avro::MultiLeaves union_nodes;
    union_nodes.add(avro::NodePtr(new avro::NodePrimitive(avro::AVRO_NULL)));
    union_nodes.add(ts_node);
    auto union_schema = avro::NodePtr(new avro::NodeUnion(union_nodes));

    auto map_schema = avro::NodePtr(new avro::NodeMap(avro::SingleLeaf(union_schema)));
    auto map_datum = avro::GenericMap(map_schema);
    auto& map_value = map_datum.value();
    {
        avro::GenericUnion null_union(union_schema);
        null_union.selectBranch(0);
        map_value.emplace_back("true", avro::GenericDatum(union_schema, null_union));
    }
    {
        avro::GenericUnion ts_union(union_schema);
        ts_union.selectBranch(1);
        ts_union.datum() =
                avro::GenericDatum(avro::AVRO_LONG, ts_type, milliseconds_since_epoch("2026-05-19 12:03:01.000"));
        map_value.emplace_back("false", avro::GenericDatum(union_schema, ts_union));
    }

    auto datum = avro::GenericDatum(map_schema, map_datum);

    TypeDescriptor type_desc = TypeDescriptor::create_map_type(
            TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH), TypeDescriptor(TYPE_UNKNOWN));
    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, false);

    CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));

    ASSERT_EQ(1, column->size());
    ASSERT_EQ("[{'true':NULL,'false':NULL}]", column->debug_string());
}

TEST_F(ComplexColumnReaderTest, test_array_unknown_element_type_skips_element_decode) {
    avro::LogicalType ts_type(avro::LogicalType::TIMESTAMP_MILLIS);
    auto ts_node = avro::NodePtr(new avro::NodePrimitive(avro::AVRO_LONG));
    ts_node->setLogicalType(ts_type);

    avro::MultiLeaves union_nodes;
    union_nodes.add(avro::NodePtr(new avro::NodePrimitive(avro::AVRO_NULL)));
    union_nodes.add(ts_node);
    auto union_schema = avro::NodePtr(new avro::NodeUnion(union_nodes));

    auto array_schema = avro::NodePtr(new avro::NodeArray(avro::SingleLeaf(union_schema)));
    auto array_datum = avro::GenericArray(array_schema);
    auto& array_value = array_datum.value();
    {
        avro::GenericUnion null_union(union_schema);
        null_union.selectBranch(0);
        array_value.emplace_back(avro::GenericDatum(union_schema, null_union));
    }
    {
        avro::GenericUnion ts_union(union_schema);
        ts_union.selectBranch(1);
        ts_union.datum() =
                avro::GenericDatum(avro::AVRO_LONG, ts_type, milliseconds_since_epoch("2026-05-19 12:03:01.000"));
        array_value.emplace_back(avro::GenericDatum(union_schema, ts_union));
    }

    auto datum = avro::GenericDatum(array_schema, array_datum);

    TypeDescriptor reader_type_desc = TypeDescriptor::create_array_type(TypeDescriptor(TYPE_UNKNOWN));
    TypeDescriptor column_type_desc = TypeDescriptor::create_array_type(TypeDescriptor{TYPE_NULL});
    auto column = create_adaptive_nullable_column(column_type_desc);
    auto reader = get_column_reader(reader_type_desc, false);

    CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));

    ASSERT_EQ(1, column->size());
    ASSERT_EQ("[[NULL,NULL]]", column->debug_string());
}

TEST_F(ComplexColumnReaderTest, test_struct_unknown_field_type_skips_field_decode) {
    avro::LogicalType ts_type(avro::LogicalType::TIMESTAMP_MILLIS);
    auto ts_node = avro::NodePtr(new avro::NodePrimitive(avro::AVRO_LONG));
    ts_node->setLogicalType(ts_type);

    avro::MultiLeaves field_nodes;
    field_nodes.add(avro::NodePtr(new avro::NodePrimitive(avro::AVRO_INT)));
    field_nodes.add(ts_node);

    avro::LeafNames field_names;
    field_names.add("int_col");
    field_names.add("ts_col");

    std::vector<avro::GenericDatum> datums;
    datums.emplace_back(avro::GenericDatum(int32_t{10}));
    datums.emplace_back(avro::GenericDatum(avro::AVRO_LONG, ts_type, milliseconds_since_epoch("2026-05-19 12:03:01.000")));

    auto record_schema =
            avro::NodePtr(new avro::NodeRecord(avro::HasName(avro::Name(_col_name)), field_nodes, field_names, datums));
    auto record_datum = avro::GenericRecord(record_schema);
    for (size_t i = 0; i < datums.size(); ++i) {
        record_datum.setFieldAt(i, datums[i]);
    }
    auto datum = avro::GenericDatum(record_schema, record_datum);

    TypeDescriptor reader_type_desc = TypeDescriptor::create_struct_type(
            {"int_col", "ts_col"}, {TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor(TYPE_UNKNOWN)});
    TypeDescriptor column_type_desc = TypeDescriptor::create_struct_type(
            {"int_col", "ts_col"}, {TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor{TYPE_NULL}});
    auto column = create_adaptive_nullable_column(column_type_desc);
    auto reader = get_column_reader(reader_type_desc, false);

    CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));

    ASSERT_EQ(1, column->size());
    ASSERT_EQ("[{int_col:10,ts_col:NULL}]", column->debug_string());
}

} // namespace starrocks::avrocpp
