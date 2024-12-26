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

#include "testutil/schema_test_helper.h"

namespace starrocks {
TabletSchemaPB SchemaTestHelper::gen_schema_pb_of_dup(TabletSchema::SchemaId schema_id, size_t num_cols,
                                                      size_t num_key_cols) {
    TabletSchemaPB schema_pb;

    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(num_key_cols);
    schema_pb.set_id(schema_id);

    for (size_t i = 0; i < num_cols; i++) {
        auto c0 = schema_pb.add_column();
        c0->set_unique_id(i);
        c0->set_name("c" + std::to_string(i));
        c0->set_type("INT");
        c0->set_is_nullable(true);
        c0->set_index_length(4);
        if (i < num_key_cols) {
            c0->set_is_key(true);
        }
    }

    return schema_pb;
}

TabletSchemaPB SchemaTestHelper::gen_varchar_schema_pb_of_dup(TabletSchema::SchemaId schema_id, size_t num_cols,
                                                              size_t num_key_cols) {
    TabletSchemaPB schema_pb;

    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(num_key_cols);
    schema_pb.set_id(schema_id);

    for (size_t i = 0; i < num_cols; i++) {
        auto c0 = schema_pb.add_column();
        c0->set_unique_id(i);
        c0->set_name("c" + std::to_string(i));
        c0->set_type("VARCHAR");
        c0->set_is_nullable(true);
        c0->set_index_length(4);
        c0->set_length(100);
        if (i < num_key_cols) {
            c0->set_is_key(true);
        }
    }

    return schema_pb;
}

TabletSchemaSPtr SchemaTestHelper::gen_schema_of_dup(TabletSchema::SchemaId schema_id, size_t num_cols,
                                                     size_t num_key_cols) {
    TabletSchemaPB schema_pb = SchemaTestHelper::gen_schema_pb_of_dup(1, 3, 1);
    return std::make_shared<TabletSchema>(schema_pb);
}

TabletSchemaSPtr SchemaTestHelper::gen_varchar_schema_of_dup(TabletSchema::SchemaId schema_id, size_t num_cols,
                                                             size_t num_key_cols) {
    TabletSchemaPB schema_pb = SchemaTestHelper::gen_varchar_schema_pb_of_dup(1, 3, 1);
    return std::make_shared<TabletSchema>(schema_pb);
}

TColumn SchemaTestHelper::gen_key_column(const std::string& col_name, TPrimitiveType::type type) {
    TColumnType col_type;
    col_type.type = type;

    TColumn col;
    col.__set_column_name(col_name);
    col.__set_column_type(col_type);
    col.__set_is_key(true);

    return col;
}

TColumn SchemaTestHelper::gen_value_column_for_dup_table(const std::string& col_name, TPrimitiveType::type type) {
    TColumnType col_type;
    col_type.type = type;

    TColumn col;
    col.__set_column_name(col_name);
    col.__set_column_type(col_type);
    col.__set_is_key(false);

    return col;
}

TColumn SchemaTestHelper::gen_value_column_for_agg_table(const std::string& col_name, TPrimitiveType::type type) {
    TColumnType col_type;
    col_type.type = type;

    TColumn col;
    col.__set_column_name(col_name);
    col.__set_column_type(col_type);
    col.__set_is_key(false);
    col.__set_aggregation_type(TAggregationType::SUM);

    return col;
}

void SchemaTestHelper::add_column_pb_to_tablet_schema(TabletSchemaPB* tablet_schema_pb, const std::string& name,
                                                      const std::string& type, const std::string& agg,
                                                      uint32_t length) {
    ColumnPB* column = tablet_schema_pb->add_column();
    column->set_unique_id(0);
    column->set_name(name);
    column->set_type(type);
    column->set_is_key(false);
    column->set_is_nullable(false);
    column->set_length(length);
    column->set_aggregation(agg);
}

} // namespace starrocks
