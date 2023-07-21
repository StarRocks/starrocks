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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/tablet_meta_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/tablet_meta.h"

#include <gtest/gtest.h>

#include <string>

#include "runtime/decimalv2_value.h"

namespace starrocks {

// NOLINTNEXTLINE
TEST(TabletMetaTest, test_create) {
    TCreateTabletReq request;
    request.__set_tablet_id(1000001);
    request.__set_partition_id(1);
    request.__set_tablet_type(TTabletType::TABLET_TYPE_DISK);
    request.__set_tablet_schema(TTabletSchema());

    TTabletSchema& schema = request.tablet_schema;
    schema.__set_schema_hash(12345);
    schema.__set_keys_type(TKeysType::DUP_KEYS);
    schema.__set_short_key_column_count(1);

    // c0 int key
    schema.columns.emplace_back();
    {
        TTypeNode type;
        type.__set_type(TTypeNodeType::SCALAR);
        type.__set_scalar_type(TScalarType());
        type.scalar_type.__set_type(TPrimitiveType::INT);

        schema.columns.back().__set_column_name("c0");
        schema.columns.back().__set_is_key(true);
        schema.columns.back().__set_index_len(sizeof(int32_t));
        schema.columns.back().__set_aggregation_type(TAggregationType::NONE);
        schema.columns.back().__set_is_allow_null(true);
        schema.columns.back().__set_type_desc(TTypeDesc());
        schema.columns.back().type_desc.__set_types({type});
    }
    // c1 ARRAY<DECIMAL(10, 3)>
    schema.columns.emplace_back();
    {
        std::vector<TTypeNode> types(2);
        types[0].__set_type(TTypeNodeType::ARRAY);
        types[1].__set_type(TTypeNodeType::SCALAR);
        types[1].scalar_type.__set_type(TPrimitiveType::DECIMALV2);
        types[1].scalar_type.__set_scale(10);
        types[1].scalar_type.__set_precision(3);

        schema.columns.back().__set_column_name("c1");
        schema.columns.back().__set_is_key(false);
        schema.columns.back().__set_index_len(0);
        schema.columns.back().__set_aggregation_type(TAggregationType::NONE);
        schema.columns.back().__set_is_allow_null(true);
        schema.columns.back().__set_type_desc(TTypeDesc());
        schema.columns.back().type_desc.__set_types(types);
    }
    // c2 ARRAY<ARRAY<VARCHAR(10)>> NOT NULL
    schema.columns.emplace_back();
    {
        std::vector<TTypeNode> types(3);
        types[0].__set_type(TTypeNodeType::ARRAY);
        types[1].__set_type(TTypeNodeType::ARRAY);
        types[2].__set_type(TTypeNodeType::SCALAR);
        types[2].scalar_type.__set_type(TPrimitiveType::VARCHAR);
        types[2].scalar_type.__set_len(10);

        schema.columns.back().__set_column_name("c2");
        schema.columns.back().__set_is_key(false);
        schema.columns.back().__set_index_len(0);
        schema.columns.back().__set_aggregation_type(TAggregationType::NONE);
        schema.columns.back().__set_is_allow_null(false);
        schema.columns.back().__set_type_desc(TTypeDesc());
        schema.columns.back().type_desc.__set_types(types);
    }

    std::unordered_map<uint32_t, uint32_t> col_ordinal_to_unique_id;
    col_ordinal_to_unique_id[0] = 10000;
    col_ordinal_to_unique_id[1] = 10001;
    col_ordinal_to_unique_id[2] = 10002;
    col_ordinal_to_unique_id[3] = 10003;

    request.__set_binlog_config(TBinlogConfig());
    TBinlogConfig& binlog_config = request.binlog_config;
    binlog_config.__set_version(5);
    binlog_config.__set_binlog_enable(true);
    binlog_config.__set_binlog_ttl_second(12323);
    binlog_config.__set_binlog_max_size(23724);

    TabletMetaSharedPtr tablet_meta;
    Status st = TabletMeta::create(request, TabletUid(321, 456), 987 /*shared_id*/, 20000 /*next_unique_id*/,
                                   col_ordinal_to_unique_id, &tablet_meta);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(tablet_meta != nullptr);

    ASSERT_EQ(TabletUid(321, 456), tablet_meta->tablet_uid());
    ASSERT_EQ(request.partition_id, tablet_meta->partition_id());
    ASSERT_EQ(request.tablet_id, tablet_meta->tablet_id());
    ASSERT_EQ(request.tablet_schema.schema_hash, tablet_meta->schema_hash());
    ASSERT_EQ(987, tablet_meta->shard_id());
    ASSERT_EQ(0, tablet_meta->num_rows());

    const TabletSchema& tablet_schema = tablet_meta->tablet_schema();
    ASSERT_EQ(3, tablet_schema.num_columns());
    ASSERT_EQ(KeysType::DUP_KEYS, tablet_schema.keys_type());

    const TabletColumn& c0 = tablet_schema.column(0);
    const TabletColumn& c1 = tablet_schema.column(1);
    const TabletColumn& c2 = tablet_schema.column(2);

    const int kInvalidUniqueId = -1;

    // check c0.
    ASSERT_EQ(col_ordinal_to_unique_id[0], c0.unique_id());
    ASSERT_EQ("c0", c0.name());
    ASSERT_EQ(TYPE_INT, c0.type());
    ASSERT_TRUE(c0.is_key());
    ASSERT_FALSE(c0.is_bf_column());
    ASSERT_TRUE(c0.is_nullable());
    ASSERT_FALSE(c0.has_bitmap_index());
    ASSERT_FALSE(c0.has_default_value());
    ASSERT_EQ(sizeof(int32_t), c0.length());
    ASSERT_EQ(sizeof(int32_t), c0.index_length());
    ASSERT_EQ(STORAGE_AGGREGATE_NONE, c0.aggregation());
    ASSERT_EQ(0, c0.subcolumn_count());

    // check c1.
    ASSERT_EQ(col_ordinal_to_unique_id[1], c1.unique_id());
    ASSERT_EQ("c1", c1.name());
    ASSERT_EQ(TYPE_ARRAY, c1.type());
    ASSERT_FALSE(c1.is_key());
    ASSERT_FALSE(c1.is_bf_column());
    ASSERT_TRUE(c1.is_nullable());
    ASSERT_FALSE(c1.has_bitmap_index());
    ASSERT_FALSE(c1.has_default_value());
    ASSERT_EQ(24, c1.length());
    ASSERT_EQ(24, c1.index_length());
    ASSERT_EQ(STORAGE_AGGREGATE_NONE, c1.aggregation());
    ASSERT_EQ(1, c1.subcolumn_count());

    ASSERT_EQ("element", c1.subcolumn(0).name());
    ASSERT_EQ(kInvalidUniqueId, c1.subcolumn(0).unique_id());
    ASSERT_EQ(TYPE_DECIMALV2, c1.subcolumn(0).type());
    ASSERT_FALSE(c1.subcolumn(0).is_key());
    ASSERT_FALSE(c1.subcolumn(0).is_bf_column());
    ASSERT_TRUE(c1.subcolumn(0).is_nullable());
    ASSERT_FALSE(c1.subcolumn(0).has_bitmap_index());
    ASSERT_FALSE(c1.subcolumn(0).has_default_value());
    ASSERT_EQ(sizeof(DecimalV2Value), c1.subcolumn(0).length());
    ASSERT_EQ(sizeof(DecimalV2Value), c1.subcolumn(0).index_length());

    // check c2.
    ASSERT_EQ(col_ordinal_to_unique_id[2], c2.unique_id());
    ASSERT_EQ("c2", c2.name());
    ASSERT_EQ(TYPE_ARRAY, c2.type());
    ASSERT_FALSE(c2.is_key());
    ASSERT_FALSE(c2.is_bf_column());
    ASSERT_FALSE(c2.is_nullable());
    ASSERT_FALSE(c2.has_bitmap_index());
    ASSERT_FALSE(c2.has_default_value());
    ASSERT_EQ(24, c2.length());
    ASSERT_EQ(24, c2.index_length());
    ASSERT_EQ(STORAGE_AGGREGATE_NONE, c2.aggregation());
    ASSERT_EQ(1, c2.subcolumn_count());

    ASSERT_EQ("element", c2.subcolumn(0).name());
    ASSERT_EQ(kInvalidUniqueId, c2.subcolumn(0).unique_id());
    ASSERT_EQ(TYPE_ARRAY, c2.subcolumn(0).type());
    ASSERT_FALSE(c2.subcolumn(0).is_key());
    ASSERT_FALSE(c2.subcolumn(0).is_bf_column());
    ASSERT_TRUE(c2.subcolumn(0).is_nullable());
    ASSERT_FALSE(c2.subcolumn(0).has_bitmap_index());
    ASSERT_FALSE(c2.subcolumn(0).has_default_value());
    ASSERT_EQ(24, c2.subcolumn(0).length());
    ASSERT_EQ(24, c2.subcolumn(0).index_length());
    ASSERT_EQ(1, c2.subcolumn(0).subcolumn_count());

    const TabletColumn& c2_1 = c2.subcolumn(0);
    ASSERT_EQ("element", c2_1.subcolumn(0).name());
    ASSERT_EQ(kInvalidUniqueId, c2_1.subcolumn(0).unique_id());
    ASSERT_EQ(TYPE_VARCHAR, c2_1.subcolumn(0).type());
    ASSERT_FALSE(c2_1.subcolumn(0).is_key());
    ASSERT_FALSE(c2_1.subcolumn(0).is_bf_column());
    ASSERT_TRUE(c2_1.subcolumn(0).is_nullable());
    ASSERT_FALSE(c2_1.subcolumn(0).has_bitmap_index());
    ASSERT_FALSE(c2_1.subcolumn(0).has_default_value());
    ASSERT_EQ(10 + sizeof(OLAP_STRING_MAX_LENGTH), c2_1.subcolumn(0).length());
    ASSERT_EQ(10 + sizeof(OLAP_STRING_MAX_LENGTH), c2_1.subcolumn(0).index_length());
    ASSERT_EQ(0, c2_1.subcolumn(0).subcolumn_count());

    std::shared_ptr<BinlogConfig> binlog_config_ptr = tablet_meta->get_binlog_config();
    ASSERT_EQ(5, binlog_config_ptr->version);
    ASSERT_TRUE(binlog_config_ptr->binlog_enable);
    ASSERT_EQ(12323, binlog_config_ptr->binlog_ttl_second);
    ASSERT_EQ(23724, binlog_config_ptr->binlog_max_size);
}

TEST(TabletMetaTest, test_init_from_pb) {
    TabletMetaSharedPtr tablet_meta = TabletMeta::create();
    std::shared_ptr<BinlogConfig> binlog_config_ptr = tablet_meta->get_binlog_config();
    ASSERT_TRUE(binlog_config_ptr == nullptr);

    BinlogConfig binlog_config;
    binlog_config.update(3, true, 823, 984);
    tablet_meta->set_binlog_config(binlog_config);
    TabletMetaPB tablet_meta_pb;
    tablet_meta->to_meta_pb(&tablet_meta_pb);

    TabletMetaSharedPtr tablet_meta1 = TabletMeta::create();
    binlog_config_ptr = tablet_meta1->get_binlog_config();
    ASSERT_TRUE(binlog_config_ptr == nullptr);

    tablet_meta1->init_from_pb(&tablet_meta_pb);
    binlog_config_ptr = tablet_meta1->get_binlog_config();
    ASSERT_EQ(3, binlog_config_ptr->version);
    ASSERT_TRUE(binlog_config_ptr->binlog_enable);
    ASSERT_EQ(823, binlog_config_ptr->binlog_ttl_second);
    ASSERT_EQ(984, binlog_config_ptr->binlog_max_size);
}

} // namespace starrocks
