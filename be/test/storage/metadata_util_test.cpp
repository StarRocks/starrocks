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

#include "storage/metadata_util.h"

#include <gtest/gtest.h>

#include <vector>

#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/Descriptors_types.h"

namespace starrocks {

TEST(MetadataUtilTest, test_convert_schema_compression_settings) {
    auto setup_basic_schema = [](TTabletSchema& schema) {
        schema.__set_schema_hash(12345);
        schema.__set_keys_type(TKeysType::DUP_KEYS);
        schema.__set_short_key_column_count(1);
        schema.columns.emplace_back();
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
    };

    // Test 1: Both ZSTD and level 9 set
    {
        TTabletSchema schema;
        setup_basic_schema(schema);
        schema.__set_compression_type(TCompressionType::ZSTD);
        schema.__set_compression_level(9);
        schema.__isset.compression_type = true;
        schema.__isset.compression_level = true;

        TabletSchemaPB schema_pb;
        ASSERT_TRUE(convert_t_schema_to_pb_schema(schema, &schema_pb).ok());
        ASSERT_EQ(schema_pb.compression_type(), ZSTD);
        ASSERT_EQ(schema_pb.compression_level(), 9);
        ASSERT_EQ(schema_pb.column().size(), 1);
        ASSERT_EQ(schema_pb.column(0).name(), "c0");
        ASSERT_EQ(schema_pb.column(0).type(), "INT");
    }

    // Test 2: Only ZSTD set
    {
        TTabletSchema schema;
        setup_basic_schema(schema);
        schema.__set_compression_type(TCompressionType::ZSTD);
        schema.__isset.compression_type = true;
        schema.__isset.compression_level = false;

        TabletSchemaPB schema_pb;
        ASSERT_TRUE(convert_t_schema_to_pb_schema(schema, &schema_pb).ok());
        ASSERT_EQ(schema_pb.compression_type(), ZSTD);
        ASSERT_EQ(schema_pb.compression_level(), -1);
    }

    // Test 3: Only level 9 set
    {
        TTabletSchema schema;
        setup_basic_schema(schema);
        schema.__set_compression_level(9);
        schema.__isset.compression_type = false;
        schema.__isset.compression_level = true;

        TabletSchemaPB schema_pb;
        ASSERT_TRUE(convert_t_schema_to_pb_schema(schema, &schema_pb).ok());
        ASSERT_EQ(schema_pb.compression_type(), LZ4_FRAME);
        ASSERT_EQ(schema_pb.compression_level(), 9);
    }

    // Test 4: Neither compression_type nor compression_level set
    {
        TTabletSchema schema;
        setup_basic_schema(schema);
        schema.__isset.compression_type = false;
        schema.__isset.compression_level = false;

        TabletSchemaPB schema_pb;
        ASSERT_TRUE(convert_t_schema_to_pb_schema(schema, &schema_pb).ok());
        ASSERT_EQ(schema_pb.compression_type(), LZ4_FRAME);
        ASSERT_EQ(schema_pb.compression_level(), -1);
    }
}

} // namespace starrocks
