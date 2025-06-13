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

TEST(MetadataUtilTest, testConvertThriftSchemaToPbSchema) {
    TTabletSchema schema;
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
    // c1 ARRAY<FLOAT>
    schema.columns.emplace_back();
    {
        std::vector<TTypeNode> types(2);
        types[0].__set_type(TTypeNodeType::ARRAY);
        types[1].__set_type(TTypeNodeType::SCALAR);
        types[1].scalar_type.__set_type(TPrimitiveType::FLOAT);

        schema.columns.back().__set_column_name("c1");
        schema.columns.back().__set_is_key(false);
        schema.columns.back().__set_index_len(0);
        schema.columns.back().__set_aggregation_type(TAggregationType::NONE);
        schema.columns.back().__set_is_allow_null(false);
        schema.columns.back().__set_type_desc(TTypeDesc());
        schema.columns.back().type_desc.__set_types(types);
    }
    // c2 VARCHAT NOT NULL
    schema.columns.emplace_back();
    {
        std::vector<TTypeNode> types(1);
        types[0].__set_type(TTypeNodeType::SCALAR);
        types[0].scalar_type.__set_type(TPrimitiveType::VARCHAR);

        schema.columns.back().__set_column_name("c2");
        schema.columns.back().__set_is_key(false);
        schema.columns.back().__set_index_len(0);
        schema.columns.back().__set_aggregation_type(TAggregationType::NONE);
        schema.columns.back().__set_is_allow_null(false);
        schema.columns.back().__set_type_desc(TTypeDesc());
        schema.columns.back().type_desc.__set_types(types);
    }

    // gin index
    schema.__isset.indexes = true;
    schema.indexes.emplace_back();
    {
        schema.indexes.back().__set_index_id(0);
        schema.indexes.back().__set_index_name("gin_index");
        schema.indexes.back().__set_index_type(TIndexType::GIN);
        schema.indexes.back().__set_columns({"c2"});
        schema.indexes.back().__set_common_properties({{"imp_lib", "clucene"}});
    }

    // vector index
    schema.indexes.emplace_back();
    {
        schema.indexes.back().__set_index_id(1);
        schema.indexes.back().__set_index_name("vector_index");
        schema.indexes.back().__set_index_type(TIndexType::VECTOR);
        schema.indexes.back().__set_columns({"c1"});
        schema.indexes.back().__set_common_properties(
                {{"index_type", "hnsw"}, {"dim", "10"}, {"metric_type", "l2_distance"}, {"IS_VECTOR_NORMED", "false"}});
        schema.indexes.back().__set_index_properties({
                {"m", "10"},
                {"efconstruction", "10"},
        });
    }

    TabletSchemaPB schemaPb;
    ASSERT_TRUE(convert_t_schema_to_pb_schema(schema, TCompressionType::LZ4, &schemaPb).ok());

    ASSERT_EQ(schemaPb.column().size(), 3);
    ASSERT_EQ(schemaPb.compression_type(), LZ4_FRAME);

    ASSERT_EQ(schemaPb.column(0).name(), "c0");
    ASSERT_EQ(schemaPb.column(0).unique_id(), 0);
    ASSERT_EQ(schemaPb.column(0).is_key(), true);
    ASSERT_EQ(schemaPb.column(0).aggregation(), "none");
    ASSERT_EQ(schemaPb.column(0).type(), "INT");
    ASSERT_EQ(schemaPb.column(0).is_nullable(), true);

    ASSERT_EQ(schemaPb.column(1).name(), "c1");
    ASSERT_EQ(schemaPb.column(1).unique_id(), 1);
    ASSERT_EQ(schemaPb.column(1).is_key(), false);
    ASSERT_EQ(schemaPb.column(1).aggregation(), "none");
    ASSERT_EQ(schemaPb.column(1).type(), "ARRAY");
    ASSERT_EQ(schemaPb.column(1).is_nullable(), false);

    ASSERT_EQ(schemaPb.column(2).name(), "c2");
    ASSERT_EQ(schemaPb.column(2).unique_id(), 2);
    ASSERT_EQ(schemaPb.column(2).is_key(), false);
    ASSERT_EQ(schemaPb.column(2).aggregation(), "none");
    ASSERT_EQ(schemaPb.column(2).type(), "VARCHAR");
    ASSERT_EQ(schemaPb.column(2).is_nullable(), false);

    ASSERT_EQ(schemaPb.table_indices_size(), 2);
    ASSERT_EQ(schemaPb.table_indices(0).index_id(), 0);
    ASSERT_EQ(schemaPb.table_indices(0).index_type(), GIN);
    ASSERT_EQ(schemaPb.table_indices(0).col_unique_id_size(), 1);
    ASSERT_EQ(schemaPb.table_indices(0).col_unique_id(0), 2);
    ASSERT_EQ(schemaPb.table_indices(0).index_name(), "gin_index");

    ASSERT_EQ(schemaPb.table_indices(1).index_id(), 1);
    ASSERT_EQ(schemaPb.table_indices(1).index_type(), VECTOR);
    ASSERT_EQ(schemaPb.table_indices(1).col_unique_id_size(), 1);
    ASSERT_EQ(schemaPb.table_indices(1).col_unique_id(0), 1);
    ASSERT_EQ(schemaPb.table_indices(1).index_name(), "vector_index");
}

} // namespace starrocks
