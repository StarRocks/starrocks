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

#pragma once

#include "gen_cpp/tablet_schema.pb.h"
#include "storage/tablet_schema.h"

namespace starrocks {
class SchemaTestHelper {
public:
    static TabletSchemaPB gen_schema_pb_of_dup(TabletSchema::SchemaId schema_id, size_t num_cols, size_t num_key_cols);
    static TabletSchemaPB gen_varchar_schema_pb_of_dup(TabletSchema::SchemaId schema_id, size_t num_cols,
                                                       size_t num_key_cols);
    static TabletSchemaSPtr gen_schema_of_dup(TabletSchema::SchemaId schema_id, size_t num_cols, size_t num_key_cols);
    static TabletSchemaSPtr gen_varchar_schema_of_dup(TabletSchema::SchemaId schema_id, size_t num_cols,
                                                      size_t num_key_cols);
    static TColumn gen_key_column(const std::string& col_name, TPrimitiveType::type type);
    static TColumn gen_value_column_for_dup_table(const std::string& col_name, TPrimitiveType::type type);
    static TColumn gen_value_column_for_agg_table(const std::string& col_name, TPrimitiveType::type type);
    static void add_column_pb_to_tablet_schema(TabletSchemaPB* tablet_schema_pb, const std::string& name,
                                               const std::string& type, const std::string& agg, uint32_t length);
};
} // namespace starrocks
