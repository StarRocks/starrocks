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

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/tablet_schema.pb.h>

#include <unordered_map>

#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "olap_common.h"
#include "types/logical_type.h"

namespace starrocks {

#define COMMON_PROPERTIES "common_properties"
#define INDEX_PROPERTIES "index_properties"
#define SEARCH_PROPERTIES "search_properties"
#define EXTRA_PROPERTIES "extra_properties"

class TTabletSchema;
class TabletSchemaPB;
enum RowsetTypePB : int;

enum class FieldTypeVersion {
    kV1,
    kV2,
};

// If the columns in |t_schema| do not have a unique id, then the columns in |out_schema| will use the
// column's position in the schema (starting from 0) as their unique id.
Status convert_t_schema_to_pb_schema(const TTabletSchema& t_schema, TCompressionType::type compression_type,
                                     TabletSchemaPB* out_schema);

// If the columns in |t_schema| do not have a unique id, then the columns in |out_schema| will use the
// column's position in the schema (starting at 0) as the key to look up the value in
// |col_ordinal_to_unique_id| for their unique ids.
Status convert_t_schema_to_pb_schema(const TTabletSchema& t_schema, uint32_t next_unique_id,
                                     const std::unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id,
                                     TabletSchemaPB* out_schema, TCompressionType::type compression_type);

void convert_to_new_version(TColumn* tcolumn);

Status t_column_to_pb_column(int32_t unique_id, const TColumn& t_column, ColumnPB* column_pb);

} // namespace starrocks
