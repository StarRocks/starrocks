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

#include <string>
#include <vector>

#include "common/statusor.h"
#include "formats/column_evaluator.h"
#include "formats/parquet/parquet_file_writer.h"
#include "gen_cpp/Descriptors_types.h"
#include "types/type_descriptor.h"

namespace starrocks::connector {

class IcebergUtils {
public:
    static StatusOr<std::string> iceberg_make_partition_name(
            const std::vector<std::string>& partition_column_names,
            const std::vector<std::unique_ptr<ColumnEvaluator>>& column_evaluators,
            const std::vector<std::string>& transform_exprs, Chunk* chunk, bool support_null_partition,
            std::vector<int8_t>& field_is_null);

    static StatusOr<std::string> iceberg_column_value(const TypeDescriptor& type_desc, const ColumnPtr& column,
                                                      const int idx, const std::string& transform_expr,
                                                      int8_t& is_null);

    static std::vector<formats::FileColumnId> generate_parquet_field_ids(
            const std::vector<TIcebergSchemaField>& fields);

    inline const static std::string DATA_DIRECTORY = "/data";
};

} // namespace starrocks::connector
