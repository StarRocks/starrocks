// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/vectorized/schema_scanner/schema_statistics_scanner.h"

#include "runtime/primitive_type.h"
#include "runtime/string_value.h"

namespace starrocks::vectorized {

SchemaScanner::ColumnDesc SchemaStatisticsScanner::_s_cols_statistics[] = {
        //   name,       type,          size,                     is_null
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"NON_UNIQUE", TYPE_BIGINT, sizeof(int64_t), false},
        {"INDEX_SCHEMA", TYPE_VARCHAR, sizeof(StringValue), false},
        {"INDEX_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"SEQ_IN_INDEX", TYPE_BIGINT, sizeof(int64_t), false},
        {"COLUMN_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"COLLATION", TYPE_VARCHAR, sizeof(StringValue), true},
        {"CARDINALITY", TYPE_BIGINT, sizeof(int64_t), true},
        {"SUB_PART", TYPE_BIGINT, sizeof(int64_t), true},
        {"PACKED", TYPE_VARCHAR, sizeof(StringValue), true},
        {"NULLABLE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"INDEX_TYPE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"COMMENT", TYPE_VARCHAR, sizeof(StringValue), true},
        {"INDEX_COMMENT", TYPE_VARCHAR, sizeof(StringValue), false},
};

SchemaStatisticsScanner::SchemaStatisticsScanner()
        : SchemaScanner(_s_cols_statistics, sizeof(_s_cols_statistics) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaStatisticsScanner::~SchemaStatisticsScanner() = default;

} // namespace starrocks::vectorized
