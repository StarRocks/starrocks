// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/vectorized/schema_scanner/schema_dummy_scanner.h"

#include "runtime/string_value.h"

namespace starrocks::vectorized {

SchemaScanner::ColumnDesc SchemaDummyScanner::_s_dummy_columns[] = {};

SchemaDummyScanner::SchemaDummyScanner()
        : SchemaScanner(_s_dummy_columns, sizeof(_s_dummy_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaDummyScanner::~SchemaDummyScanner() = default;

Status SchemaDummyScanner::start() {
    return Status::OK();
}

Status SchemaDummyScanner::get_next(ChunkPtr* chunk, bool* eos) {
    *eos = true;
    return Status::OK();
}

} // namespace starrocks::vectorized
