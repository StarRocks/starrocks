// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks::vectorized {

class SchemaTriggersScanner : public SchemaScanner {
public:
    SchemaTriggersScanner();
    ~SchemaTriggersScanner() override;

private:
    static SchemaScanner::ColumnDesc _s_cols_triggers[];
};

} // namespace starrocks::vectorized
