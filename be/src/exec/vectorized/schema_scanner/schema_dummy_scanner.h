// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks::vectorized {

class SchemaDummyScanner : public SchemaScanner {
public:
    SchemaDummyScanner();
    ~SchemaDummyScanner() override;
    virtual Status start();
    Status get_next(ChunkPtr* chunk, bool* eos) override;

private:
    static SchemaScanner::ColumnDesc _s_dummy_columns[];
};

} // namespace starrocks::vectorized
