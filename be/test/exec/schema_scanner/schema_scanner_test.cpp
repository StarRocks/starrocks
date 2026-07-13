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

#include <gtest/gtest.h>

#include "exec/builtin_schema_scanner_factory.h"
#include "exec/schema_scanner/schema_dummy_scanner.h"
#include "exec/schema_scanner/schema_tables_scanner.h"
#include "exec/schema_scanner/schema_tablet_reshard_jobs_scanner.h"
#include "gen_cpp/Descriptors_types.h"

namespace starrocks {

class SchemaScannerTest : public ::testing::Test {};

TEST_F(SchemaScannerTest, test_create) {
    auto factory = create_builtin_schema_scanner_factory();
    {
        auto scanner = factory->create(TSchemaTableType::SCH_TABLET_RESHARD_JOBS);
        ASSERT_NE(scanner, nullptr);
        auto* reshard_jobs_scanner = dynamic_cast<SchemaTabletReshardJobsScanner*>(scanner.get());
        ASSERT_NE(reshard_jobs_scanner, nullptr);
    }
    {
        // Test an existing one to ensure it still works
        auto scanner = factory->create(TSchemaTableType::SCH_TABLES);
        ASSERT_NE(scanner, nullptr);
        ASSERT_NE(dynamic_cast<SchemaTablesScanner*>(scanner.get()), nullptr);
    }
    {
        // Test default case
        auto scanner = factory->create(static_cast<TSchemaTableType::type>(-1));
        ASSERT_NE(scanner, nullptr);
        ASSERT_NE(dynamic_cast<SchemaDummyScanner*>(scanner.get()), nullptr);
    }
}

} // namespace starrocks
