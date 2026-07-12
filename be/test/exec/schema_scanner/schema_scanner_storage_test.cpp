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

#include "base/testutil/assert.h"
#include "common/object_pool.h"
#include "exec/schema_scanner/schema_be_cloud_native_compactions_scanner.h"
#include "exec/schema_scanner/schema_be_compactions_scanner.h"
#include "exec/schema_scanner/schema_be_tablet_write_log_scanner.h"
#include "exec/schema_scanner/schema_be_tablets_scanner.h"
#include "exec/schema_scanner/schema_be_txns_scanner.h"

namespace starrocks {

namespace {

template <typename Scanner>
void expect_scanner_schema_initializes() {
    Scanner scanner;
    SchemaScannerParam params;
    ObjectPool pool;

    EXPECT_OK(scanner.init(&params, &pool));
    EXPECT_FALSE(scanner.get_slot_descs().empty());
}

} // namespace

TEST(SchemaScannerStorageTest, AllScannerSchemasInitialize) {
    expect_scanner_schema_initializes<SchemaBeTabletsScanner>();
    expect_scanner_schema_initializes<SchemaBeTxnsScanner>();
    expect_scanner_schema_initializes<SchemaBeCompactionsScanner>();
    expect_scanner_schema_initializes<SchemaBeCloudNativeCompactionsScanner>();
    expect_scanner_schema_initializes<SchemaBeTabletWriteLogScanner>();
}

} // namespace starrocks
