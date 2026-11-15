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

package com.starrocks.connector.iceberg;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;

import java.util.List;
import java.util.Map;

public class MockIcebergTable extends IcebergTable {

    // Hold the synthesized native table separately from IcebergTable's own (clearable) field.
    private final org.apache.iceberg.Table mockNativeTable;

    public MockIcebergTable(long id, String srTableName, String catalogName, String resourceName, String remoteDbName,
                        String remoteTableName, List<Column> schema, org.apache.iceberg.Table nativeTable,
                        Map<String, String> icebergProperties, String comment) {
        super(id, srTableName, catalogName, resourceName, remoteDbName, remoteTableName, comment, schema,
                nativeTable, icebergProperties);
        this.mockNativeTable = nativeTable;
    }

    // The offline replay table always serves its pre-built native iceberg table. Bypass IcebergTable's lazy
    // getNativeTable(), which reloads through MetadataMgr.getTable whenever the field is null:
    // IcebergScanNode.clear() calls clearMetadata() (nulling that field) after a scan, and in the replay env
    // the reload resolves back to THIS same mock (whose field is now null), so the lazy path recurses into a
    // StackOverflowError. Returning the stored reference keeps the table usable across repeated scans (e.g. an
    // MV over iceberg base tables, which scans them during refresh and rewrite).
    @Override
    public org.apache.iceberg.Table getNativeTable() {
        return mockNativeTable;
    }
}
