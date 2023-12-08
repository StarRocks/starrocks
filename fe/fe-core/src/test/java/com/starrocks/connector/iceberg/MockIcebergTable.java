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
    private final String tableIdentifier;

    public MockIcebergTable(long id, String srTableName, String catalogName, String resourceName, String remoteDbName,
                        String remoteTableName, List<Column> schema, org.apache.iceberg.Table nativeTable,
                        Map<String, String> icebergProperties, String tableIdentifier) {
        super(id, srTableName, catalogName, resourceName, remoteDbName, remoteTableName, schema,
                nativeTable, icebergProperties);
        this.tableIdentifier = tableIdentifier;
    }

    @Override
    public String getTableIdentifier() {
        return this.tableIdentifier;
    }
}
