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

package com.starrocks.connector.benchmark;

import java.util.Locale;

public interface BenchmarkSuite {
    String SCHEMA_RESOURCE_BASE = "/connector/benchmark/schema/";

    String getName();

    long getDbId();

    RowCountEstimate estimateRowCount(String tableName, double scaleFactor);

    String getSchemaFileName();

    default String getSchemaResourcePath() {
        return SCHEMA_RESOURCE_BASE + getSchemaFileName();
    }

    default String normalizeTableName(String tableName) {
        if (tableName == null) {
            return "";
        }
        return tableName.toLowerCase(Locale.ROOT);
    }
}
