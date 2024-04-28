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

package com.starrocks.datacache.statistic;

public class CacheSelectSQLBuilder {
    public static String buildCacheSelectSQL(AccessItem accessItem) {
        StringBuilder sb = new StringBuilder();
        sb.append(
                String.format("CACHE SELECT %s FROM %s.%s.%s", accessItem.getColumnName(), accessItem.getCatalogName(),
                        accessItem.getDbName(), accessItem.getTableName()));
        if (accessItem.getPartitionName().isPresent() && !accessItem.getPartitionName().get().isEmpty()) {
            String[] parts = accessItem.getPartitionName().get().split("/");
            sb.append(" WHERE ");
            for (int i = 0; i < parts.length; i++) {
                if (i > 0) {
                    sb.append(" AND ");
                }
                sb.append(parts[i]);
            }
        }
        sb.append(";");
        return sb.toString();
    }
}
