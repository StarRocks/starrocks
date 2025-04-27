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

package com.starrocks.sql.optimizer.statistics;

import java.util.Objects;

/**
 * Class for handling NDV statistics for external tables
 */
public class ExternalColumnNDVStats {

    /**
     * Key class for caching external table NDV statistics
     */
    public static class ExternalColumnNDVStatsKey {
        private final String tableUUID;
        private final String catalogName;
        private final String dbName;
        private final String tableName;

        public ExternalColumnNDVStatsKey(String tableUUID, String catalogName, String dbName, String tableName) {
            this.tableUUID = tableUUID;
            this.catalogName = catalogName;
            this.dbName = dbName;
            this.tableName = tableName;
        }

        public String getTableUUID() {
            return tableUUID;
        }

        public String getCatalogName() {
            return catalogName;
        }

        public String getDbName() {
            return dbName;
        }

        public String getTableName() {
            return tableName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ExternalColumnNDVStatsKey that = (ExternalColumnNDVStatsKey) o;
            return Objects.equals(tableUUID, that.tableUUID) &&
                   Objects.equals(catalogName, that.catalogName) &&
                   Objects.equals(dbName, that.dbName) &&
                   Objects.equals(tableName, that.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableUUID, catalogName, dbName, tableName);
        }

        @Override
        public String toString() {
            return "ExternalColumnNDVStatsKey{" +
                    "tableUUID='" + tableUUID + '\'' +
                    ", catalogName='" + catalogName + '\'' +
                    ", dbName='" + dbName + '\'' +
                    ", tableName='" + tableName + '\'' +
                    '}';
        }
    }
}
