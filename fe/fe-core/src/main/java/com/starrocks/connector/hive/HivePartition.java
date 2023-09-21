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

package com.starrocks.connector.hive;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Column;

import java.util.List;
import java.util.Map;

public class HivePartition {
    private final String databaseName;
    private final String tableName;
    private final List<String> values;
    private String location;
    private final HiveStorageFormat storage;
    private final List<Column> columns;
    private final Map<String, String> parameters;

    public HivePartition(String databaseName, String tableName, List<String> values, String location,
                         HiveStorageFormat storage, List<Column> columns, Map<String, String> parameters) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.values = values;
        this.location = location;
        this.storage = storage;
        this.columns = columns;
        this.parameters = parameters;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getValues() {
        return values;
    }

    public HiveStorageFormat getStorage() {
        return storage;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public String getLocation() {
        return location;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String databaseName;
        private String tableName;
        private HiveStorageFormat storageFormat;
        private List<String> values;
        private List<Column> columns;
        private String location;
        private Map<String, String> parameters = ImmutableMap.of();

        private Builder() {
        }

        public Builder setDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setValues(List<String> values) {
            this.values = values;
            return this;
        }

        public Builder setStorageFormat(HiveStorageFormat storageFormat) {
            this.storageFormat = storageFormat;
            return this;
        }

        public Builder setColumns(List<Column> columns) {
            this.columns = columns;
            return this;
        }

        public Builder setParameters(Map<String, String> parameters) {
            this.parameters = parameters;
            return this;
        }

        public Builder setLocation(String location) {
            this.location = location;
            return this;
        }

        public HivePartition build() {
            return new HivePartition(databaseName, tableName, values, location, storageFormat, columns, parameters);
        }
    }
}
