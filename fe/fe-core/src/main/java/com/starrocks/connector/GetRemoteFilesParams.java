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

package com.starrocks.connector;

import com.starrocks.catalog.PartitionKey;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;

public class GetRemoteFilesParams {
    private List<PartitionKey> partitionKeys;
    private List<String> partitionNames;
    private TableVersionRange tableVersionRange;
    private ScalarOperator predicate;
    private List<String> fieldNames;
    private long limit = -1;
    private boolean useCache = true;
    private boolean checkPartitionExistence = true;

    private GetRemoteFilesParams(Builder builder) {
        this.partitionKeys = builder.partitionKeys;
        this.partitionNames = builder.partitionNames;
        this.tableVersionRange = builder.tableVersionRange;
        this.predicate = builder.predicate;
        this.fieldNames = builder.fieldNames;
        this.limit = builder.limit;
        this.useCache = builder.useCache;
        this.checkPartitionExistence = builder.checkPartitionExistence;
    }

    // Getters
    public List<PartitionKey> getPartitionKeys() {
        return partitionKeys;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public TableVersionRange getTableVersionRange() {
        return tableVersionRange;
    }

    public ScalarOperator getPredicate() {
        return predicate;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public long getLimit() {
        return limit;
    }

    public boolean isUseCache() {
        return useCache;
    }

    public boolean isCheckPartitionExistence() {
        return checkPartitionExistence;
    }

    public static class Builder {
        private List<PartitionKey> partitionKeys;
        private List<String> partitionNames;
        private TableVersionRange tableVersionRange;
        private ScalarOperator predicate;
        private List<String> fieldNames;
        private long limit = -1;
        private boolean useCache = true;
        private boolean checkPartitionExistence = true;

        public Builder setPartitionKeys(List<PartitionKey> partitionKeys) {
            this.partitionKeys = partitionKeys;
            return this;
        }

        public Builder setPartitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        public Builder setTableVersionRange(TableVersionRange tableVersionRange) {
            this.tableVersionRange = tableVersionRange;
            return this;
        }

        public Builder setPredicate(ScalarOperator predicate) {
            this.predicate = predicate;
            return this;
        }

        public Builder setFieldNames(List<String> fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setLimit(long limit) {
            this.limit = limit;
            return this;
        }

        public Builder setUseCache(boolean useCache) {
            this.useCache = useCache;
            return this;
        }

        public Builder setCheckPartitionExistence(boolean checkPartitionExistence) {
            this.checkPartitionExistence = checkPartitionExistence;
            return this;
        }

        public GetRemoteFilesParams build() {
            return new GetRemoteFilesParams(this);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }
}
