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
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class GetRemoteFilesParams {
    private List<PartitionKey> partitionKeys;
    private List<String> partitionNames;
    private List<Object> partitionAttachments;
    private TvrVersionRange tableVersionRange;
    private ScalarOperator predicate;
    private List<String> fieldNames;
    private long limit = -1;
    private boolean useCache = true;
    private boolean checkPartitionExistence = true;
    private boolean enableColumnStats = false;
    private Optional<Boolean> isRecursive = Optional.empty();
    private boolean usedForDelete = false;
    // Bounded-cost statistics-scan budgets. Each cap <= 0 means that dimension is unlimited; all three
    // <= 0 (the default) disables the whole mechanism, so ordinary reads are byte-for-byte unaffected.
    // A lazy split iterator that carries any positive cap accumulates as it emits tasks and stops early
    // once a cap is reached, turning an oversized (partition, column) statistics scan into a bounded one.
    private long scanBytesCap = -1;
    private long scanFilesCap = -1;
    private long scanRowsCap = -1;

    protected GetRemoteFilesParams(Builder builder) {
        this.partitionKeys = builder.partitionKeys;
        this.partitionNames = builder.partitionNames;
        this.partitionAttachments = builder.partitionAttachments;
        this.tableVersionRange = builder.tableVersionRange;
        this.predicate = builder.predicate;
        this.fieldNames = builder.fieldNames;
        this.limit = builder.limit;
        this.useCache = builder.useCache;
        this.checkPartitionExistence = builder.checkPartitionExistence;
        this.enableColumnStats = builder.enableColumnStats;
        this.isRecursive = builder.isRecursive;
        this.usedForDelete = builder.usedForDelete;
        this.scanBytesCap = builder.scanBytesCap;
        this.scanFilesCap = builder.scanFilesCap;
        this.scanRowsCap = builder.scanRowsCap;
    }

    public int getPartitionSize() {
        if (partitionKeys != null) {
            return partitionKeys.size();
        }
        if (partitionNames != null) {
            return partitionNames.size();
        }
        return 0;
    }

    public GetRemoteFilesParams copy() {
        Builder paramsBuilder = GetRemoteFilesParams.newBuilder()
                .setPartitionKeys(partitionKeys)
                .setPartitionNames(partitionNames)
                .setPartitionAttachments(partitionAttachments)
                .setTableVersionRange(tableVersionRange)
                .setPredicate(predicate)
                .setFieldNames(fieldNames)
                .setLimit(limit)
                .setUseCache(useCache)
                .setCheckPartitionExistence(checkPartitionExistence)
                .setEnableColumnStats(enableColumnStats)
                .setUsedForDelete(usedForDelete)
                .setScanBytesCap(scanBytesCap)
                .setScanFilesCap(scanFilesCap)
                .setScanRowsCap(scanRowsCap);
        if (isRecursive.isPresent()) {
            paramsBuilder.setIsRecursive(isRecursive.get());
        }
        return paramsBuilder.build();
    }

    public GetRemoteFilesParams sub(int start, int end) {
        GetRemoteFilesParams p = copy();
        if (p.partitionKeys != null) {
            p.partitionKeys = p.partitionKeys.subList(start, end);
        }
        if (p.partitionNames != null) {
            p.partitionNames = p.partitionNames.subList(start, end);
        }
        if (p.partitionAttachments != null) {
            p.partitionAttachments = p.partitionAttachments.subList(start, end);
        }
        return p;
    }

    @SuppressWarnings("unchecked")
    public <T extends GetRemoteFilesParams> T cast() {
        return (T) this;
    }

    // Getters
    public List<PartitionKey> getPartitionKeys() {
        return partitionKeys;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public List<Object> getPartitionAttachments() {
        return partitionAttachments;
    }

    public TvrVersionRange getTableVersionRange() {
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

    public void setUseCache(boolean useCache) {
        this.useCache = useCache;
    }

    public void setPredicate(ScalarOperator predicate) {
        this.predicate = predicate;
    }

    public void setTableVersionRange(TvrVersionRange tableVersionRange) {
        this.tableVersionRange = tableVersionRange;
    }

    public boolean isCheckPartitionExistence() {
        return checkPartitionExistence;
    }

    public boolean isEnableColumnStats() {
        return enableColumnStats;
    }

    public Optional<Boolean> getIsRecursive() {
        return isRecursive;
    }

    public boolean usedForDelete() {
        return usedForDelete;
    }

    public long getScanBytesCap() {
        return scanBytesCap;
    }

    public long getScanFilesCap() {
        return scanFilesCap;
    }

    public long getScanRowsCap() {
        return scanRowsCap;
    }

    // True when at least one bounded-cost statistics-scan budget is active. Used by connectors to
    // decide whether to bypass the split cache and enforce early stop; false leaves reads untouched.
    public boolean hasScanBudget() {
        return scanBytesCap > 0 || scanFilesCap > 0 || scanRowsCap > 0;
    }

    public static class Builder {
        private List<PartitionKey> partitionKeys;
        private List<String> partitionNames;
        private List<Object> partitionAttachments;
        private TvrVersionRange tableVersionRange;
        private ScalarOperator predicate;
        private List<String> fieldNames;
        private long limit = -1;
        private boolean useCache = true;
        private boolean checkPartitionExistence = true;
        private boolean enableColumnStats = false;
        private Optional<Boolean> isRecursive = Optional.empty();
        private boolean usedForDelete = false;
        private long scanBytesCap = -1;
        private long scanFilesCap = -1;
        private long scanRowsCap = -1;

        public Builder setPartitionKeys(List<PartitionKey> partitionKeys) {
            this.partitionKeys = partitionKeys;
            return this;
        }

        public Builder setPartitionAttachments(List partitionAttachments) {
            this.partitionAttachments = partitionAttachments;
            return this;
        }

        public Builder setPartitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        public Builder setTableVersionRange(TvrVersionRange tableVersionRange) {
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

        public Builder setEnableColumnStats(boolean enableColumnStats) {
            this.enableColumnStats = enableColumnStats;
            return this;
        }

        public Builder setIsRecursive(boolean isRecursive) {
            this.isRecursive = Optional.of(isRecursive);
            return this;
        }

        public Builder setUsedForDelete(boolean usedForDelete) {
            this.usedForDelete = usedForDelete;
            return this;
        }

        public Builder setScanBytesCap(long scanBytesCap) {
            this.scanBytesCap = scanBytesCap;
            return this;
        }

        public Builder setScanFilesCap(long scanFilesCap) {
            this.scanFilesCap = scanFilesCap;
            return this;
        }

        public Builder setScanRowsCap(long scanRowsCap) {
            this.scanRowsCap = scanRowsCap;
            return this;
        }

        public GetRemoteFilesParams build() {
            return new GetRemoteFilesParams(this);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public List<GetRemoteFilesParams> partitionExponentially(int minSize, int maxSize) {
        List<GetRemoteFilesParams> result = new ArrayList<>();
        int currentSize = minSize;
        int start = 0;
        int partitionSize = getPartitionSize();
        while (start < partitionSize) {
            int end = Math.min(start + currentSize, partitionSize);
            result.add(sub(start, end));
            start = end;
            currentSize = Math.min(currentSize * 2, maxSize);
        }
        return result;
    }
}
