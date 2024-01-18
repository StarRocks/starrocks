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


package com.starrocks.alter;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Index;
import com.starrocks.common.exception.UserException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public abstract class AlterJobV2Builder {
    protected long jobId = 0;
    protected long dbId = 0;
    protected long startTime = 0;
    protected long timeoutMs = 0;
    protected boolean bloomFilterColumnsChanged = false;
    protected Set<String> bloomFilterColumns;
    protected double bloomFilterFpp;
    protected boolean hasIndexChanged = false;
    protected List<Index> indexes;
    protected Map<Long, List<Column>> newIndexSchema = new HashMap<>();
    protected Map<Long, Short> newIndexShortKeyCount = new HashMap<>();
    protected List<Integer> sortKeyIdxes;
    protected List<Integer> sortKeyUniqueIds;

    public AlterJobV2Builder() {
    }

    public AlterJobV2Builder withJobId(long jobId) {
        this.jobId = jobId;
        return this;
    }

    public AlterJobV2Builder withDbId(long dbId) {
        this.dbId = dbId;
        return this;
    }

    public AlterJobV2Builder withTimeoutSeconds(long timeout) {
        this.timeoutMs = timeout * 1000;
        return this;
    }

    public AlterJobV2Builder withStartTime(long startTime) {
        this.startTime = startTime;
        return this;
    }

    public AlterJobV2Builder withBloomFilterColumnsChanged(boolean changed) {
        this.bloomFilterColumnsChanged = changed;
        return this;
    }

    public AlterJobV2Builder withBloomFilterColumns(@Nullable Set<String> bfColumns, double bfFpp) {
        this.bloomFilterColumns = bfColumns;
        this.bloomFilterFpp = bfFpp;
        return this;
    }

    public AlterJobV2Builder withAlterIndexInfo(boolean hasIndexChanged, @NotNull List<Index> indexes) {
        this.hasIndexChanged = hasIndexChanged;
        this.indexes = indexes;
        return this;
    }

    public AlterJobV2Builder withNewIndexShortKeyCount(long indexId, short shortKeyCount) {
        this.newIndexShortKeyCount.put(indexId, shortKeyCount);
        return this;
    }

    public AlterJobV2Builder withNewIndexSchema(long indexId, @NotNull List<Column> indexSchema) {
        newIndexSchema.put(indexId, indexSchema);
        return this;
    }

    public AlterJobV2Builder withSortKeyIdxes(@Nullable List<Integer> sortKeyIdxes) {
        this.sortKeyIdxes = sortKeyIdxes;
        return this;
    }

    public AlterJobV2Builder withSortKeyUniqueIds(@Nullable List<Integer> sortKeyUniqueIds) {
        this.sortKeyUniqueIds = sortKeyUniqueIds;
        return this;
    }

    public abstract AlterJobV2 build() throws UserException;
}
