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

package com.starrocks.scheduler.mv;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.sql.common.PListCell;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Set;

/**
 * MVRefreshParams is used to store the parameters for refreshing a materialized view.
 */
public class MVRefreshParams {
    private final String rangeStart;
    private final String rangeEnd;
    private final boolean isForce;
    private final Set<PListCell> listValues;
    private final PartitionInfo mvPartitionInfo;

    public MVRefreshParams(PartitionInfo partitionInfo,
                           Map<String, String> properties,
                           boolean tentative) {
        Preconditions.checkArgument(partitionInfo != null, "MaterializedView's partition info is null");
        Preconditions.checkArgument(properties != null, "Properties is null");
        this.rangeStart = properties.get(TaskRun.PARTITION_START);
        this.rangeEnd = properties.get(TaskRun.PARTITION_END);
        this.isForce = tentative | Boolean.parseBoolean(properties.get(TaskRun.FORCE));
        this.listValues = PListCell.batchDeserialize(properties.get(TaskRun.PARTITION_VALUES));
        this.mvPartitionInfo = partitionInfo;
    }

    public boolean isForceCompleteRefresh() {
        return isForce && isCompleteRefresh();
    }

    public boolean isForce() {
        return isForce;
    }

    public boolean isCompleteRefresh() {
        if (mvPartitionInfo.isListPartition())  {
            return isListCompleteRefresh();
        } else if (mvPartitionInfo.isUnPartitioned()) {
            return true;
        } else {
            return isRangeCompleteRefresh();
        }
    }

    private boolean isRangeCompleteRefresh() {
        return StringUtils.isEmpty(rangeStart) && StringUtils.isEmpty(rangeEnd);
    }

    private boolean isListCompleteRefresh() {
        return CollectionUtils.isEmpty(listValues);
    }

    public String getRangeEnd() {
        return rangeEnd;
    }

    public String getRangeStart() {
        return rangeStart;
    }

    public Set<PListCell> getListValues() {
        return listValues;
    }
}
