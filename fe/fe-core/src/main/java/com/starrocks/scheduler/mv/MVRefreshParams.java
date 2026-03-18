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
import com.starrocks.common.Config;
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

    public boolean isForce() {
<<<<<<< HEAD
        return isForce;
=======
        if (this.isTentative) {
            return true;
        }
        if (Boolean.parseBoolean(properties.get(TaskRun.FORCE))) {
            return true;
        }
        MaterializedView.PartitionRefreshStrategy partitionRefreshStrategy =
                mv.getPartitionRefreshStrategy();
        if (partitionRefreshStrategy == MaterializedView.PartitionRefreshStrategy.FORCE) {
            return true;
        }
        // Check if force refresh is enabled for this partition type via config
        return isForceRefreshByConfig();
    }

    /**
     * Check if force refresh is enabled for this MV's partition type via config.
     * Config value is a bitmap:
     * - 0: disabled (default)
     * - 1: force refresh non-partitioned MV
     * - 2: force refresh range partitioned MV
     * - 4: force refresh list partitioned MV
     */
    private boolean isForceRefreshByConfig() {
        int configValue = Config.mv_refresh_force_partition_type;
        if (configValue == 0) {
            return false;
        }
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (partitionInfo.isUnPartitioned() && (configValue & 1) != 0) {
            return true;
        }
        if (partitionInfo.isRangePartition() && (configValue & 2) != 0) {
            return true;
        }
        if (partitionInfo.isListPartition() && (configValue & 4) != 0) {
            return true;
        }
        return false;
    }

    public boolean isNonTentativeForce() {
        return isForce() && !isTentative;
    }

    public void setIsTentative(boolean tentative) {
        this.isTentative = tentative;
    }

    public boolean isTentative() {
        return isTentative;
>>>>>>> 90e2b980c4 ([BugFix] Add mv_refresh_force_partition_type config to support force mv refresh  (#70381))
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
