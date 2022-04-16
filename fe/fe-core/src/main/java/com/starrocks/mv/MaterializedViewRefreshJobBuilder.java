// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.mv;

import com.clearspring.analytics.util.Lists;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.starrocks.statistic.Constants;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class MaterializedViewRefreshJobBuilder  {

    private final long dbId;
    private final long mvTableId;
    private Constants.MaterializedViewRefreshMode mode = Constants.MaterializedViewRefreshMode.ASYNC;
    private Constants.MaterializedViewTriggerType triggerType = Constants.MaterializedViewTriggerType.AUTO;
    private List<IMaterializedViewRefreshTask> tasks;
    private Map<String, String> properties;

    public static MaterializedViewRefreshJobBuilder newBuilder(long dbId, long mvTableId) {
        return new MaterializedViewRefreshJobBuilder(dbId, mvTableId);
    }

    public MaterializedViewRefreshJobBuilder(long dbId, long mvTableId) {
        this.dbId = dbId;
        this.mvTableId = mvTableId;
    }

    public MaterializedViewRefreshJobBuilder mode(Constants.MaterializedViewRefreshMode mode) {
        this.mode = mode;
        return this;
    }

    public MaterializedViewRefreshJobBuilder triggerType(Constants.MaterializedViewTriggerType triggerType) {
        this.triggerType = triggerType;
        return this;
    }

    public MaterializedViewRefreshJobBuilder withRefreshDatePartitionRange(String start, String end) {
        properties = ImmutableMap.of(
                "refresh.filter.type", "date.partition",
                "refresh.partition.start", start,
                "refresh.partition.end", end);
        return this;
    }

    @VisibleForTesting
    public MaterializedViewRefreshJobBuilder setTasksAhead(List<IMaterializedViewRefreshTask> tasks) {
        this.tasks = tasks;
        return this;
    }

    public MaterializedViewRefreshJob build() {
        MaterializedViewRefreshJob job = new MaterializedViewRefreshJob();
        job.setDbId(this.dbId);
        job.setMvTableId(this.mvTableId);
        job.setMode(this.mode);
        job.setTriggerType(this.triggerType);
        job.setCreateTime(LocalDateTime.now());
        if (tasks != null) {
            List<IMaterializedViewRefreshTask> cloneTasks = Lists.newArrayList();
            for (IMaterializedViewRefreshTask task : tasks) {
                cloneTasks.add(task.cloneTask());
            }
            job.setTasks(cloneTasks);
        }
        job.setProperties(properties);
        return job;
    }

    public Long getMvTableId() {
        return mvTableId;
    }
}
