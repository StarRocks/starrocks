// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.mv;


import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.statistic.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class MaterializedViewRefreshJob implements Writable {

    private static final Logger LOG = LogManager.getLogger(MaterializedViewRefreshJob.class);
    public static final long DEFAULT_UNASSIGNED_ID = -1;

    @SerializedName("id")
    private long id = DEFAULT_UNASSIGNED_ID;

    @SerializedName("dbId")
    private long dbId;

    @SerializedName("mvTableId")
    private long mvTableId;

    @SerializedName("status")
    private Constants.MaterializedViewJobStatus status = Constants.MaterializedViewJobStatus.PENDING;

    @SerializedName("mode")
    private Constants.MaterializedViewRefreshMode mode;

    @SerializedName("triggerType")
    private Constants.MaterializedViewTriggerType triggerType;

    @SerializedName("createTime")
    private LocalDateTime createTime;

    @SerializedName("startTime")
    private LocalDateTime startTime;

    @SerializedName("endTime")
    private LocalDateTime endTime;

    @SerializedName("tasks")
    private List<IMaterializedViewRefreshTask> tasks;

    @SerializedName("properties")
    private Map<String, String> properties;

    @SerializedName("retryTime")
    protected int retryTime = 0;
    @SerializedName("mergeCount")
    private Integer mergeCount = 0;

    private Future<?> future;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getDbId() {
        return dbId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public long getMvTableId() {
        return mvTableId;
    }

    public void setMvTableId(long mvTableId) {
        this.mvTableId = mvTableId;
    }

    public Constants.MaterializedViewJobStatus getStatus() {
        return status;
    }

    public void setStatus(Constants.MaterializedViewJobStatus status) {
        this.status = status;
    }

    public Constants.MaterializedViewRefreshMode getMode() {
        return mode;
    }

    public void setMode(Constants.MaterializedViewRefreshMode mode) {
        this.mode = mode;
    }

    public Constants.MaterializedViewTriggerType getTriggerType() {
        return triggerType;
    }

    public void setTriggerType(Constants.MaterializedViewTriggerType triggerType) {
        this.triggerType = triggerType;
    }

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(LocalDateTime endTime) {
        this.endTime = endTime;
    }

    public List<IMaterializedViewRefreshTask> getTasks() {
        return tasks;
    }

    public void setTasks(List<IMaterializedViewRefreshTask> tasks) {
        this.tasks = tasks;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public Future<?> getFuture() {
        return future;
    }

    public void setFuture(Future<?> future) {
        this.future = future;
    }

    public void generateTasks() {
        // MaterializedView mv = MaterializedViewManager.getMaterializedView(dbId, mvTableId)
        // List<Partitions> partitions = mv.getPartitions()
        MaterializedViewPartitionRefreshTask task = new MaterializedViewPartitionRefreshTask();
        task.setRefreshSQL("insert overwrite xxxx");
        tasks.add(task);
    }

    public void runTasks()  {
        if (tasks == null) {
            LOG.warn("no tasks running for job {}. may forget to generate tasks? ", id);
            return;
        }
        for (IMaterializedViewRefreshTask task : tasks) {
            if (task.getStatus() == Constants.MaterializedViewTaskStatus.FAILED) {
                task.setStatus(Constants.MaterializedViewTaskStatus.PENDING);
            }
            if (task.getStatus() == Constants.MaterializedViewTaskStatus.PENDING) {
                task.beginTask();
                task.setStatus(Constants.MaterializedViewTaskStatus.RUNNING);
                try {
                    task.runTask();
                    task.setStatus(Constants.MaterializedViewTaskStatus.SUCCESS);
                } catch (Exception ex) {
                    task.setStatus(Constants.MaterializedViewTaskStatus.FAILED);
                    LOG.warn(ex.getMessage(), ex);
                    task.setErrMsg(ex.getMessage());
                }
                task.finishTask();
            }
        }
    }

    public Constants.MaterializedViewJobStatus updateStatusAfterDone() {
        if (status == Constants.MaterializedViewJobStatus.RUNNING ||
                status == Constants.MaterializedViewJobStatus.RETRYING) {
            if (tasks == null) {
                status = Constants.MaterializedViewJobStatus.FAILED;
                return status;
            }
            Map<Constants.MaterializedViewTaskStatus, Long> result = tasks.stream().collect(
                    Collectors.groupingBy(IMaterializedViewRefreshTask::getStatus, Collectors.counting())
            );
            Long successCount = result.get(Constants.MaterializedViewTaskStatus.SUCCESS);
            if (successCount == null || successCount == 0)  {
                status = Constants.MaterializedViewJobStatus.FAILED;
            } else if (successCount == tasks.size()) {
                status = Constants.MaterializedViewJobStatus.SUCCESS;
            } else {
                status = Constants.MaterializedViewJobStatus.PARTIAL_SUCCESS;
            }
        }
        return status;
    }

    public Integer getMergeCount() {
        return mergeCount;
    }

    public void incrementMergeCount() {
        this.mergeCount = mergeCount + 1;
    }

    public int getRetryTime() {
        return retryTime;
    }

    public void incrementRetryTime() {
        this.retryTime = retryTime + 1;
    }

    @Override
    public String toString() {
        return "MaterializedViewRefreshJob{" +
                "id=" + id +
                ", dbId=" + dbId +
                ", mvTableId=" + mvTableId +
                ", status=" + status +
                ", mode=" + mode +
                ", triggerType=" + triggerType +
                ", createTime=" + createTime +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", tasks=" + tasks +
                ", properties=" + properties +
                ", retryTime=" + retryTime +
                ", mergeCount=" + mergeCount +
                '}';
    }

    public static MaterializedViewRefreshJob read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, MaterializedViewRefreshJob.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

}
