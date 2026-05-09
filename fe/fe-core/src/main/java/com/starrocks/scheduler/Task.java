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

package com.starrocks.scheduler;

import com.google.gson.annotations.SerializedName;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.scheduler.persist.TaskSchedule;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Task implements Writable, GsonPostProcessable {

    @SerializedName("id")
    private long id;

    @SerializedName("name")
    private String name;

    // set default to MANUAL is for compatibility
    @SerializedName("type")
    private Constants.TaskType type = Constants.TaskType.MANUAL;

    // set default to UNKNOWN is for compatibility
    @SerializedName("state")
    private Constants.TaskState state = Constants.TaskState.UNKNOWN;

    @SerializedName("schedule")
    private TaskSchedule schedule;

    @SerializedName("createTime")
    private long createTime;

    @SerializedName("catalogName")
    private String catalogName;

    @SerializedName("dbName")
    private String dbName;

    @SerializedName("definition")
    private String definition;

    @SerializedName("postRun")
    private String postRun;

    @SerializedName("properties")
    private Map<String, String> properties;

    @SerializedName("expireTime")
    private long expireTime = -1;

    @SerializedName("source")
    private Constants.TaskSource source = Constants.TaskSource.CTAS;

    // set default to ROOT is for compatibility
    @SerializedName("createUser")
    @Deprecated
    private String createUser = AuthenticationMgr.ROOT_USER;

    @SerializedName("createUserIdentity")
    private UserIdentity userIdentity;

    // the last time this task is scheduled, unit: second
    @SerializedName("lastScheduleTime")
    private long lastScheduleTime = -1;

    // the next time this task is to be scheduled, unit: second
    @SerializedName("nextScheduleTime")
    private long nextScheduleTime = -1;

    // consecutive failure count, used to mark a task as PAUSE when it exceeds the threshold
    private volatile AtomicInteger consecutiveFailCount = new AtomicInteger();

    public Task() {}

    public Task(String name) {
        this.name = name;
        this.createTime = System.currentTimeMillis();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Constants.TaskType getType() {
        return type;
    }

    public void setType(Constants.TaskType type) {
        this.type = type;
    }

    public Constants.TaskState getState() {
        return state;
    }

    public void setState(Constants.TaskState state) {
        this.state = state;
    }

    public TaskSchedule getSchedule() {
        return schedule;
    }

    public void setSchedule(TaskSchedule schedule) {
        this.schedule = schedule;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getDbName() {
        return ClusterNamespace.getNameFromFullName(dbName);
    }

    public void setDbName(String dbName) {
        // compatible with old version
        this.dbName = ClusterNamespace.getFullName(dbName);
    }

    public String getDefinition() {
        return definition;
    }

    public void setDefinition(String definition) {
        this.definition = definition;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getPropertiesString() {
        return PropertyAnalyzer.stringifyProperties(properties);
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public Long getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(Long expireTime) {
        this.expireTime = expireTime;
    }

    public Constants.TaskSource getSource() {
        return source;
    }

    public String getWarehouseName() {
        // For MV tasks, fetch the warehouse from the MV directly to avoid stale data
        // since MV's warehouse can be changed via ALTER MATERIALIZED VIEW SET WAREHOUSE
        if (source == Constants.TaskSource.MV) {
            MaterializedView mv = TaskBuilder.getMvFromTask(this);
            if (mv != null) {
                return GlobalStateMgr.getCurrentState().getWarehouseMgr()
                        .getWarehouse(mv.getWarehouseId()).getName();
            }
        }
        if (properties != null) {
            return properties.getOrDefault(PropertyAnalyzer.PROPERTIES_WAREHOUSE,
                    WarehouseManager.DEFAULT_WAREHOUSE_NAME);
        } else {
            return WarehouseManager.DEFAULT_WAREHOUSE_NAME;
        }
    }

    public void setSource(Constants.TaskSource source) {
        this.source = source;
    }

    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public void setUserIdentity(UserIdentity userIdentity) {
        this.userIdentity = userIdentity;
    }

    public String getPostRun() {
        return postRun;
    }

    public void setPostRun(String postRun) {
        this.postRun = postRun;
    }

    // unit: second
    public long getLastScheduleTime() {
        return lastScheduleTime;
    }

    // unit: second
    public void setLastScheduleTime(long lastScheduleTime) {
        this.lastScheduleTime = lastScheduleTime;
    }

    // unit: second
    public long getNextScheduleTime() {
        return nextScheduleTime;
    }

    // unit: second
    public void setNextScheduleTime(long nextScheduleTime) {
        this.nextScheduleTime = nextScheduleTime;
    }

    public int getConsecutiveFailCount() {
        return consecutiveFailCount.get();
    }

    public int incConsecutiveFailCount() {
        return consecutiveFailCount.incrementAndGet();
    }

    public void resetConsecutiveFailCount() {
        this.consecutiveFailCount.set(0);
    }

    @Override
    public String toString() {
        return "Task{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", type=" + type +
                ", state=" + state +
                ", schedule=" + schedule +
                ", createTime=" + createTime +
                ", dbName='" + dbName + '\'' +
                ", definition='" + definition + '\'' +
                ", postRun='" + postRun + '\'' +
                ", properties=" + properties +
                ", expireTime=" + expireTime +
                ", source=" + source +
                ", createUser='" + createUser + '\'' +
                ", lastScheduleTime=" + lastScheduleTime +
                ", nextScheduleTime=" + nextScheduleTime +
                ", consecutiveFailCount='" + consecutiveFailCount + '\'' +
                '}';
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (consecutiveFailCount == null) {
            this.consecutiveFailCount = new AtomicInteger();
        }
    }
}
