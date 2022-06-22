// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.scheduler;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.scheduler.persist.Schedule;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class Task implements Writable {

    @SerializedName("id")
    private long id;

    @SerializedName("name")
    private String name;

    // set default to NORMAL is for compatibility
    @SerializedName("type")
    private Constants.TaskType type = Constants.TaskType.NORMAL;

    // set default to NONE is for compatibility
    @SerializedName("state")
    private Constants.TaskState state = Constants.TaskState.NONE;

    @SerializedName("schedule")
    private Schedule schedule;

    @SerializedName("createTime")
    private long createTime;

    @SerializedName("dbName")
    private String dbName;

    @SerializedName("definition")
    private String definition;

    @SerializedName("properties")
    private Map<String, String> properties;

    @SerializedName("expireTime")
    private long expireTime = -1;

    @SerializedName("source")
    private Constants.TaskSource source = Constants.TaskSource.CTAS;

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

    public Schedule getSchedule() {
        return schedule;
    }

    public void setSchedule(Schedule schedule) {
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

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
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

    public void setSource(Constants.TaskSource source) {
        this.source = source;
    }

    public static Task read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Task.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

}
