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
package com.starrocks.sql.spm;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class BaselinePlan implements Writable {
    public static final String SOURCE_USER = "USER";

    public static final String SOURCE_CAPTURE = "CAPTURE";

    private long id;

    private boolean isGlobal = false;

    private boolean isEnable = false;

    // bind sql with spm function, for extract placeholder
    private final String bindSql;
    // bind sql without spm function, for bind query
    private final String bindSqlDigest;
    // bind sql hash, for fast search
    private final long bindSqlHash;
    // plan sql with hints
    private final String planSql;

    private final double costs;

    private String source = SOURCE_USER;

    private double queryMs = -1;

    private LocalDateTime updateTime;

    // for transform replay log
    @SerializedName("replayIds")
    private List<Long> replayIds;
    @SerializedName("replayBindSqlHash")
    private List<Long> replayBindSQLHash;

    public BaselinePlan(long id, long bindSqlHash) {
        this.id = id;
        this.bindSqlHash = bindSqlHash;
        this.isGlobal = false;
        this.isEnable = false;
        this.bindSql = "";
        this.bindSqlDigest = "";
        this.planSql = "";
        this.costs = 0;
        this.updateTime = null;
    }

    public BaselinePlan(String bindSql, String bindSqlDigest, long bindSqlHash, String planSql, double costs) {
        this.bindSql = bindSql;
        this.bindSqlDigest = bindSqlDigest;
        this.bindSqlHash = bindSqlHash;
        this.planSql = planSql;
        this.costs = costs;
        this.updateTime = LocalDateTime.now();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public boolean isGlobal() {
        return isGlobal;
    }

    public void setGlobal(boolean global) {
        isGlobal = global;
    }

    public boolean isEnable() {
        return isEnable;
    }

    public void setEnable(boolean enable) {
        isEnable = enable;
    }

    public String getSource() {
        return source;
    }

    public String getBindSql() {
        return bindSql;
    }

    public String getBindSqlDigest() {
        return bindSqlDigest;
    }

    public long getBindSqlHash() {
        return bindSqlHash;
    }

    public String getPlanSql() {
        return planSql;
    }

    public double getCosts() {
        return costs;
    }

    public double getQueryMs() {
        return queryMs;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public void setQueryMs(double queryMs) {
        this.queryMs = queryMs;
    }

    public void setUpdateTime(LocalDateTime updateTime) {
        this.updateTime = updateTime;
    }

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }

    public List<Long> getReplayIds() {
        return replayIds;
    }

    public void setReplayIds(List<Long> replayIds) {
        this.replayIds = replayIds;
    }

    public List<Long> getReplayBindSQLHash() {
        return replayBindSQLHash;
    }

    public void setReplayBindSQLHash(List<Long> replayBindSQLHash) {
        this.replayBindSQLHash = replayBindSQLHash;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BaselinePlan that = (BaselinePlan) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bindSqlHash);
    }

    @Override
    public String toString() {
        return "BaselinePlan{" +
                "id=" + id +
                ", bindSqlHash=" + bindSqlHash +
                ", bindSqlDigest='" + bindSqlDigest + '\'' +
                '}';
    }
}
