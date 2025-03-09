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

import java.time.LocalDateTime;

public class BaselinePlan {
    private long id;

    private final boolean isGlobal;

    // bind sql with spm function, for extract placeholder
    private final String bindSql;
    // bind sql without spm function, for bind query
    private final String bindSqlDigest;
    // bind sql hash, for fast search
    private final long bindSqlHash;
    // plan sql with hints
    private final String planSql;

    private final double costs;

    private final LocalDateTime updateTime;

    public BaselinePlan(boolean isGlobal, String bindSql, String bindSqlDigest,
                        long bindSqlHash,
                        String planSql, double costs) {
        this.isGlobal = isGlobal;
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

    public LocalDateTime getUpdateTime() {
        return updateTime;
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
