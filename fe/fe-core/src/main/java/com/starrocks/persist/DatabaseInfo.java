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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/persist/DatabaseInfo.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt.QuotaType;

import java.io.DataOutput;
import java.io.IOException;

public class DatabaseInfo implements Writable {

    @SerializedName("db")
    private String dbName;
    @SerializedName("ndb")
    private String newDbName;
    @SerializedName("qt")
    private long quota;
    @SerializedName("cn")
    private String clusterName;
    @SerializedName("qp")
    private QuotaType quotaType;

    public DatabaseInfo() {
        // for persist
        this.dbName = "";
        this.newDbName = "";
        this.quota = 0;
        this.clusterName = "";
        this.quotaType = QuotaType.DATA;
    }

    public DatabaseInfo(String dbName, String newDbName, long quota, QuotaType quotaType) {
        this.dbName = dbName;
        this.newDbName = newDbName;
        this.quota = quota;
        this.clusterName = "";
        this.quotaType = quotaType;
    }

    public String getDbName() {
        return dbName;
    }

    public String getNewDbName() {
        return newDbName;
    }

    public long getQuota() {
        return quota;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public QuotaType getQuotaType() {
        return quotaType;
    }

}
