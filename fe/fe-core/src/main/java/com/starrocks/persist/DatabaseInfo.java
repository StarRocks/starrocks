// This file is made available under Elastic License 2.0.
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

import com.starrocks.analysis.AlterDatabaseQuotaStmt.QuotaType;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.server.GlobalStateMgr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DatabaseInfo implements Writable {

    private String dbName;
    private String newDbName;
    private long quota;
    private String clusterName;
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

    public static DatabaseInfo read(DataInput in) throws IOException {
        DatabaseInfo dbInfo = new DatabaseInfo();
        dbInfo.readFields(in);
        return dbInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // compatible with old version
        Text.writeString(out, ClusterNamespace.getFullName(dbName));
        if (newDbName.isEmpty()) {
            Text.writeString(out, newDbName);
        } else {
            Text.writeString(out, ClusterNamespace.getFullName(newDbName));
        }
        out.writeLong(quota);
        Text.writeString(out, this.clusterName);
        // compatible with dbState
        Text.writeString(out, "NORMAL");
        Text.writeString(out, this.quotaType.name());
    }

    public void readFields(DataInput in) throws IOException {
        this.dbName = ClusterNamespace.getNameFromFullName(Text.readString(in));
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_10) {
            newDbName = ClusterNamespace.getNameFromFullName(Text.readString(in));
        }
        this.quota = in.readLong();
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_30) {
            this.clusterName = Text.readString(in);
            // Compatible with dbState
            Text.readString(in);
        }
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_81) {
            this.quotaType = QuotaType.valueOf(Text.readString(in));
        }
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
