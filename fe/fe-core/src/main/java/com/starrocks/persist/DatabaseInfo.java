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
import com.starrocks.common.io.Writable;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt.QuotaType;

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
    @SerializedName("svId")
    private String storageVolumeId;

    public DatabaseInfo() {
        // for persist
        this.dbName = "";
        this.newDbName = "";
        this.quota = 0;
        this.clusterName = "";
        this.quotaType = QuotaType.DATA;
        this.storageVolumeId = "";
    }

    public static DatabaseInfo newRenameInfo(String dbName, String newDbName) {
        return new DatabaseInfo(dbName, newDbName, -1L, QuotaType.NONE, "");
    }

    public static DatabaseInfo newQuotaUpdateInfo(String dbName, long quota, QuotaType quotaType) {
        return new DatabaseInfo(dbName, "", quota, quotaType, "");
    }

    public static DatabaseInfo newStorageVolumeUpdateInfo(String dbName, String storageVolumeId) {
        return new DatabaseInfo(dbName, "", -1L, QuotaType.NONE, storageVolumeId);
    }

    /**
     * Internal constructor used by factory methods to create {@link DatabaseInfo} instances
     * for different kinds of database metadata updates (rename, quota update, storage volume update).
     *
     * @param dbName           original database name
     * @param newDbName        new database name when renaming, or empty when not applicable
     * @param quota            database quota value, or -1 when quota is not being modified
     * @param quotaType        database quota type, or {@link QuotaType#NONE} when quota is not being modified
     * @param storageVolumeId  storage volume id when updating storage volume, or empty when not applicable
     */
    private DatabaseInfo(String dbName, String newDbName, long quota, QuotaType quotaType, String storageVolumeId) {
        this.dbName = dbName;
        this.newDbName = newDbName;
        this.quota = quota;
        this.clusterName = "";
        this.quotaType = quotaType;
        this.storageVolumeId = storageVolumeId;
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

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public QuotaType getQuotaType() {
        return quotaType;
    }

    public String getStorageVolumeId() {
        return storageVolumeId;
    }
}
