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
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AlterMaterializedViewBaseTableInfosLog implements Writable {

    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "mvId")
    private long mvId;
    @SerializedName(value = "baseTableInfos")
    private List<BaseTableInfo> baseTableInfos;
    @SerializedName(value = "remoteMvId")
    private MvId remoteMvId;
    @SerializedName("baseTableVisibleVersionMap")
    private Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap;

    public AlterMaterializedViewBaseTableInfosLog(
            long dbId, long mvId, MvId remoteMvId,
            List<BaseTableInfo> baseTableInfos,
            Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMa) {
        this.dbId = dbId;
        this.mvId = mvId;
        this.remoteMvId = remoteMvId;
        this.baseTableInfos = baseTableInfos;
        this.baseTableVisibleVersionMap = baseTableVisibleVersionMa;
    }

    public long getDbId() {
        return dbId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public long getMvId() {
        return mvId;
    }

    public void setMvId(long mvId) {
        this.mvId = mvId;
    }

    public List<BaseTableInfo> getBaseTableInfos() {
        return baseTableInfos;
    }

    public void setBaseTableInfos(List<BaseTableInfo> baseTableInfos) {
        this.baseTableInfos = baseTableInfos;
    }

    public Map<Long, Map<String, MaterializedView.BasePartitionInfo>> getBaseTableVisibleVersionMap() {
        return baseTableVisibleVersionMap;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static AlterMaterializedViewBaseTableInfosLog read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), AlterMaterializedViewBaseTableInfosLog.class);
    }
}