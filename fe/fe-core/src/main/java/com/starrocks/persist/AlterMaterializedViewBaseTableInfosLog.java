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

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.common.io.Writable;

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

    // 0: not set, 1: alter MV add column, 2: alter MV drop column
    @SerializedName("alterType")
    private int alterType = 0;

    // External base table refreshed meta infos
    @SerializedName("baseTableInfoVisibleVersionMap")
    private Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> baseTableInfoVisibleVersionMap;

    @SerializedName(value = "viewDefineSql")
    private String viewDefineSql;
    @SerializedName(value = "simpleDefineSql")
    private String simpleDefineSql;
    @SerializedName(value = "originalViewDefineSql")
    private String originalViewDefineSql;
    @SerializedName(value = "queryOutputIndices")
    protected List<Integer> queryOutputIndices = Lists.newArrayList();

    public static enum AlterType {
        DEFAULT,
        ADD_COLUMN,
        DROP_COLUMN;

        public static AlterType valueOf(int val) {
            switch (val) {
                case 1:
                    return ADD_COLUMN;
                case 2:
                    return DROP_COLUMN;
                default:
                    return DEFAULT;
            }
        }
    }

    public AlterMaterializedViewBaseTableInfosLog(MvId remoteMvId,
                                                  MaterializedView mv) {
        this(remoteMvId, mv, AlterType.DEFAULT);
    }

    public AlterMaterializedViewBaseTableInfosLog(MvId remoteMvId,
                                                  MaterializedView mv,
                                                  AlterType alterType) {
        this.dbId = mv.getDbId();
        this.mvId = mv.getId();

        MaterializedView.AsyncRefreshContext mvContext = mv.getRefreshScheme().getAsyncRefreshContext();

        this.alterType = alterType.ordinal();
        switch (alterType) {
            case ADD_COLUMN: {
                this.viewDefineSql = mv.getViewDefineSql();
                this.simpleDefineSql = mv.getSimpleDefineSql();
                this.originalViewDefineSql = mv.getOriginalViewDefineSql();
                this.baseTableVisibleVersionMap = mvContext.getBaseTableVisibleVersionMap();
                this.baseTableInfoVisibleVersionMap = mvContext.getBaseTableInfoVisibleVersionMap();
                this.queryOutputIndices = mv.getQueryOutputIndices();
                break;
            }
            case DROP_COLUMN: {
                this.viewDefineSql = mv.getViewDefineSql();
                this.simpleDefineSql = mv.getSimpleDefineSql();
                this.originalViewDefineSql = mv.getOriginalViewDefineSql();
                this.queryOutputIndices = mv.getQueryOutputIndices();
                break;
            }
            default: {
                this.remoteMvId = remoteMvId;
                this.baseTableInfos = mv.getBaseTableInfos();
                this.baseTableVisibleVersionMap = mvContext.getBaseTableVisibleVersionMap();
                this.baseTableInfoVisibleVersionMap = mvContext.getBaseTableInfoVisibleVersionMap();
            }
        }
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

    public int getAlterType() {
        return alterType;
    }

    public Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> getBaseTableInfoVisibleVersionMap() {
        return baseTableInfoVisibleVersionMap;
    }

    public String getOriginalViewDefineSql() {
        return originalViewDefineSql;
    }

    public MvId getRemoteMvId() {
        return remoteMvId;
    }

    public String getSimpleDefineSql() {
        return simpleDefineSql;
    }

    public String getViewDefineSql() {
        return viewDefineSql;
    }

    public List<Integer> getQueryOutputIndices() {
        return queryOutputIndices;
    }
}