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

package com.starrocks.summary;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.starrocks.common.util.DateUtils;

import java.time.LocalDate;
import java.util.Map;

public class TabletStatistics {
    private LocalDate dt;
    private String catalogName;
    private String dbName;
    private String tableName;
    private String partitionName;
    private long tabletId;
    private double readCount;

    public TabletStatistics(String catalogName, String dbName, String tableName,
                            String partitionName, long tabletId, double readCount) {
        this.dt = LocalDate.now();
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.partitionName = partitionName;
        this.tabletId = tabletId;
        this.readCount = readCount;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public long getTabletId() {
        return tabletId;
    }

    public void setTabletId(long tabletId) {
        this.tabletId = tabletId;
    }

    public double getReadCount() {
        return readCount;
    }

    public void setReadCount(double readCount) {
        this.readCount = readCount;
    }

    String toJSON() {
        Map<String, Object> jsonMaps = Maps.newHashMap();
        jsonMaps.put("dt", dt.format(DateUtils.DATE_FORMATTER_UNIX));
        jsonMaps.put("catalog_name", catalogName);
        jsonMaps.put("db_name", dbName);
        jsonMaps.put("table_name", tableName);
        jsonMaps.put("partition_name", partitionName);
        jsonMaps.put("tablet_id", tabletId);
        jsonMaps.put("read_count", readCount);

        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(jsonMaps);
    }
}
