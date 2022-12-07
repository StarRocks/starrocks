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


package com.starrocks.statistic;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BasicStatsMeta implements Writable {
    @SerializedName("dbId")
    private long dbId;

    @SerializedName("tableId")
    private long tableId;

    @SerializedName("columns")
    private List<String> columns;

    @SerializedName("type")
    private StatsConstants.AnalyzeType type;

    @SerializedName("updateTime")
    private LocalDateTime updateTime;

    @SerializedName("properties")
    private Map<String, String> properties;

    @SerializedName("updateRows")
    private long updateRows;

    public BasicStatsMeta(long dbId, long tableId, List<String> columns,
                          StatsConstants.AnalyzeType type,
                          LocalDateTime updateTime,
                          Map<String, String> properties) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.columns = columns;
        this.type = type;
        this.updateTime = updateTime;
        this.properties = properties;
        this.updateRows = 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String s = GsonUtils.GSON.toJson(this);
        Text.writeString(out, s);
    }

    public static BasicStatsMeta read(DataInput in) throws IOException {
        String s = Text.readString(in);
        return GsonUtils.GSON.fromJson(s, BasicStatsMeta.class);
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public List<String> getColumns() {
        // Just for compatibility, there are no columns in the old code,
        // and the columns may be null after deserialization.
        if (columns == null) {
            return Collections.emptyList();
        }
        return columns;
    }

    public StatsConstants.AnalyzeType getType() {
        return type;
    }

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public double getHealthy() {
        Database database = GlobalStateMgr.getCurrentState().getDb(dbId);
        OlapTable table = (OlapTable) database.getTable(tableId);
        long minRowCount = Long.MAX_VALUE;
        for (Partition partition : table.getPartitions()) {
            if (!partition.hasData()) {
                //skip init empty partition
                continue;
            }
            if (partition.getRowCount() < minRowCount) {
                minRowCount = partition.getRowCount();
            }
        }

        /*
         * The ratio of the number of modified lines to the total number of lines.
         * Because we cannot obtain complete table-level information, we use the row count of
         * the partition with the smallest row count as totalRowCount.
         * It can be understood that we assume an extreme case where all imported and modified lines
         * are concentrated in only one partition
         */
        double healthy;
        if (minRowCount == Long.MAX_VALUE) {
            //All partition is empty
            healthy = 1;
        } else if (updateRows > minRowCount) {
            healthy = 0;
        } else if (minRowCount == 0) {
            // updateRows == 0 && minRowCount == 0
            // If minRowCount == 0 and partition.hasData is true.
            // Indicates that a truncate or delete operation has occurred on this table.
            healthy = 1;
        } else {
            healthy = 1 - (double) updateRows / (double) minRowCount;
        }

        return healthy;
    }

    public long getUpdateRows() {
        return updateRows;
    }

    public void increaseUpdateRows(Long delta) {
        updateRows += delta;
    }
}
