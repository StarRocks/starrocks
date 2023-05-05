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

import com.google.common.collect.Lists;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.qe.ShowResultSet;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExternalAnalyzeStatus extends AnalyzeStatus {

    private String catalogName;
    private String dbName;
    private String tableName;
    private String tableUUID;

    public ExternalAnalyzeStatus(long id, long dbId, long tableId,
                                 String catalogName, String dbName, String tableName,
                                 String tableUUID,
                                 List<String> columns,
                                 StatsConstants.AnalyzeType type,
                                 StatsConstants.ScheduleType scheduleType,
                                 Map<String, String> properties,
                                 LocalDateTime startTime) {
        super(id, dbId, tableId, columns, type, scheduleType, properties, startTime);
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.tableUUID = tableUUID;
    }

    public String getTableUUID() {
        return tableUUID;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getDbName() throws MetaNotFoundException {
        return dbName;
    }

    @Override
    public String getTableName() throws MetaNotFoundException {
        return tableName;
    }

    @Override
    public ShowResultSet toShowResult() {
        String op = "unknown";
        if (type.equals(StatsConstants.AnalyzeType.HISTOGRAM)) {
            op = "histogram";
        } else if (type.equals(StatsConstants.AnalyzeType.FULL)) {
            op = "analyze";
        } else if (type.equals(StatsConstants.AnalyzeType.SAMPLE)) {
            op = "sample";
        }

        String msgType;
        String msgText;
        if (status.equals(StatsConstants.ScheduleStatus.FAILED)) {
            msgType = "error";
            msgText = reason;
        } else {
            msgType = "status";
            msgText = "OK";
        }

        List<List<String>> rows = new ArrayList<>();
        rows.add(Lists.newArrayList(catalogName + "." + dbName + "." + tableName, op, msgType, msgText));
        return new ShowResultSet(META_DATA, rows);
    }
}
