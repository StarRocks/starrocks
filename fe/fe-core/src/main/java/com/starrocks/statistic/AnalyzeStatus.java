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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public interface AnalyzeStatus {

    ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("Table", ScalarType.createVarchar(20)))
            .addColumn(new Column("Op", ScalarType.createVarchar(20)))
            .addColumn(new Column("Msg_type", ScalarType.createVarchar(20)))
            .addColumn(new Column("Msg_text", ScalarType.createVarchar(200)))
            .build();

    long getId();

    boolean isNative();

    String getCatalogName() throws MetaNotFoundException;

    String getDbName() throws MetaNotFoundException;

    String getTableName() throws MetaNotFoundException;

    List<String> getColumns();

    StatsConstants.AnalyzeType getType();

    StatsConstants.ScheduleType getScheduleType();

    Map<String, String> getProperties();

    LocalDateTime getStartTime();

    LocalDateTime getEndTime();

    void setEndTime(LocalDateTime endTime);

    void setStatus(StatsConstants.ScheduleStatus status);

    StatsConstants.ScheduleStatus getStatus();

    String getReason();

    void setReason(String reason);

    long getProgress();

    void setProgress(long progress);

    ShowResultSet toShowResult();
}
