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

import com.starrocks.common.exception.MetaNotFoundException;
import com.starrocks.qe.ConnectContext;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public interface AnalyzeJob {
    long getId();

    void setId(long id);

    boolean isNative();

    String getCatalogName() throws MetaNotFoundException;

    String getDbName() throws MetaNotFoundException;

    String getTableName() throws MetaNotFoundException;

    List<String> getColumns();

    StatsConstants.AnalyzeType getAnalyzeType();

    StatsConstants.ScheduleType getScheduleType();

    LocalDateTime getWorkTime();

    void setWorkTime(LocalDateTime workTime);

    String getReason();

    void setReason(String reason);

    StatsConstants.ScheduleStatus getStatus();

    void setStatus(StatsConstants.ScheduleStatus status);

    Map<String, String> getProperties();

    boolean isAnalyzeAllDb();

    boolean isAnalyzeAllTable();

    void run(ConnectContext statsConnectContext, StatisticExecutor statisticExecutor);
}
