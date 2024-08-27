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


package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.scheduler.persist.TaskSchedule;
import com.starrocks.sql.ast.SubmitTaskStmt;
import org.apache.commons.collections.MapUtils;

import java.util.Map;

public class TaskAnalyzer {

    public static void analyzeSubmitTaskStmt(SubmitTaskStmt submitTaskStmt, ConnectContext session) {
        String catalogName = submitTaskStmt.getCatalogName();
        if (Strings.isNullOrEmpty(catalogName)) {
            catalogName = session.getCurrentCatalog();
        }

        String dbName = submitTaskStmt.getDbName();
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = session.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
        submitTaskStmt.setCatalogName(catalogName);
        submitTaskStmt.setDbName(dbName);
        analyzeTaskProperties(submitTaskStmt.getProperties());
        analyzeTaskSchedule(submitTaskStmt.getSchedule());
    }

    private static void analyzeTaskSchedule(TaskSchedule schedule) {
        if (schedule == null) {
            return;
        }
        long seconds = schedule.getTimeUnit().toSeconds(schedule.getPeriod());
        if (seconds < Config.task_min_schedule_interval_s) {
            ErrorReport.reportSemanticException("schedule interval is too small, the minimum value is %d SECONDS",
                    ErrorCode.ERR_INVALID_PARAMETER,
                    Config.task_min_schedule_interval_s);
        }
        if (schedule.getStartTime() == 0) {
            schedule.setStartTime(TimeUtils.getEpochSeconds());
        }
    }

    public static void analyzeTaskProperties(Map<String, String> properties) {
        if (MapUtils.isEmpty(properties)) {
            return;
        }
        String value = properties.get(SessionVariable.WAREHOUSE_NAME);
        if (value != null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_PARAMETER, SessionVariable.WAREHOUSE_NAME);
        }
    }

}
