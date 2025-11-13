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

package com.starrocks.catalog.system.statistics;

import static java.util.Objects.requireNonNull;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.statistic.StatsConstants;

// Statistics database used for storing statistics metadata.
public class StatisticsDb extends Database {
    public static final String DATABASE_NAME = StatsConstants.STATISTICS_DB_NAME;

    public StatisticsDb() { super(SystemId.STATISTICS_DB_ID, DATABASE_NAME); }

    @Override
    public Table dropTable(String name) {
        // Do nothing. Prevent dropping tables from statistics database.
        return null;
    }

    @Override
    public Table getTable(String name) {
        return super.getTable(name.toLowerCase());
    }

    public static boolean isStatisticsDb(String dbName) {
        if (dbName == null) {
            return false;
        }
        return DATABASE_NAME.equalsIgnoreCase(dbName);
    }

    public static boolean isStatisticsDb(Long dbID) {
        if (dbID == null) {
            return false;
        }
        return dbID.equals(SystemId.STATISTICS_DB_ID);
    }
}
