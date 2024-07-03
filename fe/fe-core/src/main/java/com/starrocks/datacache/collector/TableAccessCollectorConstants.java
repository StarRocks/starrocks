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

package com.starrocks.datacache.collector;

public class TableAccessCollectorConstants {
    // constants for table_access_statistics table
    public static final String TABLE_ACCESS_STATISTICS_TABLE_NAME = "table_access_statistics";
    public static final String CATALOG_NAME = "catalog_name";
    public static final String DATABASE_NAME = "database_name";
    public static final String TABLE_NAME = "table_name";
    public static final String PARTITION_NAME = "partition_name";
    public static final String COLUMN_NAME = "column_name";
    public static final String ACCESS_TIME_NAME = "access_time";
    public static final String COUNT_NAME = "count";
}