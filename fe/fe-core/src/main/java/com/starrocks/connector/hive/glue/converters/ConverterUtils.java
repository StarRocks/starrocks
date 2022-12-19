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


package com.starrocks.connector.hive.glue.converters;

import com.amazonaws.services.glue.model.Table;
import com.google.gson.Gson;

public class ConverterUtils {

    public static final String INDEX_DEFERRED_REBUILD = "DeferredRebuild";
    public static final String INDEX_TABLE_NAME = "IndexTableName";
    public static final String INDEX_HANDLER_CLASS = "IndexHandlerClass";
    public static final String INDEX_DB_NAME = "DbName";
    public static final String INDEX_ORIGIN_TABLE_NAME = "OriginTableName";
    private static final Gson GSON = new Gson();

    public static String catalogTableToString(final Table table) {
        return GSON.toJson(table);
    }

    public static Table stringToCatalogTable(final String input) {
        return GSON.fromJson(input, Table.class);
    }
}
