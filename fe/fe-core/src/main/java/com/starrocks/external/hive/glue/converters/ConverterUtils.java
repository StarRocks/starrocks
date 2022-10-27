// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive.glue.converters;

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
