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

package com.starrocks.connector.metadata;

import com.starrocks.connector.exception.StarRocksConnectorException;

import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetadataTableName {
    private static final Pattern TABLE_PATTERN = Pattern.compile("" +
            "(?<table>[^$@]+)" +
            "(?:\\$(?<type>[^@]+)){1}");

    private final String tableName;
    private final MetadataTableType tableType;

    public static boolean isMetadataTable(String tableName) {
        Matcher match = TABLE_PATTERN.matcher(tableName);
        if (!match.matches()) {
            return false;
        }

        String typeString = match.group("type");
        if (typeString == null) {
            return false;
        }

        MetadataTableType type;
        try {
            type = MetadataTableType.get(typeString);
        } catch (IllegalArgumentException e) {
            return false;
        }

        return true;
    }

    public MetadataTableName(String tableName, MetadataTableType tableType) {
        this.tableName = Objects.requireNonNull(tableName, "tableName is null");
        this.tableType = Objects.requireNonNull(tableType, "tableType is null");
    }

    public String getTableName() {
        return tableName;
    }

    public MetadataTableType getTableType() {
        return tableType;
    }

    public String getTableNameWithType() {
        return tableName + "$" + tableType.name().toLowerCase(Locale.ROOT);
    }

    @Override
    public String toString() {
        return getTableNameWithType();
    }

    public static MetadataTableName from(String name) {
        Matcher match = TABLE_PATTERN.matcher(name);
        if (!match.matches()) {
            throw new StarRocksConnectorException("Invalid metadata table name: " + name);
        }

        String table = match.group("table");
        String typeString = match.group("type");

        if (typeString == null) {
            throw new StarRocksConnectorException("Missing metadata table type");
        }

        MetadataTableType type;
        try {
            type = MetadataTableType.get(typeString);
        } catch (IllegalArgumentException e) {
            throw new StarRocksConnectorException("Invalid metadata table name (unknown type '%s'): %s", typeString, name);
        }

        return new MetadataTableName(table, type);
    }
}
