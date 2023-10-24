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

package com.starrocks.connector.iceberg;

import com.starrocks.connector.exception.StarRocksConnectorException;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class IcebergTableName {
    private static final Pattern TABLE_PATTERN = Pattern.compile("" +
            "(?<table>[^$@]+)" +
            "(?:\\$(?<type>[^@]+))?");

    private final String tableName;
    private final IcebergTableType tableType;

    public IcebergTableName(String tableName, IcebergTableType tableType) {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
    }

    public String getTableName() {
        return tableName;
    }

    public IcebergTableType getTableType() {
        return tableType;
    }

    public String getTableNameWithType() {
        return tableName + "$" + tableType.name().toLowerCase(Locale.ROOT);
    }

    @Override
    public String toString() {
        return getTableNameWithType();
    }

    public static IcebergTableName of(String name, String typeString) {
        IcebergTableType type = IcebergTableType.DATA;
        if (typeString != null) {
            try {
                type = IcebergTableType.valueOf(typeString.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new StarRocksConnectorException("Invalid Iceberg table type %s", typeString);
            }
        }
        return new IcebergTableName(name, type);
    }

    public static IcebergTableName from(String name) {
        Matcher match = TABLE_PATTERN.matcher(name);
        if (!match.matches()) {
            throw new StarRocksConnectorException("Invalid Iceberg table name: " + name);
        }

        String table = match.group("table");
        String typeString = match.group("type");

        IcebergTableType type = IcebergTableType.DATA;
        if (typeString != null) {
            try {
                type = IcebergTableType.valueOf(typeString.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new StarRocksConnectorException("Invalid Iceberg table name (unknown type '%s'): %s", typeString, name);
            }
        }

        return new IcebergTableName(table, type);
    }
}
