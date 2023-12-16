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


package com.starrocks.connector.jdbc;

import java.util.Objects;

public class JDBCTableCacheKey extends JDBCTableName {

    private final String functionName;

    public JDBCTableCacheKey(String functionName, String catalogName, String databaseName, String tableName) {
        super(catalogName, databaseName, tableName);
        this.functionName = functionName;
    }

    public static JDBCTableCacheKey of(String functionName, String catalogName, String databaseName, String tableName) {
        return new JDBCTableCacheKey(functionName, catalogName, databaseName, tableName);
    }

    public String getFunctionName() {
        return functionName;
    }

    public String toString() {
        return "[functionName: " + getFunctionName() + ", JDBCTableName: " + super.toString() + "]";
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JDBCTableCacheKey other = (JDBCTableCacheKey) o;
        return Objects.equals(getFunctionName(), other.getFunctionName()) && super.equals(other);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getFunctionName(), super.hashCode());
    }

}
