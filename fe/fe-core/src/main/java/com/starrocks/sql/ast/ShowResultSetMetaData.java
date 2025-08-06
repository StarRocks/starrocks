// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;

import java.util.List;

public record ShowResultSetMetaData(List<String> columns) {
    public int getColumnCount() {
        return columns.size();
    }

    public List<String> getColumns() {
        return columns;
    }

    public String getColumn(int idx) {
        return columns.get(idx);
    }

    public int getColumnIdx(String colName) {
        for (int idx = 0; idx < columns.size(); ++idx) {
            if (columns.get(idx).equalsIgnoreCase(colName)) {
                return idx;
            }
        }
        throw new StarRocksPlannerException("Can't get column " + colName, ErrorType.INTERNAL_ERROR);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final List<String> columns;

        public Builder() {
            columns = Lists.newArrayList();
        }

        public ShowResultSetMetaData build() {
            return new ShowResultSetMetaData(columns);
        }

        public Builder addColumn(String columnName) {
            columns.add(columnName);
            return this;
        }
    }
}
