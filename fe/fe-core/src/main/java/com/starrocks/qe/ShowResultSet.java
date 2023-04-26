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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/ShowResultSet.java

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

package com.starrocks.qe;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.thrift.TColumnDefinition;
import com.starrocks.thrift.TShowResultSet;
import com.starrocks.thrift.TShowResultSetMetaData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

// Result set of show statement.
// Redefine ResultSet now, because JDBC is too complicated.
// TODO(zhaochun): Maybe interface is better.
public class ShowResultSet {
    private static final Logger LOG = LogManager.getLogger(ShowResultSet.class);
    private ShowResultSetMetaData metaData;
    private List<List<String>> resultRows;
    int rowIdx;

    // now only support static result.
    public ShowResultSet(ShowResultSetMetaData metaData, List<List<String>> resultRows) {
        this.metaData = metaData;
        this.resultRows = resultRows;
        rowIdx = -1;
    }

    public ShowResultSet(TShowResultSet resultSet) {
        List<Column> columns = Lists.newArrayList();
        for (int i = 0; i < resultSet.getMetaData().getColumnsSize(); i++) {
            TColumnDefinition definition = (TColumnDefinition) resultSet.getMetaData().getColumns().get(i);
            columns.add(new Column(
                    definition.getColumnName(),
                    ScalarType.createType(PrimitiveType.fromThrift(definition.getColumnType().getType())))
            );
        }
        this.metaData = new ShowResultSetMetaData(columns);
        this.resultRows = resultSet.getResultRows();
        this.rowIdx = -1;

    }

    public boolean next() {
        if (rowIdx + 1 >= resultRows.size()) {
            return false;
        }
        rowIdx++;
        return true;
    }

    public List<List<String>> getResultRows() {
        return resultRows;
    }

    public ShowResultSetMetaData getMetaData() {
        return metaData;
    }

    public String getString(int col) {
        return resultRows.get(rowIdx).get(col);
    }

    public byte getByte(int col) {
        return Byte.parseByte(getString(col));
    }

    public int getInt(int col) {
        return Integer.parseInt(getString(col));
    }

    public long getLong(int col) {
        return Long.parseLong(getString(col));
    }

    public short getShort(int col) {
        return Short.parseShort(getString(col));
    }

    public TShowResultSet tothrift() {
        TShowResultSet set = new TShowResultSet();
        set.metaData = new TShowResultSetMetaData();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            Column definition = metaData.getColumn(i);
            set.metaData.addToColumns(new TColumnDefinition(
                    definition.getName(), definition.getType().toColumnTypeThrift())
            );
        }

        set.resultRows = Lists.newArrayList();
        for (int i = 0; i < resultRows.size(); i++) {
            ArrayList<String> list = Lists.newArrayList();
            list.addAll(resultRows.get(i));
            set.resultRows.add(list);
        }
        return set;
    }
}
