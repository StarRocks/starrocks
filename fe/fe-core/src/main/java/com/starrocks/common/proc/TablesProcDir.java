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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/TablesProcDir.java

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

package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.ListComparator;
import com.starrocks.common.util.TimeUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/*
 * SHOW PROC /dbs/dbId/
 * show table family groups' info within a db
 */
public class TablesProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TableId").add("TableName").add("IndexNum").add("PartitionColumnName")
            .add("PartitionNum").add("State").add("Type").add("LastConsistencyCheckTime")
            .add("ReplicaCount").add("PartitionType").add("StoragePath")
            .build();
    private static final int PARTITION_NUM_DEFAULT = 1;
    private static final int PARTITION_REPLICA_COUNT_DEFAULT = 0;
    private static final String NULL_STRING_DEFAULT = FeConstants.NULL_STRING;

    private Database db;

    public TablesProcDir(Database db) {
        this.db = db;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String tableIdOrName) throws AnalysisException {
        Preconditions.checkNotNull(db);
        if (Strings.isNullOrEmpty(tableIdOrName)) {
            throw new AnalysisException("table id or name is null or empty");
        }

        Table table;
        db.readLock();
        try {
            try {
                table = db.getTable(Long.parseLong(tableIdOrName));
            } catch (NumberFormatException e) {
                table = db.getTable(tableIdOrName);
            }
        } finally {
            db.readUnlock();
        }

        if (table == null) {
            throw new AnalysisException("unknown table id or name \"" + tableIdOrName + "\"");
        }

        return new TableProcDir(db, table);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(db);

        // get info
        List<List<Comparable>> tableInfos = new ArrayList<List<Comparable>>();
        db.readLock();
        try {
            for (Table table : db.getTables()) {
                List<Comparable> tableInfo = new ArrayList<Comparable>();
                TableType tableType = table.getType();
                tableInfo.add(table.getId());
                tableInfo.add(table.getName());
                tableInfo.add(findIndexNum(table));
                tableInfo.add(findPartitionKey(table));
                tableInfo.add(findPartitionNum(table));
                tableInfo.add(findState(table));
                tableInfo.add(tableType);
                tableInfo.add(TimeUtils.longToTimeString(table.getLastCheckTime()));
                tableInfo.add(findReplicaCount(table));
                tableInfo.add(findPartitionType(table));
                tableInfo.add(findStoragePath(table));
                tableInfos.add(tableInfo);
            }
        } finally {
            db.readUnlock();
        }

        // sort by table id
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0);
        Collections.sort(tableInfos, comparator);

        // set result
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        for (List<Comparable> info : tableInfos) {
            List<String> row = new ArrayList<String>(info.size());
            for (Comparable comparable : info) {
                row.add(comparable.toString());
            }
            result.addRow(row);
        }

        return result;
    }

    private long findReplicaCount(Table table) {
        if (table.isNativeTable()) {
            OlapTable olapTable = (OlapTable) table;
            return olapTable.getReplicaCount();
        }
        return PARTITION_REPLICA_COUNT_DEFAULT;
    }

    private String findState(Table table) {
        if (table.isNativeTable()) {
            OlapTable olapTable = (OlapTable) table;
            return olapTable.getState().toString();
        }
        return NULL_STRING_DEFAULT;
    }

    private int findPartitionNum(Table table) {
        if (table.isNativeTable()) {
            OlapTable olapTable = (OlapTable) table;
            PartitionType partitionType = olapTable.getPartitionInfo().getType();
            if (partitionType == PartitionType.RANGE || partitionType == PartitionType.EXPR_RANGE
                    || partitionType == PartitionType.LIST) {
                return olapTable.getPartitions().size();
            }
        }
        return PARTITION_NUM_DEFAULT;
    }

    private String findPartitionKey(Table table) {
        if (table.isNativeTable()) {
            OlapTable olapTable = (OlapTable) table;
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            if (partitionInfo.getType() == PartitionType.RANGE) {
                return ((RangePartitionInfo) partitionInfo).getPartitionColumns()
                        .stream()
                        .map(column -> column.getName())
                        .collect(Collectors.joining(", "));
            }
            if (partitionInfo.getType() == PartitionType.LIST) {
                return ((ListPartitionInfo) partitionInfo).getPartitionColumns()
                        .stream()
                        .map(column -> column.getName())
                        .collect(Collectors.joining(", "));
            }
        }
        return NULL_STRING_DEFAULT;
    }

    private String findPartitionType(Table table) {
        if (table.isNativeTable()) {
            OlapTable olapTable = (OlapTable) table;
            return olapTable.getPartitionInfo().getType().typeString;
        } else if (table.getType() == TableType.ELASTICSEARCH) {
            EsTable esTable = (EsTable) table;
            return esTable.getPartitionInfo().getType().typeString;
        }
        return PartitionType.UNPARTITIONED.typeString;
    }

    private String findIndexNum(Table table) {
        if (table.isNativeTable()) {
            OlapTable olapTable = (OlapTable) table;
            return String.valueOf(olapTable.getIndexNameToId().size());
        }
        return NULL_STRING_DEFAULT;
    }

    private String findStoragePath(Table table) {
        String storageGroup = null;
        if (table.isCloudNativeTable()) {
            storageGroup = ((OlapTable) table).getStoragePath();
        }
        return storageGroup != null ? storageGroup : NULL_STRING_DEFAULT;
    }
}
