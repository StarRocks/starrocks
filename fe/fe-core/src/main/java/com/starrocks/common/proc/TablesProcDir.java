// This file is made available under Elastic License 2.0.
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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
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

/*
 * SHOW PROC /dbs/dbId/
 * show table family groups' info within a db
 */
public class TablesProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TableId").add("TableName").add("IndexNum").add("PartitionColumnName")
            .add("PartitionNum").add("State").add("Type").add("LastConsistencyCheckTime").add("ReplicaCount")
            .build();

    private Database db;

    public TablesProcDir(Database db) {
        this.db = db;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String tableIdStr) throws AnalysisException {
        Preconditions.checkNotNull(db);
        if (Strings.isNullOrEmpty(tableIdStr)) {
            throw new AnalysisException("TableIdStr is null");
        }

        long tableId = -1L;
        try {
            tableId = Long.valueOf(tableIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid table id format: " + tableIdStr);
        }

        Table table = null;
        db.readLock();
        try {
            table = db.getTable(tableId);
        } finally {
            db.readUnlock();
        }
        if (table == null) {
            throw new AnalysisException("Table[" + tableId + "] does not exist");
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

                int partitionNum = 1;
                long replicaCount = 0;
                String partitionKey = FeConstants.null_string;
                if (table.getType() == TableType.OLAP) {
                    OlapTable olapTable = (OlapTable) table;
                    if (olapTable.getPartitionInfo().getType() == PartitionType.RANGE) {
                        partitionNum = olapTable.getPartitions().size();
                        RangePartitionInfo info = (RangePartitionInfo) olapTable.getPartitionInfo();
                        partitionKey = "";
                        int idx = 0;
                        for (Column column : info.getPartitionColumns()) {
                            if (idx != 0) {
                                partitionKey += ", ";
                            }
                            partitionKey += column.getName();
                            ++idx;
                        }
                    }
                    replicaCount = olapTable.getReplicaCount();
                    tableInfo.add(table.getId());
                    tableInfo.add(table.getName());
                    tableInfo.add(olapTable.getIndexNameToId().size());
                    tableInfo.add(partitionKey);
                    tableInfo.add(partitionNum);
                    tableInfo.add(olapTable.getState());
                    tableInfo.add(table.getType());
                } else {
                    tableInfo.add(table.getId());
                    tableInfo.add(table.getName());
                    tableInfo.add(FeConstants.null_string);
                    tableInfo.add(partitionKey);
                    tableInfo.add(partitionNum);
                    tableInfo.add(FeConstants.null_string);
                    tableInfo.add(table.getType());
                }

                // last check time
                tableInfo.add(TimeUtils.longToTimeString(table.getLastCheckTime()));
                tableInfo.add(replicaCount);
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
}
