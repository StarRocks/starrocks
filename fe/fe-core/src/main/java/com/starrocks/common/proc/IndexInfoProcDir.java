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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/IndexInfoProcDir.java

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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.common.AnalysisException;
import com.starrocks.meta.lock.LockType;
import com.starrocks.meta.lock.Locker;

import java.util.List;
import java.util.Set;

/*
 * SHOW PROC /dbs/dbId/tableId/index_schema
 * show indexNames(to schema)
 */
public class IndexInfoProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("IndexId").add("IndexName").add("SchemaVersion").add("SchemaHash")
            .add("ShortKeyColumnCount").add("StorageType").add("Keys")
            .build();

    private Database db;
    private Table table;

    public IndexInfoProcDir(Database db, Table table) {
        this.db = db;
        this.table = table;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(table);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            if (table.isNativeTableOrMaterializedView()) {
                OlapTable olapTable = (OlapTable) table;

                // indices order
                List<Long> indices = Lists.newArrayList();
                indices.add(olapTable.getBaseIndexId());
                indices.addAll(olapTable.getIndexIdListExceptBaseIndex());

                for (long indexId : indices) {
                    MaterializedIndexMeta indexMeta = olapTable.getIndexIdToMeta().get(indexId);

                    String type = olapTable.getKeysType().name();
                    StringBuilder builder = new StringBuilder();
                    builder.append(type).append("(");
                    List<String> columnNames = Lists.newArrayList();
                    List<Column> columns = olapTable.getSchemaByIndexId(indexId);
                    for (Column column : columns) {
                        if (column.isKey()) {
                            columnNames.add(column.getName());
                        }
                    }
                    builder.append(Joiner.on(", ").join(columnNames)).append(")");

                    result.addRow(Lists.newArrayList(String.valueOf(indexId),
                            olapTable.getIndexNameById(indexId),
                            String.valueOf(indexMeta.getSchemaVersion()),
                            String.valueOf(indexMeta.getSchemaHash()),
                            String.valueOf(indexMeta.getShortKeyColumnCount()),
                            indexMeta.getStorageType().name(),
                            builder.toString()));
                }
            } else {
                result.addRow(Lists.newArrayList("-1", table.getName(), "", "", "", "", ""));
            }

            return result;
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String idxIdStr) throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(table);

        long idxId;
        try {
            idxId = Long.valueOf(idxIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid index id format: " + idxIdStr);
        }

        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            List<Column> schema = null;
            Set<String> bfColumns = null;
            if (table.getType() == TableType.OLAP) {
                OlapTable olapTable = (OlapTable) table;
                schema = olapTable.getSchemaByIndexId(idxId);
                if (schema == null) {
                    throw new AnalysisException("Index " + idxId + " does not exist");
                }
                bfColumns = olapTable.getCopiedBfColumns();
            } else {
                schema = table.getBaseSchema();
            }
            IndexSchemaProcNode node = new IndexSchemaProcNode(schema, bfColumns);
            if (table.getType() == TableType.OLAP || table.getType() == TableType.OLAP_EXTERNAL) {
                node.setHideAggregationType(true);
            }
            return node;
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
    }

}
