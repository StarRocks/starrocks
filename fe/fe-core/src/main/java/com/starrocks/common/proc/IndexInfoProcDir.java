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
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;

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
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            if (table.isNativeTableOrMaterializedView()) {
                OlapTable olapTable = (OlapTable) table;

                // indices order
                List<Long> indices = Lists.newArrayList();
                indices.add(olapTable.getBaseIndexMetaId());
                indices.addAll(olapTable.getIndexMetaIdListExceptBaseIndex());

                for (long indexMetaId : indices) {
                    MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByMetaId(indexMetaId);

                    String type = olapTable.getKeysType().name();
                    StringBuilder builder = new StringBuilder();
                    builder.append(type).append("(");
                    List<String> columnNames = Lists.newArrayList();
                    List<Column> columns = olapTable.getSchemaByIndexMetaId(indexMetaId);
                    for (Column column : columns) {
                        if (column.isKey()) {
                            columnNames.add(column.getName());
                        }
                    }
                    builder.append(Joiner.on(", ").join(columnNames)).append(")");

                    result.addRow(Lists.newArrayList(String.valueOf(indexMetaId),
                            olapTable.getIndexNameByMetaId(indexMetaId),
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
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String idxMetaIdStr) throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(table);

        long idxMetaId;
        try {
            idxMetaId = Long.valueOf(idxMetaIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid index meta id format: " + idxMetaIdStr);
        }

        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            List<Column> schema = null;
            Set<String> bfColumns = null;
            if (table.getType() == TableType.OLAP) {
                OlapTable olapTable = (OlapTable) table;
                schema = olapTable.getSchemaByIndexMetaId(idxMetaId);
                if (schema == null) {
                    throw new AnalysisException("Index meta " + idxMetaId + " does not exist");
                }
                bfColumns = olapTable.getBfColumnNames();
            } else {
                schema = table.getBaseSchema();
            }
            IndexSchemaProcNode node = new IndexSchemaProcNode(schema, bfColumns);
            if (table.isNativeTable() || table.isOlapExternalTable()) {
                node.setHideAggregationType(true);
            }
            return node;
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
    }

}
