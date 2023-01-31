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

package com.starrocks.load.streamload;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.FunctionalExprProvider;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.privilege.PrivilegeManager;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Provide the predicate chain and comparator chain
 * which would be used in `List<StreamLoadTask>.stream().filter(predicateChain).sorted(comparatorChain).skip().limit()`
 * with a group of pre-defined ColumnValueSuppliers.
 */
public class StreamLoadFunctionalExprProvider extends FunctionalExprProvider<StreamLoadTask> {

    private static final Logger LOG = LogManager.getLogger(StreamLoadFunctionalExprProvider.class);

    
    private static final ColumnValueSupplier<StreamLoadTask> TASK_NAME_SUPPLIER =
            new ColumnValueSupplier<StreamLoadTask>() {
                @Override
                public String getColumnName() {
                    return "Label";
                }

                @Override
                public PrimitiveType getColumnType() {
                    return PrimitiveType.VARCHAR;
                }

                @Override
                @SuppressWarnings("unchecked")
                public String getColumnValue(StreamLoadTask task) {
                    return task.getLabel();
                }
            };
    private static final ColumnValueSupplier<StreamLoadTask> TASK_ID_SUPPLIER = 
            new ColumnValueSupplier<StreamLoadTask>() {
                @Override
                public String getColumnName() {
                    return "Id";
                }

                @Override
                public PrimitiveType getColumnType() {
                    return PrimitiveType.BIGINT;
                }

                @Override
                @SuppressWarnings("unchecked")
                public Long getColumnValue(StreamLoadTask task) {
                    return task.getId();
                }
            };
    private static final ColumnValueSupplier<StreamLoadTask> TASK_CREATE_TIME_SUPPLIER =
            new ColumnValueSupplier<StreamLoadTask>() {
                @Override
                public String getColumnName() {
                    return "CreateTimeMs";
                }

                @Override
                public PrimitiveType getColumnType() {
                    return PrimitiveType.DATETIME;
                }

                @Override
                @SuppressWarnings("unchecked")
                public Long getColumnValue(StreamLoadTask task) {
                    return task.createTimeMs() / 1000 * 1000;
                }
            };
    private static final ColumnValueSupplier<StreamLoadTask> TASK_DB_NAME_SUPPLIER =
            new ColumnValueSupplier<StreamLoadTask>() {
                @Override
                public String getColumnName() {
                    return "DbName";
                }

                @Override
                public PrimitiveType getColumnType() {
                    return PrimitiveType.VARCHAR;
                }

                @Override
                @SuppressWarnings("unchecked")
                public String getColumnValue(StreamLoadTask task) {
                    return task.getDBName();
                }
            };
    private static final ColumnValueSupplier<StreamLoadTask> TASK_TABLE_NAME_SUPPLIER =
            new ColumnValueSupplier<StreamLoadTask>() {
                @Override
                public String getColumnName() {
                    return "TableName";
                }

                @Override
                public PrimitiveType getColumnType() {
                    return PrimitiveType.VARCHAR;
                }

                @Override
                @SuppressWarnings("unchecked")
                public String getColumnValue(StreamLoadTask task) {
                    return task.getTableName();
                }
            };
    private static final ColumnValueSupplier<StreamLoadTask> TASK_STATE_SUPPLIER = 
            new ColumnValueSupplier<StreamLoadTask>() {
                @Override
                public String getColumnName() {
                    return "State";
                }

                @Override
                public PrimitiveType getColumnType() {
                    return PrimitiveType.VARCHAR;
                }

                @Override
                @SuppressWarnings("unchecked")
                public String getColumnValue(StreamLoadTask task) {
                    return task.getStateName();
                }
            };
    @Override
    protected ImmutableList<ColumnValueSupplier<StreamLoadTask>> delegateWhereSuppliers() {
        // return a group of ColumnValueSuppliers which are abled to be filtered and ordered.
        return new ImmutableList.Builder<ColumnValueSupplier<StreamLoadTask>>()
                .add(TASK_NAME_SUPPLIER)
                .add(TASK_ID_SUPPLIER)
                .add(TASK_CREATE_TIME_SUPPLIER)
                .add(TASK_DB_NAME_SUPPLIER)
                .add(TASK_TABLE_NAME_SUPPLIER)
                .add(TASK_STATE_SUPPLIER)
                .build();
    }

    @Override
    protected boolean delegatePostRowFilter(ConnectContext cxt, StreamLoadTask task) {
        // validate table privilege at the end of a predicateChain in the `stream().filter()`
        if (cxt.getGlobalStateMgr().isUsingNewPrivilege()) {
            return PrivilegeManager.checkTableAction(
                    cxt,
                    task.getDBName(),
                    task.getTableName(),
                    PrivilegeType.INSERT);
        } else {
            return GlobalStateMgr.getCurrentState().getAuth()
                    .checkTblPriv(cxt, task.getDBName(), task.getTableName(), PrivPredicate.LOAD);
        }
    }
}
