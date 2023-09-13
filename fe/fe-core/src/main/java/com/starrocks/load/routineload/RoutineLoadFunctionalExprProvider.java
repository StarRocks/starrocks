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

package com.starrocks.load.routineload;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.FunctionalExprProvider;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Authorizer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Provide the predicate chain and comparator chain
 * which would be used in `List<RoutineLoadJob>.stream().filter(predicateChain).sorted(comparatorChain).skip().limit()`
 * with a group of pre-defined ColumnValueSuppliers.
 */
public class RoutineLoadFunctionalExprProvider extends FunctionalExprProvider<RoutineLoadJob> {

    private static final Logger LOG = LogManager.getLogger(RoutineLoadFunctionalExprProvider.class);

    private static final ColumnValueSupplier<RoutineLoadJob> JOB_ID_SUPPLIER = new ColumnValueSupplier<RoutineLoadJob>() {
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
        public Long getColumnValue(RoutineLoadJob job) {
            return job.getId();
        }
    };
    private static final ColumnValueSupplier<RoutineLoadJob> JOB_NAME_SUPPLIER =
            new ColumnValueSupplier<RoutineLoadJob>() {
                @Override
                public String getColumnName() {
                    return "Name";
                }

                @Override
                public PrimitiveType getColumnType() {
                    return PrimitiveType.VARCHAR;
                }

                @Override
                @SuppressWarnings("unchecked")
                public String getColumnValue(RoutineLoadJob job) {
                    return job.getName();
                }
            };
    private static final ColumnValueSupplier<RoutineLoadJob> JOB_CREATE_TIME_SUPPLIER =
            new ColumnValueSupplier<RoutineLoadJob>() {
                @Override
                public String getColumnName() {
                    return "CreateTime";
                }

                @Override
                public PrimitiveType getColumnType() {
                    return PrimitiveType.DATETIME;
                }

                @Override
                @SuppressWarnings("unchecked")
                public Long getColumnValue(RoutineLoadJob job) {
                    return job.getCreateTimestamp() / 1000 * 1000;
                }
            };
    private static final ColumnValueSupplier<RoutineLoadJob> JOB_PAUSE_TIME_SUPPLIER =
            new ColumnValueSupplier<RoutineLoadJob>() {
                @Override
                public String getColumnName() {
                    return "PauseTime";
                }

                @Override
                public PrimitiveType getColumnType() {
                    return PrimitiveType.DATETIME;
                }

                @Override
                @SuppressWarnings("unchecked")
                public Long getColumnValue(RoutineLoadJob job) {
                    return job.getPauseTimestamp() / 1000 * 1000;
                }
            };
    private static final ColumnValueSupplier<RoutineLoadJob> JOB_END_TIME_SUPPLIER =
            new ColumnValueSupplier<RoutineLoadJob>() {
                @Override
                public String getColumnName() {
                    return "EndTime";
                }

                @Override
                public PrimitiveType getColumnType() {
                    return PrimitiveType.DATETIME;
                }

                @Override
                @SuppressWarnings("unchecked")
                public Long getColumnValue(RoutineLoadJob job) {
                    return job.getEndTimestamp() / 1000 * 1000;
                }
            };
    private static final ColumnValueSupplier<RoutineLoadJob> JOB_TASK_NUM_SUPPLIER =
            new ColumnValueSupplier<RoutineLoadJob>() {
                @Override
                public String getColumnName() {
                    return "CurrentTaskNum";
                }

                @Override
                public PrimitiveType getColumnType() {
                    return PrimitiveType.BIGINT;
                }

                @Override
                @SuppressWarnings("unchecked")
                public Long getColumnValue(RoutineLoadJob job) {
                    return (long) job.getSizeOfRoutineLoadTaskInfoList();
                }
            };

    private static final ColumnValueSupplier<RoutineLoadJob> TABLE_NAME_SUPPLIER =
            new ColumnValueSupplier<RoutineLoadJob>() {
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
                public String getColumnValue(RoutineLoadJob job) {
                    try {
                        return job.getTableName();
                    } catch (MetaNotFoundException e) {
                        LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, job.getId())
                                .add("error_msg", "The table metadata of this job has been changed. "
                                        + "It will be cancelled automatically")
                                .build(), e);
                        return null;
                    }
                }
            };

    private static final ColumnValueSupplier<RoutineLoadJob> STATE_SUPPLIER = new ColumnValueSupplier<RoutineLoadJob>() {
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
        public String getColumnValue(RoutineLoadJob job) {
            return job.getState().name();
        }
    };

    private static final ColumnValueSupplier<RoutineLoadJob> REASON_SUPPLIER =
            new ColumnValueSupplier<RoutineLoadJob>() {
                @Override
                public String getColumnName() {
                    return "ReasonOfStateChanged";
                }

                @Override
                public PrimitiveType getColumnType() {
                    return PrimitiveType.VARCHAR;
                }

                @Override
                @SuppressWarnings("unchecked")
                public String getColumnValue(RoutineLoadJob job) {
                    return job.getPauseReason();
                }
            };

    private static final ColumnValueSupplier<RoutineLoadJob> OTHER_MSG_SUPPLIER =
            new ColumnValueSupplier<RoutineLoadJob>() {
                @Override
                public String getColumnName() {
                    return "OtherMsg";
                }

                @Override
                public PrimitiveType getColumnType() {
                    return PrimitiveType.VARCHAR;
                }

                @Override
                @SuppressWarnings("unchecked")
                public String getColumnValue(RoutineLoadJob job) {
                    return job.getOtherMsg();
                }
            };

    @Override
    protected ImmutableList<ColumnValueSupplier<RoutineLoadJob>> delegateWhereSuppliers() {
        // return a group of ColumnValueSuppliers which are abled to be filtered and ordered.
        return new ImmutableList.Builder<ColumnValueSupplier<RoutineLoadJob>>()
                .add(JOB_ID_SUPPLIER)
                .add(JOB_NAME_SUPPLIER)
                .add(JOB_CREATE_TIME_SUPPLIER)
                .add(JOB_PAUSE_TIME_SUPPLIER)
                .add(JOB_END_TIME_SUPPLIER)
                .add(JOB_TASK_NUM_SUPPLIER)
                .add(TABLE_NAME_SUPPLIER)
                .add(STATE_SUPPLIER)
                .add(REASON_SUPPLIER)
                .add(OTHER_MSG_SUPPLIER)
                .build();
    }

    @Override
    protected boolean delegatePostRowFilter(ConnectContext cxt, RoutineLoadJob job) {
        try {
            try {
                Authorizer.checkTableAction(
                        cxt.getCurrentUserIdentity(), cxt.getCurrentRoleIds(),
                        job.getDbFullName(),
                        job.getTableName(),
                        PrivilegeType.INSERT);
            } catch (AccessDeniedException e) {
                return false;
            }

            return true;
        } catch (MetaNotFoundException e) {
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, job.getId())
                    .add("error_msg", "The metadata of this job has been changed. "
                            + "It will be cancelled automatically")
                    .build(), e);
            return false;
        }
    }
}
