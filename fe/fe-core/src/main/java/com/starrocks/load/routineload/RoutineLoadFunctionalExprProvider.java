// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.load.routineload;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.FunctionalExprProvider;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Provide the predicate chain and comparator chain
 * which would be used in `List<RoutineLoadJob>.stream().filter(predicateChain).sorted(comparatorChain).skip().limit()`
 * with a group of pre-defined ColumnValueSuppliers.
 */
public class RoutineLoadFunctionalExprProvider extends FunctionalExprProvider<RoutineLoadJob> {

    private static final Logger LOG = LogManager.getLogger(RoutineLoadFunctionalExprProvider.class);

    private static final ColumnValueSupplier<RoutineLoadJob> jobIdSupplier = new ColumnValueSupplier<RoutineLoadJob>() {
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
    private static final ColumnValueSupplier<RoutineLoadJob> jobNameSupplier =
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
    private static final ColumnValueSupplier<RoutineLoadJob> jobCreateTimeSupplier =
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
    private static final ColumnValueSupplier<RoutineLoadJob> jobPauseTimeSupplier =
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
    private static final ColumnValueSupplier<RoutineLoadJob> jobEndTimeSupplier =
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
    private static final ColumnValueSupplier<RoutineLoadJob> jobTaskNumSupplier =
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

    private static final ColumnValueSupplier<RoutineLoadJob> tableNameSupplier =
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

    private static final ColumnValueSupplier<RoutineLoadJob> stateSupplier = new ColumnValueSupplier<RoutineLoadJob>() {
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

    private static final ColumnValueSupplier<RoutineLoadJob> reasonSupplier =
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

    private static final ColumnValueSupplier<RoutineLoadJob> otherMsgSupplier =
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
                .add(jobIdSupplier)
                .add(jobNameSupplier)
                .add(jobCreateTimeSupplier)
                .add(jobPauseTimeSupplier)
                .add(jobEndTimeSupplier)
                .add(jobTaskNumSupplier)
                .add(tableNameSupplier)
                .add(stateSupplier)
                .add(reasonSupplier)
                .add(otherMsgSupplier)
                .build();
    }

    @Override
    protected boolean delegatePostRowFilter(ConnectContext cxt, RoutineLoadJob job) {
        try {
            // validate table privilege at the end of a predicateChain in the `stream().filter()`
            return GlobalStateMgr.getCurrentState().getAuth()
                    .checkTblPriv(cxt, job.getDbFullName(), job.getName(), PrivPredicate.LOAD);
        } catch (MetaNotFoundException e) {
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, job.getId())
                    .add("error_msg", "The metadata of this job has been changed. "
                            + "It will be cancelled automatically")
                    .build(), e);
            return false;
        }
    }
}
