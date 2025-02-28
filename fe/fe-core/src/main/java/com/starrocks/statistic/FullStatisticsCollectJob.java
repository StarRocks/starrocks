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

package com.starrocks.statistic;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.thrift.TStatisticData;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.statistic.StatsConstants.FULL_STATISTICS_TABLE_NAME;

public class FullStatisticsCollectJob extends StatisticsCollectJob {
    private static final Logger LOG = LogManager.getLogger(FullStatisticsCollectJob.class);

    //| table_id       | bigint           | NO   | true  | <null>  |       |
    //| partition_id   | bigint           | NO   | true  | <null>  |       |
    //| column_name    | varchar(65530)   | NO   | true  | <null>  |       |
    //| db_id          | bigint           | NO   | false | <null>  |       |
    //| table_name     | varchar(65530)   | NO   | false | <null>  |       |
    //| partition_name | varchar(65530)   | NO   | false | <null>  |       |
    //| row_count      | bigint           | NO   | false | <null>  |       |
    //| data_size      | bigint           | NO   | false | <null>  |       |
    //| ndv            | hll              | NO   | false |         |       |
    //| null_count     | bigint           | NO   | false | <null>  |       |
    //| max            | varchar(1048576) | NO   | false | <null>  |       |
    //| min            | varchar(1048576) | NO   | false | <null>  |       |
    //| update_time    | datetime         | NO   | false | <null>  |       |
    //| collection_size| bigint           | NO   | false | <null>  |       |
    private static final String TABLE_NAME = "column_statistics";
    private static final String BATCH_FULL_STATISTIC_TEMPLATE = "SELECT cast($version as INT)" +
            ", cast($partitionId as BIGINT)" + // BIGINT
            ", '$columnNameStr'" + // VARCHAR
            ", cast(COUNT(1) as BIGINT)" + // BIGINT
            ", cast($dataSize as BIGINT)" + // BIGINT
            ", $hllFunction" + // VARBINARY
            ", cast($countNullFunction as BIGINT)" + // BIGINT
            ", $maxFunction" + // VARCHAR
            ", $minFunction " + // VARCHAR
            ", cast($collectionSizeFunction as BIGINT)" + // BIGINT
            " FROM (select $quoteColumnName as column_key from `$dbName`.`$tableName` partition `$partitionName`) tt";
    private static final String OVERWRITE_PARTITION_TEMPLATE =
            "INSERT INTO " + TABLE_NAME + "(" + StatisticUtils.buildStatsColumnDef(TABLE_NAME).stream().map(ColumnDef::getName)
                    .collect(Collectors.joining(", ")) + ") " + "\n" +
                    "SELECT " +
                    "   table_id, $targetPartitionId, column_name, db_id, table_name, \n" +
                    "   partition_name, row_count, data_size, ndv, null_count, max, min, update_time, collection_size \n" +
                    "FROM " + TABLE_NAME + "\n" +
                    "WHERE `table_id`=$tableId AND `partition_id`=$sourcePartitionId";
    private static final String DELETE_PARTITION_TEMPLATE =
            "DELETE FROM " + TABLE_NAME + "\n" +
                    "WHERE `table_id`=$tableId AND `partition_id`=$sourcePartitionId";

    private final List<Long> partitionIdList;

    private final List<String> sqlBuffer = Lists.newArrayList();
    private final List<List<Expr>> rowsBuffer = Lists.newArrayList();

    public FullStatisticsCollectJob(Database db, Table table, List<Long> partitionIdList, List<String> columns,
                                    StatsConstants.AnalyzeType type, StatsConstants.ScheduleType scheduleType,
                                    Map<String, String> properties) {
        super(db, table, columns, type, scheduleType, properties);
        this.partitionIdList = partitionIdList;
    }

    public FullStatisticsCollectJob(Database db, Table table, List<Long> partitionIdList, List<String> columnNames,
                                    List<Type> columnTypes, StatsConstants.AnalyzeType type,
                                    StatsConstants.ScheduleType scheduleType, Map<String, String> properties) {
        super(db, table, columnNames, columnTypes, type, scheduleType, properties);
        this.partitionIdList = partitionIdList;
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        int parallelism = Math.max(1, context.getSessionVariable().getStatisticCollectParallelism());
        List<List<String>> collectSQLList = buildCollectSQLList(parallelism);
        long totalCollectSQL = collectSQLList.size();
        if (table.isTemporaryTable()) {
            context.setSessionId(((OlapTable) table).getSessionId());
        }
        context.getSessionVariable().setEnableAnalyzePhasePruneColumns(true);

        // First, the collection task is divided into several small tasks according to the column name and partition,
        // and then the multiple small tasks are aggregated into several tasks
        // that will actually be run according to the configured parallelism, and are connected by union all
        // Because each union will run independently, if the number of unions is greater than the degree of parallelism,
        // dop will be set to 1 to meet the requirements of the degree of parallelism.
        // If the number of unions is less than the degree of parallelism,
        // dop should be adjusted appropriately to use enough cpu cores
        long finishedSQLNum = 0;
        long failedNum = 0;
        Exception lastFailure = null;
        for (List<String> sqlUnion : collectSQLList) {
            if (sqlUnion.size() < parallelism) {
                context.getSessionVariable().setPipelineDop(parallelism / sqlUnion.size());
            } else {
                context.getSessionVariable().setPipelineDop(1);
            }

            String sql = Joiner.on(" UNION ALL ").join(sqlUnion);

            try {
                collectStatisticSync(sql, context);
            } catch (Exception e) {
                failedNum++;
                LOG.warn("collect statistics task failed in job: {}, {}", this, sql, e);

                double failureRatio = 1.0 * failedNum / collectSQLList.size();
                if (collectSQLList.size() < 100) {
                    // too few tasks, just fail this job
                    throw e;
                } else if (failureRatio > Config.statistic_full_statistics_failure_tolerance_ratio) {
                    // many tasks, tolerate partial failure
                    String message = String.format("collect statistic job failed due to " +
                                    "too many failed tasks: %d/%d, the last failure is %s",
                            failedNum, collectSQLList.size(), e);
                    LOG.warn(message, e);
                    throw new RuntimeException(message, e);
                } else {
                    lastFailure = e;
                    continue;
                }
            }
            finishedSQLNum++;
            analyzeStatus.setProgress(finishedSQLNum * 100 / totalCollectSQL);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        }

        if (lastFailure != null) {
            String message = String.format("collect statistic job partially failed but tolerated %d/%d, " +
                    "last error is %s", failedNum, collectSQLList.size(), lastFailure);
            analyzeStatus.setReason(message);
            LOG.warn(message);
        }

        flushInsertStatisticsData(context, true);
    }

    // INSERT INTO column_statistics(table_id, partition_id, column_name, db_id, table_name,
    // partition_name, row_count, data_size, ndv, null_count, max, min, update_time, collection_size) values
    // ($tableId, $partitionId, '$columnName', $dbId, '$dbName.$tableName', '$partitionName',
    //  $count, $dataSize, hll_deserialize('$hll'), $countNull, $maxFunction, $minFunction, NOW(), $collectionSizeFunction);
    @Override
    public void collectStatisticSync(String sql, ConnectContext context) throws Exception {
        LOG.debug("statistics collect sql : " + sql);
        StatisticExecutor executor = new StatisticExecutor();

        // set default session variables for stats context
        setDefaultSessionVariable(context);

        List<TStatisticData> dataList = executor.executeStatisticDQL(context, sql);

        String tableName = StringEscapeUtils.escapeSql(db.getOriginName() + "." + table.getName());
        for (TStatisticData data : dataList) {
            List<String> params = Lists.newArrayList();
            List<Expr> row = Lists.newArrayList();

            Partition partition = table.getPartition(data.getPartitionId());
            if (partition == null) {
                continue;
            }
            String partitionName = StringEscapeUtils.escapeSql(partition.getName());

            params.add(String.valueOf(table.getId()));
            params.add(String.valueOf(data.getPartitionId()));
            params.add("'" + StringEscapeUtils.escapeSql(data.getColumnName()) + "'");
            params.add(String.valueOf(db.getId()));
            params.add("'" + tableName + "'");
            params.add("'" + partitionName + "'");
            params.add(String.valueOf(data.getRowCount()));
            params.add(String.valueOf(data.getDataSize()));
            params.add("hll_deserialize(unhex('mockData'))");
            params.add(String.valueOf(data.getNullCount()));
            params.add("'" + data.getMax() + "'");
            params.add("'" + data.getMin() + "'");
            params.add("now()");
            params.add(String.valueOf(data.getCollectionSize() <= 0 ? -1 : data.getCollectionSize()));
            // int
            row.add(new IntLiteral(table.getId(), Type.BIGINT)); // table id, 8 byte
            row.add(new IntLiteral(data.getPartitionId(), Type.BIGINT)); // partition id, 8 byte
            row.add(new StringLiteral(data.getColumnName())); // column name, 20 byte
            row.add(new IntLiteral(db.getId(), Type.BIGINT)); // db id, 8 byte
            row.add(new StringLiteral(tableName)); // table name, 50 byte
            row.add(new StringLiteral(partitionName)); // partition name, 10 byte
            row.add(new IntLiteral(data.getRowCount(), Type.BIGINT)); // row count, 8 byte
            row.add(new IntLiteral((long) data.getDataSize(), Type.BIGINT)); // data size, 8 byte
            row.add(hllDeserialize(data.getHll())); // hll, 32 kB
            row.add(new IntLiteral(data.getNullCount(), Type.BIGINT)); // null count, 8 byte
            row.add(new StringLiteral(data.getMax())); // max, 200 byte
            row.add(new StringLiteral(data.getMin())); // min, 200 byte
            row.add(nowFn()); // update time, 8 byte
            row.add(new IntLiteral(data.getCollectionSize() <= 0 ? -1 : data.getCollectionSize())); // collection size, 8 byte

            rowsBuffer.add(row);
            sqlBuffer.add("(" + String.join(", ", params) + ")");
        }
        flushInsertStatisticsData(context, false);
    }

    private void flushInsertStatisticsData(ConnectContext context, boolean force) throws Exception {
        // hll serialize to hex, about 32kb
        long bufferSize = 33L * 1024 * rowsBuffer.size();
        if (bufferSize < Config.statistic_full_collect_buffer && !force) {
            return;
        }
        if (rowsBuffer.isEmpty()) {
            return;
        }

        int count = 0;
        int maxRetryTimes = 5;
        StatementBase insertStmt = createInsertStmt();
        do {
            LOG.debug("statistics insert sql size:" + rowsBuffer.size());
            StmtExecutor executor = StmtExecutor.newInternalExecutor(context, insertStmt);
            context.setExecutor(executor);
            context.setQueryId(UUIDUtil.genUUID());
            context.setStartTime();
            executor.execute();

            if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                LOG.warn("Statistics collect fail | {} | Error Message [{}]", DebugUtil.printId(context.getQueryId()),
                        context.getState().getErrorMessage());
                if (StringUtils.contains(context.getState().getErrorMessage(), "Too many versions")) {
                    Thread.sleep(Config.statistic_collect_too_many_version_sleep);
                    count++;
                } else {
                    throw new DdlException(context.getState().getErrorMessage());
                }
            } else {
                sqlBuffer.clear();
                rowsBuffer.clear();
                return;
            }
        } while (count < maxRetryTimes);

        throw new DdlException(context.getState().getErrorMessage());
    }

    private StatementBase createInsertStmt() {
        List<String> targetColumnNames = StatisticUtils.buildStatsColumnDef(FULL_STATISTICS_TABLE_NAME).stream()
                .map(ColumnDef::getName)
                .collect(Collectors.toList());

        String sql = "INSERT INTO _statistics_.column_statistics(" + String.join(", ", targetColumnNames) +
                ") values " + String.join(", ", sqlBuffer) + ";";
        QueryStatement qs = new QueryStatement(new ValuesRelation(rowsBuffer, targetColumnNames));
        InsertStmt insert = new InsertStmt(new TableName("_statistics_", "column_statistics"), qs);
        insert.setTargetColumnNames(targetColumnNames);
        insert.setOrigStmt(new OriginStatement(sql, 0));
        return insert;
    }

    /*
     * Split tasks at the partition and column levels,
     * and the number of rows to scan is the number of rows in the partition
     * where the column is located.
     * The number of rows is accumulated in turn until the maximum number of rows is accumulated.
     * Use UNION ALL connection between multiple tasks and collect them in one query
     */
    protected List<List<String>> buildCollectSQLList(int parallelism) {
        List<String> totalQuerySQL = new ArrayList<>();
        for (Long partitionId : partitionIdList) {
            Partition partition = table.getPartition(partitionId);
            if (partition == null) {
                // statistics job doesn't lock DB, partition may be dropped, skip it
                continue;
            }
            for (int i = 0; i < columnNames.size(); i++) {
                totalQuerySQL.add(buildBatchCollectFullStatisticSQL(table, partition, columnNames.get(i),
                        columnTypes.get(i)));
            }
        }

        return Lists.partition(totalQuerySQL, parallelism);
    }

    private String buildBatchCollectFullStatisticSQL(Table table, Partition partition, String columnName,
                                                     Type columnType) {
        StringBuilder builder = new StringBuilder();
        VelocityContext context = new VelocityContext();

        String columnNameStr = StringEscapeUtils.escapeSql(columnName);
        String quoteColumnName = StatisticUtils.quoting(table, columnName);
        String quoteColumnKey = "`column_key`";

        context.put("version", StatsConstants.STATISTIC_BATCH_VERSION_V5);
        context.put("partitionId", partition.getId());
        context.put("columnNameStr", columnNameStr);
        context.put("dataSize", fullAnalyzeGetDataSize(quoteColumnKey, columnType));
        context.put("partitionName", partition.getName());
        context.put("dbName", db.getOriginName());
        context.put("tableName", table.getName());
        context.put("quoteColumnName", quoteColumnName);

        if (!columnType.canStatistic()) {
            context.put("hllFunction", "hex(hll_serialize(hll_empty()))");
            context.put("countNullFunction", "0");
            context.put("maxFunction", "''");
            context.put("minFunction", "''");
            context.put("collectionSizeFunction", "-1");
        } else if (columnType.isCollectionType()) {
            String collectionSizeFunction = "AVG(" + (columnType.isArrayType() ? "ARRAY_LENGTH" : "MAP_SIZE") +
                    "(" + quoteColumnKey + ")) ";
            long elementTypeSize = columnType.isArrayType() ? ((ArrayType) columnType).getItemType().getTypeSize() :
                    ((MapType) columnType).getKeyType().getTypeSize() + ((MapType) columnType).getValueType().getTypeSize();
            String dataSizeFunction =  "COUNT(*) * " + elementTypeSize + " * " + collectionSizeFunction;
            context.put("hllFunction", "'00'");
            context.put("countNullFunction", "0");
            context.put("maxFunction", "''");
            context.put("minFunction", "''");
            context.put("collectionSizeFunction", collectionSizeFunction);
            context.put("dataSize", dataSizeFunction);
        } else {
            context.put("hllFunction", "hex(hll_serialize(IFNULL(hll_raw(" + quoteColumnKey + "), hll_empty())))");
            context.put("countNullFunction", "COUNT(1) - COUNT(" + quoteColumnKey + ")");
            context.put("maxFunction", getMinMaxFunction(columnType, quoteColumnKey, true));
            context.put("minFunction", getMinMaxFunction(columnType, quoteColumnKey, false));
            context.put("collectionSizeFunction", "-1");
        }

        builder.append(build(context, BATCH_FULL_STATISTIC_TEMPLATE));
        return builder.toString();
    }

    public static List<String> buildOverwritePartitionSQL(long tableId, long sourcePartitionId,
                                                          long targetPartitionId) {
        List<String> result = Lists.newArrayList();

        VelocityContext context = new VelocityContext();
        context.put("tableId", tableId);
        context.put("targetPartitionId", sourcePartitionId);
        context.put("sourcePartitionId", targetPartitionId);
        {
            StringWriter sw = new StringWriter();
            DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", OVERWRITE_PARTITION_TEMPLATE);
            result.add(sw.toString());
        }
        {
            StringWriter sw = new StringWriter();
            DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", DELETE_PARTITION_TEMPLATE);
            result.add(sw.toString());
        }
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FullStatisticsCollectJob{");
        sb.append("type=").append(analyzeType);
        sb.append(", scheduleType=").append(scheduleType);
        sb.append(", db=").append(db);
        sb.append(", table=").append(table);
        sb.append(", partitionIdList=").append(partitionIdList);
        sb.append(", columnNames=").append(columnNames);
        sb.append(", properties=").append(properties);
        sb.append('}');
        return sb.toString();
    }
}
