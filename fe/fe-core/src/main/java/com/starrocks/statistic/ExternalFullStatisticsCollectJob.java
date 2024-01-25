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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.iceberg.IcebergApiConverter;
import com.starrocks.connector.iceberg.IcebergPartitionTransform;
import com.starrocks.connector.iceberg.IcebergPartitionUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.thrift.TStatisticData;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.iceberg.PartitionField;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExternalFullStatisticsCollectJob extends StatisticsCollectJob {
    private static final Logger LOG = LogManager.getLogger(ExternalFullStatisticsCollectJob.class);

    private static final String BATCH_FULL_STATISTIC_TEMPLATE = "SELECT cast($version as INT)" +
            ", '$partitionNameStr'" + // VARCHAR
            ", '$columnNameStr'" + // VARCHAR
            ", cast(COUNT(1) as BIGINT)" + // BIGINT
            ", cast($dataSize as BIGINT)" + // BIGINT
            ", $hllFunction" + // VARBINARY
            ", cast($countNullFunction as BIGINT)" + // BIGINT
            ", $maxFunction" + // VARCHAR
            ", $minFunction " + // VARCHAR
            " FROM `$catalogName`.`$dbName`.`$tableName` where $partitionPredicate";

    private final String catalogName;
    private final List<String> partitionNames;
    private final List<String> sqlBuffer = Lists.newArrayList();
    private final List<List<Expr>> rowsBuffer = Lists.newArrayList();

    public ExternalFullStatisticsCollectJob(String catalogName, Database db, Table table, List<String> partitionNames,
                                            List<String> columns, StatsConstants.AnalyzeType type,
                                            StatsConstants.ScheduleType scheduleType, Map<String, String> properties) {
        super(db, table, columns, type, scheduleType, properties);
        this.catalogName = catalogName;
        this.partitionNames = partitionNames;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        long finishedSQLNum = 0;
        int parallelism = Math.max(1, context.getSessionVariable().getStatisticCollectParallelism());
        List<List<String>> collectSQLList = buildCollectSQLList(parallelism);
        long totalCollectSQL = collectSQLList.size();

        // First, the collection task is divided into several small tasks according to the column name and partition,
        // and then the multiple small tasks are aggregated into several tasks
        // that will actually be run according to the configured parallelism, and are connected by union all
        // Because each union will run independently, if the number of unions is greater than the degree of parallelism,
        // dop will be set to 1 to meet the requirements of the degree of parallelism.
        // If the number of unions is less than the degree of parallelism,
        // dop should be adjusted appropriately to use enough cpu cores
        for (List<String> sqlUnion : collectSQLList) {
            if (sqlUnion.size() < parallelism) {
                context.getSessionVariable().setPipelineDop(parallelism / sqlUnion.size());
            } else {
                context.getSessionVariable().setPipelineDop(1);
            }

            String sql = Joiner.on(" UNION ALL ").join(sqlUnion);

            collectStatisticSync(sql, context);
            finishedSQLNum++;
            analyzeStatus.setProgress(finishedSQLNum * 100 / totalCollectSQL);
            GlobalStateMgr.getCurrentAnalyzeMgr().replayAddAnalyzeStatus(analyzeStatus);
        }

        flushInsertStatisticsData(context, true);
    }

    protected List<List<String>> buildCollectSQLList(int parallelism) {
        List<String> totalQuerySQL = new ArrayList<>();
        for (String partitionName : partitionNames) {
            for (String columnName : columns) {
                totalQuerySQL.add(buildBatchCollectFullStatisticSQL(table, partitionName, columnName));
            }
        }

        return Lists.partition(totalQuerySQL, parallelism);
    }

    private String buildBatchCollectFullStatisticSQL(Table table, String partitionName, String columnName) {
        StringBuilder builder = new StringBuilder();
        VelocityContext context = new VelocityContext();
        Column column = table.getColumn(columnName);

        String columnNameStr = StringEscapeUtils.escapeSql(columnName);
        String quoteColumnName = StatisticUtils.quoting(columnName);

        String nullValue;
        if (table.isIcebergTable()) {
            nullValue = IcebergApiConverter.PARTITION_NULL_VALUE;
        } else {
            nullValue = HiveMetaClient.PARTITION_NULL_VALUE;
        }

        context.put("version", StatsConstants.STATISTIC_EXTERNAL_VERSION);
        // all table now, partition later
        context.put("partitionNameStr", PartitionUtil.normalizePartitionName(partitionName,
                table.getPartitionColumnNames(), nullValue));
        context.put("columnNameStr", columnNameStr);
        context.put("dataSize", fullAnalyzeGetDataSize(column));
        context.put("dbName", db.getOriginName());
        context.put("tableName", table.getName());
        context.put("catalogName", this.catalogName);

        if (!column.getType().canStatistic()) {
            context.put("hllFunction", "hex(hll_serialize(hll_empty()))");
            context.put("countNullFunction", "0");
            context.put("maxFunction", "''");
            context.put("minFunction", "''");
        } else {
            context.put("hllFunction", "hex(hll_serialize(IFNULL(hll_raw(" + quoteColumnName + "), hll_empty())))");
            context.put("countNullFunction", "COUNT(1) - COUNT(" + quoteColumnName + ")");
            context.put("maxFunction", getMinMaxFunction(column, quoteColumnName, true));
            context.put("minFunction", getMinMaxFunction(column, quoteColumnName, false));
        }

        if (table.isUnPartitioned()) {
            context.put("partitionPredicate", "1=1");
        } else {
            List<String> partitionColumnNames = table.getPartitionColumnNames();
            List<String> partitionValues = PartitionUtil.toPartitionValues(partitionName);
            List<String> partitionPredicate = Lists.newArrayList();
            for (int i = 0; i < partitionColumnNames.size(); i++) {
                String partitionColumnName = partitionColumnNames.get(i);
                String partitionValue = partitionValues.get(i);
                if (partitionValue.equals(nullValue)) {
                    partitionPredicate.add(StatisticUtils.quoting(partitionColumnName) + " IS NULL");
                } else if (isSupportedPartitionTransform(partitionColumnName)) {
                    partitionPredicate.add(IcebergPartitionUtils.convertPartitionFieldToPredicate((IcebergTable) table,
                            partitionColumnName, partitionValue));
                } else {
                    partitionPredicate.add(StatisticUtils.quoting(partitionColumnName) + " = '" + partitionValue + "'");
                }
            }
            context.put("partitionPredicate", Joiner.on(" AND ").join(partitionPredicate));
        }

        builder.append(build(context, BATCH_FULL_STATISTIC_TEMPLATE));
        return builder.toString();
    }

    // only iceberg table support partition transform
    // now only support identity/year/month/day/hour transform
    boolean isSupportedPartitionTransform(String partitionColumn) {
        // only iceberg table support partition transform
        if (!table.isIcebergTable()) {
            return false;
        }
        IcebergTable icebergTable = (IcebergTable) table;
        PartitionField partitionField = icebergTable.getPartitionField(partitionColumn);
        if (partitionField == null) {
            LOG.warn("Partition column {} not found in table {}", partitionColumn, table.getName());
            throw new StarRocksConnectorException("Partition column " + partitionColumn + " not found in table " +
                    table.getName());
        }

        IcebergPartitionTransform transform = IcebergPartitionTransform.fromString(partitionField.transform().toString());
        if (!IcebergPartitionUtils.isSupportedConvertPartitionTransform(transform)) {
            LOG.warn("Partition transform {} not supported to analyze, table: {}", transform, table.getName());
            throw new StarRocksConnectorException("Partition transform " + transform + " not supported to analyze, " +
                    "table: " + table.getName());
        }

        return true;
    }

    @Override
    public void collectStatisticSync(String sql, ConnectContext context) throws Exception {
        LOG.debug("statistics collect sql : " + sql);
        StatisticExecutor executor = new StatisticExecutor();

        // set default session variables for stats context
        setDefaultSessionVariable(context);

        List<TStatisticData> dataList = executor.executeStatisticDQL(context, sql);

        for (TStatisticData data : dataList) {
            List<String> params = Lists.newArrayList();
            List<Expr> row = Lists.newArrayList();

            params.add(table.getUUID());
            params.add("'" + StringEscapeUtils.escapeSql(data.getPartitionName()) + "'");
            params.add("'" + StringEscapeUtils.escapeSql(data.getColumnName()) + "'");
            params.add(catalogName);
            params.add(db.getOriginName());
            params.add(table.getName());
            params.add(String.valueOf(data.getRowCount()));
            params.add(String.valueOf(data.getDataSize()));
            params.add("hll_deserialize(unhex('mockData'))");
            params.add(String.valueOf(data.getNullCount()));
            params.add("'" + data.getMax() + "'");
            params.add("'" + data.getMin() + "'");
            params.add("now()");
            // int
            row.add(new StringLiteral(table.getUUID())); // table id, wait to byte
            row.add(new StringLiteral(data.getPartitionName()));
            row.add(new StringLiteral(data.getColumnName())); // column name, 20 byte
            row.add(new StringLiteral(catalogName));
            row.add(new StringLiteral(db.getOriginName()));
            row.add(new StringLiteral(table.getName()));
            row.add(new IntLiteral(data.getRowCount(), Type.BIGINT)); // row count, 8 byte
            row.add(new IntLiteral((long) data.getDataSize(), Type.BIGINT)); // data size, 8 byte
            row.add(hllDeserialize(data.getHll())); // hll, 32 kB
            row.add(new IntLiteral(data.getNullCount(), Type.BIGINT)); // null count, 8 byte
            row.add(new StringLiteral(data.getMax())); // max, 200 byte
            row.add(new StringLiteral(data.getMin())); // min, 200 byte
            row.add(nowFn()); // update time, 8 byte

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
            StmtExecutor executor = new StmtExecutor(context, insertStmt);
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
        String sql = "INSERT INTO external_column_statistics values " + String.join(", ", sqlBuffer) + ";";
        List<String> names = Lists.newArrayList("column_0", "column_1", "column_2", "column_3",
                "column_4", "column_5", "column_6", "column_7", "column_8", "column_9", "column_10",
                "column_11", "column_12");
        QueryStatement qs = new QueryStatement(new ValuesRelation(rowsBuffer, names));
        InsertStmt insert = new InsertStmt(new TableName("_statistics_", "external_column_statistics"), qs);
        insert.setOrigStmt(new OriginStatement(sql, 0));
        return insert;
    }
}
