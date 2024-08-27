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


package com.starrocks.connector.hive.glue.metastore;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.hive.glue.util.MetastoreClientUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TException;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.MutableDateTime;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionResponse;
import software.amazon.awssdk.services.glue.model.BinaryColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.BooleanColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.ColumnStatistics;
import software.amazon.awssdk.services.glue.model.ColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.ColumnStatisticsType;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DateColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.DecimalColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.DoubleColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForPartitionResponse;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForTableRequest;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForTableResponse;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsResponse;
import software.amazon.awssdk.services.glue.model.LongColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.PartitionError;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.PartitionValueList;
import software.amazon.awssdk.services.glue.model.Segment;
import software.amazon.awssdk.services.glue.model.StringColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;
import software.amazon.awssdk.services.glue.model.UserDefinedFunctionInput;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.starrocks.connector.hive.glue.util.AWSGlueConfig.CUSTOM_EXECUTOR_FACTORY_CONF;
import static com.starrocks.connector.hive.glue.util.AWSGlueConfig.NUM_PARTITION_SEGMENTS_CONF;
import static java.util.concurrent.CompletableFuture.supplyAsync;

public class DefaultAWSGlueMetastore implements AWSGlueMetastore {

    public static final int BATCH_GET_PARTITIONS_MAX_REQUEST_SIZE = 1000;
    /**
     * Based on the maxResults parameter at https://docs.aws.amazon.com/glue/latest/webapi/API_GetPartitions.html
     */
    public static final int GET_PARTITIONS_MAX_SIZE = 1000;
    /**
     * Maximum number of Glue Segments. A segment defines a non-overlapping region of a table's partitions,
     * allowing multiple requests to be executed in parallel.
     */
    public static final int DEFAULT_NUM_PARTITION_SEGMENTS = 5;
    /**
     * Currently the upper limit allowed by Glue is 10.
     * https://docs.aws.amazon.com/glue/latest/webapi/API_Segment.html
     */
    public static final int MAX_NUM_PARTITION_SEGMENTS = 10;

    // There are read limit for AWS Glue API : GetColumnStatisticsForPartition
    // https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-GetColumnStatisticsForPartition
    public static final int GLUE_COLUMN_STATS_SEGMENT_SIZE = 100;

    private final HiveConf conf;
    private final GlueClient glueClient;
    private final String catalogId;
    private final ExecutorService executorService;
    private final int numPartitionSegments;

    protected ExecutorService getExecutorService(HiveConf hiveConf) {
        Class<? extends ExecutorServiceFactory> executorFactoryClass = hiveConf
                .getClass(CUSTOM_EXECUTOR_FACTORY_CONF,
                        DefaultExecutorServiceFactory.class).asSubclass(
                        ExecutorServiceFactory.class);
        ExecutorServiceFactory factory = ReflectionUtils.newInstance(
                executorFactoryClass, hiveConf);
        return factory.getExecutorService(hiveConf);
    }

    public DefaultAWSGlueMetastore(HiveConf conf, GlueClient glueClient) {
        checkNotNull(conf, "Hive Config cannot be null");
        checkNotNull(glueClient, "glueClient cannot be null");
        this.numPartitionSegments = conf.getInt(NUM_PARTITION_SEGMENTS_CONF, DEFAULT_NUM_PARTITION_SEGMENTS);
        checkArgument(numPartitionSegments <= MAX_NUM_PARTITION_SEGMENTS,
                String.format("Hive Config [%s] can't exceed %d", NUM_PARTITION_SEGMENTS_CONF, MAX_NUM_PARTITION_SEGMENTS));
        this.conf = conf;
        this.glueClient = glueClient;
        this.catalogId = MetastoreClientUtils.getCatalogId(conf);
        this.executorService = getExecutorService(conf);
    }

    // ======================= Database =======================

    @Override
    public void createDatabase(DatabaseInput databaseInput) {
        CreateDatabaseRequest.Builder createDatabaseRequest =
                CreateDatabaseRequest.builder().databaseInput(databaseInput)
                        .catalogId(catalogId);
        glueClient.createDatabase(createDatabaseRequest.build());
    }

    @Override
    public Database getDatabase(String dbName) {
        GetDatabaseRequest.Builder getDatabaseRequest = GetDatabaseRequest.builder().catalogId(catalogId).name(dbName);
        GetDatabaseResponse result = glueClient.getDatabase(getDatabaseRequest.build());
        return result.database();
    }

    @Override
    public List<Database> getAllDatabases() {
        List<Database> ret = Lists.newArrayList();
        String nextToken = null;
        do {
            GetDatabasesRequest.Builder getDatabasesRequest =
                    GetDatabasesRequest.builder().nextToken(nextToken).catalogId(
                    catalogId);
            GetDatabasesResponse result = glueClient.getDatabases(getDatabasesRequest.build());
            nextToken = result.nextToken();
            ret.addAll(result.databaseList());
        } while (nextToken != null);
        return ret;
    }

    @Override
    public void updateDatabase(String databaseName, DatabaseInput databaseInput) {
        UpdateDatabaseRequest.Builder updateDatabaseRequest = UpdateDatabaseRequest.builder().name(databaseName)
                .databaseInput(databaseInput).catalogId(catalogId);
        glueClient.updateDatabase(updateDatabaseRequest.build());
    }

    @Override
    public void deleteDatabase(String dbName) {
        DeleteDatabaseRequest.Builder deleteDatabaseRequest = DeleteDatabaseRequest.builder().name(dbName).catalogId(
                catalogId);
        glueClient.deleteDatabase(deleteDatabaseRequest.build());
    }

    // ======================== Table ========================

    @Override
    public void createTable(String dbName, TableInput tableInput) {
        CreateTableRequest.Builder createTableRequest = CreateTableRequest.builder().tableInput(tableInput)
                .databaseName(dbName).catalogId(catalogId);
        glueClient.createTable(createTableRequest.build());
    }

    @Override
    public Table getTable(String dbName, String tableName) {
        GetTableRequest.Builder getTableRequest = GetTableRequest.builder().databaseName(dbName).name(tableName)
                .catalogId(catalogId);
        GetTableResponse result = glueClient.getTable(getTableRequest.build());
        return result.table();
    }

    @Override
    public List<Table> getTables(String dbname, String tablePattern) {
        List<Table> ret = new ArrayList<>();
        String nextToken = null;
        do {
            GetTablesRequest.Builder getTablesRequest = GetTablesRequest.builder().databaseName(dbname)
                    .expression(tablePattern).nextToken(nextToken).catalogId(catalogId);
            GetTablesResponse result = glueClient.getTables(getTablesRequest.build());
            ret.addAll(result.tableList());
            nextToken = result.nextToken();
        } while (nextToken != null);
        return ret;
    }

    @Override
    public void updateTable(String dbName, TableInput tableInput) {
        UpdateTableRequest.Builder updateTableRequest = UpdateTableRequest.builder().databaseName(dbName)
                .tableInput(tableInput).catalogId(catalogId);
        glueClient.updateTable(updateTableRequest.build());
    }

    @Override
    public void deleteTable(String dbName, String tableName) {
        DeleteTableRequest.Builder deleteTableRequest =
                DeleteTableRequest.builder().databaseName(dbName).name(tableName)
                        .catalogId(catalogId);
        glueClient.deleteTable(deleteTableRequest.build());
    }

    // =========================== Partition ===========================

    @Override
    public Partition getPartition(String dbName, String tableName, List<String> partitionValues) {
        GetPartitionRequest.Builder request = GetPartitionRequest.builder()
                .databaseName(dbName)
                .tableName(tableName)
                .partitionValues(partitionValues)
                .catalogId(catalogId);
        return glueClient.getPartition(request.build()).partition();
    }

    @Override
    public List<Partition> getPartitionsByNames(String dbName, String tableName,
                                                List<PartitionValueList> partitionsToGet) {

        List<List<PartitionValueList>> batchedPartitionsToGet = Lists.partition(partitionsToGet,
                BATCH_GET_PARTITIONS_MAX_REQUEST_SIZE);
        List<Future<BatchGetPartitionResponse>> batchGetPartitionFutures = Lists.newArrayList();

        for (List<PartitionValueList> batch : batchedPartitionsToGet) {
            final BatchGetPartitionRequest.Builder request = BatchGetPartitionRequest.builder()
                    .databaseName(dbName)
                    .tableName(tableName)
                    .partitionsToGet(batch)
                    .catalogId(catalogId);
            batchGetPartitionFutures.add(this.executorService.submit(new Callable<BatchGetPartitionResponse>() {
                @Override
                public BatchGetPartitionResponse call() throws Exception {
                    return glueClient.batchGetPartition(request.build());
                }
            }));
        }

        List<Partition> result = Lists.newArrayList();
        try {
            for (Future<BatchGetPartitionResponse> future : batchGetPartitionFutures) {
                result.addAll(future.get().partitions());
            }
        } catch (ExecutionException e) {
            Throwables.throwIfInstanceOf(e.getCause(), SdkException.class);
            Throwables.throwIfUnchecked(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return result;
    }

    @Override
    public List<Partition> getPartitions(String dbName, String tableName, String expression,
                                         long max) throws TException {
        if (max == 0) {
            return Collections.emptyList();
        }
        if (max < 0 || max > GET_PARTITIONS_MAX_SIZE) {
            return getPartitionsParallel(dbName, tableName, expression, max);
        } else {
            // We don't need to get too many partitions, so just do it serially.
            return getCatalogPartitions(dbName, tableName, expression, max, null);
        }
    }

    private List<Partition> getPartitionsParallel(
            final String databaseName,
            final String tableName,
            final String expression,
            final long max) throws TException {
        // Prepare the segments
        List<Segment> segments = Lists.newArrayList();
        for (int i = 0; i < numPartitionSegments; i++) {
            segments.add(Segment.builder()
                    .segmentNumber(i)
                    .totalSegments(numPartitionSegments).build());
        }
        // Submit Glue API calls in parallel using the thread pool.
        // We could convert this into a parallelStream after upgrading to JDK 8 compiler base.
        List<Future<List<Partition>>> futures = Lists.newArrayList();
        for (final Segment segment : segments) {
            futures.add(this.executorService.submit(new Callable<List<Partition>>() {
                @Override
                public List<Partition> call() throws Exception {
                    return getCatalogPartitions(databaseName, tableName, expression, max, segment);
                }
            }));
        }

        // Get the results
        List<Partition> partitions = Lists.newArrayList();
        try {
            for (Future<List<Partition>> future : futures) {
                List<Partition> segmentPartitions = future.get();
                if (partitions.size() + segmentPartitions.size() >= max && max > 0) {
                    // Extract the required number of partitions from the segment and we're done.
                    long remaining = max - partitions.size();
                    partitions.addAll(segmentPartitions.subList(0, (int) remaining));
                    break;
                } else {
                    partitions.addAll(segmentPartitions);
                }
            }
        } catch (ExecutionException e) {
            Throwables.throwIfInstanceOf(e.getCause(), SdkException.class);
            Throwables.throwIfUnchecked(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return partitions;
    }


    private List<Partition> getCatalogPartitions(String databaseName, String tableName, String expression,
                                                 long max, Segment segment) {
        List<Partition> partitions = Lists.newArrayList();
        String nextToken = null;
        do {
            GetPartitionsRequest.Builder request = GetPartitionsRequest.builder()
                    .databaseName(databaseName)
                    .tableName(tableName)
                    .expression(expression)
                    .nextToken(nextToken)
                    .catalogId(catalogId)
                    .segment(segment);
            GetPartitionsResponse res = glueClient.getPartitions(request.build());
            List<Partition> list = res.partitions();
            if ((partitions.size() + list.size()) >= max && max > 0) {
                long remaining = max - partitions.size();
                partitions.addAll(list.subList(0, (int) remaining));
                break;
            }
            partitions.addAll(list);
            nextToken = res.nextToken();
        } while (nextToken != null);
        return partitions;
    }

    @Override
    public void updatePartition(String dbName, String tableName, List<String> partitionValues,
                                PartitionInput partitionInput) {
        UpdatePartitionRequest.Builder updatePartitionRequest = UpdatePartitionRequest.builder().databaseName(dbName)
                .tableName(tableName).partitionValueList(partitionValues)
                .partitionInput(partitionInput).catalogId(catalogId);
        glueClient.updatePartition(updatePartitionRequest.build());
    }

    @Override
    public void deletePartition(String dbName, String tableName, List<String> partitionValues) {
        DeletePartitionRequest.Builder request = DeletePartitionRequest.builder()
                .databaseName(dbName)
                .tableName(tableName)
                .partitionValues(partitionValues)
                .catalogId(catalogId);
        glueClient.deletePartition(request.build());
    }

    @Override
    public List<PartitionError> createPartitions(String dbName, String tableName,
                                                 List<PartitionInput> partitionInputs) {
        BatchCreatePartitionRequest.Builder request =
                BatchCreatePartitionRequest.builder().databaseName(dbName)
                        .tableName(tableName).catalogId(catalogId)
                        .partitionInputList(partitionInputs);
        return glueClient.batchCreatePartition(request.build()).errors();
    }

    // ====================== User Defined Function ======================

    @Override
    public void createUserDefinedFunction(String dbName, UserDefinedFunctionInput functionInput) {
        CreateUserDefinedFunctionRequest.Builder createUserDefinedFunctionRequest =
                CreateUserDefinedFunctionRequest.builder()
                        .databaseName(dbName).functionInput(functionInput).catalogId(catalogId);
        glueClient.createUserDefinedFunction(createUserDefinedFunctionRequest.build());
    }

    @Override
    public UserDefinedFunction getUserDefinedFunction(String dbName, String functionName) {
        GetUserDefinedFunctionRequest.Builder getUserDefinedFunctionRequest = GetUserDefinedFunctionRequest.builder()
                .databaseName(dbName).functionName(functionName).catalogId(catalogId);
        return glueClient.getUserDefinedFunction(getUserDefinedFunctionRequest.build()).userDefinedFunction();
    }

    @Override
    public List<UserDefinedFunction> getUserDefinedFunctions(String dbName, String pattern) {
        List<UserDefinedFunction> ret = Lists.newArrayList();
        String nextToken = null;
        do {
            GetUserDefinedFunctionsRequest.Builder getUserDefinedFunctionsRequest =
                    GetUserDefinedFunctionsRequest.builder()
                            .databaseName(dbName).pattern(pattern).nextToken(nextToken).catalogId(catalogId);
            GetUserDefinedFunctionsResponse result =
                    glueClient.getUserDefinedFunctions(getUserDefinedFunctionsRequest.build());
            nextToken = result.nextToken();
            ret.addAll(result.userDefinedFunctions());
        } while (nextToken != null);
        return ret;
    }

    @Override
    public void deleteUserDefinedFunction(String dbName, String functionName) {
        DeleteUserDefinedFunctionRequest.Builder deleteUserDefinedFunctionRequest =
                DeleteUserDefinedFunctionRequest.builder()
                        .databaseName(dbName).functionName(functionName).catalogId(catalogId);
        glueClient.deleteUserDefinedFunction(deleteUserDefinedFunctionRequest.build());
    }

    @Override
    public void updateUserDefinedFunction(String dbName, String functionName, UserDefinedFunctionInput functionInput) {
        UpdateUserDefinedFunctionRequest.Builder updateUserDefinedFunctionRequest =
                UpdateUserDefinedFunctionRequest.builder()
                        .databaseName(dbName).functionName(functionName).functionInput(functionInput)
                        .catalogId(catalogId);
        glueClient.updateUserDefinedFunction(updateUserDefinedFunctionRequest.build());
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName, List<String> colNames) {
        // Split colNames into column chunks
        List<List<String>> columnChunks = Lists.partition(colNames, GLUE_COLUMN_STATS_SEGMENT_SIZE);

        // Submit Glue API calls in parallel using the thread pool.
        List<CompletableFuture<GetColumnStatisticsForTableResponse>> futures = columnChunks.stream().
                map(columnChunk -> supplyAsync(() -> {
                    GetColumnStatisticsForTableRequest.Builder request = GetColumnStatisticsForTableRequest.builder()
                            .databaseName(dbName)
                            .tableName(tableName)
                            .columnNames(columnChunk);
                    return glueClient.getColumnStatisticsForTable(request.build());
                }, this.executorService)).collect(Collectors.toList());

        // Get the column statistics
        Map<String, ColumnStatisticsObj> columnStatisticsMap = Maps.newHashMap();
        try {
            for (CompletableFuture<GetColumnStatisticsForTableResponse> future : futures) {
                GetColumnStatisticsForTableResponse columnStatisticsForTableResult = future.get();
                for (ColumnStatistics columnStatistics : columnStatisticsForTableResult.columnStatisticsList()) {
                    columnStatisticsMap.put(columnStatistics.columnName(),
                            toHiveColumnStatistic(columnStatistics));
                }
            }
        } catch (ExecutionException e) {
            Throwables.throwIfInstanceOf(e.getCause(), SdkException.class);
            Throwables.throwIfUnchecked(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return new ArrayList<>(columnStatisticsMap.values());
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName, String tableName,
                                                                               List<String> partitionNames,
                                                                               List<String> colNames) {
        Map<String, List<CompletableFuture<GetColumnStatisticsForPartitionResponse>>> futureForPartition =
                Maps.newHashMap();
        for (String partitionName : partitionNames) {
            // get column statistics for each partition
            List<String> partitionValue = PartitionUtil.toPartitionValues(partitionName);
            // Split colNames into column chunks
            List<List<String>> columnChunks = Lists.partition(colNames, GLUE_COLUMN_STATS_SEGMENT_SIZE);
            futureForPartition.put(partitionName, columnChunks.stream().map(columnChunk -> supplyAsync(() -> {
                GetColumnStatisticsForPartitionRequest.Builder request =
                        GetColumnStatisticsForPartitionRequest.builder()
                                .databaseName(dbName)
                                .tableName(tableName)
                                .columnNames(columnChunk)
                                .partitionValues(partitionValue);
                return glueClient.getColumnStatisticsForPartition(request.build());
            }, this.executorService)).collect(Collectors.toList()));
        }

        Map<String, List<ColumnStatisticsObj>> partitionColumnStats = Maps.newHashMap();
        futureForPartition.forEach((partitionName, futures) -> {
            List<ColumnStatisticsObj> columnStatisticsObjs = Lists.newArrayList();
            for (CompletableFuture<GetColumnStatisticsForPartitionResponse> future : futures) {
                try {
                    GetColumnStatisticsForPartitionResponse result = future.get();
                    result.columnStatisticsList().forEach(columnStatistics ->
                            columnStatisticsObjs.add(toHiveColumnStatistic(columnStatistics)));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    Throwables.throwIfInstanceOf(e.getCause(), SdkException.class);
                    Throwables.throwIfUnchecked(e.getCause());
                }
            }
            partitionColumnStats.put(partitionName, columnStatisticsObjs);
        });

        return partitionColumnStats;
    }

    private ColumnStatisticsObj toHiveColumnStatistic(ColumnStatistics columnStatistics) {
        ColumnStatisticsData columnStatisticsData = columnStatistics.statisticsData();
        ColumnStatisticsType type = columnStatisticsData.type();
        String columnName = columnStatistics.columnName();
        switch (type) {
            case BINARY: {
                BinaryColumnStatisticsData data = columnStatisticsData.binaryColumnStatisticsData();
                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.binaryStats(
                                new BinaryColumnStatsData(data.maximumLength(), data.averageLength(),
                                        data.numberOfNulls()));
                return new ColumnStatisticsObj(columnName, "BINARY", hiveColumnStatisticsData);
            }
            case BOOLEAN: {
                BooleanColumnStatisticsData data = columnStatisticsData.booleanColumnStatisticsData();
                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.booleanStats(
                                new BooleanColumnStatsData(data.numberOfTrues(), data.numberOfFalses(),
                                        data.numberOfNulls()));
                return new ColumnStatisticsObj(columnName, "BOOLEAN", hiveColumnStatisticsData);
            }
            case DATE: {
                DateColumnStatisticsData data = columnStatisticsData.dateColumnStatisticsData();
                DateColumnStatsData dateColumnStatsData = new DateColumnStatsData(data.numberOfNulls(),
                        data.numberOfDistinctValues());
                MutableDateTime epoch = new MutableDateTime();
                epoch.setDate(0); //Set to Epoch time
                dateColumnStatsData.setHighValue(new Date(Days.daysBetween(epoch,
                        new DateTime(data.maximumValue().getEpochSecond() * 1000)).getDays()));
                dateColumnStatsData.setLowValue(new Date(Days.daysBetween(epoch,
                        new DateTime(data.minimumValue().getEpochSecond() * 1000)).getDays()));

                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.dateStats(dateColumnStatsData);
                return new ColumnStatisticsObj(columnName, "DATE", hiveColumnStatisticsData);
            }
            case DECIMAL: {
                DecimalColumnStatisticsData data = columnStatisticsData.decimalColumnStatisticsData();
                DecimalColumnStatsData decimalColumnStatsData = new DecimalColumnStatsData(data.numberOfNulls(),
                        data.numberOfDistinctValues());
                decimalColumnStatsData.setHighValue(new Decimal(data.maximumValue().scale().shortValue(),
                        data.maximumValue().unscaledValue().asByteBuffer()));
                decimalColumnStatsData.setLowValue(new Decimal(data.minimumValue().scale().shortValue(),
                        data.minimumValue().unscaledValue().asByteBuffer()));

                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.decimalStats(decimalColumnStatsData);
                return new ColumnStatisticsObj(columnName, "DECIMAL", hiveColumnStatisticsData);
            }
            case DOUBLE: {
                DoubleColumnStatisticsData data = columnStatisticsData.doubleColumnStatisticsData();
                DoubleColumnStatsData doubleColumnStatsData = new DoubleColumnStatsData(data.numberOfNulls(),
                        data.numberOfDistinctValues());
                doubleColumnStatsData.setHighValue(data.maximumValue());
                doubleColumnStatsData.setLowValue(data.minimumValue());

                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.doubleStats(doubleColumnStatsData);
                return new ColumnStatisticsObj(columnName, "DOUBLE", hiveColumnStatisticsData);
            }
            case LONG: {
                LongColumnStatisticsData data = columnStatisticsData.longColumnStatisticsData();
                LongColumnStatsData longColumnStatsData = new LongColumnStatsData(data.numberOfNulls(),
                        data.numberOfDistinctValues());
                longColumnStatsData.setHighValue(data.maximumValue());
                longColumnStatsData.setLowValue(data.minimumValue());

                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.longStats(longColumnStatsData);
                return new ColumnStatisticsObj(columnName, "LONG", hiveColumnStatisticsData);
            }
            case STRING: {
                StringColumnStatisticsData data = columnStatisticsData.stringColumnStatisticsData();
                StringColumnStatsData stringColumnStatsData = new StringColumnStatsData(data.maximumLength(),
                        data.averageLength(), data.numberOfNulls(), data.numberOfDistinctValues());

                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.stringStats(stringColumnStatsData);
                return new ColumnStatisticsObj(columnName, "STRING", hiveColumnStatisticsData);
            }
        }

        throw new RuntimeException("Invalid glue column statistics : " + columnStatistics);
    }
}