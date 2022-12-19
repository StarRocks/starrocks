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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchGetPartitionRequest;
import com.amazonaws.services.glue.model.BatchGetPartitionResult;
import com.amazonaws.services.glue.model.BinaryColumnStatisticsData;
import com.amazonaws.services.glue.model.BooleanColumnStatisticsData;
import com.amazonaws.services.glue.model.ColumnStatistics;
import com.amazonaws.services.glue.model.ColumnStatisticsData;
import com.amazonaws.services.glue.model.ColumnStatisticsType;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.CreateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.DateColumnStatisticsData;
import com.amazonaws.services.glue.model.DecimalColumnStatisticsData;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.DeleteUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.DoubleColumnStatisticsData;
import com.amazonaws.services.glue.model.GetColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsForPartitionResult;
import com.amazonaws.services.glue.model.GetColumnStatisticsForTableRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsForTableResult;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabaseResult;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetPartitionsResult;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsResult;
import com.amazonaws.services.glue.model.LongColumnStatisticsData;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionError;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.PartitionValueList;
import com.amazonaws.services.glue.model.Segment;
import com.amazonaws.services.glue.model.StringColumnStatisticsData;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateDatabaseRequest;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.amazonaws.services.glue.model.UpdateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.UserDefinedFunction;
import com.amazonaws.services.glue.model.UserDefinedFunctionInput;
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
    private final AWSGlue glueClient;
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

    public DefaultAWSGlueMetastore(HiveConf conf, AWSGlue glueClient) {
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
        CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest().withDatabaseInput(databaseInput)
                .withCatalogId(catalogId);
        glueClient.createDatabase(createDatabaseRequest);
    }

    @Override
    public Database getDatabase(String dbName) {
        GetDatabaseRequest getDatabaseRequest = new GetDatabaseRequest().withCatalogId(catalogId).withName(dbName);
        GetDatabaseResult result = glueClient.getDatabase(getDatabaseRequest);
        return result.getDatabase();
    }

    @Override
    public List<Database> getAllDatabases() {
        List<Database> ret = Lists.newArrayList();
        String nextToken = null;
        do {
            GetDatabasesRequest getDatabasesRequest = new GetDatabasesRequest().withNextToken(nextToken).withCatalogId(
                    catalogId);
            GetDatabasesResult result = glueClient.getDatabases(getDatabasesRequest);
            nextToken = result.getNextToken();
            ret.addAll(result.getDatabaseList());
        } while (nextToken != null);
        return ret;
    }

    @Override
    public void updateDatabase(String databaseName, DatabaseInput databaseInput) {
        UpdateDatabaseRequest updateDatabaseRequest = new UpdateDatabaseRequest().withName(databaseName)
                .withDatabaseInput(databaseInput).withCatalogId(catalogId);
        glueClient.updateDatabase(updateDatabaseRequest);
    }

    @Override
    public void deleteDatabase(String dbName) {
        DeleteDatabaseRequest deleteDatabaseRequest = new DeleteDatabaseRequest().withName(dbName).withCatalogId(
                catalogId);
        glueClient.deleteDatabase(deleteDatabaseRequest);
    }

    // ======================== Table ========================

    @Override
    public void createTable(String dbName, TableInput tableInput) {
        CreateTableRequest createTableRequest = new CreateTableRequest().withTableInput(tableInput)
                .withDatabaseName(dbName).withCatalogId(catalogId);
        glueClient.createTable(createTableRequest);
    }

    @Override
    public Table getTable(String dbName, String tableName) {
        GetTableRequest getTableRequest = new GetTableRequest().withDatabaseName(dbName).withName(tableName)
                .withCatalogId(catalogId);
        GetTableResult result = glueClient.getTable(getTableRequest);
        return result.getTable();
    }

    @Override
    public List<Table> getTables(String dbname, String tablePattern) {
        List<Table> ret = new ArrayList<>();
        String nextToken = null;
        do {
            GetTablesRequest getTablesRequest = new GetTablesRequest().withDatabaseName(dbname)
                    .withExpression(tablePattern).withNextToken(nextToken).withCatalogId(catalogId);
            GetTablesResult result = glueClient.getTables(getTablesRequest);
            ret.addAll(result.getTableList());
            nextToken = result.getNextToken();
        } while (nextToken != null);
        return ret;
    }

    @Override
    public void updateTable(String dbName, TableInput tableInput) {
        UpdateTableRequest updateTableRequest = new UpdateTableRequest().withDatabaseName(dbName)
                .withTableInput(tableInput).withCatalogId(catalogId);
        glueClient.updateTable(updateTableRequest);
    }

    @Override
    public void deleteTable(String dbName, String tableName) {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest().withDatabaseName(dbName).withName(tableName)
                .withCatalogId(catalogId);
        glueClient.deleteTable(deleteTableRequest);
    }

    // =========================== Partition ===========================

    @Override
    public Partition getPartition(String dbName, String tableName, List<String> partitionValues) {
        GetPartitionRequest request = new GetPartitionRequest()
                .withDatabaseName(dbName)
                .withTableName(tableName)
                .withPartitionValues(partitionValues)
                .withCatalogId(catalogId);
        return glueClient.getPartition(request).getPartition();
    }

    @Override
    public List<Partition> getPartitionsByNames(String dbName, String tableName,
                                                List<PartitionValueList> partitionsToGet) {

        List<List<PartitionValueList>> batchedPartitionsToGet = Lists.partition(partitionsToGet,
                BATCH_GET_PARTITIONS_MAX_REQUEST_SIZE);
        List<Future<BatchGetPartitionResult>> batchGetPartitionFutures = Lists.newArrayList();

        for (List<PartitionValueList> batch : batchedPartitionsToGet) {
            final BatchGetPartitionRequest request = new BatchGetPartitionRequest()
                    .withDatabaseName(dbName)
                    .withTableName(tableName)
                    .withPartitionsToGet(batch)
                    .withCatalogId(catalogId);
            batchGetPartitionFutures.add(this.executorService.submit(new Callable<BatchGetPartitionResult>() {
                @Override
                public BatchGetPartitionResult call() throws Exception {
                    return glueClient.batchGetPartition(request);
                }
            }));
        }

        List<Partition> result = Lists.newArrayList();
        try {
            for (Future<BatchGetPartitionResult> future : batchGetPartitionFutures) {
                result.addAll(future.get().getPartitions());
            }
        } catch (ExecutionException e) {
            Throwables.throwIfInstanceOf(e.getCause(), AmazonServiceException.class);
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
            segments.add(new Segment()
                    .withSegmentNumber(i)
                    .withTotalSegments(numPartitionSegments));
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
            Throwables.throwIfInstanceOf(e.getCause(), AmazonServiceException.class);
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
            GetPartitionsRequest request = new GetPartitionsRequest()
                    .withDatabaseName(databaseName)
                    .withTableName(tableName)
                    .withExpression(expression)
                    .withNextToken(nextToken)
                    .withCatalogId(catalogId)
                    .withSegment(segment);
            GetPartitionsResult res = glueClient.getPartitions(request);
            List<Partition> list = res.getPartitions();
            if ((partitions.size() + list.size()) >= max && max > 0) {
                long remaining = max - partitions.size();
                partitions.addAll(list.subList(0, (int) remaining));
                break;
            }
            partitions.addAll(list);
            nextToken = res.getNextToken();
        } while (nextToken != null);
        return partitions;
    }

    @Override
    public void updatePartition(String dbName, String tableName, List<String> partitionValues,
                                PartitionInput partitionInput) {
        UpdatePartitionRequest updatePartitionRequest = new UpdatePartitionRequest().withDatabaseName(dbName)
                .withTableName(tableName).withPartitionValueList(partitionValues)
                .withPartitionInput(partitionInput).withCatalogId(catalogId);
        glueClient.updatePartition(updatePartitionRequest);
    }

    @Override
    public void deletePartition(String dbName, String tableName, List<String> partitionValues) {
        DeletePartitionRequest request = new DeletePartitionRequest()
                .withDatabaseName(dbName)
                .withTableName(tableName)
                .withPartitionValues(partitionValues)
                .withCatalogId(catalogId);
        glueClient.deletePartition(request);
    }

    @Override
    public List<PartitionError> createPartitions(String dbName, String tableName,
                                                 List<PartitionInput> partitionInputs) {
        BatchCreatePartitionRequest request =
                new BatchCreatePartitionRequest().withDatabaseName(dbName)
                .withTableName(tableName).withCatalogId(catalogId)
                .withPartitionInputList(partitionInputs);
        return glueClient.batchCreatePartition(request).getErrors();
    }

    // ====================== User Defined Function ======================

    @Override
    public void createUserDefinedFunction(String dbName, UserDefinedFunctionInput functionInput) {
        CreateUserDefinedFunctionRequest createUserDefinedFunctionRequest = new CreateUserDefinedFunctionRequest()
                .withDatabaseName(dbName).withFunctionInput(functionInput).withCatalogId(catalogId);
        glueClient.createUserDefinedFunction(createUserDefinedFunctionRequest);
    }

    @Override
    public UserDefinedFunction getUserDefinedFunction(String dbName, String functionName) {
        GetUserDefinedFunctionRequest getUserDefinedFunctionRequest = new GetUserDefinedFunctionRequest()
                .withDatabaseName(dbName).withFunctionName(functionName).withCatalogId(catalogId);
        return glueClient.getUserDefinedFunction(getUserDefinedFunctionRequest).getUserDefinedFunction();
    }

    @Override
    public List<UserDefinedFunction> getUserDefinedFunctions(String dbName, String pattern) {
        List<UserDefinedFunction> ret = Lists.newArrayList();
        String nextToken = null;
        do {
            GetUserDefinedFunctionsRequest getUserDefinedFunctionsRequest = new GetUserDefinedFunctionsRequest()
                    .withDatabaseName(dbName).withPattern(pattern).withNextToken(nextToken).withCatalogId(catalogId);
            GetUserDefinedFunctionsResult result = glueClient.getUserDefinedFunctions(getUserDefinedFunctionsRequest);
            nextToken = result.getNextToken();
            ret.addAll(result.getUserDefinedFunctions());
        } while (nextToken != null);
        return ret;
    }

    @Override
    public void deleteUserDefinedFunction(String dbName, String functionName) {
        DeleteUserDefinedFunctionRequest deleteUserDefinedFunctionRequest = new DeleteUserDefinedFunctionRequest()
                .withDatabaseName(dbName).withFunctionName(functionName).withCatalogId(catalogId);
        glueClient.deleteUserDefinedFunction(deleteUserDefinedFunctionRequest);
    }

    @Override
    public void updateUserDefinedFunction(String dbName, String functionName, UserDefinedFunctionInput functionInput) {
        UpdateUserDefinedFunctionRequest updateUserDefinedFunctionRequest = new UpdateUserDefinedFunctionRequest()
                .withDatabaseName(dbName).withFunctionName(functionName).withFunctionInput(functionInput)
                .withCatalogId(catalogId);
        glueClient.updateUserDefinedFunction(updateUserDefinedFunctionRequest);
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName, List<String> colNames) {
        // Split colNames into column chunks
        List<List<String>> columnChunks = Lists.partition(colNames, GLUE_COLUMN_STATS_SEGMENT_SIZE);

        // Submit Glue API calls in parallel using the thread pool.
        List<CompletableFuture<GetColumnStatisticsForTableResult>> futures = columnChunks.stream().
                map(columnChunk -> supplyAsync(() -> {
                    GetColumnStatisticsForTableRequest request = new GetColumnStatisticsForTableRequest()
                            .withDatabaseName(dbName)
                            .withTableName(tableName)
                            .withColumnNames(columnChunk);
                    return glueClient.getColumnStatisticsForTable(request);
                }, this.executorService)).collect(Collectors.toList());

        // Get the column statistics
        Map<String, ColumnStatisticsObj> columnStatisticsMap = Maps.newHashMap();
        try {
            for (CompletableFuture<GetColumnStatisticsForTableResult> future : futures) {
                GetColumnStatisticsForTableResult columnStatisticsForTableResult = future.get();
                for (ColumnStatistics columnStatistics : columnStatisticsForTableResult.getColumnStatisticsList()) {
                    columnStatisticsMap.put(columnStatistics.getColumnName(),
                            toHiveColumnStatistic(columnStatistics));
                }
            }
        } catch (ExecutionException e) {
            Throwables.throwIfInstanceOf(e.getCause(), AmazonServiceException.class);
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
        Map<String, List<CompletableFuture<GetColumnStatisticsForPartitionResult>>> futureForPartition =
                Maps.newHashMap();
        for (String partitionName : partitionNames) {
            // get column statistics for each partition
            List<String> partitionValue = PartitionUtil.toPartitionValues(partitionName);
            // Split colNames into column chunks
            List<List<String>> columnChunks = Lists.partition(colNames, GLUE_COLUMN_STATS_SEGMENT_SIZE);
            futureForPartition.put(partitionName, columnChunks.stream().map(columnChunk -> supplyAsync(() -> {
                GetColumnStatisticsForPartitionRequest request = new GetColumnStatisticsForPartitionRequest()
                        .withDatabaseName(dbName)
                        .withTableName(tableName)
                        .withColumnNames(columnChunk)
                        .withPartitionValues(partitionValue);
                return glueClient.getColumnStatisticsForPartition(request);
            }, this.executorService)).collect(Collectors.toList()));
        }

        Map<String, List<ColumnStatisticsObj>> partitionColumnStats = Maps.newHashMap();
        futureForPartition.forEach((partitionName, futures) -> {
            List<ColumnStatisticsObj> columnStatisticsObjs = Lists.newArrayList();
            for (CompletableFuture<GetColumnStatisticsForPartitionResult> future : futures) {
                try {
                    GetColumnStatisticsForPartitionResult result = future.get();
                    result.getColumnStatisticsList().forEach(columnStatistics ->
                            columnStatisticsObjs.add(toHiveColumnStatistic(columnStatistics)));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    Throwables.throwIfInstanceOf(e.getCause(), AmazonServiceException.class);
                    Throwables.throwIfUnchecked(e.getCause());
                }
            }
            partitionColumnStats.put(partitionName, columnStatisticsObjs);
        });

        return partitionColumnStats;
    }

    private ColumnStatisticsObj toHiveColumnStatistic(ColumnStatistics columnStatistics) {
        ColumnStatisticsData columnStatisticsData = columnStatistics.getStatisticsData();
        ColumnStatisticsType type = ColumnStatisticsType.fromValue(columnStatisticsData.getType());
        String columnName = columnStatistics.getColumnName();
        switch (type) {
            case BINARY: {
                BinaryColumnStatisticsData data = columnStatisticsData.getBinaryColumnStatisticsData();
                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.binaryStats(
                                new BinaryColumnStatsData(data.getMaximumLength(), data.getAverageLength(),
                                        data.getNumberOfNulls()));
                return new ColumnStatisticsObj(columnName, "BINARY", hiveColumnStatisticsData);
            }
            case BOOLEAN: {
                BooleanColumnStatisticsData data = columnStatisticsData.getBooleanColumnStatisticsData();
                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.booleanStats(
                                new BooleanColumnStatsData(data.getNumberOfTrues(), data.getNumberOfFalses(),
                                        data.getNumberOfNulls()));
                return new ColumnStatisticsObj(columnName, "BOOLEAN", hiveColumnStatisticsData);
            }
            case DATE: {
                DateColumnStatisticsData data = columnStatisticsData.getDateColumnStatisticsData();
                DateColumnStatsData dateColumnStatsData = new DateColumnStatsData(data.getNumberOfNulls(),
                        data.getNumberOfDistinctValues());
                MutableDateTime epoch = new MutableDateTime();
                epoch.setDate(0); //Set to Epoch time
                dateColumnStatsData.setHighValue(new Date(Days.daysBetween(epoch,
                        new DateTime(data.getMaximumValue().getTime())).getDays()));
                dateColumnStatsData.setLowValue(new Date(Days.daysBetween(epoch,
                        new DateTime(data.getMinimumValue().getTime())).getDays()));

                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.dateStats(dateColumnStatsData);
                return new ColumnStatisticsObj(columnName, "DATE", hiveColumnStatisticsData);
            }
            case DECIMAL: {
                DecimalColumnStatisticsData data = columnStatisticsData.getDecimalColumnStatisticsData();
                DecimalColumnStatsData decimalColumnStatsData = new DecimalColumnStatsData(data.getNumberOfNulls(),
                        data.getNumberOfDistinctValues());
                decimalColumnStatsData.setHighValue(new Decimal(data.getMaximumValue().getScale().shortValue(),
                        data.getMaximumValue().getUnscaledValue()));
                decimalColumnStatsData.setLowValue(new Decimal(data.getMinimumValue().getScale().shortValue(),
                        data.getMinimumValue().getUnscaledValue()));

                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.decimalStats(decimalColumnStatsData);
                return new ColumnStatisticsObj(columnName, "DECIMAL", hiveColumnStatisticsData);
            }
            case DOUBLE: {
                DoubleColumnStatisticsData data = columnStatisticsData.getDoubleColumnStatisticsData();
                DoubleColumnStatsData doubleColumnStatsData = new DoubleColumnStatsData(data.getNumberOfNulls(),
                        data.getNumberOfDistinctValues());
                doubleColumnStatsData.setHighValue(data.getMaximumValue());
                doubleColumnStatsData.setLowValue(data.getMinimumValue());

                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.doubleStats(doubleColumnStatsData);
                return new ColumnStatisticsObj(columnName, "DOUBLE", hiveColumnStatisticsData);
            }
            case LONG: {
                LongColumnStatisticsData data = columnStatisticsData.getLongColumnStatisticsData();
                LongColumnStatsData longColumnStatsData = new LongColumnStatsData(data.getNumberOfNulls(),
                        data.getNumberOfDistinctValues());
                longColumnStatsData.setHighValue(data.getMaximumValue());
                longColumnStatsData.setLowValue(data.getMinimumValue());

                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.longStats(longColumnStatsData);
                return new ColumnStatisticsObj(columnName, "LONG", hiveColumnStatisticsData);
            }
            case STRING: {
                StringColumnStatisticsData data = columnStatisticsData.getStringColumnStatisticsData();
                StringColumnStatsData stringColumnStatsData = new StringColumnStatsData(data.getMaximumLength(),
                        data.getAverageLength(), data.getNumberOfNulls(), data.getNumberOfDistinctValues());

                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.stringStats(stringColumnStatsData);
                return new ColumnStatisticsObj(columnName, "STRING", hiveColumnStatisticsData);
            }
        }

        throw new RuntimeException("Invalid glue column statistics : " + columnStatistics);
    }
}