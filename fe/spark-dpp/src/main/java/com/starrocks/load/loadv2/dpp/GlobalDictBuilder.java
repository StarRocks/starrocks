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

package com.starrocks.load.loadv2.dpp;

import com.starrocks.load.loadv2.datasource.DataSource;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * used for build hive global dict and encode source hive table
 * <p>
 * input: a source hive table
 * output: a intermediate hive table whose distinct column is encode with int value
 * <p>
 * usage example
 * step1,create a intermediate hive table
 * GlobalDictBuilder.createHiveIntermediateTable()
 * step2, get distinct column's value
 * GlobalDictBuilder.extractDistinctColumn()
 * step3, build global dict
 * GlobalDictBuilder.buildGlobalDict()
 * step4, encode intermediate hive table with global dict
 * GlobalDictBuilder.encodeStarRocksIntermediateHiveTable()
 */

public class GlobalDictBuilder {

    protected static final Logger LOG = LogManager.getLogger(GlobalDictBuilder.class);

    // name of the column in starrocks table which need to build global dict
    // for example: some dict columns a,b,c
    // case 1: all dict columns has no relation, then the map is as below
    //     [a=null, b=null, c=null]
    // case 2: column a's value can reuse column b's value which means column a's value is a subset of column b's value
    //  [b=a,c=null]
    private final MultiValueMap dictColumn;
    // intermediate table columns in current spark load job
    private final List<String> intermediateTableColumnList;

    // distinct columns which need to use map join to solve data skew in encodeStarRocksIntermediateHiveTable()
    // we needn't specify it until data skew happens
    private final List<String> mapSideJoinColumns;

    // source hive database name
    private final String starrocksHiveDB;

    // hive table datasource,format is db.table
    private final String sourceHiveDBTableName;
    // user-specified filter when query sourceHiveDBTable
    private final String sourceHiveFilter;
    // intermediate hive table to store the distinct value of distinct column
    private final String distinctKeyTableName;
    // current starrocks table's global dict hive table
    private String globalDictTableName;

    // used for next step to read
    private final String starrocksIntermediateHiveTable;
    private final SparkSession spark;

    // key=starrocks column name,value=column type
    private final Map<String, String> starrocksColumnNameTypeMap = new HashMap<>();

    // column in this list means need split distinct value and then encode respectively
    // to avoid the performance bottleneck to transfer origin value to dict value
    private final List<String> veryHighCardinalityColumn;
    // determine the split num of new distinct value,better can be divisible by 1
    private final int veryHighCardinalityColumnSplitNum;

    private final ExecutorService pool;

    private StructType distinctValueSchema;

    private final DataSource dataSource;

    public GlobalDictBuilder(MultiValueMap dictColumn,
                             List<String> intermediateTableColumnList,
                             List<String> mapSideJoinColumns,
                             String sourceHiveDBTableName,
                             String sourceHiveFilter,
                             String starrocksHiveDB,
                             String distinctKeyTableName,
                             String globalDictTableName,
                             String starrocksIntermediateHiveTable,
                             int buildConcurrency,
                             List<String> veryHighCardinalityColumn,
                             int veryHighCardinalityColumnSplitNum,
                             SparkSession spark,
                             Map<String, String> dataSourceOptions) {
        this.dictColumn = dictColumn;
        this.intermediateTableColumnList = intermediateTableColumnList;
        this.mapSideJoinColumns = mapSideJoinColumns;
        this.sourceHiveDBTableName = sourceHiveDBTableName;
        this.sourceHiveFilter = sourceHiveFilter;
        this.starrocksHiveDB = starrocksHiveDB;
        this.distinctKeyTableName = distinctKeyTableName;
        this.globalDictTableName = globalDictTableName;
        this.starrocksIntermediateHiveTable = starrocksIntermediateHiveTable;
        this.spark = spark;
        this.pool = Executors.newFixedThreadPool(buildConcurrency < 0 ? 1 : buildConcurrency);
        this.veryHighCardinalityColumn = veryHighCardinalityColumn;
        this.veryHighCardinalityColumnSplitNum = veryHighCardinalityColumnSplitNum;
        this.dataSource = DataSource.fromProperties(spark, dataSourceOptions);

        LOG.info("use " + starrocksHiveDB);
    }

    /**
     * Check if doris global dict table already exist.
     * If exist, use old name for compatibility.
     *
     * @param dorisGlobalDictTableName old global dict table name in previous version
     */
    public void checkGlobalDictTableName(String dorisGlobalDictTableName) {
        try {
            dataSource.getOrLoadTable(starrocksHiveDB, dorisGlobalDictTableName);
            globalDictTableName = dorisGlobalDictTableName;
        } catch (Exception e) {
            // should do nothing.
        }
        LOG.info("global dict table name: " + globalDictTableName);
    }

    public void createHiveIntermediateTable() throws AnalysisException {
        Dataset<Row> sourceHiveDBTable = dataSource.getOrLoadTable(sourceHiveDBTableName);

        Map<String, DataType> sourceHiveTableColumn = Arrays.stream(sourceHiveDBTable.schema().fields())
                .map(structField -> new ImmutablePair<>(structField.name().toLowerCase(), structField.dataType()))
                .collect(Collectors.toMap(ImmutablePair<String, DataType>::getLeft, ImmutablePair<String, DataType>::getRight));

        // check and get starrocks column type in hive
        intermediateTableColumnList.stream().map(String::toLowerCase).forEach(columnName -> {
            String columnType = sourceHiveTableColumn.get(columnName).typeName().toLowerCase();
            if (StringUtils.isEmpty(columnType)) {
                throw new RuntimeException(String.format("starrocks column %s not in source hive table", columnName));
            }
            starrocksColumnNameTypeMap.put(columnName, columnType);
        });

        Set<String> allDictColumn = new HashSet<>();
        allDictColumn.addAll(dictColumn.keySet());
        allDictColumn.addAll(dictColumn.values());

        List<String> targetColumns = intermediateTableColumnList.stream().map(columnName -> {
            if (allDictColumn.contains(columnName)) {
                return "cast(" + columnName + " as string) as " + columnName;
            } else {
                return "cast(" + columnName + " as " + starrocksColumnNameTypeMap.get(columnName) + ") as " + columnName;
            }
        }).collect(Collectors.toList());

        Dataset<Row> output = sourceHiveDBTable.selectExpr(
                JavaConverters.collectionAsScalaIterableConverter(targetColumns).asScala().toSeq());
        if (!StringUtils.isEmpty(sourceHiveFilter)) {
            output = output.where(sourceHiveFilter);
        }
        dataSource.writeTable(output, SaveMode.Overwrite, null, starrocksHiveDB, starrocksIntermediateHiveTable);
    }

    public void extractDistinctColumn() {
        Dataset<Row> intermediateHiveTable = dataSource.getOrLoadTable(starrocksHiveDB, starrocksIntermediateHiveTable);

        // extract distinct column
        List<GlobalDictBuildWorker> workerList = new ArrayList<>();
        // For the column in dictColumn's valueSet, their value is a subset of column in keyset,
        // so we don't need to extract distinct value of column in valueSet
        for (Object column : dictColumn.keySet()) {
            String columnName = column.toString();
            Dataset<Row> distinctColumn = intermediateHiveTable
                    .selectExpr(columnName, "'" + columnName + "' as dict_column")
                    .distinct()
                    .toDF("dict_key", "dict_column");

            String partitionSpec = "dict_column=" + column;
            workerList.add(() -> dataSource.writeTable(
                    distinctColumn, SaveMode.Overwrite, partitionSpec, starrocksHiveDB, distinctKeyTableName));
        }

        submitWorker(workerList);
    }

    public void buildGlobalDict() throws ExecutionException, InterruptedException {
        createIfNotExistGlobalDictTable();
        List<GlobalDictBuildWorker> globalDictBuildWorkers = new ArrayList<>();
        for (Object distinctColumnNameOrigin : dictColumn.keySet()) {
            final String distinctColumnName = distinctColumnNameOrigin.toString();
            globalDictBuildWorkers.add(() -> {
                List<Row> maxGlobalDictValueRow = dataSource.getOrLoadTable(starrocksHiveDB, globalDictTableName)
                        .where("dict_column='" + distinctColumnName + "'")
                        .selectExpr("max(dict_value) as max_value", "min(dict_value) as min_value")
                        .collectAsList();
                if (maxGlobalDictValueRow.size() == 0) {
                    throw new RuntimeException(String.format("get max dict value failed: %s", distinctColumnName));
                }

                long maxDictValue = 0;
                long minDictValue = 0;
                Row row = maxGlobalDictValueRow.get(0);
                if (row != null && row.get(0) != null) {
                    maxDictValue = (long) row.get(0);
                    minDictValue = (long) row.get(1);
                }
                LOG.info("column " + distinctColumnName + " 's max value in dict is " + maxDictValue +
                        ", min value is " + minDictValue);

                // maybe never happened, but we need detect it
                if (minDictValue < 0) {
                    throw new RuntimeException(String.format(" column %s 's cardinality has exceed bigint's max value",
                        distinctColumnName));
                }

                if (veryHighCardinalityColumn.contains(distinctColumnName) &&
                        veryHighCardinalityColumnSplitNum > 1) {
                    // split distinct key first and then encode with count
                    buildGlobalDictBySplit(maxDictValue, distinctColumnName);
                } else {
                    Dataset<Row> source = dataSource.getOrLoadTable(starrocksHiveDB, globalDictTableName)
                            .select("dict_key", "dict_value")
                            .where("dict_column='" + distinctColumnName + "'");

                    Dataset<Row> t1 = dataSource.getOrLoadTable(starrocksHiveDB, distinctKeyTableName)
                            .select("dict_key")
                            .where("dict_column='" + distinctColumnName + "'")
                            .where("dict_key is not null")
                            .alias("t1");

                    Dataset<Row> t2 = source.alias("t2");

                    Dataset<Row> joinedTable = t1
                            .join(t2, t1.col("dict_key").equalTo(t2.col("dict_key")), "leftouter")
                            .where(t2.col("dict_value").isNull())
                            .selectExpr(
                                "t1.dict_key as dict_key",
                                "(row_number() over(order by t1.dict_key)) + (" + maxDictValue + ") as dict_value");

                    Dataset<Row> result = source.unionAll(joinedTable)
                            .selectExpr("dict_key", "dict_value", "'" + distinctColumnName + "' as dict_column");

                    String partitionSpec = "dict_column=" + distinctColumnName;
                    String tmpGlobalDictTableName = "tmp_" + globalDictTableName;

                    // use tmp table to avoid "overwrite table that is also being read from"
                    dataSource.writeTable(result, SaveMode.Overwrite, partitionSpec, starrocksHiveDB, tmpGlobalDictTableName);
                    dataSource.writeTable(dataSource.getOrLoadTable(starrocksHiveDB, tmpGlobalDictTableName),
                            SaveMode.Overwrite, partitionSpec, starrocksHiveDB, globalDictTableName);
                }
            });
        }

        submitWorker(globalDictBuildWorkers);
    }

    // encode starrocksIntermediateHiveTable's distinct column
    public void encodeStarRocksIntermediateHiveTable() {
        for (Object distinctColumnObj : dictColumn.keySet()) {
            String dictColumnName = distinctColumnObj.toString();
            List<String> childColumn = (ArrayList) dictColumn.get(dictColumnName);

            Dataset<Row> t1 = dataSource.getOrLoadTable(starrocksHiveDB, starrocksIntermediateHiveTable).alias("t1");
            StructField[] schema = t1.schema().fields();

            Dataset<Row> t2 = dataSource.getOrLoadTable(starrocksHiveDB, globalDictTableName)
                    .select("dict_key", "dict_value")
                    .where("dict_column='" + dictColumnName + "'")
                    .alias("t2");

            // using map join to solve distinct column data skew
            // here is a spark sql hint
            if (mapSideJoinColumns.size() != 0 && mapSideJoinColumns.contains(dictColumnName)) {
                t2 = t2.hint("broadcast");
            }

            List<String> selectExprs = new ArrayList<>(intermediateTableColumnList.size());
            for (int idx = 0; idx < intermediateTableColumnList.size(); ++idx) {
                String columnName = intermediateTableColumnList.get(idx);
                String columnType = schema[idx].dataType().typeName();

                String expr;
                if (dictColumnName.equals(columnName)) {
                    expr = String.format("cast(t2.dict_value as %s) as %s", columnType, columnName);
                } else if (childColumn != null && childColumn.contains(columnName)) {
                    expr = String.format("cast(if(%s is null, null, t2.dict_value) as %s) as %s",
                        columnName, columnType, columnName);
                } else {
                    expr = String.format("cast(t1.%s as %s) as %s", columnName, columnType, columnName);
                }
                selectExprs.add(expr);
            }

            Dataset<Row> result = t1.join(t2, t1.col(dictColumnName).equalTo(t2.col("dict_key")), "leftouter")
                    .selectExpr(JavaConverters.asScalaBufferConverter(selectExprs).asScala());

            String tmpIntermediateHiveTable = "tmp_" + starrocksIntermediateHiveTable;
            dataSource.writeTable(result, SaveMode.Overwrite, null, starrocksHiveDB, tmpIntermediateHiveTable);

            // use tmp table to avoid "overwrite table that is also being read from"
            dataSource.writeTable(dataSource.getOrLoadTable(starrocksHiveDB, tmpIntermediateHiveTable),
                    SaveMode.Overwrite, null, starrocksHiveDB, starrocksIntermediateHiveTable);
        }
    }

    private void createIfNotExistGlobalDictTable() {
        // use empty data frame to create table.
        StructType type = new StructType()
                .add("dict_key", StringType)
                .add("dict_value", LongType)
                .add("dict_column", StringType);
        Dataset<Row> empty = spark.createDataFrame(new ArrayList<>(), type);

        StringBuilder partitionSpec = new StringBuilder();
        for (Object distinctColumnNameOrigin : dictColumn.keySet()) {
            final String distinctColumnName = distinctColumnNameOrigin.toString();
            partitionSpec.append(",").append("dict_column=").append(distinctColumnName);
        }
        dataSource.writeTable(empty, SaveMode.Ignore, partitionSpec.deleteCharAt(0).toString(),
                starrocksHiveDB, globalDictTableName);
    }

    private Dataset<Row> getNewDistinctValue(String distinctColumnName) {
        Dataset<Row> t1 = dataSource.getOrLoadTable(starrocksHiveDB, distinctKeyTableName)
                .select("dict_key")
                .where("dict_column='" + distinctColumnName + "'")
                .where("dict_key is not null");
        Dataset<Row> t2 = dataSource.getOrLoadTable(starrocksHiveDB, globalDictTableName)
                .select("dict_key", "dict_value")
                .where("dict_column='" + distinctColumnName + "'");
        return t1.join(t2, t1.col("dict_key").equalTo(t2.col("dict_key")), "leftouter")
                .select(t1.col("dict_key"))
                .where(t2.col("dict_value").isNull());
    }

    private void buildGlobalDictBySplit(long maxGlobalDictValue, String distinctColumnName) {
        // 1. get distinct value
        Dataset<Row> newDistinctValue = getNewDistinctValue(distinctColumnName);

        // 2. split the newDistinctValue to avoid window functions' single node bottleneck
        Dataset<Row>[] splitedDistinctValue = newDistinctValue.randomSplit(getRandomSplitWeights());
        long currentMaxDictValue = maxGlobalDictValue;
        Map<String, Long> distinctKeyMap = new HashMap<>();

        for (int i = 0; i < splitedDistinctValue.length; i++) {
            long currentDatasetStartDictValue = currentMaxDictValue;
            long splitDistinctValueCount = splitedDistinctValue[i].count();
            currentMaxDictValue += splitDistinctValueCount;
            String tmpDictTableName =
                    String.format("%s_%s_tmp_dict_%s", i, currentDatasetStartDictValue, distinctColumnName);
            distinctKeyMap.put(tmpDictTableName, currentDatasetStartDictValue);
            Dataset<Row> distinctValueFrame =
                    spark.createDataFrame(splitedDistinctValue[i].toJavaRDD(), getDistinctValueSchema());
            distinctValueFrame.createOrReplaceTempView(tmpDictTableName);
        }

        Dataset<Row> source = dataSource.getOrLoadTable(starrocksHiveDB, globalDictTableName)
                .select("dict_key", "dict_value", "dict_column")
                .where("dict_column='" + distinctColumnName + "'");

        for (Map.Entry<String, Long> entry : distinctKeyMap.entrySet()) {
            Dataset<Row> tmp = dataSource.getOrLoadTable(starrocksHiveDB, entry.getKey()).selectExpr("dict_key",
                    "(row_number() over(order by dict_key)) + (" + entry.getValue() + ") as dict_value",
                    "'" + distinctColumnName + "' as dict_column");
            source = source.unionAll(tmp);
        }

        String partitionSpec = "dict_column=" + distinctColumnName;
        dataSource.writeTable(source, SaveMode.Overwrite, partitionSpec, starrocksHiveDB, globalDictTableName);
    }

    private StructType getDistinctValueSchema() {
        if (distinctValueSchema == null) {
            List<StructField> fieldList = new ArrayList<>();
            fieldList.add(DataTypes.createStructField("dict_key", StringType, false));
            distinctValueSchema = DataTypes.createStructType(fieldList);
        }
        return distinctValueSchema;
    }

    private double[] getRandomSplitWeights() {
        double[] weights = new double[veryHighCardinalityColumnSplitNum];
        double weight = 1 / Double.parseDouble(String.valueOf(veryHighCardinalityColumnSplitNum));
        Arrays.fill(weights, weight);
        return weights;
    }

    private void submitWorker(List<GlobalDictBuildWorker> workerList) {
        try {
            List<Future<Boolean>> futureList = new ArrayList<>();
            for (GlobalDictBuildWorker globalDictBuildWorker : workerList) {
                futureList.add(pool.submit(() -> {
                    try {
                        globalDictBuildWorker.work();
                        return true;
                    } catch (Exception e) {
                        LOG.error("BuildGlobalDict failed", e);
                        return false;
                    }
                }));
            }

            LOG.info("begin to fetch worker result");
            for (Future<Boolean> future : futureList) {
                if (!future.get()) {
                    throw new RuntimeException("detect one worker failed");
                }
            }
            LOG.info("fetch worker result complete");
        } catch (Exception e) {
            LOG.error("submit worker failed", e);
            throw new RuntimeException("submit worker failed", e);
        }
    }

    private interface GlobalDictBuildWorker {
        void work();
    }
}
