// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load.loadv2.datasource;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;

import java.util.List;
import java.util.Map;

public class HiveDataSource extends DataSource {

    public HiveDataSource(SparkSession spark, String format, Map<String, String> dataSourceOptions) {
        super(spark, format, dataSourceOptions);
    }

    @Override
    protected Map<String, String> getDataSourceOptions(String fullTableName) {
        return dataSourceOptions;
    }

    @Override
    protected Dataset<Row> readTable(String fullTableName, String format, Map<String, String> options) {
        return spark.read().format(format).options(options).table(fullTableName);
    }

    @Override
    protected void writeData(
            Dataset<Row> data,
            String fullTableName,
            String format,
            SaveMode saveMode,
            List<String> partitionBy,
            Map<String, String> options) {
        DataFrameWriter<Row> writer = data.write();
        if (partitionBy != null && !partitionBy.isEmpty()) {
            writer = writer.partitionBy(JavaConverters.asScalaBufferConverter(partitionBy).asScala());
        }
        writer.format(format).mode(saveMode).options(options).saveAsTable(fullTableName);
    }
}
