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

import com.starrocks.load.loadv2.etl.EtlJobConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.TaskContext;
import org.apache.spark.util.SerializableConfiguration;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * merge parquet
 */
public class ParquetMergeJob extends AbstractFunction1<Iterator<Map.Entry<String, Integer>>, Void> implements Serializable {

    private static final Logger LOG = LogManager.getLogger(ParquetMergeJob.class);

    private String pathPattern;
    private long tableId;
    private EtlJobConfig.EtlIndex indexMeta;

    private SerializableConfiguration serializableHadoopConf;

    private String outputPath;

    public ParquetMergeJob(String pathPattern, long tableId, EtlJobConfig.EtlIndex indexMeta,
                           SerializableConfiguration serializableHadoopConf, String outputPath) {
        this.pathPattern = pathPattern;
        this.tableId = tableId;
        this.indexMeta = indexMeta;
        this.serializableHadoopConf = serializableHadoopConf;
        this.outputPath = outputPath;
    }

    @Override
    public Void apply(Iterator<Map.Entry<String, Integer>> v1) {
        TaskContext taskContext = TaskContext.get();
        long taskAttemptId = taskContext.taskAttemptId();
        Configuration conf = new Configuration(serializableHadoopConf.value());
        conf.setBoolean("spark.sql.parquet.writeLegacyFormat", false);
        conf.setBoolean("spark.sql.parquet.int64AsTimestampMillis", false);
        conf.setBoolean("spark.sql.parquet.int96AsTimestamp", true);
        conf.setBoolean("spark.sql.parquet.binaryAsString", false);
        conf.set("spark.sql.parquet.outputTimestampType", "INT96");

        try {
            FileSystem fs = FileSystem.get(URI.create(outputPath), conf);
            while (v1.hasNext()) {
                Map.Entry<String, Integer> divideEntry = v1.next();
                String bucketKey = divideEntry.getKey();
                String partitionId = bucketKey.split("_")[0];
                String bucketId = bucketKey.split("_")[1];
                int divideN = divideEntry.getValue();
                String dstPath = String.format(pathPattern, tableId, Integer.parseInt(partitionId), indexMeta.indexId,
                        Integer.parseInt(bucketId), indexMeta.schemaHash);
                String tmpPath = dstPath + "." + taskAttemptId;
                List<Path> fileList = getFilePathList(fs, divideN, dstPath);

                LOG.info("fileList.get(0) = " + fileList.get(0));
                LOG.info("tmpPath = " + tmpPath);

                HadoopInputFile inputFile = HadoopInputFile.fromPath(fileList.get(0), conf);
                ParquetFileReader parquetFileReader = ParquetFileReader.open(inputFile);
                ParquetMetadata metadata = parquetFileReader.getFooter();
                MessageType schema = metadata.getFileMetaData().getSchema();
                Group group = null;
                ParquetReader<Group> parquetReader = null;
                GroupWriteSupport.setSchema(schema, conf);
                GroupWriteSupport writeSupport = new GroupWriteSupport();
                ParquetWriter parquetWriter = new ParquetWriter(new Path(dstPath), writeSupport,
                        CompressionCodecName.SNAPPY,
                        256 * 1024 * 1024, 16 * 1024,
                        1024 * 1024,
                        true, false,
                        ParquetProperties.WriterVersion.PARQUET_1_0,
                        conf);
                for (Path path : fileList) {
                    GroupReadSupport readSupport = new GroupReadSupport();
                    ParquetReader.Builder<Group> builder = ParquetReader.builder(readSupport, path);
                    builder.withConf(conf);
                    parquetReader = builder.build();
                    while ((group = parquetReader.read()) != null) {
                        parquetWriter.write(group);
                    }
                }
                if (parquetWriter != null) {
                    parquetWriter.close();
                }
                if (parquetFileReader != null) {
                    parquetFileReader.close();
                }
                try {
                    fs.rename(new Path(tmpPath), new Path(dstPath));
                } catch (Exception ex) {
                    LOG.warn("rename from tmpPath" + tmpPath + " to dstPath:" + dstPath +
                            " failed. exception:" + ex);
                    throw ex;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    private List<Path> getFilePathList(FileSystem fs, int divideN, String dstPath) throws IOException {
        String pathFormat = "%s.%d";
        List<Path> pathList = new ArrayList<>();
        for (int i = 0; i < divideN; i++) {
            Path path = new Path(String.format(pathFormat, dstPath, i));
            if (fs.exists(path)) {
                pathList.add(path);
            }
        }
        return pathList;
    }
}
