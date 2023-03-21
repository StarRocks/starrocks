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
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.SerializableConfiguration;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.Serializable;
import java.util.Map;

public class FileMergeJob implements Serializable {
    private SparkSession spark;

    public FileMergeJob(SparkSession spark) {
        this.spark = spark;
    }

    public void mergeParquetFile(Map<String, Integer> bucketDivideMap, String pathPattern,
                                 long tableId, EtlJobConfig.EtlIndex indexMeta,
                                 SerializableConfiguration serializableHadoopConf, String outputPath) {
        ParquetMergeJob parquetMergeJob = new ParquetMergeJob(pathPattern, tableId,
                indexMeta, serializableHadoopConf, outputPath);

        Seq<Map.Entry<String, Integer>> seq =
                JavaConverters.asScalaIteratorConverter(bucketDivideMap.entrySet().iterator()).asScala().toSeq();
        ClassTag<Map.Entry<String, Integer>> classTag = ClassTag$.MODULE$.apply(Map.Entry.class);
        RDD<Map.Entry<String, Integer>> rdd = spark.sparkContext().parallelize(seq, bucketDivideMap.size(), classTag);
        spark.sparkContext().runJob(rdd, parquetMergeJob, ClassTag$.MODULE$.apply(Void.class));
    }
}
