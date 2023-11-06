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

import org.apache.commons.collections.MapUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 1. use the GetBucketPartitionSizeJob to obtain the data size of bucketKey.
 * 2. calculate the average amount of data processed by each task, split the skewed bucketKey into n parts.
 * 3. update the bucketKeyMap
 */
public class GetBucketPartitionSizeJobHandler implements Serializable {

    private static final Logger LOG = LogManager.getLogger(RePartitionJob.class);

    private SparkSession spark;

    private Map<String, Map<Integer, Integer>> idxSampleMap;

    private Map<String, Integer> bucketDivideMap;

    public GetBucketPartitionSizeJobHandler(SparkSession spark) {
        this.spark = spark;
    }

    private Map<String, Tuple2<Long, Map<Integer, Long>>> getBucketPartitionSize(JavaPairRDD<List<Object>,
            Object[]> javaPairRDD) {
        Map<String, Tuple2<Long, Map<Integer, Long>>> bucketPartitionSizeMap = new HashMap<>();
        LOG.info("start collecting bucket_partition size");
        GetBucketPartitionSizeJob getBucketPartitionSizeJob = new GetBucketPartitionSizeJob();
        Object obj = spark.sparkContext().runJob(javaPairRDD.rdd(), getBucketPartitionSizeJob,
                ClassTag$.MODULE$.apply(Map.class));
        if (obj != null) {
            Arrays.stream((Map<String, Map<Integer, Long>>[]) obj).forEach(item -> {
                item.forEach((k, v) -> {
                    long totalNums = v.values().stream().mapToLong(Long::longValue).sum();
                    if (bucketPartitionSizeMap.containsKey(k)) {
                        Tuple2<Long, Map<Integer, Long>> tuple2 = bucketPartitionSizeMap.get(k);
                        Map<Integer, Long> idxMap = tuple2._2;
                        v.forEach((k1, v1) -> {
                            if (idxMap.containsKey(k1)) {
                                idxMap.put(k1, idxMap.get(k1) + v1);
                            } else {
                                idxMap.put(k1, v1);
                            }
                        });
                        bucketPartitionSizeMap.put(k, new Tuple2<>(totalNums + bucketPartitionSizeMap.get(k)._1, idxMap));
                    } else {
                        bucketPartitionSizeMap.put(k, new Tuple2<>(totalNums, v));
                    }
                });
            });
        } else {
            LOG.warn("bucket partition size is null");
        }
        LOG.info("end to collect bucket_partition size");
        return bucketPartitionSizeMap;
    }

    public void resetBucketKeyMap(JavaPairRDD<List<Object>, Object[]> javaPairRDD, Map<String, Integer> bucketKeyMap) {
        Map<String, Integer> newBucketKeyMap = new HashMap<>();
        Map<String, Map<Integer, Integer>> idxSampleMap = new HashMap<>();
        Map<String, Integer> bucketDivideMap = new HashMap<>();
        Map<String, Tuple2<Long, Map<Integer, Long>>> bucketPartitionSizeMap = getBucketPartitionSize(javaPairRDD);
        long totalRows = bucketPartitionSizeMap.values().stream().mapToLong(Tuple2::_1).sum();
        int instances = Integer.parseInt(spark.sparkContext().getConf().get("spark.executor.instances"));
        int cores = Integer.parseInt(spark.sparkContext().getConf().get("spark.executor.cores"));
        int totalTasks = instances * cores * 3;
        LOG.info("totalTask = " + totalTasks + "(instances = " + instances + ", cores = " + cores + ")");
        long avgBucketPartitionRows = totalRows / totalTasks;
        int newReduceNum = 0;
        // sample size.
        long sampleSize = 1000;
        for (Map.Entry<String, Integer> bucketEntry : bucketKeyMap.entrySet()) {
            if (bucketPartitionSizeMap.containsKey(bucketEntry.getKey())) {
                long partitionRows = bucketPartitionSizeMap.get(bucketEntry.getKey())._1;
                if (1.0 * partitionRows / avgBucketPartitionRows >= 2) {
                    int n = Math.round((partitionRows / avgBucketPartitionRows));
                    bucketDivideMap.put(bucketEntry.getKey(), n);
                    for (int j = 0; j < n; j++) {
                        newBucketKeyMap.put(bucketEntry.getKey() + "_" + j, newReduceNum);
                        newReduceNum++;
                    }

                    Map<Integer, Long> idxMapp = bucketPartitionSizeMap.get(bucketEntry.getKey())._2;
                    Map<Integer, Integer> sampleMap = new HashMap<>();
                    idxMapp.forEach((k, v) -> {
                        int idxSampleSize = Math.round(((float) sampleSize * v) / partitionRows);
                        if (idxSampleSize > 0) {
                            sampleMap.put(k, idxSampleSize);
                        }
                    });

                    if (MapUtils.isNotEmpty(sampleMap)) {
                        idxSampleMap.put(bucketEntry.getKey(), sampleMap);
                    }
                } else {
                    newBucketKeyMap.put(bucketEntry.getKey() + "_0", newReduceNum);
                    newReduceNum++;
                }
            }
        }
        bucketKeyMap.clear();
        bucketKeyMap.putAll(newBucketKeyMap);
        setIdxSampleMap(idxSampleMap);
        setBucketDivideMap(bucketDivideMap);
    }

    public Map<String, Map<Integer, Integer>> getIdxSampleMap() {
        return idxSampleMap;
    }

    public void setIdxSampleMap(Map<String, Map<Integer, Integer>> idxSampleMap) {
        this.idxSampleMap = idxSampleMap;
    }

    public Map<String, Integer> getBucketDivideMap() {
        return bucketDivideMap;
    }

    public void setBucketDivideMap(Map<String, Integer> bucketDivideMap) {
        this.bucketDivideMap = bucketDivideMap;
    }
}
