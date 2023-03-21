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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.reflect.ClassTag$;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GetBucketPartitionSizeJobHandler implements Serializable {

    private static final Logger LOG = LogManager.getLogger(RePartitionJob.class);

    private SparkSession spark;

    public GetBucketPartitionSizeJobHandler(SparkSession spark) {
        this.spark = spark;
    }

    private Map<String, Long> getBucketPartitionSize(JavaPairRDD<List<Object>, Object[]> javaPairRDD) {
        Map<String, Long> bucketPartitionSizeMap = new HashMap<>();
        LOG.info("start collecting bucket_partition size");
        GetBucketPartitionSizeJob getBucketPartitionSizeJob = new GetBucketPartitionSizeJob();
        Object obj = spark.sparkContext().runJob(javaPairRDD.rdd(), getBucketPartitionSizeJob,
                ClassTag$.MODULE$.apply(Map.class));
        if (obj != null) {
            Arrays.stream((Map<String, Long>[]) obj).forEach(item -> {
                item.forEach((k, v) -> {
                    if (bucketPartitionSizeMap.containsKey(k)) {
                        bucketPartitionSizeMap.put(k, bucketPartitionSizeMap.get(k) + v);
                    } else {
                        bucketPartitionSizeMap.put(k, v);
                    }
                });
            });
        } else {
            LOG.warn("bucket partition size is null");
        }
        LOG.info("end to collect bucket_partition size");
        return bucketPartitionSizeMap;
    }

    public void resetBucketKeyMap(JavaPairRDD<List<Object>, Object[]> javaPairRDD,
                                     Map<String, Integer> bucketKeyMap, Map<String, Integer> bucketDivideMap) {
        Map<String, Integer> newBucketKeyMap = new HashMap<>();
        Map<String, Long> bucketPartitionSizeMap = getBucketPartitionSize(javaPairRDD);
        long totalRows = bucketPartitionSizeMap.values().stream().collect(Collectors.summingLong(Long::longValue));
        int instances = Integer.parseInt(spark.sparkContext().getConf().get("spark.executor.instances"));
        int cores = Integer.parseInt(spark.sparkContext().getConf().get("spark.executor.cores"));
        int totalTasks = instances * cores * 3;
        LOG.info("totalTask = " + totalTasks + "(instances = " + instances + ", cores = " + cores + ")");
        long avgBucketPartitionRows = totalRows / totalTasks;
        int newReduceNum = 0;
        for (Map.Entry<String, Integer> bucketEntry : bucketKeyMap.entrySet()) {
            if (bucketPartitionSizeMap.containsKey(bucketEntry.getKey())) {
                long partitionRows = bucketPartitionSizeMap.get(bucketEntry.getKey());
                if (1.0 * partitionRows / avgBucketPartitionRows >= 3) {
                    int n = Math.round((partitionRows / avgBucketPartitionRows));
                    bucketDivideMap.put(bucketEntry.getKey(), n);
                    for (int j = 0; j < n; j++) {
                        newBucketKeyMap.put(bucketEntry.getKey() + "_" + j, newReduceNum);
                        newReduceNum++;
                    }
                } else {
                    newBucketKeyMap.put(bucketEntry.getKey() + "_0", newReduceNum);
                    newReduceNum++;
                }
            }
        }
        bucketKeyMap.clear();
        bucketKeyMap.putAll(newBucketKeyMap);
    }
}
