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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * reset the bucketKeyMap
 */
public class RePartitionJob implements Serializable {

    private static final Logger LOG = LogManager.getLogger(RePartitionJob.class);

    private SparkSession spark;

    private HashFunction hashFunction = Hashing.sha256();

    public RePartitionJob(SparkSession spark) {
        this.spark = spark;
    }

    public JavaPairRDD<List<Object>, Object[]> repartitionBySample(Map<String, List<List<Object>>> boundsMap,
                                                           JavaPairRDD<List<Object>, Object[]> javaPairRDD) {
        javaPairRDD = javaPairRDD.mapPartitionsToPair(
                new PairFlatMapFunction<Iterator<Tuple2<List<Object>, Object[]>>, List<Object>, Object[]>() {
                    @Override
                    public Iterator<Tuple2<List<Object>, Object[]>> call(
                            Iterator<Tuple2<List<Object>, Object[]>> iter) throws Exception {
                        List<Tuple2<List<Object>, Object[]>> result = new ArrayList<>();
                        while (iter.hasNext()) {
                            Tuple2<List<Object>, Object[]> next = iter.next();
                            String oldBucketKey = String.valueOf(next._1.get(0));
                            List<Object> oldkeyColumns = next._1.subList(1, next._1.size());
                            Object[] oldvalueColumns = next._2;
                            int subPartition = 0;
                            if (boundsMap.containsKey(oldBucketKey)) {
                                subPartition = binarySearch(boundsMap.get(oldBucketKey), oldkeyColumns);
                            }
                            List<Object> tuple = new ArrayList<>();
                            tuple.add(oldBucketKey + "_" + subPartition);
                            tuple.addAll(oldkeyColumns);
                            result.add(new Tuple2<>(tuple, oldvalueColumns));
                        }
                        return result.iterator();
                    }
                });
        return javaPairRDD;
    }

    private int binarySearch(List<List<Object>> bounds, List<Object> keyColumns) {
        int left = 0;
        int right = bounds.size() - 1;
        BucketComparator bucketComparator = new BucketComparator();
        while (left <= right) {
            int mid = left + (right - left) / 2;
            int res = bucketComparator.compare(keyColumns, bounds.get(mid));
            if (res == 0) {
                return mid;
            } else if (res > 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return left;
    }
}
