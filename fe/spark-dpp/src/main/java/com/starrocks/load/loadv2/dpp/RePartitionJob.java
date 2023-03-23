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
import com.google.gson.Gson;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

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
                                List<List<Object>> bounds = boundsMap.get(oldBucketKey);
                                BucketComparator bucketComparator = new BucketComparator();
                                int[] res = {0};
                                Optional<Integer> bound = Stream.iterate(0, i -> i + 1).limit(bounds.size())
                                        .filter(i -> {
                                            res[0] = bucketComparator.compare(oldkeyColumns, bounds.get(i));
                                            return res[0] <= 0 || (res[0] > 0 && i == bounds.size() - 1);
                                        })
                                        .findAny();
                                if (bound.isPresent()) {
                                    subPartition = res[0] > 0 ? (bound.get() + 1) : bound.get();
                                }
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

    public JavaPairRDD<List<Object>, Object[]> repartitionByHash(Map<String, Integer> bucketDivideMap,
                                                                 JavaPairRDD<List<Object>, Object[]> javaPairRDD) {
        javaPairRDD = javaPairRDD.mapPartitionsToPair(
                new PairFlatMapFunction<Iterator<Tuple2<List<Object>, Object[]>>, List<Object>, Object[]>() {
                    @Override
                    public Iterator<Tuple2<List<Object>, Object[]>> call(
                            Iterator<Tuple2<List<Object>, Object[]>> iter) throws Exception {
                        List<Tuple2<List<Object>, Object[]>> result = new ArrayList<>();
                        Gson gson = new Gson();
                        while (iter.hasNext()) {
                            Tuple2<List<Object>, Object[]> next = iter.next();
                            String oldBucketKey = String.valueOf(next._1.get(0));
                            List<Object> oldkeyColumns = next._1.subList(1, next._1.size());
                            Object[] oldvalueColumns = next._2;
                            int subPartition = 0;
                            if (bucketDivideMap.containsKey(oldBucketKey)) {
                                subPartition = divideN(gson.toJson(next._1).getBytes(StandardCharsets.UTF_8),
                                        bucketDivideMap.get(oldBucketKey));
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

    public int divideN(byte[] bytes, int divideN) {
        int hashCode = Hashing.consistentHash(hashFunction.hashBytes(bytes), divideN);
        return hashCode % divideN;
    }
}
