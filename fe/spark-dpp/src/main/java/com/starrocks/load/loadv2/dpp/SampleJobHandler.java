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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SampleJobHandler {

    private static final Logger LOG = LogManager.getLogger(SampleJobHandler.class);

    private SparkSession spark;

    private JavaPairRDD<List<Object>, Object[]> javaPairRDD;

    private Map<String, Integer> bucketDivideMap;

    private Map<String, Map<Integer, Integer>> idxSampleMap;

    public SampleJobHandler(SparkSession spark,
                            JavaPairRDD<List<Object>, Object[]> javaPairRDD,
                            Map<String, Integer> bucketDivideMap,
                            Map<String, Map<Integer, Integer>> idxSampleMap) {
        this.spark = spark;
        this.javaPairRDD = javaPairRDD;
        this.bucketDivideMap = bucketDivideMap;
        this.idxSampleMap = idxSampleMap;
    }

    public Map<String, List<List<Object>>> getBoundsMap() {
        Map<String, Tuple2<Long, List<Reservoir>>> sketched = sketch();
        return getBoundsMap(sketched);
    }

    private Map<String, Tuple2<Long, List<Reservoir>>> sketch() {
        Map<String, Tuple2<Long, List<Reservoir>>> sampleMap = new HashMap<>();
        LOG.info("start collecting sample");
        SampleJob mySampleJob = new SampleJob(javaPairRDD.id(), idxSampleMap);
        Object obj = spark.sparkContext().runJob(javaPairRDD.rdd(), mySampleJob, ClassTag$.MODULE$.apply(Map.class));
        if (obj != null) {
            Map<String, Reservoir> reservoirIdxMap = new HashMap<>();
            Arrays.stream((Map<String, Map<Integer, Reservoir>>[]) obj).forEach(item -> {
                item.forEach((k, v) -> {
                    // Map<String, Map<Integer, Reservoir>>
                    // reduce
                    v.forEach((k1, v1) -> {
                        // Map<Integer, List<Reservoir>>
                        String bucketKeyIdx = String.format("%s_%s", k, k1);
                        if (reservoirIdxMap.containsKey(bucketKeyIdx)) {
                            Reservoir reservoir = reservoirIdxMap.get(bucketKeyIdx);
                            long sumL = v1.getL() + reservoir.getL();
                            reservoir.getKeysList().addAll(v1.getKeysList());
                            reservoir.setL(sumL);
                            reservoirIdxMap.put(bucketKeyIdx, reservoir);
                        } else {
                            v1.setIdx(k1);
                            reservoirIdxMap.put(bucketKeyIdx, v1);
                        }
                    });
                });
            });

            reservoirIdxMap.forEach((k, v) -> {
                String bucketKey = k.substring(0, k.lastIndexOf("_"));
                if (sampleMap.containsKey(bucketKey)) {
                    long sumL = sampleMap.get(bucketKey)._1 + v.getL();
                    List<Reservoir> reservoirList = sampleMap.get(bucketKey)._2;
                    reservoirList.add(v);
                    sampleMap.put(bucketKey, new Tuple2<>(sumL, reservoirList));
                } else {
                    List<Reservoir> reservoirList = new ArrayList<>();
                    reservoirList.add(v);
                    sampleMap.put(bucketKey, new Tuple2<>(v.getL(), reservoirList));
                }
            });
        } else {
            LOG.warn("sample size is null");
        }
        LOG.info("end to collect sample");
        return sampleMap;
    }

    private List<List<Object>> determineBounds(Tuple2<Long, List<Reservoir>> tuple2, double lstPartitions) {
        List<Tuple2<Double, List<Object>>> candidates = new ArrayList<>();
        tuple2._2.forEach(reservoir -> {
            double weight = tuple2._1 / reservoir.getKeysList().size();
            reservoir.getKeysList().forEach(starrocksKeys -> {
                candidates.add(new Tuple2<>(weight, starrocksKeys.getKeys()));
            });
        });

        BucketComparator bucketComparator = new BucketComparator();
        candidates.sort((Tuple2<Double, List<Object>> o1,
                         Tuple2<Double, List<Object>> o2) -> bucketComparator.compare(o1._2, o2._2));
        List<List<Object>> bounds = new ArrayList<>();
        double partitions = Math.min(lstPartitions, candidates.size());
        // weight
        double sumWeights = candidates.stream().collect(Collectors.summingDouble(item -> item._1()));
        double step = sumWeights / partitions;
        double cumWeight = 0.00;
        double target = step;
        int i = 0;
        int j = 0;
        while ((i < candidates.size()) && (j < partitions - 1)) {
            cumWeight += candidates.get(i)._1;
            if (cumWeight >= target) {
                if (!bounds.contains(candidates.get(i)._2)) {
                    bounds.add(candidates.get(i)._2);
                    target += step;
                    j++;
                }
            }
            i++;
        }
        LOG.info("end to collect bounds");
        return bounds;
    }

    private Map<String, List<List<Object>>> getBoundsMap(Map<String, Tuple2<Long, List<Reservoir>>> sketchMap) {
        Map<String, List<List<Object>>> boundsMap = new HashMap<>();
        for (Map.Entry<String, Tuple2<Long, List<Reservoir>>> entry : sketchMap.entrySet()) {
            List<List<Object>> bounds = determineBounds(entry.getValue(), bucketDivideMap.get(entry.getKey()));
            boundsMap.put(entry.getKey(), bounds);
        }
        return boundsMap;
    }
}
