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

import org.apache.spark.TaskContext;
import org.apache.spark.util.random.XORShiftRandom;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;
import scala.util.hashing.package$;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Perform reservoir sampling on multiple bucketKeys simultaneously.
 */
public class SampleJob extends AbstractFunction1<Iterator<Tuple2<List<Object>, Object[]>>,
        Map<String, Map<Integer, Reservoir>>> implements Serializable {
    private int shift;

    private Map<String, Map<Integer, Integer>> idxSampleMap;

    public SampleJob(int shift, Map<String, Map<Integer, Integer>> idxSampleMap) {
        this.shift = shift;
        this.idxSampleMap = idxSampleMap;
    }

    @Override
    public Map<String, Map<Integer, Reservoir>> apply(Iterator<Tuple2<List<Object>, Object[]>> iter) {
        // return [partitionId_bucketId, [rdd.partitionId, reservoir]]
        // reservoir => [rdd.partitioniId, input size, samples]
        Map<String, Map<Integer, Reservoir>> sampleMap = new HashMap<>();
        Map<String, Map<Integer, Long>> hasSampleMap = new HashMap<>();
        int idx = TaskContext.getPartitionId();
        while (iter.hasNext()) {
            Tuple2<List<Object>, Object[]> next = iter.next();
            String bucketKey = (String) next._1.get(0);
            if (!idxSampleMap.containsKey(bucketKey) || !idxSampleMap.get(bucketKey).containsKey(idx)) {
                // no sampling
                continue;
            }
            List<Object> keys = next._1.subList(1, next._1.size());
            // reservoir size
            double sampleSizePerPatition = idxSampleMap.get(bucketKey).get(idx);
            if (sampleMap.containsKey(bucketKey)) {
                Map<Integer, Reservoir> reservoirMap = sampleMap.get(bucketKey);
                Map<Integer, Long> idxMap = new HashMap<>();
                long l = 0;
                if (reservoirMap.containsKey(idx)) {
                    Reservoir reservoir = reservoirMap.get(idx);
                    l = hasSampleMap.get(bucketKey).get(idx);
                    if (l < sampleSizePerPatition) {
                        l++;
                        List<StarrocksKeys> keysList = reservoir.getKeysList();
                        keysList.add(new StarrocksKeys(keys));
                        reservoirMap.put(idx, new Reservoir(l, keysList));
                    } else {
                        // replace sample
                        int seed = package$.MODULE$.byteswap32(((Integer) idx) ^ (shift << 16));
                        XORShiftRandom random = new XORShiftRandom();
                        random.setSeed(seed);
                        l++;
                        Long replacementIndex = (long) random.nextDouble() * l;
                        if (replacementIndex < sampleSizePerPatition) {
                            List<StarrocksKeys> keysList = reservoir.getKeysList();
                            keysList.set(replacementIndex.intValue(), new StarrocksKeys(keys));
                            reservoirMap.put(idx, new Reservoir(l, keysList));
                        }
                    }
                    hasSampleMap.get(bucketKey).put(idx, l);
                } else {
                    l = 1L;
                    List<StarrocksKeys> keysList = new ArrayList<>();
                    keysList.add(new StarrocksKeys(keys));
                    reservoirMap.put(idx, new Reservoir(l, keysList));
                    sampleMap.put(bucketKey, reservoirMap);

                    idxMap.put(idx, l);
                    hasSampleMap.put(bucketKey, idxMap);
                }
            } else {
                long l = 1L;
                Map<Integer, Reservoir> reservoirMap = new HashMap<>();
                List<StarrocksKeys> keysList = new ArrayList<>();
                keysList.add(new StarrocksKeys(keys));
                reservoirMap.put(idx, new Reservoir(l, keysList));
                sampleMap.put(bucketKey, reservoirMap);

                Map<Integer, Long> idxMap = new HashMap<>();
                idxMap.put(idx, l);
                hasSampleMap.put(bucketKey, idxMap);
            }
        }
        return sampleMap;
    }
}
