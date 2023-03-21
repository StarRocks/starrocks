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

import scala.Tuple2;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetBucketPartitionSizeJob extends AbstractFunction1<Iterator<Tuple2<List<Object>, Object[]>>, Map<String, Long>>
        implements Serializable {

    @Override
    public Map<String, Long> apply(Iterator<Tuple2<List<Object>, Object[]>> iter) {
        Map<String, Long> bucketPartitionSizeMap = new HashMap<>();
        while (iter.hasNext()) {
            Tuple2<List<Object>, Object[]> next = iter.next();
            String bucketKey = (String) next._1.get(0);

            if (bucketPartitionSizeMap.containsKey(bucketKey)) {
                bucketPartitionSizeMap.put(bucketKey, bucketPartitionSizeMap.get(bucketKey) + 1);
            } else {
                bucketPartitionSizeMap.put(bucketKey, 1L);
            }
        }

        return bucketPartitionSizeMap;
    }
}
