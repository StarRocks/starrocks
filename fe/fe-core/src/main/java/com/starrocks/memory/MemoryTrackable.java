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

package com.starrocks.memory;

import com.starrocks.common.Pair;
import org.apache.spark.util.SizeEstimator;

import java.util.List;
import java.util.Map;

public interface MemoryTrackable {
    default long estimateSize() {
        List<Pair<List<Object>, Long>> samples = getSamples();
        long totalBytes = 0;
        for (Pair<List<Object>, Long> pair : samples) {
            List<Object> sampleObjects = pair.first;
            long size = pair.second;
            if (!sampleObjects.isEmpty()) {
                totalBytes += (long) (((double) SizeEstimator.estimate(sampleObjects)) / sampleObjects.size() * size);
            }
        }

        return totalBytes;
    }

    Map<String, Long> estimateCount();

    // Samples for estimateSize() to calculate memory size;
    // Pair.fist is the sample objects, Pair.second is the total size of that module.
    // For example:
    // Manager has two list attributes: List<A>, List<B>, we get 10 objects for samples,
    // this function should return:
    // Pair<10 A objects, List<A>.size()>, Pair<10 B object, List<B>.size()>
    List<Pair<List<Object>, Long>> getSamples();
}
