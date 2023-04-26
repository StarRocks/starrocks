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

package com.starrocks.sql.optimizer.statistics;

import java.util.List;
import java.util.Map;

public class Histogram {
    private final List<Bucket> buckets;
    private final Map<String, Long> mcv;

    public Histogram(List<Bucket> buckets, Map<String, Long> mcv) {
        this.buckets = buckets;
        this.mcv = mcv;

    }

    public long getTotalRows() {
        long totalRows = 0;
        if (buckets != null && !buckets.isEmpty()) {
            totalRows += buckets.get(buckets.size() - 1).getCount();
        }
        if (mcv != null) {
            totalRows += mcv.values().stream().reduce(Long::sum).orElse(0L);
        }
        return totalRows;
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public Map<String, Long> getMCV() {
        return mcv;
    }
}
