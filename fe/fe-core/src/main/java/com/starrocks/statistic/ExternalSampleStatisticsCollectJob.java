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

package com.starrocks.statistic;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ExternalSampleStatisticsCollectJob extends ExternalFullStatisticsCollectJob {
    private final int allPartitionSize;

    public ExternalSampleStatisticsCollectJob(String catalogName, Database db, Table table, List<String> partitionNames,
                                              List<String> columnNames, List<Type> columnTypes,
                                              StatsConstants.AnalyzeType type, StatsConstants.ScheduleType scheduleType,
                                              Map<String, String> properties, int allPartitionSize) {
        super(catalogName, db, table, partitionNames, columnNames, columnTypes, type, scheduleType, properties);
        this.allPartitionSize = allPartitionSize;
    }

    public Set<Long> getSampledPartitionsHashValue() {
        HashFunction hashFunction = Hashing.murmur3_128();
        return partitionNames.stream().map(s -> hashFunction.hashUnencodedChars(s).asLong()).collect(Collectors.toSet());
    }

    public int getAllPartitionSize() {
        return allPartitionSize;
    }

    @Override
    public String getName() {
        return "ExternalSample";
    }
}
