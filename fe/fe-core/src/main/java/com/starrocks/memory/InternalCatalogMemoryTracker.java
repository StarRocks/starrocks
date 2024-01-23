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

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;
import org.apache.spark.util.SizeEstimator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class InternalCatalogMemoryTracker implements MemoryTrackable {

    @Override
    public long estimateSize() {
        long estimateSize = 0L;
        List<Database> databases = new ArrayList<>(GlobalStateMgr.getCurrentState().getIdToDb().values());
        for (Database database : databases) {
            List<Table> tables = database.getTables();
            for (Table table : tables) {
                Collection<Partition> partitions = table.getPartitions();
                Iterator<Partition> iterator = partitions.iterator();
                if (iterator.hasNext()) {
                    estimateSize += SizeEstimator.estimate(iterator.next()) * partitions.size();
                }
            }
        }
        return estimateSize;
    }

    @Override
    public Map<String, Long> estimateCount() {
        long estimateCount = 0;
        List<Database> databases = new ArrayList<>(GlobalStateMgr.getCurrentState().getIdToDb().values());
        for (Database database : databases) {
            List<Table> tables = database.getTables();
            for (Table table : tables) {
                Collection<Partition> partitions = table.getPartitions();
                estimateCount += partitions.size();
            }
        }
        return ImmutableMap.of("Partition", estimateCount);
    }

}
