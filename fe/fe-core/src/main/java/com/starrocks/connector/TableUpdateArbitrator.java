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

package com.starrocks.connector;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;

import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public abstract class TableUpdateArbitrator {
    private static ImmutableList<String> SUPPORTED_SCHEMA = ImmutableList.<String>builder()
            .add("OSS")
            .add("S3")
            .add("HDFS")
            .build();

    private static final Map<Pair<Table.TableType, String>, Supplier<TableUpdateArbitrator>> TRAITS_TABLE =
            ImmutableMap.<Pair<Table.TableType, String>, Supplier<TableUpdateArbitrator>>builder()
                    .put(Pair.create(Table.TableType.HIVE, "OSS"), ObjectBasedUpdateArbitrator::new)
                    .put(Pair.create(Table.TableType.HIVE, "S3"), ObjectBasedUpdateArbitrator::new)
                    .put(Pair.create(Table.TableType.HIVE, "HDFS"), DirectoryBasedUpdateArbitrator::new)
                    .build();

    protected Table table;
    protected int partitionLimit;
    protected List<String> partitionNames;

    public static class UpdateContext {
        private final Table table;
        private final int partitionLimit;
        private final List<String> partitionNames;

        public UpdateContext(Table table, int partitionLimit, List<String> partitionNames) {
            this.table = table;
            this.partitionLimit = partitionLimit;
            this.partitionNames = partitionNames;
        }
    }

    public static TableUpdateArbitrator create(UpdateContext context) {
        String location = context.table.getTableLocation();
        URI uri = URI.create(location);
        String scheme = Optional.ofNullable(uri.getScheme()).orElse("").toUpperCase(Locale.ROOT);
        if (!SUPPORTED_SCHEMA.contains(scheme)) {
            return null;
        }
        Pair key = Pair.create(context.table.getType(), scheme);
        TableUpdateArbitrator arbitrator = Preconditions.checkNotNull(TRAITS_TABLE.get(key),
                String.format("table type:%s, schema:%s not supported in update arbitrator's traits",
                        context.table.getType(), uri.getScheme())).get();
        arbitrator.table = context.table;
        arbitrator.partitionLimit = context.partitionLimit;
        arbitrator.partitionNames = context.partitionNames;
        return arbitrator;
    }

    public abstract Map<String, Optional<HivePartitionDataInfo>> getPartitionDataInfos();

}
