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

package com.starrocks.connector.iceberg;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.starrocks.connector.PartitionUtil;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;
import java.util.stream.Collectors;

public class IcebergPartitionUtils {
    private static final Logger LOG = LogManager.getLogger(IcebergPartitionUtils.class);
    public static class IcebergPartition {
        private PartitionSpec spec;
        private StructLike data;
        private ChangelogOperation operation;

        IcebergPartition(PartitionSpec spec, StructLike data, ChangelogOperation operation) {
            this.spec = spec;
            this.data = data;
            this.operation = operation;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof IcebergPartition)) {
                return false;
            }
            IcebergPartition that = (IcebergPartition) o;
            return Objects.equal(spec, that.spec) &&
                    Objects.equal(data, that.data) && operation == that.operation;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(spec, data, operation);
        }
    }

    public static Set<String> getChangedPartitionNames(Table table, long fromTimestampMillis,
                                                       Snapshot toSnapshot) {
        Set<IcebergPartition> changedPartition = getChangedPartition(table, fromTimestampMillis,
                toSnapshot);
        return changedPartition.stream().map(partition -> PartitionUtil.
                convertIcebergPartitionToPartitionName(table.spec(), partition.data)).collect(Collectors.toSet());
    }

    public static Set<IcebergPartition> getChangedPartition(Table table, long fromExclusiveTimestampMillis,
                                                            Snapshot toSnapshot) {
        ImmutableSet.Builder<IcebergPartition> builder = ImmutableSet.builder();
        Snapshot snapShot = toSnapshot;
        if (toSnapshot.timestampMillis() >= fromExclusiveTimestampMillis) {
            // find the first snapshot which it's timestampMillis is less than or equals fromExclusiveTimestampMillis
            while (snapShot.parentId() != null) {
                snapShot = table.snapshot(snapShot.parentId());
                // snapshot is null when it's expired
                if (snapShot == null || snapShot.timestampMillis() <= fromExclusiveTimestampMillis) {
                    break;
                }
            }
            // get incremental changelog scan when find the first snapshot
            // which it's timestampMillis is less than fromExclusiveTimestampMillis
            if (snapShot != null && snapShot.timestampMillis() <= fromExclusiveTimestampMillis) {
                IncrementalChangelogScan incrementalChangelogScan = table.newIncrementalChangelogScan().
                        fromSnapshotExclusive(snapShot.snapshotId()).toSnapshot(toSnapshot.snapshotId());
                try (CloseableIterable<ChangelogScanTask> tasks = incrementalChangelogScan.planFiles()) {
                    for (ChangelogScanTask task : tasks) {
                        ChangelogOperation operation = task.operation();
                        if (operation == ChangelogOperation.INSERT) {
                            AddedRowsScanTask addedRowsScanTask = (AddedRowsScanTask) task;
                            StructLike data = addedRowsScanTask.file().partition();
                            builder.add(new IcebergPartition(addedRowsScanTask.spec(), data, operation));
                        } else if (operation == ChangelogOperation.DELETE) {
                            DeletedDataFileScanTask deletedDataFileScanTask = (DeletedDataFileScanTask) task;
                            StructLike data = deletedDataFileScanTask.file().partition();
                            builder.add(new IcebergPartition(deletedDataFileScanTask.spec(), data, operation));
                        } else {
                            LOG.warn("Do not support this iceberg change log type, operation is {}", operation);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("get incrementalChangelogScan failed", e);
                    return getAllPartition(table);
                }
                return builder.build();
            }
        }

        return getAllPartition(table);
    }

    public static Set<IcebergPartition> getAllPartition(Table table) {
        ImmutableSet.Builder<IcebergPartition> builder = ImmutableSet.builder();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                PartitionSpec spec = task.spec();
                StructLike data = task.partition();
                builder.add(new IcebergPartition(spec, data, ChangelogOperation.INSERT));
            }
        } catch (Exception e) {
            LOG.warn("get all iceberg partition failed", e);
        }
        return builder.build();
    }
}