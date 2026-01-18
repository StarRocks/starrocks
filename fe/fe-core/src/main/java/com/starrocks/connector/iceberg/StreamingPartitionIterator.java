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

import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.PartitionUtil;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.util.StructProjection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * A streaming iterator that yields partitions one-by-one without loading
 * all partitions into memory at once. This is essential for tables with
 * millions of partitions to avoid OOM.
 *
 * <p>This iterator lazily fetches partition data from Iceberg's PartitionsTable,
 * parsing rows on-demand and closing resources properly when done or on error.</p>
 */
public class StreamingPartitionIterator implements CloseableIterator<Map.Entry<String, Partition>> {
    private static final Logger LOG = LogManager.getLogger(StreamingPartitionIterator.class);

    private final Table nativeTable;
    private final IcebergTable icebergTable;
    private final CloseableIterable<FileScanTask> taskIterable;
    private final long snapshotId;

    private Iterator<FileScanTask> taskIterator;
    private CloseableIterable<StructLike> currentRows;
    private Iterator<StructLike> rowIterator;
    private Map.Entry<String, Partition> nextEntry;
    private boolean closed = false;

    public StreamingPartitionIterator(Table nativeTable, IcebergTable icebergTable,
                                      CloseableIterable<FileScanTask> taskIterable,
                                      long snapshotId) {
        this.nativeTable = nativeTable;
        this.icebergTable = icebergTable;
        this.taskIterable = taskIterable;
        this.snapshotId = snapshotId;
        this.nextEntry = null;

        // Initialize taskIterator with proper resource cleanup on failure
        try {
            this.taskIterator = taskIterable.iterator();
        } catch (Exception e) {
            // Close taskIterable if iterator() throws to prevent resource leak
            try {
                taskIterable.close();
            } catch (IOException closeEx) {
                e.addSuppressed(closeEx);
            }
            throw e;
        }
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            return false;
        }
        if (nextEntry != null) {
            return true;
        }
        nextEntry = advance();
        return nextEntry != null;
    }

    @Override
    public Map.Entry<String, Partition> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Map.Entry<String, Partition> result = nextEntry;
        nextEntry = null;
        return result;
    }

    private Map.Entry<String, Partition> advance() {
        while (true) {
            // Try to get next row from current batch
            if (rowIterator != null && rowIterator.hasNext()) {
                StructLike row = rowIterator.next();
                return parsePartitionRow(row);
            }

            // Close current rows if exists
            closeCurrentRows();

            // Move to next task
            if (!taskIterator.hasNext()) {
                return null;
            }

            FileScanTask task = taskIterator.next();
            try {
                currentRows = task.asDataTask().rows();
                rowIterator = currentRows.iterator();
            } catch (Exception e) {
                LOG.error("Failed to read partition rows for table: " + nativeTable.name(), e);
                throw new RuntimeException("Failed to read partition rows", e);
            }
        }
    }

    private Map.Entry<String, Partition> parsePartitionRow(StructLike row) {
        // partitionsTable Table schema for partitioned tables:
        // partition (index 0),
        // spec_id (index 1),
        // record_count,
        // file_count,
        // total_data_file_size_in_bytes,
        // position_delete_record_count,
        // position_delete_file_count,
        // equality_delete_record_count,
        // equality_delete_file_count,
        // last_updated_at (index 9),
        // last_updated_snapshot_id
        StructProjection partitionData = row.get(0, StructProjection.class);
        int specId = row.get(1, Integer.class);
        PartitionSpec spec = nativeTable.specs().get(specId);

        String partitionName = PartitionUtil.convertIcebergPartitionToPartitionName(
                nativeTable, spec, partitionData);

        long lastUpdated = getLastUpdated(row, partitionName);
        Partition partition = new Partition(lastUpdated, specId);

        return new AbstractMap.SimpleEntry<>(partitionName, partition);
    }

    private long getLastUpdated(StructLike row, String partitionName) {
        // last_updated_at is at index 9 for partitioned tables
        long lastUpdated = -1;
        try {
            Long value = row.get(9, Long.class);
            if (value != null) {
                lastUpdated = value;
            }
        } catch (Exception e) {
            LOG.error("Failed to get last_updated_at for partition [{}] of table [{}] under snapshot [{}]",
                    partitionName, nativeTable.name(), snapshotId, e);
        }

        if (lastUpdated == -1) {
            // Fallback to current snapshot's timestamp if last_updated_at is null
            Snapshot snapshot = nativeTable.currentSnapshot();
            if (snapshot != null) {
                lastUpdated = snapshot.timestampMillis();
                LOG.warn("The table [{}] partition [{}] last_updated_at is null (snapshot [{}] may have been expired), " +
                        "using current snapshot timestamp: {}", nativeTable.name(), partitionName, snapshotId, lastUpdated);
            } else {
                LOG.warn("The table [{}] has no current snapshot, using -1 as last updated time", nativeTable.name());
            }
        }
        return lastUpdated;
    }

    private void closeCurrentRows() {
        if (currentRows != null) {
            try {
                currentRows.close();
            } catch (IOException e) {
                LOG.warn("Failed to close rows iterator for table: " + nativeTable.name(), e);
            }
            currentRows = null;
            rowIterator = null;
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        closeCurrentRows();
        try {
            taskIterable.close();
        } catch (IOException e) {
            LOG.warn("Failed to close task iterable for table: " + nativeTable.name(), e);
        }
    }
}
