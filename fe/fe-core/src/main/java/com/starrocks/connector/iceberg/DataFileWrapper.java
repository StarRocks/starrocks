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

import com.starrocks.memory.estimate.IgnoreMemoryTrack;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.StructLike;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

// checking hashcode and equals for manifest cache
public class DataFileWrapper implements DataFile {
    private final DataFile dataFile;
    // When set, the wrapped file carries no statistics and they are read from the manifest-level store, which is
    // shared by every file of the manifest (the cache weigher counts it once per manifest).
    @IgnoreMemoryTrack
    private final ManifestColumnStats columnStats;
    private final int columnStatsIndex;

    public DataFileWrapper(DataFile dataFile) {
        this(dataFile, null, -1);
    }

    private DataFileWrapper(DataFile dataFile, ManifestColumnStats columnStats, int columnStatsIndex) {
        this.dataFile = dataFile;
        this.columnStats = columnStats;
        this.columnStatsIndex = columnStatsIndex;
    }

    public static DataFileWrapper wrap(DataFile dataFile) {
        return new DataFileWrapper(dataFile);
    }

    // `dataFile` must be a copy without statistics; its statistics are entry `index` of `columnStats`.
    public static DataFileWrapper wrap(DataFile dataFile, ManifestColumnStats columnStats, int index) {
        return new DataFileWrapper(dataFile, columnStats, index);
    }

    public ManifestColumnStats columnStats() {
        return columnStats;
    }

    // The file to hand out beyond planning: a scan task that kept this wrapper would keep the statistics of the
    // whole manifest alive after the cache drops the entry, so it gets the wrapped copy without statistics instead.
    public DataFile withoutSharedStats() {
        return columnStats == null ? this : dataFile;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DataFileWrapper that = (DataFileWrapper) o;

        return dataFile.path().toString().equals(that.dataFile.path().toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataFile.path());
    }

    @Override
    public Long pos() {
        return dataFile.pos();
    }

    @Override
    public int specId() {
        return dataFile.specId();
    }

    @Override
    public CharSequence path() {
        return dataFile.path();
    }

    @Override
    public FileFormat format() {
        return dataFile.format();
    }

    @Override
    public StructLike partition() {
        return dataFile.partition();
    }

    @Override
    public long recordCount() {
        return dataFile.recordCount();
    }

    @Override
    public long fileSizeInBytes() {
        return dataFile.fileSizeInBytes();
    }

    @Override
    public Map<Integer, Long> columnSizes() {
        return columnStats == null ? dataFile.columnSizes() : columnStats.columnSizes(columnStatsIndex);
    }

    @Override
    public Map<Integer, Long> valueCounts() {
        return columnStats == null ? dataFile.valueCounts() : columnStats.valueCounts(columnStatsIndex);
    }

    @Override
    public Map<Integer, Long> nullValueCounts() {
        return columnStats == null ? dataFile.nullValueCounts() : columnStats.nullValueCounts(columnStatsIndex);
    }

    @Override
    public Map<Integer, Long> nanValueCounts() {
        return columnStats == null ? dataFile.nanValueCounts() : columnStats.nanValueCounts(columnStatsIndex);
    }

    @Override
    public Map<Integer, ByteBuffer> lowerBounds() {
        return columnStats == null ? dataFile.lowerBounds() : columnStats.lowerBounds(columnStatsIndex);
    }

    @Override
    public Map<Integer, ByteBuffer> upperBounds() {
        return columnStats == null ? dataFile.upperBounds() : columnStats.upperBounds(columnStatsIndex);
    }

    @Override
    public ByteBuffer keyMetadata() {
        return dataFile.keyMetadata();
    }

    @Override
    public List<Long> splitOffsets() {
        return dataFile.splitOffsets();
    }

    // A file whose statistics live in the shared store cannot be copied with them: the wrapped copy has none, and
    // no production path copies a cached file with statistics (scans that need them bypass the cache). Fail loudly
    // instead of returning a copy that silently lost them.
    @Override
    public DataFile copy() {
        checkCopyWithStats();
        return dataFile.copy();
    }

    @Override
    public DataFile copyWithoutStats() {
        return dataFile.copyWithoutStats();
    }

    @Override
    public Integer sortOrderId() {
        return dataFile.sortOrderId();
    }

    @Override
    public Long dataSequenceNumber() {
        return dataFile.dataSequenceNumber();
    }

    @Override
    public Long fileSequenceNumber() {
        return dataFile.fileSequenceNumber();
    }

    @Override
    public DataFile copyWithStats(Set<Integer> requestedColumnIds) {
        checkCopyWithStats();
        return dataFile.copyWithStats(requestedColumnIds);
    }

    private void checkCopyWithStats() {
        if (columnStats != null) {
            throw new UnsupportedOperationException(
                    "cannot copy a cached data file with statistics stored column by column: " + dataFile.location());
        }
    }

    @Override
    public Long firstRowId() {
        return dataFile.firstRowId();
    }
}
