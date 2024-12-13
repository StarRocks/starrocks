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

    public DataFileWrapper(DataFile dataFile) {
        this.dataFile = dataFile;
    }

    public static DataFileWrapper wrap(DataFile dataFile) {
        return new DataFileWrapper(dataFile);
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
        return dataFile.columnSizes();
    }

    @Override
    public Map<Integer, Long> valueCounts() {
        return dataFile.valueCounts();
    }

    @Override
    public Map<Integer, Long> nullValueCounts() {
        return dataFile.nullValueCounts();
    }

    @Override
    public Map<Integer, Long> nanValueCounts() {
        return dataFile.nanValueCounts();
    }

    @Override
    public Map<Integer, ByteBuffer> lowerBounds() {
        return dataFile.lowerBounds();
    }

    @Override
    public Map<Integer, ByteBuffer> upperBounds() {
        return dataFile.upperBounds();
    }

    @Override
    public ByteBuffer keyMetadata() {
        return dataFile.keyMetadata();
    }

    @Override
    public List<Long> splitOffsets() {
        return dataFile.splitOffsets();
    }

    @Override
    public DataFile copy() {
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
        return dataFile.copyWithStats(requestedColumnIds);
    }
}
