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

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.StructLike;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

// checking hashcode and equals for manifest cache
public class DeleteFileWrapper implements DeleteFile {
    private final DeleteFile deleteFile;

    public DeleteFileWrapper(DeleteFile deleteFile) {
        this.deleteFile = deleteFile;
    }

    public static DeleteFileWrapper wrap(DeleteFile deleteFile) {
        return new DeleteFileWrapper(deleteFile);
    }

    public DeleteFile getDataFile() {
        return deleteFile;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DeleteFileWrapper that = (DeleteFileWrapper) o;

        return deleteFile.path().toString().equals(that.deleteFile.path().toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(deleteFile.path());
    }

    @Override
    public Long pos() {
        return deleteFile.pos();
    }

    @Override
    public int specId() {
        return deleteFile.specId();
    }

    @Override
    public FileContent content() {
        return deleteFile.content();
    }

    @Override
    public CharSequence path() {
        return deleteFile.path();
    }

    @Override
    public FileFormat format() {
        return deleteFile.format();
    }

    @Override
    public StructLike partition() {
        return deleteFile.partition();
    }

    @Override
    public long recordCount() {
        return deleteFile.recordCount();
    }

    @Override
    public long fileSizeInBytes() {
        return deleteFile.fileSizeInBytes();
    }

    @Override
    public Map<Integer, Long> columnSizes() {
        return deleteFile.columnSizes();
    }

    @Override
    public Map<Integer, Long> valueCounts() {
        return deleteFile.valueCounts();
    }

    @Override
    public Map<Integer, Long> nullValueCounts() {
        return deleteFile.nullValueCounts();
    }

    @Override
    public Map<Integer, Long> nanValueCounts() {
        return deleteFile.nanValueCounts();
    }

    @Override
    public Map<Integer, ByteBuffer> lowerBounds() {
        return deleteFile.lowerBounds();
    }

    @Override
    public Map<Integer, ByteBuffer> upperBounds() {
        return deleteFile.upperBounds();
    }

    @Override
    public ByteBuffer keyMetadata() {
        return deleteFile.keyMetadata();
    }

    @Override
    public List<Long> splitOffsets() {
        return deleteFile.splitOffsets();
    }

    @Override
    public List<Integer> equalityFieldIds() {
        return deleteFile.equalityFieldIds();
    }

    @Override
    public DeleteFile copy() {
        return deleteFile.copy();
    }

    @Override
    public DeleteFile copyWithoutStats() {
        return deleteFile.copyWithoutStats();
    }

    @Override
    public DeleteFile copyWithStats(Set<Integer> requestedColumnIds) {
        return deleteFile.copyWithStats(requestedColumnIds);
    }

    @Override
    public Integer sortOrderId() {
        return deleteFile.sortOrderId();
    }

    @Override
    public Long dataSequenceNumber() {
        return deleteFile.dataSequenceNumber();
    }

    @Override
    public Long fileSequenceNumber() {
        return deleteFile.fileSequenceNumber();
    }
}
