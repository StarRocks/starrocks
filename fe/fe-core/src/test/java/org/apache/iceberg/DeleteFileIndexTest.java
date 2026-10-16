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

package org.apache.iceberg;

import com.starrocks.connector.iceberg.DeleteFileWrapper;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeleteFileIndexTest {
    @Test
    public void testDeletionVectorsMatchOnlyReferencedDataFile() {
        PartitionSpec spec = PartitionSpec.unpartitioned();
        DataFile first = DataFiles.builder(spec).withPath("/data-a.parquet")
                .withFileSizeInBytes(100).withRecordCount(10).build();
        DataFile second = DataFiles.builder(spec).withPath("/data-b.parquet")
                .withFileSizeInBytes(100).withRecordCount(10).build();
        DataFile unrelated = DataFiles.builder(spec).withPath("/data-c.parquet")
                .withFileSizeInBytes(100).withRecordCount(10).build();
        DeleteFile firstDV = deletionVector(first.location(), 4L);
        DeleteFile secondDV = deletionVector(second.location(), 44L);

        DeleteFileIndex directIndex = DeleteFileIndex.builderFor(List.of(firstDV, secondDV))
                .specsById(Map.of(spec.specId(), spec)).build();
        DeleteFileIndex cachedIndex = DeleteFileIndex.builderFor(List.<DeleteFile>of())
                .cachedDeleteFiles(Set.of(firstDV, secondDV)).specsById(Map.of(spec.specId(), spec)).build();
        for (DeleteFileIndex index : List.of(directIndex, cachedIndex)) {
            assertArrayEquals(new DeleteFile[] {firstDV}, index.forDataFile(2, first));
            assertArrayEquals(new DeleteFile[] {firstDV}, index.forDataFile(3, first));
            assertArrayEquals(new DeleteFile[] {secondDV}, index.forDataFile(3, second));
            assertArrayEquals(new DeleteFile[0], index.forDataFile(3, unrelated));
            assertThrows(ValidationException.class, () -> index.forDataFile(4, first));
        }
    }

    private DeleteFile deletionVector(String dataPath, long offset) {
        DeleteFile file = mock(DeleteFile.class);
        when(file.content()).thenReturn(FileContent.POSITION_DELETES);
        when(file.format()).thenReturn(FileFormat.PUFFIN);
        when(file.path()).thenReturn("/shared.puffin");
        when(file.referencedDataFile()).thenReturn(dataPath);
        when(file.contentOffset()).thenReturn(offset);
        when(file.contentSizeInBytes()).thenReturn(40L);
        when(file.dataSequenceNumber()).thenReturn(3L);
        return DeleteFileWrapper.wrap(file);
    }
}
