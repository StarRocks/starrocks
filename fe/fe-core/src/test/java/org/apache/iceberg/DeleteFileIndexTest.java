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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeleteFileIndexTest {

    @Test
    public void emptyIndexBuildsWithoutSpecsOrSchemas() {
        // StarRocksIcebergTableScan.refreshDataFileCache() builds this sentinel with neither specsById nor
        // schemasById. build() resolves equality-delete field IDs against the table schemas up front, so
        // it must tolerate both maps being absent rather than dereference specsById.
        DeleteFileIndex index = DeleteFileIndex.builderFor(new ArrayList<>()).build();
        assertTrue(index.isEmpty());
    }

    @Test
    public void emptyIndexBuildsWithSpecsButNoSchemas() {
        // The path most internal callers take: specsById set, schemasById not. Schemas then come from the specs.
        PartitionSpec spec = PartitionSpec.unpartitioned();
        DeleteFileIndex index = DeleteFileIndex.builderFor(new ArrayList<>())
                .specsById(ImmutableMap.of(spec.specId(), spec))
                .build();
        assertTrue(index.isEmpty());
    }
}
