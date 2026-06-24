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

package com.starrocks.transaction;

import com.starrocks.proto.TabletStatPB;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PartitionCommitInfoTest {
    @Test
    public void testTabletStatsCopiedByCopyConstructor() {
        PartitionCommitInfo src = new PartitionCommitInfo(1L, 2L, 3L);
        TabletStatPB stat = new TabletStatPB();
        stat.numRows = 10L;
        stat.dataSize = 100L;
        src.getTabletStats().put(7L, stat);

        PartitionCommitInfo copy = new PartitionCommitInfo(src);
        Assertions.assertEquals(1, copy.getTabletStats().size());
        Assertions.assertEquals(100L, copy.getTabletStats().get(7L).dataSize);
    }
}
