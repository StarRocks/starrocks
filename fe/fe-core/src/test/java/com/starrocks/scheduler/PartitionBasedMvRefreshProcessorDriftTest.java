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

package com.starrocks.scheduler;

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PartitionBasedMvRefreshProcessorDriftTest {

    private static Map<String, PListCell> listCells(String... values) {
        return Arrays.stream(values).collect(Collectors.toMap(v -> "p" + v, PListCell::new));
    }

    private static OlapTable listPartitionedTable(Map<String, PListCell> cells) {
        OlapTable table = mock(OlapTable.class);
        PartitionInfo partitionInfo = mock(PartitionInfo.class);
        when(partitionInfo.isUnPartitioned()).thenReturn(false);
        when(partitionInfo.isListPartition()).thenReturn(true);
        when(table.isOlapOrCloudNativeTable()).thenReturn(true);
        when(table.getPartitionInfo()).thenReturn(partitionInfo);
        when(table.getListPartitionItems()).thenReturn(cells);
        return table;
    }

    /**
     * The drift check must compare the snapshot copy against the live table. Comparing the snapshot
     * with itself always reports "unchanged", which silently disables the retry loop that guards MV
     * refresh against base-table partitions changing mid-refresh.
     */
    @Test
    public void testListPartitionDriftIsDetectedAgainstLiveTable() {
        BaseTableInfo baseTableInfo = mock(BaseTableInfo.class);
        OlapTable snapshot = listPartitionedTable(listCells("2026-01-01", "2026-01-02"));
        TableSnapshotInfo snapshotInfo = new TableSnapshotInfo(baseTableInfo, snapshot);
        PartitionBasedMvRefreshProcessor processor = new PartitionBasedMvRefreshProcessor();

        mockLiveTable(listPartitionedTable(listCells("2026-01-01", "2026-01-02", "2026-01-03")));
        Assertions.assertTrue(processor.checkBaseTablePartitionHasChanged(snapshotInfo),
                "a partition added to the live table after the snapshot must be reported as changed");

        // Control: an unchanged live table must still report false, so the assertion above cannot pass
        // merely because the check swallows an exception and defaults to true.
        mockLiveTable(listPartitionedTable(listCells("2026-01-01", "2026-01-02")));
        Assertions.assertFalse(processor.checkBaseTablePartitionHasChanged(snapshotInfo),
                "an unchanged live table must not be reported as changed");
    }

    private static void mockLiveTable(OlapTable live) {
        new MockUp<MvUtils>() {
            @Mock
            public Optional<Table> getTableWithIdentifier(BaseTableInfo baseTableInfo) {
                return Optional.of(live);
            }
        };
    }
}
