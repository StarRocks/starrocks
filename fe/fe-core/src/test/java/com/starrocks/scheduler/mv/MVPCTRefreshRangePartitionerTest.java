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

package com.starrocks.scheduler.mv;

import com.google.common.collect.Maps;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.sql.common.PCell;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MVPCTRefreshRangePartitionerTest {

    @Test
    public void testFilterPartitionsByTTL() {
        MvTaskRunContext mvContext = mock(MvTaskRunContext.class);
        when(mvContext.getPartitionTTLNumber()).thenReturn(2);

        MaterializedView mv = mock(MaterializedView.class);
        when(mv.getTableProperty()).thenReturn(mock(TableProperty.class));
        when(mv.getPartitionInfo()).thenReturn(mock(PartitionInfo.class));
        when(mv.getTableProperty().getPartitionTTLNumber()).thenReturn(2);

        MVPCTRefreshRangePartitioner partitioner = new MVPCTRefreshRangePartitioner(mvContext, null, null, mv);

        Map<String, PCell> toRefreshPartitions = Maps.newHashMap();
        toRefreshPartitions.put("partition1", mock(PCell.class));
        toRefreshPartitions.put("partition2", mock(PCell.class));
        toRefreshPartitions.put("partition3", mock(PCell.class));

        partitioner.filterPartitionsByTTL(toRefreshPartitions, true);

        Assert.assertEquals(2, toRefreshPartitions.size());
    }
}