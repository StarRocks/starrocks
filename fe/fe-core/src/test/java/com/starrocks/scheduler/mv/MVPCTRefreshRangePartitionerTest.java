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