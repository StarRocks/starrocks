// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.alter;

import com.google.common.collect.Lists;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.server.GlobalStateMgr;
import jersey.repackaged.com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class MaterializedViewManagerTest {
    private static List<Column> columns;

    @Mocked
    private GlobalStateMgr stateMgr;

    private MaterializedViewManager materializedViewManager;

    @Before
    public void setUp() {
        columns = new LinkedList<Column>();
        columns.add(new Column("k1", ScalarType.createType(PrimitiveType.TINYINT), true, null, "", ""));
        columns.add(new Column("k2", ScalarType.createType(PrimitiveType.SMALLINT), true, null, "", ""));
        columns.add(new Column("v1", ScalarType.createType(PrimitiveType.INT), false, AggregateType.SUM, "", ""));
        materializedViewManager = new MaterializedViewManager();
    }

    @Test
    public void testRegisterMaterializedView() {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = stateMgr;

                stateMgr.getMaterializedViewManager();
                result = materializedViewManager;
            }
        };
        MaterializedView mv = new MaterializedView(1000, 100, "mv", columns, KeysType.AGG_KEYS,
                null, null, null);
        Set<Long> baseTableIds = Sets.newHashSet(1L, 2L, 3L);
        mv.setBaseTableIds(baseTableIds);
        boolean ret = GlobalStateMgr.getCurrentState().getMaterializedViewManager().registerMaterializedView(mv);
        Assert.assertTrue(ret);
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getMaterializedViewManager().containMv("mv"));
        MaterializedView mv2 = GlobalStateMgr.getCurrentState().getMaterializedViewManager().getMaterializedView("mv");
        Assert.assertTrue(mv2 != null);
        Assert.assertEquals(1000, mv2.getId());
        ret = GlobalStateMgr.getCurrentState().getMaterializedViewManager().registerMaterializedView(mv);
        Assert.assertFalse(ret);
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getMaterializedViewManager().containMv("mv"));
        GlobalStateMgr.getCurrentState().getMaterializedViewManager().deregisterMaterializedView(mv);
        Assert.assertFalse(GlobalStateMgr.getCurrentState().getMaterializedViewManager().containMv("mv"));
        MaterializedView mv3 = GlobalStateMgr.getCurrentState().getMaterializedViewManager().getMaterializedView("mv");
        Assert.assertTrue(mv3 == null);
        ret = GlobalStateMgr.getCurrentState().getMaterializedViewManager().registerMaterializedView(mv);
        Assert.assertTrue(ret);

        MaterializedView mv4 = new MaterializedView(1002, 100, "mv4", columns, KeysType.AGG_KEYS,
                null, null, null);
        Set<Long> baseTableIds2 = Sets.newHashSet(1L, 2L);
        mv4.setBaseTableIds(baseTableIds2);
        ret = GlobalStateMgr.getCurrentState().getMaterializedViewManager().registerMaterializedView(mv4);
        Assert.assertTrue(ret);

        Set<MaterializedView> materializedViewSet = GlobalStateMgr.getCurrentState().getMaterializedViewManager().getMaterializedViewSetForTableId(1L);
        Assert.assertEquals(2, materializedViewSet.size());
        Set<MaterializedView> materializedViewSet2 = GlobalStateMgr.getCurrentState().getMaterializedViewManager().getMaterializedViewSetForTableId(3L);
        Assert.assertEquals(1, materializedViewSet2.size());
        Set<MaterializedView> materializedViewSet3 = GlobalStateMgr.getCurrentState().getMaterializedViewManager().getMaterializedViewSetForTableId(4L);
        Assert.assertTrue(materializedViewSet3 == null);
    }
}
