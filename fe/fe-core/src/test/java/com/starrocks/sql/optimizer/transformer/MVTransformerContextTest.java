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

package com.starrocks.sql.optimizer.transformer;

import com.starrocks.catalog.View;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MVTransformerContextTest extends MVTestBase  {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withTable(cluster, "depts");
        starRocksAssert.withTable(cluster, "emps");
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    @Test
    public void testIsViewBasedRewrite1() throws Exception {
        connectContext.getSessionVariable().setEnableMaterializedViewTextMatchRewrite(true);
        MVTransformerContext mvTransformerContext = new MVTransformerContext(connectContext, false);
        Assertions.assertTrue(mvTransformerContext.isEnableTextBasedMVRewrite());
        Assertions.assertFalse(mvTransformerContext.isInlineView());
        Assertions.assertFalse(mvTransformerContext.isEnableViewBasedMVRewrite(null));

        starRocksAssert.withView("CREATE VIEW view1 as select * from emps");
        View view1 = getView("view1");
        Assertions.assertFalse(mvTransformerContext.isEnableViewBasedMVRewrite(view1));

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv1 " +
                "DISTRIBUTED BY HASH(empid) " +
                "REFRESH ASYNC " +
                "AS select empid, deptno from view1");
        Assertions.assertFalse(view1.getRelatedMaterializedViews().isEmpty());
        Assertions.assertTrue(mvTransformerContext.isEnableViewBasedMVRewrite(view1));
        starRocksAssert.dropView("view1");
    }

    @Test
    public void testIsViewBasedRewrite2() throws Exception {
        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);
        MVTransformerContext mvTransformerContext = new MVTransformerContext(connectContext, false);
        Assertions.assertFalse(mvTransformerContext.isEnableTextBasedMVRewrite());
        Assertions.assertFalse(mvTransformerContext.isInlineView());
        Assertions.assertFalse(mvTransformerContext.isEnableViewBasedMVRewrite(null));

        starRocksAssert.withView("CREATE VIEW view1 as select * from emps");
        View view1 = getView("view1");
        Assertions.assertFalse(mvTransformerContext.isEnableViewBasedMVRewrite(view1));

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv1 " +
                "DISTRIBUTED BY HASH(empid) " +
                "REFRESH ASYNC " +
                "AS select empid, deptno from view1");
        Assertions.assertFalse(view1.getRelatedMaterializedViews().isEmpty());
        Assertions.assertFalse(mvTransformerContext.isEnableViewBasedMVRewrite(view1));
        starRocksAssert.dropView("view1");
    }
}
