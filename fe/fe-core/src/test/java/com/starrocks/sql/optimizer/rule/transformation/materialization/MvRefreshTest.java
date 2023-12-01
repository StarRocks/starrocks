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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import org.junit.BeforeClass;
import org.junit.Test;

public class MvRefreshTest extends MvRewriteTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        MvRewriteTestBase.beforeClass();
        MvRewriteTestBase.prepareDefaultDatas();
    }

    @Test
    public void testMvWithComplexNameRefresh() throws Exception {
        createAndRefreshMv("test", "`aa.bb.cc`",
                "create materialized view `aa.bb.cc`" +
                        " partition by id_date" +
                        " distributed by hash(`t1a`)" +
                        " as" +
                        " select t1a, id_date, t1b from table_with_partition");

        createAndRefreshMv("test",
                "`LEAF_NO_ACC_CUBE_SHADOW_VIEW_.default.luchen_order_8e2c65ba-1c30`",
                "create materialized view `LEAF_NO_ACC_CUBE_SHADOW_VIEW_.default.luchen_order_8e2c65ba-1c30`" +
                        " partition by id_date" +
                        " distributed by hash(`t1a`)" +
                        " as" +
                        " select t1a, id_date, t1b from table_with_partition");
    }
}
