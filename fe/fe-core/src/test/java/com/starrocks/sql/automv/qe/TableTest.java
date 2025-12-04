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

package com.starrocks.sql.automv.qe;

import com.starrocks.catalog.TableName;
import org.junit.Assert;
import org.junit.Test;

public class TableTest {
    @Test
    public void testTableNamePlus() {
        TableName t0 = new TableName("hive", "tpch", "lineitem");
        TableNamePlus t0p = TableNamePlus.of(t0);
        Assert.assertEquals(t0p.getFqName(), t0p.getFqName(), "`hive`.`tpch`.`lineitem`");
        TableName t1 = new TableName(null, "tpch", "lineitem");
        TableNamePlus t1p = TableNamePlus.of(t1);
        Assert.assertEquals(t1p.getFqName(), t1p.getFqName(), "`default_catalog`.`tpch`.`lineitem`");
    }
}
