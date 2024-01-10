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

package com.starrocks.persist;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class TableAddOrDropColumnsInfoTest {
    @Test
    public void test() {
        TableAddOrDropColumnsInfo info = new TableAddOrDropColumnsInfo(1, 1,
                Collections.emptyMap(), Collections.emptyList(), 0, 1, 0,
                Collections.emptySet(), Collections.emptyMap());

        Assert.assertEquals(1, info.getDbId());
        Assert.assertEquals(1, info.getTableId());
        Assert.assertEquals(0, info.getIndexes().size());
        Assert.assertEquals(0, info.getIndexSchemaMap().size());
        Assert.assertEquals(0, info.getJobId());
        Assert.assertEquals(1, info.getTxnId());
        Assert.assertEquals(0, info.getStartTime());
        Assert.assertEquals(0, info.getAddColumnsName().size());
        Assert.assertEquals(0, info.getIndexToNewSchemaId().size());

        TableAddOrDropColumnsInfo info2 = new TableAddOrDropColumnsInfo(1, 1,
                Collections.emptyMap(), Collections.emptyList(), 0, 1, 0,
                Collections.emptySet(), Collections.emptyMap());

        Assert.assertEquals(info.hashCode(), info2.hashCode());
        Assert.assertEquals(info, info2);
        Assert.assertNotNull(info.toString());

    }

}
