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

package com.starrocks.qe;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

class QueryStatisticsItemTest {

    @Test
    void testQueryStatisticsItem() {
        String queryId = "123";
        String warehouseName = "wh1";

        final QueryStatisticsItem item = new QueryStatisticsItem.Builder()
                .customQueryId("abc")
                .queryId("123")
                .warehouseName("wh1").build();

        Assert.assertEquals("wh1", item.getWarehouseName());
        Assert.assertEquals("abc", item.getCustomQueryId());
        Assert.assertEquals("123", item.getQueryId());
    }
}
