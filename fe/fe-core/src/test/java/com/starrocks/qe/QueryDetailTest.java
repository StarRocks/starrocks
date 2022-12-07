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

import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

public class QueryDetailTest {
    @Test
    public void testQueryDetail() {
        QueryDetail queryDetail = new QueryDetail("219a2d5443c542d4-8fc938db37c892e3", true, 1, "127.0.0.1",
                System.currentTimeMillis(), -1, -1, QueryDetail.QueryMemState.RUNNING,
                "testDb", "select * from table1 limit 1",
                "root", "");
        queryDetail.setProfile("bbbbb");
        queryDetail.setErrorMessage("cancelled");

        QueryDetail copyOfQueryDetail = queryDetail.copy();
        Gson gson = new Gson();
        Assert.assertEquals(gson.toJson(queryDetail), gson.toJson(copyOfQueryDetail));

        queryDetail.setLatency(10);
        Assert.assertEquals(-1, copyOfQueryDetail.getLatency());
    }
}
