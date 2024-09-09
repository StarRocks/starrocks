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

import com.starrocks.thrift.TQueryStatisticsInfo;
import org.junit.Assert;
import org.junit.Test;

import static com.starrocks.common.proc.CurrentGlobalQueryStatisticsProcDirTest.QUERY_ONE_LOCAL;

public class QueryStatisticsInfoTest {
    QueryStatisticsInfo firstQuery = QUERY_ONE_LOCAL;

    @Test
    public void testEquality() {
        QueryStatisticsInfo otherQuery = new QueryStatisticsInfo(
                firstQuery.getQueryStartTime(),
                firstQuery.getFeIp(),
                firstQuery.getQueryId(),
                firstQuery.getConnId(),
                firstQuery.getDb(),
                firstQuery.getUser(),
                firstQuery.getCpuCostNs(),
                firstQuery.getScanBytes(),
                firstQuery.getScanRows(),
                firstQuery.getMemUsageBytes(),
                firstQuery.getSpillBytes(),
                firstQuery.getExecTime(),
                firstQuery.getWareHouseName(),
                firstQuery.getCustomQueryId(),
                firstQuery.getResourceGroupName()
        );
        Assert.assertEquals(firstQuery, otherQuery);
        Assert.assertEquals(firstQuery.hashCode(), otherQuery.hashCode());
    }

    @Test
    public void testThrift() {
        TQueryStatisticsInfo firstQueryThrift = firstQuery.toThrift();
        QueryStatisticsInfo firstQueryTest = QueryStatisticsInfo.fromThrift(firstQueryThrift);
        Assert.assertEquals(firstQuery, firstQueryTest);
    }
}