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

package com.starrocks.common.proc;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.QueryStatisticsItem;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CurrentQueryStatisticsProcDirTest {
    private CurrentQueryStatisticsProcDir currentQueryStatisticsProcDir;

    @Before
    public void setUp() throws DdlException, AnalysisException {
        currentQueryStatisticsProcDir = new CurrentQueryStatisticsProcDir();
    }

    @Test
    public void testFetchResult() throws AnalysisException {
        Map<String, QueryStatisticsItem> statistic = new HashMap<>();
        statistic.put("queryId1", new QueryStatisticsItem.Builder()
                .queryStartTime(1)
                .customQueryId("abc1")
                .queryId("queryId1")
                .warehouseName("wh1")
                .resourceGroupName("wg1")
                .build()
        );
        statistic.put("queryId2", new QueryStatisticsItem.Builder()
                .queryStartTime(2)
                .customQueryId("abc2")
                .queryId("queryId2")
                .warehouseName("wh1")
                .resourceGroupName("wg2")
                .build()
        );
        new MockUp<QeProcessorImpl>() {
            @Mock
            public Map<String, QueryStatisticsItem> getQueryStatistics() {
                return statistic;
            }
        };
        final Map<String, CurrentQueryInfoProvider.QueryStatistics> statisticsMap = new HashMap<>();
        statisticsMap.put("queryId1", new CurrentQueryInfoProvider.QueryStatistics());
        statisticsMap.put("queryId2", new CurrentQueryInfoProvider.QueryStatistics());
        new MockUp<CurrentQueryInfoProvider>() {
            @Mock
            public Map<String, CurrentQueryInfoProvider.QueryStatistics> getQueryStatistics(
                    Collection<QueryStatisticsItem> items) throws AnalysisException {
                return statisticsMap;
            }
        };
        BaseProcResult result = (BaseProcResult) currentQueryStatisticsProcDir.fetchResult();
        List<List<String>> rows = result.getRows();
        List<String> list1 = rows.get(0);
        Assert.assertEquals(list1.size(), CurrentQueryStatisticsProcDir.TITLE_NAMES.size());
        // QueryId
        Assert.assertEquals("queryId1", list1.get(2));
        // Warehouse
        Assert.assertEquals("wh1", list1.get(12));
        // CustomQueryId
        Assert.assertEquals("abc1", list1.get(13));
        // ResourceGroupName
        Assert.assertEquals("wg1", list1.get(14));

        List<String> list2 = rows.get(1);
        Assert.assertEquals(list2.size(), CurrentQueryStatisticsProcDir.TITLE_NAMES.size());
        // QueryId
        Assert.assertEquals("queryId2", list2.get(2));
        // Warehouse
        Assert.assertEquals("wh1", list2.get(12));
        // CustomQueryId
        Assert.assertEquals("abc2", list2.get(13));
        // ResourceGroupName
        Assert.assertEquals("wg2", list2.get(14));
    }

    @Test
    public void testRegister() {
        Assert.assertFalse(currentQueryStatisticsProcDir.register(null, null));
    }
}
