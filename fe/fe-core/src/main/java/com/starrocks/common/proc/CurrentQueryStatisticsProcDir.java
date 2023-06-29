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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/CurrentQueryStatisticsProcDir.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common.proc;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.QueryStatisticsFormatter;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.QueryStatisticsItem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * show proc "/current_queries"
 */
public class CurrentQueryStatisticsProcDir implements ProcDirInterface {
    private static final Logger LOG = LogManager.getLogger(CurrentQueryStatisticsProcDir.class);
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("StartTime")
            .add("QueryId")
            .add("ConnectionId")
            .add("Database")
            .add("User")
            .add("ScanBytes")
            .add("ScanRows")
            .add("MemoryUsage")
            .add("DiskSpillSize")
            .add("CPUTime")
            .add("ExecTime")
            .build();

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String name) throws AnalysisException {
        if (Strings.isNullOrEmpty(name)) {
            return null;
        }
        final Map<String, QueryStatisticsItem> statistic = QeProcessorImpl.INSTANCE.getQueryStatistics();
        final QueryStatisticsItem item = statistic.get(name);
        if (item == null) {
            throw new AnalysisException(name + " does't exist.");
        }
        return new CurrentQuerySqlProcDir(item);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        final BaseProcResult result = new BaseProcResult();
        final Map<String, QueryStatisticsItem> statistic =
                QeProcessorImpl.INSTANCE.getQueryStatistics();
        result.setNames(TITLE_NAMES.asList());
        final List<List<String>> sortedRowData = Lists.newArrayList();

        final CurrentQueryInfoProvider provider = new CurrentQueryInfoProvider();
        final Map<String, CurrentQueryInfoProvider.QueryStatistics> statisticsMap
                = provider.getQueryStatistics(statistic.values());
        final List<QueryStatisticsItem> sorted =
                statistic.values().stream()
                        .sorted(Comparator.comparingLong(QueryStatisticsItem::getQueryStartTime))
                        .collect(Collectors.toList());
        for (QueryStatisticsItem item : sorted) {
            final CurrentQueryInfoProvider.QueryStatistics statistics = statisticsMap.get(item.getQueryId());
            if (statistics == null) {
                continue;
            }
            final List<String> values = Lists.newArrayList();
            values.add(TimeUtils.longToTimeString(item.getQueryStartTime()));
            values.add(item.getQueryId());
            values.add(item.getConnId());
            values.add(item.getDb());
            values.add(item.getUser());
            values.add(QueryStatisticsFormatter.getBytes(statistics.getScanBytes()));
            values.add(QueryStatisticsFormatter.getRowsReturned(statistics.getScanRows()));
            values.add(QueryStatisticsFormatter.getBytes(statistics.getMemUsageBytes()));
            values.add(QueryStatisticsFormatter.getBytes(statistics.getSpillBytes()));
            values.add(QueryStatisticsFormatter.getSecondsFromNano(statistics.getCpuCostNs()));
            values.add(QueryStatisticsFormatter.getSecondsFromMilli(item.getQueryExecTime()));
            sortedRowData.add(values);
        }

        result.setRows(sortedRowData);
        return result;
    }
}
