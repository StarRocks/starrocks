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
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.QueryStatisticsInfo;
import com.starrocks.qe.QueryStatisticsItem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
            .add("feIp")
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
            .add("Warehouse")
            .add("CustomQueryId")
            .add("ResourceGroup")
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
        result.setNames(TITLE_NAMES.asList());
        List<QueryStatisticsInfo> queryInfos = QueryStatisticsInfo.makeListFromMetricsAndMgrs();
        List<List<String>> sortedRowData = queryInfos
                .stream()
                .map(QueryStatisticsInfo::formatToList)
                .collect(Collectors.toList());

        result.setRows(sortedRowData);
        return result;
    }
}
