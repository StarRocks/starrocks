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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/ProcService.java

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
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.QueryStatisticsInfo;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
 * show proc "/global_current_queries"
 */
public class CurrentGlobalQueryStatisticsProcDir implements ProcDirInterface {
    private static final Logger LOG = LogManager.getLogger(CurrentGlobalQueryStatisticsProcDir.class);

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String name) throws AnalysisException {
        if (Strings.isNullOrEmpty(name)) {
            return null;
        }

        throw new AnalysisException("not implemented");
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        final BaseProcResult result = new BaseProcResult();
        result.setNames(CurrentQueryStatisticsProcDir.TITLE_NAMES.asList());
        List<QueryStatisticsInfo> queryInfos = QueryStatisticsInfo.makeListFromMetricsAndMgrs();
        List<QueryStatisticsInfo> otherQueryInfos = GlobalStateMgr.getCurrentState().getQueryStatisticsInfoFromOtherFEs();

        List<QueryStatisticsInfo> allInfos = Stream.concat(queryInfos.stream(), otherQueryInfos.stream())
                .collect(Collectors.toList());

        List<List<String>> sortedRowData = allInfos
                .stream()
                .map(QueryStatisticsInfo::formatToList)
                .collect(Collectors.toList());

        result.setRows(sortedRowData);
        return result;
    }
}
