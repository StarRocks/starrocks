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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/SchedulerWorkingSlotsProcDir.java

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
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;

import java.util.List;

public class SchedulerWorkingSlotsProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("BeId").add("PathHash").add("AvailSlots").add("TotalSlots")
            .add("AvgRate").build();

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        List<List<String>> infos = GlobalStateMgr.getCurrentState().getTabletScheduler().getSlotsInfo();
        result.setRows(infos);
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String beIdStr) throws AnalysisException {
        if (Strings.isNullOrEmpty(beIdStr)) {
            throw new AnalysisException("Backend id is null");
        }

        long backendId = -1L;
        try {
            backendId = Long.valueOf(beIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid backend id format: " + beIdStr);
        }

        Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(backendId);
        if (backend == null) {
            throw new AnalysisException("Backend[" + backendId + "] does not exist.");
        }

        return new BackendProcNode(backend);
    }

}
