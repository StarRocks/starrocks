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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.exception.MetaNotFoundException;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;

/*
    SHOW PROC "/stream_loads"
    show statistic of all of running stream loads

    RESULT:
    Label | Id | DbName | TableName | State
 */
public class StreamLoadsProcDir implements ProcDirInterface {

    private static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>()
                    .add("Label")
                    .add("Id")
                    .add("DbName")
                    .add("TableName")
                    .add("State")
                    .build();

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String label) throws AnalysisException {
        if (Strings.isNullOrEmpty(label)) {
            throw new IllegalArgumentException("label could not be empty of null");
        }
        return new StreamLoadsLabelProcDir(label);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult baseProcResult = new BaseProcResult();
        baseProcResult.setNames(TITLE_NAMES);
        StreamLoadMgr streamLoadManager = GlobalStateMgr.getCurrentState().getStreamLoadMgr();
        try {
            List<StreamLoadTask> streamLoadTaskList = streamLoadManager.getTask(null, null, true);
            for (StreamLoadTask streamLoadTask : streamLoadTaskList) {
                baseProcResult.addRow(streamLoadTask.getShowBriefInfo());
            }
        } catch (MetaNotFoundException e) {
            throw new AnalysisException("failed to get all of stream load task");
        }
        return baseProcResult;
    }
}
