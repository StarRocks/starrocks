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

import com.starrocks.common.exception.AnalysisException;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ShowStreamLoadStmt;

import java.util.List;

/*
    SHOW PROC "/stream_loads/{label}"
    show all of stream load named task name in all of db

    RESULT
    show result is sames as show stream load {label}
 */
public class StreamLoadsLabelProcDir implements ProcNodeInterface {

    private final String label;

    public StreamLoadsLabelProcDir(String label) {
        this.label = label;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        StreamLoadMgr streamLoadManager = GlobalStateMgr.getCurrentState().getStreamLoadMgr();
        List<StreamLoadTask> streamLoadTaskList = streamLoadManager.getTaskByName(label);
        if (streamLoadTaskList.isEmpty()) {
            throw new AnalysisException("stream load label[" + label + "] does not exist");
        }
        BaseProcResult baseProcResult = new BaseProcResult();
        baseProcResult.setNames(ShowStreamLoadStmt.getTitleNames());
        for (StreamLoadTask streamLoadTask : streamLoadTaskList) {
            baseProcResult.addRow(streamLoadTask.getShowInfo());
        }
        return baseProcResult;
    }
}