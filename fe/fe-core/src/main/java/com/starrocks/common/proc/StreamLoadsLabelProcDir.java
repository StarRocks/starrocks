// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common.proc;

import com.starrocks.common.AnalysisException;
import com.starrocks.load.streamload.StreamLoadManager;
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
        StreamLoadManager streamLoadManager = GlobalStateMgr.getCurrentState().getStreamLoadManager();
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