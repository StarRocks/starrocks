// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common.proc;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.load.streamload.StreamLoadManager;
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
        StreamLoadManager streamLoadManager = GlobalStateMgr.getCurrentState().getStreamLoadManager();
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
