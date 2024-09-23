// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.server.GlobalStateMgr;

public class FailoverGroupProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> FAILOVER_GROUP_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Id")
            .add("Name")
            .add("Role")
            .add("State")
            .add("Schedule")
            .add("IsSuspended")
            .add("ScheduledTime")
            .add("FinishedTime")
            .add("FinishedRound")
            .add("ReplicatedJournalId")
            .add("LastScheduledTime")
            .add("LastFinishedTime")
            .add("Errors")
            .build();

    private String failoverGroupName;

    public FailoverGroupProcNode(String failoverGroupName) {
        this.failoverGroupName = failoverGroupName;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(FAILOVER_GROUP_PROC_NODE_TITLE_NAMES);
        FailoverGroupMgr failoverGroupMgr = GlobalStateMgr.getCurrentState().getFailoverGroupMgr();
        FailoverGroup failoverGroup = failoverGroupMgr.getFailoverGroup(failoverGroupName);
        if (failoverGroup == null) {
            return result;
        }

        result.addRow(Lists.newArrayList(
                String.valueOf(failoverGroup.getId()),
                failoverGroup.getName(),
                failoverGroup.getRole().toString(),
                failoverGroup.getState().toString(),
                failoverGroup.getSchedule().getSchedule(),
                String.valueOf(failoverGroup.getSchedule().isSuspended()),
                TimeUtils.longToTimeString(failoverGroup.getSchedule().getScheduledTimeMs()),
                TimeUtils.longToTimeString(failoverGroup.getSchedule().getFinishedTimeMs()),
                String.valueOf(failoverGroup.getSchedule().getRoundFinishedTimes()),
                String.valueOf(failoverGroup.getReplicatedJournalId()),
                TimeUtils.longToTimeString(failoverGroup.getSchedule().getLastScheduledTimeMs()),
                TimeUtils.longToTimeString(failoverGroup.getSchedule().getLastFinishedTimeMs()),
                failoverGroup.getErrorMessages().isEmpty() ? "" : failoverGroup.getErrorMessages().toString()));

        return result;
    }
}
