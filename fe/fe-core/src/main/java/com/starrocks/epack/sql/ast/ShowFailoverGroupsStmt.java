// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.catalog.Column;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.epack.authorization.AuthorizerEPack;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.epack.failover.FailoverGroupProcNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.TypeFactory;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ShowFailoverGroupsStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("Id", TypeFactory.createVarcharType(20)))
            .addColumn(new Column("Name", TypeFactory.createVarcharType(20)))
            .addColumn(new Column("Role", TypeFactory.createVarcharType(20)))
            .addColumn(new Column("State", TypeFactory.createVarcharType(20)))
            .addColumn(new Column("Schedule", TypeFactory.createVarcharType(32)))
            .addColumn(new Column("IsSuspended", TypeFactory.createVarcharType(20)))
            .addColumn(new Column("ScheduledTime", TypeFactory.createVarcharType(20)))
            .addColumn(new Column("FinishedTime", TypeFactory.createVarcharType(20)))
            .addColumn(new Column("FinishedRound", TypeFactory.createVarcharType(20)))
            .addColumn(new Column("ReplicatedJournalId", TypeFactory.createVarcharType(20)))
            .addColumn(new Column("LastScheduledTime", TypeFactory.createVarcharType(20)))
            .addColumn(new Column("LastFinishedTime", TypeFactory.createVarcharType(20)))
            .addColumn(new Column("Errors", TypeFactory.createVarcharType(1024)))
            .build();

    private final String pattern;

    public ShowFailoverGroupsStmt(
            String pattern,
            NodePosition pos) {
        super(pos);
        this.pattern = pattern;
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        if (visitor instanceof AstVisitorEPack) {
            return ((AstVisitorEPack<R, C>) visitor).visitShowFailoverGroupsStatement(this, context);
        } else {
            return null;
        }
    }



    public List<List<String>> getRows(ConnectContext connectContext) throws AnalysisException {
        Collection<FailoverGroup> failoverGroups = GlobalStateMgr.getCurrentState().getFailoverGroupMgr()
                .getFailoverGroups();
        PatternMatcher matcher = null;
        if (pattern != null && !pattern.isEmpty()) {
            matcher = PatternMatcher.createMysqlPattern(pattern,
                    CaseSensibility.FAILOVER_GROUP.getCaseSensibility());
        }
        PatternMatcher finalMatcher = matcher;
        failoverGroups = failoverGroups.stream()
                .filter(failoverGroup -> finalMatcher == null || finalMatcher.match(failoverGroup.getName()))
                .filter(failoverGroup -> {
                    if (connectContext == null) {
                        return true;
                    }
                    try {
                        AuthorizerEPack.checkAnyActionOnFailoverGroup(connectContext, failoverGroup.getName());
                        return true;
                    } catch (AccessDeniedException e) {
                        return false;
                    }
                }).collect(Collectors.toList());

        List<List<String>> rows = Lists.newArrayList();
        for (FailoverGroup failoverGroup : failoverGroups) {
            ProcResult procResult = new FailoverGroupProcNode(failoverGroup.getName()).fetchResult();
            List<List<String>> procRows = procResult.getRows();
            if (!procRows.isEmpty()) {
                rows.add(procRows.get(0));
            }
        }
        return rows;
    }
}
