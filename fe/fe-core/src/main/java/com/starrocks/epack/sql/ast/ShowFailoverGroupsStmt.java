// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.epack.failover.FailoverGroupProcNode;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ShowFailoverGroupsStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("Id", ScalarType.createVarchar(20)))
            .addColumn(new Column("Name", ScalarType.createVarchar(20)))
            .addColumn(new Column("State", ScalarType.createVarchar(20)))
            .addColumn(new Column("Role", ScalarType.createVarchar(20)))
            .addColumn(new Column("Members", ScalarType.createVarchar(256)))
            .addColumn(new Column("Schedule", ScalarType.createVarchar(32)))
            .addColumn(new Column("IsSuspended", ScalarType.createVarchar(20)))
            .addColumn(new Column("ScheduledTime", ScalarType.createVarchar(20)))
            .addColumn(new Column("FinishedTime", ScalarType.createVarchar(20)))
            .addColumn(new Column("Comment", ScalarType.createVarchar(256)))
            .addColumn(new Column("Properties", ScalarType.createVarchar(256)))
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
        return ((AstVisitorEPack<R, C>) visitor).visitShowFailoverGroupsStatement(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    public List<List<String>> getRows() throws AnalysisException {
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
                    /*
                     * TODO: Authentication
                     * try {
                     * Authorizer.checkAnyActionOnFailoverGroup(connectContext.
                     * getCurrentUserIdentity(),
                     * connectContext.getCurrentRoleIds(), failoverGroup.getName());
                     * } catch (AccessDeniedException e) {
                     * return false;
                     * }
                     */
                    return true;
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
