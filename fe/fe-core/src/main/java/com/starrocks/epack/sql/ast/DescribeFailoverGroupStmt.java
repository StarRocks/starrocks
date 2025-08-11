// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.epack.failover.FailoverGroupMember;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class DescribeFailoverGroupStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("Id", ScalarType.createVarchar(20)))
            .addColumn(new Column("Name", ScalarType.createVarchar(20)))
            .addColumn(new Column("Include Tables", ScalarType.createVarchar(256)))
            .addColumn(new Column("Exclude Tables", ScalarType.createVarchar(256)))
            .addColumn(new Column("Members", ScalarType.createVarchar(256)))
            .addColumn(new Column("Schedule", ScalarType.createVarchar(32)))
            .addColumn(new Column("Comment", ScalarType.createVarchar(256)))
            .addColumn(new Column("Properties", ScalarType.createVarchar(256)))
            .build();

    private final String failoverGroupName;

    public DescribeFailoverGroupStmt(
            String failoverGroupName,
            NodePosition pos) {
        super(pos);
        this.failoverGroupName = failoverGroupName;
    }

    public String getFailoverGroupName() {
        return failoverGroupName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        if (visitor instanceof AstVisitorEPack) {
            return ((AstVisitorEPack<R, C>) visitor).visitDescribeFailoverGroupStatement(this, context);
        } else {
            return null;
        }
    }



    public List<List<String>> getRows() throws AnalysisException {
        FailoverGroup failoverGroup = GlobalStateMgr.getCurrentState().getFailoverGroupMgr()
                .getFailoverGroup(failoverGroupName);
        if (failoverGroup == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_FAILOVER_GROUP, failoverGroupName);
        }

        List<String> includeTableNames = failoverGroup.getIncludeMgr().getIncludeTableNames();
        List<String> excludeTableNames = failoverGroup.getExcludeMgr().getExcludeTableNames();
        Collection<FailoverGroupMember> members = failoverGroup.getMembers().values();
        Map<String, String> properties = failoverGroup.getProperties();
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList(
                String.valueOf(failoverGroup.getId()),
                failoverGroup.getName(),
                includeTableNames.isEmpty() ? "" : includeTableNames.toString(),
                excludeTableNames.isEmpty() ? "" : excludeTableNames.toString(),
                members.isEmpty() ? "" : members.toString(),
                failoverGroup.getSchedule().getSchedule(),
                failoverGroup.getComment(),
                properties.isEmpty() ? "" : properties.toString()));
        return rows;
    }
}
