// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class DescribeFailoverGroupStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("Catalogs", ScalarType.createVarchar(256)))
            .addColumn(new Column("Databases", ScalarType.createVarchar(256)))
            .addColumn(new Column("Tables", ScalarType.createVarchar(256)))
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
        return ((AstVisitorEPack<R, C>) visitor).visitDescribeFailoverGroupStatement(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    public List<List<String>> getRows() throws AnalysisException {
        FailoverGroup failoverGroup = GlobalStateMgr.getCurrentState().getFailoverGroupMgr()
                .getFailoverGroup(failoverGroupName);
        if (failoverGroup == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_FAILOVER_GROUP, failoverGroupName);
        }

        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList(
                failoverGroup.getObjectMgr().getCatalogNames().toString(),
                failoverGroup.getObjectMgr().getDatabaseNames().toString(),
                failoverGroup.getObjectMgr().getTableNames().toString(),
                failoverGroup.getMembers().toString(),
                failoverGroup.getSchedule().getSchedule(),
                failoverGroup.getComment(),
                failoverGroup.getProperties().isEmpty() ? "" : failoverGroup.getProperties().toString()));
        return rows;
    }
}
