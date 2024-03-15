// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.alter.AlterOpType;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class DisableDiskClause extends AlterClause {
    private final String beHostPort;
    private final List<String> diskList;

    public DisableDiskClause(String beHostPort, List<String> diskList) {
        this(beHostPort, diskList, NodePosition.ZERO);
    }

    public DisableDiskClause(String beHostPort, List<String> diskList, NodePosition pos) {
        super(AlterOpType.ALTER_OTHER, pos);
        this.beHostPort = beHostPort;
        this.diskList = diskList;
    }

    public List<String> getDiskList() {
        return diskList;
    }

    public String getBeHostPort() {
        return beHostPort;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        if (visitor instanceof AstVisitorEPack) {
            return ((AstVisitorEPack<R, C>) visitor).visitDisableDiskClause(this, context);
        } else {
            return null;
        }
    }
}