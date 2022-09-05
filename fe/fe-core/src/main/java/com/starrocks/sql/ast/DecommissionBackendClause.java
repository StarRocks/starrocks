// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.alter.DecommissionType;
import com.starrocks.sql.ast.BackendClause;

import java.util.List;

public class DecommissionBackendClause extends BackendClause {

    private DecommissionType type;

    public DecommissionBackendClause(List<String> hostPorts) {
        super(hostPorts);
        type = DecommissionType.SystemDecommission;
    }

    public void setType(DecommissionType type) {
        this.type = type;
    }
}
