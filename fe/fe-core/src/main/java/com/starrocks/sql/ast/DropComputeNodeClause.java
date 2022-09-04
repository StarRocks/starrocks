// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.starrocks.sql.ast.ComputeNodeClause;

import java.util.List;

public class DropComputeNodeClause extends ComputeNodeClause {
    public DropComputeNodeClause(List<String> hostPorts) {
        super(hostPorts);
    }
}
