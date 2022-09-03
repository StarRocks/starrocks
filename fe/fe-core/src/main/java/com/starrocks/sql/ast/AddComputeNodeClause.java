// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import java.util.List;

public class AddComputeNodeClause extends ComputeNodeClause {
    public AddComputeNodeClause(List<String> hostPorts) {
        super(hostPorts);
    }
}
