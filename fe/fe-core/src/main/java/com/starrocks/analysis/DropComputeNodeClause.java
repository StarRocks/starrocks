// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.analysis;

import java.util.List;

public class DropComputeNodeClause extends ComputeNodeClause {

    public DropComputeNodeClause(List<String> hostPorts) {
        super(hostPorts);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP COMPUTE NODE ");
        for (int i = 0; i < hostPorts.size(); i++) {
            sb.append("\"").append(hostPorts.get(i)).append("\"");
            if (i != hostPorts.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
