// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import java.util.List;

public class DropBackendClause extends BackendClause {
    private final boolean force;

    public DropBackendClause(List<String> hostPorts) {
        super(hostPorts);
        this.force = true;
    }

    public DropBackendClause(List<String> hostPorts, boolean force) {
        this(hostPorts, force, false);
    }

    public DropBackendClause(List<String> hostPorts, boolean force, boolean oldStyle) {
        super(hostPorts);
        this.force = force;
    }

    public boolean isForce() {
        return force;
    }
}
