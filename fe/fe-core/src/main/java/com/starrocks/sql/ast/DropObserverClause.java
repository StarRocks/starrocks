// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.ha.FrontendNodeType;

public class DropObserverClause extends FrontendClause {
    public DropObserverClause(String hostPort) {
        super(hostPort, FrontendNodeType.OBSERVER);
    }
}
