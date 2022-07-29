// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.ha;

public interface StateChangeExecution {
    public void transferToLeader();
    public void transferToNonLeader(FrontendNodeType newType);
}
