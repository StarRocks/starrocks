// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.scheduler;

import com.starrocks.qe.ConnectContext;

public class TaskRunContext {
    ConnectContext ctx;
    String definition;
    String remoteIp;


    public ConnectContext getCtx() {
        return ctx;
    }

    public void setCtx(ConnectContext ctx) {
        this.ctx = ctx;
    }

    public String getDefinition() {
        return definition;
    }

    public void setDefinition(String definition) {
        this.definition = definition;
    }

    public String getRemoteIp() {
        return remoteIp;
    }

    public void setRemoteIp(String remoteIp) {
        this.remoteIp = remoteIp;
    }
}
