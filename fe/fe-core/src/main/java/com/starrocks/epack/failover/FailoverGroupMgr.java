// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.UserException;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.epack.thrift.TFailoverGroupHandshakeRequest;
import com.starrocks.epack.thrift.TFailoverGroupRequestMetaRequest;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class FailoverGroupMgr extends FrontendDaemon implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(FailoverGroupMgr.class);

    @SerializedName(value = "idToFailoverGroup")
    private Map<Long, FailoverGroup> idToFailoverGroup = Maps.newConcurrentMap();
    private Map<String, FailoverGroup> nameToFailoverGroup = Maps.newConcurrentMap();

    public FailoverGroupMgr() {
        super("FailoverGroupMgr", 1000L);
    }

    @Override
    protected void runAfterCatalogReady() {
        for (FailoverGroup failoverGroup : idToFailoverGroup.values()) {
            failoverGroup.run();
        }
    }

    public void createFailoverGroup(CreatePrimaryFailoverGroupStmt stmt) throws DdlException {
        long nextId = GlobalStateMgr.getCurrentState().getNextId();
        FailoverGroup failoverGroup = new FailoverGroup(nextId, stmt);
        FailoverGroup previous = nameToFailoverGroup.putIfAbsent(failoverGroup.getName(), failoverGroup);
        if (previous != null) {
            if (stmt.getIfNotExists()) {
                return;
            }
            LOG.warn("Failover group {} already exist", failoverGroup.getName());
            ErrorReport.reportDdlException(ErrorCode.ERR_FAILOVER_GROUP_EXISTS,
                    "Failover group " + failoverGroup.getName() + " already exist");
        }
        
        previous = idToFailoverGroup.putIfAbsent(failoverGroup.getId(), failoverGroup);
        Preconditions.checkState(previous == null);
    }

    public void createFailoverGroup(CreateSecondaryFailoverGroupStmt stmt) throws DdlException {
        long nextId = GlobalStateMgr.getCurrentState().getNextId();
        FailoverGroup failoverGroup = new FailoverGroup(nextId, stmt);
        FailoverGroup previous = nameToFailoverGroup.putIfAbsent(failoverGroup.getName(), failoverGroup);
        if (previous != null) {
            if (stmt.getIfNotExists()) {
                return;
            }
            LOG.warn("Failover group {} already exist", failoverGroup.getName());
            ErrorReport.reportDdlException(ErrorCode.ERR_FAILOVER_GROUP_EXISTS,
                    "Failover group " + failoverGroup.getName() + " already exist");
        }
        
        previous = idToFailoverGroup.putIfAbsent(failoverGroup.getId(), failoverGroup);
        Preconditions.checkState(previous == null);
    }

    public void handleHandshakeRequest(TFailoverGroupHandshakeRequest request) throws UserException {
        FailoverGroup failoverGroup = nameToFailoverGroup.get(request.getFailover_group_name());
        if (failoverGroup == null) {
            LOG.warn("Failover group {} not found", request.getFailover_group_name());
            throw new UserException(InternalErrorCode.META_NOT_FOUND_ERR, 
                    "Failover group " + request.getFailover_group_name() + " not found");
        }

        failoverGroup.handleHandshakeRequest(request);
    }

    public byte[] handleRequestMetaRequest(TFailoverGroupRequestMetaRequest request) throws UserException, IOException {
        FailoverGroup failoverGroup = nameToFailoverGroup.get(request.getFailover_group_name());
        if (failoverGroup == null) {
            LOG.warn("Failover group {} not found", request.getFailover_group_name());
            throw new UserException(InternalErrorCode.META_NOT_FOUND_ERR, 
                    "Failover group " + request.getFailover_group_name() + " not found");
        }

        return failoverGroup.handleRequestMetaRequest(request);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        for (FailoverGroup failoverGroup : idToFailoverGroup.values()) {
            nameToFailoverGroup.put(failoverGroup.getName(), failoverGroup);
        }
    }
}
