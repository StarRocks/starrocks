// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.epack.persist.SRMetaBlockIDEPack;
import com.starrocks.epack.sql.ast.AlterFailoverGroupAddStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupPrimaryStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupRefreshStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupRemoveStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupResumeStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupSetStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupSuspendStmt;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.DropFailoverGroupStmt;
import com.starrocks.epack.thrift.TFailoverGroupHandshakeRequest;
import com.starrocks.epack.thrift.TFailoverGroupHandshakeResponse;
import com.starrocks.epack.thrift.TFailoverGroupRequestMetaRequest;
import com.starrocks.epack.thrift.TFailoverGroupRequestMetaResponse;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class FailoverGroupMgr extends FrontendDaemon implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(FailoverGroupMgr.class);

    @SerializedName(value = "idToFailoverGroup")
    private final Map<Long, FailoverGroup> idToFailoverGroup = Maps.newConcurrentMap();
    private final Map<String, FailoverGroup> nameToFailoverGroup = Maps.newConcurrentMap();

    public FailoverGroupMgr() {
        super("FailoverGroupMgr", Config.failover_group_interval_ms);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!GlobalStateMgr.getServingState().isLeader()) {
            return;
        }

        for (FailoverGroup failoverGroup : idToFailoverGroup.values()) {
            failoverGroup.run();
        }
    }

    public void createFailoverGroup(CreatePrimaryFailoverGroupStmt stmt) throws DdlException {
        if (RunMode.isSharedDataMode()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NOT_SUPPORTED_STATEMENT_IN_SHARED_DATA_MODE);
        }

        long nextId = GlobalStateMgr.getServingState().getNextId();
        FailoverGroup failoverGroup = new FailoverGroup(nextId, stmt);
        FailoverGroup previous = nameToFailoverGroup.putIfAbsent(failoverGroup.getName(), failoverGroup);
        if (previous != null) {
            if (stmt.getIfNotExists()) {
                return;
            }
            LOG.warn("Failover group {} already exist", failoverGroup.getName());
            ErrorReport.reportDdlException(ErrorCode.ERR_FAILOVER_GROUP_EXISTS, failoverGroup.getName());
        }

        previous = idToFailoverGroup.putIfAbsent(failoverGroup.getId(), failoverGroup);
        Preconditions.checkState(previous == null);
        GlobalStateMgr.getServingState().getEditLog().logCreateFailoverGroup(failoverGroup);
    }

    public void createFailoverGroup(CreateSecondaryFailoverGroupStmt stmt) throws DdlException {
        if (RunMode.isSharedDataMode()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NOT_SUPPORTED_STATEMENT_IN_SHARED_DATA_MODE);
        }

        long nextId = GlobalStateMgr.getServingState().getNextId();
        FailoverGroup failoverGroup = new FailoverGroup(nextId, stmt);
        FailoverGroup previous = nameToFailoverGroup.putIfAbsent(failoverGroup.getName(), failoverGroup);
        if (previous != null) {
            if (stmt.getIfNotExists()) {
                return;
            }
            LOG.warn("Failover group {} already exist", failoverGroup.getName());
            ErrorReport.reportDdlException(ErrorCode.ERR_FAILOVER_GROUP_EXISTS, failoverGroup.getName());
        }

        previous = idToFailoverGroup.putIfAbsent(failoverGroup.getId(), failoverGroup);
        Preconditions.checkState(previous == null);
        GlobalStateMgr.getServingState().getEditLog().logCreateFailoverGroup(failoverGroup);
    }

    public void dropFailoverGroup(DropFailoverGroupStmt stmt) throws DdlException {
        FailoverGroup failoverGroup = nameToFailoverGroup.remove(stmt.getFailoverGroupName());
        if (failoverGroup == null) {
            if (stmt.getIfExists()) {
                return;
            }
            LOG.warn("Failover group {} not found", stmt.getFailoverGroupName());
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_FAILOVER_GROUP, stmt.getFailoverGroupName());
        }

        boolean result = idToFailoverGroup.remove(failoverGroup.getId(), failoverGroup);
        Preconditions.checkState(result);

        failoverGroup.cancelReplication();

        GlobalStateMgr.getServingState().getEditLog().logDropFailoverGroup(failoverGroup.getId());
        LOG.info("Failover group {} is dropped, cancel all running replication jobs", stmt.getFailoverGroupName());
    }

    public void alterFailoverGroupSet(AlterFailoverGroupSetStmt stmt) throws DdlException {
        FailoverGroup failoverGroup = nameToFailoverGroup.get(stmt.getFailoverGroupName());
        if (failoverGroup == null) {
            if (stmt.getIfExists()) {
                return;
            }
            LOG.warn("Failover group {} not found", stmt.getFailoverGroupName());
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_FAILOVER_GROUP, stmt.getFailoverGroupName());
        }

        failoverGroup.alterFailoverGroupSet(stmt);
    }

    public void alterFailoverGroupAdd(AlterFailoverGroupAddStmt stmt) throws DdlException {
        FailoverGroup failoverGroup = nameToFailoverGroup.get(stmt.getFailoverGroupName());
        if (failoverGroup == null) {
            if (stmt.getIfExists()) {
                return;
            }
            LOG.warn("Failover group {} not found", stmt.getFailoverGroupName());
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_FAILOVER_GROUP, stmt.getFailoverGroupName());
        }

        failoverGroup.alterFailoverGroupAdd(stmt);
    }

    public void alterFailoverGroupRemove(AlterFailoverGroupRemoveStmt stmt) throws DdlException {
        FailoverGroup failoverGroup = nameToFailoverGroup.get(stmt.getFailoverGroupName());
        if (failoverGroup == null) {
            if (stmt.getIfExists()) {
                return;
            }
            LOG.warn("Failover group {} not found", stmt.getFailoverGroupName());
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_FAILOVER_GROUP, stmt.getFailoverGroupName());
        }

        failoverGroup.alterFailoverGroupRemove(stmt);
    }

    public void alterFailoverGroupRefresh(AlterFailoverGroupRefreshStmt stmt) throws DdlException {
        FailoverGroup failoverGroup = nameToFailoverGroup.get(stmt.getFailoverGroupName());
        if (failoverGroup == null) {
            if (stmt.getIfExists()) {
                return;
            }
            LOG.warn("Failover group {} not found", stmt.getFailoverGroupName());
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_FAILOVER_GROUP, stmt.getFailoverGroupName());
        }

        failoverGroup.refresh();
    }

    public void alterFailoverGroupPrimary(AlterFailoverGroupPrimaryStmt stmt) throws DdlException {
        FailoverGroup failoverGroup = nameToFailoverGroup.get(stmt.getFailoverGroupName());
        if (failoverGroup == null) {
            if (stmt.getIfExists()) {
                return;
            }
            LOG.warn("Failover group {} not found", stmt.getFailoverGroupName());
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_FAILOVER_GROUP, stmt.getFailoverGroupName());
        }

        failoverGroup.promoteToPrimary();
    }

    public void alterFailoverGroupSuspend(AlterFailoverGroupSuspendStmt stmt) throws DdlException {
        FailoverGroup failoverGroup = nameToFailoverGroup.get(stmt.getFailoverGroupName());
        if (failoverGroup == null) {
            if (stmt.getIfExists()) {
                return;
            }
            LOG.warn("Failover group {} not found", stmt.getFailoverGroupName());
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_FAILOVER_GROUP, stmt.getFailoverGroupName());
        }

        failoverGroup.suspend();
    }

    public void alterFailoverGroupResume(AlterFailoverGroupResumeStmt stmt) throws DdlException {
        FailoverGroup failoverGroup = nameToFailoverGroup.get(stmt.getFailoverGroupName());
        if (failoverGroup == null) {
            if (stmt.getIfExists()) {
                return;
            }
            LOG.warn("Failover group {} not found", stmt.getFailoverGroupName());
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_FAILOVER_GROUP, stmt.getFailoverGroupName());
        }

        failoverGroup.resume();
    }

    public FailoverGroup getFailoverGroup(long id) {
        return idToFailoverGroup.get(id);
    }

    public FailoverGroup getFailoverGroup(String name) {
        return nameToFailoverGroup.get(name);
    }

    public Collection<FailoverGroup> getFailoverGroups() {
        return idToFailoverGroup.values();
    }

    public TFailoverGroupHandshakeResponse handleHandshakeRequest(TFailoverGroupHandshakeRequest request)
            throws StarRocksException {
        FailoverGroup failoverGroup = nameToFailoverGroup.get(request.getFailover_group_name());
        if (failoverGroup == null) {
            TFailoverGroupHandshakeResponse response = new TFailoverGroupHandshakeResponse();
            TStatus status = new TStatus(TStatusCode.NOT_FOUND);
            status.addToError_msgs("Failover group " + request.getFailover_group_name() + " not found");
            response.setStatus(status);
            return response;
        }
        return failoverGroup.handleHandshakeRequest(request);
    }

    public TFailoverGroupRequestMetaResponse handleRequestMetaRequest(TFailoverGroupRequestMetaRequest request)
            throws StarRocksException, IOException {
        FailoverGroup failoverGroup = nameToFailoverGroup.get(request.getFailover_group_name());
        if (failoverGroup == null) {
            TFailoverGroupRequestMetaResponse response = new TFailoverGroupRequestMetaResponse();
            TStatus status = new TStatus(TStatusCode.NOT_FOUND);
            status.addToError_msgs("Failover group " + request.getFailover_group_name() + " not found");
            response.setStatus(status);
            return response;
        }
        return failoverGroup.handleRequestMetaRequest(request);
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockIDEPack.FAILOVER_GROUP_MGR, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        FailoverGroupMgr failoverGroupMgr = reader.readJson(FailoverGroupMgr.class);
        idToFailoverGroup.putAll(failoverGroupMgr.idToFailoverGroup);
        nameToFailoverGroup.putAll(failoverGroupMgr.nameToFailoverGroup);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        for (FailoverGroup failoverGroup : idToFailoverGroup.values()) {
            nameToFailoverGroup.put(failoverGroup.getName(), failoverGroup);
        }
    }

    public void replayCreateFailoverGroup(FailoverGroup failoverGroup) {
        FailoverGroup previous = idToFailoverGroup.putIfAbsent(failoverGroup.getId(), failoverGroup);
        Preconditions.checkState(previous == null);
        previous = nameToFailoverGroup.putIfAbsent(failoverGroup.getName(), failoverGroup);
        Preconditions.checkState(previous == null);
    }

    public void replayDropFailoverGroup(long failoverGroupId) {
        FailoverGroup failoverGroup = idToFailoverGroup.remove(failoverGroupId);
        Preconditions.checkState(failoverGroup != null);
        boolean result = nameToFailoverGroup.remove(failoverGroup.getName(), failoverGroup);
        Preconditions.checkState(result);
    }

    public void replayUpdateFailoverGroup(FailoverGroup failoverGroup) {
        FailoverGroup previous = idToFailoverGroup.put(failoverGroup.getId(), failoverGroup);
        Preconditions.checkState(previous != null);
        previous = nameToFailoverGroup.put(failoverGroup.getName(), failoverGroup);
        Preconditions.checkState(previous != null);
    }
}
