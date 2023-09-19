// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.epack.persist.SRMetaBlockIDEPack;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.epack.thrift.TFailoverGroupHandshakeRequest;
import com.starrocks.epack.thrift.TFailoverGroupHandshakeResponse;
import com.starrocks.epack.thrift.TFailoverGroupRequestMetaRequest;
import com.starrocks.epack.thrift.TFailoverGroupRequestMetaResponse;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

public class FailoverGroupMgr extends FrontendDaemon implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(FailoverGroupMgr.class);

    @SerializedName(value = "idToFailoverGroup")
    private final Map<Long, FailoverGroup> idToFailoverGroup = Maps.newConcurrentMap();
    private final Map<String, FailoverGroup> nameToFailoverGroup = Maps.newConcurrentMap();

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

    public FailoverGroup getFailoverGroup(long id) throws MetaNotFoundException {
        FailoverGroup failoverGroup = idToFailoverGroup.get(id);
        if (failoverGroup == null) {
            throw new MetaNotFoundException(InternalErrorCode.META_NOT_FOUND_ERR,
                    "Failover group " + id + " not found");
        }
        return failoverGroup;
    }

    public FailoverGroup getFailoverGroup(String name) throws MetaNotFoundException {
        FailoverGroup failoverGroup = nameToFailoverGroup.get(name);
        if (failoverGroup == null) {
            throw new MetaNotFoundException(InternalErrorCode.META_NOT_FOUND_ERR,
                    "Failover group " + name + " not found");
        }
        return failoverGroup;
    }

    public TFailoverGroupHandshakeResponse handleHandshakeRequest(TFailoverGroupHandshakeRequest request)
            throws MetaNotFoundException {
        FailoverGroup failoverGroup = getFailoverGroup(request.getFailover_group_name());
        return failoverGroup.handleHandshakeRequest(request);
    }

    public TFailoverGroupRequestMetaResponse handleRequestMetaRequest(TFailoverGroupRequestMetaRequest request)
            throws MetaNotFoundException, IOException {
        FailoverGroup failoverGroup = getFailoverGroup(request.getFailover_group_name());
        return failoverGroup.handleRequestMetaRequest(request);
    }

    public void save(DataOutputStream dos) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SRMetaBlockIDEPack.FAILOVER_GROUP_MGR, 1);
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
