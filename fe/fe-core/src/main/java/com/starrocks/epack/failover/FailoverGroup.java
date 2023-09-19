// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.epack.thrift.TFailoverGroupHandshakeRequest;
import com.starrocks.epack.thrift.TFailoverGroupHandshakeResponse;
import com.starrocks.epack.thrift.TFailoverGroupRequestMetaRequest;
import com.starrocks.epack.thrift.TFailoverGroupRequestMetaResponse;
import com.starrocks.persist.Storage;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class FailoverGroup implements Writable {
    private static final Logger LOG = LogManager.getLogger(FailoverGroup.class);

    private static final String IMAGE_SUBDIR_PREFIX = "/failover/";

    @SerializedName(value = "id")
    private final long id;

    @SerializedName(value = "name")
    private final String name;

    @SerializedName(value = "state")
    private volatile FailoverGroupState state;

    @SerializedName(value = "role")
    private volatile FailoverGroupRole role;

    @SerializedName(value = "members")
    private volatile ConcurrentHashMap<String, FailoverGroupMember> members;

    @SerializedName(value = "primary")
    private volatile FailoverGroupMember primary;

    @SerializedName(value = "comment")
    private volatile String comment;

    @SerializedName(value = "properties")
    private volatile Map<String, String> properties;

    @SerializedName(value = "schedule")
    private final ReplicationSchedule schedule;

    @SerializedName(value = "objectMgr")
    private final ReplicatedObjectMgr objectMgr;

    // For primary
    public FailoverGroup(long id, CreatePrimaryFailoverGroupStmt stmt) throws DdlException {
        this.id = id;
        this.name = stmt.getFailoverGroupName();
        this.state = FailoverGroupState.INITIALIZING;
        this.role = FailoverGroupRole.PRIMARY;
        this.members = new ConcurrentHashMap<>();
        this.primary = PrimaryHelper.initPrimaryMembers(stmt.getMembers(), this.members);
        this.comment = stmt.getComment();
        this.properties = stmt.getProperties();
        this.schedule = new ReplicationSchedule(stmt.getSchedule());
        this.objectMgr = new ReplicatedObjectMgr(stmt);
    }

    // For secondary
    public FailoverGroup(long id, CreateSecondaryFailoverGroupStmt stmt) throws DdlException {
        this.id = id;
        this.name = stmt.getFailoverGroupName();
        this.state = FailoverGroupState.INITIALIZING;
        this.role = FailoverGroupRole.SECONDARY;
        this.members = new ConcurrentHashMap<>();
        this.primary = SecondaryHelper.initSecondaryMembers(stmt.getPrimaryMember(), this.members);
        this.comment = "";
        this.properties = new HashMap<>();
        this.schedule = new ReplicationSchedule();
        this.objectMgr = new ReplicatedObjectMgr();
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public ReplicatedObjectMgr getObjectMgr() {
        return objectMgr;
    }

    /*
     * State machine, run every cycle
     */
    public void run() {
        LOG.info("Failover group run, name: {}, state: {}, role: {}, primary: {}, members: {}",
                name, state, role, primary, members);

        try {
            if (role.equals(FailoverGroupRole.PRIMARY)) {
                sendHandshakes();
            } else if (role.equals(FailoverGroupRole.SECONDARY)) {
                sendRequestMeta();
            } else {
                throw new RuntimeException("Failover group " + name + " role " + role);
            }
        } catch (Exception e) {
            LOG.warn("Failover group {} run failed ", name, e);
        }
    }

    /*
     * For secondary
     * Handle handshake rpc, primary and secondary handshake
     * RPC client: Primary FE Leader
     * RPC server: Secondary FE Leader
     */
    public TFailoverGroupHandshakeResponse handleHandshakeRequest(TFailoverGroupHandshakeRequest request)
            throws MetaNotFoundException {
        FailoverGroupMember remotePrimaryMember = FailoverGroupMember.fromThrift(request.getPrimary_member());
        FailoverGroupMember localPrimaryMember = findMember(remotePrimaryMember);
        if (localPrimaryMember == null) {
            throw new MetaNotFoundException(InternalErrorCode.META_NOT_FOUND_ERR,
                    "Primary member " + remotePrimaryMember + " not found in failover group " + name + " members "
                            + members);
        }

        if (!primary.equals(remotePrimaryMember)) {
            LOG.info("Failover group {} change primary from {} to {}", name, primary, remotePrimaryMember);
            objectMgr.clearObjectIndex();
        }

        FailoverGroup primaryFailoverGroup = FailoverGroup.fromByteArray(request.getFailover_group_meta());
        updateFromPrimary(primaryFailoverGroup);

        LOG.info("HandleHandshakeRequest, failover group: {}, state: {}, role: {}, primary: {}, members: {}",
                name, state, role, primary, members);

        TFailoverGroupHandshakeResponse response = new TFailoverGroupHandshakeResponse();
        response.setStatus(new TStatus(TStatusCode.OK));
        return response;
    }

    /*
     * For primary
     * Handle request meta rpc, secondary request meta from primary
     * RPC client: Secondary FE Leader
     * RPC server: Primary FE any node
     */
    public TFailoverGroupRequestMetaResponse handleRequestMetaRequest(TFailoverGroupRequestMetaRequest request)
            throws MetaNotFoundException, IOException {
        if (!role.equals(FailoverGroupRole.PRIMARY)) {
            throw new MetaNotFoundException(InternalErrorCode.IMPOSSIBLE_ERROR_ERR,
                    "Failover group " + name + " is not primary, current role is " + role);
        }

        if (!state.equals(FailoverGroupState.RUNNING)) {
            throw new MetaNotFoundException(InternalErrorCode.IMPOSSIBLE_ERROR_ERR,
                    "Failover group " + name + " is not running, current state is " + state);
        }

        FailoverGroupMember remoteSecondaryMember = FailoverGroupMember.fromThrift(request.getSecondary_member());
        FailoverGroupMember localSecondaryMember = findMember(remoteSecondaryMember);
        if (localSecondaryMember == null || !localSecondaryMember.getRole().equals(FailoverGroupRole.SECONDARY)) {
            throw new MetaNotFoundException(InternalErrorCode.META_NOT_FOUND_ERR,
                    "Secondary member " + remoteSecondaryMember + " not found in failover group " + name + " members "
                            + members);
        }

        if (GlobalStateMgr.getServingState().isLeader()) {
            if (!remoteSecondaryMember.equals(localSecondaryMember)) { // Update secondary member
                remoteSecondaryMember.setName(localSecondaryMember.getName());
                members.put(remoteSecondaryMember.getName(), remoteSecondaryMember);
                GlobalStateMgr.getServingState().getEditLog().logUpdateFailoverGroup(this);
            }
        }

        pushNewImage(remoteSecondaryMember.getLeader().getHost(), request.getSecondary_http_port(),
                getFailoverImageSubDir(), request.getLast_meta_version());

        TFailoverGroupRequestMetaResponse response = new TFailoverGroupRequestMetaResponse();
        response.setStatus(new TStatus(TStatusCode.OK));
        response.setPrimary_token(GlobalStateMgr.getServingState().getToken());
        return response;
    }

    /*
     * For primary
     * Push new image to secondary when receive request meta rpc request
     */
    void pushNewImage(String httpHost, int httpPort, String imageSubDir, long lastMetaVersion)
            throws IOException, MetaNotFoundException {
        String imageDir = GlobalStateMgr.getServingState().getImageDir();
        Storage storage = new Storage(imageDir);
        long imageVersion = storage.getImageJournalId();
        if (imageVersion <= lastMetaVersion) {
            if (GlobalStateMgr.getServingState().isLeader()) {
                if (schedule.needSchedule()) { // Avoid duplicate trigger new image
                    schedule.startSchedule();
                    try {
                        GlobalStateMgr.getServingState().triggerNewImage();
                        schedule.finishSchedule();
                    } catch (Exception e) {
                        schedule.cancelSchedule();
                    }
                    GlobalStateMgr.getServingState().getEditLog().logUpdateFailoverGroup(this);
                }
            }
            throw new MetaNotFoundException(InternalErrorCode.META_NOT_FOUND_ERR,
                    "Current image version " + imageVersion + " is not newer than last meta version "
                            + lastMetaVersion);
        }

        PrimaryHelper.pushImageTo(httpHost, httpPort, imageVersion, imageSubDir);
    }

    /*
     * For primary
     * Primary FE Leader send handshake rpc to all secondary FE Leader
     */
    private void sendHandshakes() {
        if (!role.equals(FailoverGroupRole.PRIMARY)) {
            return;
        }

        List<FailoverGroupMember> secondaryMembers = new ArrayList<>(members.size() - 1);
        for (FailoverGroupMember member : members.values()) {
            if (!member.getRole().equals(FailoverGroupRole.PRIMARY) &&
                    !member.getRole().equals(FailoverGroupRole.SECONDARY)) {
                secondaryMembers.add(member);
            }
        }
        if (secondaryMembers.isEmpty()) {
            return;
        }

        // Update primary member, TODO: Use listener
        FailoverGroupMember localMember = FailoverGroupMember.getLocalMember(primary.getName(), role);
        if (localMember != null) {
            members.put(localMember.getName(), localMember);
            primary = localMember;
        }

        TFailoverGroupHandshakeRequest request = new TFailoverGroupHandshakeRequest();
        request.setFailover_group_name(name);
        request.setPrimary_member(primary.toThrift());
        request.setFailover_group_meta(toByteArray());

        List<FailoverGroupMember> handshakedMembers = new ArrayList<>(secondaryMembers.size());
        for (FailoverGroupMember member : secondaryMembers) {
            if (PrimaryHelper.sendHandshakeTo(member.getLeader(), request) != null) {
                handshakedMembers.add(member);
            }
        }

        if (handshakedMembers.isEmpty()) {
            return;
        }

        for (FailoverGroupMember member : handshakedMembers) {
            member.setRole(FailoverGroupRole.SECONDARY);
        }

        state = FailoverGroupState.RUNNING;
        GlobalStateMgr.getServingState().getEditLog().logUpdateFailoverGroup(this);
    }

    /*
     * For secondary
     * Secondary FE Leader send request meta rpc to primary FE any node
     */
    private void sendRequestMeta() throws IOException, UserException {
        if (!role.equals(FailoverGroupRole.SECONDARY) || !state.equals(FailoverGroupState.RUNNING)) {
            return;
        }

        if (!schedule.needSchedule()) {
            return;
        }

        FailoverGroupMember localMember = FailoverGroupMember.getLocalMember("", role);
        if (localMember == null) {
            return;
        }

        List<NetworkAddress> primaryAddresses = new ArrayList<>(primary.getAddresses());

        int randomIndex = new Random().nextInt(primaryAddresses.size());
        NetworkAddress address = primaryAddresses.get(randomIndex);

        TFailoverGroupRequestMetaRequest request = new TFailoverGroupRequestMetaRequest();
        request.setFailover_group_name(name);
        request.setSecondary_member(localMember.toThrift());
        request.setLast_meta_version(new Storage(getFailoverImageDir(), true).getImageJournalId());
        request.setSecondary_http_port(Config.http_port);

        TFailoverGroupRequestMetaResponse response = SecondaryHelper.sendRequestMetaTo(address, request);
        if (response == null) {
            return;
        }

        try {
            ReplicatedObjectMeta objectMeta;
            try {
                GlobalStateMgr.setFailoverGroupThread();
                GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
                globalStateMgr.loadImage(getFailoverImageDir());
                objectMeta = globalStateMgr.getFailoverGroupMgr().getFailoverGroup(name).getObjectMgr()
                        .saveToObjectMeta();
                objectMeta.getSystemMeta().setToken(response.getPrimary_token()); // Token is not saved in image
            } finally {
                GlobalStateMgr.resetFailoverGroupThread();
            }

            handleObjectMeta(objectMeta);
        } finally {
            GlobalStateMgr.destroyFailoverGroupState();
        }
    }

    /*
     * For secondary
     * Handle object meta of primary
     */
    private void handleObjectMeta(ReplicatedObjectMeta objectMeta) {
        // TODO
    }

    /*
     * For both primary and secondary
     * When received a rpc request，use find member to check the request is from a
     * valid cluster
     * FE members could change, so it just check an intersection
     */
    private FailoverGroupMember findMember(FailoverGroupMember member) {
        for (FailoverGroupMember m : members.values()) {
            if (!Collections.disjoint(m.getAddresses(), member.getAddresses())) {
                return m;
            }
        }
        return null;
    }

    /*
     * For secondary
     * Update secondary failover group from primary
     */
    private void updateFromPrimary(FailoverGroup primaryFailoverGroup) {
        Preconditions.checkState(name.equals(primaryFailoverGroup.name));

        state = FailoverGroupState.RUNNING;
        role = FailoverGroupRole.SECONDARY;
        members = primaryFailoverGroup.members;
        primary = primaryFailoverGroup.primary;
        comment = primaryFailoverGroup.comment;
        properties = primaryFailoverGroup.properties;

        schedule.setSchedule(primaryFailoverGroup.schedule.getSchedule());

        for (FailoverGroupMember member : members.values()) {
            if (!member.getRole().equals(FailoverGroupRole.PRIMARY)) {
                member.setRole(FailoverGroupRole.SECONDARY);
            }
        }

        GlobalStateMgr.getServingState().getEditLog().logUpdateFailoverGroup(this);
    }

    /*
     * For secondary
     * Every failover group has a dir to store image replicated from primary
     */
    private String getFailoverImageSubDir() {
        return IMAGE_SUBDIR_PREFIX + name;
    }

    private String getFailoverImageDir() {
        return GlobalStateMgr.getServingState().getImageDir() + getFailoverImageSubDir();
    }

    public byte[] toByteArray() {
        return GsonUtils.GSON.toJson(this).getBytes();
    }

    public static FailoverGroup fromByteArray(byte[] data) {
        return GsonUtils.GSON.fromJson(new String(data), FailoverGroup.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static FailoverGroup read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), FailoverGroup.class);
    }
}
