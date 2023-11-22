// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.epack.sql.ast.AlterFailoverGroupAddStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupRemoveStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupSetStmt;
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

/*
 * Main class of a failover group
 * One instance per failover group
 */
public class FailoverGroup implements Writable {
    private static final Logger LOG = LogManager.getLogger(FailoverGroup.class);

    private static final String IMAGE_SUBDIR_PREFIX = "/failover/";

    @SerializedName(value = "id")
    private final long id; // Id of failover group

    @SerializedName(value = "name")
    private final String name; // Name of failover group

    @SerializedName(value = "state")
    private volatile FailoverGroupState state; // Current state of failover group

    @SerializedName(value = "role")
    private volatile FailoverGroupRole role; // Current role of failover group, such as primary or secondary

    @SerializedName(value = "members")
    private volatile ConcurrentHashMap<String, FailoverGroupMember> members; // Members of failover group

    @SerializedName(value = "primary")
    private volatile FailoverGroupMember primary; // Primary member of failover group

    @SerializedName(value = "comment")
    private volatile String comment; // User comment in creating failover group

    @SerializedName(value = "properties")
    private volatile Map<String, String> properties; // Properties in creating failover group, currently not used

    @SerializedName(value = "schedule")
    private volatile ReplicationSchedule schedule; // Managing replication period

    @SerializedName(value = "objectMgr")
    private volatile ReplicatedObjectMgr objectMgr; // Managing replicated object, such as dbs or tables

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public FailoverGroupState getState() {
        return state;
    }

    public FailoverGroupRole getRole() {
        return role;
    }

    public ConcurrentHashMap<String, FailoverGroupMember> getMembers() {
        return members;
    }

    public String getComment() {
        return comment;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public ReplicationSchedule getSchedule() {
        return schedule;
    }

    public ReplicatedObjectMgr getObjectMgr() {
        return objectMgr;
    }

    // For primary
    public FailoverGroup(long id, CreatePrimaryFailoverGroupStmt stmt) throws DdlException {
        this.id = id;
        this.name = stmt.getFailoverGroupName();
        this.state = FailoverGroupState.INITIALIZING;
        this.role = FailoverGroupRole.PRIMARY;
        this.members = new ConcurrentHashMap<>();
        this.primary = PrimaryHelper.initPrimaryMembers(stmt.getMembers(), this.members);
        this.comment = Strings.nullToEmpty(stmt.getComment());
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

    // For primary
    public void alterFailoverGroupSet(AlterFailoverGroupSetStmt stmt) throws DdlException {
        if (!role.equals(FailoverGroupRole.PRIMARY)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FAILOVER_GROUP_ROLE, role);
        }

        ConcurrentHashMap<String, FailoverGroupMember> newMembers = null;
        FailoverGroupMember newPrimary = null;
        String newComment = stmt.getComment();
        Map<String, String> newProperties = stmt.getProperties();
        ReplicationSchedule newSchedule = null;
        ReplicatedObjectMgr newObjectMgr = new ReplicatedObjectMgr(stmt, objectMgr);

        if (stmt.getMembers() != null) {
            newMembers = new ConcurrentHashMap<>();
            newPrimary = PrimaryHelper.initPrimaryMembers(stmt.getMembers(), newMembers);
        }
        if (stmt.getSchedule() != null) {
            newSchedule = new ReplicationSchedule(stmt.getSchedule(), schedule);
        }

        if (newMembers != null) {
            members = newMembers;
            primary = newPrimary;
        }
        if (newComment != null) {
            comment = newComment;
        }
        if (newProperties != null) {
            properties.putAll(newProperties);
        }
        if (newSchedule != null) {
            schedule = newSchedule;
        }
        objectMgr = newObjectMgr;

        triggerNewHandshakes();
        GlobalStateMgr.getServingState().getEditLog().logUpdateFailoverGroup(this);
    }

    // For primary
    public void alterFailoverGroupAdd(AlterFailoverGroupAddStmt stmt) throws DdlException {
        if (!role.equals(FailoverGroupRole.PRIMARY)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FAILOVER_GROUP_ROLE, role);
        }

        Map<String, FailoverGroupMember> addMembers = null;
        Map<String, String> addProperties = stmt.getProperties();
        ReplicatedObjectMgr addObjectMgr = new ReplicatedObjectMgr(stmt);

        if (stmt.getMembers() != null) {
            addMembers = PrimaryHelper.initMembers(stmt.getMembers());
        }

        if (addMembers != null) {
            for (String memberName : addMembers.keySet()) {
                if (members.containsKey(memberName)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER,
                            "member " + memberName + " already exist");
                }
            }
        }

        objectMgr.addObjects(addObjectMgr);
        if (addMembers != null) {
            members.putAll(addMembers);
        }
        if (addProperties != null) {
            properties.putAll(addProperties);
        }

        triggerNewHandshakes();
        GlobalStateMgr.getServingState().getEditLog().logUpdateFailoverGroup(this);
    }

    // For primary
    public void alterFailoverGroupRemove(AlterFailoverGroupRemoveStmt stmt) throws DdlException {
        if (!role.equals(FailoverGroupRole.PRIMARY)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FAILOVER_GROUP_ROLE, role);
        }

        List<String> removeMembers = stmt.getMembers();
        ReplicatedObjectMgr removeObjectMgr = new ReplicatedObjectMgr(stmt);

        if (removeMembers != null) {
            for (String memberName : removeMembers) {
                if (!members.containsKey(memberName)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER,
                            "member " + memberName + " not found");
                }
            }
        }

        objectMgr.removeObjects(removeObjectMgr);
        if (removeMembers != null) {
            for (String memberName : removeMembers) {
                members.remove(memberName);
            }
        }

        triggerNewHandshakes();
        GlobalStateMgr.getServingState().getEditLog().logUpdateFailoverGroup(this);
    }

    // For secondary
    public void refresh() throws DdlException {
        if (!role.equals(FailoverGroupRole.SECONDARY)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FAILOVER_GROUP_ROLE, role);
        }

        schedule.forceSchedule();
        GlobalStateMgr.getServingState().getEditLog().logUpdateFailoverGroup(this);
    }

    /*
     * For secondary
     * Promote secondary to primary
     */
    public void promoteToPrimary() throws DdlException {
        if (!role.equals(FailoverGroupRole.SECONDARY)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FAILOVER_GROUP_ROLE, role);
        }

        if (!state.equals(FailoverGroupState.RUNNING)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FAILOVER_GROUP_STATEMENT, "Falover group is not ready");
        }

        FailoverGroupMember newLocalMember = FailoverGroupMember.getLocalMember("", FailoverGroupRole.PRIMARY);
        if (newLocalMember == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FAILOVER_GROUP_STATEMENT, "Cannot get local member");
        }

        FailoverGroupMember oldLocalMember = findMember(newLocalMember);
        if (oldLocalMember == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FAILOVER_GROUP_STATEMENT, "Cannot find local member");
        }

        newLocalMember.setName(oldLocalMember.getName());

        for (FailoverGroupMember member : members.values()) {
            member.setRole(FailoverGroupRole.NONE);
        }

        members.put(newLocalMember.getName(), newLocalMember);

        primary = newLocalMember;
        state = FailoverGroupState.INITIALIZING;
        role = FailoverGroupRole.PRIMARY;

        GlobalStateMgr.getServingState().getEditLog().logUpdateFailoverGroup(this);
        LOG.info("Failover group promoted to primary, name: {}, members: {}", name, members);
    }

    // For secondary
    public void suspend() throws DdlException {
        if (!role.equals(FailoverGroupRole.SECONDARY)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FAILOVER_GROUP_ROLE, role);
        }

        schedule.suspend();
        GlobalStateMgr.getServingState().getEditLog().logUpdateFailoverGroup(this);
    }

    // For secondary
    public void resume() throws DdlException {
        if (!role.equals(FailoverGroupRole.SECONDARY)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FAILOVER_GROUP_ROLE, role);
        }

        schedule.resume();
        GlobalStateMgr.getServingState().getEditLog().logUpdateFailoverGroup(this);
    }

    /*
     * State machine, run every cycle
     */
    public void run() {
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
            throws UserException {
        FailoverGroupMember remotePrimaryMember = FailoverGroupMember.fromThrift(request.getPrimary_member());
        FailoverGroupMember localPrimaryMember = findMember(remotePrimaryMember);
        if (localPrimaryMember == null) {
            throw new MetaNotFoundException(InternalErrorCode.META_NOT_FOUND_ERR,
                    "Primary member " + remotePrimaryMember + " not found in failover group " + name + " members "
                            + members);
        }

        if (!primary.equals(remotePrimaryMember)) {
            objectMgr.clearObjectIndex();
            LOG.info("Failover group {} change primary from {} to {}", name, primary, remotePrimaryMember);
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
            throws UserException, IOException {
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
            remoteSecondaryMember.setName(localSecondaryMember.getName()); // May no name is remoteSecondaryMember
            if (!remoteSecondaryMember.equals(localSecondaryMember)) { // Update secondary member
                members.put(remoteSecondaryMember.getName(), remoteSecondaryMember);
                triggerNewHandshakes();
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
    private void pushNewImage(String httpHost, int httpPort, String imageSubDir, long lastMetaVersion)
            throws IOException, UserException {
        String imageDir = GlobalStateMgr.getServingState().getImageDir();
        Storage storage = new Storage(imageDir);
        long imageVersion = storage.getImageJournalId();
        if (imageVersion <= lastMetaVersion) {
            if (GlobalStateMgr.getServingState().isLeader()) {
                if (schedule.needSchedule()) { // Avoid duplicate trigger new image
                    schedule.startSchedule();
                    GlobalStateMgr.getServingState().triggerNewImage();
                    schedule.finishSchedule(false);

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

        LOG.warn("Cannot find member: {} in members: {}", member, members);
        return null;
    }

    /*
     * For primary
     * Trigger new handshakes to update failover group meta
     */
    private void triggerNewHandshakes() {
        if (!role.equals(FailoverGroupRole.PRIMARY)) {
            return;
        }

        for (FailoverGroupMember member : members.values()) {
            if (member.getRole().equals(FailoverGroupRole.SECONDARY)) {
                member.setRole(FailoverGroupRole.NONE);
            }
        }
    }

    /*
     * For secondary
     * Update secondary failover group from primary
     */
    private void updateFromPrimary(FailoverGroup primaryFailoverGroup) throws DdlException {
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
