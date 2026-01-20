// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.epack.failover.job.CheckReplicatedObjectMetaJob;
import com.starrocks.epack.failover.job.UpdateReplicatedObjectJob;
import com.starrocks.epack.sql.ast.AlterFailoverGroupAddStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupRemoveStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupSetStmt;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.epack.thrift.TFailoverGroupHandshakeRequest;
import com.starrocks.epack.thrift.TFailoverGroupHandshakeResponse;
import com.starrocks.epack.thrift.TFailoverGroupRequestMetaRequest;
import com.starrocks.epack.thrift.TFailoverGroupRequestMetaResponse;
import com.starrocks.persist.ImageLoader;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
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

    @SerializedName(value = "includeMgr")
    private volatile IncludeObjectMgr includeMgr; // Managing include objects, such as dbs or tables

    @SerializedName(value = "excludeMgr")
    private volatile ExcludeObjectMgr excludeMgr; // Managing exclude objects, such as dbs or tables

    @SerializedName(value = "errorMessages")
    private final ArrayBlockingQueue<String> errorMessages; // Save recent error messages

    private volatile ReplicatedObjectMeta objectMeta; // Replicated object meta from primary

    // Failover group job and replication job executor
    private final FailoverGroupJobExecutor jobExecutor = new FailoverGroupJobExecutor();

    // Map remote object id to local object id
    private final ReplicatedObjectMap objectMap = new ReplicatedObjectMap();

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

    public IncludeObjectMgr getIncludeMgr() {
        return includeMgr;
    }

    public ExcludeObjectMgr getExcludeMgr() {
        return excludeMgr;
    }

    public void addErrorMessage(String errorMessage) {
        while (!errorMessages.offer(errorMessage)) {
            errorMessages.poll();
        }
    }

    public Queue<String> getErrorMessages() {
        return errorMessages;
    }

    public ReplicatedObjectMeta getObjectMeta() {
        return objectMeta;
    }

    public FailoverGroupJobExecutor getJobExecutor() {
        return jobExecutor;
    }

    public ReplicatedObjectMap getObjectMap() {
        return objectMap;
    }

    public long getReplicatedJournalId() {
        try {
            return new ImageLoader(getFailoverImageDir()).getImageJournalId();
        } catch (Exception e) {
            LOG.warn("Failed to get failover image journal id in failover group {}", name);
        }
        return 0;
    }

    public FailoverGroup() {
        this.id = 0;
        this.name = null;
        this.errorMessages = null;
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
        this.includeMgr = new IncludeObjectMgr(stmt);
        this.excludeMgr = new ExcludeObjectMgr(stmt);
        this.errorMessages = new ArrayBlockingQueue<>(Config.failover_group_error_message_keep_max_num);
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
        this.includeMgr = new IncludeObjectMgr();
        this.excludeMgr = new ExcludeObjectMgr();
        this.errorMessages = new ArrayBlockingQueue<>(Config.failover_group_error_message_keep_max_num);
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
        IncludeObjectMgr newIncludeMgr = new IncludeObjectMgr(includeMgr, stmt);
        ExcludeObjectMgr newExcludeMgr = new ExcludeObjectMgr(excludeMgr, stmt);

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
        includeMgr = newIncludeMgr;
        excludeMgr = newExcludeMgr;

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
        IncludeObjectMgr newIncludeMgr = new IncludeObjectMgr(includeMgr, stmt);
        ExcludeObjectMgr newExcludeMgr = new ExcludeObjectMgr(excludeMgr, stmt);

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

        if (addMembers != null) {
            members.putAll(addMembers);
        }
        if (addProperties != null) {
            properties.putAll(addProperties);
        }
        includeMgr = newIncludeMgr;
        excludeMgr = newExcludeMgr;

        triggerNewHandshakes();
        GlobalStateMgr.getServingState().getEditLog().logUpdateFailoverGroup(this);
    }

    // For primary
    public void alterFailoverGroupRemove(AlterFailoverGroupRemoveStmt stmt) throws DdlException {
        if (!role.equals(FailoverGroupRole.PRIMARY)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FAILOVER_GROUP_ROLE, role);
        }

        List<String> removeMembers = stmt.getMembers();
        IncludeObjectMgr newIncludeMgr = new IncludeObjectMgr(includeMgr, stmt);
        ExcludeObjectMgr newExcludeMgr = new ExcludeObjectMgr(excludeMgr, stmt);

        if (removeMembers != null) {
            for (String memberName : removeMembers) {
                if (!members.containsKey(memberName)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER,
                            "member " + memberName + " not found");
                }
            }
        }

        if (removeMembers != null) {
            for (String memberName : removeMembers) {
                members.remove(memberName);
            }
        }
        includeMgr = newIncludeMgr;
        excludeMgr = newExcludeMgr;

        triggerNewHandshakes();
        GlobalStateMgr.getServingState().getEditLog().logUpdateFailoverGroup(this);
    }

    // For primary and secondary
    public void refresh() throws DdlException {
        if (role.equals(FailoverGroupRole.PRIMARY)) {
            triggerNewHandshakes();
        } else if (role.equals(FailoverGroupRole.SECONDARY)) {
            schedule.forceSchedule();
            GlobalStateMgr.getServingState().getEditLog().logUpdateFailoverGroup(this);
        } else {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FAILOVER_GROUP_ROLE, role);
        }
    }

    /*
     * For secondary
     * Promote secondary to primary
     */
    public void promoteToPrimary() throws DdlException {
        if (!role.equals(FailoverGroupRole.SECONDARY)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FAILOVER_GROUP_ROLE, role);
        }

        if (state.equals(FailoverGroupState.INITIALIZING)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FAILOVER_GROUP_STATEMENT, "Failover group is not ready");
        }

        FailoverGroupMember newLocalMember = FailoverGroupMember.getLocalMember(null, FailoverGroupRole.PRIMARY);
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
        cancelReplication();

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
                checkJobsFinished();
            } else {
                throw new RuntimeException("Failover group " + name + " role " + role);
            }
        } catch (Exception e) {
            LOG.warn("Failover group {} run failed ", name, e);
            addErrorMessage(e.getMessage());
        }
    }

    /*
     * For secondary
     * Handle handshake rpc, primary and secondary handshake
     * RPC client: Primary FE Leader
     * RPC server: Secondary FE Leader
     */
    public TFailoverGroupHandshakeResponse handleHandshakeRequest(TFailoverGroupHandshakeRequest request) {
        FailoverGroupMember remotePrimaryMember = FailoverGroupMember.fromThrift(request.getPrimary_member());
        FailoverGroupMember localPrimaryMember = findMember(remotePrimaryMember);
        if (localPrimaryMember == null) {
            TFailoverGroupHandshakeResponse response = new TFailoverGroupHandshakeResponse();
            TStatus status = new TStatus(TStatusCode.NOT_FOUND);
            status.addToError_msgs("Primary member " + remotePrimaryMember +
                    " not found in failover group " + name +
                    ", members: " + members);
            response.setStatus(status);
            return response;
        }

        if (!primary.getName().equals(remotePrimaryMember.getName())) {
            cancelReplication();
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
            throws IOException {
        if (!role.equals(FailoverGroupRole.PRIMARY)) {
            TFailoverGroupRequestMetaResponse response = new TFailoverGroupRequestMetaResponse();
            TStatus status = new TStatus(TStatusCode.ILLEGAL_STATE);
            status.addToError_msgs("Failover group " + name +
                    " is not primary, current role is " + role +
                    ", members: " + members);
            response.setStatus(status);
            return response;
        }

        if (state.equals(FailoverGroupState.INITIALIZING)) {
            TFailoverGroupRequestMetaResponse response = new TFailoverGroupRequestMetaResponse();
            TStatus status = new TStatus(TStatusCode.ILLEGAL_STATE);
            status.addToError_msgs("Failover group " + name +
                    " is not ready, current state is " + state +
                    ", members: " + members);
            response.setStatus(status);
            return response;
        }

        FailoverGroupMember remoteSecondaryMember = FailoverGroupMember.fromThrift(request.getSecondary_member());
        FailoverGroupMember localSecondaryMember = findMember(remoteSecondaryMember);
        if (localSecondaryMember == null) {
            TFailoverGroupRequestMetaResponse response = new TFailoverGroupRequestMetaResponse();
            TStatus status = new TStatus(TStatusCode.NOT_FOUND);
            status.addToError_msgs("Secondary member " + remoteSecondaryMember +
                    " not found in failover group " + name +
                    ", members " + members);
            response.setStatus(status);
            return response;
        }

        if (GlobalStateMgr.getServingState().isLeader()) {
            remoteSecondaryMember.setName(localSecondaryMember.getName()); // May no name in remoteSecondaryMember
            if (!remoteSecondaryMember.equals(localSecondaryMember)) { // Update secondary member
                members.put(remoteSecondaryMember.getName(), remoteSecondaryMember);
                triggerNewHandshakes();
                GlobalStateMgr.getServingState().getEditLog().logUpdateFailoverGroup(this);
            }
        }

        long imageVersion = new ImageLoader(GlobalStateMgr.getImageDirPath()).getImageJournalId();
        if (imageVersion <= request.getLast_meta_version()) {
            triggerNewImage();
            TFailoverGroupRequestMetaResponse response = new TFailoverGroupRequestMetaResponse();
            TStatus status = new TStatus(TStatusCode.REMOTE_FILE_NOT_FOUND);
            status.addToError_msgs("Current image version " + imageVersion +
                    " is not newer than last meta version " + request.getLast_meta_version());
            response.setStatus(status);
            return response;
        }

        TFailoverGroupRequestMetaResponse response = new TFailoverGroupRequestMetaResponse();
        response.setStatus(new TStatus(TStatusCode.OK));
        response.setMeta_version(imageVersion);
        response.setPrimary_token(GlobalStateMgr.getServingState().getToken());
        response.setPrimary_http_port(Config.http_port);
        return response;
    }

    /*
     * For primary
     * Trigger new image
     */
    private void triggerNewImage() {
        if (GlobalStateMgr.getServingState().isLeader()) {
            // Avoid duplicate trigger new image
            if (schedule.canSchedule(Config.failover_group_trigger_new_image_interval_sec * 1000)) {
                schedule.startSchedule();
                GlobalStateMgr.getServingState().triggerNewImage();
                schedule.finishSchedule(false);

                GlobalStateMgr.getServingState().getEditLog().logUpdateFailoverGroup(this);
            }
        }
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
            errorMessages.clear();
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
                LOG.info("Failover group {} succeeded to handshake to {}, members: {}",
                        name, member.getLeader(), members);
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
        errorMessages.clear();
        GlobalStateMgr.getServingState().getEditLog().logUpdateFailoverGroup(this);
    }

    /*
     * For secondary
     * Secondary FE Leader send request meta rpc to primary FE any node
     */
    private void sendRequestMeta() throws IOException {
        if (!role.equals(FailoverGroupRole.SECONDARY) || state.equals(FailoverGroupState.INITIALIZING)) {
            return;
        }

        if (!schedule.needSchedule()) {
            return;
        }

        FailoverGroupMember localMember = FailoverGroupMember.getLocalMember(null, role);
        if (localMember == null) {
            return;
        }

        List<NetworkAddress> primaryAddresses = new ArrayList<>(primary.getAddresses());

        int randomIndex = new Random().nextInt(primaryAddresses.size());
        NetworkAddress address = primaryAddresses.get(randomIndex);

        TFailoverGroupRequestMetaRequest request = new TFailoverGroupRequestMetaRequest();
        request.setFailover_group_name(name);
        request.setSecondary_member(localMember.toThrift());
        long lastMetaVersion = new ImageLoader(getFailoverImageDir()).getImageJournalId();
        request.setLast_meta_version(lastMetaVersion);

        TFailoverGroupRequestMetaResponse response = SecondaryHelper.sendRequestMetaTo(address, request);
        if (response == null || response.getMeta_version() <= lastMetaVersion) {
            return;
        }

        if (!SecondaryHelper.pullImage(response.getPrimary_token(), address.getHost(), response.getPrimary_http_port(),
                response.getMeta_version(), getFailoverImageSubDir())) {
            addErrorMessage("Failed to pull image from primary");
            return;
        }

        LOG.info("Failover group {} succeeded to pull image {} from {} in primary {}, finished round: {}",
                name, response.getMeta_version(), address, primary, schedule.getRoundFinishedTimes());

        try {
            GlobalStateMgr.setFailoverGroupThread();
            GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
            globalStateMgr.loadImage(getFailoverImageDir());
            objectMeta = globalStateMgr.getFailoverGroupMgr().getFailoverGroup(name)
                    .getIncludeMgr().toObjectMeta(response.getPrimary_token()); // Token is not saved in image
        } catch (Exception e) {
            addErrorMessage("Failed to load image: " + e.getMessage());
            LOG.warn("Failover group {} load image {} failed: ",
                    name, response.getMeta_version(), e);
            return;
        } finally {
            GlobalStateMgr.resetFailoverGroupThread();
            GlobalStateMgr.destroyFailoverGroupState();
        }

        handleObjectMeta();
    }

    /*
     * For secondary
     * Handle object meta replicated from primary
     */
    private void handleObjectMeta() {
        if (!role.equals(FailoverGroupRole.SECONDARY) || state.equals(FailoverGroupState.INITIALIZING)) {
            return;
        }

        state = FailoverGroupState.REPLICATING;
        schedule.startSchedule();
        errorMessages.clear();

        CheckReplicatedObjectMetaJob job = new CheckReplicatedObjectMetaJob(this);
        job.start();

        GlobalStateMgr.getServingState().getEditLog().logUpdateFailoverGroup(this);
        LOG.info("Failover group {} start replication from primary {}, finished round: {}",
                name, primary, schedule.getRoundFinishedTimes());
    }

    /*
     * For secondary
     * Check jobs finished
     */
    private void checkJobsFinished() {
        if (!schedule.isRoundPending()) {
            return;
        }

        if (!jobExecutor.isAllJobsFinished()) {
            return;
        }

        if (state.equals(FailoverGroupState.REPLICATING)) {
            handleReplicatingFinished();
        } else {
            handleUpdatingFinished();
        }
    }

    /*
     * For secondary
     * Do some work when replication finished
     */
    private void handleReplicatingFinished() {
        // Update include object mgr
        IncludeObjectMgr primaryIncludeMgr = objectMeta != null ? objectMeta.getIncludeMgr() : null;
        if (primaryIncludeMgr != null) {
            includeMgr = IncludeObjectMgr.fromPrimaryIncludeMgr(primaryIncludeMgr, objectMap);
        }

        state = FailoverGroupState.UPDATING;

        UpdateReplicatedObjectJob job = new UpdateReplicatedObjectJob(this);
        job.start();

        GlobalStateMgr.getServingState().getEditLog().logUpdateFailoverGroup(this);
        LOG.info("Failover group {} finished replication and start updating from primary {}, finished round: {}",
                name, primary, schedule.getRoundFinishedTimes());
    }

    /*
     * For secondary
     * Do some work when updating finished
     */
    private void handleUpdatingFinished() {
        boolean needRetry = jobExecutor.hasFailedJobs();
        schedule.finishSchedule(needRetry);
        state = FailoverGroupState.RUNNING;
        objectMeta = null;
        jobExecutor.clear();
        objectMap.clear();
        GlobalStateMgr.getServingState().getEditLog().logUpdateFailoverGroup(this);
        LOG.info("Failover group {} finished updating from primary {}, finished round: {}, need retry: {}",
                name, primary, schedule.getRoundFinishedTimes(), needRetry);
    }

    /*
     * For secondary
     * Cancel replication
     */
    public void cancelReplication() {
        schedule.cancelSchedule();
        objectMeta = null;
        jobExecutor.clear();
        objectMap.clear();
    }

    /*
     * For both primary and secondary
     * Check if the member is from a valid cluster
     * FE members could change, so it just check an intersection
     */
    private FailoverGroupMember findMember(FailoverGroupMember member) {
        for (FailoverGroupMember m : members.values()) {
            if (!Collections.disjoint(m.getAddresses(), member.getAddresses())) {
                return m;
            }
        }

        LOG.warn("Failover group {} cannot find member: {} in members: {}", name, member, members);
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
    private void updateFromPrimary(FailoverGroup primaryFailoverGroup) {
        Preconditions.checkState(name.equals(primaryFailoverGroup.name));

        state = FailoverGroupState.RUNNING;
        role = FailoverGroupRole.SECONDARY;
        members = primaryFailoverGroup.members;
        primary = primaryFailoverGroup.primary;
        comment = primaryFailoverGroup.comment;
        properties = primaryFailoverGroup.properties;

        try {
            ReplicationSchedule newSchedule = new ReplicationSchedule(
                    primaryFailoverGroup.schedule.getSchedule(), schedule);
            schedule = newSchedule;
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }

        // Not update includeMgr, includeMgr uses id and is different from primary
        excludeMgr = primaryFailoverGroup.excludeMgr;

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
        return GlobalStateMgr.getImageDirPath() + getFailoverImageSubDir();
    }

    public byte[] toByteArray() {
        return GsonUtils.GSON.toJson(this).getBytes(StandardCharsets.UTF_8);
    }

    public static FailoverGroup fromByteArray(byte[] data) {
        return GsonUtils.GSON.fromJson(new String(data, StandardCharsets.UTF_8), FailoverGroup.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static FailoverGroup read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), FailoverGroup.class);
    }
}
