// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.epack.thrift.TFailoverGroupHandshakeRequest;
import com.starrocks.epack.thrift.TFailoverGroupRequestMetaRequest;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class FailoverGroup {
    private static final Logger LOG = LogManager.getLogger(FailoverGroup.class);

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

    // primary
    public FailoverGroup(long id, CreatePrimaryFailoverGroupStmt stmt) throws DdlException {
        this.id = id;
        this.name = stmt.getFailoverGroupName();
        this.state = FailoverGroupState.INITIALIZING;
        this.role = FailoverGroupRole.PRIMARY;
        this.members = new ConcurrentHashMap<>();
        this.primary = null;
        initMembersAndPrimary(stmt.getMembers());
        this.comment = stmt.getComment();
        this.properties = stmt.getProperties();
        this.schedule = new ReplicationSchedule(stmt.getSchedule());
        this.objectMgr = new ReplicatedObjectMgr(stmt);
    }

    // secondary
    public FailoverGroup(long id, CreateSecondaryFailoverGroupStmt stmt) throws DdlException {
        this.id = id;
        this.name = stmt.getFailoverGroupName();
        this.state = FailoverGroupState.INITIALIZING;
        this.role = FailoverGroupRole.SECONDARY;
        this.members = new ConcurrentHashMap<>();


        String[] splitStrings = stmt.getPrimaryMember().split(":");
        if (splitStrings.length != 2) {
            ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, stmt.getPrimaryMember());
        }
        NetworkAddress leaderAddress = new NetworkAddress(splitStrings[0], Integer.parseInt(splitStrings[1]));
        Set<NetworkAddress> addresses = new HashSet<>();
        addresses.add(leaderAddress);
        FailoverGroupMember member = new FailoverGroupMember();
        member.setName("");
        member.setAddresses(addresses);
        member.setLeader(leaderAddress);
        member.setRole(FailoverGroupRole.PRIMARY);

        this.members.put(member.getName(), member);
        this.primary = member;
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

    public void run() {
        LOG.info("Failover group run, name: {}, state: {}, role: {}, primary: {}, members: {}", 
                name, state, role, primary, members);

        // TODO: state machine
    }

    public void handleHandshakeRequest(TFailoverGroupHandshakeRequest request) throws UserException {
        // TODO
    }

    public byte[] handleRequestMetaRequest(TFailoverGroupRequestMetaRequest request) throws UserException, IOException {
        // TODO
        return null;
    }

    private void initMembersAndPrimary(List<String> membeStrings) throws DdlException {
        NetworkAddress primaryLeaderAddress = NetworkAddress.getLocalLeaderAddress();

        for (String membeString : membeStrings) {
            String[] splitStrings =  membeString.split(":");
            if (splitStrings.length == 2) {
                if (splitStrings[0].isEmpty() || 
                        !(splitStrings[1].equalsIgnoreCase("self") ||
                                splitStrings[1].equalsIgnoreCase("local"))) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, membeString);
                }

                FailoverGroupMember localMember = FailoverGroupMember.getLocalMember(
                        splitStrings[0], FailoverGroupRole.PRIMARY, FailoverGroupState.INITIALIZING);
                FailoverGroupMember previous = members.putIfAbsent(localMember.getName(), localMember);
                if (previous != null || primary != null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, membeString);
                }
                primary = localMember;
            } else if (splitStrings.length == 3) {
                NetworkAddress leaderAddress = new NetworkAddress(splitStrings[1], Integer.parseInt(splitStrings[2]));
                Set<NetworkAddress> addresses = new HashSet<>();
                addresses.add(leaderAddress);
                FailoverGroupMember member = new FailoverGroupMember();
                member.setName(splitStrings[0]);
                member.setAddresses(addresses);
                member.setLeader(leaderAddress);
                member.setRole(FailoverGroupRole.SECONDARY);
                member.setState(FailoverGroupState.INITIALIZING);
                FailoverGroupMember previous = members.putIfAbsent(member.getName(), member);
                if (previous != null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, membeString);
                }
                if (addresses.contains(primaryLeaderAddress)) {
                    if (primary != null) {
                        ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, membeString);
                    }
                    primary = member;
                }
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, membeString);
            }
        }

        if (primary == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, "No primary member");
        }
    }

    private FailoverGroupMember findMember(FailoverGroupMember member) {
        for (FailoverGroupMember m : members.values()) {
            if (!Collections.disjoint(m.getAddresses(), member.getAddresses())) {
                return m;
            }
        }
        return null;
    }

    private void update(FailoverGroup other) {
        Preconditions.checkState(name.equals(other.name));

        state = FailoverGroupState.RUNNING;
        role = FailoverGroupRole.SECONDARY;
        members = other.members;
        primary = other.primary;
        comment = other.comment;
        properties = other.properties;

        schedule.setSchedule(other.schedule.getSchedule());
    }

    public byte[] toByteArray() {
        return GsonUtils.GSON.toJson(this).getBytes();
    }

    public static FailoverGroup fromByteArray(byte[] data) {
        return GsonUtils.GSON.fromJson(new String(data), FailoverGroup.class);
    }
}
