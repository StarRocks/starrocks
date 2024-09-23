// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.gson.annotations.SerializedName;
import com.starrocks.epack.thrift.TFailoverGroupMember;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class FailoverGroupMember {
    private static final Logger LOG = LogManager.getLogger(FailoverGroupMember.class);

    @SerializedName(value = "name")
    private volatile String name;

    @SerializedName(value = "addresses")
    private volatile Set<NetworkAddress> addresses;

    @SerializedName(value = "leader")
    private volatile NetworkAddress leader;

    @SerializedName(value = "role")
    private volatile FailoverGroupRole role;

    public FailoverGroupMember(String name, Set<NetworkAddress> addresses, NetworkAddress leader,
            FailoverGroupRole role) {
        this.name = name;
        this.addresses = addresses;
        this.leader = leader;
        this.role = role;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<NetworkAddress> getAddresses() {
        return addresses;
    }

    public void setAddresses(Set<NetworkAddress> addresses) {
        this.addresses = addresses;
    }

    public NetworkAddress getLeader() {
        return leader;
    }

    public void setLeader(NetworkAddress leader) {
        this.leader = leader;
    }

    public FailoverGroupRole getRole() {
        return role;
    }

    public void setRole(FailoverGroupRole role) {
        this.role = role;
    }

    public TFailoverGroupMember toThrift() {
        TFailoverGroupMember thriftMember = new TFailoverGroupMember();
        thriftMember.setName(name);
        for (NetworkAddress address : addresses) {
            thriftMember.addToAddresses(address.toThrift());
        }
        thriftMember.setLeader(leader.toThrift());
        thriftMember.setRole(role.toThrift());
        return thriftMember;
    }

    public static FailoverGroupMember fromThrift(TFailoverGroupMember thriftMember) {
        Set<NetworkAddress> addresses = new HashSet<>();
        for (TNetworkAddress thirftAddress : thriftMember.getAddresses()) {
            addresses.add(NetworkAddress.fromThrift(thirftAddress));
        }
        return new FailoverGroupMember(thriftMember.getName(), addresses,
                NetworkAddress.fromThrift(thriftMember.getLeader()),
                FailoverGroupRole.fromThrift(thriftMember.getRole()));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        FailoverGroupMember other = (FailoverGroupMember) obj;
        return Objects.equals(name, other.name) &&
                Objects.equals(addresses, other.addresses) &&
                Objects.equals(leader, other.leader) &&
                Objects.equals(role, other.role);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(name + "(" + role + "): [");

        boolean isFirst = true;
        for (NetworkAddress address : addresses) {
            if (isFirst) {
                isFirst = false;
            } else {
                builder.append(", ");
            }

            builder.append(address.toString());

            if (address.equals(leader)) {
                builder.append("(leader)");
            }
        }

        builder.append("]");
        return builder.toString();
    }

    public static FailoverGroupMember getLocalMember(String name, FailoverGroupRole role) {
        NetworkAddress leaderAddress = NetworkAddress.getLocalLeaderAddress();

        Set<NetworkAddress> addresses = new HashSet<>();
        List<Frontend> frontends = GlobalStateMgr.getServingState().getNodeMgr().getFrontends(null);
        for (Frontend frontend : frontends) {
            addresses.add(new NetworkAddress(frontend.getHost(), frontend.getRpcPort()));
        }

        if (!addresses.contains(leaderAddress)) {
            LOG.error("Cannot find leader: {} in frontends: {}", leaderAddress, addresses);
            return null;
        }

        return new FailoverGroupMember(name, addresses, leaderAddress, role);
    }
}
